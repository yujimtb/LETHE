#!/usr/bin/env python3
"""Seed a production-scale synthetic corpus into a LETHE self-host instance.

This drives POST /api/import/observation-drafts (v1) with discord-message
shaped drafts (see gate_common.build_corpus_draft), at the volume that
exposed the v15 per-import memory leak (~568k observations). It is intended
to be pointed at a throwaway, loopback-only gate container
(`run_gate.py --data-dir` reads the resulting data directory) — never at a
production host.

Re-running with the same --tag/--source-instance/--count is idempotent: the
same (tag, index) always reproduces the same idempotency_key, so replays
land as `duplicate`, not fresh `ingested` rows.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import gate_common as gc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, help="e.g. http://127.0.0.1:18098")
    parser.add_argument("--env-file", required=True, type=Path, help="deploy .env-style file")
    parser.add_argument("--count", type=int, default=568_000)
    parser.add_argument("--batch", type=int, default=1000)
    parser.add_argument("--source-instance", default="device:gate-probe")
    parser.add_argument("--tag", required=True, help="corpus tag; namespaces channel/message ids")
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=gc.DEFAULT_PAD_BYTES,
        help="extra padding bytes appended to each message body",
    )
    parser.add_argument(
        "--token-env",
        default="LETHE_API_WRITE_TOKEN",
        help="key name inside --env-file holding the write:observations bearer token",
    )
    parser.add_argument("--timeout", type=float, default=120.0, help="per-batch HTTP timeout (s)")
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="first corpus index to send (for resuming a partial seed)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count <= 0:
        raise gc.GateError("--count must be positive")
    if args.batch <= 0:
        raise gc.GateError("--batch must be positive")
    if args.start_index < 0 or args.start_index >= args.count:
        raise gc.GateError("--start-index must be in [0, --count)")

    env = gc.read_env_file(args.env_file)
    token = gc.require_env_value(env, args.token_env)
    client = gc.ImportClient(args.base_url, token, default_timeout=args.timeout)

    total = gc.OutcomeCounts()
    last_progress_mark = args.start_index - (args.start_index % 10_000)
    started_at = time.monotonic()

    index = args.start_index
    while index < args.count:
        batch_end = min(index + args.batch, args.count)
        drafts = [
            gc.build_corpus_draft(
                tag=args.tag,
                source_instance_id=args.source_instance,
                index=i,
                pad_bytes=args.payload_bytes,
            )
            for i in range(index, batch_end)
        ]
        try:
            result = client.send_drafts(args.source_instance, drafts)
        except gc.GateError as error:
            print(f"seed failed at index {index}: {error}", file=sys.stderr)
            return 1

        gc.assert_no_failures(
            result.counts, context=f"batch [{index}, {batch_end})"
        )
        if result.counts.ingested + result.counts.duplicates != len(drafts):
            print(
                f"seed failed at index {index}: expected {len(drafts)} "
                f"ingested+duplicate, got {result.counts.as_dict()}",
                file=sys.stderr,
            )
            return 1

        total = total + result.counts
        index = batch_end

        if index - last_progress_mark >= 10_000:
            last_progress_mark = index - (index % 10_000)
            elapsed = time.monotonic() - started_at
            rate = index / elapsed if elapsed > 0 else 0.0
            print(
                f"seed progress: {index}/{args.count} "
                f"(ingested={total.ingested} duplicates={total.duplicates}) "
                f"elapsed={elapsed:.1f}s rate={rate:.0f}/s"
            )

    elapsed = time.monotonic() - started_at
    print(
        f"seed complete: {index}/{args.count} "
        f"ingested={total.ingested} duplicates={total.duplicates} "
        f"elapsed={elapsed:.1f}s"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except gc.GateError as error:
        print(f"seed_gate_corpus failed: {error}", file=sys.stderr)
        raise SystemExit(1)
