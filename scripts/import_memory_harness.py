#!/usr/bin/env python3
"""Linux-only RSS/VmHWM acceptance harness for v15 bounded imports.

The import command must accept a JSONL corpus path through the ``{corpus}``
placeholder and print ``publish_count=<integer>`` when running in CI mode.
This script never reads configuration or secrets; it creates synthetic input
in a temporary directory and runs only the explicitly supplied command.
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path


def current_rss_bytes() -> int:
    status = Path("/proc/self/status")
    for line in status.read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("/proc/self/status has no VmRSS; Linux is required")


def synthetic_corpus(path: Path, count: int, payload_bytes: int) -> None:
    payload = "x" * payload_bytes
    with path.open("w", encoding="utf-8") as handle:
        for index in range(count):
            handle.write(
                json.dumps(
                    {
                        "client_ref": str(index),
                        "object_id": f"memory-harness:{index}",
                        "payload": payload,
                    },
                    separators=(",", ":"),
                )
                + "\n"
            )


def maximum_resident_set_bytes(stderr: str) -> int:
    match = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", stderr)
    if match is None:
        raise RuntimeError("/usr/bin/time did not report Maximum resident set size")
    return int(match.group(1)) * 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("count", type=int, help="synthetic corpus size N")
    parser.add_argument("--batch-size", type=int, required=True)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--constant-bytes", type=int, required=True)
    parser.add_argument("--per-batch-byte-budget", type=int, required=True)
    parser.add_argument(
        "--command",
        required=True,
        help="command template; {corpus} and {batch_size} are replaced",
    )
    parser.add_argument("--ci", action="store_true")
    parser.add_argument("--max-publishes", type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count <= 0 or args.batch_size <= 0 or args.payload_bytes < 0:
        raise ValueError("count, batch-size must be positive and payload-bytes non-negative")
    if args.ci and args.max_publishes is None:
        raise ValueError("--max-publishes is required in --ci mode")
    if "{corpus}" not in args.command:
        raise ValueError("--command must contain the {corpus} placeholder")

    with tempfile.TemporaryDirectory(prefix="lethe-import-memory-") as temporary:
        corpus = Path(temporary) / "synthetic.jsonl"
        synthetic_corpus(corpus, args.count, args.payload_bytes)
        command = [
            part.replace("{corpus}", str(corpus)).replace(
                "{batch_size}", str(args.batch_size)
            )
            for part in shlex.split(args.command)
        ]
        idle_bytes = current_rss_bytes()
        completed = subprocess.run(
            ["/usr/bin/time", "-v", *command],
            check=False,
            text=True,
            capture_output=True,
            env={"PATH": "/usr/local/bin:/usr/bin:/bin"},
        )
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        if completed.returncode != 0:
            return completed.returncode

        peak_bytes = maximum_resident_set_bytes(completed.stderr)
        delta_bytes = peak_bytes - idle_bytes
        allowed_bytes = args.constant_bytes + args.per_batch_byte_budget
        print(
            json.dumps(
                {
                    "count": args.count,
                    "batch_size": args.batch_size,
                    "idle_rss_bytes": idle_bytes,
                    "peak_vmhwm_bytes": peak_bytes,
                    "peak_minus_idle_bytes": delta_bytes,
                    "allowed_bytes": allowed_bytes,
                },
                sort_keys=True,
            )
        )
        if delta_bytes > allowed_bytes:
            print(
                f"memory bound exceeded: {delta_bytes} > {allowed_bytes}",
                file=sys.stderr,
            )
            return 1

        if args.ci:
            publish_match = re.search(r"publish_count=(\d+)", completed.stdout)
            if publish_match is None:
                raise RuntimeError("CI command did not print publish_count=<integer>")
            publish_count = int(publish_match.group(1))
            if publish_count > args.max_publishes:
                print(
                    f"publish bound exceeded: {publish_count} > {args.max_publishes}",
                    file=sys.stderr,
                )
                return 1
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"import memory harness failed: {error}", file=sys.stderr)
        raise SystemExit(2)
