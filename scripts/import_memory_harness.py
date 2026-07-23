#!/usr/bin/env python3
"""Linux-only RSS acceptance harness for duplicate-only imports.

The server, seed, and duplicate commands are supplied by the caller.  They
are executed directly (without a shell) and receive paths to synthetic JSONL
files through the ``{corpus}`` placeholder.  The server command must exec the
selfhost process directly, because its subprocess PID is the PID whose
``/proc/<pid>/status`` is measured.
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path


BYTES_PER_MIB = 1024 * 1024
DEFAULT_COUNT = 10_000
DEFAULT_BATCH_SIZE = 25
DEFAULT_WARMUP_BATCHES = 2
DEFAULT_MEASURE_BATCHES = 10
DEFAULT_SETTLE_SECONDS = 2.0
DEFAULT_MAX_POST_DELTA_BYTES = 64 * BYTES_PER_MIB
DEFAULT_MAX_RSS_SLOPE_BYTES_PER_BATCH = 2 * BYTES_PER_MIB


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure selfhost VmRSS while repeating duplicate-only imports"
    )
    parser.add_argument("--server-command", required=True)
    parser.add_argument("--seed-command", required=True)
    parser.add_argument("--duplicate-command", required=True)
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--warmup-batches", type=int, default=DEFAULT_WARMUP_BATCHES)
    parser.add_argument("--measure-batches", type=int, default=DEFAULT_MEASURE_BATCHES)
    parser.add_argument("--settle-seconds", type=float, default=DEFAULT_SETTLE_SECONDS)
    parser.add_argument(
        "--max-post-delta-bytes",
        type=int,
        default=DEFAULT_MAX_POST_DELTA_BYTES,
    )
    parser.add_argument(
        "--max-rss-slope-bytes-per-batch",
        type=int,
        default=DEFAULT_MAX_RSS_SLOPE_BYTES_PER_BATCH,
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.count <= 0 or args.batch_size <= 0:
        raise ValueError("--count and --batch-size must be positive")
    if args.warmup_batches < 2:
        raise ValueError("--warmup-batches must be at least 2")
    if not 8 <= args.measure_batches <= 12:
        raise ValueError("--measure-batches must be between 8 and 12")
    if args.settle_seconds < 0:
        raise ValueError("--settle-seconds must be non-negative")
    if args.max_post_delta_bytes < 0:
        raise ValueError("--max-post-delta-bytes must be non-negative")
    if args.max_rss_slope_bytes_per_batch < 0:
        raise ValueError("--max-rss-slope-bytes-per-batch must be non-negative")
    for name, command in [
        ("--server-command", args.server_command),
        ("--seed-command", args.seed_command),
        ("--duplicate-command", args.duplicate_command),
    ]:
        if not shlex.split(command):
            raise ValueError(f"{name} must not be empty")
    for name, command in [
        ("--seed-command", args.seed_command),
        ("--duplicate-command", args.duplicate_command),
    ]:
        if "{corpus}" not in command:
            raise ValueError(f"{name} must contain the {{corpus}} placeholder")


def synthetic_corpus(seed_path: Path, duplicate_path: Path, count: int, batch_size: int) -> None:
    records = []
    for index in range(count):
        canonical = {"source": "import-memory-harness", "object_id": f"harness:{index}"}
        records.append(
            {
                "client_ref": str(index),
                "object_id": canonical["object_id"],
                "canonical_json": json.dumps(canonical, separators=(",", ":")),
                "payload": {"text": f"memory harness observation {index}"},
            }
        )
    with seed_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")
    with duplicate_path.open("w", encoding="utf-8") as handle:
        for record in records[:batch_size]:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")


def command_for(
    template: str,
    corpus: Path,
    count: int,
    batch_size: int,
    batch_index: int,
) -> list[str]:
    values = {
        "{corpus}": str(corpus),
        "{count}": str(count),
        "{batch_size}": str(batch_size),
        "{batch_index}": str(batch_index),
    }
    return [
        replace_placeholders(part, values)
        for part in shlex.split(template)
    ]


def replace_placeholders(value: str, values: dict[str, str]) -> str:
    for placeholder, replacement in values.items():
        value = value.replace(placeholder, replacement)
    return value


def run_command(name: str, command: list[str]) -> str:
    completed = subprocess.run(command, check=False, text=True, capture_output=True)
    if completed.stdout:
        sys.stdout.write(completed.stdout)
    if completed.stderr:
        sys.stderr.write(completed.stderr)
    if completed.returncode != 0:
        raise RuntimeError(f"{name} failed with exit code {completed.returncode}")
    return f"{completed.stdout}\n{completed.stderr}"


def assert_import_counts(output: str, expected_ingested: int, expected_duplicates: int) -> None:
    ingested = re.search(r'"?ingested"?\s*[:=]\s*(\d+)', output)
    duplicates = re.search(r'"?duplicates"?\s*[:=]\s*(\d+)', output)
    if ingested is None or duplicates is None:
        raise RuntimeError(
            "import command output must contain ingested=<integer> and duplicates=<integer>"
        )
    actual_ingested = int(ingested.group(1))
    actual_duplicates = int(duplicates.group(1))
    if (actual_ingested, actual_duplicates) != (expected_ingested, expected_duplicates):
        raise RuntimeError(
            "unexpected import counts: "
            f"got ingested={actual_ingested}/duplicates={actual_duplicates}, "
            f"expected ingested={expected_ingested}/duplicates={expected_duplicates}"
        )


def wait_for_settle(server: subprocess.Popen[str], seconds: float, stage: str) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        returncode = server.poll()
        if returncode is not None:
            raise RuntimeError(f"server exited during {stage} with code {returncode}")
        time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
    returncode = server.poll()
    if returncode is not None:
        raise RuntimeError(f"server exited during {stage} with code {returncode}")


def vmrss_bytes(pid: int) -> int:
    status_path = Path(f"/proc/{pid}/status")
    try:
        lines = status_path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError as error:
        raise RuntimeError(f"target selfhost PID {pid} no longer has /proc status") from error
    for line in lines:
        if line.startswith("VmRSS:"):
            fields = line.split()
            if len(fields) < 2 or not fields[1].isdigit():
                raise RuntimeError(f"invalid VmRSS line in {status_path}: {line!r}")
            return int(fields[1]) * 1024
    raise RuntimeError(f"{status_path} has no VmRSS; Linux is required")


def linear_slope(samples: list[int]) -> float:
    if len(samples) < 2:
        raise ValueError("at least two RSS samples are required for a slope")
    xs = list(range(len(samples)))
    x_mean = sum(xs) / len(xs)
    y_mean = sum(samples) / len(samples)
    denominator = sum((x - x_mean) ** 2 for x in xs)
    if denominator == 0:
        raise RuntimeError("RSS slope denominator unexpectedly equals zero")
    return sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, samples)) / denominator


def stop_server(server: subprocess.Popen[str]) -> None:
    if server.poll() is not None:
        return
    server.terminate()
    try:
        server.wait(timeout=10)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait(timeout=10)


def main() -> int:
    args = parse_args()
    validate_args(args)
    server: subprocess.Popen[str] | None = None
    with tempfile.TemporaryDirectory(prefix="lethe-import-memory-") as temporary:
        temporary_path = Path(temporary)
        seed_path = temporary_path / "seed.jsonl"
        duplicate_path = temporary_path / "duplicate.jsonl"
        synthetic_corpus(seed_path, duplicate_path, args.count, args.batch_size)
        try:
            server = subprocess.Popen(shlex.split(args.server_command), text=True)
            wait_for_settle(server, args.settle_seconds, "server startup")

            seed_output = run_command(
                "seed command",
                command_for(
                    args.seed_command,
                    seed_path,
                    args.count,
                    args.batch_size,
                    0,
                ),
            )
            assert_import_counts(seed_output, args.count, 0)
            wait_for_settle(server, args.settle_seconds, "seed convergence")
            seed_baseline = vmrss_bytes(server.pid)

            for batch_index in range(args.warmup_batches):
                duplicate_output = run_command(
                    f"duplicate warmup batch {batch_index}",
                    command_for(
                        args.duplicate_command,
                        duplicate_path,
                        args.count,
                        args.batch_size,
                        batch_index,
                    ),
                )
                assert_import_counts(duplicate_output, 0, args.batch_size)

            measurement_baseline = vmrss_bytes(server.pid)
            samples = []
            for batch_index in range(args.measure_batches):
                duplicate_output = run_command(
                    f"duplicate measurement batch {batch_index}",
                    command_for(
                        args.duplicate_command,
                        duplicate_path,
                        args.count,
                        args.batch_size,
                        args.warmup_batches + batch_index,
                    ),
                )
                assert_import_counts(duplicate_output, 0, args.batch_size)
                samples.append(vmrss_bytes(server.pid))

            latter_samples = samples[len(samples) // 2 :]
            slope = linear_slope(latter_samples)
            final_post_delta = max(0, samples[-1] - measurement_baseline)
            report = {
                "count": args.count,
                "batch_size": args.batch_size,
                "server_pid": server.pid,
                "seed_baseline_vmrss_bytes": seed_baseline,
                "measurement_baseline_vmrss_bytes": measurement_baseline,
                "vmrss_samples_bytes": samples,
                "latter_sample_slope_bytes_per_batch": slope,
                "final_post_delta_bytes": final_post_delta,
                "max_rss_slope_bytes_per_batch": args.max_rss_slope_bytes_per_batch,
                "max_post_delta_bytes": args.max_post_delta_bytes,
            }
            print(json.dumps(report, sort_keys=True))
            if slope > args.max_rss_slope_bytes_per_batch:
                print(
                    "RSS slope bound exceeded: "
                    f"{slope} > {args.max_rss_slope_bytes_per_batch}",
                    file=sys.stderr,
                )
                return 1
            if final_post_delta > args.max_post_delta_bytes:
                print(
                    "final RSS delta bound exceeded: "
                    f"{final_post_delta} > {args.max_post_delta_bytes}",
                    file=sys.stderr,
                )
                return 1
            return 0
        finally:
            if server is not None:
                stop_server(server)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"import memory harness failed: {error}", file=sys.stderr)
        raise SystemExit(2)
