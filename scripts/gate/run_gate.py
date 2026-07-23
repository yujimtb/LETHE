#!/usr/bin/env python3
"""Production-scale memory gate for LETHE self-host imports.

Boots a throwaway, loopback-only (127.0.0.1) Docker container from a
seeded data directory (see seed_gate_corpus.py), memory-capped with
`docker run --memory`, and runs three probes against it:

  1. dup-only replay  — resend already-seeded drafts; nothing should be
     durably retained beyond a small residual.
  2. new bulk import  — 1000 fresh drafts, once batched at 25 and once at
     1000, bounding both peak-over-baseline and post-settle residual.
  3. slope detection   — repeat dup-only batches and regress
     batch-number vs. post-settle RSS; a corpus-size-proportional leak
     shows up as a positive slope even when a single batch looks fine.

Never pushes anywhere and never talks to a non-loopback host: the
container is published on 127.0.0.1 only, and this script performs no
outbound network calls other than to that container.

The container, its copied data directory, and its docker network
attachment are always torn down in a `finally` block, including on
exceptions and OOM.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import gate_common as gc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="lethe-selfhost image tag")
    parser.add_argument(
        "--data-dir", required=True, type=Path, help="seeded data dir to copy from (read-only source)"
    )
    parser.add_argument("--config", required=True, type=Path, help="config.toml to mount")
    parser.add_argument("--jwks", required=True, type=Path, help="mcp-jwks.json to mount")
    parser.add_argument("--env-file", required=True, type=Path, help="deploy .env-style file")
    parser.add_argument("--port", type=int, default=18098, help="host port, bound to 127.0.0.1 only")
    parser.add_argument("--mem-limit", default="16g", help="docker --memory value")
    parser.add_argument("--report", required=True, type=Path, help="JSON report output path")

    parser.add_argument("--source-instance", default="device:gate-probe")
    parser.add_argument("--tag", required=True, help="must match the tag used to seed --data-dir")
    parser.add_argument(
        "--seed-count",
        type=int,
        default=568_000,
        help="corpus size the data dir was seeded with (bounds the dup-sample index range)",
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=gc.DEFAULT_PAD_BYTES,
        help="must match the value used to seed --data-dir",
    )
    parser.add_argument(
        "--token-env",
        default="LETHE_API_WRITE_TOKEN",
        help="key name inside --env-file holding the write:observations bearer token",
    )
    parser.add_argument("--container-name", default=None)

    parser.add_argument("--health-timeout-seconds", type=float, default=900.0)
    parser.add_argument("--ack-timeout-seconds", type=float, default=1800.0)
    parser.add_argument("--ack-latency-threshold-seconds", type=float, default=2.0)
    parser.add_argument("--ack-poll-interval-seconds", type=float, default=5.0)
    parser.add_argument("--baseline-settle-seconds", type=float, default=60.0)
    parser.add_argument("--stats-poll-interval-seconds", type=float, default=1.0)
    parser.add_argument(
        "--post-batch-wait-seconds",
        type=float,
        default=180.0,
        help="settle time after test 1 / test 2 before sampling residual RSS",
    )
    parser.add_argument(
        "--slope-settle-seconds",
        type=float,
        default=60.0,
        help="settle time after each of the 8 slope-test batches",
    )
    parser.add_argument(
        "--rss-average-window-seconds",
        type=float,
        default=15.0,
        help="window used to average out sampling noise when reading a residual/slope RSS point",
    )

    parser.add_argument("--dup-residual-threshold-mib", type=float, default=512.0)
    parser.add_argument("--bulk-peak-threshold-mib", type=float, default=4096.0)
    parser.add_argument("--bulk-residual-threshold-mib", type=float, default=768.0)
    parser.add_argument("--slope-threshold-mib-per-batch", type=float, default=8.0)

    return parser.parse_args()


def log(message: str) -> None:
    print(message, flush=True)


# ---------------------------------------------------------------------------
# Container lifecycle
# ---------------------------------------------------------------------------


def copy_data_dir(source: Path) -> Path:
    if not source.is_dir():
        raise gc.GateError(f"--data-dir does not exist or is not a directory: {source}")
    dest = Path(tempfile.mkdtemp(prefix="lethe-gate-data-"))
    log(f"copying seeded data dir {source} -> {dest} ...")
    shutil.copytree(source, dest, dirs_exist_ok=True)
    return dest


def start_container(
    *,
    image: str,
    container_name: str,
    port: int,
    mem_limit: str,
    env_file: Path,
    config_path: Path,
    jwks_path: Path,
    data_dir: Path,
) -> None:
    if not config_path.is_file():
        raise gc.GateError(f"--config not found: {config_path}")
    if not jwks_path.is_file():
        raise gc.GateError(f"--jwks not found: {jwks_path}")
    if not env_file.is_file():
        raise gc.GateError(f"--env-file not found: {env_file}")

    args = [
        "run",
        "-d",
        "--name",
        container_name,
        "--memory",
        mem_limit,
        "--memory-swap",
        mem_limit,  # disable swap so the memory cap actually bounds RSS
        "-p",
        f"127.0.0.1:{port}:8080",
        "--env-file",
        str(env_file),
        "-e",
        "LETHE_CONFIG_PATH=/etc/lethe/config.toml",
        "-e",
        "RUST_LOG=info",
        "-v",
        f"{config_path.resolve()}:/etc/lethe/config.toml:ro",
        "-v",
        f"{jwks_path.resolve()}:/etc/lethe/mcp-jwks.json:ro",
        "-v",
        f"{data_dir.resolve()}:/var/lib/lethe",
        image,
    ]
    gc.run_docker(args, timeout=120.0)


def wait_for_health(client: gc.ImportClient, timeout_seconds: float, container_name: str) -> float:
    log(f"waiting up to {timeout_seconds:.0f}s for /health ...")
    deadline = time.monotonic() + timeout_seconds
    started = time.monotonic()
    while time.monotonic() < deadline:
        gc.assert_container_alive(container_name)
        healthy, status_code = client.health(timeout=5.0)
        if healthy:
            elapsed = time.monotonic() - started
            log(f"/health OK after {elapsed:.1f}s")
            return elapsed
        time.sleep(2.0)
    raise gc.GateError(f"/health did not return 200 within {timeout_seconds:.0f}s")


def wait_for_ack_latency(
    client: gc.ImportClient,
    *,
    source_instance_id: str,
    tag: str,
    threshold_seconds: float,
    timeout_seconds: float,
    poll_interval_seconds: float,
    container_name: str,
) -> dict[str, Any]:
    log(
        f"waiting up to {timeout_seconds:.0f}s for single-item ACK latency "
        f"< {threshold_seconds:.1f}s (migration/index rebuild convergence) ..."
    )
    deadline = time.monotonic() + timeout_seconds
    attempt = 0
    samples: list[float] = []
    while time.monotonic() < deadline:
        gc.assert_container_alive(container_name)
        attempt += 1
        # Disjoint, effectively unbounded index space so ack probes never
        # collide with corpus or bulk-test indices.
        probe_index = 900_000_000 + attempt
        draft = gc.build_corpus_draft(
            tag=f"{tag}-ackprobe",
            source_instance_id=source_instance_id,
            index=probe_index,
        )
        result = client.send_drafts(source_instance_id, [draft], timeout=30.0)
        gc.assert_no_failures(result.counts, context=f"ack probe #{attempt}")
        samples.append(result.elapsed_seconds)
        log(f"ack probe #{attempt}: {result.elapsed_seconds:.3f}s")
        if result.elapsed_seconds < threshold_seconds:
            return {"attempts": attempt, "final_latency_seconds": result.elapsed_seconds, "samples": samples}
        time.sleep(poll_interval_seconds)
    raise gc.GateError(
        f"single-item ACK latency did not drop below {threshold_seconds:.1f}s "
        f"within {timeout_seconds:.0f}s (last={samples[-1] if samples else 'n/a'})"
    )


# ---------------------------------------------------------------------------
# Test 1: dup-only replay
# ---------------------------------------------------------------------------


def run_dup_only_batches(
    client: gc.ImportClient,
    *,
    source_instance_id: str,
    tag: str,
    start_index: int,
    batches: int,
    batch_size: int,
    container_name: str,
) -> list[float]:
    latencies: list[float] = []
    for batch_num in range(batches):
        gc.assert_container_alive(container_name)
        lo = start_index + batch_num * batch_size
        hi = lo + batch_size
        drafts = [
            gc.build_corpus_draft(tag=tag, source_instance_id=source_instance_id, index=i)
            for i in range(lo, hi)
        ]
        result = client.send_drafts(source_instance_id, drafts, timeout=120.0)
        gc.assert_all_duplicate(
            result.counts, batch_size, context=f"dup batch {batch_num + 1} [{lo},{hi})"
        )
        latencies.append(result.elapsed_seconds)
        log(f"dup batch {batch_num + 1}/{batches}: {result.elapsed_seconds:.3f}s (duplicate={result.counts.duplicates})")
    return latencies


def run_test1_dup_only(
    client: gc.ImportClient,
    sampler: gc.DockerStatsSampler,
    *,
    source_instance_id: str,
    tag: str,
    baseline_mib: float,
    container_name: str,
    post_batch_wait_seconds: float,
    rss_average_window_seconds: float,
    residual_threshold_mib: float,
) -> dict[str, Any]:
    log("=== test 1: dup-only replay (100 x 8 batches) ===")
    latencies = run_dup_only_batches(
        client,
        source_instance_id=source_instance_id,
        tag=tag,
        start_index=0,
        batches=8,
        batch_size=100,
        container_name=container_name,
    )
    log(f"test 1 sending done; settling {post_batch_wait_seconds:.0f}s before residual RSS sample ...")
    time.sleep(post_batch_wait_seconds)
    gc.assert_container_alive(container_name)
    after_mib = sampler.average_recent(rss_average_window_seconds)
    residual_mib = after_mib - baseline_mib
    passed = residual_mib <= residual_threshold_mib
    log(
        f"test 1 result: baseline={baseline_mib:.1f}MiB after={after_mib:.1f}MiB "
        f"residual={residual_mib:.1f}MiB threshold={residual_threshold_mib:.1f}MiB "
        f"pass={passed}"
    )
    return {
        "name": "dup_only_replay",
        "passed": passed,
        "batch_latencies_seconds": latencies,
        "baseline_mib": baseline_mib,
        "after_mib": after_mib,
        "residual_mib": residual_mib,
        "residual_threshold_mib": residual_threshold_mib,
    }


# ---------------------------------------------------------------------------
# Test 2: new bulk import (batch=25 and batch=1000)
# ---------------------------------------------------------------------------


def run_bulk_subtest(
    client: gc.ImportClient,
    sampler: gc.DockerStatsSampler,
    *,
    label: str,
    source_instance_id: str,
    tag: str,
    start_index: int,
    count: int,
    batch_size: int,
    baseline_mib: float,
    container_name: str,
    post_batch_wait_seconds: float,
    rss_average_window_seconds: float,
    peak_threshold_mib: float,
    residual_threshold_mib: float,
) -> dict[str, Any]:
    log(f"=== test 2 ({label}): {count} new drafts, batch={batch_size} ===")
    marker = sampler.mark()
    total = gc.OutcomeCounts()
    latencies: list[float] = []
    index = start_index
    end = start_index + count
    while index < end:
        gc.assert_container_alive(container_name)
        batch_end = min(index + batch_size, end)
        drafts = [
            gc.build_corpus_draft(tag=tag, source_instance_id=source_instance_id, index=i)
            for i in range(index, batch_end)
        ]
        result = client.send_drafts(source_instance_id, drafts, timeout=180.0)
        gc.assert_all_ingested(
            result.counts, len(drafts), context=f"{label} batch [{index},{batch_end})"
        )
        total = total + result.counts
        latencies.append(result.elapsed_seconds)
        index = batch_end

    peak_mib = sampler.peak_since(marker)
    peak_over_baseline_mib = peak_mib - baseline_mib
    log(
        f"{label} sending done ({total.ingested} ingested); "
        f"settling {post_batch_wait_seconds:.0f}s before residual RSS sample ..."
    )
    time.sleep(post_batch_wait_seconds)
    gc.assert_container_alive(container_name)
    after_mib = sampler.average_recent(rss_average_window_seconds)
    residual_mib = after_mib - baseline_mib

    peak_ok = peak_over_baseline_mib <= peak_threshold_mib
    residual_ok = residual_mib <= residual_threshold_mib
    ingested_ok = total.ingested == count and total.duplicates == 0
    passed = peak_ok and residual_ok and ingested_ok
    log(
        f"{label} result: peak={peak_mib:.1f}MiB peak_over_baseline={peak_over_baseline_mib:.1f}MiB "
        f"(threshold={peak_threshold_mib:.1f}MiB) after={after_mib:.1f}MiB "
        f"residual={residual_mib:.1f}MiB (threshold={residual_threshold_mib:.1f}MiB) "
        f"ingested={total.ingested}/{count} pass={passed}"
    )
    return {
        "name": f"bulk_new_{label}",
        "passed": passed,
        "batch_size": batch_size,
        "count": count,
        "ingested": total.ingested,
        "duplicates": total.duplicates,
        "batch_latencies_seconds": latencies,
        "peak_mib": peak_mib,
        "peak_over_baseline_mib": peak_over_baseline_mib,
        "peak_threshold_mib": peak_threshold_mib,
        "baseline_mib": baseline_mib,
        "after_mib": after_mib,
        "residual_mib": residual_mib,
        "residual_threshold_mib": residual_threshold_mib,
    }


# ---------------------------------------------------------------------------
# Test 3: slope detection
# ---------------------------------------------------------------------------


def run_test3_slope(
    client: gc.ImportClient,
    sampler: gc.DockerStatsSampler,
    *,
    source_instance_id: str,
    tag: str,
    start_index: int,
    baseline_mib: float,
    container_name: str,
    slope_settle_seconds: float,
    rss_average_window_seconds: float,
    slope_threshold_mib_per_batch: float,
) -> dict[str, Any]:
    log("=== test 3: slope detection (100 x 8 dup batches, per-batch settle) ===")
    batch_numbers: list[float] = []
    rss_after_points: list[float] = []
    latencies: list[float] = []
    for batch_num in range(8):
        gc.assert_container_alive(container_name)
        lo = start_index + batch_num * 100
        hi = lo + 100
        drafts = [
            gc.build_corpus_draft(tag=tag, source_instance_id=source_instance_id, index=i)
            for i in range(lo, hi)
        ]
        result = client.send_drafts(source_instance_id, drafts, timeout=120.0)
        gc.assert_all_duplicate(
            result.counts, 100, context=f"slope batch {batch_num + 1} [{lo},{hi})"
        )
        latencies.append(result.elapsed_seconds)
        time.sleep(slope_settle_seconds)
        gc.assert_container_alive(container_name)
        rss_mib = sampler.average_recent(rss_average_window_seconds)
        batch_numbers.append(float(batch_num + 1))
        rss_after_points.append(rss_mib)
        log(f"slope batch {batch_num + 1}/8: rss_after={rss_mib:.1f}MiB")

    slope_mib_per_batch = gc.linear_regression_slope(batch_numbers, rss_after_points)
    passed = slope_mib_per_batch <= slope_threshold_mib_per_batch
    log(
        f"test 3 result: slope={slope_mib_per_batch:.3f}MiB/batch "
        f"threshold={slope_threshold_mib_per_batch:.1f}MiB/batch pass={passed}"
    )
    return {
        "name": "slope_detection",
        "passed": passed,
        "batch_latencies_seconds": latencies,
        "batch_numbers": batch_numbers,
        "rss_after_mib": rss_after_points,
        "baseline_mib": baseline_mib,
        "slope_mib_per_batch": slope_mib_per_batch,
        "slope_threshold_mib_per_batch": slope_threshold_mib_per_batch,
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> int:
    args = parse_args()

    if args.seed_count < 1600:
        raise gc.GateError(
            "--seed-count must be >= 1600 (test 1 and test 3 each consume 800 "
            "distinct dup-sample indices from the seeded range)"
        )

    env = gc.read_env_file(args.env_file)
    token = gc.require_env_value(env, args.token_env)
    client = gc.ImportClient(f"http://127.0.0.1:{args.port}", token)

    container_name = args.container_name or f"lethe-gate-{uuid.uuid4().hex[:10]}"
    data_dir: Path | None = None
    sampler: gc.DockerStatsSampler | None = None
    container_started = False

    report: dict[str, Any] = {
        "image": args.image,
        "container_name": container_name,
        "port": args.port,
        "mem_limit": args.mem_limit,
        "tag": args.tag,
        "source_instance": args.source_instance,
        "thresholds": {
            "dup_residual_threshold_mib": args.dup_residual_threshold_mib,
            "bulk_peak_threshold_mib": args.bulk_peak_threshold_mib,
            "bulk_residual_threshold_mib": args.bulk_residual_threshold_mib,
            "slope_threshold_mib_per_batch": args.slope_threshold_mib_per_batch,
        },
        "phases": {},
        "tests": [],
        "passed": False,
        "fatal_error": None,
    }

    exit_code = 1
    try:
        data_dir = copy_data_dir(args.data_dir)

        log(f"starting container {container_name} from image {args.image} on 127.0.0.1:{args.port} ...")
        start_container(
            image=args.image,
            container_name=container_name,
            port=args.port,
            mem_limit=args.mem_limit,
            env_file=args.env_file,
            config_path=args.config,
            jwks_path=args.jwks,
            data_dir=data_dir,
        )
        container_started = True

        health_wait_seconds = wait_for_health(client, args.health_timeout_seconds, container_name)
        report["phases"]["health_wait_seconds"] = health_wait_seconds

        ack_info = wait_for_ack_latency(
            client,
            source_instance_id=args.source_instance,
            tag=args.tag,
            threshold_seconds=args.ack_latency_threshold_seconds,
            timeout_seconds=args.ack_timeout_seconds,
            poll_interval_seconds=args.ack_poll_interval_seconds,
            container_name=container_name,
        )
        report["phases"]["ack_convergence"] = ack_info

        sampler = gc.DockerStatsSampler(container_name, args.stats_poll_interval_seconds)
        sampler.start()

        log(f"settling {args.baseline_settle_seconds:.0f}s to sample baseline RSS ...")
        time.sleep(args.baseline_settle_seconds)
        gc.assert_container_alive(container_name)
        baseline_mib = sampler.average_recent(args.baseline_settle_seconds)
        log(f"baseline RSS: {baseline_mib:.1f}MiB")
        report["phases"]["baseline_mib"] = baseline_mib

        test1 = run_test1_dup_only(
            client,
            sampler,
            source_instance_id=args.source_instance,
            tag=args.tag,
            baseline_mib=baseline_mib,
            container_name=container_name,
            post_batch_wait_seconds=args.post_batch_wait_seconds,
            rss_average_window_seconds=args.rss_average_window_seconds,
            residual_threshold_mib=args.dup_residual_threshold_mib,
        )
        report["tests"].append(test1)

        bulk25_start = args.seed_count
        bulk1000_start = args.seed_count + 1000
        test2a = run_bulk_subtest(
            client,
            sampler,
            label="batch25",
            source_instance_id=args.source_instance,
            tag=args.tag,
            start_index=bulk25_start,
            count=1000,
            batch_size=25,
            baseline_mib=baseline_mib,
            container_name=container_name,
            post_batch_wait_seconds=args.post_batch_wait_seconds,
            rss_average_window_seconds=args.rss_average_window_seconds,
            peak_threshold_mib=args.bulk_peak_threshold_mib,
            residual_threshold_mib=args.bulk_residual_threshold_mib,
        )
        report["tests"].append(test2a)

        test2b = run_bulk_subtest(
            client,
            sampler,
            label="batch1000",
            source_instance_id=args.source_instance,
            tag=args.tag,
            start_index=bulk1000_start,
            count=1000,
            batch_size=1000,
            baseline_mib=baseline_mib,
            container_name=container_name,
            post_batch_wait_seconds=args.post_batch_wait_seconds,
            rss_average_window_seconds=args.rss_average_window_seconds,
            peak_threshold_mib=args.bulk_peak_threshold_mib,
            residual_threshold_mib=args.bulk_residual_threshold_mib,
        )
        report["tests"].append(test2b)

        test3 = run_test3_slope(
            client,
            sampler,
            source_instance_id=args.source_instance,
            tag=args.tag,
            start_index=800,
            baseline_mib=baseline_mib,
            container_name=container_name,
            slope_settle_seconds=args.slope_settle_seconds,
            rss_average_window_seconds=args.rss_average_window_seconds,
            slope_threshold_mib_per_batch=args.slope_threshold_mib_per_batch,
        )
        report["tests"].append(test3)

        report["peak_mib_overall"] = sampler.peak_mib
        overall_passed = all(test["passed"] for test in report["tests"])
        report["passed"] = overall_passed
        exit_code = 0 if overall_passed else 1

    except gc.GateError as error:
        report["fatal_error"] = str(error)
        report["passed"] = False
        exit_code = 1
        log(f"GATE FAILED: {error}")
    except Exception as error:  # pragma: no cover - defensive catch-all
        report["fatal_error"] = f"unexpected error: {error}"
        report["passed"] = False
        exit_code = 1
        log(f"GATE FAILED (unexpected): {error}")
    finally:
        if sampler is not None:
            sampler.stop()
        if container_started:
            log(f"stopping and removing container {container_name} ...")
            gc.stop_and_remove_container(container_name)
        if data_dir is not None:
            log(f"removing temporary data dir {data_dir} ...")
            shutil.rmtree(data_dir, ignore_errors=True)

        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

        print_summary(report)

    return exit_code


def print_summary(report: dict[str, Any]) -> None:
    log("")
    log("=" * 72)
    log(f"GATE {'PASS' if report.get('passed') else 'FAIL'}")
    if report.get("fatal_error"):
        log(f"fatal_error: {report['fatal_error']}")
    for test in report.get("tests", []):
        status = "PASS" if test.get("passed") else "FAIL"
        log(f"  [{status}] {test.get('name')}")
    if "peak_mib_overall" in report:
        log(f"peak RSS observed (overall): {report['peak_mib_overall']:.1f}MiB")
    log("=" * 72)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except gc.GateError as error:
        print(f"run_gate failed: {error}", file=sys.stderr)
        sys.exit(1)
