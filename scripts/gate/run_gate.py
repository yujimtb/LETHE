#!/usr/bin/env python3
"""Production-scale memory gate for LETHE self-host imports.

Boots a throwaway, loopback-only (127.0.0.1) Docker container from a
seeded data directory (see seed_gate_corpus.py), memory-capped with
`docker run --memory`, and runs three probes against it:

  1. dup-only replay  — resend already-seeded drafts; nothing should be
     durably retained beyond a small residual.
  1b. bulk session dup-only — same dup-only replay, but each 100-item
     batch is wrapped in its own bulk-session begin/import/end cycle.
     Targets the pathology found by the sol audit: `end` triggers a full
     corpus/materialization rebuild path
     (bulk_import.rs::end_bulk_import_session), and pre-fix that rebuild
     scaled with corpus size even for a session that only ever saw
     duplicates.
  2. new bulk import  — 1000 fresh drafts, once batched at 25 and once at
     1000, bounding both peak-over-baseline and post-settle residual.
  2b. bulk session new import — the batch=25 new-1000 case again, this
     time wrapped in a single begin/.../end bulk session.
  3. slope detection   — repeat dup-only batches and regress
     batch-number vs. post-settle RSS; a corpus-size-proportional leak
     shows up as a positive slope even when a single batch looks fine.

Tests 1b and 2b drive POST /api/import/bulk-sessions/begin and
POST /api/import/bulk-sessions/{session_id}/end (apps/selfhost/src/self_host/
server.rs). If the bulk-session API is unusable in this environment (auth,
scope, or routing — see gate_common.SESSION_API_UNAVAILABLE_STATUS_CODES),
those two tests are marked skipped (not failed) with a reason recorded in
the report, and a warning is printed to stdout.

A fresh container can take well over a minute to ACK a single import while
it works through backlog catch-up — this is expected, not a fatal error.
Every import send in this script (the ACK-convergence probe and every test
batch) treats a request timeout / connection error / 5xx as transient:
it's logged and, outside the ACK-convergence loop, retried on the same
batch up to --max-consecutive-timeouts times before the gate aborts with
an explicit reason. Only a real HTTP 4xx or an unexpected outcome
(rejected/quarantined counts, wrong ingested/duplicate totals) is a hard
failure.

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
    parser.add_argument(
        "--probe-timeout-seconds",
        type=float,
        default=900.0,
        help=(
            "per-request HTTP timeout for the ACK-convergence probe. A fresh "
            "container can take well over a minute per import during backlog "
            "catch-up; a probe that times out is treated as 'not converged "
            "yet', not a failure, as long as --ack-timeout-seconds budget "
            "remains. IMPORTANT: the server keeps processing an import after "
            "the client gives up on it — a shorter timeout here does not make "
            "convergence faster, it only orphans more in-flight requests that "
            "go on holding a concurrency permit (limits.max_concurrent_imports, "
            "commonly 2) until they finish server-side, which can starve the "
            "next probe with HTTP 429 import_concurrency_limit. Prefer raising "
            "this over lowering it"
        ),
    )
    parser.add_argument(
        "--max-consecutive-timeouts",
        type=int,
        default=3,
        help=(
            "test 1/1b/2/2b/3: number of consecutive transient failures "
            "(timeout/connection-error/5xx, NOT counting HTTP 429 — see "
            "--max-consecutive-concurrency-retries) on the same batch before "
            "the gate aborts with an explicit reason"
        ),
    )
    parser.add_argument(
        "--max-consecutive-concurrency-retries",
        type=int,
        default=60,
        help=(
            "test 1/1b/2/2b/3: number of consecutive HTTP 429 "
            "import_concurrency_limit responses on the same batch (retried "
            "honoring the server's retry_after hint, counted separately from "
            "--max-consecutive-timeouts) before the gate aborts — signals a "
            "permit that is never clearing. The ACK-convergence probe also "
            "retries 429s honoring retry_after, but is only bounded by the "
            "overall --ack-timeout-seconds budget, not this count"
        ),
    )
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
    parser.add_argument(
        "--bulk-session-end-latency-threshold-seconds",
        type=float,
        default=10.0,
        help=(
            "test 1b: max acceptable POST .../bulk-sessions/{id}/end duration per "
            "batch. The regression this gate targets showed ~26s/batch; a fixed "
            "session-end should stay in the single-digit seconds"
        ),
    )

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
    probe_timeout_seconds: float,
    container_name: str,
) -> dict[str, Any]:
    log(
        f"waiting up to {timeout_seconds:.0f}s for single-item ACK latency "
        f"< {threshold_seconds:.1f}s (migration/index rebuild convergence); "
        f"per-probe request timeout is {probe_timeout_seconds:.0f}s ..."
    )
    deadline = time.monotonic() + timeout_seconds
    started = time.monotonic()
    attempt = 0
    samples: list[float] = []
    transient_events: list[dict[str, Any]] = []
    concurrency_events: list[dict[str, Any]] = []
    # Reused across HTTP 429 retries of the *same* logical probe so that
    # draining an orphaned permit doesn't itself pile on new orphans; only
    # advanced to a fresh index once we get a non-429 outcome (success or a
    # genuine timeout).
    probe_index: int | None = None
    draft: dict[str, Any] | None = None
    advance_probe = True
    while time.monotonic() < deadline:
        gc.assert_container_alive(container_name)
        if advance_probe:
            attempt += 1
            # Disjoint, effectively unbounded index space so ack probes
            # never collide with corpus or bulk-test indices.
            probe_index = 900_000_000 + attempt
            # published is fixed to "now - 2min", independent of the (very
            # large, ever-growing) probe index — deriving it from the index
            # via corpus_published() would push it decades into the future
            # for this index range and get rejected by the server's
            # clock-skew gate (HTTP 400 "published is too far in the
            # future"). See gate_common.corpus_published()'s docstring.
            draft = gc.build_corpus_draft(
                tag=f"{tag}-ackprobe",
                source_instance_id=source_instance_id,
                index=probe_index,
                published_override=gc.near_now_published(2.0),
            )
        try:
            result = client.send_drafts(
                source_instance_id, [draft], timeout=probe_timeout_seconds
            )
        except gc.ImportConcurrencyLimitError as error:
            # The bounded import-permit pool is full — commonly because an
            # earlier probe's *client* gave up (timeout) while the server
            # kept processing it and holding a permit. Retrying our own
            # probe faster than that orphan can finish would just add more
            # load; wait the server's retry_after hint and re-poll the same
            # (not a new) probe until the permit clears, still bounded by
            # the overall --ack-timeout-seconds budget.
            wait_seconds = max(error.retry_after_seconds, 0.1)
            waited_so_far = time.monotonic() - started
            concurrency_events.append(
                {
                    "attempt": attempt,
                    "elapsed_seconds": error.elapsed_seconds,
                    "retry_after_seconds": error.retry_after_seconds,
                    "message": str(error),
                }
            )
            log(
                f"ack probe #{attempt}: HTTP 429 import_concurrency_limit; "
                f"waiting retry_after={wait_seconds:.1f}s for an orphaned import "
                f"permit to clear (waited {waited_so_far:.0f}s of {timeout_seconds:.0f}s budget)"
            )
            time.sleep(wait_seconds)
            advance_probe = False
            continue
        except gc.TransientImportError as error:
            # A fresh container doing backlog catch-up can take well over a
            # minute (observed: 90s+) to ACK a single import. That is "not
            # converged yet", not a fatal error — log it and keep polling
            # against the overall --ack-timeout-seconds budget.
            waited_so_far = time.monotonic() - started
            transient_events.append(
                {
                    "attempt": attempt,
                    "elapsed_seconds": error.elapsed_seconds,
                    "status_code": error.status_code,
                    "message": str(error),
                }
            )
            log(
                f"ack probe #{attempt}: transient failure after "
                f"{error.elapsed_seconds:.1f}s ({error}); not converged yet, "
                f"continuing (waited {waited_so_far:.0f}s of {timeout_seconds:.0f}s budget)"
            )
            time.sleep(poll_interval_seconds)
            advance_probe = True
            continue
        gc.assert_no_failures(result.counts, context=f"ack probe #{attempt}")
        samples.append(result.elapsed_seconds)
        log(f"ack probe #{attempt}: {result.elapsed_seconds:.3f}s")
        if result.elapsed_seconds < threshold_seconds:
            return {
                "attempts": attempt,
                "final_latency_seconds": result.elapsed_seconds,
                "samples": samples,
                "transient_events": transient_events,
                "concurrency_limit_events": concurrency_events,
            }
        time.sleep(poll_interval_seconds)
        advance_probe = True
    raise gc.GateError(
        f"single-item ACK latency did not drop below {threshold_seconds:.1f}s "
        f"within {timeout_seconds:.0f}s (last successful sample="
        f"{samples[-1] if samples else 'n/a'}, transient failures={len(transient_events)}, "
        f"HTTP 429 import_concurrency_limit responses={len(concurrency_events)})"
    )


def log_transient_event(context: str, event: dict[str, Any]) -> None:
    log(
        f"{context}: transient failure on attempt {event['attempt']} after "
        f"{event['elapsed_seconds']:.1f}s (status={event['status_code']}): {event['message']}"
    )


def log_concurrency_event(context: str, event: dict[str, Any]) -> None:
    log(
        f"{context}: HTTP 429 import_concurrency_limit on attempt {event['attempt']} "
        f"after {event['elapsed_seconds']:.1f}s; waiting "
        f"retry_after={event['retry_after_seconds']:.1f}s for an orphaned import "
        f"permit to clear"
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
    max_consecutive_timeouts: int,
    max_consecutive_concurrency_retries: int,
) -> tuple[list[float], list[dict[str, Any]], list[dict[str, Any]]]:
    latencies: list[float] = []
    all_transient_events: list[dict[str, Any]] = []
    all_concurrency_events: list[dict[str, Any]] = []
    for batch_num in range(batches):
        gc.assert_container_alive(container_name)
        lo = start_index + batch_num * batch_size
        hi = lo + batch_size
        drafts = [
            gc.build_corpus_draft(tag=tag, source_instance_id=source_instance_id, index=i)
            for i in range(lo, hi)
        ]
        context = f"dup batch {batch_num + 1} [{lo},{hi})"
        outcome = gc.send_drafts_with_retry(
            client,
            source_instance_id,
            drafts,
            timeout=120.0,
            max_consecutive_timeouts=max_consecutive_timeouts,
            max_consecutive_concurrency_retries=max_consecutive_concurrency_retries,
            context=context,
            on_transient=lambda event, context=context: log_transient_event(context, event),
            on_concurrency_limit=lambda event, context=context: log_concurrency_event(context, event),
        )
        result = outcome.result
        all_transient_events.extend(outcome.transient_events)
        all_concurrency_events.extend(outcome.concurrency_events)
        gc.assert_all_duplicate(result.counts, batch_size, context=context)
        latencies.append(result.elapsed_seconds)
        log(
            f"dup batch {batch_num + 1}/{batches}: {result.elapsed_seconds:.3f}s "
            f"(duplicate={result.counts.duplicates}, attempts={outcome.attempts})"
        )
    return latencies, all_transient_events, all_concurrency_events


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
    max_consecutive_timeouts: int,
    max_consecutive_concurrency_retries: int,
) -> dict[str, Any]:
    log("=== test 1: dup-only replay (100 x 8 batches) ===")
    latencies, transient_events, concurrency_events = run_dup_only_batches(
        client,
        source_instance_id=source_instance_id,
        tag=tag,
        start_index=0,
        batches=8,
        batch_size=100,
        container_name=container_name,
        max_consecutive_timeouts=max_consecutive_timeouts,
        max_consecutive_concurrency_retries=max_consecutive_concurrency_retries,
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
        "transient_events": transient_events,
        "concurrency_limit_events": concurrency_events,
        "baseline_mib": baseline_mib,
        "after_mib": after_mib,
        "residual_mib": residual_mib,
        "residual_threshold_mib": residual_threshold_mib,
    }


def _skipped_test_result(name: str, reason: str) -> dict[str, Any]:
    return {
        "name": name,
        "passed": True,
        "skipped": True,
        "skip_reason": reason,
    }


# ---------------------------------------------------------------------------
# Test 1b: bulk session dup-only (begin -> 100 dup drafts -> end, x4)
# ---------------------------------------------------------------------------


def run_test1b_bulk_session_dup_only(
    client: gc.ImportClient,
    sampler: gc.DockerStatsSampler,
    *,
    source_instance_id: str,
    tag: str,
    start_index: int,
    baseline_mib: float,
    container_name: str,
    post_batch_wait_seconds: float,
    rss_average_window_seconds: float,
    residual_threshold_mib: float,
    end_latency_threshold_seconds: float,
    max_consecutive_timeouts: int,
    max_consecutive_concurrency_retries: int,
) -> dict[str, Any]:
    log("=== test 1b: bulk session dup-only (begin -> 100 dup -> end, x4) ===")

    gc.assert_container_alive(container_name)
    try:
        first_begin = client.begin_bulk_session(timeout=60.0)
    except gc.ImportRequestError as error:
        if error.status_code in gc.SESSION_API_UNAVAILABLE_STATUS_CODES:
            reason = (
                f"bulk-sessions/begin returned HTTP {error.status_code}; "
                "treating the bulk-session API as unavailable in this environment"
            )
            log(f"WARNING: skipping test 1b — {reason}")
            return _skipped_test_result("bulk_session_dup_only", reason)
        raise

    import_latencies: list[float] = []
    import_transient_events: list[dict[str, Any]] = []
    import_concurrency_events: list[dict[str, Any]] = []
    end_durations: list[float] = []
    end_latency_violations: list[dict[str, Any]] = []
    session_states: list[str] = []

    def run_one_batch(batch_num: int, begin_result: gc.BulkSessionResult) -> None:
        session_id = begin_result.session_id
        lo = start_index + batch_num * 100
        hi = lo + 100
        drafts = [
            gc.build_corpus_draft(tag=tag, source_instance_id=source_instance_id, index=i)
            for i in range(lo, hi)
        ]
        context = f"bulk-session dup batch {batch_num + 1} [{lo},{hi})"
        outcome = gc.send_drafts_with_retry(
            client,
            source_instance_id,
            drafts,
            bulk_session_id=session_id,
            timeout=120.0,
            max_consecutive_timeouts=max_consecutive_timeouts,
            max_consecutive_concurrency_retries=max_consecutive_concurrency_retries,
            context=context,
            on_transient=lambda event, context=context: log_transient_event(context, event),
            on_concurrency_limit=lambda event, context=context: log_concurrency_event(context, event),
        )
        import_result = outcome.result
        import_transient_events.extend(outcome.transient_events)
        import_concurrency_events.extend(outcome.concurrency_events)
        gc.assert_all_duplicate(import_result.counts, 100, context=context)
        import_latencies.append(import_result.elapsed_seconds)

        end_result = client.end_bulk_session(session_id, timeout=1800.0)
        end_durations.append(end_result.elapsed_seconds)
        session_states.append(end_result.state)
        if end_result.elapsed_seconds > end_latency_threshold_seconds:
            end_latency_violations.append(
                {"batch": batch_num + 1, "end_seconds": end_result.elapsed_seconds}
            )
        log(
            f"bulk-session dup batch {batch_num + 1}/4: import={import_result.elapsed_seconds:.3f}s "
            f"end={end_result.elapsed_seconds:.3f}s state={end_result.state}"
        )

    run_one_batch(0, first_begin)
    for batch_num in range(1, 4):
        gc.assert_container_alive(container_name)
        begin_result = client.begin_bulk_session(timeout=60.0)
        run_one_batch(batch_num, begin_result)

    log(f"test 1b sending done; settling {post_batch_wait_seconds:.0f}s before residual RSS sample ...")
    time.sleep(post_batch_wait_seconds)
    gc.assert_container_alive(container_name)
    after_mib = sampler.average_recent(rss_average_window_seconds)
    residual_mib = after_mib - baseline_mib

    residual_ok = residual_mib <= residual_threshold_mib
    end_latency_ok = len(end_latency_violations) == 0
    passed = residual_ok and end_latency_ok
    log(
        f"test 1b result: baseline={baseline_mib:.1f}MiB after={after_mib:.1f}MiB "
        f"residual={residual_mib:.1f}MiB (threshold={residual_threshold_mib:.1f}MiB) "
        f"end_durations={[f'{d:.1f}s' for d in end_durations]} "
        f"(threshold={end_latency_threshold_seconds:.1f}s) pass={passed}"
    )
    if end_latency_violations:
        log(f"test 1b FAIL REASON: session-end latency regression reproduced: {end_latency_violations}")
    return {
        "name": "bulk_session_dup_only",
        "passed": passed,
        "skipped": False,
        "import_batch_latencies_seconds": import_latencies,
        "import_transient_events": import_transient_events,
        "import_concurrency_limit_events": import_concurrency_events,
        "end_duration_seconds": end_durations,
        "session_states_after_end": session_states,
        "end_latency_threshold_seconds": end_latency_threshold_seconds,
        "end_latency_violations": end_latency_violations,
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
    max_consecutive_timeouts: int,
    max_consecutive_concurrency_retries: int,
) -> dict[str, Any]:
    log(f"=== test 2 ({label}): {count} new drafts, batch={batch_size} ===")
    marker = sampler.mark()
    total = gc.OutcomeCounts()
    latencies: list[float] = []
    transient_events: list[dict[str, Any]] = []
    concurrency_events: list[dict[str, Any]] = []
    index = start_index
    end = start_index + count
    while index < end:
        gc.assert_container_alive(container_name)
        batch_end = min(index + batch_size, end)
        drafts = [
            gc.build_corpus_draft(tag=tag, source_instance_id=source_instance_id, index=i)
            for i in range(index, batch_end)
        ]
        context = f"{label} batch [{index},{batch_end})"
        outcome = gc.send_drafts_with_retry(
            client,
            source_instance_id,
            drafts,
            timeout=180.0,
            max_consecutive_timeouts=max_consecutive_timeouts,
            max_consecutive_concurrency_retries=max_consecutive_concurrency_retries,
            context=context,
            on_transient=lambda event, context=context: log_transient_event(context, event),
            on_concurrency_limit=lambda event, context=context: log_concurrency_event(context, event),
        )
        result = outcome.result
        transient_events.extend(outcome.transient_events)
        concurrency_events.extend(outcome.concurrency_events)
        gc.assert_all_ingested(result.counts, len(drafts), context=context)
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
        "transient_events": transient_events,
        "concurrency_limit_events": concurrency_events,
        "peak_mib": peak_mib,
        "peak_over_baseline_mib": peak_over_baseline_mib,
        "peak_threshold_mib": peak_threshold_mib,
        "baseline_mib": baseline_mib,
        "after_mib": after_mib,
        "residual_mib": residual_mib,
        "residual_threshold_mib": residual_threshold_mib,
    }


# ---------------------------------------------------------------------------
# Test 2b: new bulk import wrapped in a single bulk session (batch=25)
# ---------------------------------------------------------------------------


def run_test2b_bulk_session_new(
    client: gc.ImportClient,
    sampler: gc.DockerStatsSampler,
    *,
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
    max_consecutive_timeouts: int,
    max_consecutive_concurrency_retries: int,
) -> dict[str, Any]:
    log(f"=== test 2b: bulk session new import ({count} drafts, batch={batch_size}, session-wrapped) ===")

    gc.assert_container_alive(container_name)
    try:
        begin_result = client.begin_bulk_session(timeout=60.0)
    except gc.ImportRequestError as error:
        if error.status_code in gc.SESSION_API_UNAVAILABLE_STATUS_CODES:
            reason = (
                f"bulk-sessions/begin returned HTTP {error.status_code}; "
                "treating the bulk-session API as unavailable in this environment"
            )
            log(f"WARNING: skipping test 2b — {reason}")
            return _skipped_test_result("bulk_session_new_batch25", reason)
        raise

    session_id = begin_result.session_id
    marker = sampler.mark()
    total = gc.OutcomeCounts()
    latencies: list[float] = []
    transient_events: list[dict[str, Any]] = []
    concurrency_events: list[dict[str, Any]] = []
    index = start_index
    end_index = start_index + count
    while index < end_index:
        gc.assert_container_alive(container_name)
        batch_end = min(index + batch_size, end_index)
        drafts = [
            gc.build_corpus_draft(tag=tag, source_instance_id=source_instance_id, index=i)
            for i in range(index, batch_end)
        ]
        context = f"bulk-session new batch [{index},{batch_end})"
        outcome = gc.send_drafts_with_retry(
            client,
            source_instance_id,
            drafts,
            bulk_session_id=session_id,
            timeout=180.0,
            max_consecutive_timeouts=max_consecutive_timeouts,
            max_consecutive_concurrency_retries=max_consecutive_concurrency_retries,
            context=context,
            on_transient=lambda event, context=context: log_transient_event(context, event),
            on_concurrency_limit=lambda event, context=context: log_concurrency_event(context, event),
        )
        result = outcome.result
        transient_events.extend(outcome.transient_events)
        concurrency_events.extend(outcome.concurrency_events)
        gc.assert_all_ingested(result.counts, len(drafts), context=context)
        total = total + result.counts
        latencies.append(result.elapsed_seconds)
        index = batch_end

    end_result = client.end_bulk_session(session_id, timeout=1800.0)
    peak_mib = sampler.peak_since(marker)
    peak_over_baseline_mib = peak_mib - baseline_mib
    log(
        f"test 2b sending+end done ({total.ingested} ingested, end={end_result.elapsed_seconds:.1f}s); "
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
        f"test 2b result: peak={peak_mib:.1f}MiB peak_over_baseline={peak_over_baseline_mib:.1f}MiB "
        f"(threshold={peak_threshold_mib:.1f}MiB) after={after_mib:.1f}MiB "
        f"residual={residual_mib:.1f}MiB (threshold={residual_threshold_mib:.1f}MiB) "
        f"ingested={total.ingested}/{count} pass={passed}"
    )
    return {
        "name": "bulk_session_new_batch25",
        "passed": passed,
        "skipped": False,
        "batch_size": batch_size,
        "count": count,
        "ingested": total.ingested,
        "duplicates": total.duplicates,
        "batch_latencies_seconds": latencies,
        "transient_events": transient_events,
        "concurrency_limit_events": concurrency_events,
        "peak_mib": peak_mib,
        "peak_over_baseline_mib": peak_over_baseline_mib,
        "peak_threshold_mib": peak_threshold_mib,
        "baseline_mib": baseline_mib,
        "after_mib": after_mib,
        "residual_mib": residual_mib,
        "residual_threshold_mib": residual_threshold_mib,
        # informational only — this test's pass/fail uses the same
        # peak/residual thresholds as test 2, per gate policy; only test 1b
        # gates on session-end latency itself.
        "end_duration_seconds": end_result.elapsed_seconds,
        "session_state_after_end": end_result.state,
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
    max_consecutive_timeouts: int,
    max_consecutive_concurrency_retries: int,
) -> dict[str, Any]:
    log("=== test 3: slope detection (100 x 8 dup batches, per-batch settle) ===")
    batch_numbers: list[float] = []
    rss_after_points: list[float] = []
    latencies: list[float] = []
    transient_events: list[dict[str, Any]] = []
    concurrency_events: list[dict[str, Any]] = []
    for batch_num in range(8):
        gc.assert_container_alive(container_name)
        lo = start_index + batch_num * 100
        hi = lo + 100
        drafts = [
            gc.build_corpus_draft(tag=tag, source_instance_id=source_instance_id, index=i)
            for i in range(lo, hi)
        ]
        context = f"slope batch {batch_num + 1} [{lo},{hi})"
        outcome = gc.send_drafts_with_retry(
            client,
            source_instance_id,
            drafts,
            timeout=120.0,
            max_consecutive_timeouts=max_consecutive_timeouts,
            max_consecutive_concurrency_retries=max_consecutive_concurrency_retries,
            context=context,
            on_transient=lambda event, context=context: log_transient_event(context, event),
            on_concurrency_limit=lambda event, context=context: log_concurrency_event(context, event),
        )
        result = outcome.result
        transient_events.extend(outcome.transient_events)
        concurrency_events.extend(outcome.concurrency_events)
        gc.assert_all_duplicate(result.counts, 100, context=context)
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
        "transient_events": transient_events,
        "concurrency_limit_events": concurrency_events,
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

    if args.seed_count < 2000:
        raise gc.GateError(
            "--seed-count must be >= 2000 (test 1 and test 3 each consume 800 "
            "distinct dup-sample indices, and test 1b consumes another 400, "
            "all from the seeded range)"
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
            "bulk_session_end_latency_threshold_seconds": args.bulk_session_end_latency_threshold_seconds,
            "probe_timeout_seconds": args.probe_timeout_seconds,
            "max_consecutive_timeouts": args.max_consecutive_timeouts,
            "max_consecutive_concurrency_retries": args.max_consecutive_concurrency_retries,
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
            probe_timeout_seconds=args.probe_timeout_seconds,
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
            max_consecutive_timeouts=args.max_consecutive_timeouts,
            max_consecutive_concurrency_retries=args.max_consecutive_concurrency_retries,
        )
        report["tests"].append(test1)

        test1b = run_test1b_bulk_session_dup_only(
            client,
            sampler,
            source_instance_id=args.source_instance,
            tag=args.tag,
            start_index=1600,
            baseline_mib=baseline_mib,
            container_name=container_name,
            post_batch_wait_seconds=args.post_batch_wait_seconds,
            rss_average_window_seconds=args.rss_average_window_seconds,
            residual_threshold_mib=args.dup_residual_threshold_mib,
            end_latency_threshold_seconds=args.bulk_session_end_latency_threshold_seconds,
            max_consecutive_timeouts=args.max_consecutive_timeouts,
            max_consecutive_concurrency_retries=args.max_consecutive_concurrency_retries,
        )
        report["tests"].append(test1b)

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
            max_consecutive_timeouts=args.max_consecutive_timeouts,
            max_consecutive_concurrency_retries=args.max_consecutive_concurrency_retries,
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
            max_consecutive_timeouts=args.max_consecutive_timeouts,
            max_consecutive_concurrency_retries=args.max_consecutive_concurrency_retries,
        )
        report["tests"].append(test2b)

        bulk_session_new_start = args.seed_count + 2000
        test2b_session = run_test2b_bulk_session_new(
            client,
            sampler,
            source_instance_id=args.source_instance,
            tag=args.tag,
            start_index=bulk_session_new_start,
            count=1000,
            batch_size=25,
            baseline_mib=baseline_mib,
            container_name=container_name,
            post_batch_wait_seconds=args.post_batch_wait_seconds,
            rss_average_window_seconds=args.rss_average_window_seconds,
            peak_threshold_mib=args.bulk_peak_threshold_mib,
            residual_threshold_mib=args.bulk_residual_threshold_mib,
            max_consecutive_timeouts=args.max_consecutive_timeouts,
            max_consecutive_concurrency_retries=args.max_consecutive_concurrency_retries,
        )
        report["tests"].append(test2b_session)

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
            max_consecutive_timeouts=args.max_consecutive_timeouts,
            max_consecutive_concurrency_retries=args.max_consecutive_concurrency_retries,
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
        if test.get("skipped"):
            status = "SKIP"
        else:
            status = "PASS" if test.get("passed") else "FAIL"
        log(f"  [{status}] {test.get('name')}")
        if test.get("skipped"):
            log(f"         reason: {test.get('skip_reason')}")
    if "peak_mib_overall" in report:
        log(f"peak RSS observed (overall): {report['peak_mib_overall']:.1f}MiB")
    log("=" * 72)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except gc.GateError as error:
        print(f"run_gate failed: {error}", file=sys.stderr)
        sys.exit(1)
