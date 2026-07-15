#!/usr/bin/env python3
"""Prepare and run the persistent Corpus index performance benchmark."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import math
import os
import platform
import re
import stat
import statistics
import subprocess
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FORMAT_VERSION = "2.0.0"
GENERATOR_VERSION = "6.0.0"
EXPECTED_RECORDS = 500_000
EXPECTED_SIZES = (10_000, 50_000, 100_000, 500_000)
EXPECTED_BATCH_SIZE = 10_000
EXPECTED_SEARCH_REQUESTS = 60
EXPECTED_SEARCH_CONCURRENCY = 2
EXPECTED_SEARCH_LIMIT = 20
EXPECTED_WARMUP_ROUNDS = 2
EXPECTED_TIMEOUT_SECONDS = 1800.0
EXPECTED_READY_TIMEOUT_SECONDS = 300.0
BENCHMARK_SEED = 20_260_713
BENCHMARK_BODY_BYTES = 384
BENCHMARK_CHANNEL_COUNT = 4
BENCHMARK_TIMESTAMP_EPOCH_SECONDS = 1_656_633_600
BENCHMARK_TIMESTAMP_STEP_SECONDS = 220
BENCHMARK_TIMESTAMP_PERMUTATION = 104_729
FOUR_GIB = 4 * 1024 * 1024 * 1024
FOUR_CPUS_NANO = 4_000_000_000
PEAK_RSS_LIMIT_BYTES = 5 * 1024 * 1024 * 1024 // 2
SEARCH_P95_LIMIT_SECONDS = 2.0
DEVELOPMENT_HEADROOM_P95_LIMIT_SECONDS = 1.0
BENCHMARK_SOURCE_INSTANCE_ID = "persistent-index-benchmark"
BENCHMARK_PURPOSE = "persistent-search-index-benchmark"
PURPOSE_LABEL = "com.hlab.lethe.purpose"
ROOT_LABEL = "com.hlab.lethe.benchmark-root"
STORAGE_LABEL = "com.hlab.lethe.benchmark-storage"
EXPECTED_STORAGE_LABEL = "native-ext4-bind"
HTTP_PORT_LABEL = "8080/tcp"
DATABASE_DIRECTORY_NAME = "db"
DATA_MOUNT_DESTINATION = "/var/lib/lethe"
CONFIG_MOUNT_DESTINATION = "/etc/lethe/config.toml"
JWKS_MOUNT_DESTINATION = "/etc/lethe/mcp-jwks.json"
TMPFS_DESTINATION = "/tmp"
COMPOSE_PROJECT_LABEL = "com.docker.compose.project"
COMPOSE_SERVICE_LABEL = "com.docker.compose.service"
COMPOSE_PROJECT = "lethe-persistent-index-benchmark"
COMPOSE_SERVICE = "lethe-selfhost"
SOURCE_HEAD_LABEL = "com.hlab.lethe.benchmark-source-head"
SOURCE_TREE_SHA256_LABEL = "com.hlab.lethe.benchmark-source-tree-sha256"
DATASET_FILE = "drafts.jsonl"
QUERY_FILE = "queries.json"
MANIFEST_FILE = "manifest.json"
CANONICAL_JSON_META_KEY = "canonical_json"
PRIMARY_TERMS = ("ルール", "規則", "生活", "確認", "利用", "申請", "連絡", "対応")
FILLER_WORDS = (
    "persistent",
    "index",
    "benchmark",
    "corpus",
    "observation",
    "projection",
    "incremental",
    "durable",
    "検索",
    "手順",
    "運用",
    "記録",
    "受付",
    "案内",
    "検証",
    "履歴",
)


class BenchmarkError(RuntimeError):
    """Raised when a benchmark invariant is not satisfied."""


class LetheHttpError(BenchmarkError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code


class LetheTransportError(BenchmarkError):
    """Raised when the dedicated loopback endpoint is not reachable."""


@dataclass(frozen=True)
class QueryCase:
    case_id: str
    mode: str
    request: dict[str, Any]
    expected_min_matches: int


@dataclass(frozen=True)
class ContainerMetrics:
    vm_rss_kib: int
    vm_hwm_kib: int
    cgroup_memory_current_bytes: int
    cgroup_memory_peak_bytes: int
    swap_current_bytes: int
    swap_peak_bytes: int
    oom_events: int
    oom_kill_events: int
    docker_oom_killed: bool
    restart_count: int


DockerRunner = Callable[[list[str]], subprocess.CompletedProcess[str]]
GitRunner = Callable[[list[str], Path], bytes]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "fingerprint",
        description="Print the repository identity required for the benchmark image",
    )

    prepare = subparsers.add_parser(
        "prepare", description="Generate the deterministic 500k synthetic dataset"
    )
    prepare.add_argument("--work-dir", type=Path, required=True)
    prepare.add_argument("--records", type=int, required=True)
    prepare.add_argument("--seed", type=int, required=True)
    prepare.add_argument("--body-bytes", type=int, required=True)

    run = subparsers.add_parser(
        "run", description="Import staged prefixes and measure HTTP search latency"
    )
    run.add_argument("--work-dir", type=Path, required=True)
    run.add_argument("--base-url", required=True)
    run.add_argument("--read-token-env", required=True)
    run.add_argument("--write-token-env", required=True)
    run.add_argument("--docker-container", required=True)
    run.add_argument("--source-instance-id", required=True)
    run.add_argument("--sizes", required=True)
    run.add_argument("--batch-size", type=int, required=True)
    run.add_argument("--warmup-rounds", type=int, required=True)
    run.add_argument("--search-requests", type=int, required=True)
    run.add_argument("--search-concurrency", type=int, required=True)
    run.add_argument("--search-limit", type=int, required=True)
    run.add_argument("--timeout-seconds", type=float, required=True)
    run.add_argument("--report", type=Path, required=True)
    run.add_argument("--source-head", required=True)
    run.add_argument("--source-tree-sha256", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "fingerprint":
            fingerprint = repository_fingerprint()
            print(json.dumps(fingerprint, ensure_ascii=False, sort_keys=True))
            return 0
        if args.command == "prepare":
            manifest = prepare_command(args)
            print(
                json.dumps(
                    {
                        "records": manifest["records"],
                        "drafts_bytes": manifest["drafts_bytes"],
                        "drafts_sha256": manifest["drafts_sha256"],
                        "work_dir": str(args.work_dir.resolve()),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "run":
            report = run_command(args)
            print(
                json.dumps(
                    {
                        "passed": report["passed"],
                        "stages": len(report["stages"]),
                        "status": report["status"],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0 if report["passed"] else 2
    except (BenchmarkError, OSError, ValueError) as error:
        parser.error(str(error))
    raise AssertionError(f"unhandled command: {args.command}")


def prepare_command(args: argparse.Namespace) -> dict[str, Any]:
    if args.records != EXPECTED_RECORDS:
        raise BenchmarkError(
            f"--records must be exactly {EXPECTED_RECORDS}, got {args.records}"
        )
    if args.seed != BENCHMARK_SEED:
        raise BenchmarkError(f"--seed must be exactly {BENCHMARK_SEED}")
    if args.body_bytes != BENCHMARK_BODY_BYTES:
        raise BenchmarkError(
            f"--body-bytes must be exactly {BENCHMARK_BODY_BYTES}"
        )
    work_dir = prepare_empty_external_work_dir(args.work_dir)
    return prepare_dataset(
        work_dir=work_dir,
        record_count=args.records,
        seed=args.seed,
        body_bytes=args.body_bytes,
        sizes=EXPECTED_SIZES,
    )


def run_command(args: argparse.Namespace) -> dict[str, Any]:
    work_dir = require_external_work_dir(args.work_dir)
    storage = require_native_ext4_work_dir(work_dir)
    sizes = parse_sizes(args.sizes)
    validate_run_arguments(args, sizes)
    report_path = validate_report_path(args.report, work_dir)
    manifest, query_cases = validate_prepared_inputs(work_dir, sizes)
    repository = run_repository_fingerprint(args)

    if args.read_token_env == args.write_token_env:
        raise BenchmarkError("read and write token environment names must differ")
    read_token = required_environment(args.read_token_env)
    write_token = required_environment(args.write_token_env)
    if hmac.compare_digest(read_token, write_token):
        raise BenchmarkError("read and write token values must differ")

    read_client = LetheHttpClient(
        base_url=args.base_url,
        token=read_token,
        timeout_seconds=args.timeout_seconds,
    )
    write_client = LetheHttpClient(
        base_url=args.base_url,
        token=write_token,
        timeout_seconds=args.timeout_seconds,
    )
    docker_metrics = DockerProcessMetrics(
        container=args.docker_container,
        work_dir=work_dir,
        base_url=args.base_url,
        repository=repository,
    )

    target = docker_metrics.preflight()
    startup = wait_for_ready_empty_corpus(read_client, docker_metrics)
    initial_metrics = docker_metrics.snapshot()
    require_clean_initial_metrics(initial_metrics)

    report: dict[str, Any] = {
        "format_version": FORMAT_VERSION,
        "status": "running",
        "passed": False,
        "failure": None,
        "repository": repository,
        "host": host_fingerprint(),
        "container": target,
        "storage": storage,
        "startup": startup,
        "dataset": manifest,
        "limits": {
            "memory_bytes": FOUR_GIB,
            "memory_plus_swap_bytes": FOUR_GIB,
            "swap_bytes": 0,
            "cpus": 4,
            "effective_search_p95_seconds": SEARCH_P95_LIMIT_SECONDS,
            "unfiltered_search_p95_seconds": None,
            "development_headroom_p95_seconds": DEVELOPMENT_HEADROOM_P95_LIMIT_SECONDS,
            "peak_rss_bytes": PEAK_RSS_LIMIT_BYTES,
        },
        "workload": {
            "sizes": list(sizes),
            "batch_size": args.batch_size,
            "warmup_rounds": args.warmup_rounds,
            "cursor_sanity_requests_per_stage": 2,
            "warmup_requests_per_stage": args.warmup_rounds
            * len(query_cases),
            "search_requests": args.search_requests,
            "search_concurrency": args.search_concurrency,
            "search_limit": args.search_limit,
            "timeout_seconds": args.timeout_seconds,
            "query_cases": [
                {"id": case.case_id, "mode": case.mode} for case in query_cases
            ],
            "effective_query_cases": [
                case.case_id for case in query_cases if case.mode != "unfiltered"
            ],
            "unfiltered_query_cases": [
                case.case_id for case in query_cases if case.mode == "unfiltered"
            ],
        },
        "stages": [],
    }
    atomic_write_json(report_path, report)

    dataset_path = work_dir / DATASET_FILE
    drafts = iter_jsonl_objects(dataset_path)
    imported_total = 0
    previous_metrics = initial_metrics
    active_target_records: int | None = None
    try:
        for target_records in sizes:
            active_target_records = target_records
            stage = run_stage(
                target_records=target_records,
                dataset_prefix_sha256=manifest["prefix_sha256"][
                    str(target_records)
                ],
                imported_total=imported_total,
                drafts=drafts,
                write_client=write_client,
                read_client=read_client,
                docker_metrics=docker_metrics,
                query_cases=query_cases,
                source_instance_id=args.source_instance_id,
                batch_size=args.batch_size,
                warmup_rounds=args.warmup_rounds,
                search_requests=args.search_requests,
                search_concurrency=args.search_concurrency,
                search_limit=args.search_limit,
                previous_metrics=previous_metrics,
            )
            report["stages"].append(stage)
            imported_total = target_records
            previous_metrics = ContainerMetrics(**stage["container_metrics"])
            atomic_write_json(report_path, report)
            if stage["container_terminated"]:
                raise BenchmarkError(
                    f"container became unsafe to continue at {target_records} records"
                )

        try:
            next(drafts)
        except StopIteration:
            pass
        else:
            raise BenchmarkError("dataset contains records beyond the declared final stage")

        report["passed"] = all(stage["passed"] for stage in report["stages"])
        report["status"] = "complete"
        atomic_write_json(report_path, report)
        return report
    except BaseException as error:
        report["status"] = "failed"
        report["failure"] = {
            "type": type(error).__name__,
            "message": str(error),
            "target_records": active_target_records,
        }
        report["container_failure_evidence"] = docker_metrics.failure_evidence()
        atomic_write_json(report_path, report)
        raise


def validate_run_arguments(args: argparse.Namespace, sizes: tuple[int, ...]) -> None:
    if sizes != EXPECTED_SIZES:
        raise BenchmarkError(
            "--sizes must be exactly 10000,50000,100000,500000"
        )
    expected_values = {
        "--batch-size": (args.batch_size, EXPECTED_BATCH_SIZE),
        "--warmup-rounds": (args.warmup_rounds, EXPECTED_WARMUP_ROUNDS),
        "--search-requests": (args.search_requests, EXPECTED_SEARCH_REQUESTS),
        "--search-concurrency": (
            args.search_concurrency,
            EXPECTED_SEARCH_CONCURRENCY,
        ),
        "--search-limit": (args.search_limit, EXPECTED_SEARCH_LIMIT),
    }
    for name, (actual, expected) in expected_values.items():
        if actual != expected:
            raise BenchmarkError(f"{name} must be exactly {expected}, got {actual}")
    if args.timeout_seconds != EXPECTED_TIMEOUT_SECONDS:
        raise BenchmarkError(
            f"--timeout-seconds must be exactly {EXPECTED_TIMEOUT_SECONDS:g}"
        )
    if args.source_instance_id != BENCHMARK_SOURCE_INSTANCE_ID:
        raise BenchmarkError(
            f"--source-instance-id must be {BENCHMARK_SOURCE_INSTANCE_ID!r}"
        )


def parse_sizes(raw: str) -> tuple[int, ...]:
    if not raw or any(not part.isdigit() for part in raw.split(",")):
        raise BenchmarkError("--sizes must be a comma-separated list of integers")
    values = tuple(int(part) for part in raw.split(","))
    if values != EXPECTED_SIZES:
        raise BenchmarkError("--sizes must be exactly 10000,50000,100000,500000")
    return values


def wait_for_ready_empty_corpus(
    client: "LetheHttpClient", docker_metrics: "DockerProcessMetrics"
) -> dict[str, Any]:
    started = time.monotonic()
    deadline = started + EXPECTED_READY_TIMEOUT_SECONDS
    attempts = 0
    last_error: str | None = None
    while True:
        attempts += 1
        try:
            corpus_total = client.corpus_total()
        except LetheHttpError as error:
            if error.status_code != 503:
                raise
            last_error = str(error)
        except LetheTransportError as error:
            last_error = str(error)
        else:
            if corpus_total != 0:
                raise BenchmarkError("benchmark requires an empty Corpus")
            return {
                "ready_wait_seconds": time.monotonic() - started,
                "ready_attempts": attempts,
                "corpus_records": corpus_total,
            }
        if time.monotonic() >= deadline:
            raise BenchmarkError(
                "LETHE did not become ready with an empty Corpus within "
                f"{EXPECTED_READY_TIMEOUT_SECONDS:g} seconds: {last_error}"
            )
        docker_metrics.assert_runtime_invariants()
        time.sleep(0.25)


def prepare_empty_external_work_dir(path: Path) -> Path:
    resolved = resolve_external_path(path, "--work-dir")
    if resolved.exists():
        if not resolved.is_dir():
            raise BenchmarkError(f"--work-dir is not a directory: {resolved}")
        if any(resolved.iterdir()):
            raise BenchmarkError(f"--work-dir must be empty: {resolved}")
    else:
        resolved.mkdir(parents=True)
    reject_link_or_reparse_components(resolved, "--work-dir")
    return resolved


def require_external_work_dir(path: Path) -> Path:
    resolved = resolve_external_path(path, "--work-dir")
    if not resolved.is_dir():
        raise BenchmarkError(f"--work-dir does not exist: {resolved}")
    require_plain_database_directory(resolved)
    return resolved


def resolve_external_path(path: Path, name: str) -> Path:
    if not path.is_absolute():
        raise BenchmarkError(f"{name} must be an absolute path")
    reject_link_or_reparse_components(path, name)
    resolved = path.resolve()
    repository = REPOSITORY_ROOT.resolve(strict=True)
    if resolved == repository or repository in resolved.parents:
        raise BenchmarkError(f"{name} must be outside the repository")
    return resolved


def require_plain_database_directory(work_dir: Path) -> Path:
    database_dir = work_dir / DATABASE_DIRECTORY_NAME
    reject_link_or_reparse_components(database_dir, "benchmark database directory")
    if not database_dir.is_dir():
        raise BenchmarkError(
            f"benchmark database directory does not exist: {database_dir}"
        )
    resolved = database_dir.resolve(strict=True)
    if resolved.parent != work_dir.resolve(strict=True):
        raise BenchmarkError(
            "benchmark database directory must be a direct child of --work-dir"
        )
    return resolved


def require_native_ext4_work_dir(work_dir: Path) -> dict[str, str]:
    """Require the benchmark root to live on a native Linux ext4 mount."""
    if platform.system() != "Linux":
        raise BenchmarkError("benchmark must run from native WSL/Linux, not Windows")
    try:
        result = subprocess.run(
            [
                "findmnt",
                "--noheadings",
                "--output",
                "FSTYPE,SOURCE,TARGET",
                "--target",
                str(work_dir),
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=30,
        )
    except FileNotFoundError as error:
        raise BenchmarkError("findmnt is required to verify native ext4 storage") from error
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() if error.stderr else "no stderr"
        raise BenchmarkError(f"findmnt failed for benchmark storage: {detail}") from error
    except subprocess.TimeoutExpired as error:
        raise BenchmarkError("findmnt timed out while verifying benchmark storage") from error
    fields = result.stdout.split()
    if len(fields) != 3:
        raise BenchmarkError("findmnt returned an invalid benchmark storage shape")
    filesystem_type, source, target = fields
    if filesystem_type != "ext4":
        raise BenchmarkError(
            f"benchmark work-dir must be on native ext4, got {filesystem_type!r}"
        )
    return {
        "work_dir": str(work_dir),
        "filesystem_type": filesystem_type,
        "mount_source": source,
        "mount_target": target,
        "database_directory": str(work_dir / DATABASE_DIRECTORY_NAME),
        "dataset_storage": "same-native-ext4-work-dir",
    }


def reject_link_or_reparse_components(path: Path, name: str) -> None:
    if not path.is_absolute():
        raise BenchmarkError(f"{name} must be an absolute path")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            break
        attributes = getattr(metadata, "st_file_attributes", 0)
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        if stat.S_ISLNK(metadata.st_mode) or (
            reparse_flag and attributes & reparse_flag
        ):
            raise BenchmarkError(
                f"{name} must not contain a symbolic link or reparse point: {current}"
            )


def validate_report_path(path: Path, work_dir: Path) -> Path:
    if not path.is_absolute():
        raise BenchmarkError("--report must be an absolute path")
    reject_link_or_reparse_components(path, "--report")
    resolved = path.resolve()
    if not path_is_within(resolved, work_dir):
        raise BenchmarkError("--report must be inside --work-dir")
    if resolved.exists():
        raise BenchmarkError(f"--report already exists: {resolved}")
    inputs = {
        (work_dir / DATASET_FILE).resolve(),
        (work_dir / QUERY_FILE).resolve(),
        (work_dir / MANIFEST_FILE).resolve(),
    }
    if resolved in inputs:
        raise BenchmarkError("--report collides with a prepared input")
    return resolved


def path_is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def prepare_dataset(
    *,
    work_dir: Path,
    record_count: int,
    seed: int,
    body_bytes: int,
    sizes: tuple[int, ...],
) -> dict[str, Any]:
    if record_count <= 0 or body_bytes <= 0:
        raise BenchmarkError("record_count and body_bytes must be positive")
    if not sizes or sizes[-1] != record_count:
        raise BenchmarkError("the last dataset size must equal record_count")
    if any(left >= right for left, right in zip(sizes, sizes[1:])):
        raise BenchmarkError("dataset sizes must be strictly increasing")

    database_dir = work_dir / DATABASE_DIRECTORY_NAME
    database_dir.mkdir()
    database_dir.chmod(0o777)
    query_document = query_manifest_document()
    query_bytes = encode_json_document(query_document)
    query_path = work_dir / QUERY_FILE
    atomic_write_bytes(query_path, query_bytes)

    dataset_path = work_dir / DATASET_FILE
    temporary = dataset_path.with_suffix(dataset_path.suffix + ".tmp")
    digest = hashlib.sha256()
    total_bytes = 0
    prefix_sha256: dict[str, str] = {}
    size_boundaries = set(sizes)
    try:
        with temporary.open("xb") as destination:
            for index in range(record_count):
                encoded = encode_json_line(synthetic_draft(index, seed, body_bytes))
                destination.write(encoded)
                digest.update(encoded)
                total_bytes += len(encoded)
                record_number = index + 1
                if record_number in size_boundaries:
                    prefix_sha256[str(record_number)] = digest.hexdigest()
            destination.flush()
            os.fsync(destination.fileno())
        temporary.replace(dataset_path)
    except BaseException:
        if temporary.exists():
            temporary.unlink()
        raise

    manifest = {
        "format_version": FORMAT_VERSION,
        "generator": "lethe-persistent-index-benchmark",
        "generator_version": GENERATOR_VERSION,
        "records": record_count,
        "sizes": list(sizes),
        "seed": seed,
        "body_bytes": body_bytes,
        "schema": "schema:slack-message",
        "source_system": "sys:slack",
        "drafts_file": DATASET_FILE,
        "drafts_bytes": total_bytes,
        "drafts_sha256": digest.hexdigest(),
        "prefix_sha256": prefix_sha256,
        "queries_file": QUERY_FILE,
        "queries_sha256": hashlib.sha256(query_bytes).hexdigest(),
    }
    atomic_write_json(work_dir / MANIFEST_FILE, manifest)
    return manifest


def synthetic_draft(index: int, seed: int, body_bytes: int) -> dict[str, Any]:
    channel_id = "C01BENCH"
    channel_number = (index // len(PRIMARY_TERMS)) % BENCHMARK_CHANNEL_COUNT
    channel_name = f"{100 + channel_number:03d}_benchmark"
    timestamp_slot = (index * BENCHMARK_TIMESTAMP_PERMUTATION) % EXPECTED_RECORDS
    timestamp_seconds = (
        BENCHMARK_TIMESTAMP_EPOCH_SECONDS
        + timestamp_slot * BENCHMARK_TIMESTAMP_STEP_SECONDS
    )
    timestamp = f"{timestamp_seconds}.000000"
    thread_root_seconds = timestamp_seconds if index % 10 else timestamp_seconds - min(index, 9)
    thread_timestamp = f"{thread_root_seconds}.000000"
    published = datetime.fromtimestamp(timestamp_seconds, timezone.utc)
    published_text = published.isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
    user_number = index % 256
    user_id = f"U{user_number:08d}"
    user_name = f"benchmark-user-{user_number:03d}"
    email = f"benchmark-user-{user_number:03d}@example.invalid"
    text = synthetic_text(index, seed, body_bytes)
    canonical_json = json.dumps(
        {
            "sender": user_id,
            "body": text,
            "event_time": timestamp,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    identity_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    object_id = f"channel:{channel_id}:ts:{timestamp}"
    return {
        "schema": "schema:slack-message",
        "schema_version": "1.0.0",
        "observer": "obs:slack-crawler",
        "source_system": "sys:slack",
        "authority_model": "lake_authoritative",
        "capture_model": "event",
        "subject": f"message:slack:{channel_id}-{timestamp}",
        "payload": {
            "channel_id": channel_id,
            "channel_name": channel_name,
            "ts": timestamp,
            "thread_ts": thread_timestamp,
            "user_id": user_id,
            "user_name": user_name,
            "email": email,
            "text": text,
            "permalink": (
                "https://benchmark.invalid/archives/"
                f"{channel_id}/p{timestamp.replace('.', '')}"
            ),
            "is_public_channel": True,
            "visibility_status": "public",
            "is_bot": False,
            "ingress_kind": "channel",
            "mentions": [],
            "message_type": "message",
            "authority": 1,
        },
        "attachments": [],
        "published": published_text,
        "idempotency_key": f"slack:{object_id}:{identity_hash}",
        "meta": {
            "sourceAdapterVersion": "rulebot-slack-export/1.0.0",
            CANONICAL_JSON_META_KEY: canonical_json,
            "object_id": object_id,
            "source_container": channel_id,
            "communication_channel_kind": "slack",
            "communication_channel_external_id": channel_id,
            "communication_sender_id": user_id,
            "communication_thread_ref": f"slack:thread:{thread_timestamp}",
        },
    }


def synthetic_text(index: int, seed: int, minimum_bytes: int) -> str:
    primary = PRIMARY_TERMS[(index + seed) % len(PRIMARY_TERMS)]
    parts = [
        f"合成検索記録 {index:06d}",
        primary,
        "persistent",
        "index",
    ]
    if primary == "申請":
        parts.append("手順")
    if primary == "連絡":
        parts.append("対応")
    byte_count = sum(len(part.encode("utf-8")) for part in parts) + len(parts) - 1
    state = (seed ^ ((index + 1) * 0x9E3779B97F4A7C15)) & 0xFFFFFFFFFFFFFFFF
    while byte_count < minimum_bytes:
        state = (6_364_136_223_846_793_005 * state + 1_442_695_040_888_963_407) & 0xFFFFFFFFFFFFFFFF
        second = (state ^ (state >> 29) ^ ((index + 1) << 17)) & 0xFFFFFFFFFFFFFFFF
        token = base64.urlsafe_b64encode(
            state.to_bytes(8, "big") + second.to_bytes(8, "big")
        ).decode("ascii").rstrip("=")
        word = f"{FILLER_WORDS[state % len(FILLER_WORDS)]}-{token}"
        parts.append(word)
        byte_count += 1 + len(word.encode("utf-8"))
    return " ".join(parts)


def query_manifest_document() -> dict[str, Any]:
    cases: list[dict[str, Any]] = []
    base_filters = {
        "types": [],
        "from": None,
        "to": None,
        "channels": [],
        "containers": [],
    }
    recent_year = {
        "types": ["slack"],
        "from": "2025-01-01T00:00:00Z",
        "to": "2026-01-01T00:00:00Z",
        "channels": [],
        "containers": [],
    }

    def append_case(
        case_id: str,
        mode: str,
        pattern: str,
        filters: dict[str, Any],
        order: str,
    ) -> None:
        cases.append(
            {
                "id": case_id,
                "mode": mode,
                "expected_min_matches": EXPECTED_SEARCH_LIMIT,
                "request": {
                    "pattern": pattern,
                    "filters": filters,
                    "normalization": "nfkc",
                    "order": order,
                    "limit": EXPECTED_SEARCH_LIMIT,
                },
            }
        )

    for index, term in enumerate(PRIMARY_TERMS):
        if index % 3 == 0:
            filters = dict(recent_year)
            mode = "effective-date-range"
        elif index % 3 == 1:
            filters = {
                "types": ["slack"],
                "from": None,
                "to": None,
                "channels": ["101_benchmark"],
                "containers": [],
            }
            mode = "effective-channel-source"
        else:
            filters = dict(recent_year)
            filters["channels"] = ["102_benchmark"]
            mode = "effective-date-channel-source"
        append_case(
            f"wave2-{index + 1:02d}",
            mode,
            term,
            filters,
            "date_asc" if index % 2 else "date_desc",
        )

    append_case(
        "and-ascii-space",
        "effective-compound-and",
        "申請 手順",
        {
            "types": ["slack"],
            "from": "2025-01-01T00:00:00Z",
            "to": "2026-01-01T00:00:00Z",
            "channels": ["100_benchmark"],
            "containers": [],
        },
        "date_desc",
    )
    append_case(
        "and-fullwidth-space",
        "effective-compound-and",
        "連絡　対応",
        {
            "types": ["slack"],
            "from": None,
            "to": None,
            "channels": ["103_benchmark"],
            "containers": [],
        },
        "date_asc",
    )
    append_case(
        "all-records-literal",
        "unfiltered",
        "persistent",
        base_filters,
        "date_desc",
    )
    append_case(
        "unfiltered-regulation",
        "unfiltered",
        "規則",
        base_filters,
        "date_asc",
    )
    append_case(
        "unfiltered-use",
        "unfiltered",
        "利用",
        base_filters,
        "date_desc",
    )
    append_case(
        "unfiltered-and-ascii",
        "unfiltered",
        "申請 手順",
        base_filters,
        "date_desc",
    )
    append_case(
        "unfiltered-and-fullwidth",
        "unfiltered",
        "連絡　対応",
        base_filters,
        "date_asc",
    )
    return {
        "format_version": FORMAT_VERSION,
        "cases": cases,
    }


def validate_prepared_inputs(
    work_dir: Path, sizes: tuple[int, ...]
) -> tuple[dict[str, Any], list[QueryCase]]:
    manifest_path = work_dir / MANIFEST_FILE
    manifest = read_json_object(manifest_path, "dataset manifest")
    expected_keys = {
        "format_version",
        "generator",
        "generator_version",
        "records",
        "sizes",
        "seed",
        "body_bytes",
        "schema",
        "source_system",
        "drafts_file",
        "drafts_bytes",
        "drafts_sha256",
        "prefix_sha256",
        "queries_file",
        "queries_sha256",
    }
    if set(manifest) != expected_keys:
        raise BenchmarkError("dataset manifest has an unexpected shape")
    if (
        manifest["format_version"] != FORMAT_VERSION
        or manifest["generator"] != "lethe-persistent-index-benchmark"
        or manifest["generator_version"] != GENERATOR_VERSION
        or manifest["records"] != EXPECTED_RECORDS
        or manifest["sizes"] != list(sizes)
        or manifest["seed"] != BENCHMARK_SEED
        or manifest["body_bytes"] != BENCHMARK_BODY_BYTES
        or manifest["schema"] != "schema:slack-message"
        or manifest["source_system"] != "sys:slack"
        or manifest["drafts_file"] != DATASET_FILE
        or manifest["queries_file"] != QUERY_FILE
    ):
        raise BenchmarkError("dataset manifest does not authorize this benchmark")
    if not isinstance(manifest["seed"], int) or isinstance(manifest["seed"], bool):
        raise BenchmarkError("dataset manifest seed is invalid")
    if (
        not isinstance(manifest["body_bytes"], int)
        or isinstance(manifest["body_bytes"], bool)
        or manifest["body_bytes"] <= 0
    ):
        raise BenchmarkError("dataset manifest body_bytes is invalid")
    if not valid_sha256(manifest["drafts_sha256"]) or not valid_sha256(
        manifest["queries_sha256"]
    ):
        raise BenchmarkError("dataset manifest contains an invalid SHA-256")
    expected_prefix_keys = {str(size) for size in sizes}
    prefix_sha256 = manifest["prefix_sha256"]
    if (
        not isinstance(prefix_sha256, dict)
        or set(prefix_sha256) != expected_prefix_keys
        or any(not valid_sha256(value) for value in prefix_sha256.values())
        or prefix_sha256[str(sizes[-1])] != manifest["drafts_sha256"]
    ):
        raise BenchmarkError("dataset manifest prefix SHA-256 values are invalid")

    dataset_path = work_dir / DATASET_FILE
    actual_hash, actual_bytes, actual_records, actual_prefix_sha256 = (
        hash_jsonl_with_prefixes(dataset_path, sizes)
    )
    if (
        actual_hash != manifest["drafts_sha256"]
        or actual_bytes != manifest["drafts_bytes"]
        or actual_records != manifest["records"]
        or actual_prefix_sha256 != prefix_sha256
    ):
        raise BenchmarkError("prepared Draft JSONL does not match its manifest")

    query_path = work_dir / QUERY_FILE
    query_bytes = query_path.read_bytes()
    if hashlib.sha256(query_bytes).hexdigest() != manifest["queries_sha256"]:
        raise BenchmarkError("prepared query file does not match its manifest")
    query_cases = load_query_cases(query_bytes)
    if EXPECTED_SEARCH_REQUESTS % len(query_cases) != 0:
        raise BenchmarkError("search requests must be evenly divisible by query cases")
    return manifest, query_cases


def load_query_cases(raw: bytes) -> list[QueryCase]:
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BenchmarkError("query manifest is invalid JSON") from error
    if not isinstance(document, dict) or set(document) != {"format_version", "cases"}:
        raise BenchmarkError("query manifest has an unexpected shape")
    if document["format_version"] != FORMAT_VERSION:
        raise BenchmarkError("query manifest format version is unsupported")
    if document != query_manifest_document():
        raise BenchmarkError("query manifest does not match the fixed benchmark workload")
    raw_cases = document["cases"]
    if not isinstance(raw_cases, list) or len(raw_cases) != 15:
        raise BenchmarkError("query manifest must contain exactly fifteen cases")
    cases: list[QueryCase] = []
    for item in raw_cases:
        if not isinstance(item, dict) or set(item) != {
            "id",
            "mode",
            "request",
            "expected_min_matches",
        }:
            raise BenchmarkError("query case has an unexpected shape")
        case_id = item["id"]
        mode = item["mode"]
        request = item["request"]
        expected = item["expected_min_matches"]
        if not isinstance(case_id, str) or not case_id.strip():
            raise BenchmarkError("query case id must not be blank")
        if mode not in {
            "effective-date-range",
            "effective-channel-source",
            "effective-date-channel-source",
            "effective-compound-and",
            "unfiltered",
        }:
            raise BenchmarkError("query case mode is invalid")
        if not isinstance(request, dict):
            raise BenchmarkError("query case request must be an object")
        if request.get("limit") != EXPECTED_SEARCH_LIMIT:
            raise BenchmarkError("query case limit must be 20")
        if not isinstance(expected, int) or isinstance(expected, bool) or expected < 1:
            raise BenchmarkError("query expected_min_matches is invalid")
        cases.append(QueryCase(case_id, mode, request, expected))
    if len({case.case_id for case in cases}) != len(cases):
        raise BenchmarkError("query case ids must be unique")
    if sum(case.mode == "unfiltered" for case in cases) != 5:
        raise BenchmarkError("query manifest must contain five unfiltered cases")
    if sum(case.mode != "unfiltered" for case in cases) != 10:
        raise BenchmarkError("query manifest must contain ten effective cases")
    return cases


def run_stage(
    *,
    target_records: int,
    dataset_prefix_sha256: str,
    imported_total: int,
    drafts: Iterator[dict[str, Any]],
    write_client: "LetheHttpClient",
    read_client: "LetheHttpClient",
    docker_metrics: "DockerProcessMetrics",
    query_cases: list[QueryCase],
    source_instance_id: str,
    batch_size: int,
    warmup_rounds: int,
    search_requests: int,
    search_concurrency: int,
    search_limit: int,
    previous_metrics: ContainerMetrics,
) -> dict[str, Any]:
    if target_records <= imported_total:
        raise BenchmarkError("stage target must be greater than the imported total")
    if not valid_sha256(dataset_prefix_sha256):
        raise BenchmarkError("stage dataset prefix SHA-256 is invalid")
    imported = 0
    duplicates = 0
    quarantined = 0
    batches = 0
    import_started = time.perf_counter()
    remaining = target_records - imported_total
    while remaining:
        requested = min(batch_size, remaining)
        batch = take_exact(drafts, requested)
        result = write_client.import_drafts(batch, source_instance_id)
        if (
            result["ingested"] != requested
            or result["duplicates"] != 0
            or result["quarantined"] != 0
        ):
            raise BenchmarkError(
                f"invalid import batch at {target_records}: requested={requested}, "
                f"ingested={result['ingested']}, duplicates={result['duplicates']}, "
                f"quarantined={result['quarantined']}"
            )
        imported += result["ingested"]
        duplicates += result["duplicates"]
        quarantined += result["quarantined"]
        batches += 1
        remaining -= requested
    import_seconds = time.perf_counter() - import_started
    expected_delta = target_records - imported_total
    if imported != expected_delta or duplicates != 0 or quarantined != 0:
        raise BenchmarkError(
            f"invalid import counts at {target_records}: "
            f"ingested={imported}, duplicates={duplicates}, quarantined={quarantined}"
        )
    corpus_total = read_client.corpus_total()
    if corpus_total != target_records:
        raise BenchmarkError(
            f"Corpus count mismatch at {target_records}: got {corpus_total}"
        )

    effective_cases = [case for case in query_cases if case.mode != "unfiltered"]
    unfiltered_cases = [case for case in query_cases if case.mode == "unfiltered"]
    if not effective_cases or not unfiltered_cases:
        raise BenchmarkError("benchmark workload must contain effective and unfiltered cases")
    cursor_case = max(
        effective_cases,
        key=lambda case: sum(
            (
                bool(case.request["filters"].get("from")),
                bool(case.request["filters"].get("to")),
                bool(case.request["filters"].get("types")),
                bool(case.request["filters"].get("channels")),
                bool(case.request["filters"].get("containers")),
            )
        ),
    )
    cursor_sanity = measure_cursor_sanity(read_client, cursor_case, search_limit)
    if search_requests % len(query_cases) != 0:
        raise BenchmarkError("search requests must be divisible by all query cases")
    requests_per_case = search_requests // len(query_cases)
    search = {
        "effective": measure_searches(
            client=read_client,
            cases=effective_cases,
            warmup_rounds=warmup_rounds,
            requests=requests_per_case * len(effective_cases),
            concurrency=search_concurrency,
            limit=search_limit,
        ),
        "unfiltered": measure_searches(
            client=read_client,
            cases=unfiltered_cases,
            warmup_rounds=warmup_rounds,
            requests=requests_per_case * len(unfiltered_cases),
            concurrency=search_concurrency,
            limit=search_limit,
        ),
    }
    metrics = docker_metrics.snapshot()
    oom_delta = metrics.oom_events - previous_metrics.oom_events
    oom_kill_delta = metrics.oom_kill_events - previous_metrics.oom_kill_events
    search_pass, headroom_pass = search_gate_results(search, cursor_sanity)
    memory_pass = metrics.vm_hwm_kib * 1024 <= PEAK_RSS_LIMIT_BYTES
    swap_pass = metrics.swap_current_bytes == 0 and metrics.swap_peak_bytes == 0
    oom_pass = (
        oom_delta == 0
        and oom_kill_delta == 0
        and not metrics.docker_oom_killed
        and metrics.restart_count == 0
    )
    container_terminated = (
        metrics.docker_oom_killed
        or metrics.restart_count != 0
        or oom_kill_delta != 0
    )
    return {
        "target_records": target_records,
        "dataset_prefix_sha256": dataset_prefix_sha256,
        "imported_this_stage": imported,
        "duplicates_this_stage": duplicates,
        "quarantined_this_stage": quarantined,
        "batches_this_stage": batches,
        "import_seconds": import_seconds,
        "corpus_records": corpus_total,
        "cursor_sanity": cursor_sanity,
        "search": search,
        "container_metrics": asdict(metrics),
        "oom_events_delta": oom_delta,
        "oom_kill_events_delta": oom_kill_delta,
        "slo_search_pass": search_pass,
        "development_headroom_search_pass": headroom_pass,
        "slo_memory_pass": memory_pass,
        "slo_swap_pass": swap_pass,
        "slo_oom_pass": oom_pass,
        "container_terminated": container_terminated,
        "passed": search_pass and memory_pass and swap_pass and oom_pass,
    }


def search_gate_results(
    search: dict[str, Any], cursor_sanity: dict[str, Any]
) -> tuple[bool, bool]:
    effective = search["effective"]
    search_pass = (
        effective["failures"] == 0
        and effective["warmup_failures"] == 0
        and effective["p95_seconds"] is not None
        and effective["p95_seconds"] <= SEARCH_P95_LIMIT_SECONDS
        and cursor_sanity["passed"]
    )
    headroom_pass = (
        search_pass
        and effective["p95_seconds"] is not None
        and effective["p95_seconds"] <= DEVELOPMENT_HEADROOM_P95_LIMIT_SECONDS
    )
    return search_pass, headroom_pass


def take_exact(iterator: Iterator[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for _ in range(count):
        try:
            result.append(next(iterator))
        except StopIteration as error:
            raise BenchmarkError("dataset ended before the declared record count") from error
    return result


class LetheHttpClient:
    def __init__(self, *, base_url: str, token: str, timeout_seconds: float) -> None:
        parsed = urllib.parse.urlsplit(base_url)
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.port is None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or parsed.username
            or parsed.password
        ):
            raise BenchmarkError(
                "benchmark base URL must be loopback HTTP with an explicit port"
            )
        if not token.strip():
            raise BenchmarkError("LETHE API token must not be blank")
        if timeout_seconds <= 0:
            raise BenchmarkError("HTTP timeout must be greater than zero")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds

    def import_drafts(
        self, drafts: list[dict[str, Any]], source_instance_id: str
    ) -> dict[str, int]:
        if not drafts:
            raise BenchmarkError("import batch must not be empty")
        response = self.request_json(
            "POST",
            "/api/import/observation-drafts",
            {
                "source_instance_id": source_instance_id,
                "drafts": drafts,
            },
        )
        return validate_import_response(response, len(drafts))

    def corpus_total(self) -> int:
        response = self.request_json(
            "GET", "/api/projections/proj:corpus/records", None
        )
        try:
            total = response["data"]["total"]
        except (KeyError, TypeError) as error:
            raise BenchmarkError("Corpus count response has an invalid shape") from error
        if not isinstance(total, int) or isinstance(total, bool) or total < 0:
            raise BenchmarkError("Corpus count is not a non-negative integer")
        return total

    def search(
        self, request_body: dict[str, Any], expected_min_matches: int
    ) -> tuple[float, dict[str, Any]]:
        encoded = encode_compact_json(request_body)
        started = time.perf_counter()
        response = self.request_json_bytes(
            "POST", "/api/projections/proj:corpus/grep", encoded
        )
        elapsed = time.perf_counter() - started
        validate_search_response(
            response,
            request=request_body,
            expected_limit=request_body["limit"],
            expected_min_matches=expected_min_matches,
        )
        return elapsed, response

    def request_json(
        self, method: str, path: str, body: dict[str, Any] | None
    ) -> dict[str, Any]:
        encoded = None if body is None else encode_compact_json(body)
        return self.request_json_bytes(method, path, encoded)

    def request_json_bytes(
        self, method: str, path: str, encoded: bytes | None
    ) -> dict[str, Any]:
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        if encoded is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=encoded,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                if response.status < 200 or response.status >= 300:
                    raise BenchmarkError(
                        f"LETHE returned HTTP {response.status} for {method} {path}"
                    )
                raw = response.read()
        except urllib.error.HTTPError as error:
            detail = error.read(512).decode("utf-8", errors="replace")
            raise LetheHttpError(
                error.code,
                f"LETHE returned HTTP {error.code} for {method} {path}: {detail}"
            ) from error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            raise LetheTransportError(
                f"LETHE request failed for {method} {path}: {error}"
            ) from error
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BenchmarkError("LETHE returned invalid JSON") from error
        if not isinstance(value, dict):
            raise BenchmarkError("LETHE returned a non-object JSON response")
        return value


def validate_import_response(value: dict[str, Any], batch_size: int) -> dict[str, int]:
    expected_keys = {"ingested", "duplicates", "quarantined"}
    if set(value) != expected_keys:
        raise BenchmarkError("import response has an unexpected shape")
    result: dict[str, int] = {}
    for key in expected_keys:
        item = value[key]
        if not isinstance(item, int) or isinstance(item, bool) or item < 0:
            raise BenchmarkError(f"import response {key} is invalid")
        result[key] = item
    if sum(result.values()) != batch_size:
        raise BenchmarkError("import response counts do not match the batch size")
    return result


def validate_search_response(
    value: dict[str, Any],
    *,
    request: dict[str, Any],
    expected_limit: int,
    expected_min_matches: int,
) -> dict[str, Any]:
    if set(value) != {"data", "projection_metadata"}:
        raise BenchmarkError("grep response envelope has an unexpected shape")
    data = value["data"]
    metadata = value["projection_metadata"]
    if not isinstance(data, dict) or set(data) != {
        "matches",
        "next_cursor",
        "complete",
        "projection_watermark",
    }:
        raise BenchmarkError("grep response data has an unexpected shape")
    if not isinstance(metadata, dict) or metadata.get("projection_id") != "proj:corpus":
        raise BenchmarkError("grep projection metadata is invalid")
    matches = data["matches"]
    if not isinstance(matches, list):
        raise BenchmarkError("grep matches must be an array")
    required_matches = min(expected_limit, expected_min_matches)
    if len(matches) < required_matches or len(matches) > expected_limit:
        raise BenchmarkError(
            f"grep returned {len(matches)} matches; expected {required_matches}..{expected_limit}"
        )
    if not isinstance(data["complete"], bool):
        raise BenchmarkError("grep complete must be a boolean")
    if data["next_cursor"] is not None and not isinstance(data["next_cursor"], str):
        raise BenchmarkError("grep next_cursor must be a string or null")
    if (
        not isinstance(data["projection_watermark"], str)
        or not data["projection_watermark"].strip()
    ):
        raise BenchmarkError("grep projection_watermark must not be blank")
    for item in matches:
        validate_search_match(item)
    validate_search_semantics(matches, request)
    return data


def validate_search_match(value: Any) -> None:
    required = {
        "record_id",
        "source_type",
        "anchor_url",
        "source_title",
        "source_location",
        "timestamp",
        "snippet",
        "matched_ranges",
        "metadata",
    }
    if not isinstance(value, dict) or not required.issubset(value):
        raise BenchmarkError("grep match has an unexpected shape")
    for key in ("record_id", "source_type", "anchor_url", "source_title", "timestamp"):
        if not isinstance(value[key], str) or not value[key].strip():
            raise BenchmarkError(f"grep match {key} must not be blank")
    if value["source_location"] is not None and not isinstance(
        value["source_location"], str
    ):
        raise BenchmarkError("grep match source_location must be a string or null")
    if not isinstance(value["metadata"], dict):
        raise BenchmarkError("grep match metadata must be an object")
    if not isinstance(value["snippet"], str) or not value["snippet"]:
        raise BenchmarkError("grep match snippet must not be empty")
    if len(value["snippet"]) > 240:
        raise BenchmarkError("grep match snippet exceeds 240 characters")
    ranges = value["matched_ranges"]
    if not isinstance(ranges, list) or not ranges or len(ranges) > 20:
        raise BenchmarkError("grep matched_ranges must contain 1..20 ranges")
    for matched in ranges:
        if not isinstance(matched, dict) or set(matched) != {"start", "end"}:
            raise BenchmarkError("grep matched range has an unexpected shape")
        start = matched["start"]
        end = matched["end"]
        if (
            not isinstance(start, int)
            or isinstance(start, bool)
            or not isinstance(end, int)
            or isinstance(end, bool)
            or start < 0
            or end <= start
        ):
            raise BenchmarkError("grep matched range offsets are invalid")


def validate_search_semantics(
    matches: list[dict[str, Any]], request: dict[str, Any]
) -> None:
    pattern = request.get("pattern")
    normalization = request.get("normalization")
    order = request.get("order")
    filters = request.get("filters")
    if (
        not isinstance(pattern, str)
        or not pattern.strip()
        or normalization not in {"nfkc", "none"}
        or order not in {"date_asc", "date_desc"}
        or not isinstance(filters, dict)
    ):
        raise BenchmarkError("benchmark grep request has an invalid shape")
    source_types = filters.get("types")
    if not isinstance(source_types, list) or any(
        not isinstance(source_type, str) or not source_type
        for source_type in source_types
    ):
        raise BenchmarkError("benchmark grep source type filter is invalid")
    for key in ("channels", "containers"):
        values = filters.get(key)
        if not isinstance(values, list) or any(
            not isinstance(value, str) or not value for value in values
        ):
            raise BenchmarkError(f"benchmark grep {key} filter is invalid")
    from_timestamp = optional_rfc3339(filters.get("from"), "filters.from")
    to_timestamp = optional_rfc3339(filters.get("to"), "filters.to")
    if (
        from_timestamp is not None
        and to_timestamp is not None
        and from_timestamp > to_timestamp
    ):
        raise BenchmarkError("benchmark grep timestamp range is inverted")

    terms = [term for term in re.split(r"[ \t\u3000]+", pattern) if term]
    ordering_keys: list[tuple[datetime, str]] = []
    for match in matches:
        timestamp = parse_rfc3339(match["timestamp"], "grep match timestamp")
        if source_types and match["source_type"] not in source_types:
            raise BenchmarkError("grep result violates the source type filter")
        if from_timestamp is not None and timestamp < from_timestamp:
            raise BenchmarkError("grep result precedes filters.from")
        if to_timestamp is not None and timestamp > to_timestamp:
            raise BenchmarkError("grep result follows filters.to")
        channel_filters = filters["channels"] + filters["containers"]
        if channel_filters and match["source_title"] not in channel_filters:
            raise BenchmarkError("grep result violates the channel/container filter")
        snippet = match["snippet"]
        searchable = (
            unicodedata.normalize("NFKC", snippet)
            if normalization == "nfkc"
            else snippet
        )
        normalized_terms = (
            [unicodedata.normalize("NFKC", term) for term in terms]
            if normalization == "nfkc"
            else terms
        )
        if any(term not in searchable for term in normalized_terms):
            raise BenchmarkError("grep snippet does not contain every query term")
        ordering_keys.append((timestamp, match["record_id"]))

    for left, right in zip(ordering_keys, ordering_keys[1:]):
        if order == "date_asc":
            ordered = left[0] < right[0] or (
                left[0] == right[0] and left[1] <= right[1]
            )
        else:
            ordered = left[0] > right[0] or (
                left[0] == right[0] and left[1] <= right[1]
            )
        if not ordered:
            raise BenchmarkError(f"grep results violate {order} ordering")


def optional_rfc3339(value: Any, label: str) -> datetime | None:
    if value is None:
        return None
    return parse_rfc3339(value, label)


def parse_rfc3339(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise BenchmarkError(f"{label} must be an RFC 3339 timestamp or null")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise BenchmarkError(f"{label} is not an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise BenchmarkError(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc)


def measure_cursor_sanity(
    client: LetheHttpClient, case: QueryCase, limit: int
) -> dict[str, Any]:
    try:
        first_seconds, first = client.search(case.request, case.expected_min_matches)
        first_data = first["data"]
        cursor = first_data["next_cursor"]
        if first_data["complete"] or not isinstance(cursor, str) or not cursor:
            raise BenchmarkError("cursor sanity first page did not return next_cursor")
        second_request = dict(case.request)
        second_request["cursor"] = cursor
        second_seconds, second = client.search(
            second_request, case.expected_min_matches
        )
        first_ids = {item["record_id"] for item in first_data["matches"]}
        second_ids = {item["record_id"] for item in second["data"]["matches"]}
        if len(first_ids) != limit or len(second_ids) != limit:
            raise BenchmarkError("cursor sanity pages do not contain the requested limit")
        if first_ids & second_ids:
            raise BenchmarkError("cursor sanity found duplicate records across pages")
        return {
            "passed": True,
            "requests": 2,
            "case_id": case.case_id,
            "first_page_seconds": first_seconds,
            "second_page_seconds": second_seconds,
            "first_page_records": len(first_ids),
            "second_page_records": len(second_ids),
        }
    except (BenchmarkError, OSError) as error:
        return {
            "passed": False,
            "requests": 2,
            "case_id": case.case_id,
            "error": str(error),
        }


def measure_searches(
    *,
    client: LetheHttpClient,
    cases: list[QueryCase],
    warmup_rounds: int,
    requests: int,
    concurrency: int,
    limit: int,
) -> dict[str, Any]:
    if requests % len(cases) != 0:
        raise BenchmarkError("search requests must be evenly divisible by query cases")
    warmup_failures: list[dict[str, str]] = []
    for _ in range(warmup_rounds):
        for case in cases:
            try:
                client.search(case.request, case.expected_min_matches)
            except (BenchmarkError, OSError) as error:
                warmup_failures.append({"case_id": case.case_id, "error": str(error)})

    scheduled = [cases[index % len(cases)] for index in range(requests)]

    def execute(case: QueryCase) -> dict[str, Any]:
        started = time.perf_counter()
        try:
            seconds, _ = client.search(case.request, case.expected_min_matches)
            return {"case_id": case.case_id, "ok": True, "seconds": seconds}
        except (BenchmarkError, OSError) as error:
            return {
                "case_id": case.case_id,
                "ok": False,
                "seconds": time.perf_counter() - started,
                "error": str(error),
            }

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        attempts = list(pool.map(execute, scheduled))
    result = summarize_search_attempts(
        attempts=attempts,
        warmup_errors=warmup_failures,
        concurrency=concurrency,
        limit=limit,
        warmup_requests=warmup_rounds * len(cases),
    )
    case_modes = {case.case_id: case.mode for case in cases}
    by_mode: dict[str, Any] = {}
    for mode in sorted({case.mode for case in cases}):
        mode_attempts = [
            attempt
            for attempt in attempts
            if case_modes[attempt["case_id"]] == mode
        ]
        mode_errors = [
            error for error in warmup_failures if case_modes[error["case_id"]] == mode
        ]
        by_mode[mode] = summarize_search_attempts(
            attempts=mode_attempts,
            warmup_errors=mode_errors,
            concurrency=concurrency,
            limit=limit,
            warmup_requests=warmup_rounds
            * sum(case.mode == mode for case in cases),
        )
    result["by_mode"] = by_mode
    return result


def summarize_search_attempts(
    *,
    attempts: list[dict[str, Any]],
    warmup_errors: list[dict[str, str]],
    concurrency: int,
    limit: int,
    warmup_requests: int,
) -> dict[str, Any]:
    successful = sorted(
        attempt["seconds"] for attempt in attempts if attempt["ok"]
    )
    return {
        "requests": len(attempts),
        "concurrency": concurrency,
        "limit": limit,
        "warmup_requests": warmup_requests,
        "warmup_failures": len(warmup_errors),
        "warmup_errors": warmup_errors,
        "successes": len(successful),
        "failures": len(attempts) - len(successful),
        "minimum_seconds": successful[0] if successful else None,
        "mean_seconds": statistics.fmean(successful) if successful else None,
        "p95_seconds": nearest_rank_percentile(successful, 0.95)
        if successful
        else None,
        "maximum_seconds": successful[-1] if successful else None,
        "attempts": attempts,
    }


def nearest_rank_percentile(values: list[float], percentile: float) -> float:
    if not values:
        raise BenchmarkError("percentile requires at least one value")
    if not 0 < percentile <= 1:
        raise BenchmarkError("percentile must be in (0, 1]")
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


class DockerProcessMetrics:
    def __init__(
        self,
        *,
        container: str,
        work_dir: Path,
        base_url: str,
        repository: dict[str, Any],
        runner: DockerRunner | None = None,
    ) -> None:
        if not container.strip():
            raise BenchmarkError("Docker container must not be blank")
        self.container = container
        self.work_dir = work_dir.resolve(strict=True)
        require_plain_database_directory(self.work_dir)
        self.base_url = base_url
        head = repository.get("head")
        tree_sha256 = repository.get("dirty_tree_sha256")
        if not isinstance(head, str) or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", head) is None:
            raise BenchmarkError("repository fingerprint HEAD is invalid")
        if not isinstance(tree_sha256, str) or not valid_sha256(tree_sha256):
            raise BenchmarkError("repository fingerprint tree SHA-256 is invalid")
        self.repository_head = head
        self.repository_tree_sha256 = tree_sha256
        self.runner = runner if runner is not None else run_docker
        self.container_id: str | None = None
        self.image_id: str | None = None

    def preflight(self) -> dict[str, Any]:
        document = self.inspect_document()
        target = self.validate_container_document(document, require_running=True)
        self.container_id = target["id"]
        self.image_id = target["image"]
        self.validate_image_provenance(self.image_id)
        self.assert_cgroup_limits()
        self.assert_single_workload_process()
        return target

    def validate_container_document(
        self, document: dict[str, Any], *, require_running: bool
    ) -> dict[str, Any]:
        state = require_dict(document, "State", "Docker State")
        host_config = require_dict(document, "HostConfig", "Docker HostConfig")
        config = require_dict(document, "Config", "Docker Config")
        container_id = document.get("Id")
        image_id = document.get("Image")
        if not isinstance(container_id, str) or not container_id.strip():
            raise BenchmarkError("Docker container ID is missing")
        if self.container_id is not None and container_id != self.container_id:
            raise BenchmarkError("Docker container identity changed during the benchmark")
        if not isinstance(image_id, str) or not image_id.strip():
            raise BenchmarkError("Docker image ID is missing")
        if self.image_id is not None and image_id != self.image_id:
            raise BenchmarkError("Docker image identity changed during the benchmark")
        labels = config.get("Labels")
        if not isinstance(labels, dict):
            raise BenchmarkError("Docker container labels are missing")
        if labels.get(PURPOSE_LABEL) != BENCHMARK_PURPOSE:
            raise BenchmarkError("Docker container purpose label is not the benchmark label")
        labelled_root = labels.get(ROOT_LABEL)
        if not isinstance(labelled_root, str) or not same_path(
            labelled_root, self.work_dir
        ):
            raise BenchmarkError("Docker benchmark root label does not match --work-dir")
        expected_labels = {
            COMPOSE_PROJECT_LABEL: COMPOSE_PROJECT,
            COMPOSE_SERVICE_LABEL: COMPOSE_SERVICE,
            STORAGE_LABEL: EXPECTED_STORAGE_LABEL,
            SOURCE_HEAD_LABEL: self.repository_head,
            SOURCE_TREE_SHA256_LABEL: self.repository_tree_sha256,
        }
        for name, expected in expected_labels.items():
            if labels.get(name) != expected:
                raise BenchmarkError(f"Docker container label {name} is invalid")
        if require_running and state.get("Status") != "running":
            raise BenchmarkError("Docker benchmark container is not running")
        if host_config.get("Memory") != FOUR_GIB:
            raise BenchmarkError("Docker memory limit must be exactly 4 GiB")
        if host_config.get("MemorySwap") != FOUR_GIB:
            raise BenchmarkError("Docker memory+swap limit must be exactly 4 GiB")
        if host_config.get("NanoCpus") != FOUR_CPUS_NANO:
            raise BenchmarkError("Docker CPU limit must be exactly 4 CPUs")
        if host_config.get("ReadonlyRootfs") is not True:
            raise BenchmarkError("Docker root filesystem must be read-only")
        if host_config.get("Privileged") is not False:
            raise BenchmarkError("Docker benchmark container must not be privileged")
        if host_config.get("CapAdd") not in (None, []):
            raise BenchmarkError("Docker benchmark container must not add capabilities")
        if host_config.get("CapDrop") != ["ALL"]:
            raise BenchmarkError("Docker benchmark container must drop all capabilities")
        if host_config.get("SecurityOpt") != ["no-new-privileges:true"]:
            raise BenchmarkError("Docker no-new-privileges setting is invalid")
        restart_policy = require_dict(
            host_config, "RestartPolicy", "Docker restart policy"
        )
        if restart_policy.get("Name") != "no":
            raise BenchmarkError("Docker restart policy must be no")
        tmpfs = require_dict(host_config, "Tmpfs", "Docker tmpfs")
        if set(tmpfs) != {TMPFS_DESTINATION}:
            raise BenchmarkError("Docker tmpfs must contain only /tmp")
        if document.get("Path") != "/usr/local/bin/lethe-selfhost":
            raise BenchmarkError("Docker workload executable is invalid")
        if document.get("Args") not in (None, []):
            raise BenchmarkError("Docker workload executable must not have arguments")
        if config.get("User") != "lethe":
            raise BenchmarkError("Docker workload user must be lethe")
        mounts = document.get("Mounts")
        if not isinstance(mounts, list):
            raise BenchmarkError("Docker mounts are missing")
        self.validate_mounts(mounts)
        self.validate_port_mapping(document)
        return {
            "id": container_id,
            "image": image_id,
            "name": document.get("Name"),
            "purpose": labels[PURPOSE_LABEL],
            "benchmark_root": str(self.work_dir),
            "data_mount": str(
                (self.work_dir / DATABASE_DIRECTORY_NAME).resolve(strict=True)
            ),
            "memory_bytes": host_config["Memory"],
            "memory_plus_swap_bytes": host_config["MemorySwap"],
            "nano_cpus": host_config["NanoCpus"],
            "source_head": self.repository_head,
            "source_tree_sha256": self.repository_tree_sha256,
        }

    def validate_mounts(self, mounts: list[Any]) -> None:
        by_destination: dict[str, dict[str, Any]] = {}
        for mount in mounts:
            if not isinstance(mount, dict):
                raise BenchmarkError("Docker mount entry is invalid")
            destination = mount.get("Destination")
            if not isinstance(destination, str) or destination in by_destination:
                raise BenchmarkError("Docker mount destinations are invalid or duplicated")
            by_destination[destination] = mount
        optional_tmpfs = by_destination.pop(TMPFS_DESTINATION, None)
        if optional_tmpfs is not None and optional_tmpfs.get("Type") != "tmpfs":
            raise BenchmarkError("Docker /tmp mount must be tmpfs")
        expected = {
            CONFIG_MOUNT_DESTINATION: (
                REPOSITORY_ROOT / "deploy" / "persistent-index-benchmark" / "config.toml",
                False,
            ),
            JWKS_MOUNT_DESTINATION: (
                REPOSITORY_ROOT / "deploy" / "persistent-index-benchmark" / "mcp-jwks.json",
                False,
            ),
            DATA_MOUNT_DESTINATION: (
                self.work_dir / DATABASE_DIRECTORY_NAME,
                True,
            ),
        }
        if set(by_destination) != set(expected):
            raise BenchmarkError("Docker bind mount set does not match the benchmark allowlist")
        for destination, (source, writable) in expected.items():
            mount = by_destination[destination]
            if (
                mount.get("Type") != "bind"
                or not same_path(mount.get("Source"), source)
                or mount.get("RW") is not writable
            ):
                raise BenchmarkError(f"Docker bind mount is invalid: {destination}")

    def validate_image_provenance(self, image_id: str) -> None:
        result = self.runner(["docker", "image", "inspect", image_id])
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise BenchmarkError("docker image inspect returned invalid JSON") from error
        if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
            raise BenchmarkError("docker image inspect returned an unexpected shape")
        config = require_dict(value[0], "Config", "Docker image Config")
        labels = config.get("Labels")
        if not isinstance(labels, dict):
            raise BenchmarkError("Docker image labels are missing")
        if labels.get(SOURCE_HEAD_LABEL) != self.repository_head or labels.get(
            SOURCE_TREE_SHA256_LABEL
        ) != self.repository_tree_sha256:
            raise BenchmarkError("Docker image source fingerprint does not match the repository")

    def validate_port_mapping(self, document: dict[str, Any]) -> None:
        parsed = urllib.parse.urlsplit(self.base_url)
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.port is None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise BenchmarkError("benchmark base URL must be loopback HTTP")
        network = require_dict(document, "NetworkSettings", "Docker NetworkSettings")
        ports = network.get("Ports")
        if not isinstance(ports, dict):
            raise BenchmarkError("Docker port mapping is missing")
        mappings = ports.get(HTTP_PORT_LABEL)
        if not isinstance(mappings, list) or not any(
            isinstance(mapping, dict)
            and mapping.get("HostIp") == "127.0.0.1"
            and mapping.get("HostPort") == str(parsed.port)
            for mapping in mappings
        ):
            raise BenchmarkError("base URL does not match the benchmark HTTP port")

    def assert_runtime_invariants(self) -> None:
        document = self.inspect_document()
        self.validate_container_document(document, require_running=True)
        self.assert_cgroup_limits()
        self.assert_single_workload_process()

    def assert_cgroup_limits(self) -> None:
        memory_max = parse_single_integer(
            self.exec_text("cat", "/sys/fs/cgroup/memory.max"), "memory.max"
        )
        swap_max = parse_single_integer(
            self.exec_text("cat", "/sys/fs/cgroup/memory.swap.max"),
            "memory.swap.max",
        )
        if memory_max != FOUR_GIB or swap_max != 0:
            raise BenchmarkError("cgroup must enforce 4 GiB memory and zero swap")
        cpu_max = self.exec_text("cat", "/sys/fs/cgroup/cpu.max").split()
        if (
            len(cpu_max) != 2
            or not cpu_max[0].isdigit()
            or not cpu_max[1].isdigit()
            or int(cpu_max[1]) <= 0
            or int(cpu_max[0]) != 4 * int(cpu_max[1])
        ):
            raise BenchmarkError("cgroup must enforce an exact 4 CPU quota")

    def assert_single_workload_process(self) -> None:
        result = self.runner(
            ["docker", "top", self.docker_reference(), "-eo", "pid,ppid,comm"]
        )
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        if not lines or lines[0].split() != ["PID", "PPID", "COMMAND"]:
            raise BenchmarkError("Docker process list has an invalid shape")
        if len(lines) != 2 or len(lines[1].split(maxsplit=2)) != 3:
            raise BenchmarkError("benchmark requires exactly one workload process")
        if lines[1].split(maxsplit=2)[2] != "lethe-selfhost":
            raise BenchmarkError("benchmark workload process must be lethe-selfhost")

    def snapshot(self) -> ContainerMetrics:
        document = self.inspect_document()
        self.validate_container_document(document, require_running=True)
        self.assert_cgroup_limits()
        self.assert_single_workload_process()
        state = require_dict(document, "State", "Docker State")
        status = self.exec_text("cat", "/proc/1/status")
        events = parse_memory_events(
            self.exec_text("cat", "/sys/fs/cgroup/memory.events")
        )
        return ContainerMetrics(
            vm_rss_kib=parse_status_kib(status, "VmRSS"),
            vm_hwm_kib=parse_status_kib(status, "VmHWM"),
            cgroup_memory_current_bytes=parse_single_integer(
                self.exec_text("cat", "/sys/fs/cgroup/memory.current"),
                "memory.current",
            ),
            cgroup_memory_peak_bytes=parse_single_integer(
                self.exec_text("cat", "/sys/fs/cgroup/memory.peak"),
                "memory.peak",
            ),
            swap_current_bytes=parse_single_integer(
                self.exec_text("cat", "/sys/fs/cgroup/memory.swap.current"),
                "memory.swap.current",
            ),
            swap_peak_bytes=parse_single_integer(
                self.exec_text("cat", "/sys/fs/cgroup/memory.swap.peak"),
                "memory.swap.peak",
            ),
            oom_events=events["oom"],
            oom_kill_events=events["oom_kill"],
            docker_oom_killed=require_bool(state, "OOMKilled", "Docker OOMKilled"),
            restart_count=require_non_negative_int(
                document, "RestartCount", "Docker RestartCount"
            ),
        )

    def failure_evidence(self) -> dict[str, Any]:
        try:
            document = self.inspect_document()
            state = require_dict(document, "State", "Docker State")
            return {
                "available": True,
                "id": document.get("Id"),
                "image": document.get("Image"),
                "status": state.get("Status"),
                "oom_killed": state.get("OOMKilled"),
                "exit_code": state.get("ExitCode"),
                "error": state.get("Error"),
                "restart_count": document.get("RestartCount"),
                "cgroup_metrics_available": state.get("Status") == "running",
            }
        except (BenchmarkError, OSError, ValueError) as error:
            return {
                "available": False,
                "evidence_error": str(error),
            }

    def inspect_document(self) -> dict[str, Any]:
        result = self.runner(["docker", "inspect", self.docker_reference()])
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise BenchmarkError("docker inspect returned invalid JSON") from error
        if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
            raise BenchmarkError("docker inspect returned an unexpected shape")
        return value[0]

    def exec_text(self, *command: str) -> str:
        result = self.runner(["docker", "exec", self.docker_reference(), *command])
        return result.stdout

    def docker_reference(self) -> str:
        return self.container_id if self.container_id is not None else self.container


def run_docker(command: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=30,
        )
    except FileNotFoundError as error:
        raise BenchmarkError("docker CLI is not installed") from error
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() if error.stderr else "no stderr"
        raise BenchmarkError(f"docker command failed: {detail}") from error
    except subprocess.TimeoutExpired as error:
        raise BenchmarkError("docker command timed out") from error


def require_clean_initial_metrics(metrics: ContainerMetrics) -> None:
    if (
        metrics.swap_current_bytes != 0
        or metrics.swap_peak_bytes != 0
        or metrics.oom_events != 0
        or metrics.oom_kill_events != 0
        or metrics.docker_oom_killed
        or metrics.restart_count != 0
    ):
        raise BenchmarkError(
            "benchmark requires fresh zero swap/OOM/restart counters"
        )


def parse_status_kib(raw: str, key: str) -> int:
    match = re.search(rf"(?m)^{re.escape(key)}:\s+([0-9]+)\s+kB$", raw)
    if match is None:
        raise BenchmarkError(f"/proc/1/status does not contain {key}")
    return int(match.group(1))


def parse_memory_events(raw: str) -> dict[str, int]:
    events: dict[str, int] = {}
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) != 2 or not parts[1].isdigit():
            raise BenchmarkError("cgroup memory.events has an invalid shape")
        events[parts[0]] = int(parts[1])
    if "oom" not in events or "oom_kill" not in events:
        raise BenchmarkError("cgroup memory.events lacks OOM counters")
    return events


def parse_single_integer(raw: str, name: str) -> int:
    value = raw.strip()
    if not value.isdigit():
        raise BenchmarkError(f"cgroup {name} is not a non-negative integer")
    return int(value)


def same_path(left: Any, right: Path) -> bool:
    if not isinstance(left, str) or not left.strip():
        return False
    try:
        left_path = Path(left).resolve(strict=True)
        right_path = Path(right).resolve(strict=True)
    except OSError:
        return False
    return os.path.normcase(str(left_path)) == os.path.normcase(str(right_path))


def require_dict(value: dict[str, Any], key: str, label: str) -> dict[str, Any]:
    item = value.get(key)
    if not isinstance(item, dict):
        raise BenchmarkError(f"{label} is missing or invalid")
    return item


def require_bool(value: dict[str, Any], key: str, label: str) -> bool:
    item = value.get(key)
    if not isinstance(item, bool):
        raise BenchmarkError(f"{label} is missing or invalid")
    return item


def require_non_negative_int(value: dict[str, Any], key: str, label: str) -> int:
    item = value.get(key)
    if not isinstance(item, int) or isinstance(item, bool) or item < 0:
        raise BenchmarkError(f"{label} is missing or invalid")
    return item


def required_environment(name: str) -> str:
    if not name.strip():
        raise BenchmarkError("environment variable name must not be blank")
    try:
        value = os.environ[name]
    except KeyError as error:
        raise BenchmarkError(f"required environment variable {name} is not set") from error
    if not value.strip():
        raise BenchmarkError(f"required environment variable {name} is blank")
    return value


def run_repository_fingerprint(args: argparse.Namespace) -> dict[str, Any]:
    return explicit_repository_fingerprint(args.source_head, args.source_tree_sha256)


def explicit_repository_fingerprint(
    source_head: str, source_tree_sha256: str
) -> dict[str, Any]:
    if re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", source_head) is None:
        raise BenchmarkError("--source-head is not a valid object id")
    if not valid_sha256(source_tree_sha256):
        raise BenchmarkError("--source-tree-sha256 is not a valid SHA-256")
    return {
        "root": str(REPOSITORY_ROOT.resolve()),
        "head": source_head,
        "dirty": True,
        "dirty_file_count": None,
        "dirty_tree_sha256": source_tree_sha256,
        "dirty_tree_hash_format": (
            "sha256(sorted(len64be(path)+path+len64be(bytes)+bytes))"
        ),
        "dirty_paths": None,
        "source": "explicit_cli_fingerprint",
    }


def repository_fingerprint(
    repository_root: Path = REPOSITORY_ROOT,
    git_runner: GitRunner | None = None,
) -> dict[str, Any]:
    root = repository_root.resolve(strict=True)
    runner = git_runner if git_runner is not None else run_git_bytes
    head_raw = runner(["rev-parse", "HEAD"], root).strip()
    if re.fullmatch(rb"[0-9a-f]{40}|[0-9a-f]{64}", head_raw) is None:
        raise BenchmarkError("git rev-parse HEAD returned an invalid object id")

    runner(["status", "--short", "--untracked-files=all"], root)
    staged_raw = runner(["diff", "--cached", "--name-only", "-z"], root)
    if staged_raw:
        raise BenchmarkError(
            "benchmark fingerprint refuses staged changes; unstage them first"
        )
    deleted_raw = runner(["ls-files", "--deleted", "-z"], root)
    if deleted_raw:
        raise BenchmarkError(
            "benchmark fingerprint refuses deleted tracked files"
        )
    dirty_raw = runner(
        ["ls-files", "--modified", "--others", "--exclude-standard", "-z"],
        root,
    )
    if dirty_raw and not dirty_raw.endswith(b"\0"):
        raise BenchmarkError("git ls-files did not return NUL-terminated paths")
    dirty_paths_raw = [] if not dirty_raw else dirty_raw[:-1].split(b"\0")
    dirty_paths_raw = [
        path for path in dirty_paths_raw if not is_generated_python_bytecode(path)
    ]
    if any(not path for path in dirty_paths_raw):
        raise BenchmarkError("git ls-files returned an empty path")
    if len(set(dirty_paths_raw)) != len(dirty_paths_raw):
        raise BenchmarkError("git ls-files returned duplicate paths")
    dirty_paths_raw.sort()
    status_dirty = bool(dirty_paths_raw)

    digest = hashlib.sha256()
    dirty_paths: list[str] = []
    for relative_raw in dirty_paths_raw:
        relative_text = os.fsdecode(relative_raw)
        relative_path = Path(relative_text)
        if relative_path.is_absolute():
            raise BenchmarkError("git ls-files returned an absolute path")
        unresolved_path = root / relative_path
        if unresolved_path.is_symlink():
            raise BenchmarkError(
                f"dirty repository entry is a symbolic link: {relative_text}"
            )
        path = unresolved_path.resolve(strict=True)
        if not path_is_within(path, root) or not path.is_file():
            raise BenchmarkError(
                f"dirty repository entry is not a regular in-tree file: {relative_text}"
            )
        content = path.read_bytes()
        digest.update(len(relative_raw).to_bytes(8, "big"))
        digest.update(relative_raw)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
        dirty_paths.append(relative_path.as_posix())

    return {
        "root": str(root),
        "head": head_raw.decode("ascii"),
        "dirty": status_dirty,
        "dirty_file_count": len(dirty_paths),
        "dirty_tree_sha256": digest.hexdigest(),
        "dirty_tree_hash_format": (
            "sha256(sorted(len64be(path)+path+len64be(bytes)+bytes))"
        ),
        "dirty_paths": dirty_paths,
    }


def is_generated_python_bytecode(relative_raw: bytes) -> bool:
    """Exclude interpreter-generated bytecode from the source fingerprint."""
    relative_path = Path(os.fsdecode(relative_raw))
    return relative_path.suffix == ".pyc" and "__pycache__" in relative_path.parts


def run_git_bytes(arguments: list[str], repository_root: Path) -> bytes:
    command = ["git", *arguments]
    try:
        result = subprocess.run(
            command,
            cwd=repository_root,
            check=True,
            capture_output=True,
            timeout=30,
        )
    except FileNotFoundError as error:
        raise BenchmarkError("git CLI is not installed") from error
    except subprocess.CalledProcessError as error:
        detail = error.stderr.decode("utf-8", errors="replace").strip()
        raise BenchmarkError(
            f"git command failed ({' '.join(arguments)}): {detail or 'no stderr'}"
        ) from error
    except subprocess.TimeoutExpired as error:
        raise BenchmarkError(
            f"git command timed out: {' '.join(arguments)}"
        ) from error
    return result.stdout


def host_fingerprint() -> dict[str, Any]:
    uname = platform.uname()
    return {
        "system": uname.system,
        "release": uname.release,
        "version": uname.version,
        "machine": uname.machine,
        "processor": uname.processor,
        "logical_cpus": os.cpu_count(),
        "python": sys.version,
    }


def iter_jsonl_objects(path: Path) -> Iterator[dict[str, Any]]:
    if not path.is_file():
        raise BenchmarkError(f"JSONL file does not exist: {path}")
    with path.open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                raise BenchmarkError(f"blank JSONL line at {line_number}")
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise BenchmarkError(f"invalid JSONL at line {line_number}") from error
            if not isinstance(value, dict):
                raise BenchmarkError(f"JSONL line {line_number} is not an object")
            yield value


def hash_jsonl_with_prefixes(
    path: Path, prefix_sizes: tuple[int, ...]
) -> tuple[str, int, int, dict[str, str]]:
    if not path.is_file():
        raise BenchmarkError(f"prepared Draft JSONL does not exist: {path}")
    if (
        not prefix_sizes
        or any(size <= 0 for size in prefix_sizes)
        or any(left >= right for left, right in zip(prefix_sizes, prefix_sizes[1:]))
    ):
        raise BenchmarkError("JSONL prefix sizes must be positive and increasing")
    digest = hashlib.sha256()
    total_bytes = 0
    records = 0
    prefix_sha256: dict[str, str] = {}
    boundaries = set(prefix_sizes)
    with path.open("rb") as source:
        for line in source:
            if not line.strip():
                raise BenchmarkError(f"prepared Draft JSONL has a blank line at {records + 1}")
            digest.update(line)
            total_bytes += len(line)
            records += 1
            if records in boundaries:
                prefix_sha256[str(records)] = digest.hexdigest()
    if set(prefix_sha256) != {str(size) for size in prefix_sizes}:
        raise BenchmarkError("prepared Draft JSONL ended before a prefix boundary")
    return digest.hexdigest(), total_bytes, records, prefix_sha256


def read_json_object(path: Path, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise BenchmarkError(f"{label} does not exist: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BenchmarkError(f"{label} is invalid JSON") from error
    if not isinstance(value, dict):
        raise BenchmarkError(f"{label} must be a JSON object")
    return value


def valid_sha256(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def encode_json_line(value: dict[str, Any]) -> bytes:
    return encode_compact_json(value) + b"\n"


def encode_compact_json(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def encode_json_document(value: dict[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    atomic_write_bytes(path, encode_json_document(value))


def atomic_write_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("wb") as destination:
        destination.write(value)
        destination.flush()
        os.fsync(destination.fileno())
    temporary.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
