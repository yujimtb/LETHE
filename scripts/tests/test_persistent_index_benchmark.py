from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import persistent_index_benchmark as benchmark
from scripts import personal_lake_pipeline_smoke as personal_smoke


class PersistentIndexBenchmarkTests(unittest.TestCase):
    def test_search_gate_uses_effective_queries_and_keeps_full_scan_as_reference(
        self,
    ) -> None:
        cursor = {"passed": True}
        base = {"failures": 0, "warmup_failures": 0}
        unfiltered = {**base, "p95_seconds": 10.0}

        self.assertEqual(
            benchmark.search_gate_results(
                {
                    "effective": {
                        **base,
                        "p95_seconds": benchmark.DEVELOPMENT_HEADROOM_P95_LIMIT_SECONDS,
                    },
                    "unfiltered": unfiltered,
                },
                cursor,
            ),
            (True, True),
        )
        self.assertEqual(
            benchmark.search_gate_results(
                {
                    "effective": {**base, "p95_seconds": 0.5},
                    "unfiltered": {
                        **unfiltered,
                        "warmup_failures": 1,
                    },
                },
                cursor,
            ),
            (True, True),
        )

        self.assertEqual(
            benchmark.search_gate_results(
                {
                    "effective": {**base, "p95_seconds": 1.5},
                    "unfiltered": unfiltered,
                },
                cursor,
            ),
            (True, False),
        )
        self.assertEqual(
            benchmark.search_gate_results(
                {
                    "effective": {**base, "p95_seconds": 2.1},
                    "unfiltered": unfiltered,
                },
                cursor,
            ),
            (False, False),
        )
        self.assertEqual(
            benchmark.search_gate_results(
                {
                    "effective": {**base, "p95_seconds": 0.5},
                    "unfiltered": {
                        **unfiltered,
                        "failures": 1,
                    },
                },
                cursor,
            ),
            (True, True),
        )

    def test_measure_searches_reports_each_workload_mode(self) -> None:
        class FakeClient:
            def search(
                self, request: dict[str, object], expected_min_matches: int
            ) -> tuple[float, dict[str, object]]:
                del request, expected_min_matches
                return 0.001, {}

        cases = [
            benchmark.QueryCase(
                "date", "effective-date-range", {"limit": 20}, 20
            ),
            benchmark.QueryCase("full", "unfiltered", {"limit": 20}, 20),
        ]
        measured = benchmark.measure_searches(
            client=FakeClient(),
            cases=cases,
            warmup_rounds=1,
            requests=4,
            concurrency=1,
            limit=20,
        )

        self.assertEqual(measured["requests"], 4)
        self.assertEqual(
            measured["by_mode"]["effective-date-range"]["requests"], 2
        )
        self.assertEqual(measured["by_mode"]["unfiltered"]["requests"], 2)
        self.assertEqual(
            measured["by_mode"]["effective-date-range"]["warmup_requests"], 1
        )
        self.assertEqual(measured["by_mode"]["unfiltered"]["warmup_requests"], 1)

    def test_external_work_dir_rejects_relative_repository_and_non_empty_paths(self) -> None:
        with self.assertRaisesRegex(benchmark.BenchmarkError, "absolute"):
            benchmark.prepare_empty_external_work_dir(Path("relative-benchmark"))

        repository_child = benchmark.REPOSITORY_ROOT / "benchmark-must-not-be-created"
        with self.assertRaisesRegex(benchmark.BenchmarkError, "outside the repository"):
            benchmark.prepare_empty_external_work_dir(repository_child)
        self.assertFalse(repository_child.exists())

        with tempfile.TemporaryDirectory() as temporary:
            non_empty = Path(temporary) / "non-empty"
            non_empty.mkdir()
            (non_empty / "sentinel").write_text("keep", encoding="utf-8")
            with self.assertRaisesRegex(benchmark.BenchmarkError, "must be empty"):
                benchmark.prepare_empty_external_work_dir(non_empty)

    def test_external_work_dir_rejects_symbolic_link_alias(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary).resolve()
            target = parent / "target"
            target.mkdir()
            alias = parent / "alias"
            try:
                alias.symlink_to(target, target_is_directory=True)
            except OSError as error:
                self.skipTest(f"symbolic links are unavailable: {error}")
            with self.assertRaisesRegex(
                benchmark.BenchmarkError, "symbolic link or reparse point"
            ):
                benchmark.require_external_work_dir(alias)

    def test_small_dataset_generation_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as first_parent, tempfile.TemporaryDirectory() as second_parent:
            first = Path(first_parent) / "benchmark"
            second = Path(second_parent) / "benchmark"
            first.mkdir()
            second.mkdir()
            first_manifest = benchmark.prepare_dataset(
                work_dir=first,
                record_count=80,
                seed=benchmark.BENCHMARK_SEED,
                body_bytes=128,
                sizes=(10, 40, 80),
            )
            second_manifest = benchmark.prepare_dataset(
                work_dir=second,
                record_count=80,
                seed=benchmark.BENCHMARK_SEED,
                body_bytes=128,
                sizes=(10, 40, 80),
            )

            self.assertEqual(first_manifest, second_manifest)
            digest, byte_count, records, prefix_sha256 = (
                benchmark.hash_jsonl_with_prefixes(
                    first / benchmark.DATASET_FILE, (10, 40, 80)
                )
            )
            self.assertEqual(digest, first_manifest["drafts_sha256"])
            self.assertEqual(byte_count, first_manifest["drafts_bytes"])
            self.assertEqual(records, 80)
            self.assertEqual(prefix_sha256, first_manifest["prefix_sha256"])
            lines = (first / benchmark.DATASET_FILE).read_bytes().splitlines(
                keepends=True
            )
            manually_hashed_prefixes = {
                str(size): hashlib.sha256(b"".join(lines[:size])).hexdigest()
                for size in (10, 40, 80)
            }
            self.assertEqual(prefix_sha256, manually_hashed_prefixes)
            self.assertEqual(prefix_sha256["80"], digest)
            self.assertTrue((first / benchmark.DATABASE_DIRECTORY_NAME).is_dir())

            first_draft = next(
                benchmark.iter_jsonl_objects(first / benchmark.DATASET_FILE)
            )
            self.assertEqual(first_draft["schema"], "schema:slack-message")
            self.assertEqual(first_draft["source_system"], "sys:slack")
            self.assertEqual(first_draft["payload"]["channel_name"], "100_benchmark")
            self.assertEqual(first_draft["payload"]["user_id"], "U00000000")
            self.assertEqual(
                first_draft["meta"]["communication_channel_kind"], "slack"
            )
            self.assertNotIn("reply_due_at", first_draft["meta"])
            self.assertIn(
                benchmark.CANONICAL_JSON_META_KEY,
                first_draft["meta"],
            )
            self.assertGreaterEqual(
                len(first_draft["payload"]["text"].encode("utf-8")), 128
            )
            query_cases = benchmark.load_query_cases(
                (first / benchmark.QUERY_FILE).read_bytes()
            )
            self.assertEqual(len(query_cases), 15)
            self.assertEqual(query_cases[-5].case_id, "all-records-literal")
            self.assertEqual(query_cases[-5].mode, "unfiltered")
            self.assertEqual(query_cases[-5].request["pattern"], "persistent")
            self.assertEqual(
                [case.request["pattern"] for case in query_cases[8:10]],
                ["申請 手順", "連絡　対応"],
            )
            self.assertTrue(all(case.mode != "unfiltered" for case in query_cases[:10]))
            self.assertTrue(all(case.mode == "unfiltered" for case in query_cases[10:]))
            self.assertEqual(
                query_cases[0].request["filters"]["from"],
                "2025-01-01T00:00:00Z",
            )
            self.assertEqual(
                query_cases[1].request["filters"]["channels"], ["101_benchmark"]
            )

    def test_effective_queries_have_matches_in_the_smallest_stage(self) -> None:
        drafts = [
            benchmark.synthetic_draft(index, benchmark.BENCHMARK_SEED, 128)
            for index in range(10_000)
        ]
        effective_cases = [
            case
            for case in benchmark.load_query_cases(
                benchmark.encode_json_document(benchmark.query_manifest_document())
            )
            if case.mode != "unfiltered"
        ]

        for case in effective_cases:
            filters = case.request["filters"]
            terms = case.request["pattern"].replace("　", " ").split()
            matches = 0
            for draft in drafts:
                if filters["from"] is not None and not draft["published"].startswith(
                    "2025-"
                ):
                    continue
                if (
                    filters["channels"]
                    and draft["payload"]["channel_name"] not in filters["channels"]
                ):
                    continue
                if all(term in draft["payload"]["text"] for term in terms):
                    matches += 1
            self.assertGreaterEqual(matches, 20, case.case_id)

    def test_prepare_requires_the_fixed_seed_and_body_size(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parser = benchmark.build_parser()
            work_dir = str((Path(temporary) / "benchmark").resolve())
            bad_seed = parser.parse_args(
                [
                    "prepare",
                    "--work-dir",
                    work_dir,
                    "--records",
                    str(benchmark.EXPECTED_RECORDS),
                    "--seed",
                    "1",
                    "--body-bytes",
                    str(benchmark.BENCHMARK_BODY_BYTES),
                ]
            )
            with self.assertRaisesRegex(benchmark.BenchmarkError, "--seed"):
                benchmark.prepare_command(bad_seed)

            bad_body_size = parser.parse_args(
                [
                    "prepare",
                    "--work-dir",
                    work_dir,
                    "--records",
                    str(benchmark.EXPECTED_RECORDS),
                    "--seed",
                    str(benchmark.BENCHMARK_SEED),
                    "--body-bytes",
                    "1",
                ]
            )
            with self.assertRaisesRegex(benchmark.BenchmarkError, "--body-bytes"):
                benchmark.prepare_command(bad_body_size)
            self.assertFalse(Path(work_dir).exists())

    def test_sizes_and_nearest_rank_are_strict(self) -> None:
        self.assertEqual(
            benchmark.parse_sizes("10000,50000,100000,500000"),
            benchmark.EXPECTED_SIZES,
        )
        for invalid in (
            "10,50,100,500",
            "10000,50000,100000",
            "10000, 50000,100000,500000",
            "",
        ):
            with self.subTest(invalid=invalid), self.assertRaises(
                benchmark.BenchmarkError
            ):
                benchmark.parse_sizes(invalid)
        self.assertEqual(
            benchmark.nearest_rank_percentile([0.4, 0.1, 0.3, 0.2], 0.95),
            0.4,
        )

    def test_search_response_shape_checks_snippet_ranges_and_envelope(self) -> None:
        response = search_response()
        data = benchmark.validate_search_response(
            response,
            request=search_request(),
            expected_limit=20,
            expected_min_matches=20,
        )
        self.assertEqual(len(data["matches"]), 20)

        response["data"]["matches"][0]["matched_ranges"] = []
        with self.assertRaisesRegex(benchmark.BenchmarkError, "matched_ranges"):
            benchmark.validate_search_response(
                response,
                request=search_request(),
                expected_limit=20,
                expected_min_matches=20,
            )

        response = search_response()
        response["data"]["matches"][0]["source_type"] = "github-issue"
        with self.assertRaisesRegex(benchmark.BenchmarkError, "source type filter"):
            benchmark.validate_search_response(
                response,
                request=search_request(),
                expected_limit=20,
                expected_min_matches=20,
            )

    def test_docker_preflight_and_snapshot_validate_dedicated_bind_limits(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            work_dir = Path(temporary).resolve()
            (work_dir / benchmark.DATABASE_DIRECTORY_NAME).mkdir()
            runner = FakeDockerRunner(work_dir)
            metrics = benchmark.DockerProcessMetrics(
                container="lethe-benchmark",
                work_dir=work_dir,
                base_url="http://127.0.0.1:18080",
                repository=repository_fixture(),
                runner=runner,
            )

            target = metrics.preflight()
            snapshot = metrics.snapshot()

            self.assertEqual(target["memory_bytes"], benchmark.FOUR_GIB)
            self.assertEqual(target["nano_cpus"], benchmark.FOUR_CPUS_NANO)
            self.assertEqual(snapshot.vm_hwm_kib, 120_000)
            self.assertEqual(snapshot.swap_peak_bytes, 0)
            self.assertEqual(snapshot.oom_kill_events, 0)

    def test_docker_preflight_rejects_wrong_root_and_named_volume(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            work_dir = Path(temporary).resolve()
            (work_dir / benchmark.DATABASE_DIRECTORY_NAME).mkdir()
            wrong_root = FakeDockerRunner(work_dir)
            wrong_root.document["Config"]["Labels"][benchmark.ROOT_LABEL] = str(
                work_dir / "other"
            )
            with self.assertRaisesRegex(benchmark.BenchmarkError, "root label"):
                benchmark.DockerProcessMetrics(
                    container="lethe-benchmark",
                    work_dir=work_dir,
                    base_url="http://127.0.0.1:18080",
                    repository=repository_fixture(),
                    runner=wrong_root,
                ).preflight()

            extra_bind = FakeDockerRunner(work_dir)
            extra_bind.document["Mounts"].append(
                {
                    "Type": "bind",
                    "Source": str(work_dir),
                    "Destination": "/forbidden",
                    "RW": False,
                }
            )
            with self.assertRaisesRegex(benchmark.BenchmarkError, "allowlist"):
                benchmark.DockerProcessMetrics(
                    container="lethe-benchmark",
                    work_dir=work_dir,
                    base_url="http://127.0.0.1:18080",
                    repository=repository_fixture(),
                    runner=extra_bind,
                ).preflight()

    def test_docker_snapshot_pins_container_identity_and_records_oom_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            work_dir = Path(temporary).resolve()
            (work_dir / benchmark.DATABASE_DIRECTORY_NAME).mkdir()
            runner = FakeDockerRunner(work_dir)
            metrics = benchmark.DockerProcessMetrics(
                container="lethe-benchmark",
                work_dir=work_dir,
                base_url="http://127.0.0.1:18080",
                repository=repository_fixture(),
                runner=runner,
            )
            metrics.preflight()
            runner.document["Id"] = "replacement-id"
            with self.assertRaisesRegex(benchmark.BenchmarkError, "identity changed"):
                metrics.snapshot()

            runner.document["Id"] = "container-id"
            runner.document["State"].update(
                {"Status": "exited", "OOMKilled": True, "ExitCode": 137}
            )
            with self.assertRaisesRegex(benchmark.BenchmarkError, "not running"):
                metrics.snapshot()
            evidence = metrics.failure_evidence()
            self.assertTrue(evidence["available"])
            self.assertTrue(evidence["oom_killed"])
            self.assertEqual(evidence["exit_code"], 137)
            self.assertFalse(evidence["cgroup_metrics_available"])

    def test_docker_preflight_rejects_image_from_another_source_tree(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            work_dir = Path(temporary).resolve()
            (work_dir / benchmark.DATABASE_DIRECTORY_NAME).mkdir()
            runner = FakeDockerRunner(work_dir)
            runner.image_labels[benchmark.SOURCE_TREE_SHA256_LABEL] = "c" * 64
            with self.assertRaisesRegex(benchmark.BenchmarkError, "source fingerprint"):
                benchmark.DockerProcessMetrics(
                    container="lethe-benchmark",
                    work_dir=work_dir,
                    base_url="http://127.0.0.1:18080",
                    repository=repository_fixture(),
                    runner=runner,
                ).preflight()

    def test_ready_wait_retries_only_transport_and_503(self) -> None:
        class Client:
            def __init__(self) -> None:
                self.calls = 0

            def corpus_total(self) -> int:
                self.calls += 1
                if self.calls == 1:
                    raise benchmark.LetheTransportError("not listening")
                if self.calls == 2:
                    raise benchmark.LetheHttpError(503, "rebuilding")
                return 0

        class Metrics:
            def __init__(self) -> None:
                self.checks = 0

            def assert_runtime_invariants(self) -> None:
                self.checks += 1

        client = Client()
        metrics = Metrics()
        with mock.patch.object(benchmark.time, "sleep"):
            result = benchmark.wait_for_ready_empty_corpus(client, metrics)
        self.assertEqual(result["ready_attempts"], 3)
        self.assertEqual(metrics.checks, 2)

        class UnauthorizedClient:
            def corpus_total(self) -> int:
                raise benchmark.LetheHttpError(401, "unauthorized")

        with self.assertRaisesRegex(benchmark.LetheHttpError, "unauthorized"):
            benchmark.wait_for_ready_empty_corpus(UnauthorizedClient(), metrics)

    def test_atomic_json_replaces_complete_document(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "report.json"
            benchmark.atomic_write_json(output, {"stage": 1})
            benchmark.atomic_write_json(output, {"stage": 2})
            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8")), {"stage": 2}
            )
            self.assertFalse(output.with_suffix(".json.tmp").exists())

    def test_repository_fingerprint_hashes_sorted_dirty_file_paths_and_bytes(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            (root / "z.txt").write_bytes(b"zulu")
            (root / "a.bin").write_bytes(b"\x00alpha")
            head = b"a" * 40

            def git_runner(arguments: list[str], repository_root: Path) -> bytes:
                self.assertEqual(repository_root, root)
                if arguments == ["rev-parse", "HEAD"]:
                    return head + b"\n"
                if arguments == ["status", "--short", "--untracked-files=all"]:
                    return b"?? a.bin\n M z.txt\n"
                if arguments in (
                    ["diff", "--cached", "--name-only", "-z"],
                    ["ls-files", "--deleted", "-z"],
                ):
                    return b""
                if arguments == [
                    "ls-files",
                    "--modified",
                    "--others",
                    "--exclude-standard",
                    "-z",
                ]:
                    return b"z.txt\0a.bin\0"
                raise AssertionError(f"unexpected git command: {arguments!r}")

            fingerprint = benchmark.repository_fingerprint(root, git_runner)
            expected = hashlib.sha256()
            for path, content in ((b"a.bin", b"\x00alpha"), (b"z.txt", b"zulu")):
                expected.update(len(path).to_bytes(8, "big"))
                expected.update(path)
                expected.update(len(content).to_bytes(8, "big"))
                expected.update(content)

            self.assertEqual(fingerprint["head"], "a" * 40)
            self.assertTrue(fingerprint["dirty"])
            self.assertEqual(fingerprint["dirty_file_count"], 2)
            self.assertEqual(fingerprint["dirty_paths"], ["a.bin", "z.txt"])
            self.assertEqual(
                fingerprint["dirty_tree_sha256"], expected.hexdigest()
            )

            def failed_git(arguments: list[str], repository_root: Path) -> bytes:
                raise benchmark.BenchmarkError("git command failed: fixture")

            with self.assertRaisesRegex(benchmark.BenchmarkError, "git command failed"):
                benchmark.repository_fingerprint(root, failed_git)

            def staged_git(arguments: list[str], repository_root: Path) -> bytes:
                if arguments == ["rev-parse", "HEAD"]:
                    return head + b"\n"
                if arguments == ["status", "--short", "--untracked-files=all"]:
                    return b"M  staged.txt\n"
                if arguments == ["diff", "--cached", "--name-only", "-z"]:
                    return b"staged.txt\0"
                raise AssertionError(f"unexpected git command: {arguments!r}")

            with self.assertRaisesRegex(benchmark.BenchmarkError, "staged changes"):
                benchmark.repository_fingerprint(root, staged_git)

    def test_dedicated_compose_and_smoke_config_have_required_index_settings(self) -> None:
        compose = (
            benchmark.REPOSITORY_ROOT
            / "deploy"
            / "persistent-index-benchmark"
            / "compose.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn(benchmark.BENCHMARK_PURPOSE, compose)
        self.assertIn("mem_limit: 4G", compose)
        self.assertIn("memswap_limit: 4G", compose)
        self.assertIn("cpus: 4.0", compose)
        self.assertIn("type: bind", compose)
        self.assertNotIn("personal-lake", compose)

        benchmark_config = (
            benchmark.REPOSITORY_ROOT
            / "deploy"
            / "persistent-index-benchmark"
            / "config.toml"
        ).read_text(encoding="utf-8")
        self.assertIn("writer_heap_bytes = 33554432", benchmark_config)
        self.assertIn("rebuild_page_size = 4096", benchmark_config)
        self.assertIn('mode = "workspace_filtered"', benchmark_config)
        self.assertIn('id = "chan:persistent-index-benchmark:C01BENCH"', benchmark_config)
        self.assertIn('source_instance_id = "persistent-index-benchmark"', benchmark_config)
        self.assertIn('external_id = "C01BENCH"', benchmark_config)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "config.toml"
            personal_smoke.write_config(
                config_path,
                root / "lethe.sqlite3",
                root / "blobs",
                18080,
                18090,
            )
            config = config_path.read_text(encoding="utf-8")
            self.assertIn(f'index_dir = "{(root / "corpus-index").as_posix()}"', config)
            self.assertIn("writer_heap_bytes = 33554432", config)
            self.assertIn("rebuild_page_size = 512", config)


class FakeDockerRunner:
    def __init__(self, work_dir: Path) -> None:
        self.image_labels = {
            benchmark.SOURCE_HEAD_LABEL: repository_fixture()["head"],
            benchmark.SOURCE_TREE_SHA256_LABEL: repository_fixture()[
                "dirty_tree_sha256"
            ],
        }
        self.document = {
            "Id": "container-id",
            "Image": "image-id",
            "Name": "/lethe-benchmark",
            "Path": "/usr/local/bin/lethe-selfhost",
            "Args": [],
            "State": {
                "Status": "running",
                "OOMKilled": False,
                "ExitCode": 0,
                "Error": "",
            },
            "RestartCount": 0,
            "HostConfig": {
                "Memory": benchmark.FOUR_GIB,
                "MemorySwap": benchmark.FOUR_GIB,
                "NanoCpus": benchmark.FOUR_CPUS_NANO,
                "ReadonlyRootfs": True,
                "Privileged": False,
                "CapAdd": None,
                "CapDrop": ["ALL"],
                "SecurityOpt": ["no-new-privileges:true"],
                "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
                "Tmpfs": {benchmark.TMPFS_DESTINATION: ""},
            },
            "Config": {
                "User": "lethe",
                "Labels": {
                    benchmark.PURPOSE_LABEL: benchmark.BENCHMARK_PURPOSE,
                    benchmark.ROOT_LABEL: str(work_dir),
                    benchmark.STORAGE_LABEL: benchmark.EXPECTED_STORAGE_LABEL,
                    benchmark.COMPOSE_PROJECT_LABEL: benchmark.COMPOSE_PROJECT,
                    benchmark.COMPOSE_SERVICE_LABEL: benchmark.COMPOSE_SERVICE,
                    benchmark.SOURCE_HEAD_LABEL: repository_fixture()["head"],
                    benchmark.SOURCE_TREE_SHA256_LABEL: repository_fixture()[
                        "dirty_tree_sha256"
                    ],
                }
            },
            "Mounts": [
                {
                    "Type": "bind",
                    "Source": str(
                        benchmark.REPOSITORY_ROOT
                        / "deploy"
                        / "persistent-index-benchmark"
                        / "config.toml"
                    ),
                    "Destination": benchmark.CONFIG_MOUNT_DESTINATION,
                    "RW": False,
                },
                {
                    "Type": "bind",
                    "Source": str(
                        benchmark.REPOSITORY_ROOT
                        / "deploy"
                        / "persistent-index-benchmark"
                        / "mcp-jwks.json"
                    ),
                    "Destination": benchmark.JWKS_MOUNT_DESTINATION,
                    "RW": False,
                },
                {
                    "Type": "bind",
                    "Source": str(work_dir / benchmark.DATABASE_DIRECTORY_NAME),
                    "Destination": benchmark.DATA_MOUNT_DESTINATION,
                    "RW": True,
                }
            ],
            "NetworkSettings": {
                "Ports": {
                    benchmark.HTTP_PORT_LABEL: [
                        {"HostIp": "127.0.0.1", "HostPort": "18080"}
                    ]
                }
            },
        }

    def __call__(self, command: list[str]) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["docker", "inspect"] and command[2] in {
            "lethe-benchmark",
            "container-id",
        }:
            return completed(command, json.dumps([self.document]))
        if command == ["docker", "image", "inspect", "image-id"]:
            return completed(
                command,
                json.dumps(
                    [
                        {
                            "Config": {
                                "Labels": {
                                    **self.image_labels,
                                }
                            }
                        }
                    ]
                ),
            )
        if command[:2] == ["docker", "top"] and command[2] in {
            "lethe-benchmark",
            "container-id",
        }:
            return completed(command, "PID PPID COMMAND\n123 1 lethe-selfhost\n")
        if (
            command[:2] == ["docker", "exec"]
            and command[2] in {"lethe-benchmark", "container-id"}
            and command[3] == "cat"
        ):
            path = command[4]
            values = {
                "/sys/fs/cgroup/memory.max": f"{benchmark.FOUR_GIB}\n",
                "/sys/fs/cgroup/memory.swap.max": "0\n",
                "/sys/fs/cgroup/cpu.max": "400000 100000\n",
                "/proc/1/status": (
                    "Name:\tlethe-selfhost\n"
                    "VmRSS:\t100000 kB\n"
                    "VmHWM:\t120000 kB\n"
                ),
                "/sys/fs/cgroup/memory.events": (
                    "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n"
                ),
                "/sys/fs/cgroup/memory.current": "150000000\n",
                "/sys/fs/cgroup/memory.peak": "170000000\n",
                "/sys/fs/cgroup/memory.swap.current": "0\n",
                "/sys/fs/cgroup/memory.swap.peak": "0\n",
            }
            if path in values:
                return completed(command, values[path])
        raise AssertionError(f"unexpected Docker command: {command!r}")


def completed(command: list[str], stdout: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")


def repository_fixture() -> dict[str, object]:
    return {
        "head": "a" * 40,
        "dirty_tree_sha256": "b" * 64,
    }


def search_response() -> dict[str, object]:
    matches = [
        {
            "record_id": f"record-{index}",
            "source_type": "slack",
            "anchor_url": f"https://benchmark.invalid/archives/C01BENCH/p{index}",
            "source_title": "100_benchmark",
            "source_location": "benchmark-user-000",
            "timestamp": f"2026-01-01T00:00:{index:02d}Z",
            "snippet": "合成検索記録 ルール",
            "matched_ranges": [{"start": 19, "end": 28}],
            "metadata": {"observation_id": f"obs-{index}"},
        }
        for index in range(20)
    ]
    return {
        "data": {
            "matches": matches,
            "next_cursor": "opaque-cursor",
            "complete": False,
            "projection_watermark": "proj:corpus:benchmark",
        },
        "projection_metadata": {
            "projection_id": "proj:corpus",
            "version": "1.0.0",
            "built_at": "2026-01-01T00:00:00Z",
            "read_mode": "operational_latest",
            "stale": False,
        },
    }


def search_request() -> dict[str, object]:
    return {
        "pattern": "ルール",
        "filters": {
            "types": ["slack"],
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-01-01T00:00:19Z",
            "channels": [],
            "containers": [],
        },
        "normalization": "nfkc",
        "order": "date_asc",
        "limit": 20,
    }


if __name__ == "__main__":
    unittest.main()
