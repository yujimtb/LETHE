#!/usr/bin/env python3
"""Run a synthetic personal lake import smoke test through the real CLIs."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IMPORT_RE = re.compile(
    r"(?P<kind>claude|github) import complete: "
    r"ingested=(?P<ingested>\d+), "
    r"duplicates=(?P<duplicates>\d+), "
    r"quarantined=(?P<quarantined>\d+)"
)


def main() -> int:
    args = parse_args()
    work_dir = prepare_work_dir(args.work_dir)
    db_path = work_dir / "lethe.sqlite3"
    blob_dir = work_dir / "blobs"
    blob_dir.mkdir()
    config_path = work_dir / "config.toml"
    claude_zip = work_dir / "claude-export.zip"
    claude_conversations_dir = work_dir / "claude-conversations"
    github_dump = work_dir / "github-dump.json"

    write_config(config_path, db_path, blob_dir)
    write_claude_fixture(claude_zip, claude_conversations_dir)
    write_github_fixture(github_dump)

    env = smoke_env(config_path)

    claude_first = run_import(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "lethe-import-claude",
            "--",
            f"--zip={claude_zip}",
            "--source-instance=smoke-claude",
        ],
        env,
    )
    assert_report("claude first import", claude_first, ingested=2, duplicates=0, quarantined=0)

    claude_second = run_import(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "lethe-import-claude",
            "--",
            f"--zip={claude_zip}",
            "--source-instance=smoke-claude",
        ],
        env,
    )
    assert_report("claude second import", claude_second, ingested=0, duplicates=2, quarantined=0)

    github_first = run_import(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "lethe-import-github",
            "--",
            f"--dump={github_dump}",
            "--source-instance=smoke-github",
        ],
        env,
    )
    assert_report("github first import", github_first, ingested=7, duplicates=0, quarantined=0)

    github_second = run_import(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "lethe-import-github",
            "--",
            f"--dump={github_dump}",
            "--source-instance=smoke-github",
        ],
        env,
    )
    assert_report("github second import", github_second, ingested=0, duplicates=7, quarantined=0)

    sanity = run(
        [
            sys.executable,
            str(ROOT / "scripts" / "personal_lake_sanity.py"),
            "--db",
            str(db_path),
            "--github-dump",
            str(github_dump),
            "--github-source-instance",
            "smoke-github",
            "--claude-conversations-dir",
            str(claude_conversations_dir),
            "--claude-source-instance",
            "smoke-claude",
        ],
        env,
    )
    sanity_summary = json.loads(sanity.stdout)

    observation_count = sqlite_observation_count(db_path)
    if observation_count != 9:
        fail(f"expected 9 observations after smoke imports, found {observation_count}")

    print(
        json.dumps(
            {
                "status": "ok",
                "work_dir": str(work_dir),
                "database": str(db_path),
                "observations": observation_count,
                "reports": [
                    claude_first,
                    claude_second,
                    github_first,
                    github_second,
                ],
                "sanity": sanity_summary,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    return parser.parse_args()


def prepare_work_dir(path: Path) -> Path:
    resolved = path.resolve()
    if resolved.exists():
        if not resolved.is_dir():
            fail(f"--work-dir is a file: {resolved}")
        if any(resolved.iterdir()):
            fail(f"--work-dir must be empty: {resolved}")
    else:
        resolved.mkdir(parents=True)
    return resolved


def write_config(config_path: Path, db_path: Path, blob_dir: Path) -> None:
    jwks_path = config_path.with_name("mcp-jwks.json")
    jwks_path.write_text(
        json.dumps(
            {
                "keys": [
                    {
                        "kty": "EC",
                        "kid": "smoke-key",
                        "alg": "ES256",
                        "crv": "P-256",
                        "x": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                        "y": "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE",
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    config_path.write_text(
        f"""
[server]
bind_addr = "127.0.0.1:0"
mcp_bind_addr = "127.0.0.1:1"

[mcp]
resource_url = "https://mcp.example.test/mcp"
protected_resource_metadata_url = "https://mcp.example.test/.well-known/oauth-protected-resource"
oauth_issuer = "https://issuer.example.test/"
oauth_audience = "lethe-mcp"
oauth_jwks_path = "{toml_path(jwks_path)}"

[storage]
database_path = "{toml_path(db_path)}"
blob_dir = "{toml_path(blob_dir)}"
encryption_key_env = "LETHE_STORAGE_ENCRYPTION_KEY"

[routing]
key_order = "year_month_source_container_published"

[runtime]
poll_seconds = 300

[limits]
max_blob_bytes = 10485760
max_payload_bytes = 1048576
max_sync_items = 10000
max_page_size = 100
max_leaf_observations = 100000
retention_days = 3650

[corpus]
mode = "personal_all_text"

[supplemental]
reject_unregistered_kinds = true

[[api_tokens]]
token_env = "LETHE_API_READ_TOKEN"
scopes = ["read:persons", "read:timeline", "read:corpus"]

[[api_tokens]]
token_env = "LETHE_API_SYNC_TOKEN"
scopes = ["admin:sync", "admin:health"]

[sources]
slack = []
google_slides = []
""".lstrip(),
        encoding="utf-8",
    )


def write_claude_fixture(zip_path: Path, conversations_dir: Path) -> None:
    conversations_dir.mkdir()
    conversation = {
        "uuid": "smoke-conversation",
        "messages": [
            {
                "uuid": "smoke-message-1",
                "parent_message_uuid": None,
                "sender": "human",
                "text": "hello",
                "created_at": "2026-07-01T00:00:00Z",
            },
            {
                "uuid": "smoke-message-2",
                "parent_message_uuid": "smoke-message-1",
                "sender": "assistant",
                "text": "hello back",
                "created_at": "2026-07-01T00:00:01Z",
            },
        ],
    }
    (conversations_dir / "smoke-conversation.json").write_text(
        json.dumps(conversation, indent=2),
        encoding="utf-8",
    )
    export = {"conversations": [conversation]}
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("conversations.json", json.dumps(export))


def write_github_fixture(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "dumped_at": "2026-07-01T00:00:00Z",
                "repositories": [
                    {
                        "full_name": "owner/repo",
                        "issues": [
                            {
                                "number": 1,
                                "title": "Bug",
                                "body": "body",
                                "state": "open",
                                "created_at": "2026-07-01T00:01:00Z",
                                "updated_at": "2026-07-01T00:02:00Z",
                                "user": {"login": "alice"},
                            }
                        ],
                        "issue_comments": [
                            {
                                "id": 10,
                                "body": "comment",
                                "created_at": "2026-07-01T00:03:00Z",
                                "updated_at": "2026-07-01T00:04:00Z",
                                "user": {"login": "bob"},
                            }
                        ],
                        "pull_requests": [
                            {
                                "number": 2,
                                "title": "Feature",
                                "body": "pr body",
                                "state": "closed",
                                "created_at": "2026-07-01T00:05:00Z",
                                "updated_at": "2026-07-01T00:06:00Z",
                                "user": {"login": "carol"},
                                "head": {"sha": "headsha"},
                                "base": {"sha": "basesha"},
                            }
                        ],
                        "pull_request_reviews": [
                            {
                                "id": 20,
                                "state": "APPROVED",
                                "body": "looks good",
                                "submitted_at": "2026-07-01T00:07:00Z",
                                "commit_id": "reviewsha",
                                "user": {"login": "dave"},
                            }
                        ],
                        "pull_request_review_comments": [
                            {
                                "id": 30,
                                "body": "line note",
                                "path": "src/lib.rs",
                                "line": 7,
                                "original_commit_id": "anchorsha",
                                "created_at": "2026-07-01T00:08:00Z",
                                "updated_at": "2026-07-01T00:09:00Z",
                                "pull_request_review_id": 20,
                                "user": {"login": "erin"},
                            }
                        ],
                        "commits": [
                            {
                                "sha": "commitsha",
                                "commit": {
                                    "message": "commit message",
                                    "author": {
                                        "name": "Frank",
                                        "email": "frank@example.com",
                                        "date": "2026-07-01T00:10:00Z",
                                    },
                                    "committer": {"date": "2026-07-01T00:11:00Z"},
                                },
                                "author": {"login": "frank"},
                                "files": [
                                    {
                                        "filename": "src/lib.rs",
                                        "status": "modified",
                                        "sha": "filesha",
                                        "additions": 1,
                                        "deletions": 2,
                                        "changes": 3,
                                        "patch": "@@ diff content",
                                    }
                                ],
                            }
                        ],
                        "timeline_events": [
                            {
                                "id": 40,
                                "event": "future_event_type",
                                "actor": {"login": "gina"},
                                "created_at": "2026-07-01T00:12:00Z",
                                "rename": {"from": "old", "to": "new"},
                            }
                        ],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def smoke_env(config_path: Path) -> dict[str, str]:
    if shutil.which("cargo") is None:
        fail("cargo command is not available")
    env = os.environ.copy()
    env["LETHE_CONFIG_PATH"] = str(config_path)
    env["LETHE_STORAGE_ENCRYPTION_KEY"] = "01" * 32
    env["LETHE_API_READ_TOKEN"] = "smoke-read-token"
    env["LETHE_API_SYNC_TOKEN"] = "smoke-sync-token"
    return env


def run_import(command: list[str], env: dict[str, str]) -> dict[str, int | str]:
    result = run(command, env)
    match = IMPORT_RE.search(result.stdout)
    if match is None:
        fail(f"import output did not match expected report: {result.stdout!r}")
    return {
        "kind": match.group("kind"),
        "ingested": int(match.group("ingested")),
        "duplicates": int(match.group("duplicates")),
        "quarantined": int(match.group("quarantined")),
    }


def run(command: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        fail(
            "command failed: "
            + " ".join(command)
            + f"\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def assert_report(
    label: str,
    report: dict[str, int | str],
    *,
    ingested: int,
    duplicates: int,
    quarantined: int,
) -> None:
    expected = {
        "ingested": ingested,
        "duplicates": duplicates,
        "quarantined": quarantined,
    }
    actual = {
        "ingested": report["ingested"],
        "duplicates": report["duplicates"],
        "quarantined": report["quarantined"],
    }
    if actual != expected:
        fail(f"{label} report was {actual!r}; expected {expected!r}")


def sqlite_observation_count(db_path: Path) -> int:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return int(conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0])
    finally:
        conn.close()


def toml_path(path: Path) -> str:
    return path.resolve().as_posix()


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


if __name__ == "__main__":
    raise SystemExit(main())
