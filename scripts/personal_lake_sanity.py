#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check personal lake imported counts against source dumps."
    )
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--github-dump", type=Path)
    parser.add_argument("--github-source-instance")
    parser.add_argument("--claude-conversations-dir", type=Path)
    parser.add_argument("--claude-source-instance")
    args = parser.parse_args()

    if not args.db.is_file():
        raise SystemExit(f"SQLite database not found: {args.db}")

    observations = load_observations(args.db)
    summary = {
        "database": str(args.db),
        "observations": len(observations),
        "by_schema": dict(count_by(observations, lambda obs: obs["schema"])),
        "by_source_system": dict(
            count_by(observations, lambda obs: obs.get("source_system", "<none>"))
        ),
        "by_source_instance": dict(
            count_by(observations, lambda obs: obs.get("meta", {}).get("source_instance", "<none>"))
        ),
    }

    errors = []
    if args.github_dump:
        if not args.github_source_instance:
            raise SystemExit("--github-source-instance is required with --github-dump")
        expected = expected_github_count(args.github_dump)
        actual = count_matching(
            observations,
            schema="schema:github-event",
            source_instance=args.github_source_instance,
        )
        summary["github"] = {
            "expected": expected,
            "actual": actual,
            "source_instance": args.github_source_instance,
        }
        if expected != actual:
            errors.append(f"github expected {expected}, found {actual}")

    if args.claude_conversations_dir:
        if not args.claude_source_instance:
            raise SystemExit(
                "--claude-source-instance is required with --claude-conversations-dir"
            )
        expected = expected_claude_count(args.claude_conversations_dir)
        actual = count_matching(
            observations,
            schema="schema:claude-message",
            source_instance=args.claude_source_instance,
        )
        summary["claude"] = {
            "expected": expected,
            "actual": actual,
            "source_instance": args.claude_source_instance,
        }
        if expected != actual:
            errors.append(f"claude expected {expected}, found {actual}")

    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    if errors:
        for error in errors:
            print(f"sanity check failed: {error}", file=sys.stderr)
        return 1
    return 0


def load_observations(db_path: Path) -> list[dict]:
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute(
            "SELECT observation_json FROM observations ORDER BY append_seq"
        ).fetchall()
    finally:
        con.close()
    return [json.loads(row[0]) for row in rows]


def count_by(observations: list[dict], key_fn) -> Counter:
    counter = Counter()
    for observation in observations:
        counter[str(key_fn(observation))] += 1
    return counter


def count_matching(
    observations: list[dict], *, schema: str, source_instance: str
) -> int:
    return sum(
        1
        for observation in observations
        if observation["schema"] == schema
        and observation.get("meta", {}).get("source_instance") == source_instance
    )


def expected_github_count(dump_path: Path) -> int:
    if not dump_path.is_file():
        raise SystemExit(f"GitHub dump not found: {dump_path}")
    dump = json.loads(dump_path.read_text(encoding="utf-8"))
    total = 0
    for repo in dump.get("repositories", []):
        total += sum(1 for issue in repo.get("issues", []) if "pull_request" not in issue)
        total += len(repo.get("issue_comments", []))
        total += len(repo.get("pull_requests", []))
        total += len(repo.get("pull_request_reviews", []))
        total += len(repo.get("pull_request_review_comments", []))
        total += len(repo.get("commits", []))
        total += len(repo.get("timeline_events", []))
    return total


def expected_claude_count(conversations_dir: Path) -> int:
    if not conversations_dir.is_dir():
        raise SystemExit(f"Claude conversations dir not found: {conversations_dir}")
    total = 0
    for path in sorted(conversations_dir.glob("*.json")):
        conversation = json.loads(path.read_text(encoding="utf-8"))
        messages = conversation.get("messages")
        if not isinstance(messages, list):
            raise SystemExit(f"Claude conversation has no messages array: {path}")
        total += len(messages)
    return total


if __name__ == "__main__":
    raise SystemExit(main())
