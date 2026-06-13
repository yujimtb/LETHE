#!/usr/bin/env python3
import argparse
import pathlib
import re
import subprocess
import sys


SECRET_PATTERNS = [
    re.compile(r"xox[baprs]-[A-Za-z0-9-]{20,}"),
    re.compile(r"ya29\.[A-Za-z0-9_-]{20,}"),
    re.compile(r"AIza[0-9A-Za-z_-]{20,}"),
]

BLOCKED_FILES = {".env", "client_secret.json"}
IGNORED_TRACKED_FILES = {".env.example", "scripts/public-release-audit.ps1", "scripts/public_release_audit.py"}


def git_lines(*args: str) -> list[str]:
    result = subprocess.run(
        ["git", *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def audit_tracked_files(root: pathlib.Path) -> list[str]:
    findings: list[str] = []
    for rel in git_lines("ls-files"):
        if rel in IGNORED_TRACKED_FILES:
            continue
        path = pathlib.PurePosixPath(rel)
        if path.name in BLOCKED_FILES:
            findings.append(f"blocked tracked file: {rel}")
            continue
        full_path = root / rel
        if not full_path.is_file():
            continue
        try:
            text = full_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for pattern in SECRET_PATTERNS:
            if pattern.search(text):
                findings.append(f"secret-like pattern in tracked file: {rel}")
                break
    return findings


def audit_history() -> list[str]:
    findings: list[str] = []
    for blocked in BLOCKED_FILES:
        hits = git_lines("log", "--all", "--name-only", "--pretty=format:", "--", blocked)
        if hits:
            findings.append(f"blocked file appears in git history: {blocked}")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-history", action="store_true")
    args = parser.parse_args()

    root = pathlib.Path(__file__).resolve().parents[1]
    findings = audit_tracked_files(root)
    if args.check_history:
        findings.extend(audit_history())

    if findings:
        for finding in findings:
            print(finding, file=sys.stderr)
        return 1
    print("public release audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
