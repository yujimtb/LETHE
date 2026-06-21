#!/usr/bin/env python3
import argparse
import pathlib
import re
import subprocess
import sys


SECRET_PATTERNS = (
    re.compile(r"xox[baprs]-[A-Za-z0-9-]{20,}"),
    re.compile(r"ya29\.[A-Za-z0-9_-]{20,}"),
    re.compile(r"AIza[0-9A-Za-z_-]{20,}"),
)
BLOCKED_FILES = {".env", "client_secret.json"}
ALLOWED_DATA_FILES = {"data/README.md", "data/.gitkeep"}
SKIP_CONTENT_SCAN = {".env.example", "scripts/public_release_audit.py"}
SECRET_ENV_KEYS = {
    "LETHE_SLACK_BOT_TOKEN",
    "LETHE_SLACK_THREAD_TOKEN",
    "LETHE_GOOGLE_ACCESS_TOKEN",
    "LETHE_GOOGLE_CLIENT_SECRET",
    "LETHE_GOOGLE_REFRESH_TOKEN",
    "LETHE_GEMINI_API_KEY",
    "LETHE_NOTION_TOKEN",
}
SAFE_SECRET_EXAMPLES = {"", "xoxb-your-slack-bot-token"}


def git_lines(root: pathlib.Path, *args: str) -> list[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def is_blocked_path(rel: str) -> bool:
    path = pathlib.PurePosixPath(rel)
    if path.name in BLOCKED_FILES:
        return True
    if rel.startswith("target/"):
        return True
    if rel.startswith("data/") and rel not in ALLOWED_DATA_FILES:
        return True
    return False


def audit_env_example(root: pathlib.Path) -> list[str]:
    path = root / ".env.example"
    if not path.is_file():
        return ["missing .env.example"]

    findings: list[str] = []
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value

    for key in sorted(SECRET_ENV_KEYS):
        value = values.get(key, "")
        if value not in SAFE_SECRET_EXAMPLES:
            findings.append(f".env.example contains a non-placeholder secret value: {key}")
    return findings


def audit_repository_files(root: pathlib.Path) -> list[str]:
    findings: list[str] = []
    for rel in git_lines(
        root,
        "ls-files",
        "--cached",
        "--others",
        "--exclude-standard",
    ):
        if is_blocked_path(rel):
            findings.append(f"blocked tracked path: {rel}")
            continue
        if rel in SKIP_CONTENT_SCAN:
            continue
        full_path = root / rel
        if not full_path.is_file():
            continue
        try:
            text = full_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for pattern in SECRET_PATTERNS:
            if match := pattern.search(text):
                line = text.count("\n", 0, match.start()) + 1
                findings.append(f"secret-like pattern: {rel}:{line}")
                break
    findings.extend(audit_env_example(root))
    return findings


def audit_history(root: pathlib.Path) -> list[str]:
    findings: list[str] = []
    names = git_lines(root, "log", "--all", "--name-only", "--pretty=format:")
    for rel in sorted(set(names)):
        if is_blocked_path(rel):
            findings.append(f"blocked path appears in git history: {rel}")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-history", action="store_true")
    args = parser.parse_args()

    root = pathlib.Path(__file__).resolve().parents[1]
    findings = audit_repository_files(root)
    if args.check_history:
        findings.extend(audit_history(root))

    if findings:
        print("public release audit failed:", file=sys.stderr)
        for finding in findings:
            print(f" - {finding}", file=sys.stderr)
        return 1

    print("public release audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
