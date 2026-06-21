#!/usr/bin/env python3
import pathlib
import re
import sys
from urllib.parse import unquote


LINK_PATTERN = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")


def target_path(source: pathlib.Path, raw_target: str) -> pathlib.Path | None:
    target = raw_target.strip().strip("<>")
    if not target or target.startswith(("#", "http://", "https://", "mailto:")):
        return None
    target = unquote(target.split("#", 1)[0])
    if not target:
        return None
    return (source.parent / target).resolve()


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    findings: list[str] = []
    for source in root.rglob("*.md"):
        if any(part in {".git", "target"} for part in source.parts):
            continue
        text = source.read_text(encoding="utf-8")
        for line_number, line in enumerate(text.splitlines(), start=1):
            for raw_target in LINK_PATTERN.findall(line):
                resolved = target_path(source, raw_target)
                if resolved is not None and not resolved.exists():
                    findings.append(
                        f"{source.relative_to(root)}:{line_number}: missing {raw_target}"
                    )
    if findings:
        print("markdown link check failed:", file=sys.stderr)
        for finding in findings:
            print(f" - {finding}", file=sys.stderr)
        return 1
    print("markdown link check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
