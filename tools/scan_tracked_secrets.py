#!/usr/bin/env python3
"""Fail closed when tracked text contains likely credentials."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

IGNORED_REPOSITORY_PATHS = {
    "tools/scan_tracked_secrets.py",
}
PLACEHOLDER_VALUES = {
    "***",
    "ci-operator-token",
    "do-not-echo",
    "fstack-test-password",
    "invalid",
    "irrelevant",
    "password",
    "replace-me",
    "replace-with-a-long-random-token",
    "replace-with-a-long-random-operator-token",
    "smoke-operator-token",
    "test-operator-token",
    "unused",
}
ASSIGNMENT = re.compile(
    r"(?<![A-Z0-9_])(?P<quote>[\"']?)"
    r"(?P<key>(?:[A-Z][A-Z0-9_]*)?(?:TOKEN|SECRET|PASSWORD|PASSWD|DATABASE_URL|API_KEY|PRIVATE_KEY|AUTHORIZATION))"
    r"(?P=quote)\s*[:=]\s*(?P<value>.+?)\s*$",
    re.MULTILINE,
)
CREDENTIAL_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:([^@\s/]+)@")
KNOWN_SECRET = re.compile(
    r"(?:gh[opurs]_[A-Za-z0-9_]{12,}|github_pat_[A-Za-z0-9_]{12,}|"
    r"-----BEGIN (?:RSA |OPENSSH |EC |DSA |ENCRYPTED )?PRIVATE KEY-----)"
)
VARIABLE_EXPRESSION = re.compile(
    r"(?:\$\{[A-Za-z_][A-Za-z0-9_]*\}|"
    r"\$\{\{\s*(?:env|secrets|vars)\.[A-Za-z_][A-Za-z0-9_]*\s*\}\})"
)


def _normalized_value(raw: str) -> str:
    value = raw.strip().rstrip(",")
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return value


def _is_placeholder(value: str) -> bool:
    normalized = _normalized_value(value)
    if normalized in PLACEHOLDER_VALUES:
        return True
    if VARIABLE_EXPRESSION.fullmatch(normalized) is not None:
        return True
    parsed = urlsplit(normalized)
    return parsed.password is not None and unquote(parsed.password) in PLACEHOLDER_VALUES


def scan_text(path: Path, text: str) -> list[int]:
    findings: set[int] = set()
    for assignment in ASSIGNMENT.finditer(text):
        if not _is_placeholder(assignment.group("value")):
            findings.add(text.count("\n", 0, assignment.start("key")) + 1)

    for line_number, line in enumerate(text.splitlines(), start=1):
        if KNOWN_SECRET.search(line):
            findings.add(line_number)
            continue
        credential_url = CREDENTIAL_URL.search(line)
        if credential_url is not None and not _is_placeholder(credential_url.group(1)):
            findings.add(line_number)
    return sorted(findings)


def _tracked_files() -> list[Path]:
    output = subprocess.check_output(["git", "ls-files", "-z"])
    return [Path(item.decode()) for item in output.split(b"\0") if item]


def main(argv: list[str]) -> int:
    paths = [Path(argument) for argument in argv] if argv else _tracked_files()
    findings: list[tuple[Path, int]] = []
    for path in paths:
        repository_path = path.as_posix()
        if not argv and repository_path in IGNORED_REPOSITORY_PATHS:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        findings.extend((path, line_number) for line_number in scan_text(path, text))

    for path, line_number in findings:
        print(f"{path}:{line_number}: potential credential")
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
