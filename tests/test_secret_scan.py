from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

SCANNER = Path(__file__).parents[1] / "tools" / "scan_tracked_secrets.py"


def _scan(path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCANNER), str(path)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_secret_scan_rejects_fstack_operator_token_assignment(tmp_path: Path) -> None:
    candidate = tmp_path / "runtime.env"
    key = "FSTACK_OPERATOR_" + "TOKEN"
    value = "actual-live-" + "token-value"
    candidate.write_text(f"{key}={value}\n")

    result = _scan(candidate)

    assert result.returncode == 1
    assert "runtime.env:1" in result.stdout
    assert value not in result.stdout


@pytest.mark.parametrize("quote", ['"', "'"])
def test_secret_scan_rejects_quoted_operator_token_assignment(
    tmp_path: Path,
    quote: str,
) -> None:
    candidate = tmp_path / "config.json"
    key = "FSTACK_OPERATOR_" + "TOKEN"
    value = "quoted-live-" + "secret-value"
    candidate.write_text(f"{quote}{key}{quote}: {quote}{value}{quote}\n")

    result = _scan(candidate)

    assert result.returncode == 1
    assert "config.json:1" in result.stdout
    assert value not in result.stdout


def test_secret_scan_rejects_database_url_credentials(tmp_path: Path) -> None:
    candidate = tmp_path / "runtime.env"
    password = "live-db-" + "password"
    key = "FSTACK_DATABASE_" + "URL"
    prefix = "postgresql" + "://fstack:"
    candidate.write_text(f"{key}={prefix}{password}@db/fstack\n")

    result = _scan(candidate)

    assert result.returncode == 1
    assert "runtime.env:1" in result.stdout
    assert password not in result.stdout


def test_secret_scan_allows_explicit_placeholders(tmp_path: Path) -> None:
    candidate = tmp_path / "env.example"
    candidate.write_text(
        "FSTACK_OPERATOR_TOKEN=replace-with-a-long-random-operator-token\n"
        "FSTACK_DATABASE_URL=postgresql://fstack:replace-me@localhost/fstack\n"
        "sqlalchemy.url = postgresql+psycopg://unused:unused@localhost/unused\n"
    )

    result = _scan(candidate)

    assert result.returncode == 0
    assert result.stdout == ""
