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


@pytest.mark.parametrize(
    "key",
    ["TOKEN", "PASSWORD", "SECRET", "API_KEY", "DATABASE_URL"],
)
def test_secret_scan_rejects_exact_credential_keys(tmp_path: Path, key: str) -> None:
    candidate = tmp_path / "runtime.env"
    value = "exact-key-live-" + "credential"
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


def test_secret_scan_rejects_multiline_structured_assignment(tmp_path: Path) -> None:
    candidate = tmp_path / "config.json"
    key = "TOKEN"
    value = "multiline-live-" + "credential"
    candidate.write_text(f'{{\n  "{key}":\n  "{value}"\n}}\n')

    result = _scan(candidate)

    assert result.returncode == 1
    assert "config.json:2" in result.stdout
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


def test_secret_scan_rejects_encrypted_pkcs8_private_key(tmp_path: Path) -> None:
    candidate = tmp_path / "private.pem"
    header = "-----BEGIN " + "ENCRYPTED PRIVATE KEY-----"
    candidate.write_text(f"{header}\nencoded-key-material\n")

    result = _scan(candidate)

    assert result.returncode == 1
    assert "private.pem:1" in result.stdout
    assert header not in result.stdout


@pytest.mark.parametrize(
    "value",
    [
        "actual-placeholder-production-token",
        "replace-prod-secret",
        "$upersecret",
        "replace%2Dme",
        "%24%7BNAME%7D",
    ],
)
def test_secret_scan_rejects_values_that_only_resemble_placeholders(
    tmp_path: Path,
    value: str,
) -> None:
    candidate = tmp_path / "runtime.env"
    key = "FSTACK_OPERATOR_" + "TOKEN"
    candidate.write_text(f"{key}={value}\n")

    result = _scan(candidate)

    assert result.returncode == 1
    assert "runtime.env:1" in result.stdout
    assert value not in result.stdout


def test_secret_scan_allows_explicit_placeholders(tmp_path: Path) -> None:
    candidate = tmp_path / "env.example"
    operator_key = "FSTACK_OPERATOR_" + "TOKEN"
    database_key = "FSTACK_DATABASE_" + "URL"
    service_key = "SERVICE_" + "TOKEN"
    deploy_key = "DEPLOY_" + "TOKEN"
    service_reference = "${" + "SERVICE_TOKEN" + "}"
    deploy_reference = "${{ " + "secrets.DEPLOY_TOKEN" + " }}"
    candidate.write_text(
        f"{operator_key}=replace-with-a-long-random-operator-token\n"
        f"{database_key}=postgresql://fstack:***@localhost/fstack\n"
        "sqlalchemy.url = postgresql+psycopg://unused:unused@localhost/unused\n"
        f"{service_key}={service_reference}\n"
        f"{deploy_key}={deploy_reference}\n"
    )

    result = _scan(candidate)

    assert result.returncode == 0
    assert result.stdout == ""
