from __future__ import annotations

import hashlib
import io
import json
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

import fstack.runtime_submission as runtime_submission


class _ProbeCursor:
    def __init__(self, row: tuple[int, ...] = (1,)) -> None:
        self.row = row
        self.queries: list[str] = []

    def __enter__(self) -> _ProbeCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str) -> None:
        self.queries.append(query)

    def fetchone(self) -> tuple[int, ...]:
        return self.row


class _ProbeConnection:
    def __init__(self, cursor: _ProbeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> _ProbeConnection:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> _ProbeCursor:
        return self._cursor


def _probe_url(scheme: str) -> str:
    return f"{scheme}://crusher_app:app-" + "password@db.example/acme"


def test_database_probe_converts_exact_psycopg_sqlalchemy_url(
    monkeypatch: Any, capsys: Any
) -> None:
    application_url = _probe_url("postgresql+psycopg")
    expected_url = _probe_url("postgresql")
    connected: list[tuple[str, int]] = []
    cursor = _ProbeCursor()

    def connect(url: str, *, connect_timeout: int) -> _ProbeConnection:
        connected.append((url, connect_timeout))
        return _ProbeConnection(cursor)

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=connect))
    monkeypatch.setattr(
        "builtins.open",
        lambda *_args, **_kwargs: nullcontext(
            io.StringIO(json.dumps({"application_url": application_url}))
        ),
    )

    exec(runtime_submission.DATABASE_PROBE_V1, {})

    assert connected == [(expected_url, 5)]
    assert cursor.queries == ["SELECT 1"]
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("scheme", ["postgresql", "postgres"])
def test_database_probe_preserves_legacy_libpq_url(
    monkeypatch: Any, capsys: Any, scheme: str
) -> None:
    application_url = _probe_url(scheme)
    connected: list[tuple[str, int]] = []
    cursor = _ProbeCursor()

    def connect(url: str, *, connect_timeout: int) -> _ProbeConnection:
        connected.append((url, connect_timeout))
        return _ProbeConnection(cursor)

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=connect))
    monkeypatch.setattr(
        "builtins.open",
        lambda *_args, **_kwargs: nullcontext(
            io.StringIO(json.dumps({"application_url": application_url}))
        ),
    )

    exec(runtime_submission.DATABASE_PROBE_V1, {})

    assert connected == [(application_url, 5)]
    assert cursor.queries == ["SELECT 1"]
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize(
    "scheme",
    [
        "postgresql+psycopg2",
        "postgresql+asyncpg",
        "postgres+psycopg",
        "PostgreSQL+Psycopg",
        "POSTGRESQL",
        "mysql",
    ],
)
def test_database_probe_rejects_unknown_or_mixed_database_prefix(
    monkeypatch: Any, capsys: Any, scheme: str
) -> None:
    application_url = _probe_url(scheme)
    connected: list[str] = []

    def connect(url: str, *, connect_timeout: int) -> _ProbeConnection:
        connected.append(url)
        return _ProbeConnection(_ProbeCursor())

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=connect))
    monkeypatch.setattr(
        "builtins.open",
        lambda *_args, **_kwargs: nullcontext(
            io.StringIO(json.dumps({"application_url": application_url}))
        ),
    )

    with pytest.raises(SystemExit) as exc_info:
        exec(runtime_submission.DATABASE_PROBE_V1, {})

    assert exc_info.value.code == 1
    assert connected == []
    assert capsys.readouterr() == ("", "")


def test_database_probe_hides_connection_failure(monkeypatch: Any, capsys: Any) -> None:
    marker = "backend rejected " + _probe_url("postgresql")

    def connect(_url: str, *, connect_timeout: int) -> _ProbeConnection:
        raise RuntimeError(marker)

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=connect))
    monkeypatch.setattr(
        "builtins.open",
        lambda *_args, **_kwargs: nullcontext(
            io.StringIO(json.dumps({"application_url": _probe_url("postgresql+psycopg")}))
        ),
    )

    with pytest.raises(SystemExit) as exc_info:
        exec(runtime_submission.DATABASE_PROBE_V1, {})

    assert exc_info.value.code == 1
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    captured = capsys.readouterr()
    assert captured == ("", "")
    assert marker not in captured.out
    assert marker not in captured.err


def test_database_probe_hides_json_parsing_failure(monkeypatch: Any, capsys: Any) -> None:
    marker = "app-" + "password"
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda *_args, **_kwargs: pytest.fail("must not connect")),
    )
    monkeypatch.setattr(
        "builtins.open",
        lambda *_args, **_kwargs: nullcontext(io.StringIO('{"application_url":"' + marker)),
    )

    with pytest.raises(SystemExit) as exc_info:
        exec(runtime_submission.DATABASE_PROBE_V1, {})

    assert exc_info.value.code == 1
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    captured = capsys.readouterr()
    assert captured == ("", "")
    assert marker not in captured.out
    assert marker not in captured.err


def test_database_probe_hides_query_failure(monkeypatch: Any, capsys: Any) -> None:
    marker = "query failed for app-" + "password"

    class FailingCursor(_ProbeCursor):
        def execute(self, query: str) -> None:
            raise RuntimeError(marker)

    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda *_args, **_kwargs: _ProbeConnection(FailingCursor())),
    )
    monkeypatch.setattr(
        "builtins.open",
        lambda *_args, **_kwargs: nullcontext(
            io.StringIO(json.dumps({"application_url": _probe_url("postgresql+psycopg")}))
        ),
    )

    with pytest.raises(SystemExit) as exc_info:
        exec(runtime_submission.DATABASE_PROBE_V1, {})

    assert exc_info.value.code == 1
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    captured = capsys.readouterr()
    assert captured == ("", "")
    assert marker not in captured.out
    assert marker not in captured.err


def test_database_probe_rejects_nonexact_select_result(monkeypatch: Any, capsys: Any) -> None:
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda *_args, **_kwargs: _ProbeConnection(_ProbeCursor((1, 2)))),
    )
    monkeypatch.setattr(
        "builtins.open",
        lambda *_args, **_kwargs: nullcontext(
            io.StringIO(json.dumps({"application_url": _probe_url("postgresql+psycopg")}))
        ),
    )

    with pytest.raises(SystemExit) as exc_info:
        exec(runtime_submission.DATABASE_PROBE_V1, {})

    assert exc_info.value.code == 1
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert capsys.readouterr() == ("", "")


def test_frozen_launcher_and_probe_sources_have_contract_hashes() -> None:
    expected = {
        "MIGRATION_LAUNCHER_V1": "5cc3ebc8fdbff87500470236433ef65ca72607f9edc821e29fc8b2c5fcf1c592",
        "API_LAUNCHER_V1": "33bb430f5625620c4c7c5b1ffefbe11feb2b97650590b61ffcc4b9e3e82186ca",
        "DATABASE_PROBE_V1": "3e33cea9bce2ff5a450733505ad9e5f120e265057ca6a4f9388716b41e5d6615",
        "HTTP_PROBE_V1": "a6045ff06180fe16a7ca13efcf1744d377c61a9f66e00d789feecbd6bb7f4d1f",
    }

    actual = {
        name: hashlib.sha256(getattr(runtime_submission, name).encode("utf-8")).hexdigest()
        for name in expected
    }

    assert actual == expected


def test_canonical_spec_seals_complete_docker_policy() -> None:
    operation_id = UUID("11111111-1111-4111-8111-111111111111")
    deployment_id = UUID("22222222-2222-4222-8222-222222222222")
    operation = {
        "operation_id": operation_id,
        "organization_id": UUID("33333333-3333-4333-8333-333333333333"),
        "deployment_id": deployment_id,
        "crusher_image": "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
        "public_domain": "mail.acme.example",
        "database_secret_ref": "secret://local-file/acme/database",
        "object_storage_secret_ref": "secret://vault/acme/object-storage",
    }
    api_name, migration_name = runtime_submission._container_names(deployment_id)

    spec = runtime_submission._canonical_spec(
        operation,
        api_container_name=api_name,
        migration_container_name=migration_name,
        docker_network="fstack-local",
        secret_stage_root=Path("/var/lib/fstack/secrets"),
    )

    assert spec["version"] == "fstack.local-crusher-spec.v1"
    assert spec["source_hashes"] == {
        "api_launcher_v1": "33bb430f5625620c4c7c5b1ffefbe11feb2b97650590b61ffcc4b9e3e82186ca",
        "database_probe_v1": "3e33cea9bce2ff5a450733505ad9e5f120e265057ca6a4f9388716b41e5d6615",
        "http_probe_v1": "a6045ff06180fe16a7ca13efcf1744d377c61a9f66e00d789feecbd6bb7f4d1f",
        "migration_launcher_v1": "5cc3ebc8fdbff87500470236433ef65ca72607f9edc821e29fc8b2c5fcf1c592",
    }
    common = spec["docker"]["common"]
    assert common == {
        "auto_remove": False,
        "cap_add": [],
        "cap_drop": ["ALL"],
        "devices": [],
        "device_requests": [],
        "dns": [],
        "dns_options": [],
        "dns_search": [],
        "entrypoint": "inherit-from-requested-digest",
        "environment": "inherit-from-requested-digest",
        "environment_overrides": [],
        "extra_hosts": [],
        "group_add": [],
        "image": operation["crusher_image"],
        "init": None,
        "ipc_mode": None,
        "label_policy": "inherit-from-requested-digest-plus-exact-fstack-labels",
        "links": [],
        "logging": {"driver": "json-file", "options": {}},
        "network": {"aliases": "deterministic-container-name-only", "name": "fstack-local"},
        "pid_mode": None,
        "platform": "native-linux",
        "ports": {},
        "privileged": False,
        "readonly_rootfs": False,
        "security_opt": ["no-new-privileges:true"],
        "stop_signal": "inherit-from-requested-digest",
        "sysctls": {},
        "tmpfs": {},
        "user": "0:0",
        "uts_mode": None,
        "working_dir": "/app",
    }
    migration = spec["docker"]["migration"]
    assert migration["command"] == [
        "/app/.venv/bin/python",
        "-c",
        runtime_submission.MIGRATION_LAUNCHER_V1,
    ]
    assert migration["healthcheck"] == ["NONE"]
    assert migration["restart_policy"] == {"name": "no", "maximum_retry_count": 0}
    assert migration["mounts"] == [
        {
            "read_only": True,
            "source": "/var/lib/fstack/secrets/22222222222242228222222222222222/owner.json",
            "target": "/run/secrets/fstack/owner.json",
            "type": "bind",
        }
    ]
    api = spec["docker"]["api"]
    assert api["command"] == [
        "/app/.venv/bin/python",
        "-c",
        runtime_submission.API_LAUNCHER_V1,
    ]
    assert api["restart_policy"] == {"name": "unless-stopped", "maximum_retry_count": 0}
    assert api["healthcheck"] == {
        "command": [
            "CMD",
            "/app/.venv/bin/python",
            "-c",
            runtime_submission.HTTP_PROBE_V1,
        ],
        "interval_seconds": 30,
        "retries": 3,
        "start_period_seconds": 10,
        "timeout_seconds": 5,
    }
    assert api["mounts"] == [
        {
            "read_only": True,
            "source": "/var/lib/fstack/secrets/22222222222242228222222222222222/application.json",
            "target": "/run/secrets/fstack/application.json",
            "type": "bind",
        }
    ]
    assert spec["docker"]["labels"] == {
        "io.fstack.managed": "true",
        "io.fstack.operation_id": str(operation_id),
        "io.fstack.organization_id": "33333333-3333-4333-8333-333333333333",
        "io.fstack.deployment_id": str(deployment_id),
        "io.fstack.submission_key": runtime_submission._submission_key(operation_id),
        "io.fstack.spec_fingerprint": "self",
    }

    serialized = json.dumps(spec, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    assert runtime_submission._spec_fingerprint(spec) == hashlib.sha256(serialized).hexdigest()
