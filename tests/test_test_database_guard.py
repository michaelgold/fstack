from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import conftest as fixtures
import pytest


class _Connection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> Any:
        self.statements.append(statement)
        if statement == "SELECT current_database()":
            return _Result("fstack")
        raise AssertionError(f"destructive statement reached: {statement}")


class _Result:
    def __init__(self, database_name: str) -> None:
        self.database_name = database_name

    def fetchone(self) -> tuple[str]:
        return (self.database_name,)


class _ConnectionContext:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def __enter__(self) -> _Connection:
        return self.connection

    def __exit__(self, *_args: object) -> None:
        return None


def test_fixture_rejects_non_test_database_before_destructive_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection()
    monkeypatch.setenv(
        "FSTACK_TEST_DATABASE_URL",
        "postgresql://fstack:irrelevant@127.0.0.1:5432/fstack",
    )
    monkeypatch.setattr(
        fixtures.psycopg,
        "connect",
        lambda *_args, **_kwargs: _ConnectionContext(connection),
    )
    fixture: Iterator[object] = fixtures.migrated_database.__wrapped__(monkeypatch)

    with pytest.raises(RuntimeError, match="refusing destructive reset"):
        next(fixture)

    assert connection.statements == ["SELECT current_database()"]
