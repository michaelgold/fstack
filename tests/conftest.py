from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine


def _reset_public_schema(connection) -> None:
    row = connection.execute("SELECT current_database()").fetchone()
    database_name = row[0] if row is not None else None
    if not isinstance(database_name, str) or not database_name.endswith("_test"):
        raise RuntimeError(
            "refusing destructive reset: connected database name must end with '_test'"
        )
    connection.execute("DROP SCHEMA IF EXISTS public CASCADE")
    connection.execute("CREATE SCHEMA public")


@pytest.fixture
def migrated_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[Engine]:
    raw_url = os.environ.get("FSTACK_TEST_DATABASE_URL")
    if raw_url is None:
        pytest.skip("FSTACK_TEST_DATABASE_URL is required for PostgreSQL tests")

    with psycopg.connect(raw_url, autocommit=True) as connection:
        _reset_public_schema(connection)

    sqlalchemy_url = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    monkeypatch.setenv("FSTACK_DATABASE_URL", sqlalchemy_url)
    configuration = Config("alembic.ini")
    command.upgrade(configuration, "head")

    engine = create_engine(sqlalchemy_url)
    try:
        yield engine
    finally:
        engine.dispose()
        with psycopg.connect(raw_url, autocommit=True) as connection:
            _reset_public_schema(connection)
