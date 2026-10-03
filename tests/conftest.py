from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine


@pytest.fixture
def migrated_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[Engine]:
    raw_url = os.environ.get("FSTACK_TEST_DATABASE_URL")
    if raw_url is None:
        pytest.skip("FSTACK_TEST_DATABASE_URL is required for PostgreSQL tests")

    with psycopg.connect(raw_url, autocommit=True) as connection:
        connection.execute("DROP SCHEMA IF EXISTS public CASCADE")
        connection.execute("CREATE SCHEMA public")

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
            connection.execute("DROP SCHEMA IF EXISTS public CASCADE")
            connection.execute("CREATE SCHEMA public")
