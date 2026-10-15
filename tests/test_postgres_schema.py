from __future__ import annotations

import hashlib
import json
import re
import runpy
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import Connection, Engine, inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from fstack.api import create_app

VALID_BODY = {
    "organization_slug": "acme",
    "organization_name": "Acme Robotics",
    "business_slug": "mailer",
    "business_name": "Acme Mailer",
    "environment_slug": "production",
    "public_domain": "mail.acme.example",
    "crusher_image": "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
    "database_secret_ref": "secret://vault/acme/database",
    "object_storage_secret_ref": "secret://vault/acme/object-storage",
}


def _create_operation(monkeypatch: pytest.MonkeyPatch, suffix: str | None = None) -> dict[str, str]:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    body = dict(VALID_BODY)
    idempotency_key = "create-acme-mailer"
    if suffix is not None:
        body.update(
            organization_slug=f"acme-{suffix}",
            organization_name=f"Acme Robotics {suffix}",
            business_slug=f"mailer-{suffix}",
            business_name=f"Acme Mailer {suffix}",
            public_domain=f"mail-{suffix}.acme.example",
        )
        idempotency_key = f"create-acme-mailer-{suffix}"
    response = TestClient(create_app()).post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": idempotency_key,
        },
        json=body,
    )
    assert response.status_code == 202
    return response.json()


def _mark_dispatch_v2(engine: Engine, operation_id: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE workflow_dispatches DISABLE TRIGGER "
                "tr_workflow_dispatches_lifecycle_guard"
            )
        )
        connection.execute(
            text(
                "UPDATE workflow_dispatches SET workflow_type='fstack.provision_crusher.v2' "
                "WHERE operation_id=:operation_id"
            ),
            {"operation_id": operation_id},
        )
        connection.execute(
            text(
                "ALTER TABLE workflow_dispatches ENABLE TRIGGER "
                "tr_workflow_dispatches_lifecycle_guard"
            )
        )


_POSTGRES_SQL_TOKEN = re.compile(
    r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|::|>=|<=|<>|!=|\|\||[(),=~]|"
    r"[A-Za-z_][A-Za-z0-9_]*|\d+|\S"
)


def _normalize_postgres_sql(expression: str) -> tuple[str, ...]:
    """Tokenize catalog SQL so whitespace and keyword casing are irrelevant."""
    return tuple(
        token if token.startswith(("'", '"')) else token.lower()
        for token in _POSTGRES_SQL_TOKEN.findall(expression)
    )


def _ingress_values(created: dict[str, str], seed: str) -> dict[str, object]:
    return {
        "id": uuid4(),
        "organization_id": created["organization_id"],
        "operation_id": created["operation_id"],
        "deployment_id": created["deployment_id"],
        "provider": "local_caddy",
        "generation": 1,
        "submission_key": hashlib.sha256(f"submission:{seed}".encode()).hexdigest(),
        "route_fingerprint": hashlib.sha256(f"route:{seed}".encode()).hexdigest(),
        "route_id": "fstack-crusher-ingress-" + created["deployment_id"].replace("-", ""),
        "state": "prepared",
        "failure_code": None,
        "submitted_at": None,
        "confirmed_at": None,
        "failed_at": None,
    }


_INGRESS_INSERT_SQL = """
    INSERT INTO ingress_submissions (
        id, organization_id, operation_id, deployment_id, provider, generation,
        submission_key, route_fingerprint, route_id, state, failure_code,
        submitted_at, confirmed_at, failed_at
    ) VALUES (
        :id, :organization_id, :operation_id, :deployment_id, :provider, :generation,
        :submission_key, :route_fingerprint, :route_id, :state, :failure_code,
        :submitted_at, :confirmed_at, :failed_at
    )
"""


def _insert_ingress(connection: Connection, values: dict[str, object]) -> None:
    connection.execute(text(_INGRESS_INSERT_SQL), values)


def _runtime_values(created: dict[str, str], seed: str) -> dict[str, object]:
    deployment_hex = created["deployment_id"].replace("-", "")
    return {
        "id": uuid4(),
        "organization_id": created["organization_id"],
        "operation_id": created["operation_id"],
        "deployment_id": created["deployment_id"],
        "submission_key": hashlib.sha256(f"runtime-submission:{seed}".encode()).hexdigest(),
        "spec_fingerprint": hashlib.sha256(f"runtime-spec:{seed}".encode()).hexdigest(),
        "api_container_name": f"fstack-crusher-{deployment_hex}",
        "migration_container_name": f"fstack-crusher-migrate-{deployment_hex}",
    }


def _insert_runtime(connection: Connection, values: dict[str, object]) -> None:
    connection.execute(
        text(
            """
            INSERT INTO runtime_submissions (
                id, organization_id, operation_id, deployment_id, provider, generation,
                submission_key, spec_fingerprint, api_container_name,
                migration_container_name, state
            ) VALUES (
                :id, :organization_id, :operation_id, :deployment_id, 'local_docker', 1,
                :submission_key, :spec_fingerprint, :api_container_name,
                :migration_container_name, 'prepared'
            )
            """
        ),
        values,
    )


def _insert_audit(
    connection: Connection,
    created: dict[str, str],
    event_type: str,
    payload: Mapping[str, object],
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO audit_events (
                id, organization_id, operation_id, event_type, actor_principal, payload
            ) VALUES (
                :id, :organization_id, :operation_id, :event_type,
                'installation-operator', CAST(:payload AS jsonb)
            )
            """
        ),
        {
            "id": uuid4(),
            "organization_id": created["organization_id"],
            "operation_id": created["operation_id"],
            "event_type": event_type,
            "payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        },
    )


def _assert_ingress_failure(
    connection: Connection,
    statement: str,
    parameters: dict[str, object],
    expected_sqlstate: str,
) -> None:
    savepoint = connection.begin_nested()
    try:
        with pytest.raises(IntegrityError) as exc_info:
            connection.execute(text(statement), parameters)
        assert getattr(exc_info.value.orig, "sqlstate", None) == expected_sqlstate
    finally:
        savepoint.rollback()


@pytest.mark.postgres
def test_temporal_lifecycle_schema_has_no_postgres_attempt_authority(
    migrated_database: Engine,
) -> None:
    inspector = inspect(migrated_database)
    tables = set(inspector.get_table_names())
    operation_columns = {
        column["name"] for column in inspector.get_columns("provisioning_operations")
    }

    assert "runtime_submissions" in tables
    assert "workflow_dispatches" in tables
    assert "provisioning_attempts" not in tables
    assert "crusher_docker_effects" not in tables
    assert {"attempt_count", "current_attempt_id", "last_error_retryable"}.isdisjoint(
        operation_columns
    )
    assert {"updated_at", "started_at", "completed_at", "last_error_code"} <= (operation_columns)


@pytest.mark.postgres
def test_local_caddy_ingress_schema_is_exact(migrated_database: Engine) -> None:
    inspector = inspect(migrated_database)
    tables = set(inspector.get_table_names())

    assert "ingress_submissions" in tables
    columns = {column["name"]: column for column in inspector.get_columns("ingress_submissions")}
    expected_columns = {
        "id": ("UUID", None, None, None, False),
        "organization_id": ("UUID", None, None, None, False),
        "operation_id": ("UUID", None, None, None, False),
        "deployment_id": ("UUID", None, None, None, False),
        "provider": ('VARCHAR(32) COLLATE "C"', 32, "C", None, False),
        "generation": ("BIGINT", None, None, None, False),
        "submission_key": ('VARCHAR(64) COLLATE "C"', 64, "C", None, False),
        "route_fingerprint": ('VARCHAR(64) COLLATE "C"', 64, "C", None, False),
        "route_id": ('VARCHAR(200) COLLATE "C"', 200, "C", None, False),
        "state": ('VARCHAR(32) COLLATE "C"', 32, "C", None, False),
        "failure_code": ('VARCHAR(64) COLLATE "C"', 64, "C", None, True),
        "prepared_at": ("TIMESTAMP", None, None, True, False),
        "submitted_at": ("TIMESTAMP", None, None, True, True),
        "confirmed_at": ("TIMESTAMP", None, None, True, True),
        "failed_at": ("TIMESTAMP", None, None, True, True),
    }
    assert set(columns) == set(expected_columns)
    for name, (type_sql, length, collation, timezone_enabled, nullable) in expected_columns.items():
        column_type = columns[name]["type"]
        assert (
            str(column_type),
            getattr(column_type, "length", None),
            getattr(column_type, "collation", None),
            getattr(column_type, "timezone", None),
            columns[name]["nullable"],
        ) == (type_sql, length, collation, timezone_enabled, nullable)
    prepared_default = columns["prepared_at"]["default"]
    assert isinstance(prepared_default, str)
    assert _normalize_postgres_sql(prepared_default) == ("current_timestamp",)
    assert all(
        column["default"] is None for name, column in columns.items() if name != "prepared_at"
    )

    primary_key = inspector.get_pk_constraint("ingress_submissions")
    assert primary_key["constrained_columns"] == ["id"]

    foreign_keys = {
        foreign_key["name"]: foreign_key
        for foreign_key in inspector.get_foreign_keys("ingress_submissions")
    }
    assert set(foreign_keys) == {
        "fk_ingress_submissions_exact_operation",
        "fk_ingress_submissions_deployment_org",
    }
    assert {
        name: (
            foreign_key["constrained_columns"],
            foreign_key["referred_schema"],
            foreign_key["referred_table"],
            foreign_key["referred_columns"],
            foreign_key.get("options", {}),
        )
        for name, foreign_key in foreign_keys.items()
    } == {
        "fk_ingress_submissions_exact_operation": (
            ["organization_id", "operation_id", "deployment_id"],
            None,
            "provisioning_operations",
            ["organization_id", "id", "deployment_id"],
            {"ondelete": "RESTRICT", "initially": "DEFERRED", "deferrable": True},
        ),
        "fk_ingress_submissions_deployment_org": (
            ["organization_id", "deployment_id"],
            None,
            "deployments",
            ["organization_id", "id"],
            {"ondelete": "RESTRICT", "initially": "DEFERRED", "deferrable": True},
        ),
    }

    uniques = {
        unique["name"]: tuple(unique["column_names"])
        for unique in inspector.get_unique_constraints("ingress_submissions")
    }
    assert uniques == {
        "uq_ingress_submissions_operation": ("operation_id",),
        "uq_ingress_submissions_key": ("submission_key",),
        "uq_ingress_submissions_provider_generation": (
            "provider",
            "deployment_id",
            "generation",
        ),
        "uq_ingress_submissions_route_id": ("route_id",),
    }
    checks = {
        check["name"]: _normalize_postgres_sql(check["sqltext"])
        for check in inspector.get_check_constraints("ingress_submissions")
    }
    expected_checks = {
        "ck_ingress_provider": "provider::text = 'local_caddy'::text",
        "ck_ingress_generation": "generation = 1",
        "ck_ingress_lowercase_hex_identity": (
            "submission_key::text ~ '^[0-9a-f]{64}$'::text "
            "AND route_fingerprint::text ~ '^[0-9a-f]{64}$'::text"
        ),
        "ck_ingress_deterministic_route_id": (
            "route_id::text = ('fstack-crusher-ingress-'::text || "
            "replace(deployment_id::text, '-'::text, ''::text))"
        ),
        "ck_ingress_failure_code_sanitized": (
            "failure_code IS NULL OR failure_code::text ~ '^[a-z][a-z0-9_]{0,63}$'::text"
        ),
        "ck_ingress_state_bundle": (
            "(state::text = 'prepared'::text AND submitted_at IS NULL "
            "AND confirmed_at IS NULL AND failed_at IS NULL AND failure_code IS NULL "
            "OR state::text = 'submitted'::text AND submitted_at IS NOT NULL "
            "AND confirmed_at IS NULL AND failed_at IS NULL AND failure_code IS NULL "
            "OR state::text = 'confirmed'::text AND submitted_at IS NOT NULL "
            "AND confirmed_at IS NOT NULL AND failed_at IS NULL AND failure_code IS NULL "
            "OR state::text = 'failed'::text AND confirmed_at IS NULL "
            "AND failed_at IS NOT NULL AND failure_code IS NOT NULL) IS TRUE"
        ),
        "ck_ingress_timestamps_monotonic": (
            "(submitted_at IS NULL OR submitted_at >= prepared_at) "
            "AND (confirmed_at IS NULL OR confirmed_at >= submitted_at) "
            "AND (failed_at IS NULL OR failed_at >= COALESCE(submitted_at, prepared_at))"
        ),
    }
    assert checks == {
        name: _normalize_postgres_sql(expression) for name, expression in expected_checks.items()
    }

    workflow_checks = {
        check["name"]: _normalize_postgres_sql(check["sqltext"])
        for check in inspector.get_check_constraints("workflow_dispatches")
    }
    assert workflow_checks["ck_workflow_dispatches_type"] == _normalize_postgres_sql(
        "workflow_type::text = ANY (ARRAY["
        "'fstack.provision_crusher.v1'::character varying, "
        "'fstack.provision_crusher.v2'::character varying]::text[])"
    )

    with migrated_database.connect() as connection:
        trigger_names = set(
            connection.execute(
                text(
                    """
                    SELECT tgname
                    FROM pg_trigger
                    WHERE tgrelid = 'ingress_submissions'::regclass AND NOT tgisinternal
                    """
                )
            ).scalars()
        )
        assert trigger_names == {"tr_ingress_submissions_lifecycle_guard"}
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
            "20261015_0005"
        )

    forbidden_authority_terms = (
        "attempt",
        "lease",
        "claim",
        "heartbeat",
        "poll",
        "retry",
        "next_run",
        "timer",
    )
    assert not {
        table
        for table in tables
        if table != "alembic_version" and any(term in table for term in forbidden_authority_terms)
    }
    assert not {
        column for column in columns if any(term in column for term in forbidden_authority_terms)
    }


@pytest.mark.postgres
def test_workflow_dispatch_v2_cutover_preserves_existing_rows_and_changes_only_new_rows(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261006_0003")
    legacy = _create_operation(monkeypatch, "legacy-v1")

    with migrated_database.connect() as connection:
        before = (
            connection.execute(
                text(
                    "SELECT operation_id, workflow_id, workflow_type, state, created_at, "
                    "dispatched_at FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": legacy["operation_id"]},
            )
            .mappings()
            .one()
        )
    assert before["workflow_id"] == f"provisioning/{legacy['operation_id']}"
    assert before["workflow_type"] == "fstack.provision_crusher.v1"

    command.upgrade(configuration, "20261013_0004")
    admitted = _create_operation(monkeypatch, "admitted-v1")
    with migrated_database.connect() as connection:
        admitted_before_cutover = (
            connection.execute(
                text(
                    "SELECT operation_id, workflow_id, workflow_type, state, created_at, "
                    "dispatched_at FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": admitted["operation_id"]},
            )
            .mappings()
            .one()
        )
    assert admitted_before_cutover["workflow_type"] == "fstack.provision_crusher.v1"

    command.upgrade(configuration, "head")
    current = _create_operation(monkeypatch, "current-v2")

    with migrated_database.connect() as connection:
        after = (
            connection.execute(
                text(
                    "SELECT operation_id, workflow_id, workflow_type, state, created_at, "
                    "dispatched_at FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": legacy["operation_id"]},
            )
            .mappings()
            .one()
        )
        admitted_after_cutover = (
            connection.execute(
                text(
                    "SELECT operation_id, workflow_id, workflow_type, state, created_at, "
                    "dispatched_at FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": admitted["operation_id"]},
            )
            .mappings()
            .one()
        )
        current_dispatch = (
            connection.execute(
                text(
                    "SELECT operation_id, workflow_id, workflow_type, state, created_at, "
                    "dispatched_at FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": current["operation_id"]},
            )
            .mappings()
            .one()
        )

    assert after == before
    assert admitted_after_cutover == admitted_before_cutover
    assert current_dispatch["workflow_id"] == f"provisioning/{current['operation_id']}"
    assert current_dispatch["workflow_type"] == "fstack.provision_crusher.v2"
    assert current_dispatch["state"] == "pending"
    assert current_dispatch["dispatched_at"] is None

    dispatches = {
        legacy["operation_id"]: before,
        admitted["operation_id"]: admitted_before_cutover,
        current["operation_id"]: current_dispatch,
    }
    for operation_id, original in dispatches.items():
        dispatched_at = original["created_at"] + timedelta(seconds=2)
        identity_mutations = {
            "workflow_id": "provisioning/changed",
            "workflow_type": (
                "fstack.provision_crusher.v2"
                if original["workflow_type"] == "fstack.provision_crusher.v1"
                else "fstack.provision_crusher.v1"
            ),
            "created_at": original["created_at"] + timedelta(seconds=1),
            "operation_id": uuid4(),
        }
        for column, changed_value in identity_mutations.items():
            with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
                connection.execute(
                    text(
                        f"UPDATE workflow_dispatches SET {column} = :changed_value, "
                        "state = 'dispatched', dispatched_at = :dispatched_at "
                        "WHERE operation_id = :operation_id"
                    ),
                    {
                        "changed_value": changed_value,
                        "dispatched_at": dispatched_at,
                        "operation_id": operation_id,
                    },
                )
            assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"
            assert "workflow dispatch identity is immutable" in str(exc_info.value.orig)

            with migrated_database.connect() as connection:
                retained = (
                    connection.execute(
                        text(
                            "SELECT operation_id, workflow_id, workflow_type, state, created_at, "
                            "dispatched_at FROM workflow_dispatches "
                            "WHERE operation_id = :operation_id"
                        ),
                        {"operation_id": operation_id},
                    )
                    .mappings()
                    .one()
                )
            assert retained == original
            assert retained["state"] == "pending"
            assert retained["dispatched_at"] is None

        with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
            connection.execute(
                text("DELETE FROM workflow_dispatches WHERE operation_id = :operation_id"),
                {"operation_id": operation_id},
            )
        assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"

    command.downgrade(configuration, "20261013_0004")
    rollback_operation = _create_operation(monkeypatch, "rollback-v1")
    with migrated_database.connect() as connection:
        retained_v2 = (
            connection.execute(
                text(
                    "SELECT operation_id, workflow_id, workflow_type, state, created_at, "
                    "dispatched_at FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": current["operation_id"]},
            )
            .mappings()
            .one()
        )
        rollback_dispatch = (
            connection.execute(
                text(
                    "SELECT workflow_id, workflow_type, state, dispatched_at "
                    "FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": rollback_operation["operation_id"]},
            )
            .mappings()
            .one()
        )
    assert retained_v2 == current_dispatch
    assert rollback_dispatch == {
        "workflow_id": f"provisioning/{rollback_operation['operation_id']}",
        "workflow_type": "fstack.provision_crusher.v1",
        "state": "pending",
        "dispatched_at": None,
    }

    command.upgrade(configuration, "head")


@pytest.mark.postgres
@pytest.mark.parametrize("dispatch_state", ["pending", "dispatched"])
def test_local_caddy_downgrade_rejects_any_v2_workflow_dispatch_history(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    dispatch_state: str,
) -> None:
    configuration = Config("alembic.ini")
    created = _create_operation(monkeypatch, f"v2-{dispatch_state}")
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE workflow_dispatches DISABLE TRIGGER "
                "tr_workflow_dispatches_lifecycle_guard"
            )
        )
        connection.execute(
            text(
                "UPDATE workflow_dispatches SET workflow_type='fstack.provision_crusher.v2' "
                "WHERE operation_id=:operation_id"
            ),
            {"operation_id": created["operation_id"]},
        )
        connection.execute(
            text(
                "ALTER TABLE workflow_dispatches ENABLE TRIGGER "
                "tr_workflow_dispatches_lifecycle_guard"
            )
        )
        if dispatch_state == "dispatched":
            connection.execute(
                text(
                    "UPDATE workflow_dispatches SET state = 'dispatched', "
                    "dispatched_at = clock_timestamp() WHERE operation_id = :operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )

    with pytest.raises(DBAPIError) as exc_info:
        command.downgrade(configuration, "20261006_0003")
    assert getattr(exc_info.value.orig, "sqlstate", None) == "55000"

    with migrated_database.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
            "20261015_0005"
        )
        row = (
            connection.execute(
                text(
                    "SELECT workflow_id, workflow_type, state, dispatched_at "
                    "FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .one()
        )
    assert row["workflow_id"] == f"provisioning/{created['operation_id']}"
    assert row["workflow_type"] == "fstack.provision_crusher.v2"
    assert row["state"] == dispatch_state
    assert (row["dispatched_at"] is None) is (dispatch_state == "pending")


@pytest.mark.postgres
def test_local_caddy_downgrade_rejects_ingress_history(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261006_0003")
    created = _create_operation(monkeypatch, "ingress-downgrade")
    command.upgrade(configuration, "head")
    values = _ingress_values(created, "downgrade")
    with migrated_database.begin() as connection:
        _insert_ingress(connection, values)

    with migrated_database.connect() as connection:
        dispatch_before = (
            connection.execute(
                text(
                    "SELECT operation_id, workflow_id, workflow_type, state, created_at, "
                    "dispatched_at FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .one()
        )
        assert dispatch_before["workflow_type"] == "fstack.provision_crusher.v1"
        assert (
            connection.execute(
                text(
                    "SELECT count(*) FROM workflow_dispatches "
                    "WHERE workflow_type = 'fstack.provision_crusher.v2'"
                )
            ).scalar_one()
            == 0
        )

    with pytest.raises(DBAPIError) as exc_info:
        command.downgrade(configuration, "20261006_0003")
    assert getattr(exc_info.value.orig, "sqlstate", None) == "55000"

    with migrated_database.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
            "20261015_0005"
        )
        assert "ingress_submissions" in inspect(migrated_database).get_table_names()
        assert (
            connection.execute(
                text("SELECT count(*) FROM ingress_submissions WHERE id = :id"),
                {"id": values["id"]},
            ).scalar_one()
            == 1
        )
        dispatch_after = (
            connection.execute(
                text(
                    "SELECT operation_id, workflow_id, workflow_type, state, created_at, "
                    "dispatched_at FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .one()
        )
    assert dispatch_after == dispatch_before


@pytest.mark.postgres
def test_local_caddy_pristine_downgrade_upgrade_roundtrip_restores_dispatch_versions(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")

    def assert_pristine_revision(
        revision: str,
        *,
        has_ingress_submissions: bool,
        workflow_types: set[str],
        enqueued_workflow_type: str,
    ) -> None:
        inspector = inspect(migrated_database)
        assert ("ingress_submissions" in inspector.get_table_names()) is has_ingress_submissions

        workflow_checks = {
            check["name"]: _normalize_postgres_sql(check["sqltext"])
            for check in inspector.get_check_constraints("workflow_dispatches")
        }
        expected_type_check = (
            "workflow_type::text = 'fstack.provision_crusher.v1'::text"
            if workflow_types == {"fstack.provision_crusher.v1"}
            else (
                "workflow_type::text = ANY (ARRAY["
                "'fstack.provision_crusher.v1'::character varying, "
                "'fstack.provision_crusher.v2'::character varying]::text[])"
            )
        )
        assert workflow_checks["ck_workflow_dispatches_type"] == _normalize_postgres_sql(
            expected_type_check
        )

        with migrated_database.connect() as connection:
            observed_revision = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            assert observed_revision == revision
            operation_count = connection.execute(
                text("SELECT count(*) FROM provisioning_operations")
            ).scalar_one()
            assert operation_count == 0
            assert (
                connection.execute(text("SELECT count(*) FROM workflow_dispatches")).scalar_one()
                == 0
            )
            trigger_function = connection.execute(
                text(
                    "SELECT pg_get_functiondef(t.tgfoid) "
                    "FROM pg_trigger AS t "
                    "WHERE t.tgrelid = 'provisioning_operations'::regclass "
                    "AND t.tgname = 'tr_operations_enqueue_workflow_dispatch' "
                    "AND NOT t.tgisinternal AND t.tgenabled = 'O'"
                )
            ).scalar_one()
        assert all(
            (f"'{workflow_type}'" in trigger_function) is (workflow_type == enqueued_workflow_type)
            for workflow_type in (
                "fstack.provision_crusher.v1",
                "fstack.provision_crusher.v2",
            )
        )

    assert_pristine_revision(
        "20261015_0005",
        has_ingress_submissions=True,
        workflow_types={"fstack.provision_crusher.v1", "fstack.provision_crusher.v2"},
        enqueued_workflow_type="fstack.provision_crusher.v2",
    )
    for _ in range(2):
        command.downgrade(configuration, "20261013_0004")
        assert_pristine_revision(
            "20261013_0004",
            has_ingress_submissions=True,
            workflow_types={"fstack.provision_crusher.v1", "fstack.provision_crusher.v2"},
            enqueued_workflow_type="fstack.provision_crusher.v1",
        )
        command.downgrade(configuration, "20261006_0003")
        assert_pristine_revision(
            "20261006_0003",
            has_ingress_submissions=False,
            workflow_types={"fstack.provision_crusher.v1"},
            enqueued_workflow_type="fstack.provision_crusher.v1",
        )
        command.upgrade(configuration, "head")
        assert_pristine_revision(
            "20261015_0005",
            has_ingress_submissions=True,
            workflow_types={"fstack.provision_crusher.v1", "fstack.provision_crusher.v2"},
            enqueued_workflow_type="fstack.provision_crusher.v2",
        )

    created = _create_operation(monkeypatch, "roundtrip-v2")
    with migrated_database.connect() as connection:
        dispatch = (
            connection.execute(
                text(
                    "SELECT workflow_id, workflow_type, state, dispatched_at "
                    "FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .one()
        )
    assert dispatch == {
        "workflow_id": f"provisioning/{created['operation_id']}",
        "workflow_type": "fstack.provision_crusher.v2",
        "state": "pending",
        "dispatched_at": None,
    }


def test_local_caddy_migration_uses_fail_fast_complete_lock_sets() -> None:
    migration_path = Path("migrations/versions/20261013_0004_local_caddy_ingress.py")
    namespace = runpy.run_path(str(migration_path))

    assert namespace["UPGRADE_LOCK_SQL"] == (
        "LOCK TABLE organizations, deployments, provisioning_operations, runtime_submissions, "
        "workflow_dispatches, audit_events IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    assert namespace["DOWNGRADE_LOCK_SQL"] == (
        "LOCK TABLE organizations, deployments, provisioning_operations, runtime_submissions, "
        "workflow_dispatches, audit_events, ingress_submissions "
        "IN ACCESS EXCLUSIVE MODE NOWAIT"
    )


def test_v2_cutover_migration_uses_fail_fast_complete_lock_set() -> None:
    migration_path = Path("migrations/versions/20261015_0005_provisioning_workflow_v2_cutover.py")
    namespace = runpy.run_path(str(migration_path))

    assert namespace["CUTOVER_LOCK_SQL"] == (
        "LOCK TABLE provisioning_operations, workflow_dispatches IN ACCESS EXCLUSIVE MODE NOWAIT"
    )


@pytest.mark.postgres
def test_v2_cutover_upgrade_fails_fast_when_operation_writer_is_live(
    migrated_database: Engine,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261013_0004")

    with migrated_database.connect() as writer_connection:
        writer_transaction = writer_connection.begin()
        writer_connection.execute(text("LOCK TABLE provisioning_operations IN ROW EXCLUSIVE MODE"))
        try:
            with pytest.raises(DBAPIError) as exc_info:
                command.upgrade(configuration, "head")
            assert getattr(exc_info.value.orig, "sqlstate", None) == "55P03"
        finally:
            writer_transaction.rollback()

    with migrated_database.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
            "20261013_0004"
        )

    command.upgrade(configuration, "head")
    with migrated_database.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
            "20261015_0005"
        )


@pytest.mark.postgres
def test_local_caddy_upgrade_fails_fast_before_ddl_when_writer_is_live(
    migrated_database: Engine,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261006_0003")

    with migrated_database.connect() as writer_connection:
        writer_transaction = writer_connection.begin()
        writer_connection.execute(text("LOCK TABLE organizations IN ROW EXCLUSIVE MODE"))
        try:
            with pytest.raises(DBAPIError) as exc_info:
                command.upgrade(configuration, "head")
            assert getattr(exc_info.value.orig, "sqlstate", None) == "55P03"
        finally:
            writer_transaction.rollback()

    assert "ingress_submissions" not in inspect(migrated_database).get_table_names()
    with migrated_database.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
            "20261006_0003"
        )


@pytest.mark.postgres
def test_local_caddy_downgrade_fails_fast_when_ingress_writer_is_live(
    migrated_database: Engine,
) -> None:
    configuration = Config("alembic.ini")

    with migrated_database.connect() as writer_connection:
        writer_transaction = writer_connection.begin()
        writer_connection.execute(text("LOCK TABLE ingress_submissions IN ROW EXCLUSIVE MODE"))
        try:
            with pytest.raises(DBAPIError) as exc_info:
                command.downgrade(configuration, "20261006_0003")
            assert getattr(exc_info.value.orig, "sqlstate", None) == "55P03"
        finally:
            writer_transaction.rollback()

    assert "ingress_submissions" in inspect(migrated_database).get_table_names()
    with migrated_database.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
            "20261015_0005"
        )


@pytest.mark.postgres
def test_ingress_insert_accepts_only_prepared_state(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_operation(monkeypatch)
    values = _ingress_values(created, "insert-state")
    now = datetime.now(UTC)

    with migrated_database.begin() as connection:
        for state, overrides in {
            "submitted": {"submitted_at": now},
            "confirmed": {"submitted_at": now, "confirmed_at": now},
            "failed": {"failed_at": now, "failure_code": "caddy_apply_failed"},
        }.items():
            attempted = values | {"id": uuid4(), "state": state} | overrides
            _assert_ingress_failure(connection, _INGRESS_INSERT_SQL, attempted, "23000")

        _insert_ingress(connection, values)
        assert (
            connection.execute(
                text("SELECT state FROM ingress_submissions WHERE id = :id"), {"id": values["id"]}
            ).scalar_one()
            == "prepared"
        )


@pytest.mark.postgres
def test_ingress_legal_lifecycle_paths(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations = {
        path: _create_operation(monkeypatch, path)
        for path in ("confirm", "fail-prepared", "fail-submitted")
    }

    with migrated_database.begin() as connection:
        rows = {path: _ingress_values(created, path) for path, created in operations.items()}
        for values in rows.values():
            _insert_ingress(connection, values)

        prepared_at = {
            path: connection.execute(
                text("SELECT prepared_at FROM ingress_submissions WHERE id = :id"),
                {"id": values["id"]},
            ).scalar_one()
            for path, values in rows.items()
        }
        for path in ("confirm", "fail-submitted"):
            connection.execute(
                text(
                    "UPDATE ingress_submissions SET state = 'submitted', submitted_at = :at "
                    "WHERE id = :id"
                ),
                {"id": rows[path]["id"], "at": prepared_at[path] + timedelta(seconds=1)},
            )
        connection.execute(
            text(
                "UPDATE ingress_submissions SET state = 'confirmed', confirmed_at = :at "
                "WHERE id = :id"
            ),
            {
                "id": rows["confirm"]["id"],
                "at": prepared_at["confirm"] + timedelta(seconds=2),
            },
        )
        connection.execute(
            text(
                "UPDATE ingress_submissions SET state = 'failed', failed_at = :at, "
                "failure_code = 'caddy_prepare_failed' WHERE id = :id"
            ),
            {
                "id": rows["fail-prepared"]["id"],
                "at": prepared_at["fail-prepared"] + timedelta(seconds=1),
            },
        )
        connection.execute(
            text(
                "UPDATE ingress_submissions SET state = 'failed', failed_at = :at, "
                "failure_code = 'caddy_apply_failed' WHERE id = :id"
            ),
            {
                "id": rows["fail-submitted"]["id"],
                "at": prepared_at["fail-submitted"] + timedelta(seconds=2),
            },
        )

        observed_states = {
            row.id: row.state
            for row in connection.execute(
                text("SELECT id, state FROM ingress_submissions ORDER BY state, id")
            )
        }
        assert observed_states == {
            rows["confirm"]["id"]: "confirmed",
            rows["fail-prepared"]["id"]: "failed",
            rows["fail-submitted"]["id"]: "failed",
        }


@pytest.mark.postgres
def test_ingress_rejects_illegal_lifecycle_transitions(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared_operation = _create_operation(monkeypatch, "illegal-prepared")
    submitted_operation = _create_operation(monkeypatch, "illegal-submitted")

    with migrated_database.begin() as connection:
        prepared = _ingress_values(prepared_operation, "illegal-prepared")
        submitted = _ingress_values(submitted_operation, "illegal-submitted")
        _insert_ingress(connection, prepared)
        _insert_ingress(connection, submitted)
        submitted_at = connection.execute(
            text("SELECT prepared_at FROM ingress_submissions WHERE id = :id"),
            {"id": submitted["id"]},
        ).scalar_one() + timedelta(seconds=1)
        connection.execute(
            text(
                "UPDATE ingress_submissions SET state = 'submitted', submitted_at = :at "
                "WHERE id = :id"
            ),
            {"id": submitted["id"], "at": submitted_at},
        )

        _assert_ingress_failure(
            connection,
            "UPDATE ingress_submissions SET state = 'prepared' WHERE id = :id",
            {"id": prepared["id"]},
            "23000",
        )
        _assert_ingress_failure(
            connection,
            "UPDATE ingress_submissions SET state = 'confirmed', submitted_at = :at, "
            "confirmed_at = :at WHERE id = :id",
            {"id": prepared["id"], "at": submitted_at},
            "23000",
        )
        _assert_ingress_failure(
            connection,
            "UPDATE ingress_submissions SET state = 'prepared', submitted_at = NULL WHERE id = :id",
            {"id": submitted["id"]},
            "23000",
        )


@pytest.mark.postgres
def test_ingress_identity_prepared_and_submitted_facts_are_immutable(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_operation(monkeypatch)
    values = _ingress_values(created, "immutable")

    with migrated_database.begin() as connection:
        _insert_ingress(connection, values)
        identity_mutations = {
            "id": uuid4(),
            "organization_id": uuid4(),
            "operation_id": uuid4(),
            "deployment_id": uuid4(),
            "provider": "other_provider",
            "generation": 2,
            "submission_key": "a" * 64,
            "route_fingerprint": "b" * 64,
            "route_id": "fstack-crusher-ingress-other",
            "prepared_at": datetime.now(UTC) + timedelta(days=1),
        }
        for column, value in identity_mutations.items():
            _assert_ingress_failure(
                connection,
                f"UPDATE ingress_submissions SET {column} = :value WHERE id = :id",
                {"id": values["id"], "value": value},
                "23000",
            )

        prepared_at = connection.execute(
            text("SELECT prepared_at FROM ingress_submissions WHERE id = :id"),
            {"id": values["id"]},
        ).scalar_one()
        submitted_at = prepared_at + timedelta(seconds=1)
        connection.execute(
            text(
                "UPDATE ingress_submissions SET state = 'submitted', submitted_at = :at "
                "WHERE id = :id"
            ),
            {"id": values["id"], "at": submitted_at},
        )
        _assert_ingress_failure(
            connection,
            "UPDATE ingress_submissions SET state = 'confirmed', submitted_at = :changed, "
            "confirmed_at = :confirmed WHERE id = :id",
            {
                "id": values["id"],
                "changed": submitted_at + timedelta(seconds=1),
                "confirmed": submitted_at + timedelta(seconds=2),
            },
            "23000",
        )


@pytest.mark.postgres
def test_ingress_checks_enforce_bundles_codes_and_monotonic_timestamps(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_operation(monkeypatch)
    values = _ingress_values(created, "checks")

    with migrated_database.begin() as connection:
        _insert_ingress(connection, values)
        prepared_at = connection.execute(
            text("SELECT prepared_at FROM ingress_submissions WHERE id = :id"),
            {"id": values["id"]},
        ).scalar_one()

        for statement, parameters in (
            (
                "UPDATE ingress_submissions SET state = 'submitted' WHERE id = :id",
                {"id": values["id"]},
            ),
            (
                "UPDATE ingress_submissions SET state = 'failed', failed_at = :at WHERE id = :id",
                {"id": values["id"], "at": prepared_at + timedelta(seconds=1)},
            ),
            (
                "UPDATE ingress_submissions SET state = 'failed', failed_at = :at, "
                "failure_code = 'INVALID-CODE' WHERE id = :id",
                {"id": values["id"], "at": prepared_at + timedelta(seconds=1)},
            ),
            (
                "UPDATE ingress_submissions SET state = 'submitted', submitted_at = :at "
                "WHERE id = :id",
                {"id": values["id"], "at": prepared_at - timedelta(seconds=1)},
            ),
        ):
            _assert_ingress_failure(connection, statement, parameters, "23514")

        submitted_at = prepared_at + timedelta(seconds=1)
        connection.execute(
            text(
                "UPDATE ingress_submissions SET state = 'submitted', submitted_at = :at "
                "WHERE id = :id"
            ),
            {"id": values["id"], "at": submitted_at},
        )
        for statement in (
            "UPDATE ingress_submissions SET state = 'confirmed', confirmed_at = :at WHERE id = :id",
            "UPDATE ingress_submissions SET state = 'failed', failed_at = :at, "
            "failure_code = 'caddy_apply_failed' WHERE id = :id",
        ):
            _assert_ingress_failure(
                connection,
                statement,
                {"id": values["id"], "at": submitted_at - timedelta(seconds=1)},
                "23514",
            )


@pytest.mark.postgres
def test_ingress_rows_reject_update_or_delete_after_terminal_and_all_deletes(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations = {
        state: _create_operation(monkeypatch, f"immutable-{state}")
        for state in ("prepared", "submitted", "confirmed", "failed")
    }

    with migrated_database.begin() as connection:
        rows = {state: _ingress_values(created, state) for state, created in operations.items()}
        for values in rows.values():
            _insert_ingress(connection, values)
        prepared_at = {
            state: connection.execute(
                text("SELECT prepared_at FROM ingress_submissions WHERE id = :id"),
                {"id": values["id"]},
            ).scalar_one()
            for state, values in rows.items()
        }
        for state in ("submitted", "confirmed"):
            connection.execute(
                text(
                    "UPDATE ingress_submissions SET state = 'submitted', submitted_at = :at "
                    "WHERE id = :id"
                ),
                {"id": rows[state]["id"], "at": prepared_at[state] + timedelta(seconds=1)},
            )
        connection.execute(
            text(
                "UPDATE ingress_submissions SET state = 'confirmed', confirmed_at = :at "
                "WHERE id = :id"
            ),
            {"id": rows["confirmed"]["id"], "at": prepared_at["confirmed"] + timedelta(seconds=2)},
        )
        connection.execute(
            text(
                "UPDATE ingress_submissions SET state = 'failed', failed_at = :at, "
                "failure_code = 'caddy_prepare_failed' WHERE id = :id"
            ),
            {"id": rows["failed"]["id"], "at": prepared_at["failed"] + timedelta(seconds=1)},
        )

        for state, values in rows.items():
            _assert_ingress_failure(
                connection,
                "DELETE FROM ingress_submissions WHERE id = :id",
                {"id": values["id"]},
                "23000",
            )
            if state in {"confirmed", "failed"}:
                _assert_ingress_failure(
                    connection,
                    "UPDATE ingress_submissions SET failure_code = failure_code WHERE id = :id",
                    {"id": values["id"]},
                    "23000",
                )


@pytest.mark.postgres
def test_upgrade_from_released_lifecycle_revision_applies_temporal_cutover(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261004_0002")

    legacy_inspector = inspect(migrated_database)
    legacy_tables = set(legacy_inspector.get_table_names())
    legacy_columns = {
        column["name"] for column in legacy_inspector.get_columns("provisioning_operations")
    }
    assert "runtime_submissions" not in legacy_tables
    assert {"attempt_count", "current_attempt_id", "last_error_retryable"} <= legacy_columns

    created = _create_operation(monkeypatch)
    command.upgrade(configuration, "head")

    inspector = inspect(migrated_database)
    tables = set(inspector.get_table_names())
    operation_columns = {
        column["name"] for column in inspector.get_columns("provisioning_operations")
    }
    assert "runtime_submissions" in tables
    assert "workflow_dispatches" in tables
    assert {"attempt_count", "current_attempt_id", "last_error_retryable"}.isdisjoint(
        operation_columns
    )
    with migrated_database.connect() as connection:
        assert (
            connection.execute(
                text(
                    "SELECT workflow_id FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == f"provisioning/{created['operation_id']}"
        )


@pytest.mark.postgres
def test_temporal_cutover_rejects_nonpristine_legacy_attempt_history(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261004_0002")
    created = _create_operation(monkeypatch)

    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                WITH boundary AS (SELECT clock_timestamp() AS now)
                UPDATE provisioning_operations
                SET state = 'running', attempt_count = 1, current_attempt_id = :attempt_id,
                    started_at = boundary.now, updated_at = boundary.now
                FROM boundary WHERE id = :operation_id
                """
            ),
            {"attempt_id": uuid4(), "operation_id": created["operation_id"]},
        )

    with pytest.raises(DBAPIError) as exc_info:
        command.upgrade(configuration, "head")
    assert getattr(exc_info.value.orig, "sqlstate", None) == "55000"

    inspector = inspect(migrated_database)
    assert "runtime_submissions" not in inspector.get_table_names()
    operation_columns = {
        column["name"] for column in inspector.get_columns("provisioning_operations")
    }
    assert {"attempt_count", "current_attempt_id", "last_error_retryable"} <= operation_columns
    with migrated_database.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
            "20261004_0002"
        )


@pytest.mark.postgres
def test_temporal_cutover_downgrades_with_only_pristine_v1_pending_dispatch_intents(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261006_0003")
    created = _create_operation(monkeypatch)
    command.upgrade(configuration, "head")

    command.downgrade(configuration, "20261004_0002")

    inspector = inspect(migrated_database)
    assert "workflow_dispatches" not in inspector.get_table_names()
    with migrated_database.connect() as connection:
        row = (
            connection.execute(
                text(
                    "SELECT state, attempt_count, current_attempt_id "
                    "FROM provisioning_operations WHERE id = :operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .one()
        )
    assert row == {"state": "pending", "attempt_count": 0, "current_attempt_id": None}


@pytest.mark.postgres
@pytest.mark.parametrize("history_kind", ["dispatched", "runtime_submission"])
def test_temporal_cutover_downgrade_rejects_actual_temporal_history(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    history_kind: str,
) -> None:
    configuration = Config("alembic.ini")
    created = _create_operation(monkeypatch)
    with migrated_database.begin() as connection:
        if history_kind == "dispatched":
            connection.execute(
                text(
                    "UPDATE workflow_dispatches SET state = 'dispatched', "
                    "dispatched_at = clock_timestamp() WHERE operation_id = :operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
        else:
            connection.execute(
                text(
                    """
                    INSERT INTO runtime_submissions (
                        id, organization_id, operation_id, deployment_id, provider,
                        generation, submission_key, spec_fingerprint,
                        api_container_name, migration_container_name, state
                    ) VALUES (
                        :id, :organization_id, :operation_id, :deployment_id,
                        'local_docker', 1, :submission_key, :spec_fingerprint,
                        :api_container_name, :migration_container_name, 'prepared'
                    )
                    """
                ),
                {
                    "id": uuid4(),
                    "organization_id": created["organization_id"],
                    "operation_id": created["operation_id"],
                    "deployment_id": created["deployment_id"],
                    "submission_key": "b" * 64,
                    "spec_fingerprint": "c" * 64,
                    "api_container_name": "fstack-crusher-"
                    + created["deployment_id"].replace("-", ""),
                    "migration_container_name": "fstack-crusher-migrate-"
                    + created["deployment_id"].replace("-", ""),
                },
            )

    with pytest.raises(DBAPIError) as exc_info:
        command.downgrade(configuration, "20261004_0002")
    assert getattr(exc_info.value.orig, "sqlstate", None) == "55000"


def test_released_lifecycle_revision_is_byte_identical() -> None:
    migration_path = Path("migrations/versions/20261004_0002_operation_lifecycle_foundations.py")
    assert hashlib.sha256(migration_path.read_bytes()).hexdigest() == (
        "0f7309e4d4043d011fd29be85e8bb69f9e6a76264577b0626c177b559fffcb83"
    )


def test_released_initial_revision_is_byte_identical() -> None:
    migration_path = Path("migrations/versions/20261003_0001_initial_control_plane.py")
    assert hashlib.sha256(migration_path.read_bytes()).hexdigest() == (
        "396d5f39afc42fe98ecd29895f9f3bbef94c3a1fb2d92cafb835c9c1d0728cf3"
    )


def test_released_temporal_revision_is_byte_identical() -> None:
    migration_path = Path("migrations/versions/20261006_0003_workflow_dispatch_outbox.py")
    assert hashlib.sha256(migration_path.read_bytes()).hexdigest() == (
        "8018ff8db638820a81bdd8507e183cfcb54ef4bc9bf93a899db9080173ec1f2b"
    )


def test_temporal_migration_uses_fail_fast_complete_lock_sets() -> None:
    migration_path = Path("migrations/versions/20261006_0003_workflow_dispatch_outbox.py")
    namespace = runpy.run_path(str(migration_path))

    assert namespace["UPGRADE_LOCK_SQL"] == (
        "LOCK TABLE organizations, businesses, environments, workloads, deployments, "
        "database_bindings, storage_bindings, audit_events, provisioning_operations "
        "IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    assert namespace["DOWNGRADE_LOCK_SQL"] == (
        "LOCK TABLE workflow_dispatches, runtime_submissions, organizations, businesses, "
        "environments, workloads, deployments, database_bindings, storage_bindings, "
        "audit_events, provisioning_operations "
        "IN ACCESS EXCLUSIVE MODE NOWAIT"
    )


@pytest.mark.postgres
def test_upgrade_fails_fast_when_requested_graph_writer_is_live(
    migrated_database: Engine,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261004_0002")
    migration_path = Path("migrations/versions/20261006_0003_workflow_dispatch_outbox.py")
    lock_sql = runpy.run_path(str(migration_path))["UPGRADE_LOCK_SQL"]

    with migrated_database.connect() as writer_connection:
        writer_transaction = writer_connection.begin()
        writer_connection.execute(text("LOCK TABLE organizations IN ROW EXCLUSIVE MODE"))

        with migrated_database.connect() as upgrade_connection:
            upgrade_transaction = upgrade_connection.begin()
            with pytest.raises(DBAPIError) as exc_info:
                upgrade_connection.execute(text(lock_sql))
            assert getattr(exc_info.value.orig, "sqlstate", None) == "55P03"
            upgrade_transaction.rollback()

        writer_connection.execute(text("LOCK TABLE provisioning_operations IN ROW EXCLUSIVE MODE"))
        writer_transaction.rollback()


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("held_table", "followup_table"),
    [
        ("runtime_submissions", "audit_events"),
        ("audit_events", "runtime_submissions"),
        ("organizations", "provisioning_operations"),
    ],
    ids=[
        "runtime-then-audit-writer",
        "audit-then-runtime-trigger",
        "organization-then-operation-writer",
    ],
)
def test_downgrade_fails_fast_instead_of_deadlocking_with_live_writer(
    migrated_database: Engine,
    held_table: str,
    followup_table: str,
) -> None:
    migration_path = Path("migrations/versions/20261006_0003_workflow_dispatch_outbox.py")
    lock_sql = runpy.run_path(str(migration_path))["DOWNGRADE_LOCK_SQL"]
    with migrated_database.connect() as writer_connection:
        writer_transaction = writer_connection.begin()
        writer_connection.execute(text(f"LOCK TABLE {held_table} IN ROW EXCLUSIVE MODE"))

        with migrated_database.connect() as downgrade_connection:
            downgrade_transaction = downgrade_connection.begin()
            downgrade_connection.execute(text("SET LOCAL statement_timeout = '2s'"))
            with pytest.raises(DBAPIError) as exc_info:
                downgrade_connection.execute(text(lock_sql))
            assert getattr(exc_info.value.orig, "sqlstate", None) == "55P03"
            downgrade_transaction.rollback()

        writer_connection.execute(text(f"LOCK TABLE {followup_table} IN ROW EXCLUSIVE MODE"))
        writer_transaction.rollback()


@pytest.mark.postgres
def test_operation_insert_must_begin_pending(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_operation(monkeypatch)
    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO provisioning_operations (
                    id, organization_id, organization_slug, idempotency_key,
                    request_fingerprint, state, actor_principal, business_id,
                    environment_id, workload_id, deployment_id, created_at,
                    updated_at, started_at
                )
                SELECT :id, organization_id, organization_slug, :idempotency_key,
                       request_fingerprint, 'running', actor_principal, business_id,
                       environment_id, workload_id, deployment_id, created_at,
                       created_at, created_at
                FROM provisioning_operations WHERE id = :source_id
                """
            ),
            {
                "id": uuid4(),
                "idempotency_key": "illegal-running-insert",
                "source_id": created["operation_id"],
            },
        )


@pytest.mark.postgres
def test_requested_desired_state_is_immutable(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_operation(monkeypatch)
    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text("UPDATE deployments SET crusher_image = :image WHERE id = :deployment_id"),
            {
                "deployment_id": created["deployment_id"],
                "image": "ghcr.io/michaelgold/crusher@sha256:" + "b" * 64,
            },
        )


@pytest.mark.postgres
def test_requested_resource_graph_cannot_be_rebound_within_organization(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_operation(monkeypatch)
    replacement_business_id = uuid4()
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO businesses (id, organization_id, slug, name) "
                "VALUES (:id, :organization_id, 'replacement', 'Replacement')"
            ),
            {
                "id": replacement_business_id,
                "organization_id": created["organization_id"],
            },
        )

    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text("UPDATE environments SET business_id = :business_id WHERE id = :environment_id"),
            {
                "business_id": replacement_business_id,
                "environment_id": created["environment_id"],
            },
        )


@pytest.mark.postgres
def test_terminal_operation_cannot_change_state(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_operation(monkeypatch)
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                WITH boundary AS (SELECT clock_timestamp() AS now)
                UPDATE provisioning_operations
                SET state = 'running', started_at = boundary.now, updated_at = boundary.now
                FROM boundary WHERE id = :operation_id
                """
            ),
            {"operation_id": created["operation_id"]},
        )
        connection.execute(
            text(
                """
                WITH boundary AS (SELECT clock_timestamp() AS now)
                UPDATE provisioning_operations
                SET state = 'succeeded', completed_at = boundary.now,
                    updated_at = boundary.now
                FROM boundary WHERE id = :operation_id
                """
            ),
            {"operation_id": created["operation_id"]},
        )

    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text(
                "UPDATE provisioning_operations SET state = 'failed', "
                "last_error_code = 'worker_internal_error' WHERE id = :operation_id"
            ),
            {"operation_id": created["operation_id"]},
        )


@pytest.mark.postgres
def test_audit_payload_generation_accepts_only_its_persisted_workflow_shapes(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261006_0003")
    v1_cases = (
        ("provisioning.started", {"provider": "local_docker"}),
        ("provisioning.succeeded", {}),
        ("provisioning.failed", {"error_code": "container_exited"}),
    )
    prepared_v1: list[tuple[dict[str, str], dict[str, object], str, dict[str, str]]] = []
    for index, (event_type, extra_payload) in enumerate(v1_cases):
        created = _create_operation(monkeypatch, f"audit-v1-accepted-{index}")
        runtime = _runtime_values(created, f"audit-v1-accepted-{index}")
        with migrated_database.begin() as connection:
            _insert_runtime(connection, runtime)
        prepared_v1.append((created, runtime, event_type, extra_payload))

    command.upgrade(configuration, "head")
    cases: list[tuple[dict[str, str], str, dict[str, str]]] = []
    for created, runtime, event_type, extra_payload in prepared_v1:
        cases.append(
            (
                created,
                event_type,
                {"runtime_submission_id": str(runtime["id"])} | extra_payload,
            )
        )

    v2_cases = (
        ("provisioning.started", {}),
        ("provisioning.succeeded", {}),
        ("provisioning.failed", {"error_code": "caddy_apply_failed"}),
    )
    for index, (event_type, extra_payload) in enumerate(v2_cases):
        created = _create_operation(monkeypatch, f"audit-v2-accepted-{index}")
        _mark_dispatch_v2(migrated_database, created["operation_id"])
        runtime = _runtime_values(created, f"audit-v2-accepted-{index}")
        ingress = _ingress_values(created, f"audit-v2-accepted-{index}")
        with migrated_database.begin() as connection:
            _insert_runtime(connection, runtime)
            _insert_ingress(connection, ingress)
        cases.append(
            (
                created,
                event_type,
                {
                    "runtime_submission_id": str(runtime["id"]),
                    "ingress_submission_id": str(ingress["id"]),
                }
                | extra_payload,
            )
        )

    for created, event_type, payload in cases:
        with migrated_database.begin() as connection:
            _insert_audit(connection, created, event_type, payload)
        with migrated_database.connect() as connection:
            observed = connection.execute(
                text(
                    "SELECT payload FROM audit_events WHERE operation_id=:operation_id "
                    "AND event_type=:event_type"
                ),
                {"operation_id": created["operation_id"], "event_type": event_type},
            ).scalar_one()
        assert observed == payload


@pytest.mark.postgres
def test_audit_payload_generation_rejects_cross_generation_shapes_without_history_change(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261006_0003")
    event_types = (
        "provisioning.started",
        "provisioning.succeeded",
        "provisioning.failed",
    )
    v1_rows: list[tuple[dict[str, str], dict[str, object]]] = []
    for index, _event_type in enumerate(event_types):
        created = _create_operation(monkeypatch, f"audit-v1-cross-{index}")
        runtime = _runtime_values(created, f"audit-v1-cross-{index}")
        with migrated_database.begin() as connection:
            _insert_runtime(connection, runtime)
        v1_rows.append((created, runtime))

    command.upgrade(configuration, "head")
    cases: list[tuple[dict[str, str], str, dict[str, str]]] = []
    for index, (event_type, (created, runtime)) in enumerate(
        zip(event_types, v1_rows, strict=True)
    ):
        ingress = _ingress_values(created, f"audit-v1-cross-{index}")
        with migrated_database.begin() as connection:
            _insert_ingress(connection, ingress)
        cases.append(
            (
                created,
                event_type,
                {
                    "runtime_submission_id": str(runtime["id"]),
                    "ingress_submission_id": str(ingress["id"]),
                }
                | ({"error_code": "caddy_apply_failed"} if event_type.endswith("failed") else {}),
            )
        )

    for index, event_type in enumerate(event_types):
        created = _create_operation(monkeypatch, f"audit-v2-cross-{index}")
        _mark_dispatch_v2(migrated_database, created["operation_id"])
        runtime = _runtime_values(created, f"audit-v2-cross-{index}")
        ingress = _ingress_values(created, f"audit-v2-cross-{index}")
        with migrated_database.begin() as connection:
            _insert_runtime(connection, runtime)
            _insert_ingress(connection, ingress)
        v1_payload = {"runtime_submission_id": str(runtime["id"])}
        if event_type.endswith("started"):
            v1_payload["provider"] = "local_docker"
        elif event_type.endswith("failed"):
            v1_payload["error_code"] = "container_exited"
        cases.append((created, event_type, v1_payload))

    for created, event_type, payload in cases:
        with migrated_database.connect() as connection:
            before = (
                connection.execute(
                    text(
                        "SELECT event_type, payload FROM audit_events "
                        "WHERE operation_id=:operation_id ORDER BY created_at, id"
                    ),
                    {"operation_id": created["operation_id"]},
                )
                .mappings()
                .all()
            )
        with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
            _insert_audit(connection, created, event_type, payload)
        assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"
        assert "workflow type does not permit audit payload" in str(exc_info.value.orig)
        with migrated_database.connect() as connection:
            after = (
                connection.execute(
                    text(
                        "SELECT event_type, payload FROM audit_events "
                        "WHERE operation_id=:operation_id ORDER BY created_at, id"
                    ),
                    {"operation_id": created["operation_id"]},
                )
                .mappings()
                .all()
            )
        assert after == before


@pytest.mark.postgres
@pytest.mark.parametrize("dispatch_corruption", ["missing", "unknown"])
def test_audit_payload_generation_fails_closed_for_missing_or_unknown_workflow_type(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    dispatch_corruption: str,
) -> None:
    created = _create_operation(monkeypatch, f"audit-dispatch-{dispatch_corruption}")
    runtime = _runtime_values(created, f"audit-dispatch-{dispatch_corruption}")
    ingress = _ingress_values(created, f"audit-dispatch-{dispatch_corruption}")
    with migrated_database.begin() as connection:
        _insert_runtime(connection, runtime)
        _insert_ingress(connection, ingress)
        connection.execute(
            text(
                "ALTER TABLE workflow_dispatches "
                "DISABLE TRIGGER tr_workflow_dispatches_lifecycle_guard"
            )
        )
        if dispatch_corruption == "missing":
            connection.execute(
                text("DELETE FROM workflow_dispatches WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            )
        else:
            connection.execute(
                text("ALTER TABLE workflow_dispatches DROP CONSTRAINT ck_workflow_dispatches_type")
            )
            connection.execute(
                text(
                    "UPDATE workflow_dispatches "
                    "SET workflow_type='fstack.provision_crusher.unknown' "
                    "WHERE operation_id=:operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
        connection.execute(
            text(
                "ALTER TABLE workflow_dispatches "
                "ENABLE TRIGGER tr_workflow_dispatches_lifecycle_guard"
            )
        )

    with migrated_database.connect() as connection:
        before = (
            connection.execute(
                text(
                    "SELECT event_type, payload FROM audit_events WHERE operation_id=:operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .all()
        )
    with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
        _insert_audit(
            connection,
            created,
            "provisioning.started",
            {
                "runtime_submission_id": str(runtime["id"]),
                "ingress_submission_id": str(ingress["id"]),
            },
        )
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"
    assert "workflow dispatch is missing or unsupported" in str(exc_info.value.orig)
    with migrated_database.connect() as connection:
        after = (
            connection.execute(
                text(
                    "SELECT event_type, payload FROM audit_events WHERE operation_id=:operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .all()
        )
    assert after == before


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("event_type", "payload_factory"),
    [
        (
            "provisioning.started",
            lambda runtime_id, ingress_id: {"runtime_submission_id": runtime_id},
        ),
        (
            "provisioning.started",
            lambda runtime_id, ingress_id: {
                "runtime_submission_id": runtime_id,
                "ingress_submission_id": ingress_id,
                "provider": "local_docker",
            },
        ),
        (
            "provisioning.started",
            lambda runtime_id, ingress_id: {
                "runtime_submission_id": runtime_id,
                "provider": "local_docker",
                "extra": "forbidden",
            },
        ),
        (
            "provisioning.started",
            lambda runtime_id, ingress_id: {
                "runtime_submission_id": runtime_id.upper(),
                "provider": "local_docker",
            },
        ),
        (
            "provisioning.succeeded",
            lambda runtime_id, ingress_id: {
                "runtime_submission_id": runtime_id,
                "provider": "local_docker",
            },
        ),
        (
            "provisioning.failed",
            lambda runtime_id, ingress_id: {
                "runtime_submission_id": runtime_id,
                "ingress_submission_id": ingress_id,
                "provider": "local_docker",
                "error_code": "caddy_apply_failed",
            },
        ),
    ],
)
def test_audit_payload_closed_union_rejects_mixed_noncanonical_and_superset_shapes(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    event_type: str,
    payload_factory: Callable[[str, str], dict[str, str]],
) -> None:
    created = _create_operation(monkeypatch, "audit-rejected-" + uuid4().hex[:8])
    runtime = _runtime_values(created, uuid4().hex)
    ingress = _ingress_values(created, uuid4().hex)
    payload = payload_factory(str(runtime["id"]), str(ingress["id"]))

    with migrated_database.begin() as connection:
        _insert_runtime(connection, runtime)
        _insert_ingress(connection, ingress)
    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        _insert_audit(connection, created, event_type, payload)


@pytest.mark.postgres
def test_audit_and_ingress_bindings_reject_other_operation_or_organization(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _create_operation(monkeypatch, "binding-first")
    second = _create_operation(monkeypatch, "binding-second")
    runtime = _runtime_values(first, "binding-first")
    ingress = _ingress_values(first, "binding-first")
    with migrated_database.begin() as connection:
        _insert_runtime(connection, runtime)
        _insert_ingress(connection, ingress)

    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        _insert_audit(
            connection,
            first | {"organization_id": second["organization_id"]},
            "provisioning.started",
            {
                "runtime_submission_id": str(runtime["id"]),
                "ingress_submission_id": str(ingress["id"]),
            },
        )

    other_operation_id = uuid4()
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO provisioning_operations (
                    id, organization_id, organization_slug, idempotency_key,
                    request_fingerprint, state, actor_principal, business_id,
                    environment_id, workload_id, deployment_id, created_at, updated_at
                )
                SELECT :id, organization_id, organization_slug, :idempotency_key,
                       request_fingerprint, 'pending', actor_principal, business_id,
                       environment_id, workload_id, deployment_id, clock_timestamp(),
                       clock_timestamp()
                FROM provisioning_operations WHERE id=:source_id
                """
            ),
            {
                "id": other_operation_id,
                "idempotency_key": "same-organization-other-operation",
                "source_id": first["operation_id"],
            },
        )
    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        _insert_audit(
            connection,
            first | {"operation_id": str(other_operation_id)},
            "provisioning.started",
            {
                "runtime_submission_id": str(runtime["id"]),
                "ingress_submission_id": str(ingress["id"]),
            },
        )

    cross_organization_ingress = _ingress_values(first, "cross-organization") | {
        "id": uuid4(),
        "organization_id": second["organization_id"],
    }
    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        _insert_ingress(connection, cross_organization_ingress)


@pytest.mark.postgres
def test_local_caddy_upgrade_preserves_released_v1_audit_rows(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261006_0003")
    created = _create_operation(monkeypatch, "preserved-v1-audit")
    runtime = _runtime_values(created, "preserved-v1-audit")
    payload = {
        "runtime_submission_id": str(runtime["id"]),
        "provider": "local_docker",
    }
    with migrated_database.begin() as connection:
        _insert_runtime(connection, runtime)
        _insert_audit(connection, created, "provisioning.started", payload)

    command.upgrade(configuration, "head")

    with migrated_database.connect() as connection:
        observed = connection.execute(
            text(
                "SELECT payload FROM audit_events WHERE operation_id=:operation_id "
                "AND event_type='provisioning.started'"
            ),
            {"operation_id": created["operation_id"]},
        ).scalar_one()
    assert observed == payload


@pytest.mark.postgres
def test_audit_payload_rejects_unknown_or_secret_bearing_keys(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_operation(monkeypatch)
    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO audit_events (
                    id, organization_id, operation_id, event_type, actor_principal, payload
                ) VALUES (
                    :id, :organization_id, :operation_id, 'provisioning.started',
                    'installation-operator', CAST(:payload AS jsonb)
                )
                """
            ),
            {
                "id": uuid4(),
                "organization_id": created["organization_id"],
                "operation_id": created["operation_id"],
                "payload": (
                    '{"runtime_submission_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",'
                    '"provider":"local_docker","password":"not-allowed"}'
                ),
            },
        )


@pytest.mark.postgres
def test_audit_history_is_immutable(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_operation(monkeypatch)
    for statement in (
        "UPDATE audit_events SET actor_principal = 'other' WHERE operation_id = :operation_id",
        "DELETE FROM audit_events WHERE operation_id = :operation_id",
    ):
        with pytest.raises(IntegrityError), migrated_database.begin() as connection:
            connection.execute(text(statement), {"operation_id": created["operation_id"]})


@pytest.mark.postgres
def test_workflow_dispatch_history_is_immutable_after_delivery(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_operation(monkeypatch)
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE workflow_dispatches
                SET state = 'dispatched', dispatched_at = clock_timestamp()
                WHERE operation_id = :operation_id
                """
            ),
            {"operation_id": created["operation_id"]},
        )

    for statement in (
        "UPDATE workflow_dispatches SET dispatched_at = clock_timestamp() "
        "WHERE operation_id = :operation_id",
        "DELETE FROM workflow_dispatches WHERE operation_id = :operation_id",
    ):
        with pytest.raises(IntegrityError), migrated_database.begin() as connection:
            connection.execute(text(statement), {"operation_id": created["operation_id"]})


@pytest.mark.postgres
def test_operation_lifecycle_upgrade_backfills_existing_created_time(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261003_0001")
    created = _create_operation(monkeypatch)
    with migrated_database.connect() as connection:
        created_at = connection.execute(
            text("SELECT created_at FROM provisioning_operations WHERE id = :operation_id"),
            {"operation_id": created["operation_id"]},
        ).scalar_one()

    command.upgrade(configuration, "head")

    with migrated_database.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT created_at, updated_at, started_at, completed_at, last_error_code
                FROM provisioning_operations WHERE id = :operation_id
                """
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .one()
        )
    assert row == {
        "created_at": created_at,
        "updated_at": created_at,
        "started_at": None,
        "completed_at": None,
        "last_error_code": None,
    }
    with migrated_database.connect() as connection:
        dispatch = (
            connection.execute(
                text(
                    "SELECT workflow_id, workflow_type, state, created_at, dispatched_at "
                    "FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .one()
        )
    assert dispatch == {
        "workflow_id": f"provisioning/{created['operation_id']}",
        "workflow_type": "fstack.provision_crusher.v1",
        "state": "pending",
        "created_at": created_at,
        "dispatched_at": None,
    }
