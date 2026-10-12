from __future__ import annotations

import hashlib
import runpy
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import Engine, inspect, text
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


def _create_operation(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    response = TestClient(create_app()).post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "create-acme-mailer",
        },
        json=VALID_BODY,
    )
    assert response.status_code == 202
    return response.json()


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
def test_temporal_cutover_downgrades_with_only_pristine_pending_dispatch_intents(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")
    created = _create_operation(monkeypatch)

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
