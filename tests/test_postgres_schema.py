from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import IntegrityError

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


@pytest.mark.postgres
def test_initial_migration_creates_provisioning_authority_tables(
    migrated_database: Engine,
) -> None:
    tables = set(inspect(migrated_database).get_table_names())

    assert {
        "organizations",
        "businesses",
        "environments",
        "workloads",
        "deployments",
        "database_bindings",
        "storage_bindings",
        "provisioning_operations",
        "audit_events",
    } <= tables


@pytest.mark.postgres
def test_retry_pending_bundle_requires_evidence_of_a_started_attempt(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    created = TestClient(create_app()).post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "create-acme-mailer",
        },
        json=VALID_BODY,
    )

    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE provisioning_operations
                SET last_error_code = 'worker_internal_error',
                    last_error_retryable = true
                WHERE id = :operation_id
                """
            ),
            {"operation_id": created.json()["operation_id"]},
        )


@pytest.mark.postgres
def test_initial_pending_bundle_rejects_started_time_without_retry_error(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    created = TestClient(create_app()).post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "create-acme-mailer",
        },
        json=VALID_BODY,
    )

    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE provisioning_operations
                SET started_at = created_at
                WHERE id = :operation_id
                """
            ),
            {"operation_id": created.json()["operation_id"]},
        )


@pytest.mark.postgres
def test_updated_at_cannot_precede_started_at(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    created = TestClient(create_app()).post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "create-acme-mailer",
        },
        json=VALID_BODY,
    )

    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE provisioning_operations
                SET state = 'running',
                    started_at = created_at + interval '1 second',
                    current_attempt_id = :attempt_id,
                    attempt_count = 1
                WHERE id = :operation_id
                """
            ),
            {
                "attempt_id": uuid4(),
                "operation_id": created.json()["operation_id"],
            },
        )


@pytest.mark.postgres
def test_operation_lifecycle_migration_has_exact_foundation_shape(
    migrated_database: Engine,
) -> None:
    inspector = inspect(migrated_database)
    columns = {
        column["name"]: column for column in inspector.get_columns("provisioning_operations")
    }

    assert columns["updated_at"]["nullable"] is False
    assert columns["updated_at"]["type"].timezone is True
    assert columns["started_at"]["nullable"] is True
    assert columns["started_at"]["type"].timezone is True
    assert columns["completed_at"]["nullable"] is True
    assert columns["completed_at"]["type"].timezone is True
    assert columns["attempt_count"]["nullable"] is False
    assert str(columns["attempt_count"]["type"]) == "BIGINT"
    assert columns["attempt_count"]["default"] == "0"
    assert columns["current_attempt_id"]["nullable"] is True
    assert str(columns["current_attempt_id"]["type"]) == "UUID"
    assert columns["last_error_code"]["nullable"] is True
    assert columns["last_error_code"]["type"].length == 64
    assert columns["last_error_code"]["type"].collation == "C"
    assert columns["last_error_retryable"]["nullable"] is True

    unique_constraints = {
        constraint["name"]: tuple(constraint["column_names"])
        for constraint in inspector.get_unique_constraints("provisioning_operations")
    }
    assert unique_constraints["uq_operations_org_id_deployment"] == (
        "organization_id",
        "id",
        "deployment_id",
    )
    foreign_keys = inspector.get_foreign_keys("provisioning_operations")
    assert all(
        "current_attempt_id" not in foreign_key["constrained_columns"]
        for foreign_key in foreign_keys
    )

    operation_checks = {
        check["name"] for check in inspector.get_check_constraints("provisioning_operations")
    }
    assert "ck_operations_initial_pending" not in operation_checks
    assert {
        "ck_operations_lifecycle_bundle",
        "ck_operations_attempt_count_nonnegative",
        "ck_operations_error_code_sanitized",
        "ck_operations_timestamps_monotonic",
    } <= operation_checks
    audit_checks = {check["name"] for check in inspector.get_check_constraints("audit_events")}
    assert "ck_audit_initial_requested" not in audit_checks
    assert "ck_audit_provisioning_lifecycle_event" in audit_checks


@pytest.mark.postgres
def test_audit_check_accepts_only_provisioning_lifecycle_events(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    created = (
        TestClient(create_app())
        .post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": "Bearer test-operator-token",
                "Idempotency-Key": "create-acme-mailer",
            },
            json=VALID_BODY,
        )
        .json()
    )
    with migrated_database.begin() as connection:
        for event_type in (
            "provisioning.started",
            "provisioning.succeeded",
            "provisioning.failed",
        ):
            connection.execute(
                text(
                    """
                    INSERT INTO audit_events (
                        id, organization_id, operation_id, event_type, actor_principal
                    ) VALUES (
                        :id, :organization_id, :operation_id, :event_type,
                        'installation-operator'
                    )
                    """
                ),
                {
                    "id": uuid4(),
                    "organization_id": created["organization_id"],
                    "operation_id": created["operation_id"],
                    "event_type": event_type,
                },
            )

    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO audit_events (
                    id, organization_id, operation_id, event_type, actor_principal
                ) VALUES (
                    :id, :organization_id, :operation_id, 'provisioning.raw_error',
                    'installation-operator'
                )
                """
            ),
            {
                "id": uuid4(),
                "organization_id": created["organization_id"],
                "operation_id": created["operation_id"],
            },
        )


@pytest.mark.postgres
def test_operation_lifecycle_upgrade_backfills_existing_created_time(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261003_0001")
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    created = TestClient(create_app()).post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "create-before-lifecycle-upgrade",
        },
        json=VALID_BODY,
    )
    operation_id = created.json()["operation_id"]
    with migrated_database.connect() as connection:
        created_at = connection.execute(
            text("SELECT created_at FROM provisioning_operations WHERE id = :operation_id"),
            {"operation_id": operation_id},
        ).scalar_one()

    command.upgrade(configuration, "head")

    with migrated_database.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT created_at, updated_at, started_at, completed_at,
                       attempt_count, current_attempt_id,
                       last_error_code, last_error_retryable
                FROM provisioning_operations
                WHERE id = :operation_id
                """
                ),
                {"operation_id": operation_id},
            )
            .mappings()
            .one()
        )
    assert row == {
        "created_at": created_at,
        "updated_at": created_at,
        "started_at": None,
        "completed_at": None,
        "attempt_count": 0,
        "current_attempt_id": None,
        "last_error_code": None,
        "last_error_retryable": None,
    }
