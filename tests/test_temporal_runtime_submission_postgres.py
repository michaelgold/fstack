from __future__ import annotations

import pytest
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
def test_temporal_is_the_only_attempt_authority_and_runtime_submission_is_minimal(
    migrated_database: Engine,
) -> None:
    inspector = inspect(migrated_database)
    tables = set(inspector.get_table_names())

    assert "provisioning_attempts" not in tables
    assert "crusher_docker_effects" not in tables
    assert "runtime_submissions" in tables

    operation_columns = {
        column["name"] for column in inspector.get_columns("provisioning_operations")
    }
    assert "attempt_count" not in operation_columns
    assert "current_attempt_id" not in operation_columns
    assert "last_error_retryable" not in operation_columns

    submission_columns = {column["name"] for column in inspector.get_columns("runtime_submissions")}
    assert submission_columns == {
        "id",
        "organization_id",
        "operation_id",
        "deployment_id",
        "provider",
        "generation",
        "submission_key",
        "spec_fingerprint",
        "api_container_name",
        "migration_container_name",
        "state",
        "external_id",
        "failure_code",
        "prepared_at",
        "submitted_at",
        "confirmed_at",
        "failed_at",
    }


@pytest.mark.postgres
def test_prepare_operation_creates_one_replayable_submission_without_attempts(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_submission import prepare_operation

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
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

    first = prepare_operation(created["operation_id"])
    replay = prepare_operation(created["operation_id"])

    assert replay == first
    assert str(first.operation_id) == created["operation_id"]
    assert first.state == "prepared"
    assert first.provider == "local_docker"
    assert first.generation == 1

    with migrated_database.connect() as connection:
        operation = (
            connection.execute(
                text(
                    "SELECT state, started_at, completed_at, last_error_code "
                    "FROM provisioning_operations WHERE id = :operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .one()
        )
        submissions = (
            connection.execute(
                text(
                    "SELECT id, state, provider, generation FROM runtime_submissions "
                    "WHERE operation_id = :operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .all()
        )
        events = (
            connection.execute(
                text(
                    "SELECT event_type, payload FROM audit_events "
                    "WHERE operation_id = :operation_id ORDER BY created_at, event_type"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .all()
        )

    assert operation["state"] == "running"
    assert operation["started_at"] is not None
    assert operation["completed_at"] is None
    assert operation["last_error_code"] is None
    assert len(submissions) == 1
    assert submissions[0]["id"] == first.submission_id
    assert [event["event_type"] for event in events] == [
        "provisioning.requested",
        "provisioning.started",
    ]
    assert events[1]["payload"] == {
        "provider": "local_docker",
        "runtime_submission_id": str(first.submission_id),
    }


@pytest.mark.postgres
def test_terminal_audit_event_requires_the_operations_exact_submission(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_submission import prepare_operation

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
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
    prepare_operation(created["operation_id"])

    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO audit_events (
                    id, organization_id, operation_id, event_type, actor_principal, payload
                ) VALUES (
                    gen_random_uuid(), :organization_id, :operation_id,
                    'provisioning.succeeded', 'installation-operator',
                    '{"runtime_submission_id":"not-a-uuid"}'::jsonb
                )
                """
            ),
            {
                "organization_id": created["organization_id"],
                "operation_id": created["operation_id"],
            },
        )
