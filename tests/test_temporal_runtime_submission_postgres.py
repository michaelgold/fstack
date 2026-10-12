from __future__ import annotations

from uuid import UUID

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


def _create_prepared_runtime(monkeypatch: pytest.MonkeyPatch, *, idempotency_key: str):
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
                "Idempotency-Key": idempotency_key,
            },
            json=VALID_BODY,
        )
        .json()
    )
    return created, prepare_operation(created["operation_id"])


def _runtime_snapshot(engine: Engine, submission_id: UUID) -> tuple[dict, int]:
    with engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    "SELECT submission.*, operation.state AS operation_state, "
                    "operation.started_at AS operation_started_at, "
                    "operation.completed_at AS operation_completed_at, "
                    "operation.updated_at AS operation_updated_at, "
                    "operation.last_error_code AS operation_last_error_code "
                    "FROM runtime_submissions submission "
                    "JOIN provisioning_operations operation "
                    "ON operation.id=submission.operation_id "
                    "WHERE submission.id=:submission_id"
                ),
                {"submission_id": submission_id},
            )
            .mappings()
            .one()
        )
        audit_count = connection.execute(
            text(
                "SELECT count(*) FROM audit_events WHERE operation_id="
                "(SELECT operation_id FROM runtime_submissions WHERE id=:submission_id)"
            ),
            {"submission_id": submission_id},
        ).scalar_one()
    return dict(row), audit_count


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

    assert operation["state"] == "pending"
    assert operation["started_at"] is None
    assert operation["completed_at"] is None
    assert operation["last_error_code"] is None
    assert len(submissions) == 1
    assert submissions[0]["id"] == first.submission_id
    assert [event["event_type"] for event in events] == ["provisioning.requested"]


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


@pytest.mark.postgres
def test_runtime_success_transitions_and_replays_atomically(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_state import finalize_success, mark_running, persist_submitted
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
                "Idempotency-Key": "success-acme-mailer",
            },
            json=VALID_BODY,
        )
        .json()
    )
    prepared = prepare_operation(created["operation_id"])

    mark_running(prepared.submission_id, prepared.operation_id)
    mark_running(prepared.submission_id, prepared.operation_id)
    persist_submitted(prepared.submission_id, "docker-api-123")
    persist_submitted(prepared.submission_id, "docker-api-123")
    confirmed = finalize_success(prepared.submission_id, prepared.operation_id)
    replay = finalize_success(prepared.submission_id, prepared.operation_id)

    assert confirmed.state == replay.state == "confirmed"
    with migrated_database.connect() as connection:
        operation = (
            connection.execute(
                text("SELECT state, last_error_code FROM provisioning_operations WHERE id=:id"),
                {"id": created["operation_id"]},
            )
            .mappings()
            .one()
        )
        submission = (
            connection.execute(
                text(
                    "SELECT state, external_id, failure_code FROM runtime_submissions WHERE id=:id"
                ),
                {"id": prepared.submission_id},
            )
            .mappings()
            .one()
        )
        events = (
            connection.execute(
                text(
                    "SELECT event_type FROM audit_events WHERE operation_id=:id ORDER BY created_at"
                ),
                {"id": created["operation_id"]},
            )
            .scalars()
            .all()
        )
    assert operation == {"state": "succeeded", "last_error_code": None}
    assert submission == {
        "state": "confirmed",
        "external_id": "docker-api-123",
        "failure_code": None,
    }
    assert events == ["provisioning.requested", "provisioning.started", "provisioning.succeeded"]


@pytest.mark.postgres
def test_runtime_failure_preserves_submitted_identity_and_replays(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_state import finalize_failure, mark_running, persist_submitted
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
                "Idempotency-Key": "failure-acme-mailer",
            },
            json=VALID_BODY,
        )
        .json()
    )
    prepared = prepare_operation(created["operation_id"])
    mark_running(prepared.submission_id, prepared.operation_id)
    persist_submitted(prepared.submission_id, "docker-api-456")

    failed = finalize_failure(prepared.submission_id, prepared.operation_id, "container_exited")
    replay = finalize_failure(prepared.submission_id, prepared.operation_id, "container_exited")

    assert failed.state == replay.state == "failed"
    with migrated_database.connect() as connection:
        row = (
            connection.execute(
                text(
                    "SELECT submission.state, submission.external_id, submission.failure_code, "
                    "operation.state AS operation_state, operation.last_error_code "
                    "FROM runtime_submissions submission JOIN provisioning_operations operation "
                    "ON operation.id=submission.operation_id WHERE submission.id=:id"
                ),
                {"id": prepared.submission_id},
            )
            .mappings()
            .one()
        )
        events = (
            connection.execute(
                text(
                    "SELECT event_type FROM audit_events WHERE operation_id=:id ORDER BY created_at"
                ),
                {"id": created["operation_id"]},
            )
            .scalars()
            .all()
        )
    assert row == {
        "state": "failed",
        "external_id": "docker-api-456",
        "failure_code": "container_exited",
        "operation_state": "failed",
        "last_error_code": "container_exited",
    }
    assert events == ["provisioning.requested", "provisioning.started", "provisioning.failed"]


@pytest.mark.postgres
def test_mark_running_rejects_mismatched_operation_without_mutation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_state import RuntimeStateError, mark_running

    _, prepared = _create_prepared_runtime(
        monkeypatch, idempotency_key="mismatched-apply-operation"
    )
    before = _runtime_snapshot(migrated_database, prepared.submission_id)

    with pytest.raises(RuntimeStateError, match="^runtime_submission_operation_mismatch$"):
        mark_running(
            prepared.submission_id,
            UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        )

    assert _runtime_snapshot(migrated_database, prepared.submission_id) == before


@pytest.mark.postgres
def test_finalize_success_rejects_mismatched_operation_without_mutation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_state import (
        RuntimeStateError,
        finalize_success,
        mark_running,
        persist_submitted,
    )

    _, prepared = _create_prepared_runtime(
        monkeypatch, idempotency_key="mismatched-success-operation"
    )
    mark_running(prepared.submission_id, prepared.operation_id)
    persist_submitted(prepared.submission_id, "docker-api-mismatch-success")
    before = _runtime_snapshot(migrated_database, prepared.submission_id)

    with pytest.raises(RuntimeStateError, match="^runtime_submission_operation_mismatch$"):
        finalize_success(
            prepared.submission_id,
            UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        )

    assert _runtime_snapshot(migrated_database, prepared.submission_id) == before


@pytest.mark.postgres
def test_finalize_failure_rejects_mismatched_operation_without_mutation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_state import (
        RuntimeStateError,
        finalize_failure,
        mark_running,
        persist_submitted,
    )

    _, prepared = _create_prepared_runtime(
        monkeypatch, idempotency_key="mismatched-failure-operation"
    )
    mark_running(prepared.submission_id, prepared.operation_id)
    persist_submitted(prepared.submission_id, "docker-api-mismatch-failure")
    before = _runtime_snapshot(migrated_database, prepared.submission_id)

    with pytest.raises(RuntimeStateError, match="^runtime_submission_operation_mismatch$"):
        finalize_failure(
            prepared.submission_id,
            UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            "container_exited",
        )

    assert _runtime_snapshot(migrated_database, prepared.submission_id) == before


@pytest.mark.postgres
def test_ambiguous_prepare_config_drift_is_finalized_once(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_state import finalize_prepare_failure
    from fstack.runtime_submission import OperationNotClaimableError, prepare_operation

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = (
        TestClient(create_app())
        .post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": "Bearer test-operator-token",
                "Idempotency-Key": "ambiguous-prepare-acme-mailer",
            },
            json=VALID_BODY,
        )
        .json()
    )
    prepared = prepare_operation(created["operation_id"])
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-drifted")

    with pytest.raises(OperationNotClaimableError):
        prepare_operation(created["operation_id"])

    failed = finalize_prepare_failure(created["operation_id"], "prepare_not_claimable")
    replay = finalize_prepare_failure(created["operation_id"], "prepare_not_claimable")

    assert failed == replay
    assert failed.submission_id == prepared.submission_id
    assert failed.state == "failed"
    with migrated_database.connect() as connection:
        row = (
            connection.execute(
                text(
                    "SELECT submission.state, submission.failure_code, operation.state AS "
                    "operation_state, operation.last_error_code "
                    "FROM runtime_submissions submission "
                    "JOIN provisioning_operations operation "
                    "ON operation.id=submission.operation_id "
                    "WHERE submission.id=:submission_id"
                ),
                {"submission_id": prepared.submission_id},
            )
            .mappings()
            .one()
        )
        failed_events = connection.execute(
            text(
                "SELECT count(*) FROM audit_events WHERE operation_id=:operation_id "
                "AND event_type='provisioning.failed'"
            ),
            {"operation_id": created["operation_id"]},
        ).scalar_one()
    assert row == {
        "state": "failed",
        "failure_code": "prepare_not_claimable",
        "operation_state": "failed",
        "last_error_code": "prepare_not_claimable",
    }
    assert failed_events == 1


@pytest.mark.postgres
def test_prepare_failure_finalizer_does_not_fabricate_submission(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_state import RuntimeStateError, finalize_prepare_failure

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    created = (
        TestClient(create_app())
        .post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": "Bearer test-operator-token",
                "Idempotency-Key": "unprepared-failure-acme-mailer",
            },
            json=VALID_BODY,
        )
        .json()
    )

    with pytest.raises(RuntimeStateError, match="runtime_submission_not_found"):
        finalize_prepare_failure(created["operation_id"], "prepare_not_claimable")

    with migrated_database.connect() as connection:
        operation_state = connection.execute(
            text("SELECT state FROM provisioning_operations WHERE id=:operation_id"),
            {"operation_id": created["operation_id"]},
        ).scalar_one()
        submission_count = connection.execute(
            text("SELECT count(*) FROM runtime_submissions WHERE operation_id=:operation_id"),
            {"operation_id": created["operation_id"]},
        ).scalar_one()
    assert operation_state == "pending"
    assert submission_count == 0
