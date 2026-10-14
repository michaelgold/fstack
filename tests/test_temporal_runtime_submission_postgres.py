from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import Connection, Engine, create_engine, event, inspect, text
from sqlalchemy.exc import IntegrityError, OperationalError

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


class _WrongScalarResult:
    def __init__(self, result: Any, wrong_id: UUID) -> None:
        self._result = result
        self._wrong_id = wrong_id

    def scalar_one_or_none(self) -> UUID:
        assert self._result.scalar_one_or_none() is not None
        return self._wrong_id

    def __getattr__(self, name: str) -> Any:
        return getattr(self._result, name)


class _ReturningIdentityConnection:
    def __init__(self, connection: Any, target_sql: str, wrong_id: UUID) -> None:
        self._connection = connection
        self._target_sql = target_sql
        self._wrong_id = wrong_id
        self.matches = 0

    def execute(self, statement: object, *args: object, **kwargs: object) -> Any:
        result = self._connection.execute(statement, *args, **kwargs)
        normalized = " ".join(str(statement).split())
        if self._target_sql in normalized:
            self.matches += 1
            return _WrongScalarResult(result, self._wrong_id)
        return result


class _ReturningIdentityBegin:
    def __init__(self, begin: Any, engine: _ReturningIdentityEngine) -> None:
        self._begin = begin
        self._engine = engine

    def __enter__(self) -> _ReturningIdentityConnection:
        connection = self._begin.__enter__()
        wrapped = _ReturningIdentityConnection(
            connection,
            self._engine.target_sql,
            self._engine.wrong_id,
        )
        self._engine.connection = wrapped
        return wrapped

    def __exit__(self, *args: object) -> bool | None:
        return self._begin.__exit__(*args)


class _ReturningIdentityEngine:
    def __init__(self, engine: Engine, target_sql: str, wrong_id: UUID) -> None:
        self._engine = engine
        self.target_sql = target_sql
        self.wrong_id = wrong_id
        self.connection: _ReturningIdentityConnection | None = None

    def begin(self) -> _ReturningIdentityBegin:
        return _ReturningIdentityBegin(self._engine.begin(), self)

    def dispose(self) -> None:
        self._engine.dispose()


def _preparation_snapshot(engine: Engine, operation_id: UUID | str) -> tuple[object, ...]:
    with engine.connect() as connection:
        operation = connection.execute(
            text("SELECT state, started_at FROM provisioning_operations WHERE id=:operation_id"),
            {"operation_id": operation_id},
        ).one()
        runtime_count = connection.execute(
            text("SELECT count(*) FROM runtime_submissions WHERE operation_id=:operation_id"),
            {"operation_id": operation_id},
        ).scalar_one()
        ingress_count = connection.execute(
            text("SELECT count(*) FROM ingress_submissions WHERE operation_id=:operation_id"),
            {"operation_id": operation_id},
        ).scalar_one()
        audit_count = connection.execute(
            text("SELECT count(*) FROM audit_events WHERE operation_id=:operation_id"),
            {"operation_id": operation_id},
        ).scalar_one()
    return (*operation, runtime_count, ingress_count, audit_count)


def _wait_for_runtime_lock_wait(engine: Engine, application_name: str) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with engine.connect() as connection:
            waiting = connection.execute(
                text(
                    "SELECT wait_event_type = 'Lock' FROM pg_stat_activity "
                    "WHERE application_name=:application_name"
                ),
                {"application_name": application_name},
            ).scalar_one_or_none()
        if waiting is True:
            return
        threading.Event().wait(0.01)
    raise AssertionError(f"{application_name} did not reach a PostgreSQL lock wait")


def _create_prepared_runtime(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    *,
    idempotency_key: str,
):
    from fstack.runtime_submission import prepare_operation

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v1_operation(
        migrated_database,
        monkeypatch,
        idempotency_key=idempotency_key,
    )
    return created, prepare_operation(created["operation_id"])


def _create_operation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    *,
    idempotency_key: str,
    body: dict[str, str] | None = None,
) -> dict:
    from fstack import api

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setattr(
        api,
        "create_engine",
        lambda *_args, **_kwargs: migrated_database,
    )
    with TestClient(create_app()) as client:
        response = client.post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": "Bearer test-operator-token",
                "Idempotency-Key": idempotency_key,
            },
            json=VALID_BODY if body is None else VALID_BODY | body,
        )
    response.raise_for_status()
    return response.json()


def _create_v2_operation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    *,
    idempotency_key: str,
    body: dict[str, str] | None = None,
) -> dict:
    created = _create_operation(
        migrated_database,
        monkeypatch,
        idempotency_key=idempotency_key,
        body=body,
    )
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
    return created


def _create_v1_operation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    *,
    idempotency_key: str,
    body: dict[str, str] | None = None,
) -> dict:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "20261006_0003")
    created = _create_operation(
        migrated_database,
        monkeypatch,
        idempotency_key=idempotency_key,
        body=body,
    )
    command.upgrade(configuration, "head")
    with migrated_database.connect() as connection:
        assert (
            connection.execute(
                text(
                    "SELECT workflow_type FROM workflow_dispatches WHERE operation_id=:operation_id"
                ),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == "fstack.provision_crusher.v1"
        )
    return created


def _confirm_runtime_submission(operation_id: str):
    from fstack.runtime_state import finalize_success, mark_running, persist_submitted
    from fstack.runtime_submission import prepare_operation

    prepared = prepare_operation(operation_id)
    mark_running(prepared.submission_id, prepared.operation_id)
    persist_submitted(prepared.submission_id, "docker-api-confirmed")
    confirmed = finalize_success(prepared.submission_id, prepared.operation_id)
    assert confirmed.state == "confirmed"
    return confirmed


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
def test_create_operation_reuses_fixture_owned_engine(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import api

    def reject_new_pool(*_args: object, **_kwargs: object) -> Engine:
        raise AssertionError("_create_operation created a new database pool")

    monkeypatch.setattr(api, "create_engine", reject_new_pool)

    for index in range(3):
        _create_operation(
            migrated_database,
            monkeypatch,
            idempotency_key=f"fixture-engine-reuse-{index}",
            body={
                "organization_slug": f"fixture-engine-reuse-{index}",
                "business_slug": f"fixture-engine-reuse-{index}",
                "public_domain": f"fixture-engine-reuse-{index}.acme.example",
            },
        )


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
    created = _create_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="create-acme-mailer",
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
def test_prepare_operation_bundle_creates_and_replays_atomic_started_bundle(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_submission import prepare_operation_bundle

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-runtime-ingress-bundle",
    )

    first = prepare_operation_bundle(created["operation_id"])
    with migrated_database.connect() as connection:
        first_operation = (
            connection.execute(
                text(
                    "SELECT state, started_at, completed_at, last_error_code "
                    "FROM provisioning_operations WHERE id=:operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .one()
        )
    replay = prepare_operation_bundle(created["operation_id"])

    assert replay == first
    assert first.operation_id == UUID(created["operation_id"])
    with migrated_database.connect() as connection:
        operation = (
            connection.execute(
                text(
                    "SELECT state, started_at, completed_at, last_error_code "
                    "FROM provisioning_operations WHERE id=:operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .one()
        )
        runtime_ids = (
            connection.execute(
                text("SELECT id FROM runtime_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            )
            .scalars()
            .all()
        )
        ingress_ids = (
            connection.execute(
                text("SELECT id FROM ingress_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            )
            .scalars()
            .all()
        )
        events = (
            connection.execute(
                text(
                    "SELECT event_type, payload FROM audit_events "
                    "WHERE operation_id=:operation_id ORDER BY created_at, event_type"
                ),
                {"operation_id": created["operation_id"]},
            )
            .mappings()
            .all()
        )

    assert operation == first_operation
    assert operation["state"] == "running"
    assert operation["started_at"] is not None
    assert operation["completed_at"] is None
    assert operation["last_error_code"] is None
    assert runtime_ids == [first.runtime_submission_id]
    assert ingress_ids == [first.ingress_submission_id]
    assert events == [
        {"event_type": "provisioning.requested", "payload": {}},
        {
            "event_type": "provisioning.started",
            "payload": {
                "runtime_submission_id": str(first.runtime_submission_id),
                "ingress_submission_id": str(first.ingress_submission_id),
            },
        },
    ]


@pytest.mark.postgres
def test_resolve_exact_operation_bundle_rejects_drift_and_partial_state(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_submission import (
        prepare_operation_bundle,
        resolve_exact_operation_bundle,
    )

    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="resolve-exact-operation-bundle",
    )
    prepared = prepare_operation_bundle(created["operation_id"])

    assert resolve_exact_operation_bundle(prepared.operation_id) == prepared
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-drifted")
    assert resolve_exact_operation_bundle(prepared.operation_id) is None
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")

    with migrated_database.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions DISABLE TRIGGER "
                "tr_ingress_submissions_lifecycle_guard"
            )
        )
        connection.execute(
            text("DELETE FROM ingress_submissions WHERE operation_id=:operation_id"),
            {"operation_id": prepared.operation_id},
        )
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions ENABLE TRIGGER "
                "tr_ingress_submissions_lifecycle_guard"
            )
        )

    assert resolve_exact_operation_bundle(prepared.operation_id) is None


@pytest.mark.postgres
def test_prepare_v2_runtime_only_drift_remains_retryable(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from temporalio.exceptions import ApplicationError

    from fstack.runtime_submission import prepare_operation
    from fstack.temporal_worker import prepare_local_crusher_ingress_v2
    from fstack.temporal_workflow import ProvisionCrusherV2Input

    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-v2-runtime-only-drift",
    )
    prepare_operation(created["operation_id"])
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-drifted")

    with pytest.raises(ApplicationError) as caught:
        prepare_local_crusher_ingress_v2(
            ProvisionCrusherV2Input(operation_id=created["operation_id"])
        )

    assert caught.value.non_retryable is False
    assert caught.value.message == "activity_infrastructure_failure"
    assert list(caught.value.details) == ["activity_infrastructure_failure"]
    with migrated_database.connect() as connection:
        operation_state = connection.execute(
            text("SELECT state FROM provisioning_operations WHERE id=:operation_id"),
            {"operation_id": created["operation_id"]},
        ).scalar_one()
        runtime_count = connection.execute(
            text("SELECT count(*) FROM runtime_submissions WHERE operation_id=:operation_id"),
            {"operation_id": created["operation_id"]},
        ).scalar_one()
        ingress_count = connection.execute(
            text("SELECT count(*) FROM ingress_submissions WHERE operation_id=:operation_id"),
            {"operation_id": created["operation_id"]},
        ).scalar_one()
        audit_types = (
            connection.execute(
                text(
                    "SELECT event_type FROM audit_events WHERE operation_id=:operation_id "
                    "ORDER BY created_at, id"
                ),
                {"operation_id": created["operation_id"]},
            )
            .scalars()
            .all()
        )
    assert operation_state == "pending"
    assert runtime_count == 1
    assert ingress_count == 0
    assert audit_types == ["provisioning.requested"]


@pytest.mark.postgres
def test_prepare_operation_requires_runtime_submission_insert_to_create_one_row(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_submission import (
        ProvisioningSubmissionStateError,
        prepare_operation_bundle,
    )
    from fstack.runtime_submission import prepare_operation

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-zero-row-runtime-insert",
    )
    bundle_created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-bundle-zero-row-runtime-insert",
        body={
            "organization_slug": "acme-runtime-bundle-zero-row",
            "business_slug": "mailer-runtime-bundle-zero-row",
            "public_domain": "runtime-bundle-zero-row.acme.example",
        },
    )
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION skip_runtime_submission_insert() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER aaa_skip_runtime_submission_insert
                BEFORE INSERT ON runtime_submissions
                FOR EACH ROW EXECUTE FUNCTION skip_runtime_submission_insert()
                """
            )
        )

    try:
        with pytest.raises(RuntimeError, match="^runtime_submission_prepare_failed$") as exc_info:
            prepare_operation(created["operation_id"])
        assert type(exc_info.value).__name__ == "RuntimeSubmissionStateError"
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None
        with pytest.raises(
            ProvisioningSubmissionStateError,
            match="^submission_prepare_failed$",
        ) as bundle_exc_info:
            prepare_operation_bundle(bundle_created["operation_id"])
        assert bundle_exc_info.value.__cause__ is None
        assert bundle_exc_info.value.__context__ is None
    finally:
        with migrated_database.begin() as connection:
            connection.execute(
                text("DROP TRIGGER aaa_skip_runtime_submission_insert ON runtime_submissions")
            )
            connection.execute(text("DROP FUNCTION skip_runtime_submission_insert()"))

    with migrated_database.connect() as connection:
        assert connection.execute(
            text("SELECT state, started_at FROM provisioning_operations WHERE id=:operation_id"),
            {"operation_id": created["operation_id"]},
        ).one() == ("pending", None)
        assert (
            connection.execute(
                text("SELECT count(*) FROM runtime_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM ingress_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM audit_events WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 1
        )


@pytest.mark.postgres
def test_prepare_ingress_requires_ingress_submission_insert_to_create_one_row(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import ingress_submission, runtime_submission
    from fstack.ingress_submission import ProvisioningSubmissionStateError

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-zero-row-ingress-insert",
    )
    operation_id = UUID(created["operation_id"])
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION skip_ingress_submission_insert() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER aaa_skip_ingress_submission_insert
                BEFORE INSERT ON ingress_submissions
                FOR EACH ROW EXECUTE FUNCTION skip_ingress_submission_insert()
                """
            )
        )

    try:
        with (
            pytest.raises(
                ProvisioningSubmissionStateError,
                match="^submission_prepare_failed$",
            ) as exc_info,
            migrated_database.begin() as connection,
        ):
            runtime_submission._prepare(
                connection,
                operation_id,
                reject_confirmed_drift=True,
            )
            operation = ingress_submission._operation(connection, operation_id)
            ingress_submission._prepare_ingress(connection, operation)
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None
    finally:
        with migrated_database.begin() as connection:
            connection.execute(
                text("DROP TRIGGER aaa_skip_ingress_submission_insert ON ingress_submissions")
            )
            connection.execute(text("DROP FUNCTION skip_ingress_submission_insert()"))

    with migrated_database.connect() as connection:
        assert connection.execute(
            text("SELECT state, started_at FROM provisioning_operations WHERE id=:operation_id"),
            {"operation_id": operation_id},
        ).one() == ("pending", None)
        assert (
            connection.execute(
                text("SELECT count(*) FROM runtime_submissions WHERE operation_id=:operation_id"),
                {"operation_id": operation_id},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM ingress_submissions WHERE operation_id=:operation_id"),
                {"operation_id": operation_id},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM audit_events WHERE operation_id=:operation_id"),
                {"operation_id": operation_id},
            ).scalar_one()
            == 1
        )


@pytest.mark.postgres
def test_prepare_operation_bundle_requires_pending_operation_update_to_change_one_row(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_submission import (
        ProvisioningSubmissionStateError,
        prepare_operation_bundle,
    )

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-bundle-zero-row-operation-update",
    )
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION skip_pending_operation_update() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF OLD.state = 'pending' AND NEW.state = 'running' THEN
                        RETURN NULL;
                    END IF;
                    RETURN NEW;
                END
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER aaa_skip_pending_operation_update
                BEFORE UPDATE ON provisioning_operations
                FOR EACH ROW EXECUTE FUNCTION skip_pending_operation_update()
                """
            )
        )

    try:
        with pytest.raises(
            ProvisioningSubmissionStateError,
            match="^submission_prepare_failed$",
        ) as exc_info:
            prepare_operation_bundle(created["operation_id"])
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None
    finally:
        with migrated_database.begin() as connection:
            connection.execute(
                text("DROP TRIGGER aaa_skip_pending_operation_update ON provisioning_operations")
            )
            connection.execute(text("DROP FUNCTION skip_pending_operation_update()"))

    with migrated_database.connect() as connection:
        assert connection.execute(
            text("SELECT state, started_at FROM provisioning_operations WHERE id=:operation_id"),
            {"operation_id": created["operation_id"]},
        ).one() == ("pending", None)
        assert (
            connection.execute(
                text("SELECT count(*) FROM runtime_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM ingress_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM audit_events WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 1
        )


@pytest.mark.postgres
def test_prepare_operation_bundle_requires_started_audit_insert_to_create_one_row(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_submission import (
        ProvisioningSubmissionStateError,
        prepare_operation_bundle,
    )

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-bundle-zero-row-started-audit",
    )
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION skip_provisioning_started_insert() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.event_type = 'provisioning.started' THEN
                        RETURN NULL;
                    END IF;
                    RETURN NEW;
                END
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER aaa_skip_provisioning_started_insert
                BEFORE INSERT ON audit_events
                FOR EACH ROW EXECUTE FUNCTION skip_provisioning_started_insert()
                """
            )
        )

    try:
        with pytest.raises(
            ProvisioningSubmissionStateError,
            match="^submission_prepare_failed$",
        ) as exc_info:
            prepare_operation_bundle(created["operation_id"])
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None
    finally:
        with migrated_database.begin() as connection:
            connection.execute(
                text("DROP TRIGGER aaa_skip_provisioning_started_insert ON audit_events")
            )
            connection.execute(text("DROP FUNCTION skip_provisioning_started_insert()"))

    with migrated_database.connect() as connection:
        assert connection.execute(
            text("SELECT state, started_at FROM provisioning_operations WHERE id=:operation_id"),
            {"operation_id": created["operation_id"]},
        ).one() == ("pending", None)
        assert (
            connection.execute(
                text("SELECT count(*) FROM runtime_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM ingress_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM audit_events WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 1
        )


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("target", "target_sql"),
    [
        ("operation", "UPDATE provisioning_operations SET state = 'running'"),
        (
            "started-audit",
            "INSERT INTO audit_events ( id, organization_id, operation_id, event_type,",
        ),
    ],
)
def test_prepare_operation_bundle_requires_exact_returning_identity(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    target_sql: str,
) -> None:
    from fstack import ingress_submission
    from fstack.ingress_submission import ProvisioningSubmissionStateError

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key=f"prepare-bundle-wrong-returning-{target}",
    )
    operation_id = UUID(created["operation_id"])
    before = _preparation_snapshot(migrated_database, operation_id)
    injected_engine = create_engine(
        ingress_submission.runtime_submission._database_url(),
        hide_parameters=True,
    )
    proxy = _ReturningIdentityEngine(injected_engine, target_sql, UUID(int=0))
    monkeypatch.setattr(ingress_submission, "create_engine", lambda *_args, **_kwargs: proxy)

    with pytest.raises(
        ProvisioningSubmissionStateError,
        match="^submission_prepare_failed$",
    ) as exc_info:
        ingress_submission.prepare_operation_bundle(operation_id)

    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert proxy.connection is not None
    assert proxy.connection.matches == 1
    assert _preparation_snapshot(migrated_database, operation_id) == before


@pytest.mark.postgres
def test_prepare_operation_bundle_replay_rejects_missing_started_audit(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_submission import (
        ProvisioningSubmissionConflictError,
        prepare_operation_bundle,
    )

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-bundle-missing-started-audit",
    )
    prepare_operation_bundle(created["operation_id"])
    with migrated_database.begin() as connection:
        connection.execute(
            text("ALTER TABLE audit_events DISABLE TRIGGER tr_audit_events_history_guard")
        )
        connection.execute(
            text(
                "DELETE FROM audit_events WHERE operation_id=:operation_id "
                "AND event_type='provisioning.started'"
            ),
            {"operation_id": created["operation_id"]},
        )
        connection.execute(
            text("ALTER TABLE audit_events ENABLE TRIGGER tr_audit_events_history_guard")
        )

    with pytest.raises(
        ProvisioningSubmissionConflictError,
        match="^started_audit_missing$",
    ):
        prepare_operation_bundle(created["operation_id"])

    with migrated_database.connect() as connection:
        assert (
            connection.execute(
                text(
                    "SELECT count(*) FROM audit_events WHERE operation_id=:operation_id "
                    "AND event_type='provisioning.started'"
                ),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_prepare_operation_bundle_reports_durable_drift_without_second_rows(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_submission import (
        ProvisioningSubmissionConflictError,
        prepare_operation_bundle,
    )

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-bundle-drift",
    )
    prepared = prepare_operation_bundle(created["operation_id"])
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-drifted")

    with pytest.raises(
        ProvisioningSubmissionConflictError,
        match="^runtime_submission_conflict$",
    ) as exc_info:
        prepare_operation_bundle(created["operation_id"])
    assert type(exc_info.value) is ProvisioningSubmissionConflictError
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None

    with migrated_database.connect() as connection:
        assert (
            connection.execute(
                text("SELECT id FROM runtime_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == prepared.runtime_submission_id
        )
        assert (
            connection.execute(
                text("SELECT id FROM ingress_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == prepared.ingress_submission_id
        )
        assert (
            connection.execute(
                text(
                    "SELECT count(*) FROM audit_events WHERE operation_id=:operation_id "
                    "AND event_type='provisioning.started'"
                ),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 1
        )


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("operation_state", "expected_message"),
    [
        ("missing", "operation does not exist"),
        ("failed", "operation is terminally failed"),
        ("running", "operation has no replayable runtime submission"),
    ],
)
def test_prepare_operation_bundle_preserves_nonclaimable_operation_errors(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    operation_state: str,
    expected_message: str,
) -> None:
    from fstack.ingress_submission import prepare_operation_bundle
    from fstack.runtime_submission import OperationNotClaimableError

    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    if operation_state == "missing":
        operation_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    else:
        created = _create_v2_operation(
            migrated_database,
            monkeypatch,
            idempotency_key=f"prepare-bundle-not-claimable-{operation_state}",
            body={
                "organization_slug": f"acme-{operation_state}",
                "business_slug": f"mailer-{operation_state}",
                "public_domain": f"{operation_state}.acme.example",
            },
        )
        operation_id = UUID(created["operation_id"])
        with migrated_database.begin() as connection:
            connection.execute(
                text(
                    "WITH boundary AS (SELECT clock_timestamp() AS now) "
                    "UPDATE provisioning_operations SET state='running', "
                    "started_at=boundary.now, updated_at=boundary.now FROM boundary "
                    "WHERE id=:operation_id"
                ),
                {"operation_id": operation_id},
            )
            if operation_state == "failed":
                connection.execute(
                    text(
                        "WITH boundary AS (SELECT clock_timestamp() AS now) "
                        "UPDATE provisioning_operations SET state='failed', "
                        "completed_at=boundary.now, last_error_code='test_failure', "
                        "updated_at=boundary.now FROM boundary WHERE id=:operation_id"
                    ),
                    {"operation_id": operation_id},
                )
                completed_at, updated_at = connection.execute(
                    text(
                        "SELECT completed_at, updated_at FROM provisioning_operations "
                        "WHERE id=:operation_id"
                    ),
                    {"operation_id": operation_id},
                ).one()
                assert completed_at == updated_at

    with pytest.raises(OperationNotClaimableError, match=f"^{expected_message}$") as exc_info:
        prepare_operation_bundle(operation_id)

    assert type(exc_info.value) is OperationNotClaimableError
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.postgres
def test_prepare_operation_bundle_does_not_classify_infrastructure_failure_as_conflict(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import ingress_submission, runtime_submission
    from fstack.ingress_submission import ProvisioningSubmissionStateError

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-bundle-infrastructure-failure",
    )
    infrastructure_error = OperationalError("SELECT 1", {}, RuntimeError("database unavailable"))

    def fail_prepare(*_args: object, **_kwargs: object) -> None:
        raise infrastructure_error

    monkeypatch.setattr(runtime_submission, "_prepare", fail_prepare)

    with pytest.raises(
        ProvisioningSubmissionStateError,
        match="^submission_prepare_failed$",
    ) as exc_info:
        ingress_submission.prepare_operation_bundle(created["operation_id"])
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None

    with migrated_database.connect() as connection:
        assert (
            connection.execute(
                text("SELECT state FROM provisioning_operations WHERE id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == "pending"
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM runtime_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM ingress_submissions WHERE operation_id=:operation_id"),
                {"operation_id": created["operation_id"]},
            ).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_prepare_operation_bundle_rolls_back_after_every_statement(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import ingress_submission

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    engine = migrated_database.execution_options()
    monkeypatch.setattr(ingress_submission, "create_engine", lambda *_args, **_kwargs: engine)
    inventory_created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-bundle-statement-inventory",
        body={
            "organization_slug": "acme-statement-inventory",
            "business_slug": "mailer-statement-inventory",
            "public_domain": "statement-inventory.acme.example",
        },
    )
    executed_statements: list[str] = []

    def inventory_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        executed_statements.append(statement)

    event.listen(engine, "after_cursor_execute", inventory_statement)
    try:
        ingress_submission.prepare_operation_bundle(inventory_created["operation_id"])
    finally:
        event.remove(engine, "after_cursor_execute", inventory_statement)

    execute_boundary_count = len(executed_statements)
    assert execute_boundary_count == 9

    for failure_statement in range(1, execute_boundary_count + 1):
        created = _create_v2_operation(
            migrated_database,
            monkeypatch,
            idempotency_key=f"prepare-bundle-rollback-{failure_statement}",
            body={
                "organization_slug": f"acme-rollback-{failure_statement}",
                "organization_name": f"Acme Rollback {failure_statement}",
                "business_slug": f"mailer-rollback-{failure_statement}",
                "business_name": f"Mailer Rollback {failure_statement}",
                "public_domain": f"rollback-{failure_statement}.acme.example",
            },
        )
        statement_count = 0

        def fail_after_statement(
            *_args: object,
            failure_boundary: int = failure_statement,
            **_kwargs: object,
        ) -> None:
            nonlocal statement_count
            statement_count += 1
            if statement_count == failure_boundary:
                raise RuntimeError("injected_statement_failure")

        event.listen(engine, "after_cursor_execute", fail_after_statement)
        try:
            with pytest.raises(RuntimeError, match="^injected_statement_failure$"):
                ingress_submission.prepare_operation_bundle(created["operation_id"])
        finally:
            event.remove(engine, "after_cursor_execute", fail_after_statement)
        assert statement_count == failure_statement

        with migrated_database.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT state FROM provisioning_operations WHERE id=:operation_id"),
                    {"operation_id": created["operation_id"]},
                ).scalar_one()
                == "pending"
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_submissions WHERE operation_id=:operation_id"
                    ),
                    {"operation_id": created["operation_id"]},
                ).scalar_one()
                == 0
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM ingress_submissions WHERE operation_id=:operation_id"
                    ),
                    {"operation_id": created["operation_id"]},
                ).scalar_one()
                == 0
            )
            assert (
                connection.execute(
                    text("SELECT count(*) FROM audit_events WHERE operation_id=:operation_id"),
                    {"operation_id": created["operation_id"]},
                ).scalar_one()
                == 1
            )


@pytest.mark.postgres
def test_prepare_operation_bundle_hides_sealed_parameters_on_database_failure(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from fstack import ingress_submission, runtime_submission

    markers = {
        "submission_key": "submission-key-marker-5f5d",
        "fingerprint": "fingerprint-marker-7a7b",
        "route_id": "route-marker-8c8d",
        "container_name": "container-marker-9e9f",
        "public_host": "host-marker-a1b2.example",
    }
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="prepare-bundle-hidden-parameters",
        body={"public_domain": markers["public_host"]},
    )
    real_create_engine = ingress_submission.create_engine
    engine_options: dict[str, object] = {}

    def recording_create_engine(url: str, **kwargs: object):
        engine_options.update(kwargs)
        return real_create_engine(url, **kwargs)

    def fail_with_database_error(
        connection: Connection,
        _operation_id: UUID,
        **_kwargs: object,
    ) -> None:
        connection.execute(
            text(
                "SELECT CAST(:submission_key AS integer), :fingerprint, :route_id, "
                ":container_name, :public_host"
            ),
            markers,
        )

    monkeypatch.setattr(ingress_submission, "create_engine", recording_create_engine)
    monkeypatch.setattr(runtime_submission, "_prepare", fail_with_database_error)
    caplog.set_level(logging.DEBUG)

    with pytest.raises(
        ingress_submission.ProvisioningSubmissionStateError,
        match="^submission_prepare_failed$",
    ) as exc_info:
        ingress_submission.prepare_operation_bundle(created["operation_id"])

    assert engine_options["hide_parameters"] is True
    observed = [str(exc_info.value), repr(exc_info.value), caplog.text]
    current: BaseException | None = exc_info.value
    while current is not None:
        observed.extend([str(current), repr(current)])
        current = current.__cause__ or current.__context__
    combined = "\n".join(observed)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert all(marker not in combined for marker in markers.values())


@pytest.mark.postgres
def test_confirmed_runtime_submission_replays_when_sealed_identity_is_unchanged(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_submission import prepare_operation

    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v1_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="confirmed-runtime-exact-replay",
    )
    confirmed = _confirm_runtime_submission(created["operation_id"])

    assert prepare_operation(created["operation_id"]) == confirmed


@pytest.mark.postgres
def test_confirmed_v1_runtime_submission_accepts_configuration_fingerprint_drift(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_submission import prepare_operation

    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v1_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="confirmed-runtime-config-drift",
    )
    confirmed = _confirm_runtime_submission(created["operation_id"])
    before, audit_count_before = _runtime_snapshot(migrated_database, confirmed.submission_id)
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-drifted")

    assert prepare_operation(created["operation_id"]) == confirmed
    assert _runtime_snapshot(migrated_database, confirmed.submission_id) == (
        before,
        audit_count_before,
    )


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("column", "drifted_value"),
    [
        ("provider", "drifted_provider"),
        ("generation", 2),
        ("submission_key", "1" * 64),
        ("spec_fingerprint", "2" * 64),
        ("api_container_name", "drifted-api-container"),
        ("migration_container_name", "drifted-migration-container"),
    ],
)
def test_confirmed_v1_runtime_submission_accepts_each_sealed_identity_drift(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    column: str,
    drifted_value: object,
) -> None:
    from fstack.runtime_submission import prepare_operation

    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v1_operation(
        migrated_database,
        monkeypatch,
        idempotency_key=f"confirmed-runtime-{column}-drift",
        body={
            "organization_slug": f"acme-{column.replace('_', '-')}",
            "business_slug": f"mailer-{column.replace('_', '-')}",
            "public_domain": f"{column.replace('_', '-')}.acme.example",
        },
    )
    confirmed = _confirm_runtime_submission(created["operation_id"])
    with migrated_database.begin() as connection:
        if column == "provider":
            connection.execute(
                text("ALTER TABLE runtime_submissions DROP CONSTRAINT ck_runtime_provider")
            )
        elif column in {"api_container_name", "migration_container_name"}:
            connection.execute(
                text(
                    "ALTER TABLE runtime_submissions "
                    "DROP CONSTRAINT ck_runtime_deterministic_container_names"
                )
            )
        connection.execute(
            text(
                "ALTER TABLE runtime_submissions "
                "DISABLE TRIGGER tr_runtime_submissions_lifecycle_guard"
            )
        )
        connection.execute(
            text(f"UPDATE runtime_submissions SET {column}=:value WHERE id=:submission_id"),
            {"value": drifted_value, "submission_id": confirmed.submission_id},
        )
        connection.execute(
            text(
                "ALTER TABLE runtime_submissions "
                "ENABLE TRIGGER tr_runtime_submissions_lifecycle_guard"
            )
        )
    before, audit_count_before = _runtime_snapshot(migrated_database, confirmed.submission_id)

    replayed = prepare_operation(created["operation_id"])

    assert replayed.submission_id == confirmed.submission_id
    assert replayed.operation_id == confirmed.operation_id
    assert replayed.state == "confirmed"
    assert _runtime_snapshot(migrated_database, confirmed.submission_id) == (
        before,
        audit_count_before,
    )


@pytest.mark.postgres
def test_terminal_audit_event_requires_the_operations_exact_submission(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.runtime_submission import prepare_operation

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="create-acme-mailer",
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
@pytest.mark.parametrize(
    ("extra_key", "extra_value"),
    [
        ("unexpected", "value"),
        ("provider", "local_caddy"),
        ("route_id", "fstack-crusher-ingress-" + "a" * 32),
        ("api_container_name", "fstack-crusher-api-" + "b" * 32),
        ("route_fingerprint", "c" * 64),
        ("spec_fingerprint", "d" * 64),
    ],
)
def test_v2_success_audit_rejects_every_extra_field(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    extra_key: str,
    extra_value: str,
) -> None:
    from fstack.ingress_submission import prepare_operation_bundle

    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v2_operation(
        migrated_database,
        monkeypatch,
        idempotency_key=f"v2-success-audit-extra-{extra_key}",
        body={
            "organization_slug": f"audit-{extra_key.replace('_', '-')}",
            "business_slug": f"audit-{extra_key.replace('_', '-')}",
            "public_domain": f"audit-{extra_key.replace('_', '-')}.acme.example",
        },
    )
    prepared = prepare_operation_bundle(created["operation_id"])
    payload = {
        "runtime_submission_id": str(prepared.runtime_submission_id),
        "ingress_submission_id": str(prepared.ingress_submission_id),
        extra_key: extra_value,
    }

    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO audit_events (
                    id, organization_id, operation_id, event_type, actor_principal, payload
                ) VALUES (
                    gen_random_uuid(), :organization_id, :operation_id,
                    'provisioning.succeeded', 'installation-operator', CAST(:payload AS jsonb)
                )
                """
            ),
            {
                "organization_id": created["organization_id"],
                "operation_id": created["operation_id"],
                "payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
            },
        )


@pytest.mark.postgres
def test_runtime_transition_locks_operation_before_runtime_submission(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import runtime_state

    _created, prepared = _create_prepared_runtime(
        migrated_database,
        monkeypatch,
        idempotency_key="runtime-operation-first-lock-order",
    )
    worker_engine = create_engine(
        runtime_state.runtime_submission._database_url(),
        connect_args={
            "application_name": "fstack-test-runtime-operation-first",
            "options": "-c lock_timeout=4000",
        },
    )
    monkeypatch.setattr(runtime_state, "_engine", lambda: worker_engine)
    outcome: list[BaseException | None] = []

    def persist() -> None:
        try:
            runtime_state.persist_submitted(prepared.submission_id, "docker-api-operation-first")
        except BaseException as error:
            outcome.append(error)
        else:
            outcome.append(None)

    blocker = migrated_database.connect()
    transaction = blocker.begin()
    blocker.execute(
        text("SELECT id FROM runtime_submissions WHERE id=:id FOR UPDATE"),
        {"id": prepared.submission_id},
    )
    worker = threading.Thread(target=persist)
    try:
        worker.start()
        _wait_for_runtime_lock_wait(
            migrated_database,
            "fstack-test-runtime-operation-first",
        )
        with migrated_database.connect() as contender:
            contender.execute(text("SET LOCAL lock_timeout='100ms'"))
            with pytest.raises(OperationalError) as exc_info:
                contender.execute(
                    text(
                        "SELECT id FROM provisioning_operations WHERE id=:operation_id FOR UPDATE"
                    ),
                    {"operation_id": prepared.operation_id},
                )
            assert getattr(exc_info.value.orig, "sqlstate", None) == "55P03"
        transaction.rollback()
        worker.join(timeout=6)
    finally:
        if transaction.is_active:
            transaction.rollback()
        blocker.close()
        worker_engine.dispose()

    assert not worker.is_alive()
    assert outcome == [None]


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
    created = _create_v1_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="success-acme-mailer",
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
                    "SELECT event_type, payload FROM audit_events "
                    "WHERE operation_id=:id ORDER BY created_at"
                ),
                {"id": created["operation_id"]},
            )
            .mappings()
            .all()
        )
    assert operation == {"state": "succeeded", "last_error_code": None}
    assert submission == {
        "state": "confirmed",
        "external_id": "docker-api-123",
        "failure_code": None,
    }
    assert events == [
        {"event_type": "provisioning.requested", "payload": {}},
        {
            "event_type": "provisioning.started",
            "payload": {
                "provider": "local_docker",
                "runtime_submission_id": str(prepared.submission_id),
            },
        },
        {
            "event_type": "provisioning.succeeded",
            "payload": {"runtime_submission_id": str(prepared.submission_id)},
        },
    ]


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
    created = _create_v1_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="failure-acme-mailer",
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
        migrated_database, monkeypatch, idempotency_key="mismatched-apply-operation"
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
        migrated_database, monkeypatch, idempotency_key="mismatched-success-operation"
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
        migrated_database, monkeypatch, idempotency_key="mismatched-failure-operation"
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
    from fstack.runtime_submission import RuntimeSubmissionConflictError, prepare_operation

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    created = _create_v1_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="ambiguous-prepare-acme-mailer",
    )
    prepared = prepare_operation(created["operation_id"])
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-drifted")

    with pytest.raises(RuntimeSubmissionConflictError):
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
    created = _create_operation(
        migrated_database,
        monkeypatch,
        idempotency_key="unprepared-failure-acme-mailer",
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
