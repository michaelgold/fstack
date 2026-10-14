from __future__ import annotations

import logging
import queue
import threading
import time
import traceback
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, event, inspect, text
from sqlalchemy.exc import IntegrityError, OperationalError

from fstack.api import create_app

VALID_BODY = {
    "organization_slug": "ingress-state-acme",
    "organization_name": "Ingress State Acme",
    "business_slug": "mailer",
    "business_name": "Mailer",
    "environment_slug": "production",
    "public_domain": "ingress-state.acme.example",
    "crusher_image": "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
    "database_secret_ref": "secret://vault/acme/database",
    "object_storage_secret_ref": "secret://vault/acme/object-storage",
}


def _wait_for_postgres_lock_wait(engine: Engine, application_name: str) -> None:
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


def _prepared_bundle(
    engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    *,
    idempotency_key: str,
):
    from fstack import api
    from fstack.ingress_submission import prepare_operation_bundle

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    monkeypatch.setattr(api, "create_engine", lambda *_args, **_kwargs: engine)
    with TestClient(create_app()) as client:
        response = client.post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": "Bearer test-operator-token",
                "Idempotency-Key": idempotency_key,
            },
            json=VALID_BODY,
        )
    response.raise_for_status()
    return prepare_operation_bundle(response.json()["operation_id"])


def _ingress_row(engine: Engine, submission_id: UUID) -> dict[str, object]:
    with engine.connect() as connection:
        return dict(
            connection.execute(
                text("SELECT * FROM ingress_submissions WHERE id=:submission_id"),
                {"submission_id": submission_id},
            )
            .mappings()
            .one()
        )


def _snapshot(engine: Engine, submission_id: UUID) -> tuple[dict[str, object], int]:
    row = _ingress_row(engine, submission_id)
    with engine.connect() as connection:
        audit_count = connection.execute(
            text("SELECT count(*) FROM audit_events WHERE operation_id=:operation_id"),
            {"operation_id": row["operation_id"]},
        ).scalar_one()
    return row, audit_count


def _joint_success_snapshot(engine: Engine, operation_id: UUID) -> dict[str, object]:
    with engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT operation.state AS operation_state,
                       operation.completed_at, operation.updated_at,
                       operation.last_error_code,
                       runtime.state AS runtime_state,
                       runtime.confirmed_at AS runtime_confirmed_at,
                       ingress.state AS ingress_state,
                       ingress.confirmed_at AS ingress_confirmed_at
                FROM provisioning_operations AS operation
                JOIN runtime_submissions AS runtime ON runtime.operation_id=operation.id
                JOIN ingress_submissions AS ingress ON ingress.operation_id=operation.id
                WHERE operation.id=:operation_id
                """
                ),
                {"operation_id": operation_id},
            )
            .mappings()
            .one()
        )
        audit_count = connection.execute(
            text(
                "SELECT count(*) FROM audit_events WHERE operation_id=:operation_id "
                "AND event_type='provisioning.succeeded'"
            ),
            {"operation_id": operation_id},
        ).scalar_one()
    return {**dict(row), "success_audit_count": audit_count}


def _assert_error(
    action: Callable[[], None],
    code: str,
) -> None:
    from fstack.ingress_state import IngressStateError

    with pytest.raises(IngressStateError) as exc_info:
        action()
    assert exc_info.value.code == code
    assert str(exc_info.value) == code
    assert repr(exc_info.value) == f"IngressStateError('{code}')"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def _assert_sanitized_error(
    action: Callable[[], None],
    code: str,
    *,
    marker: str,
    module_name: str,
    caplog: pytest.LogCaptureFixture | None = None,
) -> None:
    from fstack.ingress_state import IngressStateError

    with pytest.raises(IngressStateError) as exc_info:
        action()
    error = exc_info.value
    surfaces = [
        str(error),
        repr(error),
        repr(error.__cause__),
        repr(error.__context__),
        "".join(traceback.format_exception(error)),
        "" if caplog is None else caplog.text,
    ]
    current = error.__traceback__
    while current is not None:
        if current.tb_frame.f_globals.get("__name__") == module_name:
            surfaces.append(repr(current.tb_frame.f_locals))
        current = current.tb_next
    assert error.code == code
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in "\n".join(surfaces)


def _selected_row(engine: Engine, submission_id: UUID) -> dict[str, object]:
    with engine.connect() as connection:
        return dict(
            connection.execute(
                text(
                    """
                    SELECT submission.operation_id,
                           submission.organization_id AS submission_organization_id,
                           submission.deployment_id AS submission_deployment_id,
                           submission.provider, submission.generation,
                           submission.submission_key, submission.route_fingerprint,
                           submission.route_id, submission.state, submission.prepared_at,
                           submission.submitted_at, submission.confirmed_at,
                           submission.failed_at, submission.failure_code,
                           operation.organization_id AS operation_organization_id,
                           operation.deployment_id AS operation_deployment_id,
                           operation.state AS operation_state,
                           deployment.id AS deployment_id,
                           deployment.organization_id AS deployment_organization_id,
                           deployment.public_domain
                    FROM ingress_submissions AS submission
                    JOIN provisioning_operations AS operation
                      ON operation.id = submission.operation_id
                    JOIN deployments AS deployment
                      ON deployment.id = submission.deployment_id
                    WHERE submission.id = :submission_id
                    """
                ),
                {"submission_id": submission_id},
            )
            .mappings()
            .one()
        )


class _StaticResult:
    def __init__(self, row: dict[str, object], failure: Exception | None = None) -> None:
        self._row = row
        self._failure = failure

    def mappings(self) -> _StaticResult:
        if self._failure is not None:
            raise self._failure
        return self

    def one_or_none(self) -> dict[str, object]:
        if self._failure is not None:
            raise self._failure
        return self._row


class _OneFailureResult(_StaticResult):
    def mappings(self) -> _OneFailureResult:
        return self

    def one_or_none(self) -> dict[str, object]:
        assert self._failure is not None
        raise self._failure


class _ScalarFailureResult(_StaticResult):
    def scalar_one_or_none(self) -> object:
        assert self._failure is not None
        raise self._failure


class _StaticConnection:
    def __init__(self, result: _StaticResult, failure: Exception | None = None) -> None:
        self._result = result
        self._failure = failure

    def execute(self, *_args: object, **_kwargs: object) -> _StaticResult:
        if self._failure is not None:
            raise self._failure
        return self._result


class _SequenceConnection(_StaticConnection):
    def __init__(self, results: list[_StaticResult]) -> None:
        self._results = iter(results)

    def execute(self, *_args: object, **_kwargs: object) -> _StaticResult:
        return next(self._results)


class _StaticBegin:
    def __init__(
        self,
        connection: _StaticConnection,
        *,
        enter_failure: Exception | None = None,
        exit_failure: Exception | None = None,
    ) -> None:
        self._connection = connection
        self._enter_failure = enter_failure
        self._exit_failure = exit_failure

    def __enter__(self) -> _StaticConnection:
        if self._enter_failure is not None:
            raise self._enter_failure
        return self._connection

    def __exit__(self, *_args: object) -> bool:
        if self._exit_failure is not None:
            raise self._exit_failure
        return False


class _StaticEngine:
    def __init__(
        self,
        begin: _StaticBegin,
        *,
        begin_failure: Exception | None = None,
        dispose_failure: Exception | None = None,
    ) -> None:
        self._begin = begin
        self._begin_failure = begin_failure
        self._dispose_failure = dispose_failure
        self.dispose_calls = 0

    def begin(self) -> _StaticBegin:
        if self._begin_failure is not None:
            raise self._begin_failure
        return self._begin

    def dispose(self) -> None:
        self.dispose_calls += 1
        if self._dispose_failure is not None:
            raise self._dispose_failure


class _WrongReturningScalarResult:
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
            return _WrongReturningScalarResult(result, self._wrong_id)
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


class _UUIDSubclass(UUID):
    pass


class _DatetimeSubclass(datetime):
    pass


class _HostileIdentifier:
    def __init__(self, marker: str) -> None:
        self._marker = marker

    def __eq__(self, _other: object) -> bool:
        raise AssertionError(self._marker)

    def __repr__(self) -> str:
        return f"HostileIdentifier({self._marker})"

    def __str__(self) -> str:
        return self._marker


@pytest.mark.parametrize("argument", ["submission", "operation"])
@pytest.mark.parametrize("kind", ["non-uuid", "uuid-subclass", "hostile"])
def test_runtime_identifiers_require_exact_uuid_before_engine_creation(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    argument: str,
    kind: str,
) -> None:
    from fstack import ingress_state

    marker = f"unique-invalid-identifier-{argument}-{kind}"
    if kind == "non-uuid":
        invalid: object = marker
    elif kind == "uuid-subclass":
        invalid = _UUIDSubclass(str(uuid4()))
    else:
        invalid = _HostileIdentifier(marker)
    submission_id: object = uuid4()
    operation_id: object = uuid4()
    if argument == "submission":
        submission_id = invalid
    else:
        operation_id = invalid
    engine_calls = 0

    def forbidden_engine() -> Engine:
        nonlocal engine_calls
        engine_calls += 1
        raise AssertionError("engine must not be created")

    monkeypatch.setattr(ingress_state, "_engine", forbidden_engine)
    caplog.set_level(logging.DEBUG)

    _assert_sanitized_error(
        lambda: ingress_state.mark_submitted(  # type: ignore[arg-type]
            submission_id,
            operation_id,
        ),
        "ingress_state_invalid_identifier",
        marker=marker,
        module_name=ingress_state.__name__,
        caplog=caplog,
    )

    assert engine_calls == 0


def test_engine_creation_value_error_is_fully_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from fstack import ingress_state

    marker = "unique-engine-creation-value-error"

    def fail_engine() -> Engine:
        raise ValueError(marker)

    monkeypatch.setattr(ingress_state, "_engine", fail_engine)
    caplog.set_level(logging.DEBUG)

    _assert_sanitized_error(
        lambda: ingress_state.mark_submitted(uuid4(), uuid4()),
        "ingress_state_infrastructure",
        marker=marker,
        module_name=ingress_state.__name__,
        caplog=caplog,
    )


def test_engine_creation_does_not_catch_base_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import ingress_state

    class StopNow(BaseException):
        pass

    def fail_engine() -> Engine:
        raise StopNow

    monkeypatch.setattr(ingress_state, "_engine", fail_engine)

    with pytest.raises(StopNow):
        ingress_state.mark_submitted(uuid4(), uuid4())


@pytest.mark.postgres
def test_mark_submitted_transitions_once_and_replay_preserves_exact_timestamp(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import mark_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-submit-replay",
    )

    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    first = _ingress_row(migrated_database, prepared.ingress_submission_id)
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    replay = _ingress_row(migrated_database, prepared.ingress_submission_id)

    assert first["state"] == "submitted"
    assert first["submitted_at"] is not None
    assert replay == first
    assert first["confirmed_at"] is None
    assert first["failed_at"] is None
    assert first["failure_code"] is None


@pytest.mark.postgres
def test_finalize_provisioning_success_confirms_joint_bundle_atomically(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import finalize_provisioning_success, mark_submitted
    from fstack.runtime_state import persist_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="joint-success-happy-path",
    )
    persist_submitted(prepared.runtime_submission_id, "docker-api-joint-success")
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)

    finalize_provisioning_success(
        prepared.runtime_submission_id,
        prepared.ingress_submission_id,
        prepared.operation_id,
    )

    with migrated_database.connect() as connection:
        runtime = (
            connection.execute(
                text(
                    "SELECT state, confirmed_at, failed_at, failure_code "
                    "FROM runtime_submissions WHERE id=:submission_id"
                ),
                {"submission_id": prepared.runtime_submission_id},
            )
            .mappings()
            .one()
        )
        ingress = (
            connection.execute(
                text(
                    "SELECT state, confirmed_at, failed_at, failure_code "
                    "FROM ingress_submissions WHERE id=:submission_id"
                ),
                {"submission_id": prepared.ingress_submission_id},
            )
            .mappings()
            .one()
        )
        operation = (
            connection.execute(
                text(
                    "SELECT state, completed_at, last_error_code "
                    "FROM provisioning_operations WHERE id=:operation_id"
                ),
                {"operation_id": prepared.operation_id},
            )
            .mappings()
            .one()
        )
        succeeded = (
            connection.execute(
                text(
                    "SELECT payload FROM audit_events WHERE operation_id=:operation_id "
                    "AND event_type='provisioning.succeeded'"
                ),
                {"operation_id": prepared.operation_id},
            )
            .mappings()
            .all()
        )

    assert runtime["state"] == "confirmed"
    assert runtime["confirmed_at"] is not None
    assert runtime["failed_at"] is None
    assert runtime["failure_code"] is None
    assert ingress["state"] == "confirmed"
    assert ingress["confirmed_at"] is not None
    assert ingress["failed_at"] is None
    assert ingress["failure_code"] is None
    assert operation["state"] == "succeeded"
    assert operation["completed_at"] is not None
    assert operation["last_error_code"] is None
    assert succeeded == [
        {
            "payload": {
                "runtime_submission_id": str(prepared.runtime_submission_id),
                "ingress_submission_id": str(prepared.ingress_submission_id),
            }
        }
    ]


@pytest.mark.postgres
def test_mark_submitted_and_joint_finalization_complete_without_lock_inversion(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import ingress_state
    from fstack.runtime_state import persist_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="joint-success-lock-order",
    )
    persist_submitted(prepared.runtime_submission_id, "docker-api-lock-order")
    database_url = ingress_state.runtime_submission._database_url()
    engines = {
        "mark-submitted": create_engine(
            database_url,
            connect_args={
                "application_name": "fstack-test-mark-submitted",
                "options": "-c lock_timeout=4000",
            },
        ),
        "joint-finalize": create_engine(
            database_url,
            connect_args={
                "application_name": "fstack-test-joint-finalize",
                "options": "-c lock_timeout=4000",
            },
        ),
    }
    monkeypatch.setattr(
        ingress_state,
        "_engine",
        lambda: engines[threading.current_thread().name],
    )
    outcomes: queue.Queue[tuple[str, BaseException | None]] = queue.Queue()

    def run(name: str, action: Callable[[], None]) -> None:
        try:
            action()
        except BaseException as error:
            outcomes.put((name, error))
        else:
            outcomes.put((name, None))

    blocker = migrated_database.connect()
    transaction = blocker.begin()
    blocker.execute(
        text("SELECT id FROM ingress_submissions WHERE id=:id FOR UPDATE"),
        {"id": prepared.ingress_submission_id},
    )
    mark_thread = threading.Thread(
        name="mark-submitted",
        target=run,
        args=(
            "mark-submitted",
            lambda: ingress_state.mark_submitted(
                prepared.ingress_submission_id,
                prepared.operation_id,
            ),
        ),
    )
    finalize_thread = threading.Thread(
        name="joint-finalize",
        target=run,
        args=(
            "joint-finalize",
            lambda: ingress_state.finalize_provisioning_success(
                prepared.runtime_submission_id,
                prepared.ingress_submission_id,
                prepared.operation_id,
            ),
        ),
    )
    try:
        mark_thread.start()
        _wait_for_postgres_lock_wait(migrated_database, "fstack-test-mark-submitted")
        finalize_thread.start()
        _wait_for_postgres_lock_wait(migrated_database, "fstack-test-joint-finalize")
        transaction.rollback()
        mark_thread.join(timeout=6)
        finalize_thread.join(timeout=6)
    finally:
        if transaction.is_active:
            transaction.rollback()
        blocker.close()
        for engine in engines.values():
            engine.dispose()

    assert not mark_thread.is_alive()
    assert not finalize_thread.is_alive()
    completed = [outcomes.get_nowait(), outcomes.get_nowait()]
    assert {name for name, _error in completed} == {"mark-submitted", "joint-finalize"}
    assert [(name, type(error).__name__) for name, error in completed if error is not None] == []
    snapshot = _joint_success_snapshot(migrated_database, prepared.operation_id)
    assert snapshot["operation_state"] == "succeeded"
    assert snapshot["runtime_state"] == "confirmed"
    assert snapshot["ingress_state"] == "confirmed"
    assert snapshot["success_audit_count"] == 1


@pytest.mark.postgres
def test_finalize_provisioning_success_exact_terminal_replay_adds_no_audit(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import finalize_provisioning_success, mark_submitted
    from fstack.runtime_state import persist_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="joint-success-replay",
    )
    persist_submitted(prepared.runtime_submission_id, "docker-api-joint-replay")
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    finalize_provisioning_success(
        prepared.runtime_submission_id,
        prepared.ingress_submission_id,
        prepared.operation_id,
    )
    with migrated_database.connect() as connection:
        before = (
            connection.execute(
                text(
                    "SELECT runtime.confirmed_at AS runtime_confirmed_at, "
                    "ingress.confirmed_at AS ingress_confirmed_at, "
                    "operation.completed_at, operation.updated_at "
                    "FROM provisioning_operations AS operation "
                    "JOIN runtime_submissions AS runtime ON runtime.operation_id=operation.id "
                    "JOIN ingress_submissions AS ingress ON ingress.operation_id=operation.id "
                    "WHERE operation.id=:operation_id"
                ),
                {"operation_id": prepared.operation_id},
            )
            .mappings()
            .one()
        )

    finalize_provisioning_success(
        prepared.runtime_submission_id,
        prepared.ingress_submission_id,
        prepared.operation_id,
    )

    with migrated_database.connect() as connection:
        after = (
            connection.execute(
                text(
                    "SELECT runtime.confirmed_at AS runtime_confirmed_at, "
                    "ingress.confirmed_at AS ingress_confirmed_at, "
                    "operation.completed_at, operation.updated_at "
                    "FROM provisioning_operations AS operation "
                    "JOIN runtime_submissions AS runtime ON runtime.operation_id=operation.id "
                    "JOIN ingress_submissions AS ingress ON ingress.operation_id=operation.id "
                    "WHERE operation.id=:operation_id"
                ),
                {"operation_id": prepared.operation_id},
            )
            .mappings()
            .one()
        )
        audit_count = connection.execute(
            text(
                "SELECT count(*) FROM audit_events WHERE operation_id=:operation_id "
                "AND event_type='provisioning.succeeded'"
            ),
            {"operation_id": prepared.operation_id},
        ).scalar_one()

    assert after == before
    assert audit_count == 1


@pytest.mark.postgres
@pytest.mark.parametrize("mismatched_identifier", ["runtime", "ingress", "operation"])
def test_finalize_provisioning_success_requires_all_three_exact_identifiers(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    mismatched_identifier: str,
) -> None:
    from fstack.ingress_state import finalize_provisioning_success, mark_submitted
    from fstack.runtime_state import persist_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key=f"joint-success-mismatch-{mismatched_identifier}",
    )
    persist_submitted(
        prepared.runtime_submission_id,
        f"docker-api-joint-mismatch-{mismatched_identifier}",
    )
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    before = _joint_success_snapshot(migrated_database, prepared.operation_id)
    runtime_id = prepared.runtime_submission_id
    ingress_id = prepared.ingress_submission_id
    operation_id = prepared.operation_id
    if mismatched_identifier == "runtime":
        runtime_id = uuid4()
    elif mismatched_identifier == "ingress":
        ingress_id = uuid4()
    else:
        operation_id = uuid4()

    _assert_error(
        lambda: finalize_provisioning_success(runtime_id, ingress_id, operation_id),
        "provisioning_success_bundle_mismatch",
    )

    assert _joint_success_snapshot(migrated_database, prepared.operation_id) == before


@pytest.mark.postgres
@pytest.mark.parametrize("invalid_state", ["runtime", "ingress", "operation"])
def test_finalize_provisioning_success_requires_joint_submitted_running_state(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    invalid_state: str,
) -> None:
    from fstack.ingress_state import finalize_provisioning_success, mark_submitted
    from fstack.runtime_state import persist_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key=f"joint-success-state-{invalid_state}",
    )
    if invalid_state != "runtime":
        persist_submitted(
            prepared.runtime_submission_id,
            f"docker-api-joint-state-{invalid_state}",
        )
    if invalid_state != "ingress":
        mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    if invalid_state == "operation":
        with migrated_database.begin() as connection:
            connection.execute(
                text(
                    "ALTER TABLE provisioning_operations DISABLE TRIGGER "
                    "tr_operations_lifecycle_guard"
                )
            )
            connection.execute(
                text(
                    "UPDATE provisioning_operations SET state='pending', started_at=NULL "
                    "WHERE id=:operation_id"
                ),
                {"operation_id": prepared.operation_id},
            )
            connection.execute(
                text(
                    "ALTER TABLE provisioning_operations ENABLE TRIGGER "
                    "tr_operations_lifecycle_guard"
                )
            )
    before = _joint_success_snapshot(migrated_database, prepared.operation_id)

    _assert_error(
        lambda: finalize_provisioning_success(
            prepared.runtime_submission_id,
            prepared.ingress_submission_id,
            prepared.operation_id,
        ),
        "provisioning_success_state_invalid",
    )

    assert _joint_success_snapshot(migrated_database, prepared.operation_id) == before


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("target", "table_name"),
    [
        ("runtime", "runtime_submissions"),
        ("ingress", "ingress_submissions"),
        ("operation", "provisioning_operations"),
    ],
)
def test_finalize_provisioning_success_requires_each_conditional_update_to_change_one_row(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    table_name: str,
) -> None:
    from fstack.ingress_state import finalize_provisioning_success, mark_submitted
    from fstack.runtime_state import persist_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key=f"joint-success-zero-row-{target}",
    )
    persist_submitted(
        prepared.runtime_submission_id,
        f"docker-api-joint-zero-row-{target}",
    )
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    before = _joint_success_snapshot(migrated_database, prepared.operation_id)
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE FUNCTION skip_{target}_update() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$
                """
            )
        )
        connection.execute(
            text(
                f"""
                CREATE TRIGGER aaa_skip_{target}_update
                BEFORE UPDATE ON {table_name}
                FOR EACH ROW EXECUTE FUNCTION skip_{target}_update()
                """
            )
        )

    _assert_error(
        lambda: finalize_provisioning_success(
            prepared.runtime_submission_id,
            prepared.ingress_submission_id,
            prepared.operation_id,
        ),
        "provisioning_success_state_invalid",
    )

    assert _joint_success_snapshot(migrated_database, prepared.operation_id) == before


@pytest.mark.postgres
def test_finalize_provisioning_success_requires_success_audit_insert_to_create_one_row(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import finalize_provisioning_success, mark_submitted
    from fstack.runtime_state import persist_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="joint-success-zero-row-audit",
    )
    persist_submitted(prepared.runtime_submission_id, "docker-api-joint-zero-row-audit")
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    before = _joint_success_snapshot(migrated_database, prepared.operation_id)
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION skip_provisioning_succeeded_insert() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.event_type = 'provisioning.succeeded' THEN
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
                CREATE TRIGGER aaa_skip_provisioning_succeeded_insert
                BEFORE INSERT ON audit_events
                FOR EACH ROW EXECUTE FUNCTION skip_provisioning_succeeded_insert()
                """
            )
        )

    try:
        _assert_error(
            lambda: finalize_provisioning_success(
                prepared.runtime_submission_id,
                prepared.ingress_submission_id,
                prepared.operation_id,
            ),
            "provisioning_success_state_invalid",
        )
    finally:
        with migrated_database.begin() as connection:
            connection.execute(
                text("DROP TRIGGER aaa_skip_provisioning_succeeded_insert ON audit_events")
            )
            connection.execute(text("DROP FUNCTION skip_provisioning_succeeded_insert()"))

    assert _joint_success_snapshot(migrated_database, prepared.operation_id) == before


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("target", "target_sql"),
    [
        ("runtime", "UPDATE runtime_submissions SET state='confirmed'"),
        ("ingress", "UPDATE ingress_submissions SET state='confirmed'"),
        ("operation", "UPDATE provisioning_operations SET state='succeeded'"),
        (
            "success-audit",
            "INSERT INTO audit_events ( id, organization_id, operation_id, event_type,",
        ),
    ],
)
def test_finalize_provisioning_success_requires_exact_returning_identity(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    target_sql: str,
) -> None:
    from fstack import ingress_state
    from fstack.runtime_state import persist_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key=f"joint-success-wrong-returning-{target}",
    )
    persist_submitted(
        prepared.runtime_submission_id,
        f"docker-api-joint-wrong-returning-{target}",
    )
    ingress_state.mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    before = _joint_success_snapshot(migrated_database, prepared.operation_id)
    injected_engine = create_engine(
        ingress_state.runtime_submission._database_url(),
        pool_pre_ping=True,
        hide_parameters=True,
    )
    proxy = _ReturningIdentityEngine(injected_engine, target_sql, UUID(int=0))
    monkeypatch.setattr(ingress_state, "_engine", lambda: proxy)

    _assert_error(
        lambda: ingress_state.finalize_provisioning_success(
            prepared.runtime_submission_id,
            prepared.ingress_submission_id,
            prepared.operation_id,
        ),
        "provisioning_success_state_invalid",
    )

    assert proxy.connection is not None
    assert proxy.connection.matches == 1
    assert _joint_success_snapshot(migrated_database, prepared.operation_id) == before


@pytest.mark.postgres
@pytest.mark.parametrize("fail_statement", range(1, 9))
def test_finalize_provisioning_success_statement_failure_rolls_back_everything(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    fail_statement: int,
) -> None:
    from fstack import ingress_state
    from fstack.runtime_state import persist_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key=f"joint-success-rollback-{fail_statement}",
    )
    persist_submitted(
        prepared.runtime_submission_id,
        f"docker-api-joint-rollback-{fail_statement}",
    )
    ingress_state.mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    before = _joint_success_snapshot(migrated_database, prepared.operation_id)
    injected_engine = create_engine(
        ingress_state.runtime_submission._database_url(),
        pool_pre_ping=True,
        hide_parameters=True,
    )
    calls = 0

    @event.listens_for(injected_engine, "before_cursor_execute")
    def fail_boundary(*_args: object) -> None:
        nonlocal calls
        calls += 1
        if calls == fail_statement:
            raise OperationalError("statement", {}, RuntimeError("joint-success-marker"))

    monkeypatch.setattr(ingress_state, "_engine", lambda: injected_engine)

    _assert_error(
        lambda: ingress_state.finalize_provisioning_success(
            prepared.runtime_submission_id,
            prepared.ingress_submission_id,
            prepared.operation_id,
        ),
        "provisioning_success_infrastructure",
    )

    assert calls == fail_statement
    assert _joint_success_snapshot(migrated_database, prepared.operation_id) == before


@pytest.mark.postgres
def test_companion_operation_mismatch_changes_no_row_or_audit(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import mark_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-operation-mismatch",
    )
    before = _snapshot(migrated_database, prepared.ingress_submission_id)

    _assert_error(
        lambda: mark_submitted(prepared.ingress_submission_id, uuid4()),
        "ingress_submission_operation_mismatch",
    )

    assert _snapshot(migrated_database, prepared.ingress_submission_id) == before


@pytest.mark.postgres
def test_missing_submission_is_sanitized_and_changes_no_audit(
    migrated_database: Engine,
) -> None:
    from fstack.ingress_state import mark_submitted

    with migrated_database.connect() as connection:
        before = connection.execute(text("SELECT count(*) FROM audit_events")).scalar_one()

    _assert_error(
        lambda: mark_submitted(uuid4(), uuid4()),
        "ingress_submission_not_found",
    )

    with migrated_database.connect() as connection:
        after = connection.execute(text("SELECT count(*) FROM audit_events")).scalar_one()
    assert after == before


@pytest.mark.postgres
def test_operation_must_still_be_running(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import mark_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-operation-not-running",
    )
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE provisioning_operations DISABLE TRIGGER tr_operations_lifecycle_guard"
            )
        )
        connection.execute(
            text(
                "UPDATE provisioning_operations SET state='pending', started_at=NULL "
                "WHERE id=:operation_id"
            ),
            {"operation_id": prepared.operation_id},
        )
        connection.execute(
            text("ALTER TABLE provisioning_operations ENABLE TRIGGER tr_operations_lifecycle_guard")
        )
    before = _snapshot(migrated_database, prepared.ingress_submission_id)

    _assert_error(
        lambda: mark_submitted(prepared.ingress_submission_id, prepared.operation_id),
        "ingress_submission_operation_not_running",
    )

    assert _snapshot(migrated_database, prepared.ingress_submission_id) == before


@pytest.mark.postgres
def test_exact_organization_deployment_agreement_is_required(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import mark_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-agreement",
    )
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions DROP CONSTRAINT "
                "fk_ingress_submissions_exact_operation"
            )
        )
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions DROP CONSTRAINT "
                "fk_ingress_submissions_deployment_org"
            )
        )
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions DISABLE TRIGGER "
                "tr_ingress_submissions_lifecycle_guard"
            )
        )
        connection.execute(
            text("UPDATE ingress_submissions SET organization_id=:organization_id WHERE id=:id"),
            {"id": prepared.ingress_submission_id, "organization_id": uuid4()},
        )
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions ENABLE TRIGGER "
                "tr_ingress_submissions_lifecycle_guard"
            )
        )
    before = _snapshot(migrated_database, prepared.ingress_submission_id)

    _assert_error(
        lambda: mark_submitted(prepared.ingress_submission_id, prepared.operation_id),
        "ingress_submission_agreement_invalid",
    )

    assert _snapshot(migrated_database, prepared.ingress_submission_id) == before


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("setup_sql", "update_sql", "value"),
    [
        (
            "ALTER TABLE ingress_submissions DROP CONSTRAINT ck_ingress_provider",
            "provider=:value",
            "other",
        ),
        (
            "ALTER TABLE ingress_submissions DROP CONSTRAINT ck_ingress_generation",
            "generation=:value",
            2,
        ),
        ("", "submission_key=:value", "0" * 64),
        ("", "route_fingerprint=:value", "0" * 64),
        (
            "ALTER TABLE ingress_submissions DROP CONSTRAINT ck_ingress_deterministic_route_id",
            "route_id=:value",
            "fstack-crusher-ingress-" + "0" * 32,
        ),
    ],
    ids=["provider", "generation", "submission-key", "route-fingerprint", "route-id"],
)
def test_corrupt_sealed_identity_fails_closed_without_mutation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    setup_sql: str,
    update_sql: str,
    value: object,
) -> None:
    from fstack.ingress_state import mark_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key=f"ingress-state-corrupt-{update_sql.split('=')[0]}",
    )
    with migrated_database.begin() as connection:
        if setup_sql:
            connection.execute(text(setup_sql))
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions DISABLE TRIGGER "
                "tr_ingress_submissions_lifecycle_guard"
            )
        )
        connection.execute(
            text(f"UPDATE ingress_submissions SET {update_sql} WHERE id=:id"),
            {"id": prepared.ingress_submission_id, "value": value},
        )
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions ENABLE TRIGGER "
                "tr_ingress_submissions_lifecycle_guard"
            )
        )
    before = _snapshot(migrated_database, prepared.ingress_submission_id)

    _assert_error(
        lambda: mark_submitted(prepared.ingress_submission_id, prepared.operation_id),
        "ingress_submission_identity_invalid",
    )

    assert _snapshot(migrated_database, prepared.ingress_submission_id) == before


@pytest.mark.postgres
@pytest.mark.parametrize(
    "corruption_sql",
    [
        "submitted_at=clock_timestamp()",
        "state='submitted'",
    ],
    ids=["prepared-with-submitted-timestamp", "submitted-without-timestamp"],
)
def test_corrupt_state_bundle_fails_closed(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    corruption_sql: str,
) -> None:
    from fstack.ingress_state import mark_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-corrupt-bundle",
    )
    with migrated_database.begin() as connection:
        connection.execute(
            text("ALTER TABLE ingress_submissions DROP CONSTRAINT ck_ingress_state_bundle")
        )
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions DISABLE TRIGGER "
                "tr_ingress_submissions_lifecycle_guard"
            )
        )
        connection.execute(
            text(f"UPDATE ingress_submissions SET {corruption_sql} WHERE id=:id"),
            {"id": prepared.ingress_submission_id},
        )
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions ENABLE TRIGGER "
                "tr_ingress_submissions_lifecycle_guard"
            )
        )
    before = _snapshot(migrated_database, prepared.ingress_submission_id)

    _assert_error(
        lambda: mark_submitted(prepared.ingress_submission_id, prepared.operation_id),
        "ingress_submission_state_invalid",
    )

    assert _snapshot(migrated_database, prepared.ingress_submission_id) == before


@pytest.mark.postgres
def test_submitted_timestamp_before_prepared_timestamp_fails_closed(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import mark_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-submitted-before-prepared",
    )
    with migrated_database.begin() as connection:
        prepared_at = connection.execute(
            text("SELECT prepared_at FROM ingress_submissions WHERE id=:id"),
            {"id": prepared.ingress_submission_id},
        ).scalar_one()
        connection.execute(
            text("ALTER TABLE ingress_submissions DROP CONSTRAINT ck_ingress_timestamps_monotonic")
        )
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions DISABLE TRIGGER "
                "tr_ingress_submissions_lifecycle_guard"
            )
        )
        connection.execute(
            text(
                "UPDATE ingress_submissions SET state='submitted', submitted_at=:submitted_at "
                "WHERE id=:id"
            ),
            {
                "id": prepared.ingress_submission_id,
                "submitted_at": prepared_at - timedelta(seconds=1),
            },
        )
        connection.execute(
            text(
                "ALTER TABLE ingress_submissions ENABLE TRIGGER "
                "tr_ingress_submissions_lifecycle_guard"
            )
        )
    before = _snapshot(migrated_database, prepared.ingress_submission_id)

    _assert_error(
        lambda: mark_submitted(prepared.ingress_submission_id, prepared.operation_id),
        "ingress_submission_state_invalid",
    )

    assert _snapshot(migrated_database, prepared.ingress_submission_id) == before


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("prepared_at", datetime(2026, 1, 1)),
        ("prepared_at", "not-a-datetime"),
        ("prepared_at", _DatetimeSubclass(2026, 1, 1, tzinfo=UTC)),
        ("submitted_at", datetime(2026, 1, 1)),
        ("submitted_at", "not-a-datetime"),
        ("submitted_at", _DatetimeSubclass(2026, 1, 1, tzinfo=UTC)),
    ],
    ids=[
        "prepared-naive",
        "prepared-wrong-type",
        "prepared-subclass",
        "submitted-naive",
        "submitted-wrong-type",
        "submitted-subclass",
    ],
)
def test_runtime_timestamp_values_require_exact_aware_datetimes(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    from fstack import ingress_state

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key=f"ingress-state-runtime-timestamp-{field}-{type(value).__name__}",
    )
    row = _selected_row(migrated_database, prepared.ingress_submission_id)
    if field == "submitted_at":
        row["state"] = "submitted"
        row["submitted_at"] = value
    else:
        row[field] = value
    fake_engine = _StaticEngine(_StaticBegin(_StaticConnection(_StaticResult(row))))
    monkeypatch.setattr(ingress_state, "_engine", lambda: cast(Engine, fake_engine))

    _assert_error(
        lambda: ingress_state.mark_submitted(
            prepared.ingress_submission_id,
            prepared.operation_id,
        ),
        "ingress_submission_state_invalid",
    )

    assert fake_engine.dispose_calls == 1


@pytest.mark.postgres
def test_aware_timestamp_runtime_seam_accepts_submitted_replay(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import ingress_state

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-aware-runtime-timestamps",
    )
    row = _selected_row(migrated_database, prepared.ingress_submission_id)
    row["state"] = "submitted"
    row["prepared_at"] = datetime(2026, 1, 1, tzinfo=UTC)
    row["submitted_at"] = datetime(2026, 1, 2, tzinfo=UTC)
    fake_engine = _StaticEngine(_StaticBegin(_StaticConnection(_StaticResult(row))))
    monkeypatch.setattr(ingress_state, "_engine", lambda: cast(Engine, fake_engine))

    ingress_state.mark_submitted(prepared.ingress_submission_id, prepared.operation_id)

    assert fake_engine.dispose_calls == 1


@pytest.mark.postgres
@pytest.mark.parametrize("terminal_state", ["confirmed", "failed"])
def test_terminal_submission_cannot_be_resubmitted(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    terminal_state: str,
) -> None:
    from fstack.ingress_state import mark_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key=f"ingress-state-terminal-{terminal_state}",
    )
    if terminal_state == "confirmed":
        mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    with migrated_database.begin() as connection:
        if terminal_state == "confirmed":
            submitted_at = connection.execute(
                text("SELECT submitted_at FROM ingress_submissions WHERE id=:id"),
                {"id": prepared.ingress_submission_id},
            ).scalar_one()
            assert type(submitted_at) is datetime
            connection.execute(
                text(
                    "UPDATE ingress_submissions SET state='confirmed', "
                    "confirmed_at=:confirmed_at WHERE id=:id"
                ),
                {
                    "id": prepared.ingress_submission_id,
                    "confirmed_at": submitted_at + timedelta(seconds=1),
                },
            )
            connection.execute(
                text(
                    "WITH boundary AS (SELECT clock_timestamp() AS now) "
                    "UPDATE provisioning_operations SET state='succeeded', "
                    "completed_at=boundary.now, updated_at=boundary.now FROM boundary "
                    "WHERE id=:operation_id AND state='running'"
                ),
                {"operation_id": prepared.operation_id},
            )
        else:
            prepared_at = connection.execute(
                text("SELECT prepared_at FROM ingress_submissions WHERE id=:id"),
                {"id": prepared.ingress_submission_id},
            ).scalar_one()
            assert type(prepared_at) is datetime
            connection.execute(
                text(
                    "UPDATE ingress_submissions SET state='failed', failure_code='ingress_failed', "
                    "failed_at=:failed_at WHERE id=:id"
                ),
                {
                    "id": prepared.ingress_submission_id,
                    "failed_at": prepared_at + timedelta(seconds=1),
                },
            )
            connection.execute(
                text(
                    "WITH boundary AS (SELECT clock_timestamp() AS now) "
                    "UPDATE provisioning_operations SET state='failed', "
                    "completed_at=boundary.now, last_error_code='ingress_failed', "
                    "updated_at=boundary.now FROM boundary "
                    "WHERE id=:operation_id AND state='running'"
                ),
                {"operation_id": prepared.operation_id},
            )
    before = _snapshot(migrated_database, prepared.ingress_submission_id)

    _assert_error(
        lambda: mark_submitted(prepared.ingress_submission_id, prepared.operation_id),
        "ingress_submission_terminal",
    )

    assert _snapshot(migrated_database, prepared.ingress_submission_id) == before


@pytest.mark.postgres
def test_submitted_timestamp_is_immutable_against_direct_dml(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import mark_submitted

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-timestamp-trigger",
    )
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    before = _snapshot(migrated_database, prepared.ingress_submission_id)
    submitted_at = before[0]["submitted_at"]
    assert isinstance(submitted_at, datetime)

    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text("UPDATE ingress_submissions SET submitted_at=:changed WHERE id=:id"),
            {
                "id": prepared.ingress_submission_id,
                "changed": submitted_at + timedelta(seconds=1),
            },
        )

    assert _snapshot(migrated_database, prepared.ingress_submission_id) == before


@pytest.mark.postgres
def test_ingress_schema_contains_no_provider_response_or_attempt_evidence(
    migrated_database: Engine,
) -> None:
    columns = {
        column["name"] for column in inspect(migrated_database).get_columns("ingress_submissions")
    }
    assert columns == {
        "id",
        "organization_id",
        "operation_id",
        "deployment_id",
        "provider",
        "generation",
        "submission_key",
        "route_fingerprint",
        "route_id",
        "state",
        "failure_code",
        "prepared_at",
        "submitted_at",
        "confirmed_at",
        "failed_at",
    }
    assert not (
        {"provider_id", "response_body", "host", "socket", "listener", "attempt_id", "lease_id"}
        & columns
    )


@pytest.mark.postgres
@pytest.mark.parametrize("fail_statement", range(1, 5))
def test_sql_failure_at_each_statement_boundary_rolls_back_without_changes(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    fail_statement: int,
) -> None:
    from fstack import ingress_state

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key=f"ingress-state-rollback-{fail_statement}",
    )
    before = _snapshot(migrated_database, prepared.ingress_submission_id)
    injected_engine = create_engine(
        ingress_state.runtime_submission._database_url(),
        pool_pre_ping=True,
        hide_parameters=True,
    )
    calls = 0
    dispose_calls = 0
    actual_dispose = injected_engine.dispose

    def tracked_dispose() -> None:
        nonlocal dispose_calls
        dispose_calls += 1
        actual_dispose()

    cast(Any, injected_engine).dispose = tracked_dispose

    @event.listens_for(injected_engine, "before_cursor_execute")
    def fail_boundary(*_args: object) -> None:
        nonlocal calls
        calls += 1
        if calls == fail_statement:
            raise OperationalError("statement", {}, RuntimeError("unique-rollback-marker"))

    monkeypatch.setattr(ingress_state, "_engine", lambda: injected_engine)

    _assert_error(
        lambda: ingress_state.mark_submitted(
            prepared.ingress_submission_id,
            prepared.operation_id,
        ),
        "ingress_state_infrastructure",
    )

    assert calls == fail_statement
    assert dispose_calls == 1
    assert _snapshot(migrated_database, prepared.ingress_submission_id) == before


@pytest.mark.postgres
def test_infrastructure_error_is_clean_on_all_public_trace_and_log_surfaces(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from fstack import ingress_state

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-sanitization",
    )
    marker = "unique-sql-secret-marker"
    injected_engine = create_engine(
        ingress_state.runtime_submission._database_url(),
        pool_pre_ping=True,
        hide_parameters=True,
    )
    sealed = _ingress_row(migrated_database, prepared.ingress_submission_id)
    calls = 0

    @event.listens_for(injected_engine, "before_cursor_execute")
    def fail_with_marker(*_args: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OperationalError("secret SQL " + marker, {"secret": marker}, RuntimeError(marker))

    monkeypatch.setattr(ingress_state, "_engine", lambda: injected_engine)
    caplog.set_level(logging.DEBUG)

    with pytest.raises(ingress_state.IngressStateError) as exc_info:
        ingress_state.mark_submitted(prepared.ingress_submission_id, prepared.operation_id)

    error = exc_info.value
    surfaces = [
        str(error),
        repr(error),
        repr(error.__cause__),
        repr(error.__context__),
        "".join(traceback.format_exception(error)),
        caplog.text,
    ]
    current = error.__traceback__
    while current is not None:
        if current.tb_frame.f_globals.get("__name__") == ingress_state.__name__:
            surfaces.append(repr(current.tb_frame.f_locals))
        current = current.tb_next
    rendered = "\n".join(surfaces)
    assert error.code == "ingress_state_infrastructure"
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in rendered
    assert "secret://" not in rendered
    assert str(sealed["route_id"]) not in rendered
    assert str(sealed["route_fingerprint"]) not in rendered
    assert VALID_BODY["public_domain"] not in rendered


@pytest.mark.postgres
@pytest.mark.parametrize(
    "phase", ["begin", "enter", "execute", "mappings", "result", "scalar", "exit"]
)
def test_unexpected_transaction_and_result_exceptions_are_fully_sanitized(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    phase: str,
) -> None:
    from fstack import ingress_state

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key=f"ingress-state-unexpected-{phase}",
    )
    row = _selected_row(migrated_database, prepared.ingress_submission_id)
    if phase != "scalar":
        row["state"] = "submitted"
        row["submitted_at"] = cast(datetime, row["prepared_at"]) + timedelta(seconds=1)
    marker = f"unique-{phase}-value-error"
    failure = ValueError(marker)
    result: _StaticResult
    if phase == "result":
        result = _OneFailureResult(row, failure)
    else:
        result = _StaticResult(row, failure if phase == "mappings" else None)
    if phase == "scalar":
        connection = _SequenceConnection([_StaticResult(row), _ScalarFailureResult(row, failure)])
    else:
        connection = _StaticConnection(result, failure if phase == "execute" else None)
    begin = _StaticBegin(
        connection,
        enter_failure=failure if phase == "enter" else None,
        exit_failure=failure if phase == "exit" else None,
    )
    fake_engine = _StaticEngine(
        begin,
        begin_failure=failure if phase == "begin" else None,
    )
    monkeypatch.setattr(ingress_state, "_engine", lambda: cast(Engine, fake_engine))
    caplog.set_level(logging.DEBUG)

    _assert_sanitized_error(
        lambda: ingress_state.mark_submitted(
            prepared.ingress_submission_id,
            prepared.operation_id,
        ),
        "ingress_state_infrastructure",
        marker=marker,
        module_name=ingress_state.__name__,
        caplog=caplog,
    )

    assert fake_engine.dispose_calls == 1


@pytest.mark.postgres
def test_dispose_value_error_is_fully_sanitized_and_attempted_once(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from fstack import ingress_state

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-dispose-value-error",
    )
    row = _selected_row(migrated_database, prepared.ingress_submission_id)
    row["state"] = "submitted"
    row["submitted_at"] = cast(datetime, row["prepared_at"]) + timedelta(seconds=1)
    marker = "unique-dispose-value-error"
    fake_engine = _StaticEngine(
        _StaticBegin(_StaticConnection(_StaticResult(row))),
        dispose_failure=ValueError(marker),
    )
    monkeypatch.setattr(ingress_state, "_engine", lambda: cast(Engine, fake_engine))
    caplog.set_level(logging.DEBUG)

    _assert_sanitized_error(
        lambda: ingress_state.mark_submitted(
            prepared.ingress_submission_id,
            prepared.operation_id,
        ),
        "ingress_state_infrastructure",
        marker=marker,
        module_name=ingress_state.__name__,
        caplog=caplog,
    )

    assert fake_engine.dispose_calls == 1


@pytest.mark.postgres
def test_repeated_calls_dispose_every_created_pool(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import ingress_state

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-pool-cleanup",
    )
    actual_create_engine = create_engine
    engines: list[Engine] = []
    dispose_counts: list[int] = []

    def tracked_create_engine(*args: Any, **kwargs: Any) -> Engine:
        engine = actual_create_engine(*args, **kwargs)
        dispose_counts.append(0)
        index = len(dispose_counts) - 1
        actual_dispose = engine.dispose

        def tracked_dispose() -> None:
            dispose_counts[index] += 1
            actual_dispose()

        cast(Any, engine).dispose = tracked_dispose
        engines.append(engine)
        return engine

    monkeypatch.setattr(ingress_state, "create_engine", tracked_create_engine)

    for _ in range(8):
        ingress_state.mark_submitted(prepared.ingress_submission_id, prepared.operation_id)

    assert len(engines) == 8
    assert dispose_counts == [1] * 8
    assert all(cast(Any, engine.pool).checkedout() == 0 for engine in engines)


@pytest.mark.postgres
def test_known_state_error_disposes_created_engine_exactly_once(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import ingress_state

    prepared = _prepared_bundle(
        migrated_database,
        monkeypatch,
        idempotency_key="ingress-state-known-error-dispose",
    )
    actual_create_engine = create_engine
    dispose_calls = 0

    def tracked_create_engine(*args: Any, **kwargs: Any) -> Engine:
        engine = actual_create_engine(*args, **kwargs)
        actual_dispose = engine.dispose

        def tracked_dispose() -> None:
            nonlocal dispose_calls
            dispose_calls += 1
            actual_dispose()

        cast(Any, engine).dispose = tracked_dispose
        return engine

    monkeypatch.setattr(ingress_state, "create_engine", tracked_create_engine)

    _assert_error(
        lambda: ingress_state.mark_submitted(prepared.ingress_submission_id, uuid4()),
        "ingress_submission_operation_mismatch",
    )

    assert dispose_calls == 1
