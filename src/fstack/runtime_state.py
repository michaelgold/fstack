"""PostgreSQL state transitions around Temporal-owned runtime application."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, create_engine, text

from fstack import runtime_submission
from fstack.runtime_application import RuntimePlan

_EXTERNAL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}\Z")
_ERROR_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")


class RuntimeStateError(RuntimeError):
    """The exact runtime submission cannot perform the requested transition."""


def _engine() -> Engine:
    return create_engine(
        runtime_submission._database_url(), pool_pre_ping=True, hide_parameters=True
    )


def _submission(connection: Connection, submission_id: UUID) -> Mapping[str, Any]:
    row = (
        connection.execute(
            text(
                """
                SELECT submission.*, operation.state AS operation_state,
                       operation.actor_principal, deployment.crusher_image,
                       deployment.public_domain,
                       database_binding.secret_ref AS database_secret_ref,
                       storage_binding.secret_ref AS object_storage_secret_ref
                FROM runtime_submissions AS submission
                JOIN provisioning_operations AS operation
                  ON operation.id = submission.operation_id
                 AND operation.organization_id = submission.organization_id
                 AND operation.deployment_id = submission.deployment_id
                JOIN deployments AS deployment
                  ON deployment.id = submission.deployment_id
                 AND deployment.organization_id = submission.organization_id
                JOIN database_bindings AS database_binding
                  ON database_binding.deployment_id = submission.deployment_id
                 AND database_binding.organization_id = submission.organization_id
                JOIN storage_bindings AS storage_binding
                  ON storage_binding.deployment_id = submission.deployment_id
                 AND storage_binding.organization_id = submission.organization_id
                WHERE submission.id = :submission_id
                FOR UPDATE OF submission, operation
                FOR SHARE OF deployment, database_binding, storage_binding
                """
            ),
            {"submission_id": submission_id},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise RuntimeStateError("runtime_submission_not_found")
    return dict(row)


def _result(row: Mapping[str, Any]) -> runtime_submission.PreparedRuntimeSubmission:
    return runtime_submission._result(row)


def load_plan(submission_id: UUID) -> RuntimePlan:
    engine = _engine()
    try:
        with engine.begin() as connection:
            row = _submission(connection, submission_id)
            if row["state"] in {"confirmed", "failed"}:
                raise RuntimeStateError("runtime_submission_terminal")
            operation = {
                "operation_id": row["operation_id"],
                "organization_id": row["organization_id"],
                "deployment_id": row["deployment_id"],
                "crusher_image": row["crusher_image"],
                "public_domain": row["public_domain"],
                "database_secret_ref": row["database_secret_ref"],
                "object_storage_secret_ref": row["object_storage_secret_ref"],
            }
            canonical = runtime_submission._canonical_spec(
                operation,
                api_container_name=row["api_container_name"],
                migration_container_name=row["migration_container_name"],
                docker_network=runtime_submission._docker_network(),
                secret_stage_root=runtime_submission._secret_stage_root(),
            )
            if (
                row["provider"] != runtime_submission.PROVIDER
                or row["generation"] != runtime_submission.GENERATION
                or row["submission_key"] != runtime_submission._submission_key(row["operation_id"])
                or runtime_submission._spec_fingerprint(canonical) != row["spec_fingerprint"]
            ):
                raise RuntimeStateError("sealed_spec_mismatch")
            source = os.environ.get("FSTACK_LOCAL_SECRET_ROOT")
            if not source:
                raise RuntimeStateError("secret_source_not_configured")
            return RuntimePlan(
                submission_id=row["id"],
                operation_id=row["operation_id"],
                deployment_id=row["deployment_id"],
                submission_key=row["submission_key"],
                spec_fingerprint=row["spec_fingerprint"],
                canonical_spec=canonical,
                database_secret_ref=row["database_secret_ref"],
                source_root=Path(source),
                stage_root=runtime_submission._secret_stage_root(),
            )
    finally:
        engine.dispose()


def mark_running(
    submission_id: UUID, expected_operation_id: UUID
) -> runtime_submission.PreparedRuntimeSubmission:
    engine = _engine()
    try:
        with engine.begin() as connection:
            row = _submission(connection, submission_id)
            if row["operation_id"] != expected_operation_id:
                raise RuntimeStateError("runtime_submission_operation_mismatch")
            if row["state"] in {"confirmed", "failed"}:
                return _result(row)
            if row["operation_state"] == "pending":
                connection.execute(
                    text(
                        """
                        WITH boundary AS (SELECT clock_timestamp() AS now)
                        UPDATE provisioning_operations
                        SET state = 'running', started_at = boundary.now, updated_at = boundary.now
                        FROM boundary WHERE id = :operation_id AND state = 'pending'
                        """
                    ),
                    {"operation_id": row["operation_id"]},
                )
                connection.execute(
                    text(
                        """
                        INSERT INTO audit_events (
                            id, organization_id, operation_id, event_type,
                            actor_principal, payload
                        ) VALUES (
                            :id, :organization_id, :operation_id, 'provisioning.started',
                            :actor_principal, CAST(:payload AS jsonb)
                        )
                        """
                    ),
                    {
                        "id": uuid4(),
                        "organization_id": row["organization_id"],
                        "operation_id": row["operation_id"],
                        "actor_principal": row["actor_principal"],
                        "payload": json.dumps(
                            {
                                "provider": runtime_submission.PROVIDER,
                                "runtime_submission_id": str(row["id"]),
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    },
                )
            elif row["operation_state"] != "running":
                raise RuntimeStateError("operation_not_running")
            return _result(row)
    finally:
        engine.dispose()


def persist_submitted(submission_id: UUID, external_id: str) -> None:
    if _EXTERNAL_ID.fullmatch(external_id) is None:
        raise RuntimeStateError("invalid_provider_identity")
    engine = _engine()
    try:
        with engine.begin() as connection:
            row = _submission(connection, submission_id)
            if row["state"] == "prepared":
                connection.execute(
                    text(
                        """
                        UPDATE runtime_submissions
                        SET state = 'submitted', external_id = :external_id,
                            submitted_at = clock_timestamp()
                        WHERE id = :submission_id AND state = 'prepared'
                        """
                    ),
                    {"submission_id": submission_id, "external_id": external_id},
                )
            elif row["state"] in {"submitted", "confirmed", "failed"}:
                if row["external_id"] != external_id:
                    raise RuntimeStateError("provider_identity_conflict")
            else:
                raise RuntimeStateError("runtime_submission_not_submittable")
    finally:
        engine.dispose()


def finalize_success(
    submission_id: UUID, expected_operation_id: UUID
) -> runtime_submission.PreparedRuntimeSubmission:
    engine = _engine()
    try:
        with engine.begin() as connection:
            row = _submission(connection, submission_id)
            if row["operation_id"] != expected_operation_id:
                raise RuntimeStateError("runtime_submission_operation_mismatch")
            if row["state"] == "confirmed":
                return _result(row)
            if row["state"] != "submitted" or row["operation_state"] != "running":
                raise RuntimeStateError("runtime_submission_not_confirmable")
            connection.execute(
                text(
                    """
                    WITH boundary AS (SELECT clock_timestamp() AS now)
                    UPDATE runtime_submissions
                    SET state = 'confirmed', confirmed_at = boundary.now
                    FROM boundary WHERE id = :submission_id AND state = 'submitted'
                    """
                ),
                {"submission_id": submission_id},
            )
            connection.execute(
                text(
                    """
                    WITH boundary AS (SELECT clock_timestamp() AS now)
                    UPDATE provisioning_operations
                    SET state = 'succeeded', completed_at = boundary.now, updated_at = boundary.now
                    FROM boundary WHERE id = :operation_id AND state = 'running'
                    """
                ),
                {"operation_id": row["operation_id"]},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO audit_events (
                        id, organization_id, operation_id, event_type, actor_principal, payload
                    ) VALUES (
                        :id, :organization_id, :operation_id, 'provisioning.succeeded',
                        :actor_principal, CAST(:payload AS jsonb)
                    )
                    """
                ),
                {
                    "id": uuid4(),
                    "organization_id": row["organization_id"],
                    "operation_id": row["operation_id"],
                    "actor_principal": row["actor_principal"],
                    "payload": json.dumps(
                        {"runtime_submission_id": str(row["id"])},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                },
            )
            updated = dict(row)
            updated["state"] = "confirmed"
            return _result(updated)
    finally:
        engine.dispose()


def finalize_prepare_failure(
    operation_id: UUID | str, error_code: str
) -> runtime_submission.PreparedRuntimeSubmission:
    """Fail an exact prepared submission after a permanent prepare replay error."""
    operation_uuid = operation_id if isinstance(operation_id, UUID) else UUID(operation_id)
    if _ERROR_CODE.fullmatch(error_code) is None:
        error_code = "prepare_failed"
    engine = _engine()
    try:
        with engine.begin() as connection:
            operation = (
                connection.execute(
                    text(
                        """
                        SELECT id, organization_id, deployment_id, state, actor_principal
                        FROM provisioning_operations
                        WHERE id = :operation_id
                        FOR UPDATE
                        """
                    ),
                    {"operation_id": operation_uuid},
                )
                .mappings()
                .one_or_none()
            )
            if operation is None:
                raise RuntimeStateError("operation_not_found")
            submission_row = (
                connection.execute(
                    text(
                        """
                        SELECT * FROM runtime_submissions
                        WHERE operation_id = :operation_id
                        FOR UPDATE
                        """
                    ),
                    {"operation_id": operation_uuid},
                )
                .mappings()
                .one_or_none()
            )
            if submission_row is None:
                raise RuntimeStateError("runtime_submission_not_found")
            row: dict[str, Any] = dict(submission_row)
            if row["state"] == "failed":
                if row["failure_code"] != error_code or operation["state"] != "failed":
                    raise RuntimeStateError("failure_code_conflict")
                return _result(row)
            if row["state"] != "prepared" or operation["state"] not in {"pending", "running"}:
                raise RuntimeStateError("runtime_submission_not_failable")
            connection.execute(
                text(
                    """
                    WITH boundary AS (SELECT clock_timestamp() AS now)
                    UPDATE runtime_submissions
                    SET state = 'failed', failure_code = :error_code, failed_at = boundary.now
                    FROM boundary
                    WHERE id = :submission_id AND state = 'prepared'
                    """
                ),
                {"submission_id": row["id"], "error_code": error_code},
            )
            if operation["state"] == "pending":
                connection.execute(
                    text(
                        """
                        WITH boundary AS (SELECT clock_timestamp() AS now)
                        UPDATE provisioning_operations
                        SET state = 'running', started_at = boundary.now, updated_at = boundary.now
                        FROM boundary
                        WHERE id = :operation_id AND state = 'pending'
                        """
                    ),
                    {"operation_id": operation_uuid},
                )
            connection.execute(
                text(
                    """
                    WITH boundary AS (SELECT clock_timestamp() AS now)
                    UPDATE provisioning_operations
                    SET state = 'failed', completed_at = boundary.now,
                        updated_at = boundary.now, last_error_code = :error_code
                    FROM boundary
                    WHERE id = :operation_id AND state = 'running'
                    """
                ),
                {"operation_id": operation_uuid, "error_code": error_code},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO audit_events (
                        id, organization_id, operation_id, event_type, actor_principal, payload
                    ) VALUES (
                        :id, :organization_id, :operation_id, 'provisioning.failed',
                        :actor_principal, CAST(:payload AS jsonb)
                    )
                    """
                ),
                {
                    "id": uuid4(),
                    "organization_id": operation["organization_id"],
                    "operation_id": operation_uuid,
                    "actor_principal": operation["actor_principal"],
                    "payload": json.dumps(
                        {
                            "error_code": error_code,
                            "runtime_submission_id": str(row["id"]),
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                },
            )
            row["state"] = "failed"
            return _result(row)
    finally:
        engine.dispose()


def finalize_failure(
    submission_id: UUID, expected_operation_id: UUID, error_code: str
) -> runtime_submission.PreparedRuntimeSubmission:
    if _ERROR_CODE.fullmatch(error_code) is None:
        error_code = "runtime_failed"
    engine = _engine()
    try:
        with engine.begin() as connection:
            row = _submission(connection, submission_id)
            if row["operation_id"] != expected_operation_id:
                raise RuntimeStateError("runtime_submission_operation_mismatch")
            if row["state"] == "failed":
                if row["failure_code"] != error_code:
                    raise RuntimeStateError("failure_code_conflict")
                return _result(row)
            if row["state"] == "confirmed" or row["operation_state"] not in {"pending", "running"}:
                raise RuntimeStateError("runtime_submission_not_failable")
            connection.execute(
                text(
                    """
                    WITH boundary AS (SELECT clock_timestamp() AS now)
                    UPDATE runtime_submissions
                    SET state = 'failed', failure_code = :error_code, failed_at = boundary.now
                    FROM boundary WHERE id = :submission_id AND state IN ('prepared', 'submitted')
                    """
                ),
                {"submission_id": submission_id, "error_code": error_code},
            )
            connection.execute(
                text(
                    """
                    WITH boundary AS (SELECT clock_timestamp() AS now)
                    UPDATE provisioning_operations
                    SET state = 'failed',
                        started_at = COALESCE(started_at, boundary.now),
                        completed_at = boundary.now, updated_at = boundary.now,
                        last_error_code = :error_code
                    FROM boundary
                    WHERE id = :operation_id AND state IN ('pending', 'running')
                    """
                ),
                {"operation_id": row["operation_id"], "error_code": error_code},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO audit_events (
                        id, organization_id, operation_id, event_type, actor_principal, payload
                    ) VALUES (
                        :id, :organization_id, :operation_id, 'provisioning.failed',
                        :actor_principal, CAST(:payload AS jsonb)
                    )
                    """
                ),
                {
                    "id": uuid4(),
                    "organization_id": row["organization_id"],
                    "operation_id": row["operation_id"],
                    "actor_principal": row["actor_principal"],
                    "payload": json.dumps(
                        {
                            "error_code": error_code,
                            "runtime_submission_id": str(row["id"]),
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                },
            )
            updated = dict(row)
            updated["state"] = "failed"
            return _result(updated)
    finally:
        engine.dispose()
