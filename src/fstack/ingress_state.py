"""Fail-closed PostgreSQL lifecycle transition for local Caddy ingress."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.engine import RowMapping

from fstack import billing, ingress_submission, runtime_submission

_ERROR_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")


class IngressStateError(RuntimeError):
    """The exact ingress submission cannot perform the requested transition."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _engine() -> Engine:
    return create_engine(
        runtime_submission._database_url(),
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _aware_datetime(value: object) -> bool:
    return type(value) is datetime and value.tzinfo is not None and value.utcoffset() is not None


def _state_code(row: Mapping[str, Any]) -> str | None:
    state = row["state"]
    prepared_at = row["prepared_at"]
    submitted_at = row["submitted_at"]
    confirmed_at = row["confirmed_at"]
    failed_at = row["failed_at"]
    failure_code = row["failure_code"]
    if type(state) is not str or not _aware_datetime(prepared_at):
        return "ingress_submission_state_invalid"
    if state == "prepared":
        valid = all(
            value is None for value in (submitted_at, confirmed_at, failed_at, failure_code)
        )
    elif state == "submitted":
        valid = (
            _aware_datetime(submitted_at)
            and submitted_at >= prepared_at
            and confirmed_at is None
            and failed_at is None
            and failure_code is None
        )
    elif state == "confirmed":
        valid = (
            _aware_datetime(submitted_at)
            and submitted_at >= prepared_at
            and _aware_datetime(confirmed_at)
            and confirmed_at >= submitted_at
            and failed_at is None
            and failure_code is None
        )
    elif state == "failed":
        valid = (
            (
                submitted_at is None
                or (_aware_datetime(submitted_at) and submitted_at >= prepared_at)
            )
            and confirmed_at is None
            and _aware_datetime(failed_at)
            and failed_at >= (submitted_at if submitted_at is not None else prepared_at)
            and type(failure_code) is str
            and _ERROR_CODE.fullmatch(failure_code) is not None
        )
    else:
        valid = False
    return None if valid else "ingress_submission_state_invalid"


def _validation_code(row: Mapping[str, Any], expected_operation_id: UUID) -> str | None:
    operation_id = row["operation_id"]
    identity_values = (
        operation_id,
        row["submission_organization_id"],
        row["submission_deployment_id"],
        row["operation_organization_id"],
        row["operation_deployment_id"],
        row["deployment_id"],
        row["deployment_organization_id"],
    )
    if any(type(value) is not UUID for value in identity_values):
        return "ingress_submission_identity_invalid"
    if operation_id != expected_operation_id:
        return "ingress_submission_operation_mismatch"
    if (
        row["submission_organization_id"] != row["operation_organization_id"]
        or row["submission_deployment_id"] != row["operation_deployment_id"]
        or row["submission_organization_id"] != row["deployment_organization_id"]
        or row["submission_deployment_id"] != row["deployment_id"]
    ):
        return "ingress_submission_agreement_invalid"
    deployment_id = row["submission_deployment_id"]
    public_domain = row["public_domain"]
    if (
        type(public_domain) is not str
        or type(row["provider"]) is not str
        or type(row["generation"]) is not int
        or type(row["submission_key"]) is not str
        or type(row["route_id"]) is not str
        or type(row["route_fingerprint"]) is not str
    ):
        return "ingress_submission_identity_invalid"
    route = ingress_submission._canonical_route(deployment_id, public_domain)
    valid = (
        row["provider"] == ingress_submission.PROVIDER
        and row["generation"] == ingress_submission.GENERATION
        and row["submission_key"] == ingress_submission._submission_key(operation_id)
        and row["route_id"] == ingress_submission._route_id(deployment_id)
        and row["route_fingerprint"] == ingress_submission._route_fingerprint(route)
    )
    if not valid:
        return "ingress_submission_identity_invalid"
    state_code = _state_code(row)
    if state_code is not None:
        return state_code
    if row["state"] in {"confirmed", "failed"}:
        return "ingress_submission_terminal"
    if type(row["operation_state"]) is not str or row["operation_state"] != "running":
        return "ingress_submission_operation_not_running"
    return None


def _lock_ingress_operation(
    connection: Connection,
    ingress_submission_id: UUID,
) -> bool:
    submission = (
        connection.execute(
            text("SELECT operation_id FROM ingress_submissions WHERE id=:submission_id"),
            {"submission_id": ingress_submission_id},
        )
        .mappings()
        .one_or_none()
    )
    if submission is None:
        return False
    operation = (
        connection.execute(
            text("SELECT id FROM provisioning_operations WHERE id=:operation_id FOR UPDATE"),
            {"operation_id": submission["operation_id"]},
        )
        .mappings()
        .one_or_none()
    )
    return operation is not None


def mark_submitted(ingress_submission_id: UUID, operation_id: UUID) -> None:
    """Persist exactly one immutable prepared-to-submitted transition."""
    identifiers_valid = type(ingress_submission_id) is UUID and type(operation_id) is UUID
    if not identifiers_valid:
        ingress_submission_id = None  # type: ignore[assignment]
        operation_id = None  # type: ignore[assignment]
        raise IngressStateError("ingress_state_invalid_identifier") from None
    engine: Engine | None = None
    error_code: str | None = None
    infrastructure_failure = False
    connection: Connection | None = None
    selected: RowMapping | None = None
    row: dict[str, Any] | None = None
    submitted_at: object = None
    try:
        try:
            engine = _engine()
            with engine.begin() as connection:
                operation_locked = _lock_ingress_operation(connection, ingress_submission_id)
                selected = (
                    None
                    if not operation_locked
                    else (
                        connection.execute(
                            text(
                                """
                            SELECT submission.operation_id,
                                   submission.organization_id AS submission_organization_id,
                                   submission.deployment_id AS submission_deployment_id,
                                   submission.provider, submission.generation,
                                   submission.submission_key, submission.route_fingerprint,
                                   submission.route_id, submission.state,
                                   submission.prepared_at,
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
                            FOR UPDATE OF submission
                            FOR SHARE OF deployment
                            """
                            ),
                            {"submission_id": ingress_submission_id},
                        )
                        .mappings()
                        .one_or_none()
                    )
                )
                if selected is None:
                    error_code = "ingress_submission_not_found"
                else:
                    row = dict(selected)
                    error_code = _validation_code(row, operation_id)
                    if error_code is None and row["state"] == "prepared":
                        submitted_at = connection.execute(
                            text(
                                """
                                WITH boundary AS (SELECT clock_timestamp() AS now)
                                UPDATE ingress_submissions
                                SET state = 'submitted', submitted_at = boundary.now
                                FROM boundary
                                WHERE id = :submission_id AND state = 'prepared'
                                RETURNING submitted_at
                                """
                            ),
                            {"submission_id": ingress_submission_id},
                        ).scalar_one_or_none()
                        if submitted_at is None:
                            error_code = "ingress_submission_state_invalid"
                    row.clear()
                    row = None
                    selected = None
            connection = None
        except Exception:
            if row is not None:
                row.clear()
            row = None
            selected = None
            connection = None
            submitted_at = None
            infrastructure_failure = True
    finally:
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                infrastructure_failure = True
        engine = None
        connection = None
        selected = None
        if row is not None:
            row.clear()
        row = None
        submitted_at = None
    if infrastructure_failure:
        raise IngressStateError("ingress_state_infrastructure") from None
    if error_code is not None:
        raise IngressStateError(error_code) from None


def _finalize_provisioning_success(
    runtime_submission_id: UUID,
    ingress_submission_id: UUID,
    operation_id: UUID,
) -> None:
    engine = _engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("SELECT id FROM provisioning_operations WHERE id=:operation_id FOR UPDATE"),
                {"operation_id": operation_id},
            ).scalar_one_or_none()
            connection.execute(
                text(
                    "SELECT id FROM runtime_submissions "
                    "WHERE id=:submission_id AND operation_id=:operation_id FOR UPDATE"
                ),
                {
                    "submission_id": runtime_submission_id,
                    "operation_id": operation_id,
                },
            ).scalar_one_or_none()
            connection.execute(
                text(
                    "SELECT id FROM ingress_submissions "
                    "WHERE id=:submission_id AND operation_id=:operation_id FOR UPDATE"
                ),
                {
                    "submission_id": ingress_submission_id,
                    "operation_id": operation_id,
                },
            ).scalar_one_or_none()
            row = (
                connection.execute(
                    text(
                        """
                        SELECT operation.organization_id, operation.deployment_id,
                               operation.actor_principal, operation.authorization_mode,
                               operation.state AS operation_state,
                               operation.completed_at, operation.updated_at,
                               runtime.state AS runtime_state,
                               runtime.confirmed_at AS runtime_confirmed_at,
                               ingress.state AS ingress_state,
                               ingress.confirmed_at AS ingress_confirmed_at
                        FROM provisioning_operations AS operation
                        JOIN runtime_submissions AS runtime
                          ON runtime.operation_id = operation.id
                         AND runtime.organization_id = operation.organization_id
                         AND runtime.deployment_id = operation.deployment_id
                        JOIN ingress_submissions AS ingress
                          ON ingress.operation_id = operation.id
                         AND ingress.organization_id = operation.organization_id
                         AND ingress.deployment_id = operation.deployment_id
                        WHERE operation.id = :operation_id
                          AND runtime.id = :runtime_submission_id
                          AND ingress.id = :ingress_submission_id
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "runtime_submission_id": runtime_submission_id,
                        "ingress_submission_id": ingress_submission_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise IngressStateError("provisioning_success_bundle_mismatch")
            terminal_success = (
                row["operation_state"] == "succeeded"
                and row["runtime_state"] == "confirmed"
                and row["ingress_state"] == "confirmed"
            )
            if terminal_success:
                audits = (
                    connection.execute(
                        text(
                            """
                            SELECT payload FROM audit_events
                            WHERE operation_id=:operation_id
                              AND event_type='provisioning.succeeded'
                            FOR SHARE
                            """
                        ),
                        {"operation_id": operation_id},
                    )
                    .mappings()
                    .all()
                )
                expected_payload = {
                    "runtime_submission_id": str(runtime_submission_id),
                    "ingress_submission_id": str(ingress_submission_id),
                }
                if len(audits) != 1 or audits[0]["payload"] != expected_payload:
                    raise IngressStateError("provisioning_success_replay_invalid")
                if row["authorization_mode"] == "required":
                    fact = (
                        connection.execute(
                            text(
                                """
                                SELECT fact.id, fact.subject_id, fact.revision_id,
                                       fact.authorization_id, fact.organization_id,
                                       fact.operation_id, fact.deployment_id, fact.metric,
                                       fact.unit, fact.quantity, fact.source_event,
                                       fact.occurred_at, fact.schema_version, fact.payload_digest
                                FROM usage_facts AS fact
                                WHERE fact.operation_id = :operation_id
                                  AND fact.metric = 'crusher.deployment.activated'
                                FOR SHARE
                                """
                            ),
                            {"operation_id": operation_id},
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if fact is None:
                        raise IngressStateError("provisioning_success_infrastructure")
                    expected_digest = billing.usage_fact_payload_digest(
                        usage_fact_id=fact["id"],
                        entitlement_subject_id=fact["subject_id"],
                        entitlement_revision_id=fact["revision_id"],
                        authorization_id=fact["authorization_id"],
                        organization_id=fact["organization_id"],
                        operation_id=fact["operation_id"],
                        deployment_id=fact["deployment_id"],
                        occurred_at=fact["occurred_at"],
                    )
                    if (
                        fact["organization_id"] != row["organization_id"]
                        or fact["operation_id"] != operation_id
                        or fact["deployment_id"] != row["deployment_id"]
                        or fact["metric"] != "crusher.deployment.activated"
                        or fact["unit"] != "deployment"
                        or fact["quantity"] != 1
                        or fact["source_event"] != "provisioning.succeeded"
                        or fact["schema_version"] != 1
                        or fact["occurred_at"] != row["completed_at"]
                        or fact["occurred_at"] != row["updated_at"]
                        or fact["occurred_at"] != row["runtime_confirmed_at"]
                        or fact["occurred_at"] != row["ingress_confirmed_at"]
                        or fact["payload_digest"] != expected_digest
                    ):
                        raise IngressStateError("provisioning_success_infrastructure")
                return
            if (
                row["operation_state"] != "running"
                or row["runtime_state"] != "submitted"
                or row["ingress_state"] != "submitted"
            ):
                raise IngressStateError("provisioning_success_state_invalid")
            boundary: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            authorization: RowMapping | None = None
            if row["authorization_mode"] == "required":
                authorization = (
                    connection.execute(
                        text(
                            """
                            SELECT id, usage_fact_id, subject_id, revision_id,
                                   organization_id, operation_id, deployment_id
                            FROM provisioning_authorizations
                            WHERE operation_id = :operation_id
                            FOR SHARE
                            """
                        ),
                        {"operation_id": operation_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    authorization is None
                    or authorization["organization_id"] != row["organization_id"]
                    or authorization["operation_id"] != operation_id
                    or authorization["deployment_id"] != row["deployment_id"]
                ):
                    raise IngressStateError("provisioning_success_bundle_mismatch")
            runtime_updated = connection.execute(
                text(
                    """
                    UPDATE runtime_submissions
                    SET state='confirmed', confirmed_at=:boundary
                    WHERE id=:runtime_submission_id AND state='submitted'
                    RETURNING id
                    """
                ),
                {"runtime_submission_id": runtime_submission_id, "boundary": boundary},
            ).scalar_one_or_none()
            if runtime_updated != runtime_submission_id:
                raise IngressStateError("provisioning_success_state_invalid")
            ingress_updated = connection.execute(
                text(
                    """
                    UPDATE ingress_submissions
                    SET state='confirmed', confirmed_at=:boundary
                    WHERE id=:ingress_submission_id AND state='submitted'
                    RETURNING id
                    """
                ),
                {"ingress_submission_id": ingress_submission_id, "boundary": boundary},
            ).scalar_one_or_none()
            if ingress_updated != ingress_submission_id:
                raise IngressStateError("provisioning_success_state_invalid")
            operation_updated = connection.execute(
                text(
                    """
                    UPDATE provisioning_operations
                    SET state='succeeded', completed_at=:boundary, updated_at=:boundary
                    WHERE id=:operation_id AND state='running'
                    RETURNING id
                    """
                ),
                {"operation_id": operation_id, "boundary": boundary},
            ).scalar_one_or_none()
            if operation_updated != operation_id:
                raise IngressStateError("provisioning_success_state_invalid")
            success_audit_id = uuid4()
            audit_inserted = connection.execute(
                text(
                    """
                    INSERT INTO audit_events (
                        id, organization_id, operation_id, event_type, actor_principal, payload
                    ) VALUES (
                        :id, :organization_id, :operation_id, 'provisioning.succeeded',
                        :actor_principal, CAST(:payload AS jsonb)
                    )
                    RETURNING id
                    """
                ),
                {
                    "id": success_audit_id,
                    "organization_id": row["organization_id"],
                    "operation_id": operation_id,
                    "actor_principal": row["actor_principal"],
                    "payload": json.dumps(
                        {
                            "runtime_submission_id": str(runtime_submission_id),
                            "ingress_submission_id": str(ingress_submission_id),
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                },
            ).scalar_one_or_none()
            if audit_inserted != success_audit_id:
                raise IngressStateError("provisioning_success_state_invalid")
            if authorization is not None:
                payload_digest = billing.usage_fact_payload_digest(
                    usage_fact_id=authorization["usage_fact_id"],
                    entitlement_subject_id=authorization["subject_id"],
                    entitlement_revision_id=authorization["revision_id"],
                    authorization_id=authorization["id"],
                    organization_id=authorization["organization_id"],
                    operation_id=authorization["operation_id"],
                    deployment_id=authorization["deployment_id"],
                    occurred_at=boundary,
                )
                inserted_fact = (
                    connection.execute(
                        text(
                            """
                            INSERT INTO usage_facts (
                                id, subject_id, revision_id, authorization_id,
                                organization_id, operation_id, deployment_id,
                                metric, unit, quantity, source_event, occurred_at,
                                schema_version, payload_digest
                            ) VALUES (
                                :id, :subject_id, :revision_id, :authorization_id,
                                :organization_id, :operation_id, :deployment_id,
                                'crusher.deployment.activated', 'deployment', 1,
                                'provisioning.succeeded', :occurred_at, 1, :payload_digest
                            )
                            RETURNING id, subject_id, revision_id, authorization_id,
                                      organization_id, operation_id, deployment_id,
                                      occurred_at, payload_digest
                            """
                        ),
                        {
                            "id": authorization["usage_fact_id"],
                            "subject_id": authorization["subject_id"],
                            "revision_id": authorization["revision_id"],
                            "authorization_id": authorization["id"],
                            "organization_id": authorization["organization_id"],
                            "operation_id": authorization["operation_id"],
                            "deployment_id": authorization["deployment_id"],
                            "occurred_at": boundary,
                            "payload_digest": payload_digest,
                        },
                    )
                    .mappings()
                    .one()
                )
                expected_fact = {
                    "id": authorization["usage_fact_id"],
                    "subject_id": authorization["subject_id"],
                    "revision_id": authorization["revision_id"],
                    "authorization_id": authorization["id"],
                    "organization_id": authorization["organization_id"],
                    "operation_id": authorization["operation_id"],
                    "deployment_id": authorization["deployment_id"],
                    "occurred_at": boundary,
                    "payload_digest": payload_digest,
                }
                if dict(inserted_fact) != expected_fact:
                    raise IngressStateError("provisioning_success_state_invalid")
    finally:
        engine.dispose()


def finalize_provisioning_success(
    runtime_submission_id: UUID,
    ingress_submission_id: UUID,
    operation_id: UUID,
) -> None:
    """Confirm the exact v2 runtime/ingress bundle and operation atomically."""
    if not all(
        type(value) is UUID
        for value in (runtime_submission_id, ingress_submission_id, operation_id)
    ):
        raise IngressStateError("provisioning_success_invalid_identifier") from None
    error_code: str | None = None
    try:
        _finalize_provisioning_success(
            runtime_submission_id,
            ingress_submission_id,
            operation_id,
        )
    except IngressStateError as error:
        error_code = error.code
    except Exception:
        error_code = "provisioning_success_infrastructure"
    if error_code is not None:
        raise IngressStateError(error_code) from None


def _failure_bundle_valid(
    operation: Mapping[str, Any],
    runtime: Mapping[str, Any],
    ingress: Mapping[str, Any],
    desired: Mapping[str, Any],
) -> bool:
    operation_id = operation["id"]
    organization_id = operation["organization_id"]
    deployment_id = operation["deployment_id"]
    identities = (
        operation_id,
        organization_id,
        deployment_id,
        runtime["id"],
        runtime["operation_id"],
        runtime["organization_id"],
        runtime["deployment_id"],
        ingress["id"],
        ingress["operation_id"],
        ingress["organization_id"],
        ingress["deployment_id"],
        desired["deployment_id"],
        desired["deployment_organization_id"],
    )
    if any(type(value) is not UUID for value in identities):
        return False
    if (
        runtime["operation_id"] != operation_id
        or ingress["operation_id"] != operation_id
        or runtime["organization_id"] != organization_id
        or ingress["organization_id"] != organization_id
        or runtime["deployment_id"] != deployment_id
        or ingress["deployment_id"] != deployment_id
        or desired["deployment_id"] != deployment_id
        or desired["deployment_organization_id"] != organization_id
    ):
        return False
    api_name, migration_name = runtime_submission._container_names(deployment_id)
    route = ingress_submission._canonical_route(deployment_id, desired["public_domain"])
    return bool(
        runtime["provider"] == runtime_submission.PROVIDER
        and runtime["generation"] == runtime_submission.GENERATION
        and runtime["submission_key"] == runtime_submission._submission_key(operation_id)
        and type(runtime["spec_fingerprint"]) is str
        and re.fullmatch(r"[0-9a-f]{64}", runtime["spec_fingerprint"]) is not None
        and runtime["api_container_name"] == api_name
        and runtime["migration_container_name"] == migration_name
        and ingress["provider"] == ingress_submission.PROVIDER
        and ingress["generation"] == ingress_submission.GENERATION
        and ingress["submission_key"] == ingress_submission._submission_key(operation_id)
        and ingress["route_id"] == ingress_submission._route_id(deployment_id)
        and ingress["route_fingerprint"] == ingress_submission._route_fingerprint(route)
    )


def _finalize_provisioning_failure(operation_id: UUID, scope: str, error_code: str) -> None:
    engine = _engine()
    try:
        with engine.begin() as connection:
            operation_selected = (
                connection.execute(
                    text(
                        """
                        SELECT id, organization_id, deployment_id, state, actor_principal,
                               completed_at, updated_at, last_error_code
                        FROM provisioning_operations
                        WHERE id=:operation_id
                        FOR UPDATE
                        """
                    ),
                    {"operation_id": operation_id},
                )
                .mappings()
                .one_or_none()
            )
            if operation_selected is None:
                raise IngressStateError("provisioning_failure_state_resolution")
            operation: dict[str, Any] = dict(operation_selected)
            runtime_selected = (
                connection.execute(
                    text(
                        "SELECT * FROM runtime_submissions "
                        "WHERE operation_id=:operation_id FOR UPDATE"
                    ),
                    {"operation_id": operation_id},
                )
                .mappings()
                .one_or_none()
            )
            if runtime_selected is None:
                raise IngressStateError("provisioning_failure_state_resolution")
            runtime: dict[str, Any] = dict(runtime_selected)
            ingress_selected = (
                connection.execute(
                    text(
                        "SELECT * FROM ingress_submissions "
                        "WHERE operation_id=:operation_id FOR UPDATE"
                    ),
                    {"operation_id": operation_id},
                )
                .mappings()
                .one_or_none()
            )
            if ingress_selected is None:
                raise IngressStateError("provisioning_failure_state_resolution")
            ingress: dict[str, Any] = dict(ingress_selected)
            desired_selected = (
                connection.execute(
                    text(
                        """
                        SELECT deployment.id AS deployment_id,
                               deployment.organization_id AS deployment_organization_id,
                               deployment.public_domain
                        FROM deployments AS deployment
                        WHERE deployment.id=:deployment_id
                        FOR SHARE OF deployment
                        """
                    ),
                    {"deployment_id": operation["deployment_id"]},
                )
                .mappings()
                .one_or_none()
            )
            if desired_selected is None:
                raise IngressStateError("provisioning_failure_state_resolution")
            desired: dict[str, Any] = dict(desired_selected)
            if not _failure_bundle_valid(operation, runtime, ingress, desired):
                raise IngressStateError("provisioning_failure_state_resolution")
            expected_payload = {
                "runtime_submission_id": str(runtime["id"]),
                "ingress_submission_id": str(ingress["id"]),
                "error_code": error_code,
            }
            audits = (
                connection.execute(
                    text(
                        "SELECT payload FROM audit_events "
                        "WHERE operation_id=:operation_id "
                        "AND event_type='provisioning.failed' FOR SHARE"
                    ),
                    {"operation_id": operation_id},
                )
                .mappings()
                .all()
            )
            terminal_runtime_scope = (
                operation["state"] == "failed"
                and operation["last_error_code"] == error_code
                and runtime["state"] == "failed"
                and runtime["failure_code"] == error_code
                and ingress["state"] == "failed"
                and ingress["failure_code"] == error_code
            )
            terminal_ingress_scope = (
                operation["state"] == "failed"
                and operation["last_error_code"] == error_code
                and runtime["state"] == "submitted"
                and ingress["state"] == "failed"
                and ingress["failure_code"] == error_code
            )
            if terminal_runtime_scope or terminal_ingress_scope:
                exact_scope = (scope == "runtime" and terminal_runtime_scope) or (
                    scope == "ingress" and terminal_ingress_scope
                )
                if not exact_scope or len(audits) != 1 or audits[0]["payload"] != expected_payload:
                    raise IngressStateError("provisioning_failure_conflict")
                return
            if operation["state"] != "running" or audits:
                raise IngressStateError("provisioning_failure_conflict")
            if scope == "runtime":
                if (
                    runtime["state"] not in {"prepared", "submitted"}
                    or ingress["state"] != "prepared"
                ):
                    raise IngressStateError("provisioning_failure_conflict")
                runtime_updated = connection.execute(
                    text(
                        """
                        WITH boundary AS (SELECT clock_timestamp() AS now)
                        UPDATE runtime_submissions
                        SET state='failed', failed_at=boundary.now, failure_code=:error_code
                        FROM boundary
                        WHERE id=:submission_id AND state IN ('prepared', 'submitted')
                        RETURNING runtime_submissions.id
                        """
                    ),
                    {"submission_id": runtime["id"], "error_code": error_code},
                ).scalar_one_or_none()
                if runtime_updated != runtime["id"]:
                    raise IngressStateError("provisioning_failure_state_resolution")
                ingress_update = text(
                    """
                    WITH boundary AS (SELECT clock_timestamp() AS now)
                    UPDATE ingress_submissions
                    SET state='failed', failed_at=boundary.now, failure_code=:error_code
                    FROM boundary
                    WHERE id=:submission_id AND state='prepared'
                    RETURNING ingress_submissions.id
                    """
                )
            else:
                if runtime["state"] != "submitted" or ingress["state"] not in {
                    "prepared",
                    "submitted",
                }:
                    raise IngressStateError("provisioning_failure_conflict")
                ingress_update = text(
                    """
                    WITH boundary AS (SELECT clock_timestamp() AS now)
                    UPDATE ingress_submissions
                    SET state='failed', failed_at=boundary.now, failure_code=:error_code
                    FROM boundary
                    WHERE id=:submission_id AND state IN ('prepared', 'submitted')
                    RETURNING ingress_submissions.id
                    """
                )
            ingress_updated = connection.execute(
                ingress_update,
                {"submission_id": ingress["id"], "error_code": error_code},
            ).scalar_one_or_none()
            if ingress_updated != ingress["id"]:
                raise IngressStateError("provisioning_failure_state_resolution")
            operation_updated = connection.execute(
                text(
                    """
                    WITH boundary AS (SELECT clock_timestamp() AS now)
                    UPDATE provisioning_operations
                    SET state='failed', completed_at=boundary.now, updated_at=boundary.now,
                        last_error_code=:error_code
                    FROM boundary
                    WHERE id=:operation_id AND state='running'
                    RETURNING provisioning_operations.id
                    """
                ),
                {"operation_id": operation_id, "error_code": error_code},
            ).scalar_one_or_none()
            if operation_updated != operation_id:
                raise IngressStateError("provisioning_failure_state_resolution")
            audit_id = uuid4()
            audit_inserted = connection.execute(
                text(
                    """
                    INSERT INTO audit_events (
                        id, organization_id, operation_id, event_type, actor_principal, payload
                    ) VALUES (
                        :id, :organization_id, :operation_id, 'provisioning.failed',
                        :actor_principal, CAST(:payload AS jsonb)
                    )
                    RETURNING id
                    """
                ),
                {
                    "id": audit_id,
                    "organization_id": operation["organization_id"],
                    "operation_id": operation_id,
                    "actor_principal": operation["actor_principal"],
                    "payload": json.dumps(expected_payload, sort_keys=True, separators=(",", ":")),
                },
            ).scalar_one_or_none()
            if audit_inserted != audit_id:
                raise IngressStateError("provisioning_failure_state_resolution")
    finally:
        engine.dispose()


def finalize_provisioning_failure(operation_id: UUID, scope: str, error_code: str) -> None:
    """Fail one exact v2 bundle by canonical operation identity and durable scope."""
    if (
        type(operation_id) is not UUID
        or type(scope) is not str
        or scope
        not in {
            "runtime",
            "ingress",
        }
    ):
        operation_id = None  # type: ignore[assignment]
        scope = ""  # avoid retaining caller-controlled text in traceback locals
        error_code = ""  # avoid retaining caller-controlled text in traceback locals
        raise IngressStateError("provisioning_failure_invalid_request") from None
    if type(error_code) is not str or _ERROR_CODE.fullmatch(error_code) is None:
        error_code = "provisioning_failed"
    final_error: str | None = None
    try:
        _finalize_provisioning_failure(operation_id, scope, error_code)
    except IngressStateError as error:
        final_error = error.code
    except Exception:
        final_error = "provisioning_failure_infrastructure"
    if final_error is not None:
        raise IngressStateError(final_error) from None
