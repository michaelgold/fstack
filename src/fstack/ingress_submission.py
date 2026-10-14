"""Canonical local Caddy ingress submission preparation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import Connection, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from fstack import runtime_submission

PROVIDER: Literal["local_caddy"] = "local_caddy"
GENERATION = 1
SUBMISSION_VERSION = "fstack.local-caddy-ingress-submission.v1"
ROUTE_SPEC_VERSION = "fstack.local-caddy-route.v1"


@dataclass(frozen=True)
class PreparedProvisioningSubmissions:
    operation_id: UUID
    runtime_submission_id: UUID
    ingress_submission_id: UUID


class ProvisioningSubmissionConflictError(RuntimeError):
    """The durable runtime and ingress preparation bundle cannot be replayed exactly."""


class ProvisioningSubmissionStateError(RuntimeError):
    """Submission preparation could not establish a safe durable state."""


def _route_id(deployment_id: UUID) -> str:
    return f"fstack-crusher-ingress-{deployment_id.hex}"


def _container_dial(deployment_id: UUID) -> str:
    return f"fstack-crusher-{deployment_id.hex}:8000"


def _canonical_route(deployment_id: UUID, public_domain: str) -> dict[str, Any]:
    return {
        "@id": _route_id(deployment_id),
        "match": [{"host": [public_domain]}],
        "handle": [
            {
                "handler": "reverse_proxy",
                "upstreams": [{"dial": _container_dial(deployment_id)}],
            }
        ],
    }


def _submission_key(operation_id: UUID) -> str:
    return hashlib.sha256(
        SUBMISSION_VERSION.encode("ascii") + b"\0" + str(operation_id).encode("ascii")
    ).hexdigest()


def _route_fingerprint(route: Mapping[str, Any]) -> str:
    serialized = json.dumps(
        {"route": route, "version": ROUTE_SPEC_VERSION},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def _prepare_ingress(
    connection: Connection,
    operation: Mapping[str, Any],
) -> UUID:
    operation_id = operation["id"]
    deployment_id = operation["deployment_id"]
    route_id = _route_id(deployment_id)
    key = _submission_key(operation_id)
    fingerprint = _route_fingerprint(_canonical_route(deployment_id, operation["public_domain"]))
    existing = (
        connection.execute(
            text("SELECT * FROM ingress_submissions WHERE operation_id = :operation_id FOR UPDATE"),
            {"operation_id": operation_id},
        )
        .mappings()
        .one_or_none()
    )
    if existing is not None:
        if (
            existing["organization_id"] != operation["organization_id"]
            or existing["deployment_id"] != deployment_id
            or existing["provider"] != PROVIDER
            or existing["generation"] != GENERATION
            or existing["submission_key"] != key
            or existing["route_fingerprint"] != fingerprint
            or existing["route_id"] != route_id
        ):
            raise ProvisioningSubmissionConflictError("ingress_submission_conflict")
        return cast(UUID, existing["id"])

    ingress_submission_id = uuid4()
    inserted_ingress_submission_id = connection.execute(
        text(
            """
            INSERT INTO ingress_submissions (
                id, organization_id, operation_id, deployment_id, provider, generation,
                submission_key, route_fingerprint, route_id, state
            ) VALUES (
                :id, :organization_id, :operation_id, :deployment_id, :provider,
                :generation, :submission_key, :route_fingerprint, :route_id, 'prepared'
            )
            RETURNING id
            """
        ),
        {
            "id": ingress_submission_id,
            "organization_id": operation["organization_id"],
            "operation_id": operation_id,
            "deployment_id": deployment_id,
            "provider": PROVIDER,
            "generation": GENERATION,
            "submission_key": key,
            "route_fingerprint": fingerprint,
            "route_id": route_id,
        },
    ).scalar_one_or_none()
    if inserted_ingress_submission_id != ingress_submission_id:
        raise ProvisioningSubmissionStateError("submission_prepare_failed")
    return ingress_submission_id


def _operation(connection: Connection, operation_id: UUID) -> Mapping[str, Any]:
    operation = (
        connection.execute(
            text(
                """
                SELECT operation.id, operation.organization_id, operation.deployment_id,
                       operation.state, operation.actor_principal, operation.started_at,
                       deployment.public_domain
                FROM provisioning_operations AS operation
                JOIN deployments AS deployment
                  ON deployment.organization_id = operation.organization_id
                 AND deployment.id = operation.deployment_id
                WHERE operation.id = :operation_id
                FOR UPDATE OF operation
                FOR SHARE OF deployment
                """
            ),
            {"operation_id": operation_id},
        )
        .mappings()
        .one_or_none()
    )
    if operation is None:
        raise ProvisioningSubmissionConflictError("operation_not_found")
    return dict(operation)


def _started_audit(
    connection: Connection,
    operation: Mapping[str, Any],
    runtime_submission_id: UUID,
    ingress_submission_id: UUID,
    *,
    operation_was_pending: bool,
) -> None:
    expected_payload = {
        "ingress_submission_id": str(ingress_submission_id),
        "runtime_submission_id": str(runtime_submission_id),
    }
    existing = (
        connection.execute(
            text(
                """
                SELECT payload FROM audit_events
                WHERE operation_id = :operation_id
                  AND event_type = 'provisioning.started'
                """
            ),
            {"operation_id": operation["id"]},
        )
        .mappings()
        .one_or_none()
    )
    if existing is not None:
        if operation_was_pending:
            raise ProvisioningSubmissionConflictError("started_audit_unexpected")
        if existing["payload"] != expected_payload:
            raise ProvisioningSubmissionConflictError("started_audit_conflict")
        return
    if not operation_was_pending:
        raise ProvisioningSubmissionConflictError("started_audit_missing")
    started_audit_id = uuid4()
    audit_inserted = connection.execute(
        text(
            """
            INSERT INTO audit_events (
                id, organization_id, operation_id, event_type, actor_principal, payload
            ) VALUES (
                :id, :organization_id, :operation_id, 'provisioning.started',
                :actor_principal, CAST(:payload AS jsonb)
            )
            RETURNING id
            """
        ),
        {
            "id": started_audit_id,
            "organization_id": operation["organization_id"],
            "operation_id": operation["id"],
            "actor_principal": operation["actor_principal"],
            "payload": json.dumps(expected_payload, sort_keys=True, separators=(",", ":")),
        },
    ).scalar_one_or_none()
    if audit_inserted != started_audit_id:
        raise ProvisioningSubmissionStateError("submission_prepare_failed")


def prepare_operation_bundle(operation_id: UUID | str) -> PreparedProvisioningSubmissions:
    """Prepare or replay one exact runtime and local Caddy ingress bundle."""
    operation_uuid = operation_id if isinstance(operation_id, UUID) else UUID(operation_id)
    engine = create_engine(runtime_submission._database_url(), hide_parameters=True)
    result: PreparedProvisioningSubmissions | None = None
    runtime_conflict = False
    database_failure = False
    try:
        try:
            with engine.begin() as connection:
                runtime = runtime_submission._prepare(
                    connection,
                    operation_uuid,
                    reject_confirmed_drift=True,
                )
                operation = _operation(connection, operation_uuid)
                ingress_submission_id = _prepare_ingress(connection, operation)
                operation_was_pending = operation["state"] == "pending"
                if operation_was_pending:
                    operation_updated = connection.execute(
                        text(
                            """
                            WITH boundary AS (SELECT clock_timestamp() AS now)
                            UPDATE provisioning_operations
                            SET state = 'running', started_at = boundary.now,
                                updated_at = boundary.now
                            FROM boundary
                            WHERE id = :operation_id AND state = 'pending'
                            RETURNING provisioning_operations.id
                            """
                        ),
                        {"operation_id": operation_uuid},
                    ).scalar_one_or_none()
                    if operation_updated != operation_uuid:
                        raise ProvisioningSubmissionStateError("submission_prepare_failed")
                elif operation["state"] != "running":
                    raise ProvisioningSubmissionConflictError("operation_not_running")
                _started_audit(
                    connection,
                    operation,
                    runtime.submission_id,
                    ingress_submission_id,
                    operation_was_pending=operation_was_pending,
                )
                result = PreparedProvisioningSubmissions(
                    operation_id=operation_uuid,
                    runtime_submission_id=runtime.submission_id,
                    ingress_submission_id=ingress_submission_id,
                )
        except runtime_submission.RuntimeSubmissionConflictError:
            runtime_conflict = True
        except runtime_submission.RuntimeSubmissionStateError:
            database_failure = True
        except SQLAlchemyError:
            database_failure = True
    finally:
        engine.dispose()
    if runtime_conflict:
        raise ProvisioningSubmissionConflictError("runtime_submission_conflict")
    if database_failure:
        raise ProvisioningSubmissionStateError("submission_prepare_failed")
    if result is None:
        raise AssertionError("submission preparation completed without a result")
    return result
