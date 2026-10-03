"""Durable Crusher provisioning command service."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy import Connection, text

from fstack.provisioning import ProvisioningRequest, request_fingerprint


class ProvisioningConflictError(Exception):
    """Base class for deterministic provisioning conflicts."""

    code: str


class IdempotencyConflictError(ProvisioningConflictError):
    code = "idempotency_conflict"


class OrganizationExistsError(ProvisioningConflictError):
    code = "organization_exists"


class PublicDomainExistsError(ProvisioningConflictError):
    code = "public_domain_exists"


@dataclass(frozen=True)
class ProvisioningResult:
    operation_id: UUID
    organization_id: UUID
    business_id: UUID
    environment_id: UUID
    workload_id: UUID
    deployment_id: UUID
    replayed: bool


_CLAIM = text(
    """
    INSERT INTO provisioning_operations (
        id, organization_id, organization_slug, idempotency_key,
        request_fingerprint, state, actor_principal, business_id,
        environment_id, workload_id, deployment_id
    ) VALUES (
        :operation_id, :organization_id, :organization_slug, :idempotency_key,
        :request_fingerprint, 'pending', :actor_principal, :business_id,
        :environment_id, :workload_id, :deployment_id
    )
    ON CONFLICT (organization_slug, idempotency_key) DO NOTHING
    RETURNING id
    """
)


def create_provisioning_operation(
    connection: Connection,
    *,
    request: ProvisioningRequest,
    idempotency_key: str,
    actor_principal: str,
) -> ProvisioningResult:
    """Create or replay the first durable Crusher desired-state transaction."""
    operation_id = uuid4()
    organization_id = uuid4()
    business_id = uuid4()
    environment_id = uuid4()
    workload_id = uuid4()
    deployment_id = uuid4()
    database_binding_id = uuid4()
    storage_binding_id = uuid4()
    audit_event_id = uuid4()
    fingerprint = request_fingerprint(request)

    identifiers = {
        "operation_id": operation_id,
        "organization_id": organization_id,
        "business_id": business_id,
        "environment_id": environment_id,
        "workload_id": workload_id,
        "deployment_id": deployment_id,
        "organization_slug": request.organization_slug,
        "idempotency_key": idempotency_key,
        "request_fingerprint": fingerprint,
        "actor_principal": actor_principal,
    }
    claimed = connection.execute(_CLAIM, identifiers).scalar_one_or_none()
    if claimed is None:
        existing = (
            connection.execute(
                text(
                    """
                SELECT id, organization_id, business_id, environment_id,
                       workload_id, deployment_id, request_fingerprint
                FROM provisioning_operations
                WHERE organization_slug = :organization_slug
                  AND idempotency_key = :idempotency_key
                """
                ),
                {
                    "organization_slug": request.organization_slug,
                    "idempotency_key": idempotency_key,
                },
            )
            .mappings()
            .one()
        )
        if existing["request_fingerprint"] != fingerprint:
            raise IdempotencyConflictError
        return ProvisioningResult(
            operation_id=existing["id"],
            organization_id=existing["organization_id"],
            business_id=existing["business_id"],
            environment_id=existing["environment_id"],
            workload_id=existing["workload_id"],
            deployment_id=existing["deployment_id"],
            replayed=True,
        )

    connection.execute(
        text("INSERT INTO organizations (id, slug, name) VALUES (:id, :slug, :name)"),
        {
            "id": organization_id,
            "slug": request.organization_slug,
            "name": request.organization_name,
        },
    )
    connection.execute(
        text(
            "INSERT INTO businesses (id, organization_id, slug, name) "
            "VALUES (:id, :organization_id, :slug, :name)"
        ),
        {
            "id": business_id,
            "organization_id": organization_id,
            "slug": request.business_slug,
            "name": request.business_name,
        },
    )
    connection.execute(
        text(
            "INSERT INTO environments (id, organization_id, business_id, slug) "
            "VALUES (:id, :organization_id, :business_id, :slug)"
        ),
        {
            "id": environment_id,
            "organization_id": organization_id,
            "business_id": business_id,
            "slug": request.environment_slug,
        },
    )
    connection.execute(
        text(
            "INSERT INTO workloads (id, organization_id, environment_id, slug, kind) "
            "VALUES (:id, :organization_id, :environment_id, 'crusher', 'crusher')"
        ),
        {
            "id": workload_id,
            "organization_id": organization_id,
            "environment_id": environment_id,
        },
    )
    connection.execute(
        text(
            "INSERT INTO deployments "
            "(id, organization_id, workload_id, crusher_image, public_domain) "
            "VALUES (:id, :organization_id, :workload_id, :crusher_image, :public_domain)"
        ),
        {
            "id": deployment_id,
            "organization_id": organization_id,
            "workload_id": workload_id,
            "crusher_image": request.crusher_image,
            "public_domain": request.public_domain,
        },
    )
    connection.execute(
        text(
            "INSERT INTO database_bindings "
            "(id, organization_id, deployment_id, secret_ref) "
            "VALUES (:id, :organization_id, :deployment_id, :secret_ref)"
        ),
        {
            "id": database_binding_id,
            "organization_id": organization_id,
            "deployment_id": deployment_id,
            "secret_ref": request.database_secret_ref,
        },
    )
    connection.execute(
        text(
            "INSERT INTO storage_bindings "
            "(id, organization_id, deployment_id, secret_ref) "
            "VALUES (:id, :organization_id, :deployment_id, :secret_ref)"
        ),
        {
            "id": storage_binding_id,
            "organization_id": organization_id,
            "deployment_id": deployment_id,
            "secret_ref": request.object_storage_secret_ref,
        },
    )
    connection.execute(
        text(
            "INSERT INTO audit_events "
            "(id, organization_id, operation_id, event_type, actor_principal) "
            "VALUES (:id, :organization_id, :operation_id, "
            "'provisioning.requested', :actor_principal)"
        ),
        {
            "id": audit_event_id,
            "organization_id": organization_id,
            "operation_id": operation_id,
            "actor_principal": actor_principal,
        },
    )
    return ProvisioningResult(
        operation_id=operation_id,
        organization_id=organization_id,
        business_id=business_id,
        environment_id=environment_id,
        workload_id=workload_id,
        deployment_id=deployment_id,
        replayed=False,
    )
