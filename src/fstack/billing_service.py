"""Transactional provider-neutral entitlement synchronization."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import Connection, text

from fstack.billing import EntitlementRevisionRequest


class EntitlementRevisionConflict(Exception):
    """The source or generation identity disagrees with committed evidence."""


class EntitlementPersistenceError(Exception):
    """PostgreSQL did not persist an expected entitlement row."""


@dataclass(frozen=True)
class EntitlementRevisionResult:
    entitlement_subject_id: UUID
    entitlement_revision_id: UUID
    replayed: bool


def _matches(
    row: Mapping[str, object],
    *,
    request: EntitlementRevisionRequest,
    subject_id: UUID,
    payload_digest: str,
) -> bool:
    return (
        row["id"] == request.entitlement_revision_id
        and row["subject_id"] == subject_id
        and row["generation"] == request.generation
        and row["plan_key"] == request.plan_key
        and row["state"] == request.state
        and row["capability"] == request.capability
        and row["quota_dimension"] == request.quota_dimension
        and row["quota_limit"] == request.quota_limit
        and row["observed_at"] == request.observed_at
        and row["valid_until"] == request.valid_until
        and row["source_revision_id"] == request.source_revision_id
        and row["payload_digest"] == payload_digest
    )


def _load_by_source(
    connection: Connection, source_revision_id: UUID
) -> Mapping[str, object] | None:
    row = (
        connection.execute(
            text(
                """
                SELECT id, subject_id, generation, plan_key, state, capability,
                       quota_dimension, quota_limit, observed_at, valid_until,
                       source_revision_id, payload_digest
                FROM entitlement_revisions
                WHERE source_revision_id = :source_revision_id
                """
            ),
            {"source_revision_id": source_revision_id},
        )
        .mappings()
        .one_or_none()
    )
    return None if row is None else dict(row)


def synchronize_entitlement_revision(
    connection: Connection,
    *,
    request: EntitlementRevisionRequest,
) -> EntitlementRevisionResult:
    """Create or exactly replay one immutable entitlement revision."""
    candidate_subject_id = uuid4()
    connection.execute(
        text(
            """
            INSERT INTO entitlement_subjects (id, subject_type, subject_key)
            VALUES (:id, 'organization_slug', :subject_key)
            ON CONFLICT (subject_type, subject_key) DO NOTHING
            RETURNING id
            """
        ),
        {"id": candidate_subject_id, "subject_key": request.organization_slug},
    ).scalar_one_or_none()
    subject_id = cast(
        UUID,
        connection.execute(
            text(
                """
                SELECT id
                FROM entitlement_subjects
                WHERE subject_type = 'organization_slug' AND subject_key = :subject_key
                FOR UPDATE
                """
            ),
            {"subject_key": request.organization_slug},
        ).scalar_one(),
    )
    digest = request.payload_digest(subject_id)

    existing_source = _load_by_source(connection, request.source_revision_id)
    if existing_source is not None:
        if not _matches(
            existing_source,
            request=request,
            subject_id=subject_id,
            payload_digest=digest,
        ):
            raise EntitlementRevisionConflict
        return EntitlementRevisionResult(
            entitlement_subject_id=subject_id,
            entitlement_revision_id=request.entitlement_revision_id,
            replayed=True,
        )

    existing_generation = (
        connection.execute(
            text(
                """
                SELECT id, subject_id, generation, plan_key, state, capability,
                       quota_dimension, quota_limit, observed_at, valid_until,
                       source_revision_id, payload_digest
                FROM entitlement_revisions
                WHERE subject_id = :subject_id AND generation = :generation
                """
            ),
            {"subject_id": subject_id, "generation": request.generation},
        )
        .mappings()
        .one_or_none()
    )
    if existing_generation is not None:
        raise EntitlementRevisionConflict

    current_generation = cast(
        int | None,
        connection.execute(
            text(
                "SELECT max(generation) FROM entitlement_revisions WHERE subject_id = :subject_id"
            ),
            {"subject_id": subject_id},
        ).scalar_one(),
    )
    if current_generation is not None and request.generation <= current_generation:
        raise EntitlementRevisionConflict

    inserted = connection.execute(
        text(
            """
            INSERT INTO entitlement_revisions (
                id, subject_id, generation, plan_key, state, capability,
                quota_dimension, quota_limit, observed_at, valid_until,
                source_revision_id, payload_digest
            ) VALUES (
                :id, :subject_id, :generation, :plan_key, :state, :capability,
                :quota_dimension, :quota_limit, :observed_at, :valid_until,
                :source_revision_id, :payload_digest
            )
            ON CONFLICT DO NOTHING
            RETURNING id
            """
        ),
        {
            "id": request.entitlement_revision_id,
            "subject_id": subject_id,
            "generation": request.generation,
            "plan_key": request.plan_key,
            "state": request.state,
            "capability": request.capability,
            "quota_dimension": request.quota_dimension,
            "quota_limit": request.quota_limit,
            "observed_at": request.observed_at,
            "valid_until": request.valid_until,
            "source_revision_id": request.source_revision_id,
            "payload_digest": digest,
        },
    ).scalar_one_or_none()
    if inserted is None:
        raced_source = _load_by_source(connection, request.source_revision_id)
        if raced_source is not None and _matches(
            raced_source,
            request=request,
            subject_id=subject_id,
            payload_digest=digest,
        ):
            return EntitlementRevisionResult(
                entitlement_subject_id=subject_id,
                entitlement_revision_id=request.entitlement_revision_id,
                replayed=True,
            )
        id_conflict = cast(
            bool,
            connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM entitlement_revisions WHERE id = :id)"),
                {"id": request.entitlement_revision_id},
            ).scalar_one(),
        )
        if raced_source is not None or id_conflict:
            raise EntitlementRevisionConflict
        raise EntitlementPersistenceError
    return EntitlementRevisionResult(
        entitlement_subject_id=subject_id,
        entitlement_revision_id=request.entitlement_revision_id,
        replayed=False,
    )
