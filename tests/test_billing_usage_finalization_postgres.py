from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text

from fstack.api import create_app

VALID_BODY = {
    "organization_slug": "usage-acme",
    "organization_name": "Usage Acme",
    "business_slug": "mailer",
    "business_name": "Usage Mailer",
    "environment_slug": "production",
    "public_domain": "usage.acme.example",
    "crusher_image": "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
    "database_secret_ref": "secret://vault/acme/database",
    "object_storage_secret_ref": "secret://vault/acme/object-storage",
}


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _required_prepared_bundle(
    engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
):
    from fstack import api
    from fstack.ingress_submission import prepare_operation_bundle

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "required")
    monkeypatch.setenv(
        "FSTACK_BILLING_RECONCILE_TOKEN", "reconcile-token-which-is-long-enough-0001"
    )
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", "export-token-which-is-long-enough-0001")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    monkeypatch.setattr(api, "create_engine", lambda *_args, **_kwargs: engine)

    now = datetime.now(UTC)
    with TestClient(create_app()) as client:
        revision = client.put(
            "/internal/billing/entitlement-revisions",
            headers={
                "Authorization": "Bearer reconcile-token-which-is-long-enough-0001",
                "Content-Type": "application/json",
            },
            json={
                "organization_slug": VALID_BODY["organization_slug"],
                "entitlement_revision_id": str(uuid4()),
                "generation": 1,
                "plan_key": "crusher.single.v1",
                "state": "active",
                "capability": "crusher.deployment.create",
                "quota_dimension": "crusher.deployment.active",
                "quota_limit": 1,
                "observed_at": _canonical_timestamp(now - timedelta(minutes=1)),
                "valid_until": _canonical_timestamp(now + timedelta(minutes=4)),
                "source_revision_id": str(uuid4()),
            },
        )
        assert revision.status_code == 201
        operation = client.post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": "Bearer test-operator-token",
                "Idempotency-Key": "required-usage-finalization",
            },
            json=VALID_BODY,
        )
        assert operation.status_code == 202

    operation_id = UUID(operation.json()["operation_id"])
    return prepare_operation_bundle(operation_id)


@pytest.mark.postgres
def test_required_joint_success_inserts_exact_immutable_usage_fact(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.billing import usage_fact_payload_digest
    from fstack.ingress_state import finalize_provisioning_success, mark_submitted
    from fstack.runtime_state import persist_submitted

    prepared = _required_prepared_bundle(migrated_database, monkeypatch)
    persist_submitted(prepared.runtime_submission_id, "docker-api-required-usage")
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)

    finalize_provisioning_success(
        prepared.runtime_submission_id,
        prepared.ingress_submission_id,
        prepared.operation_id,
    )

    with migrated_database.connect() as connection:
        fact = (
            connection.execute(
                text(
                    """
                    SELECT fact.*, auth.usage_fact_id
                    FROM usage_facts AS fact
                    JOIN provisioning_authorizations AS auth
                      ON auth.id = fact.authorization_id
                    WHERE fact.operation_id = :operation_id
                    """
                ),
                {"operation_id": prepared.operation_id},
            )
            .mappings()
            .one()
        )
        timestamps = (
            connection.execute(
                text(
                    """
                SELECT operation.completed_at, operation.updated_at,
                       runtime.confirmed_at AS runtime_confirmed_at,
                       ingress.confirmed_at AS ingress_confirmed_at
                FROM provisioning_operations AS operation
                JOIN runtime_submissions AS runtime ON runtime.operation_id = operation.id
                JOIN ingress_submissions AS ingress ON ingress.operation_id = operation.id
                WHERE operation.id = :operation_id
                """
                ),
                {"operation_id": prepared.operation_id},
            )
            .mappings()
            .one()
        )

    assert fact["id"] == fact["usage_fact_id"]
    assert fact["metric"] == "crusher.deployment.activated"
    assert fact["unit"] == "deployment"
    assert fact["quantity"] == 1
    assert fact["source_event"] == "provisioning.succeeded"
    assert fact["schema_version"] == 1
    assert fact["occurred_at"] == timestamps["completed_at"]
    assert fact["occurred_at"] == timestamps["updated_at"]
    assert fact["occurred_at"] == timestamps["runtime_confirmed_at"]
    assert fact["occurred_at"] == timestamps["ingress_confirmed_at"]
    assert fact["payload_digest"] == usage_fact_payload_digest(
        usage_fact_id=fact["id"],
        entitlement_subject_id=fact["subject_id"],
        entitlement_revision_id=fact["revision_id"],
        authorization_id=fact["authorization_id"],
        organization_id=fact["organization_id"],
        operation_id=fact["operation_id"],
        deployment_id=fact["deployment_id"],
        occurred_at=fact["occurred_at"],
    )


@pytest.mark.postgres
def test_required_joint_success_replay_adopts_one_exact_usage_fact(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import finalize_provisioning_success, mark_submitted
    from fstack.runtime_state import persist_submitted

    prepared = _required_prepared_bundle(migrated_database, monkeypatch)
    persist_submitted(prepared.runtime_submission_id, "docker-api-required-replay")
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)

    finalize_provisioning_success(
        prepared.runtime_submission_id,
        prepared.ingress_submission_id,
        prepared.operation_id,
    )
    with migrated_database.connect() as connection:
        first = dict(
            connection.execute(
                text("SELECT * FROM usage_facts WHERE operation_id=:operation_id"),
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
        facts = (
            connection.execute(
                text("SELECT * FROM usage_facts WHERE operation_id=:operation_id"),
                {"operation_id": prepared.operation_id},
            )
            .mappings()
            .all()
        )
    assert [dict(fact) for fact in facts] == [first]


@pytest.mark.postgres
def test_required_joint_success_replay_rejects_missing_usage_fact(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import (
        IngressStateError,
        finalize_provisioning_success,
        mark_submitted,
    )
    from fstack.runtime_state import persist_submitted

    prepared = _required_prepared_bundle(migrated_database, monkeypatch)
    persist_submitted(prepared.runtime_submission_id, "docker-api-missing-usage-replay")
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    finalize_provisioning_success(
        prepared.runtime_submission_id,
        prepared.ingress_submission_id,
        prepared.operation_id,
    )
    with migrated_database.begin() as connection:
        connection.execute(
            text("ALTER TABLE usage_facts DISABLE TRIGGER tr_usage_facts_append_only")
        )
        connection.execute(
            text("DELETE FROM usage_facts WHERE operation_id=:operation_id"),
            {"operation_id": prepared.operation_id},
        )
        connection.execute(
            text("ALTER TABLE usage_facts ENABLE TRIGGER tr_usage_facts_append_only")
        )

    with pytest.raises(IngressStateError, match="^provisioning_success_infrastructure$"):
        finalize_provisioning_success(
            prepared.runtime_submission_id,
            prepared.ingress_submission_id,
            prepared.operation_id,
        )


@pytest.mark.postgres
def test_disabled_joint_success_emits_no_usage_fact(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import api
    from fstack.ingress_state import finalize_provisioning_success, mark_submitted
    from fstack.ingress_submission import prepare_operation_bundle
    from fstack.runtime_state import persist_submitted

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "disabled")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    monkeypatch.setattr(api, "create_engine", lambda *_args, **_kwargs: migrated_database)
    with TestClient(create_app()) as client:
        response = client.post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": "Bearer test-operator-token",
                "Idempotency-Key": "disabled-usage-finalization",
            },
            json={**VALID_BODY, "organization_slug": "disabled-usage-acme"},
        )
    assert response.status_code == 202
    prepared = prepare_operation_bundle(UUID(response.json()["operation_id"]))
    persist_submitted(prepared.runtime_submission_id, "docker-api-disabled-usage")
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)

    finalize_provisioning_success(
        prepared.runtime_submission_id,
        prepared.ingress_submission_id,
        prepared.operation_id,
    )

    with migrated_database.connect() as connection:
        count = connection.execute(text("SELECT count(*) FROM usage_facts")).scalar_one()
    assert count == 0


@pytest.mark.postgres
def test_suppressed_usage_insert_rolls_back_joint_success(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import (
        IngressStateError,
        finalize_provisioning_success,
        mark_submitted,
    )
    from fstack.runtime_state import persist_submitted

    prepared = _required_prepared_bundle(migrated_database, monkeypatch)
    persist_submitted(prepared.runtime_submission_id, "docker-api-suppressed-usage")
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION suppress_usage_fact_insert() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER tr_suppress_usage_fact_insert
                BEFORE INSERT ON usage_facts
                FOR EACH ROW EXECUTE FUNCTION suppress_usage_fact_insert()
                """
            )
        )

    with pytest.raises(IngressStateError, match="^provisioning_success_infrastructure$"):
        finalize_provisioning_success(
            prepared.runtime_submission_id,
            prepared.ingress_submission_id,
            prepared.operation_id,
        )

    with migrated_database.connect() as connection:
        state = (
            connection.execute(
                text(
                    """
                SELECT operation.state AS operation_state,
                       runtime.state AS runtime_state, ingress.state AS ingress_state
                FROM provisioning_operations AS operation
                JOIN runtime_submissions AS runtime ON runtime.operation_id=operation.id
                JOIN ingress_submissions AS ingress ON ingress.operation_id=operation.id
                WHERE operation.id=:operation_id
                """
                ),
                {"operation_id": prepared.operation_id},
            )
            .mappings()
            .one()
        )
        usage_count = connection.execute(text("SELECT count(*) FROM usage_facts")).scalar_one()
        audit_count = connection.execute(
            text(
                "SELECT count(*) FROM audit_events WHERE operation_id=:operation_id "
                "AND event_type='provisioning.succeeded'"
            ),
            {"operation_id": prepared.operation_id},
        ).scalar_one()
    assert dict(state) == {
        "operation_state": "running",
        "runtime_state": "submitted",
        "ingress_state": "submitted",
    }
    assert usage_count == 0
    assert audit_count == 0


@pytest.mark.postgres
def test_required_failed_operation_emits_no_usage_fact(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import finalize_provisioning_failure

    prepared = _required_prepared_bundle(migrated_database, monkeypatch)
    finalize_provisioning_failure(prepared.operation_id, "runtime", "runtime_unavailable")

    with migrated_database.connect() as connection:
        count = connection.execute(text("SELECT count(*) FROM usage_facts")).scalar_one()
    assert count == 0
