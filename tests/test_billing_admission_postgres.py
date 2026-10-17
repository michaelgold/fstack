from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text

from fstack.api import create_app
from fstack.provisioning import ProvisioningRequest, request_fingerprint

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


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _required_client(
    monkeypatch: pytest.MonkeyPatch,
    *,
    raise_server_exceptions: bool = True,
) -> TestClient:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "required")
    monkeypatch.setenv(
        "FSTACK_BILLING_RECONCILE_TOKEN", "reconcile-token-which-is-long-enough-0001"
    )
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", "export-token-which-is-long-enough-0001")
    return TestClient(create_app(), raise_server_exceptions=raise_server_exceptions)


def _publish_revision(
    client: TestClient,
    *,
    state: str = "active",
    observed_at: datetime | None = None,
    valid_until: datetime | None = None,
    generation: int = 1,
) -> tuple[UUID, UUID, datetime, datetime]:
    now = datetime.now(UTC)
    observed_at = observed_at or now - timedelta(minutes=1)
    valid_until = valid_until or now + timedelta(minutes=4)
    revision_id = uuid4()
    response = client.put(
        "/internal/billing/entitlement-revisions",
        headers={
            "Authorization": "Bearer reconcile-token-which-is-long-enough-0001",
            "Content-Type": "application/json",
        },
        json={
            "organization_slug": VALID_BODY["organization_slug"],
            "entitlement_revision_id": str(revision_id),
            "generation": generation,
            "plan_key": "crusher.single.v1",
            "state": state,
            "capability": "crusher.deployment.create",
            "quota_dimension": "crusher.deployment.active",
            "quota_limit": 1,
            "observed_at": _canonical_timestamp(observed_at),
            "valid_until": _canonical_timestamp(valid_until),
            "source_revision_id": str(uuid4()),
        },
    )
    assert response.status_code == 201
    return (
        UUID(response.json()["entitlement_subject_id"]),
        revision_id,
        observed_at,
        valid_until,
    )


@pytest.mark.postgres
def test_required_mode_admits_active_entitlement_and_persists_authorization(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _required_client(monkeypatch)
    subject_id, revision_id, observed_at, valid_until = _publish_revision(client)

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-active-entitlement",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 202
    payload = response.json()
    with migrated_database.connect() as connection:
        operation = (
            connection.execute(
                text(
                    "SELECT authorization_mode, request_fingerprint "
                    "FROM provisioning_operations WHERE id = :operation_id"
                ),
                {"operation_id": UUID(payload["operation_id"])},
            )
            .mappings()
            .one()
        )
        authorization = (
            connection.execute(
                text(
                    """
                    SELECT id, usage_fact_id, subject_id, revision_id, organization_id,
                           operation_id, deployment_id, capability, quota_dimension,
                           units, request_fingerprint, decided_at
                    FROM provisioning_authorizations
                    WHERE operation_id = :operation_id
                    """
                ),
                {"operation_id": UUID(payload["operation_id"])},
            )
            .mappings()
            .one()
        )

    expected_fingerprint = request_fingerprint(ProvisioningRequest.model_validate(VALID_BODY))
    assert operation["authorization_mode"] == "required"
    assert operation["request_fingerprint"] == expected_fingerprint
    assert authorization["request_fingerprint"] == expected_fingerprint
    assert authorization["subject_id"] == subject_id
    assert authorization["revision_id"] == revision_id
    assert authorization["organization_id"] == UUID(payload["organization_id"])
    assert authorization["operation_id"] == UUID(payload["operation_id"])
    assert authorization["deployment_id"] == UUID(payload["deployment_id"])
    assert authorization["capability"] == "crusher.deployment.create"
    assert authorization["quota_dimension"] == "crusher.deployment.active"
    assert authorization["units"] == 1
    assert observed_at <= authorization["decided_at"] <= valid_until
    UUID(str(authorization["id"]))
    UUID(str(authorization["usage_fact_id"]))


@pytest.mark.postgres
def test_required_mode_missing_subject_fails_closed_without_durable_rows(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _required_client(monkeypatch)

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-missing-subject",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "entitlement_state_unavailable",
        "message": "entitlement state is unavailable",
    }
    with migrated_database.connect() as connection:
        for table in (
            "provisioning_operations",
            "provisioning_authorizations",
            "organizations",
            "deployments",
            "workflow_dispatches",
            "audit_events",
        ):
            assert connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one() == 0


@pytest.mark.postgres
def test_required_mode_inactive_entitlement_returns_subscription_inactive(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _required_client(monkeypatch)
    _publish_revision(client, state="inactive")

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-inactive-entitlement",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 402
    assert response.json() == {
        "code": "subscription_inactive",
        "message": "subscription is inactive",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM provisioning_authorizations")
            ).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_required_mode_expired_inactive_revision_is_unavailable(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _required_client(monkeypatch)
    now = datetime.now(UTC)
    _publish_revision(
        client,
        state="inactive",
        observed_at=now - timedelta(minutes=5),
        valid_until=now - timedelta(seconds=1),
    )

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-expired-inactive",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "entitlement_state_unavailable",
        "message": "entitlement state is unavailable",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_required_mode_permanent_quota_denies_second_new_operation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _required_client(monkeypatch)
    _publish_revision(client)
    first = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-quota-first",
        },
        json=VALID_BODY,
    )

    second = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-quota-second",
        },
        json=VALID_BODY,
    )

    assert first.status_code == 202
    assert second.status_code == 409
    assert second.json() == {
        "code": "entitlement_quota_exceeded",
        "message": "entitlement quota is exhausted",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 1
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM provisioning_authorizations")
            ).scalar_one()
            == 1
        )


@pytest.mark.postgres
def test_required_mode_expired_entitlement_is_unavailable(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _required_client(monkeypatch)
    now = datetime.now(UTC)
    _publish_revision(
        client,
        observed_at=now - timedelta(minutes=5),
        valid_until=now - timedelta(seconds=1),
    )

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-expired-entitlement",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "entitlement_state_unavailable",
        "message": "entitlement state is unavailable",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM provisioning_authorizations")
            ).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_required_mode_exact_replay_bypasses_later_inactive_revision(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _required_client(monkeypatch)
    _publish_revision(client)
    headers = {
        "Authorization": "Bearer test-operator-token",
        "Idempotency-Key": "required-replay-after-inactive",
    }
    first = client.post("/v1/provisioning/operations", headers=headers, json=VALID_BODY)
    _publish_revision(client, state="inactive", generation=2)

    replay = client.post("/v1/provisioning/operations", headers=headers, json=VALID_BODY)

    assert first.status_code == 202
    assert replay.status_code == 200
    assert replay.json() == {**first.json(), "replayed": True}
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 1
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM provisioning_authorizations")
            ).scalar_one()
            == 1
        )


@pytest.mark.postgres
def test_required_mode_concurrent_new_operations_commit_one_authorization(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _required_client(monkeypatch)
    _publish_revision(client)
    barrier = Barrier(2)

    def create(index: int):
        barrier.wait(timeout=5)
        return client.post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": "Bearer test-operator-token",
                "Idempotency-Key": f"required-concurrent-{index}",
            },
            json=VALID_BODY,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(create, range(2)))

    assert sorted(response.status_code for response in responses) == [202, 409]
    denied = next(response for response in responses if response.status_code == 409)
    assert denied.json() == {
        "code": "entitlement_quota_exceeded",
        "message": "entitlement quota is exhausted",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 1
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM provisioning_authorizations")
            ).scalar_one()
            == 1
        )


@pytest.mark.postgres
def test_required_mode_future_observed_revision_is_unavailable(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _required_client(monkeypatch)
    now = datetime.now(UTC)
    _publish_revision(
        client,
        observed_at=now + timedelta(minutes=1),
        valid_until=now + timedelta(minutes=4),
    )

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-future-observation",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "entitlement_state_unavailable",
        "message": "entitlement state is unavailable",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM provisioning_authorizations")
            ).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_required_mode_subject_without_revision_is_unavailable(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO entitlement_subjects (id, subject_type, subject_key)
                VALUES (:id, 'organization_slug', :subject_key)
                """
            ),
            {"id": uuid4(), "subject_key": VALID_BODY["organization_slug"]},
        )
    client = _required_client(monkeypatch)

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-missing-revision",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "entitlement_state_unavailable",
        "message": "entitlement state is unavailable",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM provisioning_authorizations")
            ).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_required_mode_rewritten_authorization_identity_rolls_back(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION rewrite_authorization_identity() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    NEW.id := 'ffffffff-ffff-4fff-bfff-ffffffffffff'::uuid;
                    RETURN NEW;
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER tr_rewrite_authorization_identity
                BEFORE INSERT ON provisioning_authorizations
                FOR EACH ROW EXECUTE FUNCTION rewrite_authorization_identity()
                """
            )
        )
    client = _required_client(monkeypatch, raise_server_exceptions=False)
    _publish_revision(client)

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-rewritten-authorization",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "entitlement_state_unavailable",
        "message": "entitlement state is unavailable",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM provisioning_authorizations")
            ).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_required_mode_rewritten_usage_identity_rolls_back(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION rewrite_usage_identity() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    NEW.usage_fact_id := 'ffffffff-ffff-4fff-bfff-ffffffffffff'::uuid;
                    RETURN NEW;
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER tr_rewrite_usage_identity
                BEFORE INSERT ON provisioning_authorizations
                FOR EACH ROW EXECUTE FUNCTION rewrite_usage_identity()
                """
            )
        )
    client = _required_client(monkeypatch, raise_server_exceptions=False)
    _publish_revision(client)

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-rewritten-usage",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "entitlement_state_unavailable",
        "message": "entitlement state is unavailable",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM provisioning_authorizations")
            ).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_required_mode_rewritten_authorization_fingerprint_rolls_back(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION rewrite_authorization_fingerprint() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    NEW.request_fingerprint := repeat('f', 64);
                    RETURN NEW;
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER tr_rewrite_authorization_fingerprint
                BEFORE INSERT ON provisioning_authorizations
                FOR EACH ROW EXECUTE FUNCTION rewrite_authorization_fingerprint()
                """
            )
        )
    client = _required_client(monkeypatch, raise_server_exceptions=False)
    _publish_revision(client)

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-rewritten-fingerprint",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "entitlement_state_unavailable",
        "message": "entitlement state is unavailable",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM provisioning_authorizations")
            ).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_required_mode_suppressed_authorization_insert_is_unavailable(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION suppress_authorization_insert() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    RETURN NULL;
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER tr_suppress_authorization_insert
                BEFORE INSERT ON provisioning_authorizations
                FOR EACH ROW EXECUTE FUNCTION suppress_authorization_insert()
                """
            )
        )
    client = _required_client(monkeypatch, raise_server_exceptions=False)
    _publish_revision(client)

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "required-suppressed-authorization",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "entitlement_state_unavailable",
        "message": "entitlement state is unavailable",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM provisioning_authorizations")
            ).scalar_one()
            == 0
        )
