from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from time import monotonic, sleep
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text
from sqlalchemy.exc import IntegrityError

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


@pytest.mark.postgres
def test_create_operation_persists_complete_pending_desired_state(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app())

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "create-acme-mailer",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["state"] == "pending"
    assert payload["replayed"] is False
    for field in (
        "operation_id",
        "organization_id",
        "business_id",
        "environment_id",
        "workload_id",
        "deployment_id",
    ):
        UUID(payload[field])

    with migrated_database.connect() as connection:
        counts = {
            table: connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()
            for table in (
                "organizations",
                "businesses",
                "environments",
                "workloads",
                "deployments",
                "database_bindings",
                "storage_bindings",
                "provisioning_operations",
                "audit_events",
            )
        }
        operation = (
            connection.execute(
                text(
                    "SELECT state, organization_slug, idempotency_key, actor_principal "
                    "FROM provisioning_operations"
                )
            )
            .mappings()
            .one()
        )
        audit = (
            connection.execute(
                text("SELECT event_type, actor_principal, payload FROM audit_events")
            )
            .mappings()
            .one()
        )

    assert set(counts.values()) == {1}
    assert operation == {
        "state": "pending",
        "organization_slug": "acme",
        "idempotency_key": "create-acme-mailer",
        "actor_principal": "installation-operator",
    }
    assert audit == {
        "event_type": "provisioning.requested",
        "actor_principal": "installation-operator",
        "payload": {},
    }


@pytest.mark.postgres
def test_idempotency_key_reuse_with_different_payload_is_a_sanitized_conflict(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app(), raise_server_exceptions=False)
    headers = {
        "Authorization": "Bearer test-operator-token",
        "Idempotency-Key": "create-acme-mailer",
    }
    assert (
        client.post("/v1/provisioning/operations", headers=headers, json=VALID_BODY).status_code
        == 202
    )

    conflicting = VALID_BODY | {"business_name": "Different Name"}
    response = client.post("/v1/provisioning/operations", headers=headers, json=conflicting)

    assert response.status_code == 409
    assert response.json() == {
        "code": "idempotency_conflict",
        "message": "idempotency key was already used for a different request",
    }
    serialized = response.text
    assert "secret://" not in serialized
    assert "Different Name" not in serialized
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 1
        )
        assert connection.execute(text("SELECT count(*) FROM audit_events")).scalar_one() == 1


@pytest.mark.postgres
def test_identical_replay_returns_the_original_operation_without_new_rows(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app())
    headers = {
        "Authorization": "Bearer test-operator-token",
        "Idempotency-Key": "create-acme-mailer",
    }

    created = client.post("/v1/provisioning/operations", headers=headers, json=VALID_BODY)
    replayed = client.post("/v1/provisioning/operations", headers=headers, json=VALID_BODY)

    assert created.status_code == 202
    assert replayed.status_code == 200
    assert replayed.json() == created.json() | {"replayed": True}
    with migrated_database.connect() as connection:
        for table in (
            "organizations",
            "businesses",
            "environments",
            "workloads",
            "deployments",
            "database_bindings",
            "storage_bindings",
            "provisioning_operations",
            "audit_events",
        ):
            assert connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one() == 1


@pytest.mark.postgres
def test_different_key_cannot_adopt_an_existing_organization(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app(), raise_server_exceptions=False)
    first_headers = {
        "Authorization": "Bearer test-operator-token",
        "Idempotency-Key": "create-acme-mailer",
    }
    assert (
        client.post(
            "/v1/provisioning/operations", headers=first_headers, json=VALID_BODY
        ).status_code
        == 202
    )

    second_headers = first_headers | {"Idempotency-Key": "different-command"}
    response = client.post(
        "/v1/provisioning/operations",
        headers=second_headers,
        json=VALID_BODY | {"business_slug": "other", "business_name": "Other"},
    )

    assert response.status_code == 409
    assert response.json() == {
        "code": "organization_exists",
        "message": "organization slug is already reserved",
    }
    assert "secret://" not in response.text
    with migrated_database.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM organizations")).scalar_one() == 1
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 1
        )
        assert connection.execute(text("SELECT count(*) FROM audit_events")).scalar_one() == 1


@pytest.mark.postgres
def test_unauthorized_request_returns_stable_error_without_persistence(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app())

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer wrong-token",
            "Idempotency-Key": "create-acme-mailer",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 401
    assert response.json() == {
        "code": "unauthorized",
        "message": "operator authorization required",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_validation_error_does_not_echo_secret_shaped_input(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app())
    submitted_secret = "postgresql://operator:do-not-echo@database/acme"

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "create-acme-mailer",
        },
        json=VALID_BODY | {"database_secret_ref": submitted_secret},
    )

    assert response.status_code == 422
    assert response.json() == {
        "code": "invalid_request",
        "message": "request validation failed",
    }
    assert submitted_secret not in response.text
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_concurrent_identical_requests_converge_on_one_operation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    barrier = Barrier(2)

    def submit() -> tuple[int, dict[str, object]]:
        client = TestClient(create_app())
        barrier.wait(timeout=5)
        response = client.post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": "Bearer test-operator-token",
                "Idempotency-Key": "create-acme-mailer",
            },
            json=VALID_BODY,
        )
        return response.status_code, response.json()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: submit(), range(2)))

    assert sorted(status_code for status_code, _ in results) == [200, 202]
    identities = [payload | {"replayed": False} for _, payload in results]
    assert identities[0] == identities[1]
    with migrated_database.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM organizations")).scalar_one() == 1
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 1
        )
        assert connection.execute(text("SELECT count(*) FROM audit_events")).scalar_one() == 1


@pytest.mark.postgres
def test_public_domain_reservation_conflict_is_sanitized_and_atomic(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app(), raise_server_exceptions=False)
    headers = {"Authorization": "Bearer test-operator-token"}
    first = client.post(
        "/v1/provisioning/operations",
        headers=headers | {"Idempotency-Key": "create-acme-mailer"},
        json=VALID_BODY | {"public_domain": "shared.example"},
    )
    assert first.status_code == 202

    response = client.post(
        "/v1/provisioning/operations",
        headers=headers | {"Idempotency-Key": "create-beta-mailer"},
        json=VALID_BODY
        | {
            "organization_slug": "beta",
            "organization_name": "Beta Robotics",
            "public_domain": "shared.example",
        },
    )

    assert response.status_code == 409
    assert response.json() == {
        "code": "public_domain_exists",
        "message": "public domain is already reserved",
    }
    assert "secret://" not in response.text
    assert "organization_id" not in response.text
    with migrated_database.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM organizations")).scalar_one() == 1
        operation_count = connection.execute(
            text("SELECT count(*) FROM provisioning_operations")
        ).scalar_one()
        assert operation_count == 1
        assert connection.execute(text("SELECT count(*) FROM audit_events")).scalar_one() == 1


@pytest.mark.postgres
def test_long_valid_immutable_image_reference_persists_without_truncation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app(), raise_server_exceptions=False)
    image = "ghcr.io/" + "a" * 480 + "/crusher@sha256:" + "b" * 64

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "create-long-image",
        },
        json=VALID_BODY | {"crusher_image": image},
    )

    assert len(image) > 500
    assert response.status_code == 202
    with migrated_database.connect() as connection:
        stored = connection.execute(text("SELECT crusher_image FROM deployments")).scalar_one()
    assert stored == image


@pytest.mark.postgres
def test_database_error_rendering_hides_secret_reference_parameters(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    database_secret = "secret://vault/acme/database"
    storage_secret = "secret://vault/acme/object-storage"
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE database_bindings ADD CONSTRAINT "
                "ck_test_reject_database_secret "
                "CHECK (secret_ref <> 'secret://vault/acme/database')"
            )
        )
    client = TestClient(create_app(), raise_server_exceptions=False)

    response = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "force-database-failure",
        },
        json=VALID_BODY,
    )

    assert response.status_code == 500
    assert response.json() == {
        "code": "persistence_failed",
        "message": "request could not be persisted",
    }
    evidence = response.text + caplog.text
    assert database_secret not in evidence
    assert storage_secret not in evidence
    with migrated_database.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM organizations")).scalar_one() == 0
        assert (
            connection.execute(text("SELECT count(*) FROM provisioning_operations")).scalar_one()
            == 0
        )


@pytest.mark.postgres
def test_postgres_constraints_reject_cross_organization_relationships(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app())
    headers = {"Authorization": "Bearer test-operator-token"}
    assert (
        client.post(
            "/v1/provisioning/operations",
            headers=headers | {"Idempotency-Key": "create-acme"},
            json=VALID_BODY,
        ).status_code
        == 202
    )
    assert (
        client.post(
            "/v1/provisioning/operations",
            headers=headers | {"Idempotency-Key": "create-beta"},
            json=VALID_BODY
            | {
                "organization_slug": "beta",
                "organization_name": "Beta Robotics",
                "business_slug": "beta-mailer",
                "business_name": "Beta Mailer",
                "public_domain": "mail.beta.example",
            },
        ).status_code
        == 202
    )

    with migrated_database.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT o.slug AS organization_slug, o.id AS organization_id,
                       b.id AS business_id, e.id AS environment_id,
                       w.id AS workload_id, d.id AS deployment_id,
                       db.id AS database_binding_id, sb.id AS storage_binding_id,
                       po.id AS operation_id, ae.id AS audit_event_id
                FROM organizations AS o
                JOIN businesses AS b ON b.organization_id = o.id
                JOIN environments AS e ON e.business_id = b.id
                JOIN workloads AS w ON w.environment_id = e.id
                JOIN deployments AS d ON d.workload_id = w.id
                JOIN database_bindings AS db ON db.deployment_id = d.id
                JOIN storage_bindings AS sb ON sb.deployment_id = d.id
                JOIN provisioning_operations AS po ON po.deployment_id = d.id
                JOIN audit_events AS ae ON ae.operation_id = po.id
                """
            )
        ).mappings()
        resources = {row["organization_slug"]: row for row in rows}

    acme = resources["acme"]
    beta = resources["beta"]
    probes = (
        ("businesses", acme["business_id"]),
        ("environments", acme["environment_id"]),
        ("workloads", acme["workload_id"]),
        ("deployments", acme["deployment_id"]),
        ("database_bindings", acme["database_binding_id"]),
        ("storage_bindings", acme["storage_binding_id"]),
        ("provisioning_operations", acme["operation_id"]),
        ("audit_events", acme["audit_event_id"]),
    )
    for table, resource_id in probes:
        connection = migrated_database.connect()
        transaction = connection.begin()
        try:
            with pytest.raises(IntegrityError):
                connection.execute(
                    text(f"UPDATE {table} SET organization_id = :other WHERE id = :id"),
                    {"other": beta["organization_id"], "id": resource_id},
                )
                connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        finally:
            transaction.rollback()
            connection.close()


@pytest.mark.postgres
def test_competing_uncommitted_claim_rollback_allows_request_to_win(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    request = ProvisioningRequest.model_validate(VALID_BODY)
    blocker_ids = [uuid4() for _ in range(6)]
    blocker_connection = migrated_database.connect()
    blocker_transaction = blocker_connection.begin()
    blocker_connection.execute(
        text(
            """
            INSERT INTO provisioning_operations (
                id, organization_id, organization_slug, idempotency_key,
                request_fingerprint, state, actor_principal, business_id,
                environment_id, workload_id, deployment_id
            ) VALUES (
                :operation_id, :organization_id, 'acme', 'rollback-race',
                :request_fingerprint, 'pending', 'installation-operator', :business_id,
                :environment_id, :workload_id, :deployment_id
            )
            """
        ),
        {
            "operation_id": blocker_ids[0],
            "organization_id": blocker_ids[1],
            "business_id": blocker_ids[2],
            "environment_id": blocker_ids[3],
            "workload_id": blocker_ids[4],
            "deployment_id": blocker_ids[5],
            "request_fingerprint": request_fingerprint(request),
        },
    )

    client = TestClient(create_app())

    def submit():
        return client.post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": "Bearer test-operator-token",
                "Idempotency-Key": "rollback-race",
            },
            json=VALID_BODY,
        )

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(submit)
            deadline = monotonic() + 5
            blocked_on_claim = False
            while monotonic() < deadline:
                with migrated_database.connect() as monitor:
                    blocked_on_claim = monitor.execute(
                        text(
                            """
                            SELECT EXISTS (
                                SELECT 1
                                FROM pg_stat_activity
                                WHERE datname = current_database()
                                  AND pid <> pg_backend_pid()
                                  AND state = 'active'
                                  AND wait_event_type = 'Lock'
                                  AND query ILIKE '%INSERT INTO provisioning_operations%'
                            )
                            """
                        )
                    ).scalar_one()
                if blocked_on_claim:
                    break
                sleep(0.05)
            assert blocked_on_claim
            blocker_transaction.rollback()
            response = future.result(timeout=5)
    finally:
        if blocker_transaction.is_active:
            blocker_transaction.rollback()
        blocker_connection.close()

    assert response.status_code == 202
    assert response.json()["replayed"] is False
    with migrated_database.connect() as connection:
        for table in (
            "organizations",
            "businesses",
            "environments",
            "workloads",
            "deployments",
            "database_bindings",
            "storage_bindings",
            "provisioning_operations",
            "audit_events",
        ):
            assert connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one() == 1
