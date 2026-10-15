from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from time import monotonic, sleep
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

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
                "workflow_dispatches",
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
        dispatch = (
            connection.execute(
                text(
                    "SELECT operation_id, workflow_id, workflow_type, state, dispatched_at "
                    "FROM workflow_dispatches"
                )
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
    assert dispatch == {
        "operation_id": UUID(payload["operation_id"]),
        "workflow_id": f"provisioning/{payload['operation_id']}",
        "workflow_type": "fstack.provision_crusher.v2",
        "state": "pending",
        "dispatched_at": None,
    }


@pytest.mark.postgres
def test_pending_operation_can_be_read_after_creation(
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

    response = client.get(
        f"/v1/provisioning/operations/{created.json()['operation_id']}",
        headers={"Authorization": "Bearer test-operator-token"},
    )

    assert created.status_code == 202
    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {
        "operation_id",
        "organization_id",
        "business_id",
        "environment_id",
        "workload_id",
        "deployment_id",
        "state",
        "created_at",
        "started_at",
        "completed_at",
        "last_error",
    }
    assert {
        key: payload[key]
        for key in (
            "operation_id",
            "organization_id",
            "business_id",
            "environment_id",
            "workload_id",
            "deployment_id",
        )
    } == {
        key: created.json()[key]
        for key in (
            "operation_id",
            "organization_id",
            "business_id",
            "environment_id",
            "workload_id",
            "deployment_id",
        )
    }
    assert payload["state"] == "pending"
    assert payload["started_at"] is None
    assert payload["completed_at"] is None
    assert payload["last_error"] is None
    assert payload["created_at"].endswith("Z")
    assert response.headers["cache-control"] == "no-store"
    assert "replayed" not in payload


@pytest.mark.postgres
def test_operation_status_reports_success_only_after_joint_finalization(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import finalize_provisioning_success, mark_submitted
    from fstack.ingress_submission import prepare_operation_bundle
    from fstack.runtime_state import persist_submitted

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    client = TestClient(create_app())
    created = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "joint-success-api-contract",
        },
        json=VALID_BODY,
    )
    created_payload = created.json()
    operation_id = UUID(created_payload["operation_id"])
    prepared = prepare_operation_bundle(operation_id)

    def get_status():
        response = client.get(
            f"/v1/provisioning/operations/{operation_id}",
            headers={"Authorization": "Bearer test-operator-token"},
        )
        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-store"
        return response

    prepared_response = get_status()
    prepared_status = prepared_response.json()
    assert prepared_status["state"] == "running"
    assert prepared_status["started_at"] is not None
    assert prepared_status["completed_at"] is None
    assert prepared_status["last_error"] is None
    for identity in (
        "operation_id",
        "organization_id",
        "business_id",
        "environment_id",
        "workload_id",
        "deployment_id",
    ):
        assert prepared_status[identity] == created_payload[identity]

    persist_submitted(prepared.runtime_submission_id, "d" * 64)
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    submitted_response = get_status()
    submitted_status = submitted_response.json()
    assert submitted_status["state"] == "running"
    assert submitted_status["started_at"] == prepared_status["started_at"]
    assert submitted_status["completed_at"] is None
    assert submitted_status["last_error"] is None

    finalize_provisioning_success(
        prepared.runtime_submission_id,
        prepared.ingress_submission_id,
        prepared.operation_id,
    )
    succeeded_response = get_status()
    succeeded = succeeded_response.json()
    assert set(succeeded) == {
        "operation_id",
        "organization_id",
        "business_id",
        "environment_id",
        "workload_id",
        "deployment_id",
        "state",
        "created_at",
        "started_at",
        "completed_at",
        "last_error",
    }
    assert succeeded["state"] == "succeeded"
    assert succeeded["started_at"] is not None
    assert succeeded["completed_at"] is not None
    assert succeeded["last_error"] is None
    for forbidden in (
        "provider",
        "generation",
        "submission_key",
        "spec_fingerprint",
        "api_container_name",
        "migration_container_name",
        "runtime_submission_id",
        "ingress_submission_id",
        "route_id",
        "route_fingerprint",
    ):
        assert forbidden not in succeeded_response.text

    with migrated_database.connect() as connection:
        runtime_details = (
            connection.execute(
                text(
                    "SELECT provider, submission_key, spec_fingerprint, "
                    "api_container_name, migration_container_name "
                    "FROM runtime_submissions WHERE id=:submission_id"
                ),
                {"submission_id": prepared.runtime_submission_id},
            )
            .mappings()
            .one()
        )
        ingress_details = (
            connection.execute(
                text(
                    "SELECT provider, submission_key, route_fingerprint, route_id "
                    "FROM ingress_submissions WHERE id=:submission_id"
                ),
                {"submission_id": prepared.ingress_submission_id},
            )
            .mappings()
            .one()
        )
        audits = (
            connection.execute(
                text(
                    "SELECT event_type, actor_principal, organization_id, operation_id, "
                    "payload, created_at FROM audit_events "
                    "WHERE operation_id=:operation_id ORDER BY created_at, id"
                ),
                {"operation_id": operation_id},
            )
            .mappings()
            .all()
        )
    for internal_value in (
        *runtime_details.values(),
        *ingress_details.values(),
        prepared.runtime_submission_id,
        prepared.ingress_submission_id,
    ):
        assert str(internal_value) not in succeeded_response.text
    assert [audit["event_type"] for audit in audits] == [
        "provisioning.requested",
        "provisioning.started",
        "provisioning.succeeded",
    ]
    assert all(audit["actor_principal"] == "installation-operator" for audit in audits)
    assert all(
        audit["organization_id"] == UUID(created_payload["organization_id"]) for audit in audits
    )
    assert all(audit["operation_id"] == operation_id for audit in audits)
    assert all(audit["created_at"].utcoffset() is not None for audit in audits)
    assert [audit["payload"] for audit in audits] == [
        {},
        {
            "runtime_submission_id": str(prepared.runtime_submission_id),
            "ingress_submission_id": str(prepared.ingress_submission_id),
        },
        {
            "runtime_submission_id": str(prepared.runtime_submission_id),
            "ingress_submission_id": str(prepared.ingress_submission_id),
        },
    ]


@pytest.mark.postgres
def test_failed_operation_status_exposes_only_sanitized_error_code(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack.ingress_state import finalize_provisioning_failure
    from fstack.ingress_submission import prepare_operation_bundle

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    client = TestClient(create_app())
    created = client.post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "failed-api-contract",
        },
        json=VALID_BODY,
    )
    created_payload = created.json()
    operation_id = UUID(created_payload["operation_id"])
    prepared = prepare_operation_bundle(operation_id)
    finalize_provisioning_failure(operation_id, "runtime", "docker_unavailable")

    response = client.get(
        f"/v1/provisioning/operations/{operation_id}",
        headers={"Authorization": "Bearer test-operator-token"},
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    payload = response.json()
    assert payload["state"] == "failed"
    assert payload["started_at"] is not None
    assert payload["completed_at"] is not None
    assert payload["last_error"] == {"code": "docker_unavailable"}
    assert set(payload) == {
        "operation_id",
        "organization_id",
        "business_id",
        "environment_id",
        "workload_id",
        "deployment_id",
        "state",
        "created_at",
        "started_at",
        "completed_at",
        "last_error",
    }
    for identity in (
        "operation_id",
        "organization_id",
        "business_id",
        "environment_id",
        "workload_id",
        "deployment_id",
    ):
        assert payload[identity] == created_payload[identity]

    with migrated_database.connect() as connection:
        failure_audit = (
            connection.execute(
                text(
                    "SELECT actor_principal, organization_id, operation_id, payload, created_at "
                    "FROM audit_events WHERE operation_id=:operation_id "
                    "AND event_type='provisioning.failed'"
                ),
                {"operation_id": operation_id},
            )
            .mappings()
            .one()
        )
    assert failure_audit["actor_principal"] == "installation-operator"
    assert failure_audit["organization_id"] == UUID(created_payload["organization_id"])
    assert failure_audit["operation_id"] == operation_id
    assert failure_audit["created_at"].utcoffset() is not None
    assert failure_audit["payload"] == {
        "runtime_submission_id": str(prepared.runtime_submission_id),
        "ingress_submission_id": str(prepared.ingress_submission_id),
        "error_code": "docker_unavailable",
    }


@pytest.mark.postgres
def test_operation_status_authenticates_before_lookup(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    lookup_called = False

    def fail_if_called(*_args: object, **_kwargs: object) -> None:
        nonlocal lookup_called
        lookup_called = True
        raise AssertionError("lookup must not run before authentication")

    monkeypatch.setattr("fstack.api.get_provisioning_operation", fail_if_called)
    client = TestClient(create_app())

    response = client.get(f"/v1/provisioning/operations/{uuid4()}")

    assert lookup_called is False
    assert response.status_code == 401
    assert response.json() == {
        "code": "unauthorized",
        "message": "operator authorization required",
    }
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.postgres
def test_unknown_operation_status_returns_exact_not_found(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app(), raise_server_exceptions=False)

    response = client.get(
        f"/v1/provisioning/operations/{uuid4()}",
        headers={"Authorization": "Bearer test-operator-token"},
    )

    assert response.status_code == 404
    assert response.json() == {
        "code": "operation_not_found",
        "message": "operation was not found",
    }
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.postgres
def test_operation_status_persistence_failure_is_sanitized(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    marker = "raw-secret-marker"

    def fail_lookup(*_args: object, **_kwargs: object) -> None:
        raise SQLAlchemyError(marker)

    monkeypatch.setattr("fstack.api.get_provisioning_operation", fail_lookup)
    client = TestClient(create_app(), raise_server_exceptions=False)

    response = client.get(
        f"/v1/provisioning/operations/{uuid4()}",
        headers={"Authorization": "Bearer test-operator-token"},
    )

    assert response.status_code == 500
    assert response.json() == {
        "code": "persistence_failed",
        "message": "request could not be persisted",
    }
    assert response.headers["cache-control"] == "no-store"
    assert marker not in response.text


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
            "workflow_dispatches",
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
            if not blocked_on_claim and blocker_transaction.is_active:
                blocker_transaction.rollback()
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
            "workflow_dispatches",
            "audit_events",
        ):
            assert connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one() == 1
