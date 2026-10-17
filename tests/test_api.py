from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from fstack.api import create_app
from fstack.provisioning import ProvisioningRequest, request_fingerprint

PROVISIONING_BODY = {
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


def test_health_endpoint_does_not_require_database_access(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")

    response = TestClient(create_app()).get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "fstack"}


def test_caddy_ask_authorizes_exact_succeeded_public_domain(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")

    class Engine:
        @contextmanager
        def connect(self):
            yield object()

    monkeypatch.setattr("fstack.api._database_engine", lambda: Engine())
    monkeypatch.setattr(
        "fstack.api.is_public_domain_authorized",
        lambda _connection, *, public_domain: public_domain == "smoke.fstack.com",
        raising=False,
    )

    response = TestClient(create_app()).get(
        "/internal/caddy/ask", params={"domain": "smoke.fstack.com"}
    )

    assert response.status_code == 200
    assert response.content == b""
    assert response.headers["cache-control"] == "no-store"


def test_caddy_ask_rejects_repeated_domain_query_before_database_access(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")

    class Engine:
        @contextmanager
        def connect(self):
            raise AssertionError("database must not be queried")
            yield

    monkeypatch.setattr("fstack.api._database_engine", lambda: Engine())

    response = TestClient(create_app()).get(
        "/internal/caddy/ask?domain=smoke.fstack.com&domain=other.fstack.com"
    )

    assert response.status_code == 400
    assert response.content == b""
    assert response.headers["cache-control"] == "no-store"


def test_caddy_ask_sanitizes_unexpected_decision_failure(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")

    class Engine:
        @contextmanager
        def connect(self):
            yield object()

    def fail_lookup(*_args, **_kwargs):
        raise ValueError("certificate-decision-secret-marker")

    monkeypatch.setattr("fstack.api._database_engine", lambda: Engine())
    monkeypatch.setattr("fstack.api.is_public_domain_authorized", fail_lookup)

    response = TestClient(create_app(), raise_server_exceptions=False).get(
        "/internal/caddy/ask", params={"domain": "smoke.fstack.com"}
    )

    assert response.status_code == 503
    assert response.content == b""
    assert response.headers["cache-control"] == "no-store"
    assert "certificate-decision-secret-marker" not in response.text


@pytest.mark.parametrize(
    "query",
    [
        "",
        "?domain=",
        "?domain=smoke",
        "?domain=SMOKE.fstack.com",
        "?domain=*.fstack.com",
        "?domain=smoke.fstack.com.",
        "?domain=smoke..fstack.com",
        "?domain=smoke.fstack.com%3A443",
        "?domain=t%C3%A9st.fstack.com",
    ],
)
def test_caddy_ask_rejects_noncanonical_domain_without_database_access(
    monkeypatch,
    query: str,
) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")

    class Engine:
        @contextmanager
        def connect(self):
            raise AssertionError("database must not be queried")
            yield

    monkeypatch.setattr("fstack.api._database_engine", lambda: Engine())

    response = TestClient(create_app()).get(f"/internal/caddy/ask{query}")

    assert response.status_code == 400
    assert response.content == b""
    assert response.headers["cache-control"] == "no-store"


def test_caddy_ask_is_excluded_from_public_openapi(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")

    assert "/internal/caddy/ask" not in create_app().openapi()["paths"]


def test_framework_generated_http_errors_use_stable_envelope(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app())

    not_found = client.get("/missing")
    method_not_allowed = client.post("/health")

    assert not_found.status_code == 404
    assert not_found.json() == {"code": "http_error", "message": "request failed"}
    assert method_not_allowed.status_code == 405
    assert method_not_allowed.json() == {"code": "http_error", "message": "request failed"}
    assert method_not_allowed.headers["allow"] == "GET"


def test_non_ascii_bearer_credential_returns_stable_unauthorized(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    client = TestClient(create_app(), raise_server_exceptions=False)
    body = {
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

    response = client.post(
        "/v1/provisioning/operations",
        headers=[
            (b"authorization", "Bearer tést".encode()),
            (b"idempotency-key", b"non-ascii-auth"),
        ],
        json=body,
    )

    assert response.status_code == 401
    assert response.json() == {
        "code": "unauthorized",
        "message": "operator authorization required",
    }


def test_unauthenticated_invalid_operation_uuid_authenticates_before_validation(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")

    response = TestClient(create_app()).get("/v1/provisioning/operations/not-a-uuid")

    assert response.status_code == 401
    assert response.json() == {
        "code": "unauthorized",
        "message": "operator authorization required",
    }
    assert response.headers["cache-control"] == "no-store"


def test_authenticated_invalid_operation_uuid_is_not_cacheable(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")

    response = TestClient(create_app()).get(
        "/v1/provisioning/operations/not-a-uuid",
        headers={"Authorization": "Bearer test-operator-token"},
    )

    assert response.status_code == 422
    assert response.json() == {
        "code": "invalid_request",
        "message": "request validation failed",
    }
    assert response.headers["cache-control"] == "no-store"


def test_unexpected_operation_status_failure_is_sanitized_and_not_cacheable(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")

    def fail_lookup(*_args, **_kwargs):
        raise ValueError("provider-secret-marker")

    class Engine:
        @contextmanager
        def connect(self):
            yield object()

    monkeypatch.setattr("fstack.api._database_engine", lambda: Engine())
    monkeypatch.setattr("fstack.api.get_provisioning_operation", fail_lookup)
    response = TestClient(create_app(), raise_server_exceptions=False).get(
        "/v1/provisioning/operations/11111111-1111-4111-8111-111111111111",
        headers={"Authorization": "Bearer test-operator-token"},
    )

    assert response.status_code == 500
    assert response.json() == {"code": "internal_error", "message": "request failed"}
    assert response.headers["cache-control"] == "no-store"
    assert "provider-secret-marker" not in response.text


def test_invalid_operation_status_serialization_is_sanitized_and_not_cacheable(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")

    class Engine:
        @contextmanager
        def connect(self):
            yield object()

    monkeypatch.setattr("fstack.api._database_engine", lambda: Engine())
    monkeypatch.setattr(
        "fstack.api.get_provisioning_operation",
        lambda *_args, **_kwargs: {"provider_secret": "provider-secret-marker"},
    )
    response = TestClient(create_app(), raise_server_exceptions=False).get(
        "/v1/provisioning/operations/11111111-1111-4111-8111-111111111111",
        headers={"Authorization": "Bearer test-operator-token"},
    )

    assert response.status_code == 500
    assert response.json() == {"code": "internal_error", "message": "request failed"}
    assert response.headers["cache-control"] == "no-store"
    assert "provider-secret-marker" not in response.text


def test_provisioning_post_openapi_and_fingerprint_remain_frozen(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    openapi = create_app().openapi()
    post = openapi["paths"]["/v1/provisioning/operations"]["post"]
    request_schema = openapi["components"]["schemas"]["ProvisioningRequest"]
    response_schema = openapi["components"]["schemas"]["ProvisioningResponse"]

    assert post["requestBody"] == {
        "required": True,
        "content": {
            "application/json": {"schema": {"$ref": "#/components/schemas/ProvisioningRequest"}}
        },
    }
    assert post["responses"]["202"] == {
        "description": "Successful Response",
        "content": {
            "application/json": {"schema": {"$ref": "#/components/schemas/ProvisioningResponse"}}
        },
    }
    assert request_schema["additionalProperties"] is False
    assert request_schema["required"] == list(PROVISIONING_BODY)
    assert {
        name: {
            key: value
            for key, value in field.items()
            if key in {"type", "format", "const", "default", "enum", "pattern"}
        }
        for name, field in request_schema["properties"].items()
    } == {
        "organization_slug": {"type": "string"},
        "organization_name": {"type": "string"},
        "business_slug": {"type": "string"},
        "business_name": {"type": "string"},
        "environment_slug": {"type": "string", "const": "production"},
        "public_domain": {"type": "string"},
        "crusher_image": {"type": "string"},
        "database_secret_ref": {"type": "string"},
        "object_storage_secret_ref": {"type": "string"},
    }
    assert response_schema["required"] == [
        "operation_id",
        "organization_id",
        "business_id",
        "environment_id",
        "workload_id",
        "deployment_id",
        "replayed",
    ]
    assert {
        name: {
            key: value
            for key, value in field.items()
            if key in {"type", "format", "const", "default", "enum", "pattern"}
        }
        for name, field in response_schema["properties"].items()
    } == {
        "operation_id": {"type": "string", "format": "uuid"},
        "organization_id": {"type": "string", "format": "uuid"},
        "business_id": {"type": "string", "format": "uuid"},
        "environment_id": {"type": "string", "format": "uuid"},
        "workload_id": {"type": "string", "format": "uuid"},
        "deployment_id": {"type": "string", "format": "uuid"},
        "state": {"type": "string", "const": "pending", "default": "pending"},
        "replayed": {"type": "boolean"},
    }
    assert request_fingerprint(ProvisioningRequest.model_validate(PROVISIONING_BODY)) == (
        "9c40ec2b755394189bab558268eecd42050e5219979ef790e06229ce91cdc572"
    )
    assert not any(
        path == "/v1/deployments" or path.startswith("/v1/deployments/")
        for path in openapi["paths"]
    )
