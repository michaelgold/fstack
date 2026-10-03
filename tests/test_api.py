from fastapi.testclient import TestClient

from fstack.api import create_app


def test_health_endpoint_does_not_require_database_access(monkeypatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")

    response = TestClient(create_app()).get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "fstack"}


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
