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
