import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text

from fstack.api import create_app

VALID_REVISION = {
    "organization_slug": "acme",
    "entitlement_revision_id": "11111111-1111-4111-8111-111111111111",
    "generation": 1,
    "plan_key": "crusher.single.v1",
    "state": "active",
    "capability": "crusher.deployment.create",
    "quota_dimension": "crusher.deployment.active",
    "quota_limit": 1,
    "observed_at": "2026-10-17T16:00:00.000000Z",
    "valid_until": "2026-10-17T16:05:00.000000Z",
    "source_revision_id": "33333333-3333-4333-8333-333333333333",
}


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "operator-token-which-is-long-enough-0001")
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "required")
    monkeypatch.setenv(
        "FSTACK_BILLING_RECONCILE_TOKEN", "reconcile-token-which-is-long-enough-0001"
    )
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", "export-token-which-is-long-enough-0001")
    return TestClient(create_app())


def _headers() -> dict[str, str]:
    return {"Authorization": "Bearer reconcile-token-which-is-long-enough-0001"}


@pytest.mark.postgres
def test_entitlement_revision_endpoint_creates_and_replays_exact_revision(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(monkeypatch)

    created = client.put(
        "/internal/billing/entitlement-revisions",
        headers=_headers(),
        json=VALID_REVISION,
    )
    replayed = client.put(
        "/internal/billing/entitlement-revisions",
        headers=_headers(),
        json=VALID_REVISION,
    )

    assert created.status_code == 201
    assert replayed.status_code == 200
    assert created.json() == {
        "entitlement_subject_id": replayed.json()["entitlement_subject_id"],
        "entitlement_revision_id": "11111111-1111-4111-8111-111111111111",
        "replayed": False,
    }
    assert replayed.json() == created.json() | {"replayed": True}
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM entitlement_subjects")).scalar_one() == 1
        )
        assert (
            connection.execute(text("SELECT count(*) FROM entitlement_revisions")).scalar_one() == 1
        )


@pytest.mark.postgres
def test_entitlement_revision_endpoint_returns_stable_conflict_without_mutation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(monkeypatch)
    client.put(
        "/internal/billing/entitlement-revisions",
        headers=_headers(),
        json=VALID_REVISION,
    ).raise_for_status()

    response = client.put(
        "/internal/billing/entitlement-revisions",
        headers=_headers(),
        json=VALID_REVISION | {"state": "inactive"},
    )

    assert response.status_code == 409
    assert response.json() == {
        "code": "entitlement_revision_conflict",
        "message": "entitlement revision conflicts with committed state",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT state FROM entitlement_revisions")).scalar_one()
            == "active"
        )


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("content", "content_type"),
    [
        (b'{"organization_slug":"acme","organization_slug":"other"}', "application/json"),
        (json.dumps(VALID_REVISION).encode(), "application/json; charset=utf-8"),
    ],
)
def test_entitlement_revision_endpoint_rejects_noncanonical_body_before_database_mutation(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
    content: bytes,
    content_type: str,
) -> None:
    response = _client(monkeypatch).put(
        "/internal/billing/entitlement-revisions",
        headers=_headers() | {"Content-Type": content_type},
        content=content,
    )

    assert response.status_code == 400
    assert response.json() == {
        "code": "invalid_entitlement_revision",
        "message": "entitlement revision is invalid",
    }
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM entitlement_subjects")).scalar_one() == 0
        )
        assert (
            connection.execute(text("SELECT count(*) FROM entitlement_revisions")).scalar_one() == 0
        )
