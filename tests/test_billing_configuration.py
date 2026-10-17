import asyncio
import json
from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from fstack.api import create_app
from fstack.temporal_worker import run_temporal_worker


def _base_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "FSTACK_DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"
    )
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "operator-token-which-is-long-enough-0001")


def test_invalid_entitlement_mode_prevents_api_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _base_environment(monkeypatch)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "REQUIRED")

    with pytest.raises(RuntimeError, match="FSTACK_ENTITLEMENT_MODE is invalid"):
        create_app()


def test_invalid_entitlement_mode_prevents_worker_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _base_environment(monkeypatch)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "REQUIRED")

    async def unexpected_connect(*_args, **_kwargs):
        raise AssertionError("worker must validate billing configuration before network access")

    monkeypatch.setattr("fstack.temporal_worker.Client.connect", unexpected_connect)

    with pytest.raises(RuntimeError, match="FSTACK_ENTITLEMENT_MODE is invalid"):
        asyncio.run(run_temporal_worker())


def test_required_mode_requires_distinct_dedicated_billing_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _base_environment(monkeypatch)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "required")
    monkeypatch.delenv("FSTACK_BILLING_RECONCILE_TOKEN", raising=False)
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", "export-token-which-is-long-enough-0001")

    with pytest.raises(RuntimeError, match="billing token configuration is invalid"):
        create_app()

    operator_token = "operator-token-which-is-long-enough-0001"
    monkeypatch.setenv("FSTACK_BILLING_RECONCILE_TOKEN", operator_token)

    with pytest.raises(RuntimeError, match="billing token configuration is invalid"):
        create_app()


@pytest.mark.parametrize("operator_token", [None, ""])
def test_required_billing_configuration_does_not_add_operator_startup_requirement(
    monkeypatch: pytest.MonkeyPatch,
    operator_token: str | None,
) -> None:
    _base_environment(monkeypatch)
    if operator_token is None:
        monkeypatch.delenv("FSTACK_OPERATOR_TOKEN", raising=False)
    else:
        monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", operator_token)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "required")
    monkeypatch.setenv(
        "FSTACK_BILLING_RECONCILE_TOKEN", "reconcile-token-which-is-long-enough-0001"
    )
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", "export-token-which-is-long-enough-0001")

    assert TestClient(create_app()).get("/health").status_code == 200


def test_disabled_mode_hides_billing_endpoint_even_when_tokens_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _base_environment(monkeypatch)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "disabled")
    monkeypatch.setenv(
        "FSTACK_BILLING_RECONCILE_TOKEN", "reconcile-token-present-but-disabled-0001"
    )
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", "export-token-present-but-disabled-0001")

    response = TestClient(create_app()).put(
        "/internal/billing/entitlement-revisions",
        headers={"Authorization": "Bearer reconcile-token-present-but-disabled-0001"},
        json={},
    )

    assert response.status_code == 404
    assert response.json() == {"code": "http_error", "message": "request failed"}


def test_required_mode_accepts_valid_distinct_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _base_environment(monkeypatch)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "required")
    monkeypatch.setenv(
        "FSTACK_BILLING_RECONCILE_TOKEN", "reconcile-token-which-is-long-enough-0001"
    )
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", "export-token-which-is-long-enough-0001")

    assert TestClient(create_app()).get("/health").status_code == 200


@pytest.mark.parametrize(
    "authorization",
    [
        None,
        "Bearer wrong-token-which-is-long-enough-0001",
        "Bearer operator-token-which-is-long-enough-0001",
        "Bearer export-token-which-is-long-enough-0001",
    ],
)
def test_entitlement_sync_requires_only_reconcile_token_before_body_processing(
    monkeypatch: pytest.MonkeyPatch,
    authorization: str | None,
) -> None:
    _base_environment(monkeypatch)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "required")
    monkeypatch.setenv(
        "FSTACK_BILLING_RECONCILE_TOKEN", "reconcile-token-which-is-long-enough-0001"
    )
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", "export-token-which-is-long-enough-0001")
    headers = {} if authorization is None else {"Authorization": authorization}

    response = TestClient(create_app()).put(
        "/internal/billing/entitlement-revisions",
        headers=headers,
        content=b"not-json",
    )

    assert response.status_code == 401
    assert response.json() == {
        "code": "authentication_required",
        "message": "billing reconciliation authentication required",
    }


def test_entitlement_sync_rejects_invalid_body_after_authentication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _base_environment(monkeypatch)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "required")
    monkeypatch.setenv(
        "FSTACK_BILLING_RECONCILE_TOKEN", "reconcile-token-which-is-long-enough-0001"
    )
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", "export-token-which-is-long-enough-0001")

    response = TestClient(create_app()).put(
        "/internal/billing/entitlement-revisions",
        headers={"Authorization": "Bearer reconcile-token-which-is-long-enough-0001"},
        content=b"not-json",
    )

    assert response.status_code == 400
    assert response.json() == {
        "code": "invalid_entitlement_revision",
        "message": "entitlement revision is invalid",
    }


def test_entitlement_sync_sanitizes_unknown_persistence_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _base_environment(monkeypatch)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "required")
    monkeypatch.setenv(
        "FSTACK_BILLING_RECONCILE_TOKEN", "reconcile-token-which-is-long-enough-0001"
    )
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", "export-token-which-is-long-enough-0001")

    class FailingEngine:
        @contextmanager
        def begin(self):
            raise ValueError("secret-persistence-marker")
            yield

    monkeypatch.setattr("fstack.api._database_engine", lambda: FailingEngine())
    body = {
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

    response = TestClient(create_app()).put(
        "/internal/billing/entitlement-revisions",
        headers={
            "Authorization": "Bearer reconcile-token-which-is-long-enough-0001",
            "Content-Type": "application/json",
        },
        content=json.dumps(body).encode(),
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "entitlement_persistence_unavailable",
        "message": "entitlement revision could not be persisted",
    }
    assert "secret-persistence-marker" not in response.text
