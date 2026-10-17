from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text

from fstack.api import create_app

EXPORT_TOKEN = "replace-with-a-long-random-token"
RECONCILE_TOKEN = "replace-with-a-long-random-operator-token"
OPERATOR_TOKEN = "ci-operator-token"


class _NoDatabase:
    def connect(self):
        raise AssertionError("database must not be accessed")


def _required_client(
    monkeypatch: pytest.MonkeyPatch, *, engine: object | None = None
) -> TestClient:
    from fstack import api

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", OPERATOR_TOKEN)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "required")
    monkeypatch.setenv("FSTACK_BILLING_RECONCILE_TOKEN", RECONCILE_TOKEN)
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", EXPORT_TOKEN)
    if engine is not None:
        monkeypatch.setattr(api, "_database_engine", lambda: engine)
    return TestClient(create_app())


def _headers(token: str = EXPORT_TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _create_usage_fact(
    engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    *,
    suffix: str,
) -> dict[str, object]:
    from fstack import api
    from fstack.ingress_state import finalize_provisioning_success, mark_submitted
    from fstack.ingress_submission import prepare_operation_bundle
    from fstack.runtime_state import persist_submitted

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", OPERATOR_TOKEN)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "required")
    monkeypatch.setenv("FSTACK_BILLING_RECONCILE_TOKEN", RECONCILE_TOKEN)
    monkeypatch.setenv("FSTACK_BILLING_EXPORT_TOKEN", EXPORT_TOKEN)
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    monkeypatch.setattr(api, "_database_engine", lambda: engine)
    now = datetime.now(UTC)
    body = {
        "organization_slug": f"usage-export-{suffix}",
        "organization_name": f"Usage Export {suffix}",
        "business_slug": "mailer",
        "business_name": "Usage Mailer",
        "environment_slug": "production",
        "public_domain": f"{suffix}.usage-export.example",
        "crusher_image": "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
        "database_secret_ref": "secret://vault/acme/database",
        "object_storage_secret_ref": "secret://vault/acme/object-storage",
    }
    with TestClient(create_app()) as client:
        revision = client.put(
            "/internal/billing/entitlement-revisions",
            headers={"Authorization": f"Bearer {RECONCILE_TOKEN}"},
            json={
                "organization_slug": body["organization_slug"],
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
        revision.raise_for_status()
        operation = client.post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": f"Bearer {OPERATOR_TOKEN}",
                "Idempotency-Key": f"usage-export-{suffix}",
            },
            json=body,
        )
        operation.raise_for_status()
    prepared = prepare_operation_bundle(UUID(operation.json()["operation_id"]))
    persist_submitted(prepared.runtime_submission_id, f"docker-api-usage-export-{suffix}")
    mark_submitted(prepared.ingress_submission_id, prepared.operation_id)
    finalize_provisioning_success(
        prepared.runtime_submission_id,
        prepared.ingress_submission_id,
        prepared.operation_id,
    )
    with engine.connect() as connection:
        return dict(
            connection.execute(
                text("SELECT * FROM usage_facts WHERE operation_id=:operation_id"),
                {"operation_id": prepared.operation_id},
            )
            .mappings()
            .one()
        )


def test_usage_export_is_hidden_when_billing_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import api

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", OPERATOR_TOKEN)
    monkeypatch.setenv("FSTACK_ENTITLEMENT_MODE", "disabled")
    monkeypatch.setattr(api, "_database_engine", lambda: _NoDatabase())

    response = TestClient(create_app()).get(
        "/internal/billing/usage-facts",
        headers=_headers(),
    )

    assert response.status_code == 404


def test_usage_export_is_absent_from_openapi(monkeypatch: pytest.MonkeyPatch) -> None:
    schema = _required_client(monkeypatch, engine=_NoDatabase()).get("/openapi.json").json()

    assert "/internal/billing/usage-facts" not in schema["paths"]


@pytest.mark.parametrize("token", [None, "wrong-token", OPERATOR_TOKEN, RECONCILE_TOKEN])
def test_usage_export_requires_only_dedicated_token_before_database_access(
    monkeypatch: pytest.MonkeyPatch,
    token: str | None,
) -> None:
    headers = {} if token is None else _headers(token)

    response = _required_client(monkeypatch, engine=_NoDatabase()).get(
        "/internal/billing/usage-facts",
        headers=headers,
    )

    assert response.status_code == 401
    assert response.json() == {
        "code": "authentication_required",
        "message": "billing usage authentication required",
    }


@pytest.mark.parametrize(
    "query",
    [
        "?limit=0",
        "?limit=101",
        "?limit=01",
        "?limit=true",
        "?limit=1&limit=2",
        "?cursor=a&cursor=b",
        "?unknown=value",
        "?cursor=e30=",
        "?cursor=not-base64url!",
    ],
)
def test_usage_export_rejects_noncanonical_query_before_database_access(
    monkeypatch: pytest.MonkeyPatch,
    query: str,
) -> None:
    response = _required_client(monkeypatch, engine=_NoDatabase()).get(
        f"/internal/billing/usage-facts{query}",
        headers=_headers(),
    )

    assert response.status_code == 400
    assert response.json() == {
        "code": "invalid_usage_cursor",
        "message": "usage cursor is invalid",
    }


def test_usage_cursor_round_trips_exact_canonical_values() -> None:
    from fstack.billing_usage import decode_usage_cursor, encode_usage_cursor

    recorded_at = datetime(2026, 10, 17, 15, 6, 2, 123456, tzinfo=UTC)
    usage_fact_id = UUID("00000000-0000-0000-0000-000000000000")

    encoded = encode_usage_cursor(recorded_at=recorded_at, usage_fact_id=usage_fact_id)

    assert encoded == (
        "eyJyZWNvcmRlZF9hdCI6IjIwMjYtMTAtMTdUMTU6MDY6MDIuMTIzNDU2WiIs"
        "InVzYWdlX2ZhY3RfaWQiOiIwMDAwMDAwMC0wMDAwLTAwMDAtMDAwMC0wMDAwMDAwMDAwMDAifQ"
    )
    assert decode_usage_cursor(encoded) == (recorded_at, usage_fact_id)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"recorded_at":"2026-10-17T15:06:02.123456Z"}',
        b'{"recorded_at":"2026-10-17T15:06:02.123456Z","usage_fact_id":"00000000-0000-0000-0000-000000000000","extra":1}',
        b'{"recorded_at":"2026-10-17T15:06:02.123456Z","recorded_at":"2026-10-17T15:06:02.123456Z","usage_fact_id":"00000000-0000-0000-0000-000000000000"}',
        b'{"recorded_at":"2026-10-17T15:06:02Z","usage_fact_id":"00000000-0000-0000-0000-000000000000"}',
        b'{"recorded_at":"2026-10-17T15:06:02.123456Z","usage_fact_id":"00000000-0000-0000-0000-00000000000A"}',
        (
            b'{ "recorded_at":"2026-10-17T15:06:02.123456Z",'
            b'"usage_fact_id":"00000000-0000-0000-0000-000000000000"}'
        ),
    ],
)
def test_usage_cursor_rejects_noncanonical_payloads(payload: bytes) -> None:
    from fstack.billing_usage import InvalidUsageCursor, decode_usage_cursor

    cursor = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")

    with pytest.raises(InvalidUsageCursor):
        decode_usage_cursor(cursor)


def test_usage_cursor_rejects_noncanonical_json_key_order() -> None:
    from fstack.billing_usage import InvalidUsageCursor, decode_usage_cursor

    payload = json.dumps(
        {
            "usage_fact_id": "00000000-0000-0000-0000-000000000000",
            "recorded_at": "2026-10-17T15:06:02.123456Z",
        },
        separators=(",", ":"),
    ).encode()
    cursor = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")

    with pytest.raises(InvalidUsageCursor):
        decode_usage_cursor(cursor)


def test_usage_export_rejects_oversized_nested_cursor_with_stable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = ("[" * 20_000 + "]" * 20_000).encode()
    cursor = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")

    response = _required_client(monkeypatch, engine=_NoDatabase()).get(
        "/internal/billing/usage-facts",
        params={"cursor": cursor},
        headers=_headers(),
    )

    assert response.status_code == 400
    assert response.json() == {
        "code": "invalid_usage_cursor",
        "message": "usage cursor is invalid",
    }


def _expected_fact(row: dict[str, object]) -> dict[str, object]:
    assert isinstance(row["occurred_at"], datetime)
    assert isinstance(row["recorded_at"], datetime)
    return {
        "usage_fact_id": str(row["id"]),
        "entitlement_subject_id": str(row["subject_id"]),
        "entitlement_revision_id": str(row["revision_id"]),
        "authorization_id": str(row["authorization_id"]),
        "organization_id": str(row["organization_id"]),
        "operation_id": str(row["operation_id"]),
        "deployment_id": str(row["deployment_id"]),
        "metric": "crusher.deployment.activated",
        "unit": "deployment",
        "quantity": 1,
        "source_event": "provisioning.succeeded",
        "occurred_at": _canonical_timestamp(row["occurred_at"]),
        "recorded_at": _canonical_timestamp(row["recorded_at"]),
        "schema_version": 1,
        "payload_digest": row["payload_digest"],
    }


@pytest.mark.postgres
def test_usage_export_returns_exact_provider_neutral_fact(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fact = _create_usage_fact(migrated_database, monkeypatch, suffix="single")

    response = _required_client(monkeypatch, engine=migrated_database).get(
        "/internal/billing/usage-facts",
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {"facts": [_expected_fact(fact)], "next_cursor": None}


@pytest.mark.postgres
def test_usage_export_pages_in_stable_exclusive_order(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _create_usage_fact(migrated_database, monkeypatch, suffix="first")
    second = _create_usage_fact(migrated_database, monkeypatch, suffix="second")
    ordered = sorted((first, second), key=lambda row: (row["recorded_at"], row["id"]))
    client = _required_client(monkeypatch, engine=migrated_database)

    page_one = client.get(
        "/internal/billing/usage-facts?limit=1",
        headers=_headers(),
    )
    page_two = client.get(
        "/internal/billing/usage-facts",
        params={"limit": "1", "cursor": page_one.json()["next_cursor"]},
        headers=_headers(),
    )

    assert page_one.status_code == 200
    assert page_one.json()["facts"] == [_expected_fact(ordered[0])]
    assert isinstance(page_one.json()["next_cursor"], str)
    assert page_two.status_code == 200
    assert page_two.json() == {"facts": [_expected_fact(ordered[1])], "next_cursor": None}


@pytest.mark.postgres
def test_usage_export_uses_uuid_tiebreaker_for_equal_recorded_times(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _create_usage_fact(migrated_database, monkeypatch, suffix="tie-first")
    second = _create_usage_fact(migrated_database, monkeypatch, suffix="tie-second")
    shared_recorded_at = datetime(2026, 10, 17, 17, 0, 0, tzinfo=UTC)
    with migrated_database.begin() as connection:
        connection.execute(
            text("ALTER TABLE usage_facts DISABLE TRIGGER tr_usage_facts_append_only")
        )
        connection.execute(
            text("UPDATE usage_facts SET recorded_at=:recorded_at"),
            {"recorded_at": shared_recorded_at},
        )
        connection.execute(
            text("ALTER TABLE usage_facts ENABLE TRIGGER tr_usage_facts_append_only")
        )
    first["recorded_at"] = shared_recorded_at
    second["recorded_at"] = shared_recorded_at
    ordered = sorted((first, second), key=lambda row: UUID(str(row["id"])))
    client = _required_client(monkeypatch, engine=migrated_database)

    page_one = client.get(
        "/internal/billing/usage-facts?limit=1",
        headers=_headers(),
    )
    page_two = client.get(
        "/internal/billing/usage-facts",
        params={"limit": "1", "cursor": page_one.json()["next_cursor"]},
        headers=_headers(),
    )

    assert page_one.json()["facts"] == [_expected_fact(ordered[0])]
    assert page_two.json() == {"facts": [_expected_fact(ordered[1])], "next_cursor": None}


def test_usage_export_sanitizes_unknown_database_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _required_client(monkeypatch, engine=_NoDatabase()).get(
        "/internal/billing/usage-facts",
        headers=_headers(),
    )

    assert response.status_code == 503
    assert response.json() == {
        "code": "usage_facts_unavailable",
        "message": "usage facts are unavailable",
    }
