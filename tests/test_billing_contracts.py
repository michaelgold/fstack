import json
from uuid import UUID

import pytest

from fstack.billing import InvalidEntitlementRevision, parse_entitlement_revision_request

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


def _body(payload: dict[str, object] | None = None) -> bytes:
    return json.dumps(VALID_REVISION if payload is None else payload).encode()


def test_entitlement_revision_parser_produces_exact_canonical_digest() -> None:
    request = parse_entitlement_revision_request(_body())

    assert request.organization_slug == "acme"
    assert request.entitlement_revision_id == UUID("11111111-1111-4111-8111-111111111111")
    assert request.source_revision_id == UUID("33333333-3333-4333-8333-333333333333")
    assert (
        request.payload_digest(UUID("22222222-2222-4222-8222-222222222222"))
        == "c68da7c1c4fcd56c4b0d7a9cc7c17f6d1ab5fec9b18dde5157eb951066e09e10"
    )


@pytest.mark.parametrize(
    "raw_body",
    [
        b"{" + b'"organization_slug":"acme",' * 2 + b'"generation":1}',
        _body(VALID_REVISION | {"unknown": "value"}),
        _body(VALID_REVISION | {"generation": True}),
        _body(VALID_REVISION | {"generation": 0}),
        _body(VALID_REVISION | {"quota_limit": 1.0}),
        _body(VALID_REVISION | {"entitlement_revision_id": "11111111111141118111111111111111"}),
        _body(VALID_REVISION | {"observed_at": "2026-10-17T16:00:00Z"}),
        _body(VALID_REVISION | {"valid_until": "2026-10-17T16:05:00.000001Z"}),
        _body(VALID_REVISION | {"plan_key": "commercial-plan"}),
        _body(VALID_REVISION | {"state": "paused"}),
        _body(VALID_REVISION | {"organization_slug": "Acme"}),
        b"not-json",
        b"{}",
    ],
)
def test_entitlement_revision_parser_rejects_noncanonical_input(raw_body: bytes) -> None:
    with pytest.raises(InvalidEntitlementRevision):
        parse_entitlement_revision_request(raw_body)
