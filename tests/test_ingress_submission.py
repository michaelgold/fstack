from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from uuid import UUID

import pytest


def test_canonical_route_is_exact_and_deterministic() -> None:
    from fstack.ingress_submission import (
        GENERATION,
        PROVIDER,
        ROUTE_SPEC_VERSION,
        SUBMISSION_VERSION,
        _canonical_route,
    )

    assert PROVIDER == "local_caddy"
    assert GENERATION == 1
    assert SUBMISSION_VERSION == "fstack.local-caddy-ingress-submission.v1"
    assert ROUTE_SPEC_VERSION == "fstack.local-caddy-route.v1"
    deployment_id = UUID("12345678-1234-4abc-8def-1234567890ab")

    assert _canonical_route(deployment_id, "mail.acme.example") == {
        "@id": "fstack-crusher-ingress-1234567812344abc8def1234567890ab",
        "match": [{"host": ["mail.acme.example"]}],
        "handle": [
            {
                "handler": "reverse_proxy",
                "upstreams": [{"dial": "fstack-crusher-1234567812344abc8def1234567890ab:8000"}],
            }
        ],
    }


def test_submission_key_and_route_fingerprint_are_stable_and_drift_sensitive() -> None:
    from fstack.ingress_submission import (
        _canonical_route,
        _route_fingerprint,
        _submission_key,
    )

    operation_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    deployment_id = UUID("12345678-1234-4abc-8def-1234567890ab")
    route = _canonical_route(deployment_id, "mail.acme.example")

    assert _submission_key(operation_id) == (
        "e2dd884eb58e8d792a8ba4d73ab693cb83977e74f9584e5508b4b1baa0b2e726"
    )
    assert _route_fingerprint(route) == (
        "6b067c7ae5cb4e4480ee828e3ef89e71a98d4f119d0280ddd8db1572b92b419f"
    )
    assert _route_fingerprint(
        _canonical_route(deployment_id, "changed.acme.example")
    ) != _route_fingerprint(route)
    assert _route_fingerprint(
        _canonical_route(UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"), "mail.acme.example")
    ) != _route_fingerprint(route)


def test_prepared_bundle_exposes_only_submission_identifiers() -> None:
    from fstack.ingress_submission import PreparedProvisioningSubmissions

    prepared = PreparedProvisioningSubmissions(
        operation_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        runtime_submission_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        ingress_submission_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
    )

    assert [field.name for field in fields(prepared)] == [
        "operation_id",
        "runtime_submission_id",
        "ingress_submission_id",
    ]
    with pytest.raises(FrozenInstanceError):
        prepared.operation_id = UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
