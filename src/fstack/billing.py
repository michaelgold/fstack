"""Provider-neutral billing request contracts and canonical encoding."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import NoReturn
from uuid import UUID

from fstack.provisioning import SLUG_PATTERN

_REVISION_DISCRIMINATOR = "fstack.entitlement-revision.v1"
_CANONICAL_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\Z")
_REQUEST_KEYS = {
    "organization_slug",
    "entitlement_revision_id",
    "generation",
    "plan_key",
    "state",
    "capability",
    "quota_dimension",
    "quota_limit",
    "observed_at",
    "valid_until",
    "source_revision_id",
}


class InvalidEntitlementRevision(ValueError):
    """The synchronization request is malformed or noncanonical."""


def _invalid() -> NoReturn:
    raise InvalidEntitlementRevision


def _pairs_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _invalid()
        result[key] = value
    return result


def _parse_uuid(value: object) -> UUID:
    if not isinstance(value, str):
        _invalid()
    try:
        parsed = UUID(value)
    except ValueError:
        _invalid()
    if str(parsed) != value:
        _invalid()
    return parsed


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or _CANONICAL_TIMESTAMP.fullmatch(value) is None:
        _invalid()
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError:
        _invalid()


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass(frozen=True)
class EntitlementRevisionRequest:
    organization_slug: str
    entitlement_revision_id: UUID
    generation: int
    plan_key: str
    state: str
    capability: str
    quota_dimension: str
    quota_limit: int
    observed_at: datetime
    valid_until: datetime
    source_revision_id: UUID

    def canonical_payload(self, entitlement_subject_id: UUID) -> bytes:
        payload = {
            "discriminator": _REVISION_DISCRIMINATOR,
            "entitlement_revision_id": str(self.entitlement_revision_id),
            "entitlement_subject_id": str(entitlement_subject_id),
            "generation": self.generation,
            "plan_key": self.plan_key,
            "state": self.state,
            "capability": self.capability,
            "quota_dimension": self.quota_dimension,
            "quota_limit": self.quota_limit,
            "observed_at": _canonical_timestamp(self.observed_at),
            "valid_until": _canonical_timestamp(self.valid_until),
            "source_revision_id": str(self.source_revision_id),
        }
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

    def payload_digest(self, entitlement_subject_id: UUID) -> str:
        return hashlib.sha256(self.canonical_payload(entitlement_subject_id)).hexdigest()


def parse_entitlement_revision_request(raw_body: bytes) -> EntitlementRevisionRequest:
    try:
        decoded = raw_body.decode("utf-8")
        payload = json.loads(
            decoded,
            object_pairs_hook=_pairs_object,
            parse_constant=lambda _value: _invalid(),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, InvalidEntitlementRevision):
        _invalid()
    if not isinstance(payload, dict) or set(payload) != _REQUEST_KEYS:
        _invalid()

    organization_slug = payload["organization_slug"]
    if not isinstance(organization_slug, str) or SLUG_PATTERN.fullmatch(organization_slug) is None:
        _invalid()
    generation = payload["generation"]
    quota_limit = payload["quota_limit"]
    if type(generation) is not int or not 1 <= generation <= 2**63 - 1:
        _invalid()
    if type(quota_limit) is not int or quota_limit != 1:
        _invalid()

    exact_strings = {
        "plan_key": "crusher.single.v1",
        "capability": "crusher.deployment.create",
        "quota_dimension": "crusher.deployment.active",
    }
    for key, expected in exact_strings.items():
        if payload[key] != expected or not isinstance(payload[key], str):
            _invalid()
    state = payload["state"]
    if not isinstance(state, str) or state not in {"active", "inactive"}:
        _invalid()

    observed_at = _parse_timestamp(payload["observed_at"])
    valid_until = _parse_timestamp(payload["valid_until"])
    if not observed_at < valid_until <= observed_at + timedelta(minutes=5):
        _invalid()

    return EntitlementRevisionRequest(
        organization_slug=organization_slug,
        entitlement_revision_id=_parse_uuid(payload["entitlement_revision_id"]),
        generation=generation,
        plan_key="crusher.single.v1",
        state=state,
        capability="crusher.deployment.create",
        quota_dimension="crusher.deployment.active",
        quota_limit=1,
        observed_at=observed_at,
        valid_until=valid_until,
        source_revision_id=_parse_uuid(payload["source_revision_id"]),
    )
