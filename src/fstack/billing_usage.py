"""Canonical installation-local usage export contracts."""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import NoReturn
from uuid import UUID

from sqlalchemy import Connection, text

_CANONICAL_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\Z")
_BASE64URL = re.compile(r"[A-Za-z0-9_-]+\Z")
_CURSOR_KEYS = {"recorded_at", "usage_fact_id"}
_CURSOR_LENGTH = 134


class InvalidUsageCursor(ValueError):
    """The usage export query or cursor is malformed or noncanonical."""


def _invalid() -> NoReturn:
    raise InvalidUsageCursor


def _pairs_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _invalid()
        result[key] = value
    return result


def canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or _CANONICAL_TIMESTAMP.fullmatch(value) is None:
        _invalid()
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError:
        _invalid()


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


def encode_usage_cursor(*, recorded_at: datetime, usage_fact_id: UUID) -> str:
    payload = json.dumps(
        {
            "recorded_at": canonical_timestamp(recorded_at),
            "usage_fact_id": str(usage_fact_id),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_usage_cursor(value: str) -> tuple[datetime, UUID]:
    if len(value) != _CURSOR_LENGTH or _BASE64URL.fullmatch(value) is None:
        _invalid()
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        decoded = raw.decode("utf-8")
        payload = json.loads(
            decoded,
            object_pairs_hook=_pairs_object,
            parse_constant=lambda _value: _invalid(),
        )
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        _invalid()
    if not isinstance(payload, dict) or set(payload) != _CURSOR_KEYS:
        _invalid()
    recorded_at = _parse_timestamp(payload["recorded_at"])
    usage_fact_id = _parse_uuid(payload["usage_fact_id"])
    if encode_usage_cursor(recorded_at=recorded_at, usage_fact_id=usage_fact_id) != value:
        _invalid()
    return recorded_at, usage_fact_id


@dataclass(frozen=True)
class UsageQuery:
    limit: int
    cursor: tuple[datetime, UUID] | None


def parse_usage_query(items: list[tuple[str, str]]) -> UsageQuery:
    values: dict[str, str] = {}
    for key, value in items:
        if key not in {"limit", "cursor"} or key in values:
            _invalid()
        values[key] = value
    raw_limit = values.get("limit", "100")
    if (
        not raw_limit.isascii()
        or not raw_limit.isdigit()
        or len(raw_limit) > 3
        or str(int(raw_limit)) != raw_limit
    ):
        _invalid()
    limit = int(raw_limit)
    if not 1 <= limit <= 100:
        _invalid()
    cursor = decode_usage_cursor(values["cursor"]) if "cursor" in values else None
    return UsageQuery(limit=limit, cursor=cursor)


def list_usage_facts(connection: Connection, *, query: UsageQuery) -> dict[str, object]:
    cursor_recorded_at = query.cursor[0] if query.cursor is not None else None
    cursor_id = query.cursor[1] if query.cursor is not None else None
    rows = (
        connection.execute(
            text(
                """
                SELECT id, subject_id, revision_id, authorization_id,
                       organization_id, operation_id, deployment_id,
                       metric, unit, quantity, source_event, occurred_at,
                       recorded_at, schema_version, payload_digest
                FROM usage_facts
                WHERE (
                    CAST(:cursor_recorded_at AS timestamptz) IS NULL
                    OR (recorded_at, id) > (
                        CAST(:cursor_recorded_at AS timestamptz), CAST(:cursor_id AS uuid)
                    )
                )
                ORDER BY recorded_at ASC, id ASC
                LIMIT :row_limit
                """
            ),
            {
                "cursor_recorded_at": cursor_recorded_at,
                "cursor_id": cursor_id,
                "row_limit": query.limit + 1,
            },
        )
        .mappings()
        .all()
    )
    visible = rows[: query.limit]
    facts = [
        {
            "usage_fact_id": str(row["id"]),
            "entitlement_subject_id": str(row["subject_id"]),
            "entitlement_revision_id": str(row["revision_id"]),
            "authorization_id": str(row["authorization_id"]),
            "organization_id": str(row["organization_id"]),
            "operation_id": str(row["operation_id"]),
            "deployment_id": str(row["deployment_id"]),
            "metric": row["metric"],
            "unit": row["unit"],
            "quantity": row["quantity"],
            "source_event": row["source_event"],
            "occurred_at": canonical_timestamp(row["occurred_at"]),
            "recorded_at": canonical_timestamp(row["recorded_at"]),
            "schema_version": row["schema_version"],
            "payload_digest": row["payload_digest"],
        }
        for row in visible
    ]
    next_cursor = None
    if len(rows) > query.limit and visible:
        last = visible[-1]
        next_cursor = encode_usage_cursor(recorded_at=last["recorded_at"], usage_fact_id=last["id"])
    return {"facts": facts, "next_cursor": next_cursor}
