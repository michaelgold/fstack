"""Crusher provisioning request contracts."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Literal
from uuid import UUID

import idna
from pydantic import BaseModel, ConfigDict, field_validator

REQUEST_VERSION = "fstack.crusher-provisioning-request.v1"
SLUG_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
IMAGE_PATTERN = re.compile(
    r"[a-z0-9]+(?:[.-][a-z0-9]+)*/"
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*@sha256:[0-9a-f]{64}\Z"
)
SECRET_REFERENCE_PATTERN = re.compile(
    r"secret://[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:/[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+\Z"
)


class ProvisioningRequest(BaseModel):
    """Validated desired state for the first Crusher provisioning slice."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    organization_slug: str
    organization_name: str
    business_slug: str
    business_name: str
    environment_slug: Literal["production"]
    public_domain: str
    crusher_image: str
    database_secret_ref: str
    object_storage_secret_ref: str

    @field_validator("organization_slug", "business_slug")
    @classmethod
    def validate_slug(cls, value: str) -> str:
        if SLUG_PATTERN.fullmatch(value) is None:
            raise ValueError("must be a canonical lowercase ASCII slug")
        return value

    @field_validator("organization_name", "business_name")
    @classmethod
    def trim_name(cls, value: str) -> str:
        if any(unicodedata.category(character) in {"Cc", "Cs"} for character in value):
            raise ValueError("must not contain control or non-scalar characters")
        normalized = value.strip()
        if not 1 <= len(normalized) <= 160:
            raise ValueError("must contain between 1 and 160 characters")
        return normalized

    @field_validator("public_domain")
    @classmethod
    def validate_public_domain(cls, value: str) -> str:
        try:
            encoded = value.encode("ascii")
        except UnicodeEncodeError as exc:
            raise ValueError("must be a canonical ASCII hostname") from exc
        labels = value.split(".")
        if not 1 <= len(encoded) <= 253 or len(labels) < 2:
            raise ValueError("must be a canonical multi-label hostname")
        if any(SLUG_PATTERN.fullmatch(label) is None for label in labels):
            raise ValueError("must be a canonical lowercase ASCII hostname")
        try:
            canonical_labels = [
                idna.encode(
                    idna.decode(label.encode("ascii"), uts46=False, std3_rules=True),
                    uts46=False,
                    std3_rules=True,
                ).decode("ascii")
                for label in labels
            ]
        except idna.IDNAError as exc:
            raise ValueError("must contain valid canonical IDNA A-labels") from exc
        if canonical_labels != labels:
            raise ValueError("must contain canonical IDNA A-labels")
        return value

    @field_validator("crusher_image")
    @classmethod
    def validate_crusher_image(cls, value: str) -> str:
        if IMAGE_PATTERN.fullmatch(value) is None:
            raise ValueError("must be an immutable lowercase sha256 image reference")
        return value

    @field_validator("database_secret_ref", "object_storage_secret_ref")
    @classmethod
    def validate_secret_reference(cls, value: str) -> str:
        try:
            encoded = value.encode("ascii")
        except UnicodeEncodeError as exc:
            raise ValueError("must be a canonical secret reference") from exc
        if len(encoded) > 500 or SECRET_REFERENCE_PATTERN.fullmatch(value) is None:
            raise ValueError("must be a canonical secret reference")
        return value


class ProvisioningResponse(BaseModel):
    """Stable public identity returned for a requested provisioning operation."""

    model_config = ConfigDict(frozen=True)

    operation_id: UUID
    organization_id: UUID
    business_id: UUID
    environment_id: UUID
    workload_id: UUID
    deployment_id: UUID
    state: Literal["pending"] = "pending"
    replayed: bool


def request_fingerprint(request: ProvisioningRequest) -> str:
    """Return the versioned canonical SHA-256 request identity."""
    canonical = {"version": REQUEST_VERSION, **request.model_dump(mode="json")}
    serialized = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()
