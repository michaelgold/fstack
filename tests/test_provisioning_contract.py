from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

import fstack.provisioning as provisioning

VALID_BODY = {
    "organization_slug": "acme",
    "organization_name": "  Acme Robotics  ",
    "business_slug": "mailer",
    "business_name": "  Acme Mailer  ",
    "environment_slug": "production",
    "public_domain": "mail.acme.example",
    "crusher_image": "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
    "database_secret_ref": "secret://vault/acme/database",
    "object_storage_secret_ref": "secret://vault/acme/object-storage",
}


def test_valid_request_is_normalized_and_fingerprinted() -> None:
    assert hasattr(provisioning, "ProvisioningRequest")
    assert hasattr(provisioning, "request_fingerprint")

    request = provisioning.ProvisioningRequest.model_validate(VALID_BODY)

    assert request.organization_name == "Acme Robotics"
    assert request.business_name == "Acme Mailer"
    canonical = {
        "business_name": "Acme Mailer",
        "business_slug": "mailer",
        "crusher_image": "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
        "database_secret_ref": "secret://vault/acme/database",
        "environment_slug": "production",
        "object_storage_secret_ref": "secret://vault/acme/object-storage",
        "organization_name": "Acme Robotics",
        "organization_slug": "acme",
        "public_domain": "mail.acme.example",
        "version": "fstack.crusher-provisioning-request.v1",
    }
    serialized = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    assert provisioning.request_fingerprint(request) == hashlib.sha256(serialized).hexdigest()


def test_name_accepts_non_control_unicode_scalar_values() -> None:
    request = provisioning.ProvisioningRequest.model_validate(
        VALID_BODY | {"organization_name": "👩‍💻 Labs"}
    )

    assert request.organization_name == "👩‍💻 Labs"


def test_name_rejects_surrogate_code_points() -> None:
    with pytest.raises(ValidationError):
        provisioning.ProvisioningRequest.model_validate(
            VALID_BODY | {"organization_name": "Invalid \ud800 Name"}
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("organization_slug", "Acme"),
        ("business_slug", "-mailer"),
        ("environment_slug", "staging"),
        ("public_domain", "https://mail.acme.example"),
        ("public_domain", "mail.Acme.example"),
        ("public_domain", "xn--a.example"),
        ("crusher_image", "ghcr.io/michaelgold/crusher:latest"),
        ("crusher_image", "ghcr.io/MichaelGold/crusher@sha256:" + "a" * 64),
        ("database_secret_ref", "postgresql://user:password@db/acme"),
        ("object_storage_secret_ref", "secret://vault/acme/store?token=secret"),
        ("organization_name", "Acme\nRobotics"),
        ("organization_name", "\tAcme Robotics"),
        ("organization_name", "Acme Robotics\n"),
    ],
)
def test_request_rejects_noncanonical_or_secret_shaped_values(field: str, value: str) -> None:
    body = VALID_BODY | {field: value}

    with pytest.raises(ValidationError):
        provisioning.ProvisioningRequest.model_validate(body)


def test_status_response_rejects_negative_attempt_count() -> None:
    identity = {
        "operation_id": uuid4(),
        "organization_id": uuid4(),
        "business_id": uuid4(),
        "environment_id": uuid4(),
        "workload_id": uuid4(),
        "deployment_id": uuid4(),
    }

    with pytest.raises(ValidationError):
        provisioning.ProvisioningStatusResponse.model_validate(
            {
                **identity,
                "state": "pending",
                "created_at": datetime.now(UTC),
                "started_at": None,
                "completed_at": None,
                "attempt_count": -1,
                "last_error": None,
            }
        )


def test_status_response_normalizes_timestamps_to_utc() -> None:
    non_utc = datetime(2026, 10, 4, 12, tzinfo=timezone(timedelta(hours=5)))
    response = provisioning.ProvisioningStatusResponse.model_validate(
        {
            "operation_id": uuid4(),
            "organization_id": uuid4(),
            "business_id": uuid4(),
            "environment_id": uuid4(),
            "workload_id": uuid4(),
            "deployment_id": uuid4(),
            "state": "pending",
            "created_at": non_utc,
            "started_at": None,
            "completed_at": None,
            "attempt_count": 0,
            "last_error": None,
        }
    )

    assert response.model_dump_json().count("2026-10-04T07:00:00Z") == 1


def test_status_error_rejects_unsanitized_code() -> None:
    with pytest.raises(ValidationError):
        provisioning.ProvisioningStatusError.model_validate(
            {"code": "Raw Docker exception: secret", "retryable": False}
        )
