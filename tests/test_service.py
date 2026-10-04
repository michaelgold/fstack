from __future__ import annotations

from typing import cast
from uuid import UUID

import pytest
from sqlalchemy import Connection

import fstack.service as service
from fstack.provisioning import ProvisioningRequest


class _FirstSqlObserved(Exception):
    pass


class _StopAtFirstExecute:
    def __init__(self, generated: list[UUID]) -> None:
        self.generated = generated

    def execute(self, *_args: object, **_kwargs: object) -> None:
        assert len(self.generated) == 9
        raise _FirstSqlObserved


def test_all_row_ids_are_generated_before_first_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    generated: list[UUID] = []

    def generate_uuid() -> UUID:
        value = UUID(int=len(generated) + 1)
        generated.append(value)
        return value

    monkeypatch.setattr(service, "uuid4", generate_uuid)
    request = ProvisioningRequest.model_validate(
        {
            "organization_slug": "acme",
            "organization_name": "Acme Robotics",
            "business_slug": "mailer",
            "business_name": "Acme Mailer",
            "environment_slug": "production",
            "public_domain": "mail.acme.example",
            "crusher_image": "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
            "database_secret_ref": "secret://vault/acme/database",
            "object_storage_secret_ref": "secret://vault/acme/object-storage",
        }
    )
    connection = cast(Connection, _StopAtFirstExecute(generated))

    with pytest.raises(_FirstSqlObserved):
        service.create_provisioning_operation(
            connection,
            request=request,
            idempotency_key="create-acme-mailer",
            actor_principal="installation-operator",
        )
