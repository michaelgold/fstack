from __future__ import annotations

import asyncio
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text

import fstack.workflow_dispatch as workflow_dispatch_module
from fstack.api import create_app
from fstack.workflow_dispatch import DispatchBatchResult, dispatch_pending_once

VALID_BODY = {
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


@pytest.mark.postgres
def test_dispatch_pending_once_starts_and_marks_exact_workflow(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    created = TestClient(create_app()).post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "create-acme-mailer",
        },
        json=VALID_BODY,
    )
    operation_id = UUID(created.json()["operation_id"])
    calls: list[tuple[object, UUID, str]] = []
    client = object()

    async def start_workflow(
        received_client: object,
        received_operation_id: UUID,
        *,
        task_queue: str,
    ) -> object:
        calls.append((received_client, received_operation_id, task_queue))
        return object()

    result = asyncio.run(
        dispatch_pending_once(
            migrated_database,
            client,
            task_queue="test-provisioning",
            limit=10,
            start_workflow=start_workflow,
        )
    )

    assert result == DispatchBatchResult(selected=1, dispatched=1, failed=0)
    assert calls == [(client, operation_id, "test-provisioning")]
    with migrated_database.connect() as connection:
        row = (
            connection.execute(
                text(
                    "SELECT workflow_id, workflow_type, state, dispatched_at "
                    "FROM workflow_dispatches WHERE operation_id = :operation_id"
                ),
                {"operation_id": operation_id},
            )
            .mappings()
            .one()
        )
    assert row["workflow_id"] == f"provisioning/{operation_id}"
    assert row["workflow_type"] == "fstack.provision_crusher.v2"
    assert row["state"] == "dispatched"
    assert row["dispatched_at"] is not None


@pytest.mark.postgres
def test_transient_start_failure_leaves_intent_pending_for_replay(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    created = TestClient(create_app()).post(
        "/v1/provisioning/operations",
        headers={
            "Authorization": "Bearer test-operator-token",
            "Idempotency-Key": "create-acme-mailer",
        },
        json=VALID_BODY,
    )
    operation_id = UUID(created.json()["operation_id"])
    calls = 0
    warning_calls: list[tuple[str, tuple[object, ...]]] = []

    def record_warning(message: str, *args: object) -> None:
        warning_calls.append((message, args))

    monkeypatch.setattr(workflow_dispatch_module.LOGGER, "warning", record_warning)

    async def start_workflow(
        _client: object,
        received_operation_id: UUID,
        *,
        task_queue: str,
    ) -> object:
        nonlocal calls
        assert received_operation_id == operation_id
        assert task_queue == "test-provisioning"
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary Temporal outage")
        return object()

    first = asyncio.run(
        dispatch_pending_once(
            migrated_database,
            object(),
            task_queue="test-provisioning",
            start_workflow=start_workflow,
        )
    )
    with migrated_database.connect() as connection:
        first_state = connection.execute(
            text("SELECT state FROM workflow_dispatches WHERE operation_id = :operation_id"),
            {"operation_id": operation_id},
        ).scalar_one()

    second = asyncio.run(
        dispatch_pending_once(
            migrated_database,
            object(),
            task_queue="test-provisioning",
            start_workflow=start_workflow,
        )
    )
    with migrated_database.connect() as connection:
        second_state = connection.execute(
            text("SELECT state FROM workflow_dispatches WHERE operation_id = :operation_id"),
            {"operation_id": operation_id},
        ).scalar_one()

    assert first == DispatchBatchResult(selected=1, dispatched=0, failed=1)
    assert first_state == "pending"
    assert second == DispatchBatchResult(selected=1, dispatched=1, failed=0)
    assert second_state == "dispatched"
    assert calls == 2
    assert warning_calls == [
        (
            "workflow dispatch start failed operation_id=%s error_type=%s",
            (operation_id, "RuntimeError"),
        )
    ]
    assert "temporary Temporal outage" not in repr(warning_calls)
