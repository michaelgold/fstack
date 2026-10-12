from __future__ import annotations

import asyncio
from uuid import UUID

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import fstack.runtime_submission as runtime_submission_module
from fstack.runtime_submission import OperationNotClaimableError, PreparedRuntimeSubmission
from fstack.temporal_client import start_provision_crusher_workflow
from fstack.temporal_worker import prepare_local_crusher
from fstack.temporal_workflow import (
    PREPARE_LOCAL_CRUSHER_ACTIVITY,
    ProvisionCrusherInput,
    ProvisionCrusherResult,
    ProvisionCrusherWorkflow,
    RuntimeSubmission,
)


def _submission(operation_id: UUID, *, state: str = "prepared") -> RuntimeSubmission:
    return RuntimeSubmission(
        submission_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        operation_id=str(operation_id),
        state=state,
        provider="local_docker",
        generation=1,
        submission_key="b" * 64,
        spec_fingerprint="c" * 64,
        api_container_name="fstack-crusher-" + operation_id.hex,
        migration_container_name="fstack-crusher-migrate-" + operation_id.hex,
    )


def test_provision_crusher_workflow_prepares_exact_operation_once() -> None:
    operation_id = UUID("11111111-1111-4111-8111-111111111111")
    calls: list[ProvisionCrusherInput] = []

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_ACTIVITY)
    async def prepare(request: ProvisionCrusherInput) -> RuntimeSubmission:
        calls.append(request)
        return _submission(operation_id)

    async def run() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-provision-crusher",
                workflows=[ProvisionCrusherWorkflow],
                activities=[prepare],
            ),
        ):
            result = await environment.client.execute_workflow(
                ProvisionCrusherWorkflow.run,
                ProvisionCrusherInput(operation_id=str(operation_id)),
                id=f"provisioning/{operation_id}",
                task_queue="test-provision-crusher",
            )

        assert result == ProvisionCrusherResult(
            operation_id=str(operation_id),
            state="prepared",
            runtime_submission=_submission(operation_id),
        )
        assert calls == [ProvisionCrusherInput(operation_id=str(operation_id))]

    asyncio.run(run())


def test_prepare_local_crusher_activity_maps_exact_durable_submission(monkeypatch) -> None:
    operation_id = UUID("22222222-2222-4222-8222-222222222222")
    submission_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    prepared = PreparedRuntimeSubmission(
        submission_id=submission_id,
        operation_id=operation_id,
        state="prepared",
        provider="local_docker",
        generation=1,
        submission_key="b" * 64,
        spec_fingerprint="c" * 64,
        api_container_name="fstack-crusher-" + operation_id.hex,
        migration_container_name="fstack-crusher-migrate-" + operation_id.hex,
    )
    calls: list[UUID] = []

    def fake_prepare_operation(requested_operation_id: UUID) -> PreparedRuntimeSubmission:
        calls.append(requested_operation_id)
        return prepared

    monkeypatch.setattr(runtime_submission_module, "prepare_operation", fake_prepare_operation)

    result = prepare_local_crusher(ProvisionCrusherInput(operation_id=str(operation_id)))

    assert calls == [operation_id]
    assert result == _submission(operation_id)


def test_prepare_activity_classifies_missing_operation_as_terminal(monkeypatch) -> None:
    operation_id = UUID("55555555-5555-4555-8555-555555555555")

    def reject_operation(_operation_id: UUID) -> PreparedRuntimeSubmission:
        raise OperationNotClaimableError("operation has no runtime submission")

    monkeypatch.setattr(runtime_submission_module, "prepare_operation", reject_operation)

    with pytest.raises(ApplicationError) as exc_info:
        prepare_local_crusher(ProvisionCrusherInput(operation_id=str(operation_id)))

    assert exc_info.value.non_retryable is True
    assert exc_info.value.type == "OperationNotClaimable"
    assert exc_info.value.message == "operation has no runtime submission"


def test_start_provision_crusher_workflow_uses_deterministic_identity() -> None:
    operation_id = UUID("33333333-3333-4333-8333-333333333333")

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_ACTIVITY)
    async def prepare(_request: ProvisionCrusherInput) -> RuntimeSubmission:
        return _submission(operation_id)

    async def run() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-temporal-dispatch",
                workflows=[ProvisionCrusherWorkflow],
                activities=[prepare],
            ),
        ):
            handle = await start_provision_crusher_workflow(
                environment.client,
                operation_id,
                task_queue="test-temporal-dispatch",
            )
            result = await handle.result()

        assert handle.id == f"provisioning/{operation_id}"
        assert result.operation_id == str(operation_id)
        assert result.state == "prepared"

    asyncio.run(run())


def test_transient_activity_failure_remains_recoverable_after_five_attempts() -> None:
    operation_id = UUID("44444444-4444-4444-8444-444444444444")
    calls = 0

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_ACTIVITY)
    async def eventually_prepare(
        _request: ProvisionCrusherInput,
    ) -> RuntimeSubmission:
        nonlocal calls
        calls += 1
        if calls <= 5:
            raise RuntimeError("temporary database outage")
        return _submission(operation_id)

    async def run() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-transient-recovery",
                workflows=[ProvisionCrusherWorkflow],
                activities=[eventually_prepare],
            ),
        ):
            result = await environment.client.execute_workflow(
                ProvisionCrusherWorkflow.run,
                ProvisionCrusherInput(operation_id=str(operation_id)),
                id=f"provisioning/{operation_id}",
                task_queue="test-transient-recovery",
            )

        assert result.state == "prepared"
        assert calls == 6

    asyncio.run(run())
