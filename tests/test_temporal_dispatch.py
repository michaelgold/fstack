from __future__ import annotations

import asyncio
from uuid import UUID

import pytest
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

import fstack.workflow_dispatch as workflow_dispatch_module
from fstack.temporal_client import (
    WORKFLOW_TYPE_V1,
    WORKFLOW_TYPE_V2,
    start_provision_crusher_workflow,
)
from fstack.temporal_worker import run_worker_services
from fstack.temporal_workflow import (
    ProvisionCrusherInput,
    ProvisionCrusherResult,
    ProvisionCrusherV2Input,
    ProvisionCrusherV2Result,
    ProvisionCrusherV2Workflow,
    ProvisionCrusherWorkflow,
)
from fstack.workflow_dispatch import (
    DispatchBatchResult,
    dispatch_pending_once,
    run_workflow_dispatcher,
)


@pytest.mark.parametrize(
    ("workflow_type", "workflow_run", "request_type"),
    [
        (WORKFLOW_TYPE_V1, ProvisionCrusherWorkflow.run, ProvisionCrusherInput),
        (WORKFLOW_TYPE_V2, ProvisionCrusherV2Workflow.run, ProvisionCrusherV2Input),
    ],
)
def test_start_workflow_selects_the_exact_persisted_generation(
    workflow_type: str,
    workflow_run: object,
    request_type: type[ProvisionCrusherInput] | type[ProvisionCrusherV2Input],
) -> None:
    operation_id = "11111111-1111-4111-8111-111111111111"
    calls: list[tuple[object, object, dict[str, object]]] = []
    handle = object()

    class Client:
        async def start_workflow(
            self, received_run: object, request: object, **kwargs: object
        ) -> object:
            calls.append((received_run, request, kwargs))
            return handle

    result = asyncio.run(
        start_provision_crusher_workflow(
            Client(),
            workflow_type,
            UUID(operation_id),
            task_queue="test-provisioning",
        )
    )

    assert result is handle
    assert calls == [
        (
            workflow_run,
            request_type(operation_id=operation_id),
            {
                "id": f"provisioning/{operation_id}",
                "task_queue": "test-provisioning",
                "id_reuse_policy": WorkflowIDReusePolicy.REJECT_DUPLICATE,
                "id_conflict_policy": WorkflowIDConflictPolicy.USE_EXISTING,
            },
        )
    ]


def test_start_workflow_rejects_unknown_persisted_generation_before_client_call() -> None:
    calls = 0

    class Client:
        async def start_workflow(self, *_args: object, **_kwargs: object) -> object:
            nonlocal calls
            calls += 1
            return object()

    with pytest.raises(ValueError, match="unsupported provisioning workflow type"):
        asyncio.run(
            start_provision_crusher_workflow(
                Client(),
                "provider-marker://unsupported",
                UUID("11111111-1111-4111-8111-111111111111"),
                task_queue="test-provisioning",
            )
        )

    assert calls == 0


@pytest.mark.parametrize(
    ("workflow_type", "result_type"),
    [
        (WORKFLOW_TYPE_V1, ProvisionCrusherResult),
        (WORKFLOW_TYPE_V2, ProvisionCrusherV2Result),
    ],
)
def test_duplicate_workflow_id_returns_existing_generation_handle(
    workflow_type: str,
    result_type: type[ProvisionCrusherResult] | type[ProvisionCrusherV2Result],
) -> None:
    operation_id = UUID("11111111-1111-4111-8111-111111111111")
    existing = object()
    handle_calls: list[tuple[str, object]] = []

    class Client:
        async def start_workflow(self, *_args: object, **_kwargs: object) -> object:
            raise WorkflowAlreadyStartedError(f"provisioning/{operation_id}", workflow_type)

        def get_workflow_handle(self, workflow_id: str, *, result_type: object) -> object:
            handle_calls.append((workflow_id, result_type))
            return existing

    result = asyncio.run(
        start_provision_crusher_workflow(
            Client(),
            workflow_type,
            operation_id,
            task_queue="test-provisioning",
        )
    )

    assert result is existing
    assert handle_calls == [(f"provisioning/{operation_id}", result_type)]


def test_dispatch_unknown_workflow_type_fails_closed_without_logging_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_id = UUID("11111111-1111-4111-8111-111111111111")
    marker = "provider-marker://unsupported"
    marked: list[UUID] = []
    warnings: list[tuple[str, tuple[object, ...]]] = []

    monkeypatch.setattr(
        workflow_dispatch_module,
        "_load_pending_dispatches",
        lambda _engine, *, limit: [(operation_id, marker)],
        raising=False,
    )
    monkeypatch.setattr(
        workflow_dispatch_module,
        "_mark_dispatched",
        lambda _engine, *, operation_id: marked.append(operation_id),
    )
    monkeypatch.setattr(
        workflow_dispatch_module.LOGGER,
        "warning",
        lambda message, *args: warnings.append((message, args)),
    )

    class Client:
        async def start_workflow(self, *_args: object, **_kwargs: object) -> object:
            pytest.fail("unknown workflow type must fail before the Temporal client call")

    result = asyncio.run(
        dispatch_pending_once(
            object(),
            Client(),
            task_queue="test-provisioning",
            start_workflow=start_provision_crusher_workflow,
        )
    )

    assert result == DispatchBatchResult(selected=1, dispatched=0, failed=1)
    assert marked == []
    assert warnings == [
        (
            "workflow dispatch start failed operation_id=%s error_type=%s",
            (operation_id, "ValueError"),
        )
    ]
    assert marker not in repr(warnings)


def test_workflow_dispatcher_repeats_until_cancelled() -> None:
    calls: list[tuple[object, object, str, int]] = []
    ready = asyncio.Event()
    engine = object()
    client = object()

    async def dispatch_once(
        received_engine: object,
        received_client: object,
        *,
        task_queue: str,
        limit: int,
    ) -> DispatchBatchResult:
        calls.append((received_engine, received_client, task_queue, limit))
        ready.set()
        return DispatchBatchResult(selected=0, dispatched=0, failed=0)

    async def run() -> None:
        task = asyncio.create_task(
            run_workflow_dispatcher(
                engine,
                client,
                task_queue="test-provisioning",
                batch_limit=17,
                poll_interval_seconds=3600,
                dispatch_once=dispatch_once,
            )
        )
        await asyncio.wait_for(ready.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert calls == [(engine, client, "test-provisioning", 17)]


def test_workflow_dispatcher_recovers_from_transient_batch_failure() -> None:
    calls = 0
    recovered = asyncio.Event()

    async def dispatch_once(*_args: object, **_kwargs: object) -> DispatchBatchResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary database outage")
        recovered.set()
        return DispatchBatchResult(selected=0, dispatched=0, failed=0)

    async def run() -> None:
        task = asyncio.create_task(
            run_workflow_dispatcher(
                object(),
                object(),
                task_queue="test-provisioning",
                poll_interval_seconds=0.001,
                dispatch_once=dispatch_once,
            )
        )
        await asyncio.wait_for(recovered.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert calls >= 2


def test_worker_services_run_dispatcher_inside_temporal_worker_lifecycle() -> None:
    events: list[object] = []
    engine = object()
    client = object()

    class Worker:
        async def __aenter__(self) -> Worker:
            events.append("worker-enter")
            return self

        async def __aexit__(self, *args: object) -> None:
            events.append("worker-exit")

    async def dispatcher(
        received_engine: object,
        received_client: object,
        *,
        task_queue: str,
    ) -> None:
        events.append((received_engine, received_client, task_queue))

    asyncio.run(
        run_worker_services(
            Worker(),
            engine,
            client,
            task_queue="test-provisioning",
            dispatcher=dispatcher,
        )
    )

    assert events == [
        "worker-enter",
        (engine, client, "test-provisioning"),
        "worker-exit",
    ]
