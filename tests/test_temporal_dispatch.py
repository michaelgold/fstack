from __future__ import annotations

import asyncio

import pytest

from fstack.temporal_worker import run_worker_services
from fstack.workflow_dispatch import DispatchBatchResult, run_workflow_dispatcher


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
