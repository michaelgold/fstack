"""Temporal worker for durable Fstack provisioning workflows."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

from sqlalchemy import Engine, create_engine
from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

import fstack.runtime_submission as runtime_submission
from fstack.temporal_workflow import (
    PREPARE_LOCAL_CRUSHER_ACTIVITY,
    ProvisionCrusherInput,
    ProvisionCrusherWorkflow,
    RuntimeSubmission,
)
from fstack.workflow_dispatch import run_workflow_dispatcher

DEFAULT_TEMPORAL_TARGET = "127.0.0.1:7233"
DEFAULT_TEMPORAL_NAMESPACE = "default"
DEFAULT_TEMPORAL_TASK_QUEUE = "fstack-provisioning"
RunDispatcher = Callable[..., Awaitable[None]]


@activity.defn(name=PREPARE_LOCAL_CRUSHER_ACTIVITY)
def prepare_local_crusher(request: ProvisionCrusherInput) -> RuntimeSubmission:
    """Prepare or replay one exact PostgreSQL-backed runtime submission."""
    try:
        operation_id = UUID(request.operation_id)
    except ValueError:
        raise ApplicationError(
            "operation ID is invalid",
            type="InvalidOperationId",
            non_retryable=True,
        ) from None
    try:
        submission = runtime_submission.prepare_operation(operation_id)
    except runtime_submission.OperationNotClaimableError as exc:
        raise ApplicationError(
            str(exc),
            type="OperationNotClaimable",
            non_retryable=True,
        ) from None
    return RuntimeSubmission(
        submission_id=str(submission.submission_id),
        operation_id=str(submission.operation_id),
        state=submission.state,
        provider=submission.provider,
        generation=submission.generation,
        submission_key=submission.submission_key,
        spec_fingerprint=submission.spec_fingerprint,
        api_container_name=submission.api_container_name,
        migration_container_name=submission.migration_container_name,
    )


async def run_worker_services(
    worker: Worker,
    engine: Engine | object,
    client: Client | object,
    *,
    task_queue: str,
    dispatcher: RunDispatcher = run_workflow_dispatcher,
) -> None:
    """Run Temporal polling and outbox dispatch under one cancellation boundary."""
    async with worker:
        await dispatcher(engine, client, task_queue=task_queue)


async def run_temporal_worker() -> None:
    target = os.environ.get("FSTACK_TEMPORAL_TARGET", DEFAULT_TEMPORAL_TARGET)
    namespace = os.environ.get("FSTACK_TEMPORAL_NAMESPACE", DEFAULT_TEMPORAL_NAMESPACE)
    task_queue = os.environ.get("FSTACK_TEMPORAL_TASK_QUEUE", DEFAULT_TEMPORAL_TASK_QUEUE)
    client = await Client.connect(target, namespace=namespace)
    engine = create_engine(
        runtime_submission._database_url(),
        pool_pre_ping=True,
        hide_parameters=True,
    )
    try:
        with ThreadPoolExecutor(
            max_workers=8, thread_name_prefix="fstack-temporal-activity"
        ) as executor:
            worker = Worker(
                client,
                task_queue=task_queue,
                workflows=[ProvisionCrusherWorkflow],
                activities=[prepare_local_crusher],
                activity_executor=executor,
            )
            await run_worker_services(
                worker,
                engine,
                client,
                task_queue=task_queue,
            )
    finally:
        engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Fstack Temporal provisioning worker")
    parser.parse_args(argv)
    asyncio.run(run_temporal_worker())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
