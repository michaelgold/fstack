"""Transactional outbox dispatch from PostgreSQL into Temporal."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import Engine, text
from temporalio.client import Client

from fstack.temporal_client import start_provision_crusher_workflow

LOGGER = logging.getLogger(__name__)
StartWorkflow = Callable[..., Awaitable[object]]
DispatchOnce = Callable[..., Awaitable["DispatchBatchResult"]]


@dataclass(frozen=True)
class DispatchBatchResult:
    selected: int
    dispatched: int
    failed: int


def _load_pending_operation_ids(engine: Engine, *, limit: int) -> list[UUID]:
    if limit < 1:
        raise ValueError("dispatch limit must be positive")
    with engine.connect() as connection:
        return list(
            connection.execute(
                text(
                    """
                    SELECT operation_id
                    FROM workflow_dispatches
                    WHERE state = 'pending'
                    ORDER BY created_at, operation_id
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).scalars()
        )


def _mark_dispatched(engine: Engine, *, operation_id: UUID) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE workflow_dispatches
                SET state = 'dispatched', dispatched_at = clock_timestamp()
                WHERE operation_id = :operation_id
                  AND state = 'pending'
                """
            ),
            {"operation_id": operation_id},
        )


async def dispatch_pending_once(
    engine: Engine,
    client: Client | object,
    *,
    task_queue: str,
    limit: int = 100,
    start_workflow: StartWorkflow = start_provision_crusher_workflow,
) -> DispatchBatchResult:
    """Attempt one at-least-once outbox batch without holding database locks over I/O."""
    operation_ids = await asyncio.to_thread(_load_pending_operation_ids, engine, limit=limit)
    dispatched = 0
    failed = 0
    for operation_id in operation_ids:
        try:
            await start_workflow(client, operation_id, task_queue=task_queue)
        except Exception as exc:
            LOGGER.warning(
                "workflow dispatch start failed operation_id=%s error_type=%s",
                operation_id,
                type(exc).__name__,
            )
            failed += 1
            continue
        await asyncio.to_thread(_mark_dispatched, engine, operation_id=operation_id)
        dispatched += 1
    return DispatchBatchResult(
        selected=len(operation_ids),
        dispatched=dispatched,
        failed=failed,
    )


async def run_workflow_dispatcher(
    engine: Engine | object,
    client: Client | object,
    *,
    task_queue: str,
    batch_limit: int = 100,
    poll_interval_seconds: float = 1.0,
    dispatch_once: DispatchOnce = dispatch_pending_once,
) -> None:
    """Continuously drain the transactional outbox until the worker is cancelled."""
    if poll_interval_seconds <= 0:
        raise ValueError("dispatch poll interval must be positive")
    while True:
        try:
            await dispatch_once(
                engine,
                client,
                task_queue=task_queue,
                limit=batch_limit,
            )
        except Exception:
            LOGGER.warning("workflow dispatch batch failed; retrying")
        await asyncio.sleep(poll_interval_seconds)
