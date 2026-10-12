"""Temporal client helpers for starting durable Fstack workflows."""

from __future__ import annotations

from uuid import UUID

from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from fstack.temporal_workflow import (
    ProvisionCrusherInput,
    ProvisionCrusherResult,
    ProvisionCrusherWorkflow,
)


def provisioning_workflow_id(operation_id: UUID) -> str:
    return f"provisioning/{operation_id}"


async def start_provision_crusher_workflow(
    client: Client,
    operation_id: UUID,
    *,
    task_queue: str,
) -> WorkflowHandle[ProvisionCrusherWorkflow, ProvisionCrusherResult]:
    """Start or return the one durable workflow for a provisioning operation."""
    workflow_id = provisioning_workflow_id(operation_id)
    try:
        return await client.start_workflow(
            ProvisionCrusherWorkflow.run,
            ProvisionCrusherInput(operation_id=str(operation_id)),
            id=workflow_id,
            task_queue=task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
    except WorkflowAlreadyStartedError:
        return client.get_workflow_handle(
            workflow_id,
            result_type=ProvisionCrusherResult,
        )
