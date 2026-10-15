"""Temporal client helpers for starting durable Fstack workflows."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from fstack.temporal_workflow import (
    ProvisionCrusherInput,
    ProvisionCrusherResult,
    ProvisionCrusherV2Input,
    ProvisionCrusherV2Result,
    ProvisionCrusherV2Workflow,
    ProvisionCrusherWorkflow,
)

WORKFLOW_TYPE_V1 = "fstack.provision_crusher.v1"
WORKFLOW_TYPE_V2 = "fstack.provision_crusher.v2"


def provisioning_workflow_id(operation_id: UUID) -> str:
    return f"provisioning/{operation_id}"


async def start_provision_crusher_workflow(
    client: Client,
    workflow_type: str,
    operation_id: UUID,
    *,
    task_queue: str,
) -> WorkflowHandle[Any, Any]:
    """Start or return the one durable workflow for a provisioning operation."""
    workflow_id = provisioning_workflow_id(operation_id)
    try:
        if workflow_type == WORKFLOW_TYPE_V1:
            return await client.start_workflow(
                ProvisionCrusherWorkflow.run,
                ProvisionCrusherInput(operation_id=str(operation_id)),
                id=workflow_id,
                task_queue=task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            )
        if workflow_type == WORKFLOW_TYPE_V2:
            return await client.start_workflow(
                ProvisionCrusherV2Workflow.run,
                ProvisionCrusherV2Input(operation_id=str(operation_id)),
                id=workflow_id,
                task_queue=task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            )
        raise ValueError("unsupported provisioning workflow type")
    except WorkflowAlreadyStartedError:
        result_type = (
            ProvisionCrusherResult
            if workflow_type == WORKFLOW_TYPE_V1
            else ProvisionCrusherV2Result
        )
        return client.get_workflow_handle(
            workflow_id,
            result_type=result_type,
        )
