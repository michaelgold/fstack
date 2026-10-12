"""Temporal workflow contract for durable Crusher provisioning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from temporalio import workflow
from temporalio.common import RetryPolicy

PREPARE_LOCAL_CRUSHER_ACTIVITY = "fstack.prepare_local_crusher.v1"


@dataclass(frozen=True)
class ProvisionCrusherInput:
    operation_id: str


@dataclass(frozen=True)
class RuntimeSubmission:
    submission_id: str
    operation_id: str
    state: Literal["prepared", "submitted", "confirmed"]
    provider: Literal["local_docker"]
    generation: int
    submission_key: str
    spec_fingerprint: str
    api_container_name: str
    migration_container_name: str


@dataclass(frozen=True)
class ProvisionCrusherResult:
    operation_id: str
    state: Literal["prepared", "submitted", "confirmed"]
    runtime_submission: RuntimeSubmission


@workflow.defn(name="fstack.provision_crusher.v1")
class ProvisionCrusherWorkflow:
    """Prepare one exact, idempotent runtime submission for an operation."""

    @workflow.run
    async def run(self, request: ProvisionCrusherInput) -> ProvisionCrusherResult:
        submission = await workflow.execute_activity(
            PREPARE_LOCAL_CRUSHER_ACTIVITY,
            request,
            result_type=RuntimeSubmission,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=10),
            ),
        )
        return ProvisionCrusherResult(
            operation_id=request.operation_id,
            state=submission.state,
            runtime_submission=submission,
        )
