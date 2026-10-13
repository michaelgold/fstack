"""Temporal workflow contract for durable Crusher provisioning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

from fstack.docker_cli import APPLY_DOCKER_TIMEOUT_BUDGET

PREPARE_LOCAL_CRUSHER_ACTIVITY = "fstack.prepare_local_crusher.v1"
APPLY_LOCAL_CRUSHER_ACTIVITY = "fstack.apply_local_crusher.v1"
FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY = "fstack.finalize_local_crusher_success.v1"
FINALIZE_LOCAL_CRUSHER_FAILURE_ACTIVITY = "fstack.finalize_local_crusher_failure.v1"
FINALIZE_LOCAL_CRUSHER_PREPARE_FAILURE_ACTIVITY = "fstack.finalize_local_crusher_prepare_failure.v1"
APPLY_ACTIVITY_START_TO_CLOSE_TIMEOUT = timedelta(minutes=30)

if APPLY_ACTIVITY_START_TO_CLOSE_TIMEOUT <= APPLY_DOCKER_TIMEOUT_BUDGET:
    raise RuntimeError("apply activity timeout must exceed the declared Docker timeout budget")


@dataclass(frozen=True)
class ProvisionCrusherInput:
    operation_id: str


@dataclass(frozen=True)
class RuntimeSubmission:
    submission_id: str
    operation_id: str
    state: Literal["prepared", "submitted", "confirmed", "failed"]
    provider: Literal["local_docker"]
    generation: int
    submission_key: str
    spec_fingerprint: str
    api_container_name: str
    migration_container_name: str


@dataclass(frozen=True)
class ProvisionCrusherResult:
    operation_id: str
    state: Literal["prepared", "submitted", "confirmed", "failed"]
    runtime_submission: RuntimeSubmission


@dataclass(frozen=True)
class RuntimeFailure:
    runtime_submission: RuntimeSubmission
    error_code: str


@dataclass(frozen=True)
class OperationFailure:
    operation_id: str
    error_code: str


def _nonretryable_code(exc: ActivityError, fallback: str) -> str | None:
    cause = exc.cause
    if not isinstance(cause, ApplicationError) or not cause.non_retryable:
        return None
    details = list(cause.details)
    return details[0] if details and isinstance(details[0], str) else fallback


@workflow.defn(name="fstack.provision_crusher.v1")
class ProvisionCrusherWorkflow:
    """Apply and finalize one exact, idempotent Crusher runtime submission."""

    @workflow.run
    async def run(self, request: ProvisionCrusherInput) -> ProvisionCrusherResult:
        try:
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
        except ActivityError as exc:
            error_code = _nonretryable_code(exc, "prepare_failed")
            if error_code is None:
                raise
            failed = await workflow.execute_activity(
                FINALIZE_LOCAL_CRUSHER_PREPARE_FAILURE_ACTIVITY,
                OperationFailure(operation_id=request.operation_id, error_code=error_code),
                result_type=RuntimeSubmission,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=10),
                ),
            )
            return ProvisionCrusherResult(
                operation_id=request.operation_id,
                state=failed.state,
                runtime_submission=failed,
            )
        try:
            applied = await workflow.execute_activity(
                APPLY_LOCAL_CRUSHER_ACTIVITY,
                submission,
                result_type=RuntimeSubmission,
                start_to_close_timeout=APPLY_ACTIVITY_START_TO_CLOSE_TIMEOUT,
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=2),
                    maximum_interval=timedelta(seconds=30),
                ),
            )
        except ActivityError as exc:
            cause = exc.cause
            if not isinstance(cause, ApplicationError) or not cause.non_retryable:
                raise
            details = list(cause.details)
            error_code = details[0] if details and isinstance(details[0], str) else "runtime_failed"
            failed = await workflow.execute_activity(
                FINALIZE_LOCAL_CRUSHER_FAILURE_ACTIVITY,
                RuntimeFailure(runtime_submission=submission, error_code=error_code),
                result_type=RuntimeSubmission,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=10),
                ),
            )
            return ProvisionCrusherResult(
                operation_id=request.operation_id,
                state=failed.state,
                runtime_submission=failed,
            )
        confirmed = await workflow.execute_activity(
            FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY,
            applied,
            result_type=RuntimeSubmission,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=10),
            ),
        )
        return ProvisionCrusherResult(
            operation_id=request.operation_id,
            state=confirmed.state,
            runtime_submission=confirmed,
        )
