"""Temporal worker for durable Fstack provisioning workflows."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import NoReturn, cast
from uuid import UUID

from sqlalchemy import Engine, create_engine
from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

import fstack.runtime_state as runtime_state
import fstack.runtime_submission as runtime_submission
from fstack.docker_cli import DockerCLI
from fstack.runtime_application import RuntimeApplicationError, apply_runtime_plan
from fstack.temporal_workflow import (
    APPLY_LOCAL_CRUSHER_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_FAILURE_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_PREPARE_FAILURE_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY,
    PREPARE_LOCAL_CRUSHER_ACTIVITY,
    OperationFailure,
    ProvisionCrusherInput,
    ProvisionCrusherWorkflow,
    RuntimeFailure,
    RuntimeSubmission,
)
from fstack.workflow_dispatch import run_workflow_dispatcher

DEFAULT_TEMPORAL_TARGET = "127.0.0.1:7233"
DEFAULT_TEMPORAL_NAMESPACE = "default"
DEFAULT_TEMPORAL_TASK_QUEUE = "fstack-provisioning"
RunDispatcher = Callable[..., Awaitable[None]]

_APPLY_STATE_CONFLICTS = {
    "invalid_provider_identity",
    "operation_not_running",
    "provider_identity_conflict",
    "runtime_submission_not_found",
    "runtime_submission_not_submittable",
    "runtime_submission_operation_mismatch",
    "runtime_submission_terminal",
    "sealed_spec_mismatch",
    "secret_source_not_configured",
}
_SUCCESS_FINALIZER_CONFLICTS = {
    "runtime_submission_not_confirmable",
    "runtime_submission_not_found",
    "runtime_submission_operation_mismatch",
}
_PREPARE_FAILURE_FINALIZER_CONFLICTS = {
    "failure_code_conflict",
    "operation_not_found",
    "runtime_submission_not_failable",
    "runtime_submission_not_found",
}
_FAILURE_FINALIZER_CONFLICTS = {
    "failure_code_conflict",
    "runtime_submission_not_failable",
    "runtime_submission_not_found",
    "runtime_submission_operation_mismatch",
}


def _raise_infrastructure_failure() -> NoReturn:
    code = "activity_infrastructure_failure"
    raise ApplicationError(
        code,
        code,
        type="ActivityInfrastructureFailure",
        non_retryable=False,
    ) from None


def _raise_state_conflict(code: str) -> NoReturn:
    raise ApplicationError(
        code,
        code,
        type="RuntimeStateConflict",
        non_retryable=True,
    ) from None


def _parse_uuid_payload(
    value: object,
    *,
    message: str,
    error_type: str,
    detail: str | None = None,
) -> UUID:
    invalid = type(value) is not str
    parsed: UUID | None = None
    if not invalid:
        try:
            parsed = UUID(cast(str, value))
        except ValueError:
            invalid = True
        else:
            invalid = cast(str, value) != str(parsed)
    if invalid or parsed is None:
        details = () if detail is None else (detail,)
        raise ApplicationError(
            message,
            *details,
            type=error_type,
            non_retryable=True,
        ) from None
    return parsed


@activity.defn(name=PREPARE_LOCAL_CRUSHER_ACTIVITY)
def prepare_local_crusher(request: ProvisionCrusherInput) -> RuntimeSubmission:
    """Prepare or replay one exact PostgreSQL-backed runtime submission."""
    operation_id = _parse_uuid_payload(
        request.operation_id,
        message="operation ID is invalid",
        error_type="InvalidOperationId",
    )
    submission = None
    not_claimable = False
    try:
        submission = runtime_submission.prepare_operation(operation_id)
    except runtime_submission.OperationNotClaimableError:
        not_claimable = True
    except Exception:
        submission = None
    if not_claimable:
        code = "prepare_not_claimable"
        raise ApplicationError(
            code,
            code,
            type="OperationNotClaimable",
            non_retryable=True,
        ) from None
    if submission is None:
        _raise_infrastructure_failure()
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


def _workflow_submission(
    submission: runtime_submission.PreparedRuntimeSubmission,
) -> RuntimeSubmission:
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


def _submission_uuid(submission: RuntimeSubmission) -> UUID:
    return _parse_uuid_payload(
        submission.submission_id,
        message="runtime submission ID is invalid",
        error_type="InvalidRuntimeSubmissionId",
    )


def _submission_operation_uuid(submission: RuntimeSubmission) -> UUID:
    return _parse_uuid_payload(
        submission.operation_id,
        message="runtime operation ID is invalid",
        error_type="InvalidOperationId",
    )


@activity.defn(name=APPLY_LOCAL_CRUSHER_ACTIVITY)
def apply_local_crusher(submission: RuntimeSubmission) -> RuntimeSubmission:
    """Mark running, reconcile Docker, and durably record provider acceptance."""
    submission_id = _submission_uuid(submission)
    operation_id = _submission_operation_uuid(submission)
    unexpected_failure = False
    application_failure: tuple[str, bool] | None = None
    try:
        runtime_state.mark_running(submission_id, operation_id)
        plan = runtime_state.load_plan(submission_id)
        apply_runtime_plan(
            plan,
            DockerCLI(),
            persist_submitted=lambda external_id: runtime_state.persist_submitted(
                submission_id, external_id
            ),
        )
    except RuntimeApplicationError as exc:
        application_failure = (exc.code, not exc.retryable)
    except runtime_state.RuntimeStateError as exc:
        code = str(exc)
        if code in _APPLY_STATE_CONFLICTS:
            application_failure = (code, True)
        else:
            unexpected_failure = True
    except Exception:
        unexpected_failure = True
    if application_failure is not None:
        code, non_retryable = application_failure
        if code == "runtime_submission_operation_mismatch":
            _raise_state_conflict(code)
        raise ApplicationError(
            code,
            code,
            type="RuntimeApplicationFailed",
            non_retryable=non_retryable,
        ) from None
    if unexpected_failure:
        _raise_infrastructure_failure()
    return RuntimeSubmission(**{**submission.__dict__, "state": "submitted"})


@activity.defn(name=FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY)
def finalize_local_crusher_success(submission: RuntimeSubmission) -> RuntimeSubmission:
    """Atomically confirm the runtime submission and public operation."""
    submission_id = _submission_uuid(submission)
    operation_id = _submission_operation_uuid(submission)
    result = None
    state_conflict: str | None = None
    try:
        result = runtime_state.finalize_success(submission_id, operation_id)
    except runtime_state.RuntimeStateError as exc:
        code = str(exc)
        if code in _SUCCESS_FINALIZER_CONFLICTS:
            state_conflict = code
    except Exception:
        result = None
    if state_conflict is not None:
        _raise_state_conflict(state_conflict)
    if result is None:
        _raise_infrastructure_failure()
    return _workflow_submission(result)


@activity.defn(name=FINALIZE_LOCAL_CRUSHER_PREPARE_FAILURE_ACTIVITY)
def finalize_local_crusher_prepare_failure(failure: OperationFailure) -> RuntimeSubmission:
    """Atomically fail an exact prepared submission after permanent prepare failure."""
    operation_id = _parse_uuid_payload(
        failure.operation_id,
        message="operation_id_invalid",
        error_type="InvalidOperationId",
        detail="operation_id_invalid",
    )
    result = None
    state_conflict: str | None = None
    try:
        result = runtime_state.finalize_prepare_failure(operation_id, failure.error_code)
    except runtime_state.RuntimeStateError as exc:
        code = str(exc)
        if code in _PREPARE_FAILURE_FINALIZER_CONFLICTS:
            state_conflict = code
    except Exception:
        result = None
    if state_conflict is not None:
        _raise_state_conflict(state_conflict)
    if result is None:
        _raise_infrastructure_failure()
    return _workflow_submission(result)


@activity.defn(name=FINALIZE_LOCAL_CRUSHER_FAILURE_ACTIVITY)
def finalize_local_crusher_failure(failure: RuntimeFailure) -> RuntimeSubmission:
    """Atomically fail the runtime submission and public operation."""
    submission_id = _submission_uuid(failure.runtime_submission)
    operation_id = _submission_operation_uuid(failure.runtime_submission)
    result = None
    state_conflict: str | None = None
    try:
        result = runtime_state.finalize_failure(submission_id, operation_id, failure.error_code)
    except runtime_state.RuntimeStateError as exc:
        code = str(exc)
        if code in _FAILURE_FINALIZER_CONFLICTS:
            state_conflict = code
    except Exception:
        result = None
    if state_conflict is not None:
        _raise_state_conflict(state_conflict)
    if result is None:
        _raise_infrastructure_failure()
    return _workflow_submission(result)


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
                activities=[
                    prepare_local_crusher,
                    apply_local_crusher,
                    finalize_local_crusher_success,
                    finalize_local_crusher_prepare_failure,
                    finalize_local_crusher_failure,
                ],
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
