"""Temporal workflow contract for durable Crusher provisioning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, cast

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

from fstack.docker_cli import APPLY_DOCKER_TIMEOUT_BUDGET

PREPARE_LOCAL_CRUSHER_ACTIVITY = "fstack.prepare_local_crusher.v1"
APPLY_LOCAL_CRUSHER_ACTIVITY = "fstack.apply_local_crusher.v1"
FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY = "fstack.finalize_local_crusher_success.v1"
FINALIZE_LOCAL_CRUSHER_FAILURE_ACTIVITY = "fstack.finalize_local_crusher_failure.v1"
FINALIZE_LOCAL_CRUSHER_PREPARE_FAILURE_ACTIVITY = "fstack.finalize_local_crusher_prepare_failure.v1"
PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY = "fstack.prepare_local_crusher_ingress.v2"
APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY = "fstack.apply_local_crusher_runtime.v2"
PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY = "fstack.preflight_local_caddy_topology.v1"
RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY = "fstack.reconcile_local_caddy_ingress.v1"
PROBE_LOCAL_CADDY_INGRESS_ACTIVITY = "fstack.probe_local_caddy_ingress.v1"
FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY = (
    "fstack.finalize_local_crusher_ingress_success.v2"
)
FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY = (
    "fstack.finalize_local_crusher_ingress_failure.v2"
)
APPLY_ACTIVITY_START_TO_CLOSE_TIMEOUT = timedelta(minutes=30)
PREPARE_V2_ACTIVITY_START_TO_CLOSE_TIMEOUT = timedelta(minutes=2)
PREFLIGHT_ACTIVITY_START_TO_CLOSE_TIMEOUT = timedelta(minutes=1)
RECONCILE_ACTIVITY_START_TO_CLOSE_TIMEOUT = timedelta(minutes=2)
PROBE_ACTIVITY_START_TO_CLOSE_TIMEOUT = timedelta(minutes=1)
FINALIZE_V2_ACTIVITY_START_TO_CLOSE_TIMEOUT = timedelta(minutes=2)

_STANDARD_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=10),
)
_APPLY_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    maximum_interval=timedelta(seconds=30),
)
_V2_FAILURE_CODES = {
    "caddy_admin_config_invalid",
    "caddy_admin_forbidden",
    "caddy_admin_incompatible",
    "caddy_admin_primitives_unavailable",
    "caddy_admin_response_invalid",
    "caddy_admin_root_unsafe",
    "caddy_admin_socket_unsafe",
    "caddy_admin_timeout",
    "caddy_admin_transport",
    "caddy_config_changed",
    "caddy_host_conflict",
    "caddy_mutation_ambiguous",
    "caddy_prerequisite_missing",
    "caddy_route_id_conflict",
    "caddy_route_rejected",
    "caddy_routes_ambiguous",
    "caddy_topology_invalid",
    "caddy_unavailable",
    "container_exited",
    "docker_name_conflict",
    "docker_spec_conflict",
    "docker_unavailable",
    "image_digest_mismatch",
    "image_unavailable",
    "ingress_readiness_unavailable",
    "ingress_state_infrastructure",
    "ingress_submission_agreement_invalid",
    "ingress_submission_conflict",
    "ingress_submission_identity_invalid",
    "ingress_submission_not_found",
    "ingress_submission_operation_mismatch",
    "ingress_submission_operation_not_running",
    "ingress_submission_state_invalid",
    "ingress_submission_terminal",
    "invalid_caddy_probe_configuration",
    "invalid_image_digest",
    "invalid_provider_identity",
    "invalid_secret_bundle",
    "migration_failed",
    "migration_unavailable",
    "operation_not_found",
    "operation_not_running",
    "provider_identity_conflict",
    "provisioning_failed",
    "readiness_unavailable",
    "runtime_submission_conflict",
    "runtime_submission_not_found",
    "runtime_submission_not_submittable",
    "runtime_submission_operation_mismatch",
    "runtime_submission_terminal",
    "sealed_spec_mismatch",
    "secret_source_not_configured",
    "secret_unavailable",
    "started_audit_conflict",
    "started_audit_missing",
    "started_audit_unexpected",
    "unsafe_secret_file",
    "unsupported_secret_reference",
}

if APPLY_ACTIVITY_START_TO_CLOSE_TIMEOUT <= APPLY_DOCKER_TIMEOUT_BUDGET:
    raise RuntimeError("apply activity timeout must exceed the declared Docker timeout budget")


@dataclass(frozen=True)
class ProvisionCrusherInput:
    operation_id: str


@dataclass(frozen=True)
class ProvisionCrusherV2Input:
    operation_id: str


@dataclass(frozen=True)
class ProvisioningSubmissionIds:
    operation_id: str
    runtime_submission_id: str
    ingress_submission_id: str


@dataclass(frozen=True)
class ProvisioningFailureV2:
    operation_id: str
    scope: Literal["runtime", "ingress"]
    error_code: str


@dataclass(frozen=True)
class ProvisionCrusherV2Result:
    operation_id: str
    state: Literal["confirmed", "failed"]


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


def _v2_nonretryable_code(exc: ActivityError) -> str | None:
    code = _nonretryable_code(exc, "provisioning_failed")
    return code if code is None or code in _V2_FAILURE_CODES else "provisioning_failed"


async def _finalize_v2_failure(
    operation_id: str,
    scope: Literal["runtime", "ingress"],
    error_code: str,
) -> ProvisionCrusherV2Result:
    return cast(
        ProvisionCrusherV2Result,
        await workflow.execute_activity(
            FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY,
            ProvisioningFailureV2(
                operation_id=operation_id,
                scope=scope,
                error_code=error_code,
            ),
            result_type=ProvisionCrusherV2Result,
            start_to_close_timeout=FINALIZE_V2_ACTIVITY_START_TO_CLOSE_TIMEOUT,
            retry_policy=_STANDARD_RETRY_POLICY,
        ),
    )


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


@workflow.defn(name="fstack.provision_crusher.v2")
class ProvisionCrusherV2Workflow:
    """Publish one prepared Crusher runtime through the local Caddy ingress."""

    @workflow.run
    async def run(self, request: ProvisionCrusherV2Input) -> ProvisionCrusherV2Result:
        try:
            ids = await workflow.execute_activity(
                PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY,
                request,
                result_type=ProvisioningSubmissionIds,
                start_to_close_timeout=PREPARE_V2_ACTIVITY_START_TO_CLOSE_TIMEOUT,
                retry_policy=_STANDARD_RETRY_POLICY,
            )
        except ActivityError as exc:
            error_code = _v2_nonretryable_code(exc)
            if error_code is None:
                raise
            return await _finalize_v2_failure(request.operation_id, "runtime", error_code)

        try:
            await workflow.execute_activity(
                APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY,
                ids,
                start_to_close_timeout=APPLY_ACTIVITY_START_TO_CLOSE_TIMEOUT,
                retry_policy=_APPLY_RETRY_POLICY,
            )
        except ActivityError as exc:
            error_code = _v2_nonretryable_code(exc)
            if error_code is None:
                raise
            return await _finalize_v2_failure(request.operation_id, "runtime", error_code)

        for activity_name, timeout in (
            (
                PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY,
                PREFLIGHT_ACTIVITY_START_TO_CLOSE_TIMEOUT,
            ),
            (RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY, RECONCILE_ACTIVITY_START_TO_CLOSE_TIMEOUT),
            (PROBE_LOCAL_CADDY_INGRESS_ACTIVITY, PROBE_ACTIVITY_START_TO_CLOSE_TIMEOUT),
        ):
            try:
                await workflow.execute_activity(
                    activity_name,
                    ids,
                    start_to_close_timeout=timeout,
                    retry_policy=_STANDARD_RETRY_POLICY,
                )
            except ActivityError as exc:
                error_code = _v2_nonretryable_code(exc)
                if error_code is None:
                    raise
                return await _finalize_v2_failure(request.operation_id, "ingress", error_code)

        return cast(
            ProvisionCrusherV2Result,
            await workflow.execute_activity(
                FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY,
                ids,
                result_type=ProvisionCrusherV2Result,
                start_to_close_timeout=FINALIZE_V2_ACTIVITY_START_TO_CLOSE_TIMEOUT,
                retry_policy=_STANDARD_RETRY_POLICY,
            ),
        )
