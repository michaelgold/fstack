"""Temporal worker for durable Fstack provisioning workflows."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, NoReturn, cast
from uuid import UUID

from sqlalchemy import Engine, create_engine
from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

import fstack.runtime_state as runtime_state
import fstack.runtime_submission as runtime_submission
from fstack import caddy_admin, caddy_topology, ingress_probe, ingress_state, ingress_submission
from fstack.docker_cli import DockerCLI
from fstack.runtime_application import RuntimeApplicationError, apply_runtime_plan
from fstack.temporal_workflow import (
    APPLY_LOCAL_CRUSHER_ACTIVITY,
    APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_FAILURE_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_PREPARE_FAILURE_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY,
    PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY,
    PREPARE_LOCAL_CRUSHER_ACTIVITY,
    PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY,
    PROBE_LOCAL_CADDY_INGRESS_ACTIVITY,
    RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY,
    OperationFailure,
    ProvisionCrusherInput,
    ProvisionCrusherV2Input,
    ProvisionCrusherV2Result,
    ProvisionCrusherWorkflow,
    ProvisioningFailureV2,
    ProvisioningSubmissionIds,
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
_RETRYABLE_CADDY_CODES = {
    "caddy_config_changed",
    "caddy_mutation_ambiguous",
    "caddy_unavailable",
}
_CADDY_ADMIN_CODES = {code.value for code in caddy_admin.CaddyAdminErrorCode}
_PREPARE_V2_CONFLICTS = {
    "ingress_submission_conflict",
    "operation_not_found",
    "operation_not_running",
    "runtime_submission_conflict",
    "started_audit_conflict",
    "started_audit_missing",
    "started_audit_unexpected",
}
_RUNTIME_APPLICATION_FAILURES = {
    "container_exited": False,
    "docker_name_conflict": False,
    "docker_spec_conflict": False,
    "docker_unavailable": True,
    "image_digest_mismatch": False,
    "image_unavailable": True,
    "invalid_image_digest": False,
    "invalid_secret_bundle": False,
    "migration_failed": False,
    "migration_unavailable": True,
    "readiness_unavailable": True,
    "sealed_spec_mismatch": False,
    "secret_unavailable": True,
    "unsafe_secret_file": False,
    "unsupported_secret_reference": False,
}
_TOPOLOGY_FAILURES = {
    "caddy_topology_invalid": False,
    "caddy_unavailable": True,
}
_PROBE_FAILURES = {
    "ingress_readiness_unavailable": True,
    "invalid_caddy_probe_configuration": False,
}
_INGRESS_RECONCILE_CONFLICTS = {
    "ingress_submission_agreement_invalid",
    "ingress_submission_identity_invalid",
    "ingress_submission_not_found",
    "ingress_submission_operation_mismatch",
    "ingress_submission_operation_not_running",
    "ingress_submission_state_invalid",
    "ingress_submission_terminal",
}
_SUCCESS_V2_FINALIZER_CONFLICTS = {
    "provisioning_success_bundle_mismatch",
    "provisioning_success_invalid_identifier",
    "provisioning_success_replay_invalid",
    "provisioning_success_state_invalid",
}
_FAILURE_V2_FINALIZER_CONFLICTS = {
    "provisioning_failure_conflict",
    "provisioning_failure_invalid_request",
}
_PROVISIONING_FAILURE_CODES = (
    _PREPARE_V2_CONFLICTS
    | set(_RUNTIME_APPLICATION_FAILURES)
    | set(_TOPOLOGY_FAILURES)
    | set(_PROBE_FAILURES)
    | _CADDY_ADMIN_CODES
    | _INGRESS_RECONCILE_CONFLICTS
    | {"ingress_state_infrastructure", "provisioning_failed"}
)


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
        value = None
        details = () if detail is None else (detail,)
        raise ApplicationError(
            message,
            *details,
            type=error_type,
            non_retryable=True,
        ) from None
    return parsed


def _canonical_uuid_payload(value: object) -> UUID | None:
    """Parse one canonical UUID without raising while raw input is live."""
    parsed: UUID | None = None
    if type(value) is str:
        try:
            candidate = UUID(value)
        except ValueError:
            pass
        else:
            if value == str(candidate):
                parsed = candidate
    value = None
    return parsed


def _require_uuid_payload(
    parsed: UUID | None,
    *,
    message: str,
    error_type: str,
    detail: str,
) -> UUID:
    if parsed is None:
        raise ApplicationError(
            message,
            detail,
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


@activity.defn(name=PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY)
def prepare_local_crusher_ingress_v2(
    request: ProvisionCrusherV2Input,
) -> ProvisioningSubmissionIds:
    """Prepare or replay the exact runtime and ingress submission bundle."""
    parsed_operation_id = _canonical_uuid_payload(request.operation_id)
    del request
    operation_id = _require_uuid_payload(
        parsed_operation_id,
        message="operation_id_invalid",
        error_type="InvalidOperationId",
        detail="operation_id_invalid",
    )
    prepared = None
    conflict_code: str | None = None
    infrastructure_failure = False
    try:
        prepared = ingress_submission.prepare_operation_bundle(operation_id)
    except ingress_submission.ProvisioningSubmissionConflictError as exc:
        raw_code = str(exc)
        conflict_code = raw_code if raw_code in _PREPARE_V2_CONFLICTS else None
        infrastructure_failure = conflict_code is None
        raw_code = ""
    except Exception:
        infrastructure_failure = True
    if conflict_code is not None:
        _raise_state_conflict(conflict_code)
    if infrastructure_failure or prepared is None:
        _raise_infrastructure_failure()
    return ProvisioningSubmissionIds(
        operation_id=str(prepared.operation_id),
        runtime_submission_id=str(prepared.runtime_submission_id),
        ingress_submission_id=str(prepared.ingress_submission_id),
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


def _provisioning_ids(ids: ProvisioningSubmissionIds) -> tuple[UUID, UUID, UUID]:
    parsed_operation_id = _canonical_uuid_payload(ids.operation_id)
    parsed_runtime_submission_id = _canonical_uuid_payload(ids.runtime_submission_id)
    parsed_ingress_submission_id = _canonical_uuid_payload(ids.ingress_submission_id)
    del ids
    operation_id = _require_uuid_payload(
        parsed_operation_id,
        message="operation_id_invalid",
        error_type="InvalidOperationId",
        detail="operation_id_invalid",
    )
    runtime_submission_id = _require_uuid_payload(
        parsed_runtime_submission_id,
        message="runtime_submission_id_invalid",
        error_type="InvalidRuntimeSubmissionId",
        detail="runtime_submission_id_invalid",
    )
    ingress_submission_id = _require_uuid_payload(
        parsed_ingress_submission_id,
        message="ingress_submission_id_invalid",
        error_type="InvalidIngressSubmissionId",
        detail="ingress_submission_id_invalid",
    )
    return operation_id, runtime_submission_id, ingress_submission_id


def _caddy_ids(ids: ProvisioningSubmissionIds) -> caddy_topology.ProvisioningSubmissionIds:
    try:
        operation_id, runtime_submission_id, ingress_submission_id = _provisioning_ids(ids)
    finally:
        del ids
    return caddy_topology.ProvisioningSubmissionIds(
        operation_id=str(operation_id),
        runtime_submission_id=str(runtime_submission_id),
        ingress_submission_id=str(ingress_submission_id),
    )


@activity.defn(name=PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY)
def preflight_local_caddy_topology_activity(ids: ProvisioningSubmissionIds) -> None:
    """Validate local Caddy topology without returning provider details."""
    try:
        caddy_ids = _caddy_ids(ids)
    finally:
        del ids
    failure: tuple[str, bool] | None = None
    try:
        caddy_topology.preflight_local_caddy_topology(caddy_ids)
    except caddy_topology.CaddyTopologyError as exc:
        raw_code = exc.code
        retryable = _TOPOLOGY_FAILURES.get(raw_code)
        failure = (raw_code, retryable) if retryable is not None else ("caddy_unavailable", True)
        raw_code = ""
    except Exception:
        failure = ("caddy_unavailable", True)
    if failure is not None:
        code, retryable = failure
        raise ApplicationError(
            code,
            code,
            type="CaddyTopologyFailed",
            non_retryable=not retryable,
        ) from None
    return None


@activity.defn(name=RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY)
def reconcile_local_caddy_ingress(ids: ProvisioningSubmissionIds) -> None:
    """Load and reconcile the exact route without returning route details."""
    try:
        operation_id, runtime_submission_id, ingress_submission_id = _provisioning_ids(ids)
    finally:
        del ids
    caddy_ids = caddy_topology.ProvisioningSubmissionIds(
        operation_id=str(operation_id),
        runtime_submission_id=str(runtime_submission_id),
        ingress_submission_id=str(ingress_submission_id),
    )
    failure: tuple[str, bool] | None = None
    infrastructure_failure = False
    desired: caddy_topology.DesiredCaddyTopology | None = None
    route: dict[str, Any] | None = None
    config: caddy_admin.CaddyAdminConfig | None = None
    admin: caddy_admin.CaddyAdmin | None = None
    try:
        desired = caddy_topology._load_desired_state(caddy_ids)
        route = ingress_submission._canonical_route(desired.deployment_id, desired.public_domain)
        config = caddy_admin.CaddyAdminConfig.from_environ()
        admin = caddy_admin.CaddyAdmin(caddy_admin.CaddyAdminTransport(config))
        admin.reconcile_submission(route, ingress_submission_id, operation_id)
    except caddy_topology.CaddyTopologyError as exc:
        raw_code = exc.code
        retryable = _TOPOLOGY_FAILURES.get(raw_code)
        if retryable is None:
            infrastructure_failure = True
        else:
            failure = (raw_code, retryable)
        raw_code = ""
    except caddy_admin.CaddyAdminError as exc:
        raw_code = exc.code.value
        if raw_code in _CADDY_ADMIN_CODES:
            failure = (raw_code, raw_code in _RETRYABLE_CADDY_CODES)
        else:
            failure = ("caddy_unavailable", True)
        raw_code = ""
    except ingress_state.IngressStateError as exc:
        raw_code = exc.code
        if raw_code == "ingress_state_infrastructure":
            failure = (raw_code, True)
        elif raw_code in _INGRESS_RECONCILE_CONFLICTS:
            failure = (raw_code, False)
        else:
            infrastructure_failure = True
        raw_code = ""
    except Exception:
        failure = ("caddy_unavailable", True)
    desired = None
    route = None
    config = None
    admin = None
    if infrastructure_failure:
        _raise_infrastructure_failure()
    if failure is not None:
        code, retryable = failure
        raise ApplicationError(
            code,
            code,
            type="CaddyIngressFailed",
            non_retryable=not retryable,
        ) from None
    return None


@activity.defn(name=PROBE_LOCAL_CADDY_INGRESS_ACTIVITY)
def probe_local_caddy_ingress(ids: ProvisioningSubmissionIds) -> None:
    """Probe the routed host while keeping host details inside the activity."""
    try:
        caddy_ids = _caddy_ids(ids)
    finally:
        del ids
    failure: tuple[str, bool] | None = None
    try:
        ingress_probe.probe_ingress(caddy_ids)
    except ingress_probe.IngressProbeError as exc:
        raw_code = exc.code
        retryable = _PROBE_FAILURES.get(raw_code)
        failure = (
            (raw_code, retryable)
            if retryable is not None
            else ("ingress_readiness_unavailable", True)
        )
        raw_code = ""
    except Exception:
        failure = ("ingress_readiness_unavailable", True)
    if failure is not None:
        code, retryable = failure
        raise ApplicationError(
            code,
            code,
            type="CaddyIngressProbeFailed",
            non_retryable=not retryable,
        ) from None
    return None


@activity.defn(name=APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY)
def apply_local_crusher_runtime_v2(ids: ProvisioningSubmissionIds) -> None:
    """Reconcile the prepared runtime without returning provider details."""
    try:
        operation_id, submission_id, _ingress_submission_id = _provisioning_ids(ids)
    finally:
        del ids
    application_failure: tuple[str, bool] | None = None
    unexpected_failure = False
    plan: object | None = None
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
        raw_code = exc.code
        retryable = _RUNTIME_APPLICATION_FAILURES.get(raw_code)
        if retryable is not None:
            application_failure = (raw_code, not retryable)
        else:
            unexpected_failure = True
        raw_code = ""
    except runtime_state.RuntimeStateError as exc:
        raw_code = str(exc)
        if raw_code in _APPLY_STATE_CONFLICTS:
            application_failure = (raw_code, True)
        else:
            unexpected_failure = True
        raw_code = ""
    except Exception:
        unexpected_failure = True
    plan = None
    if application_failure is not None:
        code, non_retryable = application_failure
        raise ApplicationError(
            code,
            code,
            type="RuntimeApplicationFailed",
            non_retryable=non_retryable,
        ) from None
    if unexpected_failure:
        _raise_infrastructure_failure()
    return None


@activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY)
def finalize_local_crusher_ingress_success_v2(
    ids: ProvisioningSubmissionIds,
) -> ProvisionCrusherV2Result:
    """Atomically confirm the exact runtime and ingress bundle."""
    try:
        operation_id, runtime_submission_id, ingress_submission_id = _provisioning_ids(ids)
    finally:
        del ids
    error_code: str | None = None
    infrastructure_failure = False
    try:
        ingress_state.finalize_provisioning_success(
            runtime_submission_id, ingress_submission_id, operation_id
        )
    except ingress_state.IngressStateError as exc:
        raw_code = exc.code
        if raw_code in _SUCCESS_V2_FINALIZER_CONFLICTS:
            error_code = raw_code
        else:
            infrastructure_failure = True
        raw_code = ""
    except Exception:
        infrastructure_failure = True
    if infrastructure_failure:
        _raise_infrastructure_failure()
    if error_code is not None:
        _raise_state_conflict(error_code)
    return ProvisionCrusherV2Result(operation_id=str(operation_id), state="confirmed")


@activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY)
def finalize_local_crusher_ingress_failure_v2(
    failure: ProvisioningFailureV2,
) -> ProvisionCrusherV2Result:
    """Fail a durable bundle using only operation identity, scope, and stable code."""
    parsed_operation_id = _canonical_uuid_payload(failure.operation_id)
    raw_scope = failure.scope
    raw_error_code = failure.error_code
    del failure
    stable_error_code = (
        raw_error_code
        if type(raw_error_code) is str and raw_error_code in _PROVISIONING_FAILURE_CODES
        else "provisioning_failed"
    )
    raw_error_code = ""
    if type(raw_scope) is not str or raw_scope not in {"runtime", "ingress"}:
        del raw_scope
        _raise_state_conflict("provisioning_failure_invalid_request")
    scope: str = raw_scope
    del raw_scope
    operation_id = _require_uuid_payload(
        parsed_operation_id,
        message="operation_id_invalid",
        error_type="InvalidOperationId",
        detail="operation_id_invalid",
    )
    error_code: str | None = None
    infrastructure_failure = False
    try:
        ingress_state.finalize_provisioning_failure(operation_id, scope, stable_error_code)
    except ingress_state.IngressStateError as exc:
        raw_code = exc.code
        if raw_code in _FAILURE_V2_FINALIZER_CONFLICTS:
            error_code = raw_code
        else:
            infrastructure_failure = True
        raw_code = ""
    except Exception:
        infrastructure_failure = True
    if infrastructure_failure:
        _raise_infrastructure_failure()
    if error_code is not None:
        _raise_state_conflict(error_code)
    return ProvisionCrusherV2Result(operation_id=str(operation_id), state="failed")


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


TEMPORAL_ACTIVITIES = (
    prepare_local_crusher,
    apply_local_crusher,
    finalize_local_crusher_success,
    finalize_local_crusher_prepare_failure,
    finalize_local_crusher_failure,
    prepare_local_crusher_ingress_v2,
    apply_local_crusher_runtime_v2,
    preflight_local_caddy_topology_activity,
    reconcile_local_caddy_ingress,
    probe_local_caddy_ingress,
    finalize_local_crusher_ingress_success_v2,
    finalize_local_crusher_ingress_failure_v2,
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
                activities=list(TEMPORAL_ACTIVITIES),
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
