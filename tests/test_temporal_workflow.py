from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy.exc import SQLAlchemyError
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import fstack.runtime_submission as runtime_submission_module
import fstack.temporal_worker as temporal_worker_module
from fstack.runtime_submission import OperationNotClaimableError, PreparedRuntimeSubmission
from fstack.temporal_client import start_provision_crusher_workflow
from fstack.temporal_worker import (
    apply_local_crusher,
    finalize_local_crusher_failure,
    finalize_local_crusher_prepare_failure,
    finalize_local_crusher_success,
    prepare_local_crusher,
)
from fstack.temporal_workflow import (
    APPLY_LOCAL_CRUSHER_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_FAILURE_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY,
    PREPARE_LOCAL_CRUSHER_ACTIVITY,
    OperationFailure,
    ProvisionCrusherInput,
    ProvisionCrusherResult,
    ProvisionCrusherWorkflow,
    RuntimeFailure,
    RuntimeSubmission,
)


def _submission(operation_id: UUID, *, state: str = "prepared") -> RuntimeSubmission:
    return RuntimeSubmission(
        submission_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        operation_id=str(operation_id),
        state=state,
        provider="local_docker",
        generation=1,
        submission_key="b" * 64,
        spec_fingerprint="c" * 64,
        api_container_name="fstack-crusher-" + operation_id.hex,
        migration_container_name="fstack-crusher-migrate-" + operation_id.hex,
    )


def _prepared_submission(
    operation_id: UUID, *, state: str = "prepared"
) -> PreparedRuntimeSubmission:
    return PreparedRuntimeSubmission(
        submission_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        operation_id=operation_id,
        state=state,
        provider="local_docker",
        generation=1,
        submission_key="b" * 64,
        spec_fingerprint="c" * 64,
        api_container_name="fstack-crusher-" + operation_id.hex,
        migration_container_name="fstack-crusher-migrate-" + operation_id.hex,
    )


def _assert_sanitized_retryable_activity_error(exc: ApplicationError, marker: str) -> None:
    assert exc.non_retryable is False
    assert exc.type == "ActivityInfrastructureFailure"
    assert exc.message == "activity_infrastructure_failure"
    assert list(exc.details) == ["activity_infrastructure_failure"]
    assert marker not in str(exc)
    assert marker not in repr(list(exc.details))
    assert exc.__cause__ is None
    assert exc.__context__ is None
    assert exc.__suppress_context__ is True


def _assert_no_exception_marker(exc: BaseException, marker: str) -> None:
    seen: set[int] = set()
    pending: list[BaseException | None] = [exc]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        assert marker not in str(current)
        assert marker not in repr(current)
        pending.extend((current.__cause__, current.__context__))


def test_provision_crusher_workflow_prepares_applies_and_finalizes_in_order() -> None:
    operation_id = UUID("11111111-1111-4111-8111-111111111111")
    calls: list[str] = []

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_ACTIVITY)
    async def prepare(request: ProvisionCrusherInput) -> RuntimeSubmission:
        calls.append(f"prepare:{request.operation_id}")
        return _submission(operation_id)

    @activity.defn(name=APPLY_LOCAL_CRUSHER_ACTIVITY)
    async def apply(submission: RuntimeSubmission) -> RuntimeSubmission:
        calls.append(f"apply:{submission.submission_id}")
        return _submission(operation_id, state="submitted")

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY)
    async def finalize(submission: RuntimeSubmission) -> RuntimeSubmission:
        calls.append(f"finalize:{submission.submission_id}")
        return _submission(operation_id, state="confirmed")

    async def run() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-provision-crusher",
                workflows=[ProvisionCrusherWorkflow],
                activities=[prepare, apply, finalize],
            ),
        ):
            result = await environment.client.execute_workflow(
                ProvisionCrusherWorkflow.run,
                ProvisionCrusherInput(operation_id=str(operation_id)),
                id=f"provisioning/{operation_id}",
                task_queue="test-provision-crusher",
            )

        assert result == ProvisionCrusherResult(
            operation_id=str(operation_id),
            state="confirmed",
            runtime_submission=_submission(operation_id, state="confirmed"),
        )
        assert calls == [
            f"prepare:{operation_id}",
            "apply:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "finalize:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        ]

    asyncio.run(run())


def test_prepare_local_crusher_activity_maps_exact_durable_submission(monkeypatch) -> None:
    operation_id = UUID("22222222-2222-4222-8222-222222222222")
    submission_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    prepared = PreparedRuntimeSubmission(
        submission_id=submission_id,
        operation_id=operation_id,
        state="prepared",
        provider="local_docker",
        generation=1,
        submission_key="b" * 64,
        spec_fingerprint="c" * 64,
        api_container_name="fstack-crusher-" + operation_id.hex,
        migration_container_name="fstack-crusher-migrate-" + operation_id.hex,
    )
    calls: list[UUID] = []

    def fake_prepare_operation(requested_operation_id: UUID) -> PreparedRuntimeSubmission:
        calls.append(requested_operation_id)
        return prepared

    monkeypatch.setattr(runtime_submission_module, "prepare_operation", fake_prepare_operation)

    result = prepare_local_crusher(ProvisionCrusherInput(operation_id=str(operation_id)))

    assert calls == [operation_id]
    assert result == _submission(operation_id)


def test_prepare_activity_classifies_missing_operation_as_terminal(monkeypatch) -> None:
    operation_id = UUID("55555555-5555-4555-8555-555555555555")

    def reject_operation(_operation_id: UUID) -> PreparedRuntimeSubmission:
        raise OperationNotClaimableError("operation has no runtime submission")

    monkeypatch.setattr(runtime_submission_module, "prepare_operation", reject_operation)

    with pytest.raises(ApplicationError) as exc_info:
        prepare_local_crusher(ProvisionCrusherInput(operation_id=str(operation_id)))

    assert exc_info.value.non_retryable is True
    assert exc_info.value.type == "OperationNotClaimable"
    assert exc_info.value.message == "prepare_not_claimable"
    assert list(exc_info.value.details) == ["prepare_not_claimable"]


def test_prepare_activity_does_not_retain_not_claimable_exception_context(monkeypatch) -> None:
    marker = "secret-marker://not-claimable?token=raw"

    def reject_operation(_operation_id: UUID) -> PreparedRuntimeSubmission:
        raise OperationNotClaimableError(marker)

    monkeypatch.setattr(runtime_submission_module, "prepare_operation", reject_operation)

    with pytest.raises(ApplicationError) as exc_info:
        prepare_local_crusher(
            ProvisionCrusherInput(operation_id="55555555-5555-4555-8555-555555555555")
        )

    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_no_exception_marker(exc_info.value, marker)


class _HostileIdentifier:
    def __repr__(self) -> str:
        return "secret-marker://hostile-identifier?token=raw"

    __str__ = __repr__


@pytest.mark.parametrize("hostile", [None, [], 7, _HostileIdentifier()])
@pytest.mark.parametrize(
    ("expected_type", "expected_message", "invoke"),
    [
        (
            "InvalidOperationId",
            "operation ID is invalid",
            lambda value: prepare_local_crusher(ProvisionCrusherInput(operation_id=value)),
        ),
        (
            "InvalidRuntimeSubmissionId",
            "runtime submission ID is invalid",
            lambda value: apply_local_crusher(
                RuntimeSubmission(**{**_submission(UUID(int=0)).__dict__, "submission_id": value})
            ),
        ),
        (
            "InvalidRuntimeSubmissionId",
            "runtime submission ID is invalid",
            lambda value: finalize_local_crusher_success(
                RuntimeSubmission(**{**_submission(UUID(int=0)).__dict__, "submission_id": value})
            ),
        ),
        (
            "InvalidRuntimeSubmissionId",
            "runtime submission ID is invalid",
            lambda value: finalize_local_crusher_failure(
                RuntimeFailure(
                    runtime_submission=RuntimeSubmission(
                        **{**_submission(UUID(int=0)).__dict__, "submission_id": value}
                    ),
                    error_code="container_exited",
                )
            ),
        ),
        (
            "InvalidOperationId",
            "operation_id_invalid",
            lambda value: finalize_local_crusher_prepare_failure(
                OperationFailure(operation_id=value, error_code="prepare_not_claimable")
            ),
        ),
    ],
)
def test_runtime_activities_reject_non_string_uuid_payloads_without_retaining_values(
    hostile: Any,
    expected_type: str,
    expected_message: str,
    invoke,
) -> None:
    marker = "secret-marker://hostile-identifier?token=raw"

    with pytest.raises(ApplicationError) as exc_info:
        invoke(hostile)

    assert exc_info.value.non_retryable is True
    assert exc_info.value.type == expected_type
    assert exc_info.value.message == expected_message
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_no_exception_marker(exc_info.value, marker)


class _StringSubclass(str):
    pass


@pytest.mark.parametrize(
    "noncanonical",
    [
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa",
        "{aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa}",
        "urn:uuid:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        " aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa ",
        _StringSubclass("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    ],
)
@pytest.mark.parametrize(
    ("expected_type", "expected_message", "invoke"),
    [
        (
            "InvalidOperationId",
            "operation ID is invalid",
            lambda value: prepare_local_crusher(ProvisionCrusherInput(operation_id=value)),
        ),
        (
            "InvalidRuntimeSubmissionId",
            "runtime submission ID is invalid",
            lambda value: apply_local_crusher(
                RuntimeSubmission(**{**_submission(UUID(int=0)).__dict__, "submission_id": value})
            ),
        ),
        (
            "InvalidRuntimeSubmissionId",
            "runtime submission ID is invalid",
            lambda value: finalize_local_crusher_success(
                RuntimeSubmission(**{**_submission(UUID(int=0)).__dict__, "submission_id": value})
            ),
        ),
        (
            "InvalidRuntimeSubmissionId",
            "runtime submission ID is invalid",
            lambda value: finalize_local_crusher_failure(
                RuntimeFailure(
                    runtime_submission=RuntimeSubmission(
                        **{**_submission(UUID(int=0)).__dict__, "submission_id": value}
                    ),
                    error_code="container_exited",
                )
            ),
        ),
        (
            "InvalidOperationId",
            "operation_id_invalid",
            lambda value: finalize_local_crusher_prepare_failure(
                OperationFailure(operation_id=value, error_code="prepare_not_claimable")
            ),
        ),
    ],
)
def test_runtime_activities_require_canonical_uuid_strings(
    noncanonical: str,
    expected_type: str,
    expected_message: str,
    invoke,
) -> None:
    with pytest.raises(ApplicationError) as exc_info:
        invoke(noncanonical)

    assert exc_info.value.non_retryable is True
    assert exc_info.value.type == expected_type
    assert exc_info.value.message == expected_message
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.parametrize(
    "invalid_operation_id",
    [
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa",
        "{aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa}",
        "urn:uuid:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        " aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa ",
        _StringSubclass("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        None,
        7,
        _HostileIdentifier(),
    ],
)
@pytest.mark.parametrize(
    ("invoke", "dependency_name"),
    [
        (lambda submission: apply_local_crusher(submission), "mark_running"),
        (lambda submission: finalize_local_crusher_success(submission), "finalize_success"),
        (
            lambda submission: finalize_local_crusher_failure(
                RuntimeFailure(runtime_submission=submission, error_code="container_exited")
            ),
            "finalize_failure",
        ),
    ],
)
def test_runtime_submission_boundaries_reject_noncanonical_operation_ids_before_state_access(
    monkeypatch: pytest.MonkeyPatch,
    invalid_operation_id: Any,
    invoke,
    dependency_name: str,
) -> None:
    calls: list[object] = []
    monkeypatch.setattr(
        temporal_worker_module.runtime_state,
        dependency_name,
        lambda *_args, **_kwargs: calls.append(object()),
    )
    submission = RuntimeSubmission(
        **{
            **_submission(UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")).__dict__,
            "operation_id": invalid_operation_id,
        }
    )

    with pytest.raises(ApplicationError) as exc_info:
        invoke(submission)

    assert calls == []
    assert exc_info.value.non_retryable is True
    assert exc_info.value.type == "InvalidOperationId"
    assert exc_info.value.message == "runtime operation ID is invalid"
    assert list(exc_info.value.details) == []
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_no_exception_marker(exc_info.value, "secret-marker://hostile-identifier?token=raw")


@pytest.mark.parametrize(
    ("dependency_name", "invoke"),
    [
        ("mark_running", lambda submission: apply_local_crusher(submission)),
        ("finalize_success", lambda submission: finalize_local_crusher_success(submission)),
        (
            "finalize_failure",
            lambda submission: finalize_local_crusher_failure(
                RuntimeFailure(runtime_submission=submission, error_code="container_exited")
            ),
        ),
    ],
)
def test_runtime_submission_operation_mismatch_from_state_is_bounded_before_provider_work(
    monkeypatch: pytest.MonkeyPatch, dependency_name: str, invoke
) -> None:
    payload_operation_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    transition_calls: list[tuple[object, ...]] = []
    provider_calls: list[object] = []

    def reject_mismatch(*args: object) -> None:
        transition_calls.append(args)
        raise temporal_worker_module.runtime_state.RuntimeStateError(
            "runtime_submission_operation_mismatch"
        )

    monkeypatch.setattr(temporal_worker_module.runtime_state, dependency_name, reject_mismatch)
    monkeypatch.setattr(
        temporal_worker_module.runtime_state,
        "load_plan",
        lambda *_args: provider_calls.append(object()),
    )

    with pytest.raises(ApplicationError) as exc_info:
        invoke(_submission(payload_operation_id))

    expected_args = (
        UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        payload_operation_id,
    )
    if dependency_name == "finalize_failure":
        expected_args += ("container_exited",)
    assert transition_calls == [expected_args]
    assert provider_calls == []
    assert exc_info.value.non_retryable is True
    assert exc_info.value.type == "RuntimeStateConflict"
    assert exc_info.value.message == "runtime_submission_operation_mismatch"
    assert list(exc_info.value.details) == ["runtime_submission_operation_mismatch"]


def test_prepare_activity_sanitizes_unexpected_infrastructure_failure(monkeypatch) -> None:
    marker = "secret-marker://prepare?token=raw"

    def fail_prepare(_operation_id: UUID) -> PreparedRuntimeSubmission:
        raise SQLAlchemyError(marker)

    monkeypatch.setattr(runtime_submission_module, "prepare_operation", fail_prepare)

    with pytest.raises(ApplicationError) as exc_info:
        prepare_local_crusher(
            ProvisionCrusherInput(operation_id="55555555-5555-4555-8555-555555555555")
        )

    _assert_sanitized_retryable_activity_error(exc_info.value, marker)


@pytest.mark.parametrize(
    ("activity_name", "dependency_name", "invoke"),
    [
        ("apply", "mark_running", lambda submission: apply_local_crusher(submission)),
        (
            "success_finalizer",
            "finalize_success",
            lambda submission: finalize_local_crusher_success(submission),
        ),
        (
            "prepare_failure_finalizer",
            "finalize_prepare_failure",
            lambda submission: finalize_local_crusher_prepare_failure(
                OperationFailure(
                    operation_id=submission.operation_id,
                    error_code="prepare_not_claimable",
                )
            ),
        ),
        (
            "failure_finalizer",
            "finalize_failure",
            lambda submission: finalize_local_crusher_failure(
                RuntimeFailure(runtime_submission=submission, error_code="container_exited")
            ),
        ),
    ],
)
def test_runtime_activities_sanitize_unexpected_infrastructure_failures(
    monkeypatch: pytest.MonkeyPatch,
    activity_name: str,
    dependency_name: str,
    invoke,
) -> None:
    marker = f"secret-marker://{activity_name}?token=raw"

    def fail(*_args: object, **_kwargs: object) -> None:
        raise temporal_worker_module.runtime_state.RuntimeStateError(marker)

    monkeypatch.setattr(temporal_worker_module.runtime_state, dependency_name, fail)

    with pytest.raises(ApplicationError) as exc_info:
        invoke(_submission(UUID("99999999-9999-4999-8999-999999999999")))

    _assert_sanitized_retryable_activity_error(exc_info.value, marker)


def test_apply_activity_sanitizes_unexpected_provider_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "secret-marker://provider?token=raw"
    operation_id = UUID("99999999-9999-4999-8999-999999999999")
    monkeypatch.setattr(
        temporal_worker_module.runtime_state,
        "mark_running",
        lambda _submission_id, _operation_id: _prepared_submission(operation_id),
    )
    monkeypatch.setattr(temporal_worker_module.runtime_state, "load_plan", lambda _value: object())

    def fail_provider(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError(marker)

    monkeypatch.setattr(temporal_worker_module, "apply_runtime_plan", fail_provider)

    with pytest.raises(ApplicationError) as exc_info:
        apply_local_crusher(_submission(operation_id))

    _assert_sanitized_retryable_activity_error(exc_info.value, marker)


def test_apply_activity_does_not_retain_known_provider_failure_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "secret-marker://provider-context?token=raw"
    operation_id = UUID("99999999-9999-4999-8999-999999999999")
    monkeypatch.setattr(
        temporal_worker_module.runtime_state,
        "mark_running",
        lambda _submission_id, _operation_id: _prepared_submission(operation_id),
    )
    monkeypatch.setattr(temporal_worker_module.runtime_state, "load_plan", lambda _value: object())

    def fail_provider(*_args: object, **_kwargs: object) -> None:
        try:
            raise RuntimeError(marker)
        except RuntimeError as exc:
            raise temporal_worker_module.RuntimeApplicationError(
                "docker_spec_conflict", retryable=False
            ) from exc

    monkeypatch.setattr(temporal_worker_module, "apply_runtime_plan", fail_provider)

    with pytest.raises(ApplicationError) as exc_info:
        apply_local_crusher(_submission(operation_id))

    assert exc_info.value.type == "RuntimeApplicationFailed"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_no_exception_marker(exc_info.value, marker)


@pytest.mark.parametrize(
    ("dependency_name", "error_code", "invoke"),
    [
        (
            "finalize_success",
            "runtime_submission_not_confirmable",
            lambda submission: finalize_local_crusher_success(submission),
        ),
        (
            "finalize_prepare_failure",
            "failure_code_conflict",
            lambda submission: finalize_local_crusher_prepare_failure(
                OperationFailure(
                    operation_id=submission.operation_id,
                    error_code="prepare_not_claimable",
                )
            ),
        ),
        (
            "finalize_failure",
            "runtime_submission_not_failable",
            lambda submission: finalize_local_crusher_failure(
                RuntimeFailure(runtime_submission=submission, error_code="container_exited")
            ),
        ),
    ],
)
def test_finalizer_state_conflicts_are_bounded_and_nonretryable(
    monkeypatch: pytest.MonkeyPatch,
    dependency_name: str,
    error_code: str,
    invoke,
) -> None:
    def conflict(*_args: object, **_kwargs: object) -> None:
        raise temporal_worker_module.runtime_state.RuntimeStateError(error_code)

    monkeypatch.setattr(temporal_worker_module.runtime_state, dependency_name, conflict)

    with pytest.raises(ApplicationError) as exc_info:
        invoke(_submission(UUID("99999999-9999-4999-8999-999999999999")))

    assert exc_info.value.non_retryable is True
    assert exc_info.value.type == "RuntimeStateConflict"
    assert exc_info.value.message == error_code
    assert list(exc_info.value.details) == [error_code]
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_finalizer_state_conflict_does_not_retain_domain_exception_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "secret-marker://state-context?token=raw"

    def conflict(_submission_id: UUID, _operation_id: UUID) -> None:
        try:
            raise RuntimeError(marker)
        except RuntimeError as exc:
            raise temporal_worker_module.runtime_state.RuntimeStateError(
                "runtime_submission_not_confirmable"
            ) from exc

    monkeypatch.setattr(temporal_worker_module.runtime_state, "finalize_success", conflict)

    with pytest.raises(ApplicationError) as exc_info:
        finalize_local_crusher_success(_submission(UUID("99999999-9999-4999-8999-999999999999")))

    assert exc_info.value.type == "RuntimeStateConflict"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_no_exception_marker(exc_info.value, marker)


@pytest.mark.parametrize(
    "invoke",
    [
        lambda submission: finalize_local_crusher_success(submission),
        lambda submission: finalize_local_crusher_failure(
            RuntimeFailure(runtime_submission=submission, error_code="container_exited")
        ),
    ],
)
def test_finalizers_preserve_invalid_submission_id_as_bounded_nonretryable(invoke) -> None:
    invalid = RuntimeSubmission(
        **{**_submission(UUID(int=0)).__dict__, "submission_id": "not-a-uuid"}
    )

    with pytest.raises(ApplicationError) as exc_info:
        invoke(invalid)

    assert exc_info.value.non_retryable is True
    assert exc_info.value.type == "InvalidRuntimeSubmissionId"
    assert exc_info.value.message == "runtime submission ID is invalid"
    assert list(exc_info.value.details) == []
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_start_provision_crusher_workflow_uses_deterministic_identity() -> None:
    operation_id = UUID("33333333-3333-4333-8333-333333333333")

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_ACTIVITY)
    async def prepare(_request: ProvisionCrusherInput) -> RuntimeSubmission:
        return _submission(operation_id)

    @activity.defn(name=APPLY_LOCAL_CRUSHER_ACTIVITY)
    async def apply(_submission_value: RuntimeSubmission) -> RuntimeSubmission:
        return _submission(operation_id, state="submitted")

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY)
    async def finalize(_submission_value: RuntimeSubmission) -> RuntimeSubmission:
        return _submission(operation_id, state="confirmed")

    async def run() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-temporal-dispatch",
                workflows=[ProvisionCrusherWorkflow],
                activities=[prepare, apply, finalize],
            ),
        ):
            handle = await start_provision_crusher_workflow(
                environment.client,
                operation_id,
                task_queue="test-temporal-dispatch",
            )
            result = await handle.result()

        assert handle.id == f"provisioning/{operation_id}"
        assert result.operation_id == str(operation_id)
        assert result.state == "confirmed"

    asyncio.run(run())


def test_transient_activity_failure_remains_recoverable_after_five_attempts() -> None:
    operation_id = UUID("44444444-4444-4444-8444-444444444444")
    calls = 0

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_ACTIVITY)
    async def eventually_prepare(
        _request: ProvisionCrusherInput,
    ) -> RuntimeSubmission:
        nonlocal calls
        calls += 1
        if calls <= 5:
            raise RuntimeError("temporary database outage")
        return _submission(operation_id)

    @activity.defn(name=APPLY_LOCAL_CRUSHER_ACTIVITY)
    async def apply(_submission_value: RuntimeSubmission) -> RuntimeSubmission:
        return _submission(operation_id, state="submitted")

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY)
    async def finalize(_submission_value: RuntimeSubmission) -> RuntimeSubmission:
        return _submission(operation_id, state="confirmed")

    async def run() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-transient-recovery",
                workflows=[ProvisionCrusherWorkflow],
                activities=[eventually_prepare, apply, finalize],
            ),
        ):
            result = await environment.client.execute_workflow(
                ProvisionCrusherWorkflow.run,
                ProvisionCrusherInput(operation_id=str(operation_id)),
                id=f"provisioning/{operation_id}",
                task_queue="test-transient-recovery",
            )

        assert result.state == "confirmed"
        assert calls == 6

    asyncio.run(run())


def test_nonretryable_apply_failure_is_finalized_once() -> None:
    operation_id = UUID("66666666-6666-4666-8666-666666666666")
    calls: list[str] = []

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_ACTIVITY)
    async def prepare(_request: ProvisionCrusherInput) -> RuntimeSubmission:
        return _submission(operation_id)

    @activity.defn(name=APPLY_LOCAL_CRUSHER_ACTIVITY)
    async def apply(_submission_value: RuntimeSubmission) -> RuntimeSubmission:
        raise ApplicationError(
            "docker_spec_conflict",
            "docker_spec_conflict",
            type="RuntimeApplicationFailed",
            non_retryable=True,
        )

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_FAILURE_ACTIVITY)
    async def finalize_failure(failure: RuntimeFailure) -> RuntimeSubmission:
        calls.append(failure.error_code)
        return _submission(operation_id, state="failed")

    async def run_failure() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-terminal-provider-failure",
                workflows=[ProvisionCrusherWorkflow],
                activities=[prepare, apply, finalize_failure],
            ),
        ):
            result = await environment.client.execute_workflow(
                ProvisionCrusherWorkflow.run,
                ProvisionCrusherInput(operation_id=str(operation_id)),
                id=f"provisioning/{operation_id}",
                task_queue="test-terminal-provider-failure",
            )
        assert result.state == "failed"
        assert calls == ["docker_spec_conflict"]

    asyncio.run(run_failure())


def test_nonretryable_prepare_failure_routes_operation_to_prepare_finalizer_once() -> None:
    operation_id = UUID("77777777-7777-4777-8777-777777777777")
    calls: list[dict[str, str]] = []

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_ACTIVITY)
    async def prepare(_request: ProvisionCrusherInput) -> RuntimeSubmission:
        raise ApplicationError(
            "prepare_not_claimable",
            "prepare_not_claimable",
            type="OperationNotClaimable",
            non_retryable=True,
        )

    @activity.defn(name="fstack.finalize_local_crusher_prepare_failure.v1")
    async def finalize_prepare_failure(failure: dict[str, str]) -> RuntimeSubmission:
        calls.append(failure)
        return _submission(operation_id, state="failed")

    async def run_failure() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-terminal-prepare-failure",
                workflows=[ProvisionCrusherWorkflow],
                activities=[prepare, finalize_prepare_failure],
            ),
        ):
            result = await environment.client.execute_workflow(
                ProvisionCrusherWorkflow.run,
                ProvisionCrusherInput(operation_id=str(operation_id)),
                id=f"provisioning/{operation_id}",
                task_queue="test-terminal-prepare-failure",
            )
        assert result.state == "failed"
        assert calls == [
            {
                "operation_id": str(operation_id),
                "error_code": "prepare_not_claimable",
            }
        ]

    asyncio.run(run_failure())


def test_prepare_failure_activity_finalizes_by_operation_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_id = UUID("88888888-8888-4888-8888-888888888888")
    expected = PreparedRuntimeSubmission(
        submission_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        operation_id=operation_id,
        state="failed",
        provider="local_docker",
        generation=1,
        submission_key="b" * 64,
        spec_fingerprint="c" * 64,
        api_container_name="fstack-crusher-" + operation_id.hex,
        migration_container_name="fstack-crusher-migrate-" + operation_id.hex,
    )
    calls: list[tuple[UUID | str, str]] = []

    def finalize(requested_operation_id: UUID | str, error_code: str) -> PreparedRuntimeSubmission:
        calls.append((requested_operation_id, error_code))
        return expected

    monkeypatch.setattr(temporal_worker_module.runtime_state, "finalize_prepare_failure", finalize)
    activity_fn = temporal_worker_module.finalize_local_crusher_prepare_failure

    result = activity_fn(
        OperationFailure(
            operation_id=str(operation_id),
            error_code="prepare_not_claimable",
        )
    )

    assert result == _submission(operation_id, state="failed")
    assert calls == [(operation_id, "prepare_not_claimable")]
