from __future__ import annotations

import asyncio
import base64
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest
from sqlalchemy.exc import SQLAlchemyError
from temporalio import activity, workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowFailureError, WorkflowHistory
from temporalio.common import RetryPolicy
from temporalio.converter import DataConverter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

import fstack.runtime_submission as runtime_submission_module
import fstack.temporal_worker as temporal_worker_module
import fstack.temporal_workflow as temporal_workflow_module
from fstack.docker_cli import APPLY_DOCKER_TIMEOUT_BUDGET
from fstack.runtime_submission import OperationNotClaimableError, PreparedRuntimeSubmission
from fstack.temporal_client import WORKFLOW_TYPE_V1, start_provision_crusher_workflow
from fstack.temporal_worker import (
    apply_local_crusher,
    apply_local_crusher_runtime_v2,
    finalize_local_crusher_failure,
    finalize_local_crusher_ingress_failure_v2,
    finalize_local_crusher_ingress_success_v2,
    finalize_local_crusher_prepare_failure,
    finalize_local_crusher_success,
    preflight_local_caddy_topology_activity,
    prepare_local_crusher,
    prepare_local_crusher_ingress_v2,
    probe_local_caddy_ingress,
    reconcile_local_caddy_ingress,
)
from fstack.temporal_workflow import (
    APPLY_LOCAL_CRUSHER_ACTIVITY,
    APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_FAILURE_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY,
    FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY,
    FINALIZE_V2_ACTIVITY_START_TO_CLOSE_TIMEOUT,
    PREFLIGHT_ACTIVITY_START_TO_CLOSE_TIMEOUT,
    PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY,
    PREPARE_LOCAL_CRUSHER_ACTIVITY,
    PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY,
    PREPARE_V2_ACTIVITY_START_TO_CLOSE_TIMEOUT,
    PROBE_ACTIVITY_START_TO_CLOSE_TIMEOUT,
    PROBE_LOCAL_CADDY_INGRESS_ACTIVITY,
    RECONCILE_ACTIVITY_START_TO_CLOSE_TIMEOUT,
    RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY,
    OperationFailure,
    ProvisionCrusherInput,
    ProvisionCrusherResult,
    ProvisionCrusherV2Input,
    ProvisionCrusherV2Result,
    ProvisionCrusherV2Workflow,
    ProvisionCrusherWorkflow,
    ProvisioningFailureV2,
    ProvisioningSubmissionIds,
    RuntimeFailure,
    RuntimeSubmission,
)


@workflow.defn(name="test.fstack.task12.sanitized_activity_history")
class _SanitizedActivityHistoryWorkflow:
    @workflow.run
    async def run(self, ids: ProvisioningSubmissionIds) -> str:
        try:
            await workflow.execute_activity(
                PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY,
                ids,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        except ActivityError:
            return "failed"
        return "unexpected-success"


def _history_fixture_content(value: object) -> str:
    observed: list[str] = []

    def inspect(nested: object) -> None:
        if isinstance(nested, dict):
            for key, item in nested.items():
                observed.append(str(key))
                if key == "data" and isinstance(item, str):
                    observed.append(base64.b64decode(item, validate=True).decode(errors="replace"))
                inspect(item)
        elif isinstance(nested, list):
            for item in nested:
                inspect(item)
        elif isinstance(nested, str):
            observed.append(nested)

    inspect(value)
    return "\n".join(observed)


def test_v2_activity_contract_names_and_payloads_are_sanitized() -> None:
    assert PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY == "fstack.prepare_local_crusher_ingress.v2"
    assert APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY == "fstack.apply_local_crusher_runtime.v2"
    assert PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY == "fstack.preflight_local_caddy_topology.v1"
    assert RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY == "fstack.reconcile_local_caddy_ingress.v1"
    assert PROBE_LOCAL_CADDY_INGRESS_ACTIVITY == "fstack.probe_local_caddy_ingress.v1"
    assert (
        FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY
        == "fstack.finalize_local_crusher_ingress_success.v2"
    )
    assert (
        FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY
        == "fstack.finalize_local_crusher_ingress_failure.v2"
    )
    assert set(ProvisionCrusherV2Input.__dataclass_fields__) == {"operation_id"}
    assert set(ProvisioningSubmissionIds.__dataclass_fields__) == {
        "operation_id",
        "runtime_submission_id",
        "ingress_submission_id",
    }
    assert set(ProvisioningFailureV2.__dataclass_fields__) == {
        "operation_id",
        "scope",
        "error_code",
    }
    assert set(ProvisionCrusherV2Result.__dataclass_fields__) == {"operation_id", "state"}


def test_v2_workflow_and_worker_share_the_same_closed_failure_codes() -> None:
    assert temporal_workflow_module._V2_FAILURE_CODES == (
        temporal_worker_module._PROVISIONING_FAILURE_CODES
    )


def test_prepare_v2_returns_only_durable_submission_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    operation_id = UUID("11111111-1111-4111-8111-111111111111")
    runtime_submission_id = UUID("22222222-2222-4222-8222-222222222222")
    ingress_submission_id = UUID("33333333-3333-4333-8333-333333333333")
    prepared = temporal_worker_module.ingress_submission.PreparedProvisioningSubmissions(
        operation_id=operation_id,
        runtime_submission_id=runtime_submission_id,
        ingress_submission_id=ingress_submission_id,
    )
    monkeypatch.setattr(
        temporal_worker_module.ingress_submission,
        "prepare_operation_bundle",
        lambda value: prepared if value == operation_id else pytest.fail("wrong operation"),
    )

    result = prepare_local_crusher_ingress_v2(
        ProvisionCrusherV2Input(operation_id=str(operation_id))
    )

    assert result == ProvisioningSubmissionIds(
        operation_id=str(operation_id),
        runtime_submission_id=str(runtime_submission_id),
        ingress_submission_id=str(ingress_submission_id),
    )
    assert set(result.__dict__) == {
        "operation_id",
        "runtime_submission_id",
        "ingress_submission_id",
    }


@pytest.mark.parametrize("exact_bundle", [False, True])
def test_prepare_v2_only_makes_conflict_permanent_for_exact_durable_bundle(
    monkeypatch: pytest.MonkeyPatch,
    exact_bundle: bool,
) -> None:
    calls: list[str] = []

    def conflict(_operation_id: UUID) -> object:
        calls.append("prepare")
        raise temporal_worker_module.ingress_submission.ProvisioningSubmissionConflictError(
            "runtime_submission_conflict"
        )

    monkeypatch.setattr(
        temporal_worker_module.ingress_submission, "prepare_operation_bundle", conflict
    )
    monkeypatch.setattr(
        temporal_worker_module.ingress_submission,
        "resolve_exact_operation_bundle",
        lambda _operation_id: (
            temporal_worker_module.ingress_submission.PreparedProvisioningSubmissions(
                operation_id=UUID(_v2_ids().operation_id),
                runtime_submission_id=UUID(_v2_ids().runtime_submission_id),
                ingress_submission_id=UUID(_v2_ids().ingress_submission_id),
            )
            if exact_bundle
            else None
        ),
        raising=False,
    )
    monkeypatch.setattr(
        temporal_worker_module.ingress_state,
        "finalize_provisioning_failure",
        lambda *_args: calls.append("finalize"),
    )

    with pytest.raises(ApplicationError) as caught:
        prepare_local_crusher_ingress_v2(
            ProvisionCrusherV2Input(operation_id=_v2_ids().operation_id)
        )

    assert calls == ["prepare"]
    assert caught.value.non_retryable is exact_bundle
    expected_code = (
        "runtime_submission_conflict" if exact_bundle else "activity_infrastructure_failure"
    )
    assert caught.value.message == expected_code
    assert list(caught.value.details) == [expected_code]


def test_prepare_v2_pre_durable_infrastructure_failure_is_retryable_without_finalizing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "secret-marker://postgres/path/provider"
    calls: list[str] = []

    def fail(_operation_id: UUID) -> object:
        calls.append("prepare")
        raise RuntimeError(marker)

    monkeypatch.setattr(temporal_worker_module.ingress_submission, "prepare_operation_bundle", fail)
    monkeypatch.setattr(
        temporal_worker_module.ingress_state,
        "finalize_provisioning_failure",
        lambda *_args: calls.append("finalize"),
    )

    with pytest.raises(ApplicationError) as caught:
        prepare_local_crusher_ingress_v2(
            ProvisionCrusherV2Input(operation_id=_v2_ids().operation_id)
        )

    assert calls == ["prepare"]
    _assert_sanitized_retryable_activity_error(caught.value, marker)


def _v2_ids() -> ProvisioningSubmissionIds:
    return ProvisioningSubmissionIds(
        operation_id="11111111-1111-4111-8111-111111111111",
        runtime_submission_id="22222222-2222-4222-8222-222222222222",
        ingress_submission_id="33333333-3333-4333-8333-333333333333",
    )


def test_apply_runtime_v2_reuses_reconciliation_and_returns_no_provider_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = _v2_ids()
    calls: list[object] = []
    plan = object()
    monkeypatch.setattr(
        temporal_worker_module.runtime_state,
        "mark_running",
        lambda submission_id, operation_id: calls.append((submission_id, operation_id)),
    )
    monkeypatch.setattr(
        temporal_worker_module.runtime_state,
        "load_plan",
        lambda submission_id: calls.append(submission_id) or plan,
    )
    monkeypatch.setattr(
        temporal_worker_module,
        "apply_runtime_plan",
        lambda value, _docker, *, persist_submitted: (
            calls.append(value),
            persist_submitted("provider-marker-must-not-return"),
        ),
    )
    monkeypatch.setattr(
        temporal_worker_module.runtime_state,
        "persist_submitted",
        lambda submission_id, external_id: calls.append((submission_id, external_id)),
    )

    assert apply_local_crusher_runtime_v2(ids) is None
    assert calls == [
        (UUID(ids.runtime_submission_id), UUID(ids.operation_id)),
        UUID(ids.runtime_submission_id),
        plan,
        (UUID(ids.runtime_submission_id), "provider-marker-must-not-return"),
    ]


def test_preflight_v2_passes_only_ids_and_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    ids = _v2_ids()
    calls: list[object] = []
    monkeypatch.setattr(
        temporal_worker_module.caddy_topology,
        "preflight_local_caddy_topology",
        lambda value: calls.append(value),
    )

    assert preflight_local_caddy_topology_activity(ids) is None
    assert calls == [
        temporal_worker_module.caddy_topology.ProvisioningSubmissionIds(**ids.__dict__)
    ]


@pytest.mark.parametrize(
    ("code", "retryable"),
    [("caddy_topology_invalid", False), ("caddy_unavailable", True)],
)
def test_preflight_v2_maps_only_sanitized_stable_code(
    monkeypatch: pytest.MonkeyPatch, code: str, retryable: bool
) -> None:
    marker = "secret-marker://docker-inspection/address/alias"

    def fail(_ids: object) -> None:
        try:
            raise RuntimeError(marker)
        except RuntimeError as exc:
            raise temporal_worker_module.caddy_topology.CaddyTopologyError(code, retryable) from exc

    monkeypatch.setattr(
        temporal_worker_module.caddy_topology,
        "preflight_local_caddy_topology",
        fail,
    )

    with pytest.raises(ApplicationError) as caught:
        preflight_local_caddy_topology_activity(_v2_ids())

    assert caught.value.message == code
    assert list(caught.value.details) == [code]
    assert caught.value.non_retryable is not retryable
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    _assert_no_exception_marker(caught.value, marker)


def test_preflight_v2_failure_keeps_markers_out_of_logs_and_temporal_history(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    marker = "secret-marker://container/network/endpoint/address/alias/inspection"

    def fail(_ids: object) -> None:
        try:
            raise RuntimeError(marker)
        except RuntimeError as exc:
            raise temporal_worker_module.caddy_topology.CaddyTopologyError(
                "caddy_topology_invalid", False
            ) from exc

    monkeypatch.setattr(
        temporal_worker_module.caddy_topology,
        "preflight_local_caddy_topology",
        fail,
    )

    async def run() -> str:
        environment = await WorkflowEnvironment.start_time_skipping()
        with ThreadPoolExecutor(max_workers=1) as executor:
            async with (
                environment,
                Worker(
                    environment.client,
                    task_queue="test-task12-sanitized-history",
                    workflows=[_SanitizedActivityHistoryWorkflow],
                    activities=[preflight_local_caddy_topology_activity],
                    activity_executor=executor,
                ),
            ):
                handle = await environment.client.start_workflow(
                    _SanitizedActivityHistoryWorkflow.run,
                    _v2_ids(),
                    id="test-task12-sanitized-history",
                    task_queue="test-task12-sanitized-history",
                )
                result = await handle.result()
                history = await handle.fetch_history()
        assert marker not in history.to_json()
        assert base64.b64encode(marker.encode()).decode() not in history.to_json()
        return result

    assert asyncio.run(run()) == "failed"
    assert marker not in caplog.text


def test_reconcile_v2_loads_route_internally_and_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = _v2_ids()
    desired = temporal_worker_module.caddy_topology.DesiredCaddyTopology(
        deployment_id=UUID("44444444-4444-4444-8444-444444444444"),
        crusher_container_name="fstack-crusher-44444444444444448444444444444444",
        public_domain="crusher.example.test",
        docker_network="fstack-local",
    )
    calls: list[object] = []
    monkeypatch.setattr(
        temporal_worker_module.caddy_topology,
        "_load_desired_state",
        lambda value: calls.append(value) or desired,
    )
    config = object()
    transport = object()
    monkeypatch.setattr(
        temporal_worker_module.caddy_admin.CaddyAdminConfig,
        "from_environ",
        lambda: config,
    )
    monkeypatch.setattr(
        temporal_worker_module.caddy_admin,
        "CaddyAdminTransport",
        lambda value: calls.append(value) or transport,
    )

    class Admin:
        def __init__(self, value: object) -> None:
            calls.append(value)

        def reconcile_submission(
            self, route: object, ingress_submission_id: UUID, operation_id: UUID
        ) -> None:
            calls.append((route, ingress_submission_id, operation_id))

    monkeypatch.setattr(temporal_worker_module.caddy_admin, "CaddyAdmin", Admin)

    assert reconcile_local_caddy_ingress(ids) is None
    assert calls == [
        temporal_worker_module.caddy_topology.ProvisioningSubmissionIds(**ids.__dict__),
        config,
        transport,
        (
            temporal_worker_module.ingress_submission._canonical_route(
                desired.deployment_id, desired.public_domain
            ),
            UUID(ids.ingress_submission_id),
            UUID(ids.operation_id),
        ),
    ]


def test_reconcile_v2_maps_unknown_ingress_state_code_to_sanitized_retryable_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "secret-marker://ingress-state/provider/path"
    desired = temporal_worker_module.caddy_topology.DesiredCaddyTopology(
        deployment_id=UUID("44444444-4444-4444-8444-444444444444"),
        crusher_container_name="fstack-crusher-44444444444444448444444444444444",
        public_domain="crusher.example.test",
        docker_network="fstack-local",
    )
    monkeypatch.setattr(
        temporal_worker_module.caddy_topology,
        "_load_desired_state",
        lambda _ids: desired,
    )
    monkeypatch.setattr(
        temporal_worker_module.caddy_admin.CaddyAdminConfig,
        "from_environ",
        lambda: object(),
    )
    monkeypatch.setattr(
        temporal_worker_module.caddy_admin,
        "CaddyAdminTransport",
        lambda _config: object(),
    )

    class Admin:
        def __init__(self, _transport: object) -> None:
            pass

        def reconcile_submission(self, *_args: object) -> None:
            raise temporal_worker_module.ingress_state.IngressStateError(marker)

    monkeypatch.setattr(temporal_worker_module.caddy_admin, "CaddyAdmin", Admin)

    with pytest.raises(ApplicationError) as caught:
        reconcile_local_caddy_ingress(_v2_ids())

    _assert_sanitized_retryable_activity_error(caught.value, marker)
    _assert_no_traceback_local_marker(caught.value, marker)


def test_probe_v2_loads_host_internally_and_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    ids = _v2_ids()
    calls: list[object] = []
    monkeypatch.setattr(
        temporal_worker_module.ingress_probe,
        "probe_ingress",
        lambda value: calls.append(value),
    )

    assert probe_local_caddy_ingress(ids) is None
    assert calls == [
        temporal_worker_module.caddy_topology.ProvisioningSubmissionIds(**ids.__dict__)
    ]


def test_success_finalizer_v2_returns_only_operation_and_confirmed_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = _v2_ids()
    calls: list[tuple[UUID, UUID, UUID]] = []
    monkeypatch.setattr(
        temporal_worker_module.ingress_state,
        "finalize_provisioning_success",
        lambda runtime_id, ingress_id, operation_id: calls.append(
            (runtime_id, ingress_id, operation_id)
        ),
    )

    result = finalize_local_crusher_ingress_success_v2(ids)

    assert result == ProvisionCrusherV2Result(operation_id=ids.operation_id, state="confirmed")
    assert set(result.__dict__) == {"operation_id", "state"}
    assert calls == [
        (
            UUID(ids.runtime_submission_id),
            UUID(ids.ingress_submission_id),
            UUID(ids.operation_id),
        )
    ]


def test_failure_finalizer_v2_resolves_submissions_from_operation_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_id = "11111111-1111-4111-8111-111111111111"
    calls: list[tuple[UUID, str, str]] = []
    monkeypatch.setattr(
        temporal_worker_module.ingress_state,
        "finalize_provisioning_failure",
        lambda operation, scope, code: calls.append((operation, scope, code)),
    )
    failure = ProvisioningFailureV2(
        operation_id=operation_id,
        scope="ingress",
        error_code="caddy_route_id_conflict",
    )

    result = finalize_local_crusher_ingress_failure_v2(failure)

    assert result == ProvisionCrusherV2Result(operation_id=operation_id, state="failed")
    assert set(failure.__dict__) == {"operation_id", "scope", "error_code"}
    assert calls == [(UUID(operation_id), "ingress", "caddy_route_id_conflict")]


@pytest.mark.parametrize(
    "invoke",
    [
        lambda: prepare_local_crusher_ingress_v2(
            ProvisionCrusherV2Input(operation_id="11111111-1111-4111-8111-111111111111 ")
        ),
        lambda: apply_local_crusher_runtime_v2(
            ProvisioningSubmissionIds(
                **{
                    **_v2_ids().__dict__,
                    "runtime_submission_id": "22222222222242228222222222222222",
                }
            )
        ),
        lambda: preflight_local_caddy_topology_activity(
            ProvisioningSubmissionIds(
                **{
                    **_v2_ids().__dict__,
                    "ingress_submission_id": "33333333-3333-4333-8333-333333333333 ",
                }
            )
        ),
        lambda: reconcile_local_caddy_ingress(
            ProvisioningSubmissionIds(
                **{**_v2_ids().__dict__, "operation_id": "11111111-1111-4111-8111-111111111111 "}
            )
        ),
        lambda: probe_local_caddy_ingress(
            ProvisioningSubmissionIds(
                **{
                    **_v2_ids().__dict__,
                    "runtime_submission_id": "22222222-2222-4222-8222-222222222222 ",
                }
            )
        ),
        lambda: finalize_local_crusher_ingress_success_v2(
            ProvisioningSubmissionIds(
                **{
                    **_v2_ids().__dict__,
                    "ingress_submission_id": "33333333333343338333333333333333",
                }
            )
        ),
        lambda: finalize_local_crusher_ingress_failure_v2(
            ProvisioningFailureV2(
                operation_id="11111111-1111-4111-8111-111111111111 ",
                scope="runtime",
                error_code="docker_spec_conflict",
            )
        ),
    ],
)
def test_v2_activities_reject_noncanonical_uuid_payloads_before_dependencies(invoke) -> None:
    with pytest.raises(ApplicationError) as caught:
        invoke()

    assert caught.value.non_retryable is True
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.parametrize(
    "invoke",
    [
        lambda marker: prepare_local_crusher_ingress_v2(
            ProvisionCrusherV2Input(operation_id=marker)
        ),
        lambda marker: apply_local_crusher_runtime_v2(
            ProvisioningSubmissionIds(
                operation_id=marker,
                runtime_submission_id=_v2_ids().runtime_submission_id,
                ingress_submission_id=_v2_ids().ingress_submission_id,
            )
        ),
        lambda marker: preflight_local_caddy_topology_activity(
            ProvisioningSubmissionIds(
                operation_id=marker,
                runtime_submission_id=_v2_ids().runtime_submission_id,
                ingress_submission_id=_v2_ids().ingress_submission_id,
            )
        ),
        lambda marker: reconcile_local_caddy_ingress(
            ProvisioningSubmissionIds(
                operation_id=marker,
                runtime_submission_id=_v2_ids().runtime_submission_id,
                ingress_submission_id=_v2_ids().ingress_submission_id,
            )
        ),
        lambda marker: probe_local_caddy_ingress(
            ProvisioningSubmissionIds(
                operation_id=marker,
                runtime_submission_id=_v2_ids().runtime_submission_id,
                ingress_submission_id=_v2_ids().ingress_submission_id,
            )
        ),
        lambda marker: finalize_local_crusher_ingress_success_v2(
            ProvisioningSubmissionIds(
                operation_id=marker,
                runtime_submission_id=_v2_ids().runtime_submission_id,
                ingress_submission_id=_v2_ids().ingress_submission_id,
            )
        ),
        lambda marker: finalize_local_crusher_ingress_failure_v2(
            ProvisioningFailureV2(
                operation_id=marker,
                scope="runtime",
                error_code="docker_spec_conflict",
            )
        ),
    ],
)
def test_v2_invalid_uuid_payloads_are_absent_from_worker_traceback_locals(invoke) -> None:
    marker = "secret-marker://provider/token"

    with pytest.raises(ApplicationError) as caught:
        invoke(marker)

    _assert_no_exception_marker(caught.value, marker)
    _assert_no_traceback_local_marker(caught.value, marker)


@pytest.mark.parametrize(
    ("dependency", "invoke"),
    [
        (
            lambda monkeypatch, fail: monkeypatch.setattr(
                temporal_worker_module.runtime_state, "mark_running", fail
            ),
            lambda: apply_local_crusher_runtime_v2(_v2_ids()),
        ),
        (
            lambda monkeypatch, fail: monkeypatch.setattr(
                temporal_worker_module.ingress_state, "finalize_provisioning_success", fail
            ),
            lambda: finalize_local_crusher_ingress_success_v2(_v2_ids()),
        ),
        (
            lambda monkeypatch, fail: monkeypatch.setattr(
                temporal_worker_module.ingress_state, "finalize_provisioning_failure", fail
            ),
            lambda: finalize_local_crusher_ingress_failure_v2(
                ProvisioningFailureV2(
                    operation_id=_v2_ids().operation_id,
                    scope="runtime",
                    error_code="docker_spec_conflict",
                )
            ),
        ),
    ],
)
def test_v2_runtime_and_finalizers_sanitize_unexpected_infrastructure_failure(
    monkeypatch: pytest.MonkeyPatch, dependency, invoke
) -> None:
    marker = "secret-marker://provider/network/path/fingerprint"

    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError(marker)

    dependency(monkeypatch, fail)

    with pytest.raises(ApplicationError) as caught:
        invoke()

    _assert_sanitized_retryable_activity_error(caught.value, marker)


@pytest.mark.parametrize(
    ("dependency", "invoke", "code"),
    [
        (
            "finalize_provisioning_success",
            lambda: finalize_local_crusher_ingress_success_v2(_v2_ids()),
            "provisioning_success_state_invalid",
        ),
        (
            "finalize_provisioning_failure",
            lambda: finalize_local_crusher_ingress_failure_v2(
                ProvisioningFailureV2(
                    operation_id=_v2_ids().operation_id,
                    scope="ingress",
                    error_code="caddy_route_id_conflict",
                )
            ),
            "provisioning_failure_conflict",
        ),
    ],
)
def test_v2_finalizer_state_conflicts_are_sanitized_nonretryable(
    monkeypatch: pytest.MonkeyPatch, dependency: str, invoke, code: str
) -> None:
    marker = "secret-marker://state/provider/path"

    def conflict(*_args: object) -> None:
        try:
            raise RuntimeError(marker)
        except RuntimeError as exc:
            raise temporal_worker_module.ingress_state.IngressStateError(code) from exc

    monkeypatch.setattr(temporal_worker_module.ingress_state, dependency, conflict)

    with pytest.raises(ApplicationError) as caught:
        invoke()

    assert caught.value.non_retryable is True
    assert caught.value.message == code
    assert list(caught.value.details) == [code]
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    _assert_no_exception_marker(caught.value, marker)


def test_v2_failure_finalizer_maps_state_resolution_to_retryable_infrastructure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        temporal_worker_module.ingress_state,
        "finalize_provisioning_failure",
        lambda *_args: (_ for _ in ()).throw(
            temporal_worker_module.ingress_state.IngressStateError(
                "provisioning_failure_state_resolution"
            )
        ),
    )

    with pytest.raises(ApplicationError) as caught:
        finalize_local_crusher_ingress_failure_v2(
            ProvisioningFailureV2(
                operation_id=_v2_ids().operation_id,
                scope="runtime",
                error_code="docker_spec_conflict",
            )
        )

    _assert_sanitized_retryable_activity_error(
        caught.value, "provisioning_failure_state_resolution"
    )


@pytest.mark.parametrize(
    ("dependency", "invoke"),
    [
        (
            "finalize_provisioning_success",
            lambda: finalize_local_crusher_ingress_success_v2(_v2_ids()),
        ),
        (
            "finalize_provisioning_failure",
            lambda: finalize_local_crusher_ingress_failure_v2(
                ProvisioningFailureV2(
                    operation_id=_v2_ids().operation_id,
                    scope="runtime",
                    error_code="docker_spec_conflict",
                )
            ),
        ),
    ],
)
def test_v2_finalizers_map_unknown_state_codes_to_sanitized_retryable_infrastructure(
    monkeypatch: pytest.MonkeyPatch, dependency: str, invoke
) -> None:
    marker = "secret-marker://provider/endpoint/path"
    monkeypatch.setattr(
        temporal_worker_module.ingress_state,
        dependency,
        lambda *_args: (_ for _ in ()).throw(
            temporal_worker_module.ingress_state.IngressStateError(marker)
        ),
    )

    with pytest.raises(ApplicationError) as caught:
        invoke()

    _assert_sanitized_retryable_activity_error(caught.value, marker)


def test_reconcile_v2_rejects_foreign_caddy_error_codes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "secret-marker://foreign-caddy/provider/endpoint"

    class ForeignCode(StrEnum):
        MARKER = marker

    desired = temporal_worker_module.caddy_topology.DesiredCaddyTopology(
        deployment_id=UUID("44444444-4444-4444-8444-444444444444"),
        crusher_container_name="fstack-crusher-44444444444444448444444444444444",
        public_domain="crusher.example.test",
        docker_network="fstack-local",
    )
    monkeypatch.setattr(
        temporal_worker_module.caddy_topology,
        "_load_desired_state",
        lambda _ids: desired,
    )
    monkeypatch.setattr(
        temporal_worker_module.caddy_admin.CaddyAdminConfig,
        "from_environ",
        lambda: object(),
    )
    monkeypatch.setattr(
        temporal_worker_module.caddy_admin,
        "CaddyAdminTransport",
        lambda _config: object(),
    )

    class Admin:
        def __init__(self, _transport: object) -> None:
            pass

        def reconcile_submission(self, *_args: object) -> None:
            raise temporal_worker_module.caddy_admin.CaddyAdminError(
                cast(temporal_worker_module.caddy_admin.CaddyAdminErrorCode, ForeignCode.MARKER)
            )

    monkeypatch.setattr(temporal_worker_module.caddy_admin, "CaddyAdmin", Admin)

    with pytest.raises(ApplicationError) as caught:
        reconcile_local_caddy_ingress(_v2_ids())

    assert caught.value.message == "caddy_unavailable"
    assert caught.value.non_retryable is False
    _assert_no_exception_marker(caught.value, marker)
    _assert_no_traceback_local_marker(caught.value, marker)


def test_failure_finalizer_v2_bounds_error_code_and_discards_payload_locals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "secret_provider_token_123"
    calls: list[str] = []

    def fail(_operation_id: UUID, _scope: str, error_code: str) -> None:
        calls.append(error_code)
        raise RuntimeError("provider failed")

    monkeypatch.setattr(
        temporal_worker_module.ingress_state,
        "finalize_provisioning_failure",
        fail,
    )

    with pytest.raises(ApplicationError) as caught:
        finalize_local_crusher_ingress_failure_v2(
            ProvisioningFailureV2(
                operation_id=_v2_ids().operation_id,
                scope="runtime",
                error_code=marker,
            )
        )

    assert calls == ["provisioning_failed"]
    _assert_sanitized_retryable_activity_error(caught.value, marker)
    _assert_no_traceback_local_marker(caught.value, marker)


@pytest.mark.parametrize(
    "error_code",
    ["secret_source_not_configured", "provider_identity_conflict"],
)
def test_failure_finalizer_v2_preserves_known_runtime_state_codes(
    monkeypatch: pytest.MonkeyPatch,
    error_code: str,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        temporal_worker_module.ingress_state,
        "finalize_provisioning_failure",
        lambda _operation_id, _scope, received: calls.append(received),
    )

    result = finalize_local_crusher_ingress_failure_v2(
        ProvisioningFailureV2(
            operation_id=_v2_ids().operation_id,
            scope="runtime",
            error_code=error_code,
        )
    )

    assert calls == [error_code]
    assert result.state == "failed"


@pytest.mark.parametrize(
    ("dependency", "invoke", "exception_factory"),
    [
        (
            lambda monkeypatch, fail: monkeypatch.setattr(
                temporal_worker_module.ingress_submission, "prepare_operation_bundle", fail
            ),
            lambda: prepare_local_crusher_ingress_v2(
                ProvisionCrusherV2Input(operation_id=_v2_ids().operation_id)
            ),
            lambda marker: (
                temporal_worker_module.ingress_submission.ProvisioningSubmissionConflictError(
                    marker
                )
            ),
        ),
        (
            lambda monkeypatch, fail: monkeypatch.setattr(
                temporal_worker_module.runtime_state, "mark_running", fail
            ),
            lambda: apply_local_crusher_runtime_v2(_v2_ids()),
            lambda marker: temporal_worker_module.runtime_state.RuntimeStateError(marker),
        ),
        (
            lambda monkeypatch, fail: monkeypatch.setattr(
                temporal_worker_module.caddy_topology, "preflight_local_caddy_topology", fail
            ),
            lambda: preflight_local_caddy_topology_activity(_v2_ids()),
            lambda marker: temporal_worker_module.caddy_topology.CaddyTopologyError(marker, False),
        ),
        (
            lambda monkeypatch, fail: monkeypatch.setattr(
                temporal_worker_module.caddy_topology, "_load_desired_state", fail
            ),
            lambda: reconcile_local_caddy_ingress(_v2_ids()),
            lambda marker: temporal_worker_module.caddy_topology.CaddyTopologyError(marker, False),
        ),
        (
            lambda monkeypatch, fail: monkeypatch.setattr(
                temporal_worker_module.ingress_probe, "probe_ingress", fail
            ),
            lambda: probe_local_caddy_ingress(_v2_ids()),
            lambda marker: temporal_worker_module.ingress_probe.IngressProbeError(marker, False),
        ),
        (
            lambda monkeypatch, fail: monkeypatch.setattr(
                temporal_worker_module.ingress_state, "finalize_provisioning_success", fail
            ),
            lambda: finalize_local_crusher_ingress_success_v2(_v2_ids()),
            lambda marker: temporal_worker_module.ingress_state.IngressStateError(marker),
        ),
        (
            lambda monkeypatch, fail: monkeypatch.setattr(
                temporal_worker_module.ingress_state, "finalize_provisioning_failure", fail
            ),
            lambda: finalize_local_crusher_ingress_failure_v2(
                ProvisioningFailureV2(
                    operation_id=_v2_ids().operation_id,
                    scope="runtime",
                    error_code="docker_spec_conflict",
                )
            ),
            lambda marker: temporal_worker_module.ingress_state.IngressStateError(marker),
        ),
    ],
)
def test_v2_unknown_domain_codes_become_generic_retryable_failures(
    monkeypatch: pytest.MonkeyPatch, dependency, invoke, exception_factory
) -> None:
    marker = "secret-marker://unknown/provider/endpoint"

    def fail(*_args: object, **_kwargs: object) -> None:
        raise exception_factory(marker)

    dependency(monkeypatch, fail)

    with pytest.raises(ApplicationError) as caught:
        invoke()

    assert caught.value.non_retryable is False
    assert marker not in str(caught.value)
    assert marker not in repr(list(caught.value.details))
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    _assert_no_traceback_local_marker(caught.value, marker)


@pytest.mark.parametrize(
    ("setup", "invoke", "expected_type", "expected_code"),
    [
        (
            lambda monkeypatch, marker: monkeypatch.setattr(
                temporal_worker_module.ingress_submission,
                "prepare_operation_bundle",
                lambda *_args: (_ for _ in ()).throw(
                    temporal_worker_module.ingress_submission.ProvisioningSubmissionConflictError(
                        marker
                    )
                ),
            ),
            lambda: prepare_local_crusher_ingress_v2(
                ProvisionCrusherV2Input(operation_id=_v2_ids().operation_id)
            ),
            "ActivityInfrastructureFailure",
            "activity_infrastructure_failure",
        ),
        (
            lambda monkeypatch, marker: monkeypatch.setattr(
                temporal_worker_module.caddy_topology,
                "preflight_local_caddy_topology",
                lambda *_args: (_ for _ in ()).throw(
                    temporal_worker_module.caddy_topology.CaddyTopologyError(marker, False)
                ),
            ),
            lambda: preflight_local_caddy_topology_activity(_v2_ids()),
            "CaddyTopologyFailed",
            "caddy_unavailable",
        ),
        (
            lambda monkeypatch, marker: monkeypatch.setattr(
                temporal_worker_module.ingress_probe,
                "probe_ingress",
                lambda *_args: (_ for _ in ()).throw(
                    temporal_worker_module.ingress_probe.IngressProbeError(marker, False)
                ),
            ),
            lambda: probe_local_caddy_ingress(_v2_ids()),
            "CaddyIngressProbeFailed",
            "ingress_readiness_unavailable",
        ),
        (
            lambda monkeypatch, marker: monkeypatch.setattr(
                temporal_worker_module.runtime_state,
                "mark_running",
                lambda *_args: (_ for _ in ()).throw(
                    temporal_worker_module.runtime_state.RuntimeStateError(marker)
                ),
            ),
            lambda: apply_local_crusher_runtime_v2(_v2_ids()),
            "ActivityInfrastructureFailure",
            "activity_infrastructure_failure",
        ),
        (
            lambda monkeypatch, marker: (
                monkeypatch.setattr(
                    temporal_worker_module.runtime_state, "mark_running", lambda *_args: None
                ),
                monkeypatch.setattr(
                    temporal_worker_module.runtime_state, "load_plan", lambda *_args: object()
                ),
                monkeypatch.setattr(
                    temporal_worker_module,
                    "apply_runtime_plan",
                    lambda *_args, **_kwargs: (_ for _ in ()).throw(
                        temporal_worker_module.RuntimeApplicationError(marker, retryable=False)
                    ),
                ),
            ),
            lambda: apply_local_crusher_runtime_v2(_v2_ids()),
            "ActivityInfrastructureFailure",
            "activity_infrastructure_failure",
        ),
    ],
)
def test_v2_activities_map_unknown_domain_codes_to_generic_retryable_failure(
    monkeypatch: pytest.MonkeyPatch,
    setup,
    invoke,
    expected_type: str,
    expected_code: str,
) -> None:
    marker = "secret-marker://unknown/provider/endpoint"
    setup(monkeypatch, marker)

    with pytest.raises(ApplicationError) as caught:
        invoke()

    assert caught.value.non_retryable is False
    assert caught.value.type == expected_type
    assert caught.value.message == expected_code
    assert list(caught.value.details) == [expected_code]
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    _assert_no_exception_marker(caught.value, marker)
    _assert_no_traceback_local_marker(caught.value, marker)


@pytest.mark.parametrize(
    ("code", "reported_retryable", "expected_nonretryable"),
    [
        ("docker_spec_conflict", True, True),
        ("docker_unavailable", False, False),
    ],
)
def test_runtime_v2_uses_closed_retryability_for_known_provider_codes(
    monkeypatch: pytest.MonkeyPatch,
    code: str,
    reported_retryable: bool,
    expected_nonretryable: bool,
) -> None:
    monkeypatch.setattr(temporal_worker_module.runtime_state, "mark_running", lambda *_args: None)
    monkeypatch.setattr(temporal_worker_module.runtime_state, "load_plan", lambda *_args: object())
    monkeypatch.setattr(
        temporal_worker_module,
        "apply_runtime_plan",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            temporal_worker_module.RuntimeApplicationError(code, retryable=reported_retryable)
        ),
    )

    with pytest.raises(ApplicationError) as caught:
        apply_local_crusher_runtime_v2(_v2_ids())

    assert caught.value.message == code
    assert list(caught.value.details) == [code]
    assert caught.value.non_retryable is expected_nonretryable


def _assert_no_traceback_local_marker(exc: BaseException, marker: str) -> None:
    traceback = exc.__traceback__
    while traceback is not None:
        if traceback.tb_frame.f_code.co_filename.endswith("fstack/temporal_worker.py"):
            assert marker not in repr(traceback.tb_frame.f_locals)
        traceback = traceback.tb_next


class _MarkerPlan:
    def __repr__(self) -> str:
        return "secret-marker://container/network/fingerprint/provider"


def test_v2_runtime_failure_discards_provider_plan_from_traceback_locals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "secret-marker://container/network/fingerprint/provider"
    monkeypatch.setattr(temporal_worker_module.runtime_state, "mark_running", lambda *_args: None)
    monkeypatch.setattr(
        temporal_worker_module.runtime_state, "load_plan", lambda *_args: _MarkerPlan()
    )
    monkeypatch.setattr(
        temporal_worker_module,
        "apply_runtime_plan",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            temporal_worker_module.RuntimeApplicationError("docker_spec_conflict", retryable=False)
        ),
    )

    with pytest.raises(ApplicationError) as caught:
        apply_local_crusher_runtime_v2(_v2_ids())

    _assert_no_traceback_local_marker(caught.value, marker)


def test_v2_reconcile_failure_discards_route_and_host_from_traceback_locals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "secret-marker.example.test"
    desired = temporal_worker_module.caddy_topology.DesiredCaddyTopology(
        deployment_id=UUID("44444444-4444-4444-8444-444444444444"),
        crusher_container_name="fstack-crusher-44444444444444448444444444444444",
        public_domain=marker,
        docker_network="fstack-local",
    )
    monkeypatch.setattr(
        temporal_worker_module.caddy_topology,
        "_load_desired_state",
        lambda _ids: desired,
    )
    monkeypatch.setattr(
        temporal_worker_module.caddy_admin.CaddyAdminConfig,
        "from_environ",
        lambda: (_ for _ in ()).throw(RuntimeError(marker)),
    )

    with pytest.raises(ApplicationError) as caught:
        reconcile_local_caddy_ingress(_v2_ids())

    _assert_no_traceback_local_marker(caught.value, marker)


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


def test_apply_activity_timeout_exceeds_declared_docker_budget() -> None:
    assert APPLY_DOCKER_TIMEOUT_BUDGET.total_seconds() == 1710
    assert timedelta(minutes=30) == temporal_workflow_module.APPLY_ACTIVITY_START_TO_CLOSE_TIMEOUT
    assert temporal_workflow_module.APPLY_ACTIVITY_START_TO_CLOSE_TIMEOUT > (
        APPLY_DOCKER_TIMEOUT_BUDGET
    )


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
            handle = await environment.client.start_workflow(
                ProvisionCrusherWorkflow.run,
                ProvisionCrusherInput(operation_id=str(operation_id)),
                id=f"provisioning/{operation_id}",
                task_queue="test-provision-crusher",
            )
            result = await handle.result()
            history = await handle.fetch_history()

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
        apply_scheduled = next(
            event.activity_task_scheduled_event_attributes
            for event in history.events
            if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
            and event.activity_task_scheduled_event_attributes.activity_type.name
            == APPLY_LOCAL_CRUSHER_ACTIVITY
        )
        assert apply_scheduled.start_to_close_timeout.ToTimedelta() == timedelta(minutes=30)
        assert apply_scheduled.start_to_close_timeout.ToTimedelta() > APPLY_DOCKER_TIMEOUT_BUDGET

    asyncio.run(run())


def test_history_fixture_scan_includes_keys_list_strings_and_decoded_data() -> None:
    encoded = base64.b64encode(b"provider_exception").decode()

    fixture_content = _history_fixture_content(
        {
            "secret://dictionary-key": "safe",
            "list": ["/home/list-value"],
            "payload": {"data": encoded},
        }
    )

    assert "secret://dictionary-key" in fixture_content
    assert "/home/list-value" in fixture_content
    assert "provider_exception" in fixture_content


def test_released_v1_history_replays_with_v1_and_v2_registered() -> None:
    fixture_path = (
        Path(__file__).parent / "fixtures" / "temporal" / "provision-crusher-v1-history.json"
    )
    fixture_text = fixture_path.read_text()
    fixture_document = json.loads(fixture_text)
    identities: set[str] = set()
    run_ids: set[str] = set()

    def inspect_fixture_metadata(value: object) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                if key == "identity" and isinstance(nested, str):
                    identities.add(nested)
                elif key in {"firstExecutionRunId", "originalExecutionRunId"} and isinstance(
                    nested, str
                ):
                    run_ids.add(nested)
                inspect_fixture_metadata(nested)
        elif isinstance(value, list):
            for nested in value:
                inspect_fixture_metadata(nested)

    inspect_fixture_metadata(fixture_document)
    history = WorkflowHistory.from_json(
        "provisioning/11111111-1111-4111-8111-111111111111",
        fixture_document,
    )

    async def run() -> list[object]:
        replay = await Replayer(
            workflows=[ProvisionCrusherWorkflow, ProvisionCrusherV2Workflow]
        ).replay_workflow(history, raise_on_replay_failure=False)
        assert replay.replay_failure is None

        converter = DataConverter.default
        decoded: list[object] = []
        for event in history.events:
            if event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
                decoded.extend(
                    await converter.decode(
                        event.workflow_execution_started_event_attributes.input.payloads
                    )
                )
            elif event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
                decoded.extend(
                    await converter.decode(
                        event.activity_task_scheduled_event_attributes.input.payloads
                    )
                )
            elif event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
                decoded.extend(
                    await converter.decode(
                        event.activity_task_completed_event_attributes.result.payloads
                    )
                )
            elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
                decoded.extend(
                    await converter.decode(
                        event.workflow_execution_completed_event_attributes.result.payloads
                    )
                )
        return decoded

    decoded = asyncio.run(run())
    operation = {"operation_id": "11111111-1111-4111-8111-111111111111"}
    submission = {
        "submission_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "operation_id": operation["operation_id"],
        "provider": "local_docker",
        "generation": 1,
        "submission_key": "fixture-v1-submission",
        "spec_fingerprint": "b" * 64,
        "api_container_name": "fstack-fixture-api",
        "migration_container_name": "fstack-fixture-migration",
    }
    assert decoded == [
        operation,
        operation,
        submission | {"state": "prepared"},
        submission | {"state": "prepared"},
        submission | {"state": "submitted"},
        submission | {"state": "submitted"},
        submission | {"state": "confirmed"},
        {
            "operation_id": operation["operation_id"],
            "state": "confirmed",
            "runtime_submission": submission | {"state": "confirmed"},
        },
    ]
    assert [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history.events
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
    ] == [
        PREPARE_LOCAL_CRUSHER_ACTIVITY,
        APPLY_LOCAL_CRUSHER_ACTIVITY,
        FINALIZE_LOCAL_CRUSHER_SUCCESS_ACTIVITY,
    ]
    assert history.events[0].workflow_execution_started_event_attributes.workflow_type.name == (
        "fstack.provision_crusher.v1"
    )
    assert identities == {"fstack-v1-fixture-worker"}
    assert run_ids == {"44444444-4444-4444-8444-444444444444"}
    fixture_content = _history_fixture_content(fixture_document)
    for forbidden in (
        "579234@spark",
        "secret://",
        "/home/",
        "/run/",
        "/var/",
        "container_id",
        "provider_body",
        "provider_exception",
    ):
        assert forbidden not in fixture_content
        assert forbidden not in repr(decoded)


def test_provision_crusher_v2_workflow_orders_sanitized_activities_and_policies() -> None:
    ids = _v2_ids()
    calls: list[tuple[str, object]] = []

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY)
    async def prepare(request: ProvisionCrusherV2Input) -> ProvisioningSubmissionIds:
        calls.append(("prepare", request))
        return ids

    @activity.defn(name=APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY)
    async def apply(payload: ProvisioningSubmissionIds) -> None:
        calls.append(("apply", payload))

    @activity.defn(name=PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY)
    async def preflight(payload: ProvisioningSubmissionIds) -> None:
        calls.append(("preflight", payload))

    @activity.defn(name=RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY)
    async def reconcile(payload: ProvisioningSubmissionIds) -> None:
        calls.append(("reconcile", payload))

    @activity.defn(name=PROBE_LOCAL_CADDY_INGRESS_ACTIVITY)
    async def probe(payload: ProvisioningSubmissionIds) -> None:
        calls.append(("probe", payload))

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY)
    async def finalize(payload: ProvisioningSubmissionIds) -> ProvisionCrusherV2Result:
        calls.append(("finalize", payload))
        return ProvisionCrusherV2Result(operation_id=payload.operation_id, state="confirmed")

    async def run() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-provision-crusher-v2",
                workflows=[ProvisionCrusherV2Workflow],
                activities=[prepare, apply, preflight, reconcile, probe, finalize],
            ),
        ):
            handle = await environment.client.start_workflow(
                ProvisionCrusherV2Workflow.run,
                ProvisionCrusherV2Input(operation_id=ids.operation_id),
                id=f"provisioning/{ids.operation_id}",
                task_queue="test-provision-crusher-v2",
            )
            result = await handle.result()
            history = await handle.fetch_history()

        assert result == ProvisionCrusherV2Result(
            operation_id=ids.operation_id,
            state="confirmed",
        )
        assert calls == [
            ("prepare", ProvisionCrusherV2Input(operation_id=ids.operation_id)),
            ("apply", ids),
            ("preflight", ids),
            ("reconcile", ids),
            ("probe", ids),
            ("finalize", ids),
        ]
        scheduled = {
            attributes.activity_type.name: attributes
            for event in history.events
            if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
            for attributes in [event.activity_task_scheduled_event_attributes]
        }
        expected_timeouts = {
            PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY: PREPARE_V2_ACTIVITY_START_TO_CLOSE_TIMEOUT,
            APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY: timedelta(minutes=30),
            PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY: PREFLIGHT_ACTIVITY_START_TO_CLOSE_TIMEOUT,
            RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY: RECONCILE_ACTIVITY_START_TO_CLOSE_TIMEOUT,
            PROBE_LOCAL_CADDY_INGRESS_ACTIVITY: PROBE_ACTIVITY_START_TO_CLOSE_TIMEOUT,
            FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY: (
                FINALIZE_V2_ACTIVITY_START_TO_CLOSE_TIMEOUT
            ),
        }
        assert set(scheduled) == set(expected_timeouts)
        for activity_name, timeout in expected_timeouts.items():
            attributes = scheduled[activity_name]
            assert attributes.start_to_close_timeout.ToTimedelta() == timeout
            is_apply = activity_name == APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY
            assert attributes.retry_policy.initial_interval.ToTimedelta() == timedelta(
                seconds=2 if is_apply else 1
            )
            assert attributes.retry_policy.maximum_interval.ToTimedelta() == timedelta(
                seconds=30 if is_apply else 10
            )

    asyncio.run(run())


def test_provision_crusher_v2_history_decodes_to_sanitized_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = _v2_ids()
    marker = (
        "secret-marker://route/host/path/fingerprint/container/network/endpoint/"
        "address/alias/inspection/provider"
    )

    def fail_preflight(_ids: object) -> None:
        try:
            raise RuntimeError(marker)
        except RuntimeError as exc:
            raise temporal_worker_module.caddy_topology.CaddyTopologyError(
                "caddy_topology_invalid", False
            ) from exc

    monkeypatch.setattr(
        temporal_worker_module.caddy_topology,
        "preflight_local_caddy_topology",
        fail_preflight,
    )

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY)
    async def prepare(_request: ProvisionCrusherV2Input) -> ProvisioningSubmissionIds:
        return ids

    @activity.defn(name=APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY)
    async def apply(_ids: ProvisioningSubmissionIds) -> None:
        return None

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY)
    async def finalize_failure(failure: ProvisioningFailureV2) -> ProvisionCrusherV2Result:
        return ProvisionCrusherV2Result(operation_id=failure.operation_id, state="failed")

    async def run() -> None:
        environment = await WorkflowEnvironment.start_time_skipping()
        with ThreadPoolExecutor(max_workers=1) as executor:
            async with (
                environment,
                Worker(
                    environment.client,
                    task_queue="test-provision-crusher-v2-sanitized-history",
                    workflows=[ProvisionCrusherV2Workflow],
                    activities=[
                        prepare,
                        apply,
                        preflight_local_caddy_topology_activity,
                        finalize_failure,
                    ],
                    activity_executor=executor,
                ),
            ):
                handle = await environment.client.start_workflow(
                    ProvisionCrusherV2Workflow.run,
                    ProvisionCrusherV2Input(operation_id=ids.operation_id),
                    id=f"provisioning/{ids.operation_id}/sanitized-history",
                    task_queue="test-provision-crusher-v2-sanitized-history",
                )
                result = await handle.result()
                history = await handle.fetch_history()
                converter = environment.client.data_converter

        assert result == ProvisionCrusherV2Result(operation_id=ids.operation_id, state="failed")
        scheduled: dict[int, tuple[str, list[object]]] = {}
        completed: dict[str, list[object]] = {}
        failed: dict[str, BaseException] = {}
        for event in history.events:
            if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
                attributes = event.activity_task_scheduled_event_attributes
                scheduled[event.event_id] = (
                    attributes.activity_type.name,
                    await converter.decode(attributes.input.payloads),
                )
            elif event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
                attributes = event.activity_task_completed_event_attributes
                activity_name = scheduled[attributes.scheduled_event_id][0]
                completed[activity_name] = await converter.decode(attributes.result.payloads)
            elif event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED:
                attributes = event.activity_task_failed_event_attributes
                activity_name = scheduled[attributes.scheduled_event_id][0]
                failed[activity_name] = await converter.decode_failure(attributes.failure)

        ids_payload = {
            "operation_id": ids.operation_id,
            "runtime_submission_id": ids.runtime_submission_id,
            "ingress_submission_id": ids.ingress_submission_id,
        }
        scheduled_payloads = {name: payload for name, payload in scheduled.values()}
        assert scheduled_payloads == {
            PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY: [{"operation_id": ids.operation_id}],
            APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY: [ids_payload],
            PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY: [ids_payload],
            FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY: [
                {
                    "operation_id": ids.operation_id,
                    "scope": "ingress",
                    "error_code": "caddy_topology_invalid",
                }
            ],
        }
        assert completed[PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY] == [ids_payload]
        assert completed[APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY] in ([], [None])
        assert completed[FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY] == [
            {"operation_id": ids.operation_id, "state": "failed"}
        ]
        assert set(failed) == {PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY}
        preflight_error = failed[PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY]
        assert isinstance(preflight_error, ApplicationError)
        assert preflight_error.message == "caddy_topology_invalid"
        assert list(preflight_error.details) == ["caddy_topology_invalid"]
        assert preflight_error.non_retryable is True
        serialized_history = history.to_json()
        assert marker not in serialized_history
        assert base64.b64encode(marker.encode()).decode() not in serialized_history
        assert marker not in repr(preflight_error)

    asyncio.run(run())


@pytest.mark.postgres
def test_provision_crusher_v2_prepare_commit_is_visible_before_runtime_apply(
    migrated_database: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient
    from sqlalchemy import text

    from fstack import api
    from fstack.api import create_app

    monkeypatch.setenv("FSTACK_OPERATOR_TOKEN", "test-operator-token")
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    monkeypatch.setattr(api, "create_engine", lambda *_args, **_kwargs: migrated_database)
    with TestClient(create_app()) as client:
        response = client.post(
            "/v1/provisioning/operations",
            headers={
                "Authorization": "Bearer test-operator-token",
                "Idempotency-Key": "task13-prepare-before-apply",
            },
            json={
                "organization_slug": "task13",
                "organization_name": "Task 13",
                "business_slug": "crusher",
                "business_name": "Crusher",
                "environment_slug": "production",
                "public_domain": "task13.example.test",
                "crusher_image": "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
                "database_secret_ref": "secret://vault/task13/database",
                "object_storage_secret_ref": "secret://vault/task13/object-storage",
            },
        )
    response.raise_for_status()
    operation_id = response.json()["operation_id"]
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE workflow_dispatches DISABLE TRIGGER "
                "tr_workflow_dispatches_lifecycle_guard"
            )
        )
        connection.execute(
            text(
                "UPDATE workflow_dispatches SET workflow_type='fstack.provision_crusher.v2' "
                "WHERE operation_id=:operation_id"
            ),
            {"operation_id": operation_id},
        )
        connection.execute(
            text(
                "ALTER TABLE workflow_dispatches ENABLE TRIGGER "
                "tr_workflow_dispatches_lifecycle_guard"
            )
        )

    observed_ids: list[ProvisioningSubmissionIds] = []

    @activity.defn(name=APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY)
    async def apply(ids: ProvisioningSubmissionIds) -> None:
        with migrated_database.connect() as connection:
            state = connection.execute(
                text("SELECT state FROM provisioning_operations WHERE id=:operation_id"),
                {"operation_id": operation_id},
            ).scalar_one()
            events = (
                connection.execute(
                    text(
                        "SELECT event_type, payload FROM audit_events "
                        "WHERE operation_id=:operation_id ORDER BY created_at, id"
                    ),
                    {"operation_id": operation_id},
                )
                .mappings()
                .all()
            )
        assert state == "running"
        assert [event["event_type"] for event in events] == [
            "provisioning.requested",
            "provisioning.started",
        ]
        assert events[1]["payload"] == {
            "runtime_submission_id": ids.runtime_submission_id,
            "ingress_submission_id": ids.ingress_submission_id,
        }
        observed_ids.append(ids)

    @activity.defn(name=PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY)
    async def preflight(_ids: ProvisioningSubmissionIds) -> None:
        return None

    @activity.defn(name=RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY)
    async def reconcile(_ids: ProvisioningSubmissionIds) -> None:
        return None

    @activity.defn(name=PROBE_LOCAL_CADDY_INGRESS_ACTIVITY)
    async def probe(_ids: ProvisioningSubmissionIds) -> None:
        return None

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY)
    async def finalize(ids: ProvisioningSubmissionIds) -> ProvisionCrusherV2Result:
        return ProvisionCrusherV2Result(operation_id=ids.operation_id, state="confirmed")

    async def run() -> None:
        environment = await WorkflowEnvironment.start_time_skipping()
        with ThreadPoolExecutor(max_workers=1) as executor:
            async with (
                environment,
                Worker(
                    environment.client,
                    task_queue="test-provision-crusher-v2-prepare-ordering",
                    workflows=[ProvisionCrusherV2Workflow],
                    activities=[
                        prepare_local_crusher_ingress_v2,
                        apply,
                        preflight,
                        reconcile,
                        probe,
                        finalize,
                    ],
                    activity_executor=executor,
                ),
            ):
                result = await environment.client.execute_workflow(
                    ProvisionCrusherV2Workflow.run,
                    ProvisionCrusherV2Input(operation_id=operation_id),
                    id=f"provisioning/{operation_id}/prepare-ordering",
                    task_queue="test-provision-crusher-v2-prepare-ordering",
                )
        assert result == ProvisionCrusherV2Result(operation_id=operation_id, state="confirmed")

    asyncio.run(run())
    assert len(observed_ids) == 1


@pytest.mark.parametrize(
    ("retrying_activity", "error_code"),
    [
        (PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY, "activity_infrastructure_failure"),
        (APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY, "docker_unavailable"),
        (PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY, "caddy_unavailable"),
        (RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY, "caddy_mutation_ambiguous"),
        (PROBE_LOCAL_CADDY_INGRESS_ACTIVITY, "ingress_readiness_unavailable"),
    ],
)
def test_provision_crusher_v2_workflow_retries_without_scheduling_downstream(
    retrying_activity: str,
    error_code: str,
) -> None:
    ids = _v2_ids()

    async def run() -> None:
        calls: list[tuple[str, object]] = []
        attempts: dict[str, int] = {}
        second_attempt_started = asyncio.Event()
        release_second_attempt = asyncio.Event()

        async def record(name: str, payload: object) -> None:
            calls.append((name, payload))
            attempts[name] = attempts.get(name, 0) + 1
            if name != retrying_activity:
                return
            if attempts[name] == 1:
                raise ApplicationError(error_code, error_code, non_retryable=False)
            if attempts[name] == 2:
                second_attempt_started.set()
                await release_second_attempt.wait()

        @activity.defn(name=PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY)
        async def prepare(request: ProvisionCrusherV2Input) -> ProvisioningSubmissionIds:
            await record(PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY, request)
            return ids

        @activity.defn(name=APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY)
        async def apply(payload: ProvisioningSubmissionIds) -> None:
            await record(APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY, payload)

        @activity.defn(name=PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY)
        async def preflight(payload: ProvisioningSubmissionIds) -> None:
            await record(PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY, payload)

        @activity.defn(name=RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY)
        async def reconcile(payload: ProvisioningSubmissionIds) -> None:
            await record(RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY, payload)

        @activity.defn(name=PROBE_LOCAL_CADDY_INGRESS_ACTIVITY)
        async def probe(payload: ProvisioningSubmissionIds) -> None:
            await record(PROBE_LOCAL_CADDY_INGRESS_ACTIVITY, payload)

        @activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY)
        async def finalize_success(payload: ProvisioningSubmissionIds) -> ProvisionCrusherV2Result:
            await record(FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY, payload)
            return ProvisionCrusherV2Result(operation_id=payload.operation_id, state="confirmed")

        @activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY)
        async def finalize_failure(_failure: ProvisioningFailureV2) -> ProvisionCrusherV2Result:
            pytest.fail("retryable failures must not schedule failure finalization")

        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-provision-crusher-v2-retry",
                workflows=[ProvisionCrusherV2Workflow],
                activities=[
                    prepare,
                    apply,
                    preflight,
                    reconcile,
                    probe,
                    finalize_success,
                    finalize_failure,
                ],
            ),
        ):
            handle = await environment.client.start_workflow(
                ProvisionCrusherV2Workflow.run,
                ProvisionCrusherV2Input(operation_id=ids.operation_id),
                id=f"provisioning/{ids.operation_id}/retry/{retrying_activity}",
                task_queue="test-provision-crusher-v2-retry",
            )
            await asyncio.wait_for(second_attempt_started.wait(), timeout=5)
            history_during_retry = await handle.fetch_history()
            scheduled_during_retry = [
                event.activity_task_scheduled_event_attributes.activity_type.name
                for event in history_during_retry.events
                if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
            ]
            ordered_activities = [
                PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY,
                APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY,
                PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY,
                RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY,
                PROBE_LOCAL_CADDY_INGRESS_ACTIVITY,
            ]
            retrying_index = ordered_activities.index(retrying_activity)
            assert scheduled_during_retry == ordered_activities[: retrying_index + 1]
            assert attempts[retrying_activity] == 2
            assert [payload for name, payload in calls if name == retrying_activity] == [
                calls[-2][1],
                calls[-1][1],
            ]
            assert calls[-2][1] == calls[-1][1]
            release_second_attempt.set()
            result = await handle.result()

        assert result == ProvisionCrusherV2Result(operation_id=ids.operation_id, state="confirmed")
        assert attempts[retrying_activity] == 2
        assert calls[-1] == (FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY, ids)

    asyncio.run(run())


@pytest.mark.parametrize("failure_path", [False, True])
def test_provision_crusher_v2_workflow_retries_finalization_with_the_same_payload(
    failure_path: bool,
) -> None:
    ids = _v2_ids()
    finalizer_payloads: list[object] = []

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY)
    async def prepare(_request: ProvisionCrusherV2Input) -> ProvisioningSubmissionIds:
        return ids

    @activity.defn(name=APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY)
    async def apply(_ids: ProvisioningSubmissionIds) -> None:
        if failure_path:
            raise ApplicationError(
                "docker_spec_conflict", "docker_spec_conflict", non_retryable=True
            )

    @activity.defn(name=PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY)
    async def preflight(_ids: ProvisioningSubmissionIds) -> None:
        return None

    @activity.defn(name=RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY)
    async def reconcile(_ids: ProvisioningSubmissionIds) -> None:
        return None

    @activity.defn(name=PROBE_LOCAL_CADDY_INGRESS_ACTIVITY)
    async def probe(_ids: ProvisioningSubmissionIds) -> None:
        return None

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY)
    async def finalize_success(payload: ProvisioningSubmissionIds) -> ProvisionCrusherV2Result:
        finalizer_payloads.append(payload)
        if len(finalizer_payloads) == 1:
            raise ApplicationError(
                "activity_infrastructure_failure",
                "activity_infrastructure_failure",
                non_retryable=False,
            )
        return ProvisionCrusherV2Result(operation_id=payload.operation_id, state="confirmed")

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY)
    async def finalize_failure(failure: ProvisioningFailureV2) -> ProvisionCrusherV2Result:
        finalizer_payloads.append(failure)
        if len(finalizer_payloads) == 1:
            raise ApplicationError(
                "activity_infrastructure_failure",
                "activity_infrastructure_failure",
                non_retryable=False,
            )
        return ProvisionCrusherV2Result(operation_id=failure.operation_id, state="failed")

    async def run() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-provision-crusher-v2-finalizer-retry",
                workflows=[ProvisionCrusherV2Workflow],
                activities=[
                    prepare,
                    apply,
                    preflight,
                    reconcile,
                    probe,
                    finalize_success,
                    finalize_failure,
                ],
            ),
        ):
            result = await environment.client.execute_workflow(
                ProvisionCrusherV2Workflow.run,
                ProvisionCrusherV2Input(operation_id=ids.operation_id),
                id=f"provisioning/{ids.operation_id}/finalizer-retry/{failure_path}",
                task_queue="test-provision-crusher-v2-finalizer-retry",
            )

        assert result.state == ("failed" if failure_path else "confirmed")
        assert len(finalizer_payloads) == 2
        assert finalizer_payloads[0] == finalizer_payloads[1]

    asyncio.run(run())


@pytest.mark.parametrize(
    ("failure_path", "expected_finalizer", "expected_code"),
    [
        (
            False,
            FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY,
            "provisioning_success_state_invalid",
        ),
        (
            True,
            FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY,
            "provisioning_failure_conflict",
        ),
    ],
)
def test_provision_crusher_v2_workflow_fails_closed_on_finalizer_conflict(
    failure_path: bool,
    expected_finalizer: str,
    expected_code: str,
) -> None:
    ids = _v2_ids()
    finalizer_calls: list[str] = []

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY)
    async def prepare(_request: ProvisionCrusherV2Input) -> ProvisioningSubmissionIds:
        return ids

    @activity.defn(name=APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY)
    async def apply(_ids: ProvisioningSubmissionIds) -> None:
        if failure_path:
            raise ApplicationError(
                "docker_spec_conflict", "docker_spec_conflict", non_retryable=True
            )

    @activity.defn(name=PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY)
    async def preflight(_ids: ProvisioningSubmissionIds) -> None:
        return None

    @activity.defn(name=RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY)
    async def reconcile(_ids: ProvisioningSubmissionIds) -> None:
        return None

    @activity.defn(name=PROBE_LOCAL_CADDY_INGRESS_ACTIVITY)
    async def probe(_ids: ProvisioningSubmissionIds) -> None:
        return None

    async def reject_finalizer(name: str, code: str) -> None:
        finalizer_calls.append(name)
        if name != expected_finalizer:
            pytest.fail("alternate finalizer must not be scheduled")
        raise ApplicationError(code, code, non_retryable=True)

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY)
    async def finalize_success(_ids: ProvisioningSubmissionIds) -> ProvisionCrusherV2Result:
        await reject_finalizer(
            FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY,
            "provisioning_success_state_invalid",
        )
        raise AssertionError("unreachable")

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY)
    async def finalize_failure(_failure: ProvisioningFailureV2) -> ProvisionCrusherV2Result:
        await reject_finalizer(
            FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY,
            "provisioning_failure_conflict",
        )
        raise AssertionError("unreachable")

    async def run() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-provision-crusher-v2-finalizer-conflict",
                workflows=[ProvisionCrusherV2Workflow],
                activities=[
                    prepare,
                    apply,
                    preflight,
                    reconcile,
                    probe,
                    finalize_success,
                    finalize_failure,
                ],
            ),
        ):
            handle = await environment.client.start_workflow(
                ProvisionCrusherV2Workflow.run,
                ProvisionCrusherV2Input(operation_id=ids.operation_id),
                id=f"provisioning/{ids.operation_id}/finalizer-conflict/{failure_path}",
                task_queue="test-provision-crusher-v2-finalizer-conflict",
            )
            with pytest.raises(WorkflowFailureError) as caught:
                await handle.result()
            history = await handle.fetch_history()

        assert finalizer_calls == [expected_finalizer]
        activity_error = caught.value.cause
        assert isinstance(activity_error, ActivityError)
        application_error = activity_error.cause
        assert isinstance(application_error, ApplicationError)
        assert application_error.message == expected_code
        assert list(application_error.details) == [expected_code]
        assert application_error.non_retryable is True
        scheduled_names = [
            event.activity_task_scheduled_event_attributes.activity_type.name
            for event in history.events
            if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
        ]
        assert scheduled_names.count(expected_finalizer) == 1
        alternate_finalizer = (
            FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY
            if expected_finalizer == FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY
            else FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY
        )
        assert alternate_finalizer not in scheduled_names

    asyncio.run(run())


@pytest.mark.parametrize(
    ("failed_activity", "error_code", "expected_scope", "expected_error_code"),
    [
        (
            PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY,
            "runtime_submission_conflict",
            "runtime",
            "runtime_submission_conflict",
        ),
        (
            APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY,
            "docker_spec_conflict",
            "runtime",
            "docker_spec_conflict",
        ),
        (
            PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY,
            "caddy_topology_invalid",
            "ingress",
            "caddy_topology_invalid",
        ),
        (
            RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY,
            "caddy_host_conflict",
            "ingress",
            "caddy_host_conflict",
        ),
        (
            PROBE_LOCAL_CADDY_INGRESS_ACTIVITY,
            "invalid_caddy_probe_configuration",
            "ingress",
            "invalid_caddy_probe_configuration",
        ),
        (
            APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY,
            "provider-marker://route/host/path/fingerprint",
            "runtime",
            "provisioning_failed",
        ),
    ],
)
def test_provision_crusher_v2_workflow_finalizes_permanent_failures_once(
    failed_activity: str,
    error_code: str,
    expected_scope: str,
    expected_error_code: str,
) -> None:
    ids = _v2_ids()
    calls: list[str] = []
    failures: list[ProvisioningFailureV2] = []

    def reject_if_selected(name: str) -> None:
        calls.append(name)
        if name == failed_activity:
            raise ApplicationError(error_code, error_code, non_retryable=True)

    @activity.defn(name=PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY)
    async def prepare(_request: ProvisionCrusherV2Input) -> ProvisioningSubmissionIds:
        reject_if_selected(PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY)
        return ids

    @activity.defn(name=APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY)
    async def apply(_ids: ProvisioningSubmissionIds) -> None:
        reject_if_selected(APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY)

    @activity.defn(name=PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY)
    async def preflight(_ids: ProvisioningSubmissionIds) -> None:
        reject_if_selected(PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY)

    @activity.defn(name=RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY)
    async def reconcile(_ids: ProvisioningSubmissionIds) -> None:
        reject_if_selected(RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY)

    @activity.defn(name=PROBE_LOCAL_CADDY_INGRESS_ACTIVITY)
    async def probe(_ids: ProvisioningSubmissionIds) -> None:
        reject_if_selected(PROBE_LOCAL_CADDY_INGRESS_ACTIVITY)

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_SUCCESS_V2_ACTIVITY)
    async def finalize_success(_ids: ProvisioningSubmissionIds) -> ProvisionCrusherV2Result:
        pytest.fail("success finalizer must not run after a permanent failure")

    @activity.defn(name=FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY)
    async def finalize_failure(failure: ProvisioningFailureV2) -> ProvisionCrusherV2Result:
        calls.append(FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY)
        failures.append(failure)
        return ProvisionCrusherV2Result(operation_id=failure.operation_id, state="failed")

    async def run() -> None:
        async with (
            await WorkflowEnvironment.start_time_skipping() as environment,
            Worker(
                environment.client,
                task_queue="test-provision-crusher-v2-failure",
                workflows=[ProvisionCrusherV2Workflow],
                activities=[
                    prepare,
                    apply,
                    preflight,
                    reconcile,
                    probe,
                    finalize_success,
                    finalize_failure,
                ],
            ),
        ):
            result = await environment.client.execute_workflow(
                ProvisionCrusherV2Workflow.run,
                ProvisionCrusherV2Input(operation_id=ids.operation_id),
                id=f"provisioning/{ids.operation_id}/{failed_activity}",
                task_queue="test-provision-crusher-v2-failure",
            )

        assert result == ProvisionCrusherV2Result(operation_id=ids.operation_id, state="failed")
        assert len(failures) == 1
        assert failures[0].operation_id == ids.operation_id
        assert failures[0].scope == expected_scope
        assert failures[0].error_code == expected_error_code
        ordered_activities = [
            PREPARE_LOCAL_CRUSHER_INGRESS_V2_ACTIVITY,
            APPLY_LOCAL_CRUSHER_RUNTIME_V2_ACTIVITY,
            PREFLIGHT_LOCAL_CADDY_TOPOLOGY_ACTIVITY,
            RECONCILE_LOCAL_CADDY_INGRESS_ACTIVITY,
            PROBE_LOCAL_CADDY_INGRESS_ACTIVITY,
        ]
        failed_index = ordered_activities.index(failed_activity)
        assert calls == [
            *ordered_activities[: failed_index + 1],
            FINALIZE_LOCAL_CRUSHER_INGRESS_FAILURE_V2_ACTIVITY,
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
                WORKFLOW_TYPE_V1,
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
