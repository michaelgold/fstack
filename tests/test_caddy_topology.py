from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest

import fstack.caddy_topology as caddy_topology
from fstack import ingress_submission, runtime_submission
from fstack.caddy_topology import (
    PRELIGHT_ACTIVITY_START_TO_CLOSE_TIMEOUT,
    PRELIGHT_TIMEOUT_BUDGET,
    CaddyTopologyError,
    DesiredCaddyTopology,
    ProvisioningSubmissionIds,
    preflight_local_caddy_topology,
    preflight_then_reconcile,
)

_IDS = ProvisioningSubmissionIds(
    operation_id="11111111-1111-4111-8111-111111111111",
    runtime_submission_id="22222222-2222-4222-8222-222222222222",
    ingress_submission_id="33333333-3333-4333-8333-333333333333",
)
_ENV = {
    "FSTACK_CADDY_CONTAINER_NAME": "fstack-caddy",
    "FSTACK_CADDY_PROBE_ADDRESS": "127.0.0.2:8080",
    "FSTACK_DOCKER_NETWORK": "fstack-local",
}
_DESIRED = DesiredCaddyTopology(
    deployment_id=UUID("44444444-4444-4444-8444-444444444444"),
    crusher_container_name="fstack-crusher-44444444444444448444444444444444",
    public_domain="mail.acme.example",
    docker_network="fstack-local",
)


def _container(name: str, identifier: str, ip: str, aliases: list[str]) -> bytes:
    return json.dumps(
        [
            {
                "Id": identifier,
                "Name": "/" + name,
                "State": {"Running": True, "Status": "running"},
                "NetworkSettings": {
                    "Networks": {
                        "fstack-local": {
                            "IPAddress": ip,
                            "GlobalIPv6Address": "",
                            "Aliases": aliases,
                        }
                    }
                },
            }
        ]
    ).encode()


def _network(crusher_id: str, *, include_caddy: bool = True) -> bytes:
    containers = {
        crusher_id: {
            "Name": _DESIRED.crusher_container_name,
            "IPv4Address": "172.20.0.3/16",
            "IPv6Address": "",
        }
    }
    if include_caddy:
        containers["a" * 64] = {
            "Name": "fstack-caddy",
            "IPv4Address": "127.0.0.2/32",
            "IPv6Address": "",
        }
    return json.dumps(
        [
            {
                "Name": "fstack-local",
                "Id": "f" * 64,
                "Containers": containers,
            }
        ]
    ).encode()


class _Socket:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_timeout_budget_fits_future_activity_timeout() -> None:
    assert timedelta(seconds=40) == PRELIGHT_TIMEOUT_BUDGET
    assert timedelta(seconds=60) == PRELIGHT_ACTIVITY_START_TO_CLOSE_TIMEOUT
    assert PRELIGHT_ACTIVITY_START_TO_CLOSE_TIMEOUT > PRELIGHT_TIMEOUT_BUDGET


def test_preflight_recomputes_one_absolute_deadline_before_every_blocking_phase() -> None:
    now = [100.0]
    caddy_id = "a" * 64
    crusher_id = "b" * 64
    outputs = [
        _container("fstack-caddy", caddy_id, "127.0.0.2", ["fstack-caddy"]),
        _container(
            _DESIRED.crusher_container_name,
            crusher_id,
            "172.20.0.3",
            [_DESIRED.crusher_container_name],
        ),
        _network(crusher_id),
    ]
    timeouts: list[float] = []

    def load(_ids: ProvisioningSubmissionIds) -> DesiredCaddyTopology:
        now[0] += 15.0
        return _DESIRED

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        del argv
        timeouts.append(timeout)
        output = outputs[len(timeouts) - 1]
        now[0] += 9.0
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=load,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: pytest.fail("deadline must stop listener connect"),
            clock=lambda: now[0],
        )
    assert caught.value.code == "caddy_unavailable"
    assert caught.value.retryable is True
    assert timeouts == [pytest.approx(10.0), pytest.approx(10.0), pytest.approx(7.0)]


def test_preflight_inspects_exact_resources_once_and_connects_listener() -> None:
    caddy_id = "a" * 64
    crusher_id = "b" * 64
    outputs = [
        _container("fstack-caddy", caddy_id, "127.0.0.2", ["fstack-caddy"]),
        _container(
            _DESIRED.crusher_container_name,
            crusher_id,
            "172.20.0.3",
            [crusher_id, crusher_id[:12], _DESIRED.crusher_container_name],
        ),
        _network(crusher_id),
    ]
    calls: list[tuple[list[str], float]] = []
    connected: list[tuple[str, int, float]] = []
    sock = _Socket()

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        calls.append((argv, timeout))
        return SimpleNamespace(returncode=0, stdout=outputs[len(calls) - 1], stderr=b"")

    def connector(host: str, port: int, timeout: float) -> _Socket:
        connected.append((host, port, timeout))
        return sock

    assert (
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=connector,
        )
        is None
    )
    assert [call[0] for call in calls] == [
        ["docker", "container", "inspect", "fstack-caddy"],
        ["docker", "container", "inspect", _DESIRED.crusher_container_name],
        ["docker", "network", "inspect", "fstack-local"],
    ]
    assert len(calls) == 3
    assert all(0 < timeout <= 10 for _, timeout in calls)
    assert len(connected) == 1
    assert connected[0][:2] == ("127.0.0.2", 8080)
    assert 0 < connected[0][2] <= 5
    assert sock.closed


@pytest.mark.parametrize(
    ("failed_call", "code", "retryable"),
    [(0, "caddy_unavailable", True), (1, "caddy_topology_invalid", False)],
)
def test_missing_caddy_is_retryable_but_missing_crusher_is_permanent(
    failed_call: int, code: str, retryable: bool
) -> None:
    crusher_id = "b" * 64
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
        _container(
            _DESIRED.crusher_container_name,
            crusher_id,
            "172.20.0.3",
            [_DESIRED.crusher_container_name],
        ),
        _network(crusher_id),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        index = calls
        calls += 1
        if index == failed_call:
            return SimpleNamespace(
                returncode=1,
                stdout=b"[]\n",
                stderr=b"Error: No such object: redacted\n",
            )
        return SimpleNamespace(returncode=0, stdout=outputs[index], stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: _Socket(),
        )
    assert caught.value.code == code
    assert caught.value.retryable is retryable
    assert caught.value.scope == "ingress"
    assert str(caught.value) == code
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_rejects_noncanonical_payload_before_any_io() -> None:
    calls: list[str] = []
    bad = replace(_IDS, operation_id="AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            bad,
            desired_loader=lambda _ids: calls.append("load") or _DESIRED,
            environ=_ENV,
            docker_runner=lambda *_args, **_kwargs: calls.append("docker"),
            listener_connector=lambda *_args: calls.append("connect"),
        )
    assert caught.value.code == "invalid_provisioning_submission_ids"
    assert caught.value.retryable is False
    assert calls == []


@pytest.mark.parametrize(
    ("container_name", "aliases"),
    [
        (_DESIRED.crusher_container_name, []),
        ("other-name", [_DESIRED.crusher_container_name]),
        ("other-name", ["extra", _DESIRED.crusher_container_name, "another"]),
    ],
)
def test_accepts_crusher_dns_name_from_exact_name_or_aliases_with_additional_aliases(
    container_name: str, aliases: list[str]
) -> None:
    crusher_id = "b" * 64
    crusher = json.loads(
        _container(_DESIRED.crusher_container_name, crusher_id, "172.20.0.3", aliases)
    )
    crusher[0]["Name"] = "/" + container_name
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
        json.dumps(crusher).encode(),
        _network(crusher_id),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        del argv, timeout
        output = outputs[calls]
        calls += 1
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    preflight_local_caddy_topology(
        _IDS,
        desired_loader=lambda _ids: _DESIRED,
        environ=_ENV,
        docker_runner=runner,
        listener_connector=lambda *_args: _Socket(),
    )
    assert calls == 3


def test_rejects_hostile_alias_shape_and_duplicate_network_identity() -> None:
    crusher_id = "b" * 64
    crusher = json.loads(_container(_DESIRED.crusher_container_name, crusher_id, "172.20.0.3", []))
    crusher[0]["NetworkSettings"]["Networks"]["fstack-local"]["Aliases"] = [
        _DESIRED.crusher_container_name,
        7,
    ]
    network = json.loads(_network(crusher_id))
    network[0]["Containers"]["c" * 64] = {
        "Name": _DESIRED.crusher_container_name,
        "IPv4Address": "172.20.0.4/16",
        "IPv6Address": "",
    }

    def run_case(hostile: bytes, expected: str) -> None:
        outputs = [
            _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
            hostile
            if expected == "caddy_unavailable"
            else _container(_DESIRED.crusher_container_name, crusher_id, "172.20.0.3", []),
            _network(crusher_id) if expected == "caddy_unavailable" else hostile,
        ]
        calls = 0

        def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
            nonlocal calls
            del argv, timeout
            output = outputs[calls]
            calls += 1
            return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

        with pytest.raises(CaddyTopologyError) as caught:
            preflight_local_caddy_topology(
                _IDS,
                desired_loader=lambda _ids: _DESIRED,
                environ=_ENV,
                docker_runner=runner,
                listener_connector=lambda *_args: pytest.fail("must not connect"),
            )
        assert caught.value.code == expected

    run_case(json.dumps(crusher).encode(), "caddy_unavailable")
    run_case(json.dumps(network).encode(), "caddy_topology_invalid")


def test_default_docker_runner_uses_explicit_local_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[dict[str, str]] = []

    def run(*_args: object, **kwargs: object) -> SimpleNamespace:
        environment = kwargs["env"]
        assert type(environment) is dict
        captured.append(environment)
        return SimpleNamespace(returncode=0, stdout=b"[]", stderr=b"")

    monkeypatch.setattr(caddy_topology.subprocess, "run", run)
    monkeypatch.setenv("DOCKER_HOST", "tcp://ambient-private:2376")
    caddy_topology._default_docker_runner(["docker", "version"], timeout=1.0)
    assert captured == [
        {
            "PATH": caddy_topology.os.environ.get("PATH", caddy_topology.os.defpath),
            "DOCKER_HOST": "unix:///var/run/docker.sock",
            "DOCKER_CONFIG": "/var/empty/fstack-docker-cli",
        }
    ]
    assert "ambient-private" not in repr(captured)


def test_rejects_crusher_without_exact_name_or_alias_without_listener_connect() -> None:
    crusher_id = "b" * 64
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
        _container("other-name", crusher_id, "172.20.0.3", [crusher_id, "extra"]),
        _network(crusher_id),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        output = outputs[calls]
        calls += 1
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: pytest.fail("must not connect"),
        )
    assert caught.value.code == "caddy_topology_invalid"
    assert calls == 3


def test_missing_network_is_permanent_topology_invalid() -> None:
    crusher_id = "b" * 64
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
        _container(
            _DESIRED.crusher_container_name,
            crusher_id,
            "172.20.0.3",
            [_DESIRED.crusher_container_name],
        ),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        index = calls
        calls += 1
        if index == 2:
            return SimpleNamespace(
                returncode=1, stdout=b"[]", stderr=b"Error: No such network: redacted"
            )
        return SimpleNamespace(returncode=0, stdout=outputs[index], stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: pytest.fail("must not connect"),
        )
    assert caught.value.code == "caddy_topology_invalid"
    assert caught.value.retryable is False
    assert calls == 3


def test_malformed_crusher_inspection_is_retryable_docker_unavailability() -> None:
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
        b'{"duplicate":1,"duplicate":2}',
        _network("b" * 64),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        output = outputs[calls]
        calls += 1
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: pytest.fail("must not connect"),
        )
    assert caught.value.code == "caddy_unavailable"
    assert caught.value.retryable is True


def test_malformed_network_inspection_is_retryable_docker_unavailability() -> None:
    crusher_id = "b" * 64
    malformed_network = json.loads(_network(crusher_id))
    malformed_network[0]["Id"] = 7
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
        _container(
            _DESIRED.crusher_container_name,
            crusher_id,
            "172.20.0.3",
            [_DESIRED.crusher_container_name],
        ),
        json.dumps(malformed_network).encode(),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        del argv, timeout
        output = outputs[calls]
        calls += 1
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: pytest.fail("must not connect"),
        )
    assert caught.value.code == "caddy_unavailable"
    assert caught.value.retryable is True


def test_caddy_wrong_network_is_permanent_topology_invalid() -> None:
    crusher_id = "b" * 64
    caddy = json.loads(_container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]))
    caddy[0]["NetworkSettings"]["Networks"] = {
        "other": {
            "IPAddress": "127.0.0.2",
            "GlobalIPv6Address": "",
            "Aliases": ["fstack-caddy"],
        }
    }
    outputs = [
        json.dumps(caddy).encode(),
        _container(
            _DESIRED.crusher_container_name,
            crusher_id,
            "172.20.0.3",
            [_DESIRED.crusher_container_name],
        ),
        _network(crusher_id),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        output = outputs[calls]
        calls += 1
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: pytest.fail("must not connect"),
        )
    assert caught.value.code == "caddy_topology_invalid"


def test_invalid_configuration_is_rejected_before_desired_state_io() -> None:
    calls: list[str] = []
    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: calls.append("load") or _DESIRED,
            environ=dict(_ENV, FSTACK_CADDY_PROBE_ADDRESS="localhost:8080"),
            docker_runner=lambda *_args, **_kwargs: calls.append("docker"),
            listener_connector=lambda *_args: calls.append("connect"),
        )
    assert caught.value.code == "caddy_topology_invalid"
    assert calls == []


def test_network_inspection_must_include_exact_caddy_endpoint() -> None:
    crusher_id = "b" * 64
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
        _container(
            _DESIRED.crusher_container_name,
            crusher_id,
            "172.20.0.3",
            [_DESIRED.crusher_container_name],
        ),
        _network(crusher_id, include_caddy=False),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        del argv, timeout
        output = outputs[calls]
        calls += 1
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: pytest.fail("must not connect"),
        )
    assert caught.value.code == "caddy_topology_invalid"
    assert caught.value.retryable is False
    assert calls == 3


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("DOCKER_HOST", "tcp://remote.example:2376"),
        ("DOCKER_CONTEXT", "remote-production"),
        ("DOCKER_CONFIG", "/sensitive/docker-config"),
        ("DOCKER_CERT_PATH", "/sensitive/certs"),
        ("DOCKER_TLS_VERIFY", "1"),
    ],
)
def test_rejects_ambient_remote_docker_selection_before_any_io(key: str, value: str) -> None:
    calls: list[str] = []
    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: calls.append("load") or _DESIRED,
            environ=dict(_ENV, **{key: value}),
            docker_runner=lambda *_args, **_kwargs: calls.append("docker"),
            listener_connector=lambda *_args: calls.append("connect"),
        )
    assert caught.value.code == "caddy_topology_invalid"
    assert caught.value.retryable is False
    assert calls == []
    assert value not in str(caught.value) + repr(caught.value)


@pytest.mark.parametrize(
    ("key", "value"),
    [("DOCKER_HOST", "unix:///var/run/docker.sock"), ("DOCKER_CONTEXT", "default")],
)
def test_accepts_explicit_local_docker_selection(key: str, value: str) -> None:
    crusher_id = "b" * 64
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
        _container(_DESIRED.crusher_container_name, crusher_id, "172.20.0.3", []),
        _network(crusher_id),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        del argv, timeout
        output = outputs[calls]
        calls += 1
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    preflight_local_caddy_topology(
        _IDS,
        desired_loader=lambda _ids: _DESIRED,
        environ=dict(_ENV, **{key: value}),
        docker_runner=runner,
        listener_connector=lambda *_args: _Socket(),
    )
    assert calls == 3


def test_rejects_noncanonical_idna_alabel_before_docker_or_listener_io() -> None:
    calls: list[str] = []
    desired = replace(_DESIRED, public_domain="xn--a.example")
    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: desired,
            environ=_ENV,
            docker_runner=lambda *_args, **_kwargs: calls.append("docker"),
            listener_connector=lambda *_args: calls.append("connect"),
        )
    assert caught.value.code == "caddy_topology_invalid"
    assert caught.value.retryable is False
    assert calls == []


@pytest.mark.parametrize(
    ("code", "retryable"),
    [("caddy_topology_invalid", False), ("caddy_unavailable", True)],
)
def test_orchestration_completes_preflight_before_admin_construction_or_mutation(
    code: str, retryable: bool
) -> None:
    calls: list[str] = []

    def fail_preflight(_ids: ProvisioningSubmissionIds) -> None:
        calls.append("preflight")
        raise CaddyTopologyError(code, retryable)

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_then_reconcile(
            _IDS,
            preflight=fail_preflight,
            route_loader=lambda _ids: calls.append("route") or {},
            admin_factory=lambda: calls.append("factory") or pytest.fail("must not construct"),
        )
    assert caught.value.code == code
    assert caught.value.retryable is retryable
    assert calls == ["preflight"]


@pytest.mark.parametrize(
    ("mode", "code", "retryable"),
    [
        ("permanent", "caddy_topology_invalid", False),
        ("retryable", "caddy_unavailable", True),
    ],
)
def test_orchestration_actual_preflight_failures_leave_admin_transport_untouched(
    mode: str, code: str, retryable: bool
) -> None:
    crusher_id = "b" * 64
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
        _container(_DESIRED.crusher_container_name, crusher_id, "172.20.0.3", []),
        _network(crusher_id),
    ]
    docker_calls = 0
    admin_calls: list[str] = []

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal docker_calls
        del argv, timeout
        output = outputs[docker_calls]
        docker_calls += 1
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    def actual_preflight(ids: ProvisioningSubmissionIds) -> None:
        preflight_local_caddy_topology(
            ids,
            desired_loader=lambda _ids: _DESIRED,
            environ=(
                dict(_ENV, DOCKER_HOST="tcp://remote.example:2376") if mode == "permanent" else _ENV
            ),
            docker_runner=runner,
            listener_connector=(
                (lambda *_args: pytest.fail("must not connect"))
                if mode == "permanent"
                else (lambda *_args: (_ for _ in ()).throw(TimeoutError("private")))
            ),
        )

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_then_reconcile(
            _IDS,
            preflight=actual_preflight,
            route_loader=lambda _ids: admin_calls.append("route") or {},
            admin_factory=lambda: (
                admin_calls.append("factory") or pytest.fail("must not construct")
            ),
        )
    assert (caught.value.code, caught.value.retryable) == (code, retryable)
    assert admin_calls == []
    assert docker_calls == (0 if mode == "permanent" else 3)


def test_orchestration_constructs_and_calls_admin_only_after_successful_preflight() -> None:
    calls: list[str] = []

    class Admin:
        def reconcile(self, route: object) -> None:
            assert route == {"route": "exact"}
            calls.append("reconcile")

    preflight_then_reconcile(
        _IDS,
        preflight=lambda _ids: calls.append("preflight"),
        route_loader=lambda _ids: calls.append("route") or {"route": "exact"},
        admin_factory=lambda: calls.append("factory") or Admin(),
    )
    assert calls == ["preflight", "route", "factory", "reconcile"]


@pytest.mark.parametrize(
    ("target", "code", "retryable"),
    [
        ("caddy", "caddy_unavailable", True),
        ("crusher", "caddy_topology_invalid", False),
    ],
)
def test_nonrunning_container_classification(target: str, code: str, retryable: bool) -> None:
    crusher_id = "b" * 64
    caddy = json.loads(_container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]))
    crusher = json.loads(_container(_DESIRED.crusher_container_name, crusher_id, "172.20.0.3", []))
    selected = caddy if target == "caddy" else crusher
    selected[0]["State"] = {"Running": False, "Status": "exited"}
    outputs = [json.dumps(caddy).encode(), json.dumps(crusher).encode(), _network(crusher_id)]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        del argv, timeout
        output = outputs[calls]
        calls += 1
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: pytest.fail("must not connect"),
        )
    assert (caught.value.code, caught.value.retryable) == (code, retryable)
    assert calls == 3


def test_probe_ip_mismatch_is_permanent_topology_invalid() -> None:
    crusher_id = "b" * 64
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.9", ["fstack-caddy"]),
        _container(_DESIRED.crusher_container_name, crusher_id, "172.20.0.3", []),
        _network(crusher_id),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        del argv, timeout
        output = outputs[calls]
        calls += 1
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: pytest.fail("must not connect"),
        )
    assert caught.value.code == "caddy_topology_invalid"
    assert caught.value.retryable is False


@pytest.mark.parametrize("failure", [OSError, TimeoutError])
def test_listener_unavailable_or_timeout_is_retryable(failure: type[Exception]) -> None:
    crusher_id = "b" * 64
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
        _container(_DESIRED.crusher_container_name, crusher_id, "172.20.0.3", []),
        _network(crusher_id),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        del argv, timeout
        output = outputs[calls]
        calls += 1
        return SimpleNamespace(returncode=0, stdout=output, stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: (_ for _ in ()).throw(failure("private")),
        )
    assert caught.value.code == "caddy_unavailable"
    assert caught.value.retryable is True


@pytest.mark.parametrize(
    ("index", "mode", "code", "retryable"),
    [
        (0, "timeout", "caddy_unavailable", True),
        (1, "timeout", "caddy_unavailable", True),
        (2, "timeout", "caddy_unavailable", True),
        (0, "malformed", "caddy_unavailable", True),
        (1, "malformed", "caddy_unavailable", True),
        (2, "malformed", "caddy_unavailable", True),
        (0, "not_found", "caddy_unavailable", True),
        (1, "not_found", "caddy_topology_invalid", False),
        (2, "not_found", "caddy_topology_invalid", False),
    ],
)
def test_inspection_failure_classification_matrix(
    index: int, mode: str, code: str, retryable: bool
) -> None:
    crusher_id = "b" * 64
    outputs = [
        _container("fstack-caddy", "a" * 64, "127.0.0.2", ["fstack-caddy"]),
        _container(_DESIRED.crusher_container_name, crusher_id, "172.20.0.3", []),
        _network(crusher_id),
    ]
    calls = 0

    def runner(argv: list[str], *, timeout: float) -> SimpleNamespace:
        nonlocal calls
        del argv, timeout
        current = calls
        calls += 1
        if current == index and mode == "timeout":
            raise TimeoutError("private")
        if current == index and mode == "malformed":
            return SimpleNamespace(returncode=0, stdout=b"not-json", stderr=b"")
        if current == index:
            return SimpleNamespace(returncode=1, stdout=b"", stderr=b"No such object")
        return SimpleNamespace(returncode=0, stdout=outputs[current], stderr=b"")

    with pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: pytest.fail("must not connect"),
        )
    assert (caught.value.code, caught.value.retryable) == (code, retryable)


def test_topology_failure_sanitizes_exception_logs_and_traceback_locals(
    caplog: pytest.LogCaptureFixture,
) -> None:
    marker = "docker-topology-secret-marker"

    def runner(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError(marker)

    with caplog.at_level(logging.DEBUG), pytest.raises(CaddyTopologyError) as caught:
        preflight_local_caddy_topology(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            docker_runner=runner,
            listener_connector=lambda *_args: pytest.fail("must not connect"),
        )
    rendered = str(caught.value) + repr(caught.value) + caplog.text
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    current = caught.value.__traceback__
    while current is not None:
        if current.tb_frame.f_globals.get("__name__") == caddy_topology.__name__:
            rendered += repr(current.tb_frame.f_locals)
        current = current.tb_next
    assert marker not in rendered


@pytest.mark.parametrize(
    ("external_id", "failure_stage", "expected_code"),
    [
        (None, None, "success"),
        ("wrong-container", None, "caddy_topology_invalid"),
        (None, "begin", "caddy_unavailable"),
        (None, "enter", "caddy_unavailable"),
        (None, "statement", "caddy_unavailable"),
        (None, "query", "caddy_unavailable"),
        (None, "mappings", "caddy_unavailable"),
        (None, "one", "caddy_unavailable"),
        (None, "exit", "caddy_unavailable"),
        (None, "malformed_row", "caddy_unavailable"),
        (None, "dispose", "caddy_unavailable"),
    ],
)
def test_production_loader_validates_identity_and_disposal(
    monkeypatch: pytest.MonkeyPatch,
    external_id: str | None,
    failure_stage: str | None,
    expected_code: str,
) -> None:
    operation_id = UUID(_IDS.operation_id)
    runtime_id = UUID(_IDS.runtime_submission_id)
    ingress_id = UUID(_IDS.ingress_submission_id)
    deployment_id = _DESIRED.deployment_id
    organization_id = UUID("55555555-5555-4555-8555-555555555555")
    api_name, migration_name = runtime_submission._container_names(deployment_id)
    operation = {
        "operation_id": operation_id,
        "organization_id": organization_id,
        "deployment_id": deployment_id,
        "crusher_image": "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
        "public_domain": _DESIRED.public_domain,
        "database_secret_ref": "secret://local/database",
        "object_storage_secret_ref": "secret://local/storage",
    }
    spec = runtime_submission._canonical_spec(
        operation,
        api_container_name=api_name,
        migration_container_name=migration_name,
        docker_network="fstack-local",
        secret_stage_root=caddy_topology.Path("/var/lib/fstack/secrets"),
    )
    now = datetime.now(UTC)
    row = {
        "operation_id": operation_id,
        "operation_organization_id": organization_id,
        "operation_deployment_id": deployment_id,
        "operation_state": "running",
        "operation_started_at": now,
        "operation_completed_at": None,
        "operation_failure_code": None,
        "runtime_id": runtime_id,
        "runtime_organization_id": organization_id,
        "runtime_deployment_id": deployment_id,
        "runtime_provider": runtime_submission.PROVIDER,
        "runtime_generation": runtime_submission.GENERATION,
        "runtime_submission_key": runtime_submission._submission_key(operation_id),
        "spec_fingerprint": runtime_submission._spec_fingerprint(spec),
        "api_container_name": api_name,
        "migration_container_name": migration_name,
        "runtime_state": "submitted",
        "runtime_external_id": "b" * 64 if external_id is None else external_id,
        "runtime_prepared_at": now,
        "runtime_submitted_at": now,
        "runtime_confirmed_at": None,
        "runtime_failed_at": None,
        "runtime_failure_code": None,
        "ingress_id": ingress_id,
        "ingress_organization_id": organization_id,
        "ingress_deployment_id": deployment_id,
        "ingress_provider": ingress_submission.PROVIDER,
        "ingress_generation": ingress_submission.GENERATION,
        "ingress_submission_key": ingress_submission._submission_key(operation_id),
        "route_fingerprint": ingress_submission._route_fingerprint(
            ingress_submission._canonical_route(deployment_id, _DESIRED.public_domain)
        ),
        "route_id": ingress_submission._route_id(deployment_id),
        "ingress_state": "submitted",
        "ingress_prepared_at": now,
        "ingress_submitted_at": now,
        "ingress_confirmed_at": None,
        "ingress_failed_at": None,
        "ingress_failure_code": None,
        "deployment_id": deployment_id,
        "deployment_organization_id": organization_id,
        "crusher_image": operation["crusher_image"],
        "public_domain": operation["public_domain"],
        "database_secret_ref": operation["database_secret_ref"],
        "object_storage_secret_ref": operation["object_storage_secret_ref"],
    }

    class Result:
        def mappings(self) -> Result:
            if failure_stage == "mappings":
                raise RuntimeError("database-mappings-secret-marker")
            return self

        def one_or_none(self) -> dict[str, object]:
            if failure_stage == "one":
                raise RuntimeError("database-one-secret-marker")
            if failure_stage == "malformed_row":
                return {}
            return row

    class Connection:
        calls = 0
        statements: list[object] = []

        def execute(self, statement: object, *_args: object, **_kwargs: object) -> Result:
            self.calls += 1
            self.statements.append(statement)
            if failure_stage == "statement" and self.calls == 1:
                raise RuntimeError("database-statement-secret-marker")
            if failure_stage == "query" and self.calls == 2:
                raise RuntimeError("database-query-secret-marker")
            return Result()

    connection = Connection()

    class Begin:
        def __enter__(self) -> Connection:
            if failure_stage == "enter":
                raise RuntimeError("database-enter-secret-marker")
            return connection

        def __exit__(self, *_args: object) -> None:
            if failure_stage == "exit":
                raise RuntimeError("database-exit-secret-marker")
            return None

    class Engine:
        dispose_calls = 0

        def begin(self) -> Begin:
            if failure_stage == "begin":
                raise RuntimeError("database-begin-secret-marker")
            return Begin()

        def dispose(self) -> None:
            self.dispose_calls += 1
            if failure_stage == "dispose":
                raise RuntimeError("database-dispose-secret-marker")

    engine = Engine()
    options: list[dict[str, object]] = []

    def create(*_args: object, **kwargs: object) -> Engine:
        options.append(kwargs)
        return engine

    monkeypatch.setattr(caddy_topology, "create_engine", create)
    monkeypatch.setattr(runtime_submission, "_database_url", lambda: "postgresql://redacted")

    if expected_code == "success":
        assert (
            caddy_topology._load_desired_state(
                _IDS,
                require_ingress_submitted=True,
                environ={
                    "FSTACK_DOCKER_NETWORK": "fstack-local",
                    "FSTACK_SECRET_STAGE_ROOT": "/var/lib/fstack/secrets",
                },
            )
            == _DESIRED
        )
    else:
        with pytest.raises(CaddyTopologyError) as caught:
            caddy_topology._load_desired_state(
                _IDS,
                require_ingress_submitted=True,
                environ={
                    "FSTACK_DOCKER_NETWORK": "fstack-local",
                    "FSTACK_SECRET_STAGE_ROOT": "/var/lib/fstack/secrets",
                },
            )
        assert caught.value.code == expected_code
        assert caught.value.__cause__ is None
        assert caught.value.__context__ is None
        rendered = str(caught.value) + repr(caught.value)
        current = caught.value.__traceback__
        while current is not None:
            if current.tb_frame.f_globals.get("__name__") == caddy_topology.__name__:
                rendered += repr(current.tb_frame.f_locals)
            current = current.tb_next
        assert "secret-marker" not in rendered
    assert options == [{"pool_pre_ping": True, "hide_parameters": True}]
    assert engine.dispose_calls == 1
    if len(connection.statements) > 1:
        assert "operation.last_error_code AS operation_failure_code" in str(
            connection.statements[1]
        )


@pytest.mark.parametrize("failure_stage", ["database_url", "create"])
def test_production_loader_sanitizes_engine_construction_failures(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
) -> None:
    marker = f"database-{failure_stage}-secret-marker"
    environment_marker = "database-environment-secret-marker"

    def database_url() -> str:
        if failure_stage == "database_url":
            raise RuntimeError(marker)
        return "postgresql://redacted"

    def create(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError(marker)

    monkeypatch.setattr(runtime_submission, "_database_url", database_url)
    monkeypatch.setattr(caddy_topology, "create_engine", create)
    with pytest.raises(CaddyTopologyError) as caught:
        caddy_topology._load_desired_state(
            _IDS,
            require_ingress_submitted=True,
            environ={
                "FSTACK_DOCKER_NETWORK": "fstack-local",
                "FSTACK_SECRET_STAGE_ROOT": f"/var/lib/fstack/{environment_marker}",
            },
        )
    assert caught.value.code == "caddy_unavailable"
    rendered = str(caught.value) + repr(caught.value)
    current = caught.value.__traceback__
    while current is not None:
        if current.tb_frame.f_globals.get("__name__") == caddy_topology.__name__:
            rendered += repr(current.tb_frame.f_locals)
        current = current.tb_next
    assert marker not in rendered
    assert environment_marker not in rendered


def test_production_loader_does_not_catch_base_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runtime_submission,
        "_database_url",
        lambda: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    with pytest.raises(KeyboardInterrupt):
        caddy_topology._load_desired_state(
            _IDS,
            require_ingress_submitted=True,
            environ={
                "FSTACK_DOCKER_NETWORK": "fstack-local",
                "FSTACK_SECRET_STAGE_ROOT": "/var/lib/fstack/secrets",
            },
        )


def test_disposal_exception_does_not_swallow_active_base_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Result:
        def mappings(self) -> Result:
            raise KeyboardInterrupt

    class Connection:
        def execute(self, *_args: object, **_kwargs: object) -> Result:
            return Result()

    class Begin:
        def __enter__(self) -> Connection:
            return Connection()

        def __exit__(self, *_args: object) -> None:
            return None

    class Engine:
        def begin(self) -> Begin:
            return Begin()

        def dispose(self) -> None:
            raise RuntimeError("dispose-must-not-mask-base-exception")

    monkeypatch.setattr(caddy_topology, "create_engine", lambda *_args, **_kwargs: Engine())
    monkeypatch.setattr(runtime_submission, "_database_url", lambda: "postgresql://redacted")
    with pytest.raises(KeyboardInterrupt):
        caddy_topology._load_desired_state(
            _IDS,
            require_ingress_submitted=True,
            environ={
                "FSTACK_DOCKER_NETWORK": "fstack-local",
                "FSTACK_SECRET_STAGE_ROOT": "/var/lib/fstack/secrets",
            },
        )
