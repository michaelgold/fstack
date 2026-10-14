from __future__ import annotations

import json
import socket
import threading
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import pytest

import fstack.http_framing as http_framing
import fstack.ingress_probe as ingress_probe
from fstack.caddy_topology import (
    CaddyTopologyError,
    DesiredCaddyTopology,
    ProvisioningSubmissionIds,
)
from fstack.ingress_probe import IngressProbeError, probe_ingress

_IDS = ProvisioningSubmissionIds(
    operation_id="11111111-1111-4111-8111-111111111111",
    runtime_submission_id="22222222-2222-4222-8222-222222222222",
    ingress_submission_id="33333333-3333-4333-8333-333333333333",
)
_DESIRED = DesiredCaddyTopology(
    deployment_id=UUID("44444444-4444-4444-8444-444444444444"),
    crusher_container_name="fstack-crusher-44444444444444448444444444444444",
    public_domain="mail.acme.example",
    docker_network="fstack-local",
)
_ENV = {
    "FSTACK_CADDY_PROBE_ADDRESS": "127.0.0.1:8080",
    "FSTACK_DOCKER_NETWORK": "fstack-local",
}


class _Socket:
    def __init__(self, response: bytes) -> None:
        self.response = bytearray(response)
        self.sent = bytearray()
        self.closed = False
        self.timeouts: list[float | None] = []

    def settimeout(self, value: float | None) -> None:
        self.timeouts.append(value)

    def send(self, data: bytes) -> int:
        self.sent.extend(data)
        return len(data)

    def recv(self, size: int) -> bytes:
        if not self.response:
            return b""
        result = bytes(self.response[:size])
        del self.response[:size]
        return result

    def close(self) -> None:
        self.closed = True


class _DeadlineSocket(_Socket):
    def __init__(
        self,
        now: list[float],
        recv_steps: list[tuple[float, bytes]],
        *,
        send_steps: list[tuple[float, int | None]] | None = None,
    ) -> None:
        super().__init__(b"")
        self.now = now
        self.recv_steps = recv_steps
        self.send_steps = send_steps or [(0.0, None)]

    def send(self, data: bytes) -> int:
        delay, count = self.send_steps.pop(0)
        self.now[0] += delay
        actual = len(data) if count is None else min(count, len(data))
        self.sent.extend(data[:actual])
        return actual

    def recv(self, size: int) -> bytes:
        if not self.recv_steps:
            return b""
        delay, data = self.recv_steps.pop(0)
        self.now[0] += delay
        result = data[:size]
        if len(result) < len(data):
            self.recv_steps.insert(0, (0.0, data[len(result) :]))
        return result


def _response(body: bytes, *, status: int = 200, framing: str = "length") -> bytes:
    if framing == "length":
        return f"HTTP/1.1 {status} OK\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body
    if framing == "chunked":
        return (
            f"HTTP/1.1 {status} OK\r\nTransfer-Encoding: chunked\r\n\r\n".encode()
            + f"{len(body):x}\r\n".encode()
            + body
            + b"\r\n0\r\n\r\n"
        )
    return f"HTTP/1.0 {status} OK\r\n\r\n".encode() + body


@pytest.mark.parametrize("framing", ["length", "chunked", "close"])
def test_probe_sends_exact_routed_health_request_and_accepts_all_supported_framing(
    framing: str,
) -> None:
    body = json.dumps({"status": "ok", "service": "crusher"}, separators=(",", ":")).encode()
    sock = _Socket(_response(body, framing=framing))
    connects: list[tuple[str, int, float]] = []

    def connector(host: str, port: int, timeout: float) -> _Socket:
        connects.append((host, port, timeout))
        return sock

    assert (
        probe_ingress(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            connector=connector,
        )
        is None
    )
    assert bytes(sock.sent) == (
        b"GET /health HTTP/1.1\r\nHost: mail.acme.example\r\nAccept: application/json\r\n"
        b"Connection: close\r\n\r\n"
    )
    assert len(connects) == 1
    assert connects[0][:2] == ("127.0.0.1", 8080)
    assert 0 < connects[0][2] <= 5
    assert sock.closed


@pytest.mark.parametrize(
    "body",
    [
        b'{"status":"ok","service":"crusher","extra":true}',
        b'{"status":"ok","status":"ok","service":"crusher"}',
        b'{"status":"OK","service":"crusher"}',
        b'{"status":"ok","service":"other"}',
        b"\xff",
        b"[]",
    ],
)
def test_probe_rejects_every_nonexact_body_as_retryable(body: bytes) -> None:
    sock = _Socket(_response(body))
    with pytest.raises(IngressProbeError) as caught:
        probe_ingress(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            connector=lambda *_args: sock,
        )
    assert caught.value.code == "ingress_readiness_unavailable"
    assert caught.value.retryable is True
    assert caught.value.scope == "ingress"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert sock.closed


def test_probe_rejects_non_200_as_retryable() -> None:
    sock = _Socket(_response(b'{"status":"ok","service":"crusher"}', status=503))
    with pytest.raises(IngressProbeError) as caught:
        probe_ingress(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            connector=lambda *_args: sock,
        )
    assert caught.value.code == "ingress_readiness_unavailable"
    assert caught.value.retryable


@pytest.mark.parametrize(
    "response",
    [
        b"HTTP/1.1 200 OK\r\nX-Oversized: " + b"x" * (16 * 1024) + b"\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: 1025\r\n\r\n" + b"x" * 1025,
    ],
)
def test_probe_enforces_header_and_body_limits(response: bytes) -> None:
    sock = _Socket(response)
    with pytest.raises(IngressProbeError) as caught:
        probe_ingress(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            connector=lambda *_args: sock,
        )
    assert caught.value.code == "ingress_readiness_unavailable"
    assert caught.value.retryable is True
    assert sock.closed


def test_probe_maps_hostile_socket_result_shape_to_sanitized_readiness_failure() -> None:
    marker = "hostile-socket-result-secret-marker"

    class HostileSocket(_Socket):
        def send(self, data: bytes) -> int:
            del data
            return marker  # type: ignore[return-value]

    sock = HostileSocket(_response(b'{"status":"ok","service":"crusher"}'))
    with pytest.raises(IngressProbeError) as caught:
        probe_ingress(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            connector=lambda *_args: sock,
        )
    assert caught.value.code == "ingress_readiness_unavailable"
    assert caught.value.retryable is True
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    rendered = str(caught.value) + repr(caught.value)
    current = caught.value.__traceback__
    while current is not None:
        if current.tb_frame.f_globals.get("__name__") == ingress_probe.__name__:
            rendered += repr(current.tb_frame.f_locals)
        current = current.tb_next
    assert marker not in rendered
    assert sock.closed


@pytest.mark.parametrize(
    "address",
    [
        "localhost:8080",
        "0.0.0.0:8080",
        "http://127.0.0.1:8080",
        "user@127.0.0.1:8080",
        "127.0.0.1:8080/health",
        "127.0.0.1:8080?query",
        "127.0.0.1:080",
        "127.0.0.1:0",
        "127.0.0.1:65536",
        "127.0.0.1 :8080",
        "2001:db8::1:8080",
        "[2001:0db8::1]:8080",
        "[::]:8080",
        "[fe80::1%eth0]:8080",
    ],
)
def test_probe_rejects_invalid_installation_address_permanently(address: str) -> None:
    calls: list[str] = []
    with pytest.raises(IngressProbeError) as caught:
        probe_ingress(
            _IDS,
            desired_loader=lambda _ids: calls.append("load") or _DESIRED,
            environ=dict(_ENV, FSTACK_CADDY_PROBE_ADDRESS=address),
            connector=lambda *_args: pytest.fail("must not connect"),
        )
    assert caught.value.code == "invalid_caddy_probe_configuration"
    assert caught.value.retryable is False
    assert calls == []


def test_probe_rejects_payload_before_loader_or_network() -> None:
    calls: list[str] = []
    bad = replace(_IDS, ingress_submission_id="not-a-uuid")
    with pytest.raises(IngressProbeError) as caught:
        probe_ingress(
            bad,
            desired_loader=lambda _ids: calls.append("load") or _DESIRED,
            environ=_ENV,
            connector=lambda *_args: calls.append("connect"),
        )
    assert caught.value.code == "invalid_provisioning_submission_ids"
    assert caught.value.retryable is False
    assert calls == []


def test_probe_maps_desired_state_failure_to_retryable_readiness() -> None:
    def fail(_ids: ProvisioningSubmissionIds) -> DesiredCaddyTopology:
        raise CaddyTopologyError("caddy_topology_invalid", False)

    with pytest.raises(IngressProbeError) as caught:
        probe_ingress(
            _IDS,
            desired_loader=fail,
            environ=_ENV,
            connector=lambda *_args: pytest.fail("must not connect"),
        )
    assert caught.value.code == "ingress_readiness_unavailable"
    assert caught.value.retryable is True


def test_probe_does_not_catch_base_exception() -> None:
    with pytest.raises(KeyboardInterrupt):
        probe_ingress(
            _IDS,
            desired_loader=lambda _ids: (_ for _ in ()).throw(KeyboardInterrupt()),
            environ=_ENV,
            connector=lambda *_args: pytest.fail("must not connect"),
        )


def test_probe_passes_only_remaining_absolute_budget_to_http_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    moments = iter([100.0, 103.0, 103.0, 103.0])
    captured: list[float] = []

    def exchange(*_args: object, **kwargs: object) -> object:
        total_timeout = kwargs["total_timeout"]
        assert type(total_timeout) is float
        captured.append(total_timeout)
        return SimpleNamespace(
            error=None,
            response=SimpleNamespace(
                status=200,
                body=b'{"status":"ok","service":"crusher"}',
            ),
        )

    monkeypatch.setattr(ingress_probe, "_perform_exchange", exchange)
    assert (
        probe_ingress(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            connector=lambda *_args: pytest.fail("patched exchange must own transport"),
            clock=lambda: next(moments),
        )
        is None
    )
    assert captured == [pytest.approx(7.0)]


@pytest.mark.parametrize(
    ("stage", "recv_steps", "send_steps", "connect_delay"),
    [
        ("slow_connect", [], [(0.0, None)], 6.0),
        ("blocked_send", [], [(11.0, None)], 0.0),
        ("partial_send", [], [(6.0, 1), (5.0, None)], 0.0),
        (
            "slow_headers",
            [
                (
                    11.0,
                    b"HTTP/1.1 200 OK\r\nContent-Length: 35\r\n\r\n"
                    b'{"status":"ok","service":"crusher"}',
                )
            ],
            [(0.0, None)],
            0.0,
        ),
        (
            "slow_content_length_body",
            [
                (0.0, b"HTTP/1.1 200 OK\r\nContent-Length: 35\r\n\r\n"),
                (11.0, b'{"status":"ok","service":"crusher"}'),
            ],
            [(0.0, None)],
            0.0,
        ),
        (
            "slow_chunks_and_trailers",
            [
                (0.0, b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"),
                (0.0, b"23\r\n" + b'{"status":"ok","service":"crusher"}' + b"\r\n0\r\n"),
                (11.0, b"X-Test: yes\r\n\r\n"),
            ],
            [(0.0, None)],
            0.0,
        ),
        (
            "slow_close_delimited_eof",
            [(0.0, b'HTTP/1.0 200 OK\r\n\r\n{"status":"ok","service":"crusher"}'), (11.0, b"")],
            [(0.0, None)],
            0.0,
        ),
        (
            "slow_trailing_byte_validation",
            [
                (
                    0.0,
                    b"HTTP/1.1 200 OK\r\nContent-Length: 35\r\n\r\n"
                    b'{"status":"ok","service":"crusher"}',
                ),
                (11.0, b""),
            ],
            [(0.0, None)],
            0.0,
        ),
    ],
)
def test_probe_public_boundary_enforces_one_deadline_across_every_transport_phase(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    recv_steps: list[tuple[float, bytes]],
    send_steps: list[tuple[float, int | None]],
    connect_delay: float,
) -> None:
    del stage
    now = [100.0]
    sock = _DeadlineSocket(now, recv_steps.copy(), send_steps=send_steps.copy())
    monkeypatch.setattr(http_framing.time, "monotonic", lambda: now[0])

    def connector(_host: str, _port: int, _timeout: float) -> _DeadlineSocket:
        now[0] += connect_delay
        return sock

    with pytest.raises(IngressProbeError) as caught:
        probe_ingress(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=_ENV,
            connector=connector,
            clock=lambda: now[0],
        )
    assert caught.value.code == "ingress_readiness_unavailable"
    assert caught.value.retryable is True
    assert now[0] > 105.0
    assert sock.closed


def test_probe_public_boundary_observes_decreasing_positive_timeouts_for_mutation_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [100.0]
    body = b'{"status":"ok","service":"crusher"}'
    sock = _DeadlineSocket(
        now,
        [
            (1.0, f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\n\r\n".encode()),
            (1.0, body),
            (1.0, b""),
        ],
        send_steps=[(1.0, 20), (1.0, None)],
    )
    monkeypatch.setattr(http_framing.time, "monotonic", lambda: now[0])

    probe_ingress(
        _IDS,
        desired_loader=lambda _ids: _DESIRED,
        environ=_ENV,
        connector=lambda *_args: sock,
        clock=lambda: now[0],
    )
    observed = [value for value in sock.timeouts if value is not None]
    assert observed
    assert all(value > 0 for value in observed)
    assert all(later < earlier for earlier, later in zip(observed, observed[1:], strict=False))
    assert now[0] == pytest.approx(105.0)


def test_probe_supports_bracketed_canonical_ipv6() -> None:
    body = b'{"status":"ok","service":"crusher"}'
    sock = _Socket(_response(body))
    seen: list[tuple[str, int]] = []

    def connector(host: str, port: int, timeout: float) -> _Socket:
        seen.append((host, port))
        return sock

    probe_ingress(
        _IDS,
        desired_loader=lambda _ids: _DESIRED,
        environ=dict(_ENV, FSTACK_CADDY_PROBE_ADDRESS="[2001:db8::1]:8080"),
        connector=connector,
    )
    assert seen == [("2001:db8::1", 8080)]


def test_probe_uses_real_tcp_server_and_exact_host_route() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    received: list[bytes] = []

    def serve() -> None:
        connection, _address = server.accept()
        try:
            request = bytearray()
            while b"\r\n\r\n" not in request:
                request.extend(connection.recv(4096))
            received.append(bytes(request))
            body = b'{"status":"ok","service":"crusher"}'
            connection.sendall(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                + f"{len(body):x}\r\n".encode()
                + body
                + b"\r\n0\r\n\r\n"
            )
        finally:
            connection.close()
            server.close()

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        probe_ingress(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=dict(_ENV, FSTACK_CADDY_PROBE_ADDRESS=f"127.0.0.1:{port}"),
            connector=lambda host, target_port, timeout: socket.create_connection(
                (host, target_port), timeout=timeout
            ),
        )
    finally:
        thread.join(timeout=2)
        server.close()
    assert received == [
        b"GET /health HTTP/1.1\r\nHost: mail.acme.example\r\nAccept: application/json\r\n"
        b"Connection: close\r\n\r\n"
    ]
    assert not thread.is_alive()


def test_probe_uses_real_ipv6_tcp_server_when_available() -> None:
    if not socket.has_ipv6:
        pytest.skip("IPv6 is unavailable")
    server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind(("::1", 0))
    except OSError:
        server.close()
        pytest.skip("IPv6 loopback is unavailable")
    server.listen(1)
    port = server.getsockname()[1]
    received: list[bytes] = []

    def serve() -> None:
        connection, _address = server.accept()
        try:
            request = bytearray()
            while b"\r\n\r\n" not in request:
                request.extend(connection.recv(4096))
            received.append(bytes(request))
            body = b'{"status":"ok","service":"crusher"}'
            connection.sendall(
                f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body
            )
        finally:
            connection.close()
            server.close()

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        probe_ingress(
            _IDS,
            desired_loader=lambda _ids: _DESIRED,
            environ=dict(_ENV, FSTACK_CADDY_PROBE_ADDRESS=f"[::1]:{port}"),
            connector=lambda host, target_port, timeout: socket.create_connection(
                (host, target_port), timeout=timeout
            ),
        )
    finally:
        thread.join(timeout=2)
        server.close()
    assert received == [
        b"GET /health HTTP/1.1\r\nHost: mail.acme.example\r\nAccept: application/json\r\n"
        b"Connection: close\r\n\r\n"
    ]
    assert not thread.is_alive()
