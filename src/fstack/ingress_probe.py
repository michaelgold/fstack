"""Exact routed Crusher health probe through the configured local Caddy listener."""

from __future__ import annotations

import json
import os
import socket
import time
from collections.abc import Callable, Mapping
from typing import Protocol, cast

from fstack.caddy_topology import (
    Clock,
    DesiredLoader,
    ProvisioningSubmissionIds,
    _desired,
    _load_installation_config,
    _parse_ids,
    _remaining,
)
from fstack.http_framing import _perform_exchange

_PROBE_CONNECT_TIMEOUT_SECONDS = 5.0
_PROBE_TOTAL_TIMEOUT_SECONDS = 10.0
_PROBE_HEADER_LIMIT = 16 * 1024
_PROBE_BODY_LIMIT = 1024


class _HTTPSocket(Protocol):
    def settimeout(self, value: float | None, /) -> None: ...

    def send(self, data: bytes, /) -> int: ...

    def recv(self, size: int, /) -> bytes: ...

    def close(self) -> None: ...


Connector = Callable[[str, int, float], _HTTPSocket]


class IngressProbeError(RuntimeError):
    """A sanitized ingress-scoped routed-readiness failure."""

    def __init__(self, code: str, retryable: bool) -> None:
        self.code = code
        self.retryable = retryable
        self.scope = "ingress"
        super().__init__(code)


def _default_connector(host: str, port: int, timeout: float) -> _HTTPSocket:
    return socket.create_connection((host, port), timeout=timeout)


def _strict_health_body(raw: bytes) -> bool:
    if type(raw) is not bytes or len(raw) > _PROBE_BODY_LIMIT:
        return False

    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if type(key) is not str or key in value:
                raise ValueError
            value[key] = item
        return value

    try:
        decoded = raw.decode("utf-8", "strict")
        value = json.loads(
            decoded,
            object_pairs_hook=object_pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (UnicodeError, ValueError, TypeError, json.JSONDecodeError):
        return False
    return (
        type(value) is dict
        and set(value) == {"status", "service"}
        and type(value["status"]) is str
        and type(value["service"]) is str
        and value["status"] == "ok"
        and value["service"] == "crusher"
    )


def _probe_outcome(
    ids: ProvisioningSubmissionIds,
    *,
    desired_loader: DesiredLoader | None,
    environ: Mapping[str, object],
    connector: Connector,
    clock: Clock,
) -> tuple[str, bool] | None:
    try:
        load_deadline = clock() + _PROBE_TOTAL_TIMEOUT_SECONDS
    except Exception:
        return "ingress_readiness_unavailable", True
    config = _load_installation_config(environ, require_container=False)
    if config is None:
        return "invalid_caddy_probe_configuration", False
    loaded = _desired(
        ids,
        desired_loader=desired_loader,
        deadline=load_deadline,
        clock=clock,
        environ=environ,
        require_ingress_submitted=True,
    )
    desired = loaded.desired
    if desired is None:
        return "ingress_readiness_unavailable", True
    if config.docker_network != desired.docker_network:
        return "invalid_caddy_probe_configuration", False
    request = (
        f"GET /health HTTP/1.1\r\nHost: {desired.public_domain}\r\n"
        "Accept: application/json\r\nConnection: close\r\n\r\n"
    ).encode("ascii")

    def exchange_connector(timeout: float) -> _HTTPSocket:
        return connector(config.probe_host, config.probe_port, timeout)

    remaining = _remaining(load_deadline, clock)
    if remaining is None:
        return "ingress_readiness_unavailable", True
    try:
        outcome = _perform_exchange(
            request,
            connector=exchange_connector,
            connect_timeout=_PROBE_CONNECT_TIMEOUT_SECONDS,
            total_timeout=remaining,
            header_limit=_PROBE_HEADER_LIMIT,
            body_limit=_PROBE_BODY_LIMIT,
        )
    except Exception:
        return "ingress_readiness_unavailable", True
    response = outcome.response
    if outcome.error is not None or response is None:
        return "ingress_readiness_unavailable", True
    if response.status != 200 or not _strict_health_body(response.body):
        return "ingress_readiness_unavailable", True
    return None


def probe_ingress(
    ids: ProvisioningSubmissionIds,
    *,
    desired_loader: DesiredLoader | None = None,
    environ: Mapping[str, object] | None = None,
    connector: Connector = _default_connector,
    clock: Clock = time.monotonic,
) -> None:
    """Probe the exact public host route through the configured Caddy listener."""
    if _parse_ids(ids) is None:
        ids = None  # type: ignore[assignment]
        desired_loader = None
        environ = None
        connector = cast(Connector, None)
        clock = cast(Clock, None)
        raise IngressProbeError("invalid_provisioning_submission_ids", False) from None
    values = os.environ if environ is None else environ
    outcome = _probe_outcome(
        ids,
        desired_loader=desired_loader,
        environ=values,
        connector=connector,
        clock=clock,
    )
    if outcome is None:
        return None
    code, retryable = outcome
    ids = None  # type: ignore[assignment]
    desired_loader = None
    environ = None
    values = None  # type: ignore[assignment]
    connector = cast(Connector, None)
    clock = cast(Clock, None)
    outcome = None
    raise IngressProbeError(code, retryable) from None
