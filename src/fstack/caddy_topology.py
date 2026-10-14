"""Read-only local Caddy/Crusher Docker topology preflight."""

from __future__ import annotations

import ipaddress
import json
import os
import re
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import UUID

import idna
from sqlalchemy import Engine, create_engine, text

from fstack import ingress_submission, runtime_submission

PRELIGHT_TIMEOUT_BUDGET = timedelta(seconds=40)
PRELIGHT_ACTIVITY_START_TO_CLOSE_TIMEOUT = timedelta(seconds=60)
if PRELIGHT_ACTIVITY_START_TO_CLOSE_TIMEOUT <= PRELIGHT_TIMEOUT_BUDGET:
    raise RuntimeError("preflight activity timeout must exceed its timeout budget")

_DOCKER_INSPECT_TIMEOUT_SECONDS = 10.0
_LISTENER_CONNECT_TIMEOUT_SECONDS = 5.0
_MAX_DOCKER_OUTPUT = 1_000_000
_RESOURCE_NAME = re.compile(r"[a-z0-9](?:[a-z0-9_.-]{0,126}[a-z0-9])?\Z")
_HEX_ID = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class ProvisioningSubmissionIds:
    operation_id: str
    runtime_submission_id: str
    ingress_submission_id: str


@dataclass(frozen=True)
class DesiredCaddyTopology:
    deployment_id: UUID
    crusher_container_name: str
    public_domain: str
    docker_network: str


class CaddyTopologyError(RuntimeError):
    """A sanitized ingress-scoped topology failure."""

    def __init__(self, code: str, retryable: bool) -> None:
        self.code = code
        self.retryable = retryable
        self.scope = "ingress"
        super().__init__(code)


class _Socket(Protocol):
    def close(self) -> None: ...


class _CaddyReconciler(Protocol):
    def reconcile(self, route: Mapping[str, object]) -> None: ...


class _RunResult(Protocol):
    returncode: int
    stdout: bytes
    stderr: bytes


DockerRunner = Callable[..., _RunResult]
ListenerConnector = Callable[[str, int, float], _Socket]
DesiredLoader = Callable[[ProvisioningSubmissionIds], DesiredCaddyTopology]
Clock = Callable[[], float]


@dataclass(frozen=True)
class _ParsedIds:
    operation_id: UUID
    runtime_submission_id: UUID
    ingress_submission_id: UUID


@dataclass(frozen=True)
class _InstallationConfig:
    caddy_container_name: str
    probe_host: str
    probe_port: int
    docker_network: str


@dataclass(frozen=True)
class _ContainerEndpoint:
    identifier: str
    name: str
    ip: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class _LoadOutcome:
    desired: DesiredCaddyTopology | None = None
    code: str | None = None
    retryable: bool = False


def _canonical_uuid(value: object) -> UUID | None:
    if type(value) is not str:
        return None
    parsed: UUID | None = None
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError, TypeError):
        return None
    return parsed if value == str(parsed) else None


def _parse_ids(ids: object) -> _ParsedIds | None:
    if type(ids) is not ProvisioningSubmissionIds:
        return None
    operation_id = _canonical_uuid(ids.operation_id)
    runtime_id = _canonical_uuid(ids.runtime_submission_id)
    ingress_id = _canonical_uuid(ids.ingress_submission_id)
    if operation_id is None or runtime_id is None or ingress_id is None:
        return None
    return _ParsedIds(operation_id, runtime_id, ingress_id)


def _parse_port(raw: str) -> int | None:
    if not raw or (len(raw) > 1 and raw[0] == "0") or not raw.isascii() or not raw.isdigit():
        return None
    if len(raw) > 5:
        return None
    port = int(raw)
    return port if 1 <= port <= 65535 else None


def _parse_probe_address(raw: object) -> tuple[str, int] | None:
    if type(raw) is not str or not raw:
        return None
    if raw.startswith("["):
        end = raw.find("]")
        if end <= 1 or raw[end + 1 : end + 2] != ":" or "]" in raw[end + 1 :]:
            return None
        host = raw[1:end]
        raw_port = raw[end + 2 :]
        expected_version = 6
    else:
        if raw.count(":") != 1:
            return None
        host, raw_port = raw.split(":", 1)
        expected_version = 4
    port = _parse_port(raw_port)
    if port is None or "%" in host:
        return None
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return None
    if address.version != expected_version or address.is_unspecified or str(address) != host:
        return None
    return host, port


def _snapshot_value(values: Mapping[str, object], key: str) -> str | None:
    try:
        via_get = values.get(key)
        via_item = values[key]
    except Exception:
        return None
    if type(via_get) is not str or type(via_item) is not str or via_get != via_item or not via_get:
        return None
    return via_get


def _load_installation_config(
    environ: Mapping[str, object], *, require_container: bool
) -> _InstallationConfig | None:
    raw_probe = _snapshot_value(environ, "FSTACK_CADDY_PROBE_ADDRESS")
    network = _snapshot_value(environ, "FSTACK_DOCKER_NETWORK")
    container = (
        _snapshot_value(environ, "FSTACK_CADDY_CONTAINER_NAME") if require_container else "unused"
    )
    probe = _parse_probe_address(raw_probe)
    if (
        probe is None
        or network is None
        or _RESOURCE_NAME.fullmatch(network) is None
        or container is None
        or (require_container and _RESOURCE_NAME.fullmatch(container) is None)
    ):
        return None
    return _InstallationConfig(container, probe[0], probe[1], network)


def _uses_local_docker(environ: Mapping[str, object]) -> bool:
    allowed = {
        "DOCKER_HOST": {None, "", "unix:///var/run/docker.sock"},
        "DOCKER_CONTEXT": {None, "", "default"},
        "DOCKER_CONFIG": {None, ""},
        "DOCKER_CERT_PATH": {None, ""},
        "DOCKER_TLS": {None, ""},
        "DOCKER_TLS_VERIFY": {None, ""},
    }
    for key, accepted in allowed.items():
        try:
            via_get = environ.get(key)
            try:
                via_item = environ[key]
            except KeyError:
                via_item = None
        except Exception:
            return False
        if via_get != via_item or via_get not in accepted:
            return False
    return True


def _remaining(deadline: float, clock: Clock) -> float | None:
    try:
        value = deadline - clock()
    except Exception:
        return None
    return value if value > 0 else None


def _strict_json(raw: bytes) -> object | None:
    if type(raw) is not bytes or len(raw) > _MAX_DOCKER_OUTPUT:
        return None

    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if type(key) is not str or key in result:
                raise ValueError
            result[key] = value
        return result

    try:
        decoded = raw.decode("utf-8", "strict")
        parsed: object = json.loads(
            decoded,
            object_pairs_hook=object_pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        return parsed
    except (UnicodeError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _json_one(raw: bytes) -> dict[str, object] | None:
    value = _strict_json(raw)
    if type(value) is not list or len(value) != 1 or type(value[0]) is not dict:
        return None
    return cast(dict[str, object], value[0])


def _default_docker_runner(argv: list[str], *, timeout: float) -> _RunResult:
    return subprocess.run(  # noqa: S603 - exact constant executable and validated arguments
        argv,
        check=False,
        capture_output=True,
        text=False,
        timeout=timeout,
        env={
            "PATH": os.environ.get("PATH", os.defpath),
            "DOCKER_HOST": "unix:///var/run/docker.sock",
            "DOCKER_CONFIG": "/var/empty/fstack-docker-cli",
        },
    )


def _default_listener_connector(host: str, port: int, timeout: float) -> _Socket:
    return socket.create_connection((host, port), timeout=timeout)


def _is_not_found(stderr: bytes) -> bool:
    if type(stderr) is not bytes or len(stderr) > 4096:
        return False
    lowered = stderr.lower()
    return any(
        token in lowered for token in (b"no such container", b"no such object", b"no such network")
    )


def _run_inspect(
    runner: DockerRunner,
    argv: list[str],
    *,
    deadline: float,
    clock: Clock,
) -> tuple[str, bytes | None]:
    remaining = _remaining(deadline, clock)
    if remaining is None:
        return "unavailable", None
    timeout = min(_DOCKER_INSPECT_TIMEOUT_SECONDS, remaining)
    result: _RunResult | None = None
    try:
        result = runner(argv, timeout=timeout)
        returncode = result.returncode
        stdout = result.stdout
        stderr = result.stderr
        if type(returncode) is not int or type(stdout) is not bytes or type(stderr) is not bytes:
            return "unavailable", None
        if _remaining(deadline, clock) is None:
            return "unavailable", None
        if returncode != 0:
            return ("not_found" if _is_not_found(stderr) else "unavailable"), None
        if len(stdout) > _MAX_DOCKER_OUTPUT or len(stderr) > _MAX_DOCKER_OUTPUT:
            return "unavailable", None
        return "ok", stdout
    except (OSError, subprocess.TimeoutExpired, TimeoutError):
        return "unavailable", None
    except Exception:
        return "unavailable", None
    finally:
        result = None


def _canonical_ip(raw: object) -> str | None:
    if type(raw) is not str or not raw:
        return None
    try:
        address = ipaddress.ip_address(raw)
    except ValueError:
        return None
    return raw if str(address) == raw and not address.is_unspecified else None


def _container_endpoint(
    value: dict[str, object], *, expected_name: str, network: str, require_name: bool = True
) -> tuple[str, _ContainerEndpoint | None]:
    identifier = value.get("Id")
    name = value.get("Name")
    state = value.get("State")
    settings = value.get("NetworkSettings")
    if (
        type(identifier) is not str
        or _HEX_ID.fullmatch(identifier) is None
        or type(name) is not str
        or type(state) is not dict
        or type(settings) is not dict
    ):
        return "malformed", None
    if require_name and name != "/" + expected_name:
        return "topology", None
    if not name.startswith("/") or _RESOURCE_NAME.fullmatch(name[1:]) is None:
        return "malformed", None
    state_map = cast(dict[str, object], state)
    running = state_map.get("Running")
    status = state_map.get("Status")
    if type(running) is not bool or type(status) is not str:
        return "malformed", None
    if running is not True or status != "running":
        return "nonrunning", None
    networks = cast(dict[str, object], settings).get("Networks")
    if type(networks) is not dict:
        return "malformed", None
    if set(networks) != {network}:
        return "topology", None
    endpoint = cast(dict[str, object], networks).get(network)
    if type(endpoint) is not dict:
        return "topology", None
    endpoint_map = cast(dict[str, object], endpoint)
    ipv4 = endpoint_map.get("IPAddress")
    ipv6 = endpoint_map.get("GlobalIPv6Address")
    aliases = endpoint_map.get("Aliases")
    if (
        type(ipv4) is not str
        or type(ipv6) is not str
        or type(aliases) is not list
        or any(type(alias) is not str for alias in aliases)
    ):
        return "malformed", None
    candidates = [candidate for candidate in (ipv4, ipv6) if candidate != ""]
    if len(candidates) != 1:
        return "topology", None
    ip = _canonical_ip(candidates[0])
    if ip is None:
        return "topology", None
    return "ok", _ContainerEndpoint(identifier, name[1:], ip, tuple(cast(list[str], aliases)))


def _network_matches(
    value: dict[str, object],
    *,
    network: str,
    caddy_name: str,
    caddy: _ContainerEndpoint,
    crusher_name: str,
    crusher: _ContainerEndpoint,
) -> str:
    name = value.get("Name")
    if type(name) is not str:
        return "malformed"
    if name != network:
        return "topology"
    network_id = value.get("Id")
    containers = value.get("Containers")
    if type(network_id) is not str or _HEX_ID.fullmatch(network_id) is None:
        return "malformed"
    if type(containers) is not dict:
        return "malformed"
    if caddy.identifier not in containers or crusher.identifier not in containers:
        return "topology"

    def exact_endpoint(name: str, expected: _ContainerEndpoint) -> str:
        matches = [
            (identifier, endpoint)
            for identifier, endpoint in cast(dict[object, object], containers).items()
            if type(endpoint) is dict and cast(dict[str, object], endpoint).get("Name") == name
        ]
        if len(matches) != 1 or matches[0][0] != expected.identifier:
            return "topology"
        endpoint = cast(dict[str, object], matches[0][1])
        ipv4 = endpoint.get("IPv4Address")
        ipv6 = endpoint.get("IPv6Address")
        if type(ipv4) is not str or type(ipv6) is not str:
            return "malformed"
        addresses = [item for item in (ipv4, ipv6) if item]
        if len(addresses) != 1:
            return "topology"
        try:
            interface = ipaddress.ip_interface(addresses[0])
        except ValueError:
            return "malformed"
        if str(interface) != addresses[0]:
            return "malformed"
        return "ok" if str(interface.ip) == expected.ip else "topology"

    caddy_state = exact_endpoint(caddy_name, caddy)
    crusher_state = exact_endpoint(crusher_name, crusher)
    if "malformed" in {caddy_state, crusher_state}:
        return "malformed"
    if caddy_state != "ok" or crusher_state != "ok":
        return "topology"
    return "ok" if crusher_name == crusher.name or crusher_name in crusher.aliases else "topology"


def _validate_public_domain(value: object) -> bool:
    if type(value) is not str or not 1 <= len(value) <= 253 or "\r" in value or "\n" in value:
        return False
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        return False
    if encoded.decode("ascii") != value:
        return False
    labels = value.split(".")
    if len(labels) < 2 or any(
        not 1 <= len(label) <= 63 or re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", label) is None
        for label in labels
    ):
        return False
    try:
        canonical = [
            idna.encode(
                idna.decode(label.encode("ascii"), uts46=False, std3_rules=True),
                uts46=False,
                std3_rules=True,
            ).decode("ascii")
            for label in labels
        ]
    except (UnicodeError, idna.IDNAError):
        return False
    return canonical == labels


def _aware_datetime(value: object) -> bool:
    return type(value) is datetime and value.tzinfo is not None and value.utcoffset() is not None


def _database_load_outcome(
    ids: _ParsedIds,
    *,
    deadline: float,
    clock: Clock,
    require_ingress_submitted: bool,
    environ: Mapping[str, object],
) -> _LoadOutcome:
    engine: Engine | None = None
    row: dict[str, Any] | None = None
    desired: DesiredCaddyTopology | None = None
    code: str | None = None
    retryable = False
    try:
        network = _snapshot_value(environ, "FSTACK_DOCKER_NETWORK")
        stage_root = _snapshot_value(environ, "FSTACK_SECRET_STAGE_ROOT")
        if (
            network is None
            or stage_root is None
            or _RESOURCE_NAME.fullmatch(network) is None
            or _remaining(deadline, clock) is None
        ):
            return _LoadOutcome(code="caddy_topology_invalid", retryable=False)
        try:
            engine = create_engine(
                runtime_submission._database_url(), pool_pre_ping=True, hide_parameters=True
            )
            with engine.begin() as connection:
                remaining = _remaining(deadline, clock)
                if remaining is None:
                    return _LoadOutcome(code="caddy_unavailable", retryable=True)
                milliseconds = max(1, min(39_999, int(remaining * 1000)))
                connection.execute(text(f"SET LOCAL statement_timeout = {milliseconds}"))
                selected = (
                    connection.execute(
                        text(
                            """
                            SELECT operation.id AS operation_id,
                                   operation.organization_id AS operation_organization_id,
                                   operation.deployment_id AS operation_deployment_id,
                                   operation.state AS operation_state,
                                   operation.started_at AS operation_started_at,
                                   operation.completed_at AS operation_completed_at,
                                   operation.failure_code AS operation_failure_code,
                                   runtime.id AS runtime_id,
                                   runtime.organization_id AS runtime_organization_id,
                                   runtime.deployment_id AS runtime_deployment_id,
                                   runtime.provider AS runtime_provider,
                                   runtime.generation AS runtime_generation,
                                   runtime.submission_key AS runtime_submission_key,
                                   runtime.spec_fingerprint,
                                   runtime.api_container_name,
                                   runtime.migration_container_name,
                                   runtime.state AS runtime_state,
                                   runtime.external_id AS runtime_external_id,
                                   runtime.prepared_at AS runtime_prepared_at,
                                   runtime.submitted_at AS runtime_submitted_at,
                                   runtime.confirmed_at AS runtime_confirmed_at,
                                   runtime.failed_at AS runtime_failed_at,
                                   runtime.failure_code AS runtime_failure_code,
                                   ingress.id AS ingress_id,
                                   ingress.organization_id AS ingress_organization_id,
                                   ingress.deployment_id AS ingress_deployment_id,
                                   ingress.provider AS ingress_provider,
                                   ingress.generation AS ingress_generation,
                                   ingress.submission_key AS ingress_submission_key,
                                   ingress.route_fingerprint,
                                   ingress.route_id,
                                   ingress.state AS ingress_state,
                                   ingress.prepared_at AS ingress_prepared_at,
                                   ingress.submitted_at AS ingress_submitted_at,
                                   ingress.confirmed_at AS ingress_confirmed_at,
                                   ingress.failed_at AS ingress_failed_at,
                                   ingress.failure_code AS ingress_failure_code,
                                   deployment.id AS deployment_id,
                                   deployment.organization_id AS deployment_organization_id,
                                   deployment.crusher_image, deployment.public_domain,
                                   database_binding.secret_ref AS database_secret_ref,
                                   storage_binding.secret_ref AS object_storage_secret_ref
                            FROM provisioning_operations AS operation
                            JOIN runtime_submissions AS runtime
                              ON runtime.operation_id = operation.id
                            JOIN ingress_submissions AS ingress
                              ON ingress.operation_id = operation.id
                            JOIN deployments AS deployment
                              ON deployment.id = operation.deployment_id
                             AND deployment.organization_id = operation.organization_id
                            JOIN database_bindings AS database_binding
                              ON database_binding.deployment_id = deployment.id
                             AND database_binding.organization_id = deployment.organization_id
                            JOIN storage_bindings AS storage_binding
                              ON storage_binding.deployment_id = deployment.id
                             AND storage_binding.organization_id = deployment.organization_id
                            WHERE operation.id = :operation_id
                              AND runtime.id = :runtime_id
                              AND ingress.id = :ingress_id
                            FOR SHARE OF operation, runtime, ingress, deployment,
                                         database_binding, storage_binding
                            """
                        ),
                        {
                            "operation_id": ids.operation_id,
                            "runtime_id": ids.runtime_submission_id,
                            "ingress_id": ids.ingress_submission_id,
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
                if selected is None:
                    code = "caddy_topology_invalid"
                else:
                    row = dict(selected)
        except Exception:
            code = "caddy_unavailable"
            retryable = True
        if code is not None or row is None:
            return _LoadOutcome(
                code=code or "caddy_unavailable", retryable=retryable or code is None
            )
        if _remaining(deadline, clock) is None:
            return _LoadOutcome(code="caddy_unavailable", retryable=True)
        identity = (
            row["operation_id"],
            row["operation_organization_id"],
            row["operation_deployment_id"],
            row["runtime_id"],
            row["runtime_organization_id"],
            row["runtime_deployment_id"],
            row["ingress_id"],
            row["ingress_organization_id"],
            row["ingress_deployment_id"],
            row["deployment_id"],
            row["deployment_organization_id"],
        )
        if any(type(item) is not UUID for item in identity):
            return _LoadOutcome(code="caddy_topology_invalid")
        organization = row["operation_organization_id"]
        deployment = row["operation_deployment_id"]
        if not (
            row["operation_id"] == ids.operation_id
            and row["runtime_id"] == ids.runtime_submission_id
            and row["ingress_id"] == ids.ingress_submission_id
            and row["runtime_organization_id"] == organization
            and row["ingress_organization_id"] == organization
            and row["deployment_organization_id"] == organization
            and row["runtime_deployment_id"] == deployment
            and row["ingress_deployment_id"] == deployment
            and row["deployment_id"] == deployment
            and row["operation_state"] == "running"
            and row["runtime_state"] == "submitted"
            and row["ingress_state"]
            in ({"submitted"} if require_ingress_submitted else {"prepared", "submitted"})
        ):
            return _LoadOutcome(code="caddy_topology_invalid")
        api_name, migration_name = runtime_submission._container_names(deployment)
        runtime_times_valid = (
            _aware_datetime(row["runtime_prepared_at"])
            and _aware_datetime(row["runtime_submitted_at"])
            and row["runtime_submitted_at"] >= row["runtime_prepared_at"]
            and row["runtime_confirmed_at"] is None
            and row["runtime_failed_at"] is None
            and row["runtime_failure_code"] is None
        )
        ingress_times_valid = (
            _aware_datetime(row["ingress_prepared_at"])
            and row["ingress_confirmed_at"] is None
            and row["ingress_failed_at"] is None
            and row["ingress_failure_code"] is None
            and (
                (row["ingress_state"] == "prepared" and row["ingress_submitted_at"] is None)
                or (
                    row["ingress_state"] == "submitted"
                    and _aware_datetime(row["ingress_submitted_at"])
                    and row["ingress_submitted_at"] >= row["ingress_prepared_at"]
                )
            )
        )
        operation_times_valid = (
            _aware_datetime(row["operation_started_at"])
            and row["operation_completed_at"] is None
            and row["operation_failure_code"] is None
        )
        if not runtime_times_valid or not ingress_times_valid or not operation_times_valid:
            return _LoadOutcome(code="caddy_topology_invalid")
        operation = {
            "operation_id": ids.operation_id,
            "organization_id": organization,
            "deployment_id": deployment,
            "crusher_image": row["crusher_image"],
            "public_domain": row["public_domain"],
            "database_secret_ref": row["database_secret_ref"],
            "object_storage_secret_ref": row["object_storage_secret_ref"],
        }
        canonical_spec = runtime_submission._canonical_spec(
            operation,
            api_container_name=api_name,
            migration_container_name=migration_name,
            docker_network=network,
            secret_stage_root=Path(stage_root),
        )
        route = ingress_submission._canonical_route(deployment, row["public_domain"])
        exact = (
            row["runtime_provider"] == runtime_submission.PROVIDER
            and row["runtime_generation"] == runtime_submission.GENERATION
            and row["runtime_submission_key"]
            == runtime_submission._submission_key(ids.operation_id)
            and row["spec_fingerprint"] == runtime_submission._spec_fingerprint(canonical_spec)
            and row["api_container_name"] == api_name
            and row["migration_container_name"] == migration_name
            and row["runtime_external_id"] == api_name
            and row["ingress_provider"] == ingress_submission.PROVIDER
            and row["ingress_generation"] == ingress_submission.GENERATION
            and row["ingress_submission_key"]
            == ingress_submission._submission_key(ids.operation_id)
            and row["route_id"] == ingress_submission._route_id(deployment)
            and row["route_fingerprint"] == ingress_submission._route_fingerprint(route)
            and _validate_public_domain(row["public_domain"])
        )
        if not exact:
            return _LoadOutcome(code="caddy_topology_invalid")
        desired = DesiredCaddyTopology(
            deployment_id=deployment,
            crusher_container_name=api_name,
            public_domain=row["public_domain"],
            docker_network=network,
        )
        return _LoadOutcome(desired=desired)
    except Exception:
        return _LoadOutcome(code="caddy_unavailable", retryable=True)
    finally:
        active_exception = sys.exception()
        if row is not None:
            row.clear()
        row = None
        desired = None
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                if active_exception is None:
                    return _LoadOutcome(code="caddy_unavailable", retryable=True)
        engine = None


def _load_desired_state(
    ids: ProvisioningSubmissionIds,
    *,
    deadline: float | None = None,
    clock: Clock = time.monotonic,
    require_ingress_submitted: bool = False,
    environ: Mapping[str, object] | None = None,
) -> DesiredCaddyTopology:
    parsed = _parse_ids(ids)
    if parsed is None:
        ids = cast(ProvisioningSubmissionIds, None)
        deadline = None
        clock = cast(Clock, None)
        environ = None
        raise CaddyTopologyError("invalid_provisioning_submission_ids", False) from None
    values = os.environ if environ is None else environ
    active_deadline = (
        clock() + PRELIGHT_TIMEOUT_BUDGET.total_seconds() if deadline is None else deadline
    )
    outcome = _database_load_outcome(
        parsed,
        deadline=active_deadline,
        clock=clock,
        require_ingress_submitted=require_ingress_submitted,
        environ=values,
    )
    if outcome.desired is not None:
        return outcome.desired
    code = outcome.code or "caddy_unavailable"
    retryable = outcome.retryable
    ids = cast(ProvisioningSubmissionIds, None)
    parsed = None
    deadline = None
    clock = cast(Clock, None)
    environ = None
    values = None  # type: ignore[assignment]
    active_deadline = 0.0
    outcome = _LoadOutcome()
    raise CaddyTopologyError(code, retryable) from None


def _desired(
    ids: ProvisioningSubmissionIds,
    *,
    desired_loader: DesiredLoader | None,
    deadline: float,
    clock: Clock,
    environ: Mapping[str, object],
    require_ingress_submitted: bool,
) -> _LoadOutcome:
    try:
        if desired_loader is None:
            value = _load_desired_state(
                ids,
                deadline=deadline,
                clock=clock,
                require_ingress_submitted=require_ingress_submitted,
                environ=environ,
            )
        else:
            value = desired_loader(ids)
    except CaddyTopologyError as exc:
        return _LoadOutcome(code=exc.code, retryable=exc.retryable)
    except Exception:
        return _LoadOutcome(code="caddy_unavailable", retryable=True)
    if _remaining(deadline, clock) is None:
        return _LoadOutcome(code="caddy_unavailable", retryable=True)
    if (
        type(value) is not DesiredCaddyTopology
        or type(value.deployment_id) is not UUID
        or type(value.crusher_container_name) is not str
        or _RESOURCE_NAME.fullmatch(value.crusher_container_name) is None
        or not _validate_public_domain(value.public_domain)
        or type(value.docker_network) is not str
        or _RESOURCE_NAME.fullmatch(value.docker_network) is None
    ):
        return _LoadOutcome(code="caddy_topology_invalid", retryable=False)
    expected_name = f"fstack-crusher-{value.deployment_id.hex}"
    if value.crusher_container_name != expected_name:
        return _LoadOutcome(code="caddy_topology_invalid", retryable=False)
    return _LoadOutcome(desired=value)


def _preflight_outcome(
    ids: ProvisioningSubmissionIds,
    *,
    desired_loader: DesiredLoader | None,
    environ: Mapping[str, object],
    docker_runner: DockerRunner,
    listener_connector: ListenerConnector,
    clock: Clock,
) -> tuple[str, bool] | None:
    try:
        deadline = clock() + PRELIGHT_TIMEOUT_BUDGET.total_seconds()
    except Exception:
        return "caddy_unavailable", True
    config = _load_installation_config(environ, require_container=True)
    if config is None or not _uses_local_docker(environ):
        return "caddy_topology_invalid", False
    loaded = _desired(
        ids,
        desired_loader=desired_loader,
        deadline=deadline,
        clock=clock,
        environ=environ,
        require_ingress_submitted=False,
    )
    desired = loaded.desired
    if desired is None:
        return loaded.code or "caddy_unavailable", loaded.retryable
    if config.docker_network != desired.docker_network:
        return "caddy_topology_invalid", False
    if _remaining(deadline, clock) is None:
        return "caddy_unavailable", True

    statuses: list[str] = []
    payloads: list[bytes | None] = []
    for argv in (
        ["docker", "container", "inspect", config.caddy_container_name],
        ["docker", "container", "inspect", desired.crusher_container_name],
        ["docker", "network", "inspect", config.docker_network],
    ):
        status, payload = _run_inspect(docker_runner, argv, deadline=deadline, clock=clock)
        statuses.append(status)
        payloads.append(payload)
    if statuses[0] != "ok":
        return "caddy_unavailable", True
    if statuses[1] == "not_found":
        return "caddy_topology_invalid", False
    if statuses[2] == "not_found":
        return "caddy_topology_invalid", False
    if statuses[1] != "ok" or statuses[2] != "ok":
        return "caddy_unavailable", True
    caddy_raw, crusher_raw, network_raw = payloads
    if caddy_raw is None or crusher_raw is None or network_raw is None:
        return "caddy_unavailable", True
    caddy_value = _json_one(caddy_raw)
    crusher_value = _json_one(crusher_raw)
    network_value = _json_one(network_raw)
    if caddy_value is None:
        return "caddy_unavailable", True
    if crusher_value is None or network_value is None:
        return "caddy_unavailable", True
    caddy_state, caddy = _container_endpoint(
        caddy_value,
        expected_name=config.caddy_container_name,
        network=config.docker_network,
    )
    if caddy_state in {"malformed", "nonrunning"}:
        return "caddy_unavailable", True
    if caddy_state != "ok" or caddy is None:
        return "caddy_topology_invalid", False
    crusher_state, crusher = _container_endpoint(
        crusher_value,
        expected_name=desired.crusher_container_name,
        network=config.docker_network,
        require_name=False,
    )
    if crusher_state == "malformed":
        return "caddy_unavailable", True
    if crusher_state != "ok" or crusher is None:
        return "caddy_topology_invalid", False
    network_state = _network_matches(
        network_value,
        network=config.docker_network,
        caddy_name=config.caddy_container_name,
        caddy=caddy,
        crusher_name=desired.crusher_container_name,
        crusher=crusher,
    )
    if network_state == "malformed":
        return "caddy_unavailable", True
    if caddy.ip != config.probe_host or network_state != "ok":
        return "caddy_topology_invalid", False
    remaining = _remaining(deadline, clock)
    if remaining is None:
        return "caddy_unavailable", True
    sock: _Socket | None = None
    connected = False
    try:
        sock = listener_connector(
            config.probe_host,
            config.probe_port,
            min(_LISTENER_CONNECT_TIMEOUT_SECONDS, remaining),
        )
        connected = _remaining(deadline, clock) is not None
    except Exception:
        connected = False
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                connected = False
        sock = None
    return None if connected else ("caddy_unavailable", True)


def preflight_local_caddy_topology(
    ids: ProvisioningSubmissionIds,
    *,
    desired_loader: DesiredLoader | None = None,
    environ: Mapping[str, object] | None = None,
    docker_runner: DockerRunner = _default_docker_runner,
    listener_connector: ListenerConnector = _default_listener_connector,
    clock: Clock = time.monotonic,
) -> None:
    """Validate the supported local Caddy topology without mutating Docker or Caddy."""
    if _parse_ids(ids) is None:
        ids = None  # type: ignore[assignment]
        desired_loader = None
        environ = None
        docker_runner = cast(DockerRunner, None)
        listener_connector = cast(ListenerConnector, None)
        clock = cast(Clock, None)
        raise CaddyTopologyError("invalid_provisioning_submission_ids", False) from None
    values = os.environ if environ is None else environ
    outcome = _preflight_outcome(
        ids,
        desired_loader=desired_loader,
        environ=values,
        docker_runner=docker_runner,
        listener_connector=listener_connector,
        clock=clock,
    )
    if outcome is None:
        return None
    code, retryable = outcome
    ids = None  # type: ignore[assignment]
    desired_loader = None
    environ = None
    values = None  # type: ignore[assignment]
    docker_runner = cast(DockerRunner, None)
    listener_connector = cast(ListenerConnector, None)
    clock = cast(Clock, None)
    outcome = None
    raise CaddyTopologyError(code, retryable) from None


def preflight_then_reconcile(
    ids: ProvisioningSubmissionIds,
    *,
    preflight: Callable[[ProvisioningSubmissionIds], None] = preflight_local_caddy_topology,
    route_loader: Callable[[ProvisioningSubmissionIds], Mapping[str, object]],
    admin_factory: Callable[[], _CaddyReconciler],
) -> None:
    """Complete topology preflight before constructing or calling the Caddy admin client."""
    preflight(ids)
    route = route_loader(ids)
    admin = admin_factory()
    admin.reconcile(route)
