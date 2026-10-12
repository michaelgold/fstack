"""Narrow local Docker CLI boundary for Crusher runtime application."""

from __future__ import annotations

import json
import os
import socket
import subprocess
from collections.abc import Mapping, Sequence
from contextlib import suppress
from http.client import HTTPConnection
from typing import Any, cast
from urllib.parse import quote

from fstack.runtime_application import (
    ContainerInspection,
    ImageInspection,
    RuntimeApplicationError,
)


class DockerCommandError(RuntimeError):
    """A Docker command failed; provider output is intentionally discarded."""


class DockerCLI:
    def __init__(self, *, command_timeout: float = 30.0) -> None:
        self._command_timeout = command_timeout

    def _run(self, arguments: Sequence[str], *, timeout: float | None = None) -> str:
        try:
            result = subprocess.run(
                ["docker", *arguments],
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout or self._command_timeout,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise DockerCommandError("docker_unavailable") from None
        if result.returncode != 0 or len(result.stdout) > 1_000_000:
            raise DockerCommandError("docker_unavailable")
        return result.stdout

    def _create_request(self, name: str, payload: Mapping[str, Any]) -> str:
        docker_host = os.environ.get("DOCKER_HOST", "unix:///var/run/docker.sock")
        if not docker_host.startswith("unix://"):
            raise DockerCommandError("docker_unavailable")
        socket_path = docker_host.removeprefix("unix://")

        class UnixConnection(HTTPConnection):
            def connect(inner_self) -> None:
                inner_self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                inner_self.sock.settimeout(self._command_timeout)
                inner_self.sock.connect(socket_path)

        connection = UnixConnection("localhost", timeout=self._command_timeout)
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        try:
            connection.request(
                "POST",
                f"/containers/create?name={quote(name, safe='')}",
                body=body,
                headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
            )
            response = connection.getresponse()
            raw = response.read(65_537)
        except (OSError, TimeoutError):
            raise DockerCommandError("docker_unavailable") from None
        finally:
            connection.close()
        if response.status != 201 or len(raw) > 65_536:
            raise DockerCommandError("docker_unavailable")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise DockerCommandError("docker_unavailable") from None
        identifier = value.get("Id") if type(value) is dict else None
        if type(identifier) is not str or not identifier or len(identifier) > 128:
            raise DockerCommandError("docker_unavailable")
        return identifier

    @staticmethod
    def _json_one(raw: str) -> Mapping[str, Any]:
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            raise DockerCommandError("docker_unavailable") from None
        if type(value) is not list or len(value) != 1 or type(value[0]) is not dict:
            raise DockerCommandError("docker_unavailable")
        return value[0]

    def inspect_image(self, reference: str) -> ImageInspection:
        try:
            raw = self._run(["image", "inspect", reference])
        except DockerCommandError:
            # A pull can complete after its client times out; reconcile by digest.
            with suppress(DockerCommandError):
                self._run(["pull", reference], timeout=600.0)
            raw = self._run(["image", "inspect", reference])
        value = self._json_one(raw)
        repo_digests = value.get("RepoDigests")
        config = value.get("Config")
        if (
            type(repo_digests) is not list
            or reference not in repo_digests
            or type(config) is not dict
        ):
            raise RuntimeApplicationError("image_digest_mismatch", retryable=False)
        config_map = cast(dict[str, Any], config)
        environment = config_map.get("Env") or []
        labels = config_map.get("Labels") or {}
        entrypoint = config_map.get("Entrypoint") or []
        stop_signal = config_map.get("StopSignal")
        if type(environment) is not list or not all(type(item) is str for item in environment):
            raise DockerCommandError("docker_unavailable")
        if type(labels) is not dict or not all(
            type(key) is str and type(item) is str for key, item in labels.items()
        ):
            raise DockerCommandError("docker_unavailable")
        if type(entrypoint) is not list or not all(type(item) is str for item in entrypoint):
            raise DockerCommandError("docker_unavailable")
        if stop_signal is not None and type(stop_signal) is not str:
            raise DockerCommandError("docker_unavailable")
        return ImageInspection(
            reference=reference,
            environment=tuple(environment),
            labels=labels,
            entrypoint=tuple(entrypoint),
            stop_signal=stop_signal,
        )

    @staticmethod
    def _seconds(value: Any) -> int:
        if type(value) is not int or value < 0 or value % 1_000_000_000:
            raise DockerCommandError("docker_unavailable")
        return value // 1_000_000_000

    @staticmethod
    def _required(mapping: Mapping[str, Any], key: str) -> Any:
        if key not in mapping:
            raise DockerCommandError("docker_unavailable")
        return mapping[key]

    @classmethod
    def _string_list(
        cls, mapping: Mapping[str, Any], key: str, *, nullable: bool = False
    ) -> list[str]:
        value = cls._required(mapping, key)
        if nullable and value is None:
            return []
        if type(value) is not list or not all(type(item) is str for item in value):
            raise DockerCommandError("docker_unavailable")
        return cast(list[str], value)

    @classmethod
    def _dict_list(
        cls, mapping: Mapping[str, Any], key: str, *, nullable: bool = False
    ) -> list[dict[str, Any]]:
        value = cls._required(mapping, key)
        if nullable and value is None:
            return []
        if type(value) is not list or not all(type(item) is dict for item in value):
            raise DockerCommandError("docker_unavailable")
        return cast(list[dict[str, Any]], value)

    @classmethod
    def _string_dict(
        cls, mapping: Mapping[str, Any], key: str, *, nullable: bool = False
    ) -> dict[str, str]:
        value = cls._required(mapping, key)
        if nullable and value is None:
            return {}
        if type(value) is not dict or not all(
            type(item_key) is str and type(item_value) is str
            for item_key, item_value in value.items()
        ):
            raise DockerCommandError("docker_unavailable")
        return cast(dict[str, str], value)

    @classmethod
    def _mounts(
        cls,
        value: object,
        *,
        target_key: str,
        writable_key: str,
        writable_means_read_only: bool,
    ) -> list[dict[str, Any]]:
        if type(value) is not list:
            raise DockerCommandError("docker_unavailable")
        normalized: list[dict[str, Any]] = []
        targets: set[str] = set()
        for item in value:
            if type(item) is not dict:
                raise DockerCommandError("docker_unavailable")
            mount = cast(dict[str, Any], item)
            if set(mount) != {"Type", "Source", target_key, writable_key}:
                raise DockerCommandError("docker_unavailable")
            mount_type = cls._required(mount, "Type")
            source = cls._required(mount, "Source")
            target = cls._required(mount, target_key)
            writable = cls._required(mount, writable_key)
            if (
                type(mount_type) is not str
                or mount_type != "bind"
                or type(source) is not str
                or not os.path.isabs(source)
                or type(target) is not str
                or not os.path.isabs(target)
                or type(writable) is not bool
                or target in targets
            ):
                raise DockerCommandError("docker_unavailable")
            targets.add(target)
            normalized.append(
                {
                    "read_only": writable if writable_means_read_only else not writable,
                    "source": source,
                    "target": target,
                    "type": mount_type,
                }
            )
        normalized.sort(
            key=lambda mount: (
                mount["target"],
                mount["source"],
                mount["type"],
                mount["read_only"],
            )
        )
        return normalized

    @classmethod
    def _host_config(cls, host: Mapping[str, Any]) -> dict[str, Any]:
        network_mode = cls._required(host, "NetworkMode")
        auto_remove = cls._required(host, "AutoRemove")
        init = cls._required(host, "Init")
        ipc_mode = cls._required(host, "IpcMode")
        log_config = cls._required(host, "LogConfig")
        pid_mode = cls._required(host, "PidMode")
        port_bindings = cls._required(host, "PortBindings")
        privileged = cls._required(host, "Privileged")
        readonly_rootfs = cls._required(host, "ReadonlyRootfs")
        restart = cls._required(host, "RestartPolicy")
        uts_mode = cls._required(host, "UTSMode")
        if (
            type(network_mode) is not str
            or type(auto_remove) is not bool
            or init is not None
            and type(init) is not bool
            or type(ipc_mode) is not str
            or type(log_config) is not dict
            or type(pid_mode) is not str
            or type(port_bindings) is not dict
            or not all(type(key) is str for key in port_bindings)
            or type(privileged) is not bool
            or type(readonly_rootfs) is not bool
            or type(restart) is not dict
            or type(uts_mode) is not str
        ):
            raise DockerCommandError("docker_unavailable")
        log_config_map = cast(dict[str, Any], log_config)
        log_type = cls._required(log_config_map, "Type")
        log_options = cls._string_dict(log_config_map, "Config")
        restart_map = cast(dict[str, Any], restart)
        restart_name = cls._required(restart_map, "Name")
        maximum_retry_count = cls._required(restart_map, "MaximumRetryCount")
        if (
            type(log_type) is not str
            or type(restart_name) is not str
            or type(maximum_retry_count) is not int
        ):
            raise DockerCommandError("docker_unavailable")
        return {
            "network_mode": network_mode,
            "auto_remove": auto_remove,
            "cap_add": cls._string_list(host, "CapAdd", nullable=True),
            "cap_drop": cls._string_list(host, "CapDrop", nullable=True),
            "devices": cls._dict_list(host, "Devices", nullable=True),
            "device_requests": cls._dict_list(host, "DeviceRequests", nullable=True),
            "dns": cls._string_list(host, "Dns", nullable=True),
            "dns_options": cls._string_list(host, "DnsOptions", nullable=True),
            "dns_search": cls._string_list(host, "DnsSearch", nullable=True),
            "extra_hosts": cls._string_list(host, "ExtraHosts", nullable=True),
            "group_add": cls._string_list(host, "GroupAdd", nullable=True),
            "init": init,
            "ipc_mode": None if ipc_mode in {"", "private"} else ipc_mode,
            "links": cls._string_list(host, "Links", nullable=True),
            "logging": {"driver": log_type, "options": log_options},
            "mounts": cls._mounts(
                cls._required(host, "Mounts"),
                target_key="Target",
                writable_key="ReadOnly",
                writable_means_read_only=True,
            ),
            "pid_mode": pid_mode or None,
            "ports": cast(dict[str, Any], port_bindings),
            "privileged": privileged,
            "readonly_rootfs": readonly_rootfs,
            "restart_policy": {
                "maximum_retry_count": maximum_retry_count,
                "name": restart_name,
            },
            "security_opt": cls._string_list(host, "SecurityOpt", nullable=True),
            "sysctls": cls._string_dict(host, "Sysctls", nullable=True),
            "tmpfs": cls._string_dict(host, "Tmpfs", nullable=True),
            "uts_mode": uts_mode or None,
        }

    @classmethod
    def _normalized(cls, value: Mapping[str, Any]) -> dict[str, Any]:
        config = value.get("Config")
        host = value.get("HostConfig")
        state = value.get("State")
        mounts = value.get("Mounts")
        network_settings = value.get("NetworkSettings")
        name = value.get("Name")
        if not all(type(item) is dict for item in (config, host, state, network_settings)):
            raise DockerCommandError("docker_unavailable")
        config_map = cast(dict[str, Any], config)
        host_map = cast(dict[str, Any], host)
        host_config = cls._host_config(host_map)
        network_settings_map = cast(dict[str, Any], network_settings)
        networks = network_settings_map.get("Networks")
        if type(name) is not str or not name.startswith("/"):
            raise DockerCommandError("docker_unavailable")
        name = name[1:]
        if type(networks) is not dict:
            raise DockerCommandError("docker_unavailable")
        normalized_mounts = cls._mounts(
            mounts,
            target_key="Destination",
            writable_key="RW",
            writable_means_read_only=False,
        )
        if host_config["mounts"] != normalized_mounts:
            raise DockerCommandError("docker_unavailable")
        network_items = list(networks.items())
        if len(network_items) != 1 or type(network_items[0][1]) is not dict:
            raise DockerCommandError("docker_unavailable")
        network_name, network_value = network_items[0]
        if type(network_name) is not str or host_config["network_mode"] != network_name:
            raise DockerCommandError("docker_unavailable")
        network = cast(dict[str, Any], network_value)
        aliases = network.get("Aliases") or []
        identifier = value.get("Id")
        if (
            type(identifier) is not str
            or not identifier
            or type(aliases) is not list
            or not all(type(alias) is str for alias in aliases)
        ):
            raise DockerCommandError("docker_unavailable")
        daemon_aliases = {identifier, identifier[:12]}
        normalized_aliases = sorted(alias for alias in aliases if alias not in daemon_aliases)
        health_config = config_map.get("Healthcheck")
        if health_config is None:
            healthcheck: Any = ["NONE"] if "migrate" in name else None
        elif type(health_config) is dict:
            test = health_config.get("Test") or []
            if test == ["NONE"]:
                healthcheck = ["NONE"]
            else:
                healthcheck = {
                    "command": test,
                    "interval_seconds": cls._seconds(health_config.get("Interval", 0)),
                    "retries": health_config.get("Retries", 0),
                    "start_period_seconds": cls._seconds(health_config.get("StartPeriod", 0)),
                    "timeout_seconds": cls._seconds(health_config.get("Timeout", 0)),
                }
        else:
            raise DockerCommandError("docker_unavailable")
        role = "migration" if "migrate" in name else "api"
        return {
            "auto_remove": host_config["auto_remove"],
            "cap_add": host_config["cap_add"],
            "cap_drop": host_config["cap_drop"],
            "devices": host_config["devices"],
            "device_requests": host_config["device_requests"],
            "dns": host_config["dns"],
            "dns_options": host_config["dns_options"],
            "dns_search": host_config["dns_search"],
            "entrypoint": config_map.get("Entrypoint") or [],
            "environment": config_map.get("Env") or [],
            "extra_hosts": host_config["extra_hosts"],
            "group_add": host_config["group_add"],
            "image": config_map.get("Image"),
            "init": host_config["init"],
            "ipc_mode": host_config["ipc_mode"],
            "links": host_config["links"],
            "logging": host_config["logging"],
            "network": {"aliases": normalized_aliases, "name": network_name},
            "pid_mode": host_config["pid_mode"],
            "platform": "native-linux",
            "ports": host_config["ports"],
            "privileged": host_config["privileged"],
            "readonly_rootfs": host_config["readonly_rootfs"],
            "security_opt": host_config["security_opt"],
            "stop_signal": config_map.get("StopSignal"),
            "sysctls": host_config["sysctls"],
            "tmpfs": host_config["tmpfs"],
            "user": config_map.get("User") or "",
            "uts_mode": host_config["uts_mode"],
            "working_dir": config_map.get("WorkingDir") or "",
            "command": config_map.get("Cmd") or [],
            "healthcheck": healthcheck,
            "mounts": normalized_mounts,
            "name": name,
            "restart_policy": host_config["restart_policy"],
            "labels": config_map.get("Labels") or {},
            "role": role,
        }

    def inspect_container(self, name: str) -> ContainerInspection | None:
        try:
            raw = self._run(["inspect", name])
        except DockerCommandError:
            try:
                names = self._run(["container", "ls", "-a", "--filter", f"name=^{name}$", "-q"])
            except DockerCommandError:
                raise
            if not names.strip():
                return None
            raise
        value = self._json_one(raw)
        state = value.get("State")
        identifier = value.get("Id")
        if type(state) is not dict or type(identifier) is not str or not identifier:
            raise DockerCommandError("docker_unavailable")
        state_map = cast(dict[str, Any], state)
        health = state_map.get("Health")
        health_status = health.get("Status") if type(health) is dict else None
        exit_code_value = state_map.get("ExitCode")
        exit_code = exit_code_value if type(exit_code_value) is int else None
        status = state_map.get("Status")
        if type(status) is not str or (
            health_status is not None and type(health_status) is not str
        ):
            raise DockerCommandError("docker_unavailable")
        return ContainerInspection(
            identifier=identifier,
            normalized_spec=self._normalized(value),
            state=status,
            health=health_status,
            exit_code=exit_code,
        )

    def create_container(self, spec: dict[str, Any]) -> str:
        healthcheck = spec["healthcheck"]
        health_payload = (
            {"Test": ["NONE"]}
            if healthcheck == ["NONE"]
            else {
                "Test": healthcheck["command"],
                "Interval": healthcheck["interval_seconds"] * 1_000_000_000,
                "Timeout": healthcheck["timeout_seconds"] * 1_000_000_000,
                "StartPeriod": healthcheck["start_period_seconds"] * 1_000_000_000,
                "Retries": healthcheck["retries"],
            }
        )
        payload = {
            "Image": spec["image"],
            "Cmd": spec["command"],
            "Entrypoint": spec["entrypoint"],
            "StopSignal": spec["stop_signal"],
            "User": spec["user"],
            "WorkingDir": spec["working_dir"],
            "Labels": spec["labels"],
            "Healthcheck": health_payload,
            "HostConfig": {
                "NetworkMode": spec["network"]["name"],
                "AutoRemove": spec["auto_remove"],
                "CapAdd": [],
                "CapDrop": ["ALL"],
                "Devices": [],
                "DeviceRequests": [],
                "Dns": [],
                "DnsOptions": [],
                "DnsSearch": [],
                "ExtraHosts": [],
                "GroupAdd": [],
                "Init": None,
                "IpcMode": "",
                "Links": [],
                "LogConfig": {
                    "Type": spec["logging"]["driver"],
                    "Config": spec["logging"]["options"],
                },
                "Mounts": [
                    {
                        "Type": "bind",
                        "Source": mount["source"],
                        "Target": mount["target"],
                        "ReadOnly": True,
                    }
                    for mount in spec["mounts"]
                ],
                "PidMode": "",
                "PortBindings": {},
                "Privileged": False,
                "ReadonlyRootfs": False,
                "RestartPolicy": {
                    "Name": spec["restart_policy"]["name"],
                    "MaximumRetryCount": spec["restart_policy"]["maximum_retry_count"],
                },
                "SecurityOpt": ["no-new-privileges:true"],
                "Sysctls": {},
                "Tmpfs": {},
                "UTSMode": "",
            },
            "NetworkingConfig": {
                "EndpointsConfig": {spec["network"]["name"]: {"Aliases": [spec["name"]]}}
            },
        }
        return self._create_request(spec["name"], payload)

    def start_container(self, name: str) -> None:
        self._run(["start", name])

    def wait_container(self, name: str) -> int:
        raw = self._run(["wait", name], timeout=600.0).strip()
        try:
            value = int(raw)
        except ValueError:
            raise DockerCommandError("docker_unavailable") from None
        if not 0 <= value <= 255:
            raise DockerCommandError("docker_unavailable")
        return value

    def exec_container(self, name: str, command: list[str]) -> int:
        try:
            result = subprocess.run(
                ["docker", "exec", name, *command],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=self._command_timeout,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise DockerCommandError("docker_unavailable") from None
        return result.returncode
