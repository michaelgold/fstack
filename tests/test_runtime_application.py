from __future__ import annotations

import json
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

import fstack.runtime_application as runtime_application_module
from fstack import runtime_submission
from fstack.docker_cli import DockerCLI, DockerCommandError
from fstack.runtime_application import (
    ContainerInspection,
    ImageInspection,
    RuntimeApplicationError,
    RuntimePlan,
    _container_spec,
    apply_runtime_plan,
)


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


@dataclass
class FakeDocker:
    image: ImageInspection
    containers: dict[str, ContainerInspection]

    def __init__(self, image_ref: str) -> None:
        self.image = ImageInspection(reference=image_ref, environment=(), labels={})
        self.containers = {}
        self.calls: list[tuple[Any, ...]] = []
        self.exec_results = [0, 0]
        self.migration_exit = 0
        self.api_health = "healthy"
        self.create_error_after_effect = False
        self.fail_api_inspect_after_create = False
        self._api_created = False
        self.after_inspect_image: Callable[[], None] | None = None
        self.after_create: Callable[[str], None] | None = None
        self.before_wait: Callable[[], None] | None = None

    def inspect_image(self, reference: str) -> ImageInspection:
        self.calls.append(("inspect_image", reference))
        if self.after_inspect_image is not None:
            self.after_inspect_image()
        return self.image

    def inspect_container(self, name: str) -> ContainerInspection | None:
        self.calls.append(("inspect_container", name))
        if self.fail_api_inspect_after_create and self._api_created:
            self.fail_api_inspect_after_create = False
            raise RuntimeError("transient inspect leaked /secret/path")
        return self.containers.get(name)

    def create_container(self, spec: dict[str, Any]) -> str:
        name = spec["name"]
        self.calls.append(("create_container", name))
        identifier = f"container-{name}"
        normalized_spec = {
            **spec,
            "entrypoint": spec["entrypoint"] or list(self.image.entrypoint),
            "stop_signal": spec["stop_signal"] or self.image.stop_signal,
        }
        self.containers[name] = ContainerInspection(
            identifier=identifier,
            normalized_spec=normalized_spec,
            state="created",
            health=None,
            exit_code=None,
        )
        if "migrate" not in name:
            self._api_created = True
        if self.create_error_after_effect:
            self.create_error_after_effect = False
            raise RuntimeError("host /secret/path token=do-not-leak")
        if self.after_create is not None:
            self.after_create(name)
        return identifier

    def start_container(self, name: str) -> None:
        self.calls.append(("start_container", name))
        current = self.containers[name]
        is_api = name.startswith("fstack-crusher-") and "migrate" not in name
        self.containers[name] = ContainerInspection(
            identifier=current.identifier,
            normalized_spec=current.normalized_spec,
            state="running" if is_api else "exited",
            health=self.api_health if is_api else None,
            exit_code=None if is_api else self.migration_exit,
        )

    def wait_container(self, name: str) -> int:
        self.calls.append(("wait_container", name))
        if self.before_wait is not None:
            self.before_wait()
        return self.migration_exit

    def exec_container(self, name: str, command: list[str]) -> int:
        self.calls.append(("exec_container", name, tuple(command)))
        return self.exec_results.pop(0) if self.exec_results else 0


class _RawFailureDocker(FakeDocker):
    def __init__(self, image_ref: str, failure: str, marker: str) -> None:
        super().__init__(image_ref)
        self.failure = failure
        self.marker = marker

    def _fail(self, operation: str) -> None:
        if self.failure == operation:
            raise RuntimeError(self.marker)

    def inspect_image(self, reference: str) -> ImageInspection:
        self._fail("inspect")
        return super().inspect_image(reference)

    def inspect_container(self, name: str) -> ContainerInspection | None:
        self._fail("inspect_container")
        return super().inspect_container(name)

    def create_container(self, spec: dict[str, Any]) -> str:
        self._fail("create")
        return super().create_container(spec)

    def start_container(self, name: str) -> None:
        self._fail("start")
        super().start_container(name)

    def wait_container(self, name: str) -> int:
        self._fail("wait")
        return super().wait_container(name)

    def exec_container(self, name: str, command: list[str]) -> int:
        self._fail("exec")
        return super().exec_container(name, command)


def _write_bundle(root: Path) -> str:
    directory = root / "acme" / "database"
    directory.mkdir(parents=True, mode=0o700)
    (root / "acme").chmod(0o700)
    directory.chmod(0o700)
    password_key = "application_" + "password"
    secret_key = "magic_link_" + "secret"
    application_password = "app-" + "password"
    database_scheme = "postgresql://"
    owner = {
        "owner_url": database_scheme + "owner:" + "owner-" + "password" + "@db.example/acme",
        "application_user": "crusher_app",
        password_key: application_password,
    }
    application = {
        "application_url": (
            database_scheme + "crusher_app:" + application_password + "@db.example/acme"
        ),
        secret_key: "magic-secret",
        "public_base_url": "https://mail.acme.example",
        "dashboard_base_url": "https://dashboard.acme.example",
    }
    for name, payload in (("owner.json", owner), ("application.json", application)):
        path = directory / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        path.chmod(0o600)
    return "secret://local-file/acme/database"


def _plan(source_root: Path, stage_root: Path) -> RuntimePlan:
    operation_id = UUID("11111111-1111-4111-8111-111111111111")
    deployment_id = UUID("22222222-2222-4222-8222-222222222222")
    image = "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64
    api_name, migration_name = runtime_submission._container_names(deployment_id)
    operation = {
        "operation_id": operation_id,
        "organization_id": UUID("33333333-3333-4333-8333-333333333333"),
        "deployment_id": deployment_id,
        "crusher_image": image,
        "public_domain": "mail.acme.example",
        "database_secret_ref": _write_bundle(source_root),
        "object_storage_secret_ref": "secret://vault/acme/object-storage",
    }
    spec = runtime_submission._canonical_spec(
        operation,
        api_container_name=api_name,
        migration_container_name=migration_name,
        docker_network="fstack-test",
        secret_stage_root=stage_root,
    )
    fingerprint = runtime_submission._spec_fingerprint(spec)
    return RuntimePlan(
        submission_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        operation_id=operation_id,
        deployment_id=deployment_id,
        submission_key=runtime_submission._submission_key(operation_id),
        spec_fingerprint=fingerprint,
        canonical_spec=spec,
        database_secret_ref=operation["database_secret_ref"],
        source_root=source_root,
        stage_root=stage_root,
    )


def test_apply_runtime_plan_stages_secrets_migrates_starts_and_probes(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])
    submitted: list[str] = []

    result = apply_runtime_plan(plan, docker, persist_submitted=submitted.append)

    assert result == f"container-{plan.canonical_spec['api_container_name']}"
    assert submitted == [result]
    stage = stage_root / plan.deployment_id.hex
    assert json.loads((stage / "owner.json").read_text())["application_user"] == "crusher_app"
    assert (
        json.loads((stage / "application.json").read_text())["magic_link_secret"] == "magic-secret"
    )
    assert oct(os.stat(stage).st_mode & 0o777) == "0o700"
    assert oct(os.stat(stage / "owner.json").st_mode & 0o777) == "0o400"
    assert [call[0] for call in docker.calls].count("create_container") == 2
    assert docker.calls[-2:] == [
        (
            "exec_container",
            plan.canonical_spec["api_container_name"],
            ("/app/.venv/bin/python", "-c", runtime_submission.DATABASE_PROBE_V1),
        ),
        (
            "exec_container",
            plan.canonical_spec["api_container_name"],
            ("/app/.venv/bin/python", "-c", runtime_submission.HTTP_PROBE_V1),
        ),
    ]


@pytest.mark.parametrize(
    ("failure", "expected_code"),
    [
        ("inspect", "image_unavailable"),
        ("inspect_container", "docker_unavailable"),
        ("create", "docker_unavailable"),
        ("start", "docker_unavailable"),
        ("wait", "migration_unavailable"),
        ("exec", "readiness_unavailable"),
    ],
)
def test_apply_runtime_plan_removes_raw_provider_exception_context(
    tmp_path: Path, failure: str, expected_code: str
) -> None:
    marker = f"secret-marker://docker-{failure}?token=raw"
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = _RawFailureDocker(plan.canonical_spec["crusher_image"], failure, marker)

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert exc_info.value.code == expected_code
    assert exc_info.value.retryable is True
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_no_exception_marker(exc_info.value, marker)


def test_apply_runtime_plan_removes_raw_filesystem_exception_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = "secret-marker://filesystem?token=raw"
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])

    def fail_stage(_plan: RuntimePlan) -> None:
        try:
            raise OSError(marker)
        except OSError as exc:
            raise RuntimeApplicationError("unsafe_secret_file", retryable=False) from exc

    monkeypatch.setattr(runtime_application_module, "_stage", fail_stage)

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert exc_info.value.code == "unsafe_secret_file"
    assert exc_info.value.retryable is False
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    _assert_no_exception_marker(exc_info.value, marker)


def test_apply_runtime_plan_rejects_invalid_digest_before_mutation(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker("ghcr.io/michaelgold/crusher@sha256:" + "b" * 64)

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert exc_info.value.code == "image_digest_mismatch"
    assert exc_info.value.retryable is False
    assert all(call[0] != "create_container" for call in docker.calls)


def test_existing_mismatched_container_fails_closed_without_mutation(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])
    migration_name = plan.canonical_spec["migration_container_name"]
    docker.containers[migration_name] = ContainerInspection(
        identifier="hostile",
        normalized_spec={"labels": {"io.fstack.managed": "true"}, "privileged": True},
        state="created",
        health=None,
        exit_code=None,
    )

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert exc_info.value.code == "docker_spec_conflict"
    assert all(call[0] not in {"create_container", "start_container"} for call in docker.calls)


def test_altered_sealed_spec_fails_before_secret_or_docker_access(tmp_path: Path) -> None:
    source_root = tmp_path / "missing-source"
    stage_root = tmp_path / "missing-stage"
    plan = _plan(tmp_path / "source", tmp_path / "stage")
    altered = dict(plan.canonical_spec)
    altered["public_domain"] = "attacker.example"
    plan = RuntimePlan(
        **{
            **plan.__dict__,
            "canonical_spec": altered,
            "source_root": source_root,
            "stage_root": stage_root,
        }
    )
    docker = FakeDocker(plan.canonical_spec["crusher_image"])

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert exc_info.value.code == "sealed_spec_mismatch"
    assert docker.calls == []


def test_secret_symlink_is_rejected_before_docker_access(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    owner = source_root / "acme" / "database" / "owner.json"
    outside = tmp_path / "outside.json"
    outside.write_text(owner.read_text(), encoding="utf-8")
    owner.unlink()
    owner.symlink_to(outside)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert exc_info.value.code == "unsafe_secret_file"
    assert docker.calls == []


def test_migration_failure_prevents_api_creation(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])
    docker.migration_exit = 17

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert exc_info.value.code == "migration_failed"
    assert plan.canonical_spec["api_container_name"] not in docker.containers


def test_unhealthy_api_preserves_submitted_identity_for_retry(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])
    docker.api_health = "starting"
    submitted: list[str] = []

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=submitted.append)

    assert exc_info.value.code == "readiness_unavailable"
    assert exc_info.value.retryable is True
    assert submitted == [f"container-{plan.canonical_spec['api_container_name']}"]


def test_successful_api_create_persists_returned_id_before_failed_post_create_inspect(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])
    docker.fail_api_inspect_after_create = True
    submitted: list[str] = []

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=submitted.append)

    expected_id = f"container-{plan.canonical_spec['api_container_name']}"
    assert exc_info.value.code == "docker_unavailable"
    assert submitted == [expected_id]
    assert "secret" not in str(exc_info.value)


def test_ambiguous_create_is_reconciled_without_exposing_provider_text(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])
    docker.create_error_after_effect = True

    result = apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert result == f"container-{plan.canonical_spec['api_container_name']}"
    assert "secret" not in result


def test_success_replay_does_not_create_or_start_again(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])

    first = apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)
    docker.calls.clear()
    second = apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert second == first
    assert all(call[0] not in {"create_container", "start_container"} for call in docker.calls)


def test_success_replay_matches_nonempty_digest_image_defaults(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])
    docker.image = ImageInspection(
        reference=plan.canonical_spec["crusher_image"],
        environment=(),
        labels={},
        entrypoint=("/usr/bin/tini", "--"),
        stop_signal="SIGQUIT",
    )

    first = apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)
    docker.calls.clear()
    second = apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert second == first
    assert all(call[0] not in {"create_container", "start_container"} for call in docker.calls)


def test_docker_create_payload_preserves_exec_healthcheck(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def capture(_self: DockerCLI, name: str, payload: dict[str, Any]) -> str:
        captured["name"] = name
        captured["payload"] = payload
        return "a" * 64

    monkeypatch.setattr(DockerCLI, "_create_request", capture)
    docker = DockerCLI()
    command = ["CMD", "/app/.venv/bin/python", "-c", runtime_submission.HTTP_PROBE_V1]
    spec = {
        "name": "fstack-crusher-test",
        "image": "example/crusher@sha256:" + "a" * 64,
        "command": ["/app/.venv/bin/python", "-c", runtime_submission.API_LAUNCHER_V1],
        "entrypoint": [],
        "stop_signal": None,
        "auto_remove": False,
        "user": "0:0",
        "working_dir": "/app",
        "labels": {"io.fstack.managed": "true"},
        "healthcheck": {
            "command": command,
            "interval_seconds": 30,
            "timeout_seconds": 5,
            "start_period_seconds": 10,
            "retries": 3,
        },
        "network": {"name": "fstack-test"},
        "logging": {"driver": "json-file", "options": {}},
        "mounts": [
            {
                "source": "/private/application.json",
                "target": "/run/secrets/fstack/application.json",
            }
        ],
        "restart_policy": {"name": "unless-stopped", "maximum_retry_count": 0},
    }

    assert docker.create_container(spec) == "a" * 64
    assert captured["name"] == "fstack-crusher-test"
    assert captured["payload"]["Healthcheck"]["Test"] == command
    assert captured["payload"]["HostConfig"]["AutoRemove"] is False
    assert captured["payload"]["HostConfig"]["Dns"] == []
    assert captured["payload"]["HostConfig"]["DnsOptions"] == []
    assert captured["payload"]["HostConfig"]["DnsSearch"] == []
    assert captured["payload"]["HostConfig"]["GroupAdd"] == []
    assert captured["payload"]["HostConfig"]["Init"] is None
    assert captured["payload"]["HostConfig"]["IpcMode"] == ""
    assert captured["payload"]["HostConfig"]["Links"] == []
    assert captured["payload"]["HostConfig"]["PidMode"] == ""
    assert captured["payload"]["HostConfig"]["PortBindings"] == {}
    assert captured["payload"]["HostConfig"]["Sysctls"] == {}
    assert captured["payload"]["HostConfig"]["UTSMode"] == ""
    assert captured["payload"]["HostConfig"]["Mounts"] == [
        {
            "Type": "bind",
            "Source": "/private/application.json",
            "Target": "/run/secrets/fstack/application.json",
            "ReadOnly": True,
        }
    ]


def test_image_inspection_captures_digest_entrypoint_and_stop_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference = "example/crusher@sha256:" + "a" * 64
    raw = json.dumps(
        [
            {
                "RepoDigests": [reference],
                "Config": {
                    "Env": ["PATH=/app/.venv/bin:/usr/bin"],
                    "Labels": {"org.opencontainers.image.source": "crusher"},
                    "Entrypoint": ["/usr/bin/tini", "--"],
                    "StopSignal": "SIGQUIT",
                },
            }
        ]
    )
    monkeypatch.setattr(DockerCLI, "_run", lambda *_args, **_kwargs: raw)

    image = DockerCLI().inspect_image(reference)

    assert image.entrypoint == ("/usr/bin/tini", "--")
    assert image.stop_signal == "SIGQUIT"


def test_realistic_docker_create_inspect_round_trip_has_exact_comparable_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    image = ImageInspection(
        reference=plan.canonical_spec["crusher_image"],
        environment=("PATH=/app/.venv/bin:/usr/bin", "PYTHONUNBUFFERED=1"),
        labels={"org.opencontainers.image.source": "crusher"},
        entrypoint=("/usr/bin/tini", "--"),
        stop_signal="SIGQUIT",
    )
    expected = _container_spec(plan, image, "api")
    captured: dict[str, Any] = {}

    def capture(_self: DockerCLI, name: str, payload: dict[str, Any]) -> str:
        captured["name"] = name
        captured["payload"] = payload
        return "a" * 64

    monkeypatch.setattr(DockerCLI, "_create_request", capture)
    docker = DockerCLI()
    identifier = docker.create_container(expected)
    payload = captured["payload"]
    host = payload["HostConfig"]
    assert host["Sysctls"] == {}
    assert host["Tmpfs"] == {}
    inspect_json = {
        "Id": identifier,
        "Name": f"/{captured['name']}",
        "Config": {
            "Image": payload["Image"],
            "Cmd": payload["Cmd"],
            "Entrypoint": list(image.entrypoint),
            "Env": list(image.environment),
            "Labels": payload["Labels"],
            "Healthcheck": payload["Healthcheck"],
            "StopSignal": image.stop_signal,
            "User": payload["User"],
            "WorkingDir": payload["WorkingDir"],
        },
        "HostConfig": {
            **host,
            "Init": None,
            "IpcMode": "private",
            "Links": None,
            "PidMode": "",
            "Sysctls": None,
            "Tmpfs": None,
            "UTSMode": "",
        },
        "Mounts": [
            {
                "Type": mount["Type"],
                "Source": mount["Source"],
                "Destination": mount["Target"],
                "RW": not mount["ReadOnly"],
            }
            for mount in host["Mounts"]
        ],
        "NetworkSettings": {
            "Networks": {
                expected["network"]["name"]: {
                    "Aliases": [identifier, expected["name"]],
                }
            }
        },
        "State": {"Status": "created", "ExitCode": 0},
    }

    assert docker._normalized(inspect_json) == expected


@pytest.mark.parametrize("mount_location", ["HostConfig", "Mounts"])
def test_docker_mounts_reject_malformed_entries(tmp_path: Path, mount_location: str) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    inspect_json = _network_alias_inspect(expected, "a" * 64, ["a" * 12, expected["name"]])
    if mount_location == "HostConfig":
        inspect_json["HostConfig"]["Mounts"] = [{}]
    else:
        inspect_json["Mounts"] = [{}]

    with pytest.raises(DockerCommandError, match="^docker_unavailable$"):
        DockerCLI()._normalized(inspect_json)


class _HostileValue:
    def __str__(self) -> str:
        raise AssertionError("hostile value must not be coerced")

    __repr__ = __str__


@pytest.mark.parametrize(
    ("mount_location", "field", "invalid"),
    [
        ("HostConfig", "Type", _HostileValue()),
        ("HostConfig", "Source", _HostileValue()),
        ("HostConfig", "Target", _HostileValue()),
        ("HostConfig", "ReadOnly", 1),
        ("Mounts", "Type", _HostileValue()),
        ("Mounts", "Source", _HostileValue()),
        ("Mounts", "Destination", _HostileValue()),
        ("Mounts", "RW", 0),
    ],
)
def test_docker_mounts_reject_wrong_exact_field_types(
    tmp_path: Path, mount_location: str, field: str, invalid: object
) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    inspect_json = _network_alias_inspect(expected, "a" * 64, ["a" * 12, expected["name"]])
    mounts = (
        inspect_json["HostConfig"]["Mounts"]
        if mount_location == "HostConfig"
        else inspect_json["Mounts"]
    )
    mounts[0][field] = invalid

    with pytest.raises(DockerCommandError, match="^docker_unavailable$"):
        DockerCLI()._normalized(inspect_json)


@pytest.mark.parametrize("mount_location", ["HostConfig", "Mounts"])
def test_docker_mounts_reject_unknown_types(tmp_path: Path, mount_location: str) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    inspect_json = _network_alias_inspect(expected, "a" * 64, ["a" * 12, expected["name"]])
    mounts = (
        inspect_json["HostConfig"]["Mounts"]
        if mount_location == "HostConfig"
        else inspect_json["Mounts"]
    )
    mounts[0]["Type"] = "volume"

    with pytest.raises(DockerCommandError, match="^docker_unavailable$"):
        DockerCLI()._normalized(inspect_json)


def test_docker_mount_views_must_agree_exactly(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    inspect_json = _network_alias_inspect(expected, "a" * 64, ["a" * 12, expected["name"]])
    inspect_json["Mounts"][0]["RW"] = True

    with pytest.raises(DockerCommandError, match="^docker_unavailable$"):
        DockerCLI()._normalized(inspect_json)


@pytest.mark.parametrize("mount_location", ["HostConfig", "Mounts"])
def test_docker_mounts_reject_duplicate_targets(tmp_path: Path, mount_location: str) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    inspect_json = _network_alias_inspect(expected, "a" * 64, ["a" * 12, expected["name"]])
    mounts = (
        inspect_json["HostConfig"]["Mounts"]
        if mount_location == "HostConfig"
        else inspect_json["Mounts"]
    )
    mounts.append(dict(mounts[0]))

    with pytest.raises(DockerCommandError, match="^docker_unavailable$"):
        DockerCLI()._normalized(inspect_json)


@pytest.mark.parametrize("mount_location", ["HostConfig", "Mounts", "both"])
def test_docker_mounts_reject_extra_keys(tmp_path: Path, mount_location: str) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    inspect_json = _network_alias_inspect(expected, "a" * 64, ["a" * 12, expected["name"]])
    if mount_location in {"HostConfig", "both"}:
        inspect_json["HostConfig"]["Mounts"][0]["Unexpected"] = "value"
    if mount_location in {"Mounts", "both"}:
        inspect_json["Mounts"][0]["Unexpected"] = "value"

    with pytest.raises(DockerCommandError, match="^docker_unavailable$"):
        DockerCLI()._normalized(inspect_json)


@pytest.mark.parametrize(
    ("mount_location", "field", "invalid"),
    [
        ("HostConfig", "Source", ""),
        ("HostConfig", "Source", "relative/source"),
        ("HostConfig", "Target", ""),
        ("HostConfig", "Target", "relative/target"),
        ("Mounts", "Source", ""),
        ("Mounts", "Source", "relative/source"),
        ("Mounts", "Destination", ""),
        ("Mounts", "Destination", "relative/target"),
    ],
)
def test_docker_mounts_require_nonempty_absolute_source_and_target(
    tmp_path: Path, mount_location: str, field: str, invalid: str
) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    inspect_json = _network_alias_inspect(expected, "a" * 64, ["a" * 12, expected["name"]])
    mounts = (
        inspect_json["HostConfig"]["Mounts"]
        if mount_location == "HostConfig"
        else inspect_json["Mounts"]
    )
    mounts[0][field] = invalid

    with pytest.raises(DockerCommandError, match="^docker_unavailable$"):
        DockerCLI()._normalized(inspect_json)


def test_docker_mount_order_is_normalized_deterministically(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    canonical = dict(plan.canonical_spec)
    docker_spec = dict(canonical["docker"])
    api_spec = dict(docker_spec["api"])
    original_mount = dict(api_spec["mounts"][0])
    additional_mount = {
        "read_only": True,
        "source": str(stage_root / plan.deployment_id.hex / "additional.json"),
        "target": "/run/secrets/fstack/additional.json",
        "type": "bind",
    }
    api_spec["mounts"] = [original_mount, additional_mount]
    docker_spec["api"] = api_spec
    canonical["docker"] = docker_spec
    plan = RuntimePlan(**{**plan.__dict__, "canonical_spec": canonical})
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    inspect_json = _network_alias_inspect(expected, "a" * 64, ["a" * 12, expected["name"]])
    inspect_json["Mounts"].reverse()

    assert [mount["target"] for mount in expected["mounts"]] == [
        "/run/secrets/fstack/additional.json",
        "/run/secrets/fstack/application.json",
    ]
    assert DockerCLI()._normalized(inspect_json) == expected


def _network_alias_inspect(
    expected: dict[str, Any], identifier: str, aliases: list[str]
) -> dict[str, Any]:
    healthcheck = expected["healthcheck"]
    health_config = (
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
    return {
        "Id": identifier,
        "Name": f"/{expected['name']}",
        "Config": {
            "Image": expected["image"],
            "Cmd": expected["command"],
            "Entrypoint": expected["entrypoint"],
            "Env": expected["environment"],
            "Labels": expected["labels"],
            "Healthcheck": health_config,
            "StopSignal": expected["stop_signal"],
            "User": expected["user"],
            "WorkingDir": expected["working_dir"],
        },
        "HostConfig": {
            "NetworkMode": expected["network"]["name"],
            "CapAdd": expected["cap_add"],
            "CapDrop": expected["cap_drop"],
            "Devices": expected["devices"],
            "DeviceRequests": expected["device_requests"],
            "Dns": expected["dns"],
            "DnsOptions": expected["dns_options"],
            "DnsSearch": expected["dns_search"],
            "ExtraHosts": expected["extra_hosts"],
            "GroupAdd": expected["group_add"],
            "AutoRemove": expected["auto_remove"],
            "Init": expected["init"],
            "IpcMode": expected["ipc_mode"] or "private",
            "Links": expected["links"],
            "LogConfig": {
                "Type": expected["logging"]["driver"],
                "Config": expected["logging"]["options"],
            },
            "Mounts": [
                {
                    "Type": mount["type"],
                    "Source": mount["source"],
                    "Target": mount["target"],
                    "ReadOnly": mount["read_only"],
                }
                for mount in expected["mounts"]
            ],
            "PidMode": expected["pid_mode"] or "",
            "PortBindings": expected["ports"],
            "Privileged": expected["privileged"],
            "ReadonlyRootfs": expected["readonly_rootfs"],
            "RestartPolicy": {
                "Name": expected["restart_policy"]["name"],
                "MaximumRetryCount": expected["restart_policy"]["maximum_retry_count"],
            },
            "SecurityOpt": expected["security_opt"],
            "Sysctls": expected["sysctls"],
            "Tmpfs": expected["tmpfs"],
            "UTSMode": expected["uts_mode"] or "",
        },
        "Mounts": [
            {
                "Type": mount["type"],
                "Source": mount["source"],
                "Destination": mount["target"],
                "RW": not mount["read_only"],
            }
            for mount in expected["mounts"]
        ],
        "NetworkSettings": {"Networks": {expected["network"]["name"]: {"Aliases": aliases}}},
        "State": {"Status": "created", "ExitCode": 0},
    }


def test_docker_network_aliases_require_exact_deterministic_name(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    identifier = "a" * 64
    docker = DockerCLI()

    matching = docker._normalized(
        _network_alias_inspect(expected, identifier, [identifier[:12], expected["name"]])
    )
    missing = docker._normalized(_network_alias_inspect(expected, identifier, [identifier[:12]]))
    additional = docker._normalized(
        _network_alias_inspect(
            expected,
            identifier,
            [identifier[:12], expected["name"], "unexpected-alias"],
        )
    )

    assert matching == expected
    assert missing != expected
    assert additional != expected


def test_docker_auto_remove_true_is_an_exact_spec_conflict(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    identifier = "a" * 64
    inspect_json = _network_alias_inspect(expected, identifier, [identifier[:12], expected["name"]])
    inspect_json["HostConfig"]["AutoRemove"] = True

    actual = DockerCLI()._normalized(inspect_json)

    assert actual != expected


@pytest.mark.parametrize(
    ("field", "host_value"),
    [
        ("Dns", ["8.8.8.8"]),
        ("DnsOptions", ["ndots:1"]),
        ("DnsSearch", ["attacker.example"]),
        ("GroupAdd", ["1234"]),
        ("Sysctls", {"net.ipv4.ip_forward": "1"}),
        ("Tmpfs", {"/run": "rw"}),
    ],
)
def test_docker_host_config_sealed_empty_fields_are_exact_spec_conflicts(
    tmp_path: Path, field: str, host_value: object
) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    identifier = "a" * 64
    inspect_json = _network_alias_inspect(expected, identifier, [identifier[:12], expected["name"]])
    inspect_json["HostConfig"][field] = host_value

    actual = DockerCLI()._normalized(inspect_json)

    assert actual != expected


@pytest.mark.parametrize(
    "field",
    [
        "NetworkMode",
        "AutoRemove",
        "CapAdd",
        "CapDrop",
        "Devices",
        "DeviceRequests",
        "Dns",
        "DnsOptions",
        "DnsSearch",
        "ExtraHosts",
        "GroupAdd",
        "Init",
        "IpcMode",
        "Links",
        "LogConfig",
        "Mounts",
        "PidMode",
        "PortBindings",
        "Privileged",
        "ReadonlyRootfs",
        "RestartPolicy",
        "SecurityOpt",
        "Sysctls",
        "Tmpfs",
        "UTSMode",
    ],
)
def test_docker_host_config_requires_every_sealed_field(tmp_path: Path, field: str) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    identifier = "a" * 64
    inspect_json = _network_alias_inspect(expected, identifier, [identifier[:12], expected["name"]])
    del inspect_json["HostConfig"][field]

    with pytest.raises(DockerCommandError, match="^docker_unavailable$"):
        DockerCLI()._normalized(inspect_json)


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("NetworkMode", None),
        ("AutoRemove", 0),
        ("CapAdd", False),
        ("CapDrop", ["ALL", 1]),
        ("Devices", {}),
        ("DeviceRequests", ""),
        ("Dns", False),
        ("DnsOptions", ""),
        ("DnsSearch", {}),
        ("ExtraHosts", 0),
        ("GroupAdd", [1]),
        ("Init", 0),
        ("IpcMode", None),
        ("Links", False),
        ("LogConfig", []),
        ("Mounts", {}),
        ("PidMode", None),
        ("PortBindings", []),
        ("Privileged", 0),
        ("ReadonlyRootfs", 0),
        ("RestartPolicy", []),
        ("SecurityOpt", {}),
        ("Sysctls", ""),
        ("Sysctls", []),
        ("Sysctls", 0),
        ("Sysctls", False),
        ("Sysctls", {"net.ipv4.ip_forward": 1}),
        ("Sysctls", {1: "net.ipv4.ip_forward"}),
        ("Tmpfs", ""),
        ("Tmpfs", []),
        ("Tmpfs", 0),
        ("Tmpfs", False),
        ("Tmpfs", {"/run": 1}),
        ("Tmpfs", {1: "rw"}),
        ("UTSMode", None),
    ],
)
def test_docker_host_config_rejects_wrong_types(
    tmp_path: Path, field: str, invalid: object
) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    expected = _container_spec(
        plan,
        ImageInspection(reference=plan.canonical_spec["crusher_image"], environment=(), labels={}),
        "api",
    )
    identifier = "a" * 64
    inspect_json = _network_alias_inspect(expected, identifier, [identifier[:12], expected["name"]])
    inspect_json["HostConfig"][field] = invalid

    with pytest.raises(DockerCommandError, match="^docker_unavailable$"):
        DockerCLI()._normalized(inspect_json)


def _replace_staged_application(plan: RuntimePlan) -> None:
    path = plan.stage_root / plan.deployment_id.hex / "application.json"
    replacement = path.with_suffix(".attacker")
    replacement.write_bytes(path.read_bytes() + b" ")
    replacement.chmod(0o400)
    replacement.replace(path)


def test_staged_secret_swap_during_image_inspection_blocks_container_creation(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])
    docker.after_inspect_image = lambda: _replace_staged_application(plan)

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert exc_info.value.code == "unsafe_secret_file"
    assert all(call[0] != "create_container" for call in docker.calls)


def test_staged_secret_swap_during_migration_blocks_api_creation(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])
    docker.before_wait = lambda: _replace_staged_application(plan)

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert exc_info.value.code == "unsafe_secret_file"
    created = [call[1] for call in docker.calls if call[0] == "create_container"]
    assert created == [plan.canonical_spec["migration_container_name"]]


def test_staged_secret_swap_between_api_create_and_start_blocks_start(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])
    api_name = plan.canonical_spec["api_container_name"]
    docker.after_create = lambda name: (
        _replace_staged_application(plan) if name == api_name else None
    )

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert exc_info.value.code == "unsafe_secret_file"
    assert ("start_container", api_name) not in docker.calls
    assert "attacker" not in str(exc_info.value)


def test_source_ancestor_swap_cannot_redirect_opened_secret_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    attacker_root = tmp_path / "attacker"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    attacker_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    _write_bundle(attacker_root)
    attacker_application = attacker_root / "acme" / "database" / "application.json"
    attacker_payload = json.loads(attacker_application.read_text(encoding="utf-8"))
    attacker_payload["magic_link_secret"] = "attacker-secret"
    attacker_application.write_text(json.dumps(attacker_payload), encoding="utf-8")
    attacker_application.chmod(0o600)
    real_close = os.close
    swapped = False

    def swap_ancestor(descriptor: int) -> None:
        nonlocal swapped
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        real_close(descriptor)
        if not swapped and target == source_root / "acme" / "database" / "owner.json":
            swapped = True
            (source_root / "acme").rename(source_root / "original-acme")
            (source_root / "acme").symlink_to(attacker_root / "acme", target_is_directory=True)

    monkeypatch.setattr(os, "close", swap_ancestor)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])

    apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    staged = json.loads(
        (stage_root / plan.deployment_id.hex / "application.json").read_text(encoding="utf-8")
    )
    assert swapped is True
    assert staged["magic_link_secret"] == "magic-secret"


@pytest.mark.parametrize("swap_kind", ["leaf", "ancestor"])
def test_staged_path_swap_after_fsync_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, swap_kind: str
) -> None:
    source_root = tmp_path / "source"
    stage_root = tmp_path / "stage"
    source_root.mkdir(mode=0o700)
    stage_root.mkdir(mode=0o700)
    plan = _plan(source_root, stage_root)
    docker = FakeDocker(plan.canonical_spec["crusher_image"])
    apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)
    stage = stage_root / plan.deployment_id.hex
    attacker = tmp_path / "attacker-stage"
    shutil.copytree(stage, attacker)
    real_fsync = os.fsync
    swapped = False

    def swap_after_validation(descriptor: int) -> None:
        nonlocal swapped
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if not swapped and target == stage:
            swapped = True
            if swap_kind == "leaf":
                (stage / "owner.json").unlink()
                (stage / "owner.json").symlink_to(attacker / "owner.json")
            else:
                stage.rename(stage_root / f"{plan.deployment_id.hex}.original")
                stage.symlink_to(attacker, target_is_directory=True)
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", swap_after_validation)

    with pytest.raises(RuntimeApplicationError) as exc_info:
        apply_runtime_plan(plan, docker, persist_submitted=lambda _identifier: None)

    assert swapped is True
    assert exc_info.value.code == "unsafe_secret_file"
    assert "attacker" not in str(exc_info.value)
    assert "magic-secret" not in str(exc_info.value)
