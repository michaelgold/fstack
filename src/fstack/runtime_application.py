"""Idempotent application of sealed local-Docker runtime submissions."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn, Protocol, cast
from urllib.parse import unquote, urlsplit
from uuid import UUID

from fstack import runtime_submission

_DIGEST = re.compile(r"[a-z0-9]+(?:[.-][a-z0-9]+)*/.+@sha256:[0-9a-f]{64}\Z")
_SAFE_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_MAX_SECRET_BYTES = 65_536


@dataclass(frozen=True)
class ImageInspection:
    reference: str
    environment: tuple[str, ...]
    labels: Mapping[str, str]
    entrypoint: tuple[str, ...] = ()
    stop_signal: str | None = None


@dataclass(frozen=True)
class ContainerInspection:
    identifier: str
    normalized_spec: Mapping[str, Any]
    state: str
    health: str | None
    exit_code: int | None


class DockerBoundary(Protocol):
    def inspect_image(self, reference: str) -> ImageInspection: ...

    def inspect_container(self, name: str) -> ContainerInspection | None: ...

    def create_container(self, spec: dict[str, Any]) -> str: ...

    def start_container(self, name: str) -> None: ...

    def wait_container(self, name: str) -> int: ...

    def exec_container(self, name: str, command: list[str]) -> int: ...


@dataclass(frozen=True)
class RuntimePlan:
    submission_id: UUID
    operation_id: UUID
    deployment_id: UUID
    submission_key: str
    spec_fingerprint: str
    canonical_spec: Mapping[str, Any]
    database_secret_ref: str
    source_root: Path
    stage_root: Path


@dataclass(frozen=True)
class _StagedFileEvidence:
    device: int
    inode: int
    size: int
    checksum: bytes


@dataclass(frozen=True)
class _StagedSecretEvidence:
    directory_device: int
    directory_inode: int
    files: Mapping[str, _StagedFileEvidence]


class RuntimeApplicationError(RuntimeError):
    """A bounded provider/domain failure safe for Temporal and PostgreSQL."""

    def __init__(self, code: str, *, retryable: bool) -> None:
        if _SAFE_CODE.fullmatch(code) is None:
            raise ValueError("runtime error code must be sanitized")
        super().__init__(code)
        self.code = code
        self.retryable = retryable


def _raise(code: str, *, retryable: bool) -> NoReturn:
    raise RuntimeApplicationError(code, retryable=retryable)


def _decode_json(raw: bytes, expected: set[str]) -> dict[str, str]:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                _raise("invalid_secret_bundle", retryable=False)
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError):
        _raise("invalid_secret_bundle", retryable=False)
    if type(value) is not dict or set(value) != expected:
        _raise("invalid_secret_bundle", retryable=False)
    if any(type(item) is not str or not item for item in value.values()):
        _raise("invalid_secret_bundle", retryable=False)
    return cast(dict[str, str], value)


def _database_url(value: str) -> tuple[str, int, str, str, str]:
    try:
        parsed = urlsplit(value)
        port = parsed.port or 5432
    except ValueError:
        _raise("invalid_secret_bundle", retryable=False)
    if (
        parsed.scheme not in {"postgresql", "postgres"}
        or not parsed.hostname
        or parsed.fragment
        or parsed.query
        or not parsed.path
        or parsed.path == "/"
        or parsed.username is None
        or parsed.password is None
    ):
        _raise("invalid_secret_bundle", retryable=False)
    hostname = parsed.hostname
    username = parsed.username
    password = parsed.password
    return (
        hostname.lower(),
        port,
        unquote(parsed.path[1:]),
        unquote(username),
        unquote(password),
    )


def _base_url(value: str) -> None:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        _raise("invalid_secret_bundle", retryable=False)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or (parsed.path not in {"", "/"})
        or port is not None
        and not 1 <= port <= 65535
    ):
        _raise("invalid_secret_bundle", retryable=False)


def _validate_bundles(owner_raw: bytes, application_raw: bytes) -> None:
    owner = _decode_json(owner_raw, {"owner_url", "application_user", "application_password"})
    application = _decode_json(
        application_raw,
        {"application_url", "magic_link_secret", "public_base_url", "dashboard_base_url"},
    )
    owner_db = _database_url(owner["owner_url"])
    application_db = _database_url(application["application_url"])
    if owner_db[:3] != application_db[:3]:
        _raise("invalid_secret_bundle", retryable=False)
    if owner_db[3] == application_db[3]:
        _raise("invalid_secret_bundle", retryable=False)
    if application_db[3:] != (owner["application_user"], owner["application_password"]):
        _raise("invalid_secret_bundle", retryable=False)
    _base_url(application["public_base_url"])
    _base_url(application["dashboard_base_url"])


def _require_descriptor_primitives() -> tuple[int, int]:
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    directory = getattr(os, "O_DIRECTORY", 0)
    if (
        not nofollow
        or not directory
        or os.open not in os.supports_dir_fd
        or os.mkdir not in os.supports_dir_fd
    ):
        _raise("unsafe_secret_file", retryable=False)
    return nofollow, directory


def _validate_descriptor(descriptor: int, *, directory: bool) -> os.stat_result:
    try:
        info = os.fstat(descriptor)
    except OSError:
        _raise("unsafe_secret_file", retryable=False)
    expected = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    forbidden = 0o077
    if not expected or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & forbidden:
        _raise("unsafe_secret_file", retryable=False)
    return info


def _close_descriptor(descriptor: int) -> None:
    try:
        os.close(descriptor)
    except OSError:
        _raise("unsafe_secret_file", retryable=False)


def _open_directory(path: str | Path, *, dir_fd: int | None = None) -> int:
    nofollow, directory = _require_descriptor_primitives()
    try:
        descriptor = os.open(path, os.O_RDONLY | nofollow | directory, dir_fd=dir_fd)
    except FileNotFoundError:
        _raise("secret_unavailable", retryable=True)
    except OSError:
        _raise("unsafe_secret_file", retryable=False)
    try:
        _validate_descriptor(descriptor, directory=True)
    except RuntimeApplicationError:
        with suppress(OSError):
            os.close(descriptor)
        raise
    return descriptor


def _read_descriptor(descriptor: int) -> bytes:
    raw = bytearray()
    try:
        while len(raw) <= _MAX_SECRET_BYTES:
            chunk = os.read(descriptor, min(8192, _MAX_SECRET_BYTES + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
    except OSError:
        _raise("unsafe_secret_file", retryable=False)
    if len(raw) > _MAX_SECRET_BYTES:
        _raise("invalid_secret_bundle", retryable=False)
    return bytes(raw)


def _open_source_file(directory_fd: int, name: str) -> bytes:
    nofollow, _ = _require_descriptor_primitives()
    try:
        descriptor = os.open(name, os.O_RDONLY | nofollow, dir_fd=directory_fd)
    except FileNotFoundError:
        _raise("secret_unavailable", retryable=True)
    except OSError:
        _raise("unsafe_secret_file", retryable=False)
    try:
        info = _validate_descriptor(descriptor, directory=False)
        if info.st_size > _MAX_SECRET_BYTES:
            _raise("invalid_secret_bundle", retryable=False)
        return _read_descriptor(descriptor)
    finally:
        _close_descriptor(descriptor)


def _read_source(root: Path, reference: str) -> tuple[bytes, bytes]:
    prefix = "secret://local-file/"
    if not reference.startswith(prefix):
        _raise("unsupported_secret_reference", retryable=False)
    segments = reference[len(prefix) :].split("/")
    if not segments or any(
        re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", part) is None for part in segments
    ):
        _raise("unsupported_secret_reference", retryable=False)
    descriptors = [_open_directory(root)]
    try:
        for segment in segments:
            descriptors.append(_open_directory(segment, dir_fd=descriptors[-1]))
        owner_raw = _open_source_file(descriptors[-1], "owner.json")
        application_raw = _open_source_file(descriptors[-1], "application.json")
    finally:
        for descriptor in reversed(descriptors):
            _close_descriptor(descriptor)
    _validate_bundles(owner_raw, application_raw)
    return owner_raw, application_raw


def _open_or_create_stage_file(directory_fd: int, name: str) -> tuple[int, bool]:
    nofollow, _ = _require_descriptor_primitives()
    try:
        return os.open(name, os.O_RDONLY | nofollow, dir_fd=directory_fd), False
    except FileNotFoundError:
        try:
            return (
                os.open(
                    name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | nofollow,
                    0o400,
                    dir_fd=directory_fd,
                ),
                True,
            )
        except FileExistsError:
            try:
                return os.open(name, os.O_RDONLY | nofollow, dir_fd=directory_fd), False
            except OSError:
                _raise("unsafe_secret_file", retryable=False)
        except OSError:
            _raise("unsafe_secret_file", retryable=False)
    except OSError:
        _raise("unsafe_secret_file", retryable=False)


def _write_all(descriptor: int, raw: bytes) -> None:
    view = memoryview(raw)
    try:
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                _raise("unsafe_secret_file", retryable=False)
            view = view[written:]
        os.fsync(descriptor)
    except OSError:
        _raise("unsafe_secret_file", retryable=False)


def _stage(plan: RuntimePlan) -> _StagedSecretEvidence:
    owner_raw, application_raw = _read_source(plan.source_root, plan.database_secret_ref)
    stage_root_fd = _open_directory(plan.stage_root)
    directory_fd: int | None = None
    try:
        directory_name = plan.deployment_id.hex
        try:
            os.mkdir(directory_name, mode=0o700, dir_fd=stage_root_fd)
        except FileExistsError:
            pass
        except OSError:
            _raise("unsafe_secret_file", retryable=False)
        directory_fd = _open_directory(directory_name, dir_fd=stage_root_fd)
        directory_info = _validate_descriptor(directory_fd, directory=True)
        expected_files = {
            "owner.json": owner_raw,
            "application.json": application_raw,
        }
        identities: dict[str, tuple[int, int]] = {}
        for name, raw in expected_files.items():
            descriptor, created = _open_or_create_stage_file(directory_fd, name)
            try:
                info = _validate_descriptor(descriptor, directory=False)
                identities[name] = (info.st_dev, info.st_ino)
                if created:
                    _write_all(descriptor, raw)
                elif info.st_size > _MAX_SECRET_BYTES or _read_descriptor(descriptor) != raw:
                    _raise("unsafe_secret_file", retryable=False)
            finally:
                _close_descriptor(descriptor)
        try:
            os.fsync(directory_fd)
        except OSError:
            _raise("unsafe_secret_file", retryable=False)

        final_directory_fd = _open_directory(directory_name, dir_fd=stage_root_fd)
        try:
            final_directory_info = _validate_descriptor(final_directory_fd, directory=True)
            if (final_directory_info.st_dev, final_directory_info.st_ino) != (
                directory_info.st_dev,
                directory_info.st_ino,
            ):
                _raise("unsafe_secret_file", retryable=False)
            nofollow, _ = _require_descriptor_primitives()
            evidence_files: dict[str, _StagedFileEvidence] = {}
            for name, raw in expected_files.items():
                try:
                    descriptor = os.open(name, os.O_RDONLY | nofollow, dir_fd=final_directory_fd)
                except OSError:
                    _raise("unsafe_secret_file", retryable=False)
                try:
                    info = _validate_descriptor(descriptor, directory=False)
                    if (
                        (info.st_dev, info.st_ino) != identities[name]
                        or info.st_size > _MAX_SECRET_BYTES
                        or _read_descriptor(descriptor) != raw
                    ):
                        _raise("unsafe_secret_file", retryable=False)
                    evidence_files[name] = _StagedFileEvidence(
                        device=info.st_dev,
                        inode=info.st_ino,
                        size=info.st_size,
                        checksum=hashlib.sha256(raw).digest(),
                    )
                finally:
                    _close_descriptor(descriptor)
        finally:
            _close_descriptor(final_directory_fd)
        try:
            os.fsync(stage_root_fd)
        except OSError:
            _raise("unsafe_secret_file", retryable=False)
        return _StagedSecretEvidence(
            directory_device=directory_info.st_dev,
            directory_inode=directory_info.st_ino,
            files=evidence_files,
        )
    finally:
        if directory_fd is not None:
            _close_descriptor(directory_fd)
        _close_descriptor(stage_root_fd)


def _validate_staged(plan: RuntimePlan, evidence: _StagedSecretEvidence) -> None:
    stage_root_fd = _open_directory(plan.stage_root)
    directory_fd: int | None = None
    try:
        directory_fd = _open_directory(plan.deployment_id.hex, dir_fd=stage_root_fd)
        directory_info = _validate_descriptor(directory_fd, directory=True)
        if (directory_info.st_dev, directory_info.st_ino) != (
            evidence.directory_device,
            evidence.directory_inode,
        ):
            _raise("unsafe_secret_file", retryable=False)
        nofollow, _ = _require_descriptor_primitives()
        for name, expected in evidence.files.items():
            try:
                descriptor = os.open(name, os.O_RDONLY | nofollow, dir_fd=directory_fd)
            except OSError:
                _raise("unsafe_secret_file", retryable=False)
            try:
                info = _validate_descriptor(descriptor, directory=False)
                raw = _read_descriptor(descriptor)
                if (
                    (info.st_dev, info.st_ino) != (expected.device, expected.inode)
                    or info.st_size != expected.size
                    or hashlib.sha256(raw).digest() != expected.checksum
                ):
                    _raise("unsafe_secret_file", retryable=False)
            finally:
                _close_descriptor(descriptor)
    finally:
        if directory_fd is not None:
            _close_descriptor(directory_fd)
        _close_descriptor(stage_root_fd)


def _labels(plan: RuntimePlan) -> dict[str, str]:
    labels = dict(plan.canonical_spec["docker"]["labels"])
    labels["io.fstack.spec_fingerprint"] = plan.spec_fingerprint
    return labels


def _container_spec(plan: RuntimePlan, image: ImageInspection, role: str) -> dict[str, Any]:
    common = dict(plan.canonical_spec["docker"]["common"])
    specific = dict(plan.canonical_spec["docker"][role])
    policy_only = {"environment", "environment_overrides", "label_policy"}
    comparable_common = {key: value for key, value in common.items() if key not in policy_only}
    network = dict(comparable_common["network"])
    network["aliases"] = [specific["name"]]
    comparable_common["network"] = network
    specific["mounts"] = sorted(
        (dict(mount) for mount in specific["mounts"]),
        key=lambda mount: (
            mount["target"],
            mount["source"],
            mount["type"],
            mount["read_only"],
        ),
    )
    return {
        **comparable_common,
        **specific,
        "entrypoint": list(image.entrypoint),
        "environment": list(image.environment),
        "labels": {**dict(image.labels), **_labels(plan)},
        "role": role,
        "stop_signal": image.stop_signal,
    }


def _matching(existing: ContainerInspection, expected: Mapping[str, Any]) -> bool:
    return dict(existing.normalized_spec) == dict(expected)


def _reconcile_create(
    docker: DockerBoundary,
    expected: dict[str, Any],
    *,
    persist_created: Callable[[str], None] | None = None,
    validate_staged: Callable[[], None],
) -> tuple[ContainerInspection, bool]:
    name = expected["name"]
    try:
        existing = docker.inspect_container(name)
    except Exception:
        _raise("docker_unavailable", retryable=True)
    if existing is not None:
        labels = existing.normalized_spec.get("labels", {})
        if not isinstance(labels, Mapping) or labels.get("io.fstack.managed") != "true":
            _raise("docker_name_conflict", retryable=False)
        if not _matching(existing, expected):
            _raise("docker_spec_conflict", retryable=False)
        validate_staged()
        return existing, False
    validate_staged()
    try:
        identifier = docker.create_container(expected)
    except Exception:
        try:
            existing = docker.inspect_container(name)
        except Exception:
            _raise("docker_unavailable", retryable=True)
        if existing is None:
            _raise("docker_unavailable", retryable=True)
        if not _matching(existing, expected):
            _raise("docker_spec_conflict", retryable=False)
        return existing, False
    if persist_created is not None:
        persist_created(identifier)
    try:
        existing = docker.inspect_container(name)
    except Exception:
        _raise("docker_unavailable", retryable=True)
    if existing is None or existing.identifier != identifier or not _matching(existing, expected):
        _raise("docker_unavailable", retryable=True)
    return existing, True


def _apply_runtime_plan(
    plan: RuntimePlan,
    docker: DockerBoundary,
    *,
    persist_submitted: Callable[[str], None],
) -> str:
    """Apply one sealed plan behind the context-sanitizing public boundary."""
    if runtime_submission._spec_fingerprint(plan.canonical_spec) != plan.spec_fingerprint:
        _raise("sealed_spec_mismatch", retryable=False)
    image_reference = plan.canonical_spec.get("crusher_image")
    if type(image_reference) is not str or _DIGEST.fullmatch(image_reference) is None:
        _raise("invalid_image_digest", retryable=False)
    staged = _stage(plan)

    def validate_staged() -> None:
        _validate_staged(plan, staged)

    try:
        image = docker.inspect_image(image_reference)
    except RuntimeApplicationError:
        raise
    except Exception:
        _raise("image_unavailable", retryable=True)
    if image.reference != image_reference:
        _raise("image_digest_mismatch", retryable=False)

    migration_expected = _container_spec(plan, image, "migration")
    migration, migration_created = _reconcile_create(
        docker,
        migration_expected,
        validate_staged=validate_staged,
    )
    if migration.state in {"dead", "removing"}:
        _raise("migration_failed", retryable=False)
    if migration.state == "created" or migration_created:
        validate_staged()
        try:
            docker.start_container(migration_expected["name"])
        except Exception:
            try:
                current = docker.inspect_container(migration_expected["name"])
            except Exception:
                _raise("docker_unavailable", retryable=True)
            if current is None or current.state not in {"running", "exited"}:
                _raise("docker_unavailable", retryable=True)
    try:
        migration_exit = docker.wait_container(migration_expected["name"])
    except Exception:
        _raise("migration_unavailable", retryable=True)
    if migration_exit != 0:
        _raise("migration_failed", retryable=False)

    api_expected = _container_spec(plan, image, "api")
    api, api_created = _reconcile_create(
        docker,
        api_expected,
        persist_created=persist_submitted,
        validate_staged=validate_staged,
    )
    if not api_created:
        persist_submitted(api.identifier)
    if api.state in {"exited", "dead", "removing"}:
        _raise("container_exited", retryable=False)
    if api.state == "created" or api_created:
        validate_staged()
        try:
            docker.start_container(api_expected["name"])
        except Exception:
            try:
                current = docker.inspect_container(api_expected["name"])
            except Exception:
                _raise("docker_unavailable", retryable=True)
            if current is None or current.state != "running":
                _raise("docker_unavailable", retryable=True)
    try:
        current = docker.inspect_container(api_expected["name"])
    except Exception:
        _raise("readiness_unavailable", retryable=True)
    if current is None or current.state != "running" or current.health != "healthy":
        _raise("readiness_unavailable", retryable=True)
    for command in (
        ["/app/.venv/bin/python", "-c", runtime_submission.DATABASE_PROBE_V1],
        ["/app/.venv/bin/python", "-c", runtime_submission.HTTP_PROBE_V1],
    ):
        validate_staged()
        try:
            result = docker.exec_container(api_expected["name"], command)
        except Exception:
            _raise("readiness_unavailable", retryable=True)
        if result != 0:
            _raise("readiness_unavailable", retryable=True)
    return api.identifier


def apply_runtime_plan(
    plan: RuntimePlan,
    docker: DockerBoundary,
    *,
    persist_submitted: Callable[[str], None],
) -> str:
    """Apply one sealed plan without exposing internal exception context."""
    failure: tuple[str, bool] | None = None
    try:
        return _apply_runtime_plan(plan, docker, persist_submitted=persist_submitted)
    except RuntimeApplicationError as exc:
        failure = (exc.code, exc.retryable)
    code, retryable = failure
    raise RuntimeApplicationError(code, retryable=retryable) from None
