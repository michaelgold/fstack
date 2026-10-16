#!/usr/bin/env python3
"""Run the isolated local-Caddy ingress acceptance canary."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import tarfile
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Protocol

_DIGEST_IMAGE = re.compile(r"^[a-z0-9][a-z0-9./:_-]*@sha256:[0-9a-f]{64}$")
_PROVISIONING_IMAGE = re.compile(
    r"[a-z0-9]+(?:[.-][a-z0-9]+)*/"
    r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*@sha256:[0-9a-f]{64}\Z"
)
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_NONCE = re.compile(r"^[a-z0-9]{6,20}$")
_CONFLICT_SCENARIOS = (
    "expected-id-different-content",
    "expected-id-plus-same-host",
    "same-host-other-id",
    "wildcard-host",
    "catch-all",
    "unclassifiable",
    "missing-routes",
)


class CanaryContractError(ValueError):
    """The requested canary would violate its isolation contract."""


@dataclass(frozen=True)
class PreparedSource:
    root: Path
    identity: str
    archive_sha256: str | None = None


def _captured(argv: list[str], *, cwd: Path | None = None) -> str:
    try:
        completed = subprocess.run(
            argv,
            cwd=cwd,
            check=True,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CanaryContractError("source verification command failed") from exc
    return completed.stdout


def prepare_source(
    repo_root: Path,
    *,
    source_commit: str | None,
    source_archive: Path | None,
    source_archive_sha256: str | None,
    workspace: Path,
) -> PreparedSource:
    if source_commit is not None:
        if source_archive is not None or source_archive_sha256 is not None:
            raise CanaryContractError("commit and archive source modes are exclusive")
        if _COMMIT.fullmatch(source_commit) is None:
            raise CanaryContractError("source commit must be an exact SHA")
        root = repo_root.resolve()
        head = _captured(["git", "rev-parse", "HEAD"], cwd=root).strip()
        status = _captured(["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=root)
        if head != source_commit or status:
            raise CanaryContractError("source commit execution requires the exact clean HEAD")
        try:
            archived = subprocess.run(
                ["git", "archive", "--format=tar", source_commit],
                cwd=root,
                check=True,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=60,
            ).stdout
        except (OSError, subprocess.SubprocessError) as exc:
            raise CanaryContractError("source commit archive failed") from exc
        digest = hashlib.sha256(archived).hexdigest()
        archive_path = workspace.parent / f".{workspace.name}-{secrets.token_hex(8)}.tar"
        try:
            descriptor = os.open(
                archive_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                0o600,
            )
            try:
                view = memoryview(archived)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise CanaryContractError("source commit archive write failed")
                    view = view[written:]
            finally:
                os.close(descriptor)
            prepared = prepare_source(
                root,
                source_commit=None,
                source_archive=archive_path,
                source_archive_sha256=digest,
                workspace=workspace,
            )
        finally:
            archive_path.unlink(missing_ok=True)
        return PreparedSource(
            root=prepared.root,
            identity=head,
            archive_sha256=digest,
        )
    if source_archive is None or source_archive_sha256 is None:
        raise CanaryContractError("immutable archive and sha256 are required")
    if re.fullmatch(r"[0-9a-f]{64}", source_archive_sha256) is None:
        raise CanaryContractError("source archive requires an exact sha256")
    try:
        descriptor = os.open(source_archive, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as exc:
        raise CanaryContractError("source archive must be a regular non-symlink file") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 100 * 1024 * 1024:
            raise CanaryContractError("source archive is invalid or too large")
        chunks: list[bytes] = []
        remaining = info.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise CanaryContractError("source archive changed while reading")
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
    finally:
        os.close(descriptor)
    actual_digest = hashlib.sha256(raw).hexdigest()
    if actual_digest != source_archive_sha256:
        raise CanaryContractError("source archive digest mismatch")
    try:
        os.mkdir(workspace, mode=0o700)
    except OSError as exc:
        raise CanaryContractError("source workspace must be new") from exc
    seen: set[PurePosixPath] = set()
    total = 0
    try:
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:*") as bundle:
            for member in bundle.getmembers():
                path = PurePosixPath(member.name)
                if (
                    path.is_absolute()
                    or not path.parts
                    or any(part in {"", ".", ".."} for part in path.parts)
                    or path in seen
                    or not (member.isdir() or member.isfile())
                ):
                    raise CanaryContractError("source archive contains an unsafe member")
                seen.add(path)
                target = workspace.joinpath(*path.parts)
                if member.isdir():
                    target.mkdir(mode=0o700, parents=True, exist_ok=True)
                    continue
                total += member.size
                if total > 100 * 1024 * 1024:
                    raise CanaryContractError("source archive expands beyond the size limit")
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                stream = bundle.extractfile(member)
                if stream is None:
                    raise CanaryContractError("source archive member is unreadable")
                data = stream.read(member.size + 1)
                if len(data) != member.size:
                    raise CanaryContractError("source archive member size changed")
                descriptor = os.open(
                    target,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                    0o600,
                )
                try:
                    view = memoryview(data)
                    while view:
                        written = os.write(descriptor, view)
                        if written <= 0:
                            raise CanaryContractError("source archive member write failed")
                        view = view[written:]
                finally:
                    os.close(descriptor)
    except CanaryContractError:
        shutil.rmtree(workspace, ignore_errors=True)
        raise
    except (OSError, tarfile.TarError) as exc:
        shutil.rmtree(workspace, ignore_errors=True)
        raise CanaryContractError("source archive extraction failed") from exc
    return PreparedSource(
        root=workspace.resolve(), identity=actual_digest, archive_sha256=actual_digest
    )


def _source_tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    try:
        entries = sorted(root.rglob("*"), key=lambda path: path.relative_to(root).as_posix())
    except OSError as exc:
        raise CanaryContractError("prepared source cannot be enumerated") from exc
    for entry in entries:
        relative = entry.relative_to(root).as_posix()
        try:
            info = entry.lstat()
        except OSError as exc:
            raise CanaryContractError("prepared source cannot be inspected") from exc
        if stat.S_ISDIR(info.st_mode):
            digest.update(b"D\0" + relative.encode() + b"\0")
            continue
        if not stat.S_ISREG(info.st_mode):
            raise CanaryContractError("prepared source contains an unsupported entry")
        digest.update(b"F\0" + relative.encode() + b"\0" + str(info.st_size).encode() + b"\0")
        try:
            descriptor = os.open(entry, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        except OSError as exc:
            raise CanaryContractError("prepared source cannot be opened") from exc
        try:
            remaining = info.st_size
            while remaining:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    raise CanaryContractError("prepared source changed while hashing")
                digest.update(chunk)
                remaining -= len(chunk)
        finally:
            os.close(descriptor)
    return digest.hexdigest()


def _verify_executing_harness(executing_file: Path, prepared_root: Path) -> None:
    prepared_file = prepared_root / "tools" / "run_local_caddy_ingress_canary.py"
    digests: list[str] = []
    for path in (executing_file, prepared_file):
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        except OSError as exc:
            raise CanaryContractError("immutable harness source is unavailable") from exc
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size > 5 * 1024 * 1024:
                raise CanaryContractError("immutable harness source is invalid")
            digest = hashlib.sha256()
            remaining = info.st_size
            while remaining:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    raise CanaryContractError("immutable harness source changed while reading")
                digest.update(chunk)
                remaining -= len(chunk)
            digests.append(digest.hexdigest())
        finally:
            os.close(descriptor)
    if digests[0] != digests[1]:
        raise CanaryContractError("executing harness does not match immutable source")


@dataclass
class EvidenceDirectory:
    path: Path
    descriptor: int

    def close(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1


def create_evidence_directory(repo_root: Path, evidence_dir: Path) -> EvidenceDirectory:
    root = repo_root.resolve()
    candidate = evidence_dir.absolute()
    resolved_parent = candidate.parent.resolve()
    resolved_candidate = resolved_parent / candidate.name
    if resolved_candidate == root or root in resolved_candidate.parents:
        raise CanaryContractError("evidence directory must be outside the source tree")
    try:
        os.mkdir(resolved_candidate, mode=0o700)
        descriptor = os.open(
            resolved_candidate,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise CanaryContractError("evidence directory must be new and private") from exc
    if os.fstat(descriptor).st_mode & 0o777 != 0o700:
        os.close(descriptor)
        raise CanaryContractError("evidence directory mode must be 0700")
    return EvidenceDirectory(resolved_candidate, descriptor)


def _run_acceptance_tests(source_root: Path, environment_root: Path) -> dict[str, str]:
    if not environment_root.is_dir() or environment_root.is_symlink():
        raise CanaryContractError("acceptance environment must be private")
    suites = {
        "caddy_protocol_and_topology": (
            "tests/test_caddy_admin.py",
            "tests/test_caddy_topology.py",
        ),
        "canary_contract": ("tests/test_local_caddy_ingress_canary_contract.py",),
    }
    environment = {
        key: value
        for key in ("HOME", "PATH", "UV_CACHE_DIR", "XDG_CACHE_HOME")
        if (value := os.environ.get(key)) is not None
    }
    environment.update(
        {
            "NO_COLOR": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTEST_ADDOPTS": "-p no:cacheprovider",
            "UV_PROJECT_ENVIRONMENT": str(environment_root / "venv"),
        }
    )
    summaries: dict[str, str] = {}
    for name, paths in suites.items():
        try:
            result = subprocess.run(
                ("uv", "run", "--frozen", "pytest", "-q", *paths),
                cwd=source_root,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=600,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise CanaryContractError("acceptance test execution failed") from exc
        combined = "\n".join((result.stdout, result.stderr))
        matches = re.findall(
            r"(?m)^(\d+ passed(?:, \d+ skipped)? in [0-9.]+s)$",
            combined,
        )
        if result.returncode != 0 or len(matches) != 1:
            raise CanaryContractError(f"acceptance test suite failed: {name}")
        summaries[name] = matches[0]
    return summaries


def write_evidence(
    evidence: EvidenceDirectory,
    filename: str,
    payload: object,
    *,
    forbidden_values: set[str],
) -> None:
    if (
        evidence.descriptor < 0
        or re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}\.json", filename) is None
    ):
        raise CanaryContractError("evidence filename is invalid")
    banned_keys = {
        "password",
        "token",
        "secret",
        "connection_string",
        "container_id",
        "route_json",
        "provider_body",
        "socket_path",
    }

    def validate(value: object) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if type(key) is not str or key.lower() in banned_keys:
                    raise CanaryContractError("evidence contains a forbidden field")
                validate(item)
        elif isinstance(value, list):
            for item in value:
                validate(item)
        elif isinstance(value, str):
            if any(forbidden and forbidden in value for forbidden in forbidden_values):
                raise CanaryContractError("evidence contains forbidden secret material")
        elif value is not None and type(value) not in {bool, int, float}:
            raise CanaryContractError("evidence contains an unsupported value")

    validate(payload)
    raw = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    try:
        descriptor = os.open(
            filename,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=evidence.descriptor,
        )
    except OSError as exc:
        raise CanaryContractError("evidence file must be new") from exc
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise CanaryContractError("evidence write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _finalize_success_evidence(
    evidence: EvidenceDirectory,
    payload: object,
    *,
    forbidden_values: set[str],
    primary_error: Exception | None,
    cleanup_error: Exception | None,
) -> None:
    if primary_error is not None or cleanup_error is not None:
        return
    write_evidence(
        evidence,
        "summary.json",
        payload,
        forbidden_values=forbidden_values,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the isolated Fstack local-Caddy ingress acceptance canary."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--source-commit")
    source.add_argument("--source-archive", type=Path)
    parser.add_argument("--source-archive-sha256")
    parser.add_argument("--caddy-image-digest", required=True)
    parser.add_argument("--postgres-image-digest", required=True)
    parser.add_argument("--temporal-image-digest", required=True)
    parser.add_argument("--crusher-image-digest", required=True)
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--nonce")
    return parser.parse_args(argv)


class CommandExecutor(Protocol):
    def run(self, argv: tuple[str, ...], *, timeout: int = 60) -> str: ...

    def exists(self, argv: tuple[str, ...], *, timeout: int = 30) -> bool: ...


class LocalCommandExecutor:
    def run(self, argv: tuple[str, ...], *, timeout: int = 60) -> str:
        if not argv or any(
            "\x00" in argument or "\n" in argument or "\r" in argument for argument in argv
        ):
            raise CanaryContractError("command arguments are invalid")
        try:
            completed = subprocess.run(
                list(argv),
                check=True,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=timeout,
                env={"PATH": os.environ.get("PATH", os.defpath)},
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise CanaryContractError("canary command failed") from exc
        if len(completed.stdout) > 64 * 1024 or len(completed.stderr) > 64 * 1024:
            raise CanaryContractError("canary command output exceeded the bound")
        return completed.stdout.strip()

    def exists(self, argv: tuple[str, ...], *, timeout: int = 30) -> bool:
        if not argv or any(
            "\x00" in argument or "\n" in argument or "\r" in argument for argument in argv
        ):
            raise CanaryContractError("command arguments are invalid")
        try:
            completed = subprocess.run(
                list(argv),
                check=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=timeout,
                env={"PATH": os.environ.get("PATH", os.defpath)},
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise CanaryContractError("collision preflight command failed") from exc
        if completed.returncode not in {0, 1}:
            raise CanaryContractError("collision preflight returned an unexpected status")
        return completed.returncode == 0


def verify_images(inputs: CanaryInputs, executor: CommandExecutor) -> dict[str, str]:
    named = {
        "caddy": inputs.caddy_image,
        "postgres": inputs.postgres_image,
        "temporal": inputs.temporal_image,
        "crusher": inputs.crusher_image,
    }
    verified: dict[str, str] = {}
    for name, image in named.items():
        executor.run(("docker", "pull", image), timeout=900)
        raw = executor.run(
            ("docker", "image", "inspect", "--format", "{{json .RepoDigests}}", image)
        )
        try:
            digests = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise CanaryContractError("image inspection returned invalid evidence") from exc
        expected = "sha256:" + image.rsplit("@sha256:", 1)[1]
        if (
            type(digests) is not list
            or not digests
            or any(type(value) is not str for value in digests)
            or not any(value.endswith("@" + expected) for value in digests)
        ):
            raise CanaryContractError("pulled image does not expose the requested digest")
        verified[name] = expected
    version = executor.run(("docker", "run", "--rm", inputs.caddy_image, "caddy", "version"))
    version_fields = version.strip().split()
    if not version_fields or version_fields[0] != "v2.11.7":
        raise CanaryContractError("Caddy image is not exactly v2.11.7")
    return verified


class Runner(Protocol):
    def register(self, name: str) -> None: ...

    def create(self, name: str, command: tuple[str, ...]) -> str: ...

    def remove(self, name: str, command: tuple[str, ...]) -> None: ...

    def verify_absent(self, name: str) -> None: ...


@dataclass
class RecordingRunner:
    events: list[tuple[str, str]] = field(default_factory=list)
    commands: list[tuple[str, tuple[str, ...]]] = field(default_factory=list)
    fail_create: set[str] = field(default_factory=set)

    def register(self, name: str) -> None:
        self.events.append(("register", name))

    def create(self, name: str, command: tuple[str, ...]) -> str:
        self.events.append(("create", name))
        self.commands.append((name, command))
        if name in self.fail_create:
            raise CanaryContractError("resource create failed")
        return f"created:{name}"

    def remove(self, name: str, command: tuple[str, ...]) -> None:
        self.events.append(("remove", name))
        self.commands.append((name, command))

    def verify_absent(self, name: str) -> None:
        self.events.append(("verify-absent", name))


@dataclass
class DockerResourceRunner:
    executor: CommandExecutor
    ownership_token: str = "test-invocation"
    kinds: dict[str, str] = field(default_factory=dict)
    identities: dict[str, str] = field(default_factory=dict)
    creation_confirmed: set[str] = field(default_factory=set)
    creation_attempted: set[str] = field(default_factory=set)

    def register(self, name: str) -> None:
        return None

    def _kind_from(self, command: tuple[str, ...]) -> str:
        if len(command) < 3 or command[0] != "docker":
            raise CanaryContractError("cleanup command is not a Docker resource command")
        kind = command[1]
        if kind not in {"container", "network", "volume", "image"}:
            raise CanaryContractError("cleanup command has an unsupported Docker resource kind")
        return kind

    def _inspect_command(self, kind: str, name: str) -> tuple[str, ...]:
        template = "{{json .CreatedAt}}" if kind == "volume" else "{{.Id}}"
        return ("docker", kind, "inspect", "--format", template, name)

    def _owner_command(self, kind: str, name: str) -> tuple[str, ...]:
        labels_path = ".Config.Labels" if kind in {"container", "image"} else ".Labels"
        template = (
            "{{index " + labels_path + ' "io.fstack.canary"}}|'
            "{{index " + labels_path + ' "io.fstack.canary.invocation"}}'
        )
        return (
            "docker",
            kind,
            "inspect",
            "--format",
            template,
            name,
        )

    def _expected_owner(self, name: str) -> str:
        match = re.match(r"^(fstack-caddy-canary-[a-z0-9]{6,20})(?:-|:)", name)
        if match is None:
            raise CanaryContractError("resource name has no canary owner")
        return f"{match.group(1)}|{self.ownership_token}"

    def create(self, name: str, command: tuple[str, ...]) -> str:
        kind = self.kinds.get(name)
        if kind is None:
            raise CanaryContractError("resource was not registered before create")
        self.creation_attempted.add(name)
        self.executor.run(command, timeout=900)
        self.creation_confirmed.add(name)
        identity = ""
        for attempt in range(10):
            try:
                identity = self.executor.run(self._inspect_command(kind, name))
            except CanaryContractError:
                identity = ""
            if identity:
                break
            if attempt + 1 < 10:
                time.sleep(0.1)
        if not identity:
            raise CanaryContractError("created Docker resource has no immutable identity")
        self.identities[name] = identity
        return identity

    def recover_identity(self, name: str) -> str:
        kind = self.kinds.get(name)
        if kind is None or name not in self.creation_attempted:
            raise CanaryContractError("resource creation was not attempted")
        identity = ""
        for attempt in range(10):
            try:
                identity = self.executor.run(self._inspect_command(kind, name))
            except CanaryContractError:
                identity = ""
            if identity:
                break
            if attempt + 1 < 10:
                time.sleep(0.1)
        if not identity:
            raise CanaryContractError("created Docker resource identity remains unavailable")
        if name not in self.creation_confirmed:
            owner = self.executor.run(self._owner_command(kind, name))
            if owner != self._expected_owner(name):
                raise CanaryContractError("ambiguous Docker resource is not canary-owned")
        self.identities[name] = identity
        return identity

    def remove(self, name: str, command: tuple[str, ...]) -> None:
        kind = self.kinds.get(name)
        expected = self.identities.get(name)
        if kind is None or expected is None:
            raise CanaryContractError("cleanup ownership is unavailable")
        current = self.executor.run(self._inspect_command(kind, name))
        if current != expected:
            raise CanaryContractError("cleanup identity no longer matches the created resource")
        owner = self.executor.run(self._owner_command(kind, name))
        if owner != self._expected_owner(name):
            raise CanaryContractError("cleanup ownership label no longer matches")
        self.executor.run((*command, name), timeout=120)

    def verify_absent(self, name: str) -> None:
        kind = self.kinds.get(name)
        if kind is None:
            raise CanaryContractError("cleanup resource kind is unavailable")
        if self.executor.exists(("docker", kind, "inspect", name)):
            raise CanaryContractError("cleanup left a Docker resource behind")

    def register_kind(self, name: str, remove_command: tuple[str, ...]) -> None:
        self.kinds[name] = self._kind_from(remove_command)


@dataclass
class CleanupRegistry:
    runner: Runner
    entries: list[tuple[str, tuple[str, ...]]] = field(default_factory=list)
    created: dict[str, str] = field(default_factory=dict)
    pending: set[str] = field(default_factory=set)

    def register(self, name: str, remove_command: tuple[str, ...]) -> None:
        if not name.startswith("fstack-caddy-canary-"):
            raise CanaryContractError("cleanup resource is outside the canary namespace")
        self.entries.append((name, remove_command))
        if isinstance(self.runner, DockerResourceRunner):
            self.runner.register_kind(name, remove_command)
        self.runner.register(name)

    def mark_created(self, name: str, resource_id: str) -> None:
        if name not in {entry_name for entry_name, _command in self.entries}:
            raise CanaryContractError("resource was not registered before create")
        if not resource_id:
            raise CanaryContractError("created resource requires immutable identity")
        self.created[name] = resource_id
        self.pending.discard(name)

    def mark_pending(self, name: str) -> None:
        if name not in {entry_name for entry_name, _command in self.entries}:
            raise CanaryContractError("resource was not registered before create")
        self.pending.add(name)

    def run_and_verify(self) -> None:
        errors: list[Exception] = []
        if isinstance(self.runner, DockerResourceRunner):
            for name in tuple(self.pending):
                try:
                    self.mark_created(name, self.runner.recover_identity(name))
                except Exception as exc:
                    errors.append(exc)
        created_entries = [entry for entry in self.entries if entry[0] in self.created]
        for name, command in reversed(created_entries):
            try:
                self.runner.remove(name, command)
            except Exception as exc:
                errors.append(exc)
        for name, _command in reversed(created_entries):
            try:
                self.runner.verify_absent(name)
            except Exception as exc:
                errors.append(exc)
        for name in sorted(self.pending):
            try:
                self.runner.verify_absent(name)
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise CanaryContractError(f"cleanup failed for {len(errors)} operation(s)")


@dataclass(frozen=True)
class RuntimeSecrets:
    postgres_password: str
    operator_token: str
    application_password: str
    magic_link_secret: str

    def __post_init__(self) -> None:
        for value in (
            self.postgres_password,
            self.operator_token,
            self.application_password,
            self.magic_link_secret,
        ):
            if not value or any(
                ord(character) < 0x21 or ord(character) > 0x7E for character in value
            ):
                raise CanaryContractError("runtime secrets must be nonempty printable ASCII")


@dataclass(frozen=True)
class CommandSpec:
    name: str
    argv: tuple[str, ...]
    environment: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ConflictFixture:
    routes: list[object] | None
    expected_code: str


@dataclass(frozen=True)
class ResourceSpec:
    name: str
    create: tuple[str, ...]
    remove: tuple[str, ...]


def execute_resource(spec: ResourceSpec, runner: Runner, cleanup: CleanupRegistry) -> str:
    cleanup.register(spec.name, spec.remove)
    try:
        identity = runner.create(spec.name, spec.create)
    except Exception:
        if isinstance(runner, DockerResourceRunner) and spec.name in runner.creation_attempted:
            cleanup.mark_pending(spec.name)
        raise
    cleanup.mark_created(spec.name, identity)
    return identity


@dataclass(frozen=True)
class CanaryPlan:
    inputs: CanaryInputs
    prefix: str
    invocation_token: str
    database_name: str
    network: str
    control_volume: str
    postgres_container: str
    temporal_container: str
    caddy_container: str
    api_container: str
    worker_container: str
    temporal_namespace: str
    temporal_task_queue: str
    worker_uid: int
    worker_gid: int
    docker_socket_gid: int
    caddy_ip: str = "172.30.0.10"
    listener_port: int = 8080
    caddy_socket_entry: str = "caddy-admin.sock"
    caddy_socket_mode: int = 0o220

    @property
    def caddy_socket_gid(self) -> int:
        return self.worker_gid

    @property
    def fstack_image(self) -> str:
        return f"{self.prefix}-fstack:source"

    @property
    def listener_address(self) -> str:
        return f"{self.caddy_ip}:{self.listener_port}"

    @property
    def temporal_database_name(self) -> str:
        return f"fstack_canary_{self.inputs.nonce}_temporal"

    @property
    def temporal_visibility_database_name(self) -> str:
        return f"fstack_canary_{self.inputs.nonce}_visibility"

    @property
    def worker_supplemental_gid(self) -> int:
        return self.caddy_socket_gid

    @property
    def worker_environment(self) -> dict[str, str]:
        return {
            "FSTACK_CADDY_CONTAINER_NAME": self.caddy_container,
            "FSTACK_CADDY_PROBE_ADDRESS": self.listener_address,
            "FSTACK_DOCKER_NETWORK": self.network,
            "FSTACK_TEMPORAL_NAMESPACE": self.temporal_namespace,
            "FSTACK_TEMPORAL_TASK_QUEUE": self.temporal_task_queue,
        }

    @property
    def maintenance_steps(self) -> tuple[str, ...]:
        return (
            "bootstrap-20261006-0003",
            "stop-api-dispatcher-worker",
            "verify-process-exit",
            "verify-no-old-poller",
            "upgrade-20261013-0004",
            "upgrade-20261015-0005",
            "start-v2-worker-dispatcher",
            "verify-v1-v2-registration",
            "verify-new-poller",
            "enable-api",
            "insert-v2-command",
        )

    @property
    def resource_names(self) -> tuple[str, ...]:
        return (
            self.database_name,
            self.network,
            self.control_volume,
            self.postgres_container,
            self.temporal_container,
            self.caddy_container,
            self.api_container,
            self.worker_container,
            self.temporal_namespace,
            self.temporal_task_queue,
        )


def _write_private(path: Path, content: str) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )
    try:
        payload = content.encode("utf-8")
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise CanaryContractError("private file write failed")
            view = view[written:]
    finally:
        os.close(descriptor)


def _env_content(values: dict[str, str]) -> str:
    for key, value in values.items():
        if re.fullmatch(r"[A-Z][A-Z0-9_]*", key) is None or any(
            character in value for character in "\x00\r\n"
        ):
            raise CanaryContractError("environment file contains an invalid value")
    return "".join(f"{key}={value}\n" for key, value in sorted(values.items()))


def prepare_runtime_files(
    plan: CanaryPlan, runtime_root: Path, secrets_bundle: RuntimeSecrets
) -> None:
    try:
        runtime_root.mkdir(mode=0o700)
    except OSError as exc:
        raise CanaryContractError("runtime workspace must be new and private") from exc
    try:
        _populate_runtime_files(plan, runtime_root, secrets_bundle)
    except Exception:
        shutil.rmtree(runtime_root, ignore_errors=True)
        raise


def _postgres_url(user: str, password: str, host: str, database: str) -> str:
    return "".join(("postgresql+psycopg://", user, ":", password, "@", host, ":5432/", database))


def _populate_runtime_files(
    plan: CanaryPlan, runtime_root: Path, secrets_bundle: RuntimeSecrets
) -> None:
    try:
        source_bundle = runtime_root / "source-secrets" / "canary" / "database"
        source_bundle.mkdir(mode=0o700, parents=True)
        (runtime_root / "runtime-secrets").mkdir(mode=0o700)
    except OSError as exc:
        raise CanaryContractError("runtime workspace must be new and private") from exc
    owner_url = _postgres_url(
        "fstack",
        secrets_bundle.postgres_password,
        plan.postgres_container,
        plan.database_name,
    )
    application_url = _postgres_url(
        "crusher_app",
        secrets_bundle.application_password,
        plan.postgres_container,
        plan.database_name,
    )
    owner_bundle = {
        "owner_url": owner_url,
        "application_user": "crusher_app",
        "application_password": secrets_bundle.application_password,
    }
    application_bundle = {
        "application_url": application_url,
        "magic_link_secret": secrets_bundle.magic_link_secret,
        "public_base_url": "http://mail.canary.example",
        "dashboard_base_url": "http://dashboard.canary.example",
    }
    _write_private(
        source_bundle / "owner.json",
        json.dumps(owner_bundle, sort_keys=True, separators=(",", ":")),
    )
    _write_private(
        source_bundle / "application.json",
        json.dumps(application_bundle, sort_keys=True, separators=(",", ":")),
    )
    caddy_config = {
        "admin": {
            "listen": "unix//run/fstack-caddy/caddy-admin.sock",
            "config": {"persist": False},
        },
        "apps": {
            "http": {"servers": {"fstack": {"listen": [f":{plan.listener_port}"], "routes": []}}}
        },
    }
    _write_private(
        runtime_root / "caddy.json",
        json.dumps(caddy_config, sort_keys=True, separators=(",", ":")),
    )
    request_body = {
        "organization_slug": f"canary-{plan.inputs.nonce}",
        "organization_name": f"Fstack canary {plan.inputs.nonce}",
        "business_slug": "mail",
        "business_name": "Canary mail",
        "environment_slug": "production",
        "public_domain": f"{plan.inputs.nonce}.canary.invalid",
        "crusher_image": plan.inputs.crusher_image,
        "database_secret_ref": "secret://local-file/canary/database",
        "object_storage_secret_ref": "secret://local-file/canary/database",
    }
    _write_private(
        runtime_root / "request.json",
        json.dumps(request_body, sort_keys=True, separators=(",", ":")),
    )
    base_image = f"{plan.prefix}-fstack-base:source"
    build_context = runtime_root / "build-context"
    build_context.mkdir(mode=0o700)
    _write_private(
        build_context / "Dockerfile",
        (
            f"FROM {base_image}\n"
            "USER root\n"
            "RUN apt-get update && apt-get install -y --no-install-recommends docker.io "
            "&& rm -rf /var/lib/apt/lists/*\n"
        ),
    )
    commands = build_runtime_commands(plan, Path("/source"), runtime_root, secrets_bundle)
    for command in commands:
        if command.environment:
            _write_private(runtime_root / f"{command.name}.env", _env_content(command.environment))


def build_runtime_commands(
    plan: CanaryPlan,
    source_root: Path,
    runtime_root: Path,
    secrets_bundle: RuntimeSecrets,
) -> tuple[CommandSpec, ...]:
    label = f"io.fstack.canary={plan.prefix}"
    invocation_label = f"io.fstack.canary.invocation={plan.invocation_token}"
    network = ("--network", plan.network)
    postgres_password_key = "POSTGRES_" + "PASSWORD"
    database_url_key = "FSTACK_DATABASE_" + "URL"
    operator_token_key = "FSTACK_OPERATOR_" + "TOKEN"
    postgres_env = {
        "POSTGRES_USER": "fstack",
        postgres_password_key: secrets_bundle.postgres_password,
        "POSTGRES_DB": plan.database_name,
    }
    database_url = _postgres_url(
        "fstack",
        secrets_bundle.postgres_password,
        plan.postgres_container,
        plan.database_name,
    )
    common_fstack = {
        database_url_key: database_url,
        "FSTACK_TEMPORAL_TARGET": f"{plan.temporal_container}:7233",
        "FSTACK_TEMPORAL_NAMESPACE": plan.temporal_namespace,
        "FSTACK_TEMPORAL_TASK_QUEUE": plan.temporal_task_queue,
        "FSTACK_DOCKER_NETWORK": plan.network,
        operator_token_key: secrets_bundle.operator_token,
    }
    source_secrets = runtime_root / "source-secrets"
    staged_secrets = runtime_root / "runtime-secrets"
    worker_environment = {
        **common_fstack,
        "FSTACK_LOCAL_SECRET_ROOT": str(source_secrets),
        "FSTACK_SECRET_STAGE_ROOT": str(staged_secrets),
        "FSTACK_CADDY_ADMIN_ROOT": "/run/fstack-caddy",
        "FSTACK_CADDY_ADMIN_UID": str(plan.worker_uid),
        "FSTACK_CADDY_ADMIN_GID": str(plan.caddy_socket_gid),
        "FSTACK_CADDY_PROBE_ADDRESS": plan.listener_address,
        "FSTACK_CADDY_CONTAINER_NAME": plan.caddy_container,
    }
    api_environment = {
        database_url_key: database_url,
        operator_token_key: secrets_bundle.operator_token,
    }
    temporal_environment = {
        "DB": "postgres12",
        "DB_PORT": "5432",
        "POSTGRES_USER": "fstack",
        "POSTGRES_PWD": secrets_bundle.postgres_password,
        "POSTGRES_SEEDS": plan.postgres_container,
        "DYNAMIC_CONFIG_FILE_PATH": "config/dynamicconfig/docker.yaml",
        "DEFAULT_NAMESPACE": plan.temporal_namespace,
        "DBNAME": plan.temporal_database_name,
        "VISIBILITY_DBNAME": plan.temporal_visibility_database_name,
    }
    socket_mount = f"{plan.control_volume}:/run/fstack-caddy"
    caddy_config = f"{runtime_root / 'caddy.json'}:/etc/caddy/caddy.json:ro"
    request_mount = f"{runtime_root / 'request.json'}:/run/fstack-canary-request.json:ro"
    source_mount = f"{source_secrets}:{source_secrets}:ro"
    stage_mount = f"{staged_secrets}:{staged_secrets}"
    base_image = f"{plan.prefix}-fstack-base:source"
    build_context = runtime_root / "build-context"
    extension = build_context / "Dockerfile"
    return (
        CommandSpec(
            "build-fstack-base",
            (
                "docker",
                "build",
                "--label",
                label,
                "--label",
                invocation_label,
                "--tag",
                base_image,
                str(source_root),
            ),
        ),
        CommandSpec(
            "build-fstack-runtime",
            (
                "docker",
                "build",
                "--label",
                label,
                "--label",
                invocation_label,
                "--tag",
                plan.fstack_image,
                "--file",
                str(extension),
                str(build_context),
            ),
        ),
        CommandSpec(
            "postgres",
            (
                "docker",
                "run",
                "-d",
                "--name",
                plan.postgres_container,
                *network,
                "--label",
                label,
                "--label",
                invocation_label,
                "--env-file",
                str(runtime_root / "postgres.env"),
                inputs_image(plan, "postgres"),
            ),
            postgres_env,
        ),
        CommandSpec(
            "temporal",
            (
                "docker",
                "run",
                "-d",
                "--name",
                plan.temporal_container,
                *network,
                "--label",
                label,
                "--label",
                invocation_label,
                "--env-file",
                str(runtime_root / "temporal.env"),
                inputs_image(plan, "temporal"),
            ),
            temporal_environment,
        ),
        CommandSpec(
            "caddy",
            (
                "docker",
                "run",
                "-d",
                "--name",
                plan.caddy_container,
                *network,
                "--ip",
                plan.caddy_ip,
                "--label",
                label,
                "--label",
                invocation_label,
                "--volume",
                socket_mount,
                "--volume",
                caddy_config,
                inputs_image(plan, "caddy"),
                "sh",
                "-euc",
                (
                    "caddy run --config /etc/caddy/caddy.json & pid=$!; "
                    "for i in $(seq 1 100); do "
                    "test -S /run/fstack-caddy/caddy-admin.sock && break; "
                    "sleep 0.1; done; test -S /run/fstack-caddy/caddy-admin.sock; "
                    f"chown {plan.worker_uid}:{plan.caddy_socket_gid} "
                    "/run/fstack-caddy/caddy-admin.sock; "
                    "chmod 0220 /run/fstack-caddy/caddy-admin.sock; wait $pid"
                ),
            ),
            {"CADDY_SOCKET_MODE": "0220"},
        ),
        CommandSpec(
            "api",
            (
                "docker",
                "run",
                "-d",
                "--name",
                plan.api_container,
                *network,
                "--label",
                label,
                "--label",
                invocation_label,
                "--env-file",
                str(runtime_root / "api.env"),
                "--volume",
                request_mount,
                plan.fstack_image,
            ),
            api_environment,
        ),
        CommandSpec(
            "worker",
            (
                "docker",
                "run",
                "-d",
                "--name",
                plan.worker_container,
                *network,
                "--label",
                label,
                "--label",
                invocation_label,
                "--user",
                f"{plan.worker_uid}:{plan.worker_gid}",
                "--group-add",
                str(plan.worker_supplemental_gid),
                "--group-add",
                str(plan.docker_socket_gid),
                "--env-file",
                str(runtime_root / "worker.env"),
                "--volume",
                "/var/run/docker.sock:/var/run/docker.sock",
                "--volume",
                socket_mount,
                "--volume",
                source_mount,
                "--volume",
                stage_mount,
                plan.fstack_image,
                "python",
                "-m",
                "fstack.temporal_worker",
            ),
            worker_environment,
        ),
    )


def build_resource_specs(
    plan: CanaryPlan,
    source_root: Path,
    runtime_root: Path,
    secrets_bundle: RuntimeSecrets,
) -> tuple[ResourceSpec, ...]:
    commands = {
        command.name: command
        for command in build_runtime_commands(plan, source_root, runtime_root, secrets_bundle)
    }
    label = f"io.fstack.canary={plan.prefix}"
    invocation_label = f"io.fstack.canary.invocation={plan.invocation_token}"
    return (
        ResourceSpec(
            plan.network,
            (
                "docker",
                "network",
                "create",
                "--label",
                label,
                "--label",
                invocation_label,
                "--subnet",
                "172.30.0.0/24",
                plan.network,
            ),
            ("docker", "network", "rm"),
        ),
        ResourceSpec(
            plan.control_volume,
            (
                "docker",
                "volume",
                "create",
                "--label",
                label,
                "--label",
                invocation_label,
                plan.control_volume,
            ),
            ("docker", "volume", "rm"),
        ),
        ResourceSpec(
            f"{plan.prefix}-fstack-base:source",
            commands["build-fstack-base"].argv,
            ("docker", "image", "rm", "--force"),
        ),
        ResourceSpec(
            plan.fstack_image,
            commands["build-fstack-runtime"].argv,
            ("docker", "image", "rm", "--force"),
        ),
        ResourceSpec(
            plan.postgres_container,
            commands["postgres"].argv,
            ("docker", "container", "rm", "--force"),
        ),
        ResourceSpec(
            plan.temporal_container,
            commands["temporal"].argv,
            ("docker", "container", "rm", "--force"),
        ),
        ResourceSpec(
            plan.caddy_container,
            commands["caddy"].argv,
            ("docker", "container", "rm", "--force"),
        ),
        ResourceSpec(
            plan.worker_container,
            commands["worker"].argv,
            ("docker", "container", "rm", "--force"),
        ),
        ResourceSpec(
            plan.api_container,
            commands["api"].argv,
            ("docker", "container", "rm", "--force"),
        ),
    )


def build_maintenance_steps(
    plan: CanaryPlan,
    source_root: Path,
    runtime_root: Path,
    secrets_bundle: RuntimeSecrets,
) -> tuple[CommandSpec, ...]:
    del source_root
    runtime_commands = {
        command.name: command
        for command in build_runtime_commands(plan, Path("/source"), runtime_root, secrets_bundle)
    }
    stop_canary_processes = (
        "docker",
        "container",
        "stop",
        "--time",
        "10",
        plan.api_container,
        plan.worker_container,
    )
    no_canary_processes = (
        "docker",
        "container",
        "ls",
        "--format",
        "{{.Names}}",
    )

    def migration(revision: str) -> tuple[str, ...]:
        return (
            "docker",
            "create",
            "--name",
            f"{plan.prefix}-migration-{revision}",
            "--network",
            plan.network,
            "--label",
            f"io.fstack.canary={plan.prefix}",
            "--label",
            f"io.fstack.canary.invocation={plan.invocation_token}",
            "--env-file",
            str(runtime_root / "worker.env"),
            plan.fstack_image,
            "alembic",
            "upgrade",
            revision,
        )

    no_old_poller = _temporal_cli_command(
        plan,
        "task-queue",
        "describe",
        "--namespace",
        plan.temporal_namespace,
        "--task-queue",
        plan.temporal_task_queue,
        "--output",
        "json",
    )
    registration_probe = (
        "docker",
        "exec",
        plan.worker_container,
        "python",
        "-c",
        (
            "from fstack.temporal_worker import TEMPORAL_WORKFLOWS;"
            "print(','.join(item.__name__ for item in TEMPORAL_WORKFLOWS))"
        ),
    )
    insert_script = (
        "import json,os,urllib.request;"
        "body=open('/run/fstack-canary-request.json','rb').read();"
        "request=urllib.request.Request('http://127.0.0.1:8000/v1/provisioning/operations',"
        "data=body,method='POST',headers={'content-type':'application/json',"
        "'authorization':'Bearer '+os.environ['FSTACK_OPERATOR_TOKEN'],"
        f"'idempotency-key':'{plan.prefix}-success'}});"
        "response=json.load(urllib.request.urlopen(request,timeout=10));"
        "print(response['operation_id'])"
    )
    return (
        CommandSpec("bootstrap-20261006-0003", migration("20261006_0003")),
        CommandSpec("stop-api-dispatcher-worker", stop_canary_processes),
        CommandSpec("verify-process-exit", no_canary_processes),
        CommandSpec("verify-no-old-poller", no_old_poller),
        CommandSpec("upgrade-20261013-0004", migration("20261013_0004")),
        CommandSpec("upgrade-20261015-0005", migration("20261015_0005")),
        CommandSpec("start-v2-worker-dispatcher", runtime_commands["worker"].argv),
        CommandSpec("verify-v1-v2-registration", registration_probe),
        CommandSpec("verify-new-poller", no_old_poller),
        CommandSpec("enable-api", runtime_commands["api"].argv),
        CommandSpec(
            "insert-v2-command",
            ("docker", "exec", plan.api_container, "python", "-c", insert_script),
        ),
    )


def build_live_verification_commands(
    plan: CanaryPlan, deployment_id: str
) -> tuple[CommandSpec, ...]:
    deployment_hex = deployment_id.replace("-", "")
    if re.fullmatch(r"[0-9a-f]{32}", deployment_hex) is None:
        raise CanaryContractError("deployment ID is invalid")
    crusher_name = f"fstack-crusher-{deployment_hex}"
    host = f"{plan.inputs.nonce}.canary.invalid"
    expected_route = {
        "@id": f"fstack-crusher-ingress-{deployment_hex}",
        "match": [{"host": [host]}],
        "handle": [
            {
                "handler": "reverse_proxy",
                "upstreams": [{"dial": f"{crusher_name}:8000"}],
            }
        ],
    }
    encoded_expected = json.dumps(expected_route, sort_keys=True, separators=(",", ":"))
    route_script = (
        "import json;"
        "from fstack.caddy_admin import CaddyAdminConfig,CaddyAdminTransport;"
        f"expected=json.loads({encoded_expected!r});"
        "request=b'GET /config/apps/http/servers/fstack/routes HTTP/1.1\\r\\n'"
        "+b'Host: localhost\\r\\nConnection: close\\r\\n\\r\\n';"
        "response=CaddyAdminTransport(CaddyAdminConfig.from_environ())._exchange(request);"
        "assert response.status==200,response.status;routes=json.loads(response.body);"
        "assert routes==[expected];print('exact')"
    )
    proxy_script = (
        "import json,urllib.request;"
        f"request=urllib.request.Request('http://{plan.caddy_ip}:{plan.listener_port}/health',"
        f"headers={{'Host':{host!r}}});"
        "body=json.load(urllib.request.urlopen(request,timeout=5));"
        "print(json.dumps(body,sort_keys=True,separators=(',',':')))"
    )
    return (
        CommandSpec(
            "inspect-exact-caddy-route",
            ("docker", "exec", plan.worker_container, "python", "-c", route_script),
        ),
        CommandSpec(
            "probe-caddy-to-crusher",
            (
                "docker",
                "exec",
                plan.caddy_container,
                "wget",
                "-qO-",
                f"http://{crusher_name}:8000/health",
            ),
        ),
        CommandSpec(
            "probe-worker-through-caddy",
            ("docker", "exec", plan.worker_container, "python", "-c", proxy_script),
        ),
    )


def _dispatch_generation_command(plan: CanaryPlan, operation_id: str) -> tuple[str, ...]:
    operation = str(uuid.UUID(operation_id))
    query = (
        f"SELECT workflow_type FROM workflow_dispatches WHERE operation_id = '{operation}'::uuid"
    )
    return (
        "docker",
        "exec",
        plan.postgres_container,
        "psql",
        "--username",
        "fstack",
        "--dbname",
        plan.database_name,
        "--tuples-only",
        "--no-align",
        "--command",
        query,
    )


def _submission_ids_command(plan: CanaryPlan, operation_id: str) -> tuple[str, ...]:
    operation = str(uuid.UUID(operation_id))
    query = (
        "SELECT runtime.id::text || '|' || ingress.id::text "
        "FROM runtime_submissions AS runtime "
        "JOIN ingress_submissions AS ingress ON ingress.operation_id = runtime.operation_id "
        f"WHERE runtime.operation_id = '{operation}'::uuid"
    )
    return (
        "docker",
        "exec",
        plan.postgres_container,
        "psql",
        "--username",
        "fstack",
        "--dbname",
        plan.database_name,
        "--tuples-only",
        "--no-align",
        "--command",
        query,
    )


def _topology_mismatch_command(
    plan: CanaryPlan,
    *,
    operation_id: str,
    runtime_submission_id: str,
    ingress_submission_id: str,
    deployment_id: str,
    scenario: str,
) -> tuple[str, ...]:
    operation = str(uuid.UUID(operation_id))
    runtime = str(uuid.UUID(runtime_submission_id))
    ingress = str(uuid.UUID(ingress_submission_id))
    deployment = str(uuid.UUID(deployment_id))
    if scenario not in {"wrong-network", "wrong-probe-ip", "missing-alias"}:
        raise CanaryContractError("unknown topology mismatch scenario")
    setup: list[str] = [f"scenario={scenario!r}", "environ=dict(os.environ)"]
    preflight_arguments = "environ=environ,desired_loader=lambda unused: desired"
    if scenario == "wrong-network":
        setup.append(f"environ['FSTACK_DOCKER_NETWORK']={plan.prefix + '-wrong-network'!r}")
    elif scenario == "wrong-probe-ip":
        setup.append(
            f"environ['FSTACK_CADDY_PROBE_ADDRESS']={'172.30.0.99:' + str(plan.listener_port)!r}"
        )
    else:
        expected_name = f"fstack-crusher-{deployment.replace('-', '')}"
        setup.extend(
            (
                "import json,subprocess",
                f"expected_name={expected_name!r}",
                "def altered_docker_runner(argv,timeout):",
                " result=subprocess.run(argv,check=False,capture_output=True,timeout=timeout)",
                " if tuple(argv[:3])==('docker','container','inspect') and expected_name in argv:",
                "  payload=json.loads(result.stdout)",
                "  for endpoint in payload[0]['NetworkSettings']['Networks'].values():",
                "   endpoint['Aliases']=[]",
                (
                    "  return subprocess.CompletedProcess("
                    "argv,result.returncode,json.dumps(payload).encode(),result.stderr)"
                ),
                " return result",
            )
        )
        preflight_arguments += ",docker_runner=altered_docker_runner"
    script = "\n".join(
        (
            "import os",
            "from uuid import UUID",
            (
                "from fstack.caddy_topology import "
                "CaddyTopologyError, DesiredCaddyTopology, ProvisioningSubmissionIds, "
                "preflight_local_caddy_topology, preflight_then_reconcile"
            ),
            (
                "ids=ProvisioningSubmissionIds("
                f"operation_id={operation!r},runtime_submission_id={runtime!r},"
                f"ingress_submission_id={ingress!r})"
            ),
            (
                "desired=DesiredCaddyTopology("
                f"deployment_id=UUID({deployment!r}),"
                f"crusher_container_name={'fstack-crusher-' + deployment.replace('-', '')!r},"
                f"public_domain={plan.inputs.nonce + '.canary.invalid'!r},"
                f"docker_network={plan.network!r})"
            ),
            *setup,
            "def forbidden(*args,**kwargs): raise AssertionError('reconcile reached')",
            "try:",
            " preflight_then_reconcile(",
            (
                "  ids,preflight=lambda value: "
                f"preflight_local_caddy_topology(value,{preflight_arguments}),"
            ),
            "  route_loader=forbidden,admin_factory=forbidden,",
            " )",
            "except CaddyTopologyError as exc:",
            " assert exc.code=='caddy_topology_invalid' and exc.retryable is False",
            " print(scenario+'|caddy_topology_invalid|reconcile_not_called')",
            "else: raise AssertionError('topology mismatch was accepted')",
        )
    )
    return ("docker", "exec", plan.worker_container, "python", "-c", script)


def _durable_counts_command(plan: CanaryPlan, operation_id: str) -> tuple[str, ...]:
    if (
        re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            operation_id,
        )
        is None
    ):
        raise CanaryContractError("operation ID is invalid")
    sql = (
        "SELECT "
        f"(SELECT count(*) FROM runtime_submissions WHERE operation_id='{operation_id}'::uuid),"
        f"(SELECT count(*) FROM ingress_submissions WHERE operation_id='{operation_id}'::uuid),"
        f"(SELECT count(*) FROM workflow_dispatches WHERE operation_id='{operation_id}'::uuid),"
        f"(SELECT count(*) FROM audit_events WHERE operation_id='{operation_id}'::uuid)"
    )
    return (
        "docker",
        "exec",
        plan.postgres_container,
        "psql",
        "-U",
        "fstack",
        "-d",
        plan.database_name,
        "-At",
        "-c",
        sql,
    )


def inputs_image(plan: CanaryPlan, name: str) -> str:
    images = {
        "caddy": plan.inputs.caddy_image,
        "postgres": plan.inputs.postgres_image,
        "temporal": plan.inputs.temporal_image,
        "crusher": plan.inputs.crusher_image,
    }
    try:
        return images[name]
    except KeyError as exc:
        raise CanaryContractError("unknown image role") from exc


def assert_no_resource_collisions(plan: CanaryPlan, executor: CommandExecutor) -> None:
    checks = [
        *(
            ("docker", "container", "inspect", name)
            for name in (
                plan.postgres_container,
                plan.temporal_container,
                plan.caddy_container,
                plan.api_container,
                plan.worker_container,
                f"{plan.prefix}-migration-20261006_0003",
                f"{plan.prefix}-migration-20261013_0004",
                f"{plan.prefix}-migration-20261015_0005",
            )
        ),
        ("docker", "network", "inspect", plan.network),
        ("docker", "volume", "inspect", plan.control_volume),
        ("docker", "image", "inspect", plan.fstack_image),
        ("docker", "image", "inspect", f"{plan.prefix}-fstack-base:source"),
    ]
    if any(executor.exists(tuple(command)) for command in checks):
        raise CanaryContractError("an exact canary resource name already exists")


def build_plan(inputs: CanaryInputs) -> CanaryPlan:
    prefix = f"fstack-caddy-canary-{inputs.nonce}"
    worker_uid = os.getuid()
    worker_gid = os.getgid()
    try:
        docker_socket_gid = os.stat("/var/run/docker.sock", follow_symlinks=False).st_gid
    except OSError:
        docker_socket_gid = worker_gid
    return CanaryPlan(
        inputs=inputs,
        prefix=prefix,
        invocation_token=secrets.token_hex(16),
        database_name=f"{prefix}-db_test",
        network=f"{prefix}-network",
        control_volume=f"{prefix}-admin",
        postgres_container=f"{prefix}-postgres",
        temporal_container=f"{prefix}-temporal",
        caddy_container=f"{prefix}-caddy",
        api_container=f"{prefix}-api",
        worker_container=f"{prefix}-worker",
        temporal_namespace=f"{prefix}-namespace",
        temporal_task_queue=f"{prefix}-task-queue",
        worker_uid=worker_uid,
        worker_gid=worker_gid,
        docker_socket_gid=docker_socket_gid,
    )


@dataclass(frozen=True)
class CanaryInputs:
    source_commit: str | None
    source_archive: Path | None
    source_archive_sha256: str | None
    caddy_image: str
    postgres_image: str
    temporal_image: str
    crusher_image: str
    evidence_dir: Path
    nonce: str

    def __post_init__(self) -> None:
        for image in (
            self.caddy_image,
            self.postgres_image,
            self.temporal_image,
            self.crusher_image,
        ):
            if _DIGEST_IMAGE.fullmatch(image) is None:
                raise CanaryContractError("every image must use an exact sha256 digest")
        if not self.caddy_image.startswith("caddy:2.11.7@sha256:"):
            raise CanaryContractError("the Caddy image must identify v2.11.7")
        if _PROVISIONING_IMAGE.fullmatch(self.crusher_image) is None:
            raise CanaryContractError(
                "Crusher image must use the provisioning API's canonical digest-only form"
            )
        if _NONCE.fullmatch(self.nonce) is None:
            raise CanaryContractError("invalid canary nonce")
        if (self.source_commit is None) == (self.source_archive is None):
            raise CanaryContractError("select exactly one immutable source")
        if self.source_commit is not None and _COMMIT.fullmatch(self.source_commit) is None:
            raise CanaryContractError("source commit must be an exact SHA")
        if self.source_archive is not None and (
            self.source_archive_sha256 is None
            or re.fullmatch(r"[0-9a-f]{64}", self.source_archive_sha256) is None
        ):
            raise CanaryContractError("source archive requires an exact sha256")


def validate_source(
    repo_root: Path,
    *,
    source_commit: str | None,
    source_archive: Path | None,
    source_archive_sha256: str | None,
    git_head: str | None,
    git_status: str | None,
    evidence_dir: Path,
) -> Path:
    root = repo_root.resolve()
    evidence = evidence_dir.resolve()
    if evidence == root or root in evidence.parents:
        raise CanaryContractError("evidence directory must be outside the source tree")
    if source_commit is not None:
        if source_archive is not None or source_archive_sha256 is not None:
            raise CanaryContractError("commit and archive source modes are exclusive")
        if _COMMIT.fullmatch(source_commit) is None or git_head != source_commit:
            raise CanaryContractError("source commit must equal the checked-out HEAD")
        if git_status != "":
            raise CanaryContractError("source commit execution requires a clean tree")
        return root
    if source_archive is None or source_archive_sha256 is None:
        raise CanaryContractError("immutable archive and sha256 are required")
    if hashlib.sha256(source_archive.read_bytes()).hexdigest() != source_archive_sha256:
        raise CanaryContractError("source archive digest mismatch")
    return source_archive.resolve()


def _canary_processes_absent(output: str, plan: CanaryPlan) -> bool:
    names = set(output.splitlines())
    return not names.intersection({plan.api_container, plan.worker_container})


def _strict_json_object(raw: str) -> dict[str, object]:
    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value

    def reject_constant(value: str) -> object:
        raise ValueError(f"invalid JSON constant: {value}")

    try:
        value = json.loads(
            raw,
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise CanaryContractError("Temporal poller evidence was invalid") from exc
    if type(value) is not dict:
        raise CanaryContractError("Temporal poller evidence was invalid")
    return value


def _task_queue_poller_snapshot(
    raw: str,
    *,
    require_concrete_collection: bool = False,
) -> tuple[str, ...]:
    value = _strict_json_object(raw)
    recognized = False
    snapshot: list[str] = []
    for key in (
        "pollers",
        "workflowTaskQueuePollers",
        "activityTaskQueuePollers",
    ):
        pollers = value.get(key)
        if key in value:
            recognized = True
        if pollers is None and key in value and require_concrete_collection:
            raise CanaryContractError("Temporal poller evidence was invalid")
        if type(pollers) is list:
            for poller in pollers:
                if type(poller) is not dict:
                    raise CanaryContractError("Temporal poller evidence was invalid")
                identity = poller.get("identity")
                last_access_time = poller.get("lastAccessTime")
                if not isinstance(identity, str) or not identity:
                    raise CanaryContractError("Temporal poller evidence was invalid")
                if not isinstance(last_access_time, str) or not last_access_time:
                    raise CanaryContractError("Temporal poller evidence was invalid")
                try:
                    parsed_last_access = datetime.fromisoformat(
                        last_access_time.replace("Z", "+00:00")
                    )
                except ValueError as exc:
                    raise CanaryContractError("Temporal poller evidence was invalid") from exc
                if parsed_last_access.tzinfo is None:
                    raise CanaryContractError("Temporal poller evidence was invalid")
                if key == "pollers":
                    task_queue_type = poller.get("taskQueueType")
                    if not isinstance(task_queue_type, str) or not task_queue_type:
                        raise CanaryContractError("Temporal poller evidence was invalid")
                snapshot.append(
                    json.dumps(
                        {"category": key, "poller": poller},
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
        if pollers is not None and type(pollers) is not list:
            raise CanaryContractError("Temporal poller evidence was invalid")
    if not recognized:
        raise CanaryContractError("Temporal poller evidence was invalid")
    return tuple(sorted(snapshot))


def _task_queue_has_pollers(raw: str) -> bool:
    return bool(_task_queue_poller_snapshot(raw))


def _verify_temporal_pollers_quiesced(
    executor: CommandExecutor,
    command: tuple[str, ...],
    *,
    observation_delay: float = 65.0,
) -> None:
    first = _task_queue_poller_snapshot(
        executor.run(command, timeout=30),
        require_concrete_collection=True,
    )
    time.sleep(observation_delay)
    second = _task_queue_poller_snapshot(
        executor.run(command, timeout=30),
        require_concrete_collection=True,
    )
    if first != second:
        raise CanaryContractError("Temporal poller quiescence was not proved")


def _temporal_cli_command(plan: CanaryPlan, *arguments: str) -> tuple[str, ...]:
    return (
        "docker",
        "exec",
        plan.temporal_container,
        "temporal",
        *arguments,
        "--address",
        f"{plan.temporal_container}:7233",
    )


def _temporal_health_command(plan: CanaryPlan) -> tuple[str, ...]:
    return _temporal_cli_command(
        plan,
        "operator",
        "cluster",
        "health",
    )


def _wait_for_output(
    executor: CommandExecutor,
    command: tuple[str, ...],
    *,
    accept: Callable[[str], bool],
    attempts: int = 90,
    delay: float = 1.0,
) -> str:
    for attempt in range(attempts):
        try:
            output = executor.run(command, timeout=30)
        except CanaryContractError:
            output = ""
        if accept(output):
            return output
        if attempt + 1 < attempts:
            time.sleep(delay)
    raise CanaryContractError("bounded readiness check did not succeed")


def _one_shot_container(
    *,
    name: str,
    create_command: tuple[str, ...],
    runner: DockerResourceRunner,
    cleanup: CleanupRegistry,
    executor: CommandExecutor,
) -> None:
    execute_resource(
        ResourceSpec(name, create_command, ("docker", "container", "rm", "--force")),
        runner,
        cleanup,
    )
    executor.run(("docker", "start", "--attach", name), timeout=300)
    exit_code = executor.run(
        ("docker", "container", "inspect", "--format", "{{.State.ExitCode}}", name)
    )
    if exit_code != "0":
        raise CanaryContractError("one-shot canary container failed")


def _api_post_command(
    plan: CanaryPlan, idempotency_key: str, *, scenario: str = "success"
) -> tuple[str, ...]:
    if re.fullmatch(r"[a-z][a-z0-9-]{0,30}", scenario) is None:
        raise CanaryContractError("provisioning scenario is invalid")
    script = (
        "import json,os,urllib.request;"
        "body_value=json.load(open('/run/fstack-canary-request.json',encoding='utf-8'));"
        f"scenario={scenario!r};"
        "body_value.update({"
        "'organization_slug':body_value['organization_slug']+'-'+scenario,"
        "'organization_name':body_value['organization_name']+' '+scenario,"
        "'public_domain':scenario+'.'+body_value['public_domain']"
        "}) if scenario!='success' else None;"
        "body=json.dumps(body_value,sort_keys=True,separators=(',',':')).encode();"
        "request=urllib.request.Request('http://127.0.0.1:8000/v1/provisioning/operations',"
        "data=body,method='POST',headers={'content-type':'application/json',"
        "'authorization':'Bearer '+os.environ['FSTACK_OPERATOR_TOKEN'],"
        f"'idempotency-key':{idempotency_key!r}}});"
        "response=json.load(urllib.request.urlopen(request,timeout=10));"
        "print(json.dumps({key:response[key] for key in "
        "('operation_id','deployment_id','replayed')},sort_keys=True,separators=(',',':')))"
    )
    return ("docker", "exec", plan.api_container, "python", "-c", script)


def _select_exact_managed_route(
    routes: object,
    deployment_hex: str,
) -> dict[str, object]:
    if re.fullmatch(r"[0-9a-f]{32}", deployment_hex) is None:
        raise CanaryContractError("deployment identity was invalid")
    if not isinstance(routes, list) or len(routes) != 1:
        raise CanaryContractError("Caddy routes response was invalid")
    route = routes[0]
    if (
        not isinstance(route, dict)
        or route.get("@id") != f"fstack-crusher-ingress-{deployment_hex}"
    ):
        raise CanaryContractError("managed Caddy route was invalid")
    return route


def _conflict_fixture(
    scenario: str,
    deployment_hex: str,
    host: str,
    managed_routes: list[object],
) -> ConflictFixture:
    if re.fullmatch(r"[0-9a-f]{32}", deployment_hex) is None:
        raise CanaryContractError("conflict deployment identity was invalid")
    expected_id = f"fstack-crusher-ingress-{deployment_hex}"
    expected_dial = f"fstack-crusher-{deployment_hex}:8000"
    desired = {
        "@id": expected_id,
        "match": [{"host": [host]}],
        "handle": [
            {
                "handler": "reverse_proxy",
                "upstreams": [{"dial": expected_dial}],
            }
        ],
    }
    wrong = {
        "@id": expected_id,
        "match": [{"host": [host]}],
        "handle": [
            {
                "handler": "reverse_proxy",
                "upstreams": [{"dial": "unmanaged.invalid:8000"}],
            }
        ],
    }
    other_same_host = {
        "@id": f"fstack-conflict-{deployment_hex}",
        "match": [{"host": [host]}],
        "handle": desired["handle"],
    }
    suffix = host.split(".", 1)[1]
    cases: dict[str, ConflictFixture] = {
        "expected-id-different-content": ConflictFixture(
            [*managed_routes, wrong], "caddy_route_id_conflict"
        ),
        "expected-id-plus-same-host": ConflictFixture(
            [*managed_routes, desired, other_same_host], "caddy_host_conflict"
        ),
        "same-host-other-id": ConflictFixture(
            [*managed_routes, other_same_host], "caddy_host_conflict"
        ),
        "wildcard-host": ConflictFixture(
            [
                *managed_routes,
                {
                    "@id": f"fstack-wildcard-{deployment_hex}",
                    "match": [{"host": [f"*.{suffix}"]}],
                    "handle": desired["handle"],
                },
            ],
            "caddy_host_conflict",
        ),
        "catch-all": ConflictFixture(
            [
                *managed_routes,
                {
                    "@id": f"fstack-catch-all-{deployment_hex}",
                    "match": [],
                    "handle": desired["handle"],
                },
            ],
            "caddy_routes_ambiguous",
        ),
        "unclassifiable": ConflictFixture(
            [
                *managed_routes,
                {
                    "@id": f"fstack-expression-{deployment_hex}",
                    "match": [{"expression": "path('/opaque')"}],
                    "handle": desired["handle"],
                },
            ],
            "caddy_routes_ambiguous",
        ),
        "missing-routes": ConflictFixture(None, "caddy_prerequisite_missing"),
    }
    try:
        return cases[scenario]
    except KeyError as exc:
        raise CanaryContractError("unknown Caddy conflict scenario") from exc


def _caddy_routes_command(
    plan: CanaryPlan,
    *,
    replacement: object | None = None,
    path: str = "/config/apps/http/servers/fstack/routes",
) -> tuple[str, ...]:
    if path not in {
        "/config/",
        "/config/apps/http/servers/fstack",
        "/config/apps/http/servers/fstack/routes",
    }:
        raise CanaryContractError("Caddy admin path is invalid")
    encoded = ""
    method = "GET"
    if replacement is not None:
        import base64

        encoded = base64.b64encode(
            json.dumps(replacement, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).decode("ascii")
        method = "PATCH"
    script = (
        "import base64;"
        "from fstack.caddy_admin import CaddyAdminConfig,CaddyAdminTransport;"
        f"body=base64.b64decode({encoded!r});method={method!r};"
        f"request=(method+' {path} HTTP/1.1\\r\\n'"
        "+'Host: localhost\\r\\nContent-Type: application/json\\r\\n'"
        "+'Content-Length: '+str(len(body))+'\\r\\nConnection: close\\r\\n\\r\\n').encode()+body;"
        "response=CaddyAdminTransport(CaddyAdminConfig.from_environ())._exchange(request);"
        "assert 200<=response.status<300,response.status;print(response.body.decode())"
    )
    return ("docker", "exec", plan.worker_container, "python", "-c", script)


def _adopt_caddy_route_command(plan: CanaryPlan, route: dict[str, object]) -> tuple[str, ...]:
    import base64

    encoded = base64.b64encode(
        json.dumps(route, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    script = (
        "import base64,json;"
        "from fstack.caddy_admin import CaddyAdmin,CaddyAdminConfig,CaddyAdminTransport;"
        f"route=json.loads(base64.b64decode({encoded!r}));"
        "config=CaddyAdminConfig.from_environ();"
        "CaddyAdmin(CaddyAdminTransport(config)).reconcile(route);print('adopted')"
    )
    return ("docker", "exec", plan.worker_container, "python", "-c", script)


def _api_status_command(plan: CanaryPlan, operation_id: str) -> tuple[str, ...]:
    script = (
        "import json,os,urllib.request;"
        f"operation_id={operation_id!r};"
        "request=urllib.request.Request('http://127.0.0.1:8000/v1/provisioning/operations/'"
        "+operation_id,headers={'authorization':'Bearer '+os.environ['FSTACK_OPERATOR_TOKEN']});"
        "response=json.load(urllib.request.urlopen(request,timeout=10));"
        "print(json.dumps({'operation_id':response['operation_id'],'state':response['state'],"
        "'error':response.get('error')},sort_keys=True,separators=(',',':')))"
    )
    return ("docker", "exec", plan.api_container, "python", "-c", script)


def _wait_for_succeeded_operation(
    executor: LocalCommandExecutor,
    plan: CanaryPlan,
    operation_id: str,
) -> dict[str, object]:
    for attempt in range(180):
        loaded = json.loads(executor.run(_api_status_command(plan, operation_id)))
        if not isinstance(loaded, dict):
            raise CanaryContractError("provisioning status was invalid")
        status: dict[str, object] = loaded
        state = status.get("state")
        if state == "succeeded":
            return status
        if state == "failed":
            raise CanaryContractError("provisioning operation failed")
        if state not in {"pending", "running"}:
            raise CanaryContractError("provisioning status was invalid")
        if attempt != 179:
            time.sleep(1)
    raise CanaryContractError("provisioning operation did not complete")


def _wait_for_failed_operation(
    executor: CommandExecutor,
    plan: CanaryPlan,
    operation_id: str,
    *,
    expected_code: str,
) -> None:
    for attempt in range(180):
        status = json.loads(executor.run(_api_status_command(plan, operation_id)))
        state = status.get("state")
        if state == "failed":
            error = status.get("error")
            code = error.get("code") if isinstance(error, dict) else None
            if code != expected_code:
                raise CanaryContractError("conflict failed with an unexpected stable code")
            return
        if state == "succeeded":
            raise CanaryContractError("conflict provisioning unexpectedly succeeded")
        if attempt + 1 < 180:
            time.sleep(1)
    raise CanaryContractError("conflict provisioning did not reach failed")


def _recover_expected_operations(
    executor: CommandExecutor,
    plan: CanaryPlan,
) -> dict[str, str]:
    base_slug = f"canary-{plan.inputs.nonce}"
    allowed_slugs = (base_slug, *(f"{base_slug}-{scenario}" for scenario in _CONFLICT_SCENARIOS))
    quoted_slugs = ",".join(f"'{slug}'" for slug in allowed_slugs)
    sql = (
        "SELECT id::text || '|' || deployment_id::text "
        "FROM provisioning_operations "
        f"WHERE organization_slug IN ({quoted_slugs}) ORDER BY id"
    )
    output = executor.run(
        (
            "docker",
            "exec",
            plan.postgres_container,
            "psql",
            "--username",
            "fstack",
            "--dbname",
            plan.database_name,
            "--tuples-only",
            "--no-align",
            "--command",
            sql,
        )
    )
    recovered: dict[str, str] = {}
    for raw_line in output.splitlines():
        parts = raw_line.strip().split("|")
        if len(parts) != 2:
            raise CanaryContractError("durable canary operation evidence was invalid")
        try:
            operation_id = str(uuid.UUID(parts[0]))
            deployment_id = str(uuid.UUID(parts[1]))
        except ValueError as exc:
            raise CanaryContractError("durable canary operation identity was invalid") from exc
        recovered[operation_id] = deployment_id
    return recovered


def _quiesce_canary_producers(executor: CommandExecutor, plan: CanaryPlan) -> None:
    names = tuple(
        name
        for name in (plan.api_container, plan.worker_container)
        if executor.exists(("docker", "container", "inspect", name))
    )
    if names:
        executor.run(("docker", "container", "stop", "--time", "10", *names), timeout=60)
    running = executor.run(("docker", "container", "ls", "--format", "{{.Names}}"))
    if not _canary_processes_absent(running, plan):
        raise CanaryContractError("canary producers remained active during cleanup")


def _capture_network_crusher_containers(
    executor: CommandExecutor,
    plan: CanaryPlan,
    expected_operations: dict[str, str],
    *,
    require_network: bool = True,
) -> list[tuple[str, str, str, str]]:
    if not expected_operations:
        return []
    if require_network and not executor.exists(("docker", "network", "inspect", plan.network)):
        return []
    filters = (
        ("--filter", f"network={plan.network}")
        if require_network
        else ("--filter", "label=io.fstack.managed=true")
    )
    output = executor.run(
        (
            "docker",
            "container",
            "ls",
            "--all",
            *filters,
            "--format",
            "{{.Names}}",
        )
    )
    expected_image_id = executor.run(
        (
            "docker",
            "image",
            "inspect",
            "--format",
            "{{.Id}}",
            plan.inputs.crusher_image,
        )
    )
    if re.fullmatch(r"sha256:[0-9a-f]{64}", expected_image_id) is None:
        raise CanaryContractError("Crusher image identity was invalid")
    captured: list[tuple[str, str, str, str]] = []
    for name in sorted(filter(None, output.splitlines())):
        if re.fullmatch(r"fstack-crusher(?:-migrate)?-[0-9a-f]{32}", name) is None:
            continue
        value = json.loads(
            executor.run(("docker", "container", "inspect", "--format", "{{json .}}", name))
        )[0]
        labels = value["Config"]["Labels"]
        configured_image = value["Config"].get("Image")
        image_id = value.get("Image")
        identifier = value["Id"]
        operation_id = labels.get("io.fstack.operation_id")
        deployment_id = labels.get("io.fstack.deployment_id")
        networks = value["NetworkSettings"]["Networks"]
        if not isinstance(operation_id, str) or operation_id not in expected_operations:
            continue
        expected_deployment = expected_operations[operation_id]
        deployment_hex = deployment_id.replace("-", "") if isinstance(deployment_id, str) else ""
        if (
            labels.get("io.fstack.managed") != "true"
            or expected_deployment != deployment_id
            or not name.endswith(deployment_hex)
            or re.fullmatch(r"[0-9a-f]{32}", deployment_hex) is None
            or configured_image != plan.inputs.crusher_image
            or image_id != expected_image_id
            or not isinstance(identifier, str)
            or re.fullmatch(r"[0-9a-f]{64}", identifier) is None
            or not isinstance(operation_id, str)
            or not isinstance(deployment_id, str)
            or (plan.network not in networks and (require_network or bool(networks)))
        ):
            raise CanaryContractError("network Crusher container ownership was ambiguous")
        captured.append((name, identifier, operation_id, deployment_id))
    return captured


def _merge_captured_resources(
    earlier: list[tuple[str, str, str, str]],
    later: list[tuple[str, str, str, str]],
) -> list[tuple[str, str, str, str]]:
    merged = {item[0]: item for item in earlier}
    for item in later:
        previous = merged.get(item[0])
        if previous is not None and previous != item:
            raise CanaryContractError("dynamic resource identity changed after capture")
        merged[item[0]] = item
    return list(merged.values())


def _cleanup_owned_crusher_containers(
    executor: CommandExecutor,
    plan: CanaryPlan,
    owned: list[tuple[str, str, str, str]],
) -> None:
    errors = 0
    if not owned:
        return
    expected_image_id = executor.run(
        (
            "docker",
            "image",
            "inspect",
            "--format",
            "{{.Id}}",
            plan.inputs.crusher_image,
        )
    )
    for name, captured_id, operation_id, deployment_id in reversed(owned):
        inspect = (
            "docker",
            "container",
            "inspect",
            "--format",
            "{{json .}}",
            name,
        )
        if not executor.exists(("docker", "container", "inspect", name)):
            continue
        try:
            raw = executor.run(inspect)
            value = json.loads(raw)
            labels = value[0]["Config"]["Labels"]
            configured_image = value[0]["Config"].get("Image")
            image_id = value[0].get("Image")
            networks = value[0]["NetworkSettings"]["Networks"]
            identifier = value[0]["Id"]
            deployment_hex = deployment_id.replace("-", "")
            if (
                labels.get("io.fstack.managed") != "true"
                or labels.get("io.fstack.operation_id") != operation_id
                or labels.get("io.fstack.deployment_id") != deployment_id
                or not name.endswith(deployment_hex)
                or configured_image != plan.inputs.crusher_image
                or image_id != expected_image_id
                or (plan.network not in networks and bool(networks))
                or identifier != captured_id
            ):
                raise CanaryContractError("dynamic Crusher container identity did not match")
            executor.run(("docker", "container", "rm", "--force", identifier), timeout=120)
            if executor.exists(("docker", "container", "inspect", name)):
                raise CanaryContractError("dynamic Crusher container remained after cleanup")
        except (CanaryContractError, KeyError, TypeError, json.JSONDecodeError):
            errors += 1
    if errors:
        raise CanaryContractError(f"dynamic cleanup failed for {errors} container(s)")


def run_live_canary(inputs: CanaryInputs) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    executor = LocalCommandExecutor()
    evidence = create_evidence_directory(repo_root, inputs.evidence_dir)
    plan = build_plan(inputs)
    source_workspace = evidence.path.parent / (f".{plan.prefix}-source-{secrets.token_hex(8)}")
    runtime_root = evidence.path.parent / (f".{plan.prefix}-runtime-{secrets.token_hex(8)}")
    acceptance_root = evidence.path.parent / (f".{plan.prefix}-tests-{secrets.token_hex(8)}")
    source_owned = False
    runtime_owned = False
    acceptance_owned = False
    prepared: PreparedSource | None = None
    prepared_tree_digest: str | None = None
    cleanup: CleanupRegistry | None = None
    docker_runner: DockerResourceRunner | None = None
    owned_crusher: list[tuple[str, str, str, str]] = []
    expected_operations: dict[str, str] = {}
    primary_error: Exception | None = None
    cleanup_error: Exception | None = None
    summary_payload: object | None = None
    secrets_bundle = RuntimeSecrets(
        postgres_password=secrets.token_hex(24),
        operator_token=secrets.token_hex(24),
        application_password=secrets.token_hex(24),
        magic_link_secret=secrets.token_hex(32),
    )
    forbidden = {
        secrets_bundle.postgres_password,
        secrets_bundle.operator_token,
        secrets_bundle.application_password,
        secrets_bundle.magic_link_secret,
    }
    try:
        prepared = prepare_source(
            repo_root,
            source_commit=inputs.source_commit,
            source_archive=inputs.source_archive,
            source_archive_sha256=inputs.source_archive_sha256,
            workspace=source_workspace,
        )
        source_owned = True
        _verify_executing_harness(Path(__file__).resolve(), prepared.root)
        prepared_tree_digest = _source_tree_digest(prepared.root)
        os.mkdir(acceptance_root, mode=0o700)
        acceptance_owned = True
        acceptance_tests = _run_acceptance_tests(prepared.root, acceptance_root)
        verified_images = verify_images(inputs, executor)
        assert_no_resource_collisions(plan, executor)
        prepare_runtime_files(plan, runtime_root, secrets_bundle)
        runtime_owned = True
        resource_specs = build_resource_specs(plan, prepared.root, runtime_root, secrets_bundle)
        by_name = {spec.name: spec for spec in resource_specs}
        docker_runner = DockerResourceRunner(executor, ownership_token=plan.invocation_token)
        cleanup = CleanupRegistry(docker_runner)

        for name in (
            plan.network,
            plan.control_volume,
            f"{plan.prefix}-fstack-base:source",
            plan.fstack_image,
            plan.postgres_container,
        ):
            execute_resource(by_name[name], docker_runner, cleanup)
        _wait_for_output(
            executor,
            (
                "docker",
                "exec",
                plan.postgres_container,
                "pg_isready",
                "-U",
                "fstack",
                "-d",
                plan.database_name,
            ),
            accept=lambda output: "accepting connections" in output,
        )
        execute_resource(by_name[plan.temporal_container], docker_runner, cleanup)
        _wait_for_output(
            executor,
            _temporal_health_command(plan),
            accept=lambda output: "SERVING" in output.upper(),
            attempts=120,
        )
        execute_resource(by_name[plan.caddy_container], docker_runner, cleanup)
        expected_socket = f"{plan.worker_uid}:{plan.caddy_socket_gid}:220"
        _wait_for_output(
            executor,
            (
                "docker",
                "exec",
                plan.caddy_container,
                "stat",
                "-c",
                "%u:%g:%a",
                "/run/fstack-caddy/caddy-admin.sock",
            ),
            accept=lambda output: output == expected_socket,
        )

        maintenance = {
            command.name: command
            for command in build_maintenance_steps(
                plan, prepared.root, runtime_root, secrets_bundle
            )
        }
        _one_shot_container(
            name=f"{plan.prefix}-migration-20261006_0003",
            create_command=maintenance["bootstrap-20261006-0003"].argv,
            runner=docker_runner,
            cleanup=cleanup,
            executor=executor,
        )
        execute_resource(by_name[plan.worker_container], docker_runner, cleanup)
        execute_resource(by_name[plan.api_container], docker_runner, cleanup)
        _wait_for_output(
            executor,
            maintenance["verify-new-poller"].argv,
            accept=_task_queue_has_pollers,
        )
        _wait_for_output(
            executor,
            (
                "docker",
                "exec",
                plan.api_container,
                "python",
                "-c",
                (
                    "import urllib.request;"
                    "urllib.request.urlopen('http://127.0.0.1:8000/health',timeout=2).read();"
                    "print('ready')"
                ),
            ),
            accept=lambda output: output == "ready",
        )
        executor.run(maintenance["stop-api-dispatcher-worker"].argv, timeout=60)
        _wait_for_output(
            executor,
            maintenance["verify-process-exit"].argv,
            accept=lambda output: _canary_processes_absent(output, plan),
        )
        _verify_temporal_pollers_quiesced(
            executor,
            maintenance["verify-no-old-poller"].argv,
        )
        _one_shot_container(
            name=f"{plan.prefix}-migration-20261013_0004",
            create_command=maintenance["upgrade-20261013-0004"].argv,
            runner=docker_runner,
            cleanup=cleanup,
            executor=executor,
        )
        _one_shot_container(
            name=f"{plan.prefix}-migration-20261015_0005",
            create_command=maintenance["upgrade-20261015-0005"].argv,
            runner=docker_runner,
            cleanup=cleanup,
            executor=executor,
        )

        executor.run(("docker", "container", "start", plan.worker_container), timeout=60)
        registration = _wait_for_output(
            executor,
            maintenance["verify-v1-v2-registration"].argv,
            accept=lambda output: output == "ProvisionCrusherWorkflow,ProvisionCrusherV2Workflow",
        )
        del registration
        _wait_for_output(
            executor,
            maintenance["verify-new-poller"].argv,
            accept=_task_queue_has_pollers,
        )
        listener_probe = (
            "docker",
            "exec",
            plan.worker_container,
            "python",
            "-c",
            (
                "import socket;"
                f"sock=socket.create_connection(({plan.caddy_ip!r},{plan.listener_port}),5);"
                "sock.close();print('connected')"
            ),
        )
        if executor.run(listener_probe) != "connected":
            raise CanaryContractError("worker could not reach the Caddy listener")

        executor.run(("docker", "container", "start", plan.api_container), timeout=60)
        _wait_for_output(
            executor,
            (
                "docker",
                "exec",
                plan.api_container,
                "python",
                "-c",
                (
                    "import urllib.request;"
                    "urllib.request.urlopen('http://127.0.0.1:8000/health',timeout=2).read();"
                    "print('ready')"
                ),
            ),
            accept=lambda output: output == "ready",
        )
        idempotency_key = f"{plan.prefix}-success"
        requested = json.loads(executor.run(_api_post_command(plan, idempotency_key)))
        operation_id = requested.get("operation_id")
        deployment_id = requested.get("deployment_id")
        if (
            type(operation_id) is not str
            or type(deployment_id) is not str
            or requested.get("replayed") is not False
        ):
            raise CanaryContractError("initial provisioning response was invalid")
        try:
            operation_hex = operation_id.replace("-", "")
            deployment_hex = deployment_id.replace("-", "")
            if len(operation_hex) != 32 or len(deployment_hex) != 32:
                raise ValueError
            int(operation_hex, 16)
            int(deployment_hex, 16)
        except (TypeError, ValueError) as exc:
            raise CanaryContractError("provisioning identities were invalid") from exc
        expected_operations[operation_id] = deployment_id

        if (
            executor.run(_dispatch_generation_command(plan, operation_id))
            != "fstack.provision_crusher.v2"
        ):
            raise CanaryContractError("post-cutover operation was not dispatched as v2")
        terminal_status = _wait_for_succeeded_operation(executor, plan, operation_id)
        if terminal_status.get("error") is not None:
            raise CanaryContractError("successful operation exposed an error")
        owned_crusher = _capture_network_crusher_containers(executor, plan, expected_operations)
        expected_crusher = f"fstack-crusher-{deployment_hex}"
        if not any(item[0] == expected_crusher for item in owned_crusher):
            raise CanaryContractError("successful provisioning Crusher was not discoverable")

        verification = {
            command.name: command
            for command in build_live_verification_commands(plan, deployment_id)
        }
        route_result = executor.run(verification["inspect-exact-caddy-route"].argv)
        if route_result != "exact":
            raise CanaryContractError("Caddy did not expose the exact expected route")
        direct_health = json.loads(executor.run(verification["probe-caddy-to-crusher"].argv))
        proxied_health = json.loads(executor.run(verification["probe-worker-through-caddy"].argv))
        expected_health = {"status": "ok", "service": "crusher"}
        if direct_health != expected_health or proxied_health != expected_health:
            raise CanaryContractError("Crusher health probes did not return the exact response")

        topology_ids_raw = executor.run(_submission_ids_command(plan, operation_id))
        topology_parts = topology_ids_raw.split("|")
        if len(topology_parts) != 2:
            raise CanaryContractError("topology submission identities were unavailable")
        topology_before = executor.run(_caddy_routes_command(plan, path="/config/"))
        topology_scenarios = ("wrong-network", "wrong-probe-ip", "missing-alias")
        for topology_scenario in topology_scenarios:
            topology_result = executor.run(
                _topology_mismatch_command(
                    plan,
                    operation_id=operation_id,
                    runtime_submission_id=topology_parts[0],
                    ingress_submission_id=topology_parts[1],
                    deployment_id=deployment_id,
                    scenario=topology_scenario,
                )
            )
            if topology_result != (
                f"{topology_scenario}|caddy_topology_invalid|reconcile_not_called"
            ):
                raise CanaryContractError("topology mismatch proof failed")
            if executor.run(_caddy_routes_command(plan, path="/config/")) != topology_before:
                raise CanaryContractError("topology rejection mutated Caddy configuration")

        counts_command = _durable_counts_command(plan, operation_id)
        counts_before_replay = executor.run(counts_command)
        replay = json.loads(executor.run(_api_post_command(plan, idempotency_key)))
        if replay.get("operation_id") != operation_id or replay.get("replayed") is not True:
            raise CanaryContractError("provisioning replay did not adopt the original operation")
        counts_after_replay = executor.run(counts_command)
        if counts_before_replay != counts_after_replay:
            raise CanaryContractError("provisioning replay created duplicate durable state")

        managed_routes_raw = executor.run(_caddy_routes_command(plan))
        managed_routes = json.loads(managed_routes_raw)
        managed_route = _select_exact_managed_route(managed_routes, deployment_hex)
        other_deployment_hex = hashlib.sha256(
            f"{plan.prefix}-unrelated-route".encode()
        ).hexdigest()[:32]
        if other_deployment_hex == deployment_hex:
            other_deployment_hex = ("0" if deployment_hex[0] != "0" else "1") + deployment_hex[1:]
        safe_route = {
            "@id": f"fstack-crusher-ingress-{other_deployment_hex}",
            "match": [{"host": [f"safe.{inputs.nonce}.canary.invalid"]}],
            "handle": [
                {
                    "handler": "reverse_proxy",
                    "upstreams": [{"dial": f"fstack-crusher-{other_deployment_hex}:8000"}],
                }
            ],
        }
        adoption_routes = [managed_route, safe_route]
        executor.run(_caddy_routes_command(plan, replacement=adoption_routes))
        adoption_before = executor.run(_caddy_routes_command(plan, path="/config/"))
        if executor.run(_adopt_caddy_route_command(plan, managed_route)) != "adopted":
            raise CanaryContractError("exact managed Caddy route was not adopted")
        if executor.run(_caddy_routes_command(plan, path="/config/")) != adoption_before:
            raise CanaryContractError("managed route adoption mutated Caddy configuration")
        executor.run(_caddy_routes_command(plan, replacement=managed_routes))
        managed_server = json.loads(
            executor.run(_caddy_routes_command(plan, path="/config/apps/http/servers/fstack"))
        )
        if not isinstance(managed_server, dict) or managed_server.get("routes") != managed_routes:
            raise CanaryContractError("managed Caddy server response was invalid")
        conflict_checks: dict[str, bool] = {"adoption": True}
        conflict_scenarios = _CONFLICT_SCENARIOS
        for scenario in conflict_scenarios:
            executor.run(("docker", "container", "stop", plan.worker_container), timeout=60)
            running = executor.run(
                (
                    "docker",
                    "container",
                    "inspect",
                    "--format",
                    "{{.State.Running}}",
                    plan.worker_container,
                )
            )
            if running != "false":
                raise CanaryContractError("worker did not stop before conflict setup")
            conflict_response = json.loads(
                executor.run(
                    _api_post_command(plan, f"{plan.prefix}-{scenario}", scenario=scenario)
                )
            )
            conflict_operation = conflict_response.get("operation_id")
            conflict_deployment = conflict_response.get("deployment_id")
            if (
                not isinstance(conflict_operation, str)
                or not isinstance(conflict_deployment, str)
                or conflict_response.get("replayed") is not False
            ):
                raise CanaryContractError("conflict provisioning response was invalid")
            try:
                conflict_operation = str(uuid.UUID(conflict_operation))
                conflict_deployment = str(uuid.UUID(conflict_deployment))
            except ValueError as exc:
                raise CanaryContractError("conflict provisioning identities were invalid") from exc
            conflict_hex = conflict_deployment.replace("-", "")
            expected_operations[conflict_operation] = conflict_deployment
            if re.fullmatch(r"[0-9a-f]{32}", conflict_hex) is None:
                raise CanaryContractError("conflict deployment identity was invalid")
            conflict_host = f"{scenario}.{inputs.nonce}.canary.invalid"
            fixture = _conflict_fixture(
                scenario,
                conflict_hex,
                conflict_host,
                managed_routes,
            )
            if fixture.routes is None:
                server_without_routes = dict(managed_server)
                server_without_routes.pop("routes", None)
                executor.run(
                    _caddy_routes_command(
                        plan,
                        replacement=server_without_routes,
                        path="/config/apps/http/servers/fstack",
                    )
                )
            else:
                executor.run(_caddy_routes_command(plan, replacement=fixture.routes))
            conflict_before = executor.run(_caddy_routes_command(plan, path="/config/"))
            executor.run(("docker", "container", "start", plan.worker_container), timeout=60)
            _wait_for_output(
                executor,
                maintenance["verify-new-poller"].argv,
                accept=_task_queue_has_pollers,
            )
            _wait_for_failed_operation(
                executor,
                plan,
                conflict_operation,
                expected_code=fixture.expected_code,
            )
            discovered_conflict = _capture_network_crusher_containers(
                executor, plan, expected_operations
            )
            owned_crusher = _merge_captured_resources(owned_crusher, discovered_conflict)
            conflict_after = executor.run(_caddy_routes_command(plan, path="/config/"))
            if conflict_after != conflict_before:
                raise CanaryContractError("conflict failure mutated Caddy configuration")
            conflict_checks[scenario] = True
            executor.run(("docker", "container", "stop", plan.worker_container), timeout=60)
            if fixture.routes is None:
                executor.run(
                    _caddy_routes_command(
                        plan,
                        replacement=managed_server,
                        path="/config/apps/http/servers/fstack",
                    )
                )
            else:
                executor.run(_caddy_routes_command(plan, replacement=managed_routes))
        executor.run(("docker", "container", "start", plan.worker_container), timeout=60)
        _wait_for_output(
            executor,
            maintenance["verify-new-poller"].argv,
            accept=_task_queue_has_pollers,
        )
        migration_head = executor.run(
            ("docker", "exec", plan.worker_container, "alembic", "current")
        )
        versions = {
            "docker": executor.run(("docker", "version", "--format", "{{.Server.Version}}")),
            "git": executor.run(("git", "--version")),
            "caddy": "v2.11.7",
        }
        summary_payload = {
            "status": "succeeded",
            "source_identity": prepared.identity,
            "source_archive_sha256": prepared.archive_sha256,
            "image_digests": verified_images,
            "test_outputs": acceptance_tests,
            "migration_head": migration_head,
            "versions": versions,
            "checks": {
                "dual_workflow_registration": True,
                "pre_cutover_processes_and_poller_stopped": True,
                "post_cutover_v2_dispatch_succeeded": True,
                "old_processes_stopped_before_cutover": True,
                "new_poller": True,
                "initial_succeeded": True,
                "replay_adopted": True,
                "topology_matrix_rejected_before_reconcile_without_mutation": True,
                "managed_route_adopted_without_mutation": conflict_checks["adoption"],
                "all_real_caddy_conflicts_no_mutation": all(
                    conflict_checks[scenario] for scenario in conflict_scenarios
                ),
            },
        }
    except Exception as exc:
        primary_error = exc
    finally:
        producers_quiesced = False
        try:
            _quiesce_canary_producers(executor, plan)
            producers_quiesced = True
        except Exception as exc:
            cleanup_error = exc
        if producers_quiesced:
            try:
                should_recover_operations = docker_runner is not None and (
                    plan.api_container in docker_runner.creation_attempted
                    or plan.worker_container in docker_runner.creation_attempted
                )
                if should_recover_operations and executor.exists(
                    ("docker", "container", "inspect", plan.postgres_container)
                ):
                    recovered_operations = _recover_expected_operations(executor, plan)
                    for recovered_operation, recovered_deployment in recovered_operations.items():
                        existing_deployment = expected_operations.get(recovered_operation)
                        if (
                            existing_deployment is not None
                            and existing_deployment != recovered_deployment
                        ):
                            raise CanaryContractError("durable operation ownership changed")
                        expected_operations[recovered_operation] = recovered_deployment
                discovered = _capture_network_crusher_containers(
                    executor,
                    plan,
                    expected_operations,
                    require_network=False,
                )
                owned_crusher = _merge_captured_resources(owned_crusher, discovered)
            except Exception as exc:
                cleanup_error = cleanup_error or exc
            try:
                _cleanup_owned_crusher_containers(executor, plan, owned_crusher)
            except Exception as exc:
                cleanup_error = cleanup_error or exc
        try:
            if cleanup is not None:
                cleanup.run_and_verify()
        except Exception as exc:
            cleanup_error = cleanup_error or exc
        try:
            if (
                prepared is not None
                and prepared_tree_digest is not None
                and _source_tree_digest(prepared.root) != prepared_tree_digest
            ):
                raise CanaryContractError("prepared immutable source changed during canary")
        except Exception as exc:
            cleanup_error = cleanup_error or exc
        for workspace, owned in (
            (acceptance_root, acceptance_owned),
            (runtime_root, runtime_owned),
            (source_workspace, source_owned),
        ):
            if owned:
                try:
                    shutil.rmtree(workspace)
                except Exception as exc:
                    cleanup_error = cleanup_error or exc
        try:
            if inputs.source_commit is not None:
                status = _captured(
                    ["git", "status", "--porcelain=v1", "--untracked-files=all"],
                    cwd=repo_root,
                )
                if status:
                    raise CanaryContractError("canary modified the source tree")
        except Exception as exc:
            cleanup_error = cleanup_error or exc
        try:
            if summary_payload is None and primary_error is None and cleanup_error is None:
                raise CanaryContractError("successful canary produced no evidence payload")
            if summary_payload is not None:
                _finalize_success_evidence(
                    evidence,
                    summary_payload,
                    forbidden_values=forbidden,
                    primary_error=primary_error,
                    cleanup_error=cleanup_error,
                )
        except Exception as exc:
            cleanup_error = cleanup_error or exc
        evidence.close()
    if primary_error is not None:
        if cleanup_error is not None:
            raise CanaryContractError(
                "canary failed and cleanup verification also failed"
            ) from primary_error
        raise primary_error
    if cleanup_error is not None:
        raise cleanup_error


def main(
    argv: list[str] | None = None,
    *,
    runner: Callable[[CanaryInputs], None] | None = None,
) -> int:
    args = parse_args(argv)
    nonce = args.nonce or secrets.token_hex(6)
    inputs = CanaryInputs(
        source_commit=args.source_commit,
        source_archive=args.source_archive,
        source_archive_sha256=args.source_archive_sha256,
        caddy_image=args.caddy_image_digest,
        postgres_image=args.postgres_image_digest,
        temporal_image=args.temporal_image_digest,
        crusher_image=args.crusher_image_digest,
        evidence_dir=args.evidence_dir,
        nonce=nonce,
    )
    (runner or run_live_canary)(inputs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
