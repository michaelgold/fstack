"""Opt-in native Tailcat runner lifecycle. Product command is local owner configuration.

The coordinator supplies only a scoped immutable assignment. It cannot supply
Docker arguments, host paths, keys or an arbitrary executable. No Docker socket
is mounted into the workload. This is a trusted user-managed runner, not a
hostile-tenant sandbox or a managed cloud provider adapter.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import select
import signal
import subprocess
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from fstack.runner_transport import exchange_json


def _docker(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=5, check=False)


def _name(assignment: dict[str, Any]) -> str:
    return "fstack-session-" + UUID(assignment["session_id"]).hex


def _remove_owned(assignment: dict[str, Any]) -> None:
    name = _name(assignment)
    present = _docker("ps", "-aq", "--filter", "name=^/" + name + "$")
    if present.returncode:
        raise RuntimeError("local cleanup unconfirmed")
    if not present.stdout.strip():
        return
    inspected = _docker("inspect", name)
    if inspected.returncode:
        check = _docker("ps", "-aq", "--filter", "name=^/" + name + "$")
        if not check.returncode and not check.stdout.strip():
            return  # --rm may have completed between inventory and inspection.
        raise RuntimeError("local cleanup unconfirmed")
    container = json.loads(inspected.stdout)[0]
    labels = container["Config"].get("Labels") or {}
    if (
        labels.get("fstack.session") != assignment["session_id"]
        or labels.get("fstack.workspace") != assignment["workspace_id"]
        or container["Config"]["Image"] != assignment["image"]
    ):
        raise RuntimeError("local cleanup unconfirmed")
    _docker("rm", "-f", container["Id"])
    check = _docker("ps", "-aq", "--filter", "name=^/" + name + "$")
    if check.returncode or check.stdout.strip():
        raise RuntimeError("local cleanup unconfirmed")


def _validate(assignment: dict[str, Any], image: str, command: list[str]) -> float:
    if (
        assignment["image"] != image
        or re.fullmatch(r"[a-z0-9./_:-]+@sha256:[0-9a-f]{64}", image) is None
        or assignment["generation"] != 1
        or assignment["state"] not in ("leased", "ready")
        or not 512 <= assignment["memory_mib"] <= 4096
        or not 100 <= assignment["cpu_millis"] <= 2000
        or re.fullmatch(r"nodekey:[0-9a-f]{64}", assignment["client_public_key"]) is None
        or assignment["client_public_key"] == "nodekey:" + "0" * 64
        or not command
        or not command[0].startswith("/")
        or any(not isinstance(arg, str) or "\x00" in arg for arg in command)
    ):
        raise ValueError("invalid local session configuration")
    UUID(assignment["workspace_id"])
    _name(assignment)
    expires = datetime.fromisoformat(assignment["expires_at"].replace("Z", "+00:00"))
    remaining = (expires - datetime.now(UTC)).total_seconds()
    if not 0 < remaining <= 300:
        raise ValueError("invalid local session lifetime")
    return remaining


def _supervise() -> int:
    """Child owns absolute expiry, EOF shutdown and removal even if poller dies."""
    process: subprocess.Popen[bytes] | None = None
    name = None
    stopped = threading.Event()
    try:
        config = json.loads(sys.stdin.buffer.readline(32769))
        assignment, image, command = config["assignment"], config["image"], config["command"]
        deadline = time.monotonic() + _validate(assignment, image, command)
        name = _name(assignment)
        # Never adopt/relaunch an ambiguous prior generation. Fail closed and let
        # the owner reconcile; automatic retry/reassignment is not this slice.
        prior = _docker("ps", "-aq", "--filter", "name=^/" + name + "$")
        if prior.returncode or prior.stdout.strip():
            name = None
            return 1
        signal.signal(signal.SIGTERM, lambda *_: stopped.set())
        signal.signal(signal.SIGINT, lambda *_: stopped.set())

        def eof():  # type: ignore[no-untyped-def]
            # A daemon may still be waiting at interpreter shutdown on expiry.
            # Never hold Python's buffered-reader lock across this blocking read.
            os.read(sys.stdin.fileno(), 1)
            stopped.set()

        threading.Thread(target=eof, daemon=True).start()
        with tempfile.TemporaryDirectory(prefix="fstack-session-private-") as directory:
            os.chmod(directory, 0o700)
            env = {
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "HOME": directory,
                "XDG_CONFIG_HOME": directory,
                "TMPDIR": directory,
            }
            generated = subprocess.run(
                [config["tailcat"], "genkey", "--key=default", "--region=302"],
                env=env,
                capture_output=True,
                timeout=20,
                check=False,
            )
            if generated.returncode:
                return 1
            address = generated.stdout.decode().strip()
            if (
                not address
                or len(address) > 4096
                or time.monotonic() >= deadline
                or stopped.is_set()
            ):
                return 1
            private = Path(directory) / "tailcat/keys/default.private.json"
            private.chmod(0o600)
            cpu = f"{assignment['cpu_millis'] // 1000}.{assignment['cpu_millis'] % 1000:03d}"
            args = [
                config["tailcat"],
                "serve",
                "--key=" + str(private),
                "--allow=" + assignment["client_public_key"],
                "exec",
                "--",
                "/usr/bin/docker",
                "run",
                "--rm",
                "-i",
                "--name",
                name,
                "--label",
                "fstack.session=" + assignment["session_id"],
                "--label",
                "fstack.workspace=" + assignment["workspace_id"],
                "--network",
                "none",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                "256",
                "--memory",
                str(assignment["memory_mib"]) + "m",
                "--cpus",
                cpu,
                "--tmpfs",
                "/tmp:rw,nosuid,nodev,size=512m",
                "--tmpfs",
                "/workspace:rw,nosuid,nodev,size=512m,uid=10001,gid=10001",
                "--entrypoint",
                command[0],
                image,
                *command[1:],
            ]
            process = subprocess.Popen(
                args,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            # Private pipe to poller, not a log or public discovery record.
            print(json.dumps({"address": address}), flush=True)
            while time.monotonic() < deadline and not stopped.wait(0.1):
                if process.poll() is not None:
                    break
            return 0
    except Exception:
        return 1
    finally:
        if process is not None:
            for sig in (signal.SIGTERM, signal.SIGKILL):
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, sig)
                if sig == signal.SIGTERM:
                    time.sleep(0.2)
            process.wait(timeout=3)
        if name is not None:
            _remove_owned(assignment)


class LocalSession:
    def __init__(
        self,
        assignment: dict[str, Any],
        *,
        image: str,
        command: list[str],
        tailcat: str,
        lock_fd: int,
    ):
        _validate(assignment, image, command)
        self.assignment = assignment
        self.process = subprocess.Popen(
            [sys.executable, "-m", "fstack.local_session_runner"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            pass_fds=(lock_fd,),
        )
        assert self.process.stdin is not None and self.process.stdout is not None
        try:
            self.process.stdin.write(
                (
                    json.dumps(
                        {
                            "assignment": assignment,
                            "image": image,
                            "command": command,
                            "tailcat": tailcat,
                        }
                    )
                    + "\n"
                ).encode()
            )
            self.process.stdin.flush()
            if not select.select([self.process.stdout], [], [], 25)[0]:
                raise RuntimeError
            data = self.process.stdout.readline(8193)
            self.address = json.loads(data)["address"]
            if self.process.poll() is not None:
                raise RuntimeError
        except Exception:
            self.close()
            raise RuntimeError("local session startup failed") from None

    def close(self) -> None:
        if self.process.stdin is not None:
            self.process.stdin.close()  # EOF is a graceful stop; supervisor removes workload.
        try:
            self.process.wait(timeout=25)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=3)
            raise RuntimeError("local cleanup unconfirmed") from None
        finally:
            if self.process.stdout is not None:
                self.process.stdout.close()
        if self.process.returncode != 0:
            raise RuntimeError("local cleanup unconfirmed")


def run_assignment(
    origin: str,
    runner: str,
    token: str,
    *,
    image: str,
    command: list[str],
    tailcat: str,
    allow_loopback_http: bool = False,
    lock_directory: Path | None = None,
) -> dict[str, Any]:
    """One local writer; the supervisor retains the lock through crash cleanup."""
    directory = lock_directory or Path(tempfile.gettempdir())
    path = directory / ("fstack-runner-" + UUID(runner).hex + ".lock")
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        stat = os.fstat(fd)
        if stat.st_uid != os.getuid() or stat.st_mode & 0o077:
            raise RuntimeError("unsafe runner lock")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return _run_assignment(
            origin,
            runner,
            token,
            image=image,
            command=command,
            tailcat=tailcat,
            allow_loopback_http=allow_loopback_http,
            lock_fd=fd,
        )
    finally:
        os.close(fd)
        # Do not unlink: concurrent opens could then lock different inodes.


def _run_assignment(
    origin: str,
    runner: str,
    token: str,
    *,
    image: str,
    command: list[str],
    tailcat: str,
    allow_loopback_http: bool,
    lock_fd: int,
) -> dict[str, Any]:
    """Pull and complete one assignment. No launch retry after an ambiguous generation."""
    prefix = f"/v1/runners/{UUID(runner)}"

    def call(path: str, body: dict[str, Any]) -> dict[str, Any]:
        return exchange_json(
            origin, prefix + path, token, body, allow_loopback_http=allow_loopback_http
        )

    pulled = call("/assignment", {})
    assigned = pulled["assignment"]
    if assigned is None:
        return {"status": "idle"}
    sid = str(UUID(assigned["session_id"]))
    suffix = f"/sessions/{sid}"
    generation = {"generation": assigned["generation"]}
    if assigned["state"] == "stopping" or not pulled["launch"]:
        # No relaunch of an ambiguous generation. Reconcile before acknowledgement.
        _remove_owned(assigned)
        call(suffix + "/stopped", generation)
        return {"status": "stopped", "session_id": sid}
    local = LocalSession(assigned, image=image, command=command, tailcat=tailcat, lock_fd=lock_fd)
    try:
        call(suffix + "/ready", {**generation, "address": local.address})
        while local.process.poll() is None:
            time.sleep(0.2)
            current = call("/assignment", {})["assignment"]
            if current is None or current["session_id"] != sid or current["state"] == "stopping":
                break
            call(suffix + "/ready", {**generation, "address": local.address})
    except RuntimeError:
        # Stop on transport/authority loss. Only a subsequently accepted deletion
        # receipt can close durable state; failed acknowledgement remains unresolved.
        pass
    finally:
        local.close()
    call(suffix + "/stopped", generation)
    return {"status": "stopped", "session_id": sid}


if __name__ == "__main__":
    # Private subprocess protocol only; no credentials or addresses in CLI args.
    raise SystemExit(_supervise())
