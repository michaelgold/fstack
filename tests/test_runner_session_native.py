"""Opt-in actual product acceptance; generic Core never imports Blender semantics."""

import contextlib
import json
import os
import queue
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path
from uuid import UUID

import pytest
import uvicorn
from sqlalchemy import text
from test_runner_discovery_postgres import discovery  # noqa: F401
from test_runner_sessions_postgres import admission

IMAGE = (
    "ghcr.io/bpy-dev/bpy-runtime@sha256:"
    "325ed79d5b34613d981786ec392d2b440bc88a05e5bfc26f89085ba32c6671b2"
)
BOOTSTRAP = (
    "import os; fd=os.dup(1); os.dup2(2,1); import bpy; "
    "bpy.ops.wm.save_as_mainfile(filepath='/workspace/start.blend'); "
    "os.dup2(fd,1); os.close(fd); "
    "os.execv('/opt/venv/bin/python',['/opt/venv/bin/python',"
    "'/opt/runtime/entrypoint.py','--transport','stdio'])"
)


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=30, check=True)


class Wire:
    def __init__(self, tailcat, home, address):
        self.process = subprocess.Popen(
            [str(tailcat), address, "80"],
            env={"HOME": str(home), "XDG_CONFIG_HOME": str(home), "PATH": "/usr/bin:/bin"},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        self.queue = queue.Queue()
        self.counter = 0

        def read():
            for line in self.process.stdout:
                with contextlib.suppress(ValueError):
                    self.queue.put(json.loads(line))

        self.reader = threading.Thread(target=read, daemon=True)
        self.reader.start()

    def call(self, method, params, timeout=60):
        self.counter += 1
        self.process.stdin.write(
            (
                json.dumps(
                    {"jsonrpc": "2.0", "id": self.counter, "method": method, "params": params}
                )
                + "\n"
            ).encode()
        )
        self.process.stdin.flush()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = self.queue.get(timeout=max(0.01, deadline - time.monotonic()))
            if response.get("id") == self.counter:
                assert "error" not in response
                assert not response["result"].get("isError")
                return response["result"]
        raise TimeoutError

    def initialize(self, timeout=60):
        self.call(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "fstack-session-native", "version": "1"},
            },
            timeout=timeout,
        )
        self.process.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
        self.process.stdin.flush()

    def close(self):
        self.process.terminate()
        try:
            self.process.wait(3)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(3)
        self.process.stdin.close()
        self.process.stdout.close()
        self.reader.join(3)
        assert not self.reader.is_alive()


@pytest.mark.skipif(os.environ.get("FSTACK_NATIVE_SESSION") != "1", reason="opt-in Docker/Tailcat")
@pytest.mark.parametrize("ending", ["stop", "expiry"])
def test_coordinator_runner_native_mcp_version_and_cleanup(
    discovery,  # noqa: F811
    tmp_path,
    monkeypatch,
    ending,  # noqa: F811
):
    from fstack.local_session_runner import run_assignment

    engine, client, account, owner, _ = discovery
    cid = docker("create", IMAGE).stdout.strip()
    tailcat = tmp_path / "tailcat"
    try:
        docker("cp", cid + ":/usr/local/bin/tailcat", str(tailcat))
    finally:
        docker("rm", cid)
    identities = tmp_path / "agent"
    identities.mkdir(mode=0o700)
    public = subprocess.run(
        [str(tailcat), "genkey", "--client", "--key=client-default"],
        env={"HOME": str(identities), "XDG_CONFIG_HOME": str(identities), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        timeout=20,
        check=True,
    ).stdout.strip()
    runner_private = tmp_path / "runner-private"
    runner_private.mkdir(mode=0o700)
    monkeypatch.setenv("TMPDIR", str(runner_private))
    enrolled_response = client.post(
        f"/v1/auth/accounts/{account}/runners",
        headers=owner,
        json={"machine_class": "native-test-cpu", "image": IMAGE, "ttl_seconds": 300},
    )
    assert enrolled_response.status_code == 200
    enrolled = enrolled_response.json()
    body = admission(
        enrolled["runner_id"],
        client_public_key=public,
        ttl_seconds=180 if ending == "stop" else 35,
        memory_mib=4096,
        cpu_millis=2000,
    )
    created = client.post(f"/v1/auth/accounts/{account}/sessions", headers=owner, json=body)
    assert created.status_code == 200
    sid = body["session_id"]
    access = {"authorization": "Bearer " + body["session_token"]}
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    origin = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(client.app, access_log=False, log_level="error"))
    server_thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    server_thread.start()
    outcome = queue.Queue()
    wire = None

    def runner():
        try:
            outcome.put(
                run_assignment(
                    origin,
                    enrolled["runner_id"],
                    enrolled["runner_token"],
                    image=IMAGE,
                    command=["/opt/venv/bin/python", "-c", BOOTSTRAP],
                    tailcat=str(tailcat),
                    allow_loopback_http=True,
                    lock_directory=tmp_path,
                )
            )
        except Exception as exc:
            outcome.put(type(exc).__name__)  # Never emit exception values / secrets.

    worker = threading.Thread(target=runner, daemon=True)
    try:
        deadline = time.monotonic() + 5
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.02)
        assert server.started
        worker.start()
        deadline = time.monotonic() + 30
        address = None
        while time.monotonic() < deadline:
            state = client.get(f"/v1/sessions/{sid}", headers=access)
            if state.status_code == 200 and state.json()["address"]:
                address = state.json()["address"]
                break
            if not outcome.empty():
                raise AssertionError("runner stopped before handoff: " + str(outcome.get()))
            time.sleep(0.1)
        assert address is not None
        if ending == "stop":
            wrong = tmp_path / "wrong-agent"
            wrong.mkdir(mode=0o700)
            subprocess.run(
                [str(tailcat), "genkey", "--client", "--key=client-default"],
                env={"HOME": str(wrong), "XDG_CONFIG_HOME": str(wrong), "PATH": "/usr/bin:/bin"},
                capture_output=True,
                timeout=20,
                check=True,
            )
            denied = Wire(tailcat, wrong, address)
            try:
                with pytest.raises(queue.Empty):
                    denied.initialize(timeout=2)
            finally:
                denied.close()
                shutil.rmtree(wrong)
            assert (
                docker(
                    "ps", "-aq", "--filter", "name=^/fstack-session-" + UUID(sid).hex + "$"
                ).stdout.strip()
                == ""
            )
        wire = Wire(tailcat, identities, address)
        wire.initialize(timeout=25)
        tools = wire.call("tools/list", {})
        assert any(tool["name"] == "execute_blender_code_for_cli" for tool in tools["tools"])
        version = wire.call(
            "tools/call",
            {
                "name": "execute_blender_code_for_cli",
                "arguments": {
                    "blend_file": "/workspace/start.blend",
                    "code": "import bpy; result={'version':bpy.app.version_string}",
                },
            },
            timeout=25,
        )
        parsed = json.loads(version["content"][0]["text"])
        assert parsed["version"].split()[0] == "5.3.0"
        container = json.loads(docker("inspect", "fstack-session-" + UUID(sid).hex).stdout)[0]
        assert not container["HostConfig"]["Binds"]
        assert container["HostConfig"]["NetworkMode"] == "none"
        client_key = json.loads(
            (identities / "tailcat/keys/client-default.private.json").read_text()
        )
        server_files = list(runner_private.glob("*/tailcat/keys/default.private.json"))
        assert len(server_files) == 1
        server_key = json.loads(server_files[0].read_text())
        with engine.connect() as db:
            durable = db.execute(
                text("SELECT row_to_json(s)::text FROM local_sessions s WHERE id=:s"), {"s": sid}
            ).scalar_one()
        serialized_container = json.dumps(container)
        assert all(
            value not in durable and value not in serialized_container
            for value in (address, client_key["Private"], server_key["Private"])
        )
        assert client_key["Private"] != server_key["Private"]
        if ending == "stop":
            assert (
                client.post(f"/v1/sessions/{sid}/stop", headers=access, json={}).status_code == 200
            )
        result = outcome.get(timeout=45)
        assert isinstance(result, dict), result
        assert result["status"] == "stopped"
        worker.join(3)
        assert not worker.is_alive()
        name = "fstack-session-" + UUID(sid).hex
        assert docker("ps", "-aq", "--filter", "name=^/" + name + "$").stdout.strip() == ""
        with engine.begin() as db:
            row = db.execute(
                text("SELECT state,stopped_at FROM local_sessions WHERE id=:s"), {"s": sid}
            ).one()
            assert row.state == "stopped" and row.stopped_at is not None
        assert list(runner_private.iterdir()) == []
        receipt = {
            "ending": ending,
            "version": parsed["version"],
            "image": IMAGE,
            "native_mcp": True,
            "version_response": version,
            "removed": True,
            "private_keys_separate_and_absent_from_db_container": True,
            "runner_private_directory_removed": True,
        }
        evidence = os.environ.get("FSTACK_NATIVE_EVIDENCE")
        if evidence:
            destination = Path(evidence)
            destination.mkdir(parents=True, exist_ok=True)
            (destination / f"native-{ending}.json").write_text(json.dumps(receipt, indent=2))
    finally:
        if wire is not None:
            wire.close()
        if worker.is_alive():
            client.post(f"/v1/sessions/{sid}/stop", headers=access, json={})
            worker.join(50)
        server.should_exit = True
        server_thread.join(5)
        sock.close()
        # Only the exact disposable test's workload; never another service.
        subprocess.run(
            ["docker", "rm", "-f", "fstack-session-" + UUID(sid).hex],
            capture_output=True,
            timeout=10,
            check=False,
        )
        shutil.rmtree(identities)
        assert not worker.is_alive() and not server_thread.is_alive()
