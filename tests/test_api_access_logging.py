from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def test_container_command_omits_certificate_candidate_from_uvicorn_logs() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    command_line = next(line for line in dockerfile.splitlines() if line.startswith("CMD "))
    command = json.loads(command_line.removeprefix("CMD "))
    port = _free_port()
    command[command.index("--host") + 1] = "127.0.0.1"
    command[command.index("--port") + 1] = str(port)
    environment = os.environ | {
        "FSTACK_DATABASE_URL": "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid",
        "FSTACK_OPERATOR_TOKEN": "test-operator-token",
    }
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    marker = "CERTIFICATE-CANDIDATE-SECRET-MARKER.fstack.com"
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1):
                    break
            except Exception:
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        try:
            urllib.request.urlopen(
                f"http://127.0.0.1:{port}/internal/caddy/ask?domain={marker}",
                timeout=2,
            )
        except urllib.error.HTTPError as exc:
            assert exc.code == 400
    finally:
        process.terminate()
        stdout, stderr = process.communicate(timeout=10)

    assert marker not in stdout
    assert marker not in stderr
