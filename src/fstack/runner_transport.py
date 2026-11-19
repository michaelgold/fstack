"""Bounded, secret-safe outbound JSON transport; no redirects or ambient proxies."""

from __future__ import annotations

import json
import re
import socket
import ssl
import subprocess
import sys
from typing import Any
from urllib.parse import urlsplit

from fstack.http_framing import _exchange


def exchange_json(
    origin: str,
    path: str,
    token: str,
    body: dict[str, Any] | None,
    *,
    allow_loopback_http: bool = False,
    timeout: float = 10,
) -> dict[str, Any]:
    parsed = urlsplit(origin)
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path
        or parsed.query
        or parsed.fragment
        or len(origin) > 2048
        or any(ord(c) <= 32 or ord(c) >= 127 for c in origin)
        or not (
            parsed.scheme == "https"
            or (
                allow_loopback_http
                and parsed.scheme == "http"
                and parsed.hostname in ("127.0.0.1", "::1")
            )
        )
        or not re.fullmatch(r"/v1/[a-zA-Z0-9/_-]{1,200}", path)
        or not re.fullmatch(r"fs_[rx]_[A-Za-z0-9_-]{1,120}", token)
        or not 0 < timeout <= 10
    ):
        raise ValueError("invalid runner configuration")
    payload = json.dumps({"origin": origin, "path": path, "token": token, "body": body}).encode()
    if len(payload) > 16384:
        raise ValueError("invalid runner configuration")
    outcome = None
    try:
        # Hard process deadline also bounds DNS/TLS/header stalls. Credentials are
        # stdin-only, never argv/env. run kills/reaps the owned child on timeout;
        # its socket disappears even if the remote peer keeps sending bytes.
        result = subprocess.run(
            [sys.executable, "-m", "fstack.runner_transport"],
            input=payload,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        if result.returncode == 0 and len(result.stdout) <= 16384:
            decoded = json.loads(result.stdout)
            if isinstance(decoded, dict):
                outcome = decoded
    except Exception:
        pass
    if outcome is None:
        raise RuntimeError("runner exchange failed")
    return outcome


def _child() -> int:
    try:
        values = json.loads(sys.stdin.buffer.read(16385))
        origin = urlsplit(values["origin"])
        host = origin.hostname
        port = origin.port or (443 if origin.scheme == "https" else 80)
        body = b"" if values["body"] is None else json.dumps(values["body"]).encode()
        method = "GET" if values["body"] is None else "POST"
        request = (
            f"{method} {values['path']} HTTP/1.1\r\nHost: {origin.netloc}\r\n"
            f"Authorization: Bearer {values['token']}\r\nConnection: close\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n"
        ).encode("ascii") + body

        def connect(timeout: float) -> socket.socket:
            raw = socket.create_connection((host, port), timeout=timeout)
            try:
                if origin.scheme == "https":
                    return ssl.create_default_context().wrap_socket(raw, server_hostname=host)
                return raw
            except BaseException:
                raw.close()
                raise

        response = _exchange(
            request,
            connector=connect,
            connect_timeout=8,
            total_timeout=9,
            header_limit=8192,
            body_limit=16384,
        )
        if response.status != 200:
            return 1
        decoded = json.loads(response.body)
        if not isinstance(decoded, dict):
            return 1
        sys.stdout.buffer.write(response.body)
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(_child())
