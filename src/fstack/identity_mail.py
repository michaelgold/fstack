"""Installation-owned Crusher HTTP mail transport (no agent mailbox).

Crusher's installed provider registry selects Kumo. This client uses its existing
send:execute service credential, not the delegated Fstack agent credential.
"""

from __future__ import annotations

import json
import os
import socket
import ssl
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from fstack.http_framing import _exchange
from fstack.identity import AuthConfiguration


@dataclass(frozen=True)
class CrusherMailer:
    endpoint: str
    credential: str = field(repr=False)
    source: str
    context: ssl.SSLContext = field(default_factory=ssl.create_default_context, repr=False)
    _address: tuple[Any, ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        url = urlsplit(self.endpoint)
        if (
            url.scheme != "https"
            or len(self.endpoint) > 2048
            or any(ord(c) <= 32 or ord(c) >= 127 for c in self.endpoint)
            or len(self.source) > 255
            or len(self.credential) > 4096
            or not url.hostname
            or url.username
            or url.password
            or url.path != "/v1/agent/emails/send"
            or url.query
            or url.fragment
            or not self.credential
            or any(ord(c) < 33 or ord(c) > 126 for c in self.credential)
            or not self.source
            or "\r" in self.source
            or "\n" in self.source
            or self.context.verify_mode != ssl.CERT_REQUIRED
            or not self.context.check_hostname
        ):
            raise ValueError("invalid mail configuration")
        # Resolve once during trusted startup, never on the bounded login path.
        addresses = socket.getaddrinfo(url.hostname, url.port or 443, type=socket.SOCK_STREAM)
        addresses.sort(key=lambda item: item[0] != socket.AF_INET)
        object.__setattr__(self, "_address", addresses[0])

    def _connect(self, timeout: float) -> ssl.SSLSocket:
        family, kind, protocol, _, address = self._address
        raw = socket.socket(family, kind, protocol)
        deadline = time.monotonic() + timeout
        try:
            raw.settimeout(timeout)
            raw.connect(address)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            raw.settimeout(remaining)
            return self.context.wrap_socket(raw, server_hostname=urlsplit(self.endpoint).hostname)
        except BaseException:
            raw.close()
            raise

    def __call__(self, email: str, link: str, key: str) -> None:
        url = urlsplit(self.endpoint)
        failed = False
        try:
            body = json.dumps(
                {
                    "source": self.source,
                    "to_addresses": [email],
                    "subject": "Confirm Fstack sign-in",
                    "body_text": link,
                    "idempotency_key": key,
                }
            ).encode()
            request = (
                f"POST {url.path} HTTP/1.1\r\nHost: {url.netloc}\r\n"
                f"Authorization: Bearer {self.credential}\r\n"
                "Content-Type: application/json\r\nConnection: close\r\n"
                f"Content-Length: {len(body)}\r\n\r\n"
            ).encode("ascii") + body
            response = _exchange(
                request,
                connector=self._connect,
                connect_timeout=3,
                total_timeout=5,
                header_limit=8192,
                body_limit=4096,
            )
            # No redirects, retries, provider body logging or response exception chaining.
            failed = response.status != 200
        except Exception:
            failed = True
        if failed:
            raise RuntimeError("authentication mail unavailable")


def load_auth_configuration() -> AuthConfiguration | None:
    mode = os.environ.get("FSTACK_AUTH_MODE", "disabled")
    if mode == "disabled":
        return None
    if mode != "magic":
        raise ValueError("invalid authentication mode")
    names = (
        "FSTACK_AUTH_ORIGIN",
        "FSTACK_AUTH_SECRET",
        "FSTACK_AUTH_MAIL_ENDPOINT",
        "FSTACK_AUTH_MAIL_TOKEN",
        "FSTACK_AUTH_MAIL_SOURCE",
    )
    if any(not os.environ.get(name) for name in names):
        raise ValueError("authentication configuration is incomplete")
    return AuthConfiguration(
        origin=os.environ[names[0]],
        secret=os.environ[names[1]],
        send_mail=CrusherMailer(os.environ[names[2]], os.environ[names[3]], os.environ[names[4]]),
    )
