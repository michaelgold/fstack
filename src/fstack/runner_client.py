"""One bounded outbound availability report; no inbound listener or workload launch."""

from __future__ import annotations

import argparse
import json
import os
import stat
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from uuid import UUID

from fstack.runner_discovery import Report


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        raise HTTPError("", 403, "redirect refused", {}, None)  # type: ignore[arg-type]


def report_once(
    origin: str,
    runner_id: str,
    token: str,
    *,
    sequence: int,
    available_slots: int,
    queue_depth: int,
    allow_loopback_http: bool = False,
) -> dict[str, Any]:
    """Replay the same sequence/body after ambiguity; never refresh its original timestamp.

    HTTP is opt-in and restricted to literal loopback addresses for disposable tests.
    Production uses verified HTTPS, no redirects, no ambient HTTP proxy credentials.
    """
    parsed = urlsplit(origin)
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path
        or parsed.query
        or parsed.fragment
        or any(ord(c) <= 32 or ord(c) >= 127 for c in origin)
        or not (
            parsed.scheme == "https"
            or (
                allow_loopback_http
                and parsed.scheme == "http"
                and parsed.hostname in ("127.0.0.1", "::1")
            )
        )
        or not token.startswith("fs_r_")
        or len(token) > 128
        or any(not c.isalnum() and c not in "_-" for c in token)
    ):
        raise ValueError("invalid runner configuration")
    runner = UUID(runner_id)
    report = Report(sequence=sequence, available_slots=available_slots, queue_depth=queue_depth)
    request = Request(
        f"{origin}/v1/runners/{runner}/report",
        data=report.model_dump_json().encode(),
        method="POST",
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    result: dict[str, Any] | None = None
    try:
        with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=10) as response:
            raw = response.read(4097)
            if response.status != 200 or len(raw) > 4096:
                raise ValueError
            decoded = json.loads(raw)
            if (
                not isinstance(decoded, dict)
                or set(decoded) != {"runner_id", "sequence", "observed_at"}
                or decoded["runner_id"] != str(runner)
                or decoded["sequence"] != sequence
                or not isinstance(decoded["observed_at"], str)
            ):
                raise ValueError
            result = decoded
    except Exception:
        pass  # No URL, token, response body, or exception context is exposed.
    if result is None:
        raise RuntimeError("runner report failed; retry only the identical sequence and body")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--runner-id", required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--sequence", type=int, required=True)
    parser.add_argument("--available-slots", type=int, required=True)
    parser.add_argument("--queue-depth", type=int, required=True)
    args = parser.parse_args()
    try:
        fd = os.open(args.token_file, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd) as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
                raise ValueError
            token = stream.read(129).strip()
        receipt = report_once(
            args.origin,
            args.runner_id,
            token,
            sequence=args.sequence,
            available_slots=args.available_slots,
            queue_depth=args.queue_depth,
        )
        print(json.dumps(receipt))
        return 0
    except Exception:
        print("Runner report unavailable; check private configuration and enrollment.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
