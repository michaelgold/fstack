"""One bounded outbound availability report; no inbound listener or workload launch."""

from __future__ import annotations

import argparse
import json
import os
import stat
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from fstack.runner_discovery import Report
from fstack.runner_transport import exchange_json


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
    result: dict[str, Any] | None = None
    try:
        decoded = exchange_json(
            origin,
            f"/v1/runners/{runner}/report",
            token,
            report.model_dump(),
            allow_loopback_http=allow_loopback_http,
        )
        if (
            set(decoded) != {"runner_id", "sequence", "observed_at"}
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
