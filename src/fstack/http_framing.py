"""Strict bounded HTTP/1.x transport and response framing."""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import NoReturn, Protocol


class _Socket(Protocol):
    def settimeout(self, value: float | None, /) -> None: ...

    def send(self, data: bytes, /) -> int: ...

    def recv(self, size: int, /) -> bytes: ...

    def close(self) -> None: ...


class _HTTPErrorCode(StrEnum):
    CONFIG = "http_config_invalid"
    TIMEOUT = "http_timeout"
    TRANSPORT = "http_transport"
    FRAMING = "http_framing_invalid"
    HEADERS_TOO_LARGE = "http_headers_too_large"
    BODY_TOO_LARGE = "http_body_too_large"
    PREMATURE_EOF = "http_premature_eof"
    TRAILING_BYTES = "http_trailing_bytes"


class _HTTPError(RuntimeError):
    """A sanitized bounded HTTP transport/framing failure."""

    def __init__(self, code: _HTTPErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


class _Failure(Exception):
    def __init__(self, code: _HTTPErrorCode) -> None:
        self.code = code


@dataclass(frozen=True)
class _HTTPResponse:
    version: str
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes


@dataclass(frozen=True)
class _Outcome:
    response: _HTTPResponse | None = None
    error: _HTTPErrorCode | None = None


_TCHARS = frozenset(
    b"!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)
_CHUNK_LINE = re.compile(
    rb"[0-9A-Fa-f]+(?:;[!#$%&'*+\-.^_`|~0-9A-Za-z]+"
    rb"(?:=(?:[!#$%&'*+\-.^_`|~0-9A-Za-z]+|\"(?:[\t !#-\[\]-~\x80-\xff]"
    rb"|\\[\t !-~\x80-\xff])*\"))?)*"
)


def _fail(code: _HTTPErrorCode) -> NoReturn:
    raise _Failure(code)


def _connect(connector: Callable[[float], _Socket], timeout: float) -> _Socket:
    try:
        return connector(timeout)
    except TimeoutError:
        _fail(_HTTPErrorCode.TIMEOUT)
    except Exception:
        _fail(_HTTPErrorCode.TRANSPORT)


def _settimeout(sock: _Socket, timeout: float) -> None:
    try:
        sock.settimeout(timeout)
    except TimeoutError:
        _fail(_HTTPErrorCode.TIMEOUT)
    except Exception:
        _fail(_HTTPErrorCode.TRANSPORT)


def _send(sock: _Socket, data: bytes) -> int:
    try:
        return sock.send(data)
    except TimeoutError:
        _fail(_HTTPErrorCode.TIMEOUT)
    except Exception:
        _fail(_HTTPErrorCode.TRANSPORT)


def _recv(sock: _Socket, size: int) -> bytes:
    try:
        return sock.recv(size)
    except TimeoutError:
        _fail(_HTTPErrorCode.TIMEOUT)
    except Exception:
        _fail(_HTTPErrorCode.TRANSPORT)


def _close_error(sock: _Socket) -> _HTTPErrorCode | None:
    try:
        sock.close()
    except TimeoutError:
        return _HTTPErrorCode.TIMEOUT
    except Exception:
        return _HTTPErrorCode.TRANSPORT
    return None


def _validate_field_line(line: bytes) -> tuple[str, str]:
    if not line or line[:1] in {b" ", b"\t"} or b":" not in line:
        _fail(_HTTPErrorCode.FRAMING)
    raw_name, raw_value = line.split(b":", 1)
    if not raw_name or any(byte not in _TCHARS for byte in raw_name):
        _fail(_HTTPErrorCode.FRAMING)
    if any(byte < 32 and byte != 9 or byte == 127 for byte in raw_value):
        _fail(_HTTPErrorCode.FRAMING)
    return raw_name.decode("ascii").lower(), raw_value.strip(b" \t").decode("latin-1")


def _parse_head(block: bytes) -> tuple[str, int, tuple[tuple[str, str], ...]]:
    lines = block.split(b"\r\n")
    status = lines[0]
    if len(status) < 13 or status[:9] not in {b"HTTP/1.0 ", b"HTTP/1.1 "}:
        _fail(_HTTPErrorCode.FRAMING)
    if len(status) < 13 or status[12:13] != b" " or not status[9:12].isdigit():
        _fail(_HTTPErrorCode.FRAMING)
    reason = status[13:]
    if any(byte < 32 and byte != 9 or byte == 127 for byte in reason):
        _fail(_HTTPErrorCode.FRAMING)
    version = status[:8].decode("ascii")
    status_code = int(status[9:12])
    if not 100 <= status_code <= 599:
        _fail(_HTTPErrorCode.FRAMING)
    headers = tuple(_validate_field_line(line) for line in lines[1:])
    return version, status_code, headers


class _Reader:
    def __init__(
        self,
        sock: _Socket,
        pending: bytearray,
        deadline: float,
    ) -> None:
        self._sock = sock
        self.pending = pending
        self._deadline = deadline

    def remaining(self) -> float:
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            _fail(_HTTPErrorCode.TIMEOUT)
        return remaining

    def receive(self, maximum: int, *, eof: _HTTPErrorCode) -> None:
        if maximum <= 0:
            _fail(_HTTPErrorCode.HEADERS_TOO_LARGE)
        _settimeout(self._sock, self.remaining())
        chunk = _recv(self._sock, min(65_536, maximum))
        self.remaining()
        if not chunk:
            _fail(eof)
        self.pending.extend(chunk)

    def exact(self, size: int) -> bytes:
        while len(self.pending) < size:
            self.receive(size - len(self.pending), eof=_HTTPErrorCode.PREMATURE_EOF)
        result = bytes(self.pending[:size])
        del self.pending[:size]
        return result

    def line(self, limit: int) -> tuple[bytes, int]:
        while True:
            end = self.pending.find(b"\r\n")
            if end >= 0:
                consumed = end + 2
                if consumed > limit:
                    _fail(_HTTPErrorCode.HEADERS_TOO_LARGE)
                result = bytes(self.pending[:end])
                del self.pending[:consumed]
                return result, consumed
            if len(self.pending) >= limit:
                _fail(_HTTPErrorCode.HEADERS_TOO_LARGE)
            self.receive(limit - len(self.pending), eof=_HTTPErrorCode.PREMATURE_EOF)

    def require_eof(self) -> None:
        if self.pending:
            _fail(_HTTPErrorCode.TRAILING_BYTES)
        _settimeout(self._sock, self.remaining())
        trailing = _recv(self._sock, 1)
        self.remaining()
        if trailing:
            _fail(_HTTPErrorCode.TRAILING_BYTES)


def _framing(headers: tuple[tuple[str, str], ...], body_limit: int) -> tuple[str, int]:
    content_lengths = [value for name, value in headers if name == "content-length"]
    transfer_encodings = [value for name, value in headers if name == "transfer-encoding"]
    if content_lengths and transfer_encodings:
        _fail(_HTTPErrorCode.FRAMING)
    if transfer_encodings:
        if len(transfer_encodings) != 1 or transfer_encodings[0].lower() != "chunked":
            _fail(_HTTPErrorCode.FRAMING)
        return "chunked", 0
    if content_lengths:
        members = [member.strip(" \t") for value in content_lengths for member in value.split(",")]
        if any(not member or not member.isascii() or not member.isdigit() for member in members):
            _fail(_HTTPErrorCode.FRAMING)
        normalized_lengths = {member.encode("ascii").lstrip(b"0") or b"0" for member in members}
        if len(normalized_lengths) != 1:
            _fail(_HTTPErrorCode.FRAMING)
        normalized_length = normalized_lengths.pop()
        maximum_length = str(body_limit).encode("ascii")
        if len(normalized_length) > len(maximum_length) or (
            len(normalized_length) == len(maximum_length) and normalized_length > maximum_length
        ):
            _fail(_HTTPErrorCode.BODY_TOO_LARGE)
        return "length", int(normalized_length)
    return "close", 0


def _read_chunked(reader: _Reader, *, header_limit: int, body_limit: int) -> bytes:
    body = bytearray()
    while True:
        size_line, _ = reader.line(header_limit)
        if _CHUNK_LINE.fullmatch(size_line) is None:
            _fail(_HTTPErrorCode.FRAMING)
        raw_size = size_line.split(b";", 1)[0]
        normalized_size = raw_size.lstrip(b"0") or b"0"
        remaining = body_limit - len(body)
        maximum_size = format(remaining, "x").encode("ascii")
        if len(normalized_size) > len(maximum_size) or (
            len(normalized_size) == len(maximum_size) and normalized_size.lower() > maximum_size
        ):
            _fail(_HTTPErrorCode.BODY_TOO_LARGE)
        size = int(normalized_size, 16)
        if size == 0:
            trailer_bytes = 0
            while True:
                trailer, consumed = reader.line(header_limit - trailer_bytes)
                trailer_bytes += consumed
                if not trailer:
                    return bytes(body)
                name, _ = _validate_field_line(trailer)
                if name in {"content-length", "transfer-encoding"}:
                    _fail(_HTTPErrorCode.FRAMING)
        body.extend(reader.exact(size))
        if reader.exact(2) != b"\r\n":
            _fail(_HTTPErrorCode.FRAMING)


def _read_close_delimited(reader: _Reader, *, body_limit: int) -> bytes:
    body = reader.pending
    reader.pending = bytearray()
    if len(body) > body_limit:
        _fail(_HTTPErrorCode.BODY_TOO_LARGE)
    while True:
        _settimeout(reader._sock, reader.remaining())
        chunk = _recv(reader._sock, min(65_536, body_limit + 1 - len(body)))
        reader.remaining()
        if not chunk:
            return bytes(body)
        body.extend(chunk)
        if len(body) > body_limit:
            _fail(_HTTPErrorCode.BODY_TOO_LARGE)


def _perform_exchange(
    request: bytes,
    *,
    connector: Callable[[float], _Socket],
    connect_timeout: float,
    total_timeout: float,
    header_limit: int,
    body_limit: int,
) -> _Outcome:
    sock: _Socket | None = None
    outcome: _Outcome | None = None
    try:
        if (
            not request
            or connect_timeout <= 0
            or total_timeout <= 0
            or header_limit < 4
            or body_limit < 0
        ):
            _fail(_HTTPErrorCode.CONFIG)
        deadline = time.monotonic() + total_timeout

        connect_deadline = min(deadline, time.monotonic() + connect_timeout)
        remaining = connect_deadline - time.monotonic()
        if remaining <= 0:
            _fail(_HTTPErrorCode.TIMEOUT)
        sock = _connect(connector, remaining)
        if connect_deadline - time.monotonic() <= 0:
            _fail(_HTTPErrorCode.TIMEOUT)

        sent = 0
        while sent < len(request):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _fail(_HTTPErrorCode.TIMEOUT)
            _settimeout(sock, remaining)
            count = _send(sock, request[sent:])
            if deadline - time.monotonic() <= 0:
                _fail(_HTTPErrorCode.TIMEOUT)
            if count <= 0 or count > len(request) - sent:
                _fail(_HTTPErrorCode.TRANSPORT)
            sent += count

        pending = bytearray()
        reader = _Reader(sock, pending, deadline)
        while True:
            end = pending.find(b"\r\n\r\n")
            if end >= 0:
                if end + 4 > header_limit:
                    _fail(_HTTPErrorCode.HEADERS_TOO_LARGE)
                break
            if len(pending) >= header_limit:
                _fail(_HTTPErrorCode.HEADERS_TOO_LARGE)
            reader.receive(header_limit - len(pending), eof=_HTTPErrorCode.PREMATURE_EOF)
        head = bytes(pending[:end])
        del pending[: end + 4]
        version, status, headers = _parse_head(head)
        mode, length = _framing(headers, body_limit)

        if mode == "length":
            body = reader.exact(length)
            reader.require_eof()
        elif mode == "chunked":
            body = _read_chunked(reader, header_limit=header_limit, body_limit=body_limit)
            reader.require_eof()
        else:
            body = _read_close_delimited(reader, body_limit=body_limit)
        outcome = _Outcome(response=_HTTPResponse(version, status, headers, body))
    except _Failure as failure:
        outcome = _Outcome(error=failure.code)
    except TimeoutError:
        outcome = _Outcome(error=_HTTPErrorCode.TIMEOUT)
    except OSError:
        outcome = _Outcome(error=_HTTPErrorCode.TRANSPORT)
    except (UnicodeError, ValueError, OverflowError):
        outcome = _Outcome(error=_HTTPErrorCode.FRAMING)
    finally:
        if sock is not None:
            close_error = _close_error(sock)
            if outcome is not None and outcome.error is None and close_error is not None:
                outcome = _Outcome(error=close_error)
    if outcome is None:  # pragma: no cover - an unexpected internal exception is propagating
        raise AssertionError("unreachable exchange outcome")
    return outcome


def _exchange(
    request: bytes,
    *,
    connector: Callable[[float], _Socket],
    connect_timeout: float,
    total_timeout: float,
    header_limit: int,
    body_limit: int,
) -> _HTTPResponse:
    """Send one request and read one complete bounded HTTP/1.x response."""
    outcome = _perform_exchange(
        request,
        connector=connector,
        connect_timeout=connect_timeout,
        total_timeout=total_timeout,
        header_limit=header_limit,
        body_limit=body_limit,
    )
    if outcome.error is not None:
        raise _HTTPError(outcome.error)
    if outcome.response is None:  # pragma: no cover - closed internal state space
        raise _HTTPError(_HTTPErrorCode.TRANSPORT)
    return outcome.response
