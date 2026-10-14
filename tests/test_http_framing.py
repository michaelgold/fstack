from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from typing import Any

import pytest

from fstack.http_framing import _exchange, _HTTPError, _HTTPErrorCode, _HTTPResponse

_REQUEST = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"


def _socketpair_connector(
    response: bytes | Sequence[tuple[float, bytes]],
    *,
    eof_delay: float = 0.0,
) -> tuple[Callable[[float], socket.socket], threading.Thread]:
    client, server = socket.socketpair()

    def serve() -> None:
        try:
            server.recv(4096)
            fragments = [(0.0, response)] if isinstance(response, bytes) else response
            for delay, fragment in fragments:
                time.sleep(delay)
                with suppress(OSError):
                    server.sendall(fragment)
            time.sleep(eof_delay)
            with suppress(OSError):
                server.shutdown(socket.SHUT_WR)
        finally:
            server.close()

    thread = threading.Thread(target=serve)
    thread.start()
    return lambda _timeout: client, thread


def _read_response(
    response: bytes | Sequence[tuple[float, bytes]],
    *,
    header_limit: int = 1024,
    body_limit: int = 1024,
    total_timeout: float = 1.0,
    eof_delay: float = 0.0,
) -> _HTTPResponse:
    connector, thread = _socketpair_connector(response, eof_delay=eof_delay)
    try:
        return _exchange(
            _REQUEST,
            connector=connector,
            connect_timeout=0.5,
            total_timeout=total_timeout,
            header_limit=header_limit,
            body_limit=body_limit,
        )
    finally:
        thread.join(timeout=1)


def _assert_error(
    response: bytes | Sequence[tuple[float, bytes]],
    code: _HTTPErrorCode,
    **kwargs: int | float,
) -> _HTTPError:
    with pytest.raises(_HTTPError) as caught:
        _read_response(response, **kwargs)
    assert caught.value.code is code
    return caught.value


@pytest.mark.parametrize("version", ["HTTP/1.0", "HTTP/1.1"])
def test_reads_content_length_response(version: str) -> None:
    response = _read_response(
        f"{version} 200 OK\r\nContent-Length: 5\r\nX-Test: yes\r\n\r\nhello".encode()
    )

    assert response == _HTTPResponse(
        version=version,
        status=200,
        headers=(("content-length", "5"), ("x-test", "yes")),
        body=b"hello",
    )


def test_accepts_duplicate_equal_content_lengths() -> None:
    response = _read_response(
        b"HTTP/1.1 200 OK\r\nContent-Length: 05\r\nContent-Length: 5\r\n\r\nhello"
    )
    assert response.body == b"hello"


@pytest.mark.parametrize(
    "headers",
    [
        b"Content-Length: 5, 5",
        b"Content-Length: 05,5",
        b"Content-Length: 5, 05\r\nContent-Length: 5",
    ],
)
def test_accepts_equal_comma_combined_content_lengths(headers: bytes) -> None:
    response = _read_response(b"HTTP/1.1 200 OK\r\n" + headers + b"\r\n\r\nhello")
    assert response.body == b"hello"


_LONG_CONTENT_LENGTH_FIVE = b"0" * 5000 + b"5"


def test_accepts_content_length_with_arbitrarily_many_leading_zeroes() -> None:
    response = _read_response(
        b"HTTP/1.1 200 OK\r\nContent-Length: " + _LONG_CONTENT_LENGTH_FIVE + b"\r\n\r\nhello",
        header_limit=6000,
        body_limit=5,
    )
    assert response.body == b"hello"


def test_accepts_equal_combined_content_length_with_long_leading_zeroes() -> None:
    response = _read_response(
        b"HTTP/1.1 200 OK\r\nContent-Length: " + _LONG_CONTENT_LENGTH_FIVE + b", 0005\r\n\r\nhello",
        header_limit=6000,
        body_limit=5,
    )
    assert response.body == b"hello"


def test_accepts_equal_repeated_content_length_with_long_leading_zeroes() -> None:
    response = _read_response(
        b"HTTP/1.1 200 OK\r\nContent-Length: "
        + _LONG_CONTENT_LENGTH_FIVE
        + b"\r\nContent-Length: 5\r\n\r\nhello",
        header_limit=6000,
        body_limit=5,
    )
    assert response.body == b"hello"


def test_rejects_unequal_content_length_with_long_leading_zeroes() -> None:
    _assert_error(
        b"HTTP/1.1 200 OK\r\nContent-Length: " + _LONG_CONTENT_LENGTH_FIVE + b", 6\r\n\r\nhello",
        _HTTPErrorCode.FRAMING,
        header_limit=6000,
        body_limit=5,
    )


def test_rejects_oversized_long_content_length_without_unbounded_integer_conversion() -> None:
    _assert_error(
        b"HTTP/1.1 200 OK\r\nContent-Length: 1" + b"0" * 5000 + b"\r\n\r\n",
        _HTTPErrorCode.BODY_TOO_LARGE,
        header_limit=6000,
        body_limit=5,
    )


@pytest.mark.parametrize(
    "headers",
    [
        b"Content-Length: 4\r\nContent-Length: 5",
        b"Content-Length: 5\r\nTransfer-Encoding: chunked",
        b"Content-Length: -1",
        b"Content-Length: +1",
        b"Content-Length: 1x",
        b"Content-Length:",
        b"Content-Length: 4,5",
        b"Content-Length: 5,",
        b"Content-Length: ,5",
        b"Content-Length: 5,,5",
        b"Content-Length: +5,5",
        b"Content-Length: 5, 5x",
        b"Content-Length: 5,5\r\nContent-Length: 4",
    ],
)
def test_rejects_invalid_or_ambiguous_content_length(headers: bytes) -> None:
    _assert_error(b"HTTP/1.1 200 OK\r\n" + headers + b"\r\n\r\nhello", _HTTPErrorCode.FRAMING)


@pytest.mark.parametrize("value", [b"chunked", b" Chunked ", b"\tCHUNKED\t"])
def test_reads_chunked_response_with_ows_and_case_insensitivity(value: bytes) -> None:
    response = _read_response(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding:" + value + b"\r\n\r\n5\r\nhello\r\n0\r\n\r\n"
    )
    assert response.body == b"hello"


@pytest.mark.parametrize(
    "headers",
    [
        b"Transfer-Encoding: gzip",
        b"Transfer-Encoding: gzip, chunked",
        b"Transfer-Encoding: chunked, chunked",
        b"Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked",
        b"Transfer-Encoding:",
    ],
)
def test_rejects_unknown_multiple_or_malformed_transfer_codings(headers: bytes) -> None:
    _assert_error(b"HTTP/1.1 200 OK\r\n" + headers + b"\r\n\r\n0\r\n\r\n", _HTTPErrorCode.FRAMING)


def test_reads_close_delimited_response() -> None:
    response = _read_response(b"HTTP/1.0 200 OK\r\nX-Test: yes\r\n\r\nhello")
    assert response.body == b"hello"


@pytest.mark.parametrize(
    "status_line",
    [
        b"HTTP/2 200 OK",
        b"HTTP/1.2 200 OK",
        b"HTTP/1.1 20 OK",
        b"HTTP/1.1 099 Bad",
        b"HTTP/1.1 600 Bad",
        b"HTTP/1.1 200",
        b"HTTP/1.1 200 OK\x00",
        b"http/1.1 200 OK",
    ],
)
def test_rejects_malformed_status_lines(status_line: bytes) -> None:
    _assert_error(status_line + b"\r\nContent-Length: 0\r\n\r\n", _HTTPErrorCode.FRAMING)


@pytest.mark.parametrize(
    "header",
    [
        b"Missing-Colon",
        b" Folded: value",
        b"\tFolded: value",
        b"Bad Name: value",
        b"Bad\xffName: value",
        b"Empty-Value\x00: value",
        b"X-Test: bad\x00value",
        b": value",
    ],
)
def test_rejects_malformed_header_lines(header: bytes) -> None:
    _assert_error(
        b"HTTP/1.1 200 OK\r\n" + header + b"\r\nContent-Length: 0\r\n\r\n",
        _HTTPErrorCode.FRAMING,
    )


def test_accepts_valid_obs_text_in_header_value() -> None:
    response = _read_response(b"HTTP/1.1 200 OK\r\nX-Test: \xff\r\nContent-Length: 0\r\n\r\n")
    assert response.headers[0] == ("x-test", "\xff")


@pytest.mark.parametrize(
    ("response", "code", "limits"),
    [
        (
            b"HTTP/1.1 200 OK\r\nX-Long: 123456789\r\n\r\n",
            _HTTPErrorCode.HEADERS_TOO_LARGE,
            {"header_limit": 20},
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\n123456",
            _HTTPErrorCode.BODY_TOO_LARGE,
            {"body_limit": 5},
        ),
        (b"HTTP/1.1 200 OK\r\n\r\n123456", _HTTPErrorCode.BODY_TOO_LARGE, {"body_limit": 5}),
        (b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n1234", _HTTPErrorCode.PREMATURE_EOF, {}),
        (
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n1234",
            _HTTPErrorCode.PREMATURE_EOF,
            {},
        ),
    ],
)
def test_rejects_oversized_or_truncated_responses(
    response: bytes, code: _HTTPErrorCode, limits: dict[str, int]
) -> None:
    _assert_error(response, code, **limits)


@pytest.mark.parametrize(
    "chunks",
    [
        b"x\r\n",
        b"1x\r\na\r\n0\r\n\r\n",
        b"1\r\naX\r\n0\r\n\r\n",
        b"1;bad ext\r\na\r\n0\r\n\r\n",
        b"0\r\nBad-Trailer\r\n\r\n",
        b"0\r\n Content: folded\r\n\r\n",
        b"0\r\nContent-Length: 0\r\n\r\n",
    ],
)
def test_rejects_malformed_chunks_and_trailers(chunks: bytes) -> None:
    _assert_error(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" + chunks,
        _HTTPErrorCode.FRAMING,
    )


def test_rejects_oversized_trailers() -> None:
    _assert_error(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        + b"0\r\nX-Long: "
        + b"x" * 64
        + b"\r\n\r\n",
        _HTTPErrorCode.HEADERS_TOO_LARGE,
        header_limit=64,
    )


def test_accepts_chunk_size_with_arbitrarily_many_leading_zeroes() -> None:
    raw_size = b"0" * 128 + b"5"
    response = _read_response(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        + raw_size
        + b"\r\nhello\r\n0\r\n\r\n",
        header_limit=256,
        body_limit=5,
    )
    assert response.body == b"hello"


def test_rejects_huge_nonzero_chunk_size_without_unbounded_integer_conversion() -> None:
    raw_size = b"f" * 128
    _assert_error(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" + raw_size + b"\r\n",
        _HTTPErrorCode.BODY_TOO_LARGE,
        header_limit=256,
        body_limit=5,
    )


@pytest.mark.parametrize(
    "response",
    [
        b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\naextra",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\n\r\nextra",
    ],
)
def test_rejects_undeclared_trailing_bytes(response: bytes) -> None:
    _assert_error(response, _HTTPErrorCode.TRAILING_BYTES)


class _SlowSendSocket:
    def __init__(self, *, delay: float, blocked: bool = False) -> None:
        self.delay = delay
        self.blocked = blocked
        self.closed = False
        self.timeouts: list[float] = []
        self.send_calls = 0
        self._installed_timeout: float | None = None

    def settimeout(self, value: float | None) -> None:
        assert value is not None
        assert value > 0
        assert self._installed_timeout is None
        self.timeouts.append(value)
        self._installed_timeout = value

    def send(self, data: bytes) -> int:
        assert self._installed_timeout is not None
        timeout = self._installed_timeout
        self._installed_timeout = None
        self.send_calls += 1
        if self.blocked or self.delay >= timeout:
            time.sleep(timeout)
            raise TimeoutError
        time.sleep(self.delay)
        return min(1, len(data))

    def recv(self, size: int) -> bytes:
        raise AssertionError("receive must not be reached")

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("blocked", [False, True])
def test_absolute_deadline_covers_blocked_and_slow_partial_sends(blocked: bool) -> None:
    sock = _SlowSendSocket(delay=0.02, blocked=blocked)
    with pytest.raises(_HTTPError) as caught:
        _exchange(
            b"four",
            connector=lambda _timeout: sock,
            connect_timeout=0.5,
            total_timeout=0.05,
            header_limit=1024,
            body_limit=1024,
        )
    assert caught.value.code is _HTTPErrorCode.TIMEOUT
    assert sock.closed
    assert sock.timeouts
    assert len(sock.timeouts) == sock.send_calls
    if not blocked:
        assert sock.send_calls >= 2
    assert all(0 < timeout <= 0.05 for timeout in sock.timeouts)
    assert all(
        later < earlier for earlier, later in zip(sock.timeouts, sock.timeouts[1:], strict=False)
    )


def test_connect_uses_connect_cap_and_is_inside_absolute_deadline() -> None:
    observed: list[float] = []
    client, server = socket.socketpair()

    def connector(timeout: float) -> socket.socket:
        observed.append(timeout)
        time.sleep(0.04)
        return client

    started = time.monotonic()
    try:
        with pytest.raises(_HTTPError) as caught:
            _exchange(
                _REQUEST,
                connector=connector,
                connect_timeout=0.01,
                total_timeout=0.2,
                header_limit=1024,
                body_limit=1024,
            )
        elapsed = time.monotonic() - started
        assert caught.value.code is _HTTPErrorCode.TIMEOUT
        assert observed == [pytest.approx(0.01, abs=0.005)]
        assert elapsed < 0.1
        assert client.fileno() == -1
    finally:
        server.close()


@pytest.mark.parametrize(
    "fragments",
    [
        [(0.02, b"HTTP/1.1 200 "), (0.02, b"OK\r\nContent-Length: 0"), (0.02, b"\r\n\r\n")],
        [(0.0, b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n"), (0.03, b"a"), (0.03, b"bc")],
        [
            (0.0, b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"),
            (0.03, b"1\r\na\r\n"),
            (0.03, b"0\r\n\r\n"),
        ],
        [
            (0.0, b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n"),
            (0.03, b"X: a\r\n"),
            (0.03, b"\r\n"),
        ],
        [(0.0, b"HTTP/1.0 200 OK\r\n\r\n"), (0.03, b"a"), (0.03, b"b")],
    ],
)
def test_repeated_slow_drip_cannot_reset_absolute_deadline(
    fragments: Sequence[tuple[float, bytes]],
) -> None:
    _assert_error(fragments, _HTTPErrorCode.TIMEOUT, total_timeout=0.05)


@pytest.mark.parametrize("iteration", range(8))
def test_absolute_deadline_repeated_stress(iteration: int) -> None:
    del iteration
    _assert_error(
        [(0.018, b"HTTP/1.1 "), (0.018, b"200 OK\r\nContent-Length: 0\r\n"), (0.018, b"\r\n")],
        _HTTPErrorCode.TIMEOUT,
        total_timeout=0.045,
    )


def test_deadline_covers_eof_validation() -> None:
    with pytest.raises(_HTTPError) as caught:
        _read_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\na",
            total_timeout=0.03,
            eof_delay=0.06,
        )
    assert caught.value.code is _HTTPErrorCode.TIMEOUT


def test_errors_are_bounded_and_do_not_retain_provider_text() -> None:
    marker = b"provider-secret-body-marker"
    error = _assert_error(
        b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\na" + marker,
        _HTTPErrorCode.TRAILING_BYTES,
    )
    assert set(vars(error)) == {"code"}
    assert marker.decode() not in str(error)
    assert marker.decode() not in repr(error)


def test_connector_failures_are_sanitized() -> None:
    def fail(_timeout: float) -> socket.socket:
        raise OSError("provider-specific socket path")

    with pytest.raises(_HTTPError) as caught:
        _exchange(
            _REQUEST,
            connector=fail,
            connect_timeout=0.5,
            total_timeout=1.0,
            header_limit=1024,
            body_limit=1024,
        )
    assert caught.value.code is _HTTPErrorCode.TRANSPORT
    assert "provider-specific" not in str(caught.value)


class _InjectedIOErrorSocket:
    def __init__(self, stage: str, error: Exception) -> None:
        self.stage = stage
        self.error = error
        self.closed = False
        self._response = bytearray(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")

    def _raise(self, stage: str) -> None:
        if self.stage == stage:
            raise self.error

    def settimeout(self, value: float | None) -> None:
        del value
        self._raise("settimeout")

    def send(self, data: bytes) -> int:
        self._raise("send")
        return len(data)

    def recv(self, size: int) -> bytes:
        self._raise("recv")
        result = bytes(self._response[:size])
        del self._response[:size]
        return result

    def close(self) -> None:
        self.closed = True
        self._raise("close")


def _assert_sanitized_injected_error(
    caught: pytest.ExceptionInfo[_HTTPError], marker: str, caplog: pytest.LogCaptureFixture
) -> None:
    error = caught.value
    assert marker not in str(error)
    assert marker not in repr(error)
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in caplog.text


@pytest.mark.parametrize("stage", ["connector", "settimeout", "send", "recv", "close"])
@pytest.mark.parametrize(
    ("exception_type", "code"),
    [
        (TimeoutError, _HTTPErrorCode.TIMEOUT),
        (OSError, _HTTPErrorCode.TRANSPORT),
        (RuntimeError, _HTTPErrorCode.TRANSPORT),
    ],
)
def test_external_io_exceptions_are_sanitized(
    stage: str,
    exception_type: type[Exception],
    code: _HTTPErrorCode,
    caplog: pytest.LogCaptureFixture,
) -> None:
    marker = f"external-io-secret-{stage}-{exception_type.__name__}"
    injected = exception_type(marker)
    sock = _InjectedIOErrorSocket(stage, injected)

    def connector(_timeout: float) -> Any:
        if stage == "connector":
            raise injected
        return sock

    with pytest.raises(_HTTPError) as caught:
        _exchange(
            _REQUEST,
            connector=connector,
            connect_timeout=0.5,
            total_timeout=1.0,
            header_limit=1024,
            body_limit=1024,
        )

    assert caught.value.code is code
    _assert_sanitized_injected_error(caught, marker, caplog)


def test_close_failure_does_not_override_internal_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    marker = "external-io-secret-close-after-framing"
    sock = _InjectedIOErrorSocket("close", RuntimeError(marker))
    sock._response = bytearray(b"not-http\r\n\r\n")

    with pytest.raises(_HTTPError) as caught:
        _exchange(
            _REQUEST,
            connector=lambda _timeout: sock,
            connect_timeout=0.5,
            total_timeout=1.0,
            header_limit=1024,
            body_limit=1024,
        )

    assert caught.value.code is _HTTPErrorCode.FRAMING
    _assert_sanitized_injected_error(caught, marker, caplog)
