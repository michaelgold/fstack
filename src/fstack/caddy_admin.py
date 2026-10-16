"""Validated configuration and confined transport for the local Caddy admin socket."""

from __future__ import annotations

import ctypes
import ipaddress
import json
import math
import os
import re
import socket
import stat
import struct
import sys
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from typing import Never, Protocol, cast
from uuid import UUID

import idna

from fstack.http_framing import (
    _HTTPErrorCode,
    _HTTPResponse,
    _perform_exchange,
)


class CaddyAdminErrorCode(StrEnum):
    CONFIG_INVALID = "caddy_admin_config_invalid"
    PRIMITIVES_UNAVAILABLE = "caddy_admin_primitives_unavailable"
    ROOT_UNSAFE = "caddy_admin_root_unsafe"
    SOCKET_UNSAFE = "caddy_admin_socket_unsafe"
    TIMEOUT = "caddy_admin_timeout"
    TRANSPORT = "caddy_admin_transport"
    RESPONSE_INVALID = "caddy_admin_response_invalid"
    ADMIN_FORBIDDEN = "caddy_admin_forbidden"
    ADMIN_INCOMPATIBLE = "caddy_admin_incompatible"
    PREREQUISITE_MISSING = "caddy_prerequisite_missing"
    ROUTE_ID_CONFLICT = "caddy_route_id_conflict"
    HOST_CONFLICT = "caddy_host_conflict"
    ROUTES_AMBIGUOUS = "caddy_routes_ambiguous"
    UNAVAILABLE = "caddy_unavailable"
    MUTATION_AMBIGUOUS = "caddy_mutation_ambiguous"
    CONFIG_CHANGED = "caddy_config_changed"
    ROUTE_REJECTED = "caddy_route_rejected"


class CaddyAdminError(RuntimeError):
    """A sanitized Caddy admin configuration or transport failure."""

    def __init__(self, code: CaddyAdminErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


_CANONICAL_DECIMAL = re.compile(r"0|[1-9][0-9]*")
_CONTAINER_NAME = re.compile(r"[a-z0-9](?:[a-z0-9_.-]{0,126}[a-z0-9])?")
_ROUTE_ID = re.compile(r"fstack-crusher-ingress-([0-9a-f]{32})")
_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_ETAG = re.compile(r'(?:W/)?"(?:[\x21\x23-\x7e\x80-\xff]+(?: [\x21\x23-\x7e\x80-\xff]+)*)?"')
_JSON_INVALID = object()
_PLATFORM_ID_MAX = (1 << (ctypes.sizeof(ctypes.c_uint) * 8)) - 2
_SOCKET_ENTRY = "caddy-admin.sock"
_SOCKET_ENTRY_BYTES = _SOCKET_ENTRY.encode()
_PROC_FD_ROOT = "/proc/self/fd"

_IN_ATTRIB = 0x00000004
_IN_MOVED_FROM = 0x00000040
_IN_MOVED_TO = 0x00000080
_IN_CREATE = 0x00000100
_IN_DELETE = 0x00000200
_IN_DELETE_SELF = 0x00000400
_IN_MOVE_SELF = 0x00000800
_IN_UNMOUNT = 0x00002000
_IN_Q_OVERFLOW = 0x00004000
_IN_IGNORED = 0x00008000
_INOTIFY_EVENT = struct.Struct("=iIII")
_INOTIFY_READ_BYTES = 65_536
_INOTIFY_MAX_READS = 16
_INOTIFY_ENTRY_MASK = _IN_ATTRIB | _IN_MOVED_FROM | _IN_MOVED_TO | _IN_CREATE | _IN_DELETE
_INOTIFY_FATAL_MASK = _IN_DELETE_SELF | _IN_MOVE_SELF | _IN_UNMOUNT | _IN_Q_OVERFLOW | _IN_IGNORED
_INOTIFY_WATCH_MASK = _INOTIFY_ENTRY_MASK | _INOTIFY_FATAL_MASK

ADMIN_CONNECT_TIMEOUT_SECONDS = 5.0
ADMIN_TOTAL_TIMEOUT_SECONDS = 15.0
ADMIN_HEADER_LIMIT_BYTES = 65_536
ADMIN_BODY_LIMIT_BYTES = 65_536

_DIRECT_ROUTES_REQUEST = (
    b"GET /config/apps/http/servers/fstack/routes HTTP/1.1\r\n"
    b"Host: localhost\r\nConnection: close\r\n\r\n"
)
_PARENT_SERVERS_REQUEST = (
    b"GET /config/apps/http/servers HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
)


class _UnixSocket(Protocol):
    def settimeout(self, value: float | None, /) -> None: ...

    def connect(self, address: str | bytes, /) -> None: ...

    def send(self, data: bytes, /) -> int: ...

    def recv(self, size: int, /) -> bytes: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class _InotifyPrimitives:
    init1: Callable[[int], int]
    add_watch: Callable[[int, bytes, int], int]
    rm_watch: Callable[[int, int], int]


@dataclass(frozen=True)
class _EntryMonitor:
    descriptor: int
    watch: int
    primitives: _InotifyPrimitives


def _parse_id(raw: str) -> int | None:
    if _CANONICAL_DECIMAL.fullmatch(raw) is None:
        return None
    maximum = str(_PLATFORM_ID_MAX)
    if len(raw) > len(maximum) or (len(raw) == len(maximum) and raw > maximum):
        return None
    return int(raw)


def _parse_port(raw: str) -> int | None:
    if _CANONICAL_DECIMAL.fullmatch(raw) is None:
        return None
    maximum = "65535"
    if len(raw) > len(maximum) or (len(raw) == len(maximum) and raw > maximum):
        return None
    port = int(raw)
    return port if port > 0 else None


def _parse_probe(raw: str) -> tuple[str, int] | None:
    if raw.startswith("["):
        end = raw.find("]")
        if end <= 1 or raw[end + 1 : end + 2] != ":":
            return None
        host = raw[1:end]
        raw_port = raw[end + 2 :]
        expected_version = 6
    else:
        if raw.count(":") != 1:
            return None
        host, raw_port = raw.split(":", 1)
        expected_version = 4
    port = _parse_port(raw_port)
    if port is None:
        return None
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if (
        address is None
        or address.version != expected_version
        or address.is_unspecified
        or "%" in host
        or str(address) != host
    ):
        return None
    return host, port


def _supplementary_groups() -> set[int] | None:
    try:
        groups = os.getgroups()
    except Exception:
        return None
    if any(type(group) is not int for group in groups):
        return None
    return set(groups)


_CONFIG_KEYS = (
    "FSTACK_CADDY_ADMIN_ROOT",
    "FSTACK_CADDY_ADMIN_UID",
    "FSTACK_CADDY_ADMIN_GID",
    "FSTACK_CADDY_PROBE_ADDRESS",
    "FSTACK_CADDY_CONTAINER_NAME",
)


def _snapshot_config(values: Mapping[str, object]) -> tuple[str, str, str, str, str] | None:
    try:
        _ = values.keys()
        snapshot: list[str] = []
        for key in _CONFIG_KEYS:
            via_get = values.get(key)
            value = values[key]
            if (
                type(via_get) is not str
                or via_get == ""
                or type(value) is not str
                or value == ""
                or value != via_get
            ):
                return None
            snapshot.append(value)
    except Exception:
        return None
    return cast(tuple[str, str, str, str, str], tuple(snapshot))


@dataclass(frozen=True)
class CaddyAdminConfig:
    admin_root: str
    admin_uid: int
    admin_gid: int
    probe_host: str
    probe_port: int
    container_name: str

    @property
    def socket_entry(self) -> str:
        return _SOCKET_ENTRY

    @classmethod
    def from_environ(cls, environ: Mapping[str, object] | None = None) -> CaddyAdminConfig:
        values = os.environ if environ is None else environ
        snapshot = _snapshot_config(values)
        if snapshot is None:
            raise CaddyAdminError(CaddyAdminErrorCode.CONFIG_INVALID)
        root, raw_uid, raw_gid, raw_probe, container_name = snapshot
        uid = _parse_id(raw_uid)
        gid = _parse_id(raw_gid)
        probe = _parse_probe(raw_probe)
        groups = _supplementary_groups()
        if (
            not os.path.isabs(root)
            or root.startswith("//")
            or os.path.normpath(root) != root
            or uid is None
            or gid is None
            or groups is None
            or gid not in groups
            or probe is None
            or _CONTAINER_NAME.fullmatch(container_name) is None
        ):
            raise CaddyAdminError(CaddyAdminErrorCode.CONFIG_INVALID)
        host, port = probe
        return cls(root, uid, gid, host, port, container_name)


def _descriptor_primitives() -> tuple[int, int, int] | None:
    opath = getattr(os, "O_PATH", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    directory = getattr(os, "O_DIRECTORY", 0)
    try:
        available = (
            sys.platform == "linux"
            and bool(opath)
            and bool(nofollow)
            and bool(directory)
            and os.open in os.supports_dir_fd
            and os.stat in os.supports_dir_fd
            and os.stat in os.supports_follow_symlinks
            and os.path.isdir(_PROC_FD_ROOT)
        )
    except Exception:
        available = False
    if not available:
        return None
    return opath, nofollow, directory


def _inotify_primitives() -> _InotifyPrimitives | None:
    nonblock = getattr(os, "O_NONBLOCK", 0)
    cloexec = getattr(os, "O_CLOEXEC", 0)
    event_constants = (
        _IN_ATTRIB,
        _IN_MOVED_FROM,
        _IN_MOVED_TO,
        _IN_CREATE,
        _IN_DELETE,
        _IN_DELETE_SELF,
        _IN_MOVE_SELF,
        _IN_UNMOUNT,
        _IN_Q_OVERFLOW,
        _IN_IGNORED,
    )
    masks_match = (
        _INOTIFY_ENTRY_MASK == _IN_ATTRIB | _IN_MOVED_FROM | _IN_MOVED_TO | _IN_CREATE | _IN_DELETE
        and _INOTIFY_FATAL_MASK
        == _IN_DELETE_SELF | _IN_MOVE_SELF | _IN_UNMOUNT | _IN_Q_OVERFLOW | _IN_IGNORED
        and _INOTIFY_WATCH_MASK == _INOTIFY_ENTRY_MASK | _INOTIFY_FATAL_MASK
    )
    if (
        sys.platform != "linux"
        or not nonblock
        or not cloexec
        or any(type(value) is not int or value <= 0 for value in event_constants)
        or not masks_match
    ):
        return None
    try:
        library = ctypes.CDLL(None, use_errno=True)
        init1 = library.inotify_init1
        add_watch = library.inotify_add_watch
        rm_watch = library.inotify_rm_watch
        init1.argtypes = [ctypes.c_int]
        init1.restype = ctypes.c_int
        add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        add_watch.restype = ctypes.c_int
        rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
        rm_watch.restype = ctypes.c_int
    except Exception:
        return None
    return _InotifyPrimitives(init1=init1, add_watch=add_watch, rm_watch=rm_watch)


def _start_entry_monitor(root_fd: int, primitives: _InotifyPrimitives) -> _EntryMonitor | None:
    descriptor = -1
    watch = -1
    try:
        descriptor = primitives.init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if descriptor < 0:
            return None
        alias = os.fsencode(f"{_PROC_FD_ROOT}/{root_fd}")
        watch = primitives.add_watch(descriptor, alias, _INOTIFY_WATCH_MASK)
        if watch < 0:
            return None
        return _EntryMonitor(descriptor=descriptor, watch=watch, primitives=primitives)
    except Exception:
        return None
    finally:
        if descriptor >= 0 and watch < 0:
            _close_fd(descriptor)


def _entry_monitor_is_safe(monitor: _EntryMonitor) -> bool:
    for _ in range(_INOTIFY_MAX_READS):
        try:
            events = os.read(monitor.descriptor, _INOTIFY_READ_BYTES)
        except BlockingIOError:
            return True
        except Exception:
            return False
        if not events:
            return False

        offset = 0
        while offset < len(events):
            if len(events) - offset < _INOTIFY_EVENT.size:
                return False
            watch, mask, _, name_length = _INOTIFY_EVENT.unpack_from(events, offset)
            offset += _INOTIFY_EVENT.size
            event_end = offset + name_length
            if event_end > len(events):
                return False
            raw_name = events[offset:event_end]
            offset = event_end
            if name_length:
                terminator = raw_name.find(b"\0")
                if terminator < 0 or any(raw_name[terminator + 1 :]):
                    return False
                name = raw_name[:terminator]
            else:
                name = b""
            if watch != monitor.watch and not (watch == -1 and mask & _IN_Q_OVERFLOW):
                return False
            if mask & _INOTIFY_FATAL_MASK:
                return False
            if mask & _INOTIFY_ENTRY_MASK and (not name or name == _SOCKET_ENTRY_BYTES):
                return False
    return False


def _close_entry_monitor(monitor: _EntryMonitor) -> bool:
    removed = False
    with suppress(Exception):
        removed = monitor.primitives.rm_watch(monitor.descriptor, monitor.watch) == 0
    closed = _close_fd(monitor.descriptor)
    return removed and closed


def _open_root(path: str, opath: int, nofollow: int, directory: int) -> int | None:
    try:
        return os.open(path, opath | nofollow | directory | os.O_CLOEXEC)
    except Exception:
        return None


def _proc_alias_matches(root_fd: int) -> bool:
    try:
        descriptor_info = os.fstat(root_fd)
        alias_info = os.stat(f"{_PROC_FD_ROOT}/{root_fd}")
    except Exception:
        return False
    return (
        stat.S_ISDIR(descriptor_info.st_mode)
        and stat.S_ISDIR(alias_info.st_mode)
        and descriptor_info.st_dev == alias_info.st_dev
        and descriptor_info.st_ino == alias_info.st_ino
    )


def _root_is_safe(descriptor: int) -> bool:
    try:
        info = os.fstat(descriptor)
    except OSError:
        return False
    return (
        stat.S_ISDIR(info.st_mode)
        and info.st_uid in {0, os.geteuid()}
        and stat.S_IMODE(info.st_mode) & 0o022 == 0
    )


def _stat_entry(root_fd: int) -> os.stat_result | None:
    try:
        return os.stat(_SOCKET_ENTRY, dir_fd=root_fd, follow_symlinks=False)
    except OSError:
        return None


def _socket_identity(
    info: os.stat_result,
) -> tuple[int, int, int, int, int, int]:
    return (
        info.st_dev,
        info.st_ino,
        stat.S_IFMT(info.st_mode),
        info.st_uid,
        info.st_gid,
        stat.S_IMODE(info.st_mode),
    )


def _socket_is_safe(info: os.stat_result | None, config: CaddyAdminConfig) -> bool:
    return bool(
        info is not None
        and stat.S_ISSOCK(info.st_mode)
        and info.st_uid == config.admin_uid
        and info.st_gid == config.admin_gid
        and stat.S_IMODE(info.st_mode) == 0o220
    )


def _close_fd(descriptor: int) -> bool:
    try:
        os.close(descriptor)
    except OSError:
        return False
    return True


class CaddyAdminTransport:
    """Revalidate and use the confined Caddy Unix admin socket for one exchange."""

    def __init__(
        self,
        config: CaddyAdminConfig,
        *,
        _socket_factory: Callable[[], _UnixSocket] | None = None,
    ) -> None:
        self._config = config
        self._socket_factory = _socket_factory or (
            lambda: cast(_UnixSocket, socket.socket(socket.AF_UNIX, socket.SOCK_STREAM))
        )

    def _exchange(self, request: bytes) -> _HTTPResponse:
        groups = _supplementary_groups()
        if groups is None or self._config.admin_gid not in groups:
            raise CaddyAdminError(CaddyAdminErrorCode.CONFIG_INVALID)
        primitives = _descriptor_primitives()
        inotify = _inotify_primitives()
        if primitives is None or inotify is None:
            raise CaddyAdminError(CaddyAdminErrorCode.PRIMITIVES_UNAVAILABLE)
        opath, nofollow, directory = primitives
        root_fd = _open_root(self._config.admin_root, opath, nofollow, directory)
        if root_fd is None:
            raise CaddyAdminError(CaddyAdminErrorCode.ROOT_UNSAFE)

        error: CaddyAdminErrorCode | None = None
        response: _HTTPResponse | None = None
        connector_error: CaddyAdminErrorCode | None = None
        monitor: _EntryMonitor | None = None
        try:
            if not _proc_alias_matches(root_fd):
                error = CaddyAdminErrorCode.PRIMITIVES_UNAVAILABLE
            elif not _root_is_safe(root_fd):
                error = CaddyAdminErrorCode.ROOT_UNSAFE
            if error is None:
                monitor = _start_entry_monitor(root_fd, inotify)
                if monitor is None:
                    error = CaddyAdminErrorCode.PRIMITIVES_UNAVAILABLE
            before = _stat_entry(root_fd) if error is None else None
            if error is None and not _socket_is_safe(before, self._config):
                error = CaddyAdminErrorCode.SOCKET_UNSAFE

            if error is None and before is not None:
                expected_identity = _socket_identity(before)

                def connector(timeout: float) -> _UnixSocket:
                    nonlocal connector_error
                    connected: _UnixSocket | None = None
                    try:
                        connected = self._socket_factory()
                        connected.settimeout(timeout)
                        connected.connect(f"{_PROC_FD_ROOT}/{root_fd}/{_SOCKET_ENTRY}")
                    except TimeoutError:
                        connector_error = CaddyAdminErrorCode.TIMEOUT
                    except Exception:
                        connector_error = CaddyAdminErrorCode.TRANSPORT
                    if connector_error is None:
                        after = _stat_entry(root_fd)
                        if (
                            not _socket_is_safe(after, self._config)
                            or after is None
                            or _socket_identity(after) != expected_identity
                        ):
                            connector_error = CaddyAdminErrorCode.SOCKET_UNSAFE
                    if monitor is None or not _entry_monitor_is_safe(monitor):
                        connector_error = CaddyAdminErrorCode.SOCKET_UNSAFE
                    if connector_error is not None or connected is None:
                        if connected is not None:
                            with suppress(Exception):
                                connected.close()
                        raise OSError
                    return connected

                outcome = _perform_exchange(
                    request,
                    connector=connector,
                    connect_timeout=ADMIN_CONNECT_TIMEOUT_SECONDS,
                    total_timeout=ADMIN_TOTAL_TIMEOUT_SECONDS,
                    header_limit=ADMIN_HEADER_LIMIT_BYTES,
                    body_limit=ADMIN_BODY_LIMIT_BYTES,
                )
                if connector_error is not None:
                    error = connector_error
                elif outcome.error is _HTTPErrorCode.TIMEOUT:
                    error = CaddyAdminErrorCode.TIMEOUT
                elif outcome.error in {
                    _HTTPErrorCode.FRAMING,
                    _HTTPErrorCode.HEADERS_TOO_LARGE,
                    _HTTPErrorCode.BODY_TOO_LARGE,
                    _HTTPErrorCode.PREMATURE_EOF,
                    _HTTPErrorCode.TRAILING_BYTES,
                }:
                    error = CaddyAdminErrorCode.RESPONSE_INVALID
                elif outcome.error is not None:
                    error = CaddyAdminErrorCode.TRANSPORT
                else:
                    response = outcome.response
        finally:
            if monitor is not None and not _close_entry_monitor(monitor) and error is None:
                error = CaddyAdminErrorCode.TRANSPORT
            if not _close_fd(root_fd) and error is None:
                error = CaddyAdminErrorCode.TRANSPORT

        if error is not None:
            raise CaddyAdminError(error)
        if response is None:
            raise CaddyAdminError(CaddyAdminErrorCode.TRANSPORT)
        return response


class CaddyRequestContext(StrEnum):
    PRE_MUTATION = "pre_mutation"
    POST_MUTATION = "post_mutation"


class CaddyInspectionState(StrEnum):
    ABSENT = "absent"
    ADOPTED = "adopted"


@dataclass(frozen=True)
class CaddyInspection:
    state: CaddyInspectionState
    etag: str | None = None


class _AdminExchange(Protocol):
    def _exchange(self, request: bytes) -> _HTTPResponse: ...


def _raise(code: CaddyAdminErrorCode) -> Never:
    raise CaddyAdminError(code)


def _raise_ingress_state(code: str) -> Never:
    from fstack.ingress_state import IngressStateError

    raise IngressStateError(code)


def _classify_status(kind: str, status: int) -> CaddyAdminErrorCode | None:
    post = kind in {"direct_post", "parent_post"}
    if status == 200:
        return None
    if kind == "id_pre" and status == 404:
        return None
    if kind in {"direct_pre", "direct_post"} and status in {400, 404}:
        return None
    if kind in {"parent_pre", "parent_post"} and status in {400, 404}:
        return CaddyAdminErrorCode.PREREQUISITE_MISSING
    if status in {401, 403}:
        return CaddyAdminErrorCode.ADMIN_FORBIDDEN
    if 300 <= status <= 499:
        return CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    return CaddyAdminErrorCode.MUTATION_AMBIGUOUS if post else CaddyAdminErrorCode.UNAVAILABLE


def _request_failure(context: CaddyRequestContext) -> CaddyAdminErrorCode:
    if context is CaddyRequestContext.POST_MUTATION:
        return CaddyAdminErrorCode.MUTATION_AMBIGUOUS
    return CaddyAdminErrorCode.UNAVAILABLE


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if type(key) is not str or key in result:
            raise ValueError
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    del value
    raise ValueError


def _parse_finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError
    return parsed


def _parse_bounded_int(value: str) -> int:
    digits = value[1:] if value.startswith("-") else value
    if len(digits) > 4300:
        raise ValueError
    return int(value)


def _json_tree_is_valid(value: object) -> bool:
    if type(value) is str:
        return not any(0xD800 <= ord(character) <= 0xDFFF for character in value)
    if type(value) is float:
        return math.isfinite(value)
    if type(value) is list:
        return all(_json_tree_is_valid(item) for item in value)
    if type(value) is dict:
        return all(
            _json_tree_is_valid(key) and _json_tree_is_valid(item) for key, item in value.items()
        )
    return type(value) in {int, bool, type(None)}


def _parse_json(body: bytes) -> object:
    try:
        if body.startswith((b"\xef\xbb\xbf", b"\xff\xfe", b"\xfe\xff", b"\x00\x00\xfe\xff")):
            return _JSON_INVALID
        text = body.decode("utf-8", errors="strict")
        parsed = cast(
            object,
            json.loads(
                text,
                object_pairs_hook=_strict_object,
                parse_constant=_reject_constant,
                parse_float=_parse_finite_float,
                parse_int=_parse_bounded_int,
            ),
        )
        return parsed if _json_tree_is_valid(parsed) else _JSON_INVALID
    except Exception:
        return _JSON_INVALID


def _canonical_host(value: object) -> str | None:
    if type(value) is not str or not value or len(value) > 253 or value.endswith("."):
        return None
    labels = value.split(".")
    if len(labels) < 2 or any(_HOST_LABEL.fullmatch(label) is None for label in labels):
        return None
    try:
        canonical = [
            idna.encode(
                idna.decode(label.encode("ascii"), uts46=False, std3_rules=True),
                uts46=False,
                std3_rules=True,
            ).decode("ascii")
            for label in labels
        ]
    except (UnicodeError, idna.IDNAError):
        return None
    if canonical != labels:
        return None
    return value


def _route_parts(route: object) -> tuple[str, str, str] | None:
    if (
        type(route) is not dict
        or any(type(key) is not str for key in route)
        or set(route) != {"@id", "match", "handle"}
    ):
        return None
    route_id = route.get("@id")
    match = route.get("match")
    handle = route.get("handle")
    if type(route_id) is not str or (identity := _ROUTE_ID.fullmatch(route_id)) is None:
        return None
    if (
        type(match) is not list
        or len(match) != 1
        or type(match[0]) is not dict
        or any(type(key) is not str for key in match[0])
        or set(match[0]) != {"host"}
    ):
        return None
    hosts = match[0].get("host")
    if type(hosts) is not list or len(hosts) != 1 or (host := _canonical_host(hosts[0])) is None:
        return None
    if (
        type(handle) is not list
        or len(handle) != 1
        or type(handle[0]) is not dict
        or any(type(key) is not str for key in handle[0])
        or set(handle[0]) != {"handler", "upstreams"}
    ):
        return None
    handler = handle[0].get("handler")
    if type(handler) is not str or handler != "reverse_proxy":
        return None
    upstreams = handle[0].get("upstreams")
    if (
        type(upstreams) is not list
        or len(upstreams) != 1
        or type(upstreams[0]) is not dict
        or any(type(key) is not str for key in upstreams[0])
        or set(upstreams[0]) != {"dial"}
    ):
        return None
    dial = upstreams[0].get("dial")
    deployment_hex = identity.group(1)
    expected_dial = f"fstack-crusher-{deployment_hex}:8000"
    if type(dial) is not str or dial != expected_dial:
        return None
    return route_id, host, dial


def _route_snapshot(route: object) -> dict[str, object] | None:
    try:
        parts = _route_parts(route)
    except Exception:
        return None
    if parts is None:
        return None
    route_id, host, dial = parts
    return {
        "@id": route_id,
        "match": [{"host": [host]}],
        "handle": [
            {
                "handler": "reverse_proxy",
                "upstreams": [{"dial": dial}],
            }
        ],
    }


def _host_matchers(route: object) -> list[list[str]]:
    if type(route) is not dict:
        return []
    match = route.get("match")
    if type(match) is not list:
        return []
    result: list[list[str]] = []
    for matcher_set in match:
        if type(matcher_set) is not dict or "host" not in matcher_set:
            continue
        hosts = matcher_set.get("host")
        if type(hosts) is list and hosts and all(type(host) is str for host in hosts):
            result.append(cast(list[str], hosts))
    return result


def _host_pattern_matches(pattern: str, host: str) -> bool:
    pattern_labels = pattern.split(".")
    host_labels = host.split(".")
    return len(pattern_labels) == len(host_labels) and all(
        pattern_label == "*" or pattern_label.casefold() == host_label.casefold()
        for pattern_label, host_label in zip(pattern_labels, host_labels, strict=True)
    )


def _classify_routes(
    routes: list[object], desired: dict[str, object]
) -> CaddyInspectionState | CaddyAdminErrorCode:
    desired_parts = _route_parts(desired)
    if desired_parts is None:
        return CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    desired_id, desired_host, _ = desired_parts

    id_routes = [
        route
        for route in routes
        if type(route) is dict and type(route.get("@id")) is str and route.get("@id") == desired_id
    ]
    if len(id_routes) > 1 or (len(id_routes) == 1 and id_routes[0] != desired):
        return CaddyAdminErrorCode.ROUTE_ID_CONFLICT

    for route in routes:
        if route == desired:
            continue
        if any(
            _host_pattern_matches(pattern, desired_host)
            for hosts in _host_matchers(route)
            for pattern in hosts
        ):
            return CaddyAdminErrorCode.HOST_CONFLICT

    for route in routes:
        if route == desired:
            continue
        if _route_parts(route) is None:
            return CaddyAdminErrorCode.ROUTES_AMBIGUOUS
    return CaddyInspectionState.ADOPTED if id_routes else CaddyInspectionState.ABSENT


def _etag(headers: tuple[tuple[str, str], ...]) -> str | None:
    if type(headers) is not tuple or any(
        type(header) is not tuple
        or len(header) != 2
        or type(header[0]) is not str
        or type(header[1]) is not str
        for header in headers
    ):
        return None
    values = [value for name, value in headers if name.lower() == "etag"]
    if len(values) != 1 or _ETAG.fullmatch(values[0]) is None:
        return None
    return values[0]


def _safe_exchange(
    transport: _AdminExchange,
    request: bytes,
    context: CaddyRequestContext,
) -> _HTTPResponse | CaddyAdminErrorCode:
    try:
        response = transport._exchange(request)
        if (
            type(response) is not _HTTPResponse
            or type(response.version) is not str
            or type(response.status) is not int
            or not 100 <= response.status <= 599
            or type(response.headers) is not tuple
            or type(response.body) is not bytes
        ):
            return _request_failure(context)
        return response
    except Exception:
        return _request_failure(context)


def _routes_classification_decision(
    routes: list[object],
    desired: dict[str, object],
    context: CaddyRequestContext,
    etag: str | None,
    *,
    parent: bool,
    id_exact: bool,
) -> CaddyInspection | CaddyAdminErrorCode:
    classification = _classify_routes(routes, desired)
    if isinstance(classification, CaddyAdminErrorCode):
        return classification
    if classification is CaddyInspectionState.ADOPTED:
        return CaddyInspection(CaddyInspectionState.ADOPTED)
    if id_exact:
        if context is CaddyRequestContext.POST_MUTATION:
            return CaddyAdminErrorCode.MUTATION_AMBIGUOUS
        return CaddyAdminErrorCode.ROUTE_ID_CONFLICT
    if context is CaddyRequestContext.POST_MUTATION:
        return CaddyAdminErrorCode.MUTATION_AMBIGUOUS
    if parent:
        return CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    if etag is None:
        return CaddyAdminErrorCode.PREREQUISITE_MISSING
    return CaddyInspection(CaddyInspectionState.ABSENT, etag)


def _parent_routes_decision(
    transport: _AdminExchange,
    desired: dict[str, object],
    context: CaddyRequestContext,
    *,
    id_exact: bool,
) -> CaddyInspection | CaddyAdminErrorCode:
    response = _safe_exchange(transport, _PARENT_SERVERS_REQUEST, context)
    if isinstance(response, CaddyAdminErrorCode):
        return response
    kind = "parent_post" if context is CaddyRequestContext.POST_MUTATION else "parent_pre"
    code = _classify_status(kind, response.status)
    if code is not None:
        return code
    parsed = _parse_json(response.body)
    if parsed is _JSON_INVALID:
        if context is CaddyRequestContext.POST_MUTATION:
            return CaddyAdminErrorCode.MUTATION_AMBIGUOUS
        return CaddyAdminErrorCode.ROUTES_AMBIGUOUS
    if type(parsed) is not dict:
        return CaddyAdminErrorCode.PREREQUISITE_MISSING
    server = cast(dict[str, object], parsed).get("fstack")
    if type(server) is not dict:
        return CaddyAdminErrorCode.PREREQUISITE_MISSING
    routes = cast(dict[str, object], server).get("routes")
    if type(routes) is not list:
        return CaddyAdminErrorCode.PREREQUISITE_MISSING
    return _routes_classification_decision(
        cast(list[object], routes), desired, context, None, parent=True, id_exact=id_exact
    )


def _routes_decision(
    transport: _AdminExchange,
    desired: dict[str, object],
    context: CaddyRequestContext,
    *,
    id_exact: bool,
) -> CaddyInspection | CaddyAdminErrorCode:
    response = _safe_exchange(transport, _DIRECT_ROUTES_REQUEST, context)
    if isinstance(response, CaddyAdminErrorCode):
        return response
    kind = "direct_post" if context is CaddyRequestContext.POST_MUTATION else "direct_pre"
    code = _classify_status(kind, response.status)
    if code is not None:
        return code
    if response.status in {400, 404}:
        return _parent_routes_decision(transport, desired, context, id_exact=id_exact)
    parsed = _parse_json(response.body)
    if type(parsed) is not list:
        if context is CaddyRequestContext.POST_MUTATION:
            return CaddyAdminErrorCode.MUTATION_AMBIGUOUS
        if parsed is _JSON_INVALID:
            return CaddyAdminErrorCode.ROUTES_AMBIGUOUS
        return CaddyAdminErrorCode.PREREQUISITE_MISSING
    etag = None
    if context is CaddyRequestContext.PRE_MUTATION:
        etag = _etag(response.headers)
        if etag is None:
            return CaddyAdminErrorCode.PREREQUISITE_MISSING
    return _routes_classification_decision(
        cast(list[object], parsed),
        desired,
        context,
        etag,
        parent=False,
        id_exact=id_exact,
    )


def _id_decision(
    transport: _AdminExchange,
    desired: dict[str, object],
) -> bool | CaddyAdminErrorCode:
    route_id = cast(str, desired["@id"])
    request = (
        f"GET /id/{route_id} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    ).encode("ascii")
    response = _safe_exchange(transport, request, CaddyRequestContext.PRE_MUTATION)
    if isinstance(response, CaddyAdminErrorCode):
        return response
    code = _classify_status("id_pre", response.status)
    if code is not None:
        return code
    if response.status == 404:
        return False
    parsed = _parse_json(response.body)
    if parsed != desired:
        return CaddyAdminErrorCode.ROUTE_ID_CONFLICT
    return True


def _inspection_decision(
    transport: _AdminExchange,
    desired: dict[str, object],
    context: CaddyRequestContext,
) -> CaddyInspection | CaddyAdminErrorCode:
    try:
        id_exact = False
        if context is CaddyRequestContext.PRE_MUTATION:
            id_decision = _id_decision(transport, desired)
            if isinstance(id_decision, CaddyAdminErrorCode):
                return id_decision
            id_exact = id_decision
        return _routes_decision(transport, desired, context, id_exact=id_exact)
    except Exception:
        return _request_failure(context)


def _canonical_route_bytes(desired: dict[str, object]) -> bytes | None:
    try:
        payload = json.dumps(
            desired,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    except Exception:
        return None
    return payload if b"\r" not in payload and b"\n" not in payload else None


def _post_request(payload: bytes, etag: str) -> bytes | None:
    try:
        raw_etag = etag.encode("latin-1")
        length = str(len(payload)).encode("ascii")
    except Exception:
        return None
    if b"\r" in raw_etag or b"\n" in raw_etag or _ETAG.fullmatch(etag) is None:
        return None
    return (
        b"POST /config/apps/http/servers/fstack/routes HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: "
        + length
        + b"\r\nIf-Match: "
        + raw_etag
        + b"\r\nConnection: close\r\n\r\n"
        + payload
    )


def _post_id_decision(
    transport: _AdminExchange,
    desired: dict[str, object],
) -> CaddyAdminErrorCode | None:
    route_id = cast(str, desired["@id"])
    request = (
        f"GET /id/{route_id} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    ).encode("ascii")
    response = _safe_exchange(transport, request, CaddyRequestContext.POST_MUTATION)
    if isinstance(response, CaddyAdminErrorCode):
        return CaddyAdminErrorCode.MUTATION_AMBIGUOUS
    status = response.status
    if status == 200:
        parsed = _parse_json(response.body)
        if parsed != desired:
            return CaddyAdminErrorCode.ROUTE_ID_CONFLICT
        confirmation = _routes_decision(
            transport,
            desired,
            CaddyRequestContext.POST_MUTATION,
            id_exact=True,
        )
        return confirmation if isinstance(confirmation, CaddyAdminErrorCode) else None
    if status in {401, 403}:
        return CaddyAdminErrorCode.ADMIN_FORBIDDEN
    if 300 <= status <= 499 and status != 404:
        return CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    return CaddyAdminErrorCode.MUTATION_AMBIGUOUS


def _mutation_decision(
    transport: _AdminExchange,
    desired: dict[str, object],
    etag: str,
    payload: bytes,
) -> CaddyAdminErrorCode | None:
    request = _post_request(payload, etag)
    if request is None:
        return CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    response = _safe_exchange(transport, request, CaddyRequestContext.POST_MUTATION)
    if isinstance(response, CaddyAdminErrorCode):
        return _post_id_decision(transport, desired)
    status = response.status
    if status == 200 or status in {400, 404}:
        confirmation = _routes_decision(
            transport,
            desired,
            CaddyRequestContext.POST_MUTATION,
            id_exact=False,
        )
        return confirmation if isinstance(confirmation, CaddyAdminErrorCode) else None
    if status == 412:
        return CaddyAdminErrorCode.CONFIG_CHANGED
    if status == 422:
        return CaddyAdminErrorCode.ROUTE_REJECTED
    if status in {401, 403}:
        return CaddyAdminErrorCode.ADMIN_FORBIDDEN
    if 300 <= status <= 499:
        return CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    return _post_id_decision(transport, desired)


class CaddyAdmin:
    """Inspect and reconcile one exact deterministic Fstack route."""

    def __init__(self, transport: _AdminExchange) -> None:
        self._transport = transport

    def inspect(
        self,
        route: Mapping[str, object],
        *,
        context: CaddyRequestContext = CaddyRequestContext.PRE_MUTATION,
    ) -> CaddyInspection:
        if type(context) is not CaddyRequestContext:
            code = CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
            del route, context, self
            _raise(code)
        desired = _route_snapshot(route)
        del route
        if desired is None:
            code = CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
            del desired, context, self
            _raise(code)
        decision = _inspection_decision(self._transport, desired, context)
        del desired
        if isinstance(decision, CaddyAdminErrorCode):
            code = decision
            del decision, context, self
            _raise(code)
        return decision

    def reconcile(self, route: Mapping[str, object]) -> None:
        """Adopt or append one exact route with optimistic concurrency."""
        desired = _route_snapshot(route)
        del route
        if desired is None:
            code = CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
            del desired, self
            _raise(code)
        payload = _canonical_route_bytes(desired)
        if payload is None:
            code = CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
            del payload, desired, self
            _raise(code)
        inspection = _inspection_decision(
            self._transport,
            desired,
            CaddyRequestContext.PRE_MUTATION,
        )
        if isinstance(inspection, CaddyAdminErrorCode):
            code = inspection
            del inspection, payload, desired, self
            _raise(code)
        if inspection.state is CaddyInspectionState.ADOPTED:
            return None
        etag = inspection.etag
        if etag is None:
            code = CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
            del inspection, etag, payload, desired, self
            _raise(code)
        decision = _mutation_decision(self._transport, desired, etag, payload)
        del inspection, etag, payload, desired
        if decision is not None:
            code = decision
            del decision, self
            _raise(code)
        return None

    def reconcile_submission(
        self,
        route: Mapping[str, object],
        ingress_submission_id: UUID,
        operation_id: UUID,
    ) -> None:
        """Reconcile Caddy first, then persist the exact submitted transition."""
        caddy_code: CaddyAdminErrorCode | None = None
        try:
            self.reconcile(route)
        except CaddyAdminError as error:
            caddy_code = error.code
        if caddy_code is not None:
            code = caddy_code
            del caddy_code, route, ingress_submission_id, operation_id, self
            _raise(code)

        from fstack import ingress_state

        state_code: str | None = None
        try:
            ingress_state.mark_submitted(ingress_submission_id, operation_id)
        except ingress_state.IngressStateError as error:
            state_code = error.code
        if state_code is not None:
            ingress_code = state_code
            del state_code, ingress_state, route, ingress_submission_id, operation_id, self
            _raise_ingress_state(ingress_code)
        return None
