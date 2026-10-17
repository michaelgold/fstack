from __future__ import annotations

import json
import math
import os
import socket
import stat
import sys
import threading
import time
from contextlib import suppress
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

import fstack.caddy_admin as caddy_admin
from fstack.caddy_admin import (
    CaddyAdmin,
    CaddyAdminConfig,
    CaddyAdminError,
    CaddyAdminErrorCode,
    CaddyAdminTransport,
    CaddyInspection,
    CaddyInspectionState,
    CaddyRequestContext,
)

_SUPPLEMENTARY_GID = os.getgroups()[0]

_ENV = {
    "FSTACK_CADDY_ADMIN_ROOT": "/run/fstack-caddy",
    "FSTACK_CADDY_ADMIN_UID": str(os.geteuid()),
    "FSTACK_CADDY_ADMIN_GID": str(_SUPPLEMENTARY_GID),
    "FSTACK_CADDY_PROBE_ADDRESS": "127.0.0.1:8080",
    "FSTACK_CADDY_CONTAINER_NAME": "fstack-caddy",
}


def test_loads_canonical_installation_configuration() -> None:
    config = CaddyAdminConfig.from_environ(_ENV)

    assert config.admin_root == "/run/fstack-caddy"
    assert config.admin_uid == os.geteuid()
    assert config.admin_gid == _SUPPLEMENTARY_GID
    assert config.probe_host == "127.0.0.1"
    assert config.probe_port == 8080
    assert config.container_name == "fstack-caddy"
    assert config.socket_entry == "caddy-admin.sock"


def test_missing_installation_configuration_is_sanitized() -> None:
    for key in _ENV:
        candidate = dict(_ENV)
        del candidate[key]
        with pytest.raises(CaddyAdminError) as caught:
            CaddyAdminConfig.from_environ(candidate)
        assert caught.value.code is CaddyAdminErrorCode.CONFIG_INVALID
        assert set(vars(caught.value)) == {"code"}
        assert caught.value.__cause__ is None
        assert caught.value.__context__ is None
        assert key not in str(caught.value)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("FSTACK_CADDY_ADMIN_ROOT", "relative/root"),
        ("FSTACK_CADDY_ADMIN_ROOT", "/run/fstack-caddy/"),
        ("FSTACK_CADDY_ADMIN_ROOT", "/run/fstack-caddy/."),
        ("FSTACK_CADDY_ADMIN_ROOT", "/run/fstack-caddy/.."),
        ("FSTACK_CADDY_ADMIN_ROOT", "//"),
        ("FSTACK_CADDY_ADMIN_ROOT", "//run/fstack-caddy"),
        ("FSTACK_CADDY_ADMIN_ROOT", "/run//fstack-caddy"),
        ("FSTACK_CADDY_ADMIN_ROOT", "/run/./fstack-caddy"),
        ("FSTACK_CADDY_ADMIN_ROOT", "/run/other/../fstack-caddy"),
        ("FSTACK_CADDY_ADMIN_UID", "00"),
        ("FSTACK_CADDY_ADMIN_UID", "+1"),
        ("FSTACK_CADDY_ADMIN_UID", "-1"),
        ("FSTACK_CADDY_ADMIN_UID", "4294967295"),
        ("FSTACK_CADDY_ADMIN_UID", "9" * 5000),
        ("FSTACK_CADDY_ADMIN_GID", "01"),
        ("FSTACK_CADDY_ADMIN_GID", " 1"),
        ("FSTACK_CADDY_ADMIN_GID", "4294967295"),
        ("FSTACK_CADDY_ADMIN_GID", "9" * 5000),
        ("FSTACK_CADDY_PROBE_ADDRESS", "localhost:8080"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "http://127.0.0.1:8080"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "user@127.0.0.1:8080"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "127.0.0.1:080"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "127.0.0.1:0"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "127.0.0.1:65536"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "127.0.0.1:" + "9" * 5000),
        ("FSTACK_CADDY_PROBE_ADDRESS", "0.0.0.0:8080"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "127.000.000.001:8080"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "[::]:8080"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "[fe80::1%eth0]:8080"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "[2001:0db8::1]:8080"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "2001:db8::1:8080"),
        ("FSTACK_CADDY_PROBE_ADDRESS", "[127.0.0.1]:8080"),
        ("FSTACK_CADDY_CONTAINER_NAME", "."),
        ("FSTACK_CADDY_CONTAINER_NAME", ".."),
        ("FSTACK_CADDY_CONTAINER_NAME", "../caddy"),
        ("FSTACK_CADDY_CONTAINER_NAME", "Caddy"),
        ("FSTACK_CADDY_CONTAINER_NAME", "caddy/name"),
        ("FSTACK_CADDY_CONTAINER_NAME", "caddy\nname"),
    ],
)
def test_rejects_noncanonical_installation_configuration(key: str, value: str) -> None:
    candidate = dict(_ENV, **{key: value})

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminConfig.from_environ(candidate)

    assert caught.value.code is CaddyAdminErrorCode.CONFIG_INVALID
    assert value not in str(caught.value)


@pytest.mark.parametrize("root", ["/run/fstack-caddy", "/"])
def test_accepts_canonical_admin_root_boundaries(root: str) -> None:
    config = CaddyAdminConfig.from_environ(dict(_ENV, FSTACK_CADDY_ADMIN_ROOT=root))
    assert config.admin_root == root


@pytest.mark.parametrize(
    ("address", "host", "port"),
    [
        ("192.0.2.1:1", "192.0.2.1", 1),
        ("192.0.2.1:65535", "192.0.2.1", 65535),
        ("[2001:db8::1]:443", "2001:db8::1", 443),
    ],
)
def test_accepts_canonical_ip_literal_probe_boundaries(address: str, host: str, port: int) -> None:
    config = CaddyAdminConfig.from_environ(dict(_ENV, FSTACK_CADDY_PROBE_ADDRESS=address))
    assert (config.probe_host, config.probe_port) == (host, port)


def test_requires_configured_gid_in_worker_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(os, "getgid", lambda: 10)
    monkeypatch.setattr(os, "getegid", lambda: 11)
    monkeypatch.setattr(os, "getgroups", lambda: [12, 13])
    candidate = dict(_ENV, FSTACK_CADDY_ADMIN_GID="14")

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminConfig.from_environ(candidate)

    assert caught.value.code is CaddyAdminErrorCode.CONFIG_INVALID


def test_group_lookup_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    marker = "group-provider-secret-marker"

    def fail_groups() -> list[int]:
        raise OSError(marker)

    monkeypatch.setattr(os, "getgroups", fail_groups)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminConfig.from_environ(_ENV)
    assert caught.value.code is CaddyAdminErrorCode.CONFIG_INVALID
    assert marker not in str(caught.value)
    assert marker not in repr(caught.value)
    assert marker not in caplog.text
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.parametrize("gid", [10, 11])
def test_rejects_real_or_effective_gid_without_supplemental_membership(
    gid: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(os, "getgid", lambda: 10)
    monkeypatch.setattr(os, "getegid", lambda: 11)
    monkeypatch.setattr(os, "getgroups", lambda: [12, 13])
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminConfig.from_environ(dict(_ENV, FSTACK_CADDY_ADMIN_GID=str(gid)))
    assert caught.value.code is CaddyAdminErrorCode.CONFIG_INVALID


def test_accepts_configured_gid_only_from_supplemental_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(os, "getgid", lambda: 10)
    monkeypatch.setattr(os, "getegid", lambda: 11)
    monkeypatch.setattr(os, "getgroups", lambda: [12, 13])
    config = CaddyAdminConfig.from_environ(dict(_ENV, FSTACK_CADDY_ADMIN_GID="12"))
    assert config.admin_gid == 12


@pytest.mark.parametrize("value", [1, b"value", None, ""])
@pytest.mark.parametrize("key", list(_ENV))
def test_configuration_requires_nonempty_exact_builtin_strings(key: str, value: object) -> None:
    candidate: dict[str, object] = dict(_ENV)
    candidate[key] = value
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminConfig.from_environ(candidate)
    assert caught.value.code is CaddyAdminErrorCode.CONFIG_INVALID
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_configuration_does_not_invoke_hostile_string_subclass_methods() -> None:
    marker = "hostile-string-subclass-marker"

    class HostileString(str):
        def __str__(self) -> str:
            raise AssertionError(marker)

        def __bool__(self) -> bool:
            raise AssertionError(marker)

        def __len__(self) -> int:
            raise AssertionError(marker)

    for key in _ENV:
        candidate = dict(_ENV)
        candidate[key] = HostileString(_ENV[key])
        with pytest.raises(CaddyAdminError) as caught:
            CaddyAdminConfig.from_environ(candidate)
        error = caught.value
        assert error.code is CaddyAdminErrorCode.CONFIG_INVALID
        assert marker not in str(error)
        assert marker not in repr(error)
        assert error.__cause__ is None
        assert error.__context__ is None


@pytest.mark.parametrize("operation", ["getitem", "get", "keys"])
def test_configuration_mapping_failures_are_sanitized(
    operation: str, caplog: pytest.LogCaptureFixture
) -> None:
    marker = f"hostile-mapping-{operation}-marker"

    class HostileMapping(dict[str, str]):
        def __getitem__(self, key: str) -> str:
            if operation == "getitem":
                raise RuntimeError(marker)
            return super().__getitem__(key)

        def get(  # type: ignore[override]
            self, key: str, default: str | None = None
        ) -> str | None:
            if operation == "get":
                raise RuntimeError(marker)
            return super().get(key, default)

        def keys(self) -> object:  # type: ignore[override]
            if operation == "keys":
                raise RuntimeError(marker)
            return super().keys()

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminConfig.from_environ(HostileMapping(_ENV))
    error = caught.value
    assert error.code is CaddyAdminErrorCode.CONFIG_INVALID
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in caplog.text
    assert error.__cause__ is None
    assert error.__context__ is None


def test_socket_entry_is_code_owned_not_environment_controlled() -> None:
    config = CaddyAdminConfig.from_environ(dict(_ENV, FSTACK_CADDY_ADMIN_SOCKET="../injected.sock"))
    assert config.socket_entry == "caddy-admin.sock"
    assert not hasattr(config, "request")
    assert not hasattr(config, "temporal_payload")


def test_platform_uid_gid_upper_boundary_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    maximum = 2**32 - 2
    monkeypatch.setattr(os, "getgroups", lambda: [maximum])
    config = CaddyAdminConfig.from_environ(
        dict(
            _ENV,
            FSTACK_CADDY_ADMIN_UID=str(maximum),
            FSTACK_CADDY_ADMIN_GID=str(maximum),
        )
    )
    assert config.admin_uid == maximum
    assert config.admin_gid == maximum


pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux-only descriptor tests")

_REQUEST = b"GET /config/ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
_RESPONSE = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"


def _config(root: Path, *, uid: int | None = None, gid: int | None = None) -> CaddyAdminConfig:
    return CaddyAdminConfig.from_environ(
        dict(
            _ENV,
            FSTACK_CADDY_ADMIN_ROOT=str(root),
            FSTACK_CADDY_ADMIN_UID=str(os.geteuid() if uid is None else uid),
            FSTACK_CADDY_ADMIN_GID=str(_SUPPLEMENTARY_GID if gid is None else gid),
        )
    )


def _set_socket_permissions(path: Path, mode: int = 0o220) -> None:
    os.chown(path, -1, _SUPPLEMENTARY_GID)
    os.chmod(path, mode)


def _listen(root: Path, response: bytes = _RESPONSE) -> tuple[socket.socket, threading.Thread]:
    path = root / "caddy-admin.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        server.bind(f"/proc/self/fd/{root_fd}/caddy-admin.sock")
    finally:
        os.close(root_fd)
    _set_socket_permissions(path)
    server.listen()

    def serve() -> None:
        try:
            connection, _ = server.accept()
            with connection:
                connection.recv(65_536)
                with suppress(OSError):
                    connection.sendall(response)
        finally:
            server.close()

    thread = threading.Thread(target=serve)
    thread.start()
    return server, thread


def test_confined_exchange_uses_fixed_socket_and_bounded_http_framing(tmp_path: Path) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    _, thread = _listen(root)
    try:
        response = CaddyAdminTransport(_config(root))._exchange(_REQUEST)
    finally:
        thread.join(timeout=1)

    assert response.status == 200
    assert response.body == b"{}"
    assert not thread.is_alive()


def test_search_only_root_fails_closed_when_monitor_cannot_be_attached(tmp_path: Path) -> None:
    root = _socket_root(tmp_path)
    root.chmod(0o100)
    try:
        with pytest.raises(CaddyAdminError) as caught:
            CaddyAdminTransport(_config(root))._exchange(_REQUEST)
        assert caught.value.code is CaddyAdminErrorCode.PRIMITIVES_UNAVAILABLE
    finally:
        root.chmod(0o700)


def test_socket_mode_boundary_is_exhaustive() -> None:
    config = _config(Path("/run/fstack-caddy"))
    accepted: list[int] = []
    for mode in range(0o1000):
        info = os.stat_result(
            (stat.S_IFSOCK | mode, 0, 0, 1, config.admin_uid, config.admin_gid, 0, 0, 0, 0)
        )
        if caddy_admin._socket_is_safe(info, config):
            accepted.append(mode)
    assert accepted == [0o220]


def test_representative_wrong_socket_mode_is_rejected_by_real_filesystem(tmp_path: Path) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        server.bind(f"/proc/self/fd/{root_fd}/caddy-admin.sock")
    finally:
        os.close(root_fd)
    _set_socket_permissions(root / "caddy-admin.sock", 0o620)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root))._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.SOCKET_UNSAFE
    server.close()


def test_accepts_socket_mode_exactly_0220(tmp_path: Path) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    _, thread = _listen(root)
    try:
        assert CaddyAdminTransport(_config(root))._exchange(_REQUEST).status == 200
    finally:
        thread.join(timeout=1)


@pytest.mark.parametrize("mode", [0o720, 0o702, 0o777])
def test_rejects_group_or_other_writable_root(tmp_path: Path, mode: int) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=mode)
    root.chmod(mode)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root))._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.ROOT_UNSAFE


def test_rejects_root_symlink_leaf_and_nondirectory(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    symlink = tmp_path / "root-link"
    symlink.symlink_to(target, target_is_directory=True)
    regular = tmp_path / "regular"
    regular.write_text("not a directory")
    for root in (symlink, regular):
        with pytest.raises(CaddyAdminError) as caught:
            CaddyAdminTransport(_config(root))._exchange(_REQUEST)
        assert caught.value.code is CaddyAdminErrorCode.ROOT_UNSAFE


@pytest.mark.parametrize("suffix", ["/", "/."])
def test_rejects_noncanonical_symlink_root_before_socket_factory_or_connect(
    tmp_path: Path, suffix: str
) -> None:
    target = _socket_root(tmp_path / "target-parent")
    symlink = tmp_path / "root-link"
    symlink.symlink_to(target, target_is_directory=True)
    socket_factory_calls = 0
    connect_calls = 0

    class RecordingSocket(_ScriptedSocket):
        def connect(self, address: str | bytes) -> None:
            nonlocal connect_calls
            connect_calls += 1
            super().connect(address)

    def recording_socket_factory() -> RecordingSocket:
        nonlocal socket_factory_calls
        socket_factory_calls += 1
        return RecordingSocket(_RESPONSE)

    with pytest.raises(CaddyAdminError) as caught:
        config = CaddyAdminConfig.from_environ(
            dict(_ENV, FSTACK_CADDY_ADMIN_ROOT=f"{symlink}{suffix}")
        )
        CaddyAdminTransport(config, _socket_factory=recording_socket_factory)._exchange(_REQUEST)

    assert caught.value.code is CaddyAdminErrorCode.CONFIG_INVALID
    assert socket_factory_calls == 0
    assert connect_calls == 0


def test_rejects_root_not_owned_by_root_or_current_euid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    real_fstat = caddy_admin.os.fstat

    def wrong_owner(fd: int) -> SimpleNamespace:
        info = real_fstat(fd)
        return SimpleNamespace(
            st_mode=info.st_mode,
            st_uid=max(os.geteuid(), 0) + 10_000,
            st_gid=info.st_gid,
            st_dev=info.st_dev,
            st_ino=info.st_ino,
        )

    monkeypatch.setattr(caddy_admin.os, "fstat", wrong_owner)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root))._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.ROOT_UNSAFE


def test_rejects_symlink_socket_regular_file_and_missing_fixed_entry(tmp_path: Path) -> None:
    for kind in ("symlink", "regular", "other-name"):
        root = tmp_path / kind
        root.mkdir(mode=0o700)
        if kind == "symlink":
            target = root / "target"
            target.write_text("not a socket")
            (root / "caddy-admin.sock").symlink_to(target)
        elif kind == "regular":
            (root / "caddy-admin.sock").write_text("not a socket")
            _set_socket_permissions(root / "caddy-admin.sock")
        else:
            (root / "../injected.sock").write_text("ignored")
        with pytest.raises(CaddyAdminError) as caught:
            CaddyAdminTransport(_config(root))._exchange(_REQUEST)
        assert caught.value.code is CaddyAdminErrorCode.SOCKET_UNSAFE


@pytest.mark.parametrize(("field", "delta"), [("st_uid", 1), ("st_gid", 1)])
def test_rejects_wrong_socket_uid_or_gid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str, delta: int
) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        server.bind(f"/proc/self/fd/{root_fd}/caddy-admin.sock")
    finally:
        os.close(root_fd)
    _set_socket_permissions(root / "caddy-admin.sock")
    real_stat = caddy_admin._stat_entry

    def wrong_identity(root_fd: int) -> SimpleNamespace:
        info = real_stat(root_fd)
        assert info is not None
        values = {
            "st_mode": info.st_mode,
            "st_uid": info.st_uid,
            "st_gid": info.st_gid,
            "st_dev": info.st_dev,
            "st_ino": info.st_ino,
        }
        values[field] += delta
        return SimpleNamespace(**values)

    monkeypatch.setattr(caddy_admin, "_stat_entry", wrong_identity)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root))._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.SOCKET_UNSAFE
    server.close()


def test_root_and_socket_are_reopened_and_revalidated_for_every_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    opens = 0
    stats = 0
    real_open = caddy_admin._open_root
    real_stat = caddy_admin._stat_entry

    def counted_open(path: str, opath: int, nofollow: int, directory: int) -> int | None:
        nonlocal opens
        opens += 1
        return real_open(path, opath, nofollow, directory)

    def counted_stat(root_fd: int) -> os.stat_result | None:
        nonlocal stats
        stats += 1
        return real_stat(root_fd)

    monkeypatch.setattr(caddy_admin, "_open_root", counted_open)
    monkeypatch.setattr(caddy_admin, "_stat_entry", counted_stat)
    transport = CaddyAdminTransport(_config(root))
    for _ in range(2):
        _, thread = _listen(root)
        try:
            assert transport._exchange(_REQUEST).status == 200
        finally:
            thread.join(timeout=1)
            (root / "caddy-admin.sock").unlink()
    assert opens == 2
    assert stats == 4


def test_supplemental_group_membership_is_rechecked_before_every_exchange(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    transport = CaddyAdminTransport(_config(root))
    lookups = 0

    def changing_groups() -> list[int]:
        nonlocal lookups
        lookups += 1
        return [_SUPPLEMENTARY_GID] if lookups == 1 else []

    monkeypatch.setattr(caddy_admin.os, "getgroups", changing_groups)
    _, thread = _listen(root)
    try:
        assert transport._exchange(_REQUEST).status == 200
    finally:
        thread.join(timeout=1)

    with pytest.raises(CaddyAdminError) as caught:
        transport._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.CONFIG_INVALID
    assert lookups == 2


def test_dropped_or_failed_group_membership_stops_before_root_or_socket(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    marker = "exchange-group-secret-marker"
    root = _socket_root(tmp_path)
    transport = CaddyAdminTransport(
        _config(root),
        _socket_factory=lambda: pytest.fail("socket factory must not run"),
    )
    root_opens = 0
    real_open_root = caddy_admin._open_root

    def counted_open_root(path: str, opath: int, nofollow: int, directory: int) -> int | None:
        nonlocal root_opens
        root_opens += 1
        return real_open_root(path, opath, nofollow, directory)

    def fail_groups() -> list[int]:
        raise RuntimeError(marker)

    monkeypatch.setattr(caddy_admin, "_open_root", counted_open_root)
    monkeypatch.setattr(caddy_admin.os, "getgroups", fail_groups)
    with pytest.raises(CaddyAdminError) as caught:
        transport._exchange(_REQUEST)
    error = caught.value
    assert error.code is CaddyAdminErrorCode.CONFIG_INVALID
    assert root_opens == 0
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in caplog.text
    assert error.__cause__ is None
    assert error.__context__ is None


def test_socket_replacement_between_connect_and_revalidation_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    old_server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    bind_path = f"/proc/self/fd/{root_fd}/caddy-admin.sock"
    old_server.bind(bind_path)
    os.close(root_fd)
    _set_socket_permissions(root / "caddy-admin.sock")
    old_server.listen()
    replacement: socket.socket | None = None

    class ReplacingSocket:
        def __init__(self) -> None:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        def settimeout(self, value: float | None) -> None:
            self.sock.settimeout(value)

        def connect(self, address: str | bytes) -> None:
            nonlocal replacement
            (root / "caddy-admin.sock").unlink()
            replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            replacement.bind(address)
            _set_socket_permissions(root / "caddy-admin.sock")
            replacement.listen()
            self.sock.connect(address)

        def send(self, data: bytes) -> int:
            return self.sock.send(data)

        def recv(self, size: int) -> bytes:
            return self.sock.recv(size)

        def close(self) -> None:
            self.sock.close()

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root), _socket_factory=ReplacingSocket)._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.SOCKET_UNSAFE
    old_server.close()
    assert replacement is not None
    replacement.close()


def _run_socket_aba_exchange(root: Path) -> tuple[object | None, CaddyAdminError | None, bytes]:
    fixed = root / "caddy-admin.sock"
    saved = root / "original.sock"
    original = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        original.bind(f"/proc/self/fd/{root_fd}/caddy-admin.sock")
    finally:
        os.close(root_fd)
    _set_socket_permissions(fixed)
    original.listen()
    replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    received = bytearray()

    def receive_from_replacement() -> None:
        connection, _ = replacement.accept()
        with connection:
            connection.settimeout(1)
            with suppress(TimeoutError):
                received.extend(connection.recv(65_536))
            with suppress(OSError):
                connection.sendall(_RESPONSE)

    receiver = threading.Thread(target=receive_from_replacement)

    class AbaSocket:
        def __init__(self) -> None:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        def settimeout(self, value: float | None) -> None:
            self.sock.settimeout(value)

        def connect(self, address: str | bytes) -> None:
            fixed.rename(saved)
            replacement.bind(address)
            _set_socket_permissions(fixed)
            replacement.listen()
            receiver.start()
            self.sock.connect(address)
            fixed.unlink()
            saved.rename(fixed)

        def send(self, data: bytes) -> int:
            return self.sock.send(data)

        def recv(self, size: int) -> bytes:
            return self.sock.recv(size)

        def close(self) -> None:
            self.sock.close()

    response: object | None = None
    error: CaddyAdminError | None = None
    try:
        response = CaddyAdminTransport(_config(root), _socket_factory=AbaSocket)._exchange(_REQUEST)
    except CaddyAdminError as caught:
        error = caught
    finally:
        original.close()
        replacement.close()
        if receiver.ident is not None:
            receiver.join(timeout=2)

    assert not receiver.is_alive()
    return response, error, bytes(received)


def test_socket_aba_replacement_is_rejected_before_request_bytes_are_sent(tmp_path: Path) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)

    response, error, received = _run_socket_aba_exchange(root)

    assert response is None
    assert error is not None
    assert error.code is CaddyAdminErrorCode.SOCKET_UNSAFE
    assert received == b""


def test_socket_aba_regression_depends_on_mutation_monitor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    monkeypatch.setattr(caddy_admin, "_entry_monitor_is_safe", lambda monitor: True)

    response, error, received = _run_socket_aba_exchange(root)

    assert error is None
    assert response is not None
    assert received == _REQUEST


def _inotify_event(watch: int, mask: int, name: bytes = b"") -> bytes:
    raw_name = name + b"\0" if name else b""
    return caddy_admin._INOTIFY_EVENT.pack(watch, mask, 0, len(raw_name)) + raw_name


@pytest.mark.parametrize(
    "mask",
    [
        caddy_admin._IN_ATTRIB,
        caddy_admin._IN_MOVED_FROM,
        caddy_admin._IN_MOVED_TO,
        caddy_admin._IN_CREATE,
        caddy_admin._IN_DELETE,
    ],
)
def test_fixed_socket_mutation_events_fail_before_send(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mask: int
) -> None:
    root = _socket_root(tmp_path)
    sock = _ScriptedSocket(_RESPONSE)
    reads: list[bytes | Exception] = [
        _inotify_event(1, mask, b"caddy-admin.sock"),
        BlockingIOError(),
    ]

    def scripted_read(descriptor: int, size: int) -> bytes:
        del descriptor, size
        result = reads.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(caddy_admin.os, "read", scripted_read)
    monkeypatch.setattr(
        caddy_admin,
        "_start_entry_monitor",
        lambda root_fd, primitives: caddy_admin._EntryMonitor(root_fd + 1000, 1, primitives),
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root), _socket_factory=lambda: sock)._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.SOCKET_UNSAFE
    assert sock.sent == b""


@pytest.mark.parametrize(
    "event",
    [
        _inotify_event(-1, caddy_admin._IN_Q_OVERFLOW),
        _inotify_event(1, caddy_admin._IN_DELETE_SELF),
        _inotify_event(1, caddy_admin._IN_MOVE_SELF),
        _inotify_event(1, caddy_admin._IN_UNMOUNT),
        _inotify_event(1, caddy_admin._IN_IGNORED),
        _inotify_event(2, caddy_admin._IN_CREATE, b"unrelated"),
    ],
)
def test_overflow_fatal_and_watch_loss_events_are_rejected(event: bytes) -> None:
    reads: list[bytes | Exception] = [event, BlockingIOError()]

    def scripted_read(descriptor: int, size: int) -> bytes:
        del descriptor, size
        result = reads.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    primitives = caddy_admin._InotifyPrimitives(
        lambda flags: 3, lambda fd, path, mask: 1, lambda fd, wd: 0
    )
    monitor = caddy_admin._EntryMonitor(3, 1, primitives)
    original_read = caddy_admin.os.read
    try:
        caddy_admin.os.read = scripted_read
        assert not caddy_admin._entry_monitor_is_safe(monitor)
    finally:
        caddy_admin.os.read = original_read


@pytest.mark.parametrize(
    "event",
    [
        b"truncated",
        caddy_admin._INOTIFY_EVENT.pack(1, caddy_admin._IN_CREATE, 0, 8) + b"short",
        caddy_admin._INOTIFY_EVENT.pack(1, caddy_admin._IN_CREATE, 0, 4) + b"name",
        caddy_admin._INOTIFY_EVENT.pack(1, caddy_admin._IN_CREATE, 0, 4) + b"a\0x\0",
    ],
)
def test_malformed_inotify_stream_is_rejected(event: bytes) -> None:
    primitives = caddy_admin._InotifyPrimitives(
        lambda flags: 3, lambda fd, path, mask: 1, lambda fd, wd: 0
    )
    monitor = caddy_admin._EntryMonitor(3, 1, primitives)
    original_read = caddy_admin.os.read
    try:
        caddy_admin.os.read = lambda descriptor, size: event
        assert not caddy_admin._entry_monitor_is_safe(monitor)
    finally:
        caddy_admin.os.read = original_read


def test_nameless_entry_mutation_event_is_rejected() -> None:
    reads: list[bytes | Exception] = [
        _inotify_event(1, caddy_admin._IN_ATTRIB),
        BlockingIOError(),
    ]

    def scripted_read(descriptor: int, size: int) -> bytes:
        del descriptor, size
        result = reads.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    primitives = caddy_admin._InotifyPrimitives(
        lambda flags: 3, lambda fd, path, mask: 1, lambda fd, wd: 0
    )
    monitor = caddy_admin._EntryMonitor(3, 1, primitives)
    original_read = caddy_admin.os.read
    try:
        caddy_admin.os.read = scripted_read
        assert not caddy_admin._entry_monitor_is_safe(monitor)
    finally:
        caddy_admin.os.read = original_read


def test_unrelated_entry_mutation_is_ignored_when_stream_is_well_formed() -> None:
    reads: list[bytes | Exception] = [
        _inotify_event(1, caddy_admin._IN_CREATE, b"unrelated.sock"),
        BlockingIOError(),
    ]

    def scripted_read(descriptor: int, size: int) -> bytes:
        del descriptor, size
        result = reads.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    primitives = caddy_admin._InotifyPrimitives(
        lambda flags: 3, lambda fd, path, mask: 1, lambda fd, wd: 0
    )
    monitor = caddy_admin._EntryMonitor(3, 1, primitives)
    original_read = caddy_admin.os.read
    try:
        caddy_admin.os.read = scripted_read
        assert caddy_admin._entry_monitor_is_safe(monitor)
    finally:
        caddy_admin.os.read = original_read


def test_monitor_drain_is_bounded_under_continuous_unrelated_mutations() -> None:
    event = _inotify_event(1, caddy_admin._IN_CREATE, b"unrelated.sock")
    calls = 0

    def continuous_read(descriptor: int, size: int) -> bytes:
        nonlocal calls
        del descriptor, size
        calls += 1
        if calls > 17:
            raise RuntimeError("unbounded monitor drain")
        return event

    primitives = caddy_admin._InotifyPrimitives(
        lambda flags: 3, lambda fd, path, mask: 1, lambda fd, wd: 0
    )
    monitor = caddy_admin._EntryMonitor(3, 1, primitives)
    original_read = caddy_admin.os.read
    try:
        caddy_admin.os.read = continuous_read
        assert not caddy_admin._entry_monitor_is_safe(monitor)
    finally:
        caddy_admin.os.read = original_read
    assert calls <= 17


def test_monitor_read_error_is_sanitized_and_stops_before_send(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    marker = "inotify-read-provider-marker"
    root = _socket_root(tmp_path)
    sock = _ScriptedSocket(_RESPONSE)

    def fail_read(descriptor: int, size: int) -> bytes:
        del descriptor, size
        raise RuntimeError(marker)

    monkeypatch.setattr(caddy_admin.os, "read", fail_read)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root), _socket_factory=lambda: sock)._exchange(_REQUEST)
    error = caught.value
    assert error.code is CaddyAdminErrorCode.SOCKET_UNSAFE
    assert sock.sent == b""
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in caplog.text
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    "primitive",
    [
        "opath",
        "nofollow",
        "directory",
        "open_dir_fd",
        "stat_dir_fd",
        "stat_nofollow",
        "proc_fd",
        "inotify",
        "nonblock",
        "cloexec",
        "watch_mask",
        "event_constant",
    ],
)
def test_missing_required_linux_primitive_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, primitive: str
) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    if primitive == "opath":
        monkeypatch.setattr(caddy_admin.os, "O_PATH", 0)
    elif primitive == "nofollow":
        monkeypatch.setattr(caddy_admin.os, "O_NOFOLLOW", 0)
    elif primitive == "directory":
        monkeypatch.setattr(caddy_admin.os, "O_DIRECTORY", 0)
    elif primitive == "open_dir_fd":
        monkeypatch.setattr(
            caddy_admin.os,
            "supports_dir_fd",
            caddy_admin.os.supports_dir_fd - {caddy_admin.os.open},
        )
    elif primitive == "stat_dir_fd":
        monkeypatch.setattr(
            caddy_admin.os,
            "supports_dir_fd",
            caddy_admin.os.supports_dir_fd - {caddy_admin.os.stat},
        )
    elif primitive == "stat_nofollow":
        monkeypatch.setattr(
            caddy_admin.os,
            "supports_follow_symlinks",
            caddy_admin.os.supports_follow_symlinks - {caddy_admin.os.stat},
        )
    elif primitive == "proc_fd":
        monkeypatch.setattr(caddy_admin, "_PROC_FD_ROOT", str(root / "missing-proc"))
    elif primitive == "inotify":
        monkeypatch.setattr(caddy_admin, "_inotify_primitives", lambda: None)
    elif primitive == "nonblock":
        monkeypatch.setattr(caddy_admin.os, "O_NONBLOCK", 0)
    elif primitive == "cloexec":
        monkeypatch.setattr(caddy_admin.os, "O_CLOEXEC", 0)
    elif primitive == "watch_mask":
        monkeypatch.setattr(caddy_admin, "_INOTIFY_WATCH_MASK", 0)
    else:
        monkeypatch.setattr(caddy_admin, "_IN_ATTRIB", 0)

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root))._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.PRIMITIVES_UNAVAILABLE


def test_inotify_watch_uses_exact_flags_alias_and_complete_mask(tmp_path: Path) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    root_fd = os.open(root, os.O_PATH | os.O_DIRECTORY | os.O_CLOEXEC)
    observed: dict[str, object] = {}

    def init1(flags: int) -> int:
        observed["flags"] = flags
        return os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC)

    def add_watch(descriptor: int, path: bytes, mask: int) -> int:
        observed["path"] = path
        observed["mask"] = mask
        return 17

    def rm_watch(descriptor: int, watch: int) -> int:
        observed["removed"] = (descriptor, watch)
        return 0

    primitives = caddy_admin._InotifyPrimitives(init1, add_watch, rm_watch)
    monitor = caddy_admin._start_entry_monitor(root_fd, primitives)
    try:
        assert monitor is not None
        assert observed["flags"] == os.O_NONBLOCK | os.O_CLOEXEC
        assert observed["path"] == os.fsencode(f"/proc/self/fd/{root_fd}")
        assert observed["mask"] == (
            caddy_admin._IN_ATTRIB
            | caddy_admin._IN_MOVED_FROM
            | caddy_admin._IN_MOVED_TO
            | caddy_admin._IN_CREATE
            | caddy_admin._IN_DELETE
            | caddy_admin._IN_DELETE_SELF
            | caddy_admin._IN_MOVE_SELF
            | caddy_admin._IN_UNMOUNT
            | caddy_admin._IN_Q_OVERFLOW
            | caddy_admin._IN_IGNORED
        )
        assert caddy_admin._close_entry_monitor(monitor)
        assert observed["removed"] == (monitor.descriptor, 17)
    finally:
        os.close(root_fd)


@pytest.mark.parametrize("failure", ["init", "add"])
def test_inotify_setup_failures_are_sanitized_and_close_partial_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure: str,
) -> None:
    marker = f"inotify-{failure}-provider-marker"
    root = _socket_root(tmp_path)
    baseline = len(os.listdir("/proc/self/fd"))

    def init1(flags: int) -> int:
        del flags
        if failure == "init":
            raise RuntimeError(marker)
        return os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC)

    def add_watch(descriptor: int, path: bytes, mask: int) -> int:
        del descriptor, path, mask
        raise RuntimeError(marker)

    primitives = caddy_admin._InotifyPrimitives(init1, add_watch, lambda fd, wd: 0)
    monkeypatch.setattr(caddy_admin, "_inotify_primitives", lambda: primitives)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(
            _config(root),
            _socket_factory=lambda: pytest.fail("socket factory must not run"),
        )._exchange(_REQUEST)
    error = caught.value
    assert error.code is CaddyAdminErrorCode.PRIMITIVES_UNAVAILABLE
    assert len(os.listdir("/proc/self/fd")) == baseline
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in caplog.text
    assert error.__cause__ is None
    assert error.__context__ is None


def test_monitor_is_not_attached_before_proc_alias_identity_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _socket_root(tmp_path)
    monkeypatch.setattr(caddy_admin, "_proc_alias_matches", lambda root_fd: False)
    monkeypatch.setattr(
        caddy_admin,
        "_start_entry_monitor",
        lambda root_fd, primitives: pytest.fail("monitor must not be attached"),
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root))._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.PRIMITIVES_UNAVAILABLE


def test_monitor_watch_is_removed_on_success_and_connect_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _socket_root(tmp_path)
    real = caddy_admin._inotify_primitives()
    assert real is not None
    removals: list[tuple[int, int]] = []

    def rm_watch(descriptor: int, watch: int) -> int:
        removals.append((descriptor, watch))
        return real.rm_watch(descriptor, watch)

    primitives = caddy_admin._InotifyPrimitives(real.init1, real.add_watch, rm_watch)
    monkeypatch.setattr(caddy_admin, "_inotify_primitives", lambda: primitives)
    success = _ScriptedSocket(_RESPONSE)
    assert CaddyAdminTransport(_config(root), _socket_factory=lambda: success)._exchange(_REQUEST)
    failure = _ScriptedSocket(_RESPONSE, connect_error=OSError("provider marker"))
    with pytest.raises(CaddyAdminError):
        CaddyAdminTransport(_config(root), _socket_factory=lambda: failure)._exchange(_REQUEST)
    assert len(removals) == 2


def test_root_is_opened_with_exact_search_only_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    observed_flags: list[int] = []
    real_open = caddy_admin.os.open

    def recording_open(path: str, flags: int, *args: int, **kwargs: int) -> int:
        observed_flags.append(flags)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(caddy_admin.os, "open", recording_open)
    descriptor = caddy_admin._open_root(
        str(root),
        caddy_admin.os.O_PATH,
        caddy_admin.os.O_NOFOLLOW,
        caddy_admin.os.O_DIRECTORY,
    )
    assert descriptor is not None
    os.close(descriptor)
    assert observed_flags == [
        caddy_admin.os.O_PATH
        | caddy_admin.os.O_DIRECTORY
        | caddy_admin.os.O_NOFOLLOW
        | caddy_admin.os.O_CLOEXEC
    ]


def test_fake_proc_fd_directory_fails_before_socket_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _socket_root(tmp_path / "real")
    fake_proc = tmp_path / "fake-proc-fd"
    fake_proc.mkdir()
    socket_factory_calls = 0

    def forbidden_socket() -> _ScriptedSocket:
        nonlocal socket_factory_calls
        socket_factory_calls += 1
        return _ScriptedSocket(_RESPONSE)

    monkeypatch.setattr(caddy_admin, "_PROC_FD_ROOT", str(fake_proc))
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root), _socket_factory=forbidden_socket)._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.PRIMITIVES_UNAVAILABLE
    assert socket_factory_calls == 0


def test_proc_fd_alias_stat_failure_is_sanitized_before_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    marker = "proc-alias-provider-marker"
    root = _socket_root(tmp_path)
    socket_factory_calls = 0
    real_stat = caddy_admin.os.stat

    def marker_stat(
        path: os.PathLike[str] | str, *args: object, **kwargs: object
    ) -> os.stat_result:
        if isinstance(path, str) and path.startswith(f"{caddy_admin._PROC_FD_ROOT}/"):
            raise RuntimeError(marker)
        return real_stat(path, *args, **kwargs)

    def forbidden_socket() -> _ScriptedSocket:
        nonlocal socket_factory_calls
        socket_factory_calls += 1
        return _ScriptedSocket(_RESPONSE)

    monkeypatch.setattr(caddy_admin.os, "stat", marker_stat)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root), _socket_factory=forbidden_socket)._exchange(_REQUEST)
    error = caught.value
    assert error.code is CaddyAdminErrorCode.PRIMITIVES_UNAVAILABLE
    assert socket_factory_calls == 0
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in caplog.text
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize("mismatch", ["device", "inode", "type"])
def test_proc_fd_alias_must_match_descriptor_identity_and_directory_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mismatch: str
) -> None:
    root = _socket_root(tmp_path)
    socket_factory_calls = 0
    real_stat = caddy_admin.os.stat

    def mismatched_alias(
        path: os.PathLike[str] | str, *args: object, **kwargs: object
    ) -> os.stat_result | SimpleNamespace:
        info = real_stat(path, *args, **kwargs)
        if not (isinstance(path, str) and path.startswith(f"{caddy_admin._PROC_FD_ROOT}/")):
            return info
        values = {
            "st_mode": info.st_mode,
            "st_dev": info.st_dev,
            "st_ino": info.st_ino,
        }
        if mismatch == "device":
            values["st_dev"] += 1
        elif mismatch == "inode":
            values["st_ino"] += 1
        else:
            values["st_mode"] = stat.S_IFREG | stat.S_IMODE(info.st_mode)
        return SimpleNamespace(**values)

    def forbidden_socket() -> _ScriptedSocket:
        nonlocal socket_factory_calls
        socket_factory_calls += 1
        return _ScriptedSocket(_RESPONSE)

    monkeypatch.setattr(caddy_admin.os, "stat", mismatched_alias)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root), _socket_factory=forbidden_socket)._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.PRIMITIVES_UNAVAILABLE
    assert socket_factory_calls == 0


class _ScriptedSocket:
    def __init__(
        self,
        response: bytes | list[tuple[float, bytes]],
        *,
        connect_delay: float = 0.0,
        send_delay: float = 0.0,
        partial_send: bool = False,
        blocked_send: bool = False,
        connect_error: Exception | None = None,
    ) -> None:
        self.fragments = [(0.0, response)] if isinstance(response, bytes) else list(response)
        self.pending = bytearray()
        self.connect_delay = connect_delay
        self.send_delay = send_delay
        self.partial_send = partial_send
        self.blocked_send = blocked_send
        self.connect_error = connect_error
        self.timeout: float | None = None
        self.closed = False
        self.sent: bytearray = bytearray()
        self.timeouts: list[float] = []

    def settimeout(self, value: float | None) -> None:
        self.timeout = value
        if value is not None:
            self.timeouts.append(value)

    def connect(self, address: str | bytes) -> None:
        del address
        if self.connect_error is not None:
            raise self.connect_error
        if self.connect_delay:
            time.sleep(self.connect_delay)

    def send(self, data: bytes) -> int:
        assert self.timeout is not None
        if self.blocked_send or self.send_delay >= self.timeout:
            time.sleep(self.timeout)
            raise TimeoutError
        time.sleep(self.send_delay)
        count = min(1, len(data)) if self.partial_send else len(data)
        self.sent.extend(data[:count])
        return count

    def recv(self, size: int) -> bytes:
        assert self.timeout is not None
        if self.pending:
            result = bytes(self.pending[:size])
            del self.pending[:size]
            return result
        if not self.fragments:
            return b""
        delay, fragment = self.fragments.pop(0)
        if delay >= self.timeout:
            time.sleep(self.timeout)
            raise TimeoutError
        time.sleep(delay)
        self.pending.extend(fragment)
        return self.recv(size)

    def close(self) -> None:
        self.closed = True


def _socket_root(tmp_path: Path) -> Path:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700, parents=True)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        server.bind(f"/proc/self/fd/{root_fd}/caddy-admin.sock")
    finally:
        os.close(root_fd)
    _set_socket_permissions(root / "caddy-admin.sock")
    server.close()
    return root


def test_admin_transport_uses_exact_bounded_exchange_constants() -> None:
    assert caddy_admin.ADMIN_CONNECT_TIMEOUT_SECONDS == 5.0
    assert caddy_admin.ADMIN_TOTAL_TIMEOUT_SECONDS == 15.0
    assert caddy_admin.ADMIN_HEADER_LIMIT_BYTES == 65_536
    assert caddy_admin.ADMIN_BODY_LIMIT_BYTES == 65_536


def test_slow_connect_consumes_connect_cap_inside_absolute_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _socket_root(tmp_path)
    sock = _ScriptedSocket(_RESPONSE, connect_delay=0.04)
    monkeypatch.setattr(caddy_admin, "ADMIN_CONNECT_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(caddy_admin, "ADMIN_TOTAL_TIMEOUT_SECONDS", 0.2)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root), _socket_factory=lambda: sock)._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.TIMEOUT
    assert sock.closed
    assert sock.timeouts[0] == pytest.approx(0.01, abs=0.005)


@pytest.mark.parametrize("blocked", [False, True])
def test_blocked_and_slow_partial_sends_share_one_admin_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocked: bool
) -> None:
    root = _socket_root(tmp_path)
    sock = _ScriptedSocket(
        _RESPONSE,
        send_delay=0.02,
        partial_send=True,
        blocked_send=blocked,
    )
    monkeypatch.setattr(caddy_admin, "ADMIN_CONNECT_TIMEOUT_SECONDS", 0.5)
    monkeypatch.setattr(caddy_admin, "ADMIN_TOTAL_TIMEOUT_SECONDS", 0.05)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root), _socket_factory=lambda: sock)._exchange(b"four")
    assert caught.value.code is CaddyAdminErrorCode.TIMEOUT
    assert sock.closed
    assert all(0 < timeout <= 0.05 for timeout in sock.timeouts[1:])


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
    ],
)
def test_slow_headers_body_and_chunks_share_one_admin_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fragments: list[tuple[float, bytes]],
) -> None:
    root = _socket_root(tmp_path)
    sock = _ScriptedSocket(fragments)
    monkeypatch.setattr(caddy_admin, "ADMIN_CONNECT_TIMEOUT_SECONDS", 0.5)
    monkeypatch.setattr(caddy_admin, "ADMIN_TOTAL_TIMEOUT_SECONDS", 0.05)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root), _socket_factory=lambda: sock)._exchange(_REQUEST)
    assert caught.value.code is CaddyAdminErrorCode.TIMEOUT
    assert sock.closed


def test_descriptors_and_socket_close_on_success_and_failure(tmp_path: Path) -> None:
    root = _socket_root(tmp_path)
    baseline = len(os.listdir("/proc/self/fd"))
    success = _ScriptedSocket(_RESPONSE)
    assert (
        CaddyAdminTransport(_config(root), _socket_factory=lambda: success)
        ._exchange(_REQUEST)
        .status
        == 200
    )
    assert success.closed
    assert len(os.listdir("/proc/self/fd")) == baseline

    failure = _ScriptedSocket(_RESPONSE, connect_error=OSError("provider path marker"))
    with pytest.raises(CaddyAdminError):
        CaddyAdminTransport(_config(root), _socket_factory=lambda: failure)._exchange(_REQUEST)
    assert failure.closed
    assert len(os.listdir("/proc/self/fd")) == baseline


def test_errors_discard_paths_provider_text_response_and_logs(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    marker = "secret-provider-path-marker"
    root = _socket_root(tmp_path / marker)
    sock = _ScriptedSocket(_RESPONSE, connect_error=RuntimeError(marker))
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdminTransport(_config(root), _socket_factory=lambda: sock)._exchange(_REQUEST)
    error = caught.value
    assert error.code is CaddyAdminErrorCode.TRANSPORT
    assert set(vars(error)) == {"code"}
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in caplog.text
    assert error.__cause__ is None
    assert error.__context__ is None


_DEPLOYMENT_HEX = "0123456789abcdef0123456789abcdef"
_OTHER_HEX = "fedcba9876543210fedcba9876543210"
_ROUTE_ID = f"fstack-crusher-ingress-{_DEPLOYMENT_HEX}"
_DESIRED_HOST = "api.example.com"
_DESIRED_ROUTE = {
    "@id": _ROUTE_ID,
    "match": [{"host": [_DESIRED_HOST]}],
    "handle": [
        {
            "handler": "reverse_proxy",
            "upstreams": [{"dial": f"fstack-crusher-{_DEPLOYMENT_HEX}:8000"}],
        }
    ],
}
_FALLBACK_ROUTE = {
    "@id": "fstack-unclaimed-host",
    "handle": [{"handler": "static_response", "status_code": 404, "body": "not found\n"}],
    "terminal": True,
}


def _with_fallback(body: object) -> object:
    if type(body) is list:
        return body if _FALLBACK_ROUTE in body else [*body, _FALLBACK_ROUTE]
    if type(body) is dict and type(body.get("fstack")) is dict:
        server = body["fstack"]
        if type(server.get("routes")) is list:
            return {**body, "fstack": {**server, "routes": _with_fallback(server["routes"])}}
    return body


def _response(status: int, body: object = None, *headers: tuple[str, str]) -> object:
    body = _with_fallback(body)
    payload = b"" if body is None else json.dumps(body, separators=(",", ":")).encode()
    return caddy_admin._HTTPResponse("HTTP/1.1", status, headers, payload)


class _RecordingTransport:
    def __init__(self, responses: list[object]) -> None:
        self.responses = list(responses)
        self.requests: list[bytes] = []

    def _exchange(self, request: bytes) -> object:
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _inspect(
    responses: list[object], context: CaddyRequestContext = CaddyRequestContext.PRE_MUTATION
) -> tuple[object, _RecordingTransport]:
    transport = _RecordingTransport(responses)
    result = CaddyAdmin(transport).inspect(_DESIRED_ROUTE, context=context)
    return result, transport


def test_pre_mutation_rejects_missing_terminal_fallback() -> None:
    missing_fallback = caddy_admin._HTTPResponse(
        "HTTP/1.1", 200, (("Etag", '"route-array-tag"'),), b"[]"
    )
    with pytest.raises(CaddyAdminError) as caught:
        _inspect([_response(404), missing_fallback])

    assert caught.value.code is CaddyAdminErrorCode.ROUTES_AMBIGUOUS


def test_pre_mutation_allows_one_exact_terminal_fallback_after_managed_routes() -> None:
    result, transport = _inspect(
        [_response(404), _response(200, [_FALLBACK_ROUTE], ("Etag", '"route-array-tag"'))]
    )

    assert isinstance(result, CaddyInspection)
    assert result.state is CaddyInspectionState.ABSENT
    assert result.etag == '"route-array-tag"'
    assert transport.requests == [_ID_REQUEST, _DIRECT_REQUEST]


@pytest.mark.parametrize(
    "routes",
    [
        [_FALLBACK_ROUTE, _DESIRED_ROUTE],
        [_FALLBACK_ROUTE, _FALLBACK_ROUTE],
        [{**_FALLBACK_ROUTE, "terminal": False}],
    ],
)
def test_pre_mutation_rejects_misordered_duplicate_or_modified_fallbacks(
    routes: list[object],
) -> None:
    with pytest.raises(CaddyAdminError) as caught:
        _inspect([_response(404), _response(200, routes, ("Etag", '"route-array-tag"'))])

    assert caught.value.code is CaddyAdminErrorCode.ROUTES_AMBIGUOUS


@pytest.mark.parametrize(
    "raw_routes",
    [
        (
            b'[{"@id":"fstack-unclaimed-host","handle":[{"handler":"static_response",'
            b'"status_code":404,"body":"not found\\n"}],"terminal":1}]'
        ),
        (
            b'[{"@id":"fstack-unclaimed-host","handle":[{"handler":"static_response",'
            b'"status_code":404.0,"body":"not found\\n"}],"terminal":true}]'
        ),
    ],
)
def test_pre_mutation_rejects_fallback_json_type_aliases(raw_routes: bytes) -> None:
    response = caddy_admin._HTTPResponse(
        "HTTP/1.1", 200, (("Etag", '"route-array-tag"'),), raw_routes
    )
    with pytest.raises(CaddyAdminError) as caught:
        _inspect([_response(404), response])

    assert caught.value.code is CaddyAdminErrorCode.ROUTES_AMBIGUOUS


@pytest.mark.parametrize(
    ("route", "code"),
    [
        (
            dict(_DESIRED_ROUTE, match=[{"host": ["changed.example.com"]}]),
            CaddyAdminErrorCode.ROUTE_ID_CONFLICT,
        ),
        (
            {
                "@id": f"fstack-crusher-ingress-{_OTHER_HEX}",
                "match": [{"host": [_DESIRED_HOST]}],
                "handle": [
                    {
                        "handler": "reverse_proxy",
                        "upstreams": [{"dial": f"fstack-crusher-{_OTHER_HEX}:8000"}],
                    }
                ],
            },
            CaddyAdminErrorCode.HOST_CONFLICT,
        ),
    ],
)
def test_route_and_host_conflicts_precede_misordered_fallback(
    route: dict[str, object], code: CaddyAdminErrorCode
) -> None:
    with pytest.raises(CaddyAdminError) as caught:
        _inspect([_response(404), _response(200, [_FALLBACK_ROUTE, route], ("Etag", '"tag"'))])

    assert caught.value.code is code


def test_exact_id_is_provisional_until_whole_array_contains_it() -> None:
    transport = _RecordingTransport(
        [_response(200, _DESIRED_ROUTE), _response(200, [], ("ETAG", '"tag"'))]
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.ROUTE_ID_CONFLICT
    assert len(transport.requests) == 2


def test_exact_route_is_adopted_only_with_safe_fstack_neighbors() -> None:
    other = {
        "@id": f"fstack-crusher-ingress-{_OTHER_HEX}",
        "match": [{"host": ["other.example.com"]}],
        "handle": [
            {
                "handler": "reverse_proxy",
                "upstreams": [{"dial": f"fstack-crusher-{_OTHER_HEX}:8000"}],
            }
        ],
    }
    result, _ = _inspect(
        [_response(404), _response(200, [_DESIRED_ROUTE, other], ("etag", '"tag"'))]
    )
    assert result.state is CaddyInspectionState.ADOPTED
    assert result.etag is None


@pytest.mark.parametrize(
    ("routes", "code"),
    [
        ([_DESIRED_ROUTE, _DESIRED_ROUTE], CaddyAdminErrorCode.ROUTE_ID_CONFLICT),
        (
            [dict(_DESIRED_ROUTE, match=[{"host": ["changed.example.com"]}])],
            CaddyAdminErrorCode.ROUTE_ID_CONFLICT,
        ),
        (
            [
                {
                    "@id": f"fstack-crusher-ingress-{_OTHER_HEX}",
                    "match": [{"host": [_DESIRED_HOST]}],
                    "handle": [
                        {
                            "handler": "reverse_proxy",
                            "upstreams": [{"dial": f"fstack-crusher-{_OTHER_HEX}:8000"}],
                        }
                    ],
                }
            ],
            CaddyAdminErrorCode.HOST_CONFLICT,
        ),
        (
            [{"match": [{"host": ["*.example.com"]}], "handle": []}],
            CaddyAdminErrorCode.HOST_CONFLICT,
        ),
        ([{"handle": []}], CaddyAdminErrorCode.ROUTES_AMBIGUOUS),
        ([42], CaddyAdminErrorCode.ROUTES_AMBIGUOUS),
        (
            [
                {
                    "@id": f"fstack-crusher-ingress-{_OTHER_HEX}",
                    "match": [{"host": ["other.example.com"]}],
                    "handle": [],
                }
            ],
            CaddyAdminErrorCode.ROUTES_AMBIGUOUS,
        ),
    ],
)
def test_whole_array_conflicts_are_deterministic(routes: object, code: CaddyAdminErrorCode) -> None:
    transport = _RecordingTransport([_response(404), _response(200, routes, ("Etag", '"tag"'))])
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)
    assert caught.value.code is code


@pytest.mark.parametrize(
    "matcher",
    [
        {"host": ["API.EXAMPLE.COM"], "path": ["/private/*"]},
        {"path": ["/ignored/*"], "host": ["*.EXAMPLE.COM"]},
        {"host": ["api.*.com"]},
        {"host": ["*.example.*"]},
        {"host": ["API.EXAMPLE.COM"], "method": ["GET"]},
    ],
)
def test_host_claim_precedes_unmanaged_route_ambiguity(matcher: dict[str, object]) -> None:
    route = {
        "match": [{"path": ["/first/*"]}, matcher, {"method": ["POST"]}],
        "handle": [],
        "terminal": True,
    }
    transport = _RecordingTransport([_response(404), _response(200, [route], ("Etag", '"tag"'))])

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)

    assert caught.value.code is CaddyAdminErrorCode.HOST_CONFLICT


@pytest.mark.parametrize(
    "pattern",
    [
        "*.example.com",
        "api.*.com",
        "*.*.com",
        "API.EXAMPLE.COM",
        "*.EXAMPLE.COM",
    ],
)
def test_caddy_host_patterns_claim_exact_label_depth_case_insensitively(pattern: str) -> None:
    route = {"match": [{"host": [pattern]}], "handle": []}
    transport = _RecordingTransport([_response(404), _response(200, [route], ("Etag", '"tag"'))])

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)

    assert caught.value.code is CaddyAdminErrorCode.HOST_CONFLICT


@pytest.mark.parametrize(
    "pattern",
    [
        "*.*.example.com",
        "example.com",
        "api.*example.com",
        "api.exa*ple.com",
        "api.example.*.com",
    ],
)
def test_caddy_host_patterns_require_equal_depth_and_complete_wildcard_labels(
    pattern: str,
) -> None:
    route = {"match": [{"host": [pattern]}], "handle": []}
    transport = _RecordingTransport([_response(404), _response(200, [route], ("Etag", '"tag"'))])

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)

    assert caught.value.code is CaddyAdminErrorCode.ROUTES_AMBIGUOUS


def test_route_id_conflict_precedes_host_claim() -> None:
    route = {
        "@id": _ROUTE_ID,
        "match": [{"host": [_DESIRED_HOST], "path": ["/also-conflicts/*"]}],
        "handle": [],
    }
    transport = _RecordingTransport([_response(404), _response(200, [route], ("Etag", '"tag"'))])

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)

    assert caught.value.code is CaddyAdminErrorCode.ROUTE_ID_CONFLICT


def test_later_valid_host_claim_precedes_earlier_malformed_host_matcher() -> None:
    route = {
        "match": [
            {"host": "not-a-list"},
            {"host": ["API.EXAMPLE.COM"], "path": ["/claimed/*"]},
        ],
        "handle": [],
    }
    transport = _RecordingTransport([_response(404), _response(200, [route], ("Etag", '"tag"'))])

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)

    assert caught.value.code is CaddyAdminErrorCode.HOST_CONFLICT


@pytest.mark.parametrize("hosts", ["api.example.com", [], [_DESIRED_HOST, 7]])
def test_malformed_host_value_without_established_claim_is_ambiguous(hosts: object) -> None:
    route = {"match": [{"host": hosts}], "handle": []}
    transport = _RecordingTransport([_response(404), _response(200, [route], ("Etag", '"tag"'))])

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)

    assert caught.value.code is CaddyAdminErrorCode.ROUTES_AMBIGUOUS


@pytest.mark.parametrize(
    ("raw_etag", "expected"),
    [
        (b'""', '""'),
        (b'"opaque!#~"', '"opaque!#~"'),
        (b'W/"weak-opaque"', 'W/"weak-opaque"'),
        (
            b'"/config/apps/http/servers/fstack/routes 2c455418182f45b0"',
            '"/config/apps/http/servers/fstack/routes 2c455418182f45b0"',
        ),
        (b'"\x80\xff"', '"\x80\xff"'),
    ],
)
def test_public_direct_pre_accepts_one_syntactically_valid_etag_unchanged(
    tmp_path: Path, raw_etag: bytes, expected: str
) -> None:
    responses = [
        _wire_response(404),
        _wire_json_response(200, [], b"Etag: " + raw_etag),
    ]
    server = _PersistentWireServer(tmp_path / "admin", responses)
    completed = False
    try:
        result = CaddyAdmin(CaddyAdminTransport(_config(server.root))).inspect(_DESIRED_ROUTE)
        assert result == CaddyInspection(CaddyInspectionState.ABSENT, expected)
        assert server.requests == [_ID_REQUEST, _DIRECT_REQUEST]
        completed = True
    finally:
        server.close(require_all=completed)


@pytest.mark.parametrize(
    "etag_headers",
    [
        [],
        [b"Etag: arbitrary-nonempty"],
        [b'Etag:  " padded "'],
        [b'Etag: "two  spaces"'],
        [b'etag: w/"lowercase-weak"'],
        [b'etag: "unterminated'],
        [b'etag: "embedded"quote"'],
        [b'etag: "control\tcharacter"'],
        [b'etag: "first"', b'ETAG: "second"'],
        [b"etag: "],
    ],
    ids=[
        "missing",
        "arbitrary-nonempty-regression",
        "whitespace-inside-opaque-tag",
        "repeated-whitespace-inside-opaque-tag",
        "lowercase-weak-prefix",
        "unterminated",
        "embedded-quote",
        "control",
        "multiple",
        "empty-field-value",
    ],
)
def test_public_direct_pre_rejects_invalid_or_nonunique_etag_syntax(
    tmp_path: Path, etag_headers: list[bytes]
) -> None:
    responses = [_wire_response(404), _wire_json_response(200, [], *etag_headers)]
    server = _PersistentWireServer(tmp_path / "admin", responses)
    completed = False
    try:
        with pytest.raises(CaddyAdminError) as caught:
            CaddyAdmin(CaddyAdminTransport(_config(server.root))).inspect(_DESIRED_ROUTE)
        assert caught.value.code is CaddyAdminErrorCode.PREREQUISITE_MISSING
        assert server.requests == [_ID_REQUEST, _DIRECT_REQUEST]
        completed = True
    finally:
        server.close(require_all=completed)


def test_direct_400_falls_back_to_parent_and_parent_only_absence_is_incompatible() -> None:
    transport = _RecordingTransport(
        [_response(404), _response(400, "ignored"), _response(200, {"fstack": {"routes": []}})]
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    assert transport.requests[-1] == (
        b"GET /config/apps/http/servers HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )


@pytest.mark.parametrize(
    "parent", [{}, {"fstack": None}, {"fstack": {}}, {"fstack": {"routes": {}}}]
)
def test_parent_200_requires_fstack_routes_array(parent: object) -> None:
    transport = _RecordingTransport([_response(404), _response(404), _response(200, parent)])
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.PREREQUISITE_MISSING


_ID_REQUEST = (
    f"GET /id/{_ROUTE_ID} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
).encode()
_DIRECT_REQUEST = (
    b"GET /config/apps/http/servers/fstack/routes HTTP/1.1\r\n"
    b"Host: localhost\r\nConnection: close\r\n\r\n"
)
_PARENT_REQUEST = (
    b"GET /config/apps/http/servers HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
)


class _PersistentWireServer:
    def __init__(self, root: Path, responses: list[bytes]) -> None:
        self.root = root
        self.root.mkdir(mode=0o700)
        self.path = root / "caddy-admin.sock"
        self.requests: list[bytes] = []
        self.errors: list[BaseException] = []
        self._responses = responses
        self._stop = threading.Event()
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            self._server.bind(f"/proc/self/fd/{root_fd}/caddy-admin.sock")
        finally:
            os.close(root_fd)
        _set_socket_permissions(self.path)
        self._server.listen()
        self._server.settimeout(0.1)
        self._thread = threading.Thread(target=self._serve)
        self._thread.start()

    def _serve(self) -> None:
        try:
            for response in self._responses:
                while True:
                    try:
                        connection, _ = self._server.accept()
                        break
                    except TimeoutError:
                        if self._stop.is_set():
                            return
                with connection:
                    request = bytearray()
                    while b"\r\n\r\n" not in request:
                        chunk = connection.recv(65_536)
                        if not chunk:
                            raise AssertionError("request ended before complete headers")
                        request.extend(chunk)
                        if len(request) > 65_536:
                            raise AssertionError("request headers exceeded test bound")
                    head, body = request.split(b"\r\n\r\n", 1)
                    lengths = [
                        line.split(b":", 1)[1].strip()
                        for line in head.split(b"\r\n")[1:]
                        if line.lower().startswith(b"content-length:")
                    ]
                    expected_body = int(lengths[0]) if lengths else 0
                    while len(body) < expected_body:
                        chunk = connection.recv(65_536)
                        if not chunk:
                            raise AssertionError("request ended before complete body")
                        body += chunk
                    if len(body) != expected_body:
                        raise AssertionError("request body length mismatch")
                    self.requests.append(bytes(head + b"\r\n\r\n" + body))
                    connection.sendall(response)
        except BaseException as error:
            if not self._stop.is_set():
                self.errors.append(error)
        finally:
            self._server.close()

    def close(self, *, require_all: bool) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        if self._thread.is_alive():
            self._server.close()
            self._thread.join(timeout=1)
        assert not self._thread.is_alive()
        assert self._server.fileno() == -1
        assert self.errors == []
        if require_all:
            assert len(self.requests) == len(self._responses)
        self.path.unlink()
        self.root.rmdir()
        assert not self.path.exists()
        assert not self.root.exists()


_ALL_WIRE_STATUSES = frozenset(range(100, 600))
_AUTH_STATUSES = frozenset({401, 403})
_ID_INCOMPATIBLE_STATUSES = frozenset(range(300, 500)) - _AUTH_STATUSES - {404}
_DIRECT_INCOMPATIBLE_STATUSES = frozenset(range(300, 500)) - _AUTH_STATUSES - {400, 404}
_PRE_UNAVAILABLE_STATUSES = frozenset(range(100, 300)) - {200} | frozenset(range(500, 600))
_POST_AMBIGUOUS_STATUSES = _PRE_UNAVAILABLE_STATUSES

_FROZEN_STATUS_CONTRACT: dict[
    str,
    tuple[tuple[frozenset[int], CaddyInspection | CaddyAdminErrorCode], ...],
] = {
    "id_pre": (
        (frozenset({200}), CaddyInspection(CaddyInspectionState.ADOPTED)),
        (frozenset({404}), CaddyInspection(CaddyInspectionState.ABSENT, '"tag"')),
        (_AUTH_STATUSES, CaddyAdminErrorCode.ADMIN_FORBIDDEN),
        (_ID_INCOMPATIBLE_STATUSES, CaddyAdminErrorCode.ADMIN_INCOMPATIBLE),
        (_PRE_UNAVAILABLE_STATUSES, CaddyAdminErrorCode.UNAVAILABLE),
    ),
    "direct_pre": (
        (frozenset({200}), CaddyInspection(CaddyInspectionState.ABSENT, '"tag"')),
        (frozenset({400, 404}), CaddyInspection(CaddyInspectionState.ADOPTED)),
        (_AUTH_STATUSES, CaddyAdminErrorCode.ADMIN_FORBIDDEN),
        (_DIRECT_INCOMPATIBLE_STATUSES, CaddyAdminErrorCode.ADMIN_INCOMPATIBLE),
        (_PRE_UNAVAILABLE_STATUSES, CaddyAdminErrorCode.UNAVAILABLE),
    ),
    "direct_post": (
        (frozenset({200, 400, 404}), CaddyInspection(CaddyInspectionState.ADOPTED)),
        (_AUTH_STATUSES, CaddyAdminErrorCode.ADMIN_FORBIDDEN),
        (_DIRECT_INCOMPATIBLE_STATUSES, CaddyAdminErrorCode.ADMIN_INCOMPATIBLE),
        (_POST_AMBIGUOUS_STATUSES, CaddyAdminErrorCode.MUTATION_AMBIGUOUS),
    ),
    "parent_pre": (
        (frozenset({200}), CaddyAdminErrorCode.ADMIN_INCOMPATIBLE),
        (frozenset({400, 404}), CaddyAdminErrorCode.PREREQUISITE_MISSING),
        (_AUTH_STATUSES, CaddyAdminErrorCode.ADMIN_FORBIDDEN),
        (_DIRECT_INCOMPATIBLE_STATUSES, CaddyAdminErrorCode.ADMIN_INCOMPATIBLE),
        (_PRE_UNAVAILABLE_STATUSES, CaddyAdminErrorCode.UNAVAILABLE),
    ),
    "parent_post": (
        (frozenset({200}), CaddyAdminErrorCode.MUTATION_AMBIGUOUS),
        (frozenset({400, 404}), CaddyAdminErrorCode.PREREQUISITE_MISSING),
        (_AUTH_STATUSES, CaddyAdminErrorCode.ADMIN_FORBIDDEN),
        (_DIRECT_INCOMPATIBLE_STATUSES, CaddyAdminErrorCode.ADMIN_INCOMPATIBLE),
        (_POST_AMBIGUOUS_STATUSES, CaddyAdminErrorCode.MUTATION_AMBIGUOUS),
    ),
}


def _frozen_status_expectation(kind: str, status: int) -> CaddyInspection | CaddyAdminErrorCode:
    matches = [
        expected for statuses, expected in _FROZEN_STATUS_CONTRACT[kind] if status in statuses
    ]
    assert len(matches) == 1, (kind, status, matches)
    return matches[0]


def _safe_marker_route(marker: str) -> dict[str, object]:
    return {
        "@id": f"fstack-crusher-ingress-{_OTHER_HEX}",
        "match": [{"host": [f"{marker}.example.net"]}],
        "handle": [
            {
                "handler": "reverse_proxy",
                "upstreams": [{"dial": f"fstack-crusher-{_OTHER_HEX}:8000"}],
            }
        ],
    }


def _wire_json_response(status: int, body: object, *headers: bytes) -> bytes:
    return _wire_response(
        status,
        json.dumps(_with_fallback(body), separators=(",", ":")).encode(),
        *headers,
    )


def _wire_status_scenario(
    kind: str, status: int, marker: str
) -> tuple[CaddyRequestContext, list[bytes], list[bytes]]:
    context = (
        CaddyRequestContext.POST_MUTATION
        if kind.endswith("post")
        else CaddyRequestContext.PRE_MUTATION
    )
    responses: list[bytes] = []
    requests: list[bytes] = []
    if kind != "direct_post" and kind != "parent_post" and kind != "id_pre":
        responses.append(_wire_response(404))
        requests.append(_ID_REQUEST)
    if kind.startswith("parent"):
        responses.append(_wire_response(400, b"direct-parent-fallback"))
        requests.append(_DIRECT_REQUEST)

    target_request = (
        _ID_REQUEST
        if kind == "id_pre"
        else _PARENT_REQUEST
        if kind.startswith("parent")
        else _DIRECT_REQUEST
    )
    requests.append(target_request)

    if status == 200:
        if kind == "id_pre":
            responses.extend(
                [
                    _wire_json_response(200, _DESIRED_ROUTE),
                    _wire_json_response(200, [_DESIRED_ROUTE], b'Etag: "tag"'),
                ]
            )
            requests.append(_DIRECT_REQUEST)
        elif kind == "direct_pre":
            responses.append(_wire_json_response(200, [], b'Etag: "tag"'))
        elif kind == "direct_post":
            responses.append(_wire_json_response(200, [_DESIRED_ROUTE]))
        else:
            responses.append(
                _wire_json_response(200, {"fstack": {"routes": [_safe_marker_route(marker)]}})
            )
    else:
        responses.append(_wire_response(status, marker.encode()))
        if kind == "id_pre" and status == 404:
            responses.append(_wire_json_response(200, [], b'Etag: "tag"'))
            requests.append(_DIRECT_REQUEST)
        elif kind.startswith("direct") and status in {400, 404}:
            responses.append(_wire_json_response(200, {"fstack": {"routes": [_DESIRED_ROUTE]}}))
            requests.append(_PARENT_REQUEST)
    return context, responses, requests


@pytest.mark.parametrize(
    "kind", ["id_pre", "direct_pre", "direct_post", "parent_pre", "parent_post"]
)
def test_real_wire_public_inspection_status_routing_is_exhaustive(
    tmp_path: Path, kind: str, caplog: pytest.LogCaptureFixture
) -> None:
    contract_statuses = frozenset(
        status for statuses, _ in _FROZEN_STATUS_CONTRACT[kind] for status in statuses
    )
    assert contract_statuses == _ALL_WIRE_STATUSES

    scenarios: list[
        tuple[
            int,
            str,
            CaddyRequestContext,
            list[bytes],
            list[bytes],
            CaddyInspection | CaddyAdminErrorCode,
        ]
    ] = []
    all_responses: list[bytes] = []
    for status in range(100, 600):
        marker = f"ignored-{kind.replace('_', '-')}-{status}-body-marker"
        context, responses, requests = _wire_status_scenario(kind, status, marker)
        expected = _frozen_status_expectation(kind, status)
        scenarios.append((status, marker, context, responses, requests, expected))
        all_responses.extend(responses)

    server = _PersistentWireServer(tmp_path / f"admin-{kind}", all_responses)
    expected_requests: list[bytes] = []
    completed = False
    try:
        admin = CaddyAdmin(CaddyAdminTransport(_config(server.root)))
        for status, marker, context, _, requests, expected in scenarios:
            expected_requests.extend(requests)
            if isinstance(expected, CaddyAdminErrorCode):
                with pytest.raises(CaddyAdminError) as caught:
                    admin.inspect(_DESIRED_ROUTE, context=context)
                assert caught.value.code is expected, (kind, status)
                _assert_inspection_error_is_traceback_local_sanitized(
                    caught.value, (marker,), caplog
                )
            else:
                result = admin.inspect(_DESIRED_ROUTE, context=context)
                assert result == expected, (kind, status)
                assert marker not in str(result)
                assert marker not in repr(result)
                assert marker not in caplog.text
            assert server.requests == expected_requests, (kind, status)
        completed = True
    finally:
        server.close(require_all=completed)


@pytest.mark.parametrize(
    ("body", "code"),
    [
        (b"not-json", CaddyAdminErrorCode.MUTATION_AMBIGUOUS),
        (b"{}", CaddyAdminErrorCode.MUTATION_AMBIGUOUS),
        (b"[]", CaddyAdminErrorCode.ROUTES_AMBIGUOUS),
    ],
)
def test_post_mutation_malformed_or_missing_fallback_confirmation_fails_closed(
    body: bytes, code: CaddyAdminErrorCode
) -> None:
    with pytest.raises(CaddyAdminError) as caught:
        _inspect(
            [caddy_admin._HTTPResponse("HTTP/1.1", 200, (), body)],
            context=CaddyRequestContext.POST_MUTATION,
        )
    assert caught.value.code is code


def test_duplicate_json_keys_are_never_accepted() -> None:
    body = b'[{"@id":"first","@id":"second"}]'
    transport = _RecordingTransport(
        [_response(404), caddy_admin._HTTPResponse("HTTP/1.1", 200, (("Etag", '"tag"'),), body)]
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.ROUTES_AMBIGUOUS


_STRICT_JSON_INVALID_BODIES = [
    pytest.param(b"\xef\xbb\xbf[]", id="utf8-bom"),
    pytest.param("[]".encode("utf-16"), id="utf16-bom"),
    pytest.param("[]".encode("utf-16-le"), id="utf16-le"),
    pytest.param("[]".encode("utf-16-be"), id="utf16-be"),
    pytest.param("[]".encode("utf-32"), id="utf32-bom"),
    pytest.param("[]".encode("utf-32-le"), id="utf32-le"),
    pytest.param("[]".encode("utf-32-be"), id="utf32-be"),
    pytest.param(b"[\xff]", id="invalid-utf8"),
    pytest.param(b'["\\ud800"]', id="high-surrogate"),
    pytest.param(b'["\\udfff"]', id="low-surrogate"),
    pytest.param(b'{"nested":["\\ud800"]}', id="nested-surrogate"),
    pytest.param(b"[1e400]", id="positive-exponent-overflow"),
    pytest.param(b"[-1e400]", id="negative-exponent-overflow"),
    pytest.param(b"[NaN]", id="nan"),
    pytest.param(b"[Infinity]", id="infinity"),
    pytest.param(b"[-Infinity]", id="negative-infinity"),
    pytest.param(b'{"duplicate":1,"duplicate":2}', id="duplicate-key"),
    pytest.param(b"[" + b"9" * 5000 + b"]", id="oversized-integer"),
]


@pytest.mark.parametrize("body", _STRICT_JSON_INVALID_BODIES)
@pytest.mark.parametrize(
    ("location", "context", "code"),
    [
        ("id", CaddyRequestContext.PRE_MUTATION, CaddyAdminErrorCode.ROUTE_ID_CONFLICT),
        ("direct", CaddyRequestContext.PRE_MUTATION, CaddyAdminErrorCode.ROUTES_AMBIGUOUS),
        ("direct", CaddyRequestContext.POST_MUTATION, CaddyAdminErrorCode.MUTATION_AMBIGUOUS),
        ("parent", CaddyRequestContext.PRE_MUTATION, CaddyAdminErrorCode.ROUTES_AMBIGUOUS),
        ("parent", CaddyRequestContext.POST_MUTATION, CaddyAdminErrorCode.MUTATION_AMBIGUOUS),
    ],
)
def test_strict_json_rejection_is_mapped_by_public_request_context(
    body: bytes,
    location: str,
    context: CaddyRequestContext,
    code: CaddyAdminErrorCode,
) -> None:
    invalid = caddy_admin._HTTPResponse("HTTP/1.1", 200, (("Etag", '"tag"'),), body)
    if location == "id":
        responses: list[object] = [invalid]
    elif location == "direct":
        responses = [invalid]
        if context is CaddyRequestContext.PRE_MUTATION:
            responses.insert(0, _response(404))
    else:
        responses = [_response(400), invalid]
        if context is CaddyRequestContext.PRE_MUTATION:
            responses.insert(0, _response(404))
    transport = _RecordingTransport(responses)

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE, context=context)

    assert caught.value.code is code


@pytest.mark.parametrize(
    "body",
    [
        b'[0,1,-1,1.25,1e308,-1e308,"plain",true,false,null]',
        b'{"nested":[{"finite":1.7976931348623157e308}]}',
    ],
)
def test_strict_json_keeps_valid_finite_json_parseable(body: bytes) -> None:
    parsed = caddy_admin._parse_json(body)
    assert parsed is not caddy_admin._JSON_INVALID

    def assert_finite(value: object) -> None:
        if type(value) is float:
            assert math.isfinite(value)
        elif type(value) is list:
            for item in value:
                assert_finite(item)
        elif type(value) is dict:
            for item in value.values():
                assert_finite(item)

    assert_finite(parsed)


def test_invalid_desired_route_fails_closed_without_request_or_marker_leak(
    caplog: pytest.LogCaptureFixture,
) -> None:
    marker = "desired-route-secret-marker"
    transport = _RecordingTransport([])
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect({"@id": marker})
    error = caught.value
    assert error.code is CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    assert transport.requests == []
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in caplog.text
    assert error.__cause__ is None
    assert error.__context__ is None


def test_exact_global_id_absent_from_parent_array_is_id_conflict() -> None:
    transport = _RecordingTransport(
        [
            _response(200, _DESIRED_ROUTE),
            _response(404),
            _response(200, {"fstack": {"routes": []}}),
        ]
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.ROUTE_ID_CONFLICT


@pytest.mark.parametrize(
    ("failure", "context", "code"),
    [
        (
            CaddyAdminError(CaddyAdminErrorCode.TRANSPORT),
            CaddyRequestContext.PRE_MUTATION,
            CaddyAdminErrorCode.UNAVAILABLE,
        ),
        (
            CaddyAdminError(CaddyAdminErrorCode.RESPONSE_INVALID),
            CaddyRequestContext.POST_MUTATION,
            CaddyAdminErrorCode.MUTATION_AMBIGUOUS,
        ),
    ],
)
def test_inspection_transport_and_framing_failures_use_request_context(
    failure: Exception, context: CaddyRequestContext, code: CaddyAdminErrorCode
) -> None:
    transport = _RecordingTransport([failure])
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE, context=context)
    assert caught.value.code is code
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_permanent_id_failure_stops_before_routes_request() -> None:
    transport = _RecordingTransport([_response(403), RuntimeError("must not request")])
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.ADMIN_FORBIDDEN
    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    "response",
    [
        object(),
        caddy_admin._HTTPResponse("HTTP/1.1", 99, (), b""),
        caddy_admin._HTTPResponse("HTTP/1.1", 600, (), b""),
        caddy_admin._HTTPResponse("HTTP/1.1", True, (), b""),
        caddy_admin._HTTPResponse("HTTP/1.1", 200, (), bytearray()),
    ],
)
def test_malformed_response_objects_are_sanitized_as_framing_context_failure(
    response: object,
) -> None:
    transport = _RecordingTransport([response])
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.UNAVAILABLE
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_inspection_uses_real_confined_unix_http_exchanges(tmp_path: Path) -> None:
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    path = root / "caddy-admin.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        server.bind(f"/proc/self/fd/{root_fd}/caddy-admin.sock")
    finally:
        os.close(root_fd)
    _set_socket_permissions(path)
    server.listen()
    responses = [
        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
        _wire_json_response(200, [], b'Etag: "routes"'),
    ]
    requests: list[bytes] = []

    def serve() -> None:
        try:
            for response in responses:
                connection, _ = server.accept()
                with connection:
                    requests.append(connection.recv(65_536))
                    connection.sendall(response)
        finally:
            server.close()

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        result = CaddyAdmin(CaddyAdminTransport(_config(root))).inspect(_DESIRED_ROUTE)
    finally:
        thread.join(timeout=2)

    assert result == caddy_admin.CaddyInspection(CaddyInspectionState.ABSENT, '"routes"')
    assert requests == [
        (f"GET /id/{_ROUTE_ID} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").encode(),
        b"GET /config/apps/http/servers/fstack/routes HTTP/1.1\r\n"
        b"Host: localhost\r\nConnection: close\r\n\r\n",
    ]
    assert not thread.is_alive()


def _wire_response(status: int, body: bytes = b"", *headers: bytes) -> bytes:
    return (
        f"HTTP/1.1 {status} Test\r\n".encode()
        + b"".join(header + b"\r\n" for header in headers)
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )


@pytest.mark.parametrize(
    "status_line",
    [b"HTTP/1.1 nope Broken", b"HTTP/1.1 099 BelowRange", b"HTTP/1.1 600 AboveRange"],
)
@pytest.mark.parametrize(
    ("kind", "context", "code", "prefix"),
    [
        ("id", CaddyRequestContext.PRE_MUTATION, CaddyAdminErrorCode.UNAVAILABLE, []),
        (
            "direct",
            CaddyRequestContext.PRE_MUTATION,
            CaddyAdminErrorCode.UNAVAILABLE,
            [_wire_response(404)],
        ),
        ("direct", CaddyRequestContext.POST_MUTATION, CaddyAdminErrorCode.MUTATION_AMBIGUOUS, []),
        (
            "parent",
            CaddyRequestContext.PRE_MUTATION,
            CaddyAdminErrorCode.UNAVAILABLE,
            [_wire_response(404), _wire_response(400)],
        ),
        (
            "parent",
            CaddyRequestContext.POST_MUTATION,
            CaddyAdminErrorCode.MUTATION_AMBIGUOUS,
            [_wire_response(400)],
        ),
    ],
)
def test_real_transport_malformed_status_lines_map_each_public_context(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    status_line: bytes,
    kind: str,
    context: CaddyRequestContext,
    code: CaddyAdminErrorCode,
    prefix: list[bytes],
) -> None:
    marker = f"real-{kind}-{context.value}-{status_line!r}-body-marker"
    bad = (
        status_line
        + b"\r\nContent-Length: "
        + str(len(marker)).encode()
        + b"\r\n\r\n"
        + marker.encode()
    )
    responses = [*prefix, bad]
    root = tmp_path / "admin"
    root.mkdir(mode=0o700)
    path = root / "caddy-admin.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        server.bind(f"/proc/self/fd/{root_fd}/caddy-admin.sock")
    finally:
        os.close(root_fd)
    _set_socket_permissions(path)
    server.listen()
    requests: list[bytes] = []

    def serve() -> None:
        try:
            for response in responses:
                connection, _ = server.accept()
                with connection:
                    requests.append(connection.recv(65_536))
                    connection.sendall(response)
        finally:
            server.close()

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        with pytest.raises(CaddyAdminError) as caught:
            CaddyAdmin(CaddyAdminTransport(_config(root))).inspect(_DESIRED_ROUTE, context=context)
    finally:
        thread.join(timeout=2)

    assert caught.value.code is code
    _assert_inspection_error_is_traceback_local_sanitized(caught.value, (marker,), caplog)
    expected_requests = {
        ("id", CaddyRequestContext.PRE_MUTATION): [_ID_REQUEST],
        ("direct", CaddyRequestContext.PRE_MUTATION): [_ID_REQUEST, _DIRECT_REQUEST],
        ("direct", CaddyRequestContext.POST_MUTATION): [_DIRECT_REQUEST],
        ("parent", CaddyRequestContext.PRE_MUTATION): [
            _ID_REQUEST,
            _DIRECT_REQUEST,
            _PARENT_REQUEST,
        ],
        ("parent", CaddyRequestContext.POST_MUTATION): [_DIRECT_REQUEST, _PARENT_REQUEST],
    }
    assert requests == expected_requests[(kind, context)]
    assert not thread.is_alive()


@pytest.mark.parametrize(
    ("context", "code"),
    [
        (CaddyRequestContext.PRE_MUTATION, CaddyAdminErrorCode.ROUTES_AMBIGUOUS),
        (CaddyRequestContext.POST_MUTATION, CaddyAdminErrorCode.MUTATION_AMBIGUOUS),
    ],
)
def test_parent_duplicate_keys_follow_request_context(
    context: CaddyRequestContext, code: CaddyAdminErrorCode
) -> None:
    body = b'{"fstack":{"routes":[]},"fstack":{"routes":[]}}'
    responses: list[object] = [
        caddy_admin._HTTPResponse("HTTP/1.1", 400, (), b""),
        caddy_admin._HTTPResponse("HTTP/1.1", 200, (), body),
    ]
    if context is CaddyRequestContext.PRE_MUTATION:
        responses.insert(0, caddy_admin._HTTPResponse("HTTP/1.1", 404, (), b""))
    transport = _RecordingTransport(responses)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE, context=context)
    assert caught.value.code is code


def test_provider_body_marker_is_discarded_from_error_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    marker = "caddy-provider-body-secret-marker"
    transport = _RecordingTransport(
        [
            _response(404),
            caddy_admin._HTTPResponse("HTTP/1.1", 200, (("Etag", '"tag"'),), marker.encode()),
        ]
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(_DESIRED_ROUTE)
    error = caught.value
    assert error.code is CaddyAdminErrorCode.ROUTES_AMBIGUOUS
    assert set(vars(error)) == {"code"}
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in caplog.text
    assert error.__cause__ is None
    assert error.__context__ is None


def _reachable_reprs(value: object, seen: set[int] | None = None) -> list[str]:
    visited = set() if seen is None else seen
    identity = id(value)
    if identity in visited:
        return []
    visited.add(identity)
    try:
        result = [repr(value)]
    except Exception:
        result = ["<repr-failed>"]
    if type(value) is dict:
        for key, item in value.items():
            result.extend(_reachable_reprs(key, visited))
            result.extend(_reachable_reprs(item, visited))
    elif type(value) in {list, tuple, set, frozenset}:
        for item in value:
            result.extend(_reachable_reprs(item, visited))
    elif hasattr(value, "__dict__") and not isinstance(value, type):
        result.extend(_reachable_reprs(vars(value), visited))
    return result


def _assert_inspection_error_is_traceback_local_sanitized(
    error: CaddyAdminError,
    markers: tuple[str, ...],
    caplog: pytest.LogCaptureFixture,
) -> None:
    rendered = "\n".join((str(error), repr(error), caplog.text))
    for marker in markers:
        assert marker not in rendered
    assert error.__cause__ is None
    assert error.__context__ is None

    traceback = error.__traceback__
    inspected_frames = 0
    while traceback is not None:
        frame = traceback.tb_frame
        if frame.f_globals.get("__name__") == "fstack.caddy_admin":
            inspected_frames += 1
            reachable = "\n".join(
                rendered_value
                for value in frame.f_locals.values()
                for rendered_value in _reachable_reprs(value)
            )
            for marker in markers:
                assert marker not in reachable, (frame.f_code.co_name, marker)
        traceback = traceback.tb_next
    assert inspected_frames > 0


@pytest.mark.parametrize(
    "scenario",
    [
        "id-malformed",
        "direct-malformed",
        "parent-malformed",
        "host-conflict",
        "transport",
        "invalid-desired",
    ],
)
def test_inspection_failures_remove_provider_and_desired_markers_from_traceback_locals(
    scenario: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    provider_marker = f"provider-{scenario}-traceback-marker"
    desired_marker = f"desired-{scenario}-traceback-marker"
    desired = json.loads(json.dumps(_DESIRED_ROUTE))
    desired["match"] = [{"host": [f"{desired_marker}.example.com"]}]

    if scenario == "id-malformed":
        responses: list[object] = [
            caddy_admin._HTTPResponse("HTTP/1.1", 200, (), provider_marker.encode())
        ]
    elif scenario == "direct-malformed":
        responses = [
            _response(404),
            caddy_admin._HTTPResponse(
                "HTTP/1.1", 200, (("Etag", '"tag"'),), provider_marker.encode()
            ),
        ]
    elif scenario == "parent-malformed":
        responses = [
            _response(404),
            _response(400, provider_marker),
            caddy_admin._HTTPResponse("HTTP/1.1", 200, (), provider_marker.encode()),
        ]
    elif scenario == "host-conflict":
        responses = [
            _response(404),
            _response(
                200,
                [
                    {
                        "match": [
                            {
                                "host": [f"{desired_marker.upper()}.EXAMPLE.COM"],
                                "path": [f"/{provider_marker}/*"],
                            }
                        ],
                        "handle": [],
                    }
                ],
                ("Etag", '"tag"'),
            ),
        ]
    elif scenario == "transport":
        responses = [RuntimeError(provider_marker)]
    else:
        desired = {"@id": desired_marker, "provider": provider_marker}
        responses = []
    transport = _RecordingTransport(responses)

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).inspect(desired)

    _assert_inspection_error_is_traceback_local_sanitized(
        caught.value, (provider_marker, desired_marker), caplog
    )


_CANONICAL_ROUTE_BYTES = (
    b'{"@id":"fstack-crusher-ingress-0123456789abcdef0123456789abcdef",'
    b'"handle":[{"handler":"reverse_proxy","upstreams":[{"dial":'
    b'"fstack-crusher-0123456789abcdef0123456789abcdef:8000"}]}],'
    b'"match":[{"host":["api.example.com"]}]}'
)
_INSERT_REQUEST = (
    b"PUT /config/apps/http/servers/fstack/routes/0 HTTP/1.1\r\n"
    b"Host: localhost\r\n"
    b"Content-Type: application/json\r\n"
    b"Content-Length: 221\r\n"
    b'If-Match: "route-array-tag"\r\n'
    b"Connection: close\r\n\r\n" + _CANONICAL_ROUTE_BYTES
)


def test_reconcile_inserts_one_exact_route_before_fallback_then_requires_confirmation() -> None:
    transport = _RecordingTransport(
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(200, "ignored-post-body-marker"),
            _response(200, [_DESIRED_ROUTE]),
        ]
    )

    assert CaddyAdmin(transport).reconcile(_DESIRED_ROUTE) is None

    assert transport.requests == [_ID_REQUEST, _DIRECT_REQUEST, _INSERT_REQUEST, _DIRECT_REQUEST]
    assert b"POST " not in b"".join(transport.requests)
    assert b"PATCH " not in b"".join(transport.requests)
    assert b"DELETE " not in b"".join(transport.requests)


def test_reconcile_adopts_exact_route_without_posting() -> None:
    transport = _RecordingTransport(
        [_response(200, _DESIRED_ROUTE), _response(200, [_DESIRED_ROUTE], ("Etag", '"tag"'))]
    )

    assert CaddyAdmin(transport).reconcile(_DESIRED_ROUTE) is None
    assert transport.requests == [_ID_REQUEST, _DIRECT_REQUEST]


def test_reconcile_412_is_config_changed_without_readback_or_retry() -> None:
    transport = _RecordingTransport(
        [_response(404), _response(200, [], ("Etag", '"route-array-tag"')), _response(412)]
    )

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).reconcile(_DESIRED_ROUTE)

    assert caught.value.code is CaddyAdminErrorCode.CONFIG_CHANGED
    assert transport.requests == [_ID_REQUEST, _DIRECT_REQUEST, _INSERT_REQUEST]


@pytest.mark.parametrize("status", [400, 404])
def test_reconcile_400_or_404_reads_back_exact_route(status: int) -> None:
    transport = _RecordingTransport(
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(status, "discard-post-body-marker"),
            _response(200, [_DESIRED_ROUTE]),
        ]
    )

    assert CaddyAdmin(transport).reconcile(_DESIRED_ROUTE) is None
    assert transport.requests == [_ID_REQUEST, _DIRECT_REQUEST, _INSERT_REQUEST, _DIRECT_REQUEST]


def test_reconcile_422_is_permanent_route_rejected() -> None:
    transport = _RecordingTransport(
        [_response(404), _response(200, [], ("Etag", '"route-array-tag"')), _response(422)]
    )

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).reconcile(_DESIRED_ROUTE)

    assert caught.value.code is CaddyAdminErrorCode.ROUTE_REJECTED


def test_reconcile_ambiguous_post_resolves_exact_id_then_confirms_full_array() -> None:
    transport = _RecordingTransport(
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(503, "discard-post-body-marker"),
            _response(200, _DESIRED_ROUTE),
            _response(200, [_DESIRED_ROUTE]),
        ]
    )

    assert CaddyAdmin(transport).reconcile(_DESIRED_ROUTE) is None
    assert transport.requests == [
        _ID_REQUEST,
        _DIRECT_REQUEST,
        _INSERT_REQUEST,
        _ID_REQUEST,
        _DIRECT_REQUEST,
    ]


def test_reconcile_retry_restarts_inspection_and_adopts_without_second_post() -> None:
    transport = _RecordingTransport(
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(503),
            _response(404),
            _response(200, _DESIRED_ROUTE),
            _response(200, [_DESIRED_ROUTE], ("Etag", '"new-tag"')),
        ]
    )
    admin = CaddyAdmin(transport)

    with pytest.raises(CaddyAdminError) as caught:
        admin.reconcile(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.MUTATION_AMBIGUOUS
    assert admin.reconcile(_DESIRED_ROUTE) is None
    assert transport.requests.count(_INSERT_REQUEST) == 1


def test_reconcile_submission_adopts_then_marks_only_the_two_submission_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import ingress_state

    transport = _RecordingTransport(
        [_response(200, _DESIRED_ROUTE), _response(200, [_DESIRED_ROUTE], ("Etag", '"tag"'))]
    )
    submission_id = uuid4()
    operation_id = uuid4()
    calls: list[tuple[object, ...]] = []

    def mark(*args: object) -> None:
        assert transport.requests == [_ID_REQUEST, _DIRECT_REQUEST]
        calls.append(args)

    monkeypatch.setattr(ingress_state, "mark_submitted", mark)

    assert (
        CaddyAdmin(transport).reconcile_submission(_DESIRED_ROUTE, submission_id, operation_id)
        is None
    )
    assert calls == [(submission_id, operation_id)]
    assert _INSERT_REQUEST not in transport.requests


def test_reconcile_submission_mutation_marks_only_after_full_array_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import ingress_state

    transport = _RecordingTransport(
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(200, "ignored-post-body-marker"),
            _response(200, [_DESIRED_ROUTE]),
        ]
    )
    submission_id = uuid4()
    operation_id = uuid4()
    calls: list[tuple[object, ...]] = []

    def mark(*args: object) -> None:
        assert transport.requests == [
            _ID_REQUEST,
            _DIRECT_REQUEST,
            _INSERT_REQUEST,
            _DIRECT_REQUEST,
        ]
        calls.append(args)

    monkeypatch.setattr(ingress_state, "mark_submitted", mark)

    assert (
        CaddyAdmin(transport).reconcile_submission(_DESIRED_ROUTE, submission_id, operation_id)
        is None
    )
    assert calls == [(submission_id, operation_id)]


@pytest.mark.parametrize(
    "responses",
    [
        [_response(503)],
        [_response(404), _response(200, [], ("Etag", '"route-array-tag"')), _response(412)],
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(200),
            caddy_admin._HTTPResponse("HTTP/1.1", 200, (), b"not-json"),
        ],
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(503),
            _response(404),
        ],
    ],
    ids=["pre-inspection", "post", "confirmation", "ambiguous-result"],
)
def test_reconcile_submission_never_marks_on_caddy_failure(
    responses: list[object], monkeypatch: pytest.MonkeyPatch
) -> None:
    from fstack import ingress_state

    calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(ingress_state, "mark_submitted", lambda *args: calls.append(args))

    with pytest.raises(CaddyAdminError):
        CaddyAdmin(_RecordingTransport(responses)).reconcile_submission(
            _DESIRED_ROUTE, uuid4(), uuid4()
        )

    assert calls == []


def test_reconcile_submission_retries_mark_after_confirmed_mutation_without_second_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fstack import ingress_state
    from fstack.ingress_state import IngressStateError

    transport = _RecordingTransport(
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(200),
            _response(200, [_DESIRED_ROUTE]),
            _response(200, _DESIRED_ROUTE),
            _response(200, [_DESIRED_ROUTE], ("Etag", '"new-tag"')),
        ]
    )
    submission_id = uuid4()
    operation_id = uuid4()
    calls: list[tuple[object, ...]] = []

    def mark(*args: object) -> None:
        calls.append(args)
        if len(calls) == 1:
            raise IngressStateError("ingress_state_infrastructure")

    monkeypatch.setattr(ingress_state, "mark_submitted", mark)
    admin = CaddyAdmin(transport)

    with pytest.raises(IngressStateError) as caught:
        admin.reconcile_submission(_DESIRED_ROUTE, submission_id, operation_id)
    assert caught.value.code == "ingress_state_infrastructure"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert transport.requests.count(_INSERT_REQUEST) == 1

    assert admin.reconcile_submission(_DESIRED_ROUTE, submission_id, operation_id) is None
    assert transport.requests.count(_INSERT_REQUEST) == 1
    assert calls == [(submission_id, operation_id), (submission_id, operation_id)]


@pytest.mark.parametrize("failure_domain", ["caddy", "state"])
def test_reconcile_submission_failure_discards_wrapper_markers_from_traceback_locals(
    failure_domain: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from fstack import ingress_state
    from fstack.ingress_state import IngressStateError

    route_marker = f"wrapper-route-{failure_domain}-marker"
    provider_marker = f"wrapper-provider-{failure_domain}-marker"
    state_marker = f"wrapper-state-{failure_domain}-marker"
    desired = json.loads(json.dumps(_DESIRED_ROUTE))
    desired["match"] = [{"host": [f"{route_marker}.example.com"]}]
    submission_id = uuid4()
    operation_id = uuid4()
    if failure_domain == "caddy":
        transport = _RecordingTransport([RuntimeError(provider_marker)])
    else:
        transport = _RecordingTransport(
            [_response(200, desired), _response(200, [desired], ("Etag", '"tag"'))]
        )

        def fail_mark(*args: object) -> None:
            del args
            error = IngressStateError("ingress_state_infrastructure")
            error.args = ("ingress_state_infrastructure", state_marker)
            raise error

        monkeypatch.setattr(ingress_state, "mark_submitted", fail_mark)

    expected_type = CaddyAdminError if failure_domain == "caddy" else IngressStateError
    with pytest.raises(expected_type) as caught:
        CaddyAdmin(transport).reconcile_submission(desired, submission_id, operation_id)

    markers = (
        route_marker,
        provider_marker,
        state_marker,
        str(submission_id),
        str(operation_id),
    )
    rendered = "\n".join((str(caught.value), repr(caught.value), caplog.text))
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    traceback = caught.value.__traceback__
    inspected_frames = 0
    while traceback is not None:
        frame = traceback.tb_frame
        if frame.f_globals.get("__name__") == "fstack.caddy_admin":
            inspected_frames += 1
            rendered += "\n" + "\n".join(
                item for value in frame.f_locals.values() for item in _reachable_reprs(value)
            )
        traceback = traceback.tb_next
    assert inspected_frames > 0
    for marker in markers:
        assert marker not in rendered


def test_caddy_admin_exposes_no_general_mutation_method() -> None:
    assert not any(
        hasattr(CaddyAdmin, name) for name in ("request", "put", "patch", "delete", "load")
    )


def _reconcile_prefix() -> tuple[list[bytes], list[bytes]]:
    return (
        [_wire_response(404), _wire_json_response(200, [], b'Etag: "route-array-tag"')],
        [_ID_REQUEST, _DIRECT_REQUEST],
    )


def _post_status_code(status: int) -> CaddyAdminErrorCode | None:
    if status in {200, 400, 404}:
        return None
    if status == 412:
        return CaddyAdminErrorCode.CONFIG_CHANGED
    if status == 422:
        return CaddyAdminErrorCode.ROUTE_REJECTED
    if status in {401, 403}:
        return CaddyAdminErrorCode.ADMIN_FORBIDDEN
    if 300 <= status <= 499:
        return CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    return CaddyAdminErrorCode.MUTATION_AMBIGUOUS


def test_real_wire_public_reconcile_post_statuses_are_exhaustive(tmp_path: Path) -> None:
    scenarios: list[tuple[int, str, list[bytes], list[bytes], CaddyAdminErrorCode | None]] = []
    all_responses: list[bytes] = []
    for status in range(100, 600):
        marker = f"post-status-{status}-body-marker"
        responses, requests = _reconcile_prefix()
        responses.append(_wire_response(status, marker.encode()))
        requests.append(_INSERT_REQUEST)
        code = _post_status_code(status)
        if status in {200, 400, 404}:
            responses.append(_wire_json_response(200, [_DESIRED_ROUTE]))
            requests.append(_DIRECT_REQUEST)
        elif code is CaddyAdminErrorCode.MUTATION_AMBIGUOUS:
            responses.append(_wire_response(404, b"id-absent-marker"))
            requests.append(_ID_REQUEST)
        scenarios.append((status, marker, responses, requests, code))
        all_responses.extend(responses)

    server = _PersistentWireServer(tmp_path / "admin-post-statuses", all_responses)
    expected_requests: list[bytes] = []
    completed = False
    try:
        admin = CaddyAdmin(CaddyAdminTransport(_config(server.root)))
        for status, marker, _, requests, code in scenarios:
            expected_requests.extend(requests)
            if code is None:
                assert admin.reconcile(_DESIRED_ROUTE) is None, status
            else:
                with pytest.raises(CaddyAdminError) as caught:
                    admin.reconcile(_DESIRED_ROUTE)
                assert caught.value.code is code, status
                assert marker not in str(caught.value)
                assert marker not in repr(caught.value)
            assert server.requests == expected_requests, status
        completed = True
    finally:
        server.close(require_all=completed)


def _post_id_status_code(status: int) -> CaddyAdminErrorCode | None:
    if status == 200:
        return None
    if status in {401, 403}:
        return CaddyAdminErrorCode.ADMIN_FORBIDDEN
    if 300 <= status <= 499 and status != 404:
        return CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    return CaddyAdminErrorCode.MUTATION_AMBIGUOUS


def test_real_wire_public_post_ambiguity_id_statuses_are_exhaustive(tmp_path: Path) -> None:
    scenarios: list[tuple[int, str, list[bytes], list[bytes], CaddyAdminErrorCode | None]] = []
    all_responses: list[bytes] = []
    for status in range(100, 600):
        marker = f"post-id-status-{status}-body-marker"
        responses, requests = _reconcile_prefix()
        responses.append(_wire_response(503, b"ambiguous-post-marker"))
        requests.append(_INSERT_REQUEST)
        if status == 200:
            responses.append(_wire_json_response(200, _DESIRED_ROUTE))
        else:
            responses.append(_wire_response(status, marker.encode()))
        requests.append(_ID_REQUEST)
        code = _post_id_status_code(status)
        if status == 200:
            responses.append(_wire_json_response(200, [_DESIRED_ROUTE]))
            requests.append(_DIRECT_REQUEST)
        scenarios.append((status, marker, responses, requests, code))
        all_responses.extend(responses)

    server = _PersistentWireServer(tmp_path / "admin-post-id-statuses", all_responses)
    expected_requests: list[bytes] = []
    completed = False
    try:
        admin = CaddyAdmin(CaddyAdminTransport(_config(server.root)))
        for status, marker, _, requests, code in scenarios:
            expected_requests.extend(requests)
            if code is None:
                assert admin.reconcile(_DESIRED_ROUTE) is None, status
            else:
                with pytest.raises(CaddyAdminError) as caught:
                    admin.reconcile(_DESIRED_ROUTE)
                assert caught.value.code is code, status
                assert marker not in str(caught.value)
                assert marker not in repr(caught.value)
            assert server.requests == expected_requests, status
        completed = True
    finally:
        server.close(require_all=completed)


def _confirmation_status_code(status: int) -> CaddyAdminErrorCode | None:
    if status in {200, 400, 404}:
        return None
    if status in {401, 403}:
        return CaddyAdminErrorCode.ADMIN_FORBIDDEN
    if 300 <= status <= 499:
        return CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    return CaddyAdminErrorCode.MUTATION_AMBIGUOUS


@pytest.mark.parametrize("origin", ["post-200", "post-400", "id-exact"])
def test_real_wire_public_post_confirmation_statuses_are_exhaustive(
    tmp_path: Path, origin: str
) -> None:
    scenarios: list[tuple[int, list[bytes], list[bytes], CaddyAdminErrorCode | None]] = []
    all_responses: list[bytes] = []
    for status in range(100, 600):
        responses, requests = _reconcile_prefix()
        if origin == "post-200":
            responses.append(_wire_response(200, b"post-accepted-marker"))
            requests.append(_INSERT_REQUEST)
        elif origin == "post-400":
            responses.append(_wire_response(400, b"post-readback-marker"))
            requests.append(_INSERT_REQUEST)
        else:
            responses.extend(
                [
                    _wire_response(503, b"post-ambiguous-marker"),
                    _wire_json_response(200, _DESIRED_ROUTE),
                ]
            )
            requests.extend([_INSERT_REQUEST, _ID_REQUEST])
        if status == 200:
            responses.append(_wire_json_response(200, [_DESIRED_ROUTE]))
        else:
            responses.append(_wire_response(status, f"confirmation-{status}-marker".encode()))
        requests.append(_DIRECT_REQUEST)
        code = _confirmation_status_code(status)
        if status in {400, 404}:
            responses.append(_wire_json_response(200, {"fstack": {"routes": [_DESIRED_ROUTE]}}))
            requests.append(_PARENT_REQUEST)
        scenarios.append((status, responses, requests, code))
        all_responses.extend(responses)

    server = _PersistentWireServer(tmp_path / f"admin-confirm-{origin}", all_responses)
    expected_requests: list[bytes] = []
    completed = False
    try:
        admin = CaddyAdmin(CaddyAdminTransport(_config(server.root)))
        for status, _, requests, code in scenarios:
            expected_requests.extend(requests)
            if code is None:
                assert admin.reconcile(_DESIRED_ROUTE) is None, (origin, status)
            else:
                with pytest.raises(CaddyAdminError) as caught:
                    admin.reconcile(_DESIRED_ROUTE)
                assert caught.value.code is code, (origin, status)
            assert server.requests == expected_requests, (origin, status)
        completed = True
    finally:
        server.close(require_all=completed)


@pytest.mark.parametrize(
    "bad_status",
    [b"HTTP/1.1 nope Broken", b"HTTP/1.1 099 BelowRange", b"HTTP/1.1 600 AboveRange"],
)
@pytest.mark.parametrize("location", ["post", "id", "confirmation"])
def test_real_wire_reconcile_malformed_and_out_of_range_statuses_are_ambiguous(
    tmp_path: Path, bad_status: bytes, location: str
) -> None:
    bad = bad_status + b"\r\nContent-Length: 18\r\n\r\nsecret-body-marker"
    responses, requests = _reconcile_prefix()
    if location == "post":
        responses.extend([bad, _wire_response(404)])
        requests.extend([_INSERT_REQUEST, _ID_REQUEST])
    elif location == "id":
        responses.extend([_wire_response(503), bad])
        requests.extend([_INSERT_REQUEST, _ID_REQUEST])
    else:
        responses.extend([_wire_response(200), bad])
        requests.extend([_INSERT_REQUEST, _DIRECT_REQUEST])
    server = _PersistentWireServer(tmp_path / f"admin-bad-{location}", responses)
    completed = False
    try:
        with pytest.raises(CaddyAdminError) as caught:
            CaddyAdmin(CaddyAdminTransport(_config(server.root))).reconcile(_DESIRED_ROUTE)
        assert caught.value.code is CaddyAdminErrorCode.MUTATION_AMBIGUOUS
        assert server.requests == requests
        completed = True
    finally:
        server.close(require_all=completed)


@pytest.mark.parametrize("post_status", [400, 404])
def test_reconcile_parent_fallback_missing_is_prerequisite(post_status: int) -> None:
    transport = _RecordingTransport(
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(post_status, "discard-post-marker"),
            _response(404, "discard-direct-marker"),
            _response(404, "discard-parent-marker"),
        ]
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).reconcile(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.PREREQUISITE_MISSING
    assert transport.requests == [
        _ID_REQUEST,
        _DIRECT_REQUEST,
        _INSERT_REQUEST,
        _DIRECT_REQUEST,
        _PARENT_REQUEST,
    ]


@pytest.mark.parametrize("post_status", [200, 400, 404])
@pytest.mark.parametrize(
    ("confirmation", "code"),
    [
        (b"not-json", CaddyAdminErrorCode.MUTATION_AMBIGUOUS),
        (b"{}", CaddyAdminErrorCode.MUTATION_AMBIGUOUS),
        (b"[]", CaddyAdminErrorCode.ROUTES_AMBIGUOUS),
    ],
)
def test_reconcile_unresolved_direct_confirmation_fails_closed(
    post_status: int, confirmation: bytes, code: CaddyAdminErrorCode
) -> None:
    transport = _RecordingTransport(
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(post_status),
            caddy_admin._HTTPResponse("HTTP/1.1", 200, (), confirmation),
        ]
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).reconcile(_DESIRED_ROUTE)
    assert caught.value.code is code


def test_reconcile_ambiguous_post_id_difference_is_permanent_conflict_without_confirmation() -> (
    None
):
    different = dict(_DESIRED_ROUTE, match=[{"host": ["changed.example.com"]}])
    transport = _RecordingTransport(
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(503),
            _response(200, different),
        ]
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).reconcile(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.ROUTE_ID_CONFLICT
    assert transport.requests == [_ID_REQUEST, _DIRECT_REQUEST, _INSERT_REQUEST, _ID_REQUEST]


def test_reconcile_exact_post_id_still_requires_fresh_full_array_confirmation() -> None:
    transport = _RecordingTransport(
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(503),
            _response(200, _DESIRED_ROUTE),
            _response(200, []),
        ]
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).reconcile(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.MUTATION_AMBIGUOUS


def test_reconcile_rejects_crlf_injection_without_post() -> None:
    injected = json.loads(json.dumps(_DESIRED_ROUTE))
    injected["match"] = [{"host": ["api.example.com\r\nInjected: true"]}]
    transport = _RecordingTransport([])
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).reconcile(injected)
    assert caught.value.code is CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    assert transport.requests == []


def test_reconcile_rejects_injected_etag_before_post() -> None:
    transport = _RecordingTransport(
        [_response(404), _response(200, [], ("Etag", '"tag\r\nInjected: true"'))]
    )
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).reconcile(_DESIRED_ROUTE)
    assert caught.value.code is CaddyAdminErrorCode.PREREQUISITE_MISSING
    assert transport.requests == [_ID_REQUEST, _DIRECT_REQUEST]


@pytest.mark.parametrize("failure_location", ["post", "id", "direct", "parent"])
def test_reconcile_failure_traceback_recursively_discards_all_sensitive_markers(
    failure_location: str, caplog: pytest.LogCaptureFixture
) -> None:
    provider_marker = f"provider-{failure_location}-reconcile-marker"
    desired_marker = f"desired-{failure_location}-reconcile-marker"
    etag_marker = f"etag-{failure_location}-reconcile-marker"
    desired = json.loads(json.dumps(_DESIRED_ROUTE))
    desired["match"] = [{"host": [f"{desired_marker}.example.com"]}]
    canonical_marker = json.dumps(desired, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    responses: list[object] = [
        _response(404),
        _response(200, [], ("Etag", f'"{etag_marker}"')),
    ]
    if failure_location == "post":
        responses.extend([RuntimeError(provider_marker), _response(404)])
    elif failure_location == "id":
        responses.extend([_response(503, provider_marker), RuntimeError(provider_marker)])
    elif failure_location == "direct":
        responses.extend(
            [
                _response(200, provider_marker),
                caddy_admin._HTTPResponse("HTTP/1.1", 200, (), provider_marker.encode()),
            ]
        )
    else:
        responses.extend(
            [
                _response(404, provider_marker),
                _response(404, provider_marker),
                _response(404, provider_marker),
            ]
        )
    transport = _RecordingTransport(responses)
    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).reconcile(desired)
    _assert_inspection_error_is_traceback_local_sanitized(
        caught.value,
        (provider_marker, desired_marker, etag_marker, canonical_marker),
        caplog,
    )


def _post_confirmation_responses(origin: str, routes: list[object]) -> list[object]:
    prefix: list[object] = [
        _response(404),
        _response(200, [], ("Etag", '"route-array-tag"')),
    ]
    if origin == "post-200-direct":
        return [*prefix, _response(200), _response(200, routes)]
    if origin == "post-400-direct":
        return [*prefix, _response(400), _response(200, routes)]
    if origin == "post-404-direct":
        return [*prefix, _response(404), _response(200, routes)]
    if origin == "post-400-parent":
        return [
            *prefix,
            _response(400),
            _response(400),
            _response(200, {"fstack": {"routes": routes}}),
        ]
    if origin == "post-404-parent":
        return [
            *prefix,
            _response(404),
            _response(404),
            _response(200, {"fstack": {"routes": routes}}),
        ]
    assert origin == "ambiguous-post-exact-id"
    return [
        *prefix,
        _response(503),
        _response(200, _DESIRED_ROUTE),
        _response(200, routes),
    ]


_OTHER_SAFE_ROUTE = {
    "@id": f"fstack-crusher-ingress-{_OTHER_HEX}",
    "match": [{"host": ["other.example.com"]}],
    "handle": [
        {
            "handler": "reverse_proxy",
            "upstreams": [{"dial": f"fstack-crusher-{_OTHER_HEX}:8000"}],
        }
    ],
}


@pytest.mark.parametrize(
    ("routes", "code"),
    [
        ([_DESIRED_ROUTE, {"unknown": True}], CaddyAdminErrorCode.ROUTES_AMBIGUOUS),
        ([_DESIRED_ROUTE, {"match": [{}], "handle": []}], CaddyAdminErrorCode.ROUTES_AMBIGUOUS),
        (
            [dict(_DESIRED_ROUTE, match=[{"host": ["changed.example.com"]}])],
            CaddyAdminErrorCode.ROUTE_ID_CONFLICT,
        ),
        (
            [dict(_OTHER_SAFE_ROUTE, match=[{"host": [_DESIRED_HOST]}])],
            CaddyAdminErrorCode.HOST_CONFLICT,
        ),
    ],
    ids=["unknown-shape", "catch-all", "route-id", "host"],
)
@pytest.mark.parametrize(
    "origin",
    [
        "post-200-direct",
        "post-400-direct",
        "post-404-direct",
        "post-400-parent",
        "post-404-parent",
        "ambiguous-post-exact-id",
    ],
)
def test_post_confirmation_preserves_permanent_whole_array_conflicts(
    origin: str, routes: list[object], code: CaddyAdminErrorCode
) -> None:
    transport = _RecordingTransport(_post_confirmation_responses(origin, routes))

    with pytest.raises(CaddyAdminError) as caught:
        CaddyAdmin(transport).reconcile(_DESIRED_ROUTE)

    assert caught.value.code is code


class _BoundaryMutatingTransport(_RecordingTransport):
    def __init__(self, caller_route: dict[str, object], responses: list[object]) -> None:
        super().__init__(responses)
        self.caller_route = caller_route
        self.mutation_count = 0

    def _exchange(self, request: bytes) -> object:
        self.requests.append(request)
        self.mutation_count += 1
        self.caller_route["@id"] = f"mutated-id-{self.mutation_count}"
        match = self.caller_route["match"]
        assert type(match) is list and type(match[0]) is dict
        match[0]["host"] = [f"mutated-{self.mutation_count}.example.net"]
        match[0]["path"] = [f"/mutated-{self.mutation_count}/*"]
        handle = self.caller_route["handle"]
        assert type(handle) is list and type(handle[0]) is dict
        upstreams = handle[0]["upstreams"]
        assert type(upstreams) is list and type(upstreams[0]) is dict
        upstreams[0]["dial"] = f"mutated-upstream-{self.mutation_count}:9999"
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_inspect_uses_detached_route_snapshot_across_every_exchange_boundary() -> None:
    original = json.loads(json.dumps(_DESIRED_ROUTE))
    caller = json.loads(json.dumps(original))
    transport = _BoundaryMutatingTransport(
        caller,
        [_response(200, original), _response(200, [original], ("Etag", '"tag"'))],
    )

    assert CaddyAdmin(transport).inspect(caller).state is CaddyInspectionState.ADOPTED
    assert transport.requests == [_ID_REQUEST, _DIRECT_REQUEST]
    assert transport.mutation_count == 2


def test_reconcile_freezes_route_and_canonical_payload_before_first_io() -> None:
    original = json.loads(json.dumps(_DESIRED_ROUTE))
    caller = json.loads(json.dumps(original))
    transport = _BoundaryMutatingTransport(
        caller,
        [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(200),
            _response(200, [original]),
        ],
    )

    assert CaddyAdmin(transport).reconcile(caller) is None
    assert transport.requests == [_ID_REQUEST, _DIRECT_REQUEST, _INSERT_REQUEST, _DIRECT_REQUEST]
    assert transport.requests[2].split(b"\r\n\r\n", 1)[1] == _CANONICAL_ROUTE_BYTES
    assert transport.mutation_count == 4


@pytest.mark.parametrize("operation", ["inspect", "reconcile"])
def test_caddy_admin_does_not_mutate_caller_owned_route(operation: str) -> None:
    caller = json.loads(json.dumps(_DESIRED_ROUTE))
    before = json.loads(json.dumps(caller))
    responses = (
        [_response(200, before), _response(200, [before], ("Etag", '"tag"'))]
        if operation == "inspect"
        else [
            _response(404),
            _response(200, [], ("Etag", '"route-array-tag"')),
            _response(200),
            _response(200, [before]),
        ]
    )
    admin = CaddyAdmin(_RecordingTransport(responses))

    if operation == "inspect":
        assert admin.inspect(caller).state is CaddyInspectionState.ADOPTED
    else:
        assert admin.reconcile(caller) is None

    assert caller == before


class _HostileDict(dict[object, object]):
    def __iter__(self):  # type: ignore[no-untyped-def]
        raise AssertionError("hostile-dict-iter")

    def get(self, key: object, default: object = None) -> object:
        raise AssertionError("hostile-dict-get")


class _HostileList(list[object]):
    def __iter__(self):  # type: ignore[no-untyped-def]
        raise AssertionError("hostile-list-iter")

    def __len__(self) -> int:
        raise AssertionError("hostile-list-len")


class _HostileStr(str):
    def __str__(self) -> str:
        raise AssertionError("hostile-str")


def _route_with_hostile_subclass(location: str) -> object:
    route = json.loads(json.dumps(_DESIRED_ROUTE))
    if location == "outer-dict":
        return _HostileDict(route)
    if location == "route-id-str":
        route["@id"] = _HostileStr(route["@id"])
    elif location == "match-list":
        route["match"] = _HostileList(route["match"])
    elif location == "matcher-dict":
        route["match"][0] = _HostileDict(route["match"][0])
    elif location == "host-list":
        route["match"][0]["host"] = _HostileList(route["match"][0]["host"])
    elif location == "host-str":
        route["match"][0]["host"][0] = _HostileStr(route["match"][0]["host"][0])
    elif location == "handle-list":
        route["handle"] = _HostileList(route["handle"])
    elif location == "handler-dict":
        route["handle"][0] = _HostileDict(route["handle"][0])
    elif location == "handler-str":
        route["handle"][0]["handler"] = _HostileStr(route["handle"][0]["handler"])
    elif location == "upstreams-list":
        route["handle"][0]["upstreams"] = _HostileList(route["handle"][0]["upstreams"])
    elif location == "upstream-dict":
        route["handle"][0]["upstreams"][0] = _HostileDict(route["handle"][0]["upstreams"][0])
    else:
        assert location == "dial-str"
        route["handle"][0]["upstreams"][0]["dial"] = _HostileStr(
            route["handle"][0]["upstreams"][0]["dial"]
        )
    return route


@pytest.mark.parametrize("operation", ["inspect", "reconcile"])
@pytest.mark.parametrize(
    "location",
    [
        "outer-dict",
        "route-id-str",
        "match-list",
        "matcher-dict",
        "host-list",
        "host-str",
        "handle-list",
        "handler-dict",
        "handler-str",
        "upstreams-list",
        "upstream-dict",
        "dial-str",
    ],
)
def test_route_snapshot_rejects_hostile_builtin_subclasses_before_io(
    operation: str, location: str
) -> None:
    transport = _RecordingTransport([])
    admin = CaddyAdmin(transport)

    with pytest.raises(CaddyAdminError) as caught:
        if operation == "inspect":
            admin.inspect(_route_with_hostile_subclass(location))  # type: ignore[arg-type]
        else:
            admin.reconcile(_route_with_hostile_subclass(location))  # type: ignore[arg-type]

    assert caught.value.code is CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    assert transport.requests == []


@pytest.mark.parametrize("operation", ["inspect", "reconcile"])
@pytest.mark.parametrize("route", [None, 7, "route", [], {"@id": _ROUTE_ID}])
def test_invalid_route_shape_fails_before_transport(operation: str, route: object) -> None:
    transport = _RecordingTransport([])
    admin = CaddyAdmin(transport)

    with pytest.raises(CaddyAdminError) as caught:
        if operation == "inspect":
            admin.inspect(route)  # type: ignore[arg-type]
        else:
            admin.reconcile(route)  # type: ignore[arg-type]

    assert caught.value.code is CaddyAdminErrorCode.ADMIN_INCOMPATIBLE
    assert transport.requests == []
