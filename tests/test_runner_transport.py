import socket
import threading
import time

import pytest


@pytest.mark.parametrize("phase", ["headers", "body"])
def test_absolute_deadline_closes_slow_drip_socket(phase):
    from fstack.runner_transport import exchange_json

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(3)
    closed = threading.Event()

    def serve():
        with listener.accept()[0] as peer:
            peer.settimeout(2)
            peer.recv(8192)
            try:
                if phase == "body":
                    peer.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
                for _ in range(100):
                    peer.sendall(b"x")
                    time.sleep(0.04)
            except OSError:
                closed.set()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    started = time.monotonic()
    try:
        with pytest.raises(RuntimeError, match="runner exchange failed"):
            exchange_json(
                f"http://127.0.0.1:{listener.getsockname()[1]}",
                "/v1/runners/test",
                "fs_r_synthetic",
                {},
                allow_loopback_http=True,
                timeout=0.8,
            )
        assert time.monotonic() - started < 2
        assert closed.wait(2)
    finally:
        listener.close()
        thread.join(3)
        assert not thread.is_alive()
