"""Real local TLS mail fixture; never contacts Crusher or a live mailbox."""

import json
import ssl
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from fstack.identity import AuthConfiguration
from fstack.identity_mail import CrusherMailer, load_auth_configuration


@pytest.fixture
def mail_server(tmp_path):
    certificate, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(certificate),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ],
        check=True,
        capture_output=True,
    )
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            requests.append(
                (
                    self.path,
                    self.headers.get("Authorization"),
                    json.loads(self.rfile.read(int(self.headers["Content-Length"]))),
                )
            )
            self.send_response(self.server.result_status)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.result_status = 200
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    trust = ssl.create_default_context(cafile=str(certificate))
    try:
        yield server, requests, trust
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive()


def test_verified_tls_crusher_mail_contract(mail_server):
    server, requests, trust = mail_server
    token = "synthetic-mail-credential"
    sender = CrusherMailer(
        f"https://localhost:{server.server_port}/v1/agent/emails/send",
        token,
        "Fstack <signin@example.test>",
        trust,
    )
    sender("human@example.test", "https://example.test/auth/confirm#synthetic", "request-one")
    assert len(requests) == 1
    path, auth, body = requests[0]
    assert path == "/v1/agent/emails/send"
    assert auth == "Bearer " + token
    assert body == {
        "source": "Fstack <signin@example.test>",
        "to_addresses": ["human@example.test"],
        "subject": "Confirm Fstack sign-in",
        "body_text": "https://example.test/auth/confirm#synthetic",
        "idempotency_key": "request-one",
    }
    assert token not in repr(sender)


@pytest.mark.parametrize("status", [302, 401, 409, 500])
def test_mail_no_retry_redirect_or_provider_details(mail_server, status):
    server, requests, trust = mail_server
    server.result_status = status
    sender = CrusherMailer(
        f"https://localhost:{server.server_port}/v1/agent/emails/send",
        "synthetic-mail-credential",
        "signin@example.test",
        trust,
    )
    with pytest.raises(RuntimeError, match="^authentication mail unavailable$") as error:
        sender("human@example.test", "https://example.test/#synthetic", "request-one")
    assert error.value.__context__ is None
    assert len(requests) == 1


def test_mail_rejects_untrusted_tls(mail_server):
    server, requests, _ = mail_server
    sender = CrusherMailer(
        f"https://localhost:{server.server_port}/v1/agent/emails/send",
        "synthetic-mail-credential",
        "signin@example.test",
    )
    with pytest.raises(RuntimeError):
        sender("human@example.test", "https://example.test/#synthetic", "request-one")
    assert requests == []


@pytest.mark.parametrize(
    "origin",
    [
        "http://example.test",
        "https://u:password@example.test",
        "https://example.test/",
        "https://example.test?query",
        "https://example.test/#fragment",
    ],
)
def test_invalid_origin(origin):
    with pytest.raises(ValueError):
        AuthConfiguration(origin, "synthetic-" + "x" * 40, lambda *_: None)


def test_enabled_requires_explicit_configuration(monkeypatch):
    monkeypatch.setenv("FSTACK_AUTH_MODE", "magic")
    monkeypatch.delenv("FSTACK_AUTH_SECRET", raising=False)
    with pytest.raises(ValueError, match="incomplete"):
        load_auth_configuration()
    with pytest.raises(ValueError):
        AuthConfiguration("https://example.test", "short", lambda *_: None)


def test_mounted_login_uses_real_tls_mail_fixture(migrated_database, mail_server):
    from urllib.parse import urlsplit
    from uuid import uuid4

    from fastapi.testclient import TestClient
    from sqlalchemy import text

    from fstack.api import create_app
    from fstack.identity import enroll_owner

    server, requests, trust = mail_server
    account = uuid4()
    with migrated_database.begin() as db:
        db.execute(
            text("INSERT INTO organizations (id,slug,name) VALUES (:a,'tls','TLS')"), {"a": account}
        )
    enroll_owner(migrated_database, account, "human@example.test")
    mailer = CrusherMailer(
        f"https://localhost:{server.server_port}/v1/agent/emails/send",
        "synthetic-mail-credential",
        "signin@example.test",
        trust,
    )
    config = AuthConfiguration("https://testserver", "synthetic-" + "x" * 40, mailer)
    with TestClient(create_app(auth_configuration=config), base_url=config.origin) as human:
        response = human.post(
            "/v1/auth/login",
            json={"email": "human@example.test"},
            headers={"Origin": config.origin},
        )
        assert response.status_code == 202
        assert len(requests) == 1
        token = urlsplit(requests[0][2]["body_text"]).fragment
        assert token not in response.text
        confirmed = human.post(
            "/v1/auth/confirm",
            json={"token": token, "csrf": response.json()["csrf"]},
            headers={"Origin": config.origin},
        )
        assert confirmed.status_code == 200
        assert human.get("/v1/auth/session").status_code == 200
