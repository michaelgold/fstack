"""Negative controls derived from the human/session/device trust boundary."""

from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from fstack.api import create_app
from fstack.identity import AuthConfiguration, enroll_owner


@pytest.fixture
def identity(migrated_database):
    account = uuid4()
    with migrated_database.begin() as db:
        db.execute(
            text("INSERT INTO organizations (id,slug,name) VALUES (:a,'owner','Owner')"),
            {"a": account},
        )
    principal = enroll_owner(migrated_database, account, "human@example.test")
    mail = []
    config = AuthConfiguration(
        "https://testserver", "synthetic-" + "x" * 40, lambda *args: mail.append(args)
    )
    app = create_app(auth_configuration=config)
    with TestClient(app, base_url=config.origin) as human:
        request = human.post(
            "/v1/auth/login",
            json={"email": "human@example.test"},
            headers={"Origin": config.origin},
        )
        token = urlsplit(mail[-1][1]).fragment
        assert (
            human.post(
                "/v1/auth/confirm",
                json={"token": token, "csrf": request.json()["csrf"]},
                headers={"Origin": config.origin},
            ).status_code
            == 200
        )
        headers = {
            "Origin": config.origin,
            "X-CSRF-Token": human.get("/v1/auth/session").json()["csrf"],
        }
        yield migrated_database, account, principal, app, human, headers, config, mail


def start(identity):
    _, account, _, _, human, _, _, _ = identity
    response = human.post(
        "/v1/auth/device/start",
        json={"account_id": str(account), "client": "agent", "scopes": ["account:read"]},
    )
    assert response.status_code == 200
    return response.json()


def issue(identity):
    codes = start(identity)
    human, headers = identity[4:6]
    assert (
        human.post(
            "/v1/auth/device/approve", json={"user_code": codes["user_code"]}, headers=headers
        ).status_code
        == 200
    )
    response = human.post("/v1/auth/device/token", json={"device_code": codes["device_code"]})
    assert response.status_code == 200
    return response.json()


@pytest.mark.parametrize("mutation", ["owner=false", "active=false", "permissions='{}'"])
def test_current_membership_rechecked(identity, mutation):
    credential = issue(identity)
    db, account, _, _, human, _, _, _ = identity
    with db.begin() as connection:
        connection.execute(text("UPDATE auth_memberships SET " + mutation))
    result = human.get(
        f"/v1/agent/accounts/{account}",
        headers={"Authorization": "Bearer " + credential["access_token"]},
    )
    assert result.status_code == 403


@pytest.mark.parametrize("stage", ["approve", "exchange"])
def test_revocation_before_issuance_denied(identity, stage):
    codes = start(identity)
    db, _, _, _, human, headers, _, _ = identity
    if stage == "exchange":
        assert (
            human.post(
                "/v1/auth/device/approve", json={"user_code": codes["user_code"]}, headers=headers
            ).status_code
            == 200
        )
    with db.begin() as connection:
        connection.execute(text("UPDATE auth_memberships SET active=false"))
    if stage == "approve":
        result = human.post(
            "/v1/auth/device/approve", json={"user_code": codes["user_code"]}, headers=headers
        )
    else:
        result = human.post("/v1/auth/device/token", json={"device_code": codes["device_code"]})
    assert result.status_code == 403


def test_csrf_and_wrong_owner_and_exchange_escalation(identity):
    codes = start(identity)
    db, account, principal, app, human, headers, config, mail = identity
    assert (
        human.post("/v1/auth/device/approve", json={"user_code": codes["user_code"]}).status_code
        == 403
    )
    assert (
        human.post(
            "/v1/auth/device/approve",
            json={"user_code": codes["user_code"]},
            headers={**headers, "Origin": "https://evil.test"},
        ).status_code
        == 403
    )
    other = uuid4()
    with db.begin() as connection:
        connection.execute(
            text("INSERT INTO organizations (id,slug,name) VALUES (:a,'other','Other')"),
            {"a": other},
        )
    codes = human.post(
        "/v1/auth/device/start",
        json={"account_id": str(other), "client": "other-agent", "scopes": ["account:read"]},
    ).json()
    assert (
        human.get("/v1/auth/device/review", params={"user_code": codes["user_code"]}).status_code
        == 403
    )
    assert (
        human.post(
            "/v1/auth/device/approve", json={"user_code": codes["user_code"]}, headers=headers
        ).status_code
        == 403
    )
    assert (
        human.post(
            "/v1/auth/device/token", json={"device_code": codes["device_code"], "scopes": ["owner"]}
        ).status_code
        == 422
    )


@pytest.mark.parametrize("table", ["auth_devices", "auth_sessions", "auth_grants"])
def test_expiry(identity, table):
    codes = start(identity)
    credential = issue(identity) if table == "auth_grants" else None
    db, account, _, _, human, headers, _, _ = identity
    with db.begin() as connection:
        connection.execute(
            text(f"UPDATE {table} SET expires_at=clock_timestamp()-interval '1 second'")
        )
    if table == "auth_devices":
        result = human.post(
            "/v1/auth/device/approve", json={"user_code": codes["user_code"]}, headers=headers
        )
    elif table == "auth_sessions":
        result = human.get("/v1/auth/session")
    else:
        result = human.get(
            f"/v1/agent/accounts/{account}",
            headers={"Authorization": "Bearer " + credential["access_token"]},
        )
    assert result.status_code in (401, 403)


def test_session_agent_separation_wrong_account_and_reset(identity):
    credential = issue(identity)
    db, account, _, app, human, headers, config, _ = identity
    bearer = {"Authorization": "Bearer " + credential["access_token"]}
    assert human.get("/v1/auth/session", headers=bearer).status_code == 401
    assert human.get(f"/v1/agent/accounts/{uuid4()}", headers=bearer).status_code == 403
    reset = create_app(
        auth_configuration=AuthConfiguration(config.origin, "rotated-" + "z" * 40, config.send_mail)
    )
    with TestClient(reset, base_url=config.origin) as client:
        client.cookies.update(human.cookies)
        assert client.get("/v1/auth/session").status_code == 401
        assert client.get(f"/v1/agent/accounts/{account}", headers=bearer).status_code == 403


def test_exchange_single_consumer(identity):
    codes = start(identity)
    db, account, _, app, human, headers, config, _ = identity
    assert (
        human.post(
            "/v1/auth/device/approve", json={"user_code": codes["user_code"]}, headers=headers
        ).status_code
        == 200
    )

    def exchange(_):
        with TestClient(app, base_url=config.origin) as client:
            return client.post(
                "/v1/auth/device/token", json={"device_code": codes["device_code"]}
            ).status_code

    import time

    # Hold the actual device row and observe database lock waiters before release.
    blocker = db.connect()
    transaction = blocker.begin()
    blocker.execute(text("SELECT * FROM auth_devices FOR UPDATE")).all()
    with ThreadPoolExecutor(max_workers=4) as pool:
        try:
            futures = [pool.submit(exchange, i) for i in range(4)]
            deadline = time.monotonic() + 5
            waiting = 0
            while time.monotonic() < deadline:
                with db.connect() as observer:
                    waiting = observer.execute(
                        text("""SELECT count(*) FROM pg_stat_activity
                        WHERE datname=current_database() AND wait_event_type='Lock'
                          AND query LIKE '%auth_devices%'""")
                    ).scalar_one()
                if waiting == 4:
                    break
                time.sleep(0.02)
            assert waiting == 4
        finally:
            transaction.rollback()
            blocker.close()
        assert sorted(f.result(timeout=5) for f in futures) == [200, 403, 403, 403]
    with db.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM auth_grants")).scalar_one() == 1


def test_magic_scanner_replay_expiry_and_enumeration(identity):
    db, _, _, app, human, _, config, mail = identity
    response = human.post(
        "/v1/auth/login", json={"email": "human@example.test"}, headers={"Origin": config.origin}
    )
    raw_binding = human.cookies.get("__Host-fstack-login")
    token = urlsplit(mail[-1][1]).fragment
    with TestClient(app, base_url=config.origin) as scanner:
        assert scanner.get(mail[-1][1]).status_code == 200
    with db.connect() as connection:
        assert (
            connection.execute(
                text("SELECT count(*) FROM auth_logins WHERE NOT consumed")
            ).scalar_one()
            == 1
        )
    body = {"token": token, "csrf": response.json()["csrf"]}
    assert (
        human.post("/v1/auth/confirm", json=body, headers={"Origin": config.origin}).status_code
        == 200
    )
    human.cookies.set("__Host-fstack-login", raw_binding, domain="testserver.local", path="/")
    assert (
        human.post("/v1/auth/confirm", json=body, headers={"Origin": config.origin}).status_code
        == 403
    )
    response = human.post(
        "/v1/auth/login", json={"email": "human@example.test"}, headers={"Origin": config.origin}
    )
    with db.begin() as connection:
        connection.execute(
            text("UPDATE auth_logins SET expires_at=clock_timestamp()-interval '1 second'")
        )
    assert (
        human.post(
            "/v1/auth/confirm",
            json={"token": urlsplit(mail[-1][1]).fragment, "csrf": response.json()["csrf"]},
            headers={"Origin": config.origin},
        ).status_code
        == 403
    )
    unknown = human.post(
        "/v1/auth/login", json={"email": "unknown@example.test"}, headers={"Origin": config.origin}
    )
    assert unknown.status_code == response.status_code == 202
    assert unknown.json().keys() == response.json().keys()
    assert unknown.json()["status"] == response.json()["status"]


def test_durable_approved_binding_immutable(identity):
    codes = start(identity)
    db, _, _, _, human, headers, _, _ = identity
    assert (
        human.post(
            "/v1/auth/device/approve", json={"user_code": codes["user_code"]}, headers=headers
        ).status_code
        == 200
    )
    with pytest.raises(IntegrityError), db.begin() as connection:
        connection.execute(text("UPDATE auth_devices SET client='changed-client'"))


def test_mail_failure_uniform_response_rate_and_cookie_flags(identity):
    db, account, _, app, human, headers, config, mail = identity

    def fail(*_):
        raise RuntimeError("sensitive-provider-detail")

    failed_app = create_app(
        auth_configuration=AuthConfiguration(config.origin, config.secret, fail)
    )
    with TestClient(failed_app, base_url=config.origin) as client:
        known = client.post(
            "/v1/auth/login",
            json={"email": "human@example.test"},
            headers={"Origin": config.origin},
        )
        unknown = client.post(
            "/v1/auth/login",
            json={"email": "unknown@example.test"},
            headers={"Origin": config.origin},
        )
        assert known.status_code == unknown.status_code == 202
        assert known.json()["status"] == unknown.json()["status"]
        assert "sensitive-provider-detail" not in known.text
        cookie = known.headers["set-cookie"]
        assert all(value in cookie for value in ("Secure", "HttpOnly", "SameSite=strict", "Path=/"))
        assert known.headers["Cache-Control"] == "no-store"
        with db.begin() as connection:
            connection.execute(text("UPDATE auth_rate_buckets SET count=30 WHERE key='login'"))
        assert (
            client.post(
                "/v1/auth/login",
                json={"email": "human@example.test"},
                headers={"Origin": config.origin},
            ).status_code
            == 429
        )
    with db.connect() as connection:
        assert (
            connection.execute(
                text("SELECT count(*) FROM auth_logins WHERE NOT consumed")
            ).scalar_one()
            == 0
        )


def test_hash_only_credentials_and_no_email_owner_escalation(identity):
    credential = issue(identity)
    db, account, principal, app, human, headers, config, mail = identity
    with db.connect() as connection:
        stored = connection.execute(text("SELECT digest FROM auth_grants")).scalar_one()
        assert len(stored) == 64
        assert stored != credential["access_token"]
        assert connection.execute(text("SELECT count(*) FROM auth_principals")).scalar_one() == 1
        assert connection.execute(text("SELECT count(*) FROM auth_memberships")).scalar_one() == 1
    assert (
        human.post(
            "/v1/auth/login",
            json={"email": "other@example.test", "account_id": str(account)},
            headers={"Origin": config.origin},
        ).status_code
        == 422
    )
    assert (
        human.post(
            "/v1/auth/device/start",
            json={"account_id": str(account), "client": "agent", "scopes": ["owner"]},
        ).status_code
        == 403
    )
    assert human.post("/v1/auth/logout", headers=headers).status_code == 200
    assert human.get("/v1/auth/session").status_code == 401
