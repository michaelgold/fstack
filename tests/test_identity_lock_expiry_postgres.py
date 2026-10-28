"""Real lock waits must not extend authority; no mail leaves fixture memory."""

import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

import pytest
from sqlalchemy import Engine, event, text

from tests.test_identity_security_postgres import identity as identity
from tests.test_identity_security_postgres import issue, start

# (HTTP boundary, expiring authority, blocked row). Later locks are deliberate:
# rechecking only immediately after the first locking SELECT is insufficient.
CASES = [
    ("confirm", "auth_logins", "auth_logins"),
    ("confirm", "auth_logins", "auth_principals"),
    ("session", "auth_sessions", "auth_sessions"),
    ("session", "auth_sessions", "auth_principals"),
    ("approve", "auth_devices", "auth_devices"),
    ("approve", "auth_devices", "auth_memberships"),
    ("approve", "auth_sessions", "auth_devices"),
    ("approve", "auth_sessions", "auth_memberships"),
    ("review", "auth_devices", "auth_devices"),
    ("review", "auth_devices", "auth_memberships"),
    ("review", "auth_sessions", "auth_memberships"),
    ("exchange", "auth_devices", "auth_devices"),
    ("exchange", "auth_devices", "auth_memberships"),
    ("exchange", "auth_devices", "auth_principals"),
    ("account", "auth_grants", "auth_grants"),
    ("account", "auth_grants", "auth_memberships"),
    ("account", "auth_grants", "auth_principals"),
    ("revoke", "auth_sessions", "auth_grants"),
    ("revoke", "auth_sessions", "auth_memberships"),
    ("logout", "auth_sessions", "auth_sessions"),
]


def operation(identity, path):
    _, account, _, _, human, headers, config, mail = identity
    if path == "confirm":
        login = human.post("/v1/auth/login", json={"email": "human@example.test"}, headers=headers)
        assert login.status_code == 202
        body = {"token": urlsplit(mail[-1][1]).fragment, "csrf": login.json()["csrf"]}
        return lambda: human.post("/v1/auth/confirm", json=body, headers={"Origin": config.origin})
    if path == "session":
        return lambda: human.get("/v1/auth/session")
    if path == "logout":
        return lambda: human.post("/v1/auth/logout", headers=headers)
    if path in ("account", "revoke"):
        credential = issue(identity)
        if path == "account":
            return lambda: human.get(
                f"/v1/agent/accounts/{account}",
                headers={"Authorization": "Bearer " + credential["access_token"]},
            )
        return lambda: human.post(
            "/v1/auth/grants/" + credential["grant_id"] + "/revoke", headers=headers
        )
    codes = start(identity)
    if path == "exchange":
        assert (
            human.post(
                "/v1/auth/device/approve", json={"user_code": codes["user_code"]}, headers=headers
            ).status_code
            == 200
        )
        return lambda: human.post(
            "/v1/auth/device/token", json={"device_code": codes["device_code"]}
        )
    if path == "review":
        return lambda: human.get("/v1/auth/device/review", params={"user_code": codes["user_code"]})
    assert path == "approve"
    return lambda: human.post(
        "/v1/auth/device/approve", json={"user_code": codes["user_code"]}, headers=headers
    )


def durable_state(engine):
    with engine.connect() as db:
        return {
            table: db.execute(text(query)).all()
            for table, query in {
                "sessions": "SELECT digest,revoked FROM auth_sessions ORDER BY digest",
                "logins": "SELECT digest,consumed FROM auth_logins ORDER BY digest",
                "devices": "SELECT digest,state,principal_id FROM auth_devices ORDER BY digest",
                "grants": "SELECT id,revoked FROM auth_grants ORDER BY id",
            }.items()
        }


def bounded_transaction(db):
    # Test-only fuse, on every production and observer connection. Finally releases
    # the blocker even on observation failure; no executor teardown can mask a wait.
    db.execute(text("SET LOCAL lock_timeout = '10s'"))
    db.execute(text("SET LOCAL statement_timeout = '10s'"))


def wait_until(check, deadline):
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.01)
    pytest.fail("bounded PostgreSQL observation deadline exceeded")


def blocked_request(engine, call, table, after_blocked):
    event.listen(Engine, "begin", bounded_transaction)
    try:
        with engine.connect() as blocker:
            transaction = blocker.begin()
            pid = blocker.execute(text("SELECT pg_backend_pid()")).scalar_one()
            blocker.execute(text(f"SELECT * FROM {table} FOR UPDATE")).all()
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(call)
                try:
                    deadline = time.monotonic() + 8

                    def waiting():
                        with engine.connect() as observer:
                            return (
                                observer.execute(
                                    text("""SELECT count(*) FROM pg_stat_activity
                                WHERE datname=current_database() AND wait_event_type='Lock'
                                  AND :pid = ANY(pg_blocking_pids(pid))"""),
                                    {"pid": pid},
                                ).scalar_one()
                                == 1
                            )

                    wait_until(waiting, deadline)
                    assert not future.done()
                    after_blocked(blocker, deadline)
                finally:
                    transaction.commit()
                return future.result(timeout=12)
    finally:
        event.remove(Engine, "begin", bounded_transaction)


@pytest.mark.parametrize("path,authority,lock", CASES, ids=["-".join(c) for c in CASES])
@pytest.mark.parametrize("expire", [True, False], ids=["expires", "valid"])
def test_authority_after_observed_lock_wait(identity, path, authority, lock, expire):
    engine = identity[0]
    call = operation(identity, path)
    with engine.begin() as db:
        db.execute(
            text(f"UPDATE {authority} SET expires_at=clock_timestamp()+make_interval(secs=>:s)"),
            {"s": 2 if expire else 60},
        )
    before = durable_state(engine)

    def after_blocked(_, deadline):
        def crossed():
            with engine.connect() as observer:
                # Fresh server wall time, independently observed; never an arbitrary
                # sleep or application-clock mock standing in for expiration.
                return observer.execute(
                    text(f"SELECT bool_and(expires_at <= clock_timestamp()) FROM {authority}")
                ).scalar_one()

        assert not crossed(), "authority must still be live when the wait is observed"
        if expire:
            wait_until(crossed, deadline)

    response = blocked_request(engine, call, lock, after_blocked)
    if expire:
        assert response.status_code in (401, 403), response.text
        assert response.json() == {"code": "http_error", "message": "request failed"}
        assert "set-cookie" not in response.headers
        assert durable_state(engine) == before
    else:
        assert response.status_code == 200, response.text
        after = durable_state(engine)
        assert len(after["grants"]) == len(before["grants"]) + (path == "exchange")
        assert len(after["sessions"]) == len(before["sessions"]) + (path == "confirm")
        if path in ("exchange", "confirm", "approve"):
            assert call().status_code == 403
            assert durable_state(engine) == after


@pytest.mark.parametrize("path", ["approve", "review", "exchange", "account"])
@pytest.mark.parametrize("mutation", ["active=false", "owner=false", "permissions='{}'"])
def test_membership_revocation_during_observed_wait(identity, path, mutation):
    engine = identity[0]
    call = operation(identity, path)
    before = durable_state(engine)

    def revoke(blocker, _):
        blocker.execute(text("UPDATE auth_memberships SET " + mutation))

    response = blocked_request(engine, call, "auth_memberships", revoke)
    assert response.status_code == 403
    assert durable_state(engine) == before


@pytest.mark.parametrize(
    "path", ["confirm", "session", "approve", "review", "exchange", "account", "revoke", "logout"]
)
def test_principal_revocation_during_observed_wait(identity, path):
    engine = identity[0]
    call = operation(identity, path)
    before = durable_state(engine)

    def revoke(blocker, _):
        blocker.execute(text("UPDATE auth_principals SET active=false"))

    response = blocked_request(engine, call, "auth_principals", revoke)
    assert response.status_code in (401, 403)
    assert durable_state(engine) == before


@pytest.mark.parametrize("mutation", ["active=false", "owner=false"])
def test_revoke_requires_current_owner_after_wait(identity, mutation):
    engine = identity[0]
    call = operation(identity, "revoke")
    before = durable_state(engine)

    def revoke(blocker, _):
        blocker.execute(text("UPDATE auth_memberships SET " + mutation))

    response = blocked_request(engine, call, "auth_memberships", revoke)
    assert response.status_code == 403
    assert durable_state(engine) == before
