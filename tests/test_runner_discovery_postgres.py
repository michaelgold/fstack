from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from fstack.api import create_app
from fstack.identity import COOKIE, AuthConfiguration, IdentityAuthority, enroll_owner


@pytest.fixture
def discovery(migrated_database):
    engine = migrated_database
    config = AuthConfiguration("https://testserver", "synthetic-" + "x" * 40, lambda *_: None)
    authority = IdentityAuthority(config)
    account = uuid4()
    with engine.begin() as db:
        db.execute(
            text("INSERT INTO organizations(id,slug,name) VALUES (:a,'runner','Runner')"),
            {"a": account},
        )
    principal = enroll_owner(engine, account, "runner@example.test")
    session, agent = "synthetic-session", "fs_a_synthetic-agent"
    with engine.begin() as db:
        db.execute(
            text(
                "INSERT INTO auth_sessions(digest,principal_id,expires_at) "
                "VALUES (:d,:p,clock_timestamp()+interval '1 hour')"
            ),
            {"d": authority.digest(session), "p": principal},
        )
        db.execute(
            text(
                "INSERT INTO auth_grants "
                "(id,digest,account_id,principal_id,client,scopes,audience,expires_at) "
                "VALUES (:id,:d,:a,:p,'test',ARRAY['account:read'],'fstack-agent',"
                "clock_timestamp()+interval '1 hour')"
            ),
            {"id": uuid4(), "d": authority.digest(agent), "a": account, "p": principal},
        )
    with TestClient(create_app(auth_configuration=config), base_url=config.origin) as client:
        client.cookies.set(COOKIE, session)
        owner = {"origin": config.origin, "x-csrf-token": authority.csrf(session)}
        yield engine, client, str(account), owner, {"authorization": "Bearer " + agent}


def enroll(client, account, owner):
    response = client.post(
        f"/v1/auth/accounts/{account}/runners",
        headers=owner,
        json={
            "machine_class": "user-cpu",
            "image": "example.test/workload@sha256:" + "a" * 64,
            "ttl_seconds": 300,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_owner_runner_agent_discovery_and_replay(discovery):
    engine, client, account, owner, agent = discovery
    enrolled = enroll(client, account, owner)
    runner = enrolled["runner_id"]
    auth = {"authorization": "Bearer " + enrolled["runner_token"]}
    path = f"/v1/runners/{runner}/report"
    report = {"sequence": 1, "available_slots": 1, "queue_depth": 0}
    response = client.post(path, headers=auth, json=report)
    assert response.status_code == 200
    assert client.post(path, headers=auth, json=report).json() == response.json()
    assert client.post(path, headers=auth, json={**report, "queue_depth": 1}).status_code == 409
    response = client.get(f"/v1/agent/accounts/{account}/runners", headers=agent)
    assert response.status_code == 200
    machine = response.json()["machines"][0]
    assert machine["freshness"] == "fresh"
    assert machine["available_slots"] == 1 and machine["capacity_guaranteed"] is False
    assert machine["estimate"]["vendor_rate"] == "0"
    assert machine["estimate"]["unit"] == "resource-second"
    assert machine["estimate"]["duration_seconds"] is None
    assert machine["estimate"]["settled_cost"] is None
    assert "provider_cost" not in machine
    assert response.headers["cache-control"] == "no-store"
    with engine.begin() as db:
        assert (
            db.execute(
                text("SELECT token_digest FROM local_runners WHERE id=:id"), {"id": runner}
            ).scalar_one()
            != enrolled["runner_token"]
        )
    assert client.get(f"/v1/agent/accounts/{uuid4()}/runners", headers=agent).status_code == 403
    assert client.post(path, headers=agent, json=report).status_code == 401
    assert (
        client.post(
            f"/v1/auth/accounts/{account}/runners/{runner}/revoke", headers=owner
        ).status_code
        == 200
    )
    assert client.post(path, headers=auth, json={**report, "sequence": 2}).status_code == 403
    assert (
        client.get(f"/v1/agent/accounts/{account}/runners", headers=agent).json()["machines"] == []
    )


def test_read_grant_cannot_enroll_and_never_seen_unknown(discovery):
    _, client, account, owner, agent = discovery
    assert (
        client.post(
            f"/v1/auth/accounts/{account}/runners",
            headers=agent,
            json={
                "machine_class": "cpu",
                "image": "x@sha256:" + "a" * 64,
                "ttl_seconds": 60,
            },
        ).status_code
        == 401
    )
    enroll(client, account, owner)
    machine = client.get(f"/v1/agent/accounts/{account}/runners", headers=agent).json()["machines"][
        0
    ]
    assert machine["freshness"] == "unknown"
    assert machine["available_slots"] is None
    assert machine["queue_depth"] is None


def test_database_rejects_identity_rewrite_and_revocation_reversal(discovery):
    from sqlalchemy.exc import IntegrityError

    engine, client, account, owner, _ = discovery
    runner = enroll(client, account, owner)["runner_id"]
    for assignment in (
        "machine_class='other'",
        "expires_at=expires_at+interval '1 second'",
        "sequence=-1",
        "available_slots=1",
        "token_digest=repeat('b',64)",
    ):
        with pytest.raises(IntegrityError), engine.begin() as db:
            db.execute(text(f"UPDATE local_runners SET {assignment} WHERE id=:id"), {"id": runner})
    client.post(f"/v1/auth/accounts/{account}/runners/{runner}/revoke", headers=owner)
    with pytest.raises(IntegrityError), engine.begin() as db:
        db.execute(text("UPDATE local_runners SET revoked=false WHERE id=:id"), {"id": runner})


def test_stale_observation_is_not_available_and_expiry_denies(discovery):
    engine, client, account, owner, agent = discovery
    enrolled = enroll(client, account, owner)
    # Historical observations are inserted, never edited around the production trigger.
    stale, expired = uuid4(), uuid4()
    config = AuthConfiguration("https://testserver", "synthetic-" + "x" * 40, lambda *_: None)
    authority = IdentityAuthority(config)
    for runner, seconds, token in ((stale, 300, "fs_r_stale"), (expired, -1, "fs_r_expired")):
        with engine.begin() as db:
            db.execute(
                text("""INSERT INTO local_runners
                (id,account_id,principal_id,token_digest,machine_class,image,created_at,
                 expires_at,sequence,last_seen,available_slots,queue_depth)
                SELECT :new,account_id,principal_id,:digest,machine_class,image,
                    clock_timestamp()-interval '1 hour',clock_timestamp()+make_interval(secs=>:s),
                    1,clock_timestamp()-interval '1 minute',1,0
                FROM local_runners WHERE id=:old"""),
                {
                    "new": runner,
                    "digest": authority.digest(token),
                    "s": seconds,
                    "old": enrolled["runner_id"],
                },
            )
    machines = client.get(f"/v1/agent/accounts/{account}/runners", headers=agent).json()["machines"]
    observed = {m["runner_id"]: m for m in machines}
    assert str(expired) not in observed
    assert observed[str(stale)]["freshness"] == "stale"
    assert observed[str(stale)]["available_slots"] is None
    assert observed[str(stale)]["queue_depth"] is None
    report = {"sequence": 1, "available_slots": 1, "queue_depth": 0}
    assert (
        client.post(
            f"/v1/runners/{expired}/report",
            json=report,
            headers={"authorization": "Bearer fs_r_expired"},
        ).status_code
        == 403
    )
    replay = client.post(
        f"/v1/runners/{stale}/report", json=report, headers={"authorization": "Bearer fs_r_stale"}
    )
    assert replay.status_code == 200
    machines = client.get(f"/v1/agent/accounts/{account}/runners", headers=agent).json()["machines"]
    assert next(m for m in machines if m["runner_id"] == str(stale))["freshness"] == "stale"


def test_real_outbound_client_over_loopback_http(discovery):
    import socket
    import threading
    import time

    import uvicorn

    from fstack.runner_client import report_once

    engine, client, account, owner, agent = discovery
    enrolled = enroll(client, account, owner)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    origin = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(client.app, log_level="error", access_log=False))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not server.started and thread.is_alive() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert server.started
        receipt = report_once(
            origin,
            enrolled["runner_id"],
            enrolled["runner_token"],
            sequence=7,
            available_slots=2,
            queue_depth=3,
            allow_loopback_http=True,
        )
        assert receipt["sequence"] == 7
        machine = client.get(f"/v1/agent/accounts/{account}/runners", headers=agent).json()[
            "machines"
        ][0]
        assert machine["available_slots"] == 2 and machine["queue_depth"] == 3
        with engine.begin() as db:
            assert (
                db.execute(
                    text("SELECT sequence FROM local_runners WHERE id=:id"),
                    {"id": enrolled["runner_id"]},
                ).scalar_one()
                == 7
            )
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        sock.close()
        assert not thread.is_alive()
