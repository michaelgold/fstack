from uuid import uuid4

import pytest
from sqlalchemy import text
from test_runner_discovery_postgres import discovery, enroll  # noqa: F401


def admission(runner, **changes):
    return {
        "session_id": str(uuid4()),
        "runner_id": runner,
        "workspace_id": str(uuid4()),
        "client_public_key": "nodekey:" + "1" * 64,
        "session_token": "fs_x_" + "s" * 43,
        "ttl_seconds": 60,
        "memory_mib": 2048,
        "cpu_millis": 1000,
        **changes,
    }


def test_owner_delegates_session_not_read_grant_and_runner_pulls(discovery):  # noqa: F811
    engine, client, account, owner, agent = discovery
    enrolled = enroll(client, account, owner)
    body = admission(enrolled["runner_id"])
    path = f"/v1/auth/accounts/{account}/sessions"
    assert client.post(path, headers=agent, json=body).status_code == 401
    created = client.post(path, headers=owner, json=body)
    assert created.status_code == 200, created.text
    assert client.post(path, headers=owner, json=body).json() == created.json()
    assert client.post(path, headers=owner, json={**body, "cpu_millis": 1500}).status_code == 409
    sid = body["session_id"]
    poll = f"/v1/runners/{enrolled['runner_id']}/assignment"
    runner = {"authorization": "Bearer " + enrolled["runner_token"]}
    assigned = client.post(poll, headers=runner, json={}).json()["assignment"]
    assert assigned["session_id"] == sid and assigned["generation"] == 1
    assert assigned["client_public_key"] == body["client_public_key"]
    assert "session_token" not in assigned
    assert client.post(poll, headers=runner, json={}).json()["assignment"] == assigned
    access = {"authorization": "Bearer " + body["session_token"]}
    assert client.get(f"/v1/sessions/{sid}", headers=agent).status_code == 401
    ready = f"/v1/runners/{enrolled['runner_id']}/sessions/{sid}/ready"
    capability = "SyntheticCapability" + "a" * 140
    assert (
        client.post(
            ready, headers=runner, json={"generation": 1, "address": capability}
        ).status_code
        == 200
    )
    response = client.get(f"/v1/sessions/{sid}", headers=access)
    assert response.json()["address"] == capability
    assert response.headers["cache-control"] == "no-store"
    with engine.begin() as db:
        row = db.execute(
            text("SELECT row_to_json(s)::text FROM local_sessions s WHERE id=:id"), {"id": sid}
        ).scalar_one()
        assert capability not in row and body["session_token"] not in row
    assert client.post(f"/v1/sessions/{sid}/stop", headers=access, json={}).status_code == 200
    assert client.get(f"/v1/sessions/{sid}", headers=access).json()["address"] is None
    assert client.post(poll, headers=runner, json={}).json()["assignment"]["state"] == "stopping"
    assert (
        client.post(
            f"/v1/runners/{enrolled['runner_id']}/sessions/{sid}/stopped",
            headers=runner,
            json={"generation": 1},
        ).status_code
        == 200
    )
    assert client.get(f"/v1/sessions/{sid}", headers=access).json()["state"] == "stopped"
    assert client.post(poll, headers=runner, json={}).json()["assignment"] is None


def test_expiry_and_generation_fail_closed(discovery):  # noqa: F811
    import time

    from sqlalchemy.exc import IntegrityError

    engine, client, account, owner, _ = discovery
    enrolled = enroll(client, account, owner)
    body = admission(enrolled["runner_id"], ttl_seconds=5)
    assert (
        client.post(f"/v1/auth/accounts/{account}/sessions", headers=owner, json=body).status_code
        == 200
    )
    runner = {"authorization": "Bearer " + enrolled["runner_token"]}
    poll = f"/v1/runners/{enrolled['runner_id']}/assignment"
    assigned = client.post(poll, headers=runner, json={}).json()
    assert assigned["launch"] is True
    assert client.post(poll, headers=runner, json={}).json()["launch"] is False
    sid = body["session_id"]
    ready = f"/v1/runners/{enrolled['runner_id']}/sessions/{sid}/ready"
    assert (
        client.post(ready, headers=runner, json={"generation": 0, "address": "a" * 154}).status_code
        == 409
    )
    for assignment in (
        "state='ready',generation=0",
        "state='requested',generation=0",
        "expires_at=expires_at+interval '1 second'",
        "memory_mib=4096",
    ):
        with pytest.raises(IntegrityError), engine.begin() as db:
            db.execute(text(f"UPDATE local_sessions SET {assignment} WHERE id=:s"), {"s": sid})
    time.sleep(5.1)
    assert (
        client.post(ready, headers=runner, json={"generation": 1, "address": "a" * 154}).status_code
        == 403
    )
    assert client.post(poll, headers=runner, json={}).json()["assignment"]["state"] == "stopping"
    access = {"authorization": "Bearer " + body["session_token"]}
    assert client.get(f"/v1/sessions/{sid}", headers=access).status_code == 403
    assert (
        client.post(
            f"/v1/runners/{enrolled['runner_id']}/sessions/{sid}/stopped",
            headers=runner,
            json={"generation": 1},
        ).status_code
        == 200
    )


def test_concurrent_claims_one_launch_with_real_lock_wait(discovery):  # noqa: F811
    import time
    from concurrent.futures import ThreadPoolExecutor

    engine, client, account, owner, _ = discovery
    enrolled = enroll(client, account, owner)
    body = admission(enrolled["runner_id"])
    assert (
        client.post(f"/v1/auth/accounts/{account}/sessions", headers=owner, json=body).status_code
        == 200
    )
    auth = {"authorization": "Bearer " + enrolled["runner_token"]}
    path = f"/v1/runners/{enrolled['runner_id']}/assignment"
    with ThreadPoolExecutor(max_workers=2) as pool:
        blocking = engine.connect()
        transaction = blocking.begin()
        try:
            blocking.execute(
                text("SELECT id FROM local_runners WHERE id=:r FOR UPDATE"),
                {"r": enrolled["runner_id"]},
            )
            futures = [pool.submit(client.post, path, headers=auth, json={}) for _ in range(2)]
            deadline = time.monotonic() + 5
            waiters = 0
            while time.monotonic() < deadline:
                with engine.connect() as observer:
                    waiters = observer.execute(
                        text("""SELECT count(*) FROM pg_stat_activity
                        WHERE datname=current_database() AND wait_event_type='Lock'
                            AND query LIKE '%local_runners%'""")
                    ).scalar_one()
                if waiters >= 2:
                    break
                time.sleep(0.02)
            assert waiters >= 2
        finally:
            transaction.rollback()
            blocking.close()
        results = [f.result(timeout=5).json() for f in futures]
    assert sorted(result["launch"] for result in results) == [False, True]
    assert all(result["assignment"]["generation"] == 1 for result in results)
    with engine.connect() as db:
        assert db.execute(text("SELECT count(*) FROM local_sessions")).scalar_one() == 1


def test_contended_runner_poll_is_bounded_and_does_not_lease(discovery):  # noqa: F811
    import time

    engine, client, account, owner, _ = discovery
    enrolled = enroll(client, account, owner)
    body = admission(enrolled["runner_id"])
    created = client.post(f"/v1/auth/accounts/{account}/sessions", headers=owner, json=body)
    assert created.status_code == 200
    with engine.begin() as blocker:
        blocker.execute(
            text("SELECT id FROM local_runners WHERE id=:r FOR UPDATE"),
            {"r": enrolled["runner_id"]},
        )
        start = time.monotonic()
        response = client.post(
            f"/v1/runners/{enrolled['runner_id']}/assignment",
            json={},
            headers={"authorization": "Bearer " + enrolled["runner_token"]},
        )
        elapsed = time.monotonic() - start
        assert response.status_code == 503
        assert 1.5 <= elapsed < 5
    with engine.connect() as db:
        assert (
            db.execute(
                text("SELECT state FROM local_sessions WHERE id=:s"), {"s": body["session_id"]}
            ).scalar_one()
            == "requested"
        )
