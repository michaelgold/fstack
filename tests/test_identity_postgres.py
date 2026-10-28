"""Mounted human-to-agent acceptance; synthetic mail only, real PostgreSQL."""

from urllib.parse import urlsplit
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import text

from fstack.api import create_app


def test_human_approves_agent_and_revokes(migrated_database):
    from fstack.identity import AuthConfiguration, enroll_owner

    account = uuid4()
    with migrated_database.begin() as db:
        db.execute(
            text("INSERT INTO organizations (id, slug, name) VALUES (:id, 'auth', 'Auth')"),
            {"id": account},
        )
    enroll_owner(migrated_database, account, "human@example.test")
    messages = []
    config = AuthConfiguration(
        origin="https://testserver",
        secret="synthetic-" + "x" * 40,
        send_mail=lambda email, link, key: messages.append((email, link, key)),
    )
    app = create_app(auth_configuration=config)
    with (
        TestClient(app, base_url=config.origin) as human,
        TestClient(app, base_url=config.origin) as agent,
    ):
        response = human.post(
            "/v1/auth/login",
            json={"email": "human@example.test"},
            headers={"Origin": config.origin},
        )
        assert response.status_code == 202
        assert len(messages) == 1
        token = urlsplit(messages[0][1]).fragment
        assert token not in response.text
        assert human.get("/auth/confirm").status_code == 200
        confirmation = {"token": token, "csrf": response.json()["csrf"]}
        assert (
            agent.post(
                "/v1/auth/confirm", json=confirmation, headers={"Origin": config.origin}
            ).status_code
            == 403
        )
        assert (
            human.post(
                "/v1/auth/confirm", json=confirmation, headers={"Origin": config.origin}
            ).status_code
            == 200
        )
        session = human.get("/v1/auth/session").json()
        headers = {"Origin": config.origin, "X-CSRF-Token": session["csrf"]}
        request = agent.post(
            "/v1/auth/device/start",
            json={"account_id": str(account), "client": "test-agent", "scopes": ["account:read"]},
        )
        assert request.status_code == 200
        codes = request.json()
        assert (
            agent.post(
                "/v1/auth/device/token", json={"device_code": codes["device_code"]}
            ).status_code
            == 403
        )
        assert (
            agent.post(
                "/v1/auth/device/approve", json={"user_code": codes["user_code"]}
            ).status_code
            == 401
        )
        review = human.get("/v1/auth/device/review", params={"user_code": codes["user_code"]})
        assert review.json()["client"] == "test-agent"
        assert review.json()["scopes"] == ["account:read"]
        approved = human.post(
            "/v1/auth/device/approve", json={"user_code": codes["user_code"]}, headers=headers
        )
        assert approved.status_code == 200
        exchange = agent.post("/v1/auth/device/token", json={"device_code": codes["device_code"]})
        assert exchange.status_code == 200
        credential = exchange.json()
        bearer = {"Authorization": "Bearer " + credential["access_token"]}
        assert human.get(f"/v1/agent/accounts/{account}").status_code == 401
        assert agent.get(f"/v1/agent/accounts/{account}", headers=bearer).status_code == 200
        assert (
            agent.post(
                "/v1/auth/device/token", json={"device_code": codes["device_code"]}
            ).status_code
            == 403
        )
        assert (
            human.post(
                "/v1/auth/grants/" + credential["grant_id"] + "/revoke", headers=headers
            ).status_code
            == 200
        )
        assert agent.get(f"/v1/agent/accounts/{account}", headers=bearer).status_code == 403
