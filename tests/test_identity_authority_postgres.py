"""The same current authority is reusable without an HTTP self-call."""

from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import text
from starlette.requests import Request

from fstack import identity


def test_reusable_owner_authority_rechecks_current_state(migrated_database):
    assert hasattr(identity, "IdentityAuthority"), "router hides reusable owner authority"
    config = identity.AuthConfiguration(
        "https://testserver", "synthetic-" + "x" * 40, lambda *_: None
    )
    authority = identity.IdentityAuthority(config)
    account = uuid4()
    with migrated_database.begin() as db:
        db.execute(
            text("INSERT INTO organizations (id,slug,name) VALUES (:a,'owner','Owner')"),
            {"a": account},
        )
    principal = identity.enroll_owner(migrated_database, account, "owner@example.test")
    raw = "synthetic-browser-session"
    request = Request(
        {"type": "http", "headers": [(b"cookie", f"{identity.COOKIE}={raw}".encode())]}
    )
    with migrated_database.begin() as db:
        db.execute(
            text(
                "INSERT INTO auth_sessions(digest,principal_id,expires_at) "
                "VALUES (:d,:p,clock_timestamp()+interval '1 hour')"
            ),
            {"d": authority.digest(raw), "p": principal},
        )
    with migrated_database.begin() as db:
        assert authority.require_owner(db, request, account) == principal
    with migrated_database.begin() as db:
        db.execute(
            text("UPDATE auth_memberships SET active=false WHERE account_id=:a"), {"a": account}
        )
    with migrated_database.begin() as db, pytest.raises(HTTPException) as error:
        authority.require_owner(db, request, account)
    assert error.value.status_code == 403
