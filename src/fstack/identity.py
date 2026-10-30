"""Human sign-in and account-authorized device delegation.

Adapted from Crusher's pending dashboard-session / locked consume pattern;
see docs/plans/2026-10-28-human-agent-auth.md for source/publication boundary.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import HTMLResponse
from itsdangerous import BadSignature, URLSafeTimedSerializer
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Connection, Engine, text

COOKIE = "__Host-fstack-session"
BINDING = "__Host-fstack-login"
AUDIENCE = "fstack-agent"
SCOPES = ["account:read"]


@dataclass(frozen=True)
class AuthConfiguration:
    origin: str
    secret: str = field(repr=False)
    send_mail: Callable[[str, str, str], None] = field(repr=False)

    def __post_init__(self) -> None:
        parsed = urlsplit(self.origin)
        if (
            parsed.scheme != "https"
            or any(ord(c) <= 32 or ord(c) >= 127 for c in self.origin)
            or len(self.origin) > 2048
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.path
            or parsed.query
            or parsed.fragment
            or len(self.secret) < 32
        ):
            raise ValueError("invalid authentication configuration")


def enroll_owner(engine: Engine, account_id: UUID, email: str) -> UUID:
    """Trusted installation bootstrap only; deliberately not an HTTP endpoint.

    Never attach an existing email to another account implicitly. An explicit
    principal membership operation is required for that separate administrative act.
    """
    if not re.fullmatch(r"[^\s@]{1,64}@[^\s@]{1,190}", email):
        raise ValueError("invalid email")
    principal = uuid4()
    with engine.begin() as db:
        db.execute(
            text("INSERT INTO auth_principals (id,email) VALUES (:p,:e)"),
            {"p": principal, "e": email.lower()},
        )
        db.execute(
            text("""INSERT INTO auth_memberships
            (account_id,principal_id,owner,permissions) VALUES (:a,:p,true,:s)"""),
            {"a": account_id, "p": principal, "s": SCOPES},
        )
    return principal


class Input(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Login(Input):
    email: str = Field(min_length=3, max_length=255, pattern=r"^[^\s@]+@[^\s@]+$")


class Confirm(Input):
    token: str = Field(min_length=1, max_length=2048)
    csrf: str = Field(min_length=1, max_length=128)


class Start(Input):
    account_id: UUID = Field(strict=False)
    client: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_. -]+$")
    scopes: list[str] = Field(min_length=1, max_length=1)


class Approve(Input):
    user_code: str = Field(min_length=1, max_length=128)


class Exchange(Input):
    device_code: str = Field(min_length=1, max_length=128)


def denied(code: int = 403) -> HTTPException:
    return HTTPException(code, "authentication or authorization denied")


class IdentityAuthority:
    """Current installation-local authority shared by routes and trusted extensions.

    Call inside a short caller-owned transaction. Never retain the result across
    external I/O: reauthorize the request after that I/O before returning data.
    Locks and a fresh clock read are deliberately part of the shared contract.
    """

    def __init__(self, config: AuthConfiguration):
        self.config = config

    def digest(self, value: str) -> str:
        return hmac.new(self.config.secret.encode(), value.encode(), hashlib.sha256).hexdigest()

    def csrf(self, value: str) -> str:
        return self.digest("csrf:" + value)

    def origin(self, request: Request) -> None:
        if request.headers.get("origin") != self.config.origin:
            raise denied()

    def live(self, db: Connection, *expiries: datetime, code: int = 403) -> None:
        # Read AFTER all authority locks; statement/transaction start is stale after waits.
        now: datetime = db.execute(text("SELECT clock_timestamp()")).scalar_one()
        if any(expiry <= now for expiry in expiries):
            raise denied(code)

    def human(
        self, db: Connection, request: Request, mutation: bool = False
    ) -> tuple[UUID, datetime]:
        raw = request.cookies.get(COOKIE, "")
        if not raw or request.headers.get("authorization"):
            raise denied(401)
        if mutation:
            self.origin(request)
            if not hmac.compare_digest(request.headers.get("x-csrf-token", ""), self.csrf(raw)):
                raise denied()
        row = db.execute(
            text("""SELECT s.principal_id, s.expires_at FROM auth_sessions s
            JOIN auth_principals p ON p.id=s.principal_id
            WHERE s.digest=:d AND NOT s.revoked AND p.active
              AND s.expires_at > clock_timestamp() FOR SHARE OF s,p"""),
            {"d": self.digest(raw)},
        ).first()
        if row is None:
            raise denied(401)
        self.live(db, row[1], code=401)
        return UUID(str(row[0])), row[1]

    def member(self, db: Connection, principal: UUID, account: UUID, scopes: list[str]) -> None:
        row = db.execute(
            text("""SELECT m.permissions FROM auth_memberships m
            JOIN auth_principals p ON p.id=m.principal_id
            WHERE m.account_id=:a AND m.principal_id=:p AND m.active AND m.owner AND p.active
            FOR SHARE OF m,p"""),
            {"a": account, "p": principal},
        ).first()
        if row is None or not set(scopes).issubset(row[0]):
            raise denied()

    def require_owner(self, db: Connection, request: Request, account: UUID) -> UUID:
        """Human current owner only; account:read agent grants confer no authority here."""
        principal, expires = self.human(db, request)
        self.member(db, principal, account, [])
        self.live(db, expires, code=401)
        return principal


def mount_identity(engine: Engine, config: AuthConfiguration) -> APIRouter:
    router = APIRouter()
    authority = IdentityAuthority(config)
    signer = URLSafeTimedSerializer(config.secret, salt="fstack-human-login-v1")

    digest = authority.digest

    csrf = authority.csrf

    origin = authority.origin

    def cookie(response: Response, name: str, value: str, age: int) -> None:
        response.set_cookie(
            name, value, max_age=age, secure=True, httponly=True, samesite="strict", path="/"
        )

    def rate(key: str, limit: int) -> None:
        # Fixed key inventory prevents attacker-controlled unbounded rate-table growth.
        with engine.begin() as db:
            count: int = db.execute(
                text("""INSERT INTO auth_rate_buckets VALUES
                (:key, date_trunc('minute', clock_timestamp()), 1)
                ON CONFLICT (key) DO UPDATE SET
                  count = CASE WHEN auth_rate_buckets.window_start = EXCLUDED.window_start
                    THEN auth_rate_buckets.count + 1 ELSE 1 END,
                  window_start = EXCLUDED.window_start RETURNING count"""),
                {"key": key},
            ).scalar_one()
        if count > limit:
            raise HTTPException(429, "try later")

    live = authority.live

    human = authority.human

    member = authority.member

    @router.get("/auth/sign-in", response_class=HTMLResponse)
    def signin_page() -> str:
        return """<!doctype html><title>Sign in</title>
        <label>Email <input id="email" type="email" maxlength="255"></label>
        <button id="send">Send sign-in email</button><p id="status"></p>
        <script src="/auth/sign-in.js"></script>"""

    @router.get("/auth/sign-in.js")
    def signin_script() -> Response:
        return Response(
            """document.getElementById('send').onclick = async () => {
          const result = await fetch('/v1/auth/login', {method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({email:document.getElementById('email').value})});
          document.getElementById('status').textContent = result.ok ?
            'Check your email. Open the link in this browser.' : 'Please try later.';
        };""",
            media_type="application/javascript",
        )

    @router.get("/auth/device", response_class=HTMLResponse)
    def device_page() -> str:
        return """<!doctype html><title>Approve an agent</title>
        <p>Sign in first. Approve only an agent you started.</p>
        <label>User code <input id="code" maxlength="128"></label>
        <button id="review">Review request</button><pre id="details"></pre>
        <button id="approve" disabled>Approve exact request</button>
        <script src="/auth/device.js"></script>"""

    @router.get("/auth/device.js")
    def device_script() -> Response:
        return Response(
            """let reviewedCode = null;
        const details = document.getElementById('details');
        const approve = document.getElementById('approve');
        document.getElementById('code').oninput = () => {
          reviewedCode = null; approve.disabled = true;
        };
        document.getElementById('review').onclick = async () => {
          approve.disabled = true; reviewedCode = null;
          const code = document.getElementById('code').value;
          const result = await fetch('/v1/auth/device/review?user_code='+encodeURIComponent(code));
          if (!result.ok) { details.textContent = 'Sign in or request a new code.'; return; }
          details.textContent = JSON.stringify(await result.json(), null, 2);
          reviewedCode = code; approve.disabled = false;
        };
        approve.onclick = async () => {
          if (!reviewedCode) return; approve.disabled = true;
          const session = await fetch('/v1/auth/session').then(r => r.json());
          const result = await fetch('/v1/auth/device/approve', {method:'POST',
            headers:{'Content-Type':'application/json','X-CSRF-Token':session.csrf},
            body:JSON.stringify({user_code:reviewedCode})});
          reviewedCode = null;
          details.textContent = result.ok ? 'Agent approved.' : 'Approval denied.';
        };""",
            media_type="application/javascript",
        )

    @router.get("/auth/confirm", response_class=HTMLResponse)
    def landing() -> str:
        # Fragment is never sent in HTTP URLs or Referer. No auto-confirm/script fetch.
        return """<!doctype html><title>Confirm sign-in</title>
        <p>Confirm sign-in in the browser that requested the email.</p>
        <button id="confirm">Confirm sign-in</button>
        <script src="/auth/confirm.js"></script>"""

    @router.get("/auth/confirm.js")
    def script() -> Response:
        return Response(
            """document.getElementById('confirm').onclick = async () => {
          const token = location.hash.slice(1); history.replaceState(null, '', '/auth/confirm');
          const state = await fetch('/v1/auth/login-binding').then(r => r.json());
          const result = await fetch('/v1/auth/confirm', {method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({token:token, csrf:state.csrf})});
          document.body.textContent = result.ok ? 'Signed in.' : 'Sign-in failed.';
        };""",
            media_type="application/javascript",
        )

    @router.get("/v1/auth/login-binding")
    def binding(request: Request) -> dict[str, str]:
        value = request.cookies.get(BINDING, "")
        if not value:
            raise denied()
        return {"csrf": csrf(value)}

    @router.post("/v1/auth/login", status_code=202)
    def login(data: Login, request: Request, response: Response) -> dict[str, str]:
        origin(request)
        rate("login", 30)
        raw_binding = secrets.token_urlsafe(32)
        raw = secrets.token_urlsafe(32)
        cookie(response, BINDING, raw_binding, 900)
        with engine.begin() as db:
            principal = db.execute(
                text("SELECT id FROM auth_principals WHERE email=:e AND active"),
                {"e": data.email.lower()},
            ).scalar_one_or_none()
            if principal is not None:
                db.execute(
                    text("""INSERT INTO auth_logins
                    (digest,principal_id,binding,expires_at)
                    VALUES (:d,:p,:b,clock_timestamp()+interval '15 minutes')"""),
                    {"d": digest(raw), "p": principal, "b": digest(raw_binding)},
                )
        if principal is not None:
            try:
                config.send_mail(
                    data.email.lower(),
                    config.origin + "/auth/confirm#" + signer.dumps(raw),
                    str(uuid4()),
                )
            except Exception:
                # Never expose provider text, signed URL, or delivery/account existence.
                with engine.begin() as db:
                    db.execute(text("DELETE FROM auth_logins WHERE digest=:d"), {"d": digest(raw)})
        return {"status": "check_email", "csrf": csrf(raw_binding)}

    @router.post("/v1/auth/confirm")
    def confirm(data: Confirm, request: Request, response: Response) -> dict[str, str]:
        origin(request)
        rate("confirm", 120)
        raw_binding = request.cookies.get(BINDING, "")
        if not raw_binding or not hmac.compare_digest(data.csrf, csrf(raw_binding)):
            raise denied()
        try:
            raw = signer.loads(data.token, max_age=900)
        except BadSignature:
            raise denied() from None
        if not isinstance(raw, str):
            raise denied()
        session = "fs_h_" + secrets.token_urlsafe(32)
        with engine.begin() as db:
            row = db.execute(
                text("""SELECT principal_id, expires_at FROM auth_logins
                WHERE digest=:d AND binding=:b AND NOT consumed
                AND expires_at>clock_timestamp() FOR UPDATE"""),
                {"d": digest(raw), "b": digest(raw_binding)},
            ).first()
            if row is None:
                raise denied()
            active: bool = db.execute(
                text("SELECT active FROM auth_principals WHERE id=:p FOR SHARE"), {"p": row[0]}
            ).scalar_one()
            if not active:
                raise denied()
            live(db, row[1])
            db.execute(
                text("UPDATE auth_logins SET consumed=true WHERE digest=:d"), {"d": digest(raw)}
            )
            db.execute(
                text("""INSERT INTO auth_sessions (digest,principal_id,expires_at)
                VALUES (:d,:p,clock_timestamp()+interval '14 days')"""),
                {"d": digest(session), "p": row[0]},
            )
            live(db, row[1])
        cookie(response, COOKIE, session, 14 * 86400)
        response.delete_cookie(BINDING, secure=True, httponly=True, samesite="strict")
        return {"status": "signed_in"}

    @router.get("/v1/auth/session")
    def session(request: Request) -> dict[str, str]:
        with engine.begin() as db:
            principal, _ = human(db, request)
        return {"principal_id": str(principal), "csrf": csrf(request.cookies[COOKIE])}

    @router.post("/v1/auth/logout")
    def logout(request: Request, response: Response) -> dict[str, str]:
        with engine.begin() as db:
            _, expires = human(db, request, True)
            db.execute(
                text("UPDATE auth_sessions SET revoked=true WHERE digest=:d"),
                {"d": digest(request.cookies[COOKIE])},
            )
            # The SHARE-to-UPDATE upgrade can itself wait. Denial rolls back.
            live(db, expires, code=401)
        response.delete_cookie(COOKIE, secure=True, httponly=True, samesite="strict")
        return {"status": "signed_out"}

    @router.post("/v1/auth/device/start")
    def start(data: Start) -> dict[str, Any]:
        rate("start", 60)
        if data.scopes != SCOPES:
            raise denied()
        raw, user = secrets.token_urlsafe(32), secrets.token_urlsafe(16)
        with engine.begin() as db:
            exists = db.execute(
                text("SELECT id FROM organizations WHERE id=:a"), {"a": data.account_id}
            ).first()
            if exists is None:
                raise denied()
            db.execute(
                text("""INSERT INTO auth_devices
                (digest,user_digest,account_id,client,scopes,expires_at)
                VALUES (:d,:u,:a,:c,:s,clock_timestamp()+interval '10 minutes')"""),
                {
                    "d": digest(raw),
                    "u": digest(user),
                    "a": data.account_id,
                    "c": data.client,
                    "s": data.scopes,
                },
            )
        return {
            "device_code": raw,
            "user_code": user,
            "expires_in": 600,
            "interval": 5,
            "verification_uri": config.origin + "/auth/device",
        }

    def device(db: Connection, code: str) -> Any:
        row = (
            db.execute(
                text("""SELECT * FROM auth_devices WHERE user_digest=:u
            AND state='pending' AND expires_at>clock_timestamp() FOR UPDATE"""),
                {"u": digest(code)},
            )
            .mappings()
            .first()
        )
        if row is None:
            raise denied()
        return row

    @router.get("/v1/auth/device/review")
    def review(request: Request, user_code: str = "") -> dict[str, Any]:
        rate("review", 120)
        if len(user_code) > 128:
            raise denied()
        with engine.begin() as db:
            principal, expires = human(db, request)
            row = device(db, user_code)
            member(db, principal, row["account_id"], row["scopes"])
            live(db, expires, row["expires_at"])
            return {
                "account_id": str(row["account_id"]),
                "client": row["client"],
                "scopes": row["scopes"],
                "expires_at": row["expires_at"].isoformat(),
                "credential_expires_in": 3600,
                "audience": AUDIENCE,
            }

    @router.post("/v1/auth/device/approve")
    def approve(data: Approve, request: Request) -> dict[str, str]:
        rate("approve", 120)
        with engine.begin() as db:
            principal, expires = human(db, request, True)
            row = device(db, data.user_code)
            member(db, principal, row["account_id"], row["scopes"])
            live(db, expires, row["expires_at"])
            db.execute(
                text("UPDATE auth_devices SET state='approved',principal_id=:p WHERE digest=:d"),
                {"p": principal, "d": row["digest"]},
            )
            live(db, expires, row["expires_at"])
        return {"status": "approved"}

    @router.post("/v1/auth/device/token")
    def exchange(data: Exchange) -> dict[str, Any]:
        rate("exchange", 240)
        raw, grant = "fs_a_" + secrets.token_urlsafe(32), uuid4()
        with engine.begin() as db:
            row = (
                db.execute(
                    text("""SELECT * FROM auth_devices WHERE digest=:d
                AND state='approved' AND expires_at>clock_timestamp() FOR UPDATE"""),
                    {"d": digest(data.device_code)},
                )
                .mappings()
                .first()
            )
            if row is None:
                raise denied()
            member(db, row["principal_id"], row["account_id"], row["scopes"])
            live(db, row["expires_at"])
            db.execute(
                text("""INSERT INTO auth_grants
                (id,digest,account_id,principal_id,client,scopes,audience,expires_at)
                VALUES (:id,:d,:a,:p,:c,:s,:aud,clock_timestamp()+interval '1 hour')"""),
                {
                    "id": grant,
                    "d": digest(raw),
                    "a": row["account_id"],
                    "p": row["principal_id"],
                    "c": row["client"],
                    "s": row["scopes"],
                    "aud": AUDIENCE,
                },
            )
            db.execute(
                text("UPDATE auth_devices SET state='consumed' WHERE digest=:d"),
                {"d": row["digest"]},
            )
            live(db, row["expires_at"])
        return {
            "access_token": raw,
            "token_type": "Bearer",
            "expires_in": 3600,
            "grant_id": str(grant),
            "audience": AUDIENCE,
            "scopes": row["scopes"],
        }

    @router.get("/v1/agent/accounts/{account}")
    def account_read(account: UUID, request: Request) -> dict[str, str]:
        header = request.headers.get("authorization", "")
        if not header.startswith("Bearer fs_a_") or len(header) > 256:
            raise denied(401)
        with engine.begin() as db:
            row = (
                db.execute(
                    text("""SELECT * FROM auth_grants WHERE digest=:d AND account_id=:a
                AND audience=:aud AND NOT revoked AND expires_at>clock_timestamp() FOR SHARE"""),
                    {"d": digest(header[7:]), "a": account, "aud": AUDIENCE},
                )
                .mappings()
                .first()
            )
            if row is None or "account:read" not in row["scopes"]:
                raise denied()
            member(db, row["principal_id"], account, row["scopes"])
            name: str = db.execute(
                text("SELECT name FROM organizations WHERE id=:a"), {"a": account}
            ).scalar_one()
            live(db, row["expires_at"])
        return {"account_id": str(account), "name": name}

    @router.post("/v1/auth/grants/{grant}/revoke")
    def revoke(grant: UUID, request: Request) -> dict[str, str]:
        with engine.begin() as db:
            principal, expires = human(db, request, True)
            row = (
                db.execute(text("SELECT * FROM auth_grants WHERE id=:id FOR UPDATE"), {"id": grant})
                .mappings()
                .first()
            )
            if row is None:
                raise denied()
            member(db, principal, row["account_id"], [])
            live(db, expires, code=401)
            db.execute(text("UPDATE auth_grants SET revoked=true WHERE id=:id"), {"id": grant})
            live(db, expires, code=401)
        return {"status": "revoked"}

    return router
