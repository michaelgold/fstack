"""One explicitly owner-authorized session per user-managed runner.

No scheduler, automatic reassignment, billing, or MCP data-plane proxy. Address
custody is volatile memory: deploy this optional composition with one API process.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import Field
from sqlalchemy import Connection, Engine, text

from fstack.identity import AuthConfiguration, IdentityAuthority, Input, denied


class SessionRequest(Input):
    session_id: UUID = Field(strict=False)
    runner_id: UUID = Field(strict=False)
    workspace_id: UUID = Field(strict=False)
    client_public_key: str = Field(pattern=r"^nodekey:[0-9a-f]{64}$")
    session_token: str = Field(pattern=r"^fs_x_[A-Za-z0-9_-]{43}$", repr=False)
    ttl_seconds: int = Field(ge=5, le=300)
    memory_mib: int = Field(ge=512, le=4096)
    cpu_millis: int = Field(ge=100, le=2000)


class Empty(Input):
    pass


class Generation(Input):
    generation: int = Field(ge=0, le=1)


class Ready(Generation):
    address: str = Field(
        min_length=100, max_length=2048, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]+$", repr=False
    )


class AddressMemory:
    """No database, logs or disk serialization of secret connection capabilities."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: dict[UUID, tuple[datetime, str]] = {}

    def get(self, session: UUID) -> str | None:
        with self._lock:
            now = datetime.now(UTC)
            self._values = {k: v for k, v in self._values.items() if v[0] > now}
            value = self._values.get(session)
            return None if value is None else value[1]

    def put(self, session: UUID, expires: datetime, address: str) -> None:
        self.get(session)
        with self._lock:
            self._values[session] = (expires, address)

    def remove(self, session: UUID) -> None:
        with self._lock:
            self._values.pop(session, None)


def mount_runner_sessions(engine: Engine, config: AuthConfiguration) -> APIRouter:
    router = APIRouter()
    authority = IdentityAuthority(config)
    addresses = AddressMemory()

    @contextmanager
    def transaction() -> Iterator[Connection]:
        with engine.begin() as db:
            db.execute(text("SET LOCAL lock_timeout='2s'"))
            db.execute(text("SET LOCAL statement_timeout='3s'"))
            yield db

    def runner_auth(db: Connection, request: Request, runner: UUID) -> Any:
        header = request.headers.get("authorization", "")
        if not header.startswith("Bearer fs_r_") or len(header) > 256:
            raise denied(401)
        digest = authority.digest(header[7:])
        binding = db.execute(
            text(
                "SELECT account_id,principal_id FROM local_runners WHERE id=:r AND token_digest=:d"
            ),
            {"r": runner, "d": digest},
        ).first()
        if binding is None:
            raise denied(401)
        authority.member(db, binding.principal_id, binding.account_id, [])
        row = (
            db.execute(
                text(
                    "SELECT * FROM local_runners WHERE id=:r AND token_digest=:d "
                    "AND NOT revoked FOR UPDATE"
                ),
                {"r": runner, "d": digest},
            )
            .mappings()
            .first()
        )
        if row is None:
            raise denied()
        authority.live(db, row["expires_at"])
        return row

    def session_row(db: Connection, session: UUID, runner: UUID) -> Any:
        row = (
            db.execute(
                text("SELECT * FROM local_sessions WHERE id=:s AND runner_id=:r FOR UPDATE"),
                {"s": session, "r": runner},
            )
            .mappings()
            .first()
        )
        if row is None:
            raise denied()
        return row

    def stop(db: Connection, session: UUID) -> None:
        found = db.execute(
            text("""UPDATE local_sessions SET state='stopping'
            WHERE id=:s AND state IN ('requested','leased','ready','stopping') RETURNING id"""),
            {"s": session},
        ).scalar_one_or_none()
        if found is None:
            raise HTTPException(409, "session transition denied")
        addresses.remove(session)

    def agent_auth(db: Connection, request: Request, session: UUID) -> Any:
        header = request.headers.get("authorization", "")
        if not header.startswith("Bearer fs_x_") or len(header) > 256:
            raise denied(401)
        binding = (
            db.execute(
                text("SELECT * FROM local_sessions WHERE id=:s AND token_digest=:d"),
                {"s": session, "d": authority.digest(header[7:])},
            )
            .mappings()
            .first()
        )
        if binding is None:
            raise denied()
        authority.member(db, binding["principal_id"], binding["account_id"], [])
        runner = (
            db.execute(
                text("SELECT * FROM local_runners WHERE id=:r FOR SHARE"),
                {"r": binding["runner_id"]},
            )
            .mappings()
            .one()
        )
        if runner["revoked"]:
            raise denied()
        row = session_row(db, session, binding["runner_id"])
        authority.live(db, row["expires_at"], runner["expires_at"])
        return row

    def public(row: Any) -> dict[str, Any]:
        return {
            "session_id": row["id"],
            "state": row["state"],
            "generation": row["generation"],
            "expires_at": row["expires_at"],
        }

    @router.post("/v1/auth/accounts/{account}/sessions")
    def create(account: UUID, body: SessionRequest, request: Request) -> dict[str, Any]:
        if body.client_public_key == "nodekey:" + "0" * 64:
            raise HTTPException(422, "invalid session request")
        digest = authority.digest(body.model_dump_json())
        with transaction() as db:
            principal, expires = authority.human(db, request, mutation=True)
            authority.member(db, principal, account, [])
            runner = (
                db.execute(
                    text(
                        "SELECT * FROM local_runners WHERE id=:r AND account_id=:a "
                        "AND NOT revoked FOR UPDATE"
                    ),
                    {"r": body.runner_id, "a": account},
                )
                .mappings()
                .first()
            )
            if runner is None or runner["principal_id"] != principal:
                raise denied()
            authority.live(db, expires, runner["expires_at"])
            old = (
                db.execute(
                    text("SELECT * FROM local_sessions WHERE id=:s FOR UPDATE"),
                    {"s": body.session_id},
                )
                .mappings()
                .first()
            )
            if old is not None:
                if (
                    old["account_id"] != account
                    or old["principal_id"] != principal
                    or old["request_digest"] != digest
                ):
                    raise HTTPException(409, "session request conflict")
                authority.live(db, expires)
                # Stable admission receipt, not the current session projection.
                return {"session_id": old["id"], "expires_at": old["expires_at"]}
            active = db.execute(
                text("SELECT id FROM local_sessions WHERE runner_id=:r AND state<>'stopped'"),
                {"r": body.runner_id},
            ).first()
            if active is not None:
                raise HTTPException(409, "runner has unresolved session")
            now: datetime = db.execute(text("SELECT clock_timestamp()")).scalar_one()
            until = min(now + timedelta(seconds=body.ttl_seconds), runner["expires_at"])
            db.execute(
                text("""INSERT INTO local_sessions
                (id,account_id,principal_id,runner_id,workspace_id,request_digest,token_digest,
                 client_public_key,image,memory_mib,cpu_millis,created_at,expires_at)
                VALUES (:s,:a,:p,:r,:w,:d,:t,:key,:image,:mem,:cpu,:now,:until) RETURNING id"""),
                {
                    "s": body.session_id,
                    "a": account,
                    "p": principal,
                    "r": body.runner_id,
                    "w": body.workspace_id,
                    "d": digest,
                    "t": authority.digest(body.session_token),
                    "key": body.client_public_key,
                    "image": runner["image"],
                    "mem": body.memory_mib,
                    "cpu": body.cpu_millis,
                    "now": now,
                    "until": until,
                },
            ).scalar_one()
            authority.live(db, expires, until)
        return {"session_id": body.session_id, "expires_at": until}

    @router.post("/v1/runners/{runner}/assignment")
    def assignment(runner: UUID, body: Empty, request: Request) -> dict[str, Any]:
        with transaction() as db:
            enrolled = runner_auth(db, request, runner)
            row = (
                db.execute(
                    text(
                        "SELECT * FROM local_sessions WHERE runner_id=:r "
                        "AND state<>'stopped' FOR UPDATE"
                    ),
                    {"r": runner},
                )
                .mappings()
                .first()
            )
            if row is None:
                return {"assignment": None}
            # Admission binds the execution owner to the enrolling owner;
            # runner_auth already holds and checks that current membership.
            now: datetime = db.execute(text("SELECT clock_timestamp()")).scalar_one()
            launch = row["state"] == "requested" and row["expires_at"] > now
            if row["expires_at"] <= now and row["state"] != "stopping":
                stop(db, row["id"])
            elif row["state"] == "requested":
                db.execute(
                    text("""UPDATE local_sessions SET state='leased',generation=1,
                    allocated_at=clock_timestamp() WHERE id=:s AND state='requested'
                    AND expires_at>clock_timestamp() RETURNING id"""),
                    {"s": row["id"]},
                ).scalar_one()
            row = session_row(db, row["id"], runner)
            result = {
                **public(row),
                "workspace_id": row["workspace_id"],
                "image": row["image"],
                "client_public_key": row["client_public_key"],
                "memory_mib": row["memory_mib"],
                "cpu_millis": row["cpu_millis"],
            }
            authority.live(db, enrolled["expires_at"])
        return {"assignment": result, "launch": launch}

    @router.post("/v1/runners/{runner}/sessions/{session}/ready")
    def ready(runner: UUID, session: UUID, body: Ready, request: Request) -> dict[str, str]:
        with transaction() as db:
            enrolled = runner_auth(db, request, runner)
            row = session_row(db, session, runner)
            authority.live(db, row["expires_at"], enrolled["expires_at"])
            if row["state"] not in ("leased", "ready") or body.generation != row["generation"]:
                raise HTTPException(409, "session transition denied")
            existing = addresses.get(session)
            if existing is not None and existing != body.address:
                raise HTTPException(409, "session address conflict")
            db.execute(
                text("UPDATE local_sessions SET state='ready' WHERE id=:s RETURNING id"),
                {"s": session},
            ).scalar_one()
            authority.live(db, row["expires_at"], enrolled["expires_at"])
            addresses.put(session, row["expires_at"], body.address)
        return {"status": "ready"}

    @router.get("/v1/sessions/{session}")
    def inspect(session: UUID, request: Request) -> dict[str, Any]:
        with transaction() as db:
            row = agent_auth(db, request, session)
            result = {
                **public(row),
                "address": addresses.get(session) if row["state"] == "ready" else None,
            }
            authority.live(db, row["expires_at"])
        return result

    @router.post("/v1/sessions/{session}/stop")
    def cancel(session: UUID, body: Empty, request: Request) -> dict[str, str]:
        with transaction() as db:
            row = agent_auth(db, request, session)
            if row["state"] != "stopped":
                stop(db, session)
            authority.live(db, row["expires_at"])
        return {"status": "stopping" if row["state"] != "stopped" else "stopped"}

    @router.post("/v1/runners/{runner}/sessions/{session}/stopped")
    def stopped(runner: UUID, session: UUID, body: Generation, request: Request) -> dict[str, str]:
        with transaction() as db:
            enrolled = runner_auth(db, request, runner)
            row = session_row(db, session, runner)
            if body.generation != row["generation"]:
                raise HTTPException(409, "session generation conflict")
            if row["state"] != "stopped":
                # Also covers failed startup: cleanup must precede this receipt.
                stop(db, session)
                db.execute(
                    text(
                        "UPDATE local_sessions SET state='stopped',stopped_at=clock_timestamp() "
                        "WHERE id=:s AND state='stopping' RETURNING id"
                    ),
                    {"s": session},
                ).scalar_one()
            addresses.remove(session)
            authority.live(db, enrolled["expires_at"])
        return {"status": "stopped"}

    return router
