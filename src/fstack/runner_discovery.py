"""Optional user-managed machine discovery. No session or spending authority."""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request
from pydantic import Field
from sqlalchemy import Engine, text

from fstack.identity import AuthConfiguration, IdentityAuthority, Input, denied


class Enrollment(Input):
    machine_class: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,63}$")
    image: str = Field(max_length=512, pattern=r"^[a-z0-9./:_-]+@sha256:[0-9a-f]{64}$")
    ttl_seconds: int = Field(ge=60, le=86400)


class Report(Input):
    sequence: int = Field(ge=1, le=9223372036854775807)
    available_slots: int = Field(ge=0, le=64)
    queue_depth: int = Field(ge=0, le=1000000)


def mount_runner_discovery(engine: Engine, config: AuthConfiguration) -> APIRouter:
    router = APIRouter()
    authority = IdentityAuthority(config)

    @router.post("/v1/auth/accounts/{account}/runners")
    def enroll(account: UUID, body: Enrollment, request: Request) -> dict[str, Any]:
        with engine.begin() as db:
            principal, expires = authority.human(db, request, mutation=True)
            authority.member(db, principal, account, [])
            authority.live(db, expires, code=401)
            runner, token = uuid4(), "fs_r_" + secrets.token_urlsafe(32)
            row = db.execute(
                text("""
                WITH boundary AS (SELECT clock_timestamp() AS now)
                INSERT INTO local_runners
                    (id,account_id,principal_id,token_digest,machine_class,image,created_at,expires_at)
                SELECT :id,:a,:p,:d,:m,:i,now,now+make_interval(secs=>:ttl) FROM boundary
                RETURNING expires_at
            """),
                {
                    "id": runner,
                    "a": account,
                    "p": principal,
                    "d": authority.digest(token),
                    "m": body.machine_class,
                    "i": body.image,
                    "ttl": body.ttl_seconds,
                },
            ).one()
            authority.live(db, expires, code=401)
        return {"runner_id": runner, "runner_token": token, "expires_at": row[0]}

    @router.post("/v1/auth/accounts/{account}/runners/{runner}/revoke")
    def revoke(account: UUID, runner: UUID, request: Request) -> dict[str, str]:
        with engine.begin() as db:
            principal, expires = authority.human(db, request, mutation=True)
            authority.member(db, principal, account, [])
            found = db.execute(
                text("""UPDATE local_runners SET revoked=true
                WHERE id=:id AND account_id=:a RETURNING id"""),
                {"id": runner, "a": account},
            ).scalar_one_or_none()
            if found is None:
                raise denied()
            authority.live(db, expires, code=401)
        return {"status": "revoked"}

    @router.post("/v1/runners/{runner}/report")
    def report(runner: UUID, body: Report, request: Request) -> dict[str, Any]:
        header = request.headers.get("authorization", "")
        if not header.startswith("Bearer fs_r_") or len(header) > 256:
            raise denied(401)
        digest = authority.digest(header[7:])
        with engine.begin() as db:
            # Consistent lock order: membership/principal before runner. The first
            # read only discovers immutable membership; authority is checked below.
            binding = db.execute(
                text("""SELECT account_id,principal_id FROM local_runners
                WHERE id=:id AND token_digest=:d"""),
                {"id": runner, "d": digest},
            ).first()
            if binding is None:
                raise denied(401)
            authority.member(db, binding.principal_id, binding.account_id, [])
            row = (
                db.execute(
                    text("""SELECT * FROM local_runners
                WHERE id=:id AND token_digest=:d AND NOT revoked FOR UPDATE"""),
                    {"id": runner, "d": digest},
                )
                .mappings()
                .first()
            )
            if row is None:
                raise denied()
            authority.live(db, row["expires_at"])
            if body.sequence <= row["sequence"]:
                if (body.sequence, body.available_slots, body.queue_depth) != (
                    row["sequence"],
                    row["available_slots"],
                    row["queue_depth"],
                ):
                    raise HTTPException(409, "report sequence conflict")
                seen = row["last_seen"]  # Replay never refreshes observation age.
            else:
                seen = db.execute(
                    text("""UPDATE local_runners SET sequence=:s,
                    available_slots=:slots,queue_depth=:queue,last_seen=clock_timestamp()
                    WHERE id=:id AND sequence=:old AND NOT revoked
                        AND expires_at>clock_timestamp() RETURNING last_seen"""),
                    {
                        "s": body.sequence,
                        "slots": body.available_slots,
                        "queue": body.queue_depth,
                        "id": runner,
                        "old": row["sequence"],
                    },
                ).scalar_one()
            authority.live(db, row["expires_at"])
        return {"runner_id": runner, "sequence": body.sequence, "observed_at": seen}

    @router.get("/v1/agent/accounts/{account}/runners")
    def discover(account: UUID, request: Request) -> dict[str, Any]:
        with engine.begin() as db:
            expires = authority.require_agent_read(db, request, account)
            rows = (
                db.execute(
                    text("""SELECT * FROM local_runners
                WHERE account_id=:a AND NOT revoked AND expires_at>clock_timestamp()
                ORDER BY id FOR SHARE"""),
                    {"a": account},
                )
                .mappings()
                .all()
            )
            now: datetime = db.execute(text("SELECT clock_timestamp()")).scalar_one()
            machines = []
            for row in rows:
                if row["expires_at"] <= now:
                    continue
                seen = row["last_seen"]
                freshness = (
                    "unknown"
                    if seen is None
                    else "fresh"
                    if now - seen <= timedelta(seconds=30)
                    else "stale"
                )
                machines.append(
                    {
                        "runner_id": row["id"],
                        "machine_class": row["machine_class"],
                        "image": row["image"],
                        "capabilities": ["availability-report-v1"],
                        "last_seen": seen,
                        "freshness": freshness,
                        "capacity_guaranteed": False,
                        "available_slots": row["available_slots"] if freshness == "fresh" else None,
                        "queue_depth": row["queue_depth"] if freshness == "fresh" else None,
                        "estimate": {
                            "quote_id": uuid4(),
                            "expires_at": min(
                                now + timedelta(seconds=30), row["expires_at"], expires
                            ),
                            "currency": "USD",
                            "unit": "resource-second",
                            "vendor_rate": "0",
                            "rate_observed_at": row["created_at"],
                            "rate_source": "user-managed-no-vendor-compute-charge",
                            "duration_seconds": None,
                            "duration_confidence": "unknown",
                            "cold_start_seconds": None,
                            "transfer_seconds": None,
                            "queue_seconds": None,
                            "job_seconds": None,
                            "assembly_seconds": None,
                            "accrued_estimated_cost": None,
                            "settled_cost": None,
                            "total_ownership_cost": None,
                            "spending_authority": False,
                            "scope": "vendor-compute-only; excludes electricity and hardware",
                        },
                    }
                )
            authority.live(db, expires)
        return {"account_id": account, "observed_at": now, "machines": machines}

    return router
