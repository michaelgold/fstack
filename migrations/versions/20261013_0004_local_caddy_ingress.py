"""Add immutable local Caddy ingress submissions.

Revision ID: 20261013_0004
Revises: 20261006_0003
Create Date: 2026-10-13
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20261013_0004"
down_revision: str | None = "20261006_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
WORKFLOW_TYPE_V1 = "fstack.provision_crusher.v1"
WORKFLOW_TYPE_V2 = "fstack.provision_crusher.v2"
UPGRADE_LOCK_SQL = (
    "LOCK TABLE organizations, deployments, provisioning_operations, runtime_submissions, "
    "workflow_dispatches, audit_events IN ACCESS EXCLUSIVE MODE NOWAIT"
)
DOWNGRADE_LOCK_SQL = (
    "LOCK TABLE organizations, deployments, provisioning_operations, runtime_submissions, "
    "workflow_dispatches, audit_events, ingress_submissions "
    "IN ACCESS EXCLUSIVE MODE NOWAIT"
)

_CANONICAL_UUID = "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"


def _replace_audit_contract_v2() -> None:
    op.execute("DROP TRIGGER tr_audit_events_history_guard ON audit_events")
    op.execute("DROP FUNCTION guard_provisioning_audit_history()")
    op.drop_constraint("ck_audit_provisioning_lifecycle_payload", "audit_events", type_="check")
    op.create_check_constraint(
        "ck_audit_provisioning_lifecycle_payload",
        "audit_events",
        f"""
        (
            jsonb_typeof(payload) = 'object'
            AND (
                (event_type = 'provisioning.requested' AND payload = '{{}}'::jsonb)
                OR
                (event_type = 'provisioning.started'
                 AND payload ?& ARRAY['runtime_submission_id', 'provider']
                 AND payload - ARRAY['runtime_submission_id', 'provider'] = '{{}}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string'
                 AND payload ->> 'runtime_submission_id' ~ '{_CANONICAL_UUID}'
                 AND payload ->> 'provider' = 'local_docker')
                OR
                (event_type = 'provisioning.started'
                 AND payload ?& ARRAY['runtime_submission_id', 'ingress_submission_id']
                 AND payload - ARRAY[
                     'runtime_submission_id', 'ingress_submission_id'
                 ] = '{{}}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string'
                 AND jsonb_typeof(payload -> 'ingress_submission_id') = 'string'
                 AND payload ->> 'runtime_submission_id' ~ '{_CANONICAL_UUID}'
                 AND payload ->> 'ingress_submission_id' ~ '{_CANONICAL_UUID}')
                OR
                (event_type = 'provisioning.succeeded'
                 AND payload ? 'runtime_submission_id'
                 AND payload - 'runtime_submission_id' = '{{}}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string'
                 AND payload ->> 'runtime_submission_id' ~ '{_CANONICAL_UUID}')
                OR
                (event_type = 'provisioning.succeeded'
                 AND payload ?& ARRAY['runtime_submission_id', 'ingress_submission_id']
                 AND payload - ARRAY[
                     'runtime_submission_id', 'ingress_submission_id'
                 ] = '{{}}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string'
                 AND jsonb_typeof(payload -> 'ingress_submission_id') = 'string'
                 AND payload ->> 'runtime_submission_id' ~ '{_CANONICAL_UUID}'
                 AND payload ->> 'ingress_submission_id' ~ '{_CANONICAL_UUID}')
                OR
                (event_type = 'provisioning.failed'
                 AND payload ?& ARRAY['runtime_submission_id', 'error_code']
                 AND payload - ARRAY['runtime_submission_id', 'error_code'] = '{{}}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string'
                 AND payload ->> 'runtime_submission_id' ~ '{_CANONICAL_UUID}'
                 AND jsonb_typeof(payload -> 'error_code') = 'string'
                 AND payload ->> 'error_code' ~ '^[a-z][a-z0-9_]{{0,63}}$')
                OR
                (event_type = 'provisioning.failed'
                 AND payload ?& ARRAY[
                     'runtime_submission_id', 'ingress_submission_id', 'error_code'
                 ]
                 AND payload - ARRAY[
                     'runtime_submission_id', 'ingress_submission_id', 'error_code'
                 ] = '{{}}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string'
                 AND jsonb_typeof(payload -> 'ingress_submission_id') = 'string'
                 AND payload ->> 'runtime_submission_id' ~ '{_CANONICAL_UUID}'
                 AND payload ->> 'ingress_submission_id' ~ '{_CANONICAL_UUID}'
                 AND jsonb_typeof(payload -> 'error_code') = 'string'
                 AND payload ->> 'error_code' ~ '^[a-z][a-z0-9_]{{0,63}}$')
            )
        ) IS TRUE
        """,
    )
    op.execute(
        f"""
        CREATE FUNCTION guard_provisioning_audit_history() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            runtime_id uuid;
            ingress_id uuid;
            persisted_workflow_type text;
            workflow_payload_allowed boolean;
        BEGIN
            IF TG_OP IN ('UPDATE', 'DELETE') THEN
                RAISE EXCEPTION 'provisioning audit history is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF NEW.event_type = 'provisioning.requested' THEN
                RETURN NEW;
            END IF;
            SELECT workflow_type
            INTO persisted_workflow_type
            FROM workflow_dispatches
            WHERE operation_id = NEW.operation_id;
            IF persisted_workflow_type IS NULL
               OR persisted_workflow_type NOT IN ('{WORKFLOW_TYPE_V1}', '{WORKFLOW_TYPE_V2}') THEN
                RAISE EXCEPTION 'audit event workflow dispatch is missing or unsupported'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF persisted_workflow_type = '{WORKFLOW_TYPE_V1}' THEN
                workflow_payload_allowed := (
                    (NEW.event_type = 'provisioning.started'
                     AND NEW.payload ?& ARRAY['runtime_submission_id', 'provider']
                     AND NEW.payload - ARRAY['runtime_submission_id', 'provider'] = '{{}}'::jsonb
                     AND jsonb_typeof(NEW.payload -> 'runtime_submission_id') = 'string'
                     AND NEW.payload ->> 'provider' = 'local_docker')
                    OR
                    (NEW.event_type = 'provisioning.succeeded'
                     AND NEW.payload ? 'runtime_submission_id'
                     AND NEW.payload - 'runtime_submission_id' = '{{}}'::jsonb
                     AND jsonb_typeof(NEW.payload -> 'runtime_submission_id') = 'string')
                    OR
                    (NEW.event_type = 'provisioning.failed'
                     AND NEW.payload ?& ARRAY['runtime_submission_id', 'error_code']
                     AND NEW.payload - ARRAY[
                         'runtime_submission_id', 'error_code'
                     ] = '{{}}'::jsonb
                     AND jsonb_typeof(NEW.payload -> 'runtime_submission_id') = 'string'
                     AND jsonb_typeof(NEW.payload -> 'error_code') = 'string')
                );
            ELSE
                workflow_payload_allowed := (
                    (NEW.event_type = 'provisioning.started'
                     AND NEW.payload ?& ARRAY[
                         'runtime_submission_id', 'ingress_submission_id'
                     ]
                     AND NEW.payload - ARRAY[
                         'runtime_submission_id', 'ingress_submission_id'
                     ] = '{{}}'::jsonb
                     AND jsonb_typeof(NEW.payload -> 'runtime_submission_id') = 'string'
                     AND jsonb_typeof(NEW.payload -> 'ingress_submission_id') = 'string')
                    OR
                    (NEW.event_type = 'provisioning.succeeded'
                     AND NEW.payload ?& ARRAY[
                         'runtime_submission_id', 'ingress_submission_id'
                     ]
                     AND NEW.payload - ARRAY[
                         'runtime_submission_id', 'ingress_submission_id'
                     ] = '{{}}'::jsonb
                     AND jsonb_typeof(NEW.payload -> 'runtime_submission_id') = 'string'
                     AND jsonb_typeof(NEW.payload -> 'ingress_submission_id') = 'string')
                    OR
                    (NEW.event_type = 'provisioning.failed'
                     AND NEW.payload ?& ARRAY[
                         'runtime_submission_id', 'ingress_submission_id', 'error_code'
                     ]
                     AND NEW.payload - ARRAY[
                         'runtime_submission_id', 'ingress_submission_id', 'error_code'
                     ] = '{{}}'::jsonb
                     AND jsonb_typeof(NEW.payload -> 'runtime_submission_id') = 'string'
                     AND jsonb_typeof(NEW.payload -> 'ingress_submission_id') = 'string'
                     AND jsonb_typeof(NEW.payload -> 'error_code') = 'string')
                );
            END IF;
            IF workflow_payload_allowed IS NOT TRUE THEN
                RAISE EXCEPTION 'workflow type does not permit audit payload'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF NEW.payload ->> 'runtime_submission_id' !~ '{_CANONICAL_UUID}' THEN
                RAISE EXCEPTION 'audit event requires a canonical runtime submission ID'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            runtime_id := (NEW.payload ->> 'runtime_submission_id')::uuid;
            IF NOT EXISTS (
                SELECT 1 FROM runtime_submissions
                WHERE id = runtime_id
                  AND organization_id = NEW.organization_id
                  AND operation_id = NEW.operation_id
            ) THEN
                RAISE EXCEPTION 'audit event runtime submission does not match operation'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF NEW.payload ? 'ingress_submission_id' THEN
                IF NEW.payload ->> 'ingress_submission_id' !~ '{_CANONICAL_UUID}' THEN
                    RAISE EXCEPTION 'audit event requires a canonical ingress submission ID'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
                ingress_id := (NEW.payload ->> 'ingress_submission_id')::uuid;
                IF NOT EXISTS (
                    SELECT 1 FROM ingress_submissions
                    WHERE id = ingress_id
                      AND organization_id = NEW.organization_id
                      AND operation_id = NEW.operation_id
                ) THEN
                    RAISE EXCEPTION 'audit event ingress submission does not match operation'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER tr_audit_events_history_guard
        BEFORE INSERT OR UPDATE OR DELETE ON audit_events
        FOR EACH ROW EXECUTE FUNCTION guard_provisioning_audit_history()
        """
    )


def _restore_audit_contract_v1() -> None:
    op.execute("DROP TRIGGER tr_audit_events_history_guard ON audit_events")
    op.execute("DROP FUNCTION guard_provisioning_audit_history()")
    op.drop_constraint("ck_audit_provisioning_lifecycle_payload", "audit_events", type_="check")
    op.create_check_constraint(
        "ck_audit_provisioning_lifecycle_payload",
        "audit_events",
        f"""
        (
            jsonb_typeof(payload) = 'object'
            AND (
                (event_type = 'provisioning.requested' AND payload = '{{}}'::jsonb)
                OR
                (event_type = 'provisioning.started'
                 AND payload ?& ARRAY['runtime_submission_id', 'provider']
                 AND payload - ARRAY['runtime_submission_id', 'provider'] = '{{}}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string'
                 AND payload ->> 'runtime_submission_id' ~ '{_CANONICAL_UUID}'
                 AND payload ->> 'provider' = 'local_docker')
                OR
                (event_type = 'provisioning.succeeded'
                 AND payload ? 'runtime_submission_id'
                 AND payload - 'runtime_submission_id' = '{{}}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string')
                OR
                (event_type = 'provisioning.failed'
                 AND payload ?& ARRAY['runtime_submission_id', 'error_code']
                 AND payload - ARRAY['runtime_submission_id', 'error_code'] = '{{}}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string'
                 AND jsonb_typeof(payload -> 'error_code') = 'string'
                 AND payload ->> 'error_code' ~ '^[a-z][a-z0-9_]{{0,63}}$')
            )
        ) IS TRUE
        """,
    )
    op.execute(
        f"""
        CREATE FUNCTION guard_provisioning_audit_history() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            submission_id uuid;
        BEGIN
            IF TG_OP IN ('UPDATE', 'DELETE') THEN
                RAISE EXCEPTION 'provisioning audit history is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF NEW.event_type = 'provisioning.requested' THEN
                RETURN NEW;
            END IF;
            IF NEW.payload ->> 'runtime_submission_id' !~ '{_CANONICAL_UUID}' THEN
                RAISE EXCEPTION 'audit event requires a canonical runtime submission ID'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            submission_id := (NEW.payload ->> 'runtime_submission_id')::uuid;
            IF NOT EXISTS (
                SELECT 1 FROM runtime_submissions
                WHERE id = submission_id
                  AND organization_id = NEW.organization_id
                  AND operation_id = NEW.operation_id
            ) THEN
                RAISE EXCEPTION 'audit event runtime submission does not match operation'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER tr_audit_events_history_guard
        BEFORE INSERT OR UPDATE OR DELETE ON audit_events
        FOR EACH ROW EXECUTE FUNCTION guard_provisioning_audit_history()
        """
    )


def upgrade() -> None:
    op.execute(UPGRADE_LOCK_SQL)
    op.execute("SET CONSTRAINTS ALL IMMEDIATE")
    op.drop_constraint(
        "ck_workflow_dispatches_type",
        "workflow_dispatches",
        type_="check",
    )
    op.create_check_constraint(
        "ck_workflow_dispatches_type",
        "workflow_dispatches",
        f"workflow_type IN ('{WORKFLOW_TYPE_V1}', '{WORKFLOW_TYPE_V2}')",
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION enqueue_provisioning_workflow_dispatch() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            INSERT INTO workflow_dispatches (
                operation_id, workflow_id, workflow_type, state, created_at
            ) VALUES (
                NEW.id,
                'provisioning/' || NEW.id::text,
                '{WORKFLOW_TYPE_V2}',
                'pending',
                NEW.created_at
            );
            RETURN NEW;
        END;
        $$
        """
    )
    op.create_table(
        "ingress_submissions",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("organization_id", UUID, nullable=False),
        sa.Column("operation_id", UUID, nullable=False),
        sa.Column("deployment_id", UUID, nullable=False),
        sa.Column("provider", sa.String(length=32, collation="C"), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("submission_key", sa.String(length=64, collation="C"), nullable=False),
        sa.Column("route_fingerprint", sa.String(length=64, collation="C"), nullable=False),
        sa.Column("route_id", sa.String(length=200, collation="C"), nullable=False),
        sa.Column("state", sa.String(length=32, collation="C"), nullable=False),
        sa.Column("failure_code", sa.String(length=64, collation="C"), nullable=True),
        sa.Column(
            "prepared_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["organization_id", "operation_id", "deployment_id"],
            [
                "provisioning_operations.organization_id",
                "provisioning_operations.id",
                "provisioning_operations.deployment_id",
            ],
            name="fk_ingress_submissions_exact_operation",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "deployment_id"],
            ["deployments.organization_id", "deployments.id"],
            name="fk_ingress_submissions_deployment_org",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.UniqueConstraint("operation_id", name="uq_ingress_submissions_operation"),
        sa.UniqueConstraint("submission_key", name="uq_ingress_submissions_key"),
        sa.UniqueConstraint(
            "provider",
            "deployment_id",
            "generation",
            name="uq_ingress_submissions_provider_generation",
        ),
        sa.UniqueConstraint("route_id", name="uq_ingress_submissions_route_id"),
        sa.CheckConstraint("provider = 'local_caddy'", name="ck_ingress_provider"),
        sa.CheckConstraint("generation = 1", name="ck_ingress_generation"),
        sa.CheckConstraint(
            "submission_key ~ '^[0-9a-f]{64}$' AND route_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_ingress_lowercase_hex_identity",
        ),
        sa.CheckConstraint(
            "route_id = 'fstack-crusher-ingress-' || replace(deployment_id::text, '-', '')",
            name="ck_ingress_deterministic_route_id",
        ),
        sa.CheckConstraint(
            "failure_code IS NULL OR failure_code ~ '^[a-z][a-z0-9_]{0,63}$'",
            name="ck_ingress_failure_code_sanitized",
        ),
        sa.CheckConstraint(
            """
            (
                (state = 'prepared' AND submitted_at IS NULL AND confirmed_at IS NULL
                 AND failed_at IS NULL AND failure_code IS NULL)
                OR
                (state = 'submitted' AND submitted_at IS NOT NULL AND confirmed_at IS NULL
                 AND failed_at IS NULL AND failure_code IS NULL)
                OR
                (state = 'confirmed' AND submitted_at IS NOT NULL AND confirmed_at IS NOT NULL
                 AND failed_at IS NULL AND failure_code IS NULL)
                OR
                (state = 'failed' AND confirmed_at IS NULL AND failed_at IS NOT NULL
                 AND failure_code IS NOT NULL)
            ) IS TRUE
            """,
            name="ck_ingress_state_bundle",
        ),
        sa.CheckConstraint(
            """
            (submitted_at IS NULL OR submitted_at >= prepared_at)
            AND (confirmed_at IS NULL OR confirmed_at >= submitted_at)
            AND (failed_at IS NULL OR failed_at >= COALESCE(submitted_at, prepared_at))
            """,
            name="ck_ingress_timestamps_monotonic",
        ),
    )
    op.execute(
        """
        CREATE FUNCTION guard_ingress_submission_lifecycle() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'ingress submissions cannot be deleted'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF NEW.state <> 'prepared' THEN
                    RAISE EXCEPTION 'ingress submissions must be inserted prepared'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
                RETURN NEW;
            END IF;
            IF OLD.state IN ('confirmed', 'failed') THEN
                RAISE EXCEPTION 'terminal ingress submissions are immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF ROW(NEW.id, NEW.organization_id, NEW.operation_id, NEW.deployment_id,
                   NEW.provider, NEW.generation, NEW.submission_key,
                   NEW.route_fingerprint, NEW.route_id, NEW.prepared_at)
               IS DISTINCT FROM
               ROW(OLD.id, OLD.organization_id, OLD.operation_id, OLD.deployment_id,
                   OLD.provider, OLD.generation, OLD.submission_key,
                   OLD.route_fingerprint, OLD.route_id, OLD.prepared_at) THEN
                RAISE EXCEPTION 'ingress submission identity is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF NOT (
                (OLD.state = 'prepared' AND NEW.state IN ('submitted', 'failed'))
                OR (OLD.state = 'submitted' AND NEW.state IN ('confirmed', 'failed'))
            ) THEN
                RAISE EXCEPTION 'invalid ingress submission transition'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF OLD.state = 'submitted'
               AND NEW.submitted_at IS DISTINCT FROM OLD.submitted_at THEN
                RAISE EXCEPTION 'submitted ingress facts are immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER tr_ingress_submissions_lifecycle_guard
        BEFORE INSERT OR UPDATE OR DELETE ON ingress_submissions
        FOR EACH ROW EXECUTE FUNCTION guard_ingress_submission_lifecycle()
        """
    )
    _replace_audit_contract_v2()


def downgrade() -> None:
    op.execute(DOWNGRADE_LOCK_SQL)
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM ingress_submissions)
               OR EXISTS (
                    SELECT 1 FROM workflow_dispatches
                    WHERE workflow_type = '{WORKFLOW_TYPE_V2}'
                ) THEN
                RAISE EXCEPTION 'cannot downgrade while local Caddy or v2 workflow history exists'
                    USING ERRCODE = 'object_not_in_prerequisite_state';
            END IF;
        END;
        $$
        """
    )
    _restore_audit_contract_v1()
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION enqueue_provisioning_workflow_dispatch() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            INSERT INTO workflow_dispatches (
                operation_id, workflow_id, workflow_type, state, created_at
            ) VALUES (
                NEW.id,
                'provisioning/' || NEW.id::text,
                '{WORKFLOW_TYPE_V1}',
                'pending',
                NEW.created_at
            );
            RETURN NEW;
        END;
        $$
        """
    )
    op.drop_constraint(
        "ck_workflow_dispatches_type",
        "workflow_dispatches",
        type_="check",
    )
    op.create_check_constraint(
        "ck_workflow_dispatches_type",
        "workflow_dispatches",
        f"workflow_type = '{WORKFLOW_TYPE_V1}'",
    )
    op.execute("DROP TRIGGER tr_ingress_submissions_lifecycle_guard ON ingress_submissions")
    op.execute("DROP FUNCTION guard_ingress_submission_lifecycle()")
    op.drop_table("ingress_submissions")
