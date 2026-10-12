"""Cut over provisioning authority to Temporal and add its dispatch outbox.

Revision ID: 20261006_0003
Revises: 20261004_0002
Create Date: 2026-10-06
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20261006_0003"
down_revision: str | None = "20261004_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
WORKFLOW_TYPE = "fstack.provision_crusher.v1"
UPGRADE_LOCK_SQL = (
    "LOCK TABLE organizations, businesses, environments, workloads, deployments, "
    "database_bindings, storage_bindings, audit_events, provisioning_operations "
    "IN ACCESS EXCLUSIVE MODE NOWAIT"
)
DOWNGRADE_LOCK_SQL = (
    "LOCK TABLE workflow_dispatches, runtime_submissions, organizations, businesses, "
    "environments, workloads, deployments, database_bindings, storage_bindings, "
    "audit_events, provisioning_operations IN ACCESS EXCLUSIVE MODE NOWAIT"
)


def _upgrade_temporal_cutover() -> None:
    op.drop_constraint(
        "ck_operations_lifecycle_bundle",
        "provisioning_operations",
        type_="check",
    )
    op.drop_constraint(
        "ck_operations_attempt_count_nonnegative",
        "provisioning_operations",
        type_="check",
    )
    op.drop_column("provisioning_operations", "last_error_retryable")
    op.drop_column("provisioning_operations", "current_attempt_id")
    op.drop_column("provisioning_operations", "attempt_count")
    op.create_check_constraint(
        "ck_operations_lifecycle_bundle",
        "provisioning_operations",
        """
        (
            (state = 'pending' AND started_at IS NULL AND completed_at IS NULL
             AND last_error_code IS NULL)
            OR
            (state = 'running' AND started_at IS NOT NULL AND completed_at IS NULL
             AND last_error_code IS NULL)
            OR
            (state = 'succeeded' AND started_at IS NOT NULL AND completed_at IS NOT NULL
             AND last_error_code IS NULL)
            OR
            (state = 'failed' AND started_at IS NOT NULL AND completed_at IS NOT NULL
             AND last_error_code IS NOT NULL)
        ) IS TRUE
        """,
    )
    op.execute(
        """
        CREATE FUNCTION guard_provisioning_operation_lifecycle() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.state <> 'pending' THEN
                    RAISE EXCEPTION 'provisioning operations must be inserted pending'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'provisioning operations cannot be deleted'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF OLD.state IN ('succeeded', 'failed') THEN
                IF NEW IS DISTINCT FROM OLD THEN
                    RAISE EXCEPTION 'terminal provisioning operations are immutable'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
                RETURN NEW;
            END IF;
            IF ROW(NEW.id, NEW.organization_id, NEW.organization_slug,
                   NEW.idempotency_key, NEW.request_fingerprint,
                   NEW.actor_principal, NEW.business_id, NEW.environment_id,
                   NEW.workload_id, NEW.deployment_id, NEW.created_at)
               IS DISTINCT FROM
               ROW(OLD.id, OLD.organization_id, OLD.organization_slug,
                   OLD.idempotency_key, OLD.request_fingerprint,
                   OLD.actor_principal, OLD.business_id, OLD.environment_id,
                   OLD.workload_id, OLD.deployment_id, OLD.created_at) THEN
                RAISE EXCEPTION 'provisioning operation identity is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF NEW.updated_at < OLD.updated_at
               OR (OLD.started_at IS NOT NULL AND NEW.started_at IS DISTINCT FROM OLD.started_at)
               OR (OLD.completed_at IS NOT NULL
                   AND NEW.completed_at IS DISTINCT FROM OLD.completed_at) THEN
                RAISE EXCEPTION 'provisioning operation lifecycle is monotonic'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF (OLD.state = 'pending' AND NEW.state NOT IN ('pending', 'running'))
               OR (OLD.state = 'running'
                   AND NEW.state NOT IN ('running', 'succeeded', 'failed')) THEN
                RAISE EXCEPTION 'invalid provisioning operation state transition'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER tr_operations_lifecycle_guard
        BEFORE INSERT OR UPDATE OR DELETE ON provisioning_operations
        FOR EACH ROW EXECUTE FUNCTION guard_provisioning_operation_lifecycle()
        """
    )
    op.execute(
        """
        CREATE FUNCTION guard_requested_desired_state() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_TABLE_NAME = 'organizations' THEN
                IF EXISTS (
                    SELECT 1 FROM provisioning_operations WHERE organization_id = OLD.id
                ) THEN
                    RAISE EXCEPTION 'requested deployment state is immutable'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
            ELSIF TG_TABLE_NAME = 'businesses' THEN
                IF EXISTS (
                    SELECT 1 FROM provisioning_operations
                    WHERE organization_id = OLD.organization_id AND business_id = OLD.id
                ) THEN
                    RAISE EXCEPTION 'requested deployment state is immutable'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
            ELSIF TG_TABLE_NAME = 'environments' THEN
                IF EXISTS (
                    SELECT 1 FROM provisioning_operations
                    WHERE organization_id = OLD.organization_id AND environment_id = OLD.id
                ) THEN
                    RAISE EXCEPTION 'requested deployment state is immutable'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
            ELSIF TG_TABLE_NAME = 'workloads' THEN
                IF EXISTS (
                    SELECT 1 FROM provisioning_operations
                    WHERE organization_id = OLD.organization_id AND workload_id = OLD.id
                ) THEN
                    RAISE EXCEPTION 'requested deployment state is immutable'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
            ELSIF TG_TABLE_NAME = 'deployments' THEN
                IF EXISTS (
                    SELECT 1 FROM provisioning_operations
                    WHERE organization_id = OLD.organization_id AND deployment_id = OLD.id
                ) THEN
                    RAISE EXCEPTION 'requested deployment state is immutable'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
            ELSE
                IF EXISTS (
                    SELECT 1 FROM provisioning_operations
                    WHERE organization_id = OLD.organization_id
                      AND deployment_id = OLD.deployment_id
                ) THEN
                    RAISE EXCEPTION 'requested deployment state is immutable'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
            END IF;
            RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
        END;
        $$
        """
    )
    for table in (
        "organizations",
        "businesses",
        "environments",
        "workloads",
        "deployments",
        "database_bindings",
        "storage_bindings",
    ):
        op.execute(
            f"""
            CREATE TRIGGER tr_{table}_requested_state_guard
            BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION guard_requested_desired_state()
            """
        )

    op.create_table(
        "runtime_submissions",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("organization_id", UUID, nullable=False),
        sa.Column("operation_id", UUID, nullable=False),
        sa.Column("deployment_id", UUID, nullable=False),
        sa.Column("provider", sa.String(length=32, collation="C"), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("submission_key", sa.String(length=64, collation="C"), nullable=False),
        sa.Column("spec_fingerprint", sa.String(length=64, collation="C"), nullable=False),
        sa.Column("api_container_name", sa.String(length=200, collation="C"), nullable=False),
        sa.Column("migration_container_name", sa.String(length=200, collation="C"), nullable=False),
        sa.Column("state", sa.String(length=32, collation="C"), nullable=False),
        sa.Column("external_id", sa.String(length=200, collation="C"), nullable=True),
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
            name="fk_runtime_submissions_exact_operation",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "deployment_id"],
            ["deployments.organization_id", "deployments.id"],
            name="fk_runtime_submissions_deployment_org",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.UniqueConstraint("operation_id", name="uq_runtime_submissions_operation"),
        sa.UniqueConstraint("submission_key", name="uq_runtime_submissions_key"),
        sa.UniqueConstraint(
            "provider",
            "deployment_id",
            "generation",
            name="uq_runtime_submissions_provider_generation",
        ),
        sa.UniqueConstraint("api_container_name", name="uq_runtime_submissions_api_container_name"),
        sa.UniqueConstraint(
            "migration_container_name",
            name="uq_runtime_submissions_migration_container_name",
        ),
        sa.CheckConstraint("provider = 'local_docker'", name="ck_runtime_provider"),
        sa.CheckConstraint("generation > 0", name="ck_runtime_generation_positive"),
        sa.CheckConstraint(
            "submission_key ~ '^[0-9a-f]{64}$' AND spec_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_runtime_lowercase_hex_identity",
        ),
        sa.CheckConstraint(
            """
            api_container_name = 'fstack-crusher-' || replace(deployment_id::text, '-', '')
            AND migration_container_name =
                'fstack-crusher-migrate-' || replace(deployment_id::text, '-', '')
            """,
            name="ck_runtime_deterministic_container_names",
        ),
        sa.CheckConstraint(
            "failure_code IS NULL OR failure_code ~ '^[a-z][a-z0-9_]{0,63}$'",
            name="ck_runtime_failure_code_sanitized",
        ),
        sa.CheckConstraint(
            """
            (
                (state = 'prepared' AND external_id IS NULL AND submitted_at IS NULL
                 AND confirmed_at IS NULL AND failed_at IS NULL AND failure_code IS NULL)
                OR
                (state = 'submitted' AND external_id IS NOT NULL AND submitted_at IS NOT NULL
                 AND confirmed_at IS NULL AND failed_at IS NULL AND failure_code IS NULL)
                OR
                (state = 'confirmed' AND external_id IS NOT NULL AND submitted_at IS NOT NULL
                 AND confirmed_at IS NOT NULL AND failed_at IS NULL AND failure_code IS NULL)
                OR
                (state = 'failed' AND confirmed_at IS NULL AND failed_at IS NOT NULL
                 AND failure_code IS NOT NULL
                 AND ((external_id IS NULL AND submitted_at IS NULL)
                      OR (external_id IS NOT NULL AND submitted_at IS NOT NULL)))
            ) IS TRUE
            """,
            name="ck_runtime_state_bundle",
        ),
        sa.CheckConstraint(
            """
            (submitted_at IS NULL OR submitted_at >= prepared_at)
            AND (confirmed_at IS NULL OR confirmed_at >= submitted_at)
            AND (failed_at IS NULL OR failed_at >= COALESCE(submitted_at, prepared_at))
            """,
            name="ck_runtime_timestamps_monotonic",
        ),
    )
    op.execute(
        """
        CREATE FUNCTION guard_runtime_submission_lifecycle() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'runtime submissions cannot be deleted'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF NEW.state <> 'prepared' THEN
                    RAISE EXCEPTION 'runtime submissions must be inserted prepared'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
                RETURN NEW;
            END IF;
            IF OLD.state IN ('confirmed', 'failed') THEN
                RAISE EXCEPTION 'terminal runtime submissions are immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF ROW(NEW.id, NEW.organization_id, NEW.operation_id, NEW.deployment_id,
                   NEW.provider, NEW.generation, NEW.submission_key,
                   NEW.spec_fingerprint, NEW.api_container_name,
                   NEW.migration_container_name, NEW.prepared_at)
               IS DISTINCT FROM
               ROW(OLD.id, OLD.organization_id, OLD.operation_id, OLD.deployment_id,
                   OLD.provider, OLD.generation, OLD.submission_key,
                   OLD.spec_fingerprint, OLD.api_container_name,
                   OLD.migration_container_name, OLD.prepared_at) THEN
                RAISE EXCEPTION 'runtime submission identity is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF NOT (
                (OLD.state = 'prepared' AND NEW.state IN ('submitted', 'failed'))
                OR (OLD.state = 'submitted' AND NEW.state IN ('confirmed', 'failed'))
            ) THEN
                RAISE EXCEPTION 'invalid runtime submission transition'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF OLD.state = 'submitted'
               AND ROW(NEW.external_id, NEW.submitted_at)
                   IS DISTINCT FROM ROW(OLD.external_id, OLD.submitted_at) THEN
                RAISE EXCEPTION 'submitted runtime facts are immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER tr_runtime_submissions_lifecycle_guard
        BEFORE INSERT OR UPDATE OR DELETE ON runtime_submissions
        FOR EACH ROW EXECUTE FUNCTION guard_runtime_submission_lifecycle()
        """
    )

    op.create_check_constraint(
        "ck_audit_provisioning_lifecycle_payload",
        "audit_events",
        """
        (
            jsonb_typeof(payload) = 'object'
            AND (
                (event_type = 'provisioning.requested' AND payload = '{}'::jsonb)
                OR
                (event_type = 'provisioning.started'
                 AND payload ?& ARRAY['runtime_submission_id', 'provider']
                 AND payload - ARRAY['runtime_submission_id', 'provider'] = '{}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string'
                 AND payload ->> 'runtime_submission_id'
                     ~ '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                 AND payload ->> 'provider' = 'local_docker')
                OR
                (event_type = 'provisioning.succeeded'
                 AND payload ? 'runtime_submission_id'
                 AND payload - 'runtime_submission_id' = '{}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string')
                OR
                (event_type = 'provisioning.failed'
                 AND payload ?& ARRAY['runtime_submission_id', 'error_code']
                 AND payload - ARRAY['runtime_submission_id', 'error_code'] = '{}'::jsonb
                 AND jsonb_typeof(payload -> 'runtime_submission_id') = 'string'
                 AND jsonb_typeof(payload -> 'error_code') = 'string'
                 AND payload ->> 'error_code' ~ '^[a-z][a-z0-9_]{0,63}$')
            )
        ) IS TRUE
        """,
    )
    op.execute(
        """
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
            IF NEW.payload ->> 'runtime_submission_id' !~
               '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' THEN
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


def _downgrade_temporal_cutover() -> None:
    op.execute("DROP TRIGGER tr_audit_events_history_guard ON audit_events")
    op.execute("DROP FUNCTION guard_provisioning_audit_history()")
    op.drop_constraint("ck_audit_provisioning_lifecycle_payload", "audit_events", type_="check")

    op.execute("DROP TRIGGER tr_runtime_submissions_lifecycle_guard ON runtime_submissions")
    op.execute("DROP FUNCTION guard_runtime_submission_lifecycle()")
    op.drop_table("runtime_submissions")

    for table in (
        "storage_bindings",
        "database_bindings",
        "deployments",
        "workloads",
        "environments",
        "businesses",
        "organizations",
    ):
        op.execute(f"DROP TRIGGER tr_{table}_requested_state_guard ON {table}")
    op.execute("DROP FUNCTION guard_requested_desired_state()")
    op.execute("DROP TRIGGER tr_operations_lifecycle_guard ON provisioning_operations")
    op.execute("DROP FUNCTION guard_provisioning_operation_lifecycle()")

    op.drop_constraint("ck_operations_lifecycle_bundle", "provisioning_operations", type_="check")
    op.add_column(
        "provisioning_operations",
        sa.Column(
            "attempt_count",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        "provisioning_operations",
        sa.Column("current_attempt_id", UUID, nullable=True),
    )
    op.add_column(
        "provisioning_operations",
        sa.Column("last_error_retryable", sa.Boolean(), nullable=True),
    )
    op.create_check_constraint(
        "ck_operations_lifecycle_bundle",
        "provisioning_operations",
        """
        (
            state = 'pending'
            AND current_attempt_id IS NULL
            AND completed_at IS NULL
            AND (
                (
                    started_at IS NULL
                    AND last_error_code IS NULL
                    AND last_error_retryable IS NULL
                )
                OR
                (
                    started_at IS NOT NULL
                    AND last_error_code IS NOT NULL
                    AND last_error_retryable IS TRUE
                )
            )
        )
        OR
        (
            state = 'running'
            AND current_attempt_id IS NOT NULL
            AND started_at IS NOT NULL
            AND completed_at IS NULL
            AND last_error_code IS NULL
            AND last_error_retryable IS NULL
        )
        OR
        (
            state = 'succeeded'
            AND current_attempt_id IS NULL
            AND started_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND last_error_code IS NULL
            AND last_error_retryable IS NULL
        )
        OR
        (
            state = 'failed'
            AND current_attempt_id IS NULL
            AND started_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND last_error_code IS NOT NULL
            AND last_error_retryable IS FALSE
        )
        """,
    )
    op.create_check_constraint(
        "ck_operations_attempt_count_nonnegative",
        "provisioning_operations",
        "attempt_count >= 0",
    )


def upgrade() -> None:
    op.execute(UPGRADE_LOCK_SQL)
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM provisioning_operations
                WHERE state <> 'pending'
                   OR started_at IS NOT NULL
                   OR completed_at IS NOT NULL
                   OR attempt_count <> 0
                   OR current_attempt_id IS NOT NULL
                   OR last_error_code IS NOT NULL
                   OR last_error_retryable IS NOT NULL
            ) OR EXISTS (
                SELECT 1 FROM audit_events
                WHERE event_type <> 'provisioning.requested' OR payload <> '{}'::jsonb
            ) THEN
                RAISE EXCEPTION 'cannot cut over non-pristine PostgreSQL provisioning history'
                    USING ERRCODE = 'object_not_in_prerequisite_state';
            END IF;
        END;
        $$
        """
    )
    _upgrade_temporal_cutover()
    op.create_table(
        "workflow_dispatches",
        sa.Column("operation_id", UUID, primary_key=True),
        sa.Column("workflow_id", sa.String(length=200, collation="C"), nullable=False),
        sa.Column("workflow_type", sa.String(length=64, collation="C"), nullable=False),
        sa.Column("state", sa.String(length=16, collation="C"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["operation_id"],
            ["provisioning_operations.id"],
            name="fk_workflow_dispatches_operation",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.UniqueConstraint("workflow_id", name="uq_workflow_dispatches_workflow_id"),
        sa.CheckConstraint(
            "workflow_id = 'provisioning/' || operation_id::text",
            name="ck_workflow_dispatches_deterministic_id",
        ),
        sa.CheckConstraint(
            f"workflow_type = '{WORKFLOW_TYPE}'",
            name="ck_workflow_dispatches_type",
        ),
        sa.CheckConstraint(
            """
            (
                (state = 'pending' AND dispatched_at IS NULL)
                OR
                (state = 'dispatched' AND dispatched_at IS NOT NULL)
            ) IS TRUE
            """,
            name="ck_workflow_dispatches_state_bundle",
        ),
        sa.CheckConstraint(
            "dispatched_at IS NULL OR dispatched_at >= created_at",
            name="ck_workflow_dispatches_timestamps_monotonic",
        ),
    )
    op.execute(
        """
        CREATE FUNCTION enqueue_provisioning_workflow_dispatch() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            INSERT INTO workflow_dispatches (
                operation_id, workflow_id, workflow_type, state, created_at
            ) VALUES (
                NEW.id,
                'provisioning/' || NEW.id::text,
                'fstack.provision_crusher.v1',
                'pending',
                NEW.created_at
            );
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER tr_operations_enqueue_workflow_dispatch
        AFTER INSERT ON provisioning_operations
        FOR EACH ROW EXECUTE FUNCTION enqueue_provisioning_workflow_dispatch()
        """
    )
    op.execute(
        """
        INSERT INTO workflow_dispatches (
            operation_id, workflow_id, workflow_type, state, created_at
        )
        SELECT
            id,
            'provisioning/' || id::text,
            'fstack.provision_crusher.v1',
            'pending',
            created_at
        FROM provisioning_operations
        WHERE state IN ('pending', 'running')
        """
    )
    op.execute(
        """
        CREATE FUNCTION guard_workflow_dispatch_lifecycle() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'workflow dispatches cannot be deleted'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF NEW.state <> 'pending' THEN
                    RAISE EXCEPTION 'workflow dispatches must be inserted pending'
                        USING ERRCODE = 'integrity_constraint_violation';
                END IF;
                RETURN NEW;
            END IF;
            IF OLD.state = 'dispatched' THEN
                RAISE EXCEPTION 'dispatched workflow intents are immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF ROW(NEW.operation_id, NEW.workflow_id, NEW.workflow_type, NEW.created_at)
               IS DISTINCT FROM
               ROW(OLD.operation_id, OLD.workflow_id, OLD.workflow_type, OLD.created_at) THEN
                RAISE EXCEPTION 'workflow dispatch identity is immutable'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            IF NEW.state <> 'dispatched' THEN
                RAISE EXCEPTION 'invalid workflow dispatch transition'
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER tr_workflow_dispatches_lifecycle_guard
        BEFORE INSERT OR UPDATE OR DELETE ON workflow_dispatches
        FOR EACH ROW EXECUTE FUNCTION guard_workflow_dispatch_lifecycle()
        """
    )


def downgrade() -> None:
    op.execute(DOWNGRADE_LOCK_SQL)
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM workflow_dispatches WHERE state <> 'pending')
               OR EXISTS (SELECT 1 FROM runtime_submissions)
               OR EXISTS (
                    SELECT 1 FROM provisioning_operations
                    WHERE state <> 'pending'
                       OR started_at IS NOT NULL
                       OR completed_at IS NOT NULL
                       OR last_error_code IS NOT NULL
                )
               OR EXISTS (
                    SELECT 1 FROM audit_events
                    WHERE event_type <> 'provisioning.requested'
                ) THEN
                RAISE EXCEPTION 'cannot downgrade while Temporal provisioning history exists'
                    USING ERRCODE = 'object_not_in_prerequisite_state';
            END IF;
        END;
        $$
        """
    )
    op.execute("DROP TRIGGER tr_workflow_dispatches_lifecycle_guard ON workflow_dispatches")
    op.execute("DROP FUNCTION guard_workflow_dispatch_lifecycle()")
    op.execute("DROP TRIGGER tr_operations_enqueue_workflow_dispatch ON provisioning_operations")
    op.execute("DROP FUNCTION enqueue_provisioning_workflow_dispatch()")
    op.drop_table("workflow_dispatches")
    _downgrade_temporal_cutover()
