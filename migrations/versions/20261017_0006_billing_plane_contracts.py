"""Add provider-neutral billing-plane contracts.

Revision ID: 20261017_0006
Revises: 20261015_0005
Create Date: 2026-10-17
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20261017_0006"
down_revision: str | None = "20261015_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
C16 = sa.String(length=16, collation="C")
C32 = sa.String(length=32, collation="C")
C64 = sa.String(length=64, collation="C")
C100 = sa.String(length=100, collation="C")
C_SLUG = sa.String(length=63, collation="C")
DOWNGRADE_LOCK_SQL = (
    "LOCK TABLE provisioning_operations, entitlement_subjects, "
    "entitlement_revisions, provisioning_authorizations, usage_facts "
    "IN ACCESS EXCLUSIVE MODE NOWAIT"
)


def _uuid_primary_key() -> sa.Column[object]:
    return sa.Column("id", UUID, primary_key=True, nullable=False)


def _created_at(name: str = "created_at") -> sa.Column[object]:
    return sa.Column(
        name,
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("clock_timestamp()"),
    )


def _install_immutability_guards() -> None:
    op.execute(
        """
        CREATE FUNCTION reject_billing_evidence_mutation() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'billing evidence is append-only' USING ERRCODE = '23000';
        END;
        $$
        """
    )
    for table in (
        "entitlement_subjects",
        "entitlement_revisions",
        "provisioning_authorizations",
        "usage_facts",
    ):
        op.execute(
            f"""
            CREATE TRIGGER tr_{table}_append_only
            BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION reject_billing_evidence_mutation()
            """
        )


def _install_authorization_bundle_guards() -> None:
    op.execute(
        """
        CREATE FUNCTION enforce_operation_authorization_bundle() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            target_operation_id uuid;
            operation_row provisioning_operations%ROWTYPE;
            authorization_subject_id uuid;
            total_count bigint;
            matching_count bigint;
            decision_clock timestamptz;
        BEGIN
            target_operation_id := CASE
                WHEN TG_TABLE_NAME = 'provisioning_operations'
                    THEN (to_jsonb(NEW) ->> 'id')::uuid
                ELSE COALESCE(
                    (to_jsonb(NEW) ->> 'operation_id')::uuid,
                    (to_jsonb(OLD) ->> 'operation_id')::uuid
                )
            END;

            SELECT * INTO operation_row
            FROM provisioning_operations
            WHERE id = target_operation_id;
            IF NOT FOUND THEN
                RETURN NULL;
            END IF;

            SELECT count(*) INTO total_count
            FROM provisioning_authorizations AS auth
            WHERE auth.operation_id = operation_row.id;

            IF total_count > 0 THEN
                SELECT auth.subject_id INTO authorization_subject_id
                FROM provisioning_authorizations AS auth
                WHERE auth.operation_id = operation_row.id;

                PERFORM 1
                FROM entitlement_subjects
                WHERE id = authorization_subject_id
                FOR UPDATE;
            END IF;

            decision_clock := clock_timestamp();

            SELECT count(*) INTO matching_count
            FROM provisioning_authorizations AS auth
            JOIN entitlement_subjects AS subject ON subject.id = auth.subject_id
            JOIN entitlement_revisions AS revision
              ON revision.id = auth.revision_id
             AND revision.subject_id = auth.subject_id
            WHERE auth.operation_id = operation_row.id
              AND auth.organization_id = operation_row.organization_id
              AND auth.deployment_id = operation_row.deployment_id
              AND subject.subject_key = operation_row.organization_slug
              AND revision.id = (
                  SELECT current_revision.id
                  FROM entitlement_revisions AS current_revision
                  WHERE current_revision.subject_id = auth.subject_id
                  ORDER BY current_revision.generation DESC
                  LIMIT 1
              )
              AND revision.state = 'active'
              AND auth.decided_at >= revision.observed_at
              AND auth.decided_at <= revision.valid_until
              AND auth.decided_at <= decision_clock
              AND decision_clock <= revision.valid_until
              AND auth.request_fingerprint = operation_row.request_fingerprint;

            IF operation_row.authorization_mode = 'required'
               AND (total_count <> 1 OR matching_count <> 1) THEN
                RAISE EXCEPTION 'required operation authorization bundle mismatch'
                    USING ERRCODE = '23000';
            END IF;
            IF operation_row.authorization_mode = 'not_required' AND total_count <> 0 THEN
                RAISE EXCEPTION 'unrequired operation has authorization'
                    USING ERRCODE = '23000';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER tr_operations_authorization_bundle
        AFTER INSERT OR UPDATE OF authorization_mode, organization_id, deployment_id,
                               organization_slug
        ON provisioning_operations
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION enforce_operation_authorization_bundle()
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER tr_authorizations_operation_bundle
        AFTER INSERT OR UPDATE OR DELETE ON provisioning_authorizations
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION enforce_operation_authorization_bundle()
        """
    )


def _install_operation_authorization_mode_guard() -> None:
    op.execute(
        """
        CREATE FUNCTION reject_operation_authorization_mode_mutation() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.authorization_mode IS DISTINCT FROM OLD.authorization_mode THEN
                RAISE EXCEPTION 'operation authorization mode is immutable'
                    USING ERRCODE = '23000';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER tr_operations_authorization_mode_immutable
        BEFORE UPDATE OF authorization_mode ON provisioning_operations
        FOR EACH ROW EXECUTE FUNCTION reject_operation_authorization_mode_mutation()
        """
    )


def _install_usage_success_guard() -> None:
    op.execute(
        """
        CREATE FUNCTION enforce_usage_fact_success_bundle() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM provisioning_operations AS operation
                JOIN runtime_submissions AS runtime
                  ON runtime.operation_id = operation.id
                 AND runtime.organization_id = operation.organization_id
                 AND runtime.deployment_id = operation.deployment_id
                JOIN ingress_submissions AS ingress
                  ON ingress.operation_id = operation.id
                 AND ingress.organization_id = operation.organization_id
                 AND ingress.deployment_id = operation.deployment_id
                JOIN audit_events AS audit
                  ON audit.operation_id = operation.id
                 AND audit.organization_id = operation.organization_id
                 AND audit.event_type = 'provisioning.succeeded'
                 AND audit.payload ->> 'runtime_submission_id' = runtime.id::text
                 AND audit.payload ->> 'ingress_submission_id' = ingress.id::text
                WHERE operation.id = NEW.operation_id
                  AND operation.organization_id = NEW.organization_id
                  AND operation.deployment_id = NEW.deployment_id
                  AND operation.state = 'succeeded'
                  AND operation.authorization_mode = 'required'
                  AND NEW.occurred_at <= clock_timestamp()
                  AND operation.completed_at = NEW.occurred_at
                  AND operation.updated_at = NEW.occurred_at
                  AND runtime.state = 'confirmed'
                  AND runtime.confirmed_at = NEW.occurred_at
                  AND ingress.state = 'confirmed'
                  AND ingress.confirmed_at = NEW.occurred_at
            ) THEN
                RAISE EXCEPTION 'usage fact success bundle mismatch'
                    USING ERRCODE = '23000';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER tr_usage_facts_success_bundle
        AFTER INSERT ON usage_facts
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION enforce_usage_fact_success_bundle()
        """
    )


def upgrade() -> None:
    op.add_column(
        "provisioning_operations",
        sa.Column(
            "authorization_mode",
            C16,
            nullable=False,
            server_default=sa.text("'not_required'"),
        ),
    )
    op.create_check_constraint(
        "ck_operations_authorization_mode",
        "provisioning_operations",
        "authorization_mode IN ('not_required', 'required')",
    )
    op.create_table(
        "entitlement_subjects",
        _uuid_primary_key(),
        sa.Column("subject_type", C32, nullable=False),
        sa.Column("subject_key", C_SLUG, nullable=False),
        _created_at(),
        sa.UniqueConstraint("subject_type", "subject_key", name="uq_entitlement_subjects_type_key"),
        sa.CheckConstraint(
            "subject_type = 'organization_slug'",
            name="ck_entitlement_subjects_type",
        ),
        sa.CheckConstraint(
            "subject_key ~ '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$'",
            name="ck_entitlement_subjects_key",
        ),
    )

    op.create_table(
        "entitlement_revisions",
        _uuid_primary_key(),
        sa.Column("subject_id", UUID, nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("plan_key", C64, nullable=False),
        sa.Column("state", C16, nullable=False),
        sa.Column("capability", C64, nullable=False),
        sa.Column("quota_dimension", C64, nullable=False),
        sa.Column("quota_limit", sa.BigInteger(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_revision_id", UUID, nullable=False),
        sa.Column("payload_digest", C64, nullable=False),
        _created_at("recorded_at"),
        sa.ForeignKeyConstraint(
            ["subject_id"],
            ["entitlement_subjects.id"],
            name="fk_entitlement_revisions_subject",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("subject_id", "id", name="uq_entitlement_revisions_subject_id"),
        sa.UniqueConstraint("subject_id", "generation", name="uq_entitlement_revisions_generation"),
        sa.UniqueConstraint("source_revision_id", name="uq_entitlement_revisions_source"),
        sa.CheckConstraint("generation > 0", name="ck_entitlement_revisions_generation"),
        sa.CheckConstraint("plan_key = 'crusher.single.v1'", name="ck_entitlement_revisions_plan"),
        sa.CheckConstraint(
            "state IN ('active', 'inactive')", name="ck_entitlement_revisions_state"
        ),
        sa.CheckConstraint(
            "capability = 'crusher.deployment.create'",
            name="ck_entitlement_revisions_capability",
        ),
        sa.CheckConstraint(
            "quota_dimension = 'crusher.deployment.active'",
            name="ck_entitlement_revisions_quota_dimension",
        ),
        sa.CheckConstraint("quota_limit = 1", name="ck_entitlement_revisions_quota_limit"),
        sa.CheckConstraint(
            "valid_until > observed_at AND valid_until <= observed_at + interval '5 minutes'",
            name="ck_entitlement_revisions_validity",
        ),
        sa.CheckConstraint(
            "payload_digest ~ '^[0-9a-f]{64}$'",
            name="ck_entitlement_revisions_digest",
        ),
    )

    op.create_table(
        "provisioning_authorizations",
        _uuid_primary_key(),
        sa.Column("usage_fact_id", UUID, nullable=False),
        sa.Column("subject_id", UUID, nullable=False),
        sa.Column("revision_id", UUID, nullable=False),
        sa.Column("organization_id", UUID, nullable=False),
        sa.Column("operation_id", UUID, nullable=False),
        sa.Column("deployment_id", UUID, nullable=False),
        sa.Column("capability", C64, nullable=False),
        sa.Column("quota_dimension", C64, nullable=False),
        sa.Column("units", sa.BigInteger(), nullable=False),
        sa.Column("request_fingerprint", C64, nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["subject_id", "revision_id"],
            ["entitlement_revisions.subject_id", "entitlement_revisions.id"],
            name="fk_provisioning_authorizations_revision_subject",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "operation_id", "deployment_id"],
            [
                "provisioning_operations.organization_id",
                "provisioning_operations.id",
                "provisioning_operations.deployment_id",
            ],
            name="fk_provisioning_authorizations_operation_deployment",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.UniqueConstraint("operation_id", name="uq_provisioning_authorizations_operation"),
        sa.UniqueConstraint(
            "subject_id",
            "quota_dimension",
            name="uq_provisioning_authorizations_subject_quota",
        ),
        sa.UniqueConstraint("usage_fact_id", name="uq_provisioning_authorizations_usage_fact"),
        sa.UniqueConstraint(
            "id",
            "usage_fact_id",
            "subject_id",
            "revision_id",
            "organization_id",
            "operation_id",
            "deployment_id",
            name="uq_provisioning_authorizations_usage_bundle",
        ),
        sa.CheckConstraint(
            "capability = 'crusher.deployment.create'",
            name="ck_provisioning_authorizations_capability",
        ),
        sa.CheckConstraint(
            "quota_dimension = 'crusher.deployment.active'",
            name="ck_provisioning_authorizations_quota_dimension",
        ),
        sa.CheckConstraint("units = 1", name="ck_provisioning_authorizations_units"),
        sa.CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_provisioning_authorizations_fingerprint",
        ),
    )

    op.create_table(
        "usage_facts",
        _uuid_primary_key(),
        sa.Column("subject_id", UUID, nullable=False),
        sa.Column("revision_id", UUID, nullable=False),
        sa.Column("authorization_id", UUID, nullable=False),
        sa.Column("organization_id", UUID, nullable=False),
        sa.Column("operation_id", UUID, nullable=False),
        sa.Column("deployment_id", UUID, nullable=False),
        sa.Column("metric", C64, nullable=False),
        sa.Column("unit", C32, nullable=False),
        sa.Column("quantity", sa.BigInteger(), nullable=False),
        sa.Column("source_event", C100, nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        _created_at("recorded_at"),
        sa.Column("schema_version", sa.BigInteger(), nullable=False),
        sa.Column("payload_digest", C64, nullable=False),
        sa.ForeignKeyConstraint(
            [
                "authorization_id",
                "id",
                "subject_id",
                "revision_id",
                "organization_id",
                "operation_id",
                "deployment_id",
            ],
            [
                "provisioning_authorizations.id",
                "provisioning_authorizations.usage_fact_id",
                "provisioning_authorizations.subject_id",
                "provisioning_authorizations.revision_id",
                "provisioning_authorizations.organization_id",
                "provisioning_authorizations.operation_id",
                "provisioning_authorizations.deployment_id",
            ],
            name="fk_usage_facts_authorization_bundle",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("operation_id", "metric", name="uq_usage_facts_operation_metric"),
        sa.UniqueConstraint("deployment_id", "metric", name="uq_usage_facts_deployment_metric"),
        sa.CheckConstraint("metric = 'crusher.deployment.activated'", name="ck_usage_facts_metric"),
        sa.CheckConstraint("unit = 'deployment'", name="ck_usage_facts_unit"),
        sa.CheckConstraint("quantity = 1", name="ck_usage_facts_quantity"),
        sa.CheckConstraint(
            "source_event = 'provisioning.succeeded'", name="ck_usage_facts_source_event"
        ),
        sa.CheckConstraint("schema_version = 1", name="ck_usage_facts_schema_version"),
        sa.CheckConstraint("payload_digest ~ '^[0-9a-f]{64}$'", name="ck_usage_facts_digest"),
    )
    op.create_index(
        "ix_usage_facts_recorded_id",
        "usage_facts",
        ["recorded_at", "id"],
        unique=False,
    )

    _install_authorization_bundle_guards()
    _install_operation_authorization_mode_guard()
    _install_usage_success_guard()
    _install_immutability_guards()


def downgrade() -> None:
    op.execute(DOWNGRADE_LOCK_SQL)
    connection = op.get_bind()
    has_history = connection.execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM entitlement_subjects) "
            "OR EXISTS (SELECT 1 FROM entitlement_revisions) "
            "OR EXISTS (SELECT 1 FROM provisioning_authorizations) "
            "OR EXISTS (SELECT 1 FROM usage_facts)"
        )
    ).scalar_one()
    if has_history:
        raise RuntimeError("billing history prevents downgrade")

    op.execute("DROP TRIGGER tr_authorizations_operation_bundle ON provisioning_authorizations")
    op.execute("DROP TRIGGER tr_operations_authorization_bundle ON provisioning_operations")
    op.execute("DROP FUNCTION enforce_operation_authorization_bundle()")
    op.execute("DROP TRIGGER tr_operations_authorization_mode_immutable ON provisioning_operations")
    op.execute("DROP FUNCTION reject_operation_authorization_mode_mutation()")
    op.execute("DROP TRIGGER tr_usage_facts_success_bundle ON usage_facts")
    op.execute("DROP FUNCTION enforce_usage_fact_success_bundle()")
    for table in (
        "usage_facts",
        "provisioning_authorizations",
        "entitlement_revisions",
        "entitlement_subjects",
    ):
        op.execute(f"DROP TRIGGER tr_{table}_append_only ON {table}")
    op.execute("DROP FUNCTION reject_billing_evidence_mutation()")
    op.drop_table("usage_facts")
    op.drop_table("provisioning_authorizations")
    op.drop_table("entitlement_revisions")
    op.drop_table("entitlement_subjects")
    op.drop_constraint("ck_operations_authorization_mode", "provisioning_operations", type_="check")
    op.drop_column("provisioning_operations", "authorization_mode")
