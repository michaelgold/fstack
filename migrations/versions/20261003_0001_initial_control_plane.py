"""Create the initial Fstack control-plane schema.

Revision ID: 20261003_0001
Revises:
Create Date: 2026-10-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20261003_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
C_SLUG = sa.String(length=63, collation="C")
C_KEY = sa.String(length=200, collation="C")
CREATED_AT = sa.DateTime(timezone=True)


def _id() -> sa.Column[object]:
    return sa.Column("id", UUID, primary_key=True)


def _organization_id() -> sa.Column[object]:
    return sa.Column("organization_id", UUID, nullable=False)


def _created_at() -> sa.Column[object]:
    return sa.Column(
        "created_at",
        CREATED_AT,
        nullable=False,
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )


def upgrade() -> None:
    op.create_table(
        "organizations",
        _id(),
        sa.Column("slug", C_SLUG, nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        _created_at(),
        sa.UniqueConstraint("slug", name="uq_organizations_slug"),
    )

    op.create_table(
        "businesses",
        _id(),
        _organization_id(),
        sa.Column("slug", C_SLUG, nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        _created_at(),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_businesses_organization",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_businesses_org_id"),
        sa.UniqueConstraint("organization_id", "slug", name="uq_businesses_org_slug"),
    )

    op.create_table(
        "environments",
        _id(),
        _organization_id(),
        sa.Column("business_id", UUID, nullable=False),
        sa.Column("slug", C_SLUG, nullable=False),
        _created_at(),
        sa.ForeignKeyConstraint(
            ["organization_id", "business_id"],
            ["businesses.organization_id", "businesses.id"],
            name="fk_environments_business_org",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_environments_org_id"),
        sa.UniqueConstraint(
            "organization_id",
            "business_id",
            "slug",
            name="uq_environments_business_slug",
        ),
        sa.CheckConstraint("slug = 'production'", name="ck_environments_initial_production"),
    )

    op.create_table(
        "workloads",
        _id(),
        _organization_id(),
        sa.Column("environment_id", UUID, nullable=False),
        sa.Column("slug", C_SLUG, nullable=False),
        sa.Column("kind", C_SLUG, nullable=False),
        _created_at(),
        sa.ForeignKeyConstraint(
            ["organization_id", "environment_id"],
            ["environments.organization_id", "environments.id"],
            name="fk_workloads_environment_org",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_workloads_org_id"),
        sa.UniqueConstraint(
            "organization_id",
            "environment_id",
            "slug",
            name="uq_workloads_environment_slug",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "environment_id",
            "kind",
            name="uq_workloads_environment_kind",
        ),
        sa.CheckConstraint("slug = 'crusher'", name="ck_workloads_initial_slug"),
        sa.CheckConstraint("kind = 'crusher'", name="ck_workloads_initial_kind"),
    )

    op.create_table(
        "deployments",
        _id(),
        _organization_id(),
        sa.Column("workload_id", UUID, nullable=False),
        sa.Column("crusher_image", sa.Text(), nullable=False),
        sa.Column("public_domain", sa.String(length=253, collation="C"), nullable=False),
        _created_at(),
        sa.ForeignKeyConstraint(
            ["organization_id", "workload_id"],
            ["workloads.organization_id", "workloads.id"],
            name="fk_deployments_workload_org",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_deployments_org_id"),
        sa.UniqueConstraint("public_domain", name="uq_deployments_public_domain"),
    )

    op.create_table(
        "database_bindings",
        _id(),
        _organization_id(),
        sa.Column("deployment_id", UUID, nullable=False),
        sa.Column("secret_ref", sa.String(length=500), nullable=False),
        _created_at(),
        sa.ForeignKeyConstraint(
            ["organization_id", "deployment_id"],
            ["deployments.organization_id", "deployments.id"],
            name="fk_database_bindings_deployment_org",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "deployment_id",
            name="uq_database_bindings_deployment",
        ),
    )

    op.create_table(
        "storage_bindings",
        _id(),
        _organization_id(),
        sa.Column("deployment_id", UUID, nullable=False),
        sa.Column("secret_ref", sa.String(length=500), nullable=False),
        _created_at(),
        sa.ForeignKeyConstraint(
            ["organization_id", "deployment_id"],
            ["deployments.organization_id", "deployments.id"],
            name="fk_storage_bindings_deployment_org",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "deployment_id",
            name="uq_storage_bindings_deployment",
        ),
    )

    op.create_table(
        "provisioning_operations",
        _id(),
        _organization_id(),
        sa.Column("organization_slug", C_SLUG, nullable=False),
        sa.Column("idempotency_key", C_KEY, nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64, collation="C"), nullable=False),
        sa.Column("state", sa.String(length=32, collation="C"), nullable=False),
        sa.Column("actor_principal", sa.String(length=200), nullable=False),
        sa.Column("business_id", UUID, nullable=False),
        sa.Column("environment_id", UUID, nullable=False),
        sa.Column("workload_id", UUID, nullable=False),
        sa.Column("deployment_id", UUID, nullable=False),
        _created_at(),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_operations_organization",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "business_id"],
            ["businesses.organization_id", "businesses.id"],
            name="fk_operations_business_org",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "environment_id"],
            ["environments.organization_id", "environments.id"],
            name="fk_operations_environment_org",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "workload_id"],
            ["workloads.organization_id", "workloads.id"],
            name="fk_operations_workload_org",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "deployment_id"],
            ["deployments.organization_id", "deployments.id"],
            name="fk_operations_deployment_org",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_operations_org_id"),
        sa.UniqueConstraint(
            "organization_slug",
            "idempotency_key",
            name="uq_operations_org_slug_idempotency",
        ),
        sa.CheckConstraint("state = 'pending'", name="ck_operations_initial_pending"),
        sa.CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_operations_request_fingerprint",
        ),
    )

    op.create_table(
        "audit_events",
        _id(),
        _organization_id(),
        sa.Column("operation_id", UUID, nullable=False),
        sa.Column("event_type", sa.String(length=100, collation="C"), nullable=False),
        sa.Column("actor_principal", sa.String(length=200), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        _created_at(),
        sa.ForeignKeyConstraint(
            ["organization_id", "operation_id"],
            ["provisioning_operations.organization_id", "provisioning_operations.id"],
            name="fk_audit_events_operation_org",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("operation_id", "event_type", name="uq_audit_operation_event"),
        sa.CheckConstraint(
            "event_type = 'provisioning.requested'",
            name="ck_audit_initial_requested",
        ),
    )


def downgrade() -> None:
    op.drop_table("audit_events")
    op.drop_table("provisioning_operations")
    op.drop_table("storage_bindings")
    op.drop_table("database_bindings")
    op.drop_table("deployments")
    op.drop_table("workloads")
    op.drop_table("environments")
    op.drop_table("businesses")
    op.drop_table("organizations")
