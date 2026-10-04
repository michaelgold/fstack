"""Add provisioning operation lifecycle foundations.

Revision ID: 20261004_0002
Revises: 20261003_0001
Create Date: 2026-10-04
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20261004_0002"
down_revision: str | None = "20261003_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)


def upgrade() -> None:
    op.add_column(
        "provisioning_operations",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=True,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    op.add_column(
        "provisioning_operations",
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "provisioning_operations",
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
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
        sa.Column("last_error_code", sa.String(length=64, collation="C"), nullable=True),
    )
    op.add_column(
        "provisioning_operations",
        sa.Column("last_error_retryable", sa.Boolean(), nullable=True),
    )

    op.execute("UPDATE provisioning_operations SET updated_at = created_at")
    op.alter_column("provisioning_operations", "updated_at", nullable=False)

    op.drop_constraint(
        "ck_operations_initial_pending",
        "provisioning_operations",
        type_="check",
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
    op.create_check_constraint(
        "ck_operations_error_code_sanitized",
        "provisioning_operations",
        "last_error_code IS NULL OR last_error_code ~ '^[a-z][a-z0-9_]{0,63}$'",
    )
    op.create_check_constraint(
        "ck_operations_timestamps_monotonic",
        "provisioning_operations",
        """
        updated_at >= created_at
        AND (
            started_at IS NULL
            OR (started_at >= created_at AND updated_at >= started_at)
        )
        AND (
            completed_at IS NULL
            OR (
                started_at IS NOT NULL
                AND completed_at >= started_at
                AND updated_at >= completed_at
            )
        )
        """,
    )
    op.create_unique_constraint(
        "uq_operations_org_id_deployment",
        "provisioning_operations",
        ["organization_id", "id", "deployment_id"],
    )

    op.drop_constraint("ck_audit_initial_requested", "audit_events", type_="check")
    op.create_check_constraint(
        "ck_audit_provisioning_lifecycle_event",
        "audit_events",
        """
        event_type IN (
            'provisioning.requested',
            'provisioning.started',
            'provisioning.succeeded',
            'provisioning.failed'
        )
        """,
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_audit_provisioning_lifecycle_event",
        "audit_events",
        type_="check",
    )
    op.create_check_constraint(
        "ck_audit_initial_requested",
        "audit_events",
        "event_type = 'provisioning.requested'",
    )

    op.drop_constraint(
        "uq_operations_org_id_deployment",
        "provisioning_operations",
        type_="unique",
    )
    op.drop_constraint(
        "ck_operations_timestamps_monotonic",
        "provisioning_operations",
        type_="check",
    )
    op.drop_constraint(
        "ck_operations_error_code_sanitized",
        "provisioning_operations",
        type_="check",
    )
    op.drop_constraint(
        "ck_operations_attempt_count_nonnegative",
        "provisioning_operations",
        type_="check",
    )
    op.drop_constraint(
        "ck_operations_lifecycle_bundle",
        "provisioning_operations",
        type_="check",
    )
    op.create_check_constraint(
        "ck_operations_initial_pending",
        "provisioning_operations",
        "state = 'pending'",
    )

    op.drop_column("provisioning_operations", "last_error_retryable")
    op.drop_column("provisioning_operations", "last_error_code")
    op.drop_column("provisioning_operations", "current_attempt_id")
    op.drop_column("provisioning_operations", "attempt_count")
    op.drop_column("provisioning_operations", "completed_at")
    op.drop_column("provisioning_operations", "started_at")
    op.drop_column("provisioning_operations", "updated_at")
