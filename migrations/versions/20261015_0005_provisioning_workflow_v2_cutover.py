"""Cut new provisioning dispatches over to workflow v2.

Revision ID: 20261015_0005
Revises: 20261013_0004
Create Date: 2026-10-15
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "20261015_0005"
down_revision: str | None = "20261013_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

WORKFLOW_TYPE_V1 = "fstack.provision_crusher.v1"
WORKFLOW_TYPE_V2 = "fstack.provision_crusher.v2"
CUTOVER_LOCK_SQL = (
    "LOCK TABLE provisioning_operations, workflow_dispatches IN ACCESS EXCLUSIVE MODE NOWAIT"
)


def _install_enqueue_function(workflow_type: str) -> None:
    op.execute(CUTOVER_LOCK_SQL)
    op.execute("SET CONSTRAINTS ALL IMMEDIATE")
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
                '{workflow_type}',
                'pending',
                NEW.created_at
            );
            RETURN NEW;
        END;
        $$
        """
    )


def upgrade() -> None:
    _install_enqueue_function(WORKFLOW_TYPE_V2)


def downgrade() -> None:
    _install_enqueue_function(WORKFLOW_TYPE_V1)
