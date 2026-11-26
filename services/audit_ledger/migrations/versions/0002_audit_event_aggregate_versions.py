"""Track applied aggregate versions for the Audit Ledger consumer.

Revision ID: 0002_audit_agg_versions
Revises: 0001_audit_ledger_initial
Create Date: 2026-11-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_audit_agg_versions"
down_revision: str | None = "0001_audit_ledger_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "aggregate_versions",
        sa.Column("consumer", sa.String(length=255), nullable=False),
        sa.Column("aggregate_type", sa.String(length=255), nullable=False),
        sa.Column("aggregate_id", sa.Text(), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("consumer", "aggregate_type", "aggregate_id"),
    )


def downgrade() -> None:
    op.drop_table("aggregate_versions")
