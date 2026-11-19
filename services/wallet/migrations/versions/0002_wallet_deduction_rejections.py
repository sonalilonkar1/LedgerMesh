"""Remember rejected Saga deductions per transaction (LM-020).

Revision ID: 0002_wallet_deduction_rejections
Revises: 0001_wallet_initial
Create Date: 2026-11-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_wallet_deduction_rejections"
down_revision: str | None = "0001_wallet_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "deduction_rejections",
        sa.Column("transaction_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("points", sa.BigInteger(), nullable=False),
        sa.Column("reason", sa.String(32), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint("points > 0", name="ck_deduction_rejections_points_positive"),
        sa.CheckConstraint(
            "reason IN ('insufficient_points', 'wallet_not_found')",
            name="ck_deduction_rejections_reason",
        ),
    )


def downgrade() -> None:
    op.drop_table("deduction_rejections")
