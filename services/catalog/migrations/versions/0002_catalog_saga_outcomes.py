"""Persist Catalog Saga command outcomes.

Revision ID: 0002_catalog_saga_outcomes
Revises: 0001_catalog_initial
Create Date: 2026-11-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_catalog_saga_outcomes"
down_revision: str | None = "0001_catalog_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "catalog_saga_outcomes",
        sa.Column("transaction_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("command_type", sa.String(32), primary_key=True),
        sa.Column("reward_id", sa.Text(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("result_event_type", sa.String(255), nullable=False),
        sa.Column("result_payload", postgresql.JSONB(), nullable=False),
        sa.Column("aggregate_version", sa.BigInteger()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "command_type IN ('reserve', 'release')",
            name="ck_catalog_saga_outcomes_command_type",
        ),
        sa.CheckConstraint("quantity > 0", name="ck_catalog_saga_outcomes_quantity_positive"),
        sa.CheckConstraint(
            "aggregate_version IS NULL OR aggregate_version >= 0",
            name="ck_catalog_saga_outcomes_version_non_negative",
        ),
    )


def downgrade() -> None:
    op.drop_table("catalog_saga_outcomes")
