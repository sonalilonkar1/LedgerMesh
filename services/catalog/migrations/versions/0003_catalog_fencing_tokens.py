"""Persist the Catalog participant fencing high-water mark and rejections.

Revision ID: 0003_catalog_fencing_tokens
Revises: 0002_catalog_saga_outcomes
Create Date: 2026-11-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_catalog_fencing_tokens"
down_revision: str | None = "0002_catalog_saga_outcomes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "participant_fencing_state",
        sa.Column("singleton_id", sa.Integer(), primary_key=True),
        sa.Column("highest_token", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint("singleton_id = 1", name="ck_catalog_fencing_singleton"),
        sa.CheckConstraint("highest_token >= 0", name="ck_catalog_fencing_token_non_negative"),
    )
    op.create_table(
        "fencing_rejections",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("transaction_id", sa.Uuid(), nullable=False),
        sa.Column("supplied_token", sa.BigInteger(), nullable=False),
        sa.Column("highest_token", sa.BigInteger(), nullable=False),
        sa.Column("command_type", sa.String(64), nullable=False),
        sa.Column("coordination_mode", sa.String(32), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "supplied_token >= 0", name="ck_catalog_fencing_rejection_supplied_non_negative"
        ),
        sa.CheckConstraint(
            "highest_token >= 0", name="ck_catalog_fencing_rejection_highest_non_negative"
        ),
    )


def downgrade() -> None:
    op.drop_table("fencing_rejections")
    op.drop_table("participant_fencing_state")
