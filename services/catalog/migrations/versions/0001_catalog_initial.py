"""Create the Rewards Catalog schema.

Revision ID: 0001_catalog_initial
Revises:
Create Date: 2026-11-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_catalog_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "rewards",
        sa.Column("reward_id", sa.Text(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("point_cost", sa.BigInteger(), nullable=False),
        sa.Column("total_inventory", sa.Integer(), nullable=False),
        sa.Column("available_inventory", sa.Integer(), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint("point_cost >= 1", name="ck_rewards_point_cost_positive"),
        sa.CheckConstraint("total_inventory >= 0", name="ck_rewards_total_inventory_non_negative"),
        sa.CheckConstraint(
            "available_inventory >= 0", name="ck_rewards_available_inventory_non_negative"
        ),
        sa.CheckConstraint(
            "available_inventory <= total_inventory", name="ck_rewards_available_within_total"
        ),
        sa.CheckConstraint("version >= 0", name="ck_rewards_version_non_negative"),
    )

    op.create_table(
        "reservations",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("transaction_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("reward_id", sa.Text(), sa.ForeignKey("rewards.reward_id"), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "status IN ('reserved', 'released', 'fulfilled', 'tombstone')",
            name="ck_reservations_status",
        ),
        sa.CheckConstraint("quantity > 0", name="ck_reservations_quantity_positive"),
        sa.UniqueConstraint("transaction_id", name="uq_reservations_transaction"),
    )
    op.create_index("ix_reservations_reward_id", "reservations", ["reward_id"])

    op.create_table(
        "participant_transactions",
        sa.Column("transaction_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True)),
        sa.Column("reward_id", sa.Text()),
        sa.Column("quantity", sa.Integer()),
        sa.Column("request_hash", sa.Text()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "state IN ('PREPARED', 'COMMITTED', 'ABORTED', 'IN_DOUBT')",
            name="ck_participant_transactions_state",
        ),
        sa.CheckConstraint(
            "quantity IS NULL OR quantity > 0",
            name="ck_participant_transactions_quantity_positive",
        ),
        sa.CheckConstraint(
            "(state = 'ABORTED' AND correlation_id IS NULL AND request_hash IS NULL "
            "AND reward_id IS NULL AND quantity IS NULL) "
            "OR (correlation_id IS NOT NULL AND request_hash IS NOT NULL "
            "AND reward_id IS NOT NULL AND quantity IS NOT NULL)",
            name="ck_participant_transactions_request_identity",
        ),
    )

    create_inbox()
    create_outbox()


def downgrade() -> None:
    op.drop_table("outbox")
    op.drop_table("inbox")
    op.drop_table("participant_transactions")
    op.drop_index("ix_reservations_reward_id", table_name="reservations")
    op.drop_table("reservations")
    op.drop_table("rewards")


# Snapshot of the ledgermesh-messaging inbox and outbox tables (LM-019). A
# migration must not import the live library schema, or a later library change
# would silently rewrite this revision.


def create_inbox() -> None:
    op.create_table(
        "inbox",
        sa.Column("consumer", sa.String(255), primary_key=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("transaction_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column(
            "result_event_ids",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def create_outbox() -> None:
    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True),
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("message_key", sa.Text(), nullable=False),
        sa.Column("envelope", postgresql.JSONB(), nullable=False),
        sa.Column(
            "headers", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text()),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
