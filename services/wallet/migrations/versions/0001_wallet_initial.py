"""Create the Wallet schema.

Revision ID: 0001_wallet_initial
Revises:
Create Date: 2026-11-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_wallet_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "wallets",
        sa.Column("user_id", sa.Text(), primary_key=True),
        sa.Column("available_points", sa.BigInteger(), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "available_points >= 0", name="ck_wallets_available_points_non_negative"
        ),
        sa.CheckConstraint("version >= 0", name="ck_wallets_version_non_negative"),
    )

    op.create_table(
        "wallet_operations",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("transaction_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", sa.Text(), sa.ForeignKey("wallets.user_id"), nullable=False),
        sa.Column("operation", sa.String(16), nullable=False),
        sa.Column("points", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "operation IN ('deduct', 'refund')", name="ck_wallet_operations_operation"
        ),
        sa.CheckConstraint(
            "status IN ('applied', 'tombstone')", name="ck_wallet_operations_status"
        ),
        sa.CheckConstraint("points > 0", name="ck_wallet_operations_points_positive"),
        sa.CheckConstraint(
            "status <> 'tombstone' OR operation = 'deduct'",
            name="ck_wallet_operations_tombstone_blocks_deduct",
        ),
        sa.UniqueConstraint(
            "transaction_id", "operation", name="uq_wallet_operations_transaction_operation"
        ),
    )
    op.create_index("ix_wallet_operations_user_id", "wallet_operations", ["user_id"])

    op.create_table(
        "participant_transactions",
        sa.Column("transaction_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True)),
        sa.Column("user_id", sa.Text()),
        sa.Column("points", sa.BigInteger()),
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
            "points IS NULL OR points > 0", name="ck_participant_transactions_points_positive"
        ),
        sa.CheckConstraint(
            "(state = 'ABORTED' AND correlation_id IS NULL AND request_hash IS NULL "
            "AND user_id IS NULL AND points IS NULL) "
            "OR (correlation_id IS NOT NULL AND request_hash IS NOT NULL "
            "AND user_id IS NOT NULL AND points IS NOT NULL)",
            name="ck_participant_transactions_request_identity",
        ),
    )

    create_inbox()
    create_outbox()


def downgrade() -> None:
    op.drop_table("outbox")
    op.drop_table("inbox")
    op.drop_table("participant_transactions")
    op.drop_index("ix_wallet_operations_user_id", table_name="wallet_operations")
    op.drop_table("wallet_operations")
    op.drop_table("wallets")


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
