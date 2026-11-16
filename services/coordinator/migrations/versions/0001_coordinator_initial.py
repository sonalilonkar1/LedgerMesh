"""Create the Redemption Coordinator schema.

Revision ID: 0001_coordinator_initial
Revises:
Create Date: 2026-11-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_coordinator_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STATUS_MATCHES_MODE = (
    "(coordination_mode = 'two_phase_commit' AND status IN "
    "('REQUESTED', 'PREPARING', 'COMMIT_DECIDED', 'ABORT_DECIDED', 'COMMITTED', 'ABORTED')) "
    "OR (coordination_mode = 'saga' AND status IN "
    "('REQUESTED', 'DEDUCTING_POINTS', 'POINTS_DEDUCTED', 'RESERVING_INVENTORY', "
    "'COMPENSATING', 'COMPLETED', 'FAILED', 'COMPENSATED'))"
)
IN_FLIGHT = "status NOT IN ('COMMITTED', 'ABORTED', 'COMPLETED', 'FAILED', 'COMPENSATED')"


def upgrade() -> None:
    op.create_table(
        "redemptions",
        sa.Column("transaction_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False, unique=True),
        sa.Column("request_hash", sa.Text(), nullable=False),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("reward_id", sa.Text(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("point_cost", sa.BigInteger()),
        sa.Column("reward_version", sa.BigInteger()),
        sa.Column("total_points", sa.BigInteger()),
        sa.Column("coordination_mode", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("workflow_step", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("current_command_id", postgresql.UUID(as_uuid=True)),
        sa.Column("fencing_token", sa.BigInteger()),
        sa.Column("version", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint("quantity > 0", name="ck_redemptions_quantity_positive"),
        sa.CheckConstraint(
            "point_cost IS NULL OR point_cost > 0", name="ck_redemptions_point_cost_positive"
        ),
        sa.CheckConstraint(
            "total_points IS NULL OR total_points > 0",
            name="ck_redemptions_total_points_positive",
        ),
        sa.CheckConstraint("workflow_step >= 0", name="ck_redemptions_workflow_step_non_negative"),
        sa.CheckConstraint("version >= 0", name="ck_redemptions_version_non_negative"),
        sa.CheckConstraint(
            "coordination_mode IN ('two_phase_commit', 'saga')",
            name="ck_redemptions_coordination_mode",
        ),
        sa.CheckConstraint(STATUS_MATCHES_MODE, name="ck_redemptions_status_matches_mode"),
    )
    op.create_index(
        "ix_redemptions_in_flight",
        "redemptions",
        ["updated_at"],
        postgresql_where=sa.text(IN_FLIGHT),
    )

    op.create_table(
        "redemption_transitions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "transaction_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("redemptions.transaction_id"),
            nullable=False,
        ),
        sa.Column("from_status", sa.String(32)),
        sa.Column("to_status", sa.String(32), nullable=False),
        sa.Column("workflow_step", sa.Integer(), nullable=False),
        sa.Column("reason", sa.Text()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    )
    op.create_index(
        "ix_redemption_transitions_transaction_id",
        "redemption_transitions",
        ["transaction_id"],
    )

    create_inbox()
    create_outbox()


def downgrade() -> None:
    op.drop_table("outbox")
    op.drop_table("inbox")
    op.drop_index("ix_redemption_transitions_transaction_id", table_name="redemption_transitions")
    op.drop_table("redemption_transitions")
    op.drop_index("ix_redemptions_in_flight", table_name="redemptions")
    op.drop_table("redemptions")


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
