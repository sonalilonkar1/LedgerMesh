"""Create the Identity schema.

Revision ID: 0001_identity_initial
Revises:
Create Date: 2026-11-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_identity_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Same append-only guard as the Audit Ledger (0001_audit_ledger_initial).
# restrict_violation (SQLSTATE 23001) is an integrity error, so callers see the
# same exception type as for any other constraint.
REJECT_CHANGES = """
CREATE FUNCTION auth_events_reject_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'auth_events is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$
"""


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("email", sa.String(254), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("email_verified_at", sa.DateTime(timezone=True)),
        sa.Column("last_login_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "email = lower(email) AND email = btrim(email)", name="ck_users_email_normalized"
        ),
        sa.CheckConstraint("position('@' in email) > 1", name="ck_users_email_shape"),
        sa.UniqueConstraint("email", name="uq_users_email"),
    )

    op.create_table(
        "login_tokens",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("token_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True)),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("expires_at > created_at", name="ck_login_tokens_expiry"),
        sa.CheckConstraint(
            "consumed_at IS NULL OR revoked_at IS NULL",
            name="ck_login_tokens_consumed_or_revoked",
        ),
        sa.UniqueConstraint("token_hash", name="uq_login_tokens_token_hash"),
    )
    op.create_index(
        "ux_login_tokens_one_active",
        "login_tokens",
        ["user_id"],
        unique=True,
        postgresql_where=sa.text("consumed_at IS NULL AND revoked_at IS NULL"),
    )

    op.create_table(
        "sessions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("session_hash", sa.String(64), nullable=False),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("idle_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("absolute_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "idle_expires_at <= absolute_expires_at", name="ck_sessions_idle_within_absolute"
        ),
        sa.CheckConstraint(
            "absolute_expires_at > created_at", name="ck_sessions_absolute_after_created"
        ),
        sa.UniqueConstraint("session_hash", name="uq_sessions_session_hash"),
    )
    op.create_index("ix_sessions_user_id", "sessions", ["user_id"])

    op.create_table(
        "rate_limit_counters",
        sa.Column("scope", sa.String(32), nullable=False),
        sa.Column("key_hash", sa.String(64), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("scope", "key_hash", "window_start", name="pk_rate_limit_counters"),
        sa.CheckConstraint(
            "scope IN ('login_email', 'login_ip', 'verify_ip')", name="ck_rate_limit_counters_scope"
        ),
        sa.CheckConstraint("count >= 1", name="ck_rate_limit_counters_count"),
    )
    op.create_index("ix_rate_limit_counters_window_start", "rate_limit_counters", ["window_start"])

    op.create_table(
        "auth_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("reason", sa.String(32)),
        sa.Column("user_id", postgresql.UUID(as_uuid=True)),
        sa.Column("session_id", postgresql.UUID(as_uuid=True)),
        sa.Column("email_masked", sa.String(254)),
        sa.Column("client_ip", postgresql.INET()),
        sa.CheckConstraint(
            "event_type IN ('login_requested', 'login_succeeded', 'login_failed', 'logout', "
            "'rate_limited')",
            name="ck_auth_events_event_type",
        ),
    )
    op.create_index("ix_auth_events_occurred_at", "auth_events", ["occurred_at"])
    op.create_index("ix_auth_events_user_id", "auth_events", ["user_id"])

    op.execute(REJECT_CHANGES)
    op.execute(
        "CREATE TRIGGER auth_events_append_only BEFORE UPDATE OR DELETE ON auth_events "
        "FOR EACH ROW EXECUTE FUNCTION auth_events_reject_change()"
    )
    op.execute(
        "CREATE TRIGGER auth_events_no_truncate BEFORE TRUNCATE ON auth_events "
        "FOR EACH STATEMENT EXECUTE FUNCTION auth_events_reject_change()"
    )


def downgrade() -> None:
    # Dropping a table drops its indexes and triggers; the function goes separately.
    op.drop_table("auth_events")
    op.execute("DROP FUNCTION auth_events_reject_change()")
    op.drop_table("rate_limit_counters")
    op.drop_table("sessions")
    op.drop_table("login_tokens")
    op.drop_table("users")
