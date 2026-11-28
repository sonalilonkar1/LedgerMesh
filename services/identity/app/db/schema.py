"""Identity Service database schema (LM-052).

The tables mirror the migrations in ``services/identity/migrations``; the
schema drift test fails if the two disagree.

The service stores no credential a user chooses. A user proves ownership of a
mailbox by presenting a single-use login token sent to it; the database keeps
only the SHA-256 hash of that token and of every session identifier, so a
database dump cannot be replayed as a login.

``auth_events`` is append-only: the migration installs a trigger that rejects
every UPDATE, DELETE and TRUNCATE. It never stores a token, a session secret or
a full email address.
"""

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import INET, UUID

metadata = MetaData()

EVENT_TYPES = ("login_requested", "login_succeeded", "login_failed", "logout", "rate_limited")
RATE_LIMIT_SCOPES = ("login_email", "login_ip", "verify_ip")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(value) for value in values)})"


users = Table(
    "users",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    # Normalized (see app/email_address.py); the unique constraint is what makes
    # one mailbox one account, whatever case the user typed.
    Column("email", String(254), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # Set by the first successful login: syntax validation alone proves nothing.
    Column("email_verified_at", DateTime(timezone=True)),
    Column("last_login_at", DateTime(timezone=True)),
    CheckConstraint(
        "email = lower(email) AND email = btrim(email)", name="ck_users_email_normalized"
    ),
    CheckConstraint("position('@' in email) > 1", name="ck_users_email_shape"),
    UniqueConstraint("email", name="uq_users_email"),
)

login_tokens = Table(
    "login_tokens",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column(
        "user_id", UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    ),
    # Hex SHA-256 of the emailed token; the token itself is never stored.
    Column("token_hash", String(64), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("consumed_at", DateTime(timezone=True)),
    Column("revoked_at", DateTime(timezone=True)),
    CheckConstraint("expires_at > created_at", name="ck_login_tokens_expiry"),
    CheckConstraint(
        "consumed_at IS NULL OR revoked_at IS NULL", name="ck_login_tokens_consumed_or_revoked"
    ),
    UniqueConstraint("token_hash", name="uq_login_tokens_token_hash"),
)
# At most one usable link per user: requesting a new one revokes the old one.
Index(
    "ux_login_tokens_one_active",
    login_tokens.c.user_id,
    unique=True,
    postgresql_where=text("consumed_at IS NULL AND revoked_at IS NULL"),
)

sessions = Table(
    "sessions",
    metadata,
    # The row id is not secret; audit events refer to it.
    Column("id", UUID(as_uuid=True), primary_key=True),
    # Hex SHA-256 of the cookie value.
    Column("session_hash", String(64), nullable=False),
    Column(
        "user_id", UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    ),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("idle_expires_at", DateTime(timezone=True), nullable=False),
    Column("absolute_expires_at", DateTime(timezone=True), nullable=False),
    Column("revoked_at", DateTime(timezone=True)),
    CheckConstraint(
        "idle_expires_at <= absolute_expires_at", name="ck_sessions_idle_within_absolute"
    ),
    CheckConstraint("absolute_expires_at > created_at", name="ck_sessions_absolute_after_created"),
    UniqueConstraint("session_hash", name="uq_sessions_session_hash"),
)
Index("ix_sessions_user_id", sessions.c.user_id)

rate_limit_counters = Table(
    "rate_limit_counters",
    metadata,
    Column("scope", String(32), nullable=False),
    # SHA-256 of the scope and the normalized email or client IP: no plaintext.
    Column("key_hash", String(64), nullable=False),
    Column("window_start", DateTime(timezone=True), nullable=False),
    Column("count", Integer, nullable=False),
    PrimaryKeyConstraint("scope", "key_hash", "window_start", name="pk_rate_limit_counters"),
    CheckConstraint(_in("scope", RATE_LIMIT_SCOPES), name="ck_rate_limit_counters_scope"),
    CheckConstraint("count >= 1", name="ck_rate_limit_counters_count"),
)
Index("ix_rate_limit_counters_window_start", rate_limit_counters.c.window_start)

auth_events = Table(
    "auth_events",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("occurred_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("event_type", String(32), nullable=False),
    # login_failed: unknown, expired, used or revoked; rate_limited: the scope.
    Column("reason", String(32)),
    # No foreign key: the audit trail must not change if a user row is removed.
    Column("user_id", UUID(as_uuid=True)),
    Column("session_id", UUID(as_uuid=True)),
    # Only when no user id is known (a per-email rate limit), e.g. d***@example.com.
    Column("email_masked", String(254)),
    Column("client_ip", INET),
    CheckConstraint(_in("event_type", EVENT_TYPES), name="ck_auth_events_event_type"),
)
Index("ix_auth_events_occurred_at", auth_events.c.occurred_at)
Index("ix_auth_events_user_id", auth_events.c.user_id)
