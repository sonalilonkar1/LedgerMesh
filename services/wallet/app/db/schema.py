"""Wallet Service database schema (LM-005).

The tables mirror the migrations in ``services/wallet/migrations``; the
schema drift test fails if the two disagree. Safety rules from ADR-001 are
enforced by the database itself, not only by application code:

- INV-01: ``available_points`` can never go below zero.
- INV-03: at most one deduction and one refund per transaction.
- ADR-001 7.1: a refund that arrives before its deduction is stored as a
  ``tombstone`` row, so the late deduction conflicts instead of applying.
"""

from ledgermesh_messaging.schema import inbox, outbox
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID

metadata = MetaData()

wallets = Table(
    "wallets",
    metadata,
    Column("user_id", Text, primary_key=True),
    Column("available_points", BigInteger, nullable=False),
    # Optimistic concurrency (ADR-001 section 8.4): every change increments it.
    Column("version", BigInteger, nullable=False, server_default=text("0")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("available_points >= 0", name="ck_wallets_available_points_non_negative"),
    CheckConstraint("version >= 0", name="ck_wallets_version_non_negative"),
)

wallet_operations = Table(
    "wallet_operations",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("transaction_id", UUID(as_uuid=True), nullable=False),
    Column("user_id", Text, ForeignKey("wallets.user_id"), nullable=False),
    Column("operation", String(16), nullable=False),
    Column("points", BigInteger, nullable=False),
    Column("status", String(16), nullable=False),
    # The command that created the row, for tracing a duplicate or late command.
    Column("event_id", UUID(as_uuid=True)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("operation IN ('deduct', 'refund')", name="ck_wallet_operations_operation"),
    CheckConstraint("status IN ('applied', 'tombstone')", name="ck_wallet_operations_status"),
    CheckConstraint("points > 0", name="ck_wallet_operations_points_positive"),
    UniqueConstraint(
        "transaction_id", "operation", name="uq_wallet_operations_transaction_operation"
    ),
)

Index("ix_wallet_operations_user_id", wallet_operations.c.user_id)

participant_transactions = Table(
    "participant_transactions",
    metadata,
    # 2PC participant state for one redemption (ADR-001 section 6).
    Column("transaction_id", UUID(as_uuid=True), primary_key=True),
    Column("state", String(16), nullable=False),
    Column("correlation_id", UUID(as_uuid=True)),
    Column("user_id", Text),
    Column("points", BigInteger),
    # Hash of the prepare request, so a different request with the same id gets 409.
    Column("request_hash", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "state IN ('PREPARED', 'COMMITTED', 'ABORTED', 'IN_DOUBT')",
        name="ck_participant_transactions_state",
    ),
    CheckConstraint(
        "points IS NULL OR points > 0", name="ck_participant_transactions_points_positive"
    ),
    # Only an abort tombstone (ABORT before PREPARE) may lack the prepared request.
    CheckConstraint(
        "state = 'ABORTED' OR (user_id IS NOT NULL AND points IS NOT NULL)",
        name="ck_participant_transactions_prepared_request",
    ),
)

inbox.to_metadata(metadata)
outbox.to_metadata(metadata)
