"""Rewards Catalog Service database schema (LM-005).

The tables mirror the migrations in ``services/catalog/migrations``; the
schema drift test fails if the two disagree. Safety rules from ADR-001 are
enforced by the database itself:

- INV-02: available inventory is never negative and never exceeds the total.
- INV-03: at most one reservation per transaction (a redemption has one reward).
- ADR-001 7.1: a release that arrives before its reservation is stored as a
  ``tombstone`` row, so the late reservation conflicts instead of applying.
"""

from ledgermesh_messaging.schema import inbox, outbox
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
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

rewards = Table(
    "rewards",
    metadata,
    Column("reward_id", Text, primary_key=True),
    Column("name", Text, nullable=False),
    Column("point_cost", BigInteger, nullable=False),
    Column("total_inventory", Integer, nullable=False),
    Column("available_inventory", Integer, nullable=False),
    # Optimistic concurrency (ADR-001 section 8.4); also the reward version that
    # Catalog prepare compares with expected_reward_version (LM-004).
    Column("version", BigInteger, nullable=False, server_default=text("0")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("point_cost >= 1", name="ck_rewards_point_cost_positive"),
    CheckConstraint("total_inventory >= 0", name="ck_rewards_total_inventory_non_negative"),
    CheckConstraint("available_inventory >= 0", name="ck_rewards_available_inventory_non_negative"),
    CheckConstraint(
        "available_inventory <= total_inventory", name="ck_rewards_available_within_total"
    ),
    CheckConstraint("version >= 0", name="ck_rewards_version_non_negative"),
)

reservations = Table(
    "reservations",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("transaction_id", UUID(as_uuid=True), nullable=False),
    Column("reward_id", Text, ForeignKey("rewards.reward_id"), nullable=False),
    Column("quantity", Integer, nullable=False),
    Column("status", String(16), nullable=False),
    # The command that created the row, for tracing a duplicate or late command.
    Column("event_id", UUID(as_uuid=True)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "status IN ('reserved', 'released', 'fulfilled', 'tombstone')",
        name="ck_reservations_status",
    ),
    CheckConstraint("quantity > 0", name="ck_reservations_quantity_positive"),
    # A redemption has exactly one reward (LM-004), so a transaction can reserve
    # once in total: never twice, and never a second, different reward.
    UniqueConstraint("transaction_id", name="uq_reservations_transaction"),
)

Index("ix_reservations_reward_id", reservations.c.reward_id)

participant_transactions = Table(
    "participant_transactions",
    metadata,
    # 2PC participant state for one redemption (ADR-001 section 6).
    Column("transaction_id", UUID(as_uuid=True), primary_key=True),
    Column("state", String(16), nullable=False),
    Column("correlation_id", UUID(as_uuid=True)),
    Column("reward_id", Text),
    Column("quantity", Integer),
    # Hash of the prepare request, so a different request with the same id gets 409.
    Column("request_hash", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "state IN ('PREPARED', 'COMMITTED', 'ABORTED', 'IN_DOUBT')",
        name="ck_participant_transactions_state",
    ),
    CheckConstraint(
        "quantity IS NULL OR quantity > 0", name="ck_participant_transactions_quantity_positive"
    ),
    # Exactly two shapes: an empty abort tombstone (ABORT before any PREPARE), or
    # a row that carries the full prepared request, whatever its state.
    CheckConstraint(
        "(state = 'ABORTED' AND correlation_id IS NULL AND request_hash IS NULL "
        "AND reward_id IS NULL AND quantity IS NULL) "
        "OR (correlation_id IS NOT NULL AND request_hash IS NOT NULL "
        "AND reward_id IS NOT NULL AND quantity IS NOT NULL)",
        name="ck_participant_transactions_request_identity",
    ),
)

inbox.to_metadata(metadata)
outbox.to_metadata(metadata)
