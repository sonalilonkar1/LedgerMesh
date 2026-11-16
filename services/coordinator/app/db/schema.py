"""Redemption Coordinator database schema (LM-005).

The tables mirror the migrations in ``services/coordinator/migrations``; the
schema drift test fails if the two disagree. Safety rules from ADR-001 are
enforced by the database itself:

- A client idempotency key creates at most one redemption (ADR-001 section 8).
- A redemption's status must be one of the states of its coordination mode
  (ADR-001 sections 6 and 7), so a Saga can never be marked COMMIT_DECIDED.
- ``workflow_step`` and ``current_command_id`` hold what the Saga is waiting
  for, which the LM-026 result checks compare against.

The state lists below are the single source for application code; the
migration keeps its own literal copy so that it never changes after release.
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
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID

TWO_PHASE_COMMIT_STATES = (
    "REQUESTED",
    "PREPARING",
    "COMMIT_DECIDED",
    "ABORT_DECIDED",
    "COMMITTED",
    "ABORTED",
)
SAGA_STATES = (
    "REQUESTED",
    "DEDUCTING_POINTS",
    "POINTS_DEDUCTED",
    "RESERVING_INVENTORY",
    "COMPENSATING",
    "COMPLETED",
    "FAILED",
    "COMPENSATED",
)
TERMINAL_STATES = ("COMMITTED", "ABORTED", "COMPLETED", "FAILED", "COMPENSATED")


def _quoted(states: tuple[str, ...]) -> str:
    return ", ".join(f"'{state}'" for state in states)


metadata = MetaData()

redemptions = Table(
    "redemptions",
    metadata,
    Column("transaction_id", UUID(as_uuid=True), primary_key=True),
    Column("correlation_id", UUID(as_uuid=True), nullable=False),
    Column("idempotency_key", String(128), nullable=False, unique=True),
    # Hash of the create request, so the same key with a different body gets 409.
    Column("request_hash", Text, nullable=False),
    Column("user_id", Text, nullable=False),
    Column("reward_id", Text, nullable=False),
    Column("quantity", Integer, nullable=False),
    # The quote read from Catalog before starting (LM-004 reward pricing).
    Column("point_cost", BigInteger),
    Column("reward_version", BigInteger),
    Column("total_points", BigInteger),
    Column("coordination_mode", String(32), nullable=False),
    Column("status", String(32), nullable=False),
    Column("workflow_step", Integer, nullable=False, server_default=text("0")),
    # event_id of the command sent for the current step (LM-026 causation check).
    Column("current_command_id", UUID(as_uuid=True)),
    Column("fencing_token", BigInteger),
    Column("version", BigInteger, nullable=False, server_default=text("0")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("quantity > 0", name="ck_redemptions_quantity_positive"),
    CheckConstraint(
        "point_cost IS NULL OR point_cost > 0", name="ck_redemptions_point_cost_positive"
    ),
    CheckConstraint(
        "total_points IS NULL OR total_points > 0", name="ck_redemptions_total_points_positive"
    ),
    CheckConstraint("workflow_step >= 0", name="ck_redemptions_workflow_step_non_negative"),
    CheckConstraint("version >= 0", name="ck_redemptions_version_non_negative"),
    CheckConstraint(
        "coordination_mode IN ('two_phase_commit', 'saga')",
        name="ck_redemptions_coordination_mode",
    ),
    CheckConstraint(
        f"(coordination_mode = 'two_phase_commit' AND status IN "
        f"({_quoted(TWO_PHASE_COMMIT_STATES)})) "
        f"OR (coordination_mode = 'saga' AND status IN ({_quoted(SAGA_STATES)}))",
        name="ck_redemptions_status_matches_mode",
    ),
)

# Recovery scans only look at redemptions that have not finished.
Index(
    "ix_redemptions_in_flight",
    redemptions.c.updated_at,
    postgresql_where=text(f"status NOT IN ({_quoted(TERMINAL_STATES)})"),
)

redemption_transitions = Table(
    "redemption_transitions",
    metadata,
    # Append-only history of status changes, so a restart can explain each step.
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column(
        "transaction_id",
        UUID(as_uuid=True),
        ForeignKey("redemptions.transaction_id"),
        nullable=False,
    ),
    Column("from_status", String(32)),
    Column("to_status", String(32), nullable=False),
    Column("workflow_step", Integer, nullable=False),
    Column("reason", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

Index("ix_redemption_transitions_transaction_id", redemption_transitions.c.transaction_id)

inbox.to_metadata(metadata)
outbox.to_metadata(metadata)
