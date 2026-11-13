"""Audit Ledger Service database schema (LM-005).

The tables mirror the migrations in ``services/audit_ledger/migrations``; the
schema drift test fails if the two disagree.

``audit_events`` is append-only: the migration installs a trigger that rejects
every UPDATE, DELETE and TRUNCATE, so history cannot be rewritten even by a
bug in the service. Each ``event_id`` is recorded once, which makes appends
idempotent under at-least-once delivery.
"""

from ledgermesh_messaging.schema import inbox
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

audit_events = Table(
    "audit_events",
    metadata,
    # Ascending id is the order the ledger recorded events in.
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("event_id", UUID(as_uuid=True), nullable=False, unique=True),
    Column("transaction_id", UUID(as_uuid=True), nullable=False),
    Column("correlation_id", UUID(as_uuid=True), nullable=False),
    Column("causation_id", UUID(as_uuid=True)),
    Column("event_type", String(255), nullable=False),
    Column("aggregate_type", String(255), nullable=False),
    Column("aggregate_id", Text, nullable=False),
    Column("aggregate_version", BigInteger),
    Column("workflow_step", Integer),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    Column("recorded_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # The full envelope as received, so nothing is lost if new fields appear.
    Column("envelope", JSONB, nullable=False),
)

Index("ix_audit_events_transaction_id", audit_events.c.transaction_id)

inbox.to_metadata(metadata)
