"""Inbox, outbox, aggregate version and workflow step tables.

Each service creates these tables in its own database. Services include
``metadata`` in their Alembic ``target_metadata`` (LM-005) rather than copying
the definitions.
"""

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
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

outbox = Table(
    "outbox",
    metadata,
    # Publication order is ascending id, so ids must be assigned at insert time.
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("event_id", UUID(as_uuid=True), nullable=False, unique=True),
    Column("topic", String(255), nullable=False),
    Column("message_key", Text, nullable=False),
    Column("envelope", JSONB, nullable=False),
    Column("headers", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("published_at", DateTime(timezone=True)),
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("last_error", Text),
)

Index(
    "ix_outbox_unpublished",
    outbox.c.id,
    postgresql_where=outbox.c.published_at.is_(None),
)

inbox = Table(
    "inbox",
    metadata,
    # The composite key is what makes a duplicate delivery a no-op: a second
    # insert for the same (consumer, event_id) conflicts instead of re-running
    # the business update.
    Column("consumer", String(255), primary_key=True),
    Column("event_id", UUID(as_uuid=True), primary_key=True),
    Column("transaction_id", UUID(as_uuid=True), nullable=False),
    Column("event_type", String(255), nullable=False),
    Column("result_event_ids", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("processed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

aggregate_versions = Table(
    "aggregate_versions",
    metadata,
    # The latest version each consumer has applied per aggregate (LM-026).
    # Consumers track versions independently, like their inbox rows.
    Column("consumer", String(255), primary_key=True),
    Column("aggregate_type", String(255), primary_key=True),
    Column("aggregate_id", Text, primary_key=True),
    Column("version", BigInteger, nullable=False),
    Column("event_id", UUID(as_uuid=True)),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

workflow_steps = Table(
    "workflow_steps",
    metadata,
    # The highest workflow step a participant has applied per transaction (LM-026).
    Column("consumer", String(255), primary_key=True),
    Column("transaction_id", UUID(as_uuid=True), primary_key=True),
    Column("step", Integer, nullable=False),
    Column("event_id", UUID(as_uuid=True)),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
