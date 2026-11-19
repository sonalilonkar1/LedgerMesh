"""PostgreSQL repository for immutable audit events (LM-009)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Protocol
from uuid import UUID

from ledgermesh_messaging import EventEnvelope
from sqlalchemy import Connection, Engine, create_engine, select
from sqlalchemy.dialects.postgresql import insert

from services.audit_ledger.app.db.schema import audit_events
from services.audit_ledger.app.errors import AuditEventConflict


@dataclass(frozen=True)
class AuditRecord:
    ledger_sequence: int
    recorded_at: datetime
    envelope: dict
    duplicate: bool = False


class AuditRepository(Protocol):
    def append(
        self,
        envelope: EventEnvelope,
        *,
        connection: Connection | None = None,
    ) -> AuditRecord: ...

    def list_by_transaction(self, transaction_id: UUID) -> list[AuditRecord]: ...


def database_url() -> str:
    if override := os.environ.get("AUDIT_LEDGER_DATABASE_URL") or os.environ.get("DATABASE_URL"):
        return override
    password = os.environ.get("POSTGRES_PASSWORD", "ledgermesh_dev")
    return f"postgresql+psycopg://audit_ledger:{password}@audit-ledger-db:5432/audit_ledger"


@lru_cache
def get_engine(url: str | None = None) -> Engine:
    return create_engine(url or database_url(), pool_pre_ping=True)


class PostgresAuditRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def append(
        self,
        envelope: EventEnvelope,
        *,
        connection: Connection | None = None,
    ) -> AuditRecord:
        if connection is not None:
            return self._append(connection, envelope)
        with self.engine.begin() as owned_connection:
            return self._append(owned_connection, envelope)

    def _append(self, connection: Connection, envelope: EventEnvelope) -> AuditRecord:
        document = envelope.to_json()
        row = connection.execute(
            insert(audit_events)
            .values(
                event_id=envelope.event_id,
                transaction_id=envelope.transaction_id,
                correlation_id=envelope.correlation_id,
                causation_id=envelope.causation_id,
                event_type=envelope.event_type,
                aggregate_type=envelope.aggregate_type,
                aggregate_id=envelope.aggregate_id,
                aggregate_version=envelope.aggregate_version,
                workflow_step=envelope.workflow_step,
                occurred_at=envelope.occurred_at,
                envelope=document,
            )
            .on_conflict_do_nothing(index_elements=[audit_events.c.event_id])
            .returning(*audit_events.c)
        ).one_or_none()
        if row is not None:
            return self._record(row)

        existing = connection.execute(
            select(audit_events).where(audit_events.c.event_id == envelope.event_id)
        ).one()
        if existing.envelope != document:
            raise AuditEventConflict(
                "The event ID is already stored with different envelope content.",
                envelope.event_id,
            )
        return self._record(existing, duplicate=True)

    def list_by_transaction(self, transaction_id: UUID) -> list[AuditRecord]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                select(audit_events)
                .where(audit_events.c.transaction_id == transaction_id)
                .order_by(audit_events.c.id.asc())
            ).all()
        return [self._record(row) for row in rows]

    @staticmethod
    def _record(row, *, duplicate: bool = False) -> AuditRecord:
        return AuditRecord(
            ledger_sequence=row.id,
            recorded_at=row.recorded_at,
            envelope=row.envelope,
            duplicate=duplicate,
        )
