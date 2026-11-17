"""Application service for immutable audit events (LM-009)."""

from uuid import UUID

from ledgermesh_messaging import EventEnvelope

from services.audit_ledger.app.models import (
    AuditAppendResult,
    AuditEventRequest,
    AuditEventView,
)
from services.audit_ledger.app.repository import AuditRecord, AuditRepository


class AuditLedgerService:
    def __init__(self, repository: AuditRepository) -> None:
        self.repository = repository

    def append(self, request: AuditEventRequest) -> AuditAppendResult:
        envelope = EventEnvelope.model_validate(request.model_dump())
        record = self.repository.append(envelope)
        return AuditAppendResult(
            **self._view(record).model_dump(),
            outcome="duplicate" if record.duplicate else "appended",
        )

    def list_by_transaction(self, transaction_id: UUID) -> list[AuditEventView]:
        return [
            self._view(record) for record in self.repository.list_by_transaction(transaction_id)
        ]

    @staticmethod
    def _view(record: AuditRecord) -> AuditEventView:
        return AuditEventView(
            ledger_sequence=record.ledger_sequence,
            recorded_at=record.recorded_at,
            envelope=AuditEventRequest.model_validate(record.envelope),
        )
