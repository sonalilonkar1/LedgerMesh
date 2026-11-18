"""Audit Ledger domain errors and stable HTTP problem details."""

from uuid import UUID


class AuditLedgerError(Exception):
    status = 500
    title = "Audit Ledger operation failed"
    problem_type = "urn:ledgermesh:audit-ledger:error"

    def __init__(self, detail: str, event_id: UUID | None = None) -> None:
        super().__init__(detail)
        self.detail = detail
        self.event_id = event_id


class AuditEventConflict(AuditLedgerError):
    status = 409
    title = "Audit event conflict"
    problem_type = "urn:ledgermesh:audit-ledger:event-conflict"
