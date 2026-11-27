from functools import lru_cache
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from ledgermesh_messaging import CorrelationMiddleware, configure_logging
from ledgermesh_messaging.safeguards import install_safeguards

from services.audit_ledger.app.errors import AuditLedgerError
from services.audit_ledger.app.models import (
    AuditAppendResult,
    AuditEventRequest,
    AuditEventView,
    Problem,
)
from services.audit_ledger.app.repository import PostgresAuditRepository, database_url, get_engine
from services.audit_ledger.app.service import AuditLedgerService

SERVICE_NAME = "audit-ledger"
configure_logging(SERVICE_NAME)  # LM-032: JSON logs with shared field names

app = FastAPI(title="LedgerMesh Audit Ledger Service", version="0.1.0")
app.add_middleware(CorrelationMiddleware)

# LM-042: internal routes and docs closed outside local/test, safe error bodies.
install_safeguards(app, SERVICE_NAME, database_url=database_url)


@lru_cache
def get_audit_ledger_service() -> AuditLedgerService:
    return AuditLedgerService(PostgresAuditRepository(get_engine()))


AuditLedgerServiceDependency = Annotated[AuditLedgerService, Depends(get_audit_ledger_service)]


@app.exception_handler(AuditLedgerError)
async def audit_error_handler(_request: Request, error: AuditLedgerError) -> JSONResponse:
    problem = Problem(
        type=error.problem_type,
        title=error.title,
        status=error.status,
        detail=error.detail,
        event_id=error.event_id,
    )
    return JSONResponse(
        status_code=error.status,
        content=problem.model_dump(mode="json"),
        media_type="application/problem+json",
    )


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.post(
    "/v1/audit-events",
    response_model=AuditAppendResult,
    responses={409: {"model": Problem}},
    tags=["audit-events"],
)
def append_event(
    request: AuditEventRequest,
    service: AuditLedgerServiceDependency,
) -> AuditAppendResult:
    return service.append(request)


@app.get(
    "/v1/transactions/{transaction_id}/events",
    response_model=list[AuditEventView],
    tags=["audit-events"],
)
def list_transaction_events(
    transaction_id: UUID,
    service: AuditLedgerServiceDependency,
) -> list[AuditEventView]:
    return service.list_by_transaction(transaction_id)
