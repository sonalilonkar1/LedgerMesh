from functools import lru_cache
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, Response
from ledgermesh_messaging import MetricsMiddleware, render_metrics

from services.wallet.app.errors import WalletError
from services.wallet.app.models import (
    DeductRequest,
    ParticipantResponse,
    Problem,
    RefundRequest,
    WalletBalance,
    WalletOperationResult,
    WalletPrepareRequest,
)
from services.wallet.app.participant import PostgresWalletParticipantRepository
from services.wallet.app.participant_service import WalletParticipantService
from services.wallet.app.repository import PostgresWalletRepository, get_engine
from services.wallet.app.service import WalletService

SERVICE_NAME = "wallet"

app = FastAPI(title="LedgerMesh Wallet Service", version="0.1.0")
app.add_middleware(MetricsMiddleware, service=SERVICE_NAME)


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    """Prometheus scrape endpoint (LM-034)."""
    body, content_type = render_metrics()
    return Response(body, media_type=content_type)


@lru_cache
def get_wallet_service() -> WalletService:
    return WalletService(PostgresWalletRepository(get_engine()))


WalletServiceDependency = Annotated[WalletService, Depends(get_wallet_service)]


@lru_cache
def get_participant_service() -> WalletParticipantService:
    return WalletParticipantService(PostgresWalletParticipantRepository(get_engine()))


ParticipantServiceDependency = Annotated[WalletParticipantService, Depends(get_participant_service)]


@app.exception_handler(WalletError)
async def wallet_error_handler(_request: Request, error: WalletError) -> JSONResponse:
    problem = Problem(
        type=error.problem_type,
        title=error.title,
        status=error.status,
        detail=error.detail,
        transaction_id=error.transaction_id,
    )
    return JSONResponse(
        status_code=error.status,
        content=problem.model_dump(mode="json"),
        media_type="application/problem+json",
    )


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.get(
    "/v1/wallets/{user_id}",
    response_model=WalletBalance,
    responses={404: {"model": Problem}},
    tags=["wallets"],
)
def get_balance(user_id: str, service: WalletServiceDependency) -> WalletBalance:
    return service.get_balance(user_id)


@app.post(
    "/v1/wallets/{user_id}/deductions",
    response_model=WalletOperationResult,
    responses={404: {"model": Problem}, 409: {"model": Problem}},
    tags=["wallets"],
)
def deduct_points(
    user_id: str,
    request: DeductRequest,
    service: WalletServiceDependency,
) -> WalletOperationResult:
    return service.deduct(user_id, request.transaction_id, request.points, request.event_id)


@app.post(
    "/v1/wallets/{user_id}/refunds",
    response_model=WalletOperationResult,
    responses={404: {"model": Problem}, 409: {"model": Problem}},
    tags=["wallets"],
)
def refund_points(
    user_id: str,
    request: RefundRequest,
    service: WalletServiceDependency,
) -> WalletOperationResult:
    return service.refund(user_id, request.transaction_id, request.event_id)


@app.post(
    "/v1/transactions/{transaction_id}/prepare",
    response_model=ParticipantResponse,
    responses={409: {"model": Problem}},
    tags=["2pc"],
)
def prepare_transaction(
    transaction_id: UUID,
    request: WalletPrepareRequest,
    service: ParticipantServiceDependency,
) -> ParticipantResponse:
    return service.prepare(transaction_id, request)


@app.post(
    "/v1/transactions/{transaction_id}/commit",
    response_model=ParticipantResponse,
    responses={404: {"model": Problem}, 409: {"model": Problem}},
    tags=["2pc"],
)
def commit_transaction(
    transaction_id: UUID, service: ParticipantServiceDependency
) -> ParticipantResponse:
    return service.commit(transaction_id)


@app.post(
    "/v1/transactions/{transaction_id}/abort",
    response_model=ParticipantResponse,
    responses={409: {"model": Problem}},
    tags=["2pc"],
)
def abort_transaction(
    transaction_id: UUID, service: ParticipantServiceDependency
) -> ParticipantResponse:
    return service.abort(transaction_id)


@app.get(
    "/v1/transactions/{transaction_id}",
    response_model=ParticipantResponse,
    responses={404: {"model": Problem}},
    tags=["2pc"],
)
def get_transaction(
    transaction_id: UUID, service: ParticipantServiceDependency
) -> ParticipantResponse:
    return service.get(transaction_id)
