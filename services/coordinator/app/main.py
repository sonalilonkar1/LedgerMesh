from functools import lru_cache
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, Request, status
from fastapi.responses import JSONResponse

from services.coordinator.app.errors import CoordinatorError
from services.coordinator.app.models import (
    CreateRedemptionRequest,
    Problem,
    RedemptionResponse,
    TransitionRequest,
    TransitionView,
)
from services.coordinator.app.repository import PostgresCoordinatorRepository, get_engine
from services.coordinator.app.service import CoordinatorService

SERVICE_NAME = "coordinator"

app = FastAPI(title="LedgerMesh Redemption Coordinator", version="0.1.0")


@lru_cache
def get_coordinator_service() -> CoordinatorService:
    return CoordinatorService(PostgresCoordinatorRepository(get_engine()))


CoordinatorServiceDependency = Annotated[CoordinatorService, Depends(get_coordinator_service)]
IdempotencyKey = Annotated[
    str,
    Header(alias="Idempotency-Key", min_length=1, max_length=128),
]


@app.exception_handler(CoordinatorError)
async def coordinator_error_handler(_request: Request, error: CoordinatorError) -> JSONResponse:
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


@app.post(
    "/v1/redemptions",
    response_model=RedemptionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={409: {"model": Problem}},
    tags=["redemptions"],
)
def create_redemption(
    request: CreateRedemptionRequest,
    idempotency_key: IdempotencyKey,
    service: CoordinatorServiceDependency,
) -> RedemptionResponse:
    return service.create(idempotency_key, request)


@app.get(
    "/v1/redemptions/{transaction_id}",
    response_model=RedemptionResponse,
    responses={404: {"model": Problem}},
    tags=["redemptions"],
)
def get_redemption(
    transaction_id: UUID, service: CoordinatorServiceDependency
) -> RedemptionResponse:
    return service.get(transaction_id)


@app.post(
    "/internal/v1/redemptions/{transaction_id}/transitions",
    response_model=RedemptionResponse,
    responses={404: {"model": Problem}, 409: {"model": Problem}},
    tags=["internal"],
)
def transition_redemption(
    transaction_id: UUID,
    request: TransitionRequest,
    service: CoordinatorServiceDependency,
) -> RedemptionResponse:
    return service.transition(transaction_id, request.to_status, request.reason)


@app.get(
    "/internal/v1/redemptions/{transaction_id}/transitions",
    response_model=list[TransitionView],
    responses={404: {"model": Problem}},
    tags=["internal"],
)
def list_redemption_transitions(
    transaction_id: UUID, service: CoordinatorServiceDependency
) -> list[TransitionView]:
    return service.list_transitions(transaction_id)
