from functools import lru_cache
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import JSONResponse
from ledgermesh_messaging import CorrelationMiddleware, configure_logging

from services.catalog.app.errors import CatalogError
from services.catalog.app.models import (
    CatalogPrepareRequest,
    FulfillRequest,
    ParticipantResponse,
    Problem,
    ReleaseRequest,
    ReservationResult,
    ReserveRequest,
    RewardQuote,
    RewardView,
)
from services.catalog.app.participant import PostgresCatalogParticipantRepository
from services.catalog.app.participant_service import CatalogParticipantService
from services.catalog.app.repository import PostgresCatalogRepository, get_engine
from services.catalog.app.service import CatalogService

SERVICE_NAME = "catalog"
configure_logging(SERVICE_NAME)  # LM-032: JSON logs with shared field names

app = FastAPI(title="LedgerMesh Rewards Catalog Service", version="0.1.0")
app.add_middleware(CorrelationMiddleware)


@lru_cache
def get_catalog_service() -> CatalogService:
    return CatalogService(PostgresCatalogRepository(get_engine()))


CatalogServiceDependency = Annotated[CatalogService, Depends(get_catalog_service)]


@lru_cache
def get_participant_service() -> CatalogParticipantService:
    return CatalogParticipantService(PostgresCatalogParticipantRepository(get_engine()))


ParticipantServiceDependency = Annotated[
    CatalogParticipantService, Depends(get_participant_service)
]


@app.exception_handler(CatalogError)
async def catalog_error_handler(_request: Request, error: CatalogError) -> JSONResponse:
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


@app.get("/v1/rewards", response_model=list[RewardView], tags=["rewards"])
def list_rewards(
    service: CatalogServiceDependency,
    available_only: Annotated[bool, Query()] = True,
) -> list[RewardView]:
    return service.list_rewards(available_only=available_only)


@app.get(
    "/v1/rewards/{reward_id}",
    response_model=RewardQuote,
    responses={404: {"model": Problem}},
    tags=["rewards"],
)
def get_reward(reward_id: str, service: CatalogServiceDependency) -> RewardQuote:
    return service.get_reward(reward_id)


@app.post(
    "/v1/rewards/{reward_id}/reservations",
    response_model=ReservationResult,
    responses={404: {"model": Problem}, 409: {"model": Problem}},
    tags=["rewards"],
)
def reserve_inventory(
    reward_id: str, request: ReserveRequest, service: CatalogServiceDependency
) -> ReservationResult:
    return service.reserve(reward_id, request.transaction_id, request.quantity, request.event_id)


@app.post(
    "/v1/rewards/{reward_id}/releases",
    response_model=ReservationResult,
    responses={404: {"model": Problem}, 409: {"model": Problem}},
    tags=["rewards"],
)
def release_inventory(
    reward_id: str, request: ReleaseRequest, service: CatalogServiceDependency
) -> ReservationResult:
    return service.release(reward_id, request.transaction_id, request.quantity, request.event_id)


@app.post(
    "/v1/rewards/{reward_id}/fulfillments",
    response_model=ReservationResult,
    responses={404: {"model": Problem}, 409: {"model": Problem}},
    tags=["rewards"],
)
def fulfill_reservation(
    reward_id: str, request: FulfillRequest, service: CatalogServiceDependency
) -> ReservationResult:
    return service.fulfill(reward_id, request.transaction_id, request.event_id)


@app.post(
    "/v1/transactions/{transaction_id}/prepare",
    response_model=ParticipantResponse,
    responses={409: {"model": Problem}},
    tags=["2pc"],
)
def prepare_transaction(
    transaction_id: UUID,
    request: CatalogPrepareRequest,
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
