import os
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse, Response
from ledgermesh_messaging import (
    CorrelationMiddleware,
    MetricsMiddleware,
    configure_logging,
    render_metrics,
)

from services.coordinator.app.errors import CoordinatorError
from services.coordinator.app.models import (
    CreateRedemptionRequest,
    InDoubtTransactionView,
    Problem,
    RecoveryRunResponse,
    RedemptionResponse,
    TransitionView,
)
from services.coordinator.app.recovery import (
    RecoveryWorker,
    TwoPhaseCommitRecovery,
    recovery_enabled,
)
from services.coordinator.app.repository import PostgresCoordinatorRepository, get_engine
from services.coordinator.app.service import CoordinatorService
from services.coordinator.app.two_phase_commit import TwoPhaseCommitWorkflow, default_workflow

SERVICE_NAME = "coordinator"
configure_logging(SERVICE_NAME)  # LM-032: JSON logs with shared field names


@asynccontextmanager
async def lifespan(_app: FastAPI):
    worker: RecoveryWorker | None = None
    if recovery_enabled():
        interval = float(os.environ.get("COORDINATOR_RECOVERY_INTERVAL_SECONDS", "5"))
        worker = RecoveryWorker(get_two_phase_commit_recovery(), interval)
        worker.start()
    try:
        yield
    finally:
        if worker:
            worker.stop()


app = FastAPI(title="LedgerMesh Redemption Coordinator", version="0.1.0", lifespan=lifespan)
app.add_middleware(MetricsMiddleware, service=SERVICE_NAME)
app.add_middleware(CorrelationMiddleware)


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    """Prometheus scrape endpoint (LM-034)."""
    body, content_type = render_metrics()
    return Response(body, media_type=content_type)


@lru_cache
def get_coordinator_service() -> CoordinatorService:
    return CoordinatorService(PostgresCoordinatorRepository(get_engine()))


CoordinatorServiceDependency = Annotated[CoordinatorService, Depends(get_coordinator_service)]


@lru_cache
def get_two_phase_commit_workflow() -> TwoPhaseCommitWorkflow:
    return default_workflow(PostgresCoordinatorRepository(get_engine()))


TwoPhaseCommitDependency = Annotated[TwoPhaseCommitWorkflow, Depends(get_two_phase_commit_workflow)]


@lru_cache
def get_two_phase_commit_recovery() -> TwoPhaseCommitRecovery:
    repository = PostgresCoordinatorRepository(get_engine())
    return TwoPhaseCommitRecovery(repository, get_two_phase_commit_workflow())


TwoPhaseCommitRecoveryDependency = Annotated[
    TwoPhaseCommitRecovery, Depends(get_two_phase_commit_recovery)
]
IdempotencyKey = Annotated[
    str,
    Header(alias="Idempotency-Key", min_length=1, max_length=128),
]


def require_local_internal_routes() -> None:
    if os.environ.get("LEDGERMESH_ENV") not in {"local", "test"}:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")


InternalRouteDependency = Annotated[None, Depends(require_local_internal_routes)]


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


@app.post(
    "/internal/v1/redemptions/{transaction_id}/execute-2pc",
    response_model=RedemptionResponse,
    responses={404: {"model": Problem}, 409: {"model": Problem}},
    tags=["internal"],
)
def execute_two_phase_commit(
    transaction_id: UUID,
    workflow: TwoPhaseCommitDependency,
    _internal: InternalRouteDependency,
) -> RedemptionResponse:
    return RedemptionResponse.model_validate(workflow.execute(transaction_id), from_attributes=True)


@app.get(
    "/internal/v1/2pc/in-doubt",
    response_model=list[InDoubtTransactionView],
    tags=["internal"],
)
def list_in_doubt_transactions(
    recovery: TwoPhaseCommitRecoveryDependency,
    _internal: InternalRouteDependency,
) -> list[InDoubtTransactionView]:
    return recovery.in_doubt()


@app.post(
    "/internal/v1/2pc/recovery/run",
    response_model=RecoveryRunResponse,
    tags=["internal"],
)
def run_two_phase_commit_recovery(
    recovery: TwoPhaseCommitRecoveryDependency,
    _internal: InternalRouteDependency,
) -> RecoveryRunResponse:
    return recovery.run_once()
