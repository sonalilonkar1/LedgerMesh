"""API and domain models for the durable Coordinator (LM-010)."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from ledgermesh_messaging.safeguards import POSTGRES_INTEGER_MAX
from pydantic import BaseModel, ConfigDict, Field

CoordinationMode = Literal["two_phase_commit", "saga"]
RedemptionStatus = Literal[
    "REQUESTED",
    "PREPARING",
    "COMMIT_DECIDED",
    "ABORT_DECIDED",
    "COMMITTED",
    "ABORTED",
    "DEDUCTING_POINTS",
    "POINTS_DEDUCTED",
    "RESERVING_INVENTORY",
    "COMPENSATING",
    "COMPLETED",
    "FAILED",
    "COMPENSATED",
]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CreateRedemptionRequest(StrictModel):
    user_id: str = Field(min_length=1)
    reward_id: str = Field(min_length=1)
    # LM-042: bounded by the INTEGER column, so an oversized value is a 422.
    quantity: int = Field(ge=1, le=POSTGRES_INTEGER_MAX)
    coordination_mode: CoordinationMode


class RedemptionResponse(StrictModel):
    transaction_id: UUID
    correlation_id: UUID
    status: RedemptionStatus
    coordination_mode: CoordinationMode
    created_at: datetime
    updated_at: datetime


class TransitionView(StrictModel):
    sequence: int
    from_status: RedemptionStatus | None
    to_status: RedemptionStatus
    workflow_step: int
    reason: str | None
    created_at: datetime


class Problem(StrictModel):
    type: str
    title: str
    status: int = Field(ge=400, le=599)
    detail: str
    transaction_id: UUID | None = None


class InDoubtTransactionView(StrictModel):
    transaction_id: UUID
    status: Literal["PREPARING", "COMMIT_DECIDED", "ABORT_DECIDED"]
    workflow_step: int = Field(ge=0)
    updated_at: datetime
    recovery_action: Literal["PRESUME_ABORT", "RETRANSMIT_COMMIT", "RETRANSMIT_ABORT"]


class RecoveryRunResponse(StrictModel):
    scanned: int = Field(ge=0)
    recovered: int = Field(ge=0)
    still_in_doubt: int = Field(ge=0)
    failed: int = Field(ge=0)
