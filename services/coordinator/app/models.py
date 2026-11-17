"""API and domain models for the durable Coordinator (LM-010)."""

from datetime import datetime
from typing import Literal
from uuid import UUID

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
    quantity: int = Field(ge=1)
    coordination_mode: CoordinationMode


class RedemptionResponse(StrictModel):
    transaction_id: UUID
    correlation_id: UUID
    status: RedemptionStatus
    coordination_mode: CoordinationMode
    created_at: datetime
    updated_at: datetime


class TransitionRequest(StrictModel):
    to_status: RedemptionStatus
    reason: str | None = Field(default=None, max_length=2000)


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
