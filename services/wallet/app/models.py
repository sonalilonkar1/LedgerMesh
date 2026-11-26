"""API and domain models for Wallet core operations (LM-007)."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DeductRequest(StrictModel):
    transaction_id: UUID
    points: int = Field(gt=0)
    event_id: UUID | None = None


class RefundRequest(StrictModel):
    transaction_id: UUID
    event_id: UUID | None = None


class WalletBalance(StrictModel):
    user_id: str
    available_points: int
    version: int
    updated_at: datetime


class WalletOperationResult(WalletBalance):
    transaction_id: UUID
    operation: Literal["deduct", "refund"]
    points: int
    outcome: Literal["applied", "duplicate"]


class WalletPrepareRequest(StrictModel):
    correlation_id: UUID
    user_id: str = Field(min_length=1)
    points: int = Field(gt=0)
    fencing_token: int = Field(ge=0)


class ParticipantResponse(StrictModel):
    transaction_id: UUID
    participant: Literal["wallet"] = "wallet"
    state: Literal["PREPARED", "COMMITTED", "ABORTED", "IN_DOUBT"]
    updated_at: datetime


class Problem(StrictModel):
    type: str
    title: str
    status: int = Field(ge=400, le=599)
    detail: str
    transaction_id: UUID | None = None
