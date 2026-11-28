"""API and domain models for Wallet core operations (LM-007)."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from ledgermesh_messaging.safeguards import POSTGRES_BIGINT_MAX
from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DeductRequest(StrictModel):
    transaction_id: UUID
    # LM-042: bounded by the BIGINT column, so an oversized value is a 422.
    points: int = Field(gt=0, le=POSTGRES_BIGINT_MAX)
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
    points: int = Field(gt=0, le=POSTGRES_BIGINT_MAX)


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
