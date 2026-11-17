"""API and domain models for Catalog core operations (LM-008)."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ReserveRequest(StrictModel):
    transaction_id: UUID
    quantity: int = Field(gt=0)
    event_id: UUID | None = None


class ReleaseRequest(StrictModel):
    transaction_id: UUID
    quantity: int = Field(gt=0)
    event_id: UUID | None = None


class FulfillRequest(StrictModel):
    transaction_id: UUID
    event_id: UUID | None = None


class RewardView(StrictModel):
    reward_id: str
    name: str
    point_cost: int
    total_inventory: int
    available_inventory: int
    version: int
    updated_at: datetime


class RewardQuote(StrictModel):
    reward_id: str
    point_cost: int
    available_inventory: int
    version: int


class ReservationResult(StrictModel):
    transaction_id: UUID
    reward_id: str
    quantity: int
    state: Literal["reserved", "released", "fulfilled", "tombstone"]
    outcome: Literal["applied", "duplicate", "tombstone"]
    available_inventory: int
    version: int
    updated_at: datetime


class CatalogPrepareRequest(StrictModel):
    correlation_id: UUID
    reward_id: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    expected_reward_version: int = Field(ge=0)
    expected_point_cost: int = Field(gt=0)


class ParticipantResponse(StrictModel):
    transaction_id: UUID
    participant: Literal["catalog"] = "catalog"
    state: Literal["PREPARED", "COMMITTED", "ABORTED", "IN_DOUBT"]
    updated_at: datetime


class Problem(StrictModel):
    type: str
    title: str
    status: int = Field(ge=400, le=599)
    detail: str
    transaction_id: UUID | None = None
