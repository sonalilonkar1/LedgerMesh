"""API and domain models for Catalog core operations (LM-008)."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from ledgermesh_messaging.safeguards import POSTGRES_BIGINT_MAX, POSTGRES_INTEGER_MAX
from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


# LM-042: numbers are bounded by their database columns (INTEGER quantities,
# BIGINT versions and costs), so an oversized value is a 422, not a 500.
class ReserveRequest(StrictModel):
    transaction_id: UUID
    quantity: int = Field(gt=0, le=POSTGRES_INTEGER_MAX)
    event_id: UUID | None = None


class ReleaseRequest(StrictModel):
    transaction_id: UUID
    quantity: int = Field(gt=0, le=POSTGRES_INTEGER_MAX)
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
    quantity: int = Field(gt=0, le=POSTGRES_INTEGER_MAX)
    expected_reward_version: int = Field(ge=0, le=POSTGRES_BIGINT_MAX)
    expected_point_cost: int = Field(gt=0, le=POSTGRES_BIGINT_MAX)


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
