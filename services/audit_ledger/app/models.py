"""API models for the immutable Audit Ledger (LM-009)."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AuditEventRequest(StrictModel):
    schema_version: Literal[1]
    event_id: UUID
    event_type: str = Field(
        min_length=1,
        pattern=r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$",
    )
    transaction_id: UUID
    correlation_id: UUID
    causation_id: UUID | None
    aggregate_type: str = Field(min_length=1)
    aggregate_id: str = Field(min_length=1)
    aggregate_version: int | None = Field(ge=0)
    workflow_step: int | None = Field(ge=0)
    fencing_token: int | None = Field(ge=0)
    occurred_at: datetime
    payload: dict[str, Any]

    @field_validator("occurred_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return value


class AuditEventView(StrictModel):
    ledger_sequence: int
    recorded_at: datetime
    envelope: AuditEventRequest


class AuditAppendResult(AuditEventView):
    outcome: Literal["appended", "duplicate"]


class Problem(StrictModel):
    type: str
    title: str
    status: int = Field(ge=400, le=599)
    detail: str
    event_id: UUID | None = None
