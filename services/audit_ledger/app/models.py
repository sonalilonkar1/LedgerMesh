"""API models for the immutable Audit Ledger (LM-009)."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from ledgermesh_messaging.safeguards import POSTGRES_BIGINT_MAX, POSTGRES_INTEGER_MAX
from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AuditEventRequest(StrictModel):
    schema_version: Literal[1]
    event_id: UUID
    # LM-042: lengths and numbers are bounded by their database columns
    # (VARCHAR(255), INTEGER, BIGINT), so oversized values are a 422, not a 500.
    event_type: str = Field(
        min_length=1,
        max_length=255,
        pattern=r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$",
    )
    transaction_id: UUID
    correlation_id: UUID
    causation_id: UUID | None
    aggregate_type: str = Field(min_length=1, max_length=255)
    aggregate_id: str = Field(min_length=1)
    aggregate_version: int | None = Field(ge=0, le=POSTGRES_BIGINT_MAX)
    workflow_step: int | None = Field(ge=0, le=POSTGRES_INTEGER_MAX)
    fencing_token: int | None = Field(ge=0, le=POSTGRES_BIGINT_MAX)
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
