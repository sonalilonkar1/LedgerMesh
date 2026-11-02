"""Event envelope carried by every LedgerMesh command and event.

Provisional: mirrors the fields required by LM-004 (#6). Once the shared
contract package exists, this module should re-export it. The inbox and outbox
only rely on ``event_id``, ``transaction_id``, ``event_type`` and the JSON form.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class EnvelopeError(ValueError):
    """Raised when raw bytes cannot be decoded into a valid EventEnvelope."""


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: int = Field(default=1, ge=1)
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str = Field(min_length=1)
    transaction_id: UUID
    correlation_id: UUID
    causation_id: UUID | None = None
    aggregate_type: str = Field(min_length=1)
    aggregate_id: str = Field(min_length=1)
    aggregate_version: int | None = Field(default=None, ge=0)
    workflow_step: int | None = Field(default=None, ge=0)
    fencing_token: int | None = Field(default=None, ge=0)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("occurred_at")
    @classmethod
    def _require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return value

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, raw: bytes | None) -> EventEnvelope:
        if not raw:
            raise EnvelopeError("message has no value")
        try:
            return cls.model_validate_json(raw)
        except ValidationError as exc:
            raise EnvelopeError(str(exc)) from exc
