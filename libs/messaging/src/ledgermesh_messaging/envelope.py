"""Event envelope carried by every LedgerMesh command and event."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar, Literal
from uuid import UUID, uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)


class EnvelopeError(ValueError):
    """Raised when raw bytes cannot be decoded into a valid EventEnvelope."""


class EventEnvelope(BaseModel):
    """Strict wire envelope with convenient defaults for local construction.

    Producers may omit generated/defaulted values while constructing an envelope;
    serialization always emits every field. Consumers validate with ``wire`` context,
    which rejects a serialized message that omitted any required contract key.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    WIRE_FIELDS: ClassVar[frozenset[str]] = frozenset(
        {
            "schema_version",
            "event_id",
            "event_type",
            "transaction_id",
            "correlation_id",
            "causation_id",
            "aggregate_type",
            "aggregate_id",
            "aggregate_version",
            "workflow_step",
            "fencing_token",
            "occurred_at",
            "payload",
        }
    )

    schema_version: Literal[1] = 1
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str = Field(
        min_length=1,
        pattern=r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$",
    )
    transaction_id: UUID
    correlation_id: UUID
    causation_id: UUID | None = None
    aggregate_type: str = Field(min_length=1)
    aggregate_id: str = Field(min_length=1)
    aggregate_version: int | None = Field(default=None, ge=0)
    workflow_step: int | None = Field(default=None, ge=0)
    fencing_token: int | None = Field(default=None, ge=0)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _require_all_wire_fields(cls, value: Any, info: ValidationInfo) -> Any:
        if info.context and info.context.get("wire") and isinstance(value, dict):
            missing = cls.WIRE_FIELDS.difference(value)
            if missing:
                names = ", ".join(sorted(missing))
                raise ValueError(f"wire envelope is missing required fields: {names}")
        return value

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
            return cls.model_validate_json(raw, context={"wire": True})
        except ValidationError as exc:
            raise EnvelopeError(str(exc)) from exc
