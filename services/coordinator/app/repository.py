"""PostgreSQL repository for durable Coordinator transactions (LM-010)."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import Protocol
from uuid import UUID, uuid4

from ledgermesh_messaging import EventEnvelope
from ledgermesh_messaging.schema import outbox
from sqlalchemy import Connection, Engine, create_engine, insert, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert

from services.coordinator.app.db.schema import redemption_transitions, redemptions
from services.coordinator.app.errors import (
    IdempotencyConflict,
    InvalidTransition,
    RedemptionNotFound,
)
from services.coordinator.app.models import CreateRedemptionRequest

logger = logging.getLogger(__name__)


def log_transition(
    *,
    transaction_id: UUID,
    correlation_id: UUID,
    coordination_mode: str,
    from_status: str | None,
    to_status: str,
    workflow_step: int,
    reason: str | None,
) -> None:
    """One structured line per Coordinator state change (LM-032).

    Shared by the 2PC repository and the Saga machine, so every transition of
    every redemption is logged with the same fields: ``state`` is the new
    status and ``from_state`` the previous one.
    """
    logger.info(
        "redemption %s -> %s (%s)",
        from_status or "-",
        to_status,
        reason or "",
        extra={
            "transaction_id": transaction_id,
            "correlation_id": correlation_id,
            "workflow_step": workflow_step,
            "state": to_status,
            "from_state": from_status,
            "coordination_mode": coordination_mode,
        },
    )


TRANSITIONS: dict[str, frozenset[str]] = {
    "two_phase_commit": frozenset(
        {
            "REQUESTED->PREPARING",
            "PREPARING->COMMIT_DECIDED",
            "PREPARING->ABORT_DECIDED",
            "COMMIT_DECIDED->COMMITTED",
            "ABORT_DECIDED->ABORTED",
        }
    ),
    "saga": frozenset(
        {
            "REQUESTED->DEDUCTING_POINTS",
            "REQUESTED->FAILED",
            "DEDUCTING_POINTS->POINTS_DEDUCTED",
            "DEDUCTING_POINTS->FAILED",
            "DEDUCTING_POINTS->COMPENSATING",
            "POINTS_DEDUCTED->RESERVING_INVENTORY",
            "RESERVING_INVENTORY->COMPLETED",
            "RESERVING_INVENTORY->COMPENSATING",
            "COMPENSATING->COMPENSATING",
            "COMPENSATING->COMPENSATED",
        }
    ),
}


@dataclass(frozen=True)
class RedemptionRecord:
    transaction_id: UUID
    correlation_id: UUID
    status: str
    coordination_mode: str
    created_at: datetime
    updated_at: datetime
    duplicate: bool = False


@dataclass(frozen=True)
class TransitionRecord:
    sequence: int
    from_status: str | None
    to_status: str
    workflow_step: int
    reason: str | None
    created_at: datetime


@dataclass(frozen=True)
class WorkflowRecord(RedemptionRecord):
    user_id: str = ""
    reward_id: str = ""
    quantity: int = 0
    point_cost: int | None = None
    reward_version: int | None = None
    total_points: int | None = None
    workflow_step: int = 0


RECOVERABLE_2PC_STATES = (
    "REQUESTED",
    "PREPARING",
    "COMMIT_DECIDED",
    "ABORT_DECIDED",
)

IN_DOUBT_2PC_STATES = ("PREPARING", "COMMIT_DECIDED", "ABORT_DECIDED")


class CoordinatorRepository(Protocol):
    def create(
        self,
        idempotency_key: str,
        request: CreateRedemptionRequest,
        *,
        connection: Connection | None = None,
    ) -> RedemptionRecord: ...

    def get(self, transaction_id: UUID) -> RedemptionRecord: ...

    def transition(
        self,
        transaction_id: UUID,
        to_status: str,
        reason: str | None = None,
        *,
        connection: Connection | None = None,
    ) -> RedemptionRecord: ...

    def list_transitions(self, transaction_id: UUID) -> list[TransitionRecord]: ...


def database_url() -> str:
    if override := os.environ.get("COORDINATOR_DATABASE_URL") or os.environ.get("DATABASE_URL"):
        return override
    password = os.environ.get("POSTGRES_PASSWORD", "ledgermesh_dev")
    return f"postgresql+psycopg://coordinator:{password}@coordinator-db:5432/coordinator"


@lru_cache
def get_engine(url: str | None = None) -> Engine:
    return create_engine(url or database_url(), pool_pre_ping=True)


def request_hash(request: CreateRedemptionRequest) -> str:
    canonical = json.dumps(request.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class PostgresCoordinatorRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def create(
        self,
        idempotency_key: str,
        request: CreateRedemptionRequest,
        *,
        connection: Connection | None = None,
    ) -> RedemptionRecord:
        if connection is not None:
            return self._create(connection, idempotency_key, request)
        with self.engine.begin() as owned_connection:
            return self._create(owned_connection, idempotency_key, request)

    def _create(
        self, connection: Connection, idempotency_key: str, request: CreateRedemptionRequest
    ) -> RedemptionRecord:
        digest = request_hash(request)
        transaction_id = uuid4()
        correlation_id = uuid4()
        row = connection.execute(
            postgresql_insert(redemptions)
            .values(
                transaction_id=transaction_id,
                correlation_id=correlation_id,
                idempotency_key=idempotency_key,
                request_hash=digest,
                user_id=request.user_id,
                reward_id=request.reward_id,
                quantity=request.quantity,
                coordination_mode=request.coordination_mode,
                status="REQUESTED",
            )
            .on_conflict_do_nothing(index_elements=[redemptions.c.idempotency_key])
            .returning(*redemptions.c)
        ).one_or_none()
        if row is None:
            existing = connection.execute(
                select(redemptions).where(redemptions.c.idempotency_key == idempotency_key)
            ).one()
            if existing.request_hash != digest:
                raise IdempotencyConflict(
                    "The idempotency key is already associated with a different request.",
                    existing.transaction_id,
                )
            return self._redemption(existing, duplicate=True)

        connection.execute(
            insert(redemption_transitions).values(
                transaction_id=transaction_id,
                from_status=None,
                to_status="REQUESTED",
                workflow_step=0,
                reason="redemption created",
            )
        )
        log_transition(
            transaction_id=transaction_id,
            correlation_id=correlation_id,
            coordination_mode=request.coordination_mode,
            from_status=None,
            to_status="REQUESTED",
            workflow_step=0,
            reason="redemption created",
        )
        return self._redemption(row)

    def get(self, transaction_id: UUID) -> RedemptionRecord:
        with self.engine.connect() as connection:
            row = connection.execute(
                select(redemptions).where(redemptions.c.transaction_id == transaction_id)
            ).one_or_none()
        if row is None:
            raise RedemptionNotFound(f"Redemption {transaction_id} does not exist.", transaction_id)
        return self._redemption(row)

    def get_workflow(self, transaction_id: UUID) -> WorkflowRecord:
        with self.engine.connect() as connection:
            row = connection.execute(
                select(redemptions).where(redemptions.c.transaction_id == transaction_id)
            ).one_or_none()
        if row is None:
            raise RedemptionNotFound(f"Redemption {transaction_id} does not exist.", transaction_id)
        return self._workflow(row)

    def list_recoverable_2pc(self, limit: int = 100) -> list[WorkflowRecord]:
        """Return durable 2PC work in a stable order for a bounded recovery pass."""
        with self.engine.connect() as connection:
            rows = connection.execute(
                select(redemptions)
                .where(
                    redemptions.c.coordination_mode == "two_phase_commit",
                    redemptions.c.status.in_(RECOVERABLE_2PC_STATES),
                )
                .order_by(redemptions.c.updated_at.asc(), redemptions.c.transaction_id.asc())
                .limit(limit)
            ).all()
        return [self._workflow(row) for row in rows]

    def list_in_doubt_2pc(self, limit: int = 100) -> list[WorkflowRecord]:
        """Return transactions that need a recovery decision or acknowledgement."""
        with self.engine.connect() as connection:
            rows = connection.execute(
                select(redemptions)
                .where(
                    redemptions.c.coordination_mode == "two_phase_commit",
                    redemptions.c.status.in_(IN_DOUBT_2PC_STATES),
                )
                .order_by(redemptions.c.updated_at.asc(), redemptions.c.transaction_id.asc())
                .limit(limit)
            ).all()
        return [self._workflow(row) for row in rows]

    def record_quote(
        self, transaction_id: UUID, point_cost: int, reward_version: int
    ) -> WorkflowRecord:
        with self.engine.begin() as connection:
            current = connection.execute(
                select(redemptions)
                .where(redemptions.c.transaction_id == transaction_id)
                .with_for_update()
            ).one()
            updated = connection.execute(
                update(redemptions)
                .where(redemptions.c.transaction_id == transaction_id)
                .values(
                    point_cost=point_cost,
                    reward_version=reward_version,
                    total_points=point_cost * current.quantity,
                    updated_at=datetime.now(UTC),
                )
                .returning(*redemptions.c)
            ).one()
        return self._workflow(updated)

    def finalize_with_audit(
        self, transaction_id: UUID, terminal_status: str, reason: str
    ) -> tuple[WorkflowRecord, EventEnvelope]:
        """Persist terminal state and its audit outbox entry atomically."""
        with self.engine.begin() as connection:
            updated = self._transition(connection, transaction_id, terminal_status, reason)
            workflow = self._workflow_row(connection, transaction_id)
            envelope = EventEnvelope(
                event_type=f"redemption.{terminal_status.lower()}",
                transaction_id=transaction_id,
                correlation_id=workflow.correlation_id,
                aggregate_type="redemption",
                aggregate_id=str(transaction_id),
                aggregate_version=workflow.workflow_step,
                workflow_step=workflow.workflow_step,
                fencing_token=None,
                payload={
                    "status": terminal_status,
                    "coordination_mode": workflow.coordination_mode,
                    "user_id": workflow.user_id,
                    "reward_id": workflow.reward_id,
                    "quantity": workflow.quantity,
                    "total_points": workflow.total_points,
                },
            )
            connection.execute(
                insert(outbox).values(
                    event_id=envelope.event_id,
                    topic="redemption.events",
                    message_key=str(transaction_id),
                    envelope=envelope.to_json(),
                )
            )
        del updated
        return workflow, envelope

    def transition(
        self,
        transaction_id: UUID,
        to_status: str,
        reason: str | None = None,
        *,
        connection: Connection | None = None,
    ) -> RedemptionRecord:
        if connection is not None:
            return self._transition(connection, transaction_id, to_status, reason)
        with self.engine.begin() as owned_connection:
            return self._transition(owned_connection, transaction_id, to_status, reason)

    def _transition(
        self,
        connection: Connection,
        transaction_id: UUID,
        to_status: str,
        reason: str | None,
    ) -> RedemptionRecord:
        current = connection.execute(
            select(redemptions)
            .where(redemptions.c.transaction_id == transaction_id)
            .with_for_update()
        ).one_or_none()
        if current is None:
            raise RedemptionNotFound(f"Redemption {transaction_id} does not exist.", transaction_id)

        # Recovery workers may retry a transition when the first response was
        # lost. Returning the durable state makes that retry idempotent and,
        # importantly, does not advance workflow_step/version or append a
        # duplicate history row. COMPENSATING is the one intentional self-loop:
        # each retry represents another durable compensation attempt.
        if current.status == to_status and to_status != "COMPENSATING":
            return self._redemption(current, duplicate=True)

        edge = f"{current.status}->{to_status}"
        if edge not in TRANSITIONS[current.coordination_mode]:
            raise InvalidTransition(
                f"Transition {edge} is not valid for {current.coordination_mode}.", transaction_id
            )

        next_step = current.workflow_step + 1
        now = datetime.now(UTC)
        updated = connection.execute(
            update(redemptions)
            .where(redemptions.c.transaction_id == transaction_id)
            .values(
                status=to_status,
                workflow_step=next_step,
                version=redemptions.c.version + 1,
                updated_at=now,
            )
            .returning(*redemptions.c)
        ).one()
        connection.execute(
            insert(redemption_transitions).values(
                transaction_id=transaction_id,
                from_status=current.status,
                to_status=to_status,
                workflow_step=next_step,
                reason=reason,
            )
        )
        log_transition(
            transaction_id=transaction_id,
            correlation_id=current.correlation_id,
            coordination_mode=current.coordination_mode,
            from_status=current.status,
            to_status=to_status,
            workflow_step=next_step,
            reason=reason,
        )
        return self._redemption(updated)

    def list_transitions(self, transaction_id: UUID) -> list[TransitionRecord]:
        self.get(transaction_id)
        with self.engine.connect() as connection:
            rows = connection.execute(
                select(redemption_transitions)
                .where(redemption_transitions.c.transaction_id == transaction_id)
                .order_by(redemption_transitions.c.id.asc())
            ).all()
        return [self._transition_record(row) for row in rows]

    @staticmethod
    def _redemption(row, *, duplicate: bool = False) -> RedemptionRecord:
        return RedemptionRecord(
            transaction_id=row.transaction_id,
            correlation_id=row.correlation_id,
            status=row.status,
            coordination_mode=row.coordination_mode,
            created_at=row.created_at,
            updated_at=row.updated_at,
            duplicate=duplicate,
        )

    @staticmethod
    def _transition_record(row) -> TransitionRecord:
        return TransitionRecord(
            sequence=row.id,
            from_status=row.from_status,
            to_status=row.to_status,
            workflow_step=row.workflow_step,
            reason=row.reason,
            created_at=row.created_at,
        )

    @staticmethod
    def _workflow(row) -> WorkflowRecord:
        return WorkflowRecord(
            transaction_id=row.transaction_id,
            correlation_id=row.correlation_id,
            status=row.status,
            coordination_mode=row.coordination_mode,
            created_at=row.created_at,
            updated_at=row.updated_at,
            user_id=row.user_id,
            reward_id=row.reward_id,
            quantity=row.quantity,
            point_cost=row.point_cost,
            reward_version=row.reward_version,
            total_points=row.total_points,
            workflow_step=row.workflow_step,
        )

    def _workflow_row(self, connection: Connection, transaction_id: UUID) -> WorkflowRecord:
        row = connection.execute(
            select(redemptions).where(redemptions.c.transaction_id == transaction_id)
        ).one()
        return self._workflow(row)
