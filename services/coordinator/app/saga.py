"""Durable Coordinator state machine for the Saga redemption workflow (LM-022)."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

import httpx
from ledgermesh_messaging import (
    CATALOG_COMMANDS,
    REDEMPTION_EVENTS,
    WALLET_COMMANDS,
    EventEnvelope,
    OutgoingMessage,
    PermanentError,
    enqueue,
)
from sqlalchemy import Connection, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.coordinator.app.db.schema import redemption_transitions, redemptions

logger = logging.getLogger(__name__)

POINTS_DEDUCTED = "wallet.points_deducted"
DEDUCTION_REJECTED = "wallet.deduction_rejected"
POINTS_REFUNDED = "wallet.points_refunded"
INVENTORY_RESERVED = "catalog.inventory_reserved"
RESERVATION_REJECTED = "catalog.reservation_rejected"
RESULT_AGGREGATES = {
    POINTS_DEDUCTED: "wallet",
    DEDUCTION_REJECTED: "wallet",
    POINTS_REFUNDED: "wallet",
    INVENTORY_RESERVED: "reward",
    RESERVATION_REJECTED: "reward",
}
TERMINAL_SAGA_STATES = {"COMPLETED", "FAILED", "COMPENSATED"}


class InvalidSagaResult(PermanentError):
    """A result cannot advance the durable workflow as delivered."""


@dataclass(frozen=True)
class RewardQuote:
    reward_id: str
    point_cost: int
    available_inventory: int
    version: int


class QuoteClient(Protocol):
    async def get_quote(self, reward_id: str) -> RewardQuote: ...


class HttpCatalogQuoteClient:
    def __init__(self, base_url: str, client: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = client

    async def get_quote(self, reward_id: str) -> RewardQuote:
        response = await self.client.get(f"{self.base_url}/v1/rewards/{reward_id}")
        response.raise_for_status()
        return RewardQuote(**response.json())


def _audit_event(
    row, from_status: str | None, to_status: str, reason: str, causation_id: UUID | None
):
    envelope = EventEnvelope(
        event_type="coordinator.transitioned",
        transaction_id=row.transaction_id,
        correlation_id=row.correlation_id,
        causation_id=causation_id,
        aggregate_type="redemption",
        aggregate_id=str(row.transaction_id),
        aggregate_version=row.version,
        workflow_step=row.workflow_step,
        fencing_token=row.fencing_token,
        payload={
            "from_status": from_status,
            "to_status": to_status,
            "reason": reason,
            "coordination_mode": "saga",
        },
    )
    return REDEMPTION_EVENTS.message(envelope)


class CoordinatorSagaMachine:
    """Advance a Saga while holding the redemption row lock."""

    def start(self, connection: Connection, transaction_id: UUID, quote: RewardQuote):
        row = self._locked(connection, transaction_id)
        if row.coordination_mode != "saga":
            raise InvalidSagaResult("only Saga redemptions can be started by the Saga worker")
        if row.status != "REQUESTED":
            return []
        if quote.reward_id != row.reward_id or quote.point_cost <= 0:
            raise InvalidSagaResult("Catalog returned an invalid reward quote")

        total_points = quote.point_cost * row.quantity
        command = EventEnvelope(
            event_type="wallet.deduct_points",
            transaction_id=row.transaction_id,
            correlation_id=row.correlation_id,
            causation_id=None,
            aggregate_type="wallet",
            aggregate_id=row.user_id,
            workflow_step=1,
            fencing_token=row.fencing_token,
            payload={"user_id": row.user_id, "points": total_points},
        )
        updated = self._transition(
            connection,
            row,
            "DEDUCTING_POINTS",
            "wallet deduction requested",
            command_id=command.event_id,
            point_cost=quote.point_cost,
            reward_version=quote.version,
            total_points=total_points,
        )
        requested_audit = _audit_event(
            row, None, "REQUESTED", "redemption created", causation_id=None
        )
        started_audit = _audit_event(
            updated, "REQUESTED", "DEDUCTING_POINTS", "wallet deduction requested", None
        )
        return [requested_audit, started_audit, WALLET_COMMANDS.message(command)]

    def handle_result(self, connection: Connection, envelope: EventEnvelope):
        row = self._locked(connection, envelope.transaction_id)
        self._validate_stable_identity(row, envelope)

        if row.status in TERMINAL_SAGA_STATES or envelope.workflow_step < row.workflow_step:
            logger.info(
                "ignoring stale Saga result transaction_id=%s event_id=%s "
                "event_step=%s current_step=%s status=%s",
                row.transaction_id,
                envelope.event_id,
                envelope.workflow_step,
                row.workflow_step,
                row.status,
            )
            return []

        self._validate_current_command(row, envelope)

        if row.status == "DEDUCTING_POINTS":
            if envelope.event_type == POINTS_DEDUCTED:
                return self._points_deducted(connection, row, envelope)
            if envelope.event_type == DEDUCTION_REJECTED:
                return self._terminal(
                    connection, row, envelope, "FAILED", "wallet deduction rejected"
                )
        elif row.status == "RESERVING_INVENTORY":
            if envelope.event_type == INVENTORY_RESERVED:
                return self._terminal(connection, row, envelope, "COMPLETED", "inventory reserved")
            if envelope.event_type == RESERVATION_REJECTED:
                return self._compensate(connection, row, envelope)
        elif row.status == "COMPENSATING" and envelope.event_type == POINTS_REFUNDED:
            return self._terminal(
                connection, row, envelope, "COMPENSATED", "wallet points refunded"
            )

        raise InvalidSagaResult(
            f"{envelope.event_type} is not expected while redemption is {row.status}"
        )

    def _points_deducted(self, connection: Connection, row, result: EventEnvelope):
        deducted = self._transition(
            connection, row, "POINTS_DEDUCTED", "wallet deduction confirmed"
        )
        command = EventEnvelope(
            event_type="catalog.reserve_inventory",
            transaction_id=row.transaction_id,
            correlation_id=row.correlation_id,
            causation_id=result.event_id,
            aggregate_type="reward",
            aggregate_id=row.reward_id,
            workflow_step=3,
            fencing_token=row.fencing_token,
            payload={"reward_id": row.reward_id, "quantity": row.quantity},
        )
        reserving = self._transition(
            connection,
            deducted,
            "RESERVING_INVENTORY",
            "inventory reservation requested",
            command_id=command.event_id,
        )
        return [
            _audit_event(
                deducted,
                "DEDUCTING_POINTS",
                "POINTS_DEDUCTED",
                "wallet deduction confirmed",
                result.event_id,
            ),
            _audit_event(
                reserving,
                "POINTS_DEDUCTED",
                "RESERVING_INVENTORY",
                "inventory reservation requested",
                result.event_id,
            ),
            CATALOG_COMMANDS.message(command),
        ]

    def _compensate(self, connection: Connection, row, result: EventEnvelope):
        command = EventEnvelope(
            event_type="wallet.refund_points",
            transaction_id=row.transaction_id,
            correlation_id=row.correlation_id,
            causation_id=result.event_id,
            aggregate_type="wallet",
            aggregate_id=row.user_id,
            workflow_step=4,
            fencing_token=row.fencing_token,
            payload={"user_id": row.user_id, "points": row.total_points},
        )
        updated = self._transition(
            connection,
            row,
            "COMPENSATING",
            "inventory reservation rejected; refund requested",
            command_id=command.event_id,
        )
        return [
            _audit_event(
                updated,
                "RESERVING_INVENTORY",
                "COMPENSATING",
                "inventory reservation rejected; refund requested",
                result.event_id,
            ),
            WALLET_COMMANDS.message(command),
        ]

    def _terminal(
        self,
        connection: Connection,
        row,
        result: EventEnvelope,
        status: str,
        reason: str,
    ):
        updated = self._transition(connection, row, status, reason, command_id=None)
        return [_audit_event(updated, row.status, status, reason, result.event_id)]

    @staticmethod
    def _validate_stable_identity(row, envelope: EventEnvelope) -> None:
        if row.coordination_mode != "saga":
            raise InvalidSagaResult("result targets a non-Saga redemption")
        if envelope.correlation_id != row.correlation_id:
            raise InvalidSagaResult("result correlation_id does not match the redemption")
        expected_aggregate = RESULT_AGGREGATES.get(envelope.event_type)
        if expected_aggregate is None:
            raise InvalidSagaResult(f"unsupported Saga result {envelope.event_type}")
        expected_id = row.user_id if expected_aggregate == "wallet" else row.reward_id
        if envelope.aggregate_type != expected_aggregate or envelope.aggregate_id != expected_id:
            raise InvalidSagaResult("result targets the wrong aggregate")

    @staticmethod
    def _validate_current_command(row, envelope: EventEnvelope) -> None:
        if envelope.causation_id != row.current_command_id:
            raise InvalidSagaResult("result does not acknowledge the current command")
        if envelope.workflow_step != row.workflow_step:
            raise InvalidSagaResult("result workflow_step does not match the current step")

    def fail_start(self, connection: Connection, transaction_id: UUID, reason: str):
        row = self._locked(connection, transaction_id)
        if row.coordination_mode != "saga":
            raise InvalidSagaResult("only Saga redemptions can be failed by the Saga worker")
        if row.status != "REQUESTED":
            return []
        updated = self._transition(connection, row, "FAILED", reason, command_id=None)
        return [
            _audit_event(row, None, "REQUESTED", "redemption created", causation_id=None),
            _audit_event(updated, "REQUESTED", "FAILED", reason, causation_id=None),
        ]

    @staticmethod
    def _locked(connection: Connection, transaction_id: UUID):
        row = connection.execute(
            select(redemptions)
            .where(redemptions.c.transaction_id == transaction_id)
            .with_for_update()
        ).one_or_none()
        if row is None:
            raise InvalidSagaResult(f"unknown redemption {transaction_id}")
        return row

    @staticmethod
    def _transition(
        connection: Connection,
        row,
        to_status: str,
        reason: str,
        *,
        command_id: UUID | None = None,
        point_cost: int | None = None,
        reward_version: int | None = None,
        total_points: int | None = None,
    ):
        values = {
            "status": to_status,
            "workflow_step": row.workflow_step + 1,
            "current_command_id": command_id,
            "version": row.version + 1,
            "updated_at": datetime.now(UTC),
        }
        if point_cost is not None:
            values.update(
                point_cost=point_cost,
                reward_version=reward_version,
                total_points=total_points,
            )
        updated = connection.execute(
            update(redemptions)
            .where(redemptions.c.transaction_id == row.transaction_id)
            .values(**values)
            .returning(*redemptions.c)
        ).one()
        connection.execute(
            redemption_transitions.insert().values(
                transaction_id=row.transaction_id,
                from_status=row.status,
                to_status=to_status,
                workflow_step=updated.workflow_step,
                reason=reason,
            )
        )
        return updated


class CoordinatorSagaResultHandler:
    def __init__(self, machine: CoordinatorSagaMachine) -> None:
        self.machine = machine

    async def __call__(
        self, session: AsyncSession, envelope: EventEnvelope
    ) -> list[OutgoingMessage]:
        return await session.run_sync(
            lambda sync: self.machine.handle_result(sync.connection(), envelope)
        )


class SagaStarter:
    """Poll durable REQUESTED rows, quote them, and enqueue the first command."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        quote_client: QuoteClient,
        machine: CoordinatorSagaMachine,
        *,
        poll_interval: float = 0.5,
        max_backoff: float = 30.0,
    ) -> None:
        self.sessions = sessions
        self.quote_client = quote_client
        self.machine = machine
        self.poll_interval = poll_interval
        self.max_backoff = max_backoff
        self._failures: dict[UUID, int] = {}
        self._retry_after: dict[UUID, float] = {}

    async def _fail_permanently(self, transaction_id: UUID, reason: str) -> bool:
        async with self.sessions() as session, session.begin():
            messages = await session.run_sync(
                lambda sync: self.machine.fail_start(sync.connection(), transaction_id, reason)
            )
            for message in messages:
                await enqueue(session, message)
        self._failures.pop(transaction_id, None)
        self._retry_after.pop(transaction_id, None)
        return bool(messages)

    async def _fail_or_back_off(self, transaction_id: UUID, reason: str) -> None:
        try:
            await self._fail_permanently(transaction_id, reason)
        except (OSError, SQLAlchemyError):
            self._back_off(transaction_id)
            logger.exception(
                "could not persist permanent Saga failure %s; retry is backed off",
                transaction_id,
            )

    def _back_off(self, transaction_id: UUID) -> None:
        failures = self._failures.get(transaction_id, 0) + 1
        self._failures[transaction_id] = failures
        delay = min(self.poll_interval * (2 ** (failures - 1)), self.max_backoff)
        self._retry_after[transaction_id] = time.monotonic() + delay

    async def run_once(self) -> int:
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(redemptions.c.transaction_id, redemptions.c.reward_id)
                    .where(
                        redemptions.c.coordination_mode == "saga",
                        redemptions.c.status == "REQUESTED",
                    )
                    .order_by(redemptions.c.created_at)
                    .limit(100)
                )
            ).all()

        started = 0
        for transaction_id, reward_id in rows:
            if time.monotonic() < self._retry_after.get(transaction_id, 0):
                continue
            try:
                quote = await self.quote_client.get_quote(reward_id)
                async with self.sessions() as session, session.begin():
                    messages = await session.run_sync(
                        lambda sync, transaction_id=transaction_id, quote=quote: self.machine.start(
                            sync.connection(), transaction_id, quote
                        )
                    )
                    for message in messages:
                        await enqueue(session, message)
                started += bool(messages)
                self._failures.pop(transaction_id, None)
                self._retry_after.pop(transaction_id, None)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    await self._fail_or_back_off(transaction_id, "Catalog reward was not found")
                    logger.warning(
                        "failed Saga %s because its reward was not found", transaction_id
                    )
                else:
                    self._back_off(transaction_id)
                    logger.exception(
                        "Catalog temporarily rejected quote for Saga %s", transaction_id
                    )
            except (InvalidSagaResult, TypeError, ValueError) as exc:
                await self._fail_or_back_off(
                    transaction_id, f"Catalog returned an invalid reward quote: {exc}"
                )
                logger.warning("failed Saga %s because its quote was invalid", transaction_id)
            except (httpx.HTTPError, OSError, SQLAlchemyError):
                self._back_off(transaction_id)
                logger.exception("could not start Saga %s; retry is backed off", transaction_id)
        return started

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.run_once()
            except (OSError, SQLAlchemyError):
                logger.exception("Saga starter scan failed; retrying after poll interval")
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.poll_interval)
