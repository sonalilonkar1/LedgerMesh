"""Durable Coordinator Saga workflow and compensations (LM-022/LM-023)."""

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
    metrics_on_commit,
    record_transition,
)
from ledgermesh_messaging.schema import outbox
from sqlalchemy import Connection, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.coordinator.app.db.schema import redemption_transitions, redemptions
from services.coordinator.app.repository import log_transition

logger = logging.getLogger(__name__)

POINTS_DEDUCTED = "wallet.points_deducted"
DEDUCTION_REJECTED = "wallet.deduction_rejected"
POINTS_REFUNDED = "wallet.points_refunded"
INVENTORY_RESERVED = "catalog.inventory_reserved"
RESERVATION_REJECTED = "catalog.reservation_rejected"
INVENTORY_RELEASED = "catalog.inventory_released"
RELEASE_REJECTED = "catalog.release_rejected"
RESULT_AGGREGATES = {
    POINTS_DEDUCTED: "wallet",
    DEDUCTION_REJECTED: "wallet",
    POINTS_REFUNDED: "wallet",
    INVENTORY_RESERVED: "reward",
    RESERVATION_REJECTED: "reward",
    INVENTORY_RELEASED: "reward",
    RELEASE_REJECTED: "reward",
}
TERMINAL_SAGA_STATES = {"COMPLETED", "FAILED", "COMPENSATED"}
COMMAND_RESULTS = {
    "wallet.deduct_points": {POINTS_DEDUCTED, DEDUCTION_REJECTED},
    "catalog.reserve_inventory": {INVENTORY_RESERVED, RESERVATION_REJECTED},
    "catalog.release_inventory": {INVENTORY_RELEASED, RELEASE_REJECTED},
    "wallet.refund_points": {POINTS_REFUNDED},
}


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
            updated,
            "REQUESTED",
            "DEDUCTING_POINTS",
            "wallet deduction requested",
            command.event_id,
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

        self._validate_current_command(connection, row, envelope)

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
                # A definite rejection proves that Catalog did not reserve, so
                # only the confirmed Wallet deduction needs compensation.
                return self._request_refund(
                    connection,
                    row,
                    "inventory reservation rejected; refund requested",
                    envelope.event_id,
                )
        elif row.status == "COMPENSATING":
            if envelope.event_type == INVENTORY_RELEASED:
                # Release is sequenced before refund when the reservation outcome
                # was unknown. Once Catalog confirms, refund the known deduction.
                return self._request_refund(
                    connection,
                    row,
                    "inventory release confirmed; refund requested",
                    envelope.event_id,
                )
            if envelope.event_type == RELEASE_REJECTED:
                return self._compensation_rejected(connection, row, envelope)
            if envelope.event_type == POINTS_REFUNDED:
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

    def begin_compensation(
        self,
        connection: Connection,
        transaction_id: UUID,
        reason: str,
        *,
        reservation_may_exist: bool,
    ):
        """Enter compensation after a timeout or another unknown outcome.

        LM-024 calls this boundary when it owns timeout detection. A deduction
        timeout requires a safe refund/tombstone. A reservation timeout first
        releases inventory because the reservation may have committed, then
        refunds the confirmed deduction.
        """
        row = self._locked(connection, transaction_id)
        if row.coordination_mode != "saga":
            raise InvalidSagaResult("only Saga redemptions can be compensated")
        if row.status in TERMINAL_SAGA_STATES or row.status == "COMPENSATING":
            return []
        if row.status == "DEDUCTING_POINTS":
            if reservation_may_exist:
                raise InvalidSagaResult("inventory cannot exist before the deduction completes")
            return self._request_refund(connection, row, reason, row.current_command_id)
        if row.status == "RESERVING_INVENTORY":
            if reservation_may_exist:
                return self._request_release(connection, row, reason, row.current_command_id)
            return self._request_refund(connection, row, reason, row.current_command_id)
        raise InvalidSagaResult(f"cannot compensate a redemption in {row.status}")

    def retry_compensation(
        self,
        connection: Connection,
        transaction_id: UUID,
        reason: str = "compensation retry requested",
    ):
        """Reissue a compensation command whose result is still unknown.

        This covers a lost result/acknowledgement: participants persist their
        outcome by transaction and operation, so replay returns the same
        applied result (or duplicate) without repeating the business effect.
        A durable ``release_rejected`` result is not retryable here; Catalog
        will replay that rejection for this transaction and operation. Such a
        case remains COMPENSATING for operator investigation.
        """
        row = self._locked(connection, transaction_id)
        if row.coordination_mode != "saga" or row.status != "COMPENSATING":
            raise InvalidSagaResult("only a compensating Saga can be retried")
        if row.current_command_id is None:
            raise InvalidSagaResult(
                "no compensation command is pending; its result was already received "
                "and requires operator review"
            )
        command = connection.scalar(
            select(outbox.c.envelope).where(outbox.c.event_id == row.current_command_id)
        )
        if command is None:
            raise InvalidSagaResult("the pending compensation command is not durable")
        if command["event_type"] == "catalog.release_inventory":
            return self._request_release(connection, row, reason, row.current_command_id)
        if command["event_type"] == "wallet.refund_points":
            return self._request_refund(connection, row, reason, row.current_command_id)
        raise InvalidSagaResult("the current command is not a compensation")

    def _request_refund(
        self,
        connection: Connection,
        row,
        reason: str,
        causation_id: UUID | None,
    ):
        command = EventEnvelope(
            event_type="wallet.refund_points",
            transaction_id=row.transaction_id,
            correlation_id=row.correlation_id,
            causation_id=causation_id,
            aggregate_type="wallet",
            aggregate_id=row.user_id,
            workflow_step=row.workflow_step + 1,
            fencing_token=row.fencing_token,
            payload={"user_id": row.user_id, "points": row.total_points},
        )
        updated = self._transition(
            connection,
            row,
            "COMPENSATING",
            reason,
            command_id=command.event_id,
        )
        return [
            _audit_event(
                updated,
                row.status,
                "COMPENSATING",
                reason,
                causation_id,
            ),
            WALLET_COMMANDS.message(command),
        ]

    def _request_release(
        self,
        connection: Connection,
        row,
        reason: str,
        causation_id: UUID | None,
    ):
        command = EventEnvelope(
            event_type="catalog.release_inventory",
            transaction_id=row.transaction_id,
            correlation_id=row.correlation_id,
            causation_id=causation_id,
            aggregate_type="reward",
            aggregate_id=row.reward_id,
            workflow_step=row.workflow_step + 1,
            fencing_token=row.fencing_token,
            payload={"reward_id": row.reward_id, "quantity": row.quantity},
        )
        updated = self._transition(
            connection,
            row,
            "COMPENSATING",
            reason,
            command_id=command.event_id,
        )
        return [
            _audit_event(
                updated,
                row.status,
                "COMPENSATING",
                reason,
                causation_id,
            ),
            CATALOG_COMMANDS.message(command),
        ]

    def _compensation_rejected(self, connection: Connection, row, result: EventEnvelope):
        reason = f"inventory release rejected: {result.payload.get('reason', 'unknown')}"
        updated = self._transition(
            connection,
            row,
            "COMPENSATING",
            reason,
            # The durable rejection is a completed response, not a pending
            # command. Keeping its ID here would make retry tooling replay a
            # Catalog outcome that is permanently stored for this operation.
            command_id=None,
        )
        # There is deliberately no terminal COMPENSATION_FAILED state. Keep the
        # redemption visible in COMPENSATING for operator investigation, but
        # clear the pending command because Catalog's durable rejection cannot
        # be changed by replaying that operation.
        return [
            _audit_event(
                updated,
                "COMPENSATING",
                "COMPENSATING",
                reason,
                result.event_id,
            )
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
    def _validate_current_command(connection: Connection, row, envelope: EventEnvelope) -> None:
        if envelope.causation_id != row.current_command_id:
            raise InvalidSagaResult("result does not acknowledge the current command")
        if envelope.workflow_step != row.workflow_step:
            raise InvalidSagaResult("result workflow_step does not match the current step")
        command = connection.scalar(
            select(outbox.c.envelope).where(outbox.c.event_id == row.current_command_id)
        )
        if command is None:
            raise InvalidSagaResult("the current command is not durable")
        expected = COMMAND_RESULTS.get(command["event_type"], set())
        if envelope.event_type not in expected:
            raise InvalidSagaResult("result type does not match the current command")

    def fail_start(self, connection: Connection, transaction_id: UUID, reason: str):
        row = self._locked(connection, transaction_id)
        if row.coordination_mode != "saga":
            raise InvalidSagaResult("only Saga redemptions can be failed by the Saga worker")
        if row.status != "REQUESTED":
            return []
        updated = self._transition(connection, row, "FAILED", reason, command_id=None)
        requested = _audit_event(row, None, "REQUESTED", "redemption created", causation_id=None)
        failed = _audit_event(
            updated,
            "REQUESTED",
            "FAILED",
            reason,
            causation_id=requested.envelope.event_id,
        )
        return [requested, failed]

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
        record_transition(
            mode=row.coordination_mode,
            from_state=row.status,
            to_state=to_status,
            created_at=row.created_at,
        )
        log_transition(
            transaction_id=row.transaction_id,
            correlation_id=row.correlation_id,
            coordination_mode=row.coordination_mode,
            from_status=row.status,
            to_status=to_status,
            workflow_step=updated.workflow_step,
            reason=reason,
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
        # Transition metrics publish only after session.begin() commits (LM-034).
        with metrics_on_commit():
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
                with metrics_on_commit():
                    async with self.sessions() as session, session.begin():
                        messages = await session.run_sync(
                            lambda sync, transaction_id=transaction_id, quote=quote: (
                                self.machine.start(sync.connection(), transaction_id, quote)
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
            except Exception:
                logger.exception("unexpected Saga starter failure; retrying after poll interval")
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.poll_interval)
