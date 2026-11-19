"""Catalog Saga command handling with transactional inbox/outbox replies (LM-021)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ledgermesh_messaging import (
    CATALOG_RESULTS,
    EventEnvelope,
    OutgoingMessage,
    PermanentError,
)
from sqlalchemy import Connection, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.ext.asyncio import AsyncSession

from services.catalog.app.db.schema import catalog_saga_outcomes, reservations
from services.catalog.app.errors import (
    InsufficientInventory,
    InvalidReservationTransition,
    ReservationConflict,
    RewardNotFound,
)
from services.catalog.app.repository import PostgresCatalogRepository

RESERVE_INVENTORY = "catalog.reserve_inventory"
RELEASE_INVENTORY = "catalog.release_inventory"
INVENTORY_RESERVED = "catalog.inventory_reserved"
RESERVATION_REJECTED = "catalog.reservation_rejected"
INVENTORY_RELEASED = "catalog.inventory_released"
RELEASE_REJECTED = "catalog.release_rejected"
RESERVE_STEP = 3


class InvalidCommand(PermanentError):
    """The command is malformed or reuses an existing command identity."""


@dataclass(frozen=True)
class CatalogCommand:
    envelope: EventEnvelope
    reward_id: str
    quantity: int

    @property
    def kind(self) -> str:
        return "reserve" if self.envelope.event_type == RESERVE_INVENTORY else "release"


def parse_command(envelope: EventEnvelope) -> CatalogCommand:
    if envelope.event_type not in {RESERVE_INVENTORY, RELEASE_INVENTORY}:
        raise InvalidCommand(f"unsupported Catalog command {envelope.event_type!r}")
    if envelope.aggregate_type != "reward":
        raise InvalidCommand("Catalog commands must target the reward aggregate")
    if envelope.workflow_step is None or envelope.workflow_step < 1:
        raise InvalidCommand("Catalog commands must carry a positive workflow_step")
    if envelope.event_type == RESERVE_INVENTORY and envelope.workflow_step != RESERVE_STEP:
        raise InvalidCommand(f"a reservation must be workflow_step {RESERVE_STEP}")
    if envelope.event_type == RELEASE_INVENTORY and envelope.workflow_step <= RESERVE_STEP:
        raise InvalidCommand("a release must be a compensation step after the reservation")
    if envelope.aggregate_version is not None:
        raise InvalidCommand("commands must not carry an aggregate_version")
    reward_id = envelope.payload.get("reward_id")
    if reward_id != envelope.aggregate_id:
        raise InvalidCommand("payload reward_id must match the command's aggregate_id")
    quantity = envelope.payload.get("quantity")
    if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity <= 0:
        raise InvalidCommand("quantity must be a positive integer")
    return CatalogCommand(envelope=envelope, reward_id=reward_id, quantity=quantity)


def result_envelope(
    command: CatalogCommand,
    event_type: str,
    payload: dict[str, Any],
    *,
    aggregate_version: int | None = None,
) -> EventEnvelope:
    source = command.envelope
    return EventEnvelope(
        event_type=event_type,
        transaction_id=source.transaction_id,
        correlation_id=source.correlation_id,
        causation_id=source.event_id,
        aggregate_type="reward",
        aggregate_id=command.reward_id,
        aggregate_version=aggregate_version,
        workflow_step=source.workflow_step,
        fencing_token=source.fencing_token,
        payload=payload,
    )


class CatalogSagaHandler:
    """Apply one Catalog Saga command inside ``process_once``'s transaction."""

    def __init__(self, repository: PostgresCatalogRepository) -> None:
        self.repository = repository

    async def __call__(
        self, session: AsyncSession, envelope: EventEnvelope
    ) -> list[OutgoingMessage]:
        command = parse_command(envelope)
        reply = await session.run_sync(lambda sync: self.apply(sync.connection(), command))
        return [CATALOG_RESULTS.message(reply)]

    def apply(self, connection: Connection, command: CatalogCommand) -> EventEnvelope:
        stored = connection.execute(
            select(catalog_saga_outcomes).where(
                catalog_saga_outcomes.c.transaction_id == command.envelope.transaction_id,
                catalog_saga_outcomes.c.command_type == command.kind,
            )
        ).one_or_none()
        if stored is not None:
            return self._stored_reply(command, stored, duplicate=True)

        reply = (
            self._reserve(connection, command)
            if command.kind == "reserve"
            else self._release(connection, command)
        )
        stored, inserted = self._store_outcome(connection, command, reply)
        # The row that won the unique-key race is authoritative. A concurrent
        # command may have recorded a different identity or result while this
        # delivery was waiting on the reward/reservation locks.
        return self._stored_reply(command, stored, duplicate=not inserted)

    def _stored_reply(self, command: CatalogCommand, stored, *, duplicate: bool) -> EventEnvelope:
        if stored.reward_id != command.reward_id or stored.quantity != command.quantity:
            return self._conflict_reply(
                command, "transaction ID is already used by a different Catalog command"
            )
        payload = dict(stored.result_payload)
        if duplicate and payload.get("outcome") == "applied":
            payload["outcome"] = "duplicate"
        return result_envelope(
            command,
            stored.result_event_type,
            payload,
            aggregate_version=None if duplicate else stored.aggregate_version,
        )

    def _store_outcome(self, connection: Connection, command: CatalogCommand, reply: EventEnvelope):
        stored = connection.execute(
            postgresql_insert(catalog_saga_outcomes)
            .values(
                transaction_id=command.envelope.transaction_id,
                command_type=command.kind,
                reward_id=command.reward_id,
                quantity=command.quantity,
                result_event_type=reply.event_type,
                result_payload=reply.payload,
                aggregate_version=reply.aggregate_version,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    catalog_saga_outcomes.c.transaction_id,
                    catalog_saga_outcomes.c.command_type,
                ]
            )
            .returning(*catalog_saga_outcomes.c)
        ).one_or_none()
        if stored is not None:
            return stored, True

        # The unique index made this statement wait for the winning transaction.
        # Under READ COMMITTED, the following statement sees the committed row.
        stored = connection.execute(
            select(catalog_saga_outcomes).where(
                catalog_saga_outcomes.c.transaction_id == command.envelope.transaction_id,
                catalog_saga_outcomes.c.command_type == command.kind,
            )
        ).one()
        return stored, False

    def _conflict_reply(self, command: CatalogCommand, detail: str) -> EventEnvelope:
        if command.kind == "reserve":
            return self._reserve_rejected(command, "conflict", detail)
        return self._release_rejected(command, "conflict", detail)

    def _reserve(self, connection: Connection, command: CatalogCommand) -> EventEnvelope:
        try:
            with connection.begin_nested():
                record = self.repository.reserve(
                    command.reward_id,
                    command.envelope.transaction_id,
                    command.quantity,
                    command.envelope.event_id,
                    connection=connection,
                )
        except InsufficientInventory as error:
            return self._reserve_rejected(command, "insufficient_inventory", error.detail)
        except RewardNotFound as error:
            return self._reserve_rejected(command, "reward_not_found", error.detail)
        except ReservationConflict as error:
            existing = self._reservation(connection, command)
            if existing is not None and existing.status == "tombstone":
                return self._reserve_rejected(command, "cancelled", error.detail)
            return self._reserve_rejected(command, "conflict", error.detail)
        except InvalidReservationTransition as error:
            return self._reserve_rejected(command, "conflict", error.detail)

        return result_envelope(
            command,
            INVENTORY_RESERVED,
            {
                "reward_id": command.reward_id,
                "quantity": record.quantity,
                "available_inventory": record.available_inventory,
                "outcome": "duplicate" if record.duplicate else "applied",
            },
            aggregate_version=None if record.duplicate else record.version,
        )

    def _release(self, connection: Connection, command: CatalogCommand) -> EventEnvelope:
        try:
            with connection.begin_nested():
                record = self.repository.release(
                    command.reward_id,
                    command.envelope.transaction_id,
                    command.quantity,
                    command.envelope.event_id,
                    connection=connection,
                )
        except RewardNotFound as error:
            return self._release_rejected(command, "reward_not_found", error.detail)
        except ReservationConflict as error:
            return self._release_rejected(command, "conflict", error.detail)
        except InvalidReservationTransition as error:
            return self._release_rejected(command, "invalid_state", error.detail)

        outcome = (
            "cancelled_before_reservation"
            if record.tombstone
            else ("duplicate" if record.duplicate else "applied")
        )
        return result_envelope(
            command,
            INVENTORY_RELEASED,
            {
                "reward_id": command.reward_id,
                "quantity": record.quantity,
                "available_inventory": record.available_inventory,
                "outcome": outcome,
            },
            aggregate_version=None if record.duplicate or record.tombstone else record.version,
        )

    def _reservation(self, connection: Connection, command: CatalogCommand):
        row = connection.execute(
            select(reservations).where(
                reservations.c.transaction_id == command.envelope.transaction_id
            )
        ).one_or_none()
        if row is None:
            return None
        if row.reward_id != command.reward_id or row.quantity != command.quantity:
            return None
        return row

    @staticmethod
    def _reserve_rejected(command: CatalogCommand, reason: str, detail: str) -> EventEnvelope:
        return result_envelope(
            command,
            RESERVATION_REJECTED,
            {
                "reward_id": command.reward_id,
                "quantity": command.quantity,
                "reason": reason,
                "detail": detail,
            },
        )

    @staticmethod
    def _release_rejected(command: CatalogCommand, reason: str, detail: str) -> EventEnvelope:
        return result_envelope(
            command,
            RELEASE_REJECTED,
            {
                "reward_id": command.reward_id,
                "quantity": command.quantity,
                "reason": reason,
                "detail": detail,
            },
        )
