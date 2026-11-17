"""PostgreSQL repository for Catalog core operations (LM-008)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import Protocol
from uuid import UUID

from sqlalchemy import Connection, Engine, create_engine, insert, select, update
from sqlalchemy.exc import IntegrityError

from services.catalog.app.db.schema import reservations, rewards
from services.catalog.app.errors import (
    InsufficientInventory,
    InvalidReservationTransition,
    ReservationConflict,
    ReservationNotFound,
    RewardNotFound,
)


@dataclass(frozen=True)
class RewardRecord:
    reward_id: str
    name: str
    point_cost: int
    total_inventory: int
    available_inventory: int
    version: int
    updated_at: datetime


@dataclass(frozen=True)
class ReservationRecord:
    transaction_id: UUID
    reward_id: str
    quantity: int
    state: str
    available_inventory: int
    version: int
    updated_at: datetime
    duplicate: bool = False
    tombstone: bool = False


class CatalogRepository(Protocol):
    def list_rewards(self, *, available_only: bool = True) -> list[RewardRecord]: ...

    def get_reward(self, reward_id: str) -> RewardRecord: ...

    def reserve(
        self,
        reward_id: str,
        transaction_id: UUID,
        quantity: int,
        event_id: UUID | None,
        *,
        connection: Connection | None = None,
    ) -> ReservationRecord: ...

    def release(
        self,
        reward_id: str,
        transaction_id: UUID,
        quantity: int,
        event_id: UUID | None,
        *,
        connection: Connection | None = None,
    ) -> ReservationRecord: ...

    def fulfill(
        self,
        reward_id: str,
        transaction_id: UUID,
        event_id: UUID | None,
        *,
        connection: Connection | None = None,
    ) -> ReservationRecord: ...


def database_url() -> str:
    if override := os.environ.get("CATALOG_DATABASE_URL") or os.environ.get("DATABASE_URL"):
        return override
    password = os.environ.get("POSTGRES_PASSWORD", "ledgermesh_dev")
    return f"postgresql+psycopg://catalog:{password}@catalog-db:5432/catalog"


@lru_cache
def get_engine(url: str | None = None) -> Engine:
    return create_engine(url or database_url(), pool_pre_ping=True)


class PostgresCatalogRepository:
    """Applies each inventory mutation and reservation row in one transaction."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def list_rewards(self, *, available_only: bool = True) -> list[RewardRecord]:
        statement = select(rewards).order_by(rewards.c.reward_id)
        if available_only:
            statement = statement.where(rewards.c.available_inventory > 0)
        with self.engine.connect() as connection:
            rows = connection.execute(statement).all()
        return [self._reward_record(row) for row in rows]

    def get_reward(self, reward_id: str) -> RewardRecord:
        with self.engine.connect() as connection:
            row = connection.execute(
                select(rewards).where(rewards.c.reward_id == reward_id)
            ).one_or_none()
        if row is None:
            raise RewardNotFound(f"Reward {reward_id!r} does not exist.")
        return self._reward_record(row)

    def reserve(
        self,
        reward_id: str,
        transaction_id: UUID,
        quantity: int,
        event_id: UUID | None,
        *,
        connection: Connection | None = None,
    ) -> ReservationRecord:
        if connection is not None:
            return self._reserve(connection, reward_id, transaction_id, quantity, event_id)
        with self.engine.begin() as owned_connection:
            return self._reserve(owned_connection, reward_id, transaction_id, quantity, event_id)

    def _reserve(
        self,
        connection: Connection,
        reward_id: str,
        transaction_id: UUID,
        quantity: int,
        event_id: UUID | None,
    ) -> ReservationRecord:
        reward = self._locked_reward(connection, reward_id)
        existing = self._existing_reservation(connection, transaction_id)
        if existing is not None:
            self._require_same_reservation(existing, reward_id, quantity, transaction_id)
            if existing.status == "reserved":
                return self._reservation_result(connection, existing, duplicate=True)
            if existing.status == "tombstone":
                raise ReservationConflict(
                    "The reservation was cancelled before it could be applied.", transaction_id
                )
            raise InvalidReservationTransition(
                f"A {existing.status} reservation cannot be reserved again.", transaction_id
            )

        if reward.available_inventory < quantity:
            raise InsufficientInventory(
                f"Reward {reward_id!r} has {reward.available_inventory} units; "
                f"{quantity} required.",
                transaction_id,
            )

        connection.execute(
            update(rewards)
            .where(rewards.c.reward_id == reward_id)
            .values(
                available_inventory=rewards.c.available_inventory - quantity,
                version=rewards.c.version + 1,
                updated_at=datetime.now(UTC),
            )
        )
        try:
            reservation = connection.execute(
                insert(reservations)
                .values(
                    transaction_id=transaction_id,
                    reward_id=reward_id,
                    quantity=quantity,
                    status="reserved",
                    event_id=event_id,
                )
                .returning(*reservations.c)
            ).one()
        except IntegrityError as error:
            raise ReservationConflict(
                "The transaction ID is already used by another reservation.", transaction_id
            ) from error
        return self._reservation_result(connection, reservation)

    def release(
        self,
        reward_id: str,
        transaction_id: UUID,
        quantity: int,
        event_id: UUID | None,
        *,
        connection: Connection | None = None,
    ) -> ReservationRecord:
        if connection is not None:
            return self._release(connection, reward_id, transaction_id, quantity, event_id)
        with self.engine.begin() as owned_connection:
            return self._release(owned_connection, reward_id, transaction_id, quantity, event_id)

    def _release(
        self,
        connection: Connection,
        reward_id: str,
        transaction_id: UUID,
        quantity: int,
        event_id: UUID | None,
    ) -> ReservationRecord:
        self._locked_reward(connection, reward_id)
        existing = self._existing_reservation(connection, transaction_id)
        if existing is None:
            try:
                tombstone = connection.execute(
                    insert(reservations)
                    .values(
                        transaction_id=transaction_id,
                        reward_id=reward_id,
                        quantity=quantity,
                        status="tombstone",
                        event_id=event_id,
                    )
                    .returning(*reservations.c)
                ).one()
            except IntegrityError as error:
                raise ReservationConflict(
                    "The transaction ID is already used by another reservation.", transaction_id
                ) from error
            return self._reservation_result(connection, tombstone, tombstone=True)

        self._require_same_reservation(existing, reward_id, quantity, transaction_id)
        if existing.status in {"released", "tombstone"}:
            return self._reservation_result(
                connection,
                existing,
                duplicate=True,
                tombstone=existing.status == "tombstone",
            )
        if existing.status == "fulfilled":
            raise InvalidReservationTransition(
                "A fulfilled reservation cannot be released.", transaction_id
            )

        connection.execute(
            update(rewards)
            .where(rewards.c.reward_id == reward_id)
            .values(
                available_inventory=rewards.c.available_inventory + existing.quantity,
                version=rewards.c.version + 1,
                updated_at=datetime.now(UTC),
            )
        )
        released = connection.execute(
            update(reservations)
            .where(reservations.c.id == existing.id)
            .values(status="released", updated_at=datetime.now(UTC))
            .returning(*reservations.c)
        ).one()
        return self._reservation_result(connection, released)

    def fulfill(
        self,
        reward_id: str,
        transaction_id: UUID,
        event_id: UUID | None,
        *,
        connection: Connection | None = None,
    ) -> ReservationRecord:
        if connection is not None:
            return self._fulfill(connection, reward_id, transaction_id, event_id)
        with self.engine.begin() as owned_connection:
            return self._fulfill(owned_connection, reward_id, transaction_id, event_id)

    def _fulfill(
        self,
        connection: Connection,
        reward_id: str,
        transaction_id: UUID,
        event_id: UUID | None,
    ) -> ReservationRecord:
        # The row's event_id identifies the command that created the reservation.
        # Transition-specific event history belongs in the inbox/audit ledger.
        del event_id
        self._locked_reward(connection, reward_id)
        existing = self._existing_reservation(connection, transaction_id)
        if existing is None:
            raise ReservationNotFound(
                "Fulfillment requires an existing reservation.", transaction_id
            )
        if existing.reward_id != reward_id:
            raise ReservationConflict(
                "The transaction ID belongs to another reward.", transaction_id
            )
        if existing.status == "fulfilled":
            return self._reservation_result(connection, existing, duplicate=True)
        if existing.status != "reserved":
            raise InvalidReservationTransition(
                f"A {existing.status} reservation cannot be fulfilled.", transaction_id
            )

        fulfilled = connection.execute(
            update(reservations)
            .where(reservations.c.id == existing.id)
            .values(status="fulfilled", updated_at=datetime.now(UTC))
            .returning(*reservations.c)
        ).one()
        return self._reservation_result(connection, fulfilled)

    @staticmethod
    def _existing_reservation(connection: Connection, transaction_id: UUID):
        return connection.execute(
            select(reservations).where(reservations.c.transaction_id == transaction_id)
        ).one_or_none()

    @staticmethod
    def _locked_reward(connection: Connection, reward_id: str):
        row = connection.execute(
            select(rewards).where(rewards.c.reward_id == reward_id).with_for_update()
        ).one_or_none()
        if row is None:
            raise RewardNotFound(f"Reward {reward_id!r} does not exist.")
        return row

    @staticmethod
    def _require_same_reservation(
        reservation, reward_id: str, quantity: int, transaction_id: UUID
    ) -> None:
        if reservation.reward_id != reward_id or reservation.quantity != quantity:
            raise ReservationConflict(
                "The transaction ID is already used by a different reservation.", transaction_id
            )

    @staticmethod
    def _reward_record(row) -> RewardRecord:
        return RewardRecord(
            reward_id=row.reward_id,
            name=row.name,
            point_cost=row.point_cost,
            total_inventory=row.total_inventory,
            available_inventory=row.available_inventory,
            version=row.version,
            updated_at=row.updated_at,
        )

    @staticmethod
    def _reservation_result(
        connection: Connection,
        reservation,
        *,
        duplicate: bool = False,
        tombstone: bool = False,
    ) -> ReservationRecord:
        reward = connection.execute(
            select(rewards).where(rewards.c.reward_id == reservation.reward_id)
        ).one()
        return ReservationRecord(
            transaction_id=reservation.transaction_id,
            reward_id=reservation.reward_id,
            quantity=reservation.quantity,
            state=reservation.status,
            available_inventory=reward.available_inventory,
            version=reward.version,
            updated_at=reservation.updated_at,
            duplicate=duplicate,
            tombstone=tombstone,
        )
