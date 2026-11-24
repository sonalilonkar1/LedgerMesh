"""PostgreSQL repository for Catalog core operations (LM-008).

Inventory changes use optimistic concurrency (LM-028, ADR-001 section 8.4):

1. read the reward and its ``version``, and the transaction's reservation,
   without locking anything;
2. decide from that state (duplicate? cancelled? enough units?);
3. in one savepoint, apply the inventory change with a compare-and-set
   ``UPDATE`` that only matches while the reward still has the version that
   was read and the new inventory stays within ``0..total_inventory``, and
   write the reservation row (insert it, or move it out of ``reserved``);
4. if either write finds the state changed (another change won, or another
   command took the transaction id), roll back the savepoint, reload and
   decide again, up to ``max_attempts`` times. A request is never replayed
   against inventory it did not check.

Reservation status changes are conditional on the row still being
``reserved``, so a concurrent release and fulfilment can't both apply.
"""

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
    ConcurrentUpdateConflict,
    InsufficientInventory,
    InvalidReservationTransition,
    ReservationConflict,
    ReservationNotFound,
    RewardNotFound,
)

# Each lost attempt means another change to the same reward or reservation
# committed, so this bounds how long one request can keep losing.
MAX_VERSION_ATTEMPTS = 10


class _LostRace(Exception):
    """Internal: a concurrent change won this attempt; reload and decide again."""


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

    def __init__(self, engine: Engine, *, max_attempts: int = MAX_VERSION_ATTEMPTS) -> None:
        self.engine = engine
        self.max_attempts = max_attempts

    def compare_and_set_inventory(
        self, connection: Connection, reward_id: str, *, expected_version: int, delta: int
    ) -> RewardRecord | None:
        """Add ``delta`` units only if the reward is still at ``expected_version``.

        The version check and both inventory bounds are in the ``WHERE``
        clause, so the check and the write are one atomic statement. Returns
        the new reward state, or ``None`` with nothing changed if the version
        moved on or the inventory would leave ``0..total_inventory``.
        """
        row = connection.execute(
            update(rewards)
            .where(
                rewards.c.reward_id == reward_id,
                rewards.c.version == expected_version,
                rewards.c.available_inventory + delta >= 0,
                rewards.c.available_inventory + delta <= rewards.c.total_inventory,
            )
            .values(
                available_inventory=rewards.c.available_inventory + delta,
                version=rewards.c.version + 1,
                updated_at=datetime.now(UTC),
            )
            .returning(*rewards.c)
        ).one_or_none()
        return None if row is None else self._reward_record(row)

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
        for _attempt in range(self.max_attempts):
            # Reload on every attempt: a lost attempt means the state changed.
            reward = self._current_reward(connection, reward_id)
            existing = self._existing_reservation(connection, transaction_id)
            if existing is not None:
                self._require_same_reservation(existing, reward_id, quantity, transaction_id)
                if existing.status == "reserved":
                    return self._reservation_result(connection, existing, duplicate=True)
                if existing.status == "tombstone":
                    raise ReservationConflict(
                        "The reservation was cancelled before it could be applied.",
                        transaction_id,
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

            try:
                with connection.begin_nested():
                    if (
                        self.compare_and_set_inventory(
                            connection, reward_id, expected_version=reward.version, delta=-quantity
                        )
                        is None
                    ):
                        raise _LostRace
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
            except (_LostRace, IntegrityError):
                # Another change won the version, or another command (a
                # duplicate, a release tombstone, a different reservation)
                # took this transaction id. The reload decides which.
                continue
            return self._reservation_result(connection, reservation)

        raise ConcurrentUpdateConflict(
            f"Reward {reward_id!r} kept changing; no reservation was applied.", transaction_id
        )

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
        for _attempt in range(self.max_attempts):
            reward = self._current_reward(connection, reward_id)
            existing = self._existing_reservation(connection, transaction_id)
            if existing is None:
                # ADR-001 7.1: a release before its reservation leaves a
                # tombstone, so the late reservation is refused. Inventory and
                # version are unchanged.
                try:
                    with connection.begin_nested():
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
                except IntegrityError:
                    continue  # a concurrent command took the id; the reload decides
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

            try:
                with connection.begin_nested():
                    if (
                        self.compare_and_set_inventory(
                            connection,
                            reward_id,
                            expected_version=reward.version,
                            delta=existing.quantity,
                        )
                        is None
                    ):
                        raise _LostRace
                    released = self._move_reserved(connection, existing.id, "released")
                    if released is None:
                        raise _LostRace  # released or fulfilled concurrently
            except _LostRace:
                continue
            return self._reservation_result(connection, released)

        raise ConcurrentUpdateConflict(
            f"Reward {reward_id!r} kept changing; no release was applied.", transaction_id
        )

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
        for _attempt in range(self.max_attempts):
            self._current_reward(connection, reward_id)
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

            # Inventory doesn't change, but a concurrent release might: only
            # a row that is still reserved can be fulfilled.
            fulfilled = self._move_reserved(connection, existing.id, "fulfilled")
            if fulfilled is not None:
                return self._reservation_result(connection, fulfilled)

        raise ConcurrentUpdateConflict(
            "The reservation kept changing; it was not fulfilled.", transaction_id
        )

    @staticmethod
    def _move_reserved(connection: Connection, reservation_id: int, status: str):
        """Move a reservation out of ``reserved``; ``None`` if it already left it."""
        return connection.execute(
            update(reservations)
            .where(reservations.c.id == reservation_id, reservations.c.status == "reserved")
            .values(status=status, updated_at=datetime.now(UTC))
            .returning(*reservations.c)
        ).one_or_none()

    @staticmethod
    def _existing_reservation(connection: Connection, transaction_id: UUID):
        return connection.execute(
            select(reservations).where(reservations.c.transaction_id == transaction_id)
        ).one_or_none()

    def _current_reward(self, connection: Connection, reward_id: str):
        """Read the latest committed reward row, without locking it."""
        row = connection.execute(
            select(rewards).where(rewards.c.reward_id == reward_id)
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
