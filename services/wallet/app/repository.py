"""PostgreSQL repository for Wallet core operations (LM-007)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import Protocol
from uuid import UUID

from sqlalchemy import Connection, Engine, create_engine, insert, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.exc import IntegrityError

from services.wallet.app.db.schema import deduction_rejections, wallet_operations, wallets
from services.wallet.app.errors import (
    DeductionAlreadyApplied,
    DeductionNotFound,
    InsufficientPoints,
    OperationConflict,
    WalletNotFound,
)


@dataclass(frozen=True)
class BalanceRecord:
    user_id: str
    available_points: int
    version: int
    updated_at: datetime


@dataclass(frozen=True)
class OperationRecord(BalanceRecord):
    transaction_id: UUID
    operation: str
    points: int
    duplicate: bool = False


class WalletRepository(Protocol):
    def get_balance(self, user_id: str) -> BalanceRecord: ...

    def deduct(
        self,
        user_id: str,
        transaction_id: UUID,
        points: int,
        event_id: UUID | None,
        *,
        connection: Connection | None = None,
    ) -> OperationRecord: ...

    def refund(
        self,
        user_id: str,
        transaction_id: UUID,
        event_id: UUID | None,
        *,
        connection: Connection | None = None,
    ) -> OperationRecord: ...


def database_url() -> str:
    if override := os.environ.get("WALLET_DATABASE_URL") or os.environ.get("DATABASE_URL"):
        return override
    password = os.environ.get("POSTGRES_PASSWORD", "ledgermesh_dev")
    return f"postgresql+psycopg://wallet:{password}@wallet-db:5432/wallet"


@lru_cache
def get_engine(url: str | None = None) -> Engine:
    return create_engine(url or database_url(), pool_pre_ping=True)


class PostgresWalletRepository:
    """Applies each balance mutation and operation record in one DB transaction."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_balance(self, user_id: str) -> BalanceRecord:
        with self.engine.connect() as connection:
            row = connection.execute(
                select(
                    wallets.c.user_id,
                    wallets.c.available_points,
                    wallets.c.version,
                    wallets.c.updated_at,
                ).where(wallets.c.user_id == user_id)
            ).one_or_none()
        if row is None:
            raise WalletNotFound(f"Wallet {user_id!r} does not exist.")
        return BalanceRecord(**row._mapping)

    def deduct(
        self,
        user_id: str,
        transaction_id: UUID,
        points: int,
        event_id: UUID | None,
        *,
        connection: Connection | None = None,
    ) -> OperationRecord:
        if connection is not None:
            return self._deduct(connection, user_id, transaction_id, points, event_id)
        with self.engine.begin() as owned_connection:
            return self._deduct(owned_connection, user_id, transaction_id, points, event_id)

    def _deduct(
        self,
        connection: Connection,
        user_id: str,
        transaction_id: UUID,
        points: int,
        event_id: UUID | None,
    ) -> OperationRecord:
        wallet = self._locked_wallet(connection, user_id)
        duplicate = self._existing_operation(connection, transaction_id, "deduct")
        if duplicate is not None:
            self._require_same_operation(duplicate, user_id, points, transaction_id)
            return self._operation_result(
                connection, duplicate, transaction_id, "deduct", duplicate=True
            )

        if wallet.available_points < points:
            raise InsufficientPoints(
                f"Wallet {user_id!r} has {wallet.available_points} points; {points} required.",
                transaction_id,
            )

        connection.execute(
            update(wallets)
            .where(wallets.c.user_id == user_id)
            .values(
                available_points=wallets.c.available_points - points,
                version=wallets.c.version + 1,
                updated_at=datetime.now(UTC),
            )
        )
        try:
            operation = connection.execute(
                insert(wallet_operations)
                .values(
                    transaction_id=transaction_id,
                    user_id=user_id,
                    operation="deduct",
                    points=points,
                    status="applied",
                    event_id=event_id,
                )
                .returning(*wallet_operations.c)
            ).one()
        except IntegrityError as error:
            raise OperationConflict(
                "The transaction ID is already used by a different deduction.", transaction_id
            ) from error
        return self._operation_result(connection, operation, transaction_id, "deduct")

    def refund(
        self,
        user_id: str,
        transaction_id: UUID,
        event_id: UUID | None,
        *,
        connection: Connection | None = None,
    ) -> OperationRecord:
        if connection is not None:
            return self._refund(connection, user_id, transaction_id, event_id)
        with self.engine.begin() as owned_connection:
            return self._refund(owned_connection, user_id, transaction_id, event_id)

    def _refund(
        self,
        connection: Connection,
        user_id: str,
        transaction_id: UUID,
        event_id: UUID | None,
    ) -> OperationRecord:
        self._locked_wallet(connection, user_id)
        existing_refund = self._existing_operation(connection, transaction_id, "refund")
        if existing_refund is not None:
            if existing_refund.user_id != user_id:
                raise OperationConflict(
                    "The transaction ID belongs to another wallet.", transaction_id
                )
            return self._operation_result(
                connection, existing_refund, transaction_id, "refund", duplicate=True
            )

        deduction = self._existing_operation(connection, transaction_id, "deduct")
        if deduction is None or deduction.status != "applied":
            raise DeductionNotFound(
                "Refund requires an applied deduction with the same transaction ID.",
                transaction_id,
            )
        if deduction.user_id != user_id:
            raise OperationConflict(
                "The original deduction belongs to another wallet.", transaction_id
            )

        connection.execute(
            update(wallets)
            .where(wallets.c.user_id == user_id)
            .values(
                available_points=wallets.c.available_points + deduction.points,
                version=wallets.c.version + 1,
                updated_at=datetime.now(UTC),
            )
        )
        try:
            operation = connection.execute(
                insert(wallet_operations)
                .values(
                    transaction_id=transaction_id,
                    user_id=user_id,
                    operation="refund",
                    points=deduction.points,
                    status="applied",
                    event_id=event_id,
                )
                .returning(*wallet_operations.c)
            ).one()
        except IntegrityError as error:
            raise OperationConflict(
                "The transaction ID is already used by a different refund.", transaction_id
            ) from error
        return self._operation_result(connection, operation, transaction_id, "refund")

    def record_deduction_tombstone(
        self,
        user_id: str,
        transaction_id: UUID,
        points: int,
        event_id: UUID | None,
        *,
        connection: Connection | None = None,
    ) -> OperationRecord:
        """Cancel a deduction that has not arrived yet (ADR-001 section 7.1).

        Stores a ``deduct`` row with status ``tombstone``. It occupies the
        deduction's unique key, so the late deduction is rejected instead of
        applied. The balance and version are unchanged.
        """
        if connection is not None:
            return self._record_deduction_tombstone(
                connection, user_id, transaction_id, points, event_id
            )
        with self.engine.begin() as owned_connection:
            return self._record_deduction_tombstone(
                owned_connection, user_id, transaction_id, points, event_id
            )

    def _record_deduction_tombstone(
        self,
        connection: Connection,
        user_id: str,
        transaction_id: UUID,
        points: int,
        event_id: UUID | None,
    ) -> OperationRecord:
        # Same lock as deduct, so a deduction cannot slip in between the check
        # and the insert.
        self._locked_wallet(connection, user_id)
        existing = self._existing_operation(connection, transaction_id, "deduct")
        if existing is not None:
            if existing.user_id != user_id:
                raise OperationConflict(
                    "The transaction ID belongs to another wallet.", transaction_id
                )
            if existing.status == "tombstone":
                # Only an identical early refund is a duplicate; a different
                # amount for the same transaction is a conflicting command.
                if existing.points != points:
                    raise OperationConflict(
                        "The transaction ID was cancelled for a different amount.",
                        transaction_id,
                    )
                return self._operation_result(
                    connection, existing, transaction_id, "deduct", duplicate=True
                )
            raise DeductionAlreadyApplied(
                "The deduction has already been applied; refund it instead.", transaction_id
            )
        try:
            tombstone = connection.execute(
                insert(wallet_operations)
                .values(
                    transaction_id=transaction_id,
                    user_id=user_id,
                    operation="deduct",
                    points=points,
                    status="tombstone",
                    event_id=event_id,
                )
                .returning(*wallet_operations.c)
            ).one()
        except IntegrityError as error:
            raise OperationConflict(
                "The transaction ID is already used by another operation.", transaction_id
            ) from error
        return self._operation_result(connection, tombstone, transaction_id, "deduct")

    def lock_wallet(self, user_id: str, *, connection: Connection) -> None:
        """Lock the wallet row for the rest of the caller's transaction."""
        self._locked_wallet(connection, user_id)

    def stored_deduction(self, transaction_id: UUID, *, connection: Connection):
        """Return the stored deduction row (``applied`` or ``tombstone``), if any."""
        return self._existing_operation(connection, transaction_id, "deduct")

    def stored_rejection(self, transaction_id: UUID, *, connection: Connection):
        """Return the stored Saga rejection for ``transaction_id``, if any."""
        return connection.execute(
            select(deduction_rejections).where(
                deduction_rejections.c.transaction_id == transaction_id
            )
        ).one_or_none()

    def record_rejection(
        self,
        transaction_id: UUID,
        user_id: str,
        points: int,
        reason: str,
        event_id: UUID | None,
        *,
        connection: Connection,
    ) -> None:
        """Remember that the deduction for ``transaction_id`` was rejected."""
        connection.execute(
            postgresql_insert(deduction_rejections)
            .values(
                transaction_id=transaction_id,
                user_id=user_id,
                points=points,
                reason=reason,
                event_id=event_id,
            )
            .on_conflict_do_nothing(index_elements=[deduction_rejections.c.transaction_id])
        )

    @staticmethod
    def _existing_operation(connection: Connection, transaction_id: UUID, operation: str):
        return connection.execute(
            select(wallet_operations).where(
                wallet_operations.c.transaction_id == transaction_id,
                wallet_operations.c.operation == operation,
            )
        ).one_or_none()

    @staticmethod
    def _locked_wallet(connection: Connection, user_id: str):
        row = connection.execute(
            select(wallets).where(wallets.c.user_id == user_id).with_for_update()
        ).one_or_none()
        if row is None:
            raise WalletNotFound(f"Wallet {user_id!r} does not exist.")
        return row

    @staticmethod
    def _require_same_operation(operation, user_id: str, points: int, transaction_id: UUID) -> None:
        if (
            operation.user_id != user_id
            or operation.points != points
            or operation.status != "applied"
        ):
            raise OperationConflict(
                "The transaction ID is already used by a different deduction.", transaction_id
            )

    @staticmethod
    def _operation_result(
        connection: Connection,
        operation,
        transaction_id: UUID,
        operation_name: str,
        *,
        duplicate: bool = False,
    ) -> OperationRecord:
        wallet = connection.execute(
            select(wallets).where(wallets.c.user_id == operation.user_id)
        ).one()
        return OperationRecord(
            user_id=wallet.user_id,
            available_points=wallet.available_points,
            version=wallet.version,
            updated_at=wallet.updated_at,
            transaction_id=transaction_id,
            operation=operation_name,
            points=operation.points,
            duplicate=duplicate,
        )
