"""PostgreSQL two-phase-commit participant for Wallet deductions (LM-013)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from psycopg import connect, sql
from psycopg.errors import UniqueViolation
from sqlalchemy import Engine, insert, select, text, update
from sqlalchemy.engine import make_url

from services.wallet.app.db.schema import (
    participant_transactions,
    wallet_operations,
)
from services.wallet.app.errors import ParticipantConflict, ParticipantTransactionNotFound


@dataclass(frozen=True)
class ParticipantRecord:
    transaction_id: UUID
    state: str
    updated_at: datetime


class WalletParticipantRepository(Protocol):
    def prepare(
        self, transaction_id: UUID, correlation_id: UUID, user_id: str, points: int
    ) -> ParticipantRecord: ...

    def commit(self, transaction_id: UUID) -> ParticipantRecord: ...

    def abort(self, transaction_id: UUID) -> ParticipantRecord: ...

    def get(self, transaction_id: UUID) -> ParticipantRecord: ...


def request_hash(correlation_id: UUID, user_id: str, points: int) -> str:
    payload = json.dumps(
        {"correlation_id": str(correlation_id), "points": points, "user_id": user_id},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def prepared_gid(transaction_id: UUID) -> str:
    return f"ledgermesh:wallet:{transaction_id}"


class PostgresWalletParticipantRepository:
    """Coordinates visible participant metadata with a PostgreSQL prepared transaction."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @contextmanager
    def _transaction_lock(self, transaction_id: UUID) -> Iterator[None]:
        """Serialize duplicate prepare/decision calls across Wallet instances."""
        connection = self.engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        key = str(transaction_id)
        connection.execute(text("SELECT pg_advisory_lock(hashtextextended(:key, 0))"), {"key": key})
        try:
            yield
        finally:
            connection.execute(
                text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"), {"key": key}
            )
            connection.close()

    def prepare(
        self, transaction_id: UUID, correlation_id: UUID, user_id: str, points: int
    ) -> ParticipantRecord:
        expected_hash = request_hash(correlation_id, user_id, points)
        with self._transaction_lock(transaction_id):
            existing = self._load(transaction_id)
            if existing is not None:
                if existing.request_hash is None:  # An earlier unknown ABORT created a tombstone.
                    return self._record(existing)
                if existing.request_hash != expected_hash:
                    raise ParticipantConflict(
                        "The transaction ID is already associated with a different "
                        "prepare request.",
                        transaction_id,
                    )
                if existing.state != "IN_DOUBT":
                    return self._record(existing)
                reconciled = self._reconcile_in_doubt(transaction_id)
                if reconciled is not None:
                    return reconciled
            else:
                self._insert_claim(transaction_id, correlation_id, user_id, points, expected_hash)

            if self._effect_exists(transaction_id):
                self._set_state(transaction_id, "ABORTED")
                raise ParticipantConflict(
                    "The transaction ID is already used by a Wallet deduction.", transaction_id
                )
            try:
                vote = self._prepare_business_transaction(transaction_id, user_id, points)
            except UniqueViolation as error:
                self._set_state(transaction_id, "ABORTED")
                raise ParticipantConflict(
                    "The transaction ID is already used by a Wallet deduction.", transaction_id
                ) from error
            return self._set_state(transaction_id, vote)

    def commit(self, transaction_id: UUID) -> ParticipantRecord:
        with self._transaction_lock(transaction_id):
            current = self._load(transaction_id)
            if current is None:
                raise ParticipantTransactionNotFound(
                    "The transaction is unknown to the Wallet participant.", transaction_id
                )
            if current.state == "COMMITTED":
                return self._record(current)
            if current.state == "ABORTED":
                raise ParticipantConflict(
                    "An aborted Wallet transaction cannot be committed.", transaction_id
                )

            if self._prepared_exists(transaction_id):
                self._finish_prepared(transaction_id, commit=True)
            elif not self._effect_exists(transaction_id):
                raise ParticipantConflict(
                    "No durable prepared Wallet transaction exists to commit.", transaction_id
                )
            return self._set_state(transaction_id, "COMMITTED")

    def abort(self, transaction_id: UUID) -> ParticipantRecord:
        with self._transaction_lock(transaction_id):
            current = self._load(transaction_id)
            if current is None:
                with self.engine.begin() as connection:
                    row = connection.execute(
                        insert(participant_transactions)
                        .values(transaction_id=transaction_id, state="ABORTED")
                        .returning(*participant_transactions.c)
                    ).one()
                return self._record(row)
            if current.state == "ABORTED":
                return self._record(current)
            if current.state == "COMMITTED" or self._effect_exists(transaction_id):
                if current.state != "COMMITTED":
                    self._set_state(transaction_id, "COMMITTED")
                raise ParticipantConflict(
                    "A committed Wallet transaction cannot be aborted.", transaction_id
                )
            if self._prepared_exists(transaction_id):
                self._finish_prepared(transaction_id, commit=False)
            return self._set_state(transaction_id, "ABORTED")

    def get(self, transaction_id: UUID) -> ParticipantRecord:
        current = self._load(transaction_id)
        if current is None:
            raise ParticipantTransactionNotFound(
                "The transaction is unknown to the Wallet participant.", transaction_id
            )
        return self._record(current)

    def _insert_claim(
        self,
        transaction_id: UUID,
        correlation_id: UUID,
        user_id: str,
        points: int,
        expected_hash: str,
    ) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                insert(participant_transactions).values(
                    transaction_id=transaction_id,
                    state="IN_DOUBT",
                    correlation_id=correlation_id,
                    user_id=user_id,
                    points=points,
                    request_hash=expected_hash,
                )
            )

    def _prepare_business_transaction(self, transaction_id: UUID, user_id: str, points: int) -> str:
        url = make_url(self.engine.url.render_as_string(hide_password=False)).set(
            drivername="postgresql"
        )
        connection = connect(url.render_as_string(hide_password=False))
        try:
            wallet = connection.execute(
                "SELECT available_points FROM wallets WHERE user_id = %s FOR UPDATE", (user_id,)
            ).fetchone()
            if wallet is None or wallet[0] < points:
                connection.rollback()
                return "ABORTED"

            connection.execute(
                """
                UPDATE wallets
                   SET available_points = available_points - %s,
                       version = version + 1,
                       updated_at = now()
                 WHERE user_id = %s
                """,
                (points, user_id),
            )
            connection.execute(
                """
                INSERT INTO wallet_operations
                    (transaction_id, user_id, operation, points, status)
                VALUES (%s, %s, 'deduct', %s, 'applied')
                """,
                (transaction_id, user_id, points),
            )
            connection.execute(
                sql.SQL("PREPARE TRANSACTION {}").format(sql.Literal(prepared_gid(transaction_id)))
            )
            return "PREPARED"
        finally:
            connection.close()

    def _reconcile_in_doubt(self, transaction_id: UUID) -> ParticipantRecord | None:
        if self._prepared_exists(transaction_id):
            return self._set_state(transaction_id, "PREPARED")
        if self._effect_exists(transaction_id):
            return self._set_state(transaction_id, "COMMITTED")
        return None

    def _load(self, transaction_id: UUID):
        with self.engine.connect() as connection:
            return connection.execute(
                select(participant_transactions).where(
                    participant_transactions.c.transaction_id == transaction_id
                )
            ).one_or_none()

    def _set_state(self, transaction_id: UUID, state: str) -> ParticipantRecord:
        with self.engine.begin() as connection:
            row = connection.execute(
                update(participant_transactions)
                .where(participant_transactions.c.transaction_id == transaction_id)
                .values(state=state, updated_at=datetime.now(UTC))
                .returning(*participant_transactions.c)
            ).one()
        return self._record(row)

    def _prepared_exists(self, transaction_id: UUID) -> bool:
        with self.engine.connect() as connection:
            return bool(
                connection.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_prepared_xacts "
                        "WHERE database = current_database() AND gid = :gid)"
                    ),
                    {"gid": prepared_gid(transaction_id)},
                )
            )

    def _effect_exists(self, transaction_id: UUID) -> bool:
        with self.engine.connect() as connection:
            return bool(
                connection.scalar(
                    select(wallet_operations.c.id).where(
                        wallet_operations.c.transaction_id == transaction_id,
                        wallet_operations.c.operation == "deduct",
                        wallet_operations.c.status == "applied",
                    )
                )
            )

    def _finish_prepared(self, transaction_id: UUID, *, commit: bool) -> None:
        verb = "COMMIT" if commit else "ROLLBACK"
        literal = "'" + prepared_gid(transaction_id).replace("'", "''") + "'"
        with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.exec_driver_sql(f"{verb} PREPARED {literal}")

    @staticmethod
    def _record(row) -> ParticipantRecord:
        return ParticipantRecord(
            transaction_id=row.transaction_id,
            state=row.state,
            updated_at=row.updated_at,
        )
