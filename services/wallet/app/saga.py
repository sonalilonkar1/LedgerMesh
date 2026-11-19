"""Wallet Saga command consumer (LM-020).

Consumes ``wallet.commands`` and answers on ``wallet.results``. Each command is
handled by ``process_once`` (LM-019), so the inbox row, the Wallet change and
the reply in the outbox commit in one database transaction. The Wallet change
runs on that same transaction through the repository's ``connection=`` form
(LM-007), inside a savepoint, so a rejected command still commits its reply.

Commands (from the Coordinator):

- ``wallet.deduct_points``: payload ``{"user_id", "points"}``
- ``wallet.refund_points``: payload ``{"user_id", "points"}``. ``points`` is the
  amount originally requested. The refund itself always returns the stored
  deduction amount; ``points`` is only used for an early-refund tombstone.

Results (to the Coordinator, keyed by ``transaction_id``):

- ``wallet.points_deducted``: outcome ``applied`` or ``duplicate``
- ``wallet.deduction_rejected``: reason ``insufficient_points``,
  ``wallet_not_found``, ``cancelled`` (a refund already cancelled it) or
  ``conflict`` (the transaction id was used for a different deduction)
- ``wallet.points_refunded``: outcome ``applied``, ``duplicate`` or
  ``cancelled_before_deduction``

Outcomes belong to the transaction, not to one command delivery:

- A rejected deduction is stored in ``deduction_rejections``. A later command
  for the same transaction (a re-send with a new event id, or a delayed copy)
  gets the same rejection, even if the wallet has since been topped up.
- A refund that arrives before its deduction stores a deduction tombstone
  (ADR-001 section 7.1), so the late deduction is answered with ``cancelled``
  instead of being applied.
- ``cancelled`` and ``duplicate`` are returned only when the user and points
  match the stored record. Any other body reusing the transaction id is a
  ``conflict``.

Commands are checked against their place in the Saga: a deduction is step 1 and
a refund is a later compensation step. Malformed commands raise
``InvalidCommand``, a ``PermanentError``, and are dead-lettered by
``InboxConsumer``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from ledgermesh_messaging import (
    WALLET_RESULTS,
    EventEnvelope,
    OutgoingMessage,
    PermanentError,
    TransientError,
)
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncSession

from services.wallet.app.errors import (
    DeductionAlreadyApplied,
    DeductionNotFound,
    InsufficientPoints,
    OperationConflict,
    WalletNotFound,
)
from services.wallet.app.repository import PostgresWalletRepository

logger = logging.getLogger(__name__)

DEDUCT_POINTS = "wallet.deduct_points"
REFUND_POINTS = "wallet.refund_points"
POINTS_DEDUCTED = "wallet.points_deducted"
DEDUCTION_REJECTED = "wallet.deduction_rejected"
POINTS_REFUNDED = "wallet.points_refunded"
DEDUCT_STEP = 1


class InvalidCommand(PermanentError):
    """The command can never succeed as sent, so it is dead-lettered."""


@dataclass(frozen=True)
class WalletCommand:
    envelope: EventEnvelope
    user_id: str
    points: int


def parse_command(envelope: EventEnvelope) -> WalletCommand:
    """Check that ``envelope`` is a well-formed Wallet command for this workflow."""
    if envelope.event_type not in {DEDUCT_POINTS, REFUND_POINTS}:
        raise InvalidCommand(f"unsupported Wallet command {envelope.event_type!r}")
    if envelope.aggregate_type != "wallet":
        raise InvalidCommand("Wallet commands must target the wallet aggregate")
    if envelope.workflow_step is None or envelope.workflow_step < 1:
        raise InvalidCommand("Wallet commands must carry a positive workflow_step")
    # In the Coordinator's Saga state machine (LM-010) the deduction is the first
    # step (REQUESTED is 0, DEDUCTING_POINTS is 1) and a refund is always a later
    # compensation step. Ordering across messages is checked by LM-026.
    if envelope.event_type == DEDUCT_POINTS and envelope.workflow_step != DEDUCT_STEP:
        raise InvalidCommand(f"a deduction must be workflow_step {DEDUCT_STEP}")
    if envelope.event_type == REFUND_POINTS and envelope.workflow_step <= DEDUCT_STEP:
        raise InvalidCommand("a refund must be a compensation step after the deduction")
    if envelope.aggregate_version is not None:
        # ADR-001 section 5: only the owning service assigns aggregate versions.
        raise InvalidCommand("commands must not carry an aggregate_version")
    user_id = envelope.payload.get("user_id")
    if user_id != envelope.aggregate_id:
        raise InvalidCommand("payload user_id must match the command's aggregate_id")
    points = envelope.payload.get("points")
    if not isinstance(points, int) or isinstance(points, bool) or points <= 0:
        raise InvalidCommand("points must be a positive integer")
    return WalletCommand(envelope=envelope, user_id=user_id, points=points)


def result_envelope(
    command: WalletCommand,
    event_type: str,
    payload: dict[str, Any],
    *,
    aggregate_version: int | None = None,
) -> EventEnvelope:
    """Build a reply that carries the command's workflow identity forward."""
    source = command.envelope
    return EventEnvelope(
        event_type=event_type,
        transaction_id=source.transaction_id,
        correlation_id=source.correlation_id,
        causation_id=source.event_id,
        aggregate_type="wallet",
        aggregate_id=command.user_id,
        # Only a reply describing a new balance change carries a version.
        aggregate_version=aggregate_version,
        workflow_step=source.workflow_step,
        fencing_token=source.fencing_token,
        payload=payload,
    )


class WalletSagaHandler:
    """``process_once`` handler for ``wallet.commands``."""

    def __init__(self, repository: PostgresWalletRepository) -> None:
        self.repository = repository

    async def __call__(
        self, session: AsyncSession, envelope: EventEnvelope
    ) -> list[OutgoingMessage]:
        command = parse_command(envelope)
        reply = await session.run_sync(lambda sync: self.apply(sync.connection(), command))
        return [WALLET_RESULTS.message(reply)]

    def apply(self, connection: Connection, command: WalletCommand) -> EventEnvelope:
        if command.envelope.event_type == DEDUCT_POINTS:
            return self._deduct(connection, command)
        return self._refund(connection, command)

    def _deduct(self, connection: Connection, command: WalletCommand) -> EventEnvelope:
        transaction_id = command.envelope.transaction_id
        try:
            with connection.begin_nested():
                # Lock first, so the stored-outcome check below and the deduction
                # see the same state as any competing command for this wallet.
                self.repository.lock_wallet(command.user_id, connection=connection)
                earlier = self._earlier_rejection(connection, command)
                if earlier is not None:
                    return earlier
                record = self.repository.deduct(
                    command.user_id,
                    transaction_id,
                    command.points,
                    command.envelope.event_id,
                    connection=connection,
                )
        except InsufficientPoints as error:
            return self._reject_and_remember(
                connection, command, "insufficient_points", error.detail
            )
        except WalletNotFound as error:
            earlier = self._earlier_rejection(connection, command)
            if earlier is not None:
                return earlier
            return self._reject_and_remember(connection, command, "wallet_not_found", error.detail)
        except OperationConflict as error:
            stored = self.repository.stored_deduction(transaction_id, connection=connection)
            # "cancelled" only for the exact deduction an early refund cancelled;
            # any other body reusing the transaction id stays a conflict.
            if (
                stored is not None
                and stored.status == "tombstone"
                and self._same_deduction(stored, command)
            ):
                logger.warning(
                    "rejecting late deduction %s for transaction %s: already cancelled",
                    command.envelope.event_id,
                    transaction_id,
                )
                return self._rejected(command, "cancelled", error.detail)
            return self._rejected(command, "conflict", error.detail)

        return result_envelope(
            command,
            POINTS_DEDUCTED,
            {
                "user_id": command.user_id,
                "points": record.points,
                "available_points": record.available_points,
                "outcome": "duplicate" if record.duplicate else "applied",
            },
            aggregate_version=None if record.duplicate else record.version,
        )

    def _refund(self, connection: Connection, command: WalletCommand) -> EventEnvelope:
        try:
            with connection.begin_nested():
                record = self.repository.refund(
                    command.user_id,
                    command.envelope.transaction_id,
                    command.envelope.event_id,
                    connection=connection,
                )
        except DeductionNotFound:
            return self._cancel_deduction(connection, command)
        except (WalletNotFound, OperationConflict) as error:
            raise InvalidCommand(f"refund cannot be applied: {error.detail}") from error

        return result_envelope(
            command,
            POINTS_REFUNDED,
            {
                "user_id": command.user_id,
                "refunded_points": record.points,
                "available_points": record.available_points,
                "outcome": "duplicate" if record.duplicate else "applied",
            },
            aggregate_version=None if record.duplicate else record.version,
        )

    def _cancel_deduction(self, connection: Connection, command: WalletCommand) -> EventEnvelope:
        try:
            with connection.begin_nested():
                record = self.repository.record_deduction_tombstone(
                    command.user_id,
                    command.envelope.transaction_id,
                    command.points,
                    command.envelope.event_id,
                    connection=connection,
                )
        except DeductionAlreadyApplied as error:
            # The deduction committed between our refund attempt and now; a
            # retry of this command will refund it normally.
            raise TransientError("deduction applied concurrently; retrying refund") from error
        except (WalletNotFound, OperationConflict) as error:
            raise InvalidCommand(f"refund cannot be applied: {error.detail}") from error

        return result_envelope(
            command,
            POINTS_REFUNDED,
            {
                "user_id": command.user_id,
                "refunded_points": 0,
                "available_points": record.available_points,
                "outcome": "cancelled_before_deduction",
            },
        )

    def _earlier_rejection(
        self, connection: Connection, command: WalletCommand
    ) -> EventEnvelope | None:
        """Return the stored rejection for this transaction, if there is one.

        A rejection belongs to the transaction, so a later command for it (a
        re-send with a new event id, or a delayed copy) gets the same answer even
        if the wallet has since been topped up or created.
        """
        stored = self.repository.stored_rejection(
            command.envelope.transaction_id, connection=connection
        )
        if stored is None:
            return None
        if not self._same_deduction(stored, command):
            return self._rejected(
                command, "conflict", "The transaction ID was rejected for a different deduction."
            )
        return self._rejected(
            command, stored.reason, "The deduction was already rejected for this transaction."
        )

    def _reject_and_remember(
        self, connection: Connection, command: WalletCommand, reason: str, detail: str
    ) -> EventEnvelope:
        with connection.begin_nested():
            stored = self.repository.record_rejection(
                command.envelope.transaction_id,
                command.user_id,
                command.points,
                reason,
                command.envelope.event_id,
                connection=connection,
            )
        # Answer from the durable row: a concurrent command for the same
        # transaction may have stored its rejection first.
        if not self._same_deduction(stored, command):
            return self._rejected(
                command, "conflict", "The transaction ID was rejected for a different deduction."
            )
        if stored.event_id != command.envelope.event_id:
            detail = "The deduction was already rejected for this transaction."
        return self._rejected(command, stored.reason, detail)

    @staticmethod
    def _same_deduction(row, command: WalletCommand) -> bool:
        return row.user_id == command.user_id and row.points == command.points

    @staticmethod
    def _rejected(command: WalletCommand, reason: str, detail: str) -> EventEnvelope:
        return result_envelope(
            command,
            DEDUCTION_REJECTED,
            {
                "user_id": command.user_id,
                "points": command.points,
                "reason": reason,
                "detail": detail,
            },
        )
