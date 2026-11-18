"""Application service for Wallet core operations (LM-007)."""

from uuid import UUID

from services.wallet.app.errors import InvalidAmount
from services.wallet.app.models import WalletBalance, WalletOperationResult
from services.wallet.app.repository import WalletRepository


class WalletService:
    def __init__(self, repository: WalletRepository) -> None:
        self.repository = repository

    def get_balance(self, user_id: str) -> WalletBalance:
        return WalletBalance.model_validate(
            self.repository.get_balance(user_id), from_attributes=True
        )

    def deduct(
        self, user_id: str, transaction_id: UUID, points: int, event_id: UUID | None = None
    ) -> WalletOperationResult:
        if points <= 0:
            raise InvalidAmount("Deduction points must be greater than zero.", transaction_id)
        record = self.repository.deduct(user_id, transaction_id, points, event_id)
        return WalletOperationResult(
            user_id=record.user_id,
            available_points=record.available_points,
            version=record.version,
            updated_at=record.updated_at,
            transaction_id=record.transaction_id,
            operation="deduct",
            points=record.points,
            outcome="duplicate" if record.duplicate else "applied",
        )

    def refund(
        self, user_id: str, transaction_id: UUID, event_id: UUID | None = None
    ) -> WalletOperationResult:
        record = self.repository.refund(user_id, transaction_id, event_id)
        return WalletOperationResult(
            user_id=record.user_id,
            available_points=record.available_points,
            version=record.version,
            updated_at=record.updated_at,
            transaction_id=record.transaction_id,
            operation="refund",
            points=record.points,
            outcome="duplicate" if record.duplicate else "applied",
        )
