"""Application service for Catalog core operations (LM-008)."""

from uuid import UUID

from services.catalog.app.errors import InvalidQuantity
from services.catalog.app.models import ReservationResult, RewardQuote, RewardView
from services.catalog.app.repository import CatalogRepository, ReservationRecord


class CatalogService:
    def __init__(self, repository: CatalogRepository) -> None:
        self.repository = repository

    def list_rewards(self, *, available_only: bool = True) -> list[RewardView]:
        return [
            RewardView.model_validate(record, from_attributes=True)
            for record in self.repository.list_rewards(available_only=available_only)
        ]

    def get_reward(self, reward_id: str) -> RewardQuote:
        return RewardQuote.model_validate(
            self.repository.get_reward(reward_id), from_attributes=True
        )

    def reserve(
        self,
        reward_id: str,
        transaction_id: UUID,
        quantity: int,
        event_id: UUID | None = None,
    ) -> ReservationResult:
        self._require_positive_quantity(quantity, transaction_id)
        return self._result(self.repository.reserve(reward_id, transaction_id, quantity, event_id))

    def release(
        self,
        reward_id: str,
        transaction_id: UUID,
        quantity: int,
        event_id: UUID | None = None,
    ) -> ReservationResult:
        self._require_positive_quantity(quantity, transaction_id)
        return self._result(self.repository.release(reward_id, transaction_id, quantity, event_id))

    def fulfill(
        self, reward_id: str, transaction_id: UUID, event_id: UUID | None = None
    ) -> ReservationResult:
        return self._result(self.repository.fulfill(reward_id, transaction_id, event_id))

    @staticmethod
    def _require_positive_quantity(quantity: int, transaction_id: UUID) -> None:
        if quantity <= 0:
            raise InvalidQuantity("Reservation quantity must be greater than zero.", transaction_id)

    @staticmethod
    def _result(record: ReservationRecord) -> ReservationResult:
        if record.tombstone:
            outcome = "tombstone"
        elif record.duplicate:
            outcome = "duplicate"
        else:
            outcome = "applied"
        return ReservationResult(
            transaction_id=record.transaction_id,
            reward_id=record.reward_id,
            quantity=record.quantity,
            state=record.state,
            outcome=outcome,
            available_inventory=record.available_inventory,
            version=record.version,
            updated_at=record.updated_at,
        )
