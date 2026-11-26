"""Application service for durable Coordinator transactions (LM-010)."""

from uuid import UUID

from services.coordinator.app.models import (
    CreateRedemptionRequest,
    RedemptionResponse,
    TransitionView,
)
from services.coordinator.app.repository import (
    CoordinatorRepository,
    RedemptionRecord,
    TransitionRecord,
)


class CoordinatorService:
    def __init__(self, repository: CoordinatorRepository) -> None:
        self.repository = repository

    def create(
        self, idempotency_key: str, request: CreateRedemptionRequest, *, fencing_token: int = 0
    ) -> RedemptionResponse:
        return self._response(
            self.repository.create(idempotency_key, request, fencing_token=fencing_token)
        )

    def get(self, transaction_id: UUID) -> RedemptionResponse:
        return self._response(self.repository.get(transaction_id))

    def transition(
        self, transaction_id: UUID, to_status: str, reason: str | None = None
    ) -> RedemptionResponse:
        return self._response(self.repository.transition(transaction_id, to_status, reason))

    def list_transitions(self, transaction_id: UUID) -> list[TransitionView]:
        return [
            self._transition_view(row) for row in self.repository.list_transitions(transaction_id)
        ]

    @staticmethod
    def _response(record: RedemptionRecord) -> RedemptionResponse:
        return RedemptionResponse.model_validate(record, from_attributes=True)

    @staticmethod
    def _transition_view(record: TransitionRecord) -> TransitionView:
        return TransitionView.model_validate(record, from_attributes=True)
