"""Application service for Catalog's synchronous 2PC participant API."""

from uuid import UUID

from services.catalog.app.models import CatalogPrepareRequest, ParticipantResponse
from services.catalog.app.participant import CatalogParticipantRepository


class CatalogParticipantService:
    def __init__(self, repository: CatalogParticipantRepository) -> None:
        self.repository = repository

    @staticmethod
    def _response(record) -> ParticipantResponse:
        return ParticipantResponse.model_validate(record, from_attributes=True)

    def prepare(self, transaction_id: UUID, request: CatalogPrepareRequest) -> ParticipantResponse:
        return self._response(
            self.repository.prepare(
                transaction_id,
                request.correlation_id,
                request.reward_id,
                request.quantity,
                request.expected_reward_version,
                request.expected_point_cost,
                request.fencing_token,
            )
        )

    def commit(self, transaction_id: UUID) -> ParticipantResponse:
        return self._response(self.repository.commit(transaction_id))

    def abort(self, transaction_id: UUID) -> ParticipantResponse:
        return self._response(self.repository.abort(transaction_id))

    def get(self, transaction_id: UUID) -> ParticipantResponse:
        return self._response(self.repository.get(transaction_id))
