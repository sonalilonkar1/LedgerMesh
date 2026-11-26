"""Application service for Wallet's synchronous 2PC participant API."""

from uuid import UUID

from services.wallet.app.models import ParticipantResponse, WalletPrepareRequest
from services.wallet.app.participant import WalletParticipantRepository


class WalletParticipantService:
    def __init__(self, repository: WalletParticipantRepository) -> None:
        self.repository = repository

    @staticmethod
    def _response(record) -> ParticipantResponse:
        return ParticipantResponse.model_validate(record, from_attributes=True)

    def prepare(self, transaction_id: UUID, request: WalletPrepareRequest) -> ParticipantResponse:
        return self._response(
            self.repository.prepare(
                transaction_id,
                request.correlation_id,
                request.user_id,
                request.points,
                request.fencing_token,
            )
        )

    def commit(self, transaction_id: UUID) -> ParticipantResponse:
        return self._response(self.repository.commit(transaction_id))

    def abort(self, transaction_id: UUID) -> ParticipantResponse:
        return self._response(self.repository.abort(transaction_id))

    def get(self, transaction_id: UUID) -> ParticipantResponse:
        return self._response(self.repository.get(transaction_id))
