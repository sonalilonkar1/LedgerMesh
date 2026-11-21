"""Catalog domain errors and their stable HTTP problem details."""

from uuid import UUID


class CatalogError(Exception):
    status = 500
    title = "Catalog operation failed"
    problem_type = "urn:ledgermesh:catalog:error"

    def __init__(self, detail: str, transaction_id: UUID | None = None) -> None:
        super().__init__(detail)
        self.detail = detail
        self.transaction_id = transaction_id


class RewardNotFound(CatalogError):
    status = 404
    title = "Reward not found"
    problem_type = "urn:ledgermesh:catalog:reward-not-found"


class ReservationNotFound(CatalogError):
    status = 404
    title = "Reservation not found"
    problem_type = "urn:ledgermesh:catalog:reservation-not-found"


class InsufficientInventory(CatalogError):
    status = 409
    title = "Insufficient inventory"
    problem_type = "urn:ledgermesh:catalog:insufficient-inventory"


class ReservationConflict(CatalogError):
    status = 409
    title = "Catalog reservation conflict"
    problem_type = "urn:ledgermesh:catalog:reservation-conflict"


class InvalidReservationTransition(CatalogError):
    status = 409
    title = "Invalid reservation transition"
    problem_type = "urn:ledgermesh:catalog:invalid-transition"


class InvalidQuantity(CatalogError):
    status = 422
    title = "Invalid reservation quantity"
    problem_type = "urn:ledgermesh:catalog:invalid-quantity"


class ParticipantTransactionNotFound(CatalogError):
    status = 404
    title = "Participant transaction not found"
    problem_type = "urn:ledgermesh:catalog:participant-not-found"


class ParticipantConflict(CatalogError):
    status = 409
    title = "Participant transaction conflict"
    problem_type = "urn:ledgermesh:catalog:participant-conflict"
