"""Coordinator domain errors and stable HTTP problem details."""

from uuid import UUID


class CoordinatorError(Exception):
    status = 500
    title = "Coordinator operation failed"
    problem_type = "urn:ledgermesh:coordinator:error"

    def __init__(self, detail: str, transaction_id: UUID | None = None) -> None:
        super().__init__(detail)
        self.detail = detail
        self.transaction_id = transaction_id


class RedemptionNotFound(CoordinatorError):
    status = 404
    title = "Redemption not found"
    problem_type = "urn:ledgermesh:coordinator:not-found"


class IdempotencyConflict(CoordinatorError):
    status = 409
    title = "Idempotency key conflict"
    problem_type = "urn:ledgermesh:coordinator:idempotency-conflict"


class InvalidTransition(CoordinatorError):
    status = 409
    title = "Invalid redemption transition"
    problem_type = "urn:ledgermesh:coordinator:invalid-transition"
