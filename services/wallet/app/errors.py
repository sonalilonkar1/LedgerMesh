"""Wallet domain errors and their stable HTTP problem details."""

from uuid import UUID


class WalletError(Exception):
    status = 500
    title = "Wallet operation failed"
    problem_type = "urn:ledgermesh:wallet:error"

    def __init__(self, detail: str, transaction_id: UUID | None = None) -> None:
        super().__init__(detail)
        self.detail = detail
        self.transaction_id = transaction_id


class WalletNotFound(WalletError):
    status = 404
    title = "Wallet not found"
    problem_type = "urn:ledgermesh:wallet:not-found"


class DeductionNotFound(WalletError):
    status = 404
    title = "Deduction not found"
    problem_type = "urn:ledgermesh:wallet:deduction-not-found"


class InsufficientPoints(WalletError):
    status = 409
    title = "Insufficient points"
    problem_type = "urn:ledgermesh:wallet:insufficient-points"


class OperationConflict(WalletError):
    status = 409
    title = "Wallet operation conflict"
    problem_type = "urn:ledgermesh:wallet:operation-conflict"


class DeductionAlreadyApplied(WalletError):
    status = 409
    title = "Deduction already applied"
    problem_type = "urn:ledgermesh:wallet:deduction-already-applied"


class InvalidAmount(WalletError):
    status = 422
    title = "Invalid point amount"
    problem_type = "urn:ledgermesh:wallet:invalid-amount"
