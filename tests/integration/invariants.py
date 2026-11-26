"""Cross-service post-experiment correctness checks (LM-017/LM-031)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Engine, text
from sqlalchemy.sql.elements import TextClause


@dataclass(frozen=True)
class InvariantReport:
    violations: tuple[str, ...]

    @property
    def count(self) -> int:
        return len(self.violations)


def _scalar(
    engine: Engine,
    statement: str | TextClause,
    parameters: dict[str, Any] | None = None,
) -> int:
    with engine.connect() as connection:
        query = text(statement) if isinstance(statement, str) else statement
        return int(connection.scalar(query, parameters or {}) or 0)


def check_project_invariants(engines: dict[str, Engine]) -> InvariantReport:
    """Check shared business invariants across the isolated service databases.

    The checker intentionally reads every coordination mode from the durable
    Coordinator record and correlates its transaction ID with participant
    effects. It is diagnostic: database constraints remain the primary guard.
    """
    violations: list[str] = []
    wallet = engines["wallet"]
    catalog = engines["catalog"]
    coordinator = engines["coordinator"]

    if count := _scalar(wallet, "SELECT count(*) FROM wallets WHERE available_points < 0"):
        violations.append(f"INV-01: {count} wallet balances are negative")
    if count := _scalar(
        catalog,
        "SELECT count(*) FROM rewards "
        "WHERE available_inventory < 0 OR available_inventory > total_inventory",
    ):
        violations.append(f"INV-02: {count} rewards have invalid inventory")
    if count := _scalar(
        wallet,
        "SELECT count(*) FROM (SELECT transaction_id, operation FROM wallet_operations "
        "GROUP BY transaction_id, operation HAVING count(*) > 1) duplicates",
    ):
        violations.append(f"INV-03: {count} duplicate wallet effects exist")
    if count := _scalar(
        catalog,
        "SELECT count(*) FROM (SELECT transaction_id FROM reservations "
        "GROUP BY transaction_id HAVING count(*) > 1) duplicates",
    ):
        violations.append(f"INV-03: {count} duplicate catalog effects exist")
    if count := _scalar(
        coordinator,
        "SELECT count(*) FROM (SELECT transaction_id FROM redemption_transitions "
        "WHERE to_status IN ('COMMITTED', 'ABORTED', 'COMPLETED', 'FAILED', 'COMPENSATED') "
        "GROUP BY transaction_id HAVING count(*) > 1) duplicate_terminals",
    ):
        violations.append(f"INV-04: {count} transactions have multiple terminal transitions")

    with coordinator.connect() as connection:
        nonterminal = connection.execute(
            text(
                "SELECT coordination_mode, status, count(*) FROM redemptions "
                "WHERE status NOT IN "
                "('COMMITTED', 'ABORTED', 'COMPLETED', 'FAILED', 'COMPENSATED') "
                "GROUP BY coordination_mode, status ORDER BY coordination_mode, status"
            )
        ).all()
        successful = connection.execute(
            text(
                "SELECT transaction_id, status FROM redemptions "
                "WHERE status IN ('COMMITTED', 'COMPLETED')"
            )
        ).all()
        unsuccessful = connection.execute(
            text(
                "SELECT transaction_id, status FROM redemptions "
                "WHERE status IN ('ABORTED', 'FAILED', 'COMPENSATED')"
            )
        ).all()

    if nonterminal:
        pending_count = sum(count for _, _, count in nonterminal)
        breakdown = ", ".join(f"{mode} {status}={count}" for mode, status, count in nonterminal)
        violations.append(
            f"INV-07: {pending_count} redemptions remain non-terminal after the test: {breakdown}"
        )

    for transaction_id, status in successful:
        wallet_effects = _scalar(
            wallet,
            "SELECT count(*) FROM wallet_operations "
            "WHERE transaction_id = :transaction_id AND operation = 'deduct' "
            "AND status = 'applied'",
            {"transaction_id": transaction_id},
        )
        catalog_effects = _scalar(
            catalog,
            "SELECT count(*) FROM reservations "
            "WHERE transaction_id = :transaction_id "
            "AND status IN ('reserved', 'fulfilled')",
            {"transaction_id": transaction_id},
        )
        if wallet_effects != 1 or catalog_effects != 1:
            violations.append(
                f"INV-05: successful {status.lower()} transaction {transaction_id} "
                f"has {wallet_effects} applied deductions and {catalog_effects} "
                "active reservations; expected one of each"
            )

    for transaction_id, status in unsuccessful:
        uncompensated_wallet_effects = _scalar(
            wallet,
            "SELECT count(*) FROM wallet_operations AS deduction "
            "WHERE deduction.transaction_id = :transaction_id "
            "AND deduction.operation = 'deduct' AND deduction.status = 'applied' "
            "AND NOT EXISTS (SELECT 1 FROM wallet_operations AS refund "
            "WHERE refund.transaction_id = deduction.transaction_id "
            "AND refund.operation = 'refund' AND refund.status = 'applied' "
            "AND refund.user_id = deduction.user_id AND refund.points = deduction.points)",
            {"transaction_id": transaction_id},
        )
        uncompensated_wallet_effects += _scalar(
            wallet,
            "SELECT count(*) FROM wallet_operations AS refund "
            "WHERE refund.transaction_id = :transaction_id "
            "AND refund.operation = 'refund' AND refund.status = 'applied' "
            "AND NOT EXISTS (SELECT 1 FROM wallet_operations AS deduction "
            "WHERE deduction.transaction_id = refund.transaction_id "
            "AND deduction.operation = 'deduct' AND deduction.status = 'applied' "
            "AND deduction.user_id = refund.user_id AND deduction.points = refund.points)",
            {"transaction_id": transaction_id},
        )
        active_reservations = _scalar(
            catalog,
            "SELECT count(*) FROM reservations WHERE transaction_id = :transaction_id "
            "AND status IN ('reserved', 'fulfilled')",
            {"transaction_id": transaction_id},
        )
        if uncompensated_wallet_effects or active_reservations:
            violations.append(
                f"INV-06: unsuccessful {status.lower()} transaction {transaction_id} "
                f"has {uncompensated_wallet_effects} uncompensated wallet effects and "
                f"{active_reservations} active reservations"
            )

    for service, engine in engines.items():
        if count := _scalar(
            engine,
            "SELECT count(*) FROM pg_prepared_xacts WHERE database = current_database()",
        ):
            violations.append(f"{service}: {count} prepared transactions leaked")
    return InvariantReport(tuple(violations))


def check_2pc_invariants(engines: dict[str, Engine]) -> InvariantReport:
    """Backward-compatible name used by the original LM-017 harness."""
    return check_project_invariants(engines)
