"""Post-experiment correctness checks shared by LM-017 failure tests."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import Engine, text


@dataclass(frozen=True)
class InvariantReport:
    violations: tuple[str, ...]

    @property
    def count(self) -> int:
        return len(self.violations)


def _scalar(engine: Engine, statement: str) -> int:
    with engine.connect() as connection:
        return int(connection.scalar(text(statement)) or 0)


def check_2pc_invariants(engines: dict[str, Engine]) -> InvariantReport:
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
        "WHERE to_status IN ('COMMITTED', 'ABORTED') GROUP BY transaction_id "
        "HAVING count(*) > 1) duplicate_terminals",
    ):
        violations.append(f"INV-04: {count} transactions have multiple terminal transitions")

    with coordinator.connect() as connection:
        terminal = connection.execute(
            text(
                "SELECT transaction_id::text, status FROM redemptions "
                "WHERE coordination_mode = 'two_phase_commit' "
                "AND status IN ('COMMITTED', 'ABORTED')"
            )
        ).all()
    for transaction_id, status in terminal:
        wallet_effect = _scalar(
            wallet,
            "SELECT count(*) FROM wallet_operations "
            f"WHERE transaction_id = '{transaction_id}' AND operation = 'deduct' "
            "AND status = 'applied'",
        )
        catalog_effect = _scalar(
            catalog,
            "SELECT count(*) FROM reservations "
            f"WHERE transaction_id = '{transaction_id}' AND status = 'reserved'",
        )
        if status == "COMMITTED" and (wallet_effect != 1 or catalog_effect != 1):
            violations.append(f"INV-05: committed {transaction_id} is incomplete")
        if status == "ABORTED" and (wallet_effect or catalog_effect):
            violations.append(f"INV-06: aborted {transaction_id} retained an effect")

    for service, engine in engines.items():
        if count := _scalar(
            engine,
            "SELECT count(*) FROM pg_prepared_xacts WHERE database = current_database()",
        ):
            violations.append(f"{service}: {count} prepared transactions leaked")
    return InvariantReport(tuple(violations))
