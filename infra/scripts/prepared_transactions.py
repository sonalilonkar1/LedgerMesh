"""Inspect and safely roll back unresolved PostgreSQL prepared transactions.

Run from the repository root through the Make targets:

    make db-prepared-list
    LEDGERMESH_ENV=local make db-prepared-clean

The cleanup command is intentionally limited to local/test environments. A
production prepared transaction requires operator investigation and a specific
COMMIT PREPARED or ROLLBACK PREPARED decision; it must never be bulk-cleaned.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Engine, create_engine, text

from infra.scripts.db import SERVICES, database_url, require_safe_environment


@dataclass(frozen=True)
class PreparedTransaction:
    gid: str
    prepared: datetime
    owner: str
    database: str


def list_prepared(engine: Engine) -> list[PreparedTransaction]:
    """Return prepared transactions owned by the engine's current database."""
    query = text(
        """
        SELECT gid, prepared, owner, database
          FROM pg_prepared_xacts
         WHERE database = current_database()
         ORDER BY prepared, gid
        """
    )
    with engine.connect() as connection:
        return [PreparedTransaction(**row._mapping) for row in connection.execute(query)]


def rollback_prepared(engine: Engine, gid: str) -> None:
    """Roll back one prepared transaction outside a transaction block."""
    # PostgreSQL does not allow bind parameters in ROLLBACK PREPARED. Escape the
    # transaction identifier as a SQL string literal, not as an SQL identifier.
    literal = "'" + gid.replace("'", "''") + "'"
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.exec_driver_sql(f"ROLLBACK PREPARED {literal}")


def cleanup_prepared(engine: Engine, *, report: bool = True) -> list[str]:
    """Roll back every unresolved transaction in the current database."""
    gids = [transaction.gid for transaction in list_prepared(engine)]
    for gid in gids:
        rollback_prepared(engine, gid)
        if report:
            print(f"rolled back {gid}")
    return gids


def _print_transactions(service: str, transactions: list[PreparedTransaction]) -> None:
    if not transactions:
        print(f"{service}: no unresolved prepared transactions")
        return
    for transaction in transactions:
        print(
            f"{service}: gid={transaction.gid} owner={transaction.owner} "
            f"prepared={transaction.prepared.isoformat()}"
        )


def run(action: str, services: list[str]) -> None:
    if action == "cleanup":
        require_safe_environment("clean up prepared transactions")
    for service in services:
        engine = create_engine(database_url(service))
        try:
            transactions = list_prepared(engine)
            if action == "list":
                _print_transactions(service, transactions)
            else:
                if not transactions:
                    print(f"{service}: no unresolved prepared transactions")
                cleanup_prepared(engine)
        finally:
            engine.dispose()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=["list", "cleanup"])
    parser.add_argument("services", nargs="*", metavar="service", help=", ".join(SERVICES))
    args = parser.parse_args(argv)
    unknown = sorted(set(args.services) - set(SERVICES))
    if unknown:
        parser.error(f"unknown service(s): {', '.join(unknown)}")
    run(args.action, args.services or list(SERVICES))


if __name__ == "__main__":
    main()
