"""Apply, roll back or reset the service database migrations (LM-005).

Usage, from the repository root:

    uv run python infra/scripts/db.py upgrade [service ...]
    uv run python infra/scripts/db.py downgrade [service ...]
    uv run python infra/scripts/db.py reset [service ...]

With no service names, every service is migrated. URLs default to the local
Compose databases (infra/compose.yaml); set ``<SERVICE>_DATABASE_URL`` (for
example ``WALLET_DATABASE_URL``) to target another database.

``downgrade`` and ``reset`` delete data, so they refuse to run unless
``LEDGERMESH_ENV`` is ``local`` or ``test``.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config

ROOT = Path(__file__).resolve().parents[2]
SAFE_ENVIRONMENTS = {"local", "test"}

# service -> (database and user name, host-port variable, default host port)
SERVICES = {
    "coordinator": ("coordinator", "COORDINATOR_DB_PORT", 54321),
    "wallet": ("wallet", "WALLET_DB_PORT", 54322),
    "catalog": ("catalog", "CATALOG_DB_PORT", 54323),
    "audit_ledger": ("audit_ledger", "AUDIT_LEDGER_DB_PORT", 54324),
}


def database_url(service: str) -> str:
    override = os.environ.get(f"{service.upper()}_DATABASE_URL")
    if override:
        return override
    name, port_variable, default_port = SERVICES[service]
    port = os.environ.get(port_variable, str(default_port))
    password = os.environ.get("POSTGRES_PASSWORD", "ledgermesh_dev")
    return f"postgresql+psycopg://{name}:{password}@localhost:{port}/{name}"


def alembic_config(service: str, url: str | None = None) -> Config:
    config = Config(str(ROOT / "services" / service / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", url or database_url(service))
    return config


def require_safe_environment(action: str) -> None:
    environment = os.environ.get("LEDGERMESH_ENV", "")
    if environment not in SAFE_ENVIRONMENTS:
        sys.exit(
            f"Refusing to {action}: it deletes data. Set LEDGERMESH_ENV=local or "
            f"LEDGERMESH_ENV=test (currently {environment!r})."
        )


def run(action: str, services: list[str]) -> None:
    if action in {"downgrade", "reset"}:
        require_safe_environment(action)
    for service in services:
        config = alembic_config(service)
        print(f"{action:<9} {service}")
        if action in {"downgrade", "reset"}:
            command.downgrade(config, "base")
        if action in {"upgrade", "reset"}:
            command.upgrade(config, "head")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=["upgrade", "downgrade", "reset"])
    parser.add_argument("services", nargs="*", metavar="service", help=", ".join(SERVICES))
    args = parser.parse_args(argv)
    unknown = sorted(set(args.services) - set(SERVICES))
    if unknown:
        parser.error(f"unknown service(s): {', '.join(unknown)}")
    run(args.action, args.services or list(SERVICES))


if __name__ == "__main__":
    main()
