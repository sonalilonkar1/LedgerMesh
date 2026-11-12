"""Throwaway PostgreSQL databases for migration tests (LM-005).

Each test gets a freshly created, empty database on the server named by
``LM_TEST_POSTGRES_URL`` and drops it afterwards, so the tests never touch the
data in your local service databases. Without that variable they use the local
Compose Wallet database server (``make infra-up``) and skip if it is not running.
"""

import os
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

ROOT = Path(__file__).resolve().parents[2]
SERVICES = ("coordinator", "wallet", "catalog", "audit_ledger")
SERVER_URL = os.environ.get(
    "LM_TEST_POSTGRES_URL",
    "postgresql+psycopg://wallet:ledgermesh_dev@localhost:54322/wallet",
)


def _admin_engine() -> Engine:
    return create_engine(SERVER_URL, isolation_level="AUTOCOMMIT")


@pytest.fixture
def database_url() -> Iterator[str]:
    admin = _admin_engine()
    name = f"lm_migration_test_{uuid4().hex[:12]}"
    try:
        with admin.connect() as connection:
            connection.execute(text(f'CREATE DATABASE "{name}"'))
    except OperationalError as exc:
        admin.dispose()
        if "LM_TEST_POSTGRES_URL" in os.environ:
            raise
        pytest.skip(f"PostgreSQL is not reachable; run make infra-up first ({exc.orig})")

    yield make_url(SERVER_URL).set(database=name).render_as_string(hide_password=False)

    with admin.connect() as connection:
        connection.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    admin.dispose()


@pytest.fixture
def engine(database_url: str) -> Iterator[Engine]:
    engine = create_engine(database_url)
    yield engine
    engine.dispose()


def alembic_config(service: str, database_url: str) -> Config:
    config = Config(str(ROOT / "services" / service / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def upgrade(service: str, database_url: str) -> None:
    command.upgrade(alembic_config(service, database_url), "head")


def downgrade(service: str, database_url: str) -> None:
    command.downgrade(alembic_config(service, database_url), "base")
