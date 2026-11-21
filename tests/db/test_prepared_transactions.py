"""LM-012 integration tests for PostgreSQL prepared transactions."""

from uuid import uuid4

import pytest
from psycopg import connect, sql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from infra.scripts.prepared_transactions import cleanup_prepared, list_prepared

pytestmark = pytest.mark.database


def _prepare(database_url: str, gid: str, value: str) -> None:
    # Use psycopg's SQL composer so PREPARE receives a safely quoted literal and
    # close the connection immediately afterwards to simulate service loss.
    url = make_url(database_url).set(drivername="postgresql")
    connection = connect(url.render_as_string(hide_password=False))
    try:
        connection.execute("INSERT INTO prepared_probe(value) VALUES (%s)", (value,))
        connection.execute(sql.SQL("PREPARE TRANSACTION {}").format(sql.Literal(gid)))
    finally:
        connection.close()


@pytest.fixture
def prepared_database(database_url: str):
    engine = create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE prepared_probe (value text NOT NULL)"))
    try:
        yield engine
    finally:
        cleanup_prepared(engine, report=False)
        engine.dispose()


def test_development_server_enables_prepared_transactions(prepared_database) -> None:
    with prepared_database.connect() as connection:
        configured = connection.scalar(text("SHOW max_prepared_transactions"))
    assert int(configured) > 0


def test_prepared_transaction_survives_connection_loss(database_url, prepared_database) -> None:
    gid = f"ledgermesh:test:survival:{uuid4()}"
    _prepare(database_url, gid, "survives")

    assert [item.gid for item in list_prepared(prepared_database)] == [gid]
    with prepared_database.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM prepared_probe")) == 0

    with prepared_database.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.exec_driver_sql(f"COMMIT PREPARED '{gid}'")
    with prepared_database.connect() as connection:
        assert connection.scalar(text("SELECT value FROM prepared_probe")) == "survives"


def test_diagnostic_query_lists_unresolved_transaction(database_url, prepared_database) -> None:
    gid = f"ledgermesh:test:diagnostic:{uuid4()}"
    _prepare(database_url, gid, "diagnostic")

    with prepared_database.connect() as connection:
        current_user = connection.scalar(text("SELECT current_user"))

    [transaction] = list_prepared(prepared_database)
    assert transaction.gid == gid
    assert transaction.database.startswith("lm_migration_test_")
    assert transaction.owner == current_user
    assert transaction.prepared.tzinfo is not None


def test_cleanup_rolls_back_every_leaked_transaction(database_url, prepared_database) -> None:
    gids = [f"ledgermesh:test:cleanup:{uuid4()}" for _ in range(2)]
    for gid in gids:
        _prepare(database_url, gid, gid)

    assert set(cleanup_prepared(prepared_database, report=False)) == set(gids)
    assert list_prepared(prepared_database) == []
    with prepared_database.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM prepared_probe")) == 0
