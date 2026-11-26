"""LM-039: participant fencing tokens are monotonic and stale terms are audited."""

from uuid import uuid4

import pytest
from ledgermesh_messaging import EventEnvelope
from sqlalchemy import select

from services.catalog.app.db.schema import fencing_rejections as catalog_rejections
from services.catalog.app.db.schema import participant_fencing_state as catalog_state
from services.catalog.app.errors import StaleFencingToken as CatalogStaleFencingToken
from services.catalog.app.fencing import accept_fencing_token as catalog_accept
from services.catalog.app.participant import PostgresCatalogParticipantRepository
from services.catalog.app.repository import PostgresCatalogRepository
from services.catalog.app.saga import CatalogSagaHandler
from services.catalog.app.saga import parse_command as parse_catalog_command
from services.wallet.app.db.schema import fencing_rejections as wallet_rejections
from services.wallet.app.db.schema import participant_fencing_state as wallet_state
from services.wallet.app.errors import StaleFencingToken as WalletStaleFencingToken
from services.wallet.app.fencing import accept_fencing_token as wallet_accept
from services.wallet.app.participant import PostgresWalletParticipantRepository
from services.wallet.app.repository import PostgresWalletRepository
from services.wallet.app.saga import WalletSagaHandler
from services.wallet.app.saga import parse_command as parse_wallet_command
from tests.db.conftest import upgrade

pytestmark = pytest.mark.database


@pytest.mark.parametrize(
    ("service", "accept", "state", "rejections"),
    [
        ("wallet", wallet_accept, wallet_state, wallet_rejections),
        ("catalog", catalog_accept, catalog_state, catalog_rejections),
    ],
)
def test_participant_advances_fence_and_audits_stale_terms(
    service, accept, state, rejections, database_url, engine
) -> None:
    upgrade(service, database_url)
    transaction_id = uuid4()

    with engine.begin() as connection:
        assert accept(connection, 4, transaction_id, "prepare", "two_phase_commit") == (True, 4)
    with engine.begin() as connection:
        assert accept(connection, 3, transaction_id, "prepare", "two_phase_commit") == (False, 4)
    with engine.begin() as connection:
        assert accept(connection, 6, transaction_id, "reserve", "saga") == (True, 6)
    with engine.begin() as connection:
        assert accept(connection, 5, transaction_id, "reserve", "saga") == (False, 6)

    with engine.connect() as connection:
        assert connection.scalar(select(state.c.highest_token)) == 6
        rows = connection.execute(select(rejections).order_by(rejections.c.id)).all()

    assert [(row.supplied_token, row.highest_token) for row in rows] == [(3, 4), (5, 6)]
    assert [row.coordination_mode for row in rows] == ["two_phase_commit", "saga"]


def test_wallet_2pc_prepare_rejects_a_token_below_the_persisted_high_watermark(
    database_url, engine
) -> None:
    upgrade("wallet", database_url)
    repository = PostgresWalletParticipantRepository(engine)
    transaction_id = uuid4()
    correlation_id = uuid4()

    # A missing wallet produces a normal NO vote, but still establishes term 10.
    assert (
        repository.prepare(transaction_id, correlation_id, "missing-user", 1, 10).state == "ABORTED"
    )
    with pytest.raises(WalletStaleFencingToken, match="older than accepted token 10"):
        repository.prepare(uuid4(), uuid4(), "missing-user", 1, 9)

    with engine.connect() as connection:
        assert connection.scalar(select(wallet_state.c.highest_token)) == 10
        assert connection.scalar(select(wallet_rejections.c.id)) is not None


def test_catalog_2pc_prepare_rejects_a_token_below_the_persisted_high_watermark(
    database_url, engine
) -> None:
    upgrade("catalog", database_url)
    repository = PostgresCatalogParticipantRepository(engine)
    transaction_id = uuid4()

    assert (
        repository.prepare(transaction_id, uuid4(), "missing-reward", 1, 0, 1, 10).state
        == "ABORTED"
    )
    with pytest.raises(CatalogStaleFencingToken, match="older than accepted token 10"):
        repository.prepare(uuid4(), uuid4(), "missing-reward", 1, 0, 1, 9)

    with engine.connect() as connection:
        assert connection.scalar(select(catalog_state.c.highest_token)) == 10
        assert connection.scalar(select(catalog_rejections.c.id)) is not None


def test_wallet_saga_rejects_and_audits_a_stale_command(database_url, engine) -> None:
    upgrade("wallet", database_url)
    transaction_id = uuid4()
    with engine.begin() as connection:
        wallet_accept(connection, 10, transaction_id, "wallet.deduct_points", "saga")
    command = EventEnvelope(
        event_type="wallet.deduct_points",
        transaction_id=transaction_id,
        correlation_id=uuid4(),
        aggregate_type="wallet",
        aggregate_id="user-1",
        workflow_step=1,
        fencing_token=9,
        payload={"user_id": "user-1", "points": 10},
    )

    with engine.begin() as connection:
        reply = WalletSagaHandler(PostgresWalletRepository(engine)).apply(
            connection, parse_wallet_command(command)
        )

    assert reply.event_type == "wallet.deduction_rejected"
    assert reply.payload["reason"] == "stale_fencing_token"
    with engine.connect() as connection:
        assert connection.scalar(select(wallet_rejections.c.id)) is not None


def test_catalog_saga_rejects_and_audits_a_stale_command(database_url, engine) -> None:
    upgrade("catalog", database_url)
    transaction_id = uuid4()
    with engine.begin() as connection:
        catalog_accept(connection, 10, transaction_id, "catalog.reserve_inventory", "saga")
    command = EventEnvelope(
        event_type="catalog.reserve_inventory",
        transaction_id=transaction_id,
        correlation_id=uuid4(),
        aggregate_type="reward",
        aggregate_id="reward-1",
        workflow_step=3,
        fencing_token=9,
        payload={"reward_id": "reward-1", "quantity": 1},
    )

    with engine.begin() as connection:
        reply = CatalogSagaHandler(PostgresCatalogRepository(engine)).apply(
            connection, parse_catalog_command(command)
        )

    assert reply.event_type == "catalog.reservation_rejected"
    assert reply.payload["reason"] == "stale_fencing_token"
    with engine.connect() as connection:
        assert connection.scalar(select(catalog_rejections.c.id)) is not None
