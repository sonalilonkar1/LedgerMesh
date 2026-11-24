# ruff: noqa: F811  (the `harness` fixture is imported, then used as a test argument)
"""LM-045: different 2PC redemptions racing for the same balance or the last unit.

Each test starts N redemptions at the same moment against a resource that can only
satisfy some of them, then checks that exactly the affordable number succeeded and
that the losers left no trace. The start order is shuffled from a fixed seed, so a
failing seed can be replayed.
"""

from __future__ import annotations

import random
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import select, update

from services.catalog.app.db.schema import rewards
from services.wallet.app.db.schema import wallets
from tests.integration.test_2pc_atomicity import (  # noqa: F401  (harness is a fixture)
    Harness,
    assert_zero_violations,
    harness,
)

pytestmark = [pytest.mark.database, pytest.mark.integration]

SEEDS = (1, 2, 3)
POINT_COST = 100  # the seeded "reward-stocked" reward costs 100 points


def set_resources(harness: Harness, *, points: int, inventory: int) -> None:
    with harness.engines["wallet"].begin() as connection:
        connection.execute(
            update(wallets)
            .where(wallets.c.user_id == "user-funded")
            .values(available_points=points)
        )
    with harness.engines["catalog"].begin() as connection:
        connection.execute(
            update(rewards)
            .where(rewards.c.reward_id == "reward-stocked")
            .values(total_inventory=max(inventory, 10), available_inventory=inventory)
        )


def balance(harness: Harness) -> int:
    with harness.engines["wallet"].connect() as connection:
        return connection.scalar(
            select(wallets.c.available_points).where(wallets.c.user_id == "user-funded")
        )


def inventory(harness: Harness) -> int:
    with harness.engines["catalog"].connect() as connection:
        return connection.scalar(
            select(rewards.c.available_inventory).where(rewards.c.reward_id == "reward-stocked")
        )


def race(harness: Harness, redemptions: int, seed: int) -> Counter:
    """Create the redemptions, then execute all of them at the same moment."""
    created = [harness.create().transaction_id for _ in range(redemptions)]
    random.Random(seed).shuffle(created)
    start = threading.Barrier(redemptions)

    def execute(transaction_id) -> str:
        start.wait()
        return harness.workflow.execute(transaction_id).status

    with ThreadPoolExecutor(max_workers=redemptions) as pool:
        return Counter(pool.map(execute, created))


def assert_no_overspend_or_oversell(harness, outcomes, *, points: int, stock: int) -> None:
    """Safety: whatever the winners were, the money and stock add up exactly."""
    winners = outcomes["COMMITTED"]
    assert winners * POINT_COST <= points, f"overspent: {outcomes}"
    assert winners <= stock, f"oversold: {outcomes}"
    assert outcomes["COMMITTED"] + outcomes["ABORTED"] == sum(outcomes.values())
    assert set(outcomes) <= {"COMMITTED", "ABORTED"}, f"not all final: {outcomes}"
    assert balance(harness) == points - winners * POINT_COST  # losers cost nothing
    assert inventory(harness) == stock - winners


@pytest.mark.parametrize("seed", SEEDS)
def test_same_user_race_never_overspends_the_balance(harness: Harness, lm017_report, seed: int):
    points, stock = 2 * POINT_COST, 10
    set_resources(harness, points=points, inventory=stock)

    outcomes = race(harness, redemptions=8, seed=seed)

    assert_no_overspend_or_oversell(harness, outcomes, points=points, stock=stock)
    assert outcomes["COMMITTED"] >= 1, outcomes  # at least one affordable redemption won
    assert_zero_violations(harness, lm017_report, f"same-user race seed {seed}")


@pytest.mark.xfail(
    strict=True,
    reason="LM-028 (#30): the Catalog aborts when the quoted reward version went stale, so "
    "racing redemptions of one reward lose even with stock left. Remove once it lands.",
)
@pytest.mark.parametrize("seed", SEEDS)
def test_same_user_race_succeeds_exactly_as_often_as_the_balance_allows(
    harness: Harness, lm017_report, seed: int
):
    affordable = 2
    set_resources(harness, points=affordable * POINT_COST, inventory=10)

    outcomes = race(harness, redemptions=8, seed=seed)

    assert outcomes["COMMITTED"] == affordable, outcomes


@pytest.mark.parametrize("seed", SEEDS)
def test_last_unit_race_has_exactly_one_winner_and_no_trace_from_the_losers(
    harness: Harness, lm017_report, seed: int
):
    points, stock = 1_000, 1
    set_resources(harness, points=points, inventory=stock)

    outcomes = race(harness, redemptions=8, seed=seed)

    assert outcomes["COMMITTED"] == 1, outcomes
    assert_no_overspend_or_oversell(harness, outcomes, points=points, stock=stock)
    assert_zero_violations(harness, lm017_report, f"last-unit race seed {seed}")
