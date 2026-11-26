"""Publish invariant violations to Prometheus (LM-034).

    make metrics-invariants

Runs the cross-service invariant check every ``--interval`` seconds against the
four service databases and serves the result as
``ledgermesh_invariant_violations{invariant="INV-01"...}`` on ``--port``
(default 9104), which Prometheus scrapes. Every known invariant is always
published, as 0 when clean, so the dashboard shows "no violations" rather than
"no data".

It reuses the checker from the 2PC integration tests (LM-017), so the rules
live in one place; the invariant checker issue (LM-031) extends that checker.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from collections.abc import Iterable

from ledgermesh_messaging import record_invariant_violations
from prometheus_client import Gauge, start_http_server
from sqlalchemy import Engine, create_engine

logger = logging.getLogger(__name__)

KNOWN_INVARIANTS = ("INV-01", "INV-02", "INV-03", "INV-04", "INV-05", "INV-06", "prepared_leak")
PASSWORD = os.environ.get("POSTGRES_PASSWORD", "ledgermesh_dev")
DEFAULT_URLS = {
    "coordinator": f"postgresql+psycopg://coordinator:{PASSWORD}@localhost:54321/coordinator",
    "wallet": f"postgresql+psycopg://wallet:{PASSWORD}@localhost:54322/wallet",
    "catalog": f"postgresql+psycopg://catalog:{PASSWORD}@localhost:54323/catalog",
    "audit_ledger": f"postgresql+psycopg://audit_ledger:{PASSWORD}@localhost:54324/audit_ledger",
}

CHECK_UP = Gauge(
    "ledgermesh_invariant_check_up",
    "1 if the last invariant check ran, 0 if it failed (violations are then stale).",
)


def classify(violations: Iterable[str]) -> dict[str, int]:
    """Count violation messages per invariant, with every known invariant present."""
    counts = dict.fromkeys(KNOWN_INVARIANTS, 0)
    for violation in violations:
        if "prepared transactions leaked" in violation:
            key = "prepared_leak"
        else:
            key = violation.split(":", 1)[0].strip()
        counts[key] = counts.get(key, 0) + 1
    return counts


def engines_from_env() -> dict[str, Engine]:
    urls = {
        service: os.environ.get(f"{service.upper()}_DATABASE_URL", default)
        for service, default in DEFAULT_URLS.items()
    }
    return {service: create_engine(url, pool_pre_ping=True) for service, url in urls.items()}


def check_once(engines: dict[str, Engine]) -> dict[str, int]:
    from tests.integration.invariants import check_2pc_invariants

    counts = classify(check_2pc_invariants(engines).violations)
    record_invariant_violations(counts)
    return counts


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=9104)
    parser.add_argument("--interval", type=float, default=15.0)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)

    engines = engines_from_env()
    start_http_server(args.port)
    logger.info("serving invariant metrics on :%d every %.0fs", args.port, args.interval)
    while True:
        try:
            counts = check_once(engines)
        except Exception:
            CHECK_UP.set(0)
            logger.exception("invariant check failed")
        else:
            CHECK_UP.set(1)
            if total := sum(counts.values()):
                logger.warning("%d invariant violations: %s", total, counts)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
