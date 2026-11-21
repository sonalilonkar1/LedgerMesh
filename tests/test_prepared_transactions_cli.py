"""Safety tests for prepared-transaction administration."""

import pytest

from infra.scripts.prepared_transactions import run


@pytest.mark.parametrize("environment", [None, "", "staging", "production"])
def test_bulk_cleanup_refuses_unsafe_environment(monkeypatch, environment) -> None:
    if environment is None:
        monkeypatch.delenv("LEDGERMESH_ENV", raising=False)
    else:
        monkeypatch.setenv("LEDGERMESH_ENV", environment)

    # The safety check occurs before a database engine or connection is created.
    with pytest.raises(SystemExit, match="Refusing to clean up prepared transactions"):
        run("cleanup", ["wallet"])
