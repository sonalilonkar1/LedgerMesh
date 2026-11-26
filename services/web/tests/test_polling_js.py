"""Runs the page's JavaScript polling tests with Node (LM-011).

CI's runners have Node; locally the test is skipped if Node isn't installed.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

TESTS = Path(__file__).parent / "polling.test.mjs"


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is not installed")
def test_polling_never_stops_on_an_unfinished_redemption():
    result = subprocess.run(
        ["node", "--test", str(TESTS)], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_the_page_loads_the_polling_module_and_offers_check_now():
    page = (Path(__file__).parents[1] / "app" / "static" / "index.html").read_text()
    script = (Path(__file__).parents[1] / "app" / "static" / "app.js").read_text()

    assert '<script type="module" src="/static/app.js">' in page
    assert 'id="check-now"' in page
    assert 'id="notice"' in page
    assert 'from "./polling.js"' in script
    assert "POLL_LIMIT" not in script
