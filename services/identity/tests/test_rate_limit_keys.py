"""Per-IP rate-limit keys group IPv6 clients by /64 (LM-052)."""

import pytest

from services.identity.app.errors import RateLimited
from services.identity.app.rate_limit import client_key

from .fakes import make_harness


@pytest.mark.parametrize(
    ("client", "key"),
    [
        ("198.51.100.7", "198.51.100.7"),
        ("2001:db8:1:2:aaaa::1", "2001:db8:1:2::/64"),
        ("2001:DB8:1:2:ffff:ffff:ffff:ffff", "2001:db8:1:2::/64"),
        ("::ffff:198.51.100.7", "198.51.100.7"),
        ("testclient", "testclient"),
    ],
)
def test_client_key(client, key):
    assert client_key(client) == key


def test_rotating_addresses_inside_one_ipv6_64_shares_the_login_limit():
    harness = make_harness()
    for index in range(20):
        assert harness.service.request_login(f"user{index}@example.com", f"2001:db8:0:1::{index:x}")

    with pytest.raises(RateLimited):
        harness.service.request_login("fresh@example.com", "2001:db8:0:1:ffff::1")

    # A different /64 has its own budget.
    assert harness.service.request_login("fresh@example.com", "2001:db8:0:2::1")
