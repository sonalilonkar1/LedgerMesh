"""infra/staging/Caddyfile: the public gateway is an explicit allowlist (LM-048).

The Caddyfile's named matchers and `handle` blocks are parsed and replayed
in order by a tiny router, so each allowed route is checked to reach the
right upstream with the right path, and each internal or write route is
checked to fall through to the catch-all 404. The live proof (the real Caddy,
encoded paths included) is part of local validation and the LM-050 hosted
smoke suite.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CADDYFILE = (ROOT / "infra" / "staging" / "Caddyfile").read_text()
UUID = "0b5c6c0e-9a4f-4d0c-8f5e-3f2a1b0c9d8e"


@dataclass
class Matcher:
    method: str | None = None
    path: str | None = None
    regexp: str | None = None

    def matches(self, method: str, path: str) -> bool:
        if self.method and method != self.method:
            return False
        if self.path is not None:
            return path == self.path
        if self.regexp is not None:
            return re.search(self.regexp, path) is not None
        return True


@dataclass
class Handle:
    matcher: str | None
    directives: list[list[str]] = field(default_factory=list)


def strip_comments(text: str) -> list[str]:
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def parse_site(text: str) -> tuple[dict[str, Matcher], list[Handle]]:
    lines = strip_comments(text)
    start = lines.index("{$LEDGERMESH_SITE_ADDRESS} {")
    matchers: dict[str, Matcher] = {}
    handles: list[Handle] = []
    index = start + 1
    while lines[index] != "}":
        line = lines[index]
        if line.startswith("@") and line.endswith("{"):
            name = line.split()[0]
            matcher = Matcher()
            index += 1
            while lines[index] != "}":
                key, value = lines[index].split(maxsplit=1)
                if key == "method":
                    matcher.method = value
                elif key == "path":
                    matcher.path = value
                elif key == "path_regexp":
                    matcher.regexp = value
                else:
                    raise AssertionError(f"unexpected matcher option: {lines[index]}")
                index += 1
            matchers[name] = matcher
        elif line.startswith("handle"):
            parts = line.split()
            handle = Handle(parts[1] if len(parts) == 3 else None)
            index += 1
            while lines[index] != "}":
                handle.directives.append(lines[index].split())
                index += 1
            handles.append(handle)
        elif line.endswith("{"):  # tls, request_body, header blocks
            while lines[index] != "}":
                index += 1
        index += 1
    return matchers, handles


MATCHERS, HANDLES = parse_site(CADDYFILE)


def route(method: str, path: str) -> tuple[str, str] | int:
    """(upstream, upstream path) for a proxied request, or the status code."""
    for handle in HANDLES:
        if handle.matcher is not None and not MATCHERS[handle.matcher].matches(method, path):
            continue
        rewritten = path
        for directive in handle.directives:
            if directive[0] == "rewrite":
                rewritten = directive[-1]
            elif directive[:2] == ["uri", "strip_prefix"]:
                assert rewritten.startswith(directive[2])
                rewritten = rewritten[len(directive[2]) :]
            elif directive[0] == "reverse_proxy":
                return directive[1], rewritten
            elif directive[0] == "respond":
                return int(directive[1])
        raise AssertionError(f"handle without a response: {handle}")
    raise AssertionError("no catch-all handle")


ALLOWED = [
    ("GET", "/health", "coordinator:8000", "/health"),
    ("POST", "/v1/redemptions", "coordinator:8000", "/v1/redemptions"),
    ("GET", f"/v1/redemptions/{UUID}", "coordinator:8000", f"/v1/redemptions/{UUID}"),
    ("GET", "/health/coordinator", "coordinator:8000", "/health"),
    ("GET", "/health/wallet", "wallet:8001", "/health"),
    ("GET", "/health/catalog", "catalog:8002", "/health"),
    ("GET", "/health/audit-ledger", "audit-ledger:8003", "/health"),
    ("GET", "/wallet/v1/wallets/user-001", "wallet:8001", "/v1/wallets/user-001"),
    ("GET", "/wallet/v1/wallets/smoke.run_1-a", "wallet:8001", "/v1/wallets/smoke.run_1-a"),
    ("GET", "/catalog/v1/rewards", "catalog:8002", "/v1/rewards"),
    ("GET", "/catalog/v1/rewards/reward-001", "catalog:8002", "/v1/rewards/reward-001"),
    (
        "GET",
        f"/audit/v1/transactions/{UUID}/events",
        "audit-ledger:8003",
        f"/v1/transactions/{UUID}/events",
    ),
]

DENIED = [
    # Coordinator internals and 2PC triggers.
    ("GET", "/internal/v1/2pc/in-doubt"),
    ("POST", "/internal/v1/2pc/recovery/run"),
    ("POST", f"/internal/v1/redemptions/{UUID}/execute-2pc"),
    ("GET", f"/internal/v1/redemptions/{UUID}/transitions"),
    ("GET", f"/v1/redemptions/{UUID}/transitions"),
    ("GET", "/v1/redemptions"),
    ("POST", f"/v1/redemptions/{UUID}"),
    ("DELETE", f"/v1/redemptions/{UUID}"),
    ("GET", "/v1/redemptions/not-a-uuid"),
    # Wallet writes and participant routes.
    ("POST", "/wallet/v1/wallets/user-001/deductions"),
    ("POST", "/wallet/v1/wallets/user-001/refunds"),
    ("GET", "/wallet/v1/wallets/user-001/deductions"),
    ("POST", "/wallet/v1/wallets/user-001"),
    ("GET", f"/wallet/v1/transactions/{UUID}"),
    ("POST", f"/wallet/v1/transactions/{UUID}/prepare"),
    ("POST", f"/wallet/v1/transactions/{UUID}/commit"),
    ("POST", f"/wallet/v1/transactions/{UUID}/abort"),
    ("GET", "/v1/wallets/user-001"),
    # Catalog writes and participant routes.
    ("POST", "/catalog/v1/rewards/reward-001/reservations"),
    ("POST", "/catalog/v1/rewards/reward-001/releases"),
    ("POST", "/catalog/v1/rewards/reward-001/fulfillments"),
    ("POST", f"/catalog/v1/transactions/{UUID}/prepare"),
    ("GET", f"/catalog/v1/transactions/{UUID}"),
    ("POST", "/catalog/v1/rewards"),
    # Audit writes.
    ("POST", "/audit/v1/audit-events"),
    ("GET", "/audit/v1/audit-events"),
    ("POST", f"/audit/v1/transactions/{UUID}/events"),
    # FastAPI documentation.
    ("GET", "/docs"),
    ("GET", "/openapi.json"),
    ("GET", "/wallet/docs"),
    ("GET", "/catalog/openapi.json"),
    # Traversal and look-alikes.
    ("GET", "/wallet/v1/wallets/.."),
    ("GET", "/wallet/v1/wallets/."),
    ("GET", "/wallet/v1/wallets/..%2Fv1%2Fwallets%2Fx%2Fdeductions"),
    ("GET", "/wallet/v1/wallets/../../internal/v1/2pc/in-doubt"),
    ("GET", "/catalog/v1/rewards/.."),
    ("GET", "/catalog/v1/rewards/x/../../v1/rewards/x/reservations"),
    ("GET", "/health/"),
    ("GET", "/health/kafka"),
    ("POST", "/health"),
    ("HEAD", "/health"),
    ("GET", "/"),
]


@pytest.mark.parametrize(("method", "path", "upstream", "upstream_path"), ALLOWED)
def test_allowed_routes_reach_the_right_service_and_path(method, path, upstream, upstream_path):
    assert route(method, path) == (upstream, upstream_path)


@pytest.mark.parametrize(("method", "path"), DENIED)
def test_everything_else_is_404(method, path):
    assert route(method, path) == 404


def test_the_last_handle_is_a_catch_all_404():
    assert HANDLES[-1].matcher is None
    assert HANDLES[-1].directives == [["respond", "404"]]


def test_every_proxied_upstream_is_a_service_api():
    upstreams = {d[1] for h in HANDLES for d in h.directives if d[0] == "reverse_proxy"}
    assert upstreams == {"coordinator:8000", "wallet:8001", "catalog:8002", "audit-ledger:8003"}


def test_no_forbidden_words_outside_comments():
    code = "\n".join(strip_comments(CADDYFILE))
    for word in (
        "internal/",
        "deductions",
        "refunds",
        "prepare",
        "commit",
        "abort",
        "reservations",
        "releases",
        "fulfillments",
        "audit-events",
        "kafka",
        "-db:",
        "2019",
    ):
        assert word not in code.replace("admin localhost:2019", ""), word


def test_tls_and_http3_settings():
    lines = strip_comments(CADDYFILE)
    assert "tls {$LEDGERMESH_TLS}" in lines
    assert "protocols h1 h2" in lines  # TCP only; the firewall does not allow UDP
    assert "admin localhost:2019" in lines
