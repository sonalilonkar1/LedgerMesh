"""Hosted smoke: the public gateway contract (LM-050).

Only the routes in infra/staging/Caddyfile may be reachable from the
internet; everything else, above all /internal/* and every participant write
route, must get Caddy's empty 404 without reaching a service.
"""

from __future__ import annotations

import socket
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from tests.hosted._support import Settings, make_client, requires_hosted

pytestmark = requires_hosted

SERVICES = ("coordinator", "wallet", "catalog", "audit-ledger")
TX = "00000000-0000-4000-8000-000000000000"


@pytest.mark.parametrize(
    ("path", "service"),
    [("/health", "coordinator")] + [(f"/health/{s}", s) for s in SERVICES],
)
def test_health_reports_the_right_service(client: httpx.Client, path: str, service: str):
    response = client.get(path)
    assert response.status_code == 200, f"GET {path}: HTTP {response.status_code}"
    assert response.json() == {"status": "ok", "service": service}, f"GET {path}"


def test_responses_are_hardened(client: httpx.Client):
    response = client.get("/health")
    assert "server" not in response.headers, "the gateway must not reveal its server software"
    assert response.headers.get("strict-transport-security", "").startswith("max-age=")
    assert "alt-svc" not in response.headers, "HTTP/3 must not be advertised (TCP only)"


def test_read_only_routes_reach_their_services(client: httpx.Client, settings: Settings):
    # Unknown ids prove the request reached the service: its problem+json 404,
    # not the gateway's empty one.
    missing = f"smoke-{settings.run_id or settings.token}-missing"
    for path in (
        f"/v1/redemptions/{uuid.uuid4()}",
        f"/wallet/v1/wallets/{missing}",
        f"/catalog/v1/rewards/{missing}",
    ):
        response = client.get(path)
        assert response.status_code == 404, f"GET {path}: HTTP {response.status_code}"
        assert response.headers.get("content-type", "").startswith("application/"), (
            f"GET {path} was answered by the gateway, not the service"
        )
        assert response.json().get("status") == 404, f"GET {path}: {response.text[:200]}"
    events = client.get(f"/audit/v1/transactions/{uuid.uuid4()}/events")
    assert events.status_code == 200 and events.json() == [], events.text[:200]
    rewards = client.get("/catalog/v1/rewards")
    assert rewards.status_code == 200 and isinstance(rewards.json(), list)


# Never public (ADR-001; infra/staging/Caddyfile): internal coordinator routes,
# 2PC participant routes, Saga participant writes, audit writes, API docs,
# wrong methods on allowed paths, and path tricks.
DENIED = [
    ("GET", "/docs"),
    ("GET", "/openapi.json"),
    ("GET", "/wallet/docs"),
    ("GET", "/wallet/openapi.json"),
    ("GET", "/catalog/openapi.json"),
    ("GET", "/audit/openapi.json"),
    ("GET", "/internal/v1/2pc/in-doubt"),
    ("POST", "/internal/v1/2pc/recovery/run"),
    ("POST", f"/internal/v1/redemptions/{TX}/execute-2pc"),
    ("GET", f"/internal/v1/redemptions/{TX}/transitions"),
    ("POST", "/wallet/v1/wallets/smoke-x/deductions"),
    ("POST", "/wallet/v1/wallets/smoke-x/refunds"),
    ("POST", f"/wallet/v1/transactions/{TX}/prepare"),
    ("POST", f"/wallet/v1/transactions/{TX}/commit"),
    ("POST", f"/wallet/v1/transactions/{TX}/abort"),
    ("GET", f"/wallet/v1/transactions/{TX}"),
    ("POST", "/catalog/v1/rewards/smoke-x/reservations"),
    ("POST", "/catalog/v1/rewards/smoke-x/releases"),
    ("POST", "/catalog/v1/rewards/smoke-x/fulfillments"),
    ("POST", f"/catalog/v1/transactions/{TX}/prepare"),
    ("POST", f"/catalog/v1/transactions/{TX}/commit"),
    ("POST", f"/catalog/v1/transactions/{TX}/abort"),
    ("GET", f"/catalog/v1/transactions/{TX}"),
    ("POST", "/audit/v1/audit-events"),
    ("GET", "/audit/v1/audit-events"),
    ("POST", "/health"),
    ("GET", "/health/kafka"),
    ("GET", "/wallet/health"),
    ("GET", "/v1/redemptions"),
    ("PUT", "/v1/redemptions"),
    ("DELETE", f"/v1/redemptions/{TX}"),
    ("POST", "/wallet/v1/wallets/smoke-x"),
    ("DELETE", "/catalog/v1/rewards/smoke-x"),
    ("GET", "/wallet/v1/wallets/"),
    ("GET", "/wallet/v1/wallets/..%2Fv1%2Fwallets%2Fsmoke-x%2Fdeductions"),
    ("POST", "/wallet/v1/wallets/..%2Fv1%2Fwallets%2Fsmoke-x%2Fdeductions"),
    ("GET", "/catalog/v1/rewards/..%2F..%2Finternal%2Fv1%2F2pc%2Fin-doubt"),
    ("GET", "/wallet/v1/wallets/../../internal/v1/2pc/in-doubt"),
    ("GET", f"/v1/redemptions/{TX}/../../internal/v1/2pc/in-doubt"),
    ("GET", "/catalog/v1/rewards/.hidden"),
]


@pytest.mark.parametrize(("method", "path"), DENIED, ids=[f"{m} {p}" for m, p in DENIED])
def test_everything_else_is_an_empty_gateway_404(client: httpx.Client, method: str, path: str):
    # raw_path: send the path exactly as written (no dot-segment clean-up).
    url = client.base_url.copy_with(raw_path=path.encode("ascii"))
    body = b"{}" if method in {"POST", "PUT"} else None
    headers = {"content-type": "application/json"} if body else {}
    response = client.request(method, url, content=body, headers=headers)
    assert response.status_code == 404, f"{method} {path}: HTTP {response.status_code}"
    assert response.content == b"", (
        f"{method} {path} reached a service ({response.text[:120]!r}); "
        "the gateway must answer it itself"
    )


def test_http_redirects_to_https(settings: Settings):
    if settings.port != 443:
        pytest.skip("the base URL uses a custom HTTPS port; the HTTP port is unknown")
    with httpx.Client(timeout=10.0, follow_redirects=False) as plain:
        try:
            response = plain.get(f"http://{settings.host}/health")
        except httpx.HTTPError as error:
            pytest.fail(f"http://{settings.host}/health: {type(error).__name__}: {error}")
    assert response.status_code in {301, 308}, f"HTTP {response.status_code}"
    assert response.headers["location"] == f"https://{settings.host}/health"


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False


def test_only_the_gateway_ports_are_reachable(settings: Settings):
    if not settings.closed_ports:
        pytest.skip("LEDGERMESH_HOSTED_CLOSED_PORTS=none")
    with ThreadPoolExecutor(max_workers=len(settings.closed_ports)) as pool:
        results = dict(
            zip(
                settings.closed_ports,
                pool.map(lambda p: _port_open(settings.host, p), settings.closed_ports),
                strict=True,
            )
        )
    open_ports = sorted(port for port, is_open in results.items() if is_open)
    assert open_ports == [], f"{settings.host} accepts TCP connections on {open_ports}"


def test_tls_is_verified(settings: Settings):
    # The client never disables verification; prove it rejects an untrusted
    # chain by trusting nothing but an unrelated CA when one was configured.
    if settings.ca_bundle is None:
        pytest.skip("publicly trusted certificate; the system trust store verified it")
    import certifi

    strict = Settings(**{**settings.__dict__, "ca_bundle": certifi.where()})
    with make_client(strict) as other, pytest.raises(httpx.ConnectError):
        other.get("/health")
