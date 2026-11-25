from urllib.error import HTTPError
from urllib.request import urlopen

from ledgermesh_messaging.metrics import (
    MetricsServer,
    record_inbox_duplicate,
    record_saga_rejection,
)


def test_metrics_endpoint_exposes_duplicate_and_rejection_counters():
    server = MetricsServer("127.0.0.1", 0).start()
    try:
        record_inbox_duplicate("wallet-saga")
        record_saga_rejection("wallet-saga", "wallet", 'insufficient "points"')

        with urlopen(
            f"http://{server.address[0]}:{server.address[1]}/metrics", timeout=3
        ) as response:
            body = response.read().decode()

        assert response.status == 200
        assert 'ledgermesh_inbox_duplicate_deliveries_total{consumer="wallet-saga"}' in body
        assert (
            'ledgermesh_saga_rejections_total{consumer="wallet-saga",service="wallet",'
            'reason="insufficient \\"points\\""}' in body
        )
    finally:
        server.close()


def test_metrics_endpoint_only_serves_metrics_path():
    server = MetricsServer("127.0.0.1", 0).start()
    try:
        try:
            urlopen(f"http://{server.address[0]}:{server.address[1]}/", timeout=3)
        except HTTPError as error:
            assert error.code == 404
        else:
            raise AssertionError("unexpected route accepted")
    finally:
        server.close()
