"""Small Prometheus-compatible metrics endpoint for LedgerMesh workers.

The counters are process-local. Each worker exposes its own ``/metrics`` endpoint;
scrapers should discover or scrape every worker process independently.
"""

from __future__ import annotations

import os
import threading
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_lock = threading.Lock()
_inbox_duplicates: dict[str, int] = defaultdict(int)
_saga_rejections: dict[tuple[str, str, str], int] = defaultdict(int)


def record_inbox_duplicate(consumer: str) -> None:
    with _lock:
        _inbox_duplicates[consumer] += 1


def record_saga_rejection(consumer: str, service: str, reason: str) -> None:
    with _lock:
        _saga_rejections[(consumer, service, reason)] += 1


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def render_metrics() -> str:
    """Return current counters in Prometheus text exposition format."""
    with _lock:
        duplicates = sorted(_inbox_duplicates.items())
        rejections = sorted(_saga_rejections.items())
    lines = [
        "# HELP ledgermesh_inbox_duplicate_deliveries_total Duplicate inbox deliveries.",
        "# TYPE ledgermesh_inbox_duplicate_deliveries_total counter",
    ]
    lines.extend(
        f'ledgermesh_inbox_duplicate_deliveries_total{{consumer="{_escape(consumer)}"}} {value}'
        for consumer, value in duplicates
    )
    lines.extend(
        [
            "# HELP ledgermesh_saga_rejections_total Durable Saga business rejections.",
            "# TYPE ledgermesh_saga_rejections_total counter",
        ]
    )
    lines.extend(
        f'ledgermesh_saga_rejections_total{{consumer="{_escape(consumer)}",'
        f'service="{_escape(service)}",reason="{_escape(reason)}"}} {value}'
        for (consumer, service, reason), value in rejections
    )
    return "\n".join(lines) + "\n"


def _handler() -> type[BaseHTTPRequestHandler]:
    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != "/metrics":
                self.send_error(404)
                return
            body = render_metrics().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            return

    return MetricsHandler


class MetricsServer:
    """Background HTTP server exposing the process metrics on ``/metrics``."""

    def __init__(self, host: str, port: int) -> None:
        self._server = ThreadingHTTPServer((host, port), _handler())
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="ledgermesh-metrics", daemon=True
        )

    @property
    def address(self) -> tuple[str, int]:
        return self._server.server_address[:2]

    def start(self) -> MetricsServer:
        self._thread.start()
        return self

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


def start_metrics_server(service: str, default_port: int) -> MetricsServer:
    """Start metrics for a worker, configured with ``<SERVICE>_METRICS_PORT``."""
    env_prefix = service.upper().replace("-", "_")
    host = os.environ.get("LEDGERMESH_METRICS_HOST", "127.0.0.1")
    port = int(os.environ.get(f"{env_prefix}_METRICS_PORT", default_port))
    return MetricsServer(host, port).start()
