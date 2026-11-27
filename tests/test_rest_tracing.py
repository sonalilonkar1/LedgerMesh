"""Trace context crosses REST calls between services (LM-033).

The Coordinator's real 2PC participant client calls a FastAPI server
instrumented the same way every service is, over a real socket, and the
server's span joins the caller's trace. The four service apps are checked to
continue a caller's ``traceparent`` and to leave health checks untraced.
"""

import importlib
import socket
import threading
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from ledgermesh_messaging import configure_tracing, instrument_fastapi, instrument_httpx
from ledgermesh_messaging.tracing import tracer
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from services.coordinator.app.two_phase_commit import HttpWalletParticipant

EXPORTER = InMemorySpanExporter()
configure_tracing("rest-tests", exporter=EXPORTER)
instrument_httpx()

SERVICES = {
    "coordinator": "services.coordinator.app.main",
    "wallet": "services.wallet.app.main",
    "catalog": "services.catalog.app.main",
    "audit-ledger": "services.audit_ledger.app.main",
}
CALLER_TRACE = "0af7651916cd43dd8448eb211c80319c"
CALLER = f"00-{CALLER_TRACE}-b7ad6b7169203331-01"


@pytest.fixture(autouse=True)
def spans():
    EXPORTER.clear()
    yield EXPORTER
    EXPORTER.clear()


@pytest.fixture
def participant_server():
    """A participant endpoint on a real port, recording the headers it received."""
    received = []
    app = FastAPI()
    instrument_fastapi(app)

    @app.post("/v1/transactions/{transaction_id}/prepare")
    async def prepare(transaction_id: str, request: Request):
        received.append(dict(request.headers))
        return {"transaction_id": transaction_id, "state": "PREPARED"}

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.01)
    host, port = sock.getsockname()
    yield SimpleNamespace(url=f"http://{host}:{port}", received=received)
    server.should_exit = True
    thread.join(timeout=5)


def test_a_2pc_participant_call_joins_the_coordinators_trace(participant_server):
    record = SimpleNamespace(
        transaction_id=uuid4(), correlation_id=uuid4(), user_id="user-1", total_points=100
    )
    with tracer().start_as_current_span("coordinator 2pc") as workflow:
        vote = HttpWalletParticipant(participant_server.url).prepare(record)

    assert vote.state == "PREPARED"
    trace_id = workflow.get_span_context().trace_id
    [headers] = participant_server.received
    assert int(headers["traceparent"].split("-")[1], 16) == trace_id

    finished = EXPORTER.get_finished_spans()
    [client] = [span for span in finished if span.kind == SpanKind.CLIENT]
    [server] = [span for span in finished if span.kind == SpanKind.SERVER]
    assert client.context.trace_id == server.context.trace_id == trace_id
    assert client.parent.span_id == workflow.get_span_context().span_id
    assert server.parent.span_id == client.context.span_id
    assert server.attributes["http.route"] == "/v1/transactions/{transaction_id}/prepare"
    # One span per request, not one per ASGI message.
    assert not [span for span in finished if span.name.endswith((" http send", " http receive"))]


@pytest.mark.parametrize(("service", "module"), SERVICES.items())
def test_every_service_continues_the_callers_trace(service, module):
    client = TestClient(importlib.import_module(module).app)

    client.get("/no/such/route", headers={"traceparent": CALLER})

    servers = [span for span in EXPORTER.get_finished_spans() if span.kind == SpanKind.SERVER]
    assert servers, f"{service} created no server span"
    assert all(format(span.context.trace_id, "032x") == CALLER_TRACE for span in servers)


@pytest.mark.parametrize("module", SERVICES.values())
def test_health_checks_are_not_traced(module):
    client = TestClient(importlib.import_module(module).app)

    assert client.get("/health").status_code == 200

    assert [span for span in EXPORTER.get_finished_spans() if span.kind == SpanKind.SERVER] == []
