"""Minimal rewards and redemption web interface (LM-011).

Serves a static page and a small backend-for-frontend under ``/api``. The
browser only ever talks to this app, which calls the Wallet, Catalog and
Coordinator APIs on its behalf, so:

- service addresses come from configuration (``WALLET_URL``, ``CATALOG_URL``,
  ``COORDINATOR_URL``), not from the page;
- there is no cross-origin setup;
- every error reaching the browser is a short, safe message. A service's
  4xx problem keeps its title and detail; a 5xx, timeout or connection
  failure becomes "<service> is unavailable"; anything unexpected becomes a
  generic error. Stack traces and internal details are logged, never sent.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path as FilePath
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

import httpx
from fastapi import Depends, FastAPI, Header, Path, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

SERVICE_NAME = "web"
STATIC = FilePath(__file__).parent / "static"
TIMEOUT_SECONDS = float(os.environ.get("WEB_UPSTREAM_TIMEOUT_SECONDS", "5"))

logger = logging.getLogger(__name__)

app = FastAPI(title="LedgerMesh Web", version="0.1.0")
app.mount("/static", StaticFiles(directory=STATIC), name="static")


# --- Errors the browser may see ---------------------------------------------------


class UserFacingError(Exception):
    """An error whose message is safe to show to the user."""

    def __init__(self, status: int, title: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.title = title
        self.detail = detail


def _error(status: int, title: str, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"title": title, "detail": detail})


@app.exception_handler(UserFacingError)
async def user_facing_error(_request: Request, error: UserFacingError) -> JSONResponse:
    return _error(error.status, error.title, error.detail)


@app.exception_handler(RequestValidationError)
async def invalid_request(_request: Request, error: RequestValidationError) -> JSONResponse:
    fields = sorted(
        {".".join(str(part) for part in e["loc"][1:]) or "request" for e in error.errors()}
    )
    return _error(422, "Invalid request", f"Please check: {', '.join(fields)}.")


@app.exception_handler(Exception)
async def unexpected_error(_request: Request, error: Exception) -> JSONResponse:
    # Logged with its traceback for operators; the browser gets no internals.
    logger.exception("unexpected error in the web interface")
    return _error(500, "Something went wrong", "Please try again in a moment.")


# --- Upstream services ---------------------------------------------------------------


@dataclass(frozen=True)
class Upstream:
    name: str
    client: httpx.Client

    def call(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = self.client.request(method, path, **kwargs)
        except httpx.HTTPError as error:
            logger.warning("%s unreachable for %s %s: %r", self.name, method, path, error)
            raise UserFacingError(
                503, f"{self.name} is unavailable", "Please try again in a moment."
            ) from error
        if response.status_code >= 500:
            logger.warning(
                "%s returned %s for %s %s", self.name, response.status_code, method, path
            )
            raise UserFacingError(
                502, f"{self.name} is unavailable", "Please try again in a moment."
            )
        if response.status_code >= 400:
            raise self._problem(response)
        return response.json()

    def _problem(self, response: httpx.Response) -> UserFacingError:
        """Pass on a service's own 4xx problem title and detail, nothing else."""
        try:
            body = response.json()
        except ValueError:
            body = {}
        title = body.get("title") if isinstance(body, dict) else None
        detail = body.get("detail") if isinstance(body, dict) else None
        if not isinstance(title, str) or not isinstance(detail, str):
            title, detail = "Request was rejected", f"{self.name} could not complete the request."
        return UserFacingError(response.status_code, title, detail)


@dataclass(frozen=True)
class Upstreams:
    wallet: Upstream
    catalog: Upstream
    coordinator: Upstream


@lru_cache
def get_upstreams() -> Upstreams:
    def upstream(name: str, variable: str, default: str) -> Upstream:
        base_url = os.environ.get(variable, default)
        return Upstream(name, httpx.Client(base_url=base_url, timeout=TIMEOUT_SECONDS))

    return Upstreams(
        wallet=upstream("Wallet", "WALLET_URL", "http://localhost:8001"),
        catalog=upstream("Catalog", "CATALOG_URL", "http://localhost:8002"),
        coordinator=upstream("Coordinator", "COORDINATOR_URL", "http://localhost:8000"),
    )


Services = Annotated[Upstreams, Depends(get_upstreams)]


# --- Pages ------------------------------------------------------------------------------


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


# --- API for the page ---------------------------------------------------------------------


class RedeemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str = Field(min_length=1, max_length=128)
    reward_id: str = Field(min_length=1, max_length=128)
    quantity: int = Field(ge=1, le=100)
    coordination_mode: Literal["two_phase_commit", "saga"]


UserId = Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,128}$")]


@app.get("/api/users/{user_id}/wallet")
def wallet(user_id: UserId, services: Services) -> Any:
    # The pattern keeps the id a single, plain path segment (no "..", no "/").
    return services.wallet.call("GET", f"/v1/wallets/{user_id}")


@app.get("/api/rewards")
def rewards(services: Services) -> Any:
    return services.catalog.call("GET", "/v1/rewards")


@app.post("/api/redemptions", status_code=202)
def redeem(
    request: RedeemRequest,
    services: Services,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key", max_length=128)] = None,
) -> Any:
    # The page sends one key per click, so a retried submit is not a second redemption.
    return services.coordinator.call(
        "POST",
        "/v1/redemptions",
        json=request.model_dump(),
        headers={"Idempotency-Key": idempotency_key or str(uuid4())},
    )


@app.get("/api/redemptions/{transaction_id}")
def redemption(transaction_id: UUID, services: Services) -> dict[str, Any]:
    """The redemption's current status and its full transition timeline."""
    current = services.coordinator.call("GET", f"/v1/redemptions/{transaction_id}")
    timeline = services.coordinator.call(
        "GET", f"/internal/v1/redemptions/{transaction_id}/transitions"
    )
    return {"redemption": current, "timeline": timeline}
