"""Operational safeguards shared by every LedgerMesh HTTP service (LM-042).

One mechanism, used the same way by all four services::

    from ledgermesh_messaging.safeguards import install_safeguards

    app = FastAPI(...)
    app.add_middleware(CorrelationMiddleware)
    install_safeguards(app, SERVICE_NAME, database_url=database_url)

It provides:

- **Restricted surfaces.** Every ``/internal/...`` route and the generated API
  docs (``/docs``, ``/redoc``, ``/openapi.json``) answer exactly like an unknown
  route (404) unless ``LEDGERMESH_ENV`` is ``local`` or ``test``, or the request
  carries the operator token from ``LEDGERMESH_OPERATOR_TOKEN``. An unset or
  unknown ``LEDGERMESH_ENV`` is treated as a deployment: it fails closed.
- **No local defaults outside local/test.** ``require_safe_database_url``
  refuses a database URL that uses the Compose development password, and
  ``check_startup_configuration`` makes a service refuse to start when a
  deployment environment (``LEDGERMESH_ENV`` set to anything other than
  ``local`` or ``test``) still has one.
- **Safe errors.** Validation errors, HTTP errors and unhandled exceptions all
  return ``application/problem+json`` with only ``type``, ``title``, ``status``
  and ``detail``: no submitted input, no stack trace, no SQL. An unhandled error
  is logged with its traceback (credentials masked, see ``logs.py``) and a
  ``correlation_id`` that the response also carries.
- **Bounded request bodies** (``LEDGERMESH_MAX_REQUEST_BYTES``, 1 MiB default).

The middleware is plain ASGI and the FastAPI pieces are imported only inside
``install_safeguards``, so the library itself still does not depend on FastAPI.
Service-to-service participant routes (``/v1/transactions/{id}/prepare`` and
the like) are deliberately *not* restricted here: the Coordinator calls them in
every environment, and network placement protects them (see docs/security.md).
"""

import hashlib
import hmac
import json
import logging
import os
import re
import uuid
from collections.abc import Callable, Iterable
from http import HTTPStatus
from typing import Any

ENVIRONMENT_VARIABLE = "LEDGERMESH_ENV"
# The only environments where development conveniences are allowed. Everything
# else, including an unset variable, is treated as a deployment.
LOCAL_ENVIRONMENTS = frozenset({"local", "test"})

OPERATOR_TOKEN_VARIABLE = "LEDGERMESH_OPERATOR_TOKEN"
OPERATOR_TOKEN_HEADER = "X-LedgerMesh-Operator-Token"
OPERATOR_TOKEN_MIN_LENGTH = 32

MAX_REQUEST_BYTES_VARIABLE = "LEDGERMESH_MAX_REQUEST_BYTES"
DEFAULT_MAX_REQUEST_BYTES = 1024 * 1024

# The password infra/compose.yaml and .env.example use for local databases.
DEV_DATABASE_PASSWORD = "ledgermesh_dev"

INTERNAL_PATH_PREFIX = "/internal"

# Largest values PostgreSQL INTEGER and BIGINT columns accept. Request models use
# them as upper bounds, so an oversized number is a 422 instead of a database
# error (and a 500).
POSTGRES_INTEGER_MAX = 2**31 - 1
POSTGRES_BIGINT_MAX = 2**63 - 1

CORRELATION_HEADER = "X-Correlation-Id"
# A client-supplied correlation id is echoed back only if it looks like an id.
_SAFE_CORRELATION_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_MAX_DETAIL_LENGTH = 1000

logger = logging.getLogger(__name__)


class UnsafeConfigurationError(RuntimeError):
    """A deployment is configured with a local-only default or a missing secret."""


# ---- environment ------------------------------------------------------------


def current_environment() -> str | None:
    return os.environ.get(ENVIRONMENT_VARIABLE) or None


def is_local_environment() -> bool:
    """True only when ``LEDGERMESH_ENV`` is explicitly ``local`` or ``test``."""
    return current_environment() in LOCAL_ENVIRONMENTS


def _digest(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


def operator_token_matches(presented: str | None) -> bool:
    """Compare a presented operator token with the configured one in constant time.

    No token, or a configured token shorter than ``OPERATOR_TOKEN_MIN_LENGTH``,
    never matches. Both sides are hashed first, so the comparison does not
    reveal the configured token's length either.
    """
    configured = os.environ.get(OPERATOR_TOKEN_VARIABLE, "")
    if len(configured) < OPERATOR_TOKEN_MIN_LENGTH or not presented:
        return False
    return hmac.compare_digest(_digest(presented), _digest(configured))


def restricted_access_allowed(presented_token: str | None) -> bool:
    """May this request reach an internal route or the API docs?"""
    return is_local_environment() or operator_token_matches(presented_token)


# ---- configuration ----------------------------------------------------------


def require_safe_database_url(url: str, *, setting: str) -> str:
    """Return ``url``, unless outside local/test it has no real password.

    Outside local/test the URL must carry a non-empty password (in the userinfo
    or as a ``password`` query parameter) that is not the local development
    password. ``setting`` names the variable an operator should set; the URL
    itself never appears in the error, because it may contain a password.
    """
    if is_local_environment():
        return url
    from sqlalchemy.engine import make_url
    from sqlalchemy.exc import ArgumentError

    try:
        parsed = make_url(url)
    except ArgumentError:
        raise UnsafeConfigurationError(f"{setting} is not a valid database URL") from None
    query_password = parsed.query.get("password")
    if isinstance(query_password, tuple):
        query_password = query_password[-1] if query_password else None
    passwords = [value for value in (parsed.password, query_password) if value is not None]
    environment = current_environment()
    hint = " (or set LEDGERMESH_ENV=local for local development)" if environment is None else ""
    if not any(passwords):
        raise UnsafeConfigurationError(
            f"the database URL has no password; set {setting} to a URL with a real "
            f"secret{hint} (LEDGERMESH_ENV={environment!r})"
        )
    if DEV_DATABASE_PASSWORD in passwords:
        raise UnsafeConfigurationError(
            f"the database URL uses the local development password; set {setting} to a URL "
            f"with a real secret{hint} (LEDGERMESH_ENV={environment!r})"
        )
    return url


def check_startup_configuration(service: str, *, database_url: Callable[[], str] | None) -> None:
    """Refuse to start a deployment that still uses local-only defaults.

    - ``LEDGERMESH_ENV`` local or test: nothing to check.
    - unset: the service starts (an image must answer ``/health`` on its own,
      LM-047), but internal routes and docs stay closed and
      ``require_safe_database_url`` still refuses the development password on
      first use.
    - anything else (``staging``, ``production``, ...): every problem found
      raises ``UnsafeConfigurationError``, so the process exits at import.
    """
    environment = current_environment()
    if environment in LOCAL_ENVIRONMENTS:
        return
    if environment is None:
        logger.warning(
            "%s is not set: internal routes and API docs are disabled and the local "
            "development database password is refused; set %s=local for development",
            ENVIRONMENT_VARIABLE,
            ENVIRONMENT_VARIABLE,
        )
        return
    problems: list[str] = []
    token = os.environ.get(OPERATOR_TOKEN_VARIABLE)
    if token is not None and len(token) < OPERATOR_TOKEN_MIN_LENGTH:
        problems.append(
            f"{OPERATOR_TOKEN_VARIABLE} must be at least {OPERATOR_TOKEN_MIN_LENGTH} characters"
        )
    if database_url is not None:
        try:
            database_url()
        except UnsafeConfigurationError as error:
            problems.append(str(error))
    if problems:
        raise UnsafeConfigurationError(
            f"{service} refuses to start with {ENVIRONMENT_VARIABLE}={environment!r}: "
            + "; ".join(problems)
        )


def max_request_bytes() -> int:
    raw = os.environ.get(MAX_REQUEST_BYTES_VARIABLE)
    if raw is None:
        return DEFAULT_MAX_REQUEST_BYTES
    try:
        value = int(raw)
    except ValueError:
        raise UnsafeConfigurationError(f"{MAX_REQUEST_BYTES_VARIABLE} must be an integer") from None
    if value < 1:
        raise UnsafeConfigurationError(f"{MAX_REQUEST_BYTES_VARIABLE} must be positive")
    return value


# ---- problem responses --------------------------------------------------------


def problem_body(status: int, detail: str | None = None, problem_type: str = "about:blank") -> dict:
    """An RFC 9457 problem with only the fields every LedgerMesh contract allows."""
    title = HTTPStatus(status).phrase
    text = detail if isinstance(detail, str) and detail else title
    return {
        "type": problem_type,
        "title": title,
        "status": status,
        "detail": text[:_MAX_DETAIL_LENGTH],
    }


async def _send_problem(
    send: Any, status: int, detail: str | None = None, headers: Iterable[tuple[bytes, bytes]] = ()
) -> None:
    # Byte for byte what Starlette's JSONResponse sends (its ``render`` and
    # ``init_headers``), so a closed route cannot be told apart from an unknown
    # one by its body, Content-Length or header order.
    body = json.dumps(
        problem_body(status, detail),
        ensure_ascii=False,
        allow_nan=False,
        indent=None,
        separators=(",", ":"),
    ).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                *headers,
                (b"content-length", str(len(body)).encode()),
                (b"content-type", b"application/problem+json"),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


def correlation_id_for(headers: Any) -> str:
    """The caller's ``X-Correlation-Id`` if it looks like an id, else a new one."""
    supplied = headers.get(CORRELATION_HEADER) if headers is not None else None
    if supplied and _SAFE_CORRELATION_ID.match(supplied):
        return supplied
    return str(uuid.uuid4())


# ---- middleware -------------------------------------------------------------


class SafeguardMiddleware:
    """Plain ASGI: closes restricted paths and bounds the request body.

    A restricted path answers 404 with the same body as an unknown route, so a
    closed internal route is indistinguishable from one that does not exist.
    The body is read (up to the limit) before the app sees it; a larger body is
    rejected with 413 without reaching any handler.
    """

    def __init__(
        self,
        app: Any,
        *,
        restricted_paths: Iterable[str] = (),
        max_bytes: int | None = None,
    ) -> None:
        self.app = app
        self.restricted_paths = frozenset(restricted_paths)
        self.max_bytes = max_bytes or max_request_bytes()

    def _restricted(self, path: str) -> bool:
        # Starlette redirects "/docs/" to "/docs" (307), which would reveal that a
        # closed route exists, so trailing slashes are ignored here.
        trimmed = path.rstrip("/") or "/"
        return (
            trimmed == INTERNAL_PATH_PREFIX
            or path.startswith(INTERNAL_PATH_PREFIX + "/")
            or trimmed in self.restricted_paths
        )

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        headers = {
            name.decode("latin-1").lower(): value.decode("latin-1")
            for name, value in scope.get("headers") or ()
        }
        if self._restricted(scope.get("path", "")) and not restricted_access_allowed(
            headers.get(OPERATOR_TOKEN_HEADER.lower())
        ):
            await _send_problem(send, 404)
            return

        declared = headers.get("content-length")
        if declared is not None:
            if not declared.isdigit():
                await _send_problem(send, 400, "Invalid Content-Length header")
                return
            if int(declared) > self.max_bytes:
                await _send_problem(send, 413, f"Request body exceeds {self.max_bytes} bytes")
                return

        chunks: list[bytes] = []
        size = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                # The client went away before sending the whole body.
                return
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > self.max_bytes:
                await _send_problem(send, 413, f"Request body exceeds {self.max_bytes} bytes")
                return
            chunks.append(chunk)
            if not message.get("more_body", False):
                break

        replayed = False

        async def replay() -> dict:
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": b"".join(chunks), "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


# ---- FastAPI wiring ---------------------------------------------------------


def _validation_detail(errors: Iterable[dict]) -> str:
    """Field locations and messages only: never the submitted input or ``ctx``.

    For an unexpected field the last location element is the key the client
    sent, which is input too, so it is replaced with a placeholder.
    """
    parts = []
    for error in errors:
        loc = list(error.get("loc", ()))
        if error.get("type") == "extra_forbidden" and loc:
            loc[-1] = "<unexpected field>"
        location = ".".join(str(part) for part in loc)
        parts.append(f"{location}: {error.get('msg', 'invalid value')}")
    return "; ".join(parts) or "Invalid request"


def install_safeguards(
    app: Any, service: str, *, database_url: Callable[[], str] | None = None
) -> None:
    """Apply every LM-042 safeguard to a FastAPI ``app`` (call once, after construction)."""
    from fastapi.exceptions import RequestValidationError
    from fastapi.responses import JSONResponse
    from starlette.exceptions import HTTPException as StarletteHTTPException

    check_startup_configuration(service, database_url=database_url)

    docs_paths = {
        path
        for path in (
            app.openapi_url,
            app.docs_url,
            app.redoc_url,
            app.swagger_ui_oauth2_redirect_url,
        )
        if path
    }
    app.add_middleware(
        SafeguardMiddleware, restricted_paths=docs_paths, max_bytes=max_request_bytes()
    )

    def problem(status: int, detail: str | None, problem_type: str = "about:blank", **kwargs):
        return JSONResponse(
            status_code=status,
            content=problem_body(status, detail, problem_type),
            media_type="application/problem+json",
            **kwargs,
        )

    async def validation_error(_request: Any, error: RequestValidationError):
        return problem(
            422,
            _validation_detail(error.errors()),
            f"urn:ledgermesh:{service}:invalid-request",
        )

    async def http_error(_request: Any, error: StarletteHTTPException):
        detail = error.detail if isinstance(error.detail, str) else None
        return problem(error.status_code, detail, headers=getattr(error, "headers", None))

    async def unhandled_error(request: Any, error: Exception):
        correlation_id = correlation_id_for(request.headers)
        logger.error(
            "unhandled error in %s %s",
            request.method,
            request.url.path,
            exc_info=(type(error), error, error.__traceback__),
            extra={"correlation_id": correlation_id},
        )
        return problem(
            500,
            f"An unexpected error occurred. Quote correlation id {correlation_id} "
            "when reporting it.",
            f"urn:ledgermesh:{service}:internal-error",
            headers={CORRELATION_HEADER: correlation_id},
        )

    app.add_exception_handler(RequestValidationError, validation_error)
    app.add_exception_handler(StarletteHTTPException, http_error)
    app.add_exception_handler(Exception, unhandled_error)
