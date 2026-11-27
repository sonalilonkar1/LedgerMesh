from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import BackgroundTasks, Depends, FastAPI, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from ledgermesh_messaging import CorrelationMiddleware, configure_logging

from services.identity.app.authz import (
    IdentityServiceDependency,
    OperatorDependency,
    UserDependency,
    client_ip,
    get_identity_service,
    session_cookie,
)
from services.identity.app.errors import (
    IdentityError,
    InvalidEmail,
    InvalidRequest,
    UnsupportedMediaType,
)
from services.identity.app.models import (
    AuthEventView,
    CurrentUser,
    LoginRequest,
    LoginRequestAccepted,
    Problem,
    SessionIntrospection,
    SessionView,
    UserView,
    VerifyRequest,
    VerifyResponse,
)
from services.identity.app.repository import SessionRecord
from services.identity.app.service import IdentityService
from services.identity.app.settings import get_settings
from services.identity.app.verify_page import PAGE, PAGE_HEADERS

SERVICE_NAME = "identity"
configure_logging(SERVICE_NAME)  # LM-032: JSON logs with shared field names

__all__ = ["app", "get_identity_service", "get_settings"]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Refuse to start with an unsafe configuration (plain-HTTP links or SMTP
    # without TLS outside local/test) instead of failing on the first login.
    app.dependency_overrides.get(get_settings, get_settings)()
    yield


app = FastAPI(title="LedgerMesh Identity Service", version="0.1.0", lifespan=lifespan)
app.add_middleware(CorrelationMiddleware)


@app.middleware("http")
async def no_store(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/v1/"):
        response.headers["Cache-Control"] = "no-store"
    return response


def problem_response(error: IdentityError, invalid_fields: list[str] | None = None) -> JSONResponse:
    problem = Problem(
        type=error.problem_type,
        title=error.title,
        status=error.status,
        detail=error.detail,
        invalid_fields=invalid_fields,
    )
    return JSONResponse(
        status_code=error.status,
        content=problem.model_dump(mode="json", exclude_none=True),
        media_type="application/problem+json",
        headers=error.headers,
    )


@app.exception_handler(IdentityError)
async def identity_error_handler(_request: Request, error: IdentityError) -> JSONResponse:
    return problem_response(error)


UNEXPECTED_FIELD = "unexpected_field"


def field_name(item: dict) -> str:
    if item["type"] == "extra_forbidden":
        return UNEXPECTED_FIELD
    return ".".join(str(part) for part in item["loc"][1:]) or "body"


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, error: RequestValidationError) -> JSONResponse:
    # FastAPI's default 422 echoes the submitted value, which here would be a
    # token or an email address. Name the declared fields only; a key the
    # client invented is reported as a fixed marker, not reflected back.
    fields = sorted({field_name(item) for item in error.errors()})
    if request.url.path == "/v1/auth/login-requests" and fields == ["email"]:
        return problem_response(InvalidEmail())
    return problem_response(InvalidRequest(), invalid_fields=fields)


def require_json(request: Request) -> None:
    """Blocks cross-site form posts (login CSRF): they cannot send JSON without CORS."""
    content_type = request.headers.get("content-type", "")
    if content_type.split(";", 1)[0].strip().lower() != "application/json":
        raise UnsupportedMediaType()


JsonOnly = Depends(require_json)
PROBLEMS = {status: {"model": Problem} for status in (400, 401, 403, 415, 422, 429)}


def session_view(session: SessionRecord) -> SessionView:
    return SessionView(
        expires_at=session.absolute_expires_at, idle_expires_at=session.idle_expires_at
    )


def cookie_attributes(service: IdentityService) -> dict:
    return {
        "key": service.settings.session_cookie_name,
        "path": "/",
        "httponly": True,
        "samesite": "lax",
        "secure": service.settings.secure_cookies,
    }


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.post(
    "/v1/auth/login-requests",
    status_code=202,
    response_model=LoginRequestAccepted,
    responses={k: PROBLEMS[k] for k in (415, 422, 429)},
    dependencies=[JsonOnly],
    tags=["auth"],
)
def request_login_link(
    body: LoginRequest,
    request: Request,
    background: BackgroundTasks,
    service: IdentityServiceDependency,
) -> LoginRequestAccepted:
    pending = service.request_login(body.email, client_ip(request))
    background.add_task(service.send_login_email, pending)
    return LoginRequestAccepted(
        message=(
            "If this address can receive email, a sign-in link is on its way. "
            f"It expires in {service.token_ttl_minutes} minutes."
        )
    )


@app.get("/v1/auth/verify", response_class=HTMLResponse, tags=["auth"])
def login_confirm_page() -> HTMLResponse:
    return HTMLResponse(PAGE, headers=PAGE_HEADERS)


@app.post(
    "/v1/auth/verify",
    response_model=VerifyResponse,
    responses={k: PROBLEMS[k] for k in (400, 415, 422, 429)},
    dependencies=[JsonOnly],
    tags=["auth"],
)
def verify_login_link(
    body: VerifyRequest,
    request: Request,
    response: Response,
    service: IdentityServiceDependency,
) -> VerifyResponse:
    signed_in = service.verify(body.token, client_ip(request), session_cookie(request, service))
    session = signed_in.session
    max_age = int((session.absolute_expires_at - service.clock()).total_seconds())
    response.set_cookie(
        value=signed_in.session_secret, max_age=max_age, **cookie_attributes(service)
    )
    return VerifyResponse(
        user=UserView(id=session.user.id, email=session.user.email, role=signed_in.role),
        session=session_view(session),
    )


@app.post(
    "/v1/auth/logout",
    status_code=204,
    responses={415: PROBLEMS[415]},
    dependencies=[JsonOnly],
    tags=["auth"],
)
def logout(request: Request, service: IdentityServiceDependency) -> Response:
    service.logout(session_cookie(request, service), client_ip(request))
    response = Response(status_code=204)
    response.delete_cookie(**cookie_attributes(service))
    return response


@app.get(
    "/v1/me",
    response_model=CurrentUser,
    responses={401: PROBLEMS[401]},
    tags=["users"],
)
def current_user(principal: UserDependency) -> CurrentUser:
    session = principal.session
    return CurrentUser(
        id=principal.user_id,
        email=principal.email,
        role=principal.kind,
        email_verified_at=session.user.email_verified_at,
        session=session_view(session),
    )


@app.get(
    "/v1/sessions/introspect",
    response_model=SessionIntrospection,
    responses={401: PROBLEMS[401]},
    tags=["sessions"],
)
def introspect_session(principal: UserDependency) -> SessionIntrospection:
    session = principal.session
    return SessionIntrospection(
        user_id=principal.user_id,
        role=principal.kind,
        expires_at=session.absolute_expires_at,
        idle_expires_at=session.idle_expires_at,
    )


@app.get(
    "/v1/operator/auth-events",
    response_model=list[AuthEventView],
    responses={k: PROBLEMS[k] for k in (401, 403, 422)},
    tags=["operator"],
)
def list_auth_events(
    _operator: OperatorDependency,
    service: IdentityServiceDependency,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    before_id: Annotated[int | None, Query(ge=1)] = None,
) -> list[AuthEventView]:
    return [
        AuthEventView(
            id=event.id,
            occurred_at=event.occurred_at,
            event_type=event.event_type,
            reason=event.reason,
            user_id=event.user_id,
            session_id=event.session_id,
            email_masked=event.email_masked,
        )
        for event in service.list_events(limit=limit, before_id=before_id)
    ]
