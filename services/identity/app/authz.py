"""Guest, user and operator authorization for FastAPI routes (LM-052).

``get_principal`` never fails: a request without a valid session is a guest.
Routes that need a signed-in person depend on ``require_user``; privileged
routes depend on ``require_operator``. Operators are the normalized addresses
in ``IDENTITY_OPERATOR_EMAILS``; the role is recomputed on every request.

No other LedgerMesh service requires authentication yet, so the guest demo is
unchanged. Other services (or the web BFF) learn the principal by calling
``GET /v1/sessions/introspect`` with the caller's cookie, never by reading the
identity database (ADR-001 section 2).
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from fastapi import Depends, Request

from services.identity.app.errors import Forbidden, Unauthenticated
from services.identity.app.mailer import SmtpMailer
from services.identity.app.repository import PostgresIdentityRepository, get_engine
from services.identity.app.service import IdentityService, Principal
from services.identity.app.settings import get_settings


@lru_cache
def get_identity_service() -> IdentityService:
    settings = get_settings()
    return IdentityService(PostgresIdentityRepository(get_engine()), settings, SmtpMailer(settings))


IdentityServiceDependency = Annotated[IdentityService, Depends(get_identity_service)]


def client_ip(request: Request) -> str:
    # Uvicorn rewrites this from X-Forwarded-For only for FORWARDED_ALLOW_IPS.
    return request.client.host if request.client else "unknown"


def session_cookie(request: Request, service: IdentityService) -> str | None:
    return request.cookies.get(service.settings.session_cookie_name)


def get_principal(request: Request, service: IdentityServiceDependency) -> Principal:
    return service.resolve(session_cookie(request, service))


PrincipalDependency = Annotated[Principal, Depends(get_principal)]


def require_user(principal: PrincipalDependency) -> Principal:
    if not principal.is_authenticated:
        raise Unauthenticated()
    return principal


def require_operator(principal: PrincipalDependency) -> Principal:
    if not principal.is_authenticated:
        raise Unauthenticated()
    if principal.kind != "operator":
        raise Forbidden()
    return principal


UserDependency = Annotated[Principal, Depends(require_user)]
OperatorDependency = Annotated[Principal, Depends(require_operator)]
