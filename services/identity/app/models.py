"""API models for the Identity Service (LM-052).

Request models forbid unknown fields, so no credential field can be smuggled
into a request: a sign-in needs an email address and nothing else.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LoginRequest(StrictModel):
    # Generous upper bound for the raw input; the validator enforces 254.
    email: str = Field(min_length=1, max_length=320)


class LoginRequestAccepted(StrictModel):
    status: Literal["accepted"] = "accepted"
    message: str


class VerifyRequest(StrictModel):
    token: str = Field(min_length=32, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")


class UserView(StrictModel):
    id: UUID
    email: str
    role: Literal["user", "operator"]


class SessionView(StrictModel):
    expires_at: datetime
    idle_expires_at: datetime


class VerifyResponse(StrictModel):
    user: UserView
    session: SessionView


class CurrentUser(StrictModel):
    id: UUID
    email: str
    role: Literal["user", "operator"]
    email_verified_at: datetime | None
    session: SessionView


class SessionIntrospection(StrictModel):
    active: Literal[True] = True
    user_id: UUID
    role: Literal["user", "operator"]
    expires_at: datetime
    idle_expires_at: datetime


class AuthEventView(StrictModel):
    id: int
    occurred_at: datetime
    event_type: Literal[
        "login_requested", "login_succeeded", "login_failed", "logout", "rate_limited"
    ]
    reason: str | None
    user_id: UUID | None
    session_id: UUID | None
    email_masked: str | None


class Problem(StrictModel):
    type: str
    title: str
    status: int = Field(ge=400, le=599)
    detail: str
    # Names of the request fields that failed validation; never their values.
    invalid_fields: list[str] | None = None
