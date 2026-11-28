"""Identity domain errors and stable HTTP problem details (LM-052).

Every message is deliberately generic: it never says whether an account
exists, why a link failed, or which limit was hit.
"""


class IdentityError(Exception):
    status = 500
    title = "Identity operation failed"
    problem_type = "urn:ledgermesh:identity:error"
    default_detail = "The request could not be completed."

    def __init__(self, detail: str | None = None, *, headers: dict[str, str] | None = None):
        self.detail = detail or self.default_detail
        super().__init__(self.detail)
        self.headers = headers or {}


class InvalidEmail(IdentityError):
    status = 422
    title = "Invalid email address"
    problem_type = "urn:ledgermesh:identity:invalid-email"
    default_detail = "Enter a valid email address."


class InvalidRequest(IdentityError):
    status = 422
    title = "Invalid request"
    problem_type = "urn:ledgermesh:identity:invalid-request"
    default_detail = "The request body is not valid."


class InvalidOrExpiredLink(IdentityError):
    status = 400
    title = "Invalid or expired sign-in link"
    problem_type = "urn:ledgermesh:identity:invalid-or-expired-link"
    default_detail = "This sign-in link is invalid or has expired. Request a new one."


class RateLimited(IdentityError):
    status = 429
    title = "Too many requests"
    problem_type = "urn:ledgermesh:identity:rate-limited"
    default_detail = "Too many requests. Try again later."

    def __init__(self, retry_after: int) -> None:
        super().__init__(headers={"Retry-After": str(max(1, retry_after))})
        self.retry_after = retry_after


class Unauthenticated(IdentityError):
    status = 401
    title = "Authentication required"
    problem_type = "urn:ledgermesh:identity:unauthenticated"
    default_detail = "Sign in to continue."


class Forbidden(IdentityError):
    status = 403
    title = "Forbidden"
    problem_type = "urn:ledgermesh:identity:forbidden"
    default_detail = "You do not have access to this resource."


class UnsupportedMediaType(IdentityError):
    status = 415
    title = "Unsupported media type"
    problem_type = "urn:ledgermesh:identity:unsupported-media-type"
    default_detail = "Send the request with Content-Type: application/json."
