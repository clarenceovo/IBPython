"""Stable, machine-readable error envelope for the REST API.

Every 4xx/5xx response body shares one shape::

    {
        "detail": "<human-readable message>",   # legacy field, unchanged text
        "code": "<STABLE_CODE>",                # machine-readable classification
        "retryable": false,                     # retrying the same request may succeed
        "retry_after_seconds": 60               # advised wait, when applicable
    }

Agents branch on ``code``/``retryable`` instead of parsing English prose. The
``detail`` field keeps its historical text so existing clients keep working.
"""

from __future__ import annotations

from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException

#: Statuses where an identical retry can succeed once the condition clears.
RETRYABLE_STATUS: frozenset[int] = frozenset({408, 425, 429, 502, 503, 504})

#: Default fallback codes per HTTP status.
_STATUS_CODES: dict[int, str] = {
    400: "BAD_REQUEST",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    405: "METHOD_NOT_ALLOWED",
    408: "REQUEST_TIMEOUT",
    409: "CONFLICT",
    410: "FEATURE_REMOVED",
    413: "PAYLOAD_TOO_LARGE",
    422: "UNPROCESSABLE_ENTITY",
    429: "RATE_LIMITED",
    500: "INTERNAL_ERROR",
    501: "NOT_IMPLEMENTED",
    502: "UPSTREAM_ERROR",
    503: "SERVICE_UNAVAILABLE",
    504: "GATEWAY_TIMEOUT",
}


class ApiError(BaseModel):
    """Error body returned by every failing operation (see ``responses.default``)."""

    detail: str = Field(description="Human-readable error message (legacy field, stable text).")
    code: str = Field(description="Stable machine-readable error code, e.g. ``IBKR_PACING``.")
    retryable: bool = Field(
        default=False,
        description="True when retrying the same request later can succeed.",
    )
    retry_after_seconds: int | None = Field(
        default=None,
        description="Advised wait in seconds before retrying (429 and transient 5xx).",
    )


def classify_error(status_code: int, detail: str = "") -> str:
    """Derive a stable error code from the HTTP status and message text."""

    text = (detail or "").lower()
    if status_code == 429:
        return "IBKR_PACING" if ("pacing" in text or "ibkr" in text) else "RATE_LIMITED"
    if status_code in (502, 503):
        for needle, code in (
            ("questdb", "QUESTDB_UNAVAILABLE"),
            ("redis", "REDIS_UNAVAILABLE"),
            ("ibkr", "IBKR_UNAVAILABLE"),
        ):
            if needle in text:
                return code
    if status_code == 422 and any(
        word in text for word in ("contract", "symbol", "qualif", "resolv", "historical")
    ):
        return "CONTRACT_RESOLUTION_FAILED"
    if status_code == 403 and ("order" in text or "disabled" in text):
        return "ORDERS_DISABLED"
    return _STATUS_CODES.get(status_code, f"HTTP_{status_code}")


def error_response(
    status_code: int,
    detail: str,
    *,
    code: str | None = None,
    retryable: bool | None = None,
    retry_after_seconds: int | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """Build the envelope ``JSONResponse`` shared by all error paths."""

    resolved_code = code or classify_error(status_code, detail)
    resolved_retryable = retryable if retryable is not None else status_code in RETRYABLE_STATUS
    body = {
        "detail": detail,
        "code": resolved_code,
        "retryable": resolved_retryable,
    }
    if retry_after_seconds is not None:
        body["retry_after_seconds"] = retry_after_seconds
    response_headers = dict(headers or {})
    if status_code == 429 and retry_after_seconds is not None and "Retry-After" not in response_headers:
        response_headers["Retry-After"] = str(retry_after_seconds)
    return JSONResponse(status_code=status_code, content=body, headers=response_headers or None)


def http_exception_response(exc: HTTPException) -> JSONResponse:
    """Envelope for plain ``HTTPException`` raises (keeps any headers set by the route)."""

    detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
    retry_after: int | None = None
    headers = dict(getattr(exc, "headers", None) or {})
    retry_header = headers.get("Retry-After")
    if retry_header is not None:
        try:
            retry_after = max(1, int(retry_header))
        except ValueError:
            retry_after = None
    if retry_after is None and exc.status_code == 429:
        retry_after = 60
    return error_response(
        exc.status_code,
        detail,
        retry_after_seconds=retry_after,
        headers=headers or None,
    )
