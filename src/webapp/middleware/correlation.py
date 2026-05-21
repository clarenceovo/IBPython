"""Correlation ID middleware for request tracing.

Generates or propagates an X-Correlation-ID header for every request,
stores it in a contextvars.ContextVar, and injects it into log records
via a logging Filter.
"""

from __future__ import annotations

import contextvars
import logging
import uuid
from collections.abc import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

HEADER_NAME = "X-Correlation-ID"

# ContextVar so downstream code can access the correlation ID without
# threading the request object through every function.
correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id",
    default="",
)


class CorrelationIdFilter(logging.Filter):
    """Logging filter that adds correlation_id to every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id.get("")  # type: ignore[attr-defined]
        return True


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Middleware that ensures every request has a correlation ID.

    If the incoming request has an ``X-Correlation-ID`` header, its value is
    reused.  Otherwise a new UUID4 is generated.  The correlation ID is:

    * Set on the ``correlation_id`` ContextVar for the duration of the request.
    * Echoed back as the ``X-Correlation-ID`` response header.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        cid = request.headers.get(HEADER_NAME) or uuid.uuid4().hex
        token = correlation_id.set(cid)
        try:
            response = await call_next(request)
            response.headers[HEADER_NAME] = cid
            return response
        finally:
            correlation_id.reset(token)
