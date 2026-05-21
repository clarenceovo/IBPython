"""Correlation ID middleware for request tracing.

Generates or propagates an X-Correlation-ID header for every request,
stores it in a contextvars.ContextVar, and injects it into log records
via a logging Filter.

Uses pure ASGI (no BaseHTTPMiddleware) to avoid the request-body
consuming wrapper that Starlette's BaseHTTPMiddleware introduces.
"""

from __future__ import annotations

import contextvars
import logging
import uuid
from collections.abc import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

HEADER_NAME = "X-Correlation-ID"

# ContextVar so downstream code can access the correlation ID without
# threading the request object through every function.
correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id",
    default="",
)


class CorrelationIdFilter(logging.Filter):
    """Logging filter that adds correlation_id to every log record.

    Only injects a non-empty correlation_id when one is actually set
    (i.e. inside an HTTP request scope).  Outside of requests the
    ContextVar defaults to "" which is left as-is.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id.get("")  # type: ignore[attr-defined]
        return True


class CorrelationIdMiddleware:
    """Pure-ASGI middleware that ensures every HTTP request has a correlation ID.

    If the incoming request has an ``X-Correlation-ID`` header, its value is
    reused.  Otherwise a new UUID4 is generated.  The correlation ID is:

    * Set on the ``correlation_id`` ContextVar for the duration of the request.
    * Echoed back as the ``X-Correlation-ID`` response header.

    Implemented as raw ASGI (no BaseHTTPMiddleware) to avoid the
    request-body consuming issues that Starlette's base class introduces.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        # Extract or generate correlation ID
        headers = dict(scope.get("headers", []))
        raw_cid = headers.get(b"x-correlation-id", b"").decode("utf-8", errors="replace")
        cid = raw_cid or uuid.uuid4().hex

        token = correlation_id.set(cid)

        header_encoded = HEADER_NAME.encode("utf-8")
        cid_encoded = cid.encode("utf-8")

        async def send_with_header(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append([header_encoded, cid_encoded])
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            correlation_id.reset(token)
