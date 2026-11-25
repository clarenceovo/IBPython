"""ASGI middleware that enforces a global request deadline.

If a request exceeds ``timeout_seconds`` the middleware returns 503 and
cancels the downstream handler.  This prevents a slow client or a stuck
dependency from occupying a worker indefinitely.

Health-check paths (/system/live, /system/health, /system/readiness) are
excluded so that the readiness probe itself never gets killed by its own
timeout.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)

# Paths that should never be timed out (health probes and long-lived SSE streams).
_EXEMPT_PREFIXES = (
    "/api/v1/system/live",
    "/api/v1/system/health",
    "/api/v1/system/readiness",
    "/metrics",
    "/llms.txt",
    "/api/v1/streaming/ticker",
    "/api/v1/account/pnl/account/stream",
    "/api/v1/account/pnl/position/stream",
    "/api/v1/realtime-bars/stream/",
)


class RequestTimeoutMiddleware:
    """Pure-ASGI middleware that enforces a per-request wall-clock deadline."""

    def __init__(self, app: ASGIApp, *, timeout_seconds: float = 60.0) -> None:
        self.app = app
        self.timeout_seconds = timeout_seconds

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if any(path.startswith(prefix) for prefix in _EXEMPT_PREFIXES):
            await self.app(scope, receive, send)
            return

        # Track whether response headers have already been sent so the timeout
        # path never emits a duplicate http.response.start (which crashes ASGI).
        timed_out = False
        response_started = False

        async def _send_wrapper(message: Message) -> None:
            nonlocal response_started
            if timed_out:
                return
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        async def _run_app() -> None:
            await self.app(scope, receive, _send_wrapper)

        try:
            await asyncio.wait_for(_run_app(), timeout=self.timeout_seconds)
        except asyncio.TimeoutError:
            timed_out = True
            logger.warning("request timed out after %.1fs: path=%s", self.timeout_seconds, path)
            if response_started:
                return
            body = b'{"detail":"request timed out","code":"SERVICE_UNAVAILABLE","retryable":true}'
            await send({
                "type": "http.response.start",
                "status": 503,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(body)).encode()],
                ],
            })
            await send({"type": "http.response.body", "body": body})
