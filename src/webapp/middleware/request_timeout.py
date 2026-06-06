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

# Paths that should never be timed out (health probes).
_EXEMPT_PREFIXES = ("/api/v1/system/live", "/api/v1/system/health", "/api/v1/system/readiness", "/metrics")


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

        # Wire a cancelled-error handler into send so the timeout path can
        # still emit a valid HTTP response instead of a broken connection.
        timed_out = False

        async def _send_wrapper(message: Message) -> None:
            if timed_out and message["type"] == "http.response.body":
                # The downstream handler is still writing — ignore.
                return
            await send(message)

        async def _run_app() -> None:
            await self.app(scope, receive, _send_wrapper)

        try:
            await asyncio.wait_for(_run_app(), timeout=self.timeout_seconds)
        except asyncio.TimeoutError:
            timed_out = True
            logger.warning("request timed out after %.1fs: path=%s", self.timeout_seconds, path)
            body = b'{"detail":"request timed out"}'
            await send({
                "type": "http.response.start",
                "status": 503,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(body)).encode()],
                ],
            })
            await send({"type": "http.response.body", "body": body})
