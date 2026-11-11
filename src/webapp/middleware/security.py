"""Security response headers middleware. Rate limiting lives in rate_limit.py."""

from __future__ import annotations

import logging
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)


class SecurityHeadersMiddleware:
    """Lightweight ASGI middleware that adds security headers to every HTTP response."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def _send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append([b"x-content-type-options", b"nosniff"])
                headers.append([b"x-frame-options", b"DENY"])
                headers.append([b"referrer-policy", b"strict-origin-when-cross-origin"])
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, _send)
