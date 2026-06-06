"""ASGI middleware that rejects oversized request bodies.

Returns 413 Content Too Large when Content-Length exceeds the configured
limit.  Requests without Content-Length are allowed through (streaming
endpoints, SSE, etc.).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send


class RequestBodyLimitMiddleware:
    """Pure-ASGI middleware that enforces Content-Length limits."""

    def __init__(self, app: ASGIApp, *, max_bytes: int = 10 * 1024 * 1024) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        content_length = headers.get(b"content-length")
        if content_length is not None:
            try:
                length = int(content_length)
            except (ValueError, TypeError):
                await self._send_413(send)
                return
            if length > self.max_bytes:
                await self._send_413(send)
                return

        await self.app(scope, receive, send)

    @staticmethod
    async def _send_413(send: Callable[[dict[str, Any]], Awaitable[None]]) -> None:
        import json as _json
        body = _json.dumps({"detail": "request body too large"}).encode("utf-8")
        await send({
            "type": "http.response.start",
            "status": 413,
            "headers": [
                [b"content-type", b"application/json"],
                [b"content-length", str(len(body)).encode()],
            ],
        })
        await send({"type": "http.response.body", "body": body})
