"""ASGI middleware that rejects oversized request bodies.

Returns 413 Content Too Large (using the standard ``ApiError`` envelope) both
when ``Content-Length`` exceeds the configured limit and when a streamed /
chunked request body exceeds ``max_bytes`` across ``http.request`` chunks.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send


class _BodyTooLargeError(Exception):
    """Raised internally when streamed request chunks exceed max_bytes."""


class RequestBodyLimitMiddleware:
    """Pure-ASGI middleware that enforces Content-Length and streaming body limits."""

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

        bytes_seen = 0
        response_started = False

        async def _limited_receive() -> Message:
            nonlocal bytes_seen
            message = await receive()
            if message["type"] == "http.request":
                bytes_seen += len(message.get("body", b""))
                if bytes_seen > self.max_bytes:
                    raise _BodyTooLargeError
            return message

        async def _tracking_send(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, _limited_receive, _tracking_send)
        except _BodyTooLargeError:
            if not response_started:
                await self._send_413(send)

    @staticmethod
    async def _send_413(send: Callable[[dict[str, Any]], Awaitable[None]]) -> None:
        body = json.dumps({
            "detail": "request body too large",
            "code": "PAYLOAD_TOO_LARGE",
            "retryable": False,
        }).encode("utf-8")
        await send({
            "type": "http.response.start",
            "status": 413,
            "headers": [
                [b"content-type", b"application/json"],
                [b"content-length", str(len(body)).encode()],
            ],
        })
        await send({"type": "http.response.body", "body": body})
