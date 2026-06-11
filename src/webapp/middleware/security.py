"""Security response headers and HTTP rate-limiting middleware."""

from __future__ import annotations

import json
import logging
import time
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


# ---------------------------------------------------------------------------
# In-process token-bucket rate limiter (fallback when Redis is unavailable)
# ---------------------------------------------------------------------------

class _TokenBucket:
    """Simple token bucket for a single key."""

    __slots__ = ("tokens", "max_tokens", "refill_rate", "last_refill")

    def __init__(self, max_tokens: int, refill_rate: float) -> None:
        self.max_tokens = max_tokens
        self.tokens = float(max_tokens)
        self.refill_rate = refill_rate  # tokens per second
        self.last_refill = time.monotonic()

    def consume(self) -> bool:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class RateLimiterMiddleware:
    """Per-IP HTTP rate limiter with optional Redis backend.

    Falls back to an in-process token-bucket when Redis is unavailable.

    Configuration is passed via constructor arguments rather than imported
    from settings, so the middleware is decoupled from the config layer.
    """

    _SKIP_PATHS = frozenset({
        "/docs",
        "/redoc",
        "/openapi.json",
        "/api/v1/openapi.json",
        "/metrics",
    })

    def __init__(
        self,
        app: Any,
        *,
        redis_url: str = "",
        redis_password: str = "",
        rate_limit_per_minute: int = 120,
    ) -> None:
        self.app = app
        self._rate_limit_per_minute = rate_limit_per_minute
        self._redis_url = redis_url
        self._redis_password = redis_password
        self._redis: Any = None  # lazy-init async Redis client
        self._buckets: dict[str, _TokenBucket] = {}
        self._refill_rate = rate_limit_per_minute / 60.0  # tokens per second

    # -- IP extraction -------------------------------------------------------

    @staticmethod
    def _get_client_ip(scope: dict[str, Any]) -> str:
        headers = dict(scope.get("headers", []))
        # X-Forwarded-For: client, proxy1, proxy2
        xff = headers.get(b"x-forwarded-for")
        if xff:
            ip = xff.decode("latin-1").split(",")[0].strip()
            if ip:
                return ip
        xri = headers.get(b"x-real-ip")
        if xri:
            ip = xri.decode("latin-1").strip()
            if ip:
                return ip
        # Direct ASGI client
        client = scope.get("client")
        if client:
            return client[0]
        return "unknown"

    # -- Redis helpers -------------------------------------------------------

    async def _get_redis(self) -> Any:
        if self._redis is not None:
            return self._redis
        if not self._redis_url:
            return None
        try:
            from redis import asyncio as redis_async

            self._redis = redis_async.from_url(
                self._redis_url,
                decode_responses=True,
                password=self._redis_password or None,
                socket_timeout=2.0,
                socket_connect_timeout=2.0,
            )
            await self._redis.ping()
            return self._redis
        except Exception:
            logger.debug("Redis unavailable for rate limiter, falling back to in-process bucket")
            self._redis = None
            return None

    async def _redis_allow(self, ip: str) -> bool:
        redis = await self._get_redis()
        if redis is None:
            return self._in_process_allow(ip)
        key = f"RateLimit:ip:{ip}"
        try:
            count = await redis.incr(key)
            if count == 1:
                await redis.expire(key, 60)
            return count <= self._rate_limit_per_minute
        except Exception:
            logger.debug("Redis rate-limit check failed, falling back to in-process")
            return self._in_process_allow(ip)

    def _in_process_allow(self, ip: str) -> bool:
        bucket = self._buckets.get(ip)
        if bucket is None:
            bucket = _TokenBucket(self._rate_limit_per_minute, self._refill_rate)
            self._buckets[ip] = bucket
        return bucket.consume()

    # -- 429 response --------------------------------------------------------

    @staticmethod
    async def _send_429(send: Callable[[dict[str, Any]], Awaitable[None]]) -> None:
        body = json.dumps({"detail": "Rate limit exceeded"}).encode("utf-8")
        await send({
            "type": "http.response.start",
            "status": 429,
            "headers": [
                [b"content-type", b"application/json"],
                [b"retry-after", b"60"],
                [b"content-length", str(len(body)).encode()],
            ],
        })
        await send({"type": "http.response.body", "body": body})

    # -- ASGI entrypoint -----------------------------------------------------

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        # Strip query string just in case, though ASGI path shouldn't have it
        if "?" in path:
            path = path.split("?", 1)[0]

        if path in self._SKIP_PATHS:
            await self.app(scope, receive, send)
            return

        ip = self._get_client_ip(scope)
        allowed = await self._redis_allow(ip)
        if not allowed:
            await self._send_429(send)
            return

        await self.app(scope, receive, send)
