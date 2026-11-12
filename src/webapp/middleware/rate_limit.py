"""Gatekeeper: per-IP HTTP rate limiting with escalating penalty blocks.

Decision per request:

1. A blocked IP is denied fast (no window counting) with ``Retry-After``
   set to the seconds left on its block.
2. Otherwise the request counts against a 60-second fixed window. Under
   the limit it is allowed and carries ``X-RateLimit-*`` response headers.
3. Over the limit is a strike: the IP is blocked and every strike within
   the strike window escalates the block (60s, 5m, 15m). Strikes decay
   with the strike window, so an IP that behaves returns to normal.

State lives in Redis when available (shared across workers) and falls
back to in-process dicts (single worker). Counters for the Matrix
snapshot are kept locally in both modes.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from ipaddress import ip_address, ip_network
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

_BLOCK_LADDER_SECONDS = (60, 300, 900)
_STRIKE_WINDOW_SECONDS = 600
_WINDOW_SECONDS = 60


@dataclass
class _Decision:
    allowed: bool
    retry_after: int
    limit: int
    remaining: int
    reset: int


class RateLimitGatekeeper:
    """Allow/deny state machine for one rate limit, keyed by client IP."""

    def __init__(self, rate_limit_per_minute: int = 120) -> None:
        self.limit_per_minute = rate_limit_per_minute
        self._redis: Any = None
        self._redis_url = ""
        self._redis_password = ""
        # In-process fallback state (monotonic deadlines).
        self._windows: dict[str, tuple[int, float]] = {}
        self._strikes: dict[str, tuple[int, float]] = {}
        self._blocks: dict[str, float] = {}
        # Local counters in both modes (Matrix snapshot).
        self.allowed_total = 0
        self.denied_total = 0
        self.blocked_total = 0

    # -- Redis client --------------------------------------------------------

    def configure_redis(self, redis_url: str, redis_password: str = "") -> None:
        self._redis_url = redis_url
        self._redis_password = redis_password

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def _get_redis(self) -> Any:
        if self._redis is not None:
            return self._redis
        if not self._redis_url:
            return None
        try:
            from redis import asyncio as redis_async

            client = redis_async.from_url(
                self._redis_url,
                decode_responses=True,
                password=self._redis_password or None,
                socket_timeout=2.0,
                socket_connect_timeout=2.0,
            )
            await client.ping()
            self._redis = client
            return client
        except Exception:
            logger.debug("Redis unavailable for rate limiter, falling back to in-process state")
            return None

    # -- Decision ------------------------------------------------------------

    async def check(self, ip: str) -> _Decision:
        redis = await self._get_redis()
        if redis is not None:
            try:
                return await self._check_redis(redis, ip)
            except Exception:
                logger.debug("Redis rate-limit check failed, falling back to in-process")
        return self._check_local(ip)

    async def _check_redis(self, redis: Any, ip: str) -> _Decision:
        block_key = f"RateLimit:block:{ip}"
        if await redis.exists(block_key):
            self.denied_total += 1
            retry_after = max(1, await redis.ttl(block_key))
            return _Decision(False, retry_after, self.limit_per_minute, 0, retry_after)

        window_key = f"RateLimit:ip:{ip}"
        count = await redis.incr(window_key)
        if count == 1:
            await redis.expire(window_key, _WINDOW_SECONDS)
        reset = max(1, await redis.ttl(window_key))
        if reset < 0:  # orphaned key (crash between INCR/EXPIRE): heal it
            await redis.expire(window_key, _WINDOW_SECONDS)
            reset = _WINDOW_SECONDS
        if count <= self.limit_per_minute:
            self.allowed_total += 1
            return _Decision(True, 0, self.limit_per_minute, self.limit_per_minute - count, reset)

        return await self._strike_redis(redis, ip, block_key)

    async def _strike_redis(self, redis: Any, ip: str, block_key: str) -> _Decision:
        strikes_key = f"RateLimit:strikes:{ip}"
        strikes = await redis.incr(strikes_key)
        if strikes == 1:
            await redis.expire(strikes_key, _STRIKE_WINDOW_SECONDS)
        elif await redis.ttl(strikes_key) < 0:  # orphaned strikes key: heal
            await redis.expire(strikes_key, _STRIKE_WINDOW_SECONDS)
        block_for = _BLOCK_LADDER_SECONDS[min(strikes, len(_BLOCK_LADDER_SECONDS)) - 1]
        await redis.set(block_key, "1", ex=block_for)
        logger.warning("gatekeeper: ip=%s blocked %ds (limit=%d/min strikes=%d)", ip, block_for, self.limit_per_minute, strikes)
        self.denied_total += 1
        self.blocked_total += 1
        return _Decision(False, block_for, self.limit_per_minute, 0, block_for)

    def _check_local(self, ip: str) -> _Decision:
        now = time.monotonic()

        unblock_at = self._blocks.get(ip)
        if unblock_at is not None:
            if now < unblock_at:
                self.denied_total += 1
                return _Decision(False, int(unblock_at - now) + 1, self.limit_per_minute, 0, int(unblock_at - now) + 1)
            self._blocks.pop(ip, None)

        window = self._windows.get(ip)
        if window is None or now >= window[1]:
            self._windows[ip] = (1, now + _WINDOW_SECONDS)
            count, reset = 1, _WINDOW_SECONDS
        else:
            count = window[0] + 1
            self._windows[ip] = (count, window[1])
            reset = int(window[1] - now) + 1

        if count <= self.limit_per_minute:
            self.allowed_total += 1
            return _Decision(True, 0, self.limit_per_minute, self.limit_per_minute - count, reset)

        strikes, strike_end = self._strikes.get(ip, (0, 0.0))
        if now >= strike_end:
            strikes = 0
        strikes += 1
        self._strikes[ip] = (strikes, now + _STRIKE_WINDOW_SECONDS)

        block_for = _BLOCK_LADDER_SECONDS[min(strikes, len(_BLOCK_LADDER_SECONDS)) - 1]
        self._blocks[ip] = now + block_for
        logger.warning("gatekeeper: ip=%s blocked %ds (limit=%d/min strikes=%d)", ip, block_for, self.limit_per_minute, strikes)
        self.denied_total += 1
        self.blocked_total += 1
        return _Decision(False, block_for, self.limit_per_minute, 0, block_for)

    # -- Introspection -------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        return {
            "limit_per_minute": self.limit_per_minute,
            "mode": "redis" if self._redis is not None else "in-process",
            "blocked_ips": {
                ip: int(deadline - now) + 1
                for ip, deadline in self._blocks.items()
                if now < deadline
            },
            "tracked_ips": len(self._windows),
            "allowed_total": self.allowed_total,
            "denied_total": self.denied_total,
            "blocked_total": self.blocked_total,
        }


class RateLimiterMiddleware:
    """ASGI middleware that gates HTTP requests through the gatekeeper."""

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
        gatekeeper: RateLimitGatekeeper | None = None,
        redis_url: str = "",
        redis_password: str = "",
        rate_limit_per_minute: int = 120,
        trusted_proxies: str = "",
    ) -> None:
        self.app = app
        self.gatekeeper = gatekeeper or RateLimitGatekeeper(rate_limit_per_minute)
        if gatekeeper is None:
            self.gatekeeper.configure_redis(redis_url, redis_password)
        self._trusted_proxy_networks = self._parse_trusted_proxies(trusted_proxies)

    # -- IP extraction (honors X-Forwarded-For from trusted proxies only) ----

    @staticmethod
    def _parse_trusted_proxies(value: str) -> tuple[Any, ...]:
        networks = []
        for item in value.split(","):
            text = item.strip()
            if not text:
                continue
            try:
                networks.append(ip_network(text, strict=False))
            except ValueError:
                logger.warning("Ignoring invalid trusted proxy entry: %s", text)
        return tuple(networks)

    def _is_trusted_proxy(self, peer_ip: str) -> bool:
        if not self._trusted_proxy_networks:
            return False
        try:
            parsed = ip_address(peer_ip)
        except ValueError:
            return False
        return any(parsed in network for network in self._trusted_proxy_networks)

    def _get_client_ip(self, scope: dict[str, Any]) -> str:
        client = scope.get("client")
        peer_ip = client[0] if client else "unknown"
        if not self._is_trusted_proxy(peer_ip):
            return peer_ip

        headers = dict(scope.get("headers", []))
        # X-Forwarded-For: client, proxy1, proxy2 — the rightmost entry was
        # appended by our own trusted proxy; the leftmost is client-controlled
        # and spoofable, and unvalidated bytes must never become key material.
        xff = headers.get(b"x-forwarded-for")
        if xff:
            for candidate in reversed(xff.decode("latin-1").split(",")):
                ip = candidate.strip()
                try:
                    ip_address(ip)
                except ValueError:
                    continue
                return ip
        xri = headers.get(b"x-real-ip")
        if xri:
            ip = xri.decode("latin-1").strip()
            if ip:
                return ip
        return peer_ip

    # -- Responses -----------------------------------------------------------

    @staticmethod
    async def _send_429(
        send: Callable[[dict[str, Any]], Awaitable[None]],
        decision: _Decision,
    ) -> None:
        body = json.dumps({"detail": "Rate limit exceeded", "retry_after": decision.retry_after}).encode("utf-8")
        await send({
            "type": "http.response.start",
            "status": 429,
            "headers": [
                [b"content-type", b"application/json"],
                [b"retry-after", str(decision.retry_after).encode()],
                [b"x-ratelimit-limit", str(decision.limit).encode()],
                [b"x-ratelimit-remaining", b"0"],
                [b"x-ratelimit-reset", str(decision.reset).encode()],
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
        if "?" in path:
            path = path.split("?", 1)[0]
        if path in self._SKIP_PATHS:
            await self.app(scope, receive, send)
            return

        ip = self._get_client_ip(scope)
        decision = await self.gatekeeper.check(ip)
        if not decision.allowed:
            await self._send_429(send, decision)
            return

        async def _send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append([b"x-ratelimit-limit", str(decision.limit).encode()])
                headers.append([b"x-ratelimit-remaining", str(max(0, decision.remaining)).encode()])
                headers.append([b"x-ratelimit-reset", str(decision.reset).encode()])
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, _send)
