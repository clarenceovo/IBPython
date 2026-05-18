from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import time as monotonic_time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from src.config import config_constant as constants
from src.feeds.ibkr_feed import IBKRHistoricalPacingGuard
from src.feeds.models import OHLCVRequest

logger = logging.getLogger(__name__)


REDIS_HISTORICAL_PACING_LUA = """
local window_key = KEYS[1]
local identical_key = KEYS[2]
local same_contract_key = KEYS[3]

local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local max_requests = tonumber(ARGV[3])
local weight = tonumber(ARGV[4])
local same_contract_ms = tonumber(ARGV[5])
local same_contract_max = tonumber(ARGV[6])
local identical_ms = tonumber(ARGV[7])
local member_prefix = ARGV[8]

redis.call('ZREMRANGEBYSCORE', window_key, 0, now_ms - window_ms)
redis.call('ZREMRANGEBYSCORE', same_contract_key, 0, now_ms - same_contract_ms)

local wait_ms = 0
local window_count = redis.call('ZCARD', window_key)
if window_count + weight > max_requests then
  local oldest = redis.call('ZRANGE', window_key, 0, 0, 'WITHSCORES')
  if oldest[2] then
    wait_ms = math.max(wait_ms, window_ms - (now_ms - tonumber(oldest[2])))
  end
end

local identical_ttl = redis.call('PTTL', identical_key)
if identical_ttl > 0 then
  wait_ms = math.max(wait_ms, identical_ttl)
end

local same_contract_count = redis.call('ZCARD', same_contract_key)
if same_contract_count >= same_contract_max then
  local oldest_same = redis.call('ZRANGE', same_contract_key, 0, 0, 'WITHSCORES')
  if oldest_same[2] then
    wait_ms = math.max(wait_ms, same_contract_ms - (now_ms - tonumber(oldest_same[2])))
  end
end

if wait_ms <= 0 then
  for i = 1, weight do
    redis.call('ZADD', window_key, now_ms, member_prefix .. ':w:' .. i)
  end
  redis.call('PEXPIRE', window_key, window_ms * 2)
  redis.call('SET', identical_key, now_ms, 'PX', identical_ms)
  redis.call('ZADD', same_contract_key, now_ms, member_prefix .. ':s')
  redis.call('PEXPIRE', same_contract_key, same_contract_ms * 2)
end

return wait_ms
"""

REDIS_GLOBAL_REQUEST_LUA = """
local window_key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local max_requests = tonumber(ARGV[3])
local weight = tonumber(ARGV[4])
local member_prefix = ARGV[5]

redis.call('ZREMRANGEBYSCORE', window_key, 0, now_ms - window_ms)
local count = redis.call('ZCARD', window_key)
if count + weight > max_requests then
  local oldest = redis.call('ZRANGE', window_key, 0, 0, 'WITHSCORES')
  if oldest[2] then
    return math.max(1, window_ms - (now_ms - tonumber(oldest[2])))
  end
  return window_ms
end

for i = 1, weight do
  redis.call('ZADD', window_key, now_ms, member_prefix .. ':' .. i)
end
redis.call('PEXPIRE', window_key, window_ms * 2)
return 0
"""

REDIS_MARKET_DATA_LEASE_LUA = """
local lease_key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local max_active = tonumber(ARGV[2])
local lease_id = ARGV[3]
local expires_at_ms = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', lease_key, 0, now_ms)
local active = redis.call('ZCARD', lease_key)
if active >= max_active then
  local oldest = redis.call('ZRANGE', lease_key, 0, 0, 'WITHSCORES')
  if oldest[2] then
    return math.max(1, tonumber(oldest[2]) - now_ms)
  end
  return 1000
end

redis.call('ZADD', lease_key, expires_at_ms, lease_id)
redis.call('PEXPIRE', lease_key, math.max(1000, expires_at_ms - now_ms) * 2)
return 0
"""

REDIS_RATE_LIMIT_SNAPSHOT_LUA = """
local global_key = KEYS[1]
local lease_key = KEYS[2]
local now_ms = tonumber(ARGV[1])
local global_window_ms = tonumber(ARGV[2])

redis.call('ZREMRANGEBYSCORE', global_key, 0, now_ms - global_window_ms)
redis.call('ZREMRANGEBYSCORE', lease_key, 0, now_ms)
return {redis.call('ZCARD', global_key), redis.call('ZCARD', lease_key)}
"""


@dataclass
class IBKRMarketDataLease:
    lease_id: str
    contract_key: str
    operation: str
    controller: "IBKRRateLimitController"
    released: bool = False

    async def release(self) -> None:
        if self.released:
            return
        self.released = True
        await self.controller.release_market_data_line(self)


@dataclass
class IBKRRateLimitStats:
    waits_by_operation: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    calls_by_operation: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    fallback_errors: int = 0
    redis_errors: int = 0

    def record(self, operation: str, wait_seconds: float) -> None:
        self.calls_by_operation[operation] += 1
        if wait_seconds > 0:
            self.waits_by_operation[operation] += wait_seconds


class RedisIBKRHistoricalPacingGuard:
    """Distributed IBKR historical-data pacing guard backed by Redis bookmarks."""

    def __init__(
        self,
        redis_client: Any,
        *,
        key_prefix: str = constants.REDIS_IBKR_RATE_LIMIT_PREFIX,
        max_requests_per_window: int = constants.IBKR_HISTORICAL_MAX_REQUESTS_PER_WINDOW,
        request_window_seconds: float = constants.IBKR_HISTORICAL_REQUEST_WINDOW_SECONDS,
        identical_cooldown_seconds: float = constants.IBKR_HISTORICAL_IDENTICAL_REQUEST_COOLDOWN_SECONDS,
        same_contract_window_seconds: float = constants.IBKR_HISTORICAL_SAME_CONTRACT_WINDOW_SECONDS,
        same_contract_max_requests: int = constants.IBKR_HISTORICAL_SAME_CONTRACT_MAX_REQUESTS,
        max_concurrent_requests: int = constants.IBKR_CONSERVATIVE_HISTORICAL_CONCURRENCY,
        fallback: IBKRHistoricalPacingGuard | None = None,
    ) -> None:
        self.redis_client = redis_client
        self.key_prefix = key_prefix
        self.max_requests_per_window = max_requests_per_window
        self.request_window_seconds = request_window_seconds
        self.identical_cooldown_seconds = identical_cooldown_seconds
        self.same_contract_window_seconds = same_contract_window_seconds
        self.same_contract_max_requests = same_contract_max_requests
        self._concurrency = asyncio.Semaphore(max_concurrent_requests)
        self._acquire_mode: ContextVar[str | None] = ContextVar("ibkr_rate_limit_acquire_mode", default=None)
        self._fallback = fallback or IBKRHistoricalPacingGuard(
            max_requests_per_window=max_requests_per_window,
            request_window_seconds=request_window_seconds,
            identical_cooldown_seconds=identical_cooldown_seconds,
            same_contract_window_seconds=same_contract_window_seconds,
            same_contract_max_requests=same_contract_max_requests,
            max_concurrent_requests=max_concurrent_requests,
        )
    async def acquire(self, request: OHLCVRequest) -> None:
        await self._concurrency.acquire()
        try:
            await self._wait_for_redis_slot(request)
            self._acquire_mode.set("redis")
        except Exception:
            self._concurrency.release()
            await self._fallback.acquire(request)
            self._acquire_mode.set("fallback")

    def release(self) -> None:
        mode = self._acquire_mode.get()
        if mode == "fallback":
            self._fallback.release()
        elif mode == "redis":
            self._concurrency.release()
        self._acquire_mode.set(None)

    async def _wait_for_redis_slot(self, request: OHLCVRequest) -> None:
        raw_redis = await _resolve_raw_redis(self.redis_client)
        keys = self._keys_for_request(request)
        weight = 2 if request.what_to_show.upper() == "BID_ASK" else 1

        while True:
            now_ms = int(monotonic_time.time() * 1000)
            member_prefix = f"{now_ms}:{uuid.uuid4().hex}"
            wait_ms = await raw_redis.eval(
                REDIS_HISTORICAL_PACING_LUA,
                3,
                keys["window"],
                keys["identical"],
                keys["same_contract"],
                now_ms,
                int(self.request_window_seconds * 1000),
                self.max_requests_per_window,
                weight,
                int(self.same_contract_window_seconds * 1000),
                self.same_contract_max_requests,
                int(self.identical_cooldown_seconds * 1000),
                member_prefix,
            )
            wait_ms = int(wait_ms)
            if wait_ms <= 0:
                return
            await asyncio.sleep(wait_ms / 1000)

    def _keys_for_request(self, request: OHLCVRequest) -> dict[str, str]:
        identical_hash = _stable_hash(
            (
                request.symbol,
                str(request.asset_class),
                request.exchange,
                request.currency,
                str(request.start_datetime),
                str(request.end_datetime),
                request.duration,
                request.bar_size,
                request.what_to_show.upper(),
                str(request.use_rth),
            )
        )
        same_contract_hash = _stable_hash(
            (
                request.symbol,
                str(request.asset_class),
                request.exchange,
                request.what_to_show.upper(),
            )
        )
        return {
            "window": f"{self.key_prefix}:window",
            "identical": f"{self.key_prefix}:identical:{identical_hash}",
            "same_contract": f"{self.key_prefix}:same_contract:{same_contract_hash}",
        }


class _LocalGlobalRequestLimiter:
    def __init__(self, *, max_requests_per_second: int) -> None:
        self.max_requests_per_second = max_requests_per_second
        self._request_times: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def wait(self, *, weight: int = 1) -> float:
        total_wait = 0.0
        while True:
            async with self._lock:
                now = monotonic_time.monotonic()
                while self._request_times and now - self._request_times[0] >= 1.0:
                    self._request_times.popleft()
                if len(self._request_times) + weight <= self.max_requests_per_second:
                    for _ in range(weight):
                        self._request_times.append(now)
                    return total_wait
                wait_seconds = max(0.001, 1.0 - (now - self._request_times[0]))
            total_wait += wait_seconds
            await asyncio.sleep(wait_seconds)

    def snapshot_count(self) -> int:
        now = monotonic_time.monotonic()
        while self._request_times and now - self._request_times[0] >= 1.0:
            self._request_times.popleft()
        return len(self._request_times)


class _LocalMarketDataLineLimiter:
    def __init__(self, *, max_active_lines: int) -> None:
        self.max_active_lines = max_active_lines
        self._leases: dict[str, tuple[float, str, str]] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, *, lease_id: str, contract_key: str, operation: str, ttl_seconds: float) -> float:
        total_wait = 0.0
        while True:
            async with self._lock:
                now = monotonic_time.monotonic()
                self._prune(now)
                if len(self._leases) < self.max_active_lines:
                    self._leases[lease_id] = (now + ttl_seconds, contract_key, operation)
                    return total_wait
                earliest_expiry = min(expires_at for expires_at, _, _ in self._leases.values())
                wait_seconds = max(0.05, earliest_expiry - now)
            total_wait += wait_seconds
            await asyncio.sleep(wait_seconds)

    async def release(self, lease_id: str) -> None:
        async with self._lock:
            self._leases.pop(lease_id, None)

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            self._prune(monotonic_time.monotonic())
            return {
                "active_market_data_lines": len(self._leases),
                "active_market_data_lease_ids": sorted(self._leases),
            }

    def _prune(self, now: float) -> None:
        for lease_id, (expires_at, _, _) in list(self._leases.items()):
            if expires_at <= now:
                del self._leases[lease_id]


class IBKRRateLimitController:
    """App-wide IBKR pacing controller with Redis coordination and local fallback."""

    def __init__(
        self,
        redis_client: Any | None = None,
        *,
        enabled: bool = constants.DEFAULT_IBKR_RATE_LIMIT_ENABLED,
        key_prefix: str = constants.REDIS_IBKR_APP_RATE_LIMIT_PREFIX,
        historical_key_prefix: str = constants.REDIS_IBKR_RATE_LIMIT_PREFIX,
        global_messages_per_second: int = constants.DEFAULT_IBKR_RATE_LIMIT_GLOBAL_MESSAGES_PER_SECOND,
        market_data_lines: int = constants.DEFAULT_IBKR_MARKET_DATA_LINES,
        market_data_line_reserve: int | None = None,
        market_data_lease_ttl_seconds: float = constants.DEFAULT_IBKR_RATE_LIMIT_MARKET_DATA_LEASE_TTL_SECONDS,
        pacing_guard: Any | None = None,
        log_wait_threshold_seconds: float = 1.0,
    ) -> None:
        self.redis_client = redis_client
        self.enabled = enabled
        self.key_prefix = key_prefix.rstrip(":")
        self.global_messages_per_second = max(1, int(global_messages_per_second))
        self.market_data_lines = max(1, int(market_data_lines))
        reserve = market_data_line_reserve
        if reserve is None:
            reserve = max(5, math.ceil(self.market_data_lines * 0.10))
        self.market_data_line_reserve = max(0, int(reserve))
        self.max_active_market_data_lines = max(1, self.market_data_lines - self.market_data_line_reserve)
        self.market_data_lease_ttl_seconds = market_data_lease_ttl_seconds
        self.log_wait_threshold_seconds = log_wait_threshold_seconds
        self.stats = IBKRRateLimitStats()
        self._global_fallback = _LocalGlobalRequestLimiter(
            max_requests_per_second=self.global_messages_per_second,
        )
        self._market_data_fallback = _LocalMarketDataLineLimiter(
            max_active_lines=self.max_active_market_data_lines,
        )
        if pacing_guard is not None:
            self.pacing_guard = pacing_guard
        elif redis_client is not None:
            self.pacing_guard = RedisIBKRHistoricalPacingGuard(
                redis_client,
                key_prefix=historical_key_prefix,
                fallback=IBKRHistoricalPacingGuard(),
            )
        else:
            self.pacing_guard = IBKRHistoricalPacingGuard()

    async def wait_for_request(self, *, operation: str, weight: int = 1) -> None:
        if not self.enabled:
            return
        weight = max(1, int(weight))
        start = monotonic_time.monotonic()
        if self.redis_client is None:
            wait_seconds = await self._global_fallback.wait(weight=weight)
        else:
            try:
                wait_seconds = await self._wait_for_redis_global_slot(weight=weight)
            except Exception:
                self.stats.redis_errors += 1
                logger.warning("IBKR rate limiter Redis unavailable; using local global limiter", exc_info=True)
                wait_seconds = await self._global_fallback.wait(weight=weight)
        elapsed = monotonic_time.monotonic() - start
        self.stats.record(operation, max(wait_seconds, elapsed if elapsed >= 0.001 else 0.0))
        if elapsed >= self.log_wait_threshold_seconds:
            logger.warning("IBKR rate limiter waited %.2fs operation=%s weight=%d", elapsed, operation, weight)

    async def acquire_market_data_line(
        self,
        *,
        contract_key: str,
        operation: str,
        ttl_seconds: float | None = None,
    ) -> IBKRMarketDataLease:
        if not self.enabled:
            return IBKRMarketDataLease(uuid.uuid4().hex, contract_key, operation, self, released=False)
        ttl = ttl_seconds or self.market_data_lease_ttl_seconds
        lease = IBKRMarketDataLease(
            lease_id=f"{operation}:{_stable_hash((contract_key, uuid.uuid4().hex))}",
            contract_key=contract_key,
            operation=operation,
            controller=self,
        )
        start = monotonic_time.monotonic()
        if self.redis_client is None:
            await self._market_data_fallback.acquire(
                lease_id=lease.lease_id,
                contract_key=contract_key,
                operation=operation,
                ttl_seconds=ttl,
            )
        else:
            try:
                await self._wait_for_redis_market_data_lease(lease, ttl_seconds=ttl)
            except Exception:
                self.stats.redis_errors += 1
                logger.warning("IBKR rate limiter Redis unavailable; using local market-data lease", exc_info=True)
                await self._market_data_fallback.acquire(
                    lease_id=lease.lease_id,
                    contract_key=contract_key,
                    operation=operation,
                    ttl_seconds=ttl,
                )
        elapsed = monotonic_time.monotonic() - start
        self.stats.record(f"{operation}:market_data_line", elapsed)
        if elapsed >= self.log_wait_threshold_seconds:
            logger.warning(
                "IBKR market-data line lease waited %.2fs operation=%s contract_key=%s",
                elapsed,
                operation,
                contract_key,
            )
        return lease

    async def release_market_data_line(self, lease: IBKRMarketDataLease) -> None:
        if not self.enabled:
            return
        if self.redis_client is None:
            await self._market_data_fallback.release(lease.lease_id)
            return
        try:
            raw_redis = await _resolve_raw_redis(self.redis_client)
            await raw_redis.zrem(self._market_data_lease_key, lease.lease_id)
        except Exception:
            self.stats.redis_errors += 1
            await self._market_data_fallback.release(lease.lease_id)

    @asynccontextmanager
    async def market_data_line(
        self,
        *,
        contract_key: str,
        operation: str,
        ttl_seconds: float | None = None,
    ) -> Any:
        lease = await self.acquire_market_data_line(
            contract_key=contract_key,
            operation=operation,
            ttl_seconds=ttl_seconds,
        )
        try:
            yield lease
        finally:
            await lease.release()

    async def snapshot(self) -> dict[str, Any]:
        global_count = self._global_fallback.snapshot_count()
        local_market_data = await self._market_data_fallback.snapshot()
        redis_available = self.redis_client is not None
        active_market_data_lines = local_market_data["active_market_data_lines"]
        if self.redis_client is not None:
            try:
                raw_redis = await _resolve_raw_redis(self.redis_client)
                now_ms = int(monotonic_time.time() * 1000)
                result = await raw_redis.eval(
                    REDIS_RATE_LIMIT_SNAPSHOT_LUA,
                    2,
                    self._global_window_key,
                    self._market_data_lease_key,
                    now_ms,
                    1000,
                )
                global_count = int(result[0])
                active_market_data_lines = int(result[1])
            except Exception:
                redis_available = False
                self.stats.redis_errors += 1
        return {
            "enabled": self.enabled,
            "redis_backed": self.redis_client is not None,
            "redis_available": redis_available,
            "global_messages_per_second": self.global_messages_per_second,
            "global_messages_in_current_second": global_count,
            "market_data_lines": self.market_data_lines,
            "market_data_line_reserve": self.market_data_line_reserve,
            "max_active_market_data_lines": self.max_active_market_data_lines,
            "active_market_data_lines": active_market_data_lines,
            "market_data_lease_ttl_seconds": self.market_data_lease_ttl_seconds,
            "calls_by_operation": dict(self.stats.calls_by_operation),
            "wait_seconds_by_operation": {
                operation: round(wait, 6)
                for operation, wait in self.stats.waits_by_operation.items()
                if wait > 0
            },
            "redis_errors": self.stats.redis_errors,
        }

    async def _wait_for_redis_global_slot(self, *, weight: int) -> float:
        raw_redis = await _resolve_raw_redis(self.redis_client)
        total_wait = 0.0
        while True:
            now_ms = int(monotonic_time.time() * 1000)
            wait_ms = await raw_redis.eval(
                REDIS_GLOBAL_REQUEST_LUA,
                1,
                self._global_window_key,
                now_ms,
                1000,
                self.global_messages_per_second,
                weight,
                f"{now_ms}:{uuid.uuid4().hex}",
            )
            wait_ms = int(wait_ms)
            if wait_ms <= 0:
                return total_wait
            wait_seconds = wait_ms / 1000
            total_wait += wait_seconds
            await asyncio.sleep(wait_seconds)

    async def _wait_for_redis_market_data_lease(
        self,
        lease: IBKRMarketDataLease,
        *,
        ttl_seconds: float,
    ) -> None:
        raw_redis = await _resolve_raw_redis(self.redis_client)
        while True:
            now_ms = int(monotonic_time.time() * 1000)
            wait_ms = await raw_redis.eval(
                REDIS_MARKET_DATA_LEASE_LUA,
                1,
                self._market_data_lease_key,
                now_ms,
                self.max_active_market_data_lines,
                lease.lease_id,
                now_ms + int(ttl_seconds * 1000),
            )
            wait_ms = int(wait_ms)
            if wait_ms <= 0:
                return
            await asyncio.sleep(wait_ms / 1000)

    @property
    def _global_window_key(self) -> str:
        return f"{self.key_prefix}:global:window"

    @property
    def _market_data_lease_key(self) -> str:
        return f"{self.key_prefix}:market_data:leases"


async def _resolve_raw_redis(redis_client: Any) -> Any:
    if hasattr(redis_client, "raw_client"):
        return await redis_client.raw_client()
    return redis_client


def _stable_hash(parts: tuple[str, ...]) -> str:
    payload = "|".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:32]
