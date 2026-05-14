from __future__ import annotations

import asyncio
import hashlib
import time as monotonic_time
import uuid
from contextvars import ContextVar
from typing import Any

from src.config import config_constant as constants
from src.feeds.ibkr_feed import IBKRHistoricalPacingGuard
from src.feeds.models import OHLCVRequest


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


async def _resolve_raw_redis(redis_client: Any) -> Any:
    if hasattr(redis_client, "raw_client"):
        return await redis_client.raw_client()
    return redis_client


def _stable_hash(parts: tuple[str, ...]) -> str:
    payload = "|".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:32]
