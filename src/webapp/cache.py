from __future__ import annotations

import asyncio
import json
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic import BaseModel

from src.transport.metrics import metrics

T = TypeVar("T")


@dataclass(frozen=True)
class CacheStats:
    size: int
    max_size: int
    default_ttl_seconds: float


@dataclass
class _CacheEntry:
    value: Any
    expires_at: float


class AsyncTTLCache:
    """Small async-safe in-process TTL cache for pacing-sensitive market data reads."""

    def __init__(self, *, ttl_seconds: float, max_size: int) -> None:
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be non-negative")
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self._entries: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._lock = asyncio.Lock()
        self._key_locks: dict[str, asyncio.Lock] = {}

    async def get(self, key: str) -> Any | None:
        async with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is None:
                metrics.cache_miss_total.inc({"cache_name": "market_data"})
                return None
            if entry.expires_at <= now:
                self._entries.pop(key, None)
                metrics.cache_miss_total.inc({"cache_name": "market_data"})
                return None
            self._entries.move_to_end(key)
            metrics.cache_hit_total.inc({"cache_name": "market_data"})
            return entry.value

    async def set(self, key: str, value: Any, *, ttl_seconds: float | None = None) -> None:
        ttl = self.ttl_seconds if ttl_seconds is None else ttl_seconds
        if ttl <= 0:
            return
        async with self._lock:
            # Only evict when at max capacity; remove oldest regardless of expiry.
            if len(self._entries) >= self.max_size and key not in self._entries:
                self._entries.popitem(last=False)
            self._entries[key] = _CacheEntry(value=value, expires_at=time.monotonic() + ttl)
            self._entries.move_to_end(key)

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[T]],
        *,
        ttl_seconds: float | None = None,
    ) -> T:
        cached = await self.get(key)
        if cached is not None:
            return cached
        async with await self._lock_for_key(key):
            cached = await self.get(key)
            if cached is not None:
                return cached
            value = await factory()
            await self.set(key, value, ttl_seconds=ttl_seconds)
            return value

    async def clear(self) -> None:
        async with self._lock:
            self._entries.clear()
            self._key_locks.clear()

    async def stats(self) -> CacheStats:
        async with self._lock:
            return CacheStats(
                size=len(self._entries),
                max_size=self.max_size,
                default_ttl_seconds=self.ttl_seconds,
            )

    async def prune(self) -> int:
        """Explicitly remove all expired entries. Returns the number pruned."""
        async with self._lock:
            now = time.monotonic()
            return self._prune_expired(now)

    def _prune_expired(self, now: float) -> int:
        pruned = 0
        for key in list(self._entries.keys()):
            if self._entries[key].expires_at <= now:
                self._entries.pop(key, None)
                pruned += 1
        return pruned

    async def _lock_for_key(self, key: str) -> asyncio.Lock:
        async with self._lock:
            return self._key_locks.setdefault(key, asyncio.Lock())


def stable_cache_key(namespace: str, payload: BaseModel | Mapping[str, Any] | Any) -> str:
    if isinstance(payload, BaseModel):
        data = payload.model_dump(mode="json")
    elif isinstance(payload, Mapping):
        data = dict(payload)
    else:
        data = payload
    encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return f"{namespace}:{encoded}"
