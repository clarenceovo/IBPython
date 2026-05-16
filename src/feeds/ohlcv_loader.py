from __future__ import annotations

import logging
from typing import Protocol

from src.feeds.models import OHLCVBar, OHLCVRequest
from src.transport.market_data_store import MarketOHLCVStore

logger = logging.getLogger(__name__)


class HistoricalOHLCVFeed(Protocol):
    async def load_historical_ohlcv(self, request: OHLCVRequest) -> list[OHLCVBar]:
        ...


class OHLCVLoader:
    """Orchestrates feed loading with optional persistence and latest-bar caching."""

    def __init__(
        self,
        feed: HistoricalOHLCVFeed,
        *,
        store: MarketOHLCVStore | None = None,
        questdb: MarketOHLCVStore | None = None,
        redis: object | None = None,
    ) -> None:
        self.feed = feed
        self.store = store or questdb
        self.questdb = self.store
        self.redis = redis

    async def load(
        self,
        request: OHLCVRequest,
        *,
        persist: bool = True,
        cache_latest: bool = True,
    ) -> list[OHLCVBar]:
        bars = await self.feed.load_historical_ohlcv(request)
        bars.sort(key=lambda bar: bar.timestamp)

        if persist and bars and self.store is not None:
            try:
                await self.store.insert_bars(bars)
            except Exception:
                logger.exception(
                    "QuestDB insert failed for %s %s bars; skipping Redis cache to avoid stale data",
                    request.symbol,
                    request.bar_size,
                )
                # DB write failed — do NOT cache to Redis, return bars anyway
                return bars

        if cache_latest and bars and self.redis is not None:
            await self.redis.set_latest_bar(bars[-1])

        return bars

    async def persist_bars(self, bars: list[OHLCVBar]) -> None:
        """Persist a list of bars to the configured OHLCV store."""
        if bars and self.store is not None:
            await self.store.insert_bars(bars)

    async def cache_latest_bar(self, bar: OHLCVBar) -> None:
        """Cache the latest bar to Redis."""
        if self.redis is not None:
            await self.redis.set_latest_bar(bar)


# ------------------------------------------------------------------
# Bar count estimation
# ------------------------------------------------------------------

# Mapping from bar_size string to approximate duration in seconds.
_BAR_SIZE_SECONDS: dict[str, float] = {
    "1 sec": 1,
    "5 secs": 5,
    "10 secs": 10,
    "15 secs": 15,
    "30 secs": 30,
    "1 min": 60,
    "2 mins": 120,
    "3 mins": 180,
    "5 mins": 300,
    "10 mins": 600,
    "15 mins": 900,
    "20 mins": 1200,
    "30 mins": 1800,
    "1 hour": 3600,
    "2 hours": 7200,
    "3 hours": 10800,
    "4 hours": 14400,
    "8 hours": 28800,
    "1 day": 86400,
    "1 week": 86400 * 7,
    "1 month": 86400 * 30,
}


def estimate_expected_bars(duration_str: str, bar_size_str: str) -> int:
    """Estimate the expected number of bars for a given duration and bar size.

    Parameters
    ----------
    duration_str : str
        IBKR duration string, e.g. "1 D", "3600 S", "18 M".
    bar_size_str : str
        IBKR bar size string, e.g. "1 min", "5 mins", "1 hour".

    Returns
    -------
    int
        Estimated bar count (rounded up). Returns 0 if inputs cannot be parsed.
    """
    duration_seconds = _parse_duration_to_seconds(duration_str)
    bar_seconds = _parse_bar_size_to_seconds(bar_size_str)
    if duration_seconds <= 0 or bar_seconds <= 0:
        return 0
    import math
    return math.ceil(duration_seconds / bar_seconds)


def _parse_duration_to_seconds(duration_str: str) -> float:
    """Convert an IBKR duration string to seconds."""
    parts = duration_str.strip().split()
    if len(parts) != 2:
        return 0.0
    try:
        amount = float(parts[0])
    except ValueError:
        return 0.0
    unit = parts[1].upper()
    if unit == "S":
        return amount
    if unit == "D":
        return amount * 86400
    if unit == "W":
        return amount * 86400 * 7
    if unit == "M":
        return amount * 86400 * 30
    if unit == "Y":
        return amount * 86400 * 365
    return 0.0


def _parse_bar_size_to_seconds(bar_size_str: str) -> float:
    """Convert a bar size string to seconds."""
    normalized = bar_size_str.strip().lower()
    # Direct lookup
    if normalized in _BAR_SIZE_SECONDS:
        return _BAR_SIZE_SECONDS[normalized]
    # Try normalizing "min" -> "mins" and vice versa
    for key, value in _BAR_SIZE_SECONDS.items():
        if key.rstrip("s") == normalized.rstrip("s"):
            return value
    return 0.0
