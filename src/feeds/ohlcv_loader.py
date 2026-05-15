from __future__ import annotations

from typing import Protocol

from src.feeds.models import OHLCVBar, OHLCVRequest
from src.transport.market_data_store import MarketOHLCVStore


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
            await self.store.insert_bars(bars)

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
