from __future__ import annotations

from typing import Protocol

from src.feeds.models import OHLCVBar, OHLCVRequest


class HistoricalOHLCVFeed(Protocol):
    async def load_historical_ohlcv(self, request: OHLCVRequest) -> list[OHLCVBar]:
        ...


class OHLCVLoader:
    """Orchestrates feed loading with optional persistence and latest-bar caching."""

    def __init__(self, feed: HistoricalOHLCVFeed, *, questdb: object | None = None, redis: object | None = None) -> None:
        self.feed = feed
        self.questdb = questdb
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

        if persist and bars and self.questdb is not None:
            await self.questdb.insert_bars(bars)

        if cache_latest and bars and self.redis is not None:
            await self.redis.set_latest_bar(bars[-1])

        return bars
