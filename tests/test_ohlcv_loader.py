from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.feeds.ohlcv_loader import OHLCVLoader


class FakeFeed:
    async def load_historical_ohlcv(self, request: OHLCVRequest) -> list[OHLCVBar]:
        return [
            OHLCVBar(
                symbol=request.symbol,
                asset_class=request.asset_class,
                exchange=request.exchange,
                currency=request.currency,
                timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
                open=100,
                high=101,
                low=99,
                close=100.5,
                volume=1000,
                bar_size=request.bar_size,
            )
        ]


class FakeStore:
    def __init__(self) -> None:
        self.batches: list[list[OHLCVBar]] = []

    async def insert_bars(self, bars: list[OHLCVBar]) -> int:
        self.batches.append(bars)
        return len(bars)


def test_ohlcv_loader_persists_to_generic_store() -> None:
    async def run() -> None:
        store = FakeStore()
        loader = OHLCVLoader(FakeFeed(), store=store)
        request = OHLCVRequest(symbol="SPY", asset_class=AssetClass.EQUITY)

        bars = await loader.load(request, persist=True, cache_latest=False)

        assert bars[0].symbol == "SPY"
        assert store.batches == [bars]

    asyncio.run(run())
