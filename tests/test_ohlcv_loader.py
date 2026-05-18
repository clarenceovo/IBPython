from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from src.feeds.data_quality import validate_ohlcv_bars
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.feeds.ohlcv_loader import OHLCVLoader


class FakeFeed:
    def __init__(self, bars: list[OHLCVBar] | None = None) -> None:
        self.bars = bars

    async def load_historical_ohlcv(self, request: OHLCVRequest) -> list[OHLCVBar]:
        if self.bars is not None:
            return list(self.bars)
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
    def __init__(self, *, fail: bool = False) -> None:
        self.batches: list[list[OHLCVBar]] = []
        self.fail = fail

    async def insert_bars(self, bars: list[OHLCVBar]) -> int:
        if self.fail:
            raise RuntimeError("database unavailable")
        self.batches.append(bars)
        return len(bars)


class FakeRedis:
    def __init__(self) -> None:
        self.latest: list[OHLCVBar] = []

    async def set_latest_bar(self, bar: OHLCVBar) -> None:
        self.latest.append(bar)


def _bar(
    timestamp: datetime,
    *,
    symbol: str = "SPY",
    open_price: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.5,
    volume: float = 1000.0,
) -> OHLCVBar:
    return OHLCVBar(
        symbol=symbol,
        asset_class=AssetClass.EQUITY,
        exchange="SMART",
        currency="USD",
        timestamp=timestamp,
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        bar_size="1 min",
    )


def test_ohlcv_loader_persists_to_generic_store() -> None:
    async def run() -> None:
        store = FakeStore()
        loader = OHLCVLoader(FakeFeed(), store=store)
        request = OHLCVRequest(symbol="SPY", asset_class=AssetClass.EQUITY)

        bars = await loader.load(request, persist=True, cache_latest=False)

        assert bars[0].symbol == "SPY"
        assert store.batches == [bars]

    asyncio.run(run())


def test_ohlcv_loader_blocks_duplicate_contract_timestamps_before_persistence() -> None:
    async def run() -> None:
        timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
        store = FakeStore()
        redis = FakeRedis()
        loader = OHLCVLoader(FakeFeed([_bar(timestamp), _bar(timestamp)]), store=store, redis=redis)
        request = OHLCVRequest(symbol="SPY", asset_class=AssetClass.EQUITY)

        with pytest.raises(RuntimeError, match="duplicate_contract_timestamp"):
            await loader.load(request, persist=True, cache_latest=True)

        assert store.batches == []
        assert redis.latest == []
        assert loader.last_quality_report is not None
        assert loader.last_quality_report.has_fatal is True

    asyncio.run(run())


def test_ohlcv_loader_records_missing_interval_warning_and_persists() -> None:
    async def run() -> None:
        store = FakeStore()
        first = _bar(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc))
        second = _bar(datetime(2026, 1, 1, 0, 3, tzinfo=timezone.utc))
        loader = OHLCVLoader(FakeFeed([second, first]), store=store)
        request = OHLCVRequest(symbol="SPY", asset_class=AssetClass.EQUITY, bar_size="1 min")

        bars = await loader.load(request, persist=True, cache_latest=False)

        assert bars == [first, second]
        assert store.batches == [bars]
        assert loader.last_quality_report is not None
        assert [issue.code for issue in loader.last_quality_report.issues] == ["missing_interval_gap"]

    asyncio.run(run())


def test_ohlcv_loader_suppresses_cache_after_persistence_failure() -> None:
    async def run() -> None:
        redis = FakeRedis()
        bar = _bar(datetime(2026, 1, 1, tzinfo=timezone.utc))
        loader = OHLCVLoader(FakeFeed([bar]), store=FakeStore(fail=True), redis=redis)
        request = OHLCVRequest(symbol="SPY", asset_class=AssetClass.EQUITY)

        bars = await loader.load(request, persist=True, cache_latest=True)

        assert bars == [bar]
        assert redis.latest == []

    asyncio.run(run())


def test_data_quality_reports_invalid_constructed_bars() -> None:
    request = OHLCVRequest(symbol="SPY", asset_class=AssetClass.EQUITY, bar_size="1 min")
    invalid = OHLCVBar.model_construct(
        symbol="SPY",
        asset_class=AssetClass.EQUITY,
        exchange="SMART",
        currency="USD",
        timestamp=datetime(2026, 1, 1),
        open=102.0,
        high=101.0,
        low=99.0,
        close=float("nan"),
        volume=1000.0,
        bar_size="1 min",
        source="ibkr",
        metadata={},
    )

    report = validate_ohlcv_bars([invalid], request=request)

    assert report.has_fatal is True
    assert {issue.code for issue in report.issues} == {
        "invalid_ohlc_range",
        "naive_timestamp",
        "non_finite_value",
    }
