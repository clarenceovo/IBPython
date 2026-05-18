import asyncio
import sys
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace

from src.feeds.models import AssetClass, OHLCVBar
from src.feeds.snapshotter import FXOptionSnapshot
from src.transport.redis_client import (
    MarketDataRedisClient,
    fx_option_snapshot_key,
    index_composition_key,
    latest_bar_key,
    ohlcv_snapshot_calendar_key,
    ohlcv_snapshot_last_ts_key,
    ohlcv_snapshot_status_key,
    scheduler_lease_key,
    scheduler_job_key,
    scheduler_run_history_key,
    scheduler_run_latest_key,
)


def test_latest_bar_key_format() -> None:
    assert latest_bar_key(AssetClass.EQUITY, "1 min") == "MarketData::equity::1_min:latest"
    assert latest_bar_key(AssetClass.EQUITY, "1 min", "spy") == "MarketData::equity::SPY::1_min:latest"
    assert latest_bar_key(AssetClass.EQUITY, "1_min", "spy") == "MarketData::equity::SPY::1_min:latest"


def test_index_composition_key_format() -> None:
    assert index_composition_key("spx") == "GlobalIndex:SPX:composition"


def test_scheduler_job_key_format() -> None:
    assert scheduler_job_key("snapshot_spy_1m") == "SchedulerJob::snapshot_spy_1m"


def test_scheduler_lease_and_run_key_formats() -> None:
    assert scheduler_lease_key("snapshot_spy_1m") == "SchedulerLease::SNAPSHOT_SPY_1M"
    assert scheduler_run_latest_key("snapshot_spy_1m") == "SchedulerRun::SNAPSHOT_SPY_1M:latest"
    assert scheduler_run_history_key("snapshot_spy_1m") == "SchedulerRun::SNAPSHOT_SPY_1M:history"


def test_ohlcv_snapshot_bookmark_key_formats() -> None:
    assert ohlcv_snapshot_last_ts_key("ohlcv_us_equity_1m", "spy", "1 min") == (
        "OhlcvSnapshot::OHLCV_US_EQUITY_1M::SPY::1_MIN:last_ts"
    )
    assert ohlcv_snapshot_status_key("ohlcv_us_equity_1m", "spy", "1 min") == (
        "OhlcvSnapshot::OHLCV_US_EQUITY_1M::SPY::1_MIN:status"
    )


def test_ohlcv_snapshot_calendar_key_includes_contract_fingerprint() -> None:
    assert ohlcv_snapshot_calendar_key(
        asset_class=AssetClass.FUTURE,
        exchange="hkfe",
        symbol="hsi",
        contract_fingerprint="202606",
        date_value="2026-06-01",
        use_rth=True,
    ) == "OhlcvSnapshotCalendar::FUTURE::HKFE::HSI::202606::2026-06-01::true:has_session"


def test_fx_option_snapshot_key_format() -> None:
    assert fx_option_snapshot_key(symbol="eurusd", expiry="20260619", strike=1.10, right="call") == (
        "FXOptionSnapshot::EURUSD::20260619::1.1::C::SMART:latest"
    )


def test_redis_client_stores_password() -> None:
    client = MarketDataRedisClient("redis://localhost:6379/0", password="secret")

    assert client.password == "secret"


def test_redis_client_passes_password_to_redis_from_url(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def from_url(url: str, **kwargs: object) -> object:
        captured["url"] = url
        captured.update(kwargs)
        return SimpleNamespace(aclose=lambda: None)

    redis_module = ModuleType("redis")
    redis_module.asyncio = SimpleNamespace(from_url=from_url)
    monkeypatch.setitem(sys.modules, "redis", redis_module)

    client = MarketDataRedisClient("redis://localhost:6379/0", password="secret")
    asyncio.run(client.connect())

    assert captured["url"] == "redis://localhost:6379/0"
    assert captured["decode_responses"] is False
    assert captured["password"] == "secret"


def test_redis_client_sets_symbol_and_legacy_latest_bar_keys() -> None:
    class FakeRedis:
        def __init__(self) -> None:
            self.store: dict[str, str] = {}

        async def set(self, key: str, value: str) -> None:
            self.store[key] = value

        async def get(self, key: str) -> str | None:
            return self.store.get(key)

    async def run() -> tuple[str, OHLCVBar | None, OHLCVBar | None, dict[str, str]]:
        fake = FakeRedis()
        client = MarketDataRedisClient(client=fake)
        bar = OHLCVBar(
            symbol="SPY",
            asset_class=AssetClass.EQUITY,
            exchange="SMART",
            currency="USD",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            open=100,
            high=101,
            low=99,
            close=100.5,
            volume=1000,
            bar_size="1 min",
        )

        key = await client.set_latest_bar(bar)
        symbol_loaded = await client.get_latest_bar(AssetClass.EQUITY, "1 min", symbol="SPY")
        legacy_loaded = await client.get_latest_bar(AssetClass.EQUITY, "1 min")
        return key, symbol_loaded, legacy_loaded, fake.store

    key, symbol_loaded, legacy_loaded, store = asyncio.run(run())

    assert key == "MarketData::equity::SPY::1_min:latest"
    assert "MarketData::equity::SPY::1_min:latest" in store
    assert "MarketData::equity::1_min:latest" in store
    assert symbol_loaded is not None
    assert symbol_loaded.symbol == "SPY"
    assert legacy_loaded is not None
    assert legacy_loaded.symbol == "SPY"


def test_redis_client_sets_latest_fx_option_snapshot() -> None:
    class FakeRedis:
        def __init__(self) -> None:
            self.store: dict[str, str] = {}

        async def set(self, key: str, value: str) -> None:
            self.store[key] = value

        async def get(self, key: str) -> str | None:
            return self.store.get(key)

    async def run() -> tuple[str, FXOptionSnapshot | None, dict[str, str]]:
        fake = FakeRedis()
        client = MarketDataRedisClient(client=fake)
        snapshot = FXOptionSnapshot(
            symbol="EURUSD",
            underlying_symbol="EUR",
            expiry="20260619",
            strike=1.1,
            right="C",
            exchange="SMART",
            currency="USD",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            bid=0.01,
            ask=0.012,
        )

        key = await client.set_latest_fx_option_snapshot(snapshot)
        loaded = await client.get_latest_fx_option_snapshot(symbol="EURUSD", expiry="20260619", strike=1.1, right="C")
        return key, loaded, fake.store

    key, loaded, store = asyncio.run(run())

    assert key == "FXOptionSnapshot::EURUSD::20260619::1.1::C::SMART:latest"
    assert key in store
    assert loaded is not None
    assert loaded.symbol == "EURUSD"


def test_redis_client_records_scheduler_run_history() -> None:
    class FakeRedis:
        def __init__(self) -> None:
            self.store: dict[str, str] = {}
            self.lists: dict[str, list[str]] = {}

        async def set(self, key: str, value: str, **kwargs: object) -> bool:
            self.store[key] = value
            return True

        async def lpush(self, key: str, value: str) -> None:
            self.lists.setdefault(key, []).insert(0, value)

        async def ltrim(self, key: str, start: int, stop: int) -> None:
            self.lists[key] = self.lists.get(key, [])[start : stop + 1]

    async def run() -> tuple[dict[str, str], dict[str, list[str]]]:
        fake = FakeRedis()
        client = MarketDataRedisClient(client=fake)
        await client.record_scheduler_run("snapshot_spy_1m", {"status": "success", "run_id": "r1"})
        return fake.store, fake.lists

    store, lists = asyncio.run(run())

    assert "SchedulerRun::SNAPSHOT_SPY_1M:latest" in store
    assert "SchedulerRun::SNAPSHOT_SPY_1M:history" in lists
    assert '"status": "success"' in lists["SchedulerRun::SNAPSHOT_SPY_1M:history"][0]
