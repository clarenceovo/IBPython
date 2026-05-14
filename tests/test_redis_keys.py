import asyncio
import sys
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace

from src.feeds.models import AssetClass, OHLCVBar
from src.transport.redis_client import MarketDataRedisClient, index_composition_key, latest_bar_key, scheduler_job_key


def test_latest_bar_key_format() -> None:
    assert latest_bar_key(AssetClass.EQUITY, "1 min") == "MarketData::equity::1_min:latest"
    assert latest_bar_key(AssetClass.EQUITY, "1 min", "spy") == "MarketData::equity::SPY::1_min:latest"
    assert latest_bar_key(AssetClass.EQUITY, "1_min", "spy") == "MarketData::equity::SPY::1_min:latest"


def test_index_composition_key_format() -> None:
    assert index_composition_key("spx") == "GlobalIndex:SPX:composition"


def test_scheduler_job_key_format() -> None:
    assert scheduler_job_key("snapshot_spy_1m") == "SchedulerJob::snapshot_spy_1m"


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
