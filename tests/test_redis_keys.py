import asyncio
import sys
from types import ModuleType, SimpleNamespace

from src.feeds.models import AssetClass
from src.transport.redis_client import MarketDataRedisClient, index_composition_key, latest_bar_key, scheduler_job_key


def test_latest_bar_key_format() -> None:
    assert latest_bar_key(AssetClass.EQUITY, "1 min") == "MarketData::equity::1_min:latest"


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
