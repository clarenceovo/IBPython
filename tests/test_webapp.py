from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.account import LivePositionDTO
from src.feeds.models import AssetClass, OHLCVBar
import src.webapp.app as app_module
from src.webapp.app import create_app
from src.webapp.cache import AsyncTTLCache


class FakeLoader:
    def __init__(self) -> None:
        self.calls = 0

    async def load(self, request: object, *, persist: bool, cache_latest: bool) -> list[OHLCVBar]:
        self.calls += 1
        symbol = getattr(request, "symbol")
        return [
            OHLCVBar(
                symbol=symbol,
                asset_class=getattr(request, "asset_class"),
                exchange=getattr(request, "exchange"),
                currency=getattr(request, "currency"),
                timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
                open=100,
                high=101,
                low=99,
                close=100.5,
                volume=1000,
                bar_size=getattr(request, "bar_size"),
                source="test",
            )
        ]


class FakeRedis:
    async def get_latest_bar(self, asset_class: AssetClass, bar_size: str) -> OHLCVBar:
        return OHLCVBar(
            symbol="SPY",
            asset_class=asset_class,
            exchange="SMART",
            currency="USD",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            open=100,
            high=101,
            low=99,
            close=100.5,
            volume=1000,
            bar_size=bar_size,
            source="redis-test",
        )


class FakeFeed:
    async def load_live_positions(self) -> list[LivePositionDTO]:
        return [
            LivePositionDTO(
                account="DU123",
                con_id=123,
                symbol="AAPL",
                sec_type="STK",
                exchange="NASDAQ",
                currency="USD",
                position=10,
                average_cost=150,
            )
        ]


class FakeState:
    def __init__(self) -> None:
        self.settings = Settings(
            ibkr_rest_app_name="IBKRRestAppTest",
            ibkr_rest_market_data_ttl_seconds=60,
            ibkr_rest_market_data_cache_maxsize=16,
        )
        self.loader = FakeLoader()
        self.feed = FakeFeed()
        self.redis = FakeRedis()
        self.market_data_cache = AsyncTTLCache(ttl_seconds=60, max_size=16)
        self.closed = False

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True


def test_webapp_registers_domain_routers() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    paths = {route.path for route in app.routes}

    assert "/api/v1/system/health" in paths
    assert "/api/v1/market-data/ohlcv" in paths
    assert "/api/v1/reference-data/options/chains" in paths
    assert "/api/v1/account/positions" in paths


def test_webapp_builds_runtime_state_inside_lifespan(monkeypatch) -> None:
    built = 0
    state = FakeState()

    def build_state(settings: Settings) -> FakeState:
        nonlocal built
        built += 1
        return state

    monkeypatch.setattr(app_module, "build_rest_app_state", build_state)
    app = create_app(settings=state.settings)

    assert built == 0

    with TestClient(app) as client:
        assert built == 1
        response = client.get("/api/v1/system/health")

    assert response.status_code == 200
    assert state.closed is True


def test_market_data_ohlcv_endpoint_uses_ttl_cache() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    payload = {
        "request": {
            "symbol": "SPY",
            "asset_class": "equity",
            "exchange": "SMART",
            "currency": "USD",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
            "use_rth": True,
        }
    }

    with TestClient(app) as client:
        first = client.post("/api/v1/market-data/ohlcv", json=payload)
        second = client.post("/api/v1/market-data/ohlcv", json=payload)
        stats = client.get("/api/v1/system/cache/market-data")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert state.loader.calls == 1
    assert stats.json()["size"] == 1
    assert state.closed is True


def test_account_positions_route_bridges_dtos() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get("/api/v1/account/positions")

    assert response.status_code == 200
    assert response.json()[0]["symbol"] == "AAPL"


def test_ttl_cache_single_flights_concurrent_same_key_requests() -> None:
    async def run() -> tuple[list[int], int]:
        cache = AsyncTTLCache(ttl_seconds=60, max_size=16)
        calls = 0

        async def factory() -> int:
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.01)
            return 42

        values = await asyncio.gather(
            cache.get_or_set("same-key", factory),
            cache.get_or_set("same-key", factory),
            cache.get_or_set("same-key", factory),
        )
        return values, calls

    values, calls = asyncio.run(run())

    assert values == [42, 42, 42]
    assert calls == 1
