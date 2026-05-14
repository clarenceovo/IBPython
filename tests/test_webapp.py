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
        self.loaded_requests: list[object] = []

    async def load(self, request: object, *, persist: bool, cache_latest: bool) -> list[OHLCVBar]:
        self.calls += 1
        self.loaded_requests.append(request)
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
    def __init__(self) -> None:
        self.latest_call: tuple[AssetClass, str, str | None] | None = None

    async def get_latest_bar(self, asset_class: AssetClass, bar_size: str, symbol: str | None = None) -> OHLCVBar:
        self.latest_call = (asset_class, bar_size, symbol)
        return OHLCVBar(
            symbol=symbol or "SPY",
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
    assert "/api/v1/market-data/ohlcv/equity" in paths
    assert "/api/v1/market-data/ohlcv/futures" in paths
    assert "/api/v1/market-data/ohlcv/fx" in paths
    assert "/api/v1/market-data/ohlcv/bond" in paths
    assert "/api/v1/reference-data/options/chains" in paths
    assert "/api/v1/market-data/options/skew" in paths
    assert "/api/v1/account/positions" in paths


def test_option_chain_swagger_examples_include_primary_exchange() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    content = app.openapi()["paths"]["/api/v1/reference-data/options/chains"]["post"]["requestBody"]["content"]["application/json"]
    examples = content["examples"]

    assert examples["tsla_equity_smart"]["value"]["request"]["primary_exchange"] == "NASDAQ"
    assert examples["spx_index_cboe"]["value"]["request"]["exchange"] == "CBOE"


def test_option_skew_swagger_examples_include_sampling_controls() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    content = app.openapi()["paths"]["/api/v1/market-data/options/skew"]["post"]["requestBody"]["content"]["application/json"]
    examples = content["examples"]

    assert examples["tsla_bounded_skew"]["value"]["request"]["chain_request"]["primary_exchange"] == "NASDAQ"
    assert examples["tsla_bounded_skew"]["value"]["request"]["max_strikes_per_expiry"] == 11
    assert examples["spx_bounded_skew"]["value"]["request"]["trading_class"] == "SPX"


def test_ohlcv_wrapper_swagger_examples_are_minimal_and_asset_specific() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    paths = app.openapi()["paths"]

    generic_examples = paths["/api/v1/market-data/ohlcv"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    equity_examples = paths["/api/v1/market-data/ohlcv/equity"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    futures_examples = paths["/api/v1/market-data/ohlcv/futures"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    fx_examples = paths["/api/v1/market-data/ohlcv/fx"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    bond_examples = paths["/api/v1/market-data/ohlcv/bond"]["post"]["requestBody"]["content"]["application/json"]["examples"]

    assert generic_examples["spy_equity_full_request"]["value"]["request"]["asset_class"] == "equity"
    assert equity_examples["minimal_spy"]["value"] == {"symbol": "SPY"}
    assert equity_examples["nasdaq_equity_with_primary_exchange"]["value"]["primary_exchange"] == "NASDAQ"
    assert futures_examples["es_by_contract_month"]["value"]["last_trade_date_or_contract_month"] == "202606"
    assert fx_examples["eurusd_minimal"]["value"] == {"symbol": "EURUSD"}
    assert fx_examples["usdjpy_hourly"]["value"]["currency"] == "JPY"
    assert bond_examples["treasury_by_cusip"]["value"]["sec_id_type"] == "CUSIP"
    assert bond_examples["bond_by_con_id"]["value"]["con_id"] == 123456789


def test_latest_bar_endpoint_documents_and_forwards_query_params() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/market-data/latest-bar",
            params={"asset_class": "equity", "bar_size": "1 min", "symbol": "SPY"},
        )

    assert response.status_code == 200
    assert response.json()["symbol"] == "SPY"
    assert response.json()["bar_size"] == "1 min"
    assert state.redis.latest_call == (AssetClass.EQUITY, "1 min", "SPY")

    params = app.openapi()["paths"]["/api/v1/market-data/latest-bar"]["get"]["parameters"]
    by_name = {param["name"]: param for param in params}

    assert set(by_name) == {"asset_class", "bar_size", "symbol"}
    assert by_name["asset_class"]["required"] is True
    assert by_name["bar_size"]["required"] is True
    assert by_name["symbol"]["required"] is False
    assert "symbol-scoped" in by_name["symbol"]["description"]


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


def test_asset_specific_ohlcv_wrappers_preset_asset_class_and_contract_fields() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        equity = client.post("/api/v1/market-data/ohlcv/equity", json={"symbol": "spy"})
        future = client.post(
            "/api/v1/market-data/ohlcv/futures",
            json={"symbol": "ES", "last_trade_date_or_contract_month": "202606"},
        )
        fx = client.post("/api/v1/market-data/ohlcv/fx", json={"symbol": "EURUSD"})
        bond = client.post("/api/v1/market-data/ohlcv/bond", json={"sec_id_type": "CUSIP", "sec_id": "91282CJN2"})

    assert equity.status_code == 200
    assert future.status_code == 200
    assert fx.status_code == 200
    assert bond.status_code == 200

    requests = state.loader.loaded_requests
    assert requests[0].asset_class is AssetClass.EQUITY
    assert requests[0].exchange == "SMART"
    assert requests[1].asset_class is AssetClass.FUTURE
    assert requests[1].exchange == "CME"
    assert requests[1].last_trade_date_or_contract_month == "202606"
    assert requests[2].asset_class is AssetClass.FX
    assert requests[2].exchange == "IDEALPRO"
    assert requests[2].what_to_show == "MIDPOINT"
    assert requests[2].use_rth is False
    assert requests[3].asset_class is AssetClass.BOND
    assert requests[3].symbol == "91282CJN2"
    assert requests[3].sec_id_type == "CUSIP"
    assert requests[3].sec_id == "91282CJN2"


def test_futures_ohlcv_wrapper_rejects_missing_contract_identifier() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post("/api/v1/market-data/ohlcv/futures", json={"symbol": "ES"})

    assert response.status_code == 422


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
