from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.account import LivePositionDTO
from src.feeds.bonds import BondInstrument
from src.feeds.fixed_income import DeliverableBasketRequest, DeliverableBondInput
from src.feeds.index_composition import IndexCompositionPayload
from src.feeds.models import AssetClass, OHLCVBar
from src.feeds.news import HistoricalNewsHeadline, NewsArticle, NewsProvider
from src.feeds.options import OptionSkewSurfaceResponse
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
        self.compositions: dict[str, IndexCompositionPayload] = {}

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

    async def get_index_composition(self, index_symbol: str) -> IndexCompositionPayload | None:
        return self.compositions.get(index_symbol.upper())


class FakeFeed:
    def __init__(self) -> None:
        self.range_calls: list[tuple[object, datetime, datetime | None]] = []
        self.qualified_contracts: list[object] = []
        self.historical_news_requests: list[object] = []
        self.article_requests: list[object] = []
        self.provider_calls = 0
        self.news_providers = [NewsProvider(provider_code="BZ", provider_name="Benzinga")]

    async def load_historical_ohlcv_range(
        self,
        request: object,
        *,
        start_datetime: datetime,
        end_datetime: datetime | None = None,
    ) -> list[OHLCVBar]:
        self.range_calls.append((request, start_datetime, end_datetime))
        return [
            OHLCVBar(
                symbol=getattr(request, "symbol"),
                asset_class=getattr(request, "asset_class"),
                exchange=getattr(request, "exchange"),
                currency=getattr(request, "currency"),
                timestamp=start_datetime,
                open=100,
                high=101,
                low=99,
                close=100.5,
                volume=1000,
                bar_size=getattr(request, "bar_size"),
                source="range-test",
            )
        ]

    async def qualify_contract(self, spec: object) -> object:
        self.qualified_contracts.append(spec)
        return SimpleNamespace(conId=8314)

    async def load_news_providers(self) -> list[NewsProvider]:
        self.provider_calls += 1
        return self.news_providers

    async def load_historical_news(self, request: object) -> list[HistoricalNewsHeadline]:
        self.historical_news_requests.append(request)
        return [
            HistoricalNewsHeadline(
                timestamp=datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc),
                provider_code="BZ",
                article_id="BZ$1",
                headline="TSLA headline",
            )
        ]

    async def load_news_article(self, request: object) -> NewsArticle:
        self.article_requests.append(request)
        return NewsArticle(
            provider_code=getattr(request, "provider_code"),
            article_id=getattr(request, "article_id"),
            article_type=0,
            article_text="Article body",
            received_at=datetime(2026, 1, 1, 12, 31, tzinfo=timezone.utc),
        )

    async def load_option_skew_surface(self, request: object) -> OptionSkewSurfaceResponse:
        return OptionSkewSurfaceResponse(
            underlying_symbol=getattr(request.chain_request, "symbol"),
            underlying_con_id=8314,
            underlying_asset_class=str(getattr(request.chain_request, "asset_class")),
            chain_exchange=getattr(request, "chain_exchange") or "SMART",
            trading_class=getattr(request, "trading_class") or getattr(request.chain_request, "symbol"),
            multiplier="100",
            spot_price=getattr(request, "spot_price") or 100.0,
            maturities=(),
        )

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


class FakeFixedIncomeProvider:
    name = "test_fixed_income_provider"

    def __init__(self) -> None:
        self.basket_requests: list[DeliverableBasketRequest] = []

    async def get_deliverable_basket(self, request: DeliverableBasketRequest) -> tuple[DeliverableBondInput, ...]:
        self.basket_requests.append(request)
        return (
            DeliverableBondInput(
                bond=BondInstrument(
                    symbol=f"{request.futures_symbol}CTD",
                    maturity_date=datetime(2031, 5, 15, tzinfo=timezone.utc).date(),
                    coupon_rate=0.04,
                    currency="USD",
                    market=request.market,
                ),
                conversion_factor=0.9,
                clean_price=91.0,
                accrued_interest=0.2,
            ),
        )


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
        self.fixed_income_reference_provider = FakeFixedIncomeProvider()
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
    assert "/api/v1/business/getBondCurve" in paths
    assert "/api/v1/business/getSymbolNews" in paths
    assert "/api/v1/business/getNewsArticle" in paths
    assert "/api/v1/business/getMarketPanel" in paths
    assert "/api/v1/business/fixed-income/getBondFutureQuotes" in paths
    assert "/api/v1/business/fixed-income/getCTD" in paths
    assert "/api/v1/business/fixed-income/getFuturesImpliedCurve" in paths
    assert "/api/v1/business/fixed-income/getCashBondCurve" in paths
    assert "/api/v1/business/fixed-income/getCurveComparison" in paths
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
    assert equity_examples["tsla_nasdaq_auto"]["value"] == {"symbol": "TSLA"}
    assert equity_examples["hk_stock_0700"]["value"] == {"symbol": "0700.HK"}
    assert equity_examples["nasdaq_equity_with_primaryExchange"]["value"]["primary_exchange"] == "NASDAQ"
    assert futures_examples["es_by_contract_month"]["value"]["last_trade_date_or_contract_month"] == "202606"
    assert futures_examples["hsi_hkfe_by_contract_month"]["value"]["symbol"] == "HSI"
    assert futures_examples["hsi_hkfe_by_contract_month"]["value"]["exchange"] == "HKFE"
    assert futures_examples["hsi_hkfe_by_contract_month"]["value"]["currency"] == "HKD"
    assert futures_examples["hstech_hkfe_by_contract_month"]["value"]["symbol"] == "HTI"
    assert futures_examples["hstech_hkfe_by_contract_month"]["value"]["exchange"] == "HKFE"
    assert futures_examples["hstech_hkfe_by_contract_month"]["value"]["currency"] == "HKD"
    assert fx_examples["eurusd_minimal"]["value"] == {"symbol": "EURUSD"}
    assert fx_examples["usdjpy_hourly"]["value"]["currency"] == "JPY"
    assert bond_examples["treasury_by_cusip"]["value"]["sec_id_type"] == "CUSIP"
    assert bond_examples["bond_by_con_id"]["value"]["con_id"] == 123456789

    schemas = app.openapi()["components"]["schemas"]
    assert "start_datetime" in schemas["OHLCVRequest"]["properties"]
    assert "end_datetime" in schemas["OHLCVRequest"]["properties"]
    assert "start_datetime" in schemas["EquityOHLCVLoadRequest"]["properties"]
    assert "end_datetime" in schemas["EquityOHLCVLoadRequest"]["properties"]
    assert "contract_month" in schemas["FutureOHLCVBar"]["properties"]
    assert "is_continuous" in schemas["FutureOHLCVBar"]["properties"]
    assert "base_currency" in schemas["FXOHLCVBar"]["properties"]
    assert "quote_currency" in schemas["FXOHLCVBar"]["properties"]
    futures_response = paths["/api/v1/market-data/ohlcv/futures"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]
    assert futures_response["items"]["$ref"] == "#/components/schemas/FutureOHLCVBar"
    fx_response = paths["/api/v1/market-data/ohlcv/fx"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]
    assert fx_response["items"]["$ref"] == "#/components/schemas/FXOHLCVBar"


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


def test_get_bond_curve_endpoint_returns_chart_ready_curve_and_documents_params() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/business/getBondCurve",
            params={"market": "UST", "valuation_date": "2026-05-16"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["market"] == "US_TREASURY"
    assert payload["currency"] == "USD"
    assert payload["render_points"][0]["tenor"] == "2Y"
    assert payload["standard_ctd_points"][0]["ctd_status"] == "indicative_placeholder"

    operation = app.openapi()["paths"]["/api/v1/business/getBondCurve"]["get"]
    assert operation["operationId"] == "getBondCurve"
    assert operation["tags"] == ["business"]
    assert {param["name"] for param in operation["parameters"]} == {
        "market",
        "valuation_date",
        "coupon_frequency",
    }


def test_get_bond_curve_endpoint_rejects_unsupported_market_alias() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get("/api/v1/business/getBondCurve", params={"market": "XYZ"})

    assert response.status_code == 422
    assert "unsupported bond curve market" in response.json()["detail"]


def test_business_news_provider_endpoint_uses_ttl_cache_and_openapi() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        first = client.get("/api/v1/business/getNewsProviders")
        second = client.get("/api/v1/business/getNewsProviders")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()[0]["provider_code"] == "BZ"
    assert state.feed.provider_calls == 1
    operation = app.openapi()["paths"]["/api/v1/business/getNewsProviders"]["get"]
    assert operation["tags"] == ["business"]


def test_business_swagger_examples_are_loaded_from_markdown() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    paths = app.openapi()["paths"]

    symbol_news_examples = paths["/api/v1/business/getSymbolNews"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    market_panel_examples = paths["/api/v1/business/getMarketPanel"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    returns_examples = paths["/api/v1/business/getReturns"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    option_skew_examples = paths["/api/v1/business/getOptionSkew"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    fixed_income_examples = paths["/api/v1/business/fixed-income/getBondFutureQuotes"]["post"]["requestBody"]["content"]["application/json"][
        "examples"
    ]
    ctd_examples = paths["/api/v1/business/fixed-income/getCTD"]["post"]["requestBody"]["content"]["application/json"]["examples"]

    assert symbol_news_examples["tsla_news"]["value"]["symbol"] == "TSLA"
    assert market_panel_examples["us_equity_panel"]["value"]["symbols"] == ["SPY", "QQQ", "TSLA"]
    assert returns_examples["us_equity_returns"]["value"]["bar_size"] == "5 mins"
    assert option_skew_examples["tsla_skew"]["value"]["primary_exchange"] == "NASDAQ"
    assert fixed_income_examples["ust_futures_quotes"]["value"]["market"] == "UST"
    assert ctd_examples["zn_ctd"]["value"]["future"]["futures_symbol"] == "ZN"


def test_fixed_income_bond_future_quotes_load_default_ust_contracts() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/fixed-income/getBondFutureQuotes",
            json={"market": "UST", "contract_month": "202606", "use_ttl_cache": False},
        )

    assert response.status_code == 200
    payload = response.json()
    assert [quote["futures_symbol"] for quote in payload] == ["ZT", "ZF", "ZN", "ZB"]
    assert payload[0]["price"] == 100.5
    assert len(state.loader.loaded_requests) == 4
    first_request = state.loader.loaded_requests[0]
    assert first_request.asset_class is AssetClass.FUTURE
    assert first_request.symbol == "ZT"
    assert first_request.exchange == "CBOT"
    assert first_request.currency == "USD"
    assert first_request.last_trade_date_or_contract_month == "202606"


def test_fixed_income_ctd_endpoint_uses_injected_reference_provider() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/fixed-income/getCTD",
            json={
                "future": {
                    "market": "UST",
                    "futures_symbol": "ZN",
                    "exchange": "CBOT",
                    "currency": "USD",
                    "contract_month": "202606",
                },
                "use_ttl_cache": False,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["provider"] == "test_fixed_income_provider"
    assert payload["selected"]["bond"]["symbol"] == "ZNCTD"
    assert state.fixed_income_reference_provider.basket_requests[0].futures_symbol == "ZN"


def test_fixed_income_futures_implied_curve_uses_quotes_and_ctd_provider() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/fixed-income/getFuturesImpliedCurve",
            json={
                "market": "UST",
                "contract_month": "202606",
                "futures": [
                    {
                        "market": "UST",
                        "futures_symbol": "ZN",
                        "exchange": "CBOT",
                        "currency": "USD",
                        "contract_month": "202606",
                    }
                ],
                "use_ttl_cache": False,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["market"] == "US_TREASURY"
    assert payload["points"][0]["futures_symbol"] == "ZN"
    assert payload["diagnostics"]["provider"] == "test_fixed_income_provider"


def test_fixed_income_provider_backed_endpoints_fail_clearly_when_provider_missing() -> None:
    state = FakeState()
    state.fixed_income_reference_provider = None
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/fixed-income/getCTD",
            json={
                "future": {
                    "market": "UST",
                    "futures_symbol": "ZN",
                    "exchange": "CBOT",
                    "currency": "USD",
                    "contract_month": "202606",
                }
            },
        )

    assert response.status_code == 503
    assert "fixed-income reference provider is not configured" in response.json()["detail"]


def test_business_symbol_news_resolves_symbol_and_defaults_to_entitled_providers() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/getSymbolNews",
            json={"symbol": "TSLA", "primary_exchange": "NASDAQ", "use_ttl_cache": False},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "TSLA"
    assert payload["con_id"] == 8314
    assert payload["provider_codes"] == ["BZ"]
    assert payload["headlines"][0]["headline"] == "TSLA headline"
    assert state.feed.qualified_contracts[0].symbol == "TSLA"
    assert state.feed.qualified_contracts[0].primary_exchange == "NASDAQ"
    assert state.feed.historical_news_requests[0].provider_codes == ("BZ",)


def test_business_symbol_news_can_include_articles() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/getSymbolNews",
            json={
                "symbol": "TSLA",
                "provider_codes": ["BZ"],
                "include_articles": True,
                "use_ttl_cache": False,
            },
        )

    assert response.status_code == 200
    assert response.json()["headlines"][0]["article"]["article_text"] == "Article body"
    assert state.feed.article_requests[0].provider_code == "BZ"
    assert state.feed.article_requests[0].article_id == "BZ$1"


def test_business_symbol_news_rejects_missing_news_providers() -> None:
    state = FakeState()
    state.feed.news_providers = []
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/getSymbolNews",
            json={"symbol": "TSLA", "use_ttl_cache": False},
        )

    assert response.status_code == 503
    assert "news providers" in response.json()["detail"]


def test_business_news_article_uses_ttl_cache() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        first = client.post("/api/v1/business/getNewsArticle", json={"provider_code": "bz", "article_id": "BZ$1"})
        second = client.post("/api/v1/business/getNewsArticle", json={"provider_code": "bz", "article_id": "BZ$1"})

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["provider_code"] == "BZ"
    assert len(state.feed.article_requests) == 1


def test_business_market_panel_and_returns_wrappers_load_long_form_bars() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    payload = {
        "symbols": ["SPY", "QQQ"],
        "asset_class": "equity",
        "start_datetime": "2026-01-02T09:30:00-05:00",
        "end_datetime": "2026-01-02T16:00:00-05:00",
        "use_ttl_cache": False,
    }

    with TestClient(app) as client:
        panel = client.post("/api/v1/business/getMarketPanel", json=payload)
        returns = client.post("/api/v1/business/getReturns", json=payload)

    assert panel.status_code == 200
    assert returns.status_code == 200
    assert {bar["symbol"] for bar in panel.json()} == {"SPY", "QQQ"}
    assert returns.json()["asset_class"] == "equity"
    assert {summary["symbol"] for summary in returns.json()["summaries"]} == {"SPY", "QQQ"}


def test_business_universe_bars_reads_redis_index_composition() -> None:
    state = FakeState()
    state.redis.compositions["SPX"] = IndexCompositionPayload(
        index_symbol="SPX",
        provider="test",
        constituents=[
            {"symbol": "AAPL", "name": "Apple"},
            {"symbol": "MSFT", "name": "Microsoft"},
        ],
    )
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/getUniverseBars",
            json={"universe": "SPX", "max_symbols": 2, "use_ttl_cache": False},
        )

    assert response.status_code == 200
    assert {bar["symbol"] for bar in response.json()} == {"AAPL", "MSFT"}


def test_business_option_skew_wrapper_uses_minimal_payload() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/getOptionSkew",
            json={"symbol": "TSLA", "primary_exchange": "NASDAQ", "spot_price": 250.0},
        )

    assert response.status_code == 200
    assert response.json()["underlying_symbol"] == "TSLA"
    assert response.json()["spot_price"] == 250.0


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


def test_generic_ohlcv_endpoint_supports_start_and_end_datetime_range() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    payload = {
        "request": {
            "symbol": "SPY",
            "asset_class": "equity",
            "exchange": "SMART",
            "currency": "USD",
            "start_datetime": "2026-01-02T09:30:00-05:00",
            "end_datetime": "2026-01-02T16:00:00-05:00",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
            "use_rth": True,
        },
        "cache_latest": False,
    }

    with TestClient(app) as client:
        response = client.post("/api/v1/market-data/ohlcv", json=payload)

    assert response.status_code == 200
    assert response.json()[0]["source"] == "range-test"
    assert state.loader.calls == 0
    request, start, end = state.feed.range_calls[0]
    assert getattr(request, "symbol") == "SPY"
    assert start == datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)
    assert end == datetime(2026, 1, 2, 21, 0, tzinfo=timezone.utc)


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
    assert future.json()[0]["contract_month"] is None
    assert future.json()[0]["is_continuous"] is False
    assert requests[2].asset_class is AssetClass.FX
    assert requests[2].exchange == "IDEALPRO"
    assert requests[2].what_to_show == "MIDPOINT"
    assert requests[2].use_rth is False
    assert fx.json()[0]["base_currency"] == "EUR"
    assert fx.json()[0]["quote_currency"] == "USD"
    assert requests[3].asset_class is AssetClass.BOND
    assert requests[3].symbol == "91282CJN2"
    assert requests[3].sec_id_type == "CUSIP"
    assert requests[3].sec_id == "91282CJN2"


def test_asset_specific_ohlcv_wrapper_supports_start_and_end_datetime_range() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/market-data/ohlcv/equity",
            json={
                "symbol": "spy",
                "start_datetime": "2026-01-02T09:30:00-05:00",
                "end_datetime": "2026-01-02T16:00:00-05:00",
                "cache_latest": False,
            },
        )

    assert response.status_code == 200
    request, start, end = state.feed.range_calls[0]
    assert getattr(request, "asset_class") is AssetClass.EQUITY
    assert getattr(request, "start_datetime") == start
    assert start == datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)
    assert end == datetime(2026, 1, 2, 21, 0, tzinfo=timezone.utc)


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
