from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.account import AccountPnLDTO, AccountSummaryDTO, AccountValueDTO, LivePositionDTO, PortfolioItemDTO
from src.feeds.bonds import BondInstrument
from src.feeds.event_contracts import (
    EventContractHistoryBar,
    EventContractHistoryResponse,
    EventContractInstrument,
    EventContractMarketData,
    EventContractOrderResponse,
    EventContractSearchResult,
    EventContractStrikesResponse,
)
from src.feeds.fixed_income import DeliverableBasketRequest, DeliverableBondInput
from src.feeds.index_composition import IndexCompositionPayload
from src.feeds.models import AssetClass, OHLCVBar, OptionOHLCVBar
from src.feeds.news import HistoricalNewsHeadline, NewsArticle, NewsProvider
from src.feeds.options import OptionAnalyticsSnapshot, OptionSkewSurfaceResponse
from src.feeds.snapshotter import EquitySnapshot, EquitySnapshotCaptureResult, FXOptionSnapshot
from src.feeds.tick_data import HistoricalTickResponse, MarketRule, PriceIncrement
import src.webapp.app as app_module
import src.webapp.routers.business as business_router
from src.webapp.app import create_app
from src.webapp.cache import AsyncTTLCache
from src.webapp.dependencies import IBKRRestAppState
from src.webapp.routers.business_shared import resolve_business_symbol
from src.config.reference_data import resolve_index


class FakeLoader:
    def __init__(self) -> None:
        self.calls = 0
        self.loaded_requests: list[object] = []

    async def load(self, request: object, *, persist: bool, cache_latest: bool) -> list[OHLCVBar]:
        self.calls += 1
        self.loaded_requests.append(request)
        symbol = getattr(request, "symbol")
        if getattr(request, "asset_class") is AssetClass.OPTION:
            return [
                OptionOHLCVBar(
                    symbol=symbol,
                    asset_class=AssetClass.OPTION,
                    exchange=getattr(request, "exchange"),
                    currency=getattr(request, "currency"),
                    timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    open=10,
                    high=11,
                    low=9,
                    close=10.5,
                    volume=100,
                    bar_size=getattr(request, "bar_size"),
                    source="test",
                    underlying_symbol=getattr(request, "underlying_symbol"),
                    expiry=getattr(request, "expiry"),
                    strike=getattr(request, "strike"),
                    right=getattr(request, "right"),
                    multiplier=getattr(request, "multiplier"),
                    trading_class=getattr(request, "trading_class"),
                    con_id=getattr(request, "con_id"),
                )
            ]
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
        self.equity_snapshots: dict[str, EquitySnapshot] = {}
        self.fx_option_snapshots: dict[tuple[str, str, float, str], FXOptionSnapshot] = {}
        self.values: dict[str, str] = {"OrderAuth::bearer_token": "test-order-token"}

    async def health_check(self) -> bool:
        return True

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

    async def get_raw(self, key: str) -> str | None:
        return self.values.get(key)

    async def set_latest_fx_option_snapshot(self, snapshot: FXOptionSnapshot) -> str:
        self.fx_option_snapshots[(snapshot.symbol, snapshot.expiry, snapshot.strike, snapshot.right)] = snapshot
        return "fx-option-key"

    async def set_latest_equity_snapshot(self, snapshot: EquitySnapshot) -> str:
        self.equity_snapshots[snapshot.symbol] = snapshot
        return "equity-snapshot-key"

    async def get_latest_fx_option_snapshot(
        self,
        *,
        symbol: str,
        expiry: str,
        strike: float,
        right: str,
        exchange: str = "SMART",
        local_symbol: str | None = None,
        con_id: int | None = None,
    ) -> FXOptionSnapshot | None:
        normalized_right = "C" if right.upper() in {"C", "CALL"} else "P"
        return self.fx_option_snapshots.get((symbol.upper(), expiry.upper(), strike, normalized_right))


class FakeFeed:
    def __init__(self) -> None:
        self.range_calls: list[tuple[object, datetime, datetime | None]] = []
        self.qualified_contracts: list[object] = []
        self.historical_news_requests: list[object] = []
        self.article_requests: list[object] = []
        self.option_analytics_requests: list[object] = []
        self.historical_tick_requests: list[object] = []
        self.market_rule_requests: list[int] = []
        self.fx_option_snapshot_requests: list[tuple[object, ...]] = []
        self.equity_snapshot_requests: list[tuple[tuple[str, str, str, str, int], ...]] = []
        self.cancelled_equity_tickers: list[object] = []
        self.account_summary_requests: list[str] = []
        self.portfolio_item_requests: list[str] = []
        self.account_pnl_requests: list[tuple[str, str, float]] = []
        self.raise_account_pnl = False
        self.provider_calls = 0
        self.news_providers = [NewsProvider(provider_code="BZ", provider_name="Benzinga")]

    async def load_historical_ohlcv_range(
        self,
        request: object,
        *,
        start_datetime: datetime,
        end_datetime: datetime | None = None,
        max_chunks: int = 60,
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
        return SimpleNamespace(conId=8314, localSymbol="CLM6", tradingClass="CL", marketRuleIds="26", minTick=0.01)

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

    async def load_option_analytics(self, request: object) -> OptionAnalyticsSnapshot:
        self.option_analytics_requests.append(request)
        return OptionAnalyticsSnapshot(contract=request.contract, implied_volatility=0.25, call_open_interest=100)

    async def load_market_rule(self, price_magnitude: int) -> MarketRule:
        self.market_rule_requests.append(price_magnitude)
        return MarketRule(price_magnitude=price_magnitude, increments=[PriceIncrement(low_edge=0, increment=0.01)])

    async def load_head_timestamp(self, request: object) -> datetime:
        return datetime(2020, 1, 1, tzinfo=timezone.utc)

    async def load_trading_schedule(self, request: object, *, ref_date: object, use_rth: bool = True) -> tuple[object, ...]:
        return (SimpleNamespace(refDate=str(ref_date), startDateTime="20260518 00:00:00", endDateTime="20260518 23:00:00"),)

    async def load_historical_ticks(self, request: object) -> HistoricalTickResponse:
        self.historical_tick_requests.append(request)
        return HistoricalTickResponse(symbol=request.symbol, ticks=[], total_count=0, truncated=False)

    async def capture_fx_option_snapshots(
        self,
        contracts: object,
        *,
        symbols: object,
        generic_ticks: object,
        snapshot_wait_seconds: float,
    ) -> list[FXOptionSnapshot]:
        self.fx_option_snapshot_requests.append(tuple(contracts))
        snapshots: list[FXOptionSnapshot] = []
        for symbol, contract in zip(symbols, contracts, strict=True):
            snapshots.append(
                FXOptionSnapshot(
                    symbol=symbol,
                    underlying_symbol=contract.underlying_symbol,
                    expiry=contract.expiry,
                    strike=contract.strike,
                    right=contract.right.value,
                    exchange=contract.exchange,
                    currency=contract.currency,
                    multiplier=contract.multiplier,
                    trading_class=contract.trading_class,
                    local_symbol=contract.local_symbol,
                    con_id=contract.con_id,
                    timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    bid=0.01,
                    ask=0.012,
                    implied_volatility=0.10,
                )
            )
        return snapshots

    async def capture_equity_snapshots(
        self,
        symbols: object,
        *,
        snapshot_wait_seconds: float = 11.5,
        lease_ttl_seconds: float = 30.0,
    ) -> list[EquitySnapshotCaptureResult]:
        symbol_rows = tuple(symbols)
        self.equity_snapshot_requests.append(symbol_rows)
        results: list[EquitySnapshotCaptureResult] = []
        for symbol, exchange, currency, primary_exchange, con_id in symbol_rows:
            if symbol == "AAPL":
                results.append(
                    EquitySnapshotCaptureResult(
                        requested_symbol=symbol,
                        symbol=symbol,
                        exchange=exchange,
                        currency=currency,
                        primary_exchange=primary_exchange,
                        con_id=con_id,
                        error="simulated subscription failure",
                    )
                )
                continue
            ticker = SimpleNamespace(
                contract=SimpleNamespace(conId=con_id or 999, symbol=symbol),
                time=datetime(2026, 1, 1, 14, 30, tzinfo=timezone.utc),
                last=100.5,
                bid=100.0,
                ask=101.0,
                volume=1000,
            )
            results.append(
                EquitySnapshotCaptureResult(
                    requested_symbol=symbol,
                    symbol=symbol,
                    exchange=exchange,
                    currency=currency,
                    primary_exchange=primary_exchange,
                    con_id=con_id or 999,
                    ticker=ticker,
                )
            )
        return results

    async def cancel_equity_tickers(self, tickers: object) -> int:
        self.cancelled_equity_tickers.extend(tickers)
        return 0

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

    async def load_account_summary(self, account: str = "") -> list[AccountSummaryDTO]:
        self.account_summary_requests.append(account)
        return [
            AccountSummaryDTO(
                account="DU123",
                values={
                    "NetLiquidation": AccountValueDTO(account="DU123", tag="NetLiquidation", value="100000", currency="USD"),
                    "TotalCashValue": AccountValueDTO(account="DU123", tag="TotalCashValue", value="25000", currency="USD"),
                    "AvailableFunds": AccountValueDTO(account="DU123", tag="AvailableFunds", value="75000", currency="USD"),
                    "ExcessLiquidity": AccountValueDTO(account="DU123", tag="ExcessLiquidity", value="70000", currency="USD"),
                    "Cushion": AccountValueDTO(account="DU123", tag="Cushion", value="0.70", currency=""),
                    "GrossPositionValue": AccountValueDTO(account="DU123", tag="GrossPositionValue", value="50000", currency="USD"),
                },
            )
        ]

    async def load_portfolio_items(self, account: str = "") -> list[PortfolioItemDTO]:
        self.portfolio_item_requests.append(account)
        return [
            PortfolioItemDTO(
                account="DU123",
                con_id=123,
                symbol="AAPL",
                sec_type="STK",
                exchange="NASDAQ",
                currency="USD",
                position=10,
                market_price=160,
                market_value=1600,
                average_cost=150,
                unrealized_pnl=100,
                realized_pnl=10,
            ),
            PortfolioItemDTO(
                account="DU123",
                con_id=456,
                symbol="EUR",
                sec_type="CASH",
                exchange="IDEALPRO",
                currency="USD",
                position=-5000,
                market_price=1.1,
                market_value=-5500,
                average_cost=1.12,
                unrealized_pnl=-100,
                realized_pnl=0,
            ),
        ]

    async def load_account_pnl_snapshot(
        self,
        account: str,
        model_code: str = "",
        *,
        wait_seconds: float = 1.2,
    ) -> AccountPnLDTO:
        self.account_pnl_requests.append((account, model_code, wait_seconds))
        if self.raise_account_pnl:
            raise RuntimeError("simulated PnL timeout")
        return AccountPnLDTO(
            account=account,
            model_code=model_code,
            daily_pnl=250,
            unrealized_pnl=100,
            realized_pnl=150,
        )

    # -- Order management stubs for FakeFeed --

    async def place_order(self, request: object) -> object:
        from src.feeds.orders import OrderResponse, OrderStatus
        return OrderResponse(order_id=1001, status=OrderStatus.SUBMITTED)

    async def cancel_order(self, account_id: str, order_id: int) -> object:
        from src.feeds.orders import CancelOrderResponse
        return CancelOrderResponse(order_id=order_id, status="cancel_requested")

    async def modify_order(self, account_id: str, order_id: int, modifications: object) -> object:
        from src.feeds.orders import OrderResponse, OrderStatus
        return OrderResponse(order_id=order_id, status=OrderStatus.SUBMITTED)

    async def load_open_orders(self) -> list:
        from src.feeds.orders import OpenOrder
        return [OpenOrder(
            order_id=1001,
            symbol="AAPL",
            sec_type="STK",
            action="BUY",
            order_type="LMT",
            quantity=100,
            price=150.0,
            status="Submitted",
        )]

    async def load_executions(self, request: object) -> object:
        from src.feeds.orders import ExecutionResponse
        return ExecutionResponse(executions=[], total_count=0)

    async def preview_order(self, request: object) -> object:
        from src.feeds.orders import WhatIfOrderResponse
        return WhatIfOrderResponse(
            initial_margin=5000.0,
            maintenance_margin=2500.0,
            commission=1.0,
        )

    async def load_completed_orders(self) -> list:
        return []


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


class FakeEventContractsClient:
    def __init__(self) -> None:
        self.order_requests: list[object] = []

    async def load_category_tree(self) -> list:
        return []

    async def search(self, request: object) -> list[EventContractSearchResult]:
        return [
            EventContractSearchResult(
                con_id=658663572,
                symbol=getattr(request, "symbol"),
                description="FORECASTX",
                company_name="US Fed Funds Target Rate",
                company_header="US Fed Funds Target Rate - FORECASTX",
                opt_expirations=("20260616",),
            )
        ]

    async def strikes(self, request: object) -> EventContractStrikesResponse:
        return EventContractStrikesResponse(call=(4.875, 5.125), put=(4.875, 5.125), all_strikes=(4.875, 5.125))

    async def info(self, request: object) -> list[EventContractInstrument]:
        return [
            EventContractInstrument(
                con_id=713921696,
                symbol="FF",
                sec_type="OPT",
                exchange="FORECASTX",
                right="C",
                yes_no="YES",
                strike=4.875,
                trading_class="FF",
            )
        ]

    async def snapshot(self, request: object) -> list[EventContractMarketData]:
        return [EventContractMarketData(con_id=getattr(request, "con_ids")[0], last=0.81, bid=0.79, ask=0.82)]

    async def history(self, request: object) -> EventContractHistoryResponse:
        return EventContractHistoryResponse(
            con_id=getattr(request, "con_id"),
            symbol="FF",
            period=getattr(request, "period"),
            bars=(EventContractHistoryBar(timestamp=datetime(2026, 6, 1, tzinfo=timezone.utc), open=0.2, high=0.2, low=0.19, close=0.2),),
        )

    async def place_order(self, request: object) -> EventContractOrderResponse:
        self.order_requests.append(request)
        return EventContractOrderResponse(
            account_id=getattr(request, "account_id"),
            submitted=True,
            response={"order_id": "987654", "order_status": "Submitted"},
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
        self.event_contracts = FakeEventContractsClient()
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
    assert "/api/v1/system/readiness" in paths
    assert "/api/v1/system/rate-limits" in paths
    assert "/api/v1/market-data/ohlcv" in paths
    assert "/api/v1/market-data/ohlcv/equity" in paths
    assert "/api/v1/market-data/ohlcv/futures" in paths
    assert "/api/v1/market-data/ohlcv/commodities" in paths
    assert "/api/v1/market-data/ohlcv/commodity-options" in paths
    assert "/api/v1/market-data/ohlcv/fx" in paths
    assert "/api/v1/market-data/ohlcv/fx-options" in paths
    assert "/api/v1/market-data/ohlcv/bond" in paths
    assert "/api/v1/market-data/commodities/options/analytics" in paths
    assert "/api/v1/market-data/commodities/metadata" in paths
    assert "/api/v1/market-data/commodities/historical-ticks" in paths
    assert "/api/v1/market-data/commodities/news" in paths
    assert "/api/v1/business/getBondCurve" in paths
    assert "/api/v1/business/getSymbolNews" in paths
    assert "/api/v1/business/getNewsArticle" in paths
    assert "/api/v1/business/getMarketPanel" in paths
    assert "/api/v1/business/commodities/getFutures" in paths
    assert "/api/v1/business/portfolio/getRiskSnapshot" in paths
    assert "/api/v1/business/fixed-income/getBondFutureQuotes" in paths
    assert "/api/v1/business/fixed-income/getCTD" in paths
    assert "/api/v1/business/fixed-income/getFuturesImpliedCurve" in paths
    assert "/api/v1/business/fixed-income/getCashBondCurve" in paths
    assert "/api/v1/business/fixed-income/getCurveComparison" in paths
    assert "/api/v1/business/event-contracts/discover/search" in paths
    assert "/api/v1/business/event-contracts/market-data/snapshot" in paths
    assert "/api/v1/business/event-contracts/orders/place" in paths
    assert "/api/v1/reference-data/options/chains" in paths
    assert "/api/v1/market-data/options/skew" in paths
    assert "/api/v1/snapshot/fx-options/capture" in paths
    assert "/api/v1/snapshot/fx-options/latest" in paths
    assert "/api/v1/snapshot/fx-options/query" in paths
    assert "/api/v1/account/positions" in paths


def test_event_contract_search_endpoint_uses_web_api_client() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/event-contracts/discover/search",
            json={"symbol": "FF"},
        )

    assert response.status_code == 200
    assert response.json()[0]["con_id"] == 658663572
    assert response.json()[0]["opt_expirations"] == ["20260616"]


def test_event_contract_snapshot_endpoint_normalizes_market_fields() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/event-contracts/market-data/snapshot",
            json={"con_ids": [713921696]},
        )

    assert response.status_code == 200
    payload = response.json()[0]
    assert payload["con_id"] == 713921696
    assert payload["last"] == 0.81


def test_event_contract_streaming_messages_match_ibkr_websocket_protocol() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/event-contracts/market-data/streaming-messages",
            json={"con_id": 721095500, "fields": ["31", "84"]},
        )

    assert response.status_code == 200
    assert response.json() == {
        "subscribe": 'smd+721095500+{"fields":["31","84"]}',
        "unsubscribe": "umd+721095500+{}",
    }


def test_event_contract_live_order_is_disabled_by_default() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/event-contracts/orders/place",
            headers={"Authorization": "Bearer test-order-token"},
            json={
                "account_id": "DU123",
                "con_id": 713921696,
                "quantity": 1,
                "price": 0.81,
                "confirm_live_order": True,
            },
        )

    assert response.status_code == 403
    assert state.event_contracts.order_requests == []


def test_event_contract_live_order_requires_explicit_confirmation_when_enabled() -> None:
    state = FakeState()
    state.settings.ibkr_event_contracts_live_orders_enabled = True
    app = create_app(settings=state.settings, state=state)
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/event-contracts/orders/place",
            headers={"Authorization": "Bearer test-order-token"},
            json={
                "account_id": "DU123",
                "con_id": 713921696,
                "quantity": 1,
                "price": 0.81,
            },
        )

    assert response.status_code == 422
    assert state.event_contracts.order_requests == []


def test_event_contract_live_order_submits_when_enabled_confirmed_and_authorized() -> None:
    state = FakeState()
    state.settings.ibkr_event_contracts_live_orders_enabled = True
    app = create_app(settings=state.settings, state=state)
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/event-contracts/orders/place",
            headers={"Authorization": "Bearer test-order-token"},
            json={
                "account_id": "DU123",
                "con_id": 713921696,
                "quantity": 1,
                "price": 0.81,
                "confirm_live_order": True,
            },
        )

    assert response.status_code == 200
    assert response.json()["response"]["order_status"] == "Submitted"
    assert len(state.event_contracts.order_requests) == 1


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


def test_index_resolution_uses_reference_map_for_business_symbols() -> None:
    resolved = resolve_business_symbol(symbol="HSI", asset_class=AssetClass.INDEX)

    assert resolved.symbol == "HSI"
    assert resolved.exchange == "HKFE"
    assert resolved.currency == "HKD"
    assert resolve_index("RUT") == {"symbol": "RUT", "exchange": "CBOE", "currency": "USD"}


def test_ohlcv_wrapper_swagger_examples_are_minimal_and_asset_specific() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    paths = app.openapi()["paths"]

    generic_examples = paths["/api/v1/market-data/ohlcv"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    equity_examples = paths["/api/v1/market-data/ohlcv/equity"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    futures_examples = paths["/api/v1/market-data/ohlcv/futures"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    fx_examples = paths["/api/v1/market-data/ohlcv/fx"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    fx_option_examples = paths["/api/v1/market-data/ohlcv/fx-options"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    bond_examples = paths["/api/v1/market-data/ohlcv/bond"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    commodity_examples = paths["/api/v1/market-data/ohlcv/commodities"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    commodity_option_examples = paths["/api/v1/market-data/ohlcv/commodity-options"]["post"]["requestBody"]["content"]["application/json"]["examples"]

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
    assert fx_option_examples["eurusd_call"]["value"]["symbol"] == "EURUSD"
    assert fx_option_examples["eurusd_call"]["value"]["right"] == "C"
    assert bond_examples["treasury_by_cusip"]["value"]["sec_id_type"] == "CUSIP"
    assert bond_examples["bond_by_con_id"]["value"]["con_id"] == 123456789
    assert commodity_examples["cl_crude_nymex"]["value"]["symbol"] == "CL"
    assert commodity_examples["gc_gold_comex"]["value"]["symbol"] == "GC"
    assert commodity_examples["ng_by_local_symbol"]["value"]["local_symbol"] == "NGM6"
    assert commodity_option_examples["cl_fop_call"]["value"]["underlying_symbol"] == "CL"
    assert commodity_option_examples["cl_fop_call"]["value"]["right"] == "C"

    schemas = app.openapi()["components"]["schemas"]
    assert "start_datetime" in schemas["OHLCVRequest"]["properties"]
    assert "end_datetime" in schemas["OHLCVRequest"]["properties"]
    assert "start_datetime" in schemas["EquityOHLCVLoadRequest"]["properties"]
    assert "end_datetime" in schemas["EquityOHLCVLoadRequest"]["properties"]
    assert "contract_month" in schemas["FutureOHLCVBar"]["properties"]
    assert "is_continuous" in schemas["FutureOHLCVBar"]["properties"]
    assert "base_currency" in schemas["FXOHLCVBar"]["properties"]
    assert "quote_currency" in schemas["FXOHLCVBar"]["properties"]
    assert "option_sec_type" in schemas["OHLCVRequest"]["properties"]
    assert "underlying_symbol" in schemas["OptionOHLCVBar"]["properties"]
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


def test_business_portfolio_risk_snapshot_aggregates_account_and_position_risk() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/portfolio/getRiskSnapshot",
            json={"account": "DU123", "use_ttl_cache": False},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["account"] == "DU123"
    assert payload["base_currency"] == "USD"
    assert payload["net_liquidation"] == 100000
    assert payload["total_cash_value"] == 25000
    assert payload["available_funds"] == 75000
    assert payload["excess_liquidity"] == 70000
    assert payload["cushion"] == 0.70
    assert payload["gross_position_value"] == 50000
    assert payload["leverage"] == 0.5
    assert payload["daily_pnl"] == 250
    assert payload["unrealized_pnl"] == 100
    assert payload["realized_pnl"] == 150
    assert [position["symbol"] for position in payload["positions"]] == ["AAPL", "EUR"]
    assert payload["positions"][0]["gross_exposure"] == 1600
    assert payload["positions"][0]["weight_of_net_liquidation"] == 0.016
    assert {row["key"]: row["gross_exposure"] for row in payload["exposures_by_asset_class"]} == {
        "CASH": 5500,
        "STK": 1600,
    }
    assert payload["exposures_by_currency"] == [
        {"key": "USD", "gross_exposure": 7100, "net_exposure": -3900, "market_value": -3900, "weight_of_net_liquidation": 0.071}
    ]
    assert payload["top_concentrations"][0]["symbol"] == "EUR"
    assert payload["warnings"] == []
    assert state.feed.account_summary_requests == ["DU123"]
    assert state.feed.portfolio_item_requests == ["DU123"]
    assert state.feed.account_pnl_requests == [("DU123", "", 1.2)]


def test_business_portfolio_risk_snapshot_uses_ttl_cache() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        first = client.post(
            "/api/v1/business/portfolio/getRiskSnapshot",
            json={"account": "DU123", "cache_ttl_seconds": 5},
        )
        second = client.post(
            "/api/v1/business/portfolio/getRiskSnapshot",
            json={"account": "DU123", "cache_ttl_seconds": 5},
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["net_liquidation"] == second.json()["net_liquidation"]
    assert state.feed.account_summary_requests == ["DU123"]


def test_business_portfolio_risk_snapshot_isolates_account_pnl_failure() -> None:
    state = FakeState()
    state.feed.raise_account_pnl = True
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/portfolio/getRiskSnapshot",
            json={"account": "DU123", "use_ttl_cache": False},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["net_liquidation"] == 100000
    assert payload["positions"][0]["symbol"] == "AAPL"
    assert payload["daily_pnl"] is None
    assert payload["warnings"] == ["account PnL unavailable: simulated PnL timeout"]


def test_business_portfolio_risk_snapshot_openapi_schema_is_registered() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    spec = app.openapi()

    operation = spec["paths"]["/api/v1/business/portfolio/getRiskSnapshot"]["post"]
    assert operation["tags"] == ["business"]
    content = operation["requestBody"]["content"]["application/json"]
    assert content["schema"]["$ref"] == "#/components/schemas/BusinessPortfolioRiskRequest"
    assert content["examples"]["account_risk"]["value"]["cache_ttl_seconds"] == 5
    response_schema = operation["responses"]["200"]["content"]["application/json"]["schema"]
    assert response_schema["$ref"] == "#/components/schemas/BusinessPortfolioRiskResponse"

    schemas = spec["components"]["schemas"]
    request_properties = schemas["BusinessPortfolioRiskRequest"]["properties"]
    response_properties = schemas["BusinessPortfolioRiskResponse"]["properties"]
    position_properties = schemas["BusinessPortfolioPosition"]["properties"]

    assert request_properties["include_account_pnl"]["default"] is True
    assert request_properties["include_positions"]["default"] is True
    assert request_properties["cache_ttl_seconds"]["default"] == 5
    assert "exposures_by_asset_class" in response_properties
    assert "top_concentrations" in response_properties
    assert "weight_of_net_liquidation" in position_properties


def test_business_swagger_examples_are_loaded_from_markdown() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    paths = app.openapi()["paths"]

    symbol_news_examples = paths["/api/v1/business/getSymbolNews"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    market_panel_examples = paths["/api/v1/business/getMarketPanel"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    portfolio_examples = paths["/api/v1/business/portfolio/getRiskSnapshot"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    returns_examples = paths["/api/v1/business/getReturns"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    option_skew_examples = paths["/api/v1/business/getOptionSkew"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    fixed_income_examples = paths["/api/v1/business/fixed-income/getBondFutureQuotes"]["post"]["requestBody"]["content"]["application/json"][
        "examples"
    ]
    ctd_examples = paths["/api/v1/business/fixed-income/getCTD"]["post"]["requestBody"]["content"]["application/json"]["examples"]
    curve_comparison_examples = paths["/api/v1/business/fixed-income/getCurveComparison"]["post"]["requestBody"]["content"][
        "application/json"
    ]["examples"]

    assert symbol_news_examples["tsla_news"]["value"]["symbol"] == "TSLA"
    assert market_panel_examples["us_equity_panel"]["value"]["symbols"] == ["SPY", "QQQ", "TSLA"]
    assert portfolio_examples["account_risk"]["value"]["account"] == "DU123456"
    assert returns_examples["us_equity_returns"]["value"]["bar_size"] == "5 mins"
    assert option_skew_examples["tsla_skew"]["value"]["primary_exchange"] == "NASDAQ"
    assert fixed_income_examples["ust_futures_quotes"]["value"]["market"] == "UST"
    assert ctd_examples["zn_ctd"]["value"]["future"]["futures_symbol"] == "ZN"
    assert "src.feeds.fixed_income_reference:provider" in ctd_examples["zn_ctd"]["description"]
    assert "FIXED_INCOME_REFERENCE_PROVIDER" in curve_comparison_examples["ust_curve_comparison"]["description"]


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


def test_business_symbol_news_rejects_unentitled_provider_codes() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/business/getSymbolNews",
            json={"symbol": "TSLA", "provider_codes": ["FLY"], "use_ttl_cache": False},
        )

    assert response.status_code == 422
    assert "not entitled" in response.json()["detail"]
    assert "FLY" in response.json()["detail"]
    assert state.feed.historical_news_requests == []


def test_reference_historical_news_rejects_unentitled_provider_codes() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/reference-data/news/historical",
            json={"con_id": 8314, "provider_codes": ["FLY"], "total_results": 5},
        )

    assert response.status_code == 422
    assert "not entitled" in response.json()["detail"]
    assert state.feed.historical_news_requests == []


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
    body = response.json()
    assert body["ibkr_connection"] == "disconnected"  # FakeFeed has no _ib
    assert body["redis_connection"] == "connected"  # FakeRedis.health_check returns True
    assert state.closed is True


def test_rest_app_state_shutdown_disconnects_ibkr_feed_before_redis() -> None:
    events: list[str] = []

    class DisconnectableFeed:
        async def disconnect(self) -> None:
            events.append("feed.disconnect")

    class CloseableRedis:
        async def close(self) -> None:
            events.append("redis.close")

    async def run() -> None:
        state = IBKRRestAppState(
            settings=Settings(),
            redis=CloseableRedis(),
            feed=DisconnectableFeed(),
            loader=object(),
            market_data_cache=AsyncTTLCache(ttl_seconds=60, max_size=16),
            event_contracts=object(),
        )

        await state.close()

    asyncio.run(run())

    assert events == ["feed.disconnect", "redis.close"]


def test_system_rate_limits_endpoint_returns_not_configured_for_fake_state() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)
    with TestClient(app) as client:
        response = client.get("/api/v1/system/rate-limits")

    assert response.status_code == 200
    assert response.json()["enabled"] is False


def test_system_readiness_reports_ready_when_ibkr_and_redis_are_connected() -> None:
    class ReadyFeed(FakeFeed):
        def connection_status(self) -> str:
            return "connected"

    state = FakeState()
    state.feed = ReadyFeed()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get("/api/v1/system/readiness")

    assert response.status_code == 200
    assert response.json()["status"] == "ready"
    assert response.json()["ibkr_connection"] == "connected"
    assert response.json()["redis_connection"] == "connected"


def test_system_readiness_reports_unavailable_when_ibkr_is_disconnected() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get("/api/v1/system/readiness")

    assert response.status_code == 503
    assert response.json()["status"] == "unavailable"
    assert response.json()["ibkr_connection"] == "disconnected"
    assert response.json()["redis_connection"] == "connected"


def test_system_readiness_reports_unavailable_when_redis_is_down() -> None:
    class ReadyFeed(FakeFeed):
        def connection_status(self) -> str:
            return "connected"

    class DownRedis(FakeRedis):
        async def health_check(self) -> bool:
            return False

    state = FakeState()
    state.feed = ReadyFeed()
    state.redis = DownRedis()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get("/api/v1/system/readiness")

    assert response.status_code == 503
    assert response.json()["status"] == "unavailable"
    assert response.json()["ibkr_connection"] == "connected"
    assert response.json()["redis_connection"] == "down"


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


def test_ohlcv_endpoint_auto_chunks_oversized_ibkr_request() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/market-data/ohlcv/equity",
            json={
                "symbol": "SPY",
                "duration": "2 D",
                "bar_size": "1 min",
                "end_datetime": "2026-01-03T21:00:00Z",
                "cache_latest": False,
                "use_ttl_cache": False,
            },
        )

    assert response.status_code == 200
    assert response.json()[0]["source"] == "range-test"
    assert state.loader.calls == 0
    request, start, end = state.feed.range_calls[0]
    assert getattr(request, "symbol") == "SPY"
    assert start == datetime(2026, 1, 1, 21, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 1, 3, 21, 0, tzinfo=timezone.utc)


def test_ohlcv_endpoint_rejects_auto_chunk_request_over_configured_cap() -> None:
    state = FakeState()
    state.settings = Settings(ibkr_historical_max_chunks=1)
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/market-data/ohlcv/equity",
            json={
                "symbol": "SPY",
                "duration": "2 D",
                "bar_size": "1 min",
                "end_datetime": "2026-01-03T21:00:00Z",
                "cache_latest": False,
                "use_ttl_cache": False,
            },
        )

    assert response.status_code == 422
    assert "exceeding configured max 1" in response.json()["detail"]
    assert state.loader.calls == 0
    assert state.feed.range_calls == []


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
        index = client.post("/api/v1/market-data/ohlcv/index", json={"symbol": "rut", "con_id": 123456})
        future = client.post(
            "/api/v1/market-data/ohlcv/futures",
            json={"symbol": "ES", "last_trade_date_or_contract_month": "202606"},
        )
        fx = client.post("/api/v1/market-data/ohlcv/fx", json={"symbol": "EURUSD"})
        bond = client.post("/api/v1/market-data/ohlcv/bond", json={"sec_id_type": "CUSIP", "sec_id": "91282CJN2"})

    assert equity.status_code == 200
    assert index.status_code == 200
    assert future.status_code == 200
    assert fx.status_code == 200
    assert bond.status_code == 200

    requests = state.loader.loaded_requests
    assert requests[0].asset_class is AssetClass.EQUITY
    assert requests[0].exchange == "SMART"
    assert requests[1].asset_class is AssetClass.INDEX
    assert requests[1].exchange == "CBOE"
    assert requests[1].con_id == 123456
    assert requests[2].asset_class is AssetClass.FUTURE
    assert requests[2].exchange == "CME"
    assert requests[2].last_trade_date_or_contract_month == "202606"
    assert future.json()[0]["contract_month"] is None
    assert future.json()[0]["is_continuous"] is False
    assert requests[3].asset_class is AssetClass.FX
    assert requests[3].exchange == "IDEALPRO"
    assert requests[3].what_to_show == "MIDPOINT"
    assert requests[3].use_rth is False
    assert fx.json()[0]["base_currency"] == "EUR"
    assert fx.json()[0]["quote_currency"] == "USD"
    assert requests[4].asset_class is AssetClass.BOND
    assert requests[4].symbol == "91282CJN2"
    assert requests[4].sec_id_type == "CUSIP"
    assert requests[4].sec_id == "91282CJN2"


def test_commodity_ohlcv_and_option_wrappers_forward_fop_contract_fields() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        future = client.post(
            "/api/v1/market-data/ohlcv/commodities",
            json={"symbol": "CL", "last_trade_date_or_contract_month": "202606", "use_ttl_cache": False},
        )
        option = client.post(
            "/api/v1/market-data/ohlcv/commodity-options",
            json={
                "underlying_symbol": "CL",
                "expiry": "20260617",
                "strike": 80,
                "right": "call",
                "multiplier": "1000",
                "use_ttl_cache": False,
            },
        )
        analytics = client.post(
            "/api/v1/market-data/commodities/options/analytics",
            json={
                "contract": {
                    "underlying_symbol": "CL",
                    "expiry": "20260617",
                    "strike": 80,
                    "right": "C",
                    "multiplier": "1000",
                },
                "use_ttl_cache": False,
            },
        )

    assert future.status_code == 200
    assert option.status_code == 200
    assert analytics.status_code == 200
    future_request = state.loader.loaded_requests[0]
    option_request = state.loader.loaded_requests[1]
    assert future_request.asset_class is AssetClass.FUTURE
    assert future_request.symbol == "CL"
    assert future_request.exchange == "NYMEX"
    assert future_request.currency == "USD"
    assert future_request.use_rth is False
    assert option_request.asset_class is AssetClass.OPTION
    assert option_request.option_sec_type == "FOP"
    assert option_request.underlying_symbol == "CL"
    assert option_request.right == "C"
    assert option.json()[0]["underlying_symbol"] == "CL"
    analytics_request = state.feed.option_analytics_requests[0]
    assert analytics_request.contract.sec_type == "FOP"
    assert analytics_request.contract.underlying_symbol == "CL"


def test_fx_option_ohlcv_wrapper_and_snapshot_collection() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        ohlcv = client.post(
            "/api/v1/market-data/ohlcv/fx-options",
            json={
                "symbol": "EURUSD",
                "expiry": "20260619",
                "strike": 1.10,
                "right": "call",
                "use_ttl_cache": False,
            },
        )
        capture = client.post(
            "/api/v1/snapshot/fx-options/capture",
            json={
                "contracts": [
                    {
                        "symbol": "EURUSD",
                        "expiry": "20260619",
                        "strike": 1.10,
                        "right": "C",
                    }
                ],
                "snapshot_wait_seconds": 0.01,
                "persist": True,
                "cache_latest": True,
            },
        )
        latest = client.get(
            "/api/v1/snapshot/fx-options/latest",
            params={"symbol": "EURUSD", "expiry": "20260619", "strike": 1.10, "right": "C"},
        )
        query = client.post("/api/v1/snapshot/fx-options/query", json={"symbol": "EURUSD", "expiry": "20260619"})

    assert ohlcv.status_code == 200
    fx_option_request = state.loader.loaded_requests[-1]
    assert fx_option_request.asset_class is AssetClass.OPTION
    assert fx_option_request.option_sec_type == "OPT"
    assert fx_option_request.underlying_symbol == "EUR"
    assert fx_option_request.currency == "USD"
    assert fx_option_request.right == "C"
    assert capture.status_code == 200
    assert capture.json()["captured"] == 1
    assert state.feed.fx_option_snapshot_requests[0][0].underlying_symbol == "EUR"
    assert capture.json()["persisted"] == 0
    assert latest.status_code == 200
    assert latest.json()["symbol"] == "EURUSD"
    assert query.status_code == 410


def test_equity_snapshot_capture_preserves_symbol_identity_and_cleans_up_tickers() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/snapshot/capture",
            json={"symbols": ["AAPL", "MSFT"], "persist": True, "cache_latest": True},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["symbols_requested"] == 2
    assert body["symbols_captured"] == 1
    assert body["symbols_failed"] == 1
    assert body["failed_symbols"] == ["AAPL"]
    assert body["snapshots"][0]["symbol"] == "MSFT"
    assert state.redis.equity_snapshots["MSFT"].symbol == "MSFT"
    assert len(state.feed.cancelled_equity_tickers) == 1


def test_commodity_metadata_ticks_news_and_business_front_forward_contracts() -> None:
    state = FakeState()
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        metadata = client.post(
            "/api/v1/market-data/commodities/metadata",
            json={
                "symbol": "CL",
                "last_trade_date_or_contract_month": "202606",
                "include_trading_schedule": True,
                "use_ttl_cache": False,
            },
        )
        ticks = client.post(
            "/api/v1/market-data/commodities/historical-ticks",
            json={
                "symbol": "GC",
                "last_trade_date_or_contract_month": "202606",
                "start_date": "2026-05-18T00:00:00Z",
                "end_date": "2026-05-18T01:00:00Z",
            },
        )
        news = client.post(
            "/api/v1/market-data/commodities/news",
            json={
                "symbol": "CL",
                "last_trade_date_or_contract_month": "202606",
                "provider_codes": ["BZ"],
                "use_ttl_cache": False,
            },
        )
        business = client.post(
            "/api/v1/business/commodities/getFutures",
            json={"symbol": "GC", "as_of_date": "2026-05-18", "forward_count": 1, "use_ttl_cache": False},
        )

    assert metadata.status_code == 200
    assert metadata.json()["con_id"] == 8314
    assert metadata.json()["market_rule_ids"] == [26]
    assert metadata.json()["market_rules"][0]["increments"][0]["increment"] == 0.01
    assert ticks.status_code == 200
    assert state.feed.historical_tick_requests[0].symbol == "GC"
    assert state.feed.historical_tick_requests[0].exchange == "COMEX"
    assert news.status_code == 200
    assert news.json()["provider_codes"] == ["BZ"]
    assert business.status_code == 200
    contracts = business.json()["contracts"]
    assert [item["role"] for item in contracts] == ["front", "forward_1"]
    assert [item["contract_month"] for item in contracts] == ["202606", "202608"]


def test_crude_oil_contract_months_skip_expired_front_after_last_trade_date() -> None:
    assert business_router._nymex_crude_oil_last_trade_date("202606") == datetime(2026, 5, 19).date()
    assert business_router._commodity_contract_months("CL", datetime(2026, 5, 19).date(), 2) == ("202606", "202607")
    assert business_router._commodity_contract_months("CL", datetime(2026, 5, 20).date(), 2) == ("202607", "202608")


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


# ── API-wide bearer token auth tests ────────────────────────────────────────


def test_api_bearer_auth_disabled_by_default() -> None:
    """When IBKR_API_BEARER_TOKEN is empty (default), all endpoints are open."""
    state = FakeState()
    assert state.settings.ibkr_api_bearer_token == ""
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get("/api/v1/system/health")

    assert response.status_code == 200


def test_api_bearer_auth_enabled_allows_valid_token() -> None:
    """When IBKR_API_BEARER_TOKEN is set, valid token grants access."""
    state = FakeState()
    state.settings = Settings(
        ibkr_rest_app_name="IBKRRestAppTest",
        ibkr_rest_market_data_ttl_seconds=60,
        ibkr_rest_market_data_cache_maxsize=16,
        ibkr_api_bearer_token="secret-api-key",
    )
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/system/health",
            headers={"Authorization": "Bearer secret-api-key"},
        )

    assert response.status_code == 200


def test_api_bearer_auth_enabled_rejects_missing_token() -> None:
    """When IBKR_API_BEARER_TOKEN is set, requests without token get 401."""
    state = FakeState()
    state.settings = Settings(
        ibkr_rest_app_name="IBKRRestAppTest",
        ibkr_rest_market_data_ttl_seconds=60,
        ibkr_rest_market_data_cache_maxsize=16,
        ibkr_api_bearer_token="secret-api-key",
    )
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get("/api/v1/system/health")

    assert response.status_code == 401
    assert response.json()["detail"] == "API bearer token required"
    assert "Bearer" in response.headers.get("www-authenticate", "")


def test_api_bearer_auth_enabled_rejects_wrong_token() -> None:
    """When IBKR_API_BEARER_TOKEN is set, wrong token gets 401."""
    state = FakeState()
    state.settings = Settings(
        ibkr_rest_app_name="IBKRRestAppTest",
        ibkr_rest_market_data_ttl_seconds=60,
        ibkr_rest_market_data_cache_maxsize=16,
        ibkr_api_bearer_token="secret-api-key",
    )
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/system/health",
            headers={"Authorization": "Bearer wrong-key"},
        )

    assert response.status_code == 401
    assert response.json()["detail"] == "invalid API bearer token"


def test_api_bearer_auth_enabled_rejects_empty_token() -> None:
    """When IBKR_API_BEARER_TOKEN is set, empty bearer token gets 401."""
    state = FakeState()
    state.settings = Settings(
        ibkr_rest_app_name="IBKRRestAppTest",
        ibkr_rest_market_data_ttl_seconds=60,
        ibkr_rest_market_data_cache_maxsize=16,
        ibkr_api_bearer_token="secret-api-key",
    )
    app = create_app(settings=state.settings, state=state)

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/system/health",
            headers={"Authorization": "Bearer "},
        )

    assert response.status_code == 401
