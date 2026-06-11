"""Tests for tick data models, market data extensions sub-client, and tick data router."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from src.feeds.contracts import ContractSpec
from src.feeds.exceptions import IBKRMarketDataLeaseTimeoutError, IBKRMarketDataUnavailableError
from src.feeds.tick_data import (
    HeadTimestampRequest,
    HistogramDataPoint,
    HistogramDataRequest,
    HistogramDataResponse,
    HistoricalTickRequest,
    HistoricalTickResponse,
    IVCalcRequest,
    MarketDepthLevel,
    MarketDepthSnapshot,
    MarketRule,
    OptionPriceCalcRequest,
    PriceIncrement,
    SmartComponent,
    SymbolDescription,
    TickByTickData,
    TickSubscribeRequest,
    TickType,
    TickUnsubscribeRequest,
)
from src.feeds.ibkr_marketdata_ext import (
    IBKRMarketDataExtClient,
    _normalize_histogram_data_point,
    _normalize_ibkr_tick,
    _what_to_show_to_tick_type,
    _parse_tick_timestamp,
)
from src.transport.metrics import metrics


# ---------------------------------------------------------------------------
# Data model tests
# ---------------------------------------------------------------------------


class TestTickByTickData:
    def test_valid_tick(self) -> None:
        tick = TickByTickData(
            timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            tick_type=TickType.ALL_LAST,
            price=100.0,
            size=10,
            exchange="NYSE",
        )
        assert tick.price == 100.0
        assert tick.tick_type == TickType.ALL_LAST

    def test_timestamp_normalized_to_utc(self) -> None:
        tick = TickByTickData(
            timestamp=datetime(2026, 1, 1, 12, 0),
            tick_type=TickType.LAST,
            price=50.0,
        )
        assert tick.timestamp.tzinfo is not None

    def test_timestamp_from_iso_string(self) -> None:
        tick = TickByTickData(
            timestamp="2026-01-01T12:00:00Z",
            tick_type=TickType.BID_ASK,
            bid=99.0,
            ask=101.0,
        )
        assert tick.timestamp.year == 2026

    def test_bid_ask_tick(self) -> None:
        tick = TickByTickData(
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            tick_type=TickType.BID_ASK,
            bid=99.5,
            ask=100.5,
            size=None,
        )
        assert tick.bid == 99.5
        assert tick.ask == 100.5
        assert tick.price is None

    def test_extra_fields_forbidden(self) -> None:
        with pytest.raises(Exception):
            TickByTickData(
                timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
                tick_type=TickType.LAST,
                unknown_field="fail",
            )


class TestHistoricalTickRequest:
    def test_valid_request(self) -> None:
        req = HistoricalTickRequest(
            symbol="AAPL",
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        assert req.symbol == "AAPL"
        assert req.sec_type == "STK"
        assert req.what_to_show == "TRADES"

    def test_datetime_normalization(self) -> None:
        req = HistoricalTickRequest(
            symbol="SPY",
            start_date="2026-01-01T00:00:00Z",
            end_date="2026-01-02T00:00:00Z",
        )
        assert req.start_date.tzinfo is not None

    def test_max_ticks_bounds(self) -> None:
        with pytest.raises(Exception):
            HistoricalTickRequest(
                symbol="AAPL",
                start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
                end_date=datetime(2026, 1, 2, tzinfo=timezone.utc),
                max_ticks=0,
            )


class TestMarketRule:
    def test_valid_market_rule(self) -> None:
        rule = MarketRule(
            price_magnitude=1,
            increments=[
                PriceIncrement(low_edge=0.0, increment=0.01),
                PriceIncrement(low_edge=1.0, increment=0.05),
            ],
        )
        assert len(rule.increments) == 2
        assert rule.increments[0].increment == 0.01


class TestSmartComponent:
    def test_valid_component(self) -> None:
        comp = SmartComponent(exchange="NYSE", con_id=123, description="New York Stock Exchange")
        assert comp.exchange == "NYSE"


class TestMarketDepthSnapshot:
    def test_valid_depth_snapshot(self) -> None:
        snapshot = MarketDepthSnapshot(
            symbol="AAPL",
            asset_class="equity",
            exchange="SMART",
            currency="USD",
            primary_exchange="NASDAQ",
            con_id=265598,
            sec_type="STK",
            num_rows=5,
            is_smart_depth=True,
            snapshot_wait_seconds=1.5,
            received_at=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            bids=[MarketDepthLevel(position=0, price=100.0, size=10, market_maker="ARCA")],
            asks=[MarketDepthLevel(position=0, price=100.1, size=12, market_maker="ISLAND")],
        )

        assert snapshot.symbol == "AAPL"
        assert snapshot.asset_class == "equity"
        assert snapshot.bids[0].market_maker == "ARCA"


class TestSymbolDescription:
    def test_valid_description(self) -> None:
        desc = SymbolDescription(
            con_id=265598,
            symbol="AAPL",
            name="APPLE INC",
            sec_type="STK",
            exchange="NASDAQ",
        )
        assert desc.symbol == "AAPL"
        assert desc.industry is None


class TestIVCalcRequest:
    def test_valid_iv_request(self) -> None:
        req = IVCalcRequest(
            symbol="AAPL",
            expiry="20260619",
            strike=200,
            right="C",
            option_price=5.50,
            under_price=195.0,
        )
        assert req.option_price == 5.50

    def test_negative_price_rejected(self) -> None:
        with pytest.raises(Exception):
            IVCalcRequest(
                symbol="AAPL",
                expiry="20260619",
                strike=200,
                right="C",
                option_price=-1.0,
                under_price=195.0,
            )


class TestOptionPriceCalcRequest:
    def test_valid_request(self) -> None:
        req = OptionPriceCalcRequest(
            symbol="SPY",
            expiry="20260619",
            strike=500,
            right="P",
            volatility=0.25,
            under_price=495.0,
        )
        assert req.volatility == 0.25


class TestHeadTimestampRequest:
    def test_defaults(self) -> None:
        req = HeadTimestampRequest(symbol="AAPL")
        assert req.sec_type == "STK"
        assert req.what_to_show == "TRADES"


class TestHistogramData:
    def test_request_defaults(self) -> None:
        req = HistogramDataRequest(symbol="AAPL")
        assert req.sec_type == "STK"
        assert req.period == "1 week"
        assert req.use_rth is True

    def test_valid_point(self) -> None:
        point = HistogramDataPoint(price=100.0, size=50)
        assert point.price == 100.0
        assert point.size == 50


class TestTickSubscribeRequest:
    def test_defaults(self) -> None:
        req = TickSubscribeRequest(symbol="AAPL")
        assert req.tick_type == TickType.ALL_LAST
        assert req.max_ticks == 10_000


class TestTickUnsubscribeRequest:
    def test_minimal(self) -> None:
        req = TickUnsubscribeRequest(symbol="AAPL")
        assert req.sec_type == "STK"


# ---------------------------------------------------------------------------
# Helper / normalization tests
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_what_to_show_mapping(self) -> None:
        assert _what_to_show_to_tick_type("TRADES") == TickType.ALL_LAST
        assert _what_to_show_to_tick_type("BID_ASK") == TickType.BID_ASK
        assert _what_to_show_to_tick_type("MIDPOINT") == TickType.MIDPOINT
        assert _what_to_show_to_tick_type("UNKNOWN") == TickType.ALL_LAST

    def test_parse_tick_timestamp_from_datetime(self) -> None:
        dt = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        assert _parse_tick_timestamp(dt) == dt

    def test_parse_tick_timestamp_from_float(self) -> None:
        ts = _parse_tick_timestamp(1735689600.0)
        assert isinstance(ts, datetime)
        assert ts.tzinfo is not None

    def test_parse_tick_timestamp_from_string(self) -> None:
        ts = _parse_tick_timestamp("2026-01-01T12:00:00Z")
        assert ts.year == 2026

    def test_normalize_ibkr_tick_trades(self) -> None:
        ibkr_tick = SimpleNamespace(
            time=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            price=100.0,
            size=50,
            exchange="NYSE",
            specialConditions="",
        )
        tick = _normalize_ibkr_tick(ibkr_tick, TickType.ALL_LAST)
        assert tick.price == 100.0
        assert tick.size == 50
        assert tick.tick_type == TickType.ALL_LAST
        assert tick.bid is None

    def test_normalize_ibkr_tick_bidask(self) -> None:
        ibkr_tick = SimpleNamespace(
            time=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            priceBid=99.5,
            priceAsk=100.5,
            sizeBid=100,
            sizeAsk=200,
            exchange="NYSE",
        )
        tick = _normalize_ibkr_tick(ibkr_tick, TickType.BID_ASK)
        assert tick.bid == 99.5
        assert tick.ask == 100.5
        assert tick.price is None

    def test_normalize_histogram_point_accepts_size(self) -> None:
        point = _normalize_histogram_data_point(SimpleNamespace(price=100.25, size=42))
        assert point.price == pytest.approx(100.25)
        assert point.size == pytest.approx(42)

    def test_normalize_histogram_point_accepts_count(self) -> None:
        point = _normalize_histogram_data_point(SimpleNamespace(price=99.75, count=17))
        assert point.price == pytest.approx(99.75)
        assert point.size == pytest.approx(17)


# ---------------------------------------------------------------------------
# Sub-client tests (with mocked IB)
# ---------------------------------------------------------------------------


class _FakeConnection:
    """Minimal connection manager mock for sub-client tests."""

    def __init__(self, ib: Any | None = None) -> None:
        self._ib_instance = ib
        self._pacing_guard = _FakePacingGuard()

    @property
    def ib(self) -> Any | None:
        return self._ib_instance

    @property
    def pacing_guard(self) -> _FakePacingGuard:
        return self._pacing_guard

    async def ensure_connected(self) -> None:
        if self._ib_instance is None:
            raise RuntimeError("not connected")

    async def with_retry(self, call: Any, *, operation: str) -> Any:
        return await call()

    def is_transient_error(self, exc: BaseException) -> bool:
        return isinstance(exc, (ConnectionError, OSError))


class _FakeMarketDataLease:
    def __init__(self) -> None:
        self.released = False

    async def release(self) -> None:
        self.released = True


class _LeaseConnection(_FakeConnection):
    def __init__(self, ib: Any | None = None, *, lease: _FakeMarketDataLease | None = None) -> None:
        super().__init__(ib)
        self.lease = lease or _FakeMarketDataLease()

    async def acquire_market_data_line(
        self,
        *,
        contract_key: str,
        operation: str,
        ttl_seconds: float | None = None,
    ) -> _FakeMarketDataLease:
        return self.lease


class _SlowLeaseConnection(_FakeConnection):
    async def acquire_market_data_line(
        self,
        *,
        contract_key: str,
        operation: str,
        ttl_seconds: float | None = None,
    ) -> _FakeMarketDataLease:
        await asyncio.sleep(0.1)
        return _FakeMarketDataLease()


class _FakePacingGuard:
    async def acquire(self, request: Any) -> None:
        pass

    def release(self) -> None:
        pass


class FakeIBTickByTick:
    """IB mock that supports tick-by-tick data."""

    def __init__(self) -> None:
        self.cancelled: list[Any] = []
        self._tick_handlers: list[Any] = []

    async def reqTickByTickDataAsync(
        self,
        contract: Any,
        tick_type: str,
        numberOfTicks: int = 0,
        ignoreSize: bool = True,
    ) -> SimpleNamespace:
        handle = SimpleNamespace(contract=contract, tick_type=tick_type)
        self._tick_handlers.append(handle)
        return handle

    def cancelTickByTickData(self, handle: Any) -> None:
        self.cancelled.append(handle)

    def isConnected(self) -> bool:
        return True


class FakeIBMarketDepth:
    """IB mock that supports market depth snapshots."""

    def __init__(self) -> None:
        self.requests: list[tuple[Any, int, bool, list[Any]]] = []
        self.cancelled: list[tuple[Any, bool]] = []

    def reqMktDepth(
        self,
        contract: Any,
        *,
        numRows: int,
        isSmartDepth: bool,
        mktDepthOptions: list[Any],
    ) -> SimpleNamespace:
        self.requests.append((contract, numRows, isSmartDepth, mktDepthOptions))
        return SimpleNamespace(
            domBids=[
                SimpleNamespace(price=100.0, size=10, marketMaker="ARCA"),
                SimpleNamespace(price=99.9, size=20, marketMaker="NYSE"),
            ],
            domAsks=[
                SimpleNamespace(price=100.1, size=12, marketMaker="ISLAND"),
            ],
            domTicks=[SimpleNamespace()],
        )

    def cancelMktDepth(self, contract: Any, *, isSmartDepth: bool = False) -> None:
        self.cancelled.append((contract, isSmartDepth))

    def isConnected(self) -> bool:
        return True


class FakeIBEmptyMarketDepth(FakeIBMarketDepth):
    """IB mock that returns a market-depth ticker with no DOM levels."""

    def reqMktDepth(
        self,
        contract: Any,
        *,
        numRows: int,
        isSmartDepth: bool,
        mktDepthOptions: list[Any],
    ) -> SimpleNamespace:
        self.requests.append((contract, numRows, isSmartDepth, mktDepthOptions))
        return SimpleNamespace(domBids=[], domAsks=[], domTicks=[])


class FakeIBCancelFailsMarketDepth(FakeIBMarketDepth):
    """IB mock that raises while cancelling an otherwise successful DOM subscription."""

    def cancelMktDepth(self, contract: Any, *, isSmartDepth: bool = False) -> None:
        super().cancelMktDepth(contract, isSmartDepth=isSmartDepth)
        raise RuntimeError("cancel failed")


def _metric_value(exposition: str, metric_prefix: str) -> float:
    pattern = re.compile(rf"^{re.escape(metric_prefix)}\s+([0-9.]+)$", re.MULTILINE)
    match = pattern.search(exposition)
    return float(match.group(1)) if match else 0.0


class TestIBKRMarketDataExtTickByTick:
    def test_start_and_get_ticks(self) -> None:
        fake_ib = FakeIBTickByTick()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        handle = asyncio.run(
            client.start_tick_by_tick("AAPL", tick_type=TickType.LAST, max_ticks=100)
        )
        assert handle is not None

        # Manually inject a tick into the buffer
        key = ("AAPL", "STK", "SMART")
        tick = TickByTickData(
            timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            tick_type=TickType.LAST,
            price=150.0,
            size=10,
        )
        client._tick_buffers[key].append(tick)

        ticks = client.get_latest_ticks("AAPL", n=10)
        assert len(ticks) == 1
        assert ticks[0].price == 150.0

    def test_stop_subscription(self) -> None:
        fake_ib = FakeIBTickByTick()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        handle = asyncio.run(
            client.start_tick_by_tick("SPY", tick_type=TickType.ALL_LAST)
        )
        assert ("SPY", "STK", "SMART") in client._tick_subscriptions

        asyncio.run(client.stop_tick_by_tick("SPY"))
        assert ("SPY", "STK", "SMART") not in client._tick_subscriptions
        assert len(fake_ib.cancelled) == 1

    def test_get_latest_ticks_empty(self) -> None:
        conn = _FakeConnection(FakeIBTickByTick())
        client = IBKRMarketDataExtClient(conn)
        ticks = client.get_latest_ticks("UNKNOWN")
        assert ticks == []

    def test_list_active_subscriptions(self) -> None:
        fake_ib = FakeIBTickByTick()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        asyncio.run(client.start_tick_by_tick("AAPL"))
        asyncio.run(client.start_tick_by_tick("SPY"))

        subs = client.list_active_subscriptions()
        symbols = {s["symbol"] for s in subs}
        assert symbols == {"AAPL", "SPY"}

    def test_on_tick_callback(self) -> None:
        fake_ib = FakeIBTickByTick()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        received: list[TickByTickData] = []

        asyncio.run(
            client.start_tick_by_tick(
                "MSFT",
                tick_type=TickType.LAST,
                on_tick=lambda t: received.append(t),
            )
        )

        # Simulate tick arrival via buffer
        tick = TickByTickData(
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            tick_type=TickType.LAST,
            price=300.0,
        )
        key = ("MSFT", "STK", "SMART")
        client._tick_buffers[key].append(tick)
        # Callback would fire in real scenario via ib_insync event
        # Here we just test the buffer append worked
        ticks = client.get_latest_ticks("MSFT")
        assert len(ticks) == 1


class TestIBKRMarketDataExtMarketDepth:
    @staticmethod
    def _contract_and_spec() -> tuple[Any, ContractSpec]:
        contract = SimpleNamespace(
            conId=265598,
            symbol="AAPL",
            secType="STK",
            exchange="SMART",
            currency="USD",
            primaryExchange="NASDAQ",
        )
        spec = ContractSpec(
            symbol="AAPL",
            asset_class="equity",
            exchange="SMART",
            currency="USD",
            primary_exchange="NASDAQ",
        )
        return contract, spec

    def test_load_market_depth_snapshot_requests_and_cancels(self) -> None:
        fake_ib = FakeIBMarketDepth()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)
        contract, spec = self._contract_and_spec()

        snapshot = asyncio.run(
            client.load_market_depth_snapshot(
                contract=contract,
                spec=spec,
                num_rows=5,
                is_smart_depth=True,
                snapshot_wait_seconds=0.001,
            )
        )

        assert snapshot.symbol == "AAPL"
        assert snapshot.con_id == 265598
        assert len(snapshot.bids) == 2
        assert snapshot.asks[0].price == pytest.approx(100.1)
        assert fake_ib.requests[0][1] == 5
        assert fake_ib.requests[0][2] is True
        assert fake_ib.cancelled == [(contract, True)]

    def test_load_market_depth_snapshot_empty_book_is_unavailable(self) -> None:
        fake_ib = FakeIBEmptyMarketDepth()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)
        contract, spec = self._contract_and_spec()

        with pytest.raises(IBKRMarketDataUnavailableError, match="No market depth levels"):
            asyncio.run(
                client.load_market_depth_snapshot(
                    contract=contract,
                    spec=spec,
                    snapshot_wait_seconds=0.001,
                )
            )

        assert fake_ib.cancelled == [(contract, False)]

    def test_load_market_depth_snapshot_lease_timeout_is_backpressure(self) -> None:
        fake_ib = FakeIBMarketDepth()
        conn = _SlowLeaseConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)
        contract, spec = self._contract_and_spec()

        with pytest.raises(IBKRMarketDataLeaseTimeoutError, match="line unavailable"):
            asyncio.run(
                client.load_market_depth_snapshot(
                    contract=contract,
                    spec=spec,
                    snapshot_wait_seconds=0.001,
                    request_timeout_seconds=1.0,
                    lease_wait_seconds=0.001,
                )
            )

        assert fake_ib.requests == []

    def test_load_market_depth_snapshot_cancel_failure_releases_lease_and_records_metric(self) -> None:
        fake_ib = FakeIBCancelFailsMarketDepth()
        lease = _FakeMarketDataLease()
        conn = _LeaseConnection(fake_ib, lease=lease)
        client = IBKRMarketDataExtClient(conn)
        contract, spec = self._contract_and_spec()
        before = _metric_value(
            metrics.expose(),
            'market_depth_cleanup_failures_total{operation="cancelMktDepth"}',
        )

        snapshot = asyncio.run(
            client.load_market_depth_snapshot(
                contract=contract,
                spec=spec,
                snapshot_wait_seconds=0.001,
            )
        )

        after = _metric_value(
            metrics.expose(),
            'market_depth_cleanup_failures_total{operation="cancelMktDepth"}',
        )
        assert snapshot.symbol == "AAPL"
        assert lease.released is True
        assert after == before + 1


class FakeIBHistoricalTicks:
    """IB mock for historical tick data."""

    def __init__(self, ticks: list[Any] | None = None) -> None:
        self._ticks = ticks or []
        self.call_count = 0

    async def reqHistoricalTicksAsync(self, *args: Any, **kwargs: Any) -> list[Any]:
        self.call_count += 1
        if self.call_count == 1:
            return self._ticks
        return []  # Second call returns empty to stop pagination


class TestIBKRMarketDataExtHistoricalTicks:
    def test_load_historical_ticks_basic(self) -> None:
        ibkr_ticks = [
            SimpleNamespace(
                time=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
                price=100.0,
                size=50,
                exchange="NYSE",
                specialConditions="",
            ),
            SimpleNamespace(
                time=datetime(2026, 1, 1, 12, 1, tzinfo=timezone.utc),
                price=101.0,
                size=30,
                exchange="NYSE",
                specialConditions="",
            ),
        ]
        fake_ib = FakeIBHistoricalTicks(ibkr_ticks)
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        request = HistoricalTickRequest(
            symbol="AAPL",
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        response = asyncio.run(client.load_historical_ticks(request))
        assert isinstance(response, HistoricalTickResponse)
        assert response.total_count == 2
        assert response.truncated is False
        assert response.ticks[0].price == 100.0

    def test_load_historical_ticks_truncation(self) -> None:
        base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ibkr_ticks = [
            SimpleNamespace(
                time=base + timedelta(seconds=i),
                price=100.0 + i,
                size=10,
                exchange="NYSE",
                specialConditions="",
            )
            for i in range(1000)
        ]
        fake_ib = FakeIBHistoricalTicks(ibkr_ticks)
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        request = HistoricalTickRequest(
            symbol="AAPL",
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2026, 1, 2, tzinfo=timezone.utc),
            max_ticks=50,
        )
        response = asyncio.run(client.load_historical_ticks(request))
        assert response.truncated is True
        assert response.total_count == 50


class FakeIBHistogram:
    """IB mock for histogram data."""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, bool, str]] = []

    async def reqHistogramDataAsync(self, contract: Any, useRTH: bool, period: str) -> list[SimpleNamespace]:
        self.calls.append((contract, useRTH, period))
        return [
            SimpleNamespace(price=100.0, size=25),
            SimpleNamespace(price=101.0, count=30),
        ]


class TestIBKRMarketDataExtHistogram:
    def test_load_histogram_data_uses_period_argument(self) -> None:
        fake_ib = FakeIBHistogram()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        request = HistogramDataRequest(
            symbol="AAPL",
            use_rth=False,
            period="3 days",
        )
        response = asyncio.run(client.load_histogram_data(request))

        assert isinstance(response, HistogramDataResponse)
        assert response.total_count == 2
        assert response.period == "3 days"
        assert response.use_rth is False
        assert response.data[0].size == pytest.approx(25)
        assert response.data[1].size == pytest.approx(30)
        assert len(fake_ib.calls) == 1
        _, use_rth, period = fake_ib.calls[0]
        assert use_rth is False
        assert period == "3 days"


class FakeIBMarketRule:
    """IB mock for market rule."""

    def __init__(self) -> None:
        pass

    async def reqMarketRuleAsync(self, price_magnitude: int) -> SimpleNamespace:
        return SimpleNamespace(
            priceIncrements=[
                SimpleNamespace(lowEdge=0.0, increment=0.01),
                SimpleNamespace(lowEdge=1.0, increment=0.05),
            ]
        )


class TestIBKRMarketDataExtMarketRule:
    def test_load_market_rule(self) -> None:
        fake_ib = FakeIBMarketRule()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        rule = asyncio.run(client.load_market_rule(1))
        assert isinstance(rule, MarketRule)
        assert rule.price_magnitude == 1
        assert len(rule.increments) == 2
        assert rule.increments[0].low_edge == 0.0
        assert rule.increments[0].increment == 0.01


class FakeIBSmartComponents:
    """IB mock for smart components."""

    async def reqSmartComponentsAsync(self, exchange: str) -> list[SimpleNamespace]:
        return [
            SimpleNamespace(exchange="NYSE", conId=1, description="New York Stock Exchange"),
            SimpleNamespace(exchange="NASDAQ", conId=2, description="NASDAQ"),
        ]


class TestIBKRMarketDataExtSmartComponents:
    def test_load_smart_components(self) -> None:
        fake_ib = FakeIBSmartComponents()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        components = asyncio.run(client.load_smart_components("SMART"))
        assert len(components) == 2
        assert isinstance(components[0], SmartComponent)
        assert components[0].exchange == "NYSE"


class FakeIBHeadTimestamp:
    """IB mock for head timestamp."""

    async def reqHeadTimeStampAsync(
        self, contract: Any, *, whatToShow: str, useRTH: bool, formatDate: int
    ) -> str:
        return "20100101 00:00:00"


class TestIBKRMarketDataExtHeadTimestamp:
    def test_load_head_timestamp(self) -> None:
        fake_ib = FakeIBHeadTimestamp()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        request = HeadTimestampRequest(symbol="AAPL")
        ts = asyncio.run(client.load_head_timestamp(request))
        assert ts is not None
        assert isinstance(ts, datetime)
        assert ts.year == 2010


class FakeIBImpliedVol:
    """IB mock for IV/option price calculations."""

    async def calculateImpliedVolatilityAsync(
        self, contract: Any, *, optionPrice: float, underPrice: float
    ) -> tuple[float, str]:
        return (0.25, "")

    async def calculateOptionPriceAsync(
        self, contract: Any, *, volatility: float, underPrice: float
    ) -> tuple[float, str]:
        return (5.50, "")


class TestIBKRMarketDataExtIVCalc:
    def test_calculate_iv(self) -> None:
        fake_ib = FakeIBImpliedVol()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        contract = SimpleNamespace(conId=123, symbol="AAPL", secType="OPT")
        iv = asyncio.run(client.calculate_iv(contract, 5.50, 195.0))
        assert iv == pytest.approx(0.25)

    def test_calculate_option_price(self) -> None:
        fake_ib = FakeIBImpliedVol()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        contract = SimpleNamespace(conId=123, symbol="AAPL", secType="OPT")
        price = asyncio.run(client.calculate_option_price(contract, 0.25, 195.0))
        assert price == pytest.approx(5.50)


class FakeIBSymbolSearch:
    """IB mock for symbol search."""

    async def reqMatchingSymbolsAsync(self, pattern: str) -> list[SimpleNamespace] | None:
        if pattern == "AAPL":
            return [
                SimpleNamespace(
                    conId=265598,
                    symbol="AAPL",
                    name="APPLE INC",
                    secType="STK",
                    primaryExchange="NASDAQ",
                    listingExchange=None,
                    industry="Technology",
                    category="Computers",
                    subcategory=None,
                ),
            ]
        return None


class TestIBKRMarketDataExtSymbolSearch:
    def test_search_matching_symbols(self) -> None:
        fake_ib = FakeIBSymbolSearch()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        results = asyncio.run(client.search_matching_symbols("AAPL"))
        assert len(results) == 1
        assert isinstance(results[0], SymbolDescription)
        assert results[0].symbol == "AAPL"
        assert results[0].con_id == 265598

    def test_search_matching_symbols_empty(self) -> None:
        fake_ib = FakeIBSymbolSearch()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        results = asyncio.run(client.search_matching_symbols("ZZZZZ"))
        assert results == []


class TestCancelAllSubscriptions:
    def test_cancel_all(self) -> None:
        fake_ib = FakeIBTickByTick()
        conn = _FakeConnection(fake_ib)
        client = IBKRMarketDataExtClient(conn)

        asyncio.run(client.start_tick_by_tick("AAPL"))
        asyncio.run(client.start_tick_by_tick("SPY"))
        assert len(client._tick_subscriptions) == 2

        asyncio.run(client.cancel_all_subscriptions())
        assert len(client._tick_subscriptions) == 0
        assert len(client._tick_buffers) == 0


# ---------------------------------------------------------------------------
# Facade delegation tests
# ---------------------------------------------------------------------------


class TestFacadeDelegation:
    """Verify IBKRFeedClient delegates to IBKRMarketDataExtClient."""

    def test_facade_has_marketdata_ext(self) -> None:
        from src.feeds.ibkr_feed import IBKRFeedClient

        client = IBKRFeedClient()
        assert hasattr(client, "_marketdata_ext")
        assert isinstance(client._marketdata_ext, IBKRMarketDataExtClient)

    def test_facade_exposes_tick_methods(self) -> None:
        from src.feeds.ibkr_feed import IBKRFeedClient

        client = IBKRFeedClient()
        assert callable(getattr(client, "start_tick_by_tick", None))
        assert callable(getattr(client, "stop_tick_by_tick", None))
        assert callable(getattr(client, "get_latest_ticks", None))
        assert callable(getattr(client, "load_historical_ticks", None))
        assert callable(getattr(client, "load_histogram_data", None))
        assert callable(getattr(client, "load_market_rule", None))
        assert callable(getattr(client, "load_smart_components", None))
        assert callable(getattr(client, "load_head_timestamp", None))
        assert callable(getattr(client, "calculate_iv", None))
        assert callable(getattr(client, "calculate_option_price", None))
        assert callable(getattr(client, "search_matching_symbols", None))


# ---------------------------------------------------------------------------
# Router tests (with test client)
# ---------------------------------------------------------------------------


class TestTickDataRouter:
    def test_router_registers_endpoints(self) -> None:
        from fastapi.testclient import TestClient
        from src.config.settings import Settings
        from src.webapp.app import create_app
        from src.webapp.cache import AsyncTTLCache
        from src.feeds.models import OHLCVBar

        class _FakeFeed:
            pass

        class _FakeState:
            def __init__(self) -> None:
                self.settings = Settings(
                    ibkr_rest_app_name="Test",
                    ibkr_rest_market_data_ttl_seconds=60,
                    ibkr_rest_market_data_cache_maxsize=16,
                )
                self.feed = _FakeFeed()
                self.redis = SimpleNamespace(health_check=lambda: True)
                self.market_data_cache = AsyncTTLCache(ttl_seconds=60, max_size=16)

            async def connect(self) -> None:
                pass

            async def close(self) -> None:
                pass

        state = _FakeState()
        app = create_app(settings=state.settings, state=state)

        paths = {route.path for route in app.routes}
        assert "/api/v1/tick-data/subscribe" in paths
        assert "/api/v1/tick-data/unsubscribe" in paths
        assert "/api/v1/tick-data/latest/{symbol}" in paths
        assert "/api/v1/tick-data/historical" in paths
        assert "/api/v1/tick-data/histogram" in paths
        assert "/api/v1/tick-data/market-rules/{magnitude}" in paths
        assert "/api/v1/tick-data/smart-components/{exchange}" in paths
        assert "/api/v1/tick-data/head-timestamp" in paths
        assert "/api/v1/tick-data/calculate/iv" in paths
        assert "/api/v1/tick-data/calculate/option-price" in paths
        assert "/api/v1/tick-data/symbol-search" in paths

    def test_option_calc_routes_use_feed_qualification_facade(self) -> None:
        from fastapi.testclient import TestClient
        from src.config.settings import Settings
        from src.webapp.app import create_app
        from src.webapp.cache import AsyncTTLCache

        class _FakeFeed:
            def __init__(self) -> None:
                self.qualified_specs: list[Any] = []
                self.iv_contracts: list[Any] = []
                self.price_contracts: list[Any] = []

            async def qualify_contract(self, spec: Any) -> Any:
                self.qualified_specs.append(spec)
                return SimpleNamespace(conId=123, symbol=spec.symbol, secType=spec.option_sec_type)

            async def calculate_iv(self, contract: Any, option_price: float, under_price: float) -> float:
                self.iv_contracts.append(contract)
                assert option_price == pytest.approx(5.5)
                assert under_price == pytest.approx(195.0)
                return 0.25

            async def calculate_option_price(self, contract: Any, volatility: float, under_price: float) -> float:
                self.price_contracts.append(contract)
                assert volatility == pytest.approx(0.25)
                assert under_price == pytest.approx(195.0)
                return 5.5

        class _FakeState:
            def __init__(self) -> None:
                self.settings = Settings(
                    ibkr_rest_app_name="Test",
                    ibkr_rest_market_data_ttl_seconds=60,
                    ibkr_rest_market_data_cache_maxsize=16,
                )
                self.feed = _FakeFeed()
                self.redis = SimpleNamespace(health_check=lambda: True)
                self.market_data_cache = AsyncTTLCache(ttl_seconds=60, max_size=16)

            async def connect(self) -> None:
                pass

            async def close(self) -> None:
                pass

        state = _FakeState()
        app = create_app(settings=state.settings, state=state)
        with TestClient(app) as client:
            iv_response = client.post(
                "/api/v1/tick-data/calculate/iv",
                json={
                    "symbol": "AAPL",
                    "expiry": "20260619",
                    "strike": 195.0,
                    "right": "C",
                    "option_price": 5.5,
                    "under_price": 195.0,
                },
            )
            assert iv_response.status_code == 200
            assert iv_response.json()["implied_volatility"] == pytest.approx(0.25)

            price_response = client.post(
                "/api/v1/tick-data/calculate/option-price",
                json={
                    "symbol": "AAPL",
                    "expiry": "20260619",
                    "strike": 195.0,
                    "right": "C",
                    "volatility": 0.25,
                    "under_price": 195.0,
                },
            )
            assert price_response.status_code == 200
            assert price_response.json()["option_price"] == pytest.approx(5.5)

        assert len(state.feed.qualified_specs) == 2
        assert state.feed.qualified_specs[0].symbol == "AAPL"
        assert state.feed.qualified_specs[0].option_sec_type == "OPT"
        assert state.feed.iv_contracts[0].conId == 123
        assert state.feed.price_contracts[0].conId == 123
