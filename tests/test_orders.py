"""Tests for IBKR order management — data models, sub-client, and router."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from fastapi.testclient import TestClient

from src.feeds.orders import (
    CancelOrderResponse,
    CompletedOrder,
    ExecutionDetail,
    ExecutionRequest,
    ExecutionResponse,
    ModifyOrderRequest,
    OpenOrder,
    OrderAction,
    OrderResponse,
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    TIF,
    WhatIfOrderResponse,
    _ibkr_order_status_to_enum,
    _safe_float,
    normalize_completed_order,
    normalize_execution,
    normalize_open_order,
    normalize_what_if_response,
)
from src.feeds.ibkr_order_client import IBKROrderClient
from src.feeds.ibkr_connection import IBKRConnectionManager


# ---------------------------------------------------------------------------
# Helpers — fakes
# ---------------------------------------------------------------------------


class FakeOrderStatus:
    def __init__(
        self,
        *,
        status: str = "Submitted",
        filled: float = 0,
        avgFillPrice: float | None = None,
        commission: float | None = None,
        initMarginAfter: float | None = None,
        maintMarginAfter: float | None = None,
        equityWithLoanAfter: float | None = None,
        initMarginBefore: float | None = None,
        maintMarginBefore: float | None = None,
        lastFillTime: datetime | None = None,
        completedTime: datetime | None = None,
        message: str = "",
    ) -> None:
        self.status = status
        self.filled = filled
        self.avgFillPrice = avgFillPrice
        self.commission = commission
        self.initMarginAfter = initMarginAfter
        self.maintMarginAfter = maintMarginAfter
        self.equityWithLoanAfter = equityWithLoanAfter
        self.initMarginBefore = initMarginBefore
        self.maintMarginBefore = maintMarginBefore
        self.lastFillTime = lastFillTime
        self.completedTime = completedTime
        self.message = message


class FakeContract:
    def __init__(self, *, symbol: str = "AAPL", secType: str = "STK", exchange: str = "SMART", currency: str = "USD", conId: int = 8314) -> None:
        self.symbol = symbol
        self.secType = secType
        self.exchange = exchange
        self.currency = currency
        self.conId = conId


class FakeOrder:
    def __init__(
        self,
        *,
        orderId: int = 0,
        action: str = "BUY",
        orderType: str = "LMT",
        totalQuantity: float = 100,
        lmtPrice: float | None = 150.0,
        tif: str = "DAY",
        acctCode: str = "DU123",
        account: str = "",
        auxPrice: float | None = None,
        status: str = "Submitted",
    ) -> None:
        self.orderId = orderId
        self.action = action
        self.orderType = orderType
        self.totalQuantity = totalQuantity
        self.lmtPrice = lmtPrice
        self.tif = tif
        self.acctCode = acctCode
        self.account = account
        self.auxPrice = auxPrice
        self.status = status


class FakeTrade:
    def __init__(
        self,
        *,
        contract: FakeContract | None = None,
        order: FakeOrder | None = None,
        orderStatus: FakeOrderStatus | None = None,
    ) -> None:
        self.contract = contract or FakeContract()
        self.order = order or FakeOrder()
        self.orderStatus = orderStatus or FakeOrderStatus()
        self.orderId = self.order.orderId


class FakeFill:
    def __init__(
        self,
        *,
        exec_id: str = "exec001",
        order_id: int = 1001,
        symbol: str = "AAPL",
        sec_type: str = "STK",
        side: str = "BOT",
        shares: float = 100,
        price: float = 150.0,
        commission: float = 1.0,
        realized_pnl: float | None = None,
        exchange: str = "SMART",
        fill_time: str = "20260101 12:00:00",
    ) -> None:
        self.contract = SimpleNamespace(symbol=symbol, secType=sec_type, exchange=exchange)
        self.execution = SimpleNamespace(
            execId=exec_id,
            orderId=order_id,
            side=side,
            shares=shares,
            price=price,
            exchange=exchange,
            time=fill_time,
        )
        self.commissionReport = SimpleNamespace(
            commission=commission,
            realizedPNL=realized_pnl,
        )


class FakeIB:
    """Minimal fake IB instance for testing order operations."""

    def __init__(self) -> None:
        self._open_trades: list[FakeTrade] = []
        self._placed_orders: list[tuple[Any, Any]] = []
        self._cancelled_orders: list[FakeOrder] = []
        self._connected = True

    def isConnected(self) -> bool:
        return self._connected

    def qualifyContractsAsync(self, contract: Any) -> Any:
        async def _qual() -> list[Any]:
            return [contract]
        return _qual()

    def placeOrder(self, contract: Any, order: Any) -> FakeTrade:
        self._placed_orders.append((contract, order))
        trade = FakeTrade(contract=contract, order=order)
        self._open_trades.append(trade)
        return trade

    def cancelOrder(self, order: Any) -> None:
        self._cancelled_orders.append(order)

    def openTrades(self) -> list[FakeTrade]:
        return list(self._open_trades)

    def reqOpenOrdersAsync(self) -> Any:
        async def _req() -> list[FakeTrade]:
            return list(self._open_trades)
        return _req()

    def reqExecutionsAsync(self, exec_filter: Any) -> Any:
        async def _req() -> list[FakeFill]:
            return [
                FakeFill(exec_id="exec001", order_id=1001),
                FakeFill(exec_id="exec002", order_id=1002, symbol="MSFT"),
            ]
        return _req()

    def reqCompletedOrdersAsync(self) -> Any:
        async def _req() -> list[FakeTrade]:
            return [
                FakeTrade(
                    contract=FakeContract(symbol="AAPL"),
                    order=FakeOrder(orderId=1001, action="BUY", totalQuantity=100),
                    orderStatus=FakeOrderStatus(
                        status="Filled",
                        filled=100,
                        avgFillPrice=150.0,
                        commission=1.0,
                        lastFillTime=datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc),
                    ),
                ),
            ]
        return _req()


class FakeConnection:
    """Fake IBKRConnectionManager for testing."""

    def __init__(self, ib: FakeIB | None = None) -> None:
        self._ib_instance = ib or FakeIB()

    @property
    def ib(self) -> FakeIB:
        return self._ib_instance

    async def ensure_connected(self) -> None:
        if not self._ib_instance.isConnected():
            raise RuntimeError("IBKR not available")

    async def with_retry(self, call: Any, *, operation: str) -> Any:
        return await call()


# ---------------------------------------------------------------------------
# Data model tests
# ---------------------------------------------------------------------------


class TestPlaceOrderRequest:
    def test_market_order_minimal(self) -> None:
        req = PlaceOrderRequest(
            symbol="AAPL",
            action=OrderAction.BUY,
            order_type=OrderType.MARKET,
            quantity=100,
        )
        assert req.symbol == "AAPL"
        assert req.order_type == OrderType.MARKET
        assert req.tif == TIF.DAY

    def test_limit_order_with_price(self) -> None:
        req = PlaceOrderRequest(
            symbol="MSFT",
            action=OrderAction.SELL,
            order_type=OrderType.LIMIT,
            quantity=50,
            price=400.0,
        )
        assert req.price == 400.0

    def test_limit_order_missing_price_raises(self) -> None:
        with pytest.raises(ValueError, match="price is required"):
            PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.LIMIT,
                quantity=100,
                price=None,
            )

    def test_stop_order_missing_aux_price_raises(self) -> None:
        with pytest.raises(ValueError, match="aux_price is required"):
            PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.STOP,
                quantity=100,
                aux_price=None,
            )

    def test_trailing_order_with_params(self) -> None:
        req = PlaceOrderRequest(
            symbol="AAPL",
            action=OrderAction.BUY,
            order_type=OrderType.TRAIL,
            quantity=100,
            trailing_type="%",
            trailing_amount=5.0,
        )
        assert req.trailing_type == "%"

    def test_invalid_trailing_type_raises(self) -> None:
        with pytest.raises(ValueError, match="trailing_type must be"):
            PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.MARKET,
                quantity=100,
                trailing_type="invalid",
            )

    def test_symbol_normalized_upper(self) -> None:
        req = PlaceOrderRequest(
            symbol="  aapl  ",
            action=OrderAction.BUY,
            order_type=OrderType.MARKET,
            quantity=100,
        )
        assert req.symbol == "AAPL"

    def test_quantity_must_be_positive(self) -> None:
        with pytest.raises(ValueError):
            PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.MARKET,
                quantity=0,
            )

    def test_all_order_types_valid(self) -> None:
        for ot in OrderType:
            req = PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=ot,
                quantity=100,
                price=150.0 if ot in (OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TRAIL_LIMIT, OrderType.LIMIT_ON_CLOSE) else None,
                aux_price=140.0 if ot in (OrderType.STOP, OrderType.STOP_LIMIT) else None,
            )
            assert req.order_type == ot

    def test_all_tif_values_valid(self) -> None:
        for tif in TIF:
            req = PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.MARKET,
                quantity=100,
                tif=tif,
            )
            assert req.tif == tif


class TestModifyOrderRequest:
    def test_empty_modification(self) -> None:
        req = ModifyOrderRequest()
        assert req.price is None
        assert req.quantity is None

    def test_partial_modification(self) -> None:
        req = ModifyOrderRequest(price=155.0, quantity=200)
        assert req.price == 155.0
        assert req.quantity == 200.0

    def test_invalid_trailing_type_raises(self) -> None:
        with pytest.raises(ValueError, match="trailing_type must be"):
            ModifyOrderRequest(trailing_type="bad")


class TestOrderResponse:
    def test_defaults(self) -> None:
        resp = OrderResponse(order_id=1001, status=OrderStatus.SUBMITTED)
        assert resp.warnings == []
        assert resp.message is None


class TestCancelOrderResponse:
    def test_cancel_requested(self) -> None:
        resp = CancelOrderResponse(order_id=1001, status="cancel_requested")
        assert resp.status == "cancel_requested"


class TestOpenOrder:
    def test_basic_fields(self) -> None:
        oo = OpenOrder(
            order_id=1001,
            symbol="AAPL",
            sec_type="STK",
            action="BUY",
            order_type="LMT",
            quantity=100,
            price=150.0,
            status="Submitted",
        )
        assert oo.filled_quantity == 0.0
        assert oo.tif == ""


class TestExecutionDetail:
    def test_all_fields(self) -> None:
        ed = ExecutionDetail(
            exec_id="exec001",
            order_id=1001,
            symbol="AAPL",
            sec_type="STK",
            side="BOT",
            quantity=100,
            price=150.0,
            commission=1.0,
        )
        assert ed.realized_pnl is None
        assert ed.fill_time is None


class TestExecutionResponse:
    def test_empty(self) -> None:
        resp = ExecutionResponse(executions=[], total_count=0)
        assert resp.total_count == 0


class TestWhatIfOrderResponse:
    def test_defaults(self) -> None:
        resp = WhatIfOrderResponse()
        assert resp.initial_margin is None
        assert resp.warnings == []


class TestCompletedOrder:
    def test_basic(self) -> None:
        co = CompletedOrder(
            order_id=1001,
            symbol="AAPL",
            sec_type="STK",
            action="BUY",
            order_type="LMT",
            quantity=100,
            status="Filled",
        )
        assert co.filled_quantity == 0.0
        assert co.avg_fill_price is None


# ---------------------------------------------------------------------------
# Normalization tests
# ---------------------------------------------------------------------------


class TestIbkrOrderStatusMapping:
    def test_submitted(self) -> None:
        assert _ibkr_order_status_to_enum("Submitted") == OrderStatus.SUBMITTED

    def test_filled(self) -> None:
        assert _ibkr_order_status_to_enum("Filled") == OrderStatus.FILLED

    def test_cancelled(self) -> None:
        assert _ibkr_order_status_to_enum("Cancelled") == OrderStatus.CANCELLED

    def test_partial(self) -> None:
        assert _ibkr_order_status_to_enum("PartiallyFilled") == OrderStatus.PARTIAL

    def test_inactive(self) -> None:
        assert _ibkr_order_status_to_enum("Inactive") == OrderStatus.INACTIVE

    def test_pending(self) -> None:
        assert _ibkr_order_status_to_enum("PendingSubmit") == OrderStatus.PENDING

    def test_unknown_falls_to_pending(self) -> None:
        assert _ibkr_order_status_to_enum("SomethingUnknown") == OrderStatus.PENDING

    def test_none_falls_to_pending(self) -> None:
        assert _ibkr_order_status_to_enum(None) == OrderStatus.PENDING


class TestSafeFloat:
    def test_none_returns_none(self) -> None:
        assert _safe_float(None) is None

    def test_valid_number(self) -> None:
        assert _safe_float(3.14) == pytest.approx(3.14)

    def test_string_number(self) -> None:
        assert _safe_float("42.5") == pytest.approx(42.5)

    def test_invalid_string_returns_none(self) -> None:
        assert _safe_float("abc") is None


class TestNormalizeOpenOrder:
    def test_basic_normalization(self) -> None:
        trade = FakeTrade(
            contract=FakeContract(symbol="AAPL"),
            order=FakeOrder(orderId=1001, action="BUY", totalQuantity=100, lmtPrice=150.0),
            orderStatus=FakeOrderStatus(status="Submitted", filled=0),
        )
        result = normalize_open_order(trade)
        assert result.order_id == 1001
        assert result.symbol == "AAPL"
        assert result.action == "BUY"
        assert result.quantity == pytest.approx(100)
        assert result.filled_quantity == pytest.approx(0)

    def test_handles_none_contract(self) -> None:
        trade = FakeTrade(contract=None)
        # FakeTrade constructor provides defaults, but let's force None behavior
        trade.contract = None
        result = normalize_open_order(trade)
        assert result.symbol == ""


class TestNormalizeExecution:
    def test_basic_fill(self) -> None:
        fill = FakeFill(
            exec_id="exec001",
            order_id=1001,
            symbol="AAPL",
            side="BOT",
            shares=100,
            price=150.0,
            commission=1.0,
        )
        result = normalize_execution(fill)
        assert result.exec_id == "exec001"
        assert result.quantity == pytest.approx(100)
        assert result.commission == pytest.approx(1.0)

    def test_with_realized_pnl(self) -> None:
        fill = FakeFill(realized_pnl=50.0)
        result = normalize_execution(fill)
        assert result.realized_pnl == pytest.approx(50.0)


class TestNormalizeCompletedOrder:
    def test_filled_order(self) -> None:
        trade = FakeTrade(
            contract=FakeContract(symbol="AAPL"),
            order=FakeOrder(orderId=1001, action="BUY", totalQuantity=100),
            orderStatus=FakeOrderStatus(
                status="Filled",
                filled=100,
                avgFillPrice=150.0,
                commission=1.0,
                lastFillTime=datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc),
            ),
        )
        result = normalize_completed_order(trade)
        assert result.order_id == 1001
        assert result.filled_quantity == pytest.approx(100)
        assert result.avg_fill_price == pytest.approx(150.0)
        assert result.commission == pytest.approx(1.0)


class TestNormalizeWhatIfResponse:
    def test_with_margin_data(self) -> None:
        trade = FakeTrade(
            orderStatus=FakeOrderStatus(
                initMarginAfter=5000.0,
                maintMarginAfter=2500.0,
                equityWithLoanAfter=100000.0,
                commission=1.0,
            ),
        )
        result = normalize_what_if_response(trade)
        assert result.initial_margin == pytest.approx(5000.0)
        assert result.maintenance_margin == pytest.approx(2500.0)
        assert result.commission == pytest.approx(1.0)

    def test_empty_response(self) -> None:
        trade = FakeTrade(orderStatus=FakeOrderStatus())
        result = normalize_what_if_response(trade)
        assert result.initial_margin is None
        assert result.warnings == []


# ---------------------------------------------------------------------------
# Sub-client tests (with mocked IB)
# ---------------------------------------------------------------------------


class TestIBKROrderClientPlaceOrder:
    def test_place_market_order(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> OrderResponse:
            return await client.place_order(PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.MARKET,
                quantity=100,
            ))

        result = asyncio.run(run())
        assert result.order_id >= 0
        assert result.status in (OrderStatus.SUBMITTED, OrderStatus.PENDING)
        assert len(fake_ib._placed_orders) == 1

    def test_place_limit_order(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> OrderResponse:
            return await client.place_order(PlaceOrderRequest(
                symbol="MSFT",
                action=OrderAction.BUY,
                order_type=OrderType.LIMIT,
                quantity=50,
                price=400.0,
                tif=TIF.GTC,
                account_id="DU123",
            ))

        result = asyncio.run(run())
        assert result.order_id >= 0
        assert result.status in (OrderStatus.SUBMITTED, OrderStatus.PENDING)


class TestIBKROrderClientCancelOrder:
    def test_cancel_existing_order(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> CancelOrderResponse:
            # Place an order first
            await client.place_order(PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.LIMIT,
                quantity=100,
                price=150.0,
            ))
            # Cancel it
            return await client.cancel_order("DU123", 0)

        result = asyncio.run(run())
        assert result.status == "cancel_requested"
        assert len(fake_ib._cancelled_orders) == 1

    def test_cancel_nonexistent_order(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> CancelOrderResponse:
            return await client.cancel_order("DU123", 9999)

        result = asyncio.run(run())
        assert result.status == "not_found"


class TestIBKROrderClientModifyOrder:
    def test_modify_existing_order(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> OrderResponse:
            # Place an order first
            await client.place_order(PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.LIMIT,
                quantity=100,
                price=150.0,
            ))
            # Modify it
            return await client.modify_order("DU123", 0, ModifyOrderRequest(price=155.0, quantity=200))

        result = asyncio.run(run())
        assert result.order_id >= 0
        assert result.status in (OrderStatus.SUBMITTED, OrderStatus.PENDING)

    def test_modify_nonexistent_order_raises(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> None:
            with pytest.raises(RuntimeError, match="not found"):
                await client.modify_order("DU123", 9999, ModifyOrderRequest(price=155.0))

        asyncio.run(run())


class TestIBKROrderClientLoadOpenOrders:
    def test_load_open_orders(self) -> None:
        fake_ib = FakeIB()
        # Pre-populate with a trade
        fake_ib.placeOrder(
            FakeContract(symbol="AAPL"),
            FakeOrder(orderId=1001, action="BUY", totalQuantity=100, lmtPrice=150.0),
        )
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> list[OpenOrder]:
            return await client.load_open_orders()

        result = asyncio.run(run())
        assert len(result) >= 1
        assert result[0].symbol == "AAPL"


class TestIBKROrderClientLoadExecutions:
    def test_load_executions(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> ExecutionResponse:
            return await client.load_executions(ExecutionRequest(account_id="DU123"))

        result = asyncio.run(run())
        assert result.total_count == 2
        assert result.executions[0].exec_id == "exec001"

    def test_load_executions_empty_filter(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> ExecutionResponse:
            return await client.load_executions(ExecutionRequest())

        result = asyncio.run(run())
        assert result.total_count >= 0


class TestIBKROrderClientPreviewOrder:
    def test_preview_returns_margin_data(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> WhatIfOrderResponse:
            return await client.preview_order(PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.LIMIT,
                quantity=100,
                price=150.0,
            ))

        result = asyncio.run(run())
        # The fake doesn't populate margin data, but it should succeed
        assert isinstance(result, WhatIfOrderResponse)
        # No real order should have been placed (whatIf was True)
        for _, order in fake_ib._placed_orders:
            assert getattr(order, "whatIf", False) is True


class TestIBKROrderClientLoadCompletedOrders:
    def test_load_completed_orders(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> list[CompletedOrder]:
            return await client.load_completed_orders()

        result = asyncio.run(run())
        assert len(result) == 1
        assert result[0].symbol == "AAPL"
        assert result[0].filled_quantity == pytest.approx(100)
        assert result[0].avg_fill_price == pytest.approx(150.0)


# ---------------------------------------------------------------------------
# Order lifecycle test: place → modify → cancel
# ---------------------------------------------------------------------------


class TestOrderLifecycle:
    def test_place_modify_cancel(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> tuple[OrderResponse, OrderResponse, CancelOrderResponse]:
            # Place
            placed = await client.place_order(PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.LIMIT,
                quantity=100,
                price=150.0,
                account_id="DU123",
            ))
            # Modify
            modified = await client.modify_order(
                "DU123",
                placed.order_id,
                ModifyOrderRequest(price=155.0),
            )
            # Cancel
            cancelled = await client.cancel_order("DU123", placed.order_id)
            return placed, modified, cancelled

        placed, modified, cancelled = asyncio.run(run())
        assert placed.status in (OrderStatus.SUBMITTED, OrderStatus.PENDING)
        assert modified.status in (OrderStatus.SUBMITTED, OrderStatus.PENDING)
        assert cancelled.status == "cancel_requested"
        assert len(fake_ib._placed_orders) == 2  # initial + modify
        assert len(fake_ib._cancelled_orders) == 1


# ---------------------------------------------------------------------------
# Router tests (using FastAPI TestClient)
# ---------------------------------------------------------------------------


class TestOrdersRouter:
    def _make_app(self) -> tuple[Any, FakeIB]:
        from src.webapp.app import create_app
        from src.config.settings import Settings
        from src.webapp.cache import AsyncTTLCache
        from src.webapp.dependencies import IBKRRestAppState

        fake_ib = FakeIB()
        fake_conn = FakeConnection(fake_ib)

        # Build a minimal order client that uses our fake connection
        from src.feeds.ibkr_order_client import IBKROrderClient
        from src.feeds.ibkr_feed import IBKRFeedClient
        from src.feeds.ohlcv_loader import OHLCVLoader

        class OrderFakeFeed:
            """Minimal feed that delegates order ops to IBKROrderClient."""
            async def connect(self) -> None:
                pass
            async def disconnect(self) -> None:
                pass

            def __init__(self, order_client: IBKROrderClient) -> None:
                self._order_client = order_client

            async def place_order(self, request: PlaceOrderRequest) -> OrderResponse:
                return await self._order_client.place_order(request)
            async def cancel_order(self, account_id: str, order_id: int) -> CancelOrderResponse:
                return await self._order_client.cancel_order(account_id, order_id)
            async def modify_order(self, account_id: str, order_id: int, modifications: ModifyOrderRequest) -> OrderResponse:
                return await self._order_client.modify_order(account_id, order_id, modifications)
            async def load_open_orders(self) -> list[OpenOrder]:
                return await self._order_client.load_open_orders()
            async def load_executions(self, request: ExecutionRequest) -> ExecutionResponse:
                return await self._order_client.load_executions(request)
            async def preview_order(self, request: PlaceOrderRequest) -> WhatIfOrderResponse:
                return await self._order_client.preview_order(request)
            async def load_completed_orders(self) -> list[CompletedOrder]:
                return await self._order_client.load_completed_orders()

        order_client = IBKROrderClient(fake_conn)
        feed = OrderFakeFeed(order_client)

        class RouterTestState:
            def __init__(self) -> None:
                self.settings = Settings(ibkr_rest_app_name="TestOrdersApp")
                self.feed = feed
                self.loader = None  # type: ignore[assignment]
                self.redis = FakeRedisForRouter()
                self.market_data_cache = AsyncTTLCache(ttl_seconds=60, max_size=16)
                self.fixed_income_reference_provider = None
            async def connect(self) -> None:
                pass
            async def close(self) -> None:
                pass

        state = RouterTestState()
        app = create_app(settings=state.settings, state=state)
        return app, fake_ib

    def test_place_order_endpoint(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            response = client.post("/api/v1/orders/place", json={
                "symbol": "AAPL",
                "action": "BUY",
                "order_type": "LMT",
                "quantity": 100,
                "price": 150.0,
            })
            assert response.status_code == 200
            data = response.json()
            assert "order_id" in data
            assert data["status"] in ("submitted", "pending")

    def test_preview_order_endpoint(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            response = client.post("/api/v1/orders/preview", json={
                "symbol": "AAPL",
                "action": "BUY",
                "order_type": "LMT",
                "quantity": 100,
                "price": 150.0,
            })
            assert response.status_code == 200
            data = response.json()
            assert "warnings" in data

    def test_open_orders_endpoint(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            response = client.get("/api/v1/orders/open")
            assert response.status_code == 200
            assert isinstance(response.json(), list)

    def test_completed_orders_endpoint(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            response = client.get("/api/v1/orders/completed")
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 1
            assert data[0]["symbol"] == "AAPL"

    def test_executions_endpoint(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            response = client.post("/api/v1/orders/executions", json={"account_id": "DU123"})
            assert response.status_code == 200
            data = response.json()
            assert data["total_count"] == 2

    def test_cancel_order_endpoint(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            # Place first
            place_resp = client.post("/api/v1/orders/place", json={
                "symbol": "AAPL",
                "action": "BUY",
                "order_type": "LMT",
                "quantity": 100,
                "price": 150.0,
            })
            order_id = place_resp.json()["order_id"]
            # Cancel
            response = client.post(f"/api/v1/orders/{order_id}/cancel?account_id=DU123")
            assert response.status_code == 200
            assert response.json()["status"] == "cancel_requested"

    def test_modify_order_endpoint(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            # Place first
            place_resp = client.post("/api/v1/orders/place", json={
                "symbol": "AAPL",
                "action": "BUY",
                "order_type": "LMT",
                "quantity": 100,
                "price": 150.0,
            })
            order_id = place_resp.json()["order_id"]
            # Modify
            response = client.post(f"/api/v1/orders/{order_id}/modify?account_id=DU123", json={
                "price": 155.0,
                "quantity": 200,
            })
            assert response.status_code == 200
            assert response.json()["order_id"] == order_id

    def test_invalid_order_request_returns_422(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            response = client.post("/api/v1/orders/place", json={
                "symbol": "AAPL",
                "action": "BUY",
                "order_type": "LMT",
                "quantity": 100,
                # Missing price for limit order
            })
            assert response.status_code == 422

    def test_router_registered_in_app(self) -> None:
        """Verify order router paths appear in the app's route list."""
        app, _ = self._make_app()
        paths = {route.path for route in app.routes}
        assert "/api/v1/orders/place" in paths
        assert "/api/v1/orders/open" in paths
        assert "/api/v1/orders/completed" in paths
        assert "/api/v1/orders/preview" in paths
        assert "/api/v1/orders/executions" in paths


class FakeRedisForRouter:
    """Minimal Redis fake for router tests."""
    async def health_check(self) -> bool:
        return True
