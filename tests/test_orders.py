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
from src.feeds.exceptions import IBKROrderError


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
        orderState: Any | None = None,
    ) -> None:
        self.contract = contract or FakeContract()
        self.order = order or FakeOrder()
        self.orderStatus = orderStatus or FakeOrderStatus()
        self.orderState = orderState
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
        self._what_if_orders: list[tuple[Any, Any]] = []
        self._cancelled_orders: list[FakeOrder] = []
        self._open_order_requests = 0
        self._connected = True
        self._next_order_id = 1001

    def isConnected(self) -> bool:
        return self._connected

    def qualifyContractsAsync(self, contract: Any) -> Any:
        async def _qual() -> list[Any]:
            return [contract]
        return _qual()

    def placeOrder(self, contract: Any, order: Any) -> FakeTrade:
        if getattr(order, "orderId", 0) in (None, 0):
            order.orderId = self._next_order_id
            self._next_order_id += 1
        if getattr(order, "whatIf", False):
            self._what_if_orders.append((contract, order))
            return FakeTrade(contract=contract, order=order)
        self._placed_orders.append((contract, order))
        trade = FakeTrade(contract=contract, order=order)
        self._open_trades.append(trade)
        return trade

    def cancelOrder(self, order: Any) -> None:
        self._cancelled_orders.append(order)

    def openTrades(self) -> list[FakeTrade]:
        return list(self._open_trades)

    def reqOpenOrdersAsync(self) -> Any:
        self._open_order_requests += 1

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
        self.rate_limit_calls: list[tuple[str, int]] = []

    @property
    def ib(self) -> FakeIB:
        return self._ib_instance

    async def ensure_connected(self) -> None:
        if not self._ib_instance.isConnected():
            raise RuntimeError("IBKR not available")

    async def with_retry(self, call: Any, *, operation: str) -> Any:
        await self.wait_for_ibkr_request(operation=operation)
        return await call()

    async def wait_for_ibkr_request(self, *, operation: str, weight: int = 1) -> None:
        self.rate_limit_calls.append((operation, weight))


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

    def test_trailing_stop_limit_contract_fields(self) -> None:
        schema_properties = set(PlaceOrderRequest.model_json_schema()["properties"])
        assert {"trail_stop_price", "limit_price_offset"} <= schema_properties
        req = PlaceOrderRequest(
            symbol="AAPL",
            action=OrderAction.SELL,
            order_type=OrderType.TRAIL_LIMIT,
            quantity=100,
            trailing_type="amt",
            trailing_amount=1.0,
            trail_stop_price=145.0,
            limit_price_offset=0.25,
        )
        assert req.trail_stop_price == pytest.approx(145.0)
        assert req.limit_price_offset == pytest.approx(0.25)

    def test_trailing_stop_limit_rejects_limit_price(self) -> None:
        with pytest.raises(ValueError, match="limit_price_offset, not price"):
            PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.SELL,
                order_type=OrderType.TRAIL_LIMIT,
                quantity=100,
                price=144.75,
                trailing_type="amt",
                trailing_amount=1.0,
                trail_stop_price=145.0,
                limit_price_offset=0.25,
            )

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
            trail_limit_fields = {
                "trail_stop_price": 145.0,
                "limit_price_offset": 0.25,
            } if ot == OrderType.TRAIL_LIMIT else {}
            req = PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=ot,
                quantity=100,
                price=150.0 if ot in (OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.LIMIT_ON_CLOSE) else None,
                aux_price=140.0 if ot in (OrderType.STOP, OrderType.STOP_LIMIT) else None,
                trailing_type="%" if ot in (OrderType.TRAIL, OrderType.TRAIL_LIMIT) else None,
                trailing_amount=5.0 if ot in (OrderType.TRAIL, OrderType.TRAIL_LIMIT) else None,
                **trail_limit_fields,
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
    def test_empty_modification_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one of price, quantity, or tif is required"):
            ModifyOrderRequest()

    def test_partial_modification(self) -> None:
        req = ModifyOrderRequest(price=155.0, quantity=200)
        assert req.price == 155.0
        assert req.quantity == 200.0

    def test_modify_contract_allows_only_price_quantity_and_tif(self) -> None:
        schema_properties = set(ModifyOrderRequest.model_json_schema()["properties"])
        assert schema_properties == {"price", "quantity", "tif"}

    def test_trailing_fields_rejected(self) -> None:
        with pytest.raises(ValueError, match="Extra inputs are not permitted"):
            ModifyOrderRequest(trailing_type="amt")


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

    def test_reads_margin_from_order_state(self) -> None:
        trade = FakeTrade(
            orderState=SimpleNamespace(
                initMarginAfter="5000",
                maintMarginAfter="2500",
                equityWithLoanAfter="100000",
                initMarginBefore="4000",
                maintMarginBefore="2000",
                commissionAndFees="1.25",
                warningText="IBKR warning",
            ),
        )
        result = normalize_what_if_response(trade)
        assert result.initial_margin == pytest.approx(5000)
        assert result.maintenance_margin == pytest.approx(2500)
        assert result.equity_with_loan == pytest.approx(100000)
        assert result.init_margin_before == pytest.approx(4000)
        assert result.maint_margin_before == pytest.approx(2000)
        assert result.commission == pytest.approx(1.25)
        assert "IBKR warning" in result.warnings


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
        assert ("qualify_order_contract:AAPL", 1) in conn.rate_limit_calls
        assert ("place_order:AAPL", 1) in conn.rate_limit_calls

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
            placed = await client.place_order(PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.LIMIT,
                quantity=100,
                price=150.0,
            ))
            # Cancel it
            return await client.cancel_order("DU123", placed.order_id)

        result = asyncio.run(run())
        assert result.status == "cancel_requested"
        assert len(fake_ib._cancelled_orders) == 1
        assert fake_ib._open_order_requests >= 1

    def test_cancel_nonexistent_order(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> CancelOrderResponse:
            return await client.cancel_order("DU123", 9999)

        result = asyncio.run(run())
        assert result.status == "not_found"
        assert fake_ib._open_order_requests >= 1

    def test_cancel_unbound_order_id_rejected(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> None:
            with pytest.raises(IBKROrderError, match="bound IBKR order_id"):
                await client.cancel_order("DU123", 0)

        asyncio.run(run())


class TestIBKROrderClientModifyOrder:
    def test_modify_existing_order(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> OrderResponse:
            # Place an order first
            placed = await client.place_order(PlaceOrderRequest(
                symbol="AAPL",
                action=OrderAction.BUY,
                order_type=OrderType.LIMIT,
                quantity=100,
                price=150.0,
            ))
            # Modify it
            return await client.modify_order("DU123", placed.order_id, ModifyOrderRequest(price=155.0, quantity=200))

        result = asyncio.run(run())
        assert result.order_id >= 0
        assert result.status in (OrderStatus.SUBMITTED, OrderStatus.PENDING)
        assert fake_ib._open_order_requests >= 1

    def test_modify_nonexistent_order_raises(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> None:
            with pytest.raises(IBKROrderError, match="not found"):
                await client.modify_order("DU123", 9999, ModifyOrderRequest(price=155.0))

        asyncio.run(run())
        assert fake_ib._open_order_requests >= 1

    def test_modify_unbound_order_id_rejected(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        async def run() -> None:
            with pytest.raises(IBKROrderError, match="bound IBKR order_id"):
                await client.modify_order("DU123", 0, ModifyOrderRequest(price=155.0))

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
        assert not fake_ib._placed_orders
        for _, order in fake_ib._what_if_orders:
            assert getattr(order, "whatIf", False) is True
        assert len(fake_ib._cancelled_orders) == 1

    def test_trailing_stop_limit_mapping(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        order = client._build_ibkr_order(PlaceOrderRequest(
            symbol="AAPL",
            action=OrderAction.SELL,
            order_type=OrderType.TRAIL_LIMIT,
            quantity=100,
            trail_stop_price=145.0,
            trailing_type="amt",
            trailing_amount=1.0,
            limit_price_offset=0.25,
        ))
        assert order.orderType == "TRAIL LIMIT"
        assert order.trailStopPrice == pytest.approx(145.0)
        assert order.auxPrice == pytest.approx(1.0)
        assert order.lmtPriceOffset == pytest.approx(0.25)
        assert not hasattr(order, "trailingType") or order.trailingType == ""
        assert getattr(order, "lmtPrice", 1.7976931348623157e308) == pytest.approx(1.7976931348623157e308)

    def test_trailing_percent_mapping(self) -> None:
        fake_ib = FakeIB()
        conn = FakeConnection(fake_ib)
        client = IBKROrderClient(conn)

        order = client._build_ibkr_order(PlaceOrderRequest(
            symbol="AAPL",
            action=OrderAction.SELL,
            order_type=OrderType.TRAIL,
            quantity=100,
            trail_stop_price=145.0,
            trailing_type="%",
            trailing_amount=5.0,
        ))
        assert order.orderType == "TRAIL"
        assert order.trailStopPrice == pytest.approx(145.0)
        assert order.trailingPercent == pytest.approx(5.0)
        assert not hasattr(order, "trailingType") or order.trailingType == ""
        assert getattr(order, "lmtPrice", 1.7976931348623157e308) == pytest.approx(1.7976931348623157e308)


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
    AUTH_HEADERS = {"Authorization": "Bearer test-order-token"}

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
            }, headers=self.AUTH_HEADERS)
            assert response.status_code == 200
            data = response.json()
            assert "order_id" in data
            assert data["status"] in ("submitted", "pending")

    def test_place_order_endpoint_does_not_auto_preview_every_order(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            response = client.post("/api/v1/orders/place", json={
                "symbol": "AAPL",
                "action": "BUY",
                "order_type": "LMT",
                "quantity": 100,
                "price": 150.0,
            }, headers=self.AUTH_HEADERS)
            assert response.status_code == 200
            assert len(fake_ib._placed_orders) == 1
            assert fake_ib._what_if_orders == []

    def test_preview_order_endpoint(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            response = client.post("/api/v1/orders/preview", json={
                "symbol": "AAPL",
                "action": "BUY",
                "order_type": "LMT",
                "quantity": 100,
                "price": 150.0,
            }, headers=self.AUTH_HEADERS)
            assert response.status_code == 200
            data = response.json()
            assert "warnings" in data
            assert fake_ib._placed_orders == []
            assert len(fake_ib._what_if_orders) == 1
            assert len(fake_ib._cancelled_orders) == 1
            for _, order in fake_ib._what_if_orders:
                assert getattr(order, "whatIf", False) is True

    def test_open_orders_endpoint(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            response = client.get("/api/v1/orders/open", headers=self.AUTH_HEADERS)
            assert response.status_code == 200
            assert isinstance(response.json(), list)

    def test_completed_orders_endpoint(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            response = client.get("/api/v1/orders/completed", headers=self.AUTH_HEADERS)
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 1
            assert data[0]["symbol"] == "AAPL"

    def test_executions_endpoint(self) -> None:
        app, fake_ib = self._make_app()
        with TestClient(app) as client:
            response = client.post("/api/v1/orders/executions", json={"account_id": "DU123"}, headers=self.AUTH_HEADERS)
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
                "account_id": "DU123",
            }, headers=self.AUTH_HEADERS)
            order_id = place_resp.json()["order_id"]
            # Cancel
            response = client.post(f"/api/v1/orders/{order_id}/cancel?account_id=DU123", headers=self.AUTH_HEADERS)
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
                "account_id": "DU123",
            }, headers=self.AUTH_HEADERS)
            order_id = place_resp.json()["order_id"]
            # Modify
            response = client.post(f"/api/v1/orders/{order_id}/modify?account_id=DU123", json={
                "price": 155.0,
                "quantity": 200,
            }, headers=self.AUTH_HEADERS)
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
            }, headers=self.AUTH_HEADERS)
            assert response.status_code == 422

    def test_order_endpoint_requires_bearer_token(self) -> None:
        app, _ = self._make_app()
        with TestClient(app) as client:
            response = client.get("/api/v1/orders/open")
            assert response.status_code == 401

    def test_order_endpoint_rejects_wrong_bearer_token(self) -> None:
        app, _ = self._make_app()
        with TestClient(app) as client:
            response = client.get("/api/v1/orders/open", headers={"Authorization": "Bearer wrong"})
            assert response.status_code == 401

    def test_router_registered_in_app(self) -> None:
        """Verify order router paths appear in the app's route list."""
        app, _ = self._make_app()
        paths = {route.path for route in app.routes}
        assert "/api/v1/orders/place" in paths
        assert "/api/v1/orders/open" in paths
        assert "/api/v1/orders/completed" in paths
        assert "/api/v1/orders/preview" in paths
        assert "/api/v1/orders/executions" in paths

    def test_order_openapi_declares_bearer_security(self) -> None:
        app, _ = self._make_app()
        spec = app.openapi()
        assert spec["components"]["securitySchemes"]["OrderBearerAuth"]["type"] == "http"
        assert spec["components"]["securitySchemes"]["OrderBearerAuth"]["scheme"] == "bearer"
        operation = spec["paths"]["/api/v1/orders/place"]["post"]
        assert {"OrderBearerAuth": []} in operation["security"]


class FakeRedisForRouter:
    """Minimal Redis fake for router tests."""
    def __init__(self) -> None:
        self.values = {"OrderAuth::bearer_token": "test-order-token"}

    async def health_check(self) -> bool:
        return True

    async def get_raw(self, key: str) -> str | None:
        return self.values.get(key)


# ===========================================================================
# UUID tagging & Redis cache tests
# ===========================================================================


class FakeOrderRedis:
    """In-memory fake for the order envelope Redis methods."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    async def cache_order_envelope(self, envelope_json: str, *, ttl: int | None = None) -> str:
        import json
        data = json.loads(envelope_json)
        key = f"OrderCache::{data['order_uuid']}"
        self._store[key] = envelope_json
        return key

    async def get_order_envelope(self, order_uuid: str) -> str | None:
        return self._store.get(f"OrderCache::{order_uuid}")

    async def scan_order_envelopes(self) -> list[str]:
        return list(self._store.keys())

    async def delete_order_envelope(self, order_uuid: str) -> bool:
        key = f"OrderCache::{order_uuid}"
        if key in self._store:
            del self._store[key]
            return True
        return False

    async def get_raw(self, key: str) -> str | None:
        return self._store.get(key)


class TestOrderEnvelopeModel:
    """Tests for OrderEnvelope Pydantic model."""

    def test_default_uuid_generated(self) -> None:
        from src.feeds.orders import OrderEnvelope
        env = OrderEnvelope(action="place", request={"symbol": "AAPL"})
        assert env.order_uuid is not None
        assert len(env.order_uuid) == 36  # UUID4 format

    def test_explicit_uuid(self) -> None:
        from src.feeds.orders import OrderEnvelope
        uid = "12345678-1234-1234-1234-123456789abc"
        env = OrderEnvelope(order_uuid=uid, action="cancel", request={})
        assert env.order_uuid == uid

    def test_timestamps_auto_set(self) -> None:
        from src.feeds.orders import OrderEnvelope
        env = OrderEnvelope(action="place", request={})
        assert env.created_at is not None
        assert env.updated_at is not None
        assert env.created_at.tzinfo is not None

    def test_touch_updates_timestamp(self) -> None:
        import time
        from src.feeds.orders import OrderEnvelope
        env = OrderEnvelope(action="place", request={})
        before = env.updated_at
        time.sleep(0.01)
        env.touch()
        assert env.updated_at > before

    def test_parent_uuid_link(self) -> None:
        from src.feeds.orders import OrderEnvelope
        parent = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        env = OrderEnvelope(
            action="modify",
            request={"price": 155.0},
            parent_uuid=parent,
            ibkr_order_id=42,
        )
        assert env.parent_uuid == parent
        assert env.ibkr_order_id == 42

    def test_serialization_roundtrip(self) -> None:
        from src.feeds.orders import OrderEnvelope
        env = OrderEnvelope(
            action="place",
            request={"symbol": "AAPL", "quantity": 100},
            metadata={"source": "test"},
        )
        json_str = env.model_dump_json()
        restored = OrderEnvelope.model_validate_json(json_str)
        assert restored.order_uuid == env.order_uuid
        assert restored.action == "place"
        assert restored.request["symbol"] == "AAPL"
        assert restored.metadata["source"] == "test"

    def test_extra_fields_rejected(self) -> None:
        from src.feeds.orders import OrderEnvelope
        with pytest.raises(Exception):
            OrderEnvelope(action="place", request={}, unknown_field=True)


class TestCachedOrderLookupModel:
    """Tests for CachedOrderLookup model."""

    def test_not_found(self) -> None:
        from src.feeds.orders import CachedOrderLookup
        result = CachedOrderLookup(order_uuid="abc", found=False)
        assert not result.found
        assert result.envelope is None

    def test_found_with_envelope(self) -> None:
        from src.feeds.orders import CachedOrderLookup, OrderEnvelope
        env = OrderEnvelope(action="place", request={"symbol": "AAPL"})
        result = CachedOrderLookup(order_uuid=env.order_uuid, found=True, envelope=env)
        assert result.found
        assert result.envelope is not None
        assert result.envelope.action == "place"


class TestOrderClientUUIDCaching:
    """Tests for UUID tagging and Redis caching in IBKROrderClient."""

    def _make_client(self, *, with_redis: bool = True) -> tuple[IBKROrderClient, FakeIB, FakeOrderRedis | None]:
        fake_ib = FakeIB()
        fake_conn = FakeConnection(fake_ib)
        redis = FakeOrderRedis() if with_redis else None
        client = IBKROrderClient(fake_conn, redis=redis)
        return client, fake_ib, redis

    @pytest.mark.asyncio
    async def test_place_order_gets_uuid(self) -> None:
        client, _, redis = self._make_client()
        request = PlaceOrderRequest(
            symbol="AAPL",
            action=OrderAction.BUY,
            order_type=OrderType.LIMIT,
            quantity=100,
            price=150.0,
        )
        response = await client.place_order(request)
        assert response.order_uuid is not None
        assert len(response.order_uuid) == 36

    @pytest.mark.asyncio
    async def test_place_order_cached_to_redis(self) -> None:
        client, _, redis = self._make_client()
        request = PlaceOrderRequest(
            symbol="AAPL",
            action=OrderAction.BUY,
            order_type=OrderType.LIMIT,
            quantity=100,
            price=150.0,
        )
        response = await client.place_order(request)
        assert redis is not None
        lookup = await client.get_cached_order(response.order_uuid)  # type: ignore[arg-type]
        assert lookup.found
        assert lookup.envelope is not None
        assert lookup.envelope.action == "place"
        assert lookup.envelope.request["symbol"] == "AAPL"

    @pytest.mark.asyncio
    async def test_cancel_order_cached(self) -> None:
        client, _, redis = self._make_client()
        result = await client.cancel_order("DU123", 42)
        assert redis is not None
        # Cancel creates its own envelope
        keys = await redis.scan_order_envelopes()
        assert len(keys) == 1
        raw = await redis.get_order_envelope(keys[0].split("::")[-1])
        assert raw is not None
        from src.feeds.orders import OrderEnvelope
        env = OrderEnvelope.model_validate_json(raw)
        assert env.action == "cancel"

    @pytest.mark.asyncio
    async def test_modify_order_gets_uuid(self) -> None:
        client, fake_ib, _ = self._make_client()
        # Set up a fake open trade for modify to find
        fake_order = SimpleNamespace(
            orderId=99, action="BUY", orderType="LMT",
            totalQuantity=100, lmtPrice=150.0, tif="DAY",
            auxPrice=None, trailingType=None,
        )
        fake_contract = SimpleNamespace(symbol="AAPL", secType="STK")
        fake_trade = SimpleNamespace(order=fake_order, contract=fake_contract)
        fake_ib._open_trades = [fake_trade]

        mods = ModifyOrderRequest(price=155.0, quantity=200)
        response = await client.modify_order("DU123", 99, mods)
        assert response.order_uuid is not None
        assert len(response.order_uuid) == 36

    @pytest.mark.asyncio
    async def test_preview_order_cached(self) -> None:
        client, _, redis = self._make_client()
        request = PlaceOrderRequest(
            symbol="TSLA",
            action=OrderAction.SELL,
            order_type=OrderType.MARKET,
            quantity=50,
        )
        await client.preview_order(request)
        assert redis is not None
        keys = await redis.scan_order_envelopes()
        assert len(keys) == 1
        raw = await redis.get_order_envelope(keys[0].split("::")[-1])
        assert raw is not None
        from src.feeds.orders import OrderEnvelope
        env = OrderEnvelope.model_validate_json(raw)
        assert env.action == "preview"
        assert env.request["symbol"] == "TSLA"

    @pytest.mark.asyncio
    async def test_list_cached_orders(self) -> None:
        client, _, redis = self._make_client()
        # Place two orders
        for sym in ["AAPL", "TSLA"]:
            await client.place_order(PlaceOrderRequest(
                symbol=sym,
                action=OrderAction.BUY,
                order_type=OrderType.MARKET,
                quantity=10,
            ))
        envelopes = await client.list_cached_orders()
        assert len(envelopes) == 2
        symbols = {e.request["symbol"] for e in envelopes}
        assert symbols == {"AAPL", "TSLA"}

    @pytest.mark.asyncio
    async def test_get_cached_order_not_found(self) -> None:
        client, _, _ = self._make_client()
        result = await client.get_cached_order("nonexistent-uuid")
        assert not result.found
        assert result.envelope is None

    @pytest.mark.asyncio
    async def test_no_redis_order_still_works(self) -> None:
        """Order proceeds without Redis — no crash, just no caching."""
        client, _, _ = self._make_client(with_redis=False)
        request = PlaceOrderRequest(
            symbol="AAPL",
            action=OrderAction.BUY,
            order_type=OrderType.MARKET,
            quantity=100,
        )
        response = await client.place_order(request)
        assert response.order_uuid is not None
        # Cache lookup returns not-found gracefully
        lookup = await client.get_cached_order(response.order_uuid)  # type: ignore[arg-type]
        assert not lookup.found

    @pytest.mark.asyncio
    async def test_envelope_ibkr_order_id_linked(self) -> None:
        """The envelope's ibkr_order_id matches the OrderResponse.order_id."""
        client, _, redis = self._make_client()
        response = await client.place_order(PlaceOrderRequest(
            symbol="AAPL",
            action=OrderAction.BUY,
            order_type=OrderType.MARKET,
            quantity=100,
        ))
        assert redis is not None
        lookup = await client.get_cached_order(response.order_uuid)  # type: ignore[arg-type]
        assert lookup.found
        assert lookup.envelope is not None
        assert lookup.envelope.ibkr_order_id == response.order_id


class TestOrderResponseUUID:
    """Test that OrderResponse includes order_uuid."""

    def test_order_response_has_uuid_field(self) -> None:
        resp = OrderResponse(order_id=1, status=OrderStatus.SUBMITTED, order_uuid="test-uuid")
        assert resp.order_uuid == "test-uuid"

    def test_order_response_uuid_defaults_none(self) -> None:
        resp = OrderResponse(order_id=1, status=OrderStatus.SUBMITTED)
        assert resp.order_uuid is None
