"""IBKR order management sub-client — place, cancel, modify, executions, what-if margin.

This module follows the same sub-client pattern as ``ibkr_account_feed.py``:
- Constructor takes an ``IBKRConnectionManager``
- All methods are async
- Uses ``self._ib`` to access the ib_insync IB instance
- Uses ``self._connection.with_retry()`` for retry logic
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from src.feeds.ibkr_connection import IBKRConnectionManager
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

logger = logging.getLogger(__name__)


class IBKROrderClient:
    """Order management sub-client for IBKR — lifecycle, executions, and pre-trade risk."""

    def __init__(self, connection: IBKRConnectionManager) -> None:
        self._connection = connection

    @property
    def _ib(self) -> Any:
        return self._connection.ib

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_ibkr_contract(self, request: PlaceOrderRequest) -> Any:
        """Build an ib_insync Contract from a PlaceOrderRequest."""
        from ib_insync import Contract
        contract = Contract()
        contract.symbol = request.symbol
        contract.secType = request.sec_type
        contract.exchange = request.exchange
        contract.currency = request.currency
        return contract

    def _build_ibkr_order(self, request: PlaceOrderRequest, *, what_if: bool = False) -> Any:
        """Build an ib_insync Order from a PlaceOrderRequest."""
        from ib_insync import Order
        order = Order()
        order.action = request.action.value
        order.orderType = request.order_type.value
        order.totalQuantity = request.quantity
        order.tif = request.tif.value
        order.whatIf = what_if

        if request.account_id:
            order.account = request.account_id

        if request.price is not None:
            order.lmtPrice = request.price

        if request.aux_price is not None:
            order.auxPrice = request.aux_price

        if request.trailing_type is not None and request.trailing_amount is not None:
            order.trailingType = request.trailing_type
            order.trailingPercent = request.trailing_amount if request.trailing_type == "%" else 0
            order.trailStopPrice = request.trailing_amount if request.trailing_type == "amt" else 0

        if request.outside_rth:
            order.outsideRth = True

        return order

    async def _wait_for_order_status(
        self,
        trade: Any,
        *,
        timeout_seconds: float = 5.0,
    ) -> OrderStatus:
        """Wait for an order to reach a terminal or submitted status.

        Polls the trade's orderStatus for up to ``timeout_seconds``.
        Returns the final mapped OrderStatus.
        """
        deadline = asyncio.get_event_loop().time() + timeout_seconds
        while asyncio.get_event_loop().time() < deadline:
            status_str = str(getattr(trade.orderStatus, "status", ""))
            lower = status_str.lower()
            if lower in ("filled", "cancelled", "api cancelled", "inactive"):
                return _ibkr_order_status_to_enum(status_str)
            if lower in ("submitted", "presubmitted"):
                return OrderStatus.SUBMITTED
            await asyncio.sleep(0.1)
        # Timeout — return whatever the current status is.
        return _ibkr_order_status_to_enum(getattr(trade.orderStatus, "status", None))

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def place_order(self, request: PlaceOrderRequest) -> OrderResponse:
        """Submit a new order to IBKR.

        Creates the ib_insync Contract and Order from the request, places
        the order, waits briefly for acknowledgment, and returns the result.
        """
        await self._connection.ensure_connected()
        logger.info(
            "place_order: symbol=%s action=%s type=%s qty=%.0f price=%s",
            request.symbol,
            request.action.value,
            request.order_type.value,
            request.quantity,
            request.price,
        )
        contract = self._build_ibkr_contract(request)
        order = self._build_ibkr_order(request)

        # Qualify the contract first.
        qualified = await self._connection.with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_order_contract:{request.symbol}",
        )
        if qualified:
            contract = qualified[0]

        trade = self._ib.placeOrder(contract, order)
        status = await self._wait_for_order_status(trade, timeout_seconds=5.0)

        warnings: list[str] = []
        msg: str | None = None
        ibkr_status = getattr(trade.orderStatus, "status", "")
        if ibkr_status.lower() == "inactive":
            msg = getattr(trade.orderStatus, "message", "") or "Order submitted but inactive"
            warnings.append(msg)

        order_id = getattr(order, "orderId", 0) or getattr(trade, "orderId", 0)

        logger.info(
            "place_order: order_id=%s status=%s symbol=%s",
            order_id,
            status.value,
            request.symbol,
        )

        return OrderResponse(
            order_id=order_id,
            status=status,
            message=msg,
            warnings=warnings,
        )

    async def cancel_order(self, account_id: str, order_id: int) -> CancelOrderResponse:
        """Cancel an existing order by account ID and order ID.

        Looks up the order from open orders and calls ib_insync cancelOrder.
        """
        await self._connection.ensure_connected()
        logger.info("cancel_order: account=%s order_id=%d", account_id, order_id)

        # Find the order in open trades.
        open_trades = self._ib.openTrades() if hasattr(self._ib, "openTrades") else []
        target_order = None
        for trade in open_trades:
            trade_order = getattr(trade, "order", None)
            if trade_order and getattr(trade_order, "orderId", None) == order_id:
                target_order = trade_order
                break

        if target_order is None:
            logger.warning("cancel_order: order_id=%d not found in open trades", order_id)
            return CancelOrderResponse(
                order_id=order_id,
                status="not_found",
                message=f"Order {order_id} not found in open orders",
            )

        current_status = str(getattr(target_order, "status", "")).lower()
        if current_status in ("filled", "cancelled", "api cancelled"):
            logger.info("cancel_order: order_id=%d already %s", order_id, current_status)
            return CancelOrderResponse(
                order_id=order_id,
                status="already_terminal",
                message=f"Order already {current_status}",
            )

        self._ib.cancelOrder(target_order)
        logger.info("cancel_order: cancel requested for order_id=%d", order_id)

        return CancelOrderResponse(
            order_id=order_id,
            status="cancel_requested",
            message="Cancel request sent",
        )

    async def modify_order(
        self,
        account_id: str,
        order_id: int,
        modifications: ModifyOrderRequest,
    ) -> OrderResponse:
        """Modify an existing order.

        Finds the order in open trades, applies modifications, and resubmits.
        """
        await self._connection.ensure_connected()
        logger.info("modify_order: account=%s order_id=%d", account_id, order_id)

        open_trades = self._ib.openTrades() if hasattr(self._ib, "openTrades") else []
        target_trade = None
        for trade in open_trades:
            trade_order = getattr(trade, "order", None)
            if trade_order and getattr(trade_order, "orderId", None) == order_id:
                target_trade = trade
                break

        if target_trade is None:
            raise RuntimeError(f"Order {order_id} not found in open orders")

        order = target_trade.order
        contract = target_trade.contract

        # Apply modifications.
        if modifications.price is not None:
            order.lmtPrice = modifications.price
        if modifications.quantity is not None:
            order.totalQuantity = modifications.quantity
        if modifications.order_type is not None:
            order.orderType = modifications.order_type.value
        if modifications.tif is not None:
            order.tif = modifications.tif.value
        if modifications.aux_price is not None:
            order.auxPrice = modifications.aux_price
        if modifications.trailing_type is not None:
            order.trailingType = modifications.trailing_type
        if modifications.trailing_amount is not None:
            if modifications.trailing_type == "%":
                order.trailingPercent = modifications.trailing_amount
            elif modifications.trailing_type == "amt":
                order.trailStopPrice = modifications.trailing_amount

        modified_trade = self._ib.placeOrder(contract, order)
        status = await self._wait_for_order_status(modified_trade, timeout_seconds=5.0)

        logger.info(
            "modify_order: order_id=%d status=%s",
            order_id,
            status.value,
        )

        return OrderResponse(
            order_id=order_id,
            status=status,
            message="Order modified",
        )

    async def load_open_orders(self) -> list[OpenOrder]:
        """Load all currently open (working) orders.

        Uses ``ib.reqOpenOrders()`` to fetch orders and normalizes them.
        """
        await self._connection.ensure_connected()
        logger.info("load_open_orders: starting")

        trades = await self._connection.with_retry(
            lambda: self._ib.reqOpenOrdersAsync(),
            operation="open_orders",
        )

        result = [normalize_open_order(trade) for trade in (trades or [])]
        logger.info("load_open_orders: %d open orders loaded", len(result))
        return result

    async def load_executions(self, request: ExecutionRequest) -> ExecutionResponse:
        """Load execution/fill details with optional filtering.

        Uses ``ib.reqExecutionsAsync()`` with an ExecutionFilter.
        """
        await self._connection.ensure_connected()
        logger.info(
            "load_executions: account=%s symbol=%s since=%s",
            request.account_id,
            request.symbol,
            request.since,
        )
        from ib_insync import ExecutionFilter

        exec_filter = ExecutionFilter()
        if request.account_id:
            exec_filter.acctCode = request.account_id
        if request.symbol:
            exec_filter.symbol = request.symbol
        if request.sec_type:
            exec_filter.secType = request.sec_type
        if request.exchange:
            exec_filter.exchange = request.exchange
        if request.side:
            exec_filter.side = request.side
        if request.since:
            # ib_insync expects time as a string "YYYYMMDD HH:MM:SS"
            exec_filter.time = request.since.strftime("%Y%m%d %H:%M:%S")

        fills = await self._connection.with_retry(
            lambda: self._ib.reqExecutionsAsync(exec_filter),
            operation="executions",
        )

        executions = [normalize_execution(fill) for fill in (fills or [])]
        logger.info("load_executions: %d fills loaded", len(executions))

        return ExecutionResponse(
            executions=executions,
            total_count=len(executions),
        )

    async def preview_order(self, request: PlaceOrderRequest) -> WhatIfOrderResponse:
        """Pre-trade margin and commission preview (what-if order).

        Uses ``order.whatIf = True`` to get margin impact without
        actually placing the order.
        """
        await self._connection.ensure_connected()
        logger.info(
            "preview_order: symbol=%s action=%s type=%s qty=%.0f",
            request.symbol,
            request.action.value,
            request.order_type.value,
            request.quantity,
        )

        contract = self._build_ibkr_contract(request)
        order = self._build_ibkr_order(request, what_if=True)

        # Qualify the contract.
        qualified = await self._connection.with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_preview_contract:{request.symbol}",
        )
        if qualified:
            contract = qualified[0]

        trade = self._ib.placeOrder(contract, order)

        # For what-if orders, the response is typically immediate.
        await asyncio.sleep(0.5)

        result = normalize_what_if_response(trade)
        logger.info(
            "preview_order: symbol=%s initial_margin=%s maintenance_margin=%s",
            request.symbol,
            result.initial_margin,
            result.maintenance_margin,
        )
        return result

    async def load_completed_orders(self) -> list[CompletedOrder]:
        """Load completed (filled/cancelled) order history.

        Uses ``ib.reqCompletedOrdersAsync()`` to fetch historical orders.
        """
        await self._connection.ensure_connected()
        logger.info("load_completed_orders: starting")

        trades = await self._connection.with_retry(
            lambda: self._ib.reqCompletedOrdersAsync(),
            operation="completed_orders",
        )

        result = [normalize_completed_order(trade) for trade in (trades or [])]
        logger.info("load_completed_orders: %d completed orders loaded", len(result))
        return result
