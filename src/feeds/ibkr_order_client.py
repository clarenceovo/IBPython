"""IBKR order management sub-client — place, cancel, modify, executions, what-if margin.

This module follows the same sub-client pattern as ``ibkr_account_feed.py``:
- Constructor takes an ``IBKRConnectionManager``
- All methods are async
- Uses ``self._ib`` to access the ib_insync IB instance
- Uses ``self._connection.with_retry()`` for retry logic

Every order action is UUID-tagged and cached to Redis for auditability and
client-side tracking.
"""

from __future__ import annotations

import asyncio
import logging
import uuid as _uuid
from typing import Any

from src.feeds.ibkr_connection import IBKRConnectionManager, wait_for_ibkr_request
from src.feeds.contracts import ContractSpec, build_ibkr_contract
from src.feeds.models import AssetClass
from src.feeds.orders import (
    CachedOrderLookup,
    CancelOrderResponse,
    CompletedOrder,
    ExecutionDetail,
    ExecutionRequest,
    ExecutionResponse,
    ModifyOrderRequest,
    OpenOrder,
    OrderAction,
    OrderEnvelope,
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

from src.feeds.ibkr_order_client_ops import (
    place_order as _place_order,
    cancel_order as _cancel_order,
    modify_order as _modify_order,
    preview_order as _preview_order,
    load_open_orders as _load_open_orders,
    load_executions as _load_executions,
    load_completed_orders as _load_completed_orders,
)

logger = logging.getLogger(__name__)



class IBKROrderClient:
    """Order management sub-client for IBKR — lifecycle, executions, and pre-trade risk.

    Every order action produces an ``OrderEnvelope`` tagged with a UUID and
    cached to Redis. The cache is best-effort — if Redis is unavailable the
    order still proceeds; only the caching step is skipped.
    """

    def __init__(
        self,
        connection: IBKRConnectionManager,
        redis: Any | None = None,
    ) -> None:
        self._connection = connection
        self._redis = redis

    @property
    def _ib(self) -> Any:
        return self._connection.ib

    # ------------------------------------------------------------------
    # Redis order cache helpers
    # ------------------------------------------------------------------

    async def _cache_envelope(self, envelope: OrderEnvelope, *, ttl_seconds: int | None = None) -> str | None:
        """Cache an OrderEnvelope to Redis. Returns the Redis key or None on failure."""
        if self._redis is None:
            return None
        try:
            envelope_json = envelope.model_dump_json()
            key = await self._redis.cache_order_envelope(envelope_json, ttl=ttl_seconds)
            logger.debug("_cache_envelope: cached uuid=%s key=%s", envelope.order_uuid, key)
            return key
        except Exception:
            logger.warning("_cache_envelope: failed for uuid=%s, order proceeds uncached", envelope.order_uuid, exc_info=True)
            return None

    async def _update_cached_envelope(self, envelope: OrderEnvelope) -> None:
        """Update an existing cached envelope in-place."""
        if self._redis is None:
            return
        try:
            envelope.touch()
            envelope_json = envelope.model_dump_json()
            await self._redis.cache_order_envelope(envelope_json)
        except Exception:
            logger.warning("_update_cached_envelope: failed for uuid=%s", envelope.order_uuid, exc_info=True)

    async def get_cached_order(self, order_uuid: str) -> CachedOrderLookup:
        """Look up a cached order envelope by UUID."""
        if self._redis is None:
            return CachedOrderLookup(order_uuid=order_uuid, found=False)
        try:
            payload = await self._redis.get_order_envelope(order_uuid)
            if payload is None:
                return CachedOrderLookup(order_uuid=order_uuid, found=False)
            envelope = OrderEnvelope.model_validate_json(payload)
            return CachedOrderLookup(order_uuid=order_uuid, found=True, envelope=envelope)
        except Exception:
            logger.warning("get_cached_order: failed for uuid=%s", order_uuid, exc_info=True)
            return CachedOrderLookup(order_uuid=order_uuid, found=False)

    async def list_cached_orders(self) -> list[OrderEnvelope]:
        """List all cached order envelopes from Redis."""
        if self._redis is None:
            return []
        try:
            keys = await self._redis.scan_order_envelopes()
            envelopes: list[OrderEnvelope] = []
            for key in keys:
                raw = await self._redis.get_raw(key)
                if raw is not None:
                    payload = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                    envelopes.append(OrderEnvelope.model_validate_json(payload))
            return envelopes
        except Exception:
            logger.warning("list_cached_orders: failed", exc_info=True)
            return []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_ibkr_contract(self, request: PlaceOrderRequest) -> Any:
        """Build an ib_insync Contract from a PlaceOrderRequest."""
        asset_class = _asset_class_from_sec_type(request.sec_type)
        if asset_class is not None:
            return build_ibkr_contract(
                ContractSpec(
                    symbol=request.symbol,
                    asset_class=asset_class,
                    exchange=request.exchange,
                    currency=request.currency,
                    primary_exchange=request.primary_exchange,
                    last_trade_date_or_contract_month=request.last_trade_date_or_contract_month,
                    multiplier=request.multiplier,
                    local_symbol=request.local_symbol,
                    sec_id_type=request.sec_id_type,
                    sec_id=request.sec_id,
                    con_id=request.con_id,
                )
            )
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

        if request.price is not None and request.order_type in (
            OrderType.LIMIT,
            OrderType.STOP_LIMIT,
            OrderType.LIMIT_ON_CLOSE,
        ):
            order.lmtPrice = request.price

        if request.aux_price is not None:
            order.auxPrice = request.aux_price

        if request.trail_stop_price is not None:
            order.trailStopPrice = request.trail_stop_price

        if request.limit_price_offset is not None:
            order.lmtPriceOffset = request.limit_price_offset

        if request.trailing_type is not None and request.trailing_amount is not None:
            if request.trailing_type == "%":
                order.trailingPercent = request.trailing_amount
            else:
                order.auxPrice = request.trailing_amount

        if request.outside_rth:
            order.outsideRth = True

        return order

    async def _preflight_order(self, request: PlaceOrderRequest, *, envelope: OrderEnvelope) -> WhatIfOrderResponse:
        """Run a what-if order before a live submission and reject obvious margin failures."""
        contract = self._build_ibkr_contract(request)
        order = self._build_ibkr_order(request, what_if=True)
        order.orderRef = envelope.order_uuid
        qualified = await self._connection.with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_preflight_contract:{request.symbol}",
        )
        if qualified:
            contract = qualified[0]
        await wait_for_ibkr_request(self._connection, operation=f"preflight_order:place:{request.symbol}")
        trade = self._ib.placeOrder(contract, order)
        try:
            await asyncio.sleep(0.5)
            result = normalize_what_if_response(trade)
            if (
                result.initial_margin is not None
                and result.equity_with_loan is not None
                and result.equity_with_loan > 0
                and result.initial_margin > result.equity_with_loan
            ):
                raise RuntimeError(
                    "pre-trade risk rejected order: initial margin after trade exceeds equity with loan"
                )
            return result
        finally:
            try:
                await wait_for_ibkr_request(self._connection, operation=f"preflight_order:cancel:{request.symbol}")
                self._ib.cancelOrder(order)
            except Exception:
                logger.debug("_preflight_order: uuid=%s what-if cancel skipped", envelope.order_uuid, exc_info=True)

    async def _load_open_trades_for_action(self, *, operation: str) -> list[Any]:
        """Refresh open orders from IBKR before cancel/modify, with local cache fallback."""
        if hasattr(self._ib, "reqOpenOrdersAsync"):
            try:
                trades = await self._connection.with_retry(
                    lambda: self._ib.reqOpenOrdersAsync(),
                    operation=operation,
                )
                if trades is not None:
                    return list(trades)
            except Exception:
                logger.warning("%s: reqOpenOrdersAsync failed; falling back to openTrades", operation, exc_info=True)
        if hasattr(self._ib, "openTrades"):
            return list(self._ib.openTrades())
        return []

    @staticmethod
    def _find_trade_by_order_id(trades: list[Any], order_id: int) -> Any | None:
        for trade in trades:
            trade_order = getattr(trade, "order", None)
            if trade_order and getattr(trade_order, "orderId", None) == order_id:
                return trade
        return None

    async def _wait_for_order_status(
        self,
        trade: Any,
        *,
        timeout_seconds: float = 5.0,
    ) -> OrderStatus:
        """Wait for an order to reach a terminal or submitted status.

        Uses ib_insync's ``trade.statusEvent`` for event-driven notification
        instead of polling. Falls back to checking the current status on timeout.
        Terminal states: filled, cancelled, api cancelled, inactive.
        Target states: submitted, presubmitted.
        """
        # First check if already in a target state
        status_str = str(getattr(trade.orderStatus, "status", ""))
        lower = status_str.lower()
        if lower in ("filled", "cancelled", "api cancelled", "inactive", "submitted", "presubmitted"):
            return _ibkr_order_status_to_enum(status_str)

        # Create a future that fires when statusEvent emits
        loop = asyncio.get_running_loop()
        status_future: asyncio.Future[None] = loop.create_future()

        def _on_status_event(_trade: Any) -> None:
            """Callback for trade.statusEvent."""
            if not status_future.done():
                current = str(getattr(_trade.orderStatus, "status", "")).lower()
                if current in ("filled", "cancelled", "api cancelled", "inactive", "submitted", "presubmitted"):
                    status_future.set_result(None)

        # Register the event handler
        trade.statusEvent += _on_status_event
        try:
            # Check once more in case status changed between initial check and handler registration
            status_str = str(getattr(trade.orderStatus, "status", ""))
            lower = status_str.lower()
            if lower in ("filled", "cancelled", "api cancelled", "inactive", "submitted", "presubmitted"):
                return _ibkr_order_status_to_enum(status_str)

            # Wait for the event with a timeout
            await asyncio.wait_for(status_future, timeout=timeout_seconds)
        except asyncio.TimeoutError:
            # Timeout — return whatever the current status is
            pass
        finally:
            try:
                trade.statusEvent -= _on_status_event
            except Exception:
                pass

        return _ibkr_order_status_to_enum(getattr(trade.orderStatus, "status", None))


    # ------------------------------------------------------------------
    # Public methods — delegated to ibkr_order_client_ops
    # ------------------------------------------------------------------

    async def place_order(self, request: PlaceOrderRequest) -> OrderResponse:
        return await _place_order(self, request)

    async def cancel_order(self, account_id: str, order_id: int) -> CancelOrderResponse:
        return await _cancel_order(self, account_id, order_id)

    async def modify_order(
        self,
        account_id: str,
        order_id: int,
        modifications: ModifyOrderRequest,
    ) -> OrderResponse:
        return await _modify_order(self, account_id, order_id, modifications)

    async def preview_order(self, request: PlaceOrderRequest) -> WhatIfOrderResponse:
        return await _preview_order(self, request)

    async def load_open_orders(self) -> list[OpenOrder]:
        return await _load_open_orders(self)

    async def load_executions(self, request: ExecutionRequest) -> ExecutionResponse:
        return await _load_executions(self, request)

    async def load_completed_orders(self) -> list[CompletedOrder]:
        return await _load_completed_orders(self)


def _asset_class_from_sec_type(sec_type: str) -> AssetClass | None:
    mapping = {
        "STK": AssetClass.EQUITY,
        "CASH": AssetClass.FX,
        "FUT": AssetClass.FUTURE,
        "IND": AssetClass.INDEX,
        "BOND": AssetClass.BOND,
        "CRYPTO": AssetClass.CRYPTO,
    }
    return mapping.get(sec_type.strip().upper())
