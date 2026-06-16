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
import inspect
import logging
import uuid as _uuid
from types import SimpleNamespace
from typing import Any

from src.feeds.ibkr_connection import IBKRConnectionManager, wait_for_ibkr_request
from src.feeds.contracts import ContractSpec, build_ibkr_contract
from src.feeds.models import AssetClass
from src.feeds.exceptions import IBKROrderError
from src.feeds.orders import (
    BracketOrderRequest,
    BracketOrderResponse,
    CachedOrderLookup,
    CancelOrderResponse,
    CompletedOrder,
    ExecutionDetail,
    ExecutionRequest,
    ExecutionResponse,
    ModifyOrderRequest,
    OcaGroupRequest,
    OcaOrderResponse,
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



logger = logging.getLogger(__name__)

READ_ONLY_ORDER_API_MESSAGE = (
    "IBKR order API request rejected because the TWS/IB Gateway API interface is in Read-Only mode. "
    "Disable Read-Only API in TWS/Gateway API settings, reconnect this app, then retry order endpoints."
)


def _is_read_only_api_error(value: Any) -> bool:
    if value is None:
        return False
    try:
        code, message = value
    except (TypeError, ValueError):
        return False
    return int(code) == 321 and "read-only mode" in str(message).lower()



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
            spec = ContractSpec(
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
            try:
                return build_ibkr_contract(spec)
            except RuntimeError:
                logger.debug("ib_insync Contract unavailable; using simple contract namespace", exc_info=True)
                return SimpleNamespace(
                    symbol=spec.symbol,
                    secType=request.sec_type,
                    exchange=spec.exchange,
                    currency=spec.currency,
                    primaryExchange=spec.primary_exchange or "",
                    lastTradeDateOrContractMonth=spec.last_trade_date_or_contract_month or "",
                    multiplier=spec.multiplier or "",
                    localSymbol=spec.local_symbol or "",
                    secIdType=spec.sec_id_type or "",
                    secId=spec.sec_id or "",
                    conId=spec.con_id or 0,
                )
        try:
            from ib_insync import Contract
            contract = Contract()
        except ImportError:
            logger.debug("ib_insync Contract unavailable; using simple contract namespace", exc_info=True)
            contract = SimpleNamespace()
        contract.symbol = request.symbol
        contract.secType = request.sec_type
        contract.exchange = request.exchange
        contract.currency = request.currency
        return contract

    def _build_ibkr_order(self, request: PlaceOrderRequest, *, what_if: bool = False) -> Any:
        """Build an ib_insync Order from a PlaceOrderRequest."""
        try:
            from ib_insync import Order
            order = Order()
        except ImportError:
            logger.debug("ib_insync Order unavailable; using simple order namespace", exc_info=True)
            order = SimpleNamespace()
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

    def _raise_if_order_api_read_only(self) -> None:
        if _is_read_only_api_error(getattr(self._connection, "last_ibkr_error", None)):
            raise IBKROrderError(READ_ONLY_ORDER_API_MESSAGE)

    async def _with_order_retry(self, call: Any, *, operation: str) -> Any:
        self._raise_if_order_api_read_only()
        try:
            return await self._connection.with_retry(call, operation=operation)
        except RuntimeError as exc:
            if _is_read_only_api_error(getattr(self._connection, "last_ibkr_error", None)):
                raise IBKROrderError(READ_ONLY_ORDER_API_MESSAGE) from exc
            raise

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
                raise IBKROrderError(
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
    # Public methods — order lifecycle
    # ------------------------------------------------------------------

    async def place_order(self, request: PlaceOrderRequest) -> OrderResponse:
        """Submit a new order to IBKR.

        Creates the ib_insync Contract and Order from the request, places
        the order, waits briefly for acknowledgment, and returns the result.
        The order is UUID-tagged and cached to Redis for auditability.

        If ``request.idempotency_key`` is provided, reserves it in Redis before
        any live submission. Concurrent callers with the same key wait for the
        reserved order's cached response instead of submitting another order.
        """
        order_uuid = str(_uuid.uuid4())
        if request.idempotency_key and self._redis is not None:
            reserved, reserved_uuid = await self._reserve_idempotency_key(request.idempotency_key, order_uuid)
            if not reserved:
                logger.info(
                    "place_order: idempotency key=%s is already reserved by order_uuid=%s",
                    request.idempotency_key,
                    reserved_uuid,
                )
                return await self._wait_for_idempotent_response(request.idempotency_key, reserved_uuid)

        envelope = OrderEnvelope(
            order_uuid=order_uuid,
            action="place",
            request=request.model_dump(),
            account_id=request.account_id,
            metadata={"symbol": request.symbol, "action": request.action.value},
        )

        logger.info(
            "place_order: uuid=%s symbol=%s action=%s type=%s qty=%.0f price=%s",
            order_uuid,
            request.symbol,
            request.action.value,
            request.order_type.value,
            request.quantity,
            request.price,
        )
        contract = self._build_ibkr_contract(request)
        order = self._build_ibkr_order(request)
        order.orderRef = order_uuid

        await self._cache_envelope(envelope, ttl_seconds=0)
        try:
            await self._connection.ensure_connected()
            qualified = await self._connection.with_retry(
                lambda: self._ib.qualifyContractsAsync(contract),
                operation=f"qualify_order_contract:{request.symbol}",
            )
            if qualified:
                contract = qualified[0]

            await wait_for_ibkr_request(self._connection, operation=f"place_order:{request.symbol}")
            trade = self._ib.placeOrder(contract, order)
            status = await self._wait_for_order_status(trade, timeout_seconds=5.0)
        except Exception as exc:
            response = OrderResponse(
                order_id=getattr(order, "orderId", 0) or 0,
                status=OrderStatus.INACTIVE,
                message=str(exc),
                warnings=[str(exc)],
                order_uuid=order_uuid,
            )
            envelope.ibkr_order_id = response.order_id
            envelope.status = OrderStatus.INACTIVE
            envelope.response = response.model_dump()
            envelope.touch()
            await self._cache_envelope(envelope, ttl_seconds=0)
            raise

        warnings: list[str] = []
        msg: str | None = None
        ibkr_status = getattr(trade.orderStatus, "status", "")
        if ibkr_status.lower() == "inactive":
            msg = getattr(trade.orderStatus, "message", "") or "Order submitted but inactive"
            warnings.append(msg)

        order_id = getattr(order, "orderId", 0) or getattr(trade, "orderId", 0)

        response = OrderResponse(
            order_id=order_id,
            status=status,
            message=msg,
            warnings=warnings,
            order_uuid=order_uuid,
        )
        envelope.ibkr_order_id = order_id
        envelope.status = status
        envelope.response = response.model_dump()
        envelope.touch()

        await self._cache_envelope(envelope, ttl_seconds=0)

        logger.info(
            "place_order: uuid=%s order_id=%s status=%s symbol=%s",
            order_uuid,
            order_id,
            status.value,
            request.symbol,
        )

        return response

    async def cancel_order(self, account_id: str, order_id: int) -> CancelOrderResponse:
        """Cancel an existing order by account ID and order ID."""
        await self._connection.ensure_connected()
        if not account_id.strip():
            raise IBKROrderError("account_id is required to cancel orders")
        if order_id <= 0:
            raise IBKROrderError("bound IBKR order_id is required to cancel orders")

        cancel_uuid = str(_uuid.uuid4())
        logger.info("cancel_order: uuid=%s account=%s order_id=%d", cancel_uuid, account_id, order_id)

        envelope = OrderEnvelope(
            order_uuid=cancel_uuid,
            ibkr_order_id=order_id,
            action="cancel",
            request={"account_id": account_id, "order_id": order_id},
            account_id=account_id,
            metadata={"cancel_of_order_id": order_id},
        )

        open_trades = await self._load_open_trades_for_action(operation="cancel_open_orders_refresh")
        target_trade = self._find_trade_by_order_id(open_trades, order_id)
        target_order = getattr(target_trade, "order", None) if target_trade is not None else None

        if target_order is None:
            logger.warning("cancel_order: order_id=%d not found in open trades", order_id)
            result = CancelOrderResponse(
                order_id=order_id,
                status="not_found",
                message=f"Order {order_id} not found in open orders",
            )
            envelope.status = OrderStatus.PENDING
            envelope.response = result.model_dump()
            await self._cache_envelope(envelope, ttl_seconds=0)
            return result

        target_account = str(getattr(target_order, "acctCode", "") or getattr(target_order, "account", "")).strip()
        if target_account and target_account != account_id:
            raise IBKROrderError(f"Order {order_id} belongs to account {target_account}, not {account_id}")

        order_status = getattr(target_trade, "orderStatus", None) or target_order
        current_status = str(getattr(order_status, "status", "")).lower()
        if current_status in ("filled", "cancelled", "api cancelled"):
            logger.info("cancel_order: order_id=%d already %s", order_id, current_status)
            result = CancelOrderResponse(
                order_id=order_id,
                status="already_terminal",
                message=f"Order already {current_status}",
            )
            envelope.status = OrderStatus.CANCELLED
            envelope.response = result.model_dump()
            await self._cache_envelope(envelope, ttl_seconds=0)
            return result

        await wait_for_ibkr_request(self._connection, operation="cancel_order")
        self._ib.cancelOrder(target_order)
        logger.info("cancel_order: cancel requested for order_id=%d", order_id)

        result = CancelOrderResponse(
            order_id=order_id,
            status="cancel_requested",
            message="Cancel request sent",
        )
        envelope.status = OrderStatus.PENDING
        envelope.response = result.model_dump()
        await self._cache_envelope(envelope, ttl_seconds=0)
        return result

    async def modify_order(
        self,
        account_id: str,
        order_id: int,
        modifications: ModifyOrderRequest,
    ) -> OrderResponse:
        """Modify an existing order."""
        await self._connection.ensure_connected()
        if not account_id.strip():
            raise IBKROrderError("account_id is required to modify orders")
        if order_id <= 0:
            raise IBKROrderError("bound IBKR order_id is required to modify orders")

        modify_uuid = str(_uuid.uuid4())
        logger.info("modify_order: uuid=%s account=%s order_id=%d", modify_uuid, account_id, order_id)

        envelope = OrderEnvelope(
            order_uuid=modify_uuid,
            ibkr_order_id=order_id,
            action="modify",
            request=modifications.model_dump(exclude_none=True),
            account_id=account_id,
            metadata={"modify_of_order_id": order_id},
        )

        open_trades = await self._load_open_trades_for_action(operation="modify_open_orders_refresh")
        target_trade = self._find_trade_by_order_id(open_trades, order_id)

        if target_trade is None:
            raise IBKROrderError(f"Order {order_id} not found in open orders")

        order = target_trade.order
        contract = target_trade.contract
        target_account = str(getattr(order, "acctCode", "") or getattr(order, "account", "")).strip()
        if target_account and target_account != account_id:
            raise IBKROrderError(f"Order {order_id} belongs to account {target_account}, not {account_id}")

        if modifications.price is not None:
            order.lmtPrice = modifications.price
        if modifications.quantity is not None:
            order.totalQuantity = modifications.quantity
        if modifications.tif is not None:
            order.tif = modifications.tif.value
        order.orderRef = modify_uuid

        await wait_for_ibkr_request(self._connection, operation="modify_order")
        modified_trade = self._ib.placeOrder(contract, order)
        status = await self._wait_for_order_status(modified_trade, timeout_seconds=5.0)

        response = OrderResponse(
            order_id=order_id,
            status=status,
            message="Order modified",
            order_uuid=modify_uuid,
        )

        envelope.status = status
        envelope.response = response.model_dump()
        await self._cache_envelope(envelope, ttl_seconds=0)

        logger.info(
            "modify_order: uuid=%s order_id=%d status=%s",
            modify_uuid,
            order_id,
            status.value,
        )

        return response

    async def preview_order(self, request: PlaceOrderRequest) -> WhatIfOrderResponse:
        """Pre-trade margin and commission preview (what-if order)."""
        await self._connection.ensure_connected()

        preview_uuid = str(_uuid.uuid4())
        logger.info(
            "preview_order: uuid=%s symbol=%s action=%s type=%s qty=%.0f",
            preview_uuid,
            request.symbol,
            request.action.value,
            request.order_type.value,
            request.quantity,
        )

        envelope = OrderEnvelope(
            order_uuid=preview_uuid,
            action="preview",
            request=request.model_dump(),
            account_id=request.account_id,
            metadata={"symbol": request.symbol, "action": request.action.value},
        )

        contract = self._build_ibkr_contract(request)
        order = self._build_ibkr_order(request, what_if=True)

        qualified = await self._connection.with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_preview_contract:{request.symbol}",
        )
        if qualified:
            contract = qualified[0]

        order.orderRef = preview_uuid
        await wait_for_ibkr_request(self._connection, operation=f"preview_order:place:{request.symbol}")
        trade = self._ib.placeOrder(contract, order)

        try:
            await asyncio.sleep(0.5)
            result = normalize_what_if_response(trade)
        finally:
            try:
                await wait_for_ibkr_request(self._connection, operation=f"preview_order:cancel:{request.symbol}")
                self._ib.cancelOrder(order)
            except Exception:
                logger.debug("preview_order: uuid=%s what-if cancel skipped", preview_uuid, exc_info=True)

        envelope.status = OrderStatus.PENDING
        envelope.response = result.model_dump()
        await self._cache_envelope(envelope, ttl_seconds=0)

        logger.info(
            "preview_order: uuid=%s symbol=%s initial_margin=%s maintenance_margin=%s",
            preview_uuid,
            request.symbol,
            result.initial_margin,
            result.maintenance_margin,
        )
        return result

    async def load_open_orders(self) -> list[OpenOrder]:
        """Load all currently open (working) orders."""
        await self._connection.ensure_connected()
        logger.info("load_open_orders: starting")

        trades = await self._with_order_retry(
            lambda: self._ib.reqOpenOrdersAsync(),
            operation="open_orders",
        )

        result = [normalize_open_order(trade) for trade in (trades or [])]
        logger.info("load_open_orders: %d open orders loaded", len(result))
        return result

    async def load_executions(self, request: ExecutionRequest) -> ExecutionResponse:
        """Load execution/fill details with optional filtering."""
        await self._connection.ensure_connected()
        logger.info(
            "load_executions: account=%s symbol=%s since=%s",
            request.account_id,
            request.symbol,
            request.since,
        )
        try:
            from ib_insync import ExecutionFilter
            exec_filter = ExecutionFilter()
        except ImportError:
            logger.debug("ib_insync ExecutionFilter unavailable; using simple namespace", exc_info=True)
            exec_filter = SimpleNamespace()
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
            exec_filter.time = request.since.strftime("%Y%m%d %H:%M:%S")

        fills = await self._with_order_retry(
            lambda: self._ib.reqExecutionsAsync(exec_filter),
            operation="executions",
        )

        executions = [normalize_execution(fill) for fill in (fills or [])]
        logger.info("load_executions: %d fills loaded", len(executions))

        return ExecutionResponse(
            executions=executions,
            total_count=len(executions),
        )

    async def load_completed_orders(self) -> list[CompletedOrder]:
        """Load completed (filled/cancelled) order history."""
        await self._connection.ensure_connected()
        logger.info("load_completed_orders: starting")

        trades = await self._with_order_retry(
            lambda: self._ib.reqCompletedOrdersAsync(),
            operation="completed_orders",
        )

        result = [normalize_completed_order(trade) for trade in (trades or [])]
        logger.info("load_completed_orders: %d completed orders loaded", len(result))
        return result

    # ------------------------------------------------------------------
    # Bracket and OCA orders
    # ------------------------------------------------------------------

    @staticmethod
    def _sec_type_from_asset_class(value: str) -> str:
        mapping = {
            "EQUITY": "STK",
            "STOCK": "STK",
            "STK": "STK",
            "FUTURES": "FUT",
            "FUTURE": "FUT",
            "FUT": "FUT",
            "OPTION": "OPT",
            "OPT": "OPT",
            "FX": "CASH",
            "CASH": "CASH",
            "INDEX": "IND",
            "IND": "IND",
            "BOND": "BOND",
        }
        return mapping.get(value.strip().upper(), "STK")

    @classmethod
    def _simple_contract(cls, *, symbol: str, asset_class: str, exchange: str, currency: str) -> Any:
        sec_type = cls._sec_type_from_asset_class(asset_class)
        try:
            from ib_insync import Contract
            return Contract(
                symbol=symbol,
                secType=sec_type,
                exchange=exchange,
                currency=currency,
            )
        except ImportError:
            logger.debug("ib_insync Contract unavailable; using simple contract namespace", exc_info=True)
            return SimpleNamespace(symbol=symbol, secType=sec_type, exchange=exchange, currency=currency, conId=0)

    async def place_bracket_order(self, request: BracketOrderRequest) -> BracketOrderResponse:
        """Place a parent order with attached take-profit and stop-loss orders."""
        await self._connection.ensure_connected()
        order_uuid = str(_uuid.uuid4())
        logger.info(
            "place_bracket_order: uuid=%s symbol=%s action=%s qty=%s",
            order_uuid,
            request.symbol,
            request.action.value,
            request.quantity,
        )

        envelope = OrderEnvelope(
            order_uuid=order_uuid,
            action="bracket",
            request=request.model_dump(mode="json"),
            account_id=request.account or None,
            metadata={"symbol": request.symbol, "action": request.action.value, "order_type": request.order_type.value},
        )
        await self._cache_envelope(envelope, ttl_seconds=0)

        contract = self._simple_contract(
            symbol=request.symbol,
            asset_class=request.asset_class,
            exchange=request.exchange,
            currency=request.currency,
        )
        qualified = await self._connection.with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_bracket_contract:{request.symbol}",
        )
        if not qualified:
            raise IBKROrderError(f"Could not qualify contract for {request.symbol}")
        contract = qualified[0]

        bracket = self._ib.bracketOrder(
            action=request.action.value,
            quantity=request.quantity,
            limitPrice=request.limit_price or 0.0,
            takeProfitPrice=request.take_profit_price,
            stopLossPrice=request.stop_loss_price,
        )
        if not getattr(bracket, "orders", None) or len(bracket.orders) < 3:
            raise IBKROrderError("IBKR did not build the expected three-leg bracket order")

        bracket.orders[0].orderType = request.order_type.value
        bracket.orders[0].tif = request.tif.value
        if request.account:
            for order in bracket.orders:
                order.account = request.account
                order.acctCode = request.account
        for order in bracket.orders:
            order.orderRef = order_uuid

        trades = []
        try:
            for index, order in enumerate(bracket.orders):
                await wait_for_ibkr_request(self._connection, operation=f"place_bracket_order:{request.symbol}:{index}")
                trades.append(self._ib.placeOrder(contract, order))
        except Exception:
            envelope.status = OrderStatus.INACTIVE
            envelope.response = {"error": "bracket order submission failed"}
            await self._cache_envelope(envelope, ttl_seconds=0)
            raise

        response = BracketOrderResponse(
            symbol=request.symbol,
            action=request.action,
            quantity=request.quantity,
            orders_placed=len(trades),
            parent_order_id=getattr(bracket.orders[0], "orderId", None),
            take_profit_order_id=getattr(bracket.orders[1], "orderId", None),
            stop_loss_order_id=getattr(bracket.orders[2], "orderId", None),
            order_uuid=order_uuid,
        )
        envelope.ibkr_order_id = response.parent_order_id
        envelope.status = OrderStatus.SUBMITTED
        envelope.response = response.model_dump(mode="json")
        await self._cache_envelope(envelope, ttl_seconds=0)
        return response

    async def place_oca_group(self, request: OcaGroupRequest) -> list[OcaOrderResponse]:
        """Place an OCA group after qualifying every leg up front."""
        await self._connection.ensure_connected()
        order_uuid = str(_uuid.uuid4())
        group_name = request.oca_group or f"oca_{_uuid.uuid4().hex[:8]}"
        logger.info("place_oca_group: uuid=%s group=%s orders=%d", order_uuid, group_name, len(request.orders))

        envelope = OrderEnvelope(
            order_uuid=order_uuid,
            action="oca",
            request=request.model_dump(mode="json"),
            account_id=request.account or None,
            metadata={"oca_group": group_name, "orders": len(request.orders)},
        )
        await self._cache_envelope(envelope, ttl_seconds=0)

        qualified_contracts: list[Any] = []
        qualification_errors: list[str] = []
        for item in request.orders:
            contract = self._simple_contract(
                symbol=item.symbol,
                asset_class=item.asset_class,
                exchange=item.exchange,
                currency=item.currency,
            )
            try:
                qualified = await self._connection.with_retry(
                    lambda c=contract, s=item.symbol: self._ib.qualifyContractsAsync(c),
                    operation=f"qualify_oca_contract:{item.symbol}",
                )
            except Exception as exc:
                qualification_errors.append(f"{item.symbol}: {exc}")
                continue
            if not qualified:
                qualification_errors.append(f"{item.symbol}: could not qualify contract")
                continue
            qualified_contracts.append(qualified[0])

        if qualification_errors:
            envelope.status = OrderStatus.INACTIVE
            envelope.response = {"errors": qualification_errors}
            await self._cache_envelope(envelope, ttl_seconds=0)
            raise IBKROrderError("OCA group rejected before submission: " + "; ".join(qualification_errors))

        try:
            from ib_insync import Order
        except ImportError:
            logger.debug("ib_insync Order unavailable; using simple order namespace", exc_info=True)
            Order = SimpleNamespace

        responses: list[OcaOrderResponse] = []
        try:
            for item, contract in zip(request.orders, qualified_contracts, strict=True):
                order = Order()
                order.action = item.action.value
                order.totalQuantity = item.quantity
                order.orderType = item.order_type.value
                order.tif = item.tif.value
                order.ocaGroup = group_name
                order.ocaType = request.oca_type
                order.orderRef = order_uuid
                if item.price is not None:
                    order.lmtPrice = item.price
                if request.account:
                    order.account = request.account
                    order.acctCode = request.account

                await wait_for_ibkr_request(self._connection, operation=f"place_oca_order:{item.symbol}")
                trade = self._ib.placeOrder(contract, order)
                responses.append(
                    OcaOrderResponse(
                        symbol=item.symbol,
                        action=item.action,
                        quantity=item.quantity,
                        order_id=getattr(trade.order, "orderId", getattr(order, "orderId", 0)) or 0,
                        oca_group=group_name,
                        order_uuid=order_uuid,
                    )
                )
        except Exception:
            envelope.status = OrderStatus.INACTIVE
            envelope.response = {"error": "OCA group submission failed after qualification"}
            await self._cache_envelope(envelope, ttl_seconds=0)
            raise

        envelope.ibkr_order_id = responses[0].order_id if responses else None
        envelope.status = OrderStatus.SUBMITTED
        envelope.response = {"orders": [item.model_dump(mode="json") for item in responses]}
        await self._cache_envelope(envelope, ttl_seconds=0)
        return responses

    # ------------------------------------------------------------------
    # Global cancel
    # ------------------------------------------------------------------

    async def cancel_all_orders(self) -> dict[str, Any]:
        """Cancel all open orders globally via ``reqGlobalCancel()``.

        **WARNING**: This cancels every open order across all accounts
        on the connected TWS/IB Gateway instance.
        """
        await self._connection.ensure_connected()
        logger.warning("cancel_all_orders: issuing global cancel for all open orders")

        await wait_for_ibkr_request(self._connection, operation="global_cancel")
        self._ib.reqGlobalCancel()

        logger.info("cancel_all_orders: global cancel requested")
        return {
            "status": "global_cancel_requested",
            "message": "All open orders across all accounts have been cancelled.",
        }

    # ------------------------------------------------------------------
    # All open orders (across all accounts)
    # ------------------------------------------------------------------

    async def get_all_open_orders(self) -> list[OpenOrder]:
        """Load all open orders across all accounts via ``reqAllOpenOrdersAsync()``.

        Unlike ``load_open_orders`` which uses ``reqOpenOrdersAsync()`` (client-id
        scoped), this method returns orders from *all* API clients.
        """
        await self._connection.ensure_connected()
        logger.info("get_all_open_orders: starting")

        trades = await self._with_order_retry(
            lambda: self._ib.reqAllOpenOrdersAsync(),
            operation="all_open_orders",
        )

        result = [normalize_open_order(trade) for trade in (trades or [])]
        logger.info("get_all_open_orders: %d open orders loaded", len(result))
        return result

    # ------------------------------------------------------------------
    # Option exercise / lapse
    # ------------------------------------------------------------------

    async def exercise_option(
        self,
        symbol: str,
        right: str,
        strike: float,
        expiry: str,
        exercise_action: int,  # 1=exercise, 2=lapse
        quantity: int,
        account: str,
        exchange: str = "SMART",
        currency: str = "USD",
        override: bool = False,
        manual_order_time: str = "",
    ) -> dict[str, Any]:
        """Exercise or lapse an option position.

        Args:
            symbol: Underlying symbol (e.g. ``"AAPL"``).
            right: Option right (``"C"`` or ``"P"``).
            strike: Strike price.
            expiry: Expiry in ``YYYYMMDD`` format.
            exercise_action: ``1`` to exercise, ``2`` to lapse.
            quantity: Number of contracts.
            account: IBKR account code.
            exchange: Exchange (default ``"SMART"``).
            currency: Currency (default ``"USD"``).
            override: Override exercise restrictions.
            manual_order_time: Manual order time for position transfer.
        """
        await self._connection.ensure_connected()
        action_label = "exercise" if exercise_action == 1 else "lapse"
        logger.warning(
            "exercise_option: symbol=%s right=%s strike=%.2f expiry=%s action=%s qty=%d account=%s",
            symbol, right, strike, expiry, action_label, quantity, account,
        )

        try:
            from ib_insync import Option as IBKROption
            contract = IBKROption()
        except ImportError:
            logger.debug("ib_insync Option unavailable; using simple option namespace", exc_info=True)
            contract = SimpleNamespace()
        contract.symbol = symbol.upper()
        contract.secType = "OPT"
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiry
        contract.strike = strike
        contract.right = right.upper()

        # Qualify the contract
        qualified = await self._connection.with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_exercise_option:{symbol}",
        )
        if qualified:
            contract = qualified[0]

        await wait_for_ibkr_request(self._connection, operation=f"exercise_option:{symbol}")
        exercise_kwargs: dict[str, Any] = {
            "exerciseAction": exercise_action,
            "exerciseQuantity": quantity,
            "account": account,
            "override": override,
        }
        if "manualOrderTime" in inspect.signature(self._ib.exerciseOptions).parameters:
            exercise_kwargs["manualOrderTime"] = manual_order_time
        elif manual_order_time:
            logger.warning(
                "exercise_option: manual_order_time=%s ignored because installed ib_insync exerciseOptions() "
                "does not expose manualOrderTime",
                manual_order_time,
            )
        self._ib.exerciseOptions(contract, **exercise_kwargs)

        logger.info(
            "exercise_option: %s requested for %s %s %.2f %s qty=%d",
            action_label, symbol, right, strike, expiry, quantity,
        )
        return {
            "status": "exercise_requested",
            "symbol": symbol,
            "right": right.upper(),
            "strike": strike,
            "expiry": expiry,
            "exercise_action": action_label,
            "quantity": quantity,
            "account": account,
        }

    # ------------------------------------------------------------------
    # Idempotency key helpers
    # ------------------------------------------------------------------

    _IDEMPOTENCY_KEY_PREFIX = "order_idempotency:"
    _IDEMPOTENCY_TTL_SECONDS = 86400  # 24 hours
    _IDEMPOTENCY_WAIT_TIMEOUT_SECONDS = 10.0
    _IDEMPOTENCY_WAIT_INTERVAL_SECONDS = 0.05

    async def _reserve_idempotency_key(self, idempotency_key: str, order_uuid: str) -> tuple[bool, str]:
        """Atomically reserve an idempotency key before live order submission."""
        try:
            key = f"{self._IDEMPOTENCY_KEY_PREFIX}{idempotency_key}"
            raw_client = await self._redis.raw_client()
            reserved = await raw_client.set(
                key,
                order_uuid,
                ex=self._IDEMPOTENCY_TTL_SECONDS,
                nx=True,
            )
            if reserved:
                logger.debug(
                    "_reserve_idempotency_key: reserved key=%s -> uuid=%s (TTL=%ds)",
                    idempotency_key,
                    order_uuid,
                    self._IDEMPOTENCY_TTL_SECONDS,
                )
                return True, order_uuid

            cached = await raw_client.get(key)
            if cached is not None:
                return False, cached.decode("utf-8") if isinstance(cached, bytes) else str(cached)

            # The key may have expired between the failed SET NX and GET. Try
            # once more; if it still cannot be read, fail closed.
            reserved = await raw_client.set(
                key,
                order_uuid,
                ex=self._IDEMPOTENCY_TTL_SECONDS,
                nx=True,
            )
            if reserved:
                return True, order_uuid
            cached = await raw_client.get(key)
            if cached is not None:
                return False, cached.decode("utf-8") if isinstance(cached, bytes) else str(cached)
            raise IBKROrderError("idempotency key reservation disappeared before it could be read")
        except IBKROrderError:
            raise
        except Exception:
            logger.warning("_reserve_idempotency_key: failed for key=%s", idempotency_key, exc_info=True)
            raise IBKROrderError("idempotency key could not be reserved in Redis")

    async def _wait_for_idempotent_response(self, idempotency_key: str, order_uuid: str) -> OrderResponse:
        """Wait for the original request with this idempotency key to finish."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._IDEMPOTENCY_WAIT_TIMEOUT_SECONDS
        while True:
            lookup = await self.get_cached_order(order_uuid)
            if lookup.found and lookup.envelope is not None and lookup.envelope.response:
                return OrderResponse.model_validate(lookup.envelope.response)
            if loop.time() >= deadline:
                raise IBKROrderError(
                    f"idempotency key {idempotency_key!r} is already processing order_uuid={order_uuid}; retry later"
                )
            await asyncio.sleep(self._IDEMPOTENCY_WAIT_INTERVAL_SECONDS)


def _asset_class_from_sec_type(sec_type: str) -> AssetClass | None:
    mapping = {
        "STK": AssetClass.EQUITY,
        "CASH": AssetClass.FX,
        "FUT": AssetClass.FUTURE,
        "IND": AssetClass.INDEX,
        "BOND": AssetClass.BOND,
        "CRYPTO": AssetClass.CRYPTO,
        "OPT": AssetClass.OPTION,
        "FOP": AssetClass.OPTION,
    }
    return mapping.get(sec_type.strip().upper())
