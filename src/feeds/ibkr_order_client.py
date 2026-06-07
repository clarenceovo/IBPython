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
from src.feeds.exceptions import IBKROrderError
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
    }
    return mapping.get(sec_type.strip().upper())
