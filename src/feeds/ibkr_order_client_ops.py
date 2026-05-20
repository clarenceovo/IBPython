"""IBKR order operations — place, cancel, modify, preview, executions.

Public order methods extracted from ``ibkr_order_client.py`` as module-level
async functions. Each takes the ``IBKROrderClient`` instance as its first
argument so the main class can delegate without maintaining all the logic
in one file.
"""

from __future__ import annotations

import asyncio
import logging
import uuid as _uuid
from typing import Any

from src.feeds.ibkr_connection import wait_for_ibkr_request
from src.feeds.orders import (
    CancelOrderResponse,
    CompletedOrder,
    ExecutionResponse,
    ModifyOrderRequest,
    OpenOrder,
    OrderEnvelope,
    OrderResponse,
    OrderStatus,
    PlaceOrderRequest,
    WhatIfOrderResponse,
    normalize_completed_order,
    normalize_execution,
    normalize_open_order,
    normalize_what_if_response,
)

logger = logging.getLogger(__name__)


async def place_order(client: Any, request: PlaceOrderRequest) -> OrderResponse:
    """Submit a new order to IBKR.

    Creates the ib_insync Contract and Order from the request, places
    the order, waits briefly for acknowledgment, and returns the result.
    The order is UUID-tagged and cached to Redis for auditability.
    """
    await client._connection.ensure_connected()

    order_uuid = str(_uuid.uuid4())
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
    contract = client._build_ibkr_contract(request)
    order = client._build_ibkr_order(request)
    order.orderRef = order_uuid

    await client._cache_envelope(envelope, ttl_seconds=0)
    try:
        qualified = await client._connection.with_retry(
            lambda: client._ib.qualifyContractsAsync(contract),
            operation=f"qualify_order_contract:{request.symbol}",
        )
        if qualified:
            contract = qualified[0]

        await wait_for_ibkr_request(client._connection, operation=f"place_order:{request.symbol}")
        trade = client._ib.placeOrder(contract, order)
        status = await client._wait_for_order_status(trade, timeout_seconds=5.0)
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
        await client._cache_envelope(envelope, ttl_seconds=0)
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

    await client._cache_envelope(envelope, ttl_seconds=0)

    logger.info(
        "place_order: uuid=%s order_id=%s status=%s symbol=%s",
        order_uuid,
        order_id,
        status.value,
        request.symbol,
    )

    return response


async def cancel_order(client: Any, account_id: str, order_id: int) -> CancelOrderResponse:
    """Cancel an existing order by account ID and order ID."""
    await client._connection.ensure_connected()
    if not account_id.strip():
        raise RuntimeError("account_id is required to cancel orders")
    if order_id <= 0:
        raise RuntimeError("bound IBKR order_id is required to cancel orders")

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

    open_trades = await client._load_open_trades_for_action(operation="cancel_open_orders_refresh")
    target_trade = client._find_trade_by_order_id(open_trades, order_id)
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
        await client._cache_envelope(envelope, ttl_seconds=0)
        return result

    target_account = str(getattr(target_order, "acctCode", "") or getattr(target_order, "account", "")).strip()
    if target_account and target_account != account_id:
        raise RuntimeError(f"Order {order_id} belongs to account {target_account}, not {account_id}")

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
        await client._cache_envelope(envelope, ttl_seconds=0)
        return result

    await wait_for_ibkr_request(client._connection, operation="cancel_order")
    client._ib.cancelOrder(target_order)
    logger.info("cancel_order: cancel requested for order_id=%d", order_id)

    result = CancelOrderResponse(
        order_id=order_id,
        status="cancel_requested",
        message="Cancel request sent",
    )
    envelope.status = OrderStatus.PENDING
    envelope.response = result.model_dump()
    await client._cache_envelope(envelope, ttl_seconds=0)
    return result


async def modify_order(
    client: Any,
    account_id: str,
    order_id: int,
    modifications: ModifyOrderRequest,
) -> OrderResponse:
    """Modify an existing order."""
    await client._connection.ensure_connected()
    if not account_id.strip():
        raise RuntimeError("account_id is required to modify orders")
    if order_id <= 0:
        raise RuntimeError("bound IBKR order_id is required to modify orders")

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

    open_trades = await client._load_open_trades_for_action(operation="modify_open_orders_refresh")
    target_trade = client._find_trade_by_order_id(open_trades, order_id)

    if target_trade is None:
        raise RuntimeError(f"Order {order_id} not found in open orders")

    order = target_trade.order
    contract = target_trade.contract
    target_account = str(getattr(order, "acctCode", "") or getattr(order, "account", "")).strip()
    if target_account and target_account != account_id:
        raise RuntimeError(f"Order {order_id} belongs to account {target_account}, not {account_id}")

    if modifications.price is not None:
        order.lmtPrice = modifications.price
    if modifications.quantity is not None:
        order.totalQuantity = modifications.quantity
    if modifications.tif is not None:
        order.tif = modifications.tif.value
    order.orderRef = modify_uuid

    await wait_for_ibkr_request(client._connection, operation="modify_order")
    modified_trade = client._ib.placeOrder(contract, order)
    status = await client._wait_for_order_status(modified_trade, timeout_seconds=5.0)

    response = OrderResponse(
        order_id=order_id,
        status=status,
        message="Order modified",
        order_uuid=modify_uuid,
    )

    envelope.status = status
    envelope.response = response.model_dump()
    await client._cache_envelope(envelope, ttl_seconds=0)

    logger.info(
        "modify_order: uuid=%s order_id=%d status=%s",
        modify_uuid,
        order_id,
        status.value,
    )

    return response


async def preview_order(client: Any, request: PlaceOrderRequest) -> WhatIfOrderResponse:
    """Pre-trade margin and commission preview (what-if order)."""
    await client._connection.ensure_connected()

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

    contract = client._build_ibkr_contract(request)
    order = client._build_ibkr_order(request, what_if=True)

    qualified = await client._connection.with_retry(
        lambda: client._ib.qualifyContractsAsync(contract),
        operation=f"qualify_preview_contract:{request.symbol}",
    )
    if qualified:
        contract = qualified[0]

    order.orderRef = preview_uuid
    await wait_for_ibkr_request(client._connection, operation=f"preview_order:place:{request.symbol}")
    trade = client._ib.placeOrder(contract, order)

    try:
        await asyncio.sleep(0.5)
        result = normalize_what_if_response(trade)
    finally:
        try:
            await wait_for_ibkr_request(client._connection, operation=f"preview_order:cancel:{request.symbol}")
            client._ib.cancelOrder(order)
        except Exception:
            logger.debug("preview_order: uuid=%s what-if cancel skipped", preview_uuid, exc_info=True)

    envelope.status = OrderStatus.PENDING
    envelope.response = result.model_dump()
    await client._cache_envelope(envelope, ttl_seconds=0)

    logger.info(
        "preview_order: uuid=%s symbol=%s initial_margin=%s maintenance_margin=%s",
        preview_uuid,
        request.symbol,
        result.initial_margin,
        result.maintenance_margin,
    )
    return result


async def load_open_orders(client: Any) -> list[OpenOrder]:
    """Load all currently open (working) orders."""
    await client._connection.ensure_connected()
    logger.info("load_open_orders: starting")

    trades = await client._connection.with_retry(
        lambda: client._ib.reqOpenOrdersAsync(),
        operation="open_orders",
    )

    result = [normalize_open_order(trade) for trade in (trades or [])]
    logger.info("load_open_orders: %d open orders loaded", len(result))
    return result


async def load_executions(client: Any, request: Any) -> ExecutionResponse:
    """Load execution/fill details with optional filtering."""
    await client._connection.ensure_connected()
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

    fills = await client._connection.with_retry(
        lambda: client._ib.reqExecutionsAsync(exec_filter),
        operation="executions",
    )

    executions = [normalize_execution(fill) for fill in (fills or [])]
    logger.info("load_executions: %d fills loaded", len(executions))

    return ExecutionResponse(
        executions=executions,
        total_count=len(executions),
    )


async def load_completed_orders(client: Any) -> list[CompletedOrder]:
    """Load completed (filled/cancelled) order history."""
    await client._connection.ensure_connected()
    logger.info("load_completed_orders: starting")

    trades = await client._connection.with_retry(
        lambda: client._ib.reqCompletedOrdersAsync(),
        operation="completed_orders",
    )

    result = [normalize_completed_order(trade) for trade in (trades or [])]
    logger.info("load_completed_orders: %d completed orders loaded", len(result))
    return result
