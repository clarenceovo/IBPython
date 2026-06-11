"""REST router for IBKR order management — place, cancel, modify, executions, preview, cache, bracket, OCA."""

from __future__ import annotations

import json
import logging
import secrets
import uuid
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Response, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.orders import (
    CachedOrderLookup,
    CancelOrderResponse,
    CompletedOrder,
    ExecutionRequest,
    ExecutionResponse,
    ModifyOrderRequest,
    OpenOrder,
    OrderEnvelope,
    OrderResponse,
    PlaceOrderRequest,
    WhatIfOrderResponse,
)
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/orders", tags=["orders"])
order_bearer_scheme = HTTPBearer(
    auto_error=False,
    scheme_name="OrderBearerAuth",
    description="Bearer token payload stored in Redis under IBKR_ORDER_AUTH_REDIS_KEY.",
)


async def require_order_bearer_token(
    credentials: HTTPAuthorizationCredentials | None = Security(order_bearer_scheme),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> IBKRRestAppState:
    """Authorize order endpoints with a bearer token payload stored in Redis."""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = credentials.credentials.strip()
    if credentials.scheme.lower() != "bearer" or not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        raw_expected = await state.redis.get_raw(state.settings.ibkr_order_auth_redis_key)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="order bearer token could not be read from Redis",
        ) from exc
    if raw_expected is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="order bearer token is not configured in Redis",
        )
    expected = raw_expected.decode("utf-8") if isinstance(raw_expected, bytes) else str(raw_expected)
    if not secrets.compare_digest(token, expected.strip()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return state


# ------------------------------------------------------------------
# Order lifecycle
# ------------------------------------------------------------------

@router.post("/place", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def place_order(
    request: PlaceOrderRequest,
    response: Response,
    idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> OrderResponse:
    """Place a new order — supports market, limit, stop, trailing, and more.

    Every order is UUID-tagged and cached to Redis for auditability.
    The response includes `order_uuid` for client-side tracking.

    Supports idempotent requests via the `Idempotency-Key` header.
    If the same key is reused within 24h, the cached response is returned.
    """
    # Idempotency: replay cached response if available
    if idempotency_key is not None:
        cache_key = f"Idempotency::{idempotency_key}"
        cached_raw = await state.redis.get_raw(cache_key)
        if cached_raw is not None:
            logger.info("Replaying idempotent order response: key=%s", idempotency_key)
            cached_data = json.loads(cached_raw if isinstance(cached_raw, str) else cached_raw.decode("utf-8"))
            response.headers["Idempotency-Replayed"] = "true"
            return OrderResponse.model_validate(cached_data)
        result = await state.feed.place_order(request)
        # Cache the response in Redis with a 24h TTL
        try:
            await state.redis.set_raw(cache_key, result.model_dump_json(), ex=86400)
        except Exception:
            logger.warning("failed to cache idempotency response for key=%s", idempotency_key, exc_info=True)
        return result
    return await state.feed.place_order(request)


@router.post("/{order_id}/cancel", response_model=CancelOrderResponse)
async def cancel_order(
    order_id: int,
    account_id: str = Query(..., min_length=1, alias="account_id"),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> CancelOrderResponse:
    """Cancel an existing order by order ID and account ID.

    Creates a cancel envelope linked to the original order's UUID.
    """
    return await state.feed.cancel_order(account_id, order_id)


@router.post("/{order_id}/modify", response_model=OrderResponse)
async def modify_order(
    order_id: int,
    modifications: ModifyOrderRequest,
    account_id: str = Query(..., min_length=1, alias="account_id"),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> OrderResponse:
    """Modify an existing order — limited to price, quantity, and TIF.

    Creates a modify envelope linked to the original order's UUID.
    """
    return await state.feed.modify_order(account_id, order_id, modifications)


@router.get("/open", response_model=list[OpenOrder], summary="Get all open (working) orders")
async def load_open_orders(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> list[OpenOrder]:
    """Get all currently open (working) orders."""
    results = await state.feed.load_open_orders()
    return results[offset : offset + limit]


@router.post("/executions", response_model=ExecutionResponse)
async def load_executions(
    request: ExecutionRequest,
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> ExecutionResponse:
    """Get execution/fill details with optional filtering."""
    return await state.feed.load_executions(request)


@router.post("/preview", response_model=WhatIfOrderResponse)
async def preview_order(
    request: PlaceOrderRequest,
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> WhatIfOrderResponse:
    """Pre-trade margin & commission preview (what-if) — no order placed."""
    return await state.feed.preview_order(request)


@router.get("/completed", response_model=list[CompletedOrder], summary="Get completed order history")
async def load_completed_orders(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> list[CompletedOrder]:
    """Get completed (filled/cancelled) order history."""
    results = await state.feed.load_completed_orders()
    return results[offset : offset + limit]


# ------------------------------------------------------------------
# Order cache (UUID-tagged envelopes in Redis)
# ------------------------------------------------------------------

@router.get("/cache/{order_uuid}", response_model=CachedOrderLookup)
async def get_cached_order(
    order_uuid: str,
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> CachedOrderLookup:
    """Look up a cached order envelope by UUID.

    Every place/cancel/modify/preview action is cached to Redis with a UUID.
    Use this endpoint to retrieve the full envelope including original request,
    IBKR response, timestamps, and metadata.
    """
    return await state.feed.get_cached_order(order_uuid)


@router.get("/cache", response_model=list[OrderEnvelope], summary="List cached order envelopes")
async def list_cached_orders(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> list[OrderEnvelope]:
    """List all cached order envelopes from Redis.

    Returns the full audit trail of all order actions cached in Redis.
    Envelopes expire after 24 hours by default.
    """
    results = await state.feed.list_cached_orders()
    return results[offset : offset + limit]


# ------------------------------------------------------------------
# Bracket orders
# ------------------------------------------------------------------

class BracketOrderRequest(BaseModel):
    """Request body for placing a bracket order (parent + take-profit + stop-loss)."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    action: str = Field(default="BUY", pattern="^(BUY|SELL)$")
    quantity: float = Field(default=1.0, gt=0)
    limit_price: float | None = Field(default=None, gt=0)
    take_profit_price: float | None = Field(default=None, gt=0)
    stop_loss_price: float | None = Field(default=None, gt=0)
    order_type: str = Field(default="LMT", pattern="^(MKT|LMT)$")
    tif: str = Field(default="GTC")
    asset_class: str = Field(default="EQUITY")
    exchange: str = Field(default="SMART")
    currency: str = Field(default="USD")
    account: str = Field(default="")


BRACKET_ORDER_EXAMPLES = {
    "aapl_bracket": {
        "summary": "AAPL bracket order",
        "value": {
            "symbol": "AAPL",
            "action": "BUY",
            "quantity": 100,
            "limit_price": 185.0,
            "take_profit_price": 195.0,
            "stop_loss_price": 180.0,
            "order_type": "LMT",
            "tif": "GTC",
        },
    },
}


@router.post("/bracket", status_code=status.HTTP_201_CREATED, summary="Place a bracket order")
async def place_bracket_order(
    payload: Annotated[BracketOrderRequest, Body(openapi_examples=BRACKET_ORDER_EXAMPLES)],  # noqa: F821
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> dict:
    """Place a bracket order (parent + take-profit + stop-loss).

    Creates three linked orders: a parent entry order, an attached take-profit,
    and an attached stop-loss. When one of the exit orders fills, the other is cancelled.
    """
    from src.feeds.models import AssetClass

    asset_class_map = {
        "equity": "STK", "futures": "FUT", "future": "FUT",
        "option": "OPT", "fx": "CASH", "index": "IND", "bond": "BOND",
    }
    sec_type = asset_class_map.get(payload.asset_class.lower(), "STK")

    try:
        from ib_insync import Contract
    except ImportError:
        raise HTTPException(status_code=503, detail="ib_insync not available")

    ib = state.feed._connection.ib
    if ib is None:
        raise HTTPException(status_code=503, detail="IBKR connection not available")

    contract = Contract(
        symbol=payload.symbol,
        secType=sec_type,
        exchange=payload.exchange,
        currency=payload.currency,
    )
    qualified = await ib.qualifyContractsAsync(contract)
    if not qualified:
        raise HTTPException(status_code=400, detail=f"Could not qualify contract for {payload.symbol}")
    contract = qualified[0]

    bracket = ib.bracketOrder(
        action=payload.action.upper(),
        quantity=payload.quantity,
        limitPrice=payload.limit_price or 0.0,
        takeProfitPrice=payload.take_profit_price or 0.0,
        stopLossPrice=payload.stop_loss_price or 0.0,
    )
    bracket.orders[0].orderType = payload.order_type.upper()
    bracket.orders[0].tif = payload.tif

    if payload.account:
        for o in bracket.orders:
            o.account = payload.account

    trades = []
    for o in bracket.orders:
        trade = ib.placeOrder(contract, o)
        trades.append(trade)

    return {
        "symbol": payload.symbol,
        "action": payload.action,
        "quantity": payload.quantity,
        "orders_placed": len(trades),
        "parent_order_id": bracket.orders[0].orderId if bracket.orders else None,
        "take_profit_order_id": bracket.orders[1].orderId if len(bracket.orders) > 1 else None,
        "stop_loss_order_id": bracket.orders[2].orderId if len(bracket.orders) > 2 else None,
    }


# ------------------------------------------------------------------
# OCA (One-Cancels-All) groups
# ------------------------------------------------------------------

class OcaOrderItem(BaseModel):
    """Single order within an OCA group."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    action: str = Field(default="BUY", pattern="^(BUY|SELL)$")
    quantity: float = Field(gt=0)
    order_type: str = Field(default="LMT", pattern="^(MKT|LMT)$")
    price: float | None = Field(default=None, gt=0)
    asset_class: str = Field(default="EQUITY")
    exchange: str = Field(default="SMART")
    currency: str = Field(default="USD")


class OcaGroupRequest(BaseModel):
    """Request body for placing an OCA (One-Cancels-All) order group."""

    model_config = ConfigDict(extra="forbid")

    orders: list[OcaOrderItem] = Field(min_length=2)
    oca_group: str = Field(default="")
    oca_type: int = Field(default=1, ge=1, le=3)
    account: str = Field(default="")


OCA_GROUP_EXAMPLES = {
    "oca_two_legs": {
        "summary": "Two-leg OCA group",
        "value": {
            "orders": [
                {"symbol": "AAPL", "action": "BUY", "quantity": 100, "order_type": "LMT", "price": 185.0},
                {"symbol": "AAPL", "action": "BUY", "quantity": 100, "order_type": "LMT", "price": 180.0},
            ],
            "oca_type": 1,
        },
    },
}


@router.post("/oca", status_code=status.HTTP_201_CREATED, summary="Place an OCA order group")
async def place_oca_group(
    payload: Annotated[OcaGroupRequest, Body(openapi_examples=OCA_GROUP_EXAMPLES)],  # noqa: F821
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> list[dict]:
    """Place a One-Cancels-All group of orders.

    When any order in the group fills, all others are automatically cancelled.
    Requires at least 2 orders.
    """
    try:
        from ib_insync import Contract, Order
    except ImportError:
        raise HTTPException(status_code=503, detail="ib_insync not available")

    ib = state.feed._connection.ib
    if ib is None:
        raise HTTPException(status_code=503, detail="IBKR connection not available")

    group_name = payload.oca_group or f"oca_{uuid.uuid4().hex[:8]}"
    asset_class_map = {
        "equity": "STK", "futures": "FUT", "future": "FUT",
        "option": "OPT", "fx": "CASH", "index": "IND", "bond": "BOND",
    }
    results: list[dict] = []

    for od in payload.orders:
        sec_type = asset_class_map.get(od.asset_class.lower(), "STK")

        contract = Contract(
            symbol=od.symbol,
            secType=sec_type,
            exchange=od.exchange,
            currency=od.currency,
        )
        qualified = await ib.qualifyContractsAsync(contract)
        if not qualified:
            results.append({"error": f"Could not qualify contract for {od.symbol}", "symbol": od.symbol})
            continue
        contract = qualified[0]

        order = Order()
        order.action = od.action.upper()
        order.quantity = od.quantity
        order.orderType = od.order_type.upper()
        order.tif = "GTC"
        order.ocaGroup = group_name
        order.ocaType = payload.oca_type
        if od.price is not None:
            order.lmtPrice = od.price
        if payload.account:
            order.account = payload.account

        trade = ib.placeOrder(contract, order)
        results.append({
            "symbol": od.symbol,
            "action": od.action.upper(),
            "quantity": od.quantity,
            "order_id": trade.order.orderId,
            "oca_group": group_name,
        })

    return results
