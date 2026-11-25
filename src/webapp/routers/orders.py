"""REST router for IBKR order management — place, cancel, modify, executions, preview, cache, bracket, OCA."""

from __future__ import annotations

import logging
import secrets
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Path, Query, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.feeds.orders import (
    BracketOrderRequest,
    BracketOrderResponse,
    CachedOrderLookup,
    CancelOrderResponse,
    CompletedOrder,
    ExecutionRequest,
    ExecutionResponse,
    ModifyOrderRequest,
    OcaGroupRequest,
    OcaOrderResponse,
    OpenOrder,
    OrderEnvelope,
    OrderResponse,
    PlaceOrderRequest,
    WhatIfOrderResponse,
)
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from datetime import datetime
from src.feeds.research import aware_utc
from src.feeds.tca import TCAReport, build_tca_report

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/orders", tags=["orders"])

ORDERS_DISABLED_DETAIL = (
    "order pathways are disabled; set IBKR_ORDERS_ENABLED=true to enable "
    "place/modify/cancel/bracket/OCA/preview operations"
)


async def require_orders_enabled(state: IBKRRestAppState = Depends(get_rest_state)) -> IBKRRestAppState:
    """Reject order-writing endpoints when the IBKR_ORDERS_ENABLED master switch is off."""
    if not state.settings.ibkr_orders_enabled:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=ORDERS_DISABLED_DETAIL)
    return state


order_bearer_scheme = HTTPBearer(
    auto_error=False,
    scheme_name="OrderBearerAuth",
    description=(
        "Bearer token payload stored in Redis under IBKR_ORDER_AUTH_REDIS_KEY. "
        "When IBKR_API_BEARER_TOKEN is also set, send the order token via X-Order-Authorization."
    ),
)


async def require_order_bearer_token(
    credentials: HTTPAuthorizationCredentials | None = Security(order_bearer_scheme),
    order_authorization: str | None = Header(
        default=None,
        alias="X-Order-Authorization",
        description="Order-level bearer token (used alongside Authorization when IBKR_API_BEARER_TOKEN is set).",
    ),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> IBKRRestAppState:
    """Authorize order endpoints with a bearer token payload stored in Redis.

    Accepts ``X-Order-Authorization`` (``Bearer <token>`` or raw ``<token>``) so
    callers can pass both an API-wide bearer token in ``Authorization`` and a
    distinct order token without header collision. Falls back to
    ``Authorization: Bearer <token>`` when ``X-Order-Authorization`` is absent.
    """
    token = ""
    if order_authorization is not None:
        raw = order_authorization.strip()
        if raw.lower().startswith("bearer "):
            token = raw[7:].strip()
        else:
            token = raw
        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )
    else:
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

@router.get("/tca", response_model=TCAReport)
async def transaction_cost_analysis(
    account_id: str = Query(..., min_length=1, description="IBKR account id whose archived executions to analyze."),
    strategy_id: str | None = Query(
        default=None,
        description="Optional strategy tag to filter executions before aggregation.",
    ),
    start: datetime | None = Query(default=None, description="Optional inclusive start of the analysis window (UTC)."),
    end: datetime | None = Query(default=None, description="Optional exclusive end of the analysis window (UTC); must follow start."),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> TCAReport:
    """Analyze archived executions; sync /orders/executions after reconnects.

    Builds a transaction-cost report (fills, slippage vs arrival and VWAP,
    and implementation shortfall) from the execution analytics archive.
    The archive is populated when executions are loaded, so run
    POST /orders/executions first after reconnecting. Window inversions
    return 422; a missing archive returns 503.
    """
    if state.analytics_store is None:
        raise HTTPException(503, "execution analytics archive is not configured")
    try:
        start = aware_utc(start) if start else None
        end = aware_utc(end) if end else None
        if start and end and end <= start:
            raise ValueError("end must follow start")
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    envelopes, executions = await state.analytics_store.execution_inputs(account_id)
    return build_tca_report(account_id, envelopes, executions, strategy_id=strategy_id, start=start, end=end)

@router.post("/place", response_model=OrderResponse, status_code=status.HTTP_201_CREATED, dependencies=[Depends(require_orders_enabled)])
async def place_order(
    request: PlaceOrderRequest,
    idempotency_key: str | None = Header(
        default=None,
        alias="Idempotency-Key",
        description="Client-supplied key; reusing it within 24h returns the cached response instead of placing again.",
    ),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> OrderResponse:
    """Place a new order — supports market, limit, stop, trailing, and more.

    Every order is UUID-tagged and cached to Redis for auditability.
    The response includes `order_uuid` for client-side tracking.

    Supports idempotent requests via the `Idempotency-Key` header.
    If the same key is reused within 24h, the cached response is returned.
    """
    if idempotency_key is not None:
        request = request.model_copy(update={"idempotency_key": idempotency_key})
    return await state.feed.place_order(request)


@router.post("/{order_id}/cancel", response_model=CancelOrderResponse, dependencies=[Depends(require_orders_enabled)])
async def cancel_order(
    order_id: int = Path(description="IBKR order id (permId-based orderId) to cancel."),
    account_id: str = Query(..., min_length=1, alias="account_id", description="IBKR account id the order lives in."),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> CancelOrderResponse:
    """Cancel an existing order by order ID and account ID.

    Creates a cancel envelope linked to the original order's UUID.
    """
    return await state.feed.cancel_order(account_id, order_id)


@router.post("/{order_id}/modify", response_model=OrderResponse, dependencies=[Depends(require_orders_enabled)])
async def modify_order(
    order_id: int = Path(description="IBKR order id (permId-based orderId) to modify."),
    modifications: ModifyOrderRequest = Body(
        ...,
        description="Fields to change: limited to price, quantity, and time-in-force.",
    ),
    account_id: str = Query(..., min_length=1, alias="account_id", description="IBKR account id the order lives in."),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> OrderResponse:
    """Modify an existing order — limited to price, quantity, and TIF.

    Creates a modify envelope linked to the original order's UUID.
    """
    return await state.feed.modify_order(account_id, order_id, modifications)


@router.get("/open", response_model=list[OpenOrder], summary="Get all open (working) orders")
async def load_open_orders(
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of open orders to return."),
    offset: int = Query(default=0, ge=0, description="Number of open orders to skip for paging."),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> list[OpenOrder]:
    """Get all currently open (working) orders.

    Returns bound open orders for this API client's session. Results are
    paginated with limit/offset; ordering follows IBKR. Use
    GET /orders/open/all for a one-time cross-client snapshot.
    """
    results = await state.feed.load_open_orders()
    return results[offset : offset + limit]


@router.post("/executions", response_model=ExecutionResponse)
async def load_executions(
    request: ExecutionRequest,
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> ExecutionResponse:
    """Get execution/fill details with optional filtering.

    Fetches executions known to this session and mirrors them into the
    analytics archive used by GET /orders/tca. Run it after reconnects so
    TCA sees the fills.
    """
    return await state.feed.load_executions(request)


@router.post("/preview", response_model=WhatIfOrderResponse, dependencies=[Depends(require_orders_enabled)])
async def preview_order(
    request: PlaceOrderRequest,
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> WhatIfOrderResponse:
    """Pre-trade margin & commission preview (what-if) — no order placed."""
    return await state.feed.preview_order(request)


@router.get("/completed", response_model=list[CompletedOrder], summary="Get completed order history")
async def load_completed_orders(
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of completed orders to return."),
    offset: int = Query(default=0, ge=0, description="Number of completed orders to skip for paging."),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> list[CompletedOrder]:
    """Get completed (filled/cancelled) order history.

    Returns completed order reports for the session. Results are paginated
    with limit/offset; IBKR caps how far back reports are available.
    """
    results = await state.feed.load_completed_orders()
    return results[offset : offset + limit]


# ------------------------------------------------------------------
# Order cache (UUID-tagged envelopes in Redis)
# ------------------------------------------------------------------

@router.get("/cache/{order_uuid}", response_model=CachedOrderLookup)
async def get_cached_order(
    order_uuid: str = Path(description="Order UUID returned by place/cancel/modify/preview responses."),
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
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of envelopes to return."),
    offset: int = Query(default=0, ge=0, description="Number of envelopes to skip for paging."),
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


@router.post("/bracket", response_model=BracketOrderResponse, status_code=status.HTTP_201_CREATED, summary="Place a bracket order", dependencies=[Depends(require_orders_enabled)])
async def place_bracket_order(
    payload: Annotated[BracketOrderRequest, Body(openapi_examples=BRACKET_ORDER_EXAMPLES)],  # noqa: F821
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> BracketOrderResponse:
    """Place a bracket order (parent + take-profit + stop-loss).

    Creates three linked orders: a parent entry order, an attached take-profit,
    and an attached stop-loss. When one of the exit orders fills, the other is cancelled.
    """
    return await state.feed.place_bracket_order(payload)


# ------------------------------------------------------------------
# OCA (One-Cancels-All) groups
# ------------------------------------------------------------------

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


@router.post("/oca", response_model=list[OcaOrderResponse], status_code=status.HTTP_201_CREATED, summary="Place an OCA order group", dependencies=[Depends(require_orders_enabled)])
async def place_oca_group(
    payload: Annotated[OcaGroupRequest, Body(openapi_examples=OCA_GROUP_EXAMPLES)],  # noqa: F821
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> list[OcaOrderResponse]:
    """Place a One-Cancels-All group of orders.

    When any order in the group fills, all others are automatically cancelled.
    Requires at least 2 orders.
    """
    return await state.feed.place_oca_group(payload)


@router.get("/open/all", response_model=list[OpenOrder], summary="Snapshot open orders across API clients")
async def load_all_open_orders(
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of open orders to return."),
    offset: int = Query(default=0, ge=0, description="Number of open orders to skip for paging."),
    state: IBKRRestAppState = Depends(require_order_bearer_token),
) -> list[OpenOrder]:
    """One-time snapshot in associated accounts; does not bind orders for modification.

    Fetches all open orders across associated clients and accounts in a
    single request. The returned orders are not bound to this session, so
    cancel/modify may not act on them; use GET /orders/open for bound
    orders. Results are paginated with limit/offset.
    """
    results = await state.feed.get_all_open_orders()
    return results[offset : offset + limit]
