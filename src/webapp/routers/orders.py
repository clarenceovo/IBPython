"""REST router for IBKR order management — place, cancel, modify, executions, preview, cache."""

from __future__ import annotations

import json
import logging
import secrets

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

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
