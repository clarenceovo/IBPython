"""REST router for IBKR order management — place, cancel, modify, executions, preview."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from src.feeds.orders import (
    CancelOrderResponse,
    CompletedOrder,
    ExecutionRequest,
    ExecutionResponse,
    ModifyOrderRequest,
    OpenOrder,
    OrderResponse,
    PlaceOrderRequest,
    WhatIfOrderResponse,
)
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("/place", response_model=OrderResponse)
async def place_order(
    request: PlaceOrderRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OrderResponse:
    """Place a new order — supports market, limit, stop, trailing, and more."""
    return await state.feed.place_order(request)


@router.post("/{order_id}/cancel", response_model=CancelOrderResponse)
async def cancel_order(
    order_id: int,
    account_id: str = Query(default="", alias="account_id"),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> CancelOrderResponse:
    """Cancel an existing order by order ID and account ID."""
    return await state.feed.cancel_order(account_id, order_id)


@router.post("/{order_id}/modify", response_model=OrderResponse)
async def modify_order(
    order_id: int,
    modifications: ModifyOrderRequest,
    account_id: str = Query(default="", alias="account_id"),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OrderResponse:
    """Modify an existing order — price, quantity, type, TIF, trailing params."""
    return await state.feed.modify_order(account_id, order_id, modifications)


@router.get("/open", response_model=list[OpenOrder])
async def load_open_orders(
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OpenOrder]:
    """Get all currently open (working) orders."""
    return await state.feed.load_open_orders()


@router.post("/executions", response_model=ExecutionResponse)
async def load_executions(
    request: ExecutionRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> ExecutionResponse:
    """Get execution/fill details with optional filtering."""
    return await state.feed.load_executions(request)


@router.post("/preview", response_model=WhatIfOrderResponse)
async def preview_order(
    request: PlaceOrderRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> WhatIfOrderResponse:
    """Pre-trade margin & commission preview (what-if) — no order placed."""
    return await state.feed.preview_order(request)


@router.get("/completed", response_model=list[CompletedOrder])
async def load_completed_orders(
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[CompletedOrder]:
    """Get completed (filled/cancelled) order history."""
    return await state.feed.load_completed_orders()
