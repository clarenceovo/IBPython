from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.account import (
    AccountPnLDTO,
    AccountSummaryDTO,
    LivePositionDTO,
    PortfolioItemDTO,
    PositionPnLDTO,
    normalize_account_pnl,
    normalize_position_pnl,
)
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/account", tags=["account"])


class AccountPnLSnapshotRequest(BaseModel):
    """Request body for a single-account PnL snapshot."""

    model_config = ConfigDict(extra="forbid")

    account: str = Field(min_length=1, description="IBKR account id, e.g. DU1234567.")
    model_code: str = Field(default="", description="IBKR PnL model code; empty string selects the base model.")
    wait_seconds: float = Field(default=1.2, ge=0, description="Seconds to wait for the first PnL update before returning nulls.")


class PositionPnLSnapshotRequest(AccountPnLSnapshotRequest):
    """Request body for a single-position PnL snapshot."""

    con_id: int = Field(gt=0, description="IBKR contract id of the position to query.")


@router.get("/summary", response_model=list[AccountSummaryDTO], summary="Get account summary")
async def load_account_summary(
    account: str = Query(default="", description="IBKR account id filter; empty string returns all accounts."),
    limit: int = Query(default=50, ge=1, le=500, description="Maximum number of account groups to return (applied after offset)."),
    offset: int = Query(default=0, ge=0, description="Number of account groups to skip for pagination."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[AccountSummaryDTO]:
    """Return IBKR account summary values grouped by account.

    Use this endpoint for NetLiquidation, TotalCashValue, AvailableFunds,
    ExcessLiquidity, Cushion, and GrossPositionValue per account. Each group
    carries a ``values`` map keyed by IBKR tag. Values are strings as IBKR
    reports them. Results are paginated with limit and offset; the response
    does not include a total count.
    """
    results = await state.feed.load_account_summary(account)
    return results[offset : offset + limit]


@router.get("/positions", response_model=list[LivePositionDTO], summary="Get live positions")
async def load_live_positions(
    limit: int = Query(default=50, ge=1, le=500, description="Maximum number of positions to return (applied after offset)."),
    offset: int = Query(default=0, ge=0, description="Number of positions to skip for pagination."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[LivePositionDTO]:
    """Return the current live positions across all accounts.

    Each position carries account, symbol, security type, signed quantity,
    and average cost. Option positions include strike (float), expiry
    (YYYYMMDD or YYYYMM), right (C or P), multiplier, and underlying symbol.
    Results are paginated with limit and offset; the response does not
    include a total count.
    """
    results = await state.feed.load_live_positions()
    return results[offset : offset + limit]


@router.get("/portfolio", response_model=list[PortfolioItemDTO], summary="Get portfolio items")
async def load_portfolio_items(
    account: str = Query(default="", description="IBKR account id filter; empty string returns all accounts. The sentinel ALL is normalized to all accounts."),
    limit: int = Query(default=50, ge=1, le=500, description="Maximum number of portfolio items to return (applied after offset)."),
    offset: int = Query(default=0, ge=0, description="Number of portfolio items to skip for pagination."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[PortfolioItemDTO]:
    """Return portfolio items from the account's local portfolio cache.

    Each item carries position, mark price, market value, average cost, and
    unrealized/realized PnL. Mark fields are null when not yet reported.
    Results are paginated with limit and offset; the response does not
    include a total count.
    """
    results = await state.feed.load_portfolio_items(account)
    return results[offset : offset + limit]


@router.post("/pnl/account", response_model=AccountPnLDTO)
async def load_account_pnl_snapshot(
    request: AccountPnLSnapshotRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> AccountPnLDTO:
    """Return a one-shot daily/unrealized/realized PnL snapshot for one account.

    Starts a PnL subscription, waits up to wait_seconds for the first update,
    and releases the subscription. PnL fields are null when IBKR reports no
    value within the wait budget.
    """
    return await state.feed.load_account_pnl_snapshot(
        account=request.account,
        model_code=request.model_code,
        wait_seconds=request.wait_seconds,
    )


@router.post("/pnl/position", response_model=PositionPnLDTO)
async def load_position_pnl_snapshot(
    request: PositionPnLSnapshotRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> PositionPnLDTO:
    """Return a one-shot PnL snapshot for one position of one account.

    Identify the position by con_id. Starts a single-position PnL
    subscription, waits up to wait_seconds for the first update, and
    releases the subscription. PnL fields are null when IBKR reports no
    value within the wait budget.
    """
    return await state.feed.load_position_pnl_snapshot(
        account=request.account,
        con_id=request.con_id,
        model_code=request.model_code,
        wait_seconds=request.wait_seconds,
    )


# ---------------------------------------------------------------------------
# Live PnL SSE streams
# ---------------------------------------------------------------------------


def _feed_connection(state: IBKRRestAppState) -> Any:
    """Reach the feed's connection manager without requiring it to exist."""
    return getattr(state.feed, "_connection", None)


def _require_feed_connected(state: IBKRRestAppState) -> None:
    """Reject the request up front when the feed is known to be disconnected.

    The check must run before the StreamingResponse starts: once SSE headers
    are sent, a connection failure could no longer surface as a 503 status.
    """
    if getattr(_feed_connection(state), "is_connected", None) is False:
        raise HTTPException(status_code=503, detail="IBKR feed is not connected")


def _cancel_pnl_subscription(connection: Any, cancel_method: str, *args: Any) -> None:
    """Release a live PnL subscription through the ib_async cancel API."""
    ib = getattr(connection, "ib", None)
    cancel = getattr(ib, cancel_method, None)
    if not callable(cancel):
        return
    try:
        cancel(*args)
    except Exception:
        logger.debug("PnL SSE subscription cleanup failed", exc_info=True)


async def _pnl_event_stream(
    subscribe: Callable[[], Awaitable[Any]],
    snapshot: Callable[[Any], tuple[dict[str, Any], str]],
    release: Callable[[], None],
    interval: float,
    max_updates: int,
) -> AsyncIterator[str]:
    """Yield SSE data events for one live PnL subscription.

    ib_async PnL objects are plain dataclasses mutated in place by the
    wrapper, so the stream polls the subscription and emits a DTO snapshot
    whenever the payload changes (ignoring the volatile timestamp field).
    The subscription is always released in the finally block, which runs on
    client disconnect, natural end, or error.
    """
    subscription: Any = None
    updates_sent = 0
    try:
        subscription = await subscribe()
        last_signature: dict[str, Any] | None = None
        while True:
            payload, data = snapshot(subscription)
            signature = {key: value for key, value in payload.items() if key != "timestamp"}
            if last_signature is None or signature != last_signature:
                last_signature = signature
                updates_sent += 1
                yield f"data: {data}\n\n"
                if 0 < max_updates <= updates_sent:
                    return
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("PnL SSE stream cancelled by client disconnect (updates_sent=%d)", updates_sent)
    except Exception:
        logger.exception("PnL SSE stream failed (updates_sent=%d)", updates_sent)
        error_data = json.dumps({"error": "pnl_stream_interrupted"})
        yield f"event: error\ndata: {error_data}\n\n"
    finally:
        if subscription is not None:
            release()


_SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


@router.get(
    "/pnl/account/stream",
    summary="Stream live account PnL updates via Server-Sent Events",
    description=(
        "Starts a live account PnL subscription and streams AccountPnLDTO snapshots "
        "as SSE data events. The stream ends when the client disconnects or after "
        "max_updates events (0 = unlimited). The subscription is released on disconnect."
    ),
)
async def stream_account_pnl(
    account_id: str = Query(min_length=1, description="IBKR account id, e.g. DU1234567"),
    model_code: str = Query(default="", description="IBKR PnL model code; empty string selects the base model."),
    interval: float = Query(default=1.0, ge=0.1, le=60.0, description="Polling interval in seconds"),
    max_updates: int = Query(default=0, ge=0, description="Stop after N updates. 0 = stream until disconnect."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> StreamingResponse:
    """Stream live account PnL as SSE until the client disconnects."""
    _require_feed_connected(state)
    connection = _feed_connection(state)

    async def subscribe() -> Any:
        return await state.feed.subscribe_account_pnl(account_id, model_code)

    def snapshot(subscription: Any) -> tuple[dict[str, Any], str]:
        dto = normalize_account_pnl(subscription, account_id, model_code)
        return dto.model_dump(mode="json"), dto.model_dump_json()

    def release() -> None:
        _cancel_pnl_subscription(connection, "cancelPnL", account_id, model_code)

    return StreamingResponse(
        _pnl_event_stream(subscribe, snapshot, release, interval, max_updates),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )


@router.get(
    "/pnl/position/stream",
    summary="Stream live single-position PnL updates via Server-Sent Events",
    description=(
        "Starts a live PnL subscription for one position (identified by con_id) and "
        "streams PositionPnLDTO snapshots as SSE data events. The stream ends when the "
        "client disconnects or after max_updates events (0 = unlimited). "
        "The subscription is released on disconnect."
    ),
)
async def stream_position_pnl(
    account_id: str = Query(min_length=1, description="IBKR account id, e.g. DU1234567"),
    con_id: int = Query(gt=0, description="IBKR contract id of the position"),
    model_code: str = Query(default="", description="IBKR PnL model code; empty string selects the base model."),
    interval: float = Query(default=1.0, ge=0.1, le=60.0, description="Polling interval in seconds"),
    max_updates: int = Query(default=0, ge=0, description="Stop after N updates. 0 = stream until disconnect."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> StreamingResponse:
    """Stream live single-position PnL as SSE until the client disconnects."""
    _require_feed_connected(state)
    connection = _feed_connection(state)

    async def subscribe() -> Any:
        return await state.feed.subscribe_position_pnl(account_id, con_id, model_code)

    def snapshot(subscription: Any) -> tuple[dict[str, Any], str]:
        dto = normalize_position_pnl(subscription, account_id, con_id, model_code)
        return dto.model_dump(mode="json"), dto.model_dump_json()

    def release() -> None:
        _cancel_pnl_subscription(connection, "cancelPnLSingle", account_id, model_code, con_id)

    return StreamingResponse(
        _pnl_event_stream(subscribe, snapshot, release, interval, max_updates),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )
