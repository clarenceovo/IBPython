"""REST router for real-time 5-second bar subscriptions via SSE streaming."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from src.webapp.dependencies import IBKRRestAppState, get_rest_state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/realtime-bars", tags=["streaming"])

# In-memory active subscriptions (single-process only).
_active_realtime_subscriptions: dict[str, dict[str, Any]] = {}


class RealtimeSubscriptionInfo(BaseModel):
    subscription_id: str
    symbol: str
    asset_class: str
    exchange: str
    currency: str
    what_to_show: str
    use_rth: bool
    started_at: str


class RealtimeStartResponse(BaseModel):
    status: str
    subscription_id: str
    symbol: str
    message: str


class RealtimeStatusResponse(BaseModel):
    active_count: int
    subscriptions: list[RealtimeSubscriptionInfo]


class RealtimeStopResponse(BaseModel):
    status: str
    subscription_id: str
    symbol: str


class RealtimeBarsStartRequest(BaseModel):
    """Request body to start a real-time bars subscription."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    asset_class: str = Field(default="EQUITY", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True


@router.post("/start", summary="Start real-time 5-second bar subscription", response_model=RealtimeStartResponse)
async def start_realtime_bars(
    payload: RealtimeBarsStartRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> RealtimeStartResponse:
    """Start a real-time 5-second bar subscription. Data is streamed via SSE."""
    subscription_id = str(uuid.uuid4())[:8]
    key = payload.symbol.upper()

    if key in _active_realtime_subscriptions:
        return RealtimeStartResponse(
            status="already_active",
            subscription_id=_active_realtime_subscriptions[key]["subscription_id"],
            symbol=payload.symbol,
            message=f"Subscription already active for {payload.symbol}",
        )

    _active_realtime_subscriptions[key] = {
        "subscription_id": subscription_id,
        "symbol": payload.symbol,
        "asset_class": payload.asset_class,
        "exchange": payload.exchange,
        "currency": payload.currency,
        "what_to_show": payload.what_to_show,
        "use_rth": payload.use_rth,
        "started_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    return RealtimeStartResponse(
        status="started",
        subscription_id=subscription_id,
        symbol=payload.symbol,
        message=f"Real-time bars subscription started for {payload.symbol}. Stream via SSE.",
    )


@router.get("/status", summary="List active real-time bar subscriptions", response_model=RealtimeStatusResponse)
async def realtime_bars_status(
    state: IBKRRestAppState = Depends(get_rest_state),
) -> RealtimeStatusResponse:
    """Return the list of active real-time bar subscriptions."""
    subs = [
        RealtimeSubscriptionInfo(**v) for v in _active_realtime_subscriptions.values()
    ]
    return RealtimeStatusResponse(
        active_count=len(subs),
        subscriptions=subs,
    )


# NOTE: Returns 200 (not 204) because the response body contains the deleted
# subscription state. REST permits 200 with body for DELETE when the client
# benefits from seeing the removed resource representation.
@router.delete("/stop/{symbol}", summary="Stop real-time bar subscription", response_model=RealtimeStopResponse)
async def stop_realtime_bars(
    symbol: str,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> RealtimeStopResponse:
    """Stop a real-time bar subscription by symbol."""
    key = symbol.upper()
    sub = _active_realtime_subscriptions.pop(key, None)
    if sub is None:
        raise HTTPException(status_code=404, detail=f"No active subscription for {symbol}")
    return RealtimeStopResponse(
        status="stopped",
        subscription_id=sub["subscription_id"],
        symbol=symbol,
    )


@router.get("/stream/{symbol}", summary="SSE stream for real-time 5-second bars")
async def stream_realtime_bars(
    symbol: str,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> StreamingResponse:
    """Stream real-time 5-second bars for a symbol via Server-Sent Events."""

    key = symbol.upper()
    sub = _active_realtime_subscriptions.get(key)
    if sub is None:
        raise HTTPException(status_code=404, detail=f"No active subscription for {symbol}. POST /start first.")

    async def _sse_generator() -> Any:
        try:
            bar_stream = await state.feed.subscribe_realtime_bars(
                symbol=sub["symbol"],
                asset_class=sub["asset_class"],
                exchange=sub["exchange"],
                currency=sub["currency"],
                what_to_show=sub["what_to_show"],
                use_rth=sub["use_rth"],
            )
            async for bar in bar_stream:
                bar["symbol"] = sub["symbol"]
                bar["vwap"] = bar.pop("wap", None)
                bar["trade_count"] = bar.pop("count", None)
                data = json.dumps(bar, default=str)
                yield f"data: {data}\n\n"
        except Exception as exc:
            logger.error("SSE realtime bars error for %s: %s", symbol, exc, exc_info=True)
            error_data = json.dumps({"error": str(exc)})
            yield f"data: {error_data}\n\n"
        finally:
            yield "data: {\"event\": \"stream_end\"}\n\n"

    return StreamingResponse(
        _sse_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
