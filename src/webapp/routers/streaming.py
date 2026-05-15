from __future__ import annotations

import asyncio
import json
import logging
import math
import uuid
from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException
from fastapi.responses import StreamingResponse

from src.feeds.contracts import ContractSpec
from src.feeds.exchange_resolver import resolve_equity
from src.feeds.models import AssetClass
from src.feeds.streaming import StreamRequest, StreamSubscription, StreamingTickerSnapshot
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/streaming", tags=["market-data"])

# Track active subscriptions in-memory
_active_subscriptions: dict[str, StreamSubscription] = {}


STREAM_REQUEST_EXAMPLES = {
    "spy_equity": {
        "summary": "Stream SPY real-time ticks",
        "description": "Streams real-time bid/ask/last for SPY via Server-Sent Events.",
        "value": {
            "symbol": "SPY",
            "asset_class": "equity",
            "update_interval_seconds": 0.5,
        },
    },
    "eurusd_fx": {
        "summary": "Stream EURUSD forex",
        "description": "Streams midpoint data for EURUSD.",
        "value": {
            "symbol": "EURUSD",
            "asset_class": "fx",
            "exchange": "IDEALPRO",
            "currency": "USD",
            "update_interval_seconds": 1.0,
        },
    },
    "aapl_nasdaq": {
        "summary": "Stream AAPL with primary exchange",
        "value": {
            "symbol": "AAPL",
            "asset_class": "equity",
            "primary_exchange": "NASDAQ",
            "update_interval_seconds": 0.5,
        },
    },
}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _ticker_to_snapshot(ticker: Any, spec: ContractSpec) -> StreamingTickerSnapshot:
    """Convert an ib_insync Ticker to a StreamingTickerSnapshot."""
    return StreamingTickerSnapshot(
        symbol=spec.symbol,
        asset_class=spec.asset_class,
        exchange=spec.exchange,
        currency=spec.currency,
        timestamp=datetime.now(timezone.utc),
        last=_safe_float(getattr(ticker, "last", None)),
        bid=_safe_float(getattr(ticker, "bid", None)),
        ask=_safe_float(getattr(ticker, "ask", None)),
        bid_size=_safe_float(getattr(ticker, "bidSize", None)),
        ask_size=_safe_float(getattr(ticker, "askSize", None)),
        volume=_safe_float(getattr(ticker, "volume", None)),
        high=_safe_float(getattr(ticker, "high", None)),
        low=_safe_float(getattr(ticker, "low", None)),
        close=_safe_float(getattr(ticker, "close", None)),
        open=_safe_float(getattr(ticker, "open_", None)),
        vwap=_safe_float(getattr(ticker, "vwap", None)),
        implied_volatility=_safe_float(getattr(ticker, "impliedVolatility", None)),
        mark_price=_safe_float(getattr(ticker, "markPrice", None)),
        halted=getattr(ticker, "halted", None),
    )


def _resolve_stream_contract(request: StreamRequest) -> ContractSpec:
    """Resolve the stream request to a ContractSpec."""
    if request.asset_class == AssetClass.EQUITY and not request.primary_exchange:
        resolved = resolve_equity(request.symbol)
        return ContractSpec(
            symbol=resolved.symbol,
            asset_class=request.asset_class,
            exchange=request.exchange,
            currency=request.currency,
            primary_exchange=resolved.primary_exchange or None,
        )
    return ContractSpec(
        symbol=request.symbol,
        asset_class=request.asset_class,
        exchange=request.exchange,
        currency=request.currency,
        primary_exchange=request.primary_exchange,
    )


@router.post(
    "/ticker",
    summary="Stream real-time market data via Server-Sent Events",
    description=(
        "Opens an SSE connection that streams real-time ticker updates. "
        "The stream stays open until the client disconnects or max_updates is reached. "
        "Each SSE event contains a JSON-encoded StreamingTickerSnapshot."
    ),
)
async def stream_ticker(
    payload: Annotated[StreamRequest, Body(openapi_examples=STREAM_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> StreamingResponse:
    request = payload
    spec = _resolve_stream_contract(request)
    subscription_id = uuid.uuid4().hex[:12]

    subscription = StreamSubscription(
        subscription_id=subscription_id,
        symbol=spec.symbol,
        asset_class=spec.asset_class,
        exchange=spec.exchange,
        currency=spec.currency,
        connected_at=datetime.now(timezone.utc),
    )
    _active_subscriptions[subscription_id] = subscription

    async def event_generator():
        ticker = None
        updates_sent = 0
        try:
            ticker = await state.feed.subscribe_ticker(spec)
            while True:
                if request.max_updates > 0 and updates_sent >= request.max_updates:
                    yield f'event: done\ndata: {{"subscription_id": "{subscription_id}", "reason": "max_updates_reached"}}\n\n'
                    break

                snapshot = _ticker_to_snapshot(ticker, spec)
                data = snapshot.model_dump_json()
                yield f"data: {data}\n\n"
                updates_sent += 1
                subscription.updates_sent = updates_sent

                await asyncio.sleep(request.update_interval_seconds)
        except asyncio.CancelledError:
            logger.info("SSE stream cancelled: subscription_id=%s updates_sent=%d", subscription_id, updates_sent)
        except Exception:
            logger.exception("SSE stream error: subscription_id=%s", subscription_id)
            error_data = json.dumps({"subscription_id": subscription_id, "error": "stream_interrupted"})
            yield f"event: error\ndata: {error_data}\n\n"
        finally:
            if ticker is not None:
                await state.feed.unsubscribe_ticker(ticker)
            _active_subscriptions.pop(subscription_id, None)
            logger.info("SSE stream closed: subscription_id=%s updates_sent=%d", subscription_id, updates_sent)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "X-Subscription-Id": subscription_id,
        },
    )


@router.get(
    "/subscriptions",
    response_model=list[StreamSubscription],
    summary="List active streaming subscriptions",
)
async def list_active_subscriptions() -> list[StreamSubscription]:
    return list(_active_subscriptions.values())


@router.delete(
    "/subscriptions/{subscription_id}",
    summary="Stop a streaming subscription",
)
async def stop_subscription(subscription_id: str) -> dict[str, str]:
    sub = _active_subscriptions.get(subscription_id)
    if sub is None:
        raise HTTPException(status_code=404, detail=f"Subscription {subscription_id} not found")
    # The subscription will be cleaned up by the event generator's finally block
    _active_subscriptions.pop(subscription_id, None)
    return {"status": "stopped", "subscription_id": subscription_id}
