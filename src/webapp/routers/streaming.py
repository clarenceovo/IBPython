from __future__ import annotations

import asyncio
import json
import logging
import math
import uuid
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from src.feeds.contracts import ContractSpec
from src.feeds.exchange_resolver import resolve_equity
from src.feeds.models import AssetClass
from src.feeds.streaming import StreamRequest, StreamSubscription, StreamingTickerSnapshot
from src.transport.metrics import metrics
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/streaming", tags=["market-data"])

# Track active subscriptions in-memory.
# NOTE: This is single-process state. For multi-worker deployments (e.g. uvicorn --workers),
# use sticky sessions (ip_hash or cookie-based) so that subscription management requests
# always route to the worker that owns the subscription.
_active_subscriptions: dict[str, StreamSubscription] = {}
_sub_lock = asyncio.Lock()
"""Lock for mutations to _active_subscriptions.

In asyncio, a Lock prevents interleaving between the SSE generator's finally block,
the cleanup task, and the API handlers.  Without it, dict mutation during iteration
or a race between stop_subscription and the generator's cleanup could cause issues.
"""

# TTL-based auto-cleanup: subscriptions older than this are considered stale.
# Configurable via environment or direct patch; defaults to 1 hour.
MAX_SUBSCRIPTION_LIFETIME_SECONDS: float = 3600.0

# Interval for the background cleanup task.
_CLEANUP_INTERVAL_SECONDS: float = 60.0

# Reference to the background cleanup task (single-process only).
_cleanup_task: asyncio.Task[None] | None = None


def _pop_subscription_locked(subscription_id: str) -> StreamSubscription | None:
    """Remove a subscription once and decrement active telemetry once."""

    sub = _active_subscriptions.pop(subscription_id, None)
    if sub is not None:
        metrics.streaming_subscriptions_active.dec()
    return sub


def _is_subscription_expired(sub: StreamSubscription) -> bool:
    """Check if a subscription has exceeded its TTL."""
    age = (datetime.now(timezone.utc) - sub.connected_at).total_seconds()
    return age > MAX_SUBSCRIPTION_LIFETIME_SECONDS


async def _cleanup_orphaned_subscriptions() -> None:
    """Background task that periodically stops and removes stale subscriptions."""
    while True:
        await asyncio.sleep(_CLEANUP_INTERVAL_SECONDS)
        try:
            async with _sub_lock:
                expired_ids = [
                    sid
                    for sid, sub in _active_subscriptions.items()
                    if _is_subscription_expired(sub)
                ]
                for sid in expired_ids:
                    sub = _active_subscriptions.get(sid)
                    if sub is not None:
                        logger.warning(
                            "Cleaning up expired subscription: subscription_id=%s age=%.0fs",
                            sid,
                            (datetime.now(timezone.utc) - sub.connected_at).total_seconds(),
                        )
                        sub.stop()
                        _pop_subscription_locked(sid)
        except Exception:
            logger.exception("Error during subscription cleanup")


def start_cleanup_task() -> None:
    """Start the background cleanup task if not already running."""
    global _cleanup_task
    if _cleanup_task is None or _cleanup_task.done():
        _cleanup_task = asyncio.create_task(_cleanup_orphaned_subscriptions())
        _cleanup_task.set_name("streaming-subscription-cleanup")


def stop_cleanup_task() -> None:
    """Cancel the background cleanup task."""
    global _cleanup_task
    if _cleanup_task is not None and not _cleanup_task.done():
        _cleanup_task.cancel()
        _cleanup_task = None


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
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _ticker_to_snapshot(ticker: Any, spec: ContractSpec) -> StreamingTickerSnapshot:
    """Convert an ib_insync Ticker to a StreamingTickerSnapshot."""
    # Prefer the ticker's own timestamp; fall back to wall clock with a warning.
    ticker_time = getattr(ticker, "time", None)
    if ticker_time is not None:
        ts = ticker_time if isinstance(ticker_time, datetime) else datetime.fromtimestamp(ticker_time, tz=timezone.utc)
    else:
        logger.warning("ticker.time is None for %s streaming — falling back to datetime.now(UTC)", spec.symbol)
        ts = datetime.now(timezone.utc)
    return StreamingTickerSnapshot(
        symbol=spec.symbol,
        asset_class=spec.asset_class,
        exchange=spec.exchange,
        currency=spec.currency,
        timestamp=ts,
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

    stop_event = asyncio.Event()

    subscription = StreamSubscription(
        subscription_id=subscription_id,
        symbol=spec.symbol,
        asset_class=spec.asset_class,
        exchange=spec.exchange,
        currency=spec.currency,
        connected_at=datetime.now(timezone.utc),
    )
    subscription._set_stop_event(stop_event)
    async with _sub_lock:
        _active_subscriptions[subscription_id] = subscription
    metrics.streaming_subscriptions_active.inc()

    async def event_generator():
        ticker = None
        updates_sent = 0
        queue: asyncio.Queue[StreamingTickerSnapshot] = asyncio.Queue(maxsize=100)
        try:
            ticker = await state.feed.subscribe_ticker(spec)

            async def _ticker_to_queue() -> None:
                """Producer: push ticker snapshots to the bounded queue."""
                while not stop_event.is_set():
                    snapshot = _ticker_to_snapshot(ticker, spec)
                    try:
                        queue.put_nowait(snapshot)
                    except asyncio.QueueFull:
                        # Drop oldest item to make room
                        try:
                            queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                        try:
                            queue.put_nowait(snapshot)
                        except asyncio.QueueFull:
                            pass
                        subscription.dropped_updates += 1
                        logger.warning(
                            "SSE queue full, dropped oldest update: subscription_id=%s dropped=%d",
                            subscription_id,
                            subscription.dropped_updates,
                        )
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=request.update_interval_seconds)
                        return  # stop_event was set
                    except TimeoutError:
                        pass  # normal tick interval elapsed

            producer_task = asyncio.create_task(_ticker_to_queue())
            try:
                while not stop_event.is_set():
                    if request.max_updates > 0 and updates_sent >= request.max_updates:
                        yield f'event: done\ndata: {{"subscription_id": "{subscription_id}", "reason": "max_updates_reached"}}\n\n'
                        break

                    try:
                        snapshot = await asyncio.wait_for(
                            queue.get(),
                            timeout=request.update_interval_seconds * 5,
                        )
                    except TimeoutError:
                        # No data for a while — send keepalive
                        yield ": keepalive\n\n"
                        continue

                    data = snapshot.model_dump_json()
                    yield f"data: {data}\n\n"
                    updates_sent += 1
                    subscription.updates_sent = updates_sent
            finally:
                producer_task.cancel()
                try:
                    await producer_task
                except asyncio.CancelledError:
                    pass
        except asyncio.CancelledError:
            logger.info("SSE stream cancelled: subscription_id=%s updates_sent=%d", subscription_id, updates_sent)
        except Exception:
            logger.exception("SSE stream error: subscription_id=%s", subscription_id)
            error_data = json.dumps({"subscription_id": subscription_id, "error": "stream_interrupted"})
            yield f"event: error\ndata: {error_data}\n\n"
        finally:
            if ticker is not None:
                await state.feed.unsubscribe_ticker(ticker)
            async with _sub_lock:
                _pop_subscription_locked(subscription_id)
            logger.info("SSE stream closed: subscription_id=%s updates_sent=%d dropped=%d", subscription_id, updates_sent, subscription.dropped_updates)

    # Start the background cleanup task on first subscription
    start_cleanup_task()

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
async def list_active_subscriptions(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> list[StreamSubscription]:
    async with _sub_lock:
        # Clean up expired subscriptions on read
        expired_ids = [
            sid for sid, sub in _active_subscriptions.items()
            if _is_subscription_expired(sub)
        ]
        for sid in expired_ids:
            sub = _pop_subscription_locked(sid)
            if sub is not None:
                sub.stop()
        all_subs = list(_active_subscriptions.values())
        return all_subs[offset : offset + limit]


@router.delete(
    "/subscriptions/{subscription_id}",
    summary="Stop a streaming subscription",
    # NOTE: Returns 200 (not 204) because the response body contains the deleted
    # subscription state. REST permits 200 with body for DELETE when the client
    # benefits from seeing the removed resource representation.
)
async def stop_subscription(subscription_id: str) -> dict[str, str]:
    async with _sub_lock:
        sub = _active_subscriptions.get(subscription_id)
        if sub is None:
            raise HTTPException(status_code=404, detail=f"Subscription {subscription_id} not found")
        # Signal the SSE generator to stop, then clean up
        sub.stop()
        _pop_subscription_locked(subscription_id)
    return {"status": "stopped", "subscription_id": subscription_id}
