"""Realtime WebSocket feed channel: subscribe/unsubscribe JSON protocol over one socket.

Protocol (JSON text frames, endpoint ``/api/v1/ws/market-data``)
---------------------------------------------------------------

Client -> server frames:

- ``{"action": "subscribe", "channel": "quotes"|"ticks"|"bars", "symbol": "<any notation>",
   "asset_class": "equity|fx|future|crypto|index|option|bond" (optional),
   "exchange": "..." (optional), "currency": "..." (optional),
   "interval_seconds": 0.5 (optional, quotes only),
   "tick_type": "ALL_LAST|LAST|BID_ASK|MIDPOINT" (optional, ticks only),
   "what_to_show": "TRADES" (optional, bars only),
   "max_updates": 0 (optional, 0 = unlimited, auto-unsubscribe when reached)}``
- ``{"action": "unsubscribe", "channel": ..., "symbol": ...}``
- ``{"action": "status"}``
- ``{"action": "ping"}``

Server -> client frames:

- ``{"type": "subscribed", "channel", "symbol", "resolved": <describe() dict>}``
- ``{"type": "unsubscribed", "channel", "symbol"}``
- ``{"type": "error", "action", "detail"}`` for malformed frames, unknown actions,
  unknown channels, symbol resolution failures, duplicate keys, and limits.
- ``{"type": "quotes", "symbol", "data": <snapshot dict>, "server_ts"}``
- ``{"type": "ticks", "symbol", "data": <tick dict>, "server_ts"}``
- ``{"type": "bars", "symbol", "data": <bar dict>, "server_ts"}``
- ``{"type": "status", "subscriptions": [{"channel", "symbol", ...}]}``
- ``{"type": "pong"}``

Example session::

    > {"action": "subscribe", "channel": "ticks", "symbol": "ESZ26"}
    < {"type": "subscribed", "channel": "ticks", "symbol": "ESZ26", "resolved": {...}}
    < {"type": "ticks", "symbol": "ESZ26", "data": {...}, "server_ts": "2026-11-11T18:00:00+00:00"}
    > {"action": "ping"}
    < {"type": "pong"}
    > {"action": "unsubscribe", "channel": "ticks", "symbol": "ESZ26"}
    < {"type": "unsubscribed", "channel": "ticks", "symbol": "ESZ26"}

Architecture: one connection runs one reader loop (receive text frame, parse,
dispatch) and one writer task that drains a bounded outbound queue. Each
subscription owns its producer (a polling task for quotes, a callback
registration for ticks, a consuming task for bars). Disconnect or fatal
receive error cancels every producer and releases every feed subscription.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from src.feeds.models import AssetClass
from src.feeds.streaming import StreamRequest
from src.feeds.symbol_resolver import ResolvedSymbol, SymbolResolutionError, resolve_symbol
from src.feeds.tick_data import TickByTickData, TickType
from src.webapp.routers.market_data_consolidated import SEC_TYPE_BY_ASSET_CLASS
from src.webapp.routers.streaming import _resolve_stream_contract, _ticker_to_snapshot

logger = logging.getLogger(__name__)

router = APIRouter(tags=["market-data"])

VALID_CHANNELS: frozenset[str] = frozenset({"quotes", "ticks", "bars"})
MAX_SUBSCRIPTIONS_PER_CONNECTION = 32
OUTBOX_MAXSIZE = 1000
DEFAULT_QUOTE_INTERVAL_SECONDS = 0.5
DEFAULT_TICK_TYPE_NAME = "ALL_LAST"
DEFAULT_WHAT_TO_SHOW = "TRADES"

QueuePut = Callable[[dict[str, Any]], None]
Cleanup = Callable[[], Awaitable[None]]


def _now_iso() -> str:
    """Current UTC time as ISO-8601 for outbound server_ts fields."""
    return datetime.now(timezone.utc).isoformat()


def _parse_asset_class(raw: Any) -> AssetClass | None:
    """Convert an optional client string to an AssetClass override."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return None
    if isinstance(raw, AssetClass):
        return raw
    return AssetClass(str(raw).strip().lower())


def _parse_optional_str(raw: Any) -> str | None:
    """Normalize an optional passthrough string (exchange, currency)."""
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _parse_tick_type(raw: Any) -> TickType:
    """Accept an enum name (ALL_LAST) or an IBKR value (AllLast)."""
    if isinstance(raw, TickType):
        return raw
    text = str(raw).strip()
    try:
        return TickType[text.upper()]
    except KeyError:
        return TickType(text)


@dataclass
class _Subscription:
    """One active per-connection feed subscription."""

    channel: str
    symbol: str
    canonical_symbol: str
    asset_class: AssetClass
    exchange: str
    currency: str
    max_updates: int = 0
    updates_sent: int = 0
    stopping: bool = False
    params: dict[str, Any] = field(default_factory=dict)
    task: asyncio.Task[None] | None = None
    cleanup: Cleanup | None = None
    tick_key: tuple[str, str, str] | None = None

    def status_frame(self) -> dict[str, Any]:
        info: dict[str, Any] = {
            "channel": self.channel,
            "symbol": self.symbol,
            "canonical_symbol": self.canonical_symbol,
            "asset_class": self.asset_class.value,
            "exchange": self.exchange,
            "currency": self.currency,
            "updates_sent": self.updates_sent,
        }
        info.update(self.params or {})
        return info


async def _start_quotes_subscription(
    state: Any,
    resolved: ResolvedSymbol,
    interval_seconds: float,
    enqueue: QueuePut,
) -> Cleanup:
    """Start the reqMktData-backed quote poller for one quotes subscription.

    Mirrors the SSE router: resolve the ticker contract the way
    ``streaming.stream_ticker`` does (via ``_resolve_stream_contract`` and
    ``state.feed.subscribe_ticker``), then poll the ticker object into
    snapshots until the returned cleanup coroutine runs.
    """
    request = StreamRequest(
        symbol=resolved.canonical_symbol,
        asset_class=resolved.asset_class,
        exchange=resolved.exchange,
        currency=resolved.currency,
        primary_exchange=resolved.primary_exchange or None,
    )
    try:
        spec = _resolve_stream_contract(request)
    except ValueError:
        # Asset classes with extra required contract fields (futures, options)
        # cannot ride the plain StreamRequest path; use the full resolver spec.
        spec = resolved.to_contract_spec()
    ticker = await state.feed.subscribe_ticker(spec)

    async def _poll() -> None:
        while True:
            snapshot = _ticker_to_snapshot(ticker, spec)
            enqueue(snapshot.model_dump(mode="json"))
            await asyncio.sleep(interval_seconds)

    task = asyncio.create_task(_poll())
    task.set_name(f"ws-quotes-{resolved.canonical_symbol}")

    async def _cleanup() -> None:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        try:
            await state.feed.unsubscribe_ticker(ticker)
        except Exception:
            logger.warning("ws quotes: unsubscribe_ticker failed for %s", resolved.canonical_symbol, exc_info=True)

    return _cleanup


class _Connection:
    """State for one WebSocket connection: outbox, subscriptions, reader loop."""

    def __init__(self, ws: WebSocket, state: Any) -> None:
        self.ws = ws
        self.state = state
        self.outbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=OUTBOX_MAXSIZE)
        self.subscriptions: dict[tuple[str, str], _Subscription] = {}
        self.dropped_frames = 0

    # ------------------------------------------------------------------
    # Outbound queue
    # ------------------------------------------------------------------

    def enqueue(self, message: dict[str, Any]) -> None:
        """Put one frame on the outbound queue; drop the oldest when full."""
        try:
            self.outbox.put_nowait(message)
            return
        except asyncio.QueueFull:
            pass
        try:
            self.outbox.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            self.outbox.put_nowait(message)
        except asyncio.QueueFull:
            pass
        self.dropped_frames += 1
        logger.warning("ws outbox full; dropped oldest frame (dropped=%d)", self.dropped_frames)

    def send_error(self, action: Any, detail: str) -> None:
        self.enqueue({"type": "error", "action": action, "detail": detail})

    def publish(self, sub: _Subscription, data: Any) -> None:
        """Enqueue one data frame and honor the max_updates limit."""
        self.enqueue({"type": sub.channel, "symbol": sub.symbol, "data": data, "server_ts": _now_iso()})
        sub.updates_sent += 1
        if sub.max_updates > 0 and sub.updates_sent >= sub.max_updates and not sub.stopping:
            sub.stopping = True
            asyncio.create_task(self._auto_stop(sub))

    async def _auto_stop(self, sub: _Subscription) -> None:
        key = (sub.channel, sub.canonical_symbol.upper())
        await self.teardown(key, notify=True)

    # ------------------------------------------------------------------
    # Subscription lifecycle
    # ------------------------------------------------------------------

    async def teardown(self, key: tuple[str, str], *, notify: bool) -> _Subscription | None:
        """Stop one subscription: cancel its task, run cleanup, stop ticks."""
        sub = self.subscriptions.pop(key, None)
        if sub is None:
            return None
        sub.stopping = True
        if sub.task is not None and not sub.task.done():
            sub.task.cancel()
            await asyncio.gather(sub.task, return_exceptions=True)
        if sub.cleanup is not None:
            try:
                await sub.cleanup()
            except Exception:
                logger.warning("ws cleanup failed: channel=%s symbol=%s", sub.channel, sub.symbol, exc_info=True)
        if sub.tick_key is not None:
            try:
                await self.state.feed.stop_tick_by_tick(*sub.tick_key)
            except Exception:
                logger.warning("ws stop_tick_by_tick failed: channel=%s symbol=%s", sub.channel, sub.symbol, exc_info=True)
        if notify:
            self.enqueue({"type": "unsubscribed", "channel": sub.channel, "symbol": sub.symbol})
        logger.info("ws subscription stopped: channel=%s symbol=%s updates=%d", sub.channel, sub.symbol, sub.updates_sent)
        return sub

    def _find_subscription(self, channel: str, symbol: str) -> tuple[tuple[str, str], _Subscription] | None:
        """Locate a subscription by canonical symbol or by client notation."""
        wanted = symbol.strip().upper()
        exact = self.subscriptions.get((channel, wanted))
        if exact is not None:
            return (channel, wanted), exact
        for key, sub in self.subscriptions.items():
            if sub.channel == channel and sub.symbol.upper() == wanted:
                return key, sub
        return None

    # ------------------------------------------------------------------
    # Frame dispatch
    # ------------------------------------------------------------------

    async def handle_frame(self, frame: Any) -> None:
        if not isinstance(frame, dict):
            self.send_error(None, "frame must be a JSON object")
            return
        action = frame.get("action")
        if action == "ping":
            self.enqueue({"type": "pong"})
        elif action == "status":
            self.enqueue({"type": "status", "subscriptions": [sub.status_frame() for sub in self.subscriptions.values()]})
        elif action == "subscribe":
            await self._handle_subscribe(frame)
        elif action == "unsubscribe":
            await self._handle_unsubscribe(frame)
        else:
            self.send_error(action, f"unknown action: {action!r}")

    async def _handle_subscribe(self, frame: dict[str, Any]) -> None:
        channel = frame.get("channel")
        if channel not in VALID_CHANNELS:
            self.send_error("subscribe", f"unknown channel: {channel!r}")
            return
        symbol = frame.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            self.send_error("subscribe", "symbol must be a non-empty string")
            return
        if len(self.subscriptions) >= MAX_SUBSCRIPTIONS_PER_CONNECTION:
            self.send_error("subscribe", f"subscription limit reached ({MAX_SUBSCRIPTIONS_PER_CONNECTION} per connection)")
            return
        try:
            asset_class = _parse_asset_class(frame.get("asset_class"))
        except ValueError as exc:
            self.send_error("subscribe", f"invalid asset_class: {exc}")
            return
        try:
            resolved = resolve_symbol(
                symbol,
                asset_class=asset_class,
                exchange=_parse_optional_str(frame.get("exchange")),
                currency=_parse_optional_str(frame.get("currency")),
            )
        except SymbolResolutionError as exc:
            self.send_error("subscribe", str(exc))
            return
        key = (channel, resolved.canonical_symbol.upper())
        if key in self.subscriptions:
            self.send_error("subscribe", f"already subscribed to {channel} for {resolved.canonical_symbol}")
            return
        try:
            sub = await self._start_subscription(channel, symbol.strip(), resolved, frame)
        except Exception as exc:
            logger.warning("ws subscribe failed: channel=%s symbol=%s", channel, symbol, exc_info=True)
            self.send_error("subscribe", f"failed to start {channel} subscription: {exc}")
            return
        self.subscriptions[key] = sub
        self.enqueue({"type": "subscribed", "channel": channel, "symbol": sub.symbol, "resolved": resolved.describe()})
        logger.info("ws subscription started: channel=%s symbol=%s canonical=%s", channel, sub.symbol, sub.canonical_symbol)

    async def _start_subscription(self, channel: str, symbol: str, resolved: ResolvedSymbol, frame: dict[str, Any]) -> _Subscription:
        max_updates = frame.get("max_updates") or 0
        if not isinstance(max_updates, int) or isinstance(max_updates, bool) or max_updates < 0:
            raise ValueError("max_updates must be a non-negative integer")
        sub = _Subscription(
            channel=channel,
            symbol=symbol,
            canonical_symbol=resolved.canonical_symbol,
            asset_class=resolved.asset_class,
            exchange=resolved.exchange,
            currency=resolved.currency,
            max_updates=max_updates,
            params={},
        )
        if channel == "quotes":
            await self._start_quotes(sub, resolved, frame)
        elif channel == "ticks":
            await self._start_ticks(sub, resolved, frame)
        else:
            await self._start_bars(sub, resolved, frame)
        return sub

    async def _start_quotes(self, sub: _Subscription, resolved: ResolvedSymbol, frame: dict[str, Any]) -> None:
        try:
            interval = float(frame.get("interval_seconds", DEFAULT_QUOTE_INTERVAL_SECONDS))
        except (TypeError, ValueError) as exc:
            raise ValueError("interval_seconds must be a number") from exc
        if not 0.0 < interval <= 60.0:
            raise ValueError("interval_seconds must be in (0, 60]")
        sub.params = {"interval_seconds": interval}

        def enqueue(data: Any) -> None:
            self.publish(sub, data)

        sub.cleanup = await _start_quotes_subscription(self.state, resolved, interval, enqueue)

    async def _start_ticks(self, sub: _Subscription, resolved: ResolvedSymbol, frame: dict[str, Any]) -> None:
        try:
            tick_type = _parse_tick_type(frame.get("tick_type", DEFAULT_TICK_TYPE_NAME))
        except (KeyError, ValueError) as exc:
            raise ValueError(f"invalid tick_type: {frame.get('tick_type')!r}") from exc
        sub.params = {"tick_type": tick_type.value}
        sec_type = SEC_TYPE_BY_ASSET_CLASS[resolved.asset_class]

        def on_tick(tick: TickByTickData) -> None:
            dump = getattr(tick, "model_dump", None)
            payload = dump(mode="json") if callable(dump) else tick
            self.publish(sub, payload)

        await self.state.feed.start_tick_by_tick(
            resolved.canonical_symbol,
            sec_type=sec_type,
            exchange=resolved.exchange,
            currency=resolved.currency,
            tick_type=tick_type,
            on_tick=on_tick,
        )
        sub.tick_key = (resolved.canonical_symbol, sec_type, resolved.exchange)

    async def _start_bars(self, sub: _Subscription, resolved: ResolvedSymbol, frame: dict[str, Any]) -> None:
        what_to_show = str(frame.get("what_to_show") or DEFAULT_WHAT_TO_SHOW).strip()
        if not what_to_show:
            raise ValueError("what_to_show must be a non-empty string")
        sub.params = {"what_to_show": what_to_show}

        async def consume() -> None:
            maybe_stream = self.state.feed.subscribe_realtime_bars(
                resolved.canonical_symbol,
                asset_class=resolved.asset_class.value,
                exchange=resolved.exchange,
                currency=resolved.currency,
                what_to_show=what_to_show,
            )
            # The facade returns the async generator after awaiting; a raw
            # async-generator function returns it directly. Support both.
            stream = maybe_stream if hasattr(maybe_stream, "__aiter__") else await maybe_stream
            try:
                async for bar in stream:
                    self.publish(sub, dict(bar))
            finally:
                close = getattr(stream, "aclose", None)
                if close is not None:
                    try:
                        await close()
                    except Exception:
                        logger.debug("ws bars: stream close failed for %s", resolved.canonical_symbol, exc_info=True)

        sub.task = asyncio.create_task(consume())
        sub.task.set_name(f"ws-bars-{resolved.canonical_symbol}")

    async def _handle_unsubscribe(self, frame: dict[str, Any]) -> None:
        channel = frame.get("channel")
        symbol = frame.get("symbol")
        if channel not in VALID_CHANNELS:
            self.send_error("unsubscribe", f"unknown channel: {channel!r}")
            return
        if not isinstance(symbol, str) or not symbol.strip():
            self.send_error("unsubscribe", "symbol must be a non-empty string")
            return
        found = self._find_subscription(channel, symbol)
        if found is None:
            self.send_error("unsubscribe", f"no active {channel} subscription for {symbol}")
            return
        key, _ = found
        await self.teardown(key, notify=True)

    # ------------------------------------------------------------------
    # Main loops
    # ------------------------------------------------------------------

    async def writer(self) -> None:
        """Drain the outbound queue and send each frame as JSON text."""
        while True:
            message = await self.outbox.get()
            await self.ws.send_text(json.dumps(message))

    async def run(self) -> None:
        """Reader loop: receive, parse, dispatch; clean up on disconnect."""
        writer_task = asyncio.create_task(self.writer())
        writer_task.set_name("ws-writer")
        try:
            while True:
                raw = await self.ws.receive_text()
                try:
                    frame = json.loads(raw)
                except json.JSONDecodeError:
                    self.send_error(None, "malformed JSON frame")
                    continue
                try:
                    await self.handle_frame(frame)
                except Exception:
                    logger.exception("ws frame dispatch failed")
                    self.send_error(frame.get("action") if isinstance(frame, dict) else None, "internal dispatch error")
        except WebSocketDisconnect:
            logger.info("ws client disconnected (subscriptions=%d dropped=%d)", len(self.subscriptions), self.dropped_frames)
        except Exception:
            logger.exception("ws reader loop failed")
        finally:
            for key in list(self.subscriptions):
                try:
                    await self.teardown(key, notify=False)
                except Exception:
                    logger.warning("ws teardown during disconnect failed: key=%s", key, exc_info=True)
            writer_task.cancel()
            await asyncio.gather(writer_task, return_exceptions=True)
            try:
                await self.ws.close()
            except Exception:
                logger.debug("ws close after disconnect failed", exc_info=True)


@router.websocket("/ws/market-data")
async def market_data_socket(websocket: WebSocket) -> None:
    """Realtime market-data feed socket.

    State access mirrors ``get_rest_state`` (which reads
    ``request.app.state.ibkr_rest_state``) but through the WebSocket scope,
    because FastAPI dependency injection does not apply to plain WS handlers.
    """
    state = websocket.app.state.ibkr_rest_state
    await websocket.accept()
    await _Connection(websocket, state).run()
