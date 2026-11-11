"""Realtime WebSocket feed channel: topic pub/sub over one socket.

Protocol (JSON text frames), client to server:

    {"action": "subscribe", "channel": "quotes", "symbol": "AAPL", "interval_seconds": 0.5}
    {"action": "subscribe", "channel": "ticks", "symbol": "ESZ26", "tick_type": "ALL_LAST"}
    {"action": "subscribe", "channel": "bars", "symbol": "EURUSD", "what_to_show": "MIDPOINT"}
    {"action": "unsubscribe", "channel": "ticks", "symbol": "ESZ26"}
    {"action": "status"}
    {"action": "ping"}

Server to client:

    {"type": "subscribed", "channel", "symbol", "topic", "subscribers", "resolved"}
    {"type": "quotes"|"ticks"|"bars", "symbol", "topic", "seq", "data", "server_ts"}
    {"type": "unsubscribed", "channel", "symbol"}
    {"type": "status", "subscriptions": [...], "topics": [...]}
    {"type": "pong"}
    {"type": "error", "action", "detail"}

Pub/sub model: every subscription resolves to a broker topic keyed by channel
plus contract identity plus stream parameters. The first subscriber starts the
upstream IBKR stream; the last unsubscribe stops it. All subscribers on one
topic share one upstream market-data line and receive frames fanned out by the
broker (see ``src/webapp/ws_broker.py``). Each frame carries a per-topic ``seq``
so clients detect gaps caused by their own queue overflow. ``interval_seconds``
applies to the first subscriber of a quotes topic; later subscribers ride that
cadence and may thin frames client-side.
"""

from __future__ import annotations

import asyncio
import json
import logging
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
from src.webapp.ws_broker import MarketDataBroker, TopicKey, _now_iso, broker, topic_name

logger = logging.getLogger(__name__)

router = APIRouter()

VALID_CHANNELS = ("quotes", "ticks", "bars")
MAX_SUBSCRIPTIONS_PER_CONNECTION = 32
OUTBOX_MAXSIZE = 1000
DEFAULT_QUOTE_INTERVAL_SECONDS = 0.5
DEFAULT_TICK_TYPE_NAME = "ALL_LAST"
DEFAULT_WHAT_TO_SHOW = "TRADES"

Cleanup = Any


def _parse_asset_class(raw: Any) -> AssetClass | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    return AssetClass(text.lower())


def _parse_optional_str(raw: Any) -> str | None:
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
    """One connection's attachment to a broker topic."""

    channel: str
    symbol: str
    canonical_symbol: str
    topic_key: TopicKey
    asset_class: str = ""
    exchange: str = ""
    currency: str = ""
    subscriber_id: int = 0
    max_updates: int = 0
    updates_sent: int = 0
    stopping: bool = False
    params: dict[str, Any] = field(default_factory=dict)

    def status_frame(self) -> dict[str, Any]:
        info: dict[str, Any] = {
            "channel": self.channel,
            "symbol": self.symbol,
            "canonical_symbol": self.canonical_symbol,
            "asset_class": self.asset_class,
            "exchange": self.exchange,
            "currency": self.currency,
            "topic": topic_name(self.topic_key),
            "updates_sent": self.updates_sent,
        }
        info.update(self.params or {})
        return info


# ---------------------------------------------------------------------------
# Upstream factories: one IBKR stream per topic, publishing into the broker
# ---------------------------------------------------------------------------


async def _start_quotes_upstream(
    state: Any,
    resolved: ResolvedSymbol,
    interval_seconds: float,
    publish: Any,
) -> Cleanup:
    """reqMktData-backed quote poller publishing snapshots to the topic."""
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
            publish(snapshot.model_dump(mode="json"))
            await asyncio.sleep(interval_seconds)

    task = asyncio.create_task(_poll())
    task.set_name(f"ws-topic-quotes-{resolved.canonical_symbol}")

    async def _cleanup() -> None:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        try:
            await state.feed.unsubscribe_ticker(ticker)
        except Exception:
            logger.warning("ws quotes: unsubscribe_ticker failed for %s", resolved.canonical_symbol, exc_info=True)

    return _cleanup


def _make_quotes_upstream(state: Any, resolved: ResolvedSymbol, interval: float):
    """Build a start-upstream factory for one quotes topic."""

    async def start(publish: Any) -> Cleanup:
        return await _start_quotes_upstream(state, resolved, interval, publish)

    return start


def _make_ticks_upstream(state: Any, resolved: ResolvedSymbol, tick_type: TickType):
    """Build a start-upstream factory for one tick-by-tick topic."""

    async def start(publish: Any) -> Cleanup:
        sec_type = SEC_TYPE_BY_ASSET_CLASS[resolved.asset_class]

        def on_tick(tick: TickByTickData) -> None:
            dump = getattr(tick, "model_dump", None)
            publish(dump(mode="json") if callable(dump) else tick)

        await state.feed.start_tick_by_tick(
            resolved.canonical_symbol,
            sec_type=sec_type,
            exchange=resolved.exchange,
            currency=resolved.currency,
            tick_type=tick_type,
            on_tick=on_tick,
        )

        async def _cleanup() -> None:
            try:
                await state.feed.stop_tick_by_tick(resolved.canonical_symbol, sec_type, resolved.exchange)
            except Exception:
                logger.warning(
                    "ws ticks: stop_tick_by_tick failed for %s", resolved.canonical_symbol, exc_info=True
                )

        return _cleanup

    return start


def _make_bars_upstream(state: Any, resolved: ResolvedSymbol, what_to_show: str):
    """Build a start-upstream factory for one realtime-bars topic."""

    async def start(publish: Any) -> Cleanup:
        async def consume() -> None:
            maybe_stream = state.feed.subscribe_realtime_bars(
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
                    publish(dict(bar))
            finally:
                close = getattr(stream, "aclose", None)
                if close is not None:
                    try:
                        await close()
                    except Exception:
                        logger.debug("ws bars: stream close failed for %s", resolved.canonical_symbol, exc_info=True)

        task = asyncio.create_task(consume())
        task.set_name(f"ws-topic-bars-{resolved.canonical_symbol}")

        async def _cleanup() -> None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        return _cleanup

    return start


class _Connection:
    """State for one WebSocket connection: outbox, topic attachments, reader loop."""

    def __init__(self, ws: WebSocket, state: Any, hub: MarketDataBroker) -> None:
        self.ws = ws
        self.state = state
        self.hub = hub
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

    def _make_deliver(self, sub: _Subscription) -> Any:
        """Subscriber callback: stamp the client notation and honor max_updates."""

        def deliver(frame: dict[str, Any]) -> None:
            if sub.stopping:
                return
            message = dict(frame)
            message["symbol"] = sub.symbol
            self.enqueue(message)
            sub.updates_sent += 1
            if sub.max_updates > 0 and sub.updates_sent >= sub.max_updates:
                sub.stopping = True
                asyncio.get_running_loop().create_task(self._auto_detach(sub))

        return deliver

    async def _auto_detach(self, sub: _Subscription) -> None:
        key = (sub.channel, sub.canonical_symbol.upper())
        await self.teardown(key, notify=True)

    # ------------------------------------------------------------------
    # Subscription lifecycle (broker attach/detach)
    # ------------------------------------------------------------------

    async def teardown(self, key: tuple[str, str], *, notify: bool) -> _Subscription | None:
        """Detach one subscription from its broker topic."""
        sub = self.subscriptions.pop(key, None)
        if sub is None:
            return None
        sub.stopping = True
        remaining = await self.hub.detach(sub.topic_key, sub.subscriber_id)
        if notify:
            self.enqueue({"type": "unsubscribed", "channel": sub.channel, "symbol": sub.symbol})
        logger.info(
            "ws subscription detached: channel=%s symbol=%s topic=%s updates=%d topic_subscribers_left=%d",
            sub.channel, sub.symbol, topic_name(sub.topic_key), sub.updates_sent, remaining,
        )
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
            self.enqueue({
                "type": "status",
                "subscriptions": [sub.status_frame() for sub in self.subscriptions.values()],
                "topics": self.hub.topic_status(),
            })
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
            sub, subscribers = await self._attach(channel, symbol.strip(), resolved, frame)
        except Exception as exc:
            logger.warning("ws subscribe failed: channel=%s symbol=%s", channel, symbol, exc_info=True)
            self.send_error("subscribe", f"failed to start {channel} subscription: {exc}")
            return
        self.subscriptions[key] = sub
        self.enqueue({
            "type": "subscribed",
            "channel": channel,
            "symbol": sub.symbol,
            "topic": topic_name(sub.topic_key),
            "subscribers": subscribers,
            "resolved": resolved.describe(),
        })
        logger.info(
            "ws subscription attached: channel=%s symbol=%s canonical=%s topic=%s subscribers=%d",
            channel, sub.symbol, sub.canonical_symbol, topic_name(sub.topic_key), subscribers,
        )

    async def _attach(
        self, channel: str, symbol: str, resolved: ResolvedSymbol, frame: dict[str, Any]
    ) -> tuple[_Subscription, int]:
        """Build the topic key, start/attach the upstream, register the deliver callback."""
        max_updates = frame.get("max_updates") or 0
        if not isinstance(max_updates, int) or isinstance(max_updates, bool) or max_updates < 0:
            raise ValueError("max_updates must be a non-negative integer")

        if channel == "quotes":
            try:
                interval = float(frame.get("interval_seconds", DEFAULT_QUOTE_INTERVAL_SECONDS))
            except (TypeError, ValueError) as exc:
                raise ValueError("interval_seconds must be a number") from exc
            if not 0.0 < interval <= 60.0:
                raise ValueError("interval_seconds must be in (0, 60]")
            topic_key: TopicKey = (
                "quotes", resolved.canonical_symbol.upper(),
                resolved.asset_class.value, resolved.exchange, resolved.currency,
            )
            meta = {"interval_seconds": interval, **resolved.describe()}
            start = _make_quotes_upstream(self.state, resolved, interval)
            params: dict[str, Any] = {"interval_seconds": interval}
        elif channel == "ticks":
            try:
                tick_type = _parse_tick_type(frame.get("tick_type", DEFAULT_TICK_TYPE_NAME))
            except (KeyError, ValueError) as exc:
                raise ValueError(f"invalid tick_type: {frame.get('tick_type')!r}") from exc
            sec_type = SEC_TYPE_BY_ASSET_CLASS[resolved.asset_class]
            topic_key = (
                "ticks", resolved.canonical_symbol.upper(), sec_type, resolved.exchange, tick_type.value,
            )
            meta = {"tick_type": tick_type.value, **resolved.describe()}
            start = _make_ticks_upstream(self.state, resolved, tick_type)
            params = {"tick_type": tick_type.value}
        else:
            what_to_show = str(frame.get("what_to_show") or DEFAULT_WHAT_TO_SHOW).strip()
            if not what_to_show:
                raise ValueError("what_to_show must be a non-empty string")
            topic_key = (
                "bars", resolved.canonical_symbol.upper(),
                resolved.asset_class.value, resolved.exchange, resolved.currency, what_to_show,
            )
            meta = {"what_to_show": what_to_show, **resolved.describe()}
            start = _make_bars_upstream(self.state, resolved, what_to_show)
            params = {"what_to_show": what_to_show}

        sub = _Subscription(
            channel=channel,
            symbol=symbol,
            canonical_symbol=resolved.canonical_symbol,
            topic_key=topic_key,
            asset_class=resolved.asset_class.value,
            exchange=resolved.exchange,
            currency=resolved.currency,
            max_updates=max_updates,
            params=params,
        )
        subscriber_id, subscribers = await self.hub.attach(
            topic_key,
            channel=channel,
            start_upstream=start,
            deliver=self._make_deliver(sub),
            meta=meta,
        )
        sub.subscriber_id = subscriber_id
        return sub, subscribers


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


# ---------------------------------------------------------------------------
# Main loops
# ---------------------------------------------------------------------------

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
                    detail_source = frame.get("action") if isinstance(frame, dict) else None
                    self.send_error(detail_source, "internal dispatch error")
        except WebSocketDisconnect:
            logger.info(
                "ws client disconnected (subscriptions=%d dropped=%d)",
                len(self.subscriptions), self.dropped_frames,
            )
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
    """Realtime market-data feed socket (topic pub/sub; see module docstring).

    State access mirrors ``get_rest_state`` (which reads
    ``request.app.state.ibkr_rest_state``) but through the WebSocket scope,
    because FastAPI dependency injection does not apply to plain WS handlers.
    """
    state = websocket.app.state.ibkr_rest_state
    await websocket.accept()
    connection = _Connection(websocket, state, broker)
    await connection.run()
