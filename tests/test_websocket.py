"""WebSocket market-data channel protocol and lifecycle tests.

Uses a FakeFeed whose tick-by-tick start emits two ticks then idles, whose
realtime-bars method returns an async generator yielding two bars, and whose
ticker path (quotes channel) mirrors streaming.py: subscribe_ticker returns a
ticker-like object that the poller converts to snapshots.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.tick_data import TickByTickData, TickType

WS_PATH = "/api/v1/ws/market-data"


class FakeFeed:
    """Feed double: records calls and emits deterministic data."""

    def __init__(self) -> None:
        self.tick_starts: list[tuple[Any, ...]] = []
        self.tick_stops: list[tuple[str, str, str]] = []
        self.bar_requests: list[tuple[Any, ...]] = []
        self.ticker_specs: list[Any] = []
        self.ticker_unsubscribes: list[Any] = []
        self._emit_tasks: list[asyncio.Task[None]] = []

    # Ticks: call on_tick twice, then hold the subscription open.

    async def start_tick_by_tick(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        tick_type: TickType | None = None,
        max_ticks: int = 10000,
        on_tick: Any | None = None,
    ) -> str:
        self.tick_starts.append((symbol, sec_type, exchange, currency, tick_type))
        effective_type = tick_type or TickType.ALL_LAST

        async def emit() -> None:
            for index in range(2):
                assert on_tick is not None
                on_tick(
                    TickByTickData(
                        timestamp=datetime.now(timezone.utc),
                        tick_type=effective_type,
                        price=100.0 + index,
                        size=1.0,
                    )
                )
                await asyncio.sleep(0.01)
            await asyncio.sleep(3600)

        task = asyncio.get_running_loop().create_task(emit())
        self._emit_tasks.append(task)
        return "tick-handle"

    async def stop_tick_by_tick(self, symbol: str, sec_type: str = "STK", exchange: str = "SMART") -> None:
        self.tick_stops.append((symbol, sec_type, exchange))
        for task in self._emit_tasks:
            task.cancel()

    # Bars: async method returning an async generator yielding two bars.

    async def subscribe_realtime_bars(
        self,
        symbol: str,
        asset_class: str = "EQUITY",
        exchange: str = "SMART",
        currency: str = "USD",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> Any:
        self.bar_requests.append((symbol, asset_class, exchange, currency, what_to_show, use_rth))

        async def stream() -> Any:
            for index in range(2):
                await asyncio.sleep(0)
                yield {
                    "time": 1731325200 + index * 5,
                    "open": 100.0 + index,
                    "high": 101.0 + index,
                    "low": 99.0 + index,
                    "close": 100.5 + index,
                    "volume": 10.0,
                    "wap": 100.25,
                    "count": 2,
                }

        return stream()

    # Quotes: same mechanism streaming.py uses (reqMktData ticker object).

    async def subscribe_ticker(self, spec: Any) -> Any:
        self.ticker_specs.append(spec)
        return SimpleNamespace(
            time=datetime.now(timezone.utc),
            last=150.0,
            bid=149.0,
            ask=151.0,
            bidSize=10.0,
            askSize=12.0,
            volume=1000.0,
        )

    async def unsubscribe_ticker(self, ticker: Any) -> None:
        self.ticker_unsubscribes.append(ticker)


class _FakeRedis:
    async def get_raw(self, _key: str) -> bytes:
        return b""


class FakeState:
    """Minimal IBKRRestAppState stand-in; only settings and feed matter here."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.feed = FakeFeed()
        self.loader = None
        self.redis = _FakeRedis()
        self.market_data_cache = None
        self.fixed_income_reference_provider = None
        self.event_contracts = SimpleNamespace()

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass


def _make_client() -> tuple[TestClient, FakeState]:
    from src.webapp.app import create_app

    state = FakeState(Settings())
    app = create_app(settings=state.settings, state=state)  # type: ignore[arg-type]
    return TestClient(app), state


def _next_of(ws: Any, *types: str) -> dict[str, Any]:
    """Read frames until one matches the wanted types (skips interleaved data)."""
    while True:
        message = ws.receive_json()
        assert isinstance(message, dict)
        if message.get("type") in types:
            return message


def _wait_for(predicate: Any, timeout: float = 5.0) -> None:
    """Poll until the predicate holds; server cleanup runs on another thread."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    assert predicate(), "timed out waiting for server-side cleanup"


# ---------------------------------------------------------------------------
# 1. ping / pong
# ---------------------------------------------------------------------------


def test_ping_returns_pong() -> None:
    client, _ = _make_client()
    with client, client.websocket_connect(WS_PATH) as ws:
        ws.send_json({"action": "ping"})
        assert ws.receive_json() == {"type": "pong"}


# ---------------------------------------------------------------------------
# 2. ticks lifecycle
# ---------------------------------------------------------------------------


def test_ticks_subscribe_data_unsubscribe() -> None:
    client, state = _make_client()
    feed = state.feed
    with client, client.websocket_connect(WS_PATH) as ws:
        ws.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26"})
        subscribed = _next_of(ws, "subscribed")
        assert subscribed["channel"] == "ticks"
        assert subscribed["symbol"] == "ESZ26"
        assert subscribed["resolved"]["asset_class"] == "future"

        ticks = [_next_of(ws, "ticks"), _next_of(ws, "ticks")]
        assert [tick["data"]["price"] for tick in ticks] == [100.0, 101.0]
        assert all(tick["symbol"] == "ESZ26" and "server_ts" in tick for tick in ticks)

        ws.send_json({"action": "unsubscribe", "channel": "ticks", "symbol": "ESZ26"})
        unsubscribed = _next_of(ws, "unsubscribed")
        assert unsubscribed == {"type": "unsubscribed", "channel": "ticks", "symbol": "ESZ26"}

    assert len(feed.tick_starts) == 1
    started = feed.tick_starts[0]
    assert started[0] == "ES"  # canonical symbol
    assert started[3] == "USD"
    _wait_for(lambda: len(feed.tick_stops) == 1)
    stopped = feed.tick_stops[0]
    assert stopped == ("ES", "FUT", "CME")


# ---------------------------------------------------------------------------
# 3. bars stream
# ---------------------------------------------------------------------------


def test_bars_subscribe_streams_bars() -> None:
    client, state = _make_client()
    feed = state.feed
    with client, client.websocket_connect(WS_PATH) as ws:
        ws.send_json({"action": "subscribe", "channel": "bars", "symbol": "AAPL"})
        subscribed = _next_of(ws, "subscribed")
        assert subscribed["resolved"]["asset_class"] == "equity"

        first_bar = _next_of(ws, "bars")
        assert first_bar["symbol"] == "AAPL"
        assert first_bar["data"]["open"] == 100.0
        assert "server_ts" in first_bar
        second_bar = _next_of(ws, "bars")
        assert second_bar["data"]["close"] == 101.5

    assert len(feed.bar_requests) == 1
    symbol, asset_class, exchange, currency, what_to_show, _use_rth = feed.bar_requests[0]
    assert (symbol, asset_class, exchange, currency, what_to_show) == ("AAPL", "equity", "SMART", "USD", "TRADES")


# ---------------------------------------------------------------------------
# 4. duplicate subscription
# ---------------------------------------------------------------------------


def test_duplicate_subscribe_returns_error() -> None:
    client, _ = _make_client()
    with client, client.websocket_connect(WS_PATH) as ws:
        ws.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26"})
        assert _next_of(ws, "subscribed")["type"] == "subscribed"
        ws.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26"})
        error = _next_of(ws, "error")
        assert error["action"] == "subscribe"
        assert "already subscribed" in error["detail"]


# ---------------------------------------------------------------------------
# 5. unknown channel and malformed frames keep the socket alive
# ---------------------------------------------------------------------------


def test_unknown_channel_and_malformed_frame_then_ping() -> None:
    client, _ = _make_client()
    with client, client.websocket_connect(WS_PATH) as ws:
        ws.send_json({"action": "subscribe", "channel": "widgets", "symbol": "AAPL"})
        error = _next_of(ws, "error")
        assert error["action"] == "subscribe"
        assert "unknown channel" in error["detail"]

        ws.send_text("not json")
        malformed = _next_of(ws, "error")
        assert malformed["type"] == "error"
        assert "malformed" in malformed["detail"].lower()

        ws.send_json({"action": "ping"})
        assert _next_of(ws, "pong") == {"type": "pong"}


def test_unknown_action_returns_error() -> None:
    client, _ = _make_client()
    with client, client.websocket_connect(WS_PATH) as ws:
        ws.send_json({"action": "detonate"})
        error = _next_of(ws, "error")
        assert error["action"] == "detonate"
        assert "unknown action" in error["detail"]


# ---------------------------------------------------------------------------
# 6. status
# ---------------------------------------------------------------------------


def test_status_lists_active_subscriptions() -> None:
    client, _ = _make_client()
    with client, client.websocket_connect(WS_PATH) as ws:
        ws.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26"})
        assert _next_of(ws, "subscribed")["type"] == "subscribed"

        ws.send_json({"action": "status"})
        status = _next_of(ws, "status")
        entries = status["subscriptions"]
        assert len(entries) == 1
        assert entries[0]["channel"] == "ticks"
        assert entries[0]["symbol"] == "ESZ26"
        assert entries[0]["canonical_symbol"] == "ES"
        assert entries[0]["asset_class"] == "future"


# ---------------------------------------------------------------------------
# 7. disconnect cleanup
# ---------------------------------------------------------------------------


def test_disconnect_stops_feed_subscriptions() -> None:
    client, state = _make_client()
    feed = state.feed
    with client:
        with client.websocket_connect(WS_PATH) as ws:
            ws.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26"})
            assert _next_of(ws, "subscribed")["type"] == "subscribed"
        # Exiting the session block disconnects the socket.
    _wait_for(lambda: len(feed.tick_stops) == 1)
    assert feed.tick_stops[0] == ("ES", "FUT", "CME")


# ---------------------------------------------------------------------------
# 8. resolution failure surfaces the resolver message
# ---------------------------------------------------------------------------


def test_unresolvable_explicit_fx_pair_returns_error() -> None:
    client, _ = _make_client()
    with client, client.websocket_connect(WS_PATH) as ws:
        ws.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ZZZ999", "asset_class": "fx"})
        error = _next_of(ws, "error")
        assert error["action"] == "subscribe"
        assert "not a currency pair" in error["detail"]


# ---------------------------------------------------------------------------
# 9. quotes producer (real _start_quotes_subscription path via fake ticker)
# ---------------------------------------------------------------------------


def test_quotes_subscribe_streams_snapshots_and_releases_ticker() -> None:
    client, state = _make_client()
    feed = state.feed
    with client, client.websocket_connect(WS_PATH) as ws:
        ws.send_json({"action": "subscribe", "channel": "quotes", "symbol": "AAPL", "interval_seconds": 0.05})
        subscribed = _next_of(ws, "subscribed")
        assert subscribed["channel"] == "quotes"
        assert subscribed["resolved"]["asset_class"] == "equity"

        first = _next_of(ws, "quotes")
        assert first["symbol"] == "AAPL"
        assert first["data"]["last"] == 150.0
        assert first["data"]["bid"] == 149.0
        assert "server_ts" in first

        ws.send_json({"action": "unsubscribe", "channel": "quotes", "symbol": "AAPL"})
        assert _next_of(ws, "unsubscribed")["type"] == "unsubscribed"

    assert len(feed.ticker_specs) == 1
    _wait_for(lambda: len(feed.ticker_unsubscribes) == 1)
    assert feed.ticker_unsubscribes[0] is not None
