"""Pub/sub broker tests: refcounting, fan-out, sequence numbers, isolation."""

from __future__ import annotations

import asyncio
import json
import time as _time
from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.tick_data import TickByTickData, TickType
from src.webapp.ws_broker import MarketDataBroker


# ---------------------------------------------------------------------------
# Broker unit tests (no WebSocket)
# ---------------------------------------------------------------------------


async def _noop_upstream(publish: Any) -> Any:
    async def cleanup() -> None:
        pass

    return cleanup


class _QueueSubscriber:
    """Subscriber with a bounded queue that mirrors a connection outbox."""

    def __init__(self, maxsize: int = 100) -> None:
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=maxsize)
        self.dropped = 0

    def deliver(self, frame: dict[str, Any]) -> None:
        try:
            self.queue.put_nowait(frame)
            return
        except asyncio.QueueFull:
            pass
        try:
            self.queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            self.queue.put_nowait(frame)
        except asyncio.QueueFull:
            pass
        self.dropped += 1


@pytest.mark.asyncio
async def test_first_subscriber_starts_upstream_last_stops_it() -> None:
    hub = MarketDataBroker()
    starts = 0
    stops = 0

    async def start(publish: Any) -> Any:
        nonlocal starts
        starts += 1

        async def cleanup() -> None:
            nonlocal stops
            stops += 1

        return cleanup

    sub_a = _QueueSubscriber()
    sub_b = _QueueSubscriber()
    _, count = await hub.attach(("ticks", "ES"), channel="ticks", start_upstream=start, deliver=sub_a.deliver)
    assert count == 1 and starts == 1
    _, count = await hub.attach(("ticks", "ES"), channel="ticks", start_upstream=start, deliver=sub_b.deliver)
    assert count == 2 and starts == 1  # second attach must not start another upstream

    remaining = await hub.detach(("ticks", "ES"), 1)
    assert remaining == 1 and stops == 0  # one left: upstream stays
    remaining = await hub.detach(("ticks", "ES"), 2)
    assert remaining == 0 and stops == 1  # last leaves: upstream stops
    assert hub.active_topic_count() == 0


@pytest.mark.asyncio
async def test_fan_out_and_sequence_numbers() -> None:
    hub = MarketDataBroker()
    received_a: list[dict[str, Any]] = []
    received_b: list[dict[str, Any]] = []
    publishers: list[Any] = []

    async def start(publish: Any) -> Any:
        publishers.append(publish)

        async def cleanup() -> None:
            pass

        return cleanup

    await hub.attach(("t",), channel="ticks", start_upstream=start, deliver=received_a.append)
    await hub.attach(("t",), channel="ticks", start_upstream=start, deliver=received_b.append)
    assert len(publishers) == 1  # upstream started once

    publishers[0]({"price": 1})
    publishers[0]({"price": 2})
    publishers[0]({"price": 3})
    assert [f["seq"] for f in received_a] == [1, 2, 3]
    assert [f["seq"] for f in received_b] == [1, 2, 3]
    assert received_a[0]["topic"] == "t"
    assert received_a[0]["type"] == "ticks"
    assert "server_ts" in received_a[0]


@pytest.mark.asyncio
async def test_slow_subscriber_drops_only_own_frames() -> None:
    hub = MarketDataBroker()
    fast = _QueueSubscriber(maxsize=100)
    slow = _QueueSubscriber(maxsize=2)
    publishers: list[Any] = []

    async def start(publish: Any) -> Any:
        publishers.append(publish)

        async def cleanup() -> None:
            pass

        return cleanup

    await hub.attach(("ticks", "ES"), channel="ticks", start_upstream=start, deliver=fast.deliver)
    await hub.attach(("ticks", "ES"), channel="ticks", start_upstream=start, deliver=slow.deliver)

    for index in range(6):
        publishers[0]({"i": index})
    assert fast.queue.qsize() == 6 and fast.dropped == 0
    assert slow.dropped == 4  # kept the newest 2
    kept: list[dict[str, Any]] = []
    while not slow.queue.empty():
        kept.append(slow.queue.get_nowait())
    assert [frame["data"]["i"] for frame in kept] == [4, 5]
    # Sequence numbers expose the gap to the slow client.
    assert [frame["seq"] for frame in kept] == [5, 6]


@pytest.mark.asyncio
async def test_topic_status_reports_fanout() -> None:
    hub = MarketDataBroker()
    sub = _QueueSubscriber()
    await hub.attach(("bars", "AAPL", "equity"), channel="bars", start_upstream=_noop_upstream, deliver=sub.deliver, meta={"symbol": "AAPL"})
    status = hub.topic_status()
    assert status[0]["topic"] == "bars.AAPL.equity"
    assert status[0]["subscribers"] == 1
    assert status[0]["seq"] == 0


# ---------------------------------------------------------------------------
# End-to-end WebSocket pub/sub tests
# ---------------------------------------------------------------------------


class PubSubFakeFeed:
    """Feed fake with on-demand tick emitters and call recording."""

    def __init__(self) -> None:
        self.start_calls: list[tuple[str, str, str]] = []
        self.stop_calls: list[tuple[str, str, str]] = []
        self.emitters: dict[tuple[str, str, str], Any] = {}

    async def start_tick_by_tick(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        tick_type: TickType = TickType.ALL_LAST,
        max_ticks: int = 10_000,
        on_tick: Any = None,
    ) -> Any:
        key = (symbol.upper(), sec_type.upper(), exchange.upper())
        self.start_calls.append(key)
        self.emitters[key] = on_tick
        return object()

    async def stop_tick_by_tick(self, symbol: str, sec_type: str = "STK", exchange: str = "SMART") -> None:
        key = (symbol.upper(), sec_type.upper(), exchange.upper())
        self.stop_calls.append(key)
        self.emitters.pop(key, None)

    def emit(self, key: tuple[str, str, str], price: float) -> None:
        callback = self.emitters.get(key)
        assert callback is not None, f"no emitter for {key}"
        callback(
            TickByTickData(
                timestamp=datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc),
                tick_type=TickType.ALL_LAST,
                price=price,
                size=1,
            )
        )


class _FakeRedis:
    async def get_raw(self, _key: str) -> bytes:
        return b"token"


class PubSubState:
    def __init__(self) -> None:
        self.settings = Settings()
        self.feed = PubSubFakeFeed()
        self.loader = None
        self.redis = _FakeRedis()
        self.market_data_cache = None
        self.fixed_income_reference_provider = None

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass


def _make_app() -> tuple[TestClient, PubSubFakeFeed]:
    from src.webapp.app import create_app
    from src.webapp.routers import websocket as ws_router

    ws_router.broker.reset()
    state = PubSubState()
    app = create_app(settings=state.settings, state=state)  # type: ignore[arg-type]
    return TestClient(app), state.feed


def _next_of(ws: Any, frame_type: str, timeout: float = 5.0) -> dict[str, Any]:
    end = _time.monotonic() + timeout
    while _time.monotonic() < end:
        message = ws.receive_json()
        if message.get("type") == frame_type:
            return message
    raise AssertionError(f"no {frame_type} frame within {timeout}s")


ES_TICK_KEY = ("ES", "FUT", "CME")


def test_two_clients_share_one_upstream_and_both_receive() -> None:
    client, feed = _make_app()
    with client, client.websocket_connect("/api/v1/ws/market-data") as ws_a, client.websocket_connect("/api/v1/ws/market-data") as ws_b:
        ws_a.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26"})
        ack_a = _next_of(ws_a, "subscribed")
        assert ack_a["resolved"]["asset_class"] == "future"
        assert ack_a["subscribers"] == 1

        ws_b.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26"})
        ack_b = _next_of(ws_b, "subscribed")
        assert ack_b["topic"] == ack_a["topic"]
        assert ack_b["subscribers"] == 2

        assert len(feed.start_calls) == 1  # ONE upstream for two subscribers

        feed.emit(ES_TICK_KEY, 6000.25)
        frame_a = _next_of(ws_a, "ticks")
        frame_b = _next_of(ws_b, "ticks")
        assert frame_a["seq"] == frame_b["seq"] == 1
        assert frame_a["topic"] == frame_b["topic"]
        assert frame_a["data"]["price"] == 6000.25


def test_partial_unsubscribe_keeps_upstream_running() -> None:
    client, feed = _make_app()
    with client, client.websocket_connect("/api/v1/ws/market-data") as ws_a, client.websocket_connect("/api/v1/ws/market-data") as ws_b:
        ws_a.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26"})
        _next_of(ws_a, "subscribed")
        ws_b.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26"})
        _next_of(ws_b, "subscribed")

        ws_a.send_json({"action": "unsubscribe", "channel": "ticks", "symbol": "ESZ26"})
        _next_of(ws_a, "unsubscribed")
        assert feed.stop_calls == []  # one subscriber left: upstream stays

        feed.emit(ES_TICK_KEY, 6001.0)
        frame_b = _next_of(ws_b, "ticks")
        assert frame_b["data"]["price"] == 6001.0
        assert frame_b["seq"] == 1

    # Both sockets closed now: last detach stops the upstream.
    assert feed.stop_calls == [ES_TICK_KEY]


def test_status_reports_topic_fanout() -> None:
    client, _ = _make_app()
    with client, client.websocket_connect("/api/v1/ws/market-data") as ws:
        ws.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26"})
        _next_of(ws, "subscribed")
        ws.send_json({"action": "status"})
        status = _next_of(ws, "status")
        topics = status["topics"]
        assert len(topics) == 1
        assert topics[0]["channel"] == "ticks"
        assert topics[0]["subscribers"] == 1
        assert topics[0]["seq"] == 0
        assert status["subscriptions"][0]["topic"] == topics[0]["topic"]


def test_different_stream_params_are_distinct_topics() -> None:
    client, feed = _make_app()
    with client, client.websocket_connect("/api/v1/ws/market-data") as ws:
        ws.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26", "tick_type": "ALL_LAST"})
        ack_one = _next_of(ws, "subscribed")
        # Same channel+symbol from one connection is one subscription slot:
        # a second subscribe with different parameters is rejected.
        ws.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26", "tick_type": "BID_ASK"})
        duplicate = _next_of(ws, "error")
        assert "already subscribed" in duplicate["detail"]
        # Unsubscribe, then resubscribe with different stream parameters.
        ws.send_json({"action": "unsubscribe", "channel": "ticks", "symbol": "ESZ26"})
        _next_of(ws, "unsubscribed")
        assert feed.stop_calls == [ES_TICK_KEY]
        ws.send_json({"action": "subscribe", "channel": "ticks", "symbol": "ESZ26", "tick_type": "BID_ASK"})
        ack_two = _next_of(ws, "subscribed")
        assert ack_one["topic"] != ack_two["topic"]
        assert len(feed.start_calls) == 2  # distinct stream parameters: distinct upstreams
        assert len(feed.stop_calls) == 1
