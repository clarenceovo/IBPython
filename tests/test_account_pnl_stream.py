"""Live PnL SSE streams and cancel-all-subscriptions REST coverage.

Uses the fake-state pattern from tests/test_orders_disabled.py: the app is
built via create_app with a stub feed so the SSE endpoints can be exercised
against TestClient without an IBKR connection.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.models import AssetClass
from src.feeds.streaming import StreamSubscription
from src.webapp.routers import streaming as streaming_router


class _FakeAccountPnl:
    """Stand-in for an ib_async PnL object mutated in place by the wrapper.

    ``dailyPnL`` changes between reads so the SSE poll loop sees fresh values.
    """

    def __init__(self) -> None:
        self._reads = 0

    @property
    def dailyPnL(self) -> float:
        self._reads += 1
        return 100.0 + self._reads

    unrealizedPnL: float = 40.0
    realizedPnL: float = 0.0


class _FakePositionPnl:
    """Stand-in for an ib_async PnLSingle object mutated in place."""

    def __init__(self) -> None:
        self._reads = 0

    @property
    def dailyPnL(self) -> float:
        self._reads += 1
        return 5.0 + self._reads

    unrealizedPnL: float = 2.0
    realizedPnL: float = 0.0
    position: int = 10
    value: float = 1500.0


class _FakeIB:
    """Records ib_async PnL cancel calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def cancelPnL(self, *args: Any) -> None:
        self.calls.append(("cancelPnL", args))

    def cancelPnLSingle(self, *args: Any) -> None:
        self.calls.append(("cancelPnLSingle", args))


class _FakeMarketDataExt:
    """Records feed-level cancel_all_subscriptions calls."""

    def __init__(self) -> None:
        self.cancel_all_calls = 0

    async def cancel_all_subscriptions(self) -> None:
        self.cancel_all_calls += 1


class _FakePnlFeed:
    """Feed stub exposing the PnL subscribe pass-throughs."""

    def __init__(self, connection: Any, marketdata_ext: Any | None = None) -> None:
        self._connection = connection
        self._marketdata_ext = marketdata_ext
        self.account_pnl: _FakeAccountPnl | None = None
        self.position_pnl: _FakePositionPnl | None = None

    async def subscribe_account_pnl(self, account: str, model_code: str = "") -> _FakeAccountPnl:
        self.account_pnl = _FakeAccountPnl()
        return self.account_pnl

    async def subscribe_position_pnl(self, account: str, con_id: int, model_code: str = "") -> _FakePositionPnl:
        self.position_pnl = _FakePositionPnl()
        return self.position_pnl


class _FakeRedis:
    async def get_raw(self, _key: str) -> bytes:
        return b"test-pnl-token"


class _FakeState:
    def __init__(self, *, connected: bool = True, with_marketdata_ext: bool = False) -> None:
        self.settings = Settings()
        self.ib = _FakeIB()
        self.connection = SimpleNamespace(ib=self.ib, is_connected=connected)
        self.marketdata_ext: _FakeMarketDataExt | None = _FakeMarketDataExt() if with_marketdata_ext else None
        self.feed = _FakePnlFeed(self.connection, self.marketdata_ext)
        self.loader = None
        self.redis = _FakeRedis()  # type: ignore[assignment]
        self.market_data_cache = None
        self.event_contracts = None
        self.fixed_income_reference_provider = None

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None


def _make_client(state: _FakeState) -> TestClient:
    from src.webapp.app import create_app

    app = create_app(settings=state.settings, state=state)  # type: ignore[arg-type]
    return TestClient(app)


def _parse_data_events(body: str) -> list[dict[str, Any]]:
    prefix = "data: "
    return [json.loads(line[len(prefix) :]) for line in body.splitlines() if line.startswith(prefix)]


@pytest.fixture(autouse=True)
def _clean_active_subscriptions() -> Iterator[None]:
    streaming_router._active_subscriptions.clear()
    yield
    streaming_router._active_subscriptions.clear()


# ---------------------------------------------------------------------------
# GET /api/v1/account/pnl/account/stream
# ---------------------------------------------------------------------------


def test_account_pnl_stream_returns_sse_events_then_ends() -> None:
    state = _FakeState()
    client = _make_client(state)
    with client:
        response = client.get(
            "/api/v1/account/pnl/account/stream",
            params={"account_id": "DU123", "interval": 0.1, "max_updates": 2},
        )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events = _parse_data_events(response.text)
    assert len(events) == 2
    assert events[0]["account"] == "DU123"
    assert events[0]["daily_pnl"] is not None
    # The subscription is released once the stream ends.
    assert ("cancelPnL", ("DU123", "")) in state.ib.calls


def test_account_pnl_stream_releases_subscription_on_client_disconnect() -> None:
    state = _FakeState()
    client = _make_client(state)
    with client:
        with client.stream(
            "GET",
            "/api/v1/account/pnl/account/stream",
            params={"account_id": "DU123", "interval": 0.1},
        ) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            for line in response.iter_lines():
                if line.startswith("data: "):
                    assert json.loads(line[len("data: ") :])["account"] == "DU123"
                    break
    # Leaving the stream block disconnects the client. The endpoint must release the subscription.
    assert ("cancelPnL", ("DU123", "")) in state.ib.calls


def test_account_pnl_stream_returns_503_when_feed_disconnected() -> None:
    state = _FakeState(connected=False)
    client = _make_client(state)
    with client:
        response = client.get("/api/v1/account/pnl/account/stream", params={"account_id": "DU123"})
    assert response.status_code == 503
    # Never subscribed.
    assert state.feed.account_pnl is None


# ---------------------------------------------------------------------------
# GET /api/v1/account/pnl/position/stream
# ---------------------------------------------------------------------------


def test_position_pnl_stream_returns_sse_events_then_ends() -> None:
    state = _FakeState()
    client = _make_client(state)
    with client:
        response = client.get(
            "/api/v1/account/pnl/position/stream",
            params={"account_id": "DU123", "con_id": 12345, "interval": 0.1, "max_updates": 1},
        )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events = _parse_data_events(response.text)
    assert len(events) == 1
    assert events[0]["account"] == "DU123"
    assert events[0]["con_id"] == 12345
    assert events[0]["position"] == 10
    assert ("cancelPnLSingle", ("DU123", "", 12345)) in state.ib.calls


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/api/v1/account/pnl/account/stream", {}),
        ("/api/v1/account/pnl/position/stream", {"account_id": "DU123"}),
        ("/api/v1/account/pnl/position/stream", {"con_id": 123}),
    ],
)
def test_pnl_streams_reject_missing_required_params(path: str, params: dict[str, Any]) -> None:
    client = _make_client(_FakeState())
    with client:
        response = client.get(path, params=params)
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# DELETE /api/v1/streaming/subscriptions
# ---------------------------------------------------------------------------


def _seeded_stream_subscription(subscription_id: str) -> StreamSubscription:
    return StreamSubscription(
        subscription_id=subscription_id,
        symbol="SPY",
        asset_class=AssetClass.EQUITY,
        exchange="SMART",
        currency="USD",
        connected_at=datetime.now(timezone.utc),
    )


def test_cancel_all_subscriptions_stops_tracked_and_feed_level() -> None:
    state = _FakeState(with_marketdata_ext=True)
    streaming_router._active_subscriptions["sub-a"] = _seeded_stream_subscription("sub-a")
    streaming_router._active_subscriptions["sub-b"] = _seeded_stream_subscription("sub-b")
    client = _make_client(state)
    with client:
        response = client.delete("/api/v1/streaming/subscriptions")
    assert response.status_code == 200
    assert response.json() == {"cancelled": 2}
    assert streaming_router._active_subscriptions == {}
    assert state.marketdata_ext is not None and state.marketdata_ext.cancel_all_calls == 1


def test_cancel_all_subscriptions_with_none_active_returns_zero() -> None:
    state = _FakeState()
    client = _make_client(state)
    with client:
        response = client.delete("/api/v1/streaming/subscriptions")
    assert response.status_code == 200
    assert response.json() == {"cancelled": 0}
    assert streaming_router._active_subscriptions == {}
