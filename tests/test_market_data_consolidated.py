"""Consolidated market-data symbol endpoint behavior.

Covers the full-section response, the section whitelist, unknown-section
rejection, and per-section error isolation. Uses a fake app state so no
IBKR gateway or Redis instance is needed.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import Any

from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.contracts import ContractSpec
from src.feeds.models import AssetClass, OHLCVRequest
from src.feeds.snapshotter import EquitySnapshot
from src.feeds.tick_data import MarketDepthLevel, MarketDepthSnapshot, TickByTickData, TickType

SYMBOL = "AAPL"
ENDPOINT = "/api/v1/market-data/consolidated/symbol/AAPL"
ALL_SECTION_NAMES = ("snapshot", "ticks", "depth", "trading_schedule")

FIXED_TS = datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)


def _equity_snapshot() -> EquitySnapshot:
    return EquitySnapshot(
        symbol=SYMBOL,
        exchange="SMART",
        currency="USD",
        con_id=265598,
        timestamp=FIXED_TS,
        last=185.5,
        bid=185.4,
        ask=185.6,
        volume=1_000,
    )


def _tick() -> TickByTickData:
    return TickByTickData(
        timestamp=FIXED_TS,
        tick_type=TickType.LAST,
        price=185.5,
        size=10,
        exchange="SMART",
    )


def _depth_snapshot() -> MarketDepthSnapshot:
    return MarketDepthSnapshot(
        symbol=SYMBOL,
        asset_class=AssetClass.EQUITY.value,
        exchange="SMART",
        currency="USD",
        num_rows=5,
        is_smart_depth=True,
        snapshot_wait_seconds=1.5,
        received_at=FIXED_TS,
        bids=[MarketDepthLevel(position=0, price=185.4, size=100)],
        asks=[MarketDepthLevel(position=0, price=185.6, size=100)],
    )


def _session() -> SimpleNamespace:
    return SimpleNamespace(
        refDate="20260105",
        startDateTime="2026-01-05 09:30:00 US/Eastern",
        endDateTime="2026-01-05 16:00:00 US/Eastern",
    )


class FakeFeed:
    """Feed stub that records calls and can fail selected sections."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail_sections: set[str] = set()
        self.tick_calls: list[tuple[str, str, str, int]] = []
        self.depth_specs: list[ContractSpec] = []
        self.schedule_requests: list[OHLCVRequest] = []

    def get_latest_ticks(self, symbol: str, sec_type: str, exchange: str, n: int) -> list[TickByTickData]:
        self.calls.append("ticks")
        if "ticks" in self.fail_sections:
            raise RuntimeError("ticks unavailable")
        self.tick_calls.append((symbol, sec_type, exchange, n))
        return [_tick()]

    async def load_market_depth_snapshot(
        self,
        spec: ContractSpec,
        *,
        num_rows: int = 5,
        is_smart_depth: bool = False,
        snapshot_wait_seconds: float = 1.5,
        request_timeout_seconds: float = 10.0,
        lease_wait_seconds: float = 5.0,
    ) -> MarketDepthSnapshot:
        self.calls.append("depth")
        if "depth" in self.fail_sections:
            raise RuntimeError("depth unavailable")
        self.depth_specs.append(spec)
        return _depth_snapshot()

    async def load_trading_schedule(
        self,
        request: OHLCVRequest,
        *,
        ref_date: date,
        use_rth: bool = True,
    ) -> tuple[Any, ...]:
        self.calls.append("trading_schedule")
        if "trading_schedule" in self.fail_sections:
            raise RuntimeError("schedule unavailable")
        self.schedule_requests.append(request)
        return (_session(),)


class FakeRedis:
    """Redis stub that serves one cached equity snapshot."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    async def get_latest_equity_snapshots(self, symbols: list[str]) -> dict[str, EquitySnapshot]:
        self.calls.append(list(symbols))
        wanted = {s.strip().upper() for s in symbols}
        return {SYMBOL: _equity_snapshot()} if SYMBOL in wanted else {}


class FakeState:
    def __init__(self) -> None:
        self.settings = Settings()
        self.feed = FakeFeed()
        self.loader = None
        self.redis = FakeRedis()
        self.market_data_cache = None
        self.fixed_income_reference_provider = None
        self.event_contracts = None

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass


def _make_client(state: FakeState) -> TestClient:
    from src.webapp.app import create_app

    app = create_app(settings=state.settings, state=state)  # type: ignore[arg-type]
    return TestClient(app)


def test_all_sections_return_200_with_every_field_set() -> None:
    state = FakeState()
    client = _make_client(state)

    with client:
        response = client.get(ENDPOINT)

    assert response.status_code == 200, response.text
    data = response.json()

    assert data["symbol"] == SYMBOL
    assert data["exchange"] == "SMART"
    assert data["currency"] == "USD"

    assert data["snapshot"]["symbol"] == SYMBOL
    assert data["snapshot"]["last"] == 185.5

    assert len(data["ticks"]) == 1
    assert data["ticks"][0]["price"] == 185.5

    assert data["depth"]["symbol"] == SYMBOL
    assert data["depth"]["bids"][0]["price"] == 185.4

    assert data["trading_schedule"][0]["refDate"] == "20260105"

    assert set(data["sections"]) == set(ALL_SECTION_NAMES)
    for name in ALL_SECTION_NAMES:
        assert data["sections"][name]["status"] == "ok", name
        assert data["sections"][name]["error"] is None

    assert datetime.fromisoformat(data["requested_at"]) is not None

    # Each section built its request from the path symbol.
    assert state.redis.calls == [[SYMBOL]]
    assert state.feed.tick_calls == [(SYMBOL, "STK", "SMART", 10)]
    depth_spec = state.feed.depth_specs[0]
    assert (depth_spec.symbol, depth_spec.exchange, depth_spec.currency) == (SYMBOL, "SMART", "USD")
    assert depth_spec.asset_class is AssetClass.EQUITY
    schedule_request = state.feed.schedule_requests[0]
    assert schedule_request.symbol == SYMBOL
    assert schedule_request.what_to_show == "SCHEDULE"
    assert schedule_request.duration == "7 D"


def test_section_whitelist_runs_only_requested_sections() -> None:
    state = FakeState()
    client = _make_client(state)

    with client:
        response = client.get(ENDPOINT, params={"sections": "snapshot,ticks"})

    assert response.status_code == 200, response.text
    data = response.json()

    assert data["snapshot"]["symbol"] == SYMBOL
    assert data["ticks"][0]["price"] == 185.5

    assert data["depth"] is None
    assert data["trading_schedule"] is None

    assert data["sections"]["snapshot"]["status"] == "ok"
    assert data["sections"]["ticks"]["status"] == "ok"
    assert data["sections"]["depth"]["status"] == "skipped"
    assert data["sections"]["trading_schedule"]["status"] == "skipped"

    assert state.redis.calls == [[SYMBOL]]
    assert state.feed.calls == ["ticks"]
    assert state.feed.depth_specs == []
    assert state.feed.schedule_requests == []


def test_unknown_section_name_returns_422() -> None:
    state = FakeState()
    client = _make_client(state)

    with client:
        response = client.get(ENDPOINT, params={"sections": "snapshot,bogus"})

    assert response.status_code == 422, response.text
    assert "bogus" in response.json()["detail"]
    assert state.redis.calls == []
    assert state.feed.calls == []


def test_failing_section_is_isolated() -> None:
    state = FakeState()
    state.feed.fail_sections = {"depth"}
    client = _make_client(state)

    with client:
        response = client.get(ENDPOINT)

    assert response.status_code == 200, response.text
    data = response.json()

    assert data["depth"] is None
    assert data["sections"]["depth"]["status"] == "error"
    assert "depth unavailable" in data["sections"]["depth"]["error"]

    for name in ("snapshot", "ticks", "trading_schedule"):
        assert data["sections"][name]["status"] == "ok", name

    assert data["snapshot"]["symbol"] == SYMBOL
    assert data["ticks"][0]["price"] == 185.5
    assert data["trading_schedule"][0]["refDate"] == "20260105"
