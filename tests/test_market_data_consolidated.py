"""Consolidated market-data symbol endpoint behavior.

Covers the full-section response, the section whitelist, unknown-section
rejection, and per-section error isolation. Uses a fake app state so no
IBKR gateway or Redis instance is needed.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote

from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.contracts import ContractSpec
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
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


# ── Universal resolver + /universal endpoint ──────────────────────────────────


def _bar(symbol: str, asset_class: AssetClass, exchange: str = "SMART", currency: str = "USD") -> OHLCVBar:
    return OHLCVBar(
        symbol=symbol,
        timestamp=FIXED_TS,
        open=185.0,
        high=186.0,
        low=184.5,
        close=185.5,
        volume=100,
        asset_class=asset_class,
        exchange=exchange,
        currency=currency,
        bar_size="1 hour",
    )


def _hk_snapshot() -> EquitySnapshot:
    return EquitySnapshot(
        symbol="0700",
        exchange="SEHK",
        currency="HKD",
        timestamp=FIXED_TS,
        last=310.0,
        bid=309.8,
        ask=310.2,
        volume=2_000_000,
    )


class UniversalFakeFeed(FakeFeed):
    """FakeFeed extension that also captures universal OHLCV requests."""

    def __init__(self) -> None:
        super().__init__()
        self.ohlcv_requests: list[OHLCVRequest] = []

    async def load_historical_ohlcv(self, request: OHLCVRequest, *, max_chunks: int = 8) -> list[OHLCVBar]:
        self.calls.append("ohlcv")
        if "ohlcv" in self.fail_sections:
            raise RuntimeError("ohlcv unavailable")
        self.ohlcv_requests.append(request)
        return [_bar(request.symbol, request.asset_class, request.exchange, request.currency)]


class ExtraSnapshotRedis(FakeRedis):
    """Redis stub that serves one extra cached snapshot beyond AAPL."""

    def __init__(self, extra: dict[str, EquitySnapshot]) -> None:
        super().__init__()
        self._extra = extra

    async def get_latest_equity_snapshots(self, symbols: list[str]) -> dict[str, EquitySnapshot]:
        self.calls.append(list(symbols))
        wanted = {s.strip().upper() for s in symbols}
        served: dict[str, EquitySnapshot] = {}
        if SYMBOL in wanted:
            served[SYMBOL] = _equity_snapshot()
        for key, value in self._extra.items():
            if key in wanted:
                served[key] = value
        return served


UNIVERSAL_BASE = "/api/v1/market-data/consolidated"


def _universal_state() -> tuple[FakeState, UniversalFakeFeed]:
    state = FakeState()
    feed = UniversalFakeFeed()
    state.feed = feed
    return state, feed


def test_resolve_endpoint_returns_dated_future_fields() -> None:
    state = FakeState()
    client = _make_client(state)

    with client:
        response = client.get(f"{UNIVERSAL_BASE}/resolve/ESZ26")

    assert response.status_code == 200, response.text
    data = response.json()

    assert data["input"] == "ESZ26"
    assert data["symbol"] == "ES"
    assert data["asset_class"] == "future"
    assert data["exchange"] == "CME"
    assert data["currency"] == "USD"
    assert data["local_symbol"] == "ESZ26"
    assert data["contract_month"] == "202612"
    assert data["continuous"] is False
    assert data["matched_rule"] == "future_pattern"


def test_resolve_endpoint_bad_fx_symbol_returns_422() -> None:
    state = FakeState()
    client = _make_client(state)

    with client:
        response = client.get(f"{UNIVERSAL_BASE}/resolve/{quote('BADFX??')}", params={"asset_class": "fx"})

    assert response.status_code == 422, response.text
    assert "not a currency pair" in response.json()["detail"]


def test_universal_future_runs_all_sections_and_skips_snapshot() -> None:
    state, feed = _universal_state()
    client = _make_client(state)

    with client:
        response = client.get(f"{UNIVERSAL_BASE}/universal/ESZ26")

    assert response.status_code == 200, response.text
    data = response.json()

    # Resolved symbol echo.
    assert data["symbol"] == "ES"
    assert data["asset_class"] == "future"
    assert data["exchange"] == "CME"
    assert data["currency"] == "USD"
    assert data["local_symbol"] == "ESZ26"
    assert data["contract_month"] == "202612"
    assert data["continuous"] is False
    assert data["matched_rule"] == "future_pattern"

    for name in ("ohlcv", "ticks", "depth", "trading_schedule"):
        assert data["sections"][name]["status"] == "ok", name

    # Snapshot is equity-only, so it must be skipped with a note.
    assert data["snapshot"] is None
    assert data["sections"]["snapshot"]["status"] == "skipped"
    assert data["sections"]["snapshot"]["error"]

    # Section payloads.
    assert data["ohlcv"][0]["symbol"] == "ES"
    assert data["ticks"][0]["price"] == 185.5
    assert data["depth"]["symbol"] == SYMBOL
    assert data["trading_schedule"][0]["refDate"] == "20260105"

    # OHLCV request conventions for futures.
    ohlcv_request = feed.ohlcv_requests[0]
    assert ohlcv_request.symbol == "ES"
    assert ohlcv_request.asset_class is AssetClass.FUTURE
    assert ohlcv_request.duration == "2 D"
    assert ohlcv_request.bar_size == "1 hour"
    assert ohlcv_request.what_to_show == "TRADES"
    assert ohlcv_request.use_rth is False
    assert ohlcv_request.local_symbol == "ESZ26"
    assert ohlcv_request.last_trade_date_or_contract_month == "202612"
    assert ohlcv_request.continuous is False

    # Ticks map to the FUT sec type on the resolved exchange.
    assert feed.tick_calls == [("ES", "FUT", "CME", 10)]

    # Depth runs for futures with the resolved contract fields.
    depth_spec = feed.depth_specs[0]
    assert depth_spec.asset_class is AssetClass.FUTURE
    assert depth_spec.symbol == "ES"
    assert depth_spec.exchange == "CME"
    assert depth_spec.currency == "USD"
    assert depth_spec.local_symbol == "ESZ26"

    # Schedule request keeps the futures contract identity.
    schedule_request = feed.schedule_requests[0]
    assert schedule_request.asset_class is AssetClass.FUTURE
    assert schedule_request.what_to_show == "SCHEDULE"
    assert schedule_request.duration == "7 D"
    assert schedule_request.local_symbol == "ESZ26"

    # Redis is never touched for non-equity symbols.
    assert state.redis.calls == []


def test_universal_equity_hk_runs_snapshot_and_ohlcv() -> None:
    state, feed = _universal_state()
    state.redis = ExtraSnapshotRedis({"0700": _hk_snapshot()})
    client = _make_client(state)

    with client:
        response = client.get(f"{UNIVERSAL_BASE}/universal/0700.HK")

    assert response.status_code == 200, response.text
    data = response.json()

    # Resolved symbol echo.
    assert data["symbol"] == "0700"
    assert data["asset_class"] == "equity"
    assert data["exchange"] == "SEHK"
    assert data["currency"] == "HKD"
    assert data["local_symbol"] is None
    assert data["contract_month"] is None
    assert data["continuous"] is False
    assert data["matched_rule"] == "equity_fallback"

    # Equity: snapshot and ohlcv sections run.
    assert data["sections"]["snapshot"]["status"] == "ok"
    assert data["sections"]["ohlcv"]["status"] == "ok"
    assert data["snapshot"]["symbol"] == "0700"
    assert data["snapshot"]["last"] == 310.0
    assert data["ohlcv"][0]["symbol"] == "0700"

    assert state.redis.calls == [["0700"]]

    ohlcv_request = feed.ohlcv_requests[0]
    assert ohlcv_request.symbol == "0700"
    assert ohlcv_request.exchange == "SEHK"
    assert ohlcv_request.currency == "HKD"
    assert ohlcv_request.primary_exchange == "SEHK"
    assert ohlcv_request.what_to_show == "TRADES"
    assert ohlcv_request.use_rth is True

    assert feed.tick_calls == [("0700", "STK", "SEHK", 10)]


def test_universal_fx_skips_depth_and_requests_midpoint() -> None:
    state, feed = _universal_state()
    client = _make_client(state)

    with client:
        response = client.get(f"{UNIVERSAL_BASE}/universal/EURUSD")

    assert response.status_code == 200, response.text
    data = response.json()

    # Resolved symbol echo.
    assert data["symbol"] == "EURUSD"
    assert data["asset_class"] == "fx"
    assert data["exchange"] == "IDEALPRO"
    assert data["currency"] == "USD"
    assert data["matched_rule"] == "fx_pair"

    # FX has no aggregated DOM: depth is skipped with a note.
    assert data["depth"] is None
    assert data["sections"]["depth"]["status"] == "skipped"
    assert data["sections"]["depth"]["error"]
    assert "fx" in data["sections"]["depth"]["error"]
    assert feed.depth_specs == []

    # Snapshots are equity-only.
    assert data["sections"]["snapshot"]["status"] == "skipped"
    assert state.redis.calls == []

    # OHLCV uses the FX MIDPOINT convention.
    ohlcv_request = feed.ohlcv_requests[0]
    assert ohlcv_request.asset_class is AssetClass.FX
    assert ohlcv_request.what_to_show == "MIDPOINT"
    assert ohlcv_request.use_rth is False
    assert ohlcv_request.exchange == "IDEALPRO"

    # Ticks map to the CASH sec type.
    assert feed.tick_calls == [("EURUSD", "CASH", "IDEALPRO", 10)]


def test_universal_unknown_section_returns_422() -> None:
    state, feed = _universal_state()
    client = _make_client(state)

    with client:
        response = client.get(f"{UNIVERSAL_BASE}/universal/ESZ26", params={"sections": "ohlcv,bogus"})

    assert response.status_code == 422, response.text
    assert "bogus" in response.json()["detail"]
    assert feed.calls == []


def test_mcp_resolve_symbol_tool_resolves_and_wraps_errors() -> None:
    from src import mcp_server

    ctx = SimpleNamespace(request_context=SimpleNamespace(lifespan_context={"ibkr_state": SimpleNamespace()}))

    result = asyncio.run(mcp_server.resolve_symbol(ctx, "ESZ26"))
    assert result["symbol"] == "ES"
    assert result["asset_class"] == "future"
    assert result["contract_month"] == "202612"
    assert result["matched_rule"] == "future_pattern"

    failure = asyncio.run(mcp_server.resolve_symbol(ctx, "BADFX??", asset_class="fx"))
    assert "error" in failure
    assert "not a currency pair" in failure["error"]
