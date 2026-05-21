from __future__ import annotations

import asyncio
import math
from datetime import datetime, timezone

import pytest

from src.config import config_constant as constants
from src.config.settings import Settings
from src.feeds.models import AssetClass
from src.feeds.snapshotter import (
    EquitySnapshot,
    FXOptionSnapshot,
    SnapshotQuery,
    SnapshotResult,
    SnapshotWatchlist,
    fx_option_contract_key,
    ticker_to_fx_option_snapshot,
    ticker_to_snapshot,
)
from src.feeds.snapshot_converters import _safe_float
from src.feeds.options import OptionContractSpec, OptionRight
from src.transport.questdb_client import fx_option_snapshot_to_row, snapshot_to_row
from src.webapp.cache import AsyncTTLCache
import src.webapp.app as app_module
from src.webapp.app import create_app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeTicker:
    """Minimal ib_insync-compatible ticker for testing."""

    def __init__(self, **kwargs):
        self.contract = FakeContract()
        for k, v in kwargs.items():
            setattr(self, k, v)


class FakeContract:
    def __init__(self):
        self.conId = 12345
        self.symbol = "AAPL"
        self.exchange = "SMART"
        self.currency = "USD"


# ---------------------------------------------------------------------------
# EquitySnapshot model tests
# ---------------------------------------------------------------------------

class TestEquitySnapshot:
    def test_basic_snapshot_creation(self):
        snap = EquitySnapshot(
            symbol="AAPL",
            timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            last=150.0,
            bid=149.99,
            ask=150.01,
            volume=1_000_000,
        )
        assert snap.symbol == "AAPL"
        assert snap.last == 150.0
        assert snap.mid_price == pytest.approx(150.0)
        assert snap.spread == pytest.approx(0.02)
        assert snap.spread_bps == pytest.approx(1.33, rel=0.01)

    def test_derived_fields_no_spread(self):
        snap = EquitySnapshot(
            symbol="SPY",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            last=500.0,
            volume=5_000_000,
        )
        assert snap.mid_price is None
        assert snap.spread is None
        assert snap.spread_bps is None

    def test_normalizes_symbol_to_upper(self):
        snap = EquitySnapshot(
            symbol="aapl",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        assert snap.symbol == "AAPL"

    def test_normalizes_timestamp_to_utc(self):
        snap = EquitySnapshot(
            symbol="AAPL",
            timestamp="2026-01-01T08:00:00+08:00",
        )
        assert snap.timestamp.tzinfo is not None
        assert snap.timestamp.hour == 0  # UTC

    def test_redis_roundtrip(self):
        snap = EquitySnapshot(
            symbol="AAPL",
            timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            last=150.0,
            bid=149.99,
            ask=150.01,
            volume=1_000_000,
        )
        json_str = snap.to_redis_json()
        restored = EquitySnapshot.from_redis_json(json_str)
        assert restored.symbol == "AAPL"
        assert restored.last == 150.0
        assert restored.mid_price == pytest.approx(150.0)

    def test_redis_roundtrip_bytes(self):
        snap = EquitySnapshot(
            symbol="MSFT",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            last=400.0,
        )
        json_bytes = snap.to_redis_json().encode("utf-8")
        restored = EquitySnapshot.from_redis_json(json_bytes)
        assert restored.symbol == "MSFT"


class TestTickerToSnapshot:
    def test_converts_ticker_to_snapshot(self):
        ticker = FakeTicker(
            last=150.0,
            bid=149.99,
            ask=150.01,
            bidSize=100,
            askSize=200,
            volume=1_000_000,
            high=151.0,
            low=149.0,
            close=150.5,
            halted=False,
        )
        snap = ticker_to_snapshot(ticker, symbol="AAPL", exchange="SMART", currency="USD")
        assert snap.symbol == "AAPL"
        assert snap.last == 150.0
        assert snap.bid == 149.99
        assert snap.ask == 150.01
        assert snap.volume == 1_000_000
        assert snap.mid_price == pytest.approx(150.0)
        assert snap.spread == pytest.approx(0.02)

    def test_handles_none_fields(self):
        ticker = FakeTicker()
        snap = ticker_to_snapshot(ticker, symbol="SPY", exchange="SMART", currency="USD")
        assert snap.last is None
        assert snap.bid is None


class TestFXOptionSnapshot:
    def test_fx_option_snapshot_derives_spread_and_round_trips(self):
        snap = FXOptionSnapshot(
            symbol="eurusd",
            underlying_symbol="eur",
            expiry="20260619",
            strike=1.1,
            right="call",
            exchange="smart",
            currency="usd",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            bid=0.01,
            ask=0.012,
        )

        restored = FXOptionSnapshot.from_redis_json(snap.to_redis_json())

        assert snap.symbol == "EURUSD"
        assert snap.right == "C"
        assert snap.mid_price == pytest.approx(0.011)
        assert restored.contract_key == snap.contract_key

    def test_fx_option_contract_key_is_stable(self):
        assert fx_option_contract_key(symbol="eurusd", expiry="20260619", strike=1.10, right="call") == "EURUSD:20260619:1.1:C:SMART"

    def test_ticker_to_fx_option_snapshot_reads_price_and_greeks(self):
        ticker = FakeTicker(
            bid=0.01,
            ask=0.012,
            last=0.011,
            modelGreeks=type("Greeks", (), {"impliedVol": 0.1, "delta": 0.45, "gamma": 0.2, "theta": -0.01, "vega": 0.03})(),
            callOpenInterest=100,
        )
        contract = OptionContractSpec(
            underlying_symbol="EUR",
            expiry="20260619",
            strike=1.1,
            right=OptionRight.CALL,
            currency="USD",
        )

        snap = ticker_to_fx_option_snapshot(ticker, contract, symbol="EURUSD")

        assert snap.symbol == "EURUSD"
        assert snap.model_greeks is not None
        assert snap.model_greeks.delta == pytest.approx(0.45)
        assert snap.open_interest == pytest.approx(100)
        assert snap.volume is None

    def test_handles_nan_values(self):
        ticker = FakeTicker(last=float("nan"), bid=float("inf"))
        snap = ticker_to_snapshot(ticker, symbol="SPY", exchange="SMART", currency="USD")
        assert snap.last is None
        assert snap.bid is None


class TestSafeFloat:
    def test_returns_none_for_none(self):
        assert _safe_float(None) is None

    def test_returns_float_for_valid(self):
        assert _safe_float(150.0) == 150.0

    def test_returns_none_for_nan(self):
        assert _safe_float(float("nan")) is None

    def test_returns_none_for_inf(self):
        assert _safe_float(float("inf")) is None

    def test_converts_string(self):
        assert _safe_float("150.5") == 150.5


# ---------------------------------------------------------------------------
# SnapshotWatchlist tests
# ---------------------------------------------------------------------------

class TestSnapshotWatchlist:
    def test_basic_watchlist(self):
        wl = SnapshotWatchlist(
            name="us_tech",
            symbols=["AAPL", "MSFT", "GOOGL"],
        )
        assert wl.name == "us_tech"
        assert wl.symbols == ("AAPL", "MSFT", "GOOGL")
        assert wl.exchange == "SMART"
        assert wl.currency == "USD"

    def test_normalizes_symbols(self):
        wl = SnapshotWatchlist(
            name="test",
            symbols=["aapl", " msft ", "GOOGL"],
        )
        assert wl.symbols == ("AAPL", "MSFT", "GOOGL")

    def test_rejects_empty_symbols(self):
        with pytest.raises(Exception):
            SnapshotWatchlist(name="test", symbols=[])


# ---------------------------------------------------------------------------
# SnapshotQuery tests
# ---------------------------------------------------------------------------

class TestSnapshotQuery:
    def test_basic_query(self):
        q = SnapshotQuery(symbol="AAPL")
        assert q.symbol == "AAPL"
        assert q.limit == 1000

    def test_with_time_range(self):
        q = SnapshotQuery(
            symbol="AAPL",
            start="2026-01-01T00:00:00Z",
            end="2026-01-02T00:00:00Z",
            limit=500,
        )
        assert q.start.year == 2026
        assert q.end.year == 2026
        assert q.limit == 500

    def test_normalizes_symbol(self):
        q = SnapshotQuery(symbol="aapl")
        assert q.symbol == "AAPL"


# ---------------------------------------------------------------------------
# QuestDB snapshot row test
# ---------------------------------------------------------------------------

class TestSnapshotToRow:
    def test_row_fields_match_snapshot(self):
        snap = EquitySnapshot(
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            primary_exchange="NASDAQ",
            con_id=12345,
            timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            last=150.0,
            bid=149.99,
            ask=150.01,
            volume=1_000_000,
        )
        row = snapshot_to_row(snap)
        assert row[0] == "AAPL"  # symbol
        assert row[1] == "SMART"  # exchange
        assert row[2] == "USD"  # currency
        assert row[3] == "NASDAQ"  # primary_exchange
        assert row[4] == 12345  # con_id
        assert row[6] == 150.0  # last

    def test_fx_option_snapshot_row_shape(self):
        snap = FXOptionSnapshot(
            symbol="EURUSD",
            underlying_symbol="EUR",
            expiry="20260619",
            strike=1.1,
            right="C",
            exchange="SMART",
            currency="USD",
            timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            bid=0.01,
            ask=0.012,
        )

        row = fx_option_snapshot_to_row(snap)

        assert row[0] == "EURUSD"
        assert row[1] == "EUR"
        assert row[2] == "20260619"
        assert row[3] == 1.1
        assert row[4] == "C"
        assert row[11] == datetime(2026, 1, 1, 12, 0)


# ---------------------------------------------------------------------------
# SnapshotResult tests
# ---------------------------------------------------------------------------

class TestSnapshotResult:
    def test_result_construction(self):
        result = SnapshotResult(
            watchlist_name="us_tech",
            symbols_requested=3,
            symbols_captured=2,
            symbols_failed=1,
            failed_symbols=("INVALID",),
            duration_seconds=1.5,
        )
        assert result.watchlist_name == "us_tech"
        assert result.symbols_captured == 2
        assert result.failed_symbols == ("INVALID",)


# ---------------------------------------------------------------------------
# Web integration: verify snapshot endpoints are registered
# ---------------------------------------------------------------------------

class TestSnapshotEndpoints:
    def test_snapshot_endpoints_registered(self):
        s = Settings(
            ibkr_rest_app_name="test",
            ibkr_rest_market_data_ttl_seconds=60,
            ibkr_rest_market_data_cache_maxsize=16,
        )
        app = create_app(settings=s)
        paths = {route.path for route in app.routes}

        assert "/api/v1/snapshot/capture" in paths
        assert "/api/v1/snapshot/fx-options/capture" in paths
        assert "/api/v1/snapshot/fx-options/latest" in paths
        assert "/api/v1/snapshot/fx-options/query" in paths
        assert "/api/v1/snapshot/latest" in paths
        assert "/api/v1/snapshot/latest-all" in paths
        assert "/api/v1/snapshot/query" in paths
        assert "/api/v1/snapshot/watchlists" in paths
        assert "/api/v1/snapshot/watchlists/{name}" in paths
        assert "/api/v1/snapshot/watchlists/{name}/capture" in paths
