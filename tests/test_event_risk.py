"""Event risk tests: straddle math, IV rank gates, and router pipelines."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.account import LivePositionDTO
from src.feeds.contracts import OptionChain, OptionChainRequest
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.feeds.options_models import (
    OptionAnalyticsRequest,
    OptionAnalyticsSnapshot,
    OptionContractSpec,
    OptionGreekSet,
    OptionGreekSource,
)
from src.webapp.routers.event_risk import (
    EventSpec,
    iv_rank,
    nearest_event,
    straddle_expected_move,
)

UNDERLYING_PATH = "/api/v1/event-risk/underlying"
EXPOSURE_PATH = "/api/v1/event-risk/exposure"


# ---------------------------------------------------------------------------
# Closed-form math gates
# ---------------------------------------------------------------------------


def test_straddle_expected_move_closed_form() -> None:
    move = straddle_expected_move(call_price=5.2, put_price=4.8, underlying_price=200.0, multiplier=100.0)
    assert move["straddle_price"] == pytest.approx(1_000.0)
    assert move["expected_move_pct"] == pytest.approx(5.0)
    assert move["expected_move_approx"] == pytest.approx(800.0)  # 0.8 rule


def test_straddle_expected_move_without_spot() -> None:
    move = straddle_expected_move(call_price=5.2, put_price=4.8, underlying_price=None, multiplier=100.0)
    assert move["expected_move_pct"] is None
    assert move["expected_move_approx"] == pytest.approx(800.0)


def test_iv_rank_closed_form() -> None:
    history = [10.0, 20.0, 30.0, 40.0]
    assert iv_rank(25.0, history)["iv_rank_pct"] == pytest.approx(50.0)
    assert iv_rank(10.0, history)["iv_rank_pct"] == pytest.approx(0.0)
    assert iv_rank(40.0, history)["iv_rank_pct"] == pytest.approx(100.0)
    assert iv_rank(25.0, history)["mean_iv"] == pytest.approx(25.0)
    assert iv_rank(25.0, [])["iv_rank_pct"] is None
    assert iv_rank(25.0, [30.0, 30.0])["iv_rank_pct"] is None  # zero span


def test_nearest_event_matches_label_and_skips_past() -> None:
    today = date(2026, 10, 16)
    events = [
        EventSpec(date=today - timedelta(days=1), label="AAPL earnings yesterday"),
        EventSpec(date=today + timedelta(days=5), label="AAPL earnings"),
        EventSpec(date=today + timedelta(days=2), label="MSFT earnings"),
    ]
    aapl = nearest_event(events, "AAPL", today)
    assert aapl is not None and aapl.label == "AAPL earnings"
    assert nearest_event(events, "TSLA", today) is None


# ---------------------------------------------------------------------------
# Fake feed and state
# ---------------------------------------------------------------------------


def _expiry(days: int) -> str:
    return (datetime.now(tz=timezone.utc) + timedelta(days=days)).strftime("%Y%m%d")


def _chain() -> OptionChain:
    return OptionChain(
        underlying_symbol="AAPL",
        underlying_asset_class=AssetClass.EQUITY,
        underlying_con_id=100,
        exchange="SMART",
        trading_class="AAPL",
        multiplier="100",
        expirations=(_expiry(3), _expiry(30), _expiry(90)),
        strikes=(190.0, 200.0, 210.0),
    )


def _snapshot(contract: OptionContractSpec, **prices: Any) -> OptionAnalyticsSnapshot:
    def greeks(option_price: float | None) -> OptionGreekSet | None:
        if option_price is None:
            return None
        return OptionGreekSet(source=OptionGreekSource.MODEL, option_price=option_price)

    iv = prices.get("iv", 0.25)
    return OptionAnalyticsSnapshot(
        contract=contract,
        model_greeks=OptionGreekSet(
            source=OptionGreekSource.MODEL,
            implied_vol=iv,
            vega=prices.get("vega", 0.2),
            underlying_price=prices.get("underlying_price", 205.0),
        ),
        bid_greeks=greeks(prices.get("bid")),
        ask_greeks=greeks(prices.get("ask")),
        implied_volatility=iv,
    )


class FakeFeed:
    """Chain, analytics, IV history, and positions double."""

    def __init__(self) -> None:
        self.analytics_calls: list[tuple[str, float, str]] = []
        self.iv_history_requests: list[OHLCVRequest] = []

    async def load_option_chains(self, request: OptionChainRequest) -> list[OptionChain]:
        return [_chain()]

    async def load_option_analytics(self, request: OptionAnalyticsRequest) -> OptionAnalyticsSnapshot:
        contract = request.contract
        self.analytics_calls.append((contract.expiry, contract.strike, contract.right.value))
        # ATM is 200 for a 205 reference; quote the straddle legs there.
        if contract.strike == 200.0 and contract.expiry == _expiry(30):
            if contract.right.value == "C":
                return _snapshot(contract, bid=6.1, ask=6.3, iv=0.28)
            return _snapshot(contract, bid=5.7, ask=5.9, iv=0.26)
        if contract.strike == 200.0 and contract.expiry == _expiry(90):
            return _snapshot(contract, iv=0.22)  # both legs share the same IV

        # Probe contract: mid-strike snapshot provides the underlying price.
        return _snapshot(contract, iv=0.25)

    async def load_historical_ohlcv(self, request: OHLCVRequest, **kwargs: Any) -> list[OHLCVBar]:
        self.iv_history_requests.append(request)
        now = datetime.now(tz=timezone.utc)
        return [
            OHLCVBar(
                symbol="AAPL",
                timestamp=now,
                open=0,
                high=0,
                low=0,
                close=0,
                volume=0,
                asset_class=AssetClass.EQUITY,
                exchange="SMART",
                currency="USD",
                bar_size="1 day",
                vwap=vwap,
            )
            for vwap in (0.20, 0.30, 0.40)
        ]

    async def load_live_positions(self) -> list[LivePositionDTO]:
        return [
            LivePositionDTO(
                account="U1",
                con_id=2,
                symbol="AAPL",
                sec_type="OPT",
                exchange="SMART",
                currency="USD",
                position=2.0,
                average_cost=5.0,
                strike=200.0,
                expiry=_expiry(30),
                right="C",
                multiplier=100.0,
                underlying_symbol="AAPL",
            ),
            LivePositionDTO(
                account="U1",
                con_id=3,
                symbol="MSFT",
                sec_type="OPT",
                exchange="SMART",
                currency="USD",
                position=-1.0,
                average_cost=4.0,
                strike=400.0,
                expiry=_expiry(20),
                right="P",
                multiplier=100.0,
                underlying_symbol="MSFT",
            ),
        ]


class FakeState:
    def __init__(self) -> None:
        self.settings = Settings()
        self.feed = FakeFeed()
        self.loader = None
        self.redis = None
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


# ---------------------------------------------------------------------------
# Router tests
# ---------------------------------------------------------------------------


def test_underlying_event_risk_pipeline() -> None:
    with _make_client(FakeState()) as client:
        today = date.today()
        response = client.post(
            UNDERLYING_PATH,
            json={
                "symbol": "AAPL",
                "max_expiries": 2,
                "events": [{"date": (today + timedelta(days=6)).isoformat(), "label": "AAPL earnings"}],
            },
        )
        assert response.status_code == 200
        body = response.json()

    # 3-day expiry is below min_days_to_expiry=7; the 30-day expiry is used.
    assert body["selected_expiry"] == _expiry(30)
    assert body["days_to_expiry"] == pytest.approx(30, abs=1)
    assert body["atm_strike"] == 200.0
    assert body["underlying_price"] == pytest.approx(205.0)

    # Straddle mid = (6.2 + 5.8) * 100 = 1200; approx move = 960.
    assert body["straddle_price"] == pytest.approx(1_200.0)
    assert body["expected_move_pct"] == pytest.approx(1_200.0 / 205.0)
    assert body["expected_move_approx"] == pytest.approx(960.0)

    # ATM IV is the average of the call and put IV.
    assert body["atm_iv"] == pytest.approx(0.27)

    # Term structure covers the two eligible expiries.
    assert len(body["term_structure"]) == 2
    assert body["term_structure"][0]["atm_iv"] == pytest.approx(0.27)
    assert body["term_structure"][1]["atm_iv"] == pytest.approx(0.22)

    # IV history: current 0.27 against [0.20, 0.30, 0.40] -> rank 58.33%.
    assert body["iv_history"] is not None
    assert body["iv_history"]["observations"] == 3
    assert body["iv_history"]["iv_rank_pct"] == pytest.approx(7.0 / 20.0 * 100.0, rel=1e-6)

    assert body["nearest_event"]["label"] == "AAPL earnings"


def test_underlying_event_risk_chain_failure_is_warning() -> None:
    state = FakeState()

    async def broken_chain(request: OptionChainRequest) -> list[OptionChain]:
        raise RuntimeError("no market data line")

    state.feed.load_option_chains = broken_chain  # type: ignore[method-assign]
    with _make_client(state) as client:
        response = client.post(UNDERLYING_PATH, json={"symbol": "AAPL"})
        assert response.status_code == 200
        body = response.json()
        assert body["selected_expiry"] is None
        assert any("option chain unavailable" in warning for warning in body["warnings"])


def test_event_exposure_pipeline() -> None:
    with _make_client(FakeState()) as client:
        today = date.today()
        response = client.post(
            EXPOSURE_PATH,
            json={
                "horizon_days": 10,
                "assumed_iv_crush_pts": 8.0,
                "events": [
                    {"date": (today + timedelta(days=5)).isoformat(), "label": "AAPL earnings"},
                    {"date": (today + timedelta(days=20)).isoformat(), "label": "MSFT earnings"},
                ],
            },
        )
        assert response.status_code == 200
        body = response.json()

    # AAPL is inside the horizon; MSFT at 20 days is not.
    assert [item["underlying"] for item in body["exposed"]] == ["AAPL"]
    exposed = body["exposed"][0]
    assert exposed["option_position_count"] == 1
    # Vega: 0.2 * 2 contracts * 100 multiplier = 40 per pt.
    assert exposed["net_vega_per_pt"] == pytest.approx(40.0)
    assert exposed["vega_at_risk"] == pytest.approx(320.0)  # 40 * 8 pts.
    assert body["total_vega_at_risk"] == pytest.approx(320.0)
    assert body["no_event_data_symbols"] == ["MSFT"]


def test_event_exposure_no_events() -> None:
    with _make_client(FakeState()) as client:
        response = client.post(EXPOSURE_PATH, json={})
        assert response.status_code == 200
        body = response.json()
        assert body["exposed"] == []
        assert set(body["no_event_data_symbols"]) == {"AAPL", "MSFT"}
        assert any("no events matched" in warning for warning in body["warnings"])
