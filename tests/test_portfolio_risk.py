"""Portfolio risk cockpit tests: closed-form math gates plus router pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.account import LivePositionDTO
from src.feeds.contracts import OptionChainRequest
from src.feeds.options_models import (
    OptionAnalyticsRequest,
    OptionAnalyticsSnapshot,
    OptionContractSpec,
    OptionGreekSet,
    OptionGreekSource,
)
from src.webapp.routers.portfolio_risk import expiry_bucket, scenario_pnl

COCKPIT_PATH = "/api/v1/portfolio-risk/cockpit"


# ---------------------------------------------------------------------------
# Closed-form math gates
# ---------------------------------------------------------------------------


def test_expiry_bucket_boundaries() -> None:
    assert expiry_bucket(0) == "front"
    assert expiry_bucket(45) == "front"
    assert expiry_bucket(46) == "next"
    assert expiry_bucket(135) == "next"
    assert expiry_bucket(136) == "back"
    assert expiry_bucket(None) == "unknown"


def test_scenario_pnl_pure_linear_exact() -> None:
    # Linear delta lives only in the linear term: option delta must stay 0.
    assert scenario_pnl(50_000.0, 0.0, 0.0, 0.0, 100.0, 10.0, 5.0) == pytest.approx(5_000.0)
    assert scenario_pnl(50_000.0, 0.0, 0.0, 0.0, 100.0, -2.0, 0.0) == pytest.approx(-1_000.0)


def test_scenario_pnl_delta_gamma_vega_closed_form() -> None:
    # Option delta 500 shares, gamma per 1% = 2.0 dollars, vega 30 $/pt, spot 100.
    # Shock +5% spot, +3 vol pts: dS = 5.
    expected = 500.0 * 5.0 + 0.5 * 2.0 * 25.0 + 30.0 * 3.0
    assert scenario_pnl(0.0, 500.0, 2.0, 30.0, 100.0, 5.0, 3.0) == pytest.approx(expected)

    # Shock -10% spot, vol flat: dS = -10; gamma term stays positive.
    expected = 500.0 * -10.0 + 0.5 * 2.0 * 100.0
    assert scenario_pnl(0.0, 500.0, 2.0, 30.0, 100.0, -10.0, 0.0) == pytest.approx(expected)


def test_scenario_pnl_linear_plus_option_no_double_count() -> None:
    # Stock 100 shares at 100 (value 10,000) plus option delta 100 shares.
    # The stock counts once (linear term); option delta counts once.
    assert scenario_pnl(10_000.0, 100.0, 0.0, 0.0, 100.0, 10.0, 0.0) == pytest.approx(2_000.0)


# ---------------------------------------------------------------------------
# Router pipeline with a fake feed
# ---------------------------------------------------------------------------


def _expiry(days: int) -> str:
    return (datetime.now(tz=timezone.utc) + timedelta(days=days)).strftime("%Y%m%d")


def _position(
    con_id: int,
    symbol: str,
    sec_type: str,
    position: float,
    **kwargs: Any,
) -> LivePositionDTO:
    return LivePositionDTO(
        account="U1",
        con_id=con_id,
        symbol=symbol,
        sec_type=sec_type,
        exchange="SMART",
        currency="USD",
        position=position,
        average_cost=100.0,
        **kwargs,
    )


def _snapshot(contract: OptionContractSpec, greeks: OptionGreekSet) -> OptionAnalyticsSnapshot:
    return OptionAnalyticsSnapshot(contract=contract, model_greeks=greeks)


class FakeFeed:
    """Feed double: one stock, one option, deterministic Greeks."""

    def __init__(self) -> None:
        self.analytics_requests: list[OptionAnalyticsRequest] = []

    async def load_live_positions(self) -> list[LivePositionDTO]:
        return [
            _position(1, "AAPL", "STK", 100.0),
            _position(
                2,
                "AAPL",
                "OPT",
                2.0,
                strike=105.0,
                expiry=_expiry(30),
                right="C",
                multiplier=100.0,
                underlying_symbol="AAPL",
                local_symbol="AAPL  260115C00105000",
            ),
        ]

    async def load_portfolio_items(self, account: str = "") -> list[Any]:
        from src.feeds.account import PortfolioItemDTO

        return [
            PortfolioItemDTO(
                account="U1",
                con_id=1,
                symbol="AAPL",
                sec_type="STK",
                position=100.0,
                market_price=100.0,
                market_value=10_000.0,
            )
        ]

    async def load_option_analytics(self, request: OptionAnalyticsRequest) -> OptionAnalyticsSnapshot:
        self.analytics_requests.append(request)
        return _snapshot(
            request.contract,
            OptionGreekSet(
                source=OptionGreekSource.MODEL,
                implied_vol=0.25,
                delta=0.5,
                gamma=0.01,
                theta=-0.05,
                vega=0.2,
                underlying_price=100.0,
            ),
        )


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


def test_cockpit_pipeline_stock_plus_option() -> None:
    with _make_client(FakeState()) as client:
        response = client.post(COCKPIT_PATH, json={})
        assert response.status_code == 200
        body = response.json()

    aapl = body["underlyings"][0]
    assert aapl["underlying"] == "AAPL"
    assert aapl["linear_position"] == 100.0
    assert aapl["linear_value"] == pytest.approx(10_000.0)
    assert aapl["option_position_count"] == 1
    # Option delta shares: 0.5 * 100 * 2 = 100; plus 100 stock = 200.
    assert aapl["net_delta_shares"] == pytest.approx(200.0)
    assert aapl["dollar_delta"] == pytest.approx(20_000.0)
    assert aapl["vega_per_pt"] == pytest.approx(40.0)
    assert aapl["theta_per_day"] == pytest.approx(-10.0)
    # Expiry 30 days out lands in the front bucket.
    assert aapl["expiry_buckets"][0]["bucket"] == "front"

    # Hedge suggestion: 200 net delta shares >= 100 minimum.
    hedge = body["hedges"][0]
    assert hedge["underlying"] == "AAPL"
    assert hedge["shares_to_trade"] == -200
    assert abs(hedge["residual_delta_shares"]) < 1.0

    # Base scenario is zero; the +10%/+5pt cell matches the formula.
    base = next(s for s in body["scenarios"] if s["label"] == "base")
    assert base["pnl"] == 0.0
    cell = next(
        s for s in body["scenarios"] if s["spot_shock_pct"] == 10.0 and s["vol_shock_pts"] == 5.0
    )
    # Linear 10,000 * 10% = 1,000; option delta 100 * 10 = 1,000; gamma 0.5 * 2.0 * 100 = 100; vega 40 * 5 = 200.
    # The stock delta appears only in the linear term (no double count).
    assert cell["pnl"] == pytest.approx(2_300.0)

    assert len(body["stress_episodes"]) == 5
    assert body["totals"]["net_vega_per_pt"] == pytest.approx(40.0)


def test_cockpit_skip_greeks_and_missing_identity() -> None:
    state = FakeState()

    async def broken_positions() -> list[LivePositionDTO]:
        # Option without strike/expiry: identity incomplete.
        return [_position(9, "MSFT", "OPT", 1.0)]

    state.feed.load_live_positions = broken_positions  # type: ignore[method-assign]
    with _make_client(state) as client:
        response = client.post(COCKPIT_PATH, json={})
        assert response.status_code == 200
        body = response.json()
        assert body["underlyings"][0]["option_position_count"] == 1
        assert body["underlyings"][0]["net_delta_shares"] == 0.0
        assert any("Greeks skipped" in warning for warning in body["warnings"])


def test_cockpit_skip_greeks_flag_makes_no_analytics_calls() -> None:
    state = FakeState()
    with _make_client(state) as client:
        response = client.post(COCKPIT_PATH, json={"skip_greeks": True})
        assert response.status_code == 200
        body = response.json()
        assert state.feed.analytics_requests == []
        assert body["underlyings"][0]["vega_per_pt"] == 0.0


def test_cockpit_empty_positions() -> None:
    state = FakeState()

    async def no_positions() -> list[LivePositionDTO]:
        return []

    state.feed.load_live_positions = no_positions  # type: ignore[method-assign]
    with _make_client(state) as client:
        response = client.post(COCKPIT_PATH, json={})
        assert response.status_code == 200
        body = response.json()
        assert body["underlyings"] == []
        assert "no open positions" in body["warnings"]


def test_cockpit_handles_greek_snapshot_failure() -> None:
    state = FakeState()

    async def failing_analytics(request: OptionAnalyticsRequest) -> OptionAnalyticsSnapshot:
        raise RuntimeError("market data line unavailable")

    state.feed.load_option_analytics = failing_analytics  # type: ignore[method-assign]
    with _make_client(state) as client:
        response = client.post(COCKPIT_PATH, json={})
        assert response.status_code == 200
        body = response.json()
        # Stock keeps its 1:1 delta; the failed option adds none.
        assert body["underlyings"][0]["net_delta_shares"] == pytest.approx(100.0)
        assert any("Greeks unavailable" in warning for warning in body["warnings"])
