"""Portfolio risk cockpit: net Greeks, scenario P&L, stress episodes, hedges.

Layers on top of the account-level snapshot in ``business_portfolio.py``:

- Option positions resolve to IBKR model-Greek snapshots; position Greeks
  scale quote Greeks by position size and contract multiplier.
- Per-underlying aggregation: net delta shares, dollar delta, gamma per 1%
  spot move, vega per volatility point, theta per day, expiry buckets.
- Scenario grid: spot shock times volatility shock. Option P&L uses the
  delta-gamma-vega approximation::

      pnl = delta_shares * dS + 0.5 * gamma_position * dS**2 + vega_per_pt * dVol

  Linear positions scale market value by the spot percentage.
- Stress episodes apply the same math with preset shocks.
- Hedge suggestion: whole shares that flatten each underlying's net delta.

Conventions: vega is dollars per one volatility point (percent); gamma is
dollars per (dollar move squared) on position scale; theta is dollars per
calendar day.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.account import LivePositionDTO, PortfolioItemDTO
from src.feeds.options_models import (
    OptionAnalyticsRequest,
    days_to_expiry,
    to_option_contract_spec,
)
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/portfolio-risk", tags=["portfolio-risk"])

OPTION_SEC_TYPES = frozenset({"OPT", "FOP"})

DEFAULT_SPOT_SHOCKS_PCT = (-10.0, -5.0, -2.0, 2.0, 5.0, 10.0)
DEFAULT_VOL_SHOCKS_PTS = (-5.0, -3.0, 0.0, 3.0, 5.0)
DEFAULT_MULTIPLIER = 100.0

# Expiry buckets in calendar days.
FRONT_MAX_DAYS = 45
NEXT_MAX_DAYS = 135

# Deterministic stress presets. Every episode is a (spot_pct, vol_pts) pair.
STRESS_EPISODES: tuple[tuple[str, str, float, float], ...] = (
    ("equity_flash_crash", "Spot -8%, implied vol +6 pts", -8.0, 6.0),
    ("covid_crash", "Spot -12%, implied vol +10 pts", -12.0, 10.0),
    ("vol_spike_flat", "Spot flat, implied vol +12 pts", 0.0, 12.0),
    ("melt_up", "Spot +8%, implied vol -3 pts", 8.0, -3.0),
    ("rates_shock_2022", "Spot -15%, implied vol +8 pts", -15.0, 8.0),
)

HEDGE_MIN_SHARES = 100.0
HEDGE_MIN_NOTIONAL = 5000.0


# ---------------------------------------------------------------------------
# Request and response models
# ---------------------------------------------------------------------------


class CockpitRequest(BaseModel):
    """Cockpit request body."""

    model_config = ConfigDict(extra="forbid")

    account: str = Field(default="ALL", examples=["ALL", "U1234567"])
    spot_shocks_pct: list[float] = Field(default_factory=lambda: list(DEFAULT_SPOT_SHOCKS_PCT))
    vol_shocks_pts: list[float] = Field(default_factory=lambda: list(DEFAULT_VOL_SHOCKS_PTS))
    skip_greeks: bool = Field(default=False, description="Skip per-contract Greek snapshots; aggregate linear exposure only.")


class ExpiryBucketRisk(BaseModel):
    """Greeks summed over one expiry bucket."""

    model_config = ConfigDict(extra="forbid")

    bucket: str
    delta_shares: float = 0.0
    vega_per_pt: float = 0.0


class UnderlyingRisk(BaseModel):
    """All risk aggregated on one underlying."""

    model_config = ConfigDict(extra="forbid")

    underlying: str
    spot: float | None = None
    linear_position: float = 0.0
    linear_value: float = 0.0
    option_position_count: int = 0
    net_delta_shares: float = 0.0
    dollar_delta: float = 0.0
    gamma_per_1pct: float = 0.0
    vega_per_pt: float = 0.0
    theta_per_day: float = 0.0
    expiry_buckets: list[ExpiryBucketRisk] = Field(default_factory=list)


class ScenarioPnl(BaseModel):
    """Portfolio P&L under one (spot, vol) shock pair."""

    model_config = ConfigDict(extra="forbid")

    spot_shock_pct: float
    vol_shock_pts: float
    label: str = ""
    pnl: float
    by_underlying: dict[str, float] = Field(default_factory=dict)


class EpisodePnl(ScenarioPnl):
    """Scenario P&L plus the episode description."""

    description: str = ""


class HedgeSuggestion(BaseModel):
    """Whole shares that flatten one underlying's net delta."""

    model_config = ConfigDict(extra="forbid")

    underlying: str
    net_delta_shares: float
    shares_to_trade: int
    residual_delta_shares: float
    hedge_notional: float


class CockpitTotals(BaseModel):
    """Portfolio-wide sums."""

    model_config = ConfigDict(extra="forbid")

    net_dollar_delta: float = 0.0
    net_vega_per_pt: float = 0.0
    net_theta_per_day: float = 0.0
    net_gamma_per_1pct: float = 0.0
    gross_exposure: float = 0.0


class CockpitResponse(BaseModel):
    """Cockpit response body."""

    model_config = ConfigDict(extra="forbid")

    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    account: str = "ALL"
    underlyings: list[UnderlyingRisk] = Field(default_factory=list)
    totals: CockpitTotals = Field(default_factory=CockpitTotals)
    scenarios: list[ScenarioPnl] = Field(default_factory=list)
    stress_episodes: list[EpisodePnl] = Field(default_factory=list)
    hedges: list[HedgeSuggestion] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Pure math helpers
# ---------------------------------------------------------------------------


def expiry_bucket(days: int | None) -> str:
    """Classify expiry days into front / next / back buckets."""
    if days is None:
        return "unknown"
    if days <= FRONT_MAX_DAYS:
        return "front"
    if days <= NEXT_MAX_DAYS:
        return "next"
    return "back"


def _option_greeks_contribution(
    position: float,
    multiplier: float,
    greeks: Any,
) -> dict[str, Any]:
    """Scale quote Greeks to position size.

    Returns delta shares, gamma (position scale), vega per point, theta per
    day, and the underlying price reference from the snapshot.
    """
    delta = getattr(greeks, "delta", None)
    gamma = getattr(greeks, "gamma", None)
    vega = getattr(greeks, "vega", None)
    theta = getattr(greeks, "theta", None)
    scale = position * multiplier
    return {
        "delta_shares": (delta or 0.0) * scale,
        "gamma_position": (gamma or 0.0) * scale,
        "vega_per_pt": (vega or 0.0) * scale,
        "theta_per_day": (theta or 0.0) * scale,
        "underlying_price": getattr(greeks, "underlying_price", None),
    }


def scenario_pnl(
    linear_value: float,
    option_delta_shares: float,
    gamma_per_1pct: float,
    vega_per_pt: float,
    spot: float | None,
    spot_shock_pct: float,
    vol_shock_pts: float,
) -> float:
    """P&L under one shock pair.

    Linear part: market value scales with the spot percentage.
    Option part: option delta shares times dS, plus one half gamma times dS
    squared, plus vega per point times the vol shock. Linear delta lives
    only in the linear part; counting it in the delta term would double it.
    """
    pnl = linear_value * spot_shock_pct / 100.0
    if spot is None:
        return pnl
    d_s = spot * spot_shock_pct / 100.0
    gamma_position = gamma_per_1pct / (spot * 0.01) if spot > 0 else 0.0
    return pnl + option_delta_shares * d_s + 0.5 * gamma_position * d_s * d_s + vega_per_pt * vol_shock_pts


# ---------------------------------------------------------------------------
# Cockpit pipeline
# ---------------------------------------------------------------------------


def _linear_value(position: LivePositionDTO, items_by_con_id: dict[int, PortfolioItemDTO]) -> float:
    """Market value for a linear position; position times price as fallback."""
    item = items_by_con_id.get(position.con_id) if position.con_id is not None else None
    if item is not None and item.market_value is not None:
        return item.market_value
    if item is not None and item.market_price is not None:
        return item.market_price * position.position
    return 0.0


async def _resolve_option_greeks(
    state: IBKRRestAppState,
    position: LivePositionDTO,
    warnings: list[str],
) -> dict[str, Any] | None:
    """Load model Greeks for one option position; None on failure."""
    label = f"{position.symbol} {position.expiry} {position.strike}{position.right or ''}"
    spec = to_option_contract_spec(position, DEFAULT_MULTIPLIER)
    if spec is None:
        warnings.append(f"option identity incomplete, Greeks skipped: {label}")
        return None
    try:
        snapshot = await state.feed.load_option_analytics(OptionAnalyticsRequest(contract=spec))
        greeks = snapshot.model_greeks or snapshot.last_greeks or snapshot.bid_greeks or snapshot.ask_greeks
        if greeks is None:
            warnings.append(f"no Greek set returned for {label}")
            return None
        return _option_greeks_contribution(
            position.position, position.multiplier or DEFAULT_MULTIPLIER, greeks
        )
    except Exception as exc:
        warnings.append(f"Greeks unavailable for {label}: {exc}")
        return None


async def build_cockpit(payload: CockpitRequest, state: IBKRRestAppState) -> CockpitResponse:
    """Assemble the risk cockpit from positions and option Greek snapshots."""
    warnings: list[str] = []
    positions = await state.feed.load_live_positions()
    items = await state.feed.load_portfolio_items(payload.account)
    items_by_con_id = {item.con_id: item for item in items if item.con_id is not None}

    today = datetime.now(tz=timezone.utc).date()
    underlyings: dict[str, UnderlyingRisk] = {}
    gamma_position_sums: dict[str, float] = {}
    option_delta_sums: dict[str, float] = {}

    def _risk(symbol: str) -> UnderlyingRisk:
        key = symbol.upper()
        gamma_position_sums.setdefault(key, 0.0)
        option_delta_sums.setdefault(key, 0.0)
        return underlyings.setdefault(key, UnderlyingRisk(underlying=key))

    for position in positions:
        risk = _risk(position.underlying_symbol or position.symbol)
        if position.sec_type.upper() in OPTION_SEC_TYPES:
            risk.option_position_count += 1
            if payload.skip_greeks:
                continue
            contribution = await _resolve_option_greeks(state, position, warnings)
            if contribution is None:
                continue
            risk.net_delta_shares += contribution["delta_shares"]
            option_delta_sums[risk.underlying] += contribution["delta_shares"]
            spot_ref = contribution.get("underlying_price")
            if spot_ref:
                risk.spot = spot_ref
            gamma_position_sums[risk.underlying] += contribution["gamma_position"]
            risk.vega_per_pt += contribution["vega_per_pt"]
            risk.theta_per_day += contribution["theta_per_day"]
            days = days_to_expiry(position.expiry, today)
            bucket_name = expiry_bucket(days)
            for bucket in risk.expiry_buckets:
                if bucket.bucket == bucket_name:
                    bucket.delta_shares += contribution["delta_shares"]
                    bucket.vega_per_pt += contribution["vega_per_pt"]
                    break
            else:
                risk.expiry_buckets.append(
                    ExpiryBucketRisk(
                        bucket=bucket_name,
                        delta_shares=contribution["delta_shares"],
                        vega_per_pt=contribution["vega_per_pt"],
                    )
                )
        else:
            risk.linear_position += position.position
            risk.linear_value += _linear_value(position, items_by_con_id)
            risk.net_delta_shares += position.position  # linear delta is 1:1
            item = items_by_con_id.get(position.con_id) if position.con_id is not None else None
            if item is not None and item.market_price is not None and item.market_price > 0:
                risk.spot = item.market_price

    for risk in underlyings.values():
        risk.dollar_delta = risk.net_delta_shares * risk.spot if risk.spot is not None else 0.0
        if risk.spot is not None and risk.spot > 0:
            risk.gamma_per_1pct = gamma_position_sums.get(risk.underlying, 0.0) * risk.spot * 0.01

    ordered = sorted(underlyings.values(), key=lambda risk: risk.underlying)
    totals = CockpitTotals(
        net_dollar_delta=sum(r.dollar_delta for r in ordered),
        net_vega_per_pt=sum(r.vega_per_pt for r in ordered),
        net_theta_per_day=sum(r.theta_per_day for r in ordered),
        net_gamma_per_1pct=sum(r.gamma_per_1pct for r in ordered),
        gross_exposure=sum(abs(r.linear_value) for r in ordered),
    )

    scenarios: list[ScenarioPnl] = [ScenarioPnl(spot_shock_pct=0.0, vol_shock_pts=0.0, label="base", pnl=0.0)]
    for spot_shock in payload.spot_shocks_pct:
        for vol_shock in payload.vol_shocks_pts:
            if spot_shock == 0.0 and vol_shock == 0.0:
                continue
            by_underlying = {
                risk.underlying: scenario_pnl(
                    risk.linear_value,
                    option_delta_sums.get(risk.underlying, 0.0),
                    risk.gamma_per_1pct,
                    risk.vega_per_pt,
                    risk.spot,
                    spot_shock,
                    vol_shock,
                )
                for risk in ordered
            }
            scenarios.append(
                ScenarioPnl(
                    spot_shock_pct=spot_shock,
                    vol_shock_pts=vol_shock,
                    label=f"spot {spot_shock:+g}% / vol {vol_shock:+g}pt",
                    pnl=sum(by_underlying.values()),
                    by_underlying=by_underlying,
                )
            )

    episodes: list[EpisodePnl] = []
    for name, description, spot_shock, vol_shock in STRESS_EPISODES:
        by_underlying = {
            risk.underlying: scenario_pnl(
                risk.linear_value,
                option_delta_sums.get(risk.underlying, 0.0),
                risk.gamma_per_1pct,
                risk.vega_per_pt,
                risk.spot,
                spot_shock,
                vol_shock,
            )
            for risk in ordered
        }
        episodes.append(
            EpisodePnl(
                spot_shock_pct=spot_shock,
                vol_shock_pts=vol_shock,
                label=name,
                description=description,
                pnl=sum(by_underlying.values()),
                by_underlying=by_underlying,
            )
        )

    hedges: list[HedgeSuggestion] = []
    for risk in ordered:
        shares = int(round(-risk.net_delta_shares))
        residual = risk.net_delta_shares + shares
        notional = abs(shares) * (risk.spot or 0.0)
        if abs(risk.net_delta_shares) >= HEDGE_MIN_SHARES or notional >= HEDGE_MIN_NOTIONAL:
            hedges.append(
                HedgeSuggestion(
                    underlying=risk.underlying,
                    net_delta_shares=risk.net_delta_shares,
                    shares_to_trade=shares,
                    residual_delta_shares=residual,
                    hedge_notional=notional,
                )
            )

    if not positions:
        warnings.append("no open positions")

    return CockpitResponse(
        account=payload.account,
        underlyings=ordered,
        totals=totals,
        scenarios=scenarios,
        stress_episodes=episodes,
        hedges=hedges,
        warnings=warnings,
    )


@router.post("/cockpit", response_model=CockpitResponse)
async def get_risk_cockpit(
    payload: Annotated[CockpitRequest, Body(openapi_examples={"default": {"value": {}}})],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> CockpitResponse:
    """Net Greeks, scenario P&L grid, stress episodes, and hedge suggestions."""
    return await build_cockpit(payload, state)
