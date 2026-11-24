"""Event and earnings risk: expected move, IV term structure, vega at risk.

Two views:

- ``/event-risk/underlying`` — for one symbol, the options-implied expected
  move from the nearest acceptable expiry, the ATM IV term structure, and
  IV history stats (mean, rank) from the underlying's OPTION_IMPLIED_VOLATILITY
  series.
- ``/event-risk/exposure`` — for one account, option underlyings with a
  known event inside the horizon, with vega at risk under an assumed
  implied-vol crush.

Expected-move convention: the straddle price (ATM call mid plus ATM put
mid, times the multiplier) is the one-standard-deviation move the market
prices over the option life. The ``expected_move_approx`` field applies
the classic 0.8 rule of thumb used for event-window moves; both values
are reported so the user chooses the convention.

Event dates: IBKR WSH event data needs a research subscription and the
legacy endpoint can fail; events are therefore optional and user-supplied
in the request. Missing dates never fail a request — affected symbols
appear in ``no_event_data_symbols``.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, datetime, timezone
from typing import Annotated, Any
from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.account import LivePositionDTO
from src.feeds.contracts import OptionChain, OptionChainRequest
from src.feeds.models import AssetClass
from src.feeds.options_models import (
    OptionAnalyticsRequest,
    OptionAnalyticsSnapshot,
    OptionContractSpec,
    OptionRight,
    days_to_expiry,
    to_option_contract_spec,
)
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/event-risk", tags=["event-risk"])

OPTION_SEC_TYPES = frozenset({"OPT", "FOP"})
DEFAULT_MULTIPLIER = 100.0
STRADDLE_EVENT_FACTOR = 0.8
OPTION_ANALYTICS_WAIT_SECONDS = 2.0
# Term-structure fan-out is up to max_expiries x 2 legs inside one request;
# a shorter wait keeps the total inside the global request deadline.
TERM_STRUCTURE_WAIT_SECONDS = 1.0
VEGA_CONCURRENCY = 8


# ---------------------------------------------------------------------------
# Request and response models
# ---------------------------------------------------------------------------


class EventSpec(BaseModel):
    """One user-supplied event date."""

    model_config = ConfigDict(extra="forbid")

    date: Annotated[date, Field(description="Event date (ISO 8601).")]
    label: str = Field(default="event", min_length=1, description="Event description. Symbols are matched on word boundaries, e.g. 'AAPL earnings' matches AAPL but not 'TESLA earnings'.")


class UnderlyingEventRiskRequest(BaseModel):
    """Request body for the per-symbol event risk view."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, examples=["AAPL"], description="Underlying symbol, e.g. AAPL or SPX.")
    asset_class: str = Field(default="EQUITY", examples=["EQUITY", "INDEX"], description="Underlying asset class: EQUITY or INDEX (case-insensitive).")
    exchange: str = Field(default="SMART", description="Option exchange to load the chain from, e.g. SMART or CBOE.")
    currency: str = Field(default="USD", description="ISO 4217 currency, e.g. USD.")
    min_days_to_expiry: int = Field(default=7, ge=0, le=365, description="Skip expiries closer than this many calendar days.")
    max_expiries: int = Field(default=4, ge=1, le=12, description="Number of eligible expiries to include in the IV term structure.")
    events: list[EventSpec] = Field(default_factory=list, description="User-supplied event dates; IBKR WSH lookups are not attempted. Labels are matched to the symbol on word boundaries.")
    iv_history_days: int = Field(default=252, ge=30, le=5000, description="Days of OPTION_IMPLIED_VOLATILITY history for the IV rank.")


class TermStructurePoint(BaseModel):
    """ATM IV for one expiry."""

    model_config = ConfigDict(extra="forbid")

    expiry: str = Field(description="Option expiry (YYYYMMDD).")
    days_to_expiry: int = Field(description="Calendar days from today to expiry.")
    atm_iv: float | None = Field(default=None, description="Average of the ATM call and put implied volatilities; null when both legs are missing.")


class IVHistoryStats(BaseModel):
    """IV history statistics for the underlying."""

    model_config = ConfigDict(extra="forbid")

    observations: int = Field(description="Number of IV history bars used.")
    mean_iv: float | None = Field(default=None, description="Mean IV over the history window.")
    min_iv: float | None = Field(default=None, description="Minimum IV in the window.")
    max_iv: float | None = Field(default=None, description="Maximum IV in the window.")
    current_iv: float | None = Field(default=None, description="Current ATM IV used as the rank reference.")
    iv_rank_pct: float | None = Field(default=None, description="IV rank in percent (0-100) against the window; null when the window is flat or empty.")


class UnderlyingEventRiskResponse(BaseModel):
    """Per-symbol event risk response."""

    model_config = ConfigDict(extra="forbid")

    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the response was assembled.")
    symbol: str = Field(description="Uppercase underlying symbol.")
    selected_expiry: str | None = Field(default=None, description="Nearest eligible expiry (YYYYMMDD); null when none qualified.")
    days_to_expiry: int | None = Field(default=None, description="Calendar days to the selected expiry; null when none qualified.")
    atm_strike: float | None = Field(default=None, description="Strike closest to the underlying price.")
    underlying_price: float | None = Field(default=None, description="Reference underlying price; falls back to the chain strike midpoint when unavailable.")
    straddle_price: float | None = Field(default=None, description="ATM call mid plus ATM put mid, times the multiplier (dollars).")
    expected_move_abs: float | None = Field(default=None, description="One-standard-deviation move in price units; equals the straddle price.")
    expected_move_pct: float | None = Field(default=None, description="Straddle price divided by the underlying price (decimal fraction).")
    expected_move_approx: float | None = Field(default=None, description="Event-window approximation: straddle price times 0.8.")
    atm_iv: float | None = Field(default=None, description="Average ATM call and put IV at the selected expiry.")
    term_structure: list[TermStructurePoint] = Field(default_factory=list, description="ATM IV per eligible expiry, up to max_expiries points.")
    iv_history: IVHistoryStats | None = Field(default=None, description="IV history stats and rank; null when history is unavailable.")
    nearest_event: EventSpec | None = Field(default=None, description="Nearest future event whose label mentions the symbol on a word boundary; null when none matches.")
    warnings: list[str] = Field(default_factory=list, description="Degradation notes; a response with only warnings and null metrics means the chain or quotes were unavailable.")


class EventExposureRequest(BaseModel):
    """Request body for the account event-exposure view."""

    model_config = ConfigDict(extra="forbid")

    account: str = Field(default="ALL", examples=["ALL", "U1234567"], description="Account filter for positions; ALL keeps every account.")
    horizon_days: int = Field(default=10, ge=1, le=365, description="Only events within this many calendar days count as exposed.")
    assumed_iv_crush_pts: float = Field(default=8.0, gt=0.0, description="Assumed implied-volatility drop in points applied to net vega.")
    events: list[EventSpec] = Field(default_factory=list, description="User-supplied event dates; underlyings without a matching event are listed in no_event_data_symbols.")


class ExposedUnderlying(BaseModel):
    """One option underlying with an event inside the horizon."""

    model_config = ConfigDict(extra="forbid")

    underlying: str = Field(description="Uppercase underlying symbol.")
    option_position_count: int = Field(description="Number of option positions on the underlying.")
    net_vega_per_pt: float | None = Field(default=None, description="Net vega per volatility point across the underlying's options; null when vega could not be resolved.")
    days_to_event: int = Field(description="Calendar days until the matched event.")
    event_label: str = Field(description="Label of the matched event.")
    vega_at_risk: float | None = Field(default=None, description="Absolute net vega times assumed_iv_crush_pts; null when vega is unknown.")


class EventExposureResponse(BaseModel):
    """Account event-exposure response."""

    model_config = ConfigDict(extra="forbid")

    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="UTC time the response was assembled.")
    account: str = Field(default="ALL", description="Account filter echoed back.")
    horizon_days: int = Field(default=10, description="Event horizon in days, echoed back.")
    assumed_iv_crush_pts: float = Field(default=8.0, description="Assumed IV crush in points, echoed back.")
    exposed: list[ExposedUnderlying] = Field(default_factory=list, description="Underlyings with a matched event inside the horizon, sorted by symbol.")
    total_vega_at_risk: float | None = Field(default=None, description="Sum of vega_at_risk over underlyings with known vega; null when none is known.")
    no_event_data_symbols: list[str] = Field(default_factory=list, description="Option underlyings with no matched event; supply events to cover them.")
    warnings: list[str] = Field(default_factory=list, description="Degradation notes, for example positions unavailable or vega missing.")


# ---------------------------------------------------------------------------
# Pure math helpers
# ---------------------------------------------------------------------------


def days_until(target: date, today: date) -> int:
    """Calendar days from today to the target date."""
    return (target - today).days


def straddle_expected_move(
    call_price: float,
    put_price: float,
    underlying_price: float | None,
    multiplier: float,
) -> dict[str, float | None]:
    """Straddle price, percentage move, and the 0.8 event approximation."""
    straddle = (call_price + put_price) * multiplier
    pct = straddle / underlying_price if underlying_price else None
    approx = straddle * STRADDLE_EVENT_FACTOR
    return {
        "straddle_price": straddle,
        "expected_move_pct": pct,
        "expected_move_approx": approx,
    }


def iv_rank(current: float, history: list[float]) -> dict[str, float | None]:
    """IV rank against a history series, as a percentage in [0, 100]."""
    if not history:
        return {"mean_iv": None, "min_iv": None, "max_iv": None, "iv_rank_pct": None}
    low = min(history)
    high = max(history)
    span = high - low
    rank = None
    if span > 0:
        rank = max(0.0, min(100.0, (current - low) / span * 100.0))
    return {"mean_iv": sum(history) / len(history), "min_iv": low, "max_iv": high, "iv_rank_pct": rank}


def nearest_event(events: list[EventSpec], symbol: str, today: date) -> EventSpec | None:
    """Nearest future event whose label mentions the symbol, if any."""
    best: EventSpec | None = None
    best_days = None
    for event in events:
        # Word-boundary match: a substring match would let "ES" hit
        # "TESLA earnings" or "F" hit "CPI FRIDAY".
        if not re.search(rf"\b{re.escape(symbol.upper())}\b", event.label.upper()):
            continue
        event_days = days_until(event.date, today)
        if event_days < 0:
            continue
        if best_days is None or event_days < best_days:
            best, best_days = event, event_days
    return best


# ---------------------------------------------------------------------------
# Chain and snapshot plumbing
# ---------------------------------------------------------------------------


def _mid_price(snapshot: OptionAnalyticsSnapshot) -> float | None:
    """Call or put leg mid from the snapshot; option_price as fallback."""
    bid = snapshot.bid_greeks.option_price if snapshot.bid_greeks else None
    ask = snapshot.ask_greeks.option_price if snapshot.ask_greeks else None
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    for source in (snapshot.model_greeks, snapshot.last_greeks, snapshot.bid_greeks, snapshot.ask_greeks):
        if source is not None and source.option_price is not None:
            return source.option_price
    return None


def _atm_strike(strikes: list[float], reference: float) -> float:
    """Strike closest to the reference price."""
    return min(strikes, key=lambda strike: abs(strike - reference))


async def _snapshot_for(
    state: IBKRRestAppState,
    chain: OptionChain,
    expiry: str,
    strike: float,
    right: OptionRight,
    wait_seconds: float = OPTION_ANALYTICS_WAIT_SECONDS,
) -> OptionAnalyticsSnapshot:
    """One analytics snapshot for a chain contract."""
    spec = OptionContractSpec(
        sec_type="OPT" if chain.underlying_asset_class == AssetClass.EQUITY else "FOP",
        underlying_symbol=chain.underlying_symbol,
        expiry=expiry,
        strike=strike,
        right=right,
        exchange=chain.exchange,
        currency="USD",
        multiplier=chain.multiplier,
        trading_class=chain.trading_class,
    )
    return await state.feed.load_option_analytics(
        OptionAnalyticsRequest(
            contract=spec,
            snapshot_wait_seconds=wait_seconds,
        )
    )


async def _reference_price(
    state: IBKRRestAppState,
    chain: OptionChain,
    expiry: str,
    strikes: list[float],
) -> float | None:
    """Underlying price from a mid-strike snapshot; None when unavailable."""
    probe_strike = (min(strikes) + max(strikes)) / 2.0
    snapshot = await _snapshot_for(state, chain, expiry, probe_strike, OptionRight.CALL)
    for source in (snapshot.model_greeks, snapshot.last_greeks, snapshot.bid_greeks, snapshot.ask_greeks):
        if source is not None and source.underlying_price:
            return source.underlying_price
    return None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/underlying", response_model=UnderlyingEventRiskResponse)
async def get_underlying_event_risk(
    payload: Annotated[UnderlyingEventRiskRequest, Body(openapi_examples={"aapl": {"value": {"symbol": "AAPL"}}})],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> UnderlyingEventRiskResponse:
    """Expected move, ATM IV term structure, and IV history for one symbol.

    Loads the option chain, prices the ATM straddle at the nearest eligible
    expiry, and reports the one-standard-deviation expected move (straddle
    price) plus the 0.8 event approximation. Also returns ATM IV per expiry
    (term_structure) and IV rank against iv_history_days of history. Missing
    data never fails the request: metrics stay null and warnings explains
    what was skipped.
    """
    warnings: list[str] = []
    response = UnderlyingEventRiskResponse(symbol=payload.symbol.upper(), warnings=warnings)
    today = datetime.now(tz=timezone.utc).date()

    asset_class = AssetClass(payload.asset_class.lower())
    chain_request = OptionChainRequest(
        symbol=payload.symbol,
        asset_class=asset_class,
        exchange=payload.exchange,
        currency=payload.currency,
    )
    try:
        chains = await state.feed.load_option_chains(chain_request)
    except Exception as exc:
        warnings.append(f"option chain unavailable: {exc}")
        response.warnings = warnings
        return response
    if not chains:
        warnings.append("option chain returned no exchanges")
        response.warnings = warnings
        return response
    chain = chains[0]

    eligible = [
        expiry
        for expiry in chain.expirations
        if (days := days_to_expiry(expiry, today)) is not None
        and days >= payload.min_days_to_expiry
    ]
    if not eligible:
        warnings.append(f"no expiry at least {payload.min_days_to_expiry} days out")
        response.warnings = warnings
        return response
    selected = eligible[0]

    strikes = list(chain.strikes)
    if not strikes:
        warnings.append("option chain returned no strikes")
        response.warnings = warnings
        return response
    try:
        reference = await _reference_price(state, chain, selected, strikes)
    except Exception as exc:
        warnings.append(f"underlying price unavailable: {exc}")
        response.warnings = warnings
        return response
    if reference is None:
        warnings.append("underlying price unavailable; ATM strike chosen from chain midpoint")
        reference = (min(strikes) + max(strikes)) / 2.0
    atm = _atm_strike(strikes, reference)

    multiplier = float(chain.multiplier) if chain.multiplier else DEFAULT_MULTIPLIER
    try:
        call_snapshot = await _snapshot_for(state, chain, selected, atm, OptionRight.CALL)
        put_snapshot = await _snapshot_for(state, chain, selected, atm, OptionRight.PUT)
    except Exception as exc:
        warnings.append(f"straddle legs unavailable: {exc}")
        response.warnings = warnings
        return response
    call_price = _mid_price(call_snapshot)
    put_price = _mid_price(put_snapshot)

    response.selected_expiry = selected
    response.days_to_expiry = days_to_expiry(selected, today)
    response.atm_strike = atm
    response.underlying_price = reference

    if call_price is not None and put_price is not None:
        move = straddle_expected_move(call_price, put_price, reference, multiplier)
        response.straddle_price = move["straddle_price"]
        response.expected_move_pct = move["expected_move_pct"]
        response.expected_move_approx = move["expected_move_approx"]
        response.expected_move_abs = move["straddle_price"]
    else:
        warnings.append("straddle legs incomplete; expected move skipped")

    call_iv = call_snapshot.implied_volatility
    put_iv = put_snapshot.implied_volatility
    ivs = [value for value in (call_iv, put_iv) if value is not None]
    response.atm_iv = sum(ivs) / len(ivs) if ivs else None

    for expiry in eligible[: payload.max_expiries]:
        try:
            expiry_call = await _snapshot_for(state, chain, expiry, atm, OptionRight.CALL, TERM_STRUCTURE_WAIT_SECONDS)
            expiry_put = await _snapshot_for(state, chain, expiry, atm, OptionRight.PUT, TERM_STRUCTURE_WAIT_SECONDS)
            ivs = [
                value
                for value in (expiry_call.implied_volatility, expiry_put.implied_volatility)
                if value is not None
            ]
            expiry_iv = sum(ivs) / len(ivs) if ivs else None
        except Exception as exc:
            warnings.append(f"term structure point skipped for {expiry}: {exc}")
            expiry_iv = None
        response.term_structure.append(
            TermStructurePoint(
                expiry=expiry,
                days_to_expiry=days_to_expiry(expiry, today),
                atm_iv=expiry_iv,
            )
        )

    if response.atm_iv is not None:
        try:
            history = await _load_iv_history(state, payload)
            if history:
                rank = iv_rank(response.atm_iv, history)
                response.iv_history = IVHistoryStats(
                    observations=len(history),
                    current_iv=response.atm_iv,
                    **rank,
                )
        except Exception as exc:
            warnings.append(f"IV history unavailable: {exc}")

    event = nearest_event(payload.events, payload.symbol, today)
    if event is not None:
        response.nearest_event = event

    response.warnings = warnings
    return response


async def _load_iv_history(
    state: IBKRRestAppState,
    payload: UnderlyingEventRiskRequest,
) -> list[float]:
    """Implied-vol history for the underlying from the OHLCV IV series."""
    from src.feeds.models import OHLCVRequest, normalize_bar_size

    request = OHLCVRequest(
        symbol=payload.symbol,
        asset_class=AssetClass(payload.asset_class.lower()),
        exchange=payload.exchange,
        currency=payload.currency,
        bar_size=normalize_bar_size("1 day"),
        duration=f"{payload.iv_history_days} D",
        what_to_show="OPTION_IMPLIED_VOLATILITY",
    )
    bars = await state.feed.load_historical_ohlcv(request)
    return [bar.vwap for bar in bars if bar.vwap is not None and bar.vwap > 0]


@router.post("/exposure", response_model=EventExposureResponse)
async def get_event_exposure(
    payload: Annotated[EventExposureRequest, Body(openapi_examples={"default": {"value": {}}})],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> EventExposureResponse:
    """Option underlyings with events inside the horizon and their vega at risk.

    Reads live option positions, matches each underlying against the
    user-supplied events (word-boundary label match), resolves net vega per
    underlying from IBKR model Greeks, and applies the assumed
    implied-volatility crush to compute vega at risk. Underlyings without a
    matched event are listed in no_event_data_symbols; a warning is added
    when no events match or vega is unavailable for every exposed
    underlying.
    """
    warnings: list[str] = []
    response = EventExposureResponse(
        account=payload.account,
        horizon_days=payload.horizon_days,
        assumed_iv_crush_pts=payload.assumed_iv_crush_pts,
        warnings=warnings,
    )
    today = datetime.now(tz=timezone.utc).date()

    positions: list[LivePositionDTO] = []
    try:
        positions = await state.feed.load_live_positions()
    except Exception as exc:
        warnings.append(f"positions unavailable: {exc}")
        return response

    option_positions = [p for p in positions if p.sec_type.upper() in OPTION_SEC_TYPES]
    if not option_positions:
        warnings.append("no option positions")
        return response

    vega_by_underlying: dict[str, float] = {}
    for position in option_positions:
        underlying = (position.underlying_symbol or position.symbol).upper()
        vega_by_underlying.setdefault(underlying, 0.0)

    event_map: dict[str, EventSpec] = {}
    for underlying in vega_by_underlying:
        event = nearest_event(payload.events, underlying, today)
        if event is not None and 0 <= days_until(event.date, today) <= payload.horizon_days:
            event_map[underlying] = event

    matched_con_ids = 0
    vega_known: dict[str, float | None] = {underlying: None for underlying in vega_by_underlying}
    # Resolve vega concurrently (bounded): sequential per-position round trips
    # breach the request deadline on option-heavy portfolios; results stay in
    # position order so the aggregation below is deterministic.
    event_positions = [
        p for p in option_positions if (p.underlying_symbol or p.symbol).upper() in event_map
    ]
    matched_con_ids = len(event_positions)
    if event_positions:
        semaphore = asyncio.Semaphore(VEGA_CONCURRENCY)

        async def _resolve(position: LivePositionDTO) -> float | None:
            async with semaphore:
                return await _position_vega(state, position, warnings)

        contributions = await asyncio.gather(*(_resolve(p) for p in event_positions))
    else:
        contributions = []
    for position, contribution in zip(event_positions, contributions):
        underlying = (position.underlying_symbol or position.symbol).upper()
        if contribution is None:
            continue
        current = vega_known[underlying] or 0.0
        vega_known[underlying] = current + contribution

    totals_known: list[float] = []
    for underlying, event in sorted(event_map.items()):
        net_vega = vega_known[underlying]
        var = abs(net_vega) * payload.assumed_iv_crush_pts if net_vega is not None else None
        if var is not None:
            totals_known.append(var)
        response.exposed.append(
            ExposedUnderlying(
                underlying=underlying,
                option_position_count=sum(
                    1
                    for p in option_positions
                    if (p.underlying_symbol or p.symbol).upper() == underlying
                ),
                net_vega_per_pt=net_vega,
                days_to_event=days_until(event.date, today),
                event_label=event.label,
                vega_at_risk=var,
            )
        )

    response.no_event_data_symbols = sorted(
        underlying for underlying in vega_by_underlying if underlying not in event_map
    )
    if totals_known:
        response.total_vega_at_risk = sum(totals_known)
    if not event_map:
        warnings.append("no events matched inside the horizon")
    if matched_con_ids == 0 and event_map:
        warnings.append("vega unavailable for all exposed underlyings")

    response.warnings = warnings
    return response


async def _position_vega(
    state: IBKRRestAppState,
    position: LivePositionDTO,
    warnings: list[str],
) -> float | None:
    """Vega per volatility point for one option position; None on failure."""
    label = f"{position.symbol} {position.expiry} {position.strike}{position.right or ''}"
    spec = to_option_contract_spec(position, DEFAULT_MULTIPLIER)
    if spec is None:
        warnings.append(f"option identity incomplete, vega skipped: {label}")
        return None
    try:
        snapshot = await state.feed.load_option_analytics(OptionAnalyticsRequest(contract=spec))
        greeks = snapshot.model_greeks or snapshot.last_greeks or snapshot.bid_greeks or snapshot.ask_greeks
        if greeks is None or greeks.vega is None:
            warnings.append(f"vega missing for {label}")
            return None
        return greeks.vega * position.position * (position.multiplier or DEFAULT_MULTIPLIER)
    except Exception as exc:
        warnings.append(f"vega unavailable for {label}: {exc}")
        return None
