"""Option analytics — contract building, normalization, skew calculation.

Pydantic models live in ``options_models.py``; this module contains the
analytics / calculation functions that operate on those models.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Any

from src.feeds.contracts import OptionChain, OptionChainRequest
from src.feeds.options_models import (
    DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS,
    OptionAnalyticsRequest,
    OptionAnalyticsSnapshot,
    OptionContractSpec,
    OptionGreekSet,
    OptionGreekSource,
    OptionMaturitySkew,
    OptionRight,
    OptionSkewPoint,
    OptionSkewSelectionMethod,
    OptionSkewSurfaceRequest,
    OptionSkewSurfaceResponse,
)

__all__ = [
    "DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS",
    "OptionAnalyticsRequest",
    "OptionAnalyticsSnapshot",
    "OptionContractSpec",
    "OptionGreekSet",
    "OptionGreekSource",
    "OptionMaturitySkew",
    "OptionRight",
    "OptionSkewPoint",
    "OptionSkewSelectionMethod",
    "OptionSkewSurfaceRequest",
    "OptionSkewSurfaceResponse",
    "build_ibkr_option_contract",
    "build_skew_option_contracts",
    "calculate_maturity_skew",
    "normalize_option_analytics_from_ticker",
    "select_option_chain",
    "select_skew_expirations",
    "select_skew_strikes",
]


def build_ibkr_option_contract(spec: OptionContractSpec) -> Any:
    try:
        from ib_insync import Contract, Option
    except ImportError as exc:
        raise RuntimeError("ib_insync is required for option contract creation") from exc
    if spec.sec_type == "FOP":
        contract = Contract(
            secType="FOP",
            symbol=spec.underlying_symbol,
            lastTradeDateOrContractMonth=spec.expiry,
            strike=spec.strike,
            right=spec.right.value,
            exchange=spec.exchange,
            currency=spec.currency,
            multiplier=spec.multiplier,
        )
    else:
        contract = Option(
            symbol=spec.underlying_symbol,
            lastTradeDateOrContractMonth=spec.expiry,
            strike=spec.strike,
            right=spec.right.value,
            exchange=spec.exchange,
            currency=spec.currency,
            multiplier=spec.multiplier,
        )
    if spec.trading_class:
        contract.tradingClass = spec.trading_class
    if spec.local_symbol:
        contract.localSymbol = spec.local_symbol
    if spec.con_id:
        contract.conId = spec.con_id
    return contract


def normalize_option_analytics_from_ticker(
    ticker: Any,
    contract: OptionContractSpec,
    *,
    timestamp: datetime | None = None,
) -> OptionAnalyticsSnapshot:
    return OptionAnalyticsSnapshot(
        contract=contract,
        timestamp=timestamp or datetime.now(timezone.utc),
        bid_greeks=_normalize_greeks(getattr(ticker, "bidGreeks", None), OptionGreekSource.BID),
        ask_greeks=_normalize_greeks(getattr(ticker, "askGreeks", None), OptionGreekSource.ASK),
        last_greeks=_normalize_greeks(getattr(ticker, "lastGreeks", None), OptionGreekSource.LAST),
        model_greeks=_normalize_greeks(getattr(ticker, "modelGreeks", None), OptionGreekSource.MODEL),
        implied_volatility=getattr(ticker, "impliedVolatility", None),
        historical_volatility=getattr(ticker, "histVolatility", None),
        average_option_volume=getattr(ticker, "avOptionVolume", None),
        call_open_interest=getattr(ticker, "callOpenInterest", None),
        put_open_interest=getattr(ticker, "putOpenInterest", None),
        call_volume=getattr(ticker, "callVolume", None),
        put_volume=getattr(ticker, "putVolume", None),
        option_volume=getattr(ticker, "volume", None),
    )


def _normalize_greeks(value: Any, source: OptionGreekSource) -> OptionGreekSet | None:
    if value is None:
        return None
    return OptionGreekSet(
        source=source,
        implied_vol=getattr(value, "impliedVol", None),
        delta=getattr(value, "delta", None),
        gamma=getattr(value, "gamma", None),
        theta=getattr(value, "theta", None),
        vega=getattr(value, "vega", None),
        option_price=getattr(value, "optPrice", None),
        pv_dividend=getattr(value, "pvDividend", None),
        underlying_price=getattr(value, "undPrice", None),
    )


def select_option_chain(chains: list[OptionChain], request: OptionSkewSurfaceRequest) -> OptionChain:
    """Choose the chain slice used for skew analytics."""

    if not chains:
        raise ValueError("option skew requires at least one option chain")

    def score(chain: OptionChain) -> int:
        value = 0
        if request.chain_exchange and chain.exchange == request.chain_exchange:
            value += 500
        if request.trading_class and chain.trading_class == request.trading_class:
            value += 500
        if not request.chain_exchange and chain.exchange == request.chain_request.exchange:
            value += 120
        if not request.trading_class and chain.trading_class == request.chain_request.symbol:
            value += 80
        if chain.exchange == "SMART":
            value += 30
        value += min(len(chain.expirations), 100)
        return value

    candidates = [
        chain
        for chain in chains
        if (request.chain_exchange is None or chain.exchange == request.chain_exchange)
        and (request.trading_class is None or chain.trading_class == request.trading_class)
    ]
    if not candidates:
        raise ValueError("no option chain matched requested chain_exchange/trading_class")
    return max(candidates, key=score)


def select_skew_expirations(chain: OptionChain, request: OptionSkewSurfaceRequest) -> tuple[str, ...]:
    expirations = chain.expirations
    if request.expirations:
        requested = set(request.expirations)
        expirations = tuple(expiry for expiry in expirations if expiry in requested)
    return expirations[: request.max_expirations]


def select_skew_strikes(strikes: tuple[float, ...], *, spot_price: float, window_pct: float, max_count: int) -> tuple[float, ...]:
    lower = spot_price * (1 - window_pct)
    upper = spot_price * (1 + window_pct)
    candidates = [strike for strike in strikes if lower <= strike <= upper]
    if not candidates:
        candidates = list(strikes)
    selected = sorted(candidates, key=lambda strike: (abs(strike - spot_price), strike))[:max_count]
    return tuple(sorted(selected))


def build_skew_option_contracts(
    *,
    chain: OptionChain,
    request: OptionSkewSurfaceRequest,
    expiry: str,
    strikes: tuple[float, ...],
) -> tuple[OptionContractSpec, ...]:
    exchange = request.option_exchange or chain.exchange or request.chain_request.exchange
    contracts: list[OptionContractSpec] = []
    for strike in strikes:
        for right in (OptionRight.CALL, OptionRight.PUT):
            contracts.append(
                OptionContractSpec(
                    underlying_symbol=request.chain_request.symbol,
                    expiry=expiry,
                    strike=strike,
                    right=right,
                    exchange=exchange,
                    currency=request.chain_request.currency,
                    multiplier=chain.multiplier or "100",
                    trading_class=chain.trading_class or None,
                )
            )
    return tuple(contracts)


def calculate_maturity_skew(
    *,
    underlying_symbol: str,
    expiry: str,
    spot_price: float,
    target_abs_delta: float,
    fallback_moneyness_pct: float,
    snapshots: list[OptionAnalyticsSnapshot],
    warnings: tuple[str, ...] = (),
) -> OptionMaturitySkew:
    points = [_snapshot_to_skew_point(snapshot) for snapshot in snapshots]
    calls = [point for point in points if point.contract.right is OptionRight.CALL]
    puts = [point for point in points if point.contract.right is OptionRight.PUT]

    call = _select_delta_target(calls, target_delta=target_abs_delta)
    put = _select_delta_target(puts, target_delta=-target_abs_delta)
    selection_method = OptionSkewSelectionMethod.DELTA_TARGET

    if call is None or put is None:
        call = call or _select_moneyness_target(calls, spot_price=spot_price, target_moneyness=1 + fallback_moneyness_pct)
        put = put or _select_moneyness_target(puts, spot_price=spot_price, target_moneyness=1 - fallback_moneyness_pct)
        selection_method = OptionSkewSelectionMethod.MONEYNESS_FALLBACK

    skew = None
    risk_reversal = None
    if call and put and call.implied_volatility is not None and put.implied_volatility is not None:
        skew = put.implied_volatility - call.implied_volatility
        risk_reversal = call.implied_volatility - put.implied_volatility
    elif call is None or put is None:
        selection_method = OptionSkewSelectionMethod.INSUFFICIENT_DATA

    return OptionMaturitySkew(
        underlying_symbol=underlying_symbol,
        expiry=expiry,
        days_to_expiry=_days_to_expiry(expiry),
        spot_price=spot_price,
        target_abs_delta=target_abs_delta,
        selection_method=selection_method,
        selected_call=call,
        selected_put=put,
        skew_put_minus_call_iv=skew,
        risk_reversal_call_minus_put_iv=risk_reversal,
        max_call_open_interest=_max_open_interest(calls),
        max_put_open_interest=_max_open_interest(puts),
        sampled_contract_count=len(snapshots),
        warnings=warnings,
    )


# ── Private analytics helpers ───────────────────────────────────────────────

def _snapshot_to_skew_point(snapshot: OptionAnalyticsSnapshot) -> OptionSkewPoint:
    return OptionSkewPoint(
        contract=snapshot.contract,
        implied_volatility=_snapshot_implied_volatility(snapshot),
        delta=_snapshot_delta(snapshot),
        open_interest=_snapshot_open_interest(snapshot),
        option_volume=_snapshot_option_volume(snapshot),
    )


def _snapshot_implied_volatility(snapshot: OptionAnalyticsSnapshot) -> float | None:
    values = [
        getattr(snapshot.model_greeks, "implied_vol", None),
        getattr(snapshot.last_greeks, "implied_vol", None),
        _mid_optional(getattr(snapshot.bid_greeks, "implied_vol", None), getattr(snapshot.ask_greeks, "implied_vol", None)),
        getattr(snapshot.bid_greeks, "implied_vol", None),
        getattr(snapshot.ask_greeks, "implied_vol", None),
        snapshot.implied_volatility,
    ]
    return _first_finite_positive(values)


def _snapshot_delta(snapshot: OptionAnalyticsSnapshot) -> float | None:
    values = [
        getattr(snapshot.model_greeks, "delta", None),
        getattr(snapshot.last_greeks, "delta", None),
        _mid_optional(getattr(snapshot.bid_greeks, "delta", None), getattr(snapshot.ask_greeks, "delta", None)),
        getattr(snapshot.bid_greeks, "delta", None),
        getattr(snapshot.ask_greeks, "delta", None),
    ]
    return _first_finite(values)


def _snapshot_open_interest(snapshot: OptionAnalyticsSnapshot) -> float | None:
    if snapshot.contract.right is OptionRight.CALL:
        values = [snapshot.call_open_interest, snapshot.open_interest]
    else:
        values = [snapshot.put_open_interest, snapshot.open_interest]
    return _first_finite_non_negative(values)


def _snapshot_option_volume(snapshot: OptionAnalyticsSnapshot) -> float | None:
    if snapshot.contract.right is OptionRight.CALL:
        values = [snapshot.call_volume, snapshot.option_volume]
    else:
        values = [snapshot.put_volume, snapshot.option_volume]
    return _first_finite_non_negative(values)


def _select_delta_target(points: list[OptionSkewPoint], *, target_delta: float) -> OptionSkewPoint | None:
    candidates = [
        point
        for point in points
        if point.implied_volatility is not None and point.delta is not None and math.copysign(1, point.delta) == math.copysign(1, target_delta)
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda point: abs(abs(point.delta or 0) - abs(target_delta)))


def _select_moneyness_target(points: list[OptionSkewPoint], *, spot_price: float, target_moneyness: float) -> OptionSkewPoint | None:
    candidates = [point for point in points if point.implied_volatility is not None]
    if not candidates:
        return None
    target_strike = spot_price * target_moneyness
    return min(candidates, key=lambda point: abs(point.contract.strike - target_strike))


def _max_open_interest(points: list[OptionSkewPoint]) -> OptionSkewPoint | None:
    candidates = [point for point in points if point.open_interest is not None]
    if not candidates:
        return None
    return max(candidates, key=lambda point: (point.open_interest or 0, point.option_volume or 0, point.contract.strike))


def _mid_optional(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return (left + right) / 2


def _first_finite(values: list[float | None]) -> float | None:
    for value in values:
        if value is not None and math.isfinite(value):
            return float(value)
    return None


def _first_finite_positive(values: list[float | None]) -> float | None:
    for value in values:
        if value is not None and math.isfinite(value) and value > 0:
            return float(value)
    return None


def _first_finite_non_negative(values: list[float | None]) -> float | None:
    for value in values:
        if value is not None and math.isfinite(value) and value >= 0:
            return float(value)
    return None


def _days_to_expiry(expiry: str) -> int | None:
    try:
        expiry_date = date.fromisoformat(f"{expiry[:4]}-{expiry[4:6]}-{expiry[6:8]}")
    except (ValueError, IndexError):
        return None
    return (expiry_date - datetime.now(timezone.utc).date()).days
