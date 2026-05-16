from __future__ import annotations

import math
from datetime import date, datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.contracts import OptionChain, OptionChainRequest


DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS = ("100", "101", "104", "105", "106")


class OptionRight(StrEnum):
    CALL = "C"
    PUT = "P"


class OptionGreekSource(StrEnum):
    BID = "bid"
    ASK = "ask"
    LAST = "last"
    MODEL = "model"


class OptionSkewSelectionMethod(StrEnum):
    DELTA_TARGET = "delta_target"
    MONEYNESS_FALLBACK = "moneyness_fallback"
    INSUFFICIENT_DATA = "insufficient_data"


class OptionContractSpec(BaseModel):
    """Option contract definition suitable for IBKR market data requests."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    underlying_symbol: str = Field(min_length=1)
    expiry: str = Field(min_length=6)
    strike: float = Field(gt=0)
    right: OptionRight
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    multiplier: str = Field(default="100", min_length=1)
    trading_class: str | None = None
    local_symbol: str | None = None
    con_id: int | None = Field(default=None, gt=0)

    @field_validator("underlying_symbol", "exchange", "currency", "trading_class", "local_symbol", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None


class OptionGreekSet(BaseModel):
    """One IBKR option computation tick."""

    model_config = ConfigDict(extra="forbid")

    source: OptionGreekSource
    implied_vol: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    option_price: float | None = None
    pv_dividend: float | None = None
    underlying_price: float | None = None

    @field_validator("implied_vol", "delta", "gamma", "theta", "vega", "option_price", "pv_dividend", "underlying_price", mode="before")
    @classmethod
    def normalize_optional_float(cls, value: Any) -> float | None:
        if value is None:
            return None
        numeric = float(value)
        if not math.isfinite(numeric) or abs(numeric) > 1e300:
            return None
        return numeric


class OptionAnalyticsRequest(BaseModel):
    """Short-lived option market-data request for Greeks, OI, volume, and volatility."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    contract: OptionContractSpec
    generic_ticks: tuple[str, ...] = DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS
    snapshot_wait_seconds: float = Field(default=2.0, gt=0)
    regulatory_snapshot: bool = False

    @field_validator("generic_ticks", mode="before")
    @classmethod
    def normalize_generic_ticks(cls, value: Any) -> tuple[str, ...]:
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("generic_ticks must be a sequence")
        return tuple(str(item).strip() for item in value if str(item).strip())

    @property
    def generic_tick_list(self) -> str:
        return ",".join(self.generic_ticks)


class OptionAnalyticsSnapshot(BaseModel):
    """Option analytics snapshot from IBKR market data."""

    model_config = ConfigDict(extra="forbid")

    contract: OptionContractSpec
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    bid_greeks: OptionGreekSet | None = None
    ask_greeks: OptionGreekSet | None = None
    last_greeks: OptionGreekSet | None = None
    model_greeks: OptionGreekSet | None = None
    implied_volatility: float | None = None
    historical_volatility: float | None = None
    option_volume: float | None = None
    average_option_volume: float | None = None
    open_interest: float | None = None
    call_open_interest: float | None = None
    put_open_interest: float | None = None
    call_volume: float | None = None
    put_volume: float | None = None
    source: str = Field(default="ibkr", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "implied_volatility",
        "historical_volatility",
        "option_volume",
        "average_option_volume",
        "open_interest",
        "call_open_interest",
        "put_open_interest",
        "call_volume",
        "put_volume",
        mode="before",
    )
    @classmethod
    def normalize_optional_float(cls, value: Any) -> float | None:
        return OptionGreekSet.normalize_optional_float(value)

    @model_validator(mode="after")
    def derive_totals(self) -> "OptionAnalyticsSnapshot":
        if self.open_interest is None:
            values = [value for value in (self.call_open_interest, self.put_open_interest) if value is not None]
            if values:
                self.open_interest = sum(values)
        if self.option_volume is None:
            values = [value for value in (self.call_volume, self.put_volume) if value is not None]
            if values:
                self.option_volume = sum(values)
        return self


class OptionSkewSurfaceRequest(BaseModel):
    """Request a bounded option skew scan by expiry.

    The request intentionally samples a strike window instead of pulling a full
    option surface. That keeps the API usable under IBKR market-data limits.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    chain_request: OptionChainRequest
    expirations: tuple[str, ...] | None = None
    chain_exchange: str | None = None
    trading_class: str | None = None
    option_exchange: str | None = None
    spot_price: float | None = Field(default=None, gt=0)
    strike_window_pct: float = Field(default=0.30, gt=0, le=2.0)
    max_expirations: int = Field(default=6, ge=1, le=36)
    max_strikes_per_expiry: int = Field(default=11, ge=3, le=50)
    # IBKR default market data line budget is 100 per account (shared with
    # TWS watchlist and other API clients).  max_total_lines caps the total
    # number of reqMktData snapshot calls across all expirations and strikes
    # to stay within budget.  Each snapshot call holds one market data line
    # for the duration of the request.
    max_total_lines: int = Field(
        default=60,
        ge=10,
        le=500,
        description=(
            "Hard cap on total snapshot reqMktData calls for the entire surface. "
            "IBKR default is 100 market data lines per account; reserve headroom "
            "for other subscriptions (TWS watchlist, equity snapshots, etc.)."
        ),
    )
    target_abs_delta: float = Field(default=0.25, gt=0, lt=1)
    fallback_moneyness_pct: float = Field(default=0.05, ge=0, le=1)
    snapshot_wait_seconds: float = Field(default=2.0, gt=0)
    max_concurrent_requests: int = Field(default=4, ge=1, le=20)
    generic_ticks: tuple[str, ...] = DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS
    regulatory_snapshot: bool = False

    @field_validator("expirations", mode="before")
    @classmethod
    def normalize_optional_tuple(cls, value: Any) -> tuple[str, ...] | None:
        if value is None:
            return None
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("value must be a sequence")
        normalized = tuple(str(item).strip().upper() for item in value if str(item).strip())
        return normalized or None

    @field_validator("generic_ticks", mode="before")
    @classmethod
    def normalize_generic_ticks(cls, value: Any) -> tuple[str, ...]:
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("generic_ticks must be a sequence")
        normalized = tuple(str(item).strip() for item in value if str(item).strip())
        if not normalized:
            raise ValueError("generic_ticks cannot be empty")
        return normalized

    @field_validator("chain_exchange", "trading_class", "option_exchange", mode="before")
    @classmethod
    def normalize_optional_upper(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    @model_validator(mode="after")
    def validate_sampling(self) -> "OptionSkewSurfaceRequest":
        # Cap max_strikes_per_expiry so that total lines stay within budget.
        # max_total_lines is the absolute ceiling; max_expirations * max_strikes
        # must not exceed it.
        max_feasible_strikes = max(3, self.max_total_lines // max(1, self.max_expirations))
        if self.max_strikes_per_expiry > max_feasible_strikes:
            self.max_strikes_per_expiry = max_feasible_strikes if max_feasible_strikes % 2 == 1 else max_feasible_strikes - 1
            if self.max_strikes_per_expiry < 3:
                self.max_strikes_per_expiry = 3
        if self.max_strikes_per_expiry % 2 == 0:
            self.max_strikes_per_expiry += 1 if self.max_strikes_per_expiry < 50 else -1
        return self


class OptionSkewPoint(BaseModel):
    """Selected option point used for skew or open-interest reporting."""

    model_config = ConfigDict(extra="forbid")

    contract: OptionContractSpec
    implied_volatility: float | None = None
    delta: float | None = None
    open_interest: float | None = None
    option_volume: float | None = None


class OptionMaturitySkew(BaseModel):
    """Per-expiry option skew and open-interest summary."""

    model_config = ConfigDict(extra="forbid")

    underlying_symbol: str
    expiry: str
    days_to_expiry: int | None = None
    spot_price: float
    target_abs_delta: float
    selection_method: OptionSkewSelectionMethod
    selected_call: OptionSkewPoint | None = None
    selected_put: OptionSkewPoint | None = None
    skew_put_minus_call_iv: float | None = None
    risk_reversal_call_minus_put_iv: float | None = None
    max_call_open_interest: OptionSkewPoint | None = None
    max_put_open_interest: OptionSkewPoint | None = None
    sampled_contract_count: int
    warnings: tuple[str, ...] = ()


class OptionSkewSurfaceResponse(BaseModel):
    """Bounded skew surface summary grouped by option maturity."""

    model_config = ConfigDict(extra="forbid")

    underlying_symbol: str
    underlying_con_id: int
    underlying_asset_class: str
    chain_exchange: str
    trading_class: str
    multiplier: str
    spot_price: float
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    maturities: tuple[OptionMaturitySkew, ...]
    source: str = Field(default="ibkr", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


def build_ibkr_option_contract(spec: OptionContractSpec) -> Any:
    try:
        from ib_insync import Option
    except ImportError as exc:
        raise RuntimeError("ib_insync is required for option contract creation") from exc
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
