from __future__ import annotations

import math
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS = ("100", "101", "104", "105", "106")


class OptionRight(StrEnum):
    CALL = "C"
    PUT = "P"


class OptionGreekSource(StrEnum):
    BID = "bid"
    ASK = "ask"
    LAST = "last"
    MODEL = "model"


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
    """Snapshot request for option analytics from IBKR market data."""

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
        normalized = tuple(str(item).strip() for item in value if str(item).strip())
        if not normalized:
            raise ValueError("generic_ticks cannot be empty")
        return normalized

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
