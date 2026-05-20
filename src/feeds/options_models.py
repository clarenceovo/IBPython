"""Option domain models — Pydantic schemas for option contracts, Greeks, analytics, and skew.

Extracted from ``options.py`` so that the analytics / calculation functions
can be maintained separately from the data definitions.
"""

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

    sec_type: str = Field(
        default="OPT",
        min_length=1,
        description="IBKR option secType. Use OPT for stock/index options and FOP for futures options.",
    )
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

    @field_validator("sec_type", "underlying_symbol", "exchange", "currency", "trading_class", "local_symbol", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    @field_validator("sec_type")
    @classmethod
    def validate_sec_type(cls, value: str) -> str:
        if value not in {"OPT", "FOP"}:
            raise ValueError("sec_type must be OPT or FOP")
        return value


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
    """Request a bounded option skew scan by expiry."""

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
