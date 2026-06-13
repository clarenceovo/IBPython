from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class AssetClass(StrEnum):
    EQUITY = "equity"
    FX = "fx"
    FUTURE = "future"
    BOND = "bond"
    INDEX = "index"
    CRYPTO = "crypto"
    OPTION = "option"


class WhatToShow(StrEnum):
    """Valid IBKR whatToShow values for historical data requests."""

    TRADES = "TRADES"
    MIDPOINT = "MIDPOINT"
    BID = "BID"
    ASK = "ASK"
    BID_ASK = "BID_ASK"
    ADJUSTED_LAST = "ADJUSTED_LAST"
    HISTORICAL_VOLATILITY = "HISTORICAL_VOLATILITY"
    OPTION_IMPLIED_VOLATILITY = "OPTION_IMPLIED_VOLATILITY"
    AGGTRADES = "AGGTRADES"
    FEE_RATE = "FEE_RATE"
    SCHEDULE = "SCHEDULE"
    YIELD_ASK = "YIELD_ASK"
    YIELD_BID = "YIELD_BID"
    YIELD_BID_ASK = "YIELD_BID_ASK"
    YIELD_LAST = "YIELD_LAST"


# Canonical IBKR bar sizes in ascending order of granularity.
_VALID_IBKR_BAR_SIZES: frozenset[str] = frozenset({
    "1 secs", "5 secs", "10 secs", "15 secs", "30 secs",
    "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins",
    "1 hour", "2 hours", "3 hours", "4 hours", "8 hours",
    "1 day", "1 week", "1 month",
})

# Short-form aliases → canonical IBKR bar size.
_BAR_SIZE_ALIASES: dict[str, str] = {
    # Compact forms: 5m, 1h, 1d, 1w
    "1s": "1 secs",
    "5s": "5 secs",
    "10s": "10 secs",
    "15s": "15 secs",
    "30s": "30 secs",
    "1m": "1 min",
    "2m": "2 mins",
    "3m": "3 mins",
    "5m": "5 mins",
    "10m": "10 mins",
    "15m": "15 mins",
    "20m": "20 mins",
    "30m": "30 mins",
    "1h": "1 hour",
    "2h": "2 hours",
    "3h": "3 hours",
    "4h": "4 hours",
    "8h": "8 hours",
    "1d": "1 day",
    "1w": "1 week",
    "1mo": "1 month",
    # Near-miss plural/singular normalizations
    "1 secs": "1 secs",
    "5 sec": "5 secs",
    "10 sec": "10 secs",
    "15 sec": "15 secs",
    "30 sec": "30 secs",
    "1 min": "1 min",
    "2 min": "2 mins",
    "3 min": "3 mins",
    "5 min": "5 mins",
    "10 min": "10 mins",
    "15 min": "15 mins",
    "20 min": "20 mins",
    "30 min": "30 mins",
    "1 hr": "1 hour",
    "2 hr": "2 hours",
    "3 hr": "3 hours",
    "4 hr": "4 hours",
    "8 hr": "8 hours",
}


def normalize_bar_size(value: str) -> str:
    """Normalize a bar size string to its canonical IBKR form.

    Accepts short aliases (``"5m"``, ``"1h"``, ``"1d"``), near-miss
    plural/singular variants (``"5 min"`` → ``"5 mins"``), and already
    canonical values (``"5 mins"``).  Returns the canonical string or
    the trimmed original if no mapping is found (allowing forward
    compatibility with future IBKR bar sizes).
    """
    if not value or not isinstance(value, str):
        return value
    stripped = value.strip()
    lower = stripped.lower()
    # Check alias table first (case-insensitive key).
    mapped = _BAR_SIZE_ALIASES.get(lower)
    if mapped is not None:
        return mapped
    # If it's already canonical (exact match), return as-is.
    if stripped in _VALID_IBKR_BAR_SIZES:
        return stripped
    # Unknown value — return trimmed original for forward compatibility.
    return stripped


class BaseOHLCVBar(BaseModel):
    """Base OHLCV bar with the shared price payload and UTC timestamp."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = Field(ge=0)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        return str(value).strip().upper()

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp_utc(cls, value: Any) -> datetime:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("timestamp must be a datetime")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("open", "high", "low", "close", "volume")
    @classmethod
    def validate_finite_number(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("numeric market data fields must be finite")
        return value

    @model_validator(mode="after")
    def validate_price_range(self) -> Self:
        if self.high < self.low:
            raise ValueError("high must be greater than or equal to low")
        if self.open > self.high or self.open < self.low:
            raise ValueError("open must be inside the high/low range")
        if self.close > self.high or self.close < self.low:
            raise ValueError("close must be inside the high/low range")
        return self

    def to_redis_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_redis_json(cls, payload: str | bytes) -> Self:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return cls.model_validate_json(payload)

    def metadata_json(self) -> str:
        metadata = getattr(self, "metadata", {})
        return json.dumps(metadata, sort_keys=True, default=str)

    @property
    def contract_key(self) -> str:
        return ohlcv_contract_key(self)


class OHLCVBar(BaseOHLCVBar):
    """Vendor-neutral OHLCV bar with market metadata."""

    asset_class: AssetClass
    exchange: str = Field(min_length=1)
    currency: str = Field(min_length=1)
    bar_size: str = Field(min_length=1)
    source: str = Field(default="ibkr", min_length=1)
    vwap: float | None = Field(default=None, description="Volume-weighted average price (IBKR BarData.average)")
    trade_count: int | None = Field(default=None, description="Number of trades in bar (IBKR BarData.barCount)")
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("exchange", "currency", mode="before")
    @classmethod
    def normalize_upper_tokens(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        return str(value).strip().upper()

    @field_validator("source", "bar_size", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("value cannot be empty")
        return normalized


class FutureOHLCVBar(OHLCVBar):
    """Futures OHLCV bar with contract-level identity."""

    asset_class: AssetClass = Field(default=AssetClass.FUTURE)
    contract_month: str | None = Field(
        default=None,
        description="Futures contract month or expiry, for example 202606.",
    )
    is_continuous: bool = Field(
        default=False,
        description="True when the bar comes from a continuous or rolled futures series.",
    )

    @field_validator("contract_month", mode="before")
    @classmethod
    def normalize_contract_month(cls, value: Any) -> str | None:
        if value is None or value == "":
            return None
        return str(value).strip().upper() or None

    @model_validator(mode="after")
    def validate_future_asset_class(self) -> Self:
        if self.asset_class is not AssetClass.FUTURE:
            raise ValueError("FutureOHLCVBar requires asset_class=future")
        return self


class FXOHLCVBar(OHLCVBar):
    """FX OHLCV bar with currency-pair identity."""

    asset_class: AssetClass = Field(default=AssetClass.FX)
    base_currency: str | None = Field(default=None, min_length=1)
    quote_currency: str | None = Field(default=None, min_length=1)

    @field_validator("base_currency", "quote_currency", mode="before")
    @classmethod
    def normalize_optional_currency(cls, value: Any) -> str | None:
        if value is None or value == "":
            return None
        return str(value).strip().upper() or None

    @model_validator(mode="after")
    def validate_fx_bar(self) -> Self:
        if self.asset_class is not AssetClass.FX:
            raise ValueError("FXOHLCVBar requires asset_class=fx")
        if self.base_currency is None and len(self.symbol) >= 6:
            object.__setattr__(self, "base_currency", self.symbol[:3])
        if self.quote_currency is None:
            object.__setattr__(self, "quote_currency", self.currency or self.symbol[3:6])
        return self


class OptionOHLCVBar(OHLCVBar):
    """Option OHLCV bar with option contract identity."""

    asset_class: AssetClass = Field(default=AssetClass.OPTION)
    underlying_symbol: str = Field(min_length=1)
    expiry: str = Field(min_length=1, description="Option expiry in YYYYMMDD or YYYYMM format.")
    strike: float = Field(gt=0)
    right: str = Field(min_length=1, description="Option right: C/CALL or P/PUT.")
    multiplier: str | None = None
    trading_class: str | None = None
    contract_month: str | None = None
    con_id: int | None = Field(default=None, gt=0)

    @field_validator("underlying_symbol", "expiry", "right", "multiplier", "trading_class", "contract_month", mode="before")
    @classmethod
    def normalize_option_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    @field_validator("right")
    @classmethod
    def normalize_option_right(cls, value: str) -> str:
        if value in {"C", "CALL"}:
            return "C"
        if value in {"P", "PUT"}:
            return "P"
        raise ValueError("right must be C/CALL or P/PUT")

    @model_validator(mode="after")
    def validate_option_bar(self) -> Self:
        if self.asset_class is not AssetClass.OPTION:
            raise ValueError("OptionOHLCVBar requires asset_class=option")
        if self.contract_month is None:
            object.__setattr__(self, "contract_month", self.expiry[:6])
        return self


# ─── OHLCV Response Envelope ───────────────────────────────────────

class OHLCVRequestMeta(BaseModel):
    """Echoes the original request parameters for client-side validation."""
    symbol: str
    asset_class: str
    exchange: str | None = None
    currency: str | None = None
    bar_size: str
    what_to_show: str
    use_rth: bool = True
    start_time: datetime | None = Field(default=None, alias="start_time")
    end_time: datetime | None = Field(default=None, alias="end_time")
    duration: str | None = None
    
    model_config = ConfigDict(populate_by_name=True)


class OHLCVDataQuality(BaseModel):
    """Quality metrics for the returned dataset."""
    total_bars: int
    bars_with_missing_volume: int = 0
    bars_with_zero_volume: int = 0
    bars_with_vwap: int = 0
    bars_with_trade_count: int = 0
    gap_count: int = 0
    coverage_ratio: float | None = Field(default=None, description="Fraction of expected bars present")


class OHLCVResponseEnvelope(BaseModel):
    """
    Unified response wrapper for all OHLCV endpoints.
    
    Provides bars + request echo + data quality + timing metadata.
    """
    bars: list[OHLCVBar]
    request: OHLCVRequestMeta
    quality: OHLCVDataQuality
    latency_ms: float = Field(description="Server-side processing time in milliseconds")
    cache_hit: bool = Field(default=False, description="Whether the response was served from cache")
    chunk_count: int = Field(default=1, description="Number of IBKR API paginated calls made")
    source: str = Field(default="ibkr", description="Data source identifier")
    
    model_config = ConfigDict(strict=True)


class FutureOHLCVResponseEnvelope(OHLCVResponseEnvelope):
    bars: list[FutureOHLCVBar]  # type: ignore[assignment]

class FXOHLCVResponseEnvelope(OHLCVResponseEnvelope):
    bars: list[FXOHLCVBar]  # type: ignore[assignment]

class OptionOHLCVResponseEnvelope(OHLCVResponseEnvelope):
    bars: list[OptionOHLCVBar]  # type: ignore[assignment]


def compute_ohlcv_quality(bars: list[OHLCVBar]) -> OHLCVDataQuality:
    """Compute data quality metrics for a list of OHLCV bars."""
    total = len(bars)
    if total == 0:
        return OHLCVDataQuality(total_bars=0)
    
    return OHLCVDataQuality(
        total_bars=total,
        bars_with_missing_volume=sum(1 for b in bars if b.volume is None),
        bars_with_zero_volume=sum(1 for b in bars if b.volume == 0),
        bars_with_vwap=sum(1 for b in bars if b.vwap is not None),
        bars_with_trade_count=sum(1 for b in bars if b.trade_count is not None),
    )


class OHLCVRequest(BaseModel):
    """Historical OHLCV request independent of the downstream provider."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    asset_class: AssetClass
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    start_datetime: datetime | None = Field(
        default=None,
        description="Optional inclusive start timestamp for paginated historical range loads.",
    )
    end_datetime: datetime | None = None
    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True
    source: str = Field(default="ibkr", min_length=1)
    primary_exchange: str | None = None
    last_trade_date_or_contract_month: str | None = None
    multiplier: str | None = None
    local_symbol: str | None = None
    continuous: bool = Field(
        default=False,
        description="Use an IBKR continuous futures contract (CONTFUT) for historical data only.",
    )
    option_sec_type: str | None = Field(
        default=None,
        description="IBKR option secType. Use OPT for stock/index options and FOP for futures options.",
    )
    underlying_symbol: str | None = None
    expiry: str | None = None
    strike: float | None = Field(default=None, gt=0)
    right: str | None = Field(default=None, description="Option right: C/CALL or P/PUT.")
    trading_class: str | None = None
    sec_id_type: str | None = None
    sec_id: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol", "exchange", "currency", mode="before")
    @classmethod
    def normalize_upper_tokens(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        return str(value).strip().upper()

    @field_validator("start_datetime", "end_datetime", mode="before")
    @classmethod
    def normalize_datetime_utc(cls, value: Any) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("datetime fields must be datetimes")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("bar_size", mode="before")
    @classmethod
    def normalize_bar_size_value(cls, value: Any) -> str:
        """Normalize bar_size aliases to canonical IBKR form before further validation."""
        if value is None:
            raise ValueError("value is required")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("value cannot be empty")
        from src.feeds.models import normalize_bar_size
        return normalize_bar_size(normalized)

    @field_validator("duration", "what_to_show", "source", mode="before")
    @classmethod
    def normalize_non_empty_text(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("value cannot be empty")
        return normalized

    @field_validator("what_to_show")
    @classmethod
    def validate_what_to_show(cls, value: str) -> str:
        """Accept WhatToShow enum values or plain strings for backward compat."""
        from src.feeds.models import WhatToShow
        upper = value.upper().strip()
        try:
            WhatToShow(upper)
        except ValueError:
            # Allow unknown values for forward compatibility; just warn via debug.
            pass
        return upper

    @field_validator(
        "primary_exchange",
        "last_trade_date_or_contract_month",
        "multiplier",
        "local_symbol",
        "option_sec_type",
        "underlying_symbol",
        "expiry",
        "right",
        "trading_class",
        "sec_id_type",
        "sec_id",
        mode="before",
    )
    @classmethod
    def normalize_optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized.upper() or None

    @field_validator("right")
    @classmethod
    def normalize_option_right(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value in {"C", "CALL"}:
            return "C"
        if value in {"P", "PUT"}:
            return "P"
        raise ValueError("right must be C/CALL or P/PUT")

    @field_validator("option_sec_type")
    @classmethod
    def validate_option_sec_type(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value not in {"OPT", "FOP"}:
            raise ValueError("option_sec_type must be OPT or FOP")
        return value

    @model_validator(mode="after")
    def validate_contract_identifiers(self) -> Self:
        if self.start_datetime is not None and self.end_datetime is not None and self.start_datetime >= self.end_datetime:
            raise ValueError("start_datetime must be before end_datetime")
        if self.continuous and self.asset_class is not AssetClass.FUTURE:
            raise ValueError("continuous OHLCV requests are only supported for futures")
        if self.asset_class is AssetClass.FUTURE:
            if self.continuous and (self.last_trade_date_or_contract_month or self.local_symbol or self.con_id):
                raise ValueError("continuous future OHLCV requests cannot include contract_month, local_symbol, or con_id")
            if not self.continuous and not (self.last_trade_date_or_contract_month or self.local_symbol or self.con_id):
                raise ValueError("future OHLCV requests require last_trade_date_or_contract_month, local_symbol, or con_id")
        if self.asset_class is AssetClass.BOND and not (self.symbol or self.sec_id or self.con_id):
            raise ValueError("bond OHLCV requests require symbol, sec_id, or con_id")
        if self.asset_class is AssetClass.OPTION:
            if self.option_sec_type is None:
                object.__setattr__(self, "option_sec_type", "OPT")
            missing = [
                name
                for name, value in (
                    ("underlying_symbol", self.underlying_symbol),
                    ("expiry", self.expiry or self.last_trade_date_or_contract_month),
                    ("strike", self.strike),
                    ("right", self.right),
                )
                if value is None
            ]
            if missing:
                raise ValueError(f"option OHLCV requests require {', '.join(missing)}")
        # Validate whatToShow is appropriate for the asset class
        self._validate_what_to_show_for_asset_class()
        return self

    def _validate_what_to_show_for_asset_class(self) -> None:
        """Warn (not raise) if whatToShow is not typically valid for the requested asset class.

        This is a soft validation — invalid combinations will fail at the IBKR API level,
        but this gives the user early feedback.
        """
        wts = self.what_to_show.upper().strip()
        ac = self.asset_class
        # Asset-class-specific whatToShow constraints from IBKR documentation
        _BOND_ONLY = {"YIELD_ASK", "YIELD_BID", "YIELD_BID_ASK", "YIELD_LAST"}
        _CRYPTO_ONLY = {"AGGTRADES"}
        if ac is AssetClass.BOND and wts not in _BOND_ONLY and wts not in {"TRADES", "MIDPOINT", "BID", "ASK", "BID_ASK"}:
            pass  # Bonds support both yield and price types
        if ac is not AssetClass.BOND and wts in _BOND_ONLY:
            import logging
            logging.getLogger(__name__).warning(
                "whatToShow=%s is typically only valid for bonds, not %s", wts, ac.value
            )
        if ac is not AssetClass.CRYPTO and wts in _CRYPTO_ONLY:
            import logging
            logging.getLogger(__name__).warning(
                "whatToShow=%s is typically only valid for crypto, not %s", wts, ac.value
            )


def ohlcv_contract_identity(bar: Any) -> dict[str, Any]:
    metadata = getattr(bar, "metadata", {}) or {}
    con_id = getattr(bar, "con_id", None) or metadata.get("con_id")
    local_symbol = getattr(bar, "local_symbol", None) or metadata.get("local_symbol")
    contract_month = (
        getattr(bar, "contract_month", None)
        or metadata.get("contract_month")
        or metadata.get("last_trade_date_or_contract_month")
    )
    expiry = getattr(bar, "expiry", None) or metadata.get("expiry")
    strike = getattr(bar, "strike", None) or metadata.get("strike")
    right = getattr(bar, "right", None) or metadata.get("right")
    trading_class = getattr(bar, "trading_class", None) or metadata.get("trading_class")
    return {
        "contract_key": ohlcv_contract_key(bar),
        "con_id": _positive_int_or_none(con_id),
        "local_symbol": _optional_upper(local_symbol),
        "contract_month": _optional_upper(contract_month),
        "expiry": _optional_upper(expiry),
        "strike": float(strike) if strike is not None else None,
        "right": _normalize_option_right_or_none(right),
        "trading_class": _optional_upper(trading_class),
        "what_to_show": _optional_upper(metadata.get("what_to_show")),
        "use_rth": metadata.get("use_rth") if isinstance(metadata.get("use_rth"), bool) else None,
    }


def ohlcv_contract_key(bar: Any) -> str:
    metadata = getattr(bar, "metadata", {}) or {}
    con_id = getattr(bar, "con_id", None) or metadata.get("con_id")
    parsed_con_id = _positive_int_or_none(con_id)
    if parsed_con_id is not None:
        return f"conId:{parsed_con_id}"

    local_symbol = getattr(bar, "local_symbol", None) or metadata.get("local_symbol")
    normalized_local_symbol = _optional_upper(local_symbol)
    if normalized_local_symbol:
        return f"localSymbol:{normalized_local_symbol}"

    asset_class = str(bar.asset_class)
    if bar.asset_class is AssetClass.OPTION:
        expiry = getattr(bar, "expiry", None) or metadata.get("expiry") or getattr(bar, "contract_month", None)
        strike = getattr(bar, "strike", None) or metadata.get("strike")
        right = _normalize_option_right_or_none(getattr(bar, "right", None) or metadata.get("right"))
        underlying = getattr(bar, "underlying_symbol", None) or metadata.get("underlying_symbol") or bar.symbol
        return _join_contract_key(
            "option",
            underlying,
            expiry,
            f"{float(strike):g}" if strike is not None else None,
            right,
            bar.exchange,
            bar.currency,
            getattr(bar, "trading_class", None) or metadata.get("trading_class"),
        )

    if bar.asset_class is AssetClass.FUTURE:
        is_continuous = bool(getattr(bar, "is_continuous", False) or metadata.get("is_continuous") or metadata.get("continuous"))
        if is_continuous:
            return _join_contract_key("future", "continuous", bar.symbol, bar.exchange, bar.currency)
        contract_month = (
            getattr(bar, "contract_month", None)
            or metadata.get("contract_month")
            or metadata.get("last_trade_date_or_contract_month")
        )
        return _join_contract_key("future", bar.symbol, contract_month, bar.exchange, bar.currency)

    return _join_contract_key(asset_class, bar.symbol, bar.exchange, bar.currency)


def _join_contract_key(*parts: Any) -> str:
    return ":".join(_contract_key_token(part) for part in parts if _contract_key_token(part))


def _contract_key_token(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper().replace(" ", "_")


def _optional_upper(value: Any) -> str | None:
    token = _contract_key_token(value)
    return token or None


def _positive_int_or_none(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _normalize_option_right_or_none(value: Any) -> str | None:
    normalized = _optional_upper(value)
    if normalized in {"C", "CALL"}:
        return "C"
    if normalized in {"P", "PUT"}:
        return "P"
    return normalized
