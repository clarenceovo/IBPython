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

    @field_validator("bar_size", "duration", "what_to_show", "source", mode="before")
    @classmethod
    def normalize_non_empty_text(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("value cannot be empty")
        return normalized

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
        return self


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
