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


class OHLCVBar(BaseModel):
    """Vendor-neutral OHLCV bar with UTC timestamps."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    asset_class: AssetClass
    exchange: str = Field(min_length=1)
    currency: str = Field(min_length=1)
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = Field(ge=0)
    bar_size: str = Field(min_length=1)
    source: str = Field(default="ibkr", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol", "exchange", "currency", mode="before")
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
        return json.dumps(self.metadata, sort_keys=True, default=str)


class OHLCVRequest(BaseModel):
    """Historical OHLCV request independent of the downstream provider."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    asset_class: AssetClass
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
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

    @field_validator("end_datetime", mode="before")
    @classmethod
    def normalize_end_datetime_utc(cls, value: Any) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("end_datetime must be a datetime")
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

    @model_validator(mode="after")
    def validate_contract_identifiers(self) -> Self:
        if self.asset_class is AssetClass.FUTURE and not (
            self.last_trade_date_or_contract_month or self.local_symbol or self.con_id
        ):
            raise ValueError("future OHLCV requests require last_trade_date_or_contract_month, local_symbol, or con_id")
        if self.asset_class is AssetClass.BOND and not (self.symbol or self.sec_id or self.con_id):
            raise ValueError("bond OHLCV requests require symbol, sec_id, or con_id")
        return self
