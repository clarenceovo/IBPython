"""Pydantic models for equity and FX option snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.options import OptionGreekSet, OptionGreekSource


class EquitySnapshot(BaseModel):
    """Point-in-time equity market data snapshot."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    primary_exchange: str = Field(default="")
    con_id: int = Field(default=0, ge=0)
    timestamp: datetime

    # Price data
    last: float | None = None
    bid: float | None = None
    ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    last_size: float | None = None
    volume: float | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    vwap: float | None = None
    mark_price: float | None = None

    # Derived
    mid_price: float | None = None
    spread: float | None = None
    spread_bps: float | None = None

    # Reference
    halted: bool | None = None
    source: str = Field(default="ibkr_snapshot", min_length=1)

    @field_validator("symbol", "exchange", "currency", "primary_exchange", mode="before")
    @classmethod
    def normalize_upper(cls, v: Any) -> str:
        if v is None:
            return ""
        return str(v).strip().upper()

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp_utc(cls, v: Any) -> datetime:
        if isinstance(v, str):
            v = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc)
        raise TypeError("timestamp must be a datetime")

    @model_validator(mode="after")
    def compute_derived(self) -> EquitySnapshot:
        mid_price: float | None = None
        spread: float | None = None
        spread_bps: float | None = None
        if self.bid is not None and self.ask is not None and self.bid > 0 and self.ask > 0:
            mid_price = (self.bid + self.ask) / 2
            spread = self.ask - self.bid
            if mid_price > 0:
                spread_bps = round((spread / mid_price) * 10_000, 2)
        object.__setattr__(self, "mid_price", mid_price)
        object.__setattr__(self, "spread", spread)
        object.__setattr__(self, "spread_bps", spread_bps)
        return self

    def to_redis_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_redis_json(cls, payload: str | bytes) -> EquitySnapshot:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return cls.model_validate_json(payload)


class SnapshotWatchlist(BaseModel):
    """A named watchlist of equity symbols for periodic snapshotting."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, description="Watchlist name, e.g. 'us_tech', 'hk_large_cap'")
    symbols: tuple[str, ...] = Field(min_length=1, description="Equity symbols to snapshot")
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    snapshot_interval_seconds: float = Field(default=60, gt=0)

    @field_validator("symbols", mode="before")
    @classmethod
    def normalize_symbols(cls, v: Any) -> tuple[str, ...]:
        if isinstance(v, str):
            v = [v]
        return tuple(str(s).strip().upper() for s in v if str(s).strip())


class SnapshotResult(BaseModel):
    """Result of a single snapshot run for one watchlist."""

    model_config = ConfigDict(extra="forbid")

    watchlist_name: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    symbols_requested: int = Field(ge=0)
    symbols_captured: int = Field(ge=0)
    symbols_failed: int = Field(ge=0)
    failed_symbols: tuple[str, ...] = ()
    duration_seconds: float = Field(default=0.0, ge=0)
    snapshots: list[EquitySnapshot] = Field(default_factory=list)


class FXOptionSnapshot(BaseModel):
    """Point-in-time FX option market data and analytics snapshot."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, description="FX pair, for example EURUSD.")
    underlying_symbol: str = Field(min_length=1, description="IBKR option underlying/base currency symbol.")
    expiry: str = Field(min_length=6)
    strike: float = Field(gt=0)
    right: str = Field(min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    multiplier: str | None = None
    trading_class: str | None = None
    local_symbol: str | None = None
    con_id: int | None = Field(default=None, ge=0)
    timestamp: datetime

    last: float | None = None
    bid: float | None = None
    ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    last_size: float | None = None
    volume: float | None = None
    mark_price: float | None = None
    implied_volatility: float | None = None
    historical_volatility: float | None = None
    option_volume: float | None = None
    average_option_volume: float | None = None
    open_interest: float | None = None
    call_open_interest: float | None = None
    put_open_interest: float | None = None
    call_volume: float | None = None
    put_volume: float | None = None
    bid_greeks: OptionGreekSet | None = None
    ask_greeks: OptionGreekSet | None = None
    last_greeks: OptionGreekSet | None = None
    model_greeks: OptionGreekSet | None = None
    mid_price: float | None = None
    spread: float | None = None
    spread_bps: float | None = None
    source: str = Field(default="ibkr_fx_option_snapshot", min_length=1)

    @field_validator("symbol", "underlying_symbol", "right", "exchange", "currency", "multiplier", "trading_class", "local_symbol", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    @field_validator("right")
    @classmethod
    def normalize_right(cls, value: str) -> str:
        if value in {"C", "CALL"}:
            return "C"
        if value in {"P", "PUT"}:
            return "P"
        raise ValueError("right must be C/CALL or P/PUT")

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp_utc(cls, value: Any) -> datetime:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        raise TypeError("timestamp must be a datetime")

    @model_validator(mode="after")
    def compute_derived(self) -> FXOptionSnapshot:
        mid_price: float | None = None
        spread: float | None = None
        spread_bps: float | None = None
        if self.bid is not None and self.ask is not None and self.bid > 0 and self.ask > 0:
            mid_price = (self.bid + self.ask) / 2
            spread = self.ask - self.bid
            if mid_price > 0:
                spread_bps = round((spread / mid_price) * 10_000, 2)
        object.__setattr__(self, "mid_price", mid_price)
        object.__setattr__(self, "spread", spread)
        object.__setattr__(self, "spread_bps", spread_bps)
        return self

    @property
    def contract_key(self) -> str:
        components = [
            self.symbol,
            self.expiry,
            f"{self.strike:g}",
            self.right,
            self.exchange,
            self.local_symbol or "",
            str(self.con_id or ""),
        ]
        return ":".join(component.strip().upper().replace(" ", "_") for component in components if component != "")

    def to_redis_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_redis_json(cls, payload: str | bytes) -> FXOptionSnapshot:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return cls.model_validate_json(payload)


class FXOptionSnapshotQuery(BaseModel):
    """Query parameters for retrieving historical FX option snapshots."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    expiry: str | None = None
    strike: float | None = Field(default=None, gt=0)
    right: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    limit: int = Field(default=1000, ge=1, le=10000)

    @field_validator("symbol", "expiry", "right", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    @field_validator("right")
    @classmethod
    def normalize_optional_right(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value in {"C", "CALL"}:
            return "C"
        if value in {"P", "PUT"}:
            return "P"
        raise ValueError("right must be C/CALL or P/PUT")

    @field_validator("start", "end", mode="before")
    @classmethod
    def normalize_datetime(cls, value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        raise TypeError("must be a datetime")


class SnapshotQuery(BaseModel):
    """Query parameters for retrieving historical snapshots."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    start: datetime | None = None
    end: datetime | None = None
    limit: int = Field(default=1000, ge=1, le=10000)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_upper(cls, v: Any) -> str:
        return str(v).strip().upper()

    @field_validator("start", "end", mode="before")
    @classmethod
    def normalize_datetime(cls, v: Any) -> datetime | None:
        if v is None:
            return None
        if isinstance(v, str):
            v = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc)
        raise TypeError("must be a datetime")
