"""Generic equity snapshot collector — point-in-time market data for a universe of equities.

The snapshotter fetches real-time ticker snapshots from IBKR (bid/ask/last/volume/etc.)
for a configurable watchlist of equity symbols, then persists them to QuestDB and
caches the latest in Redis.

Typical usage:
  - Periodic scheduler job snapshots the current watchlist every N seconds
  - REST endpoint queries the latest or historical snapshots from QuestDB
  - Redis provides sub-millisecond reads for the most recent snapshot per symbol
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.models import AssetClass
from src.feeds.options import OptionContractSpec, OptionGreekSet, OptionGreekSource


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def ticker_to_snapshot(ticker: Any, *, symbol: str, exchange: str, currency: str, primary_exchange: str = "", con_id: int = 0) -> EquitySnapshot:
    """Convert an ib_insync Ticker to an EquitySnapshot."""
    return EquitySnapshot(
        symbol=symbol,
        exchange=exchange,
        currency=currency,
        primary_exchange=primary_exchange,
        con_id=con_id,
        timestamp=datetime.now(timezone.utc),
        last=_safe_float(getattr(ticker, "last", None)),
        bid=_safe_float(getattr(ticker, "bid", None)),
        ask=_safe_float(getattr(ticker, "ask", None)),
        bid_size=_safe_float(getattr(ticker, "bidSize", None)),
        ask_size=_safe_float(getattr(ticker, "askSize", None)),
        last_size=_safe_float(getattr(ticker, "lastSize", None)),
        volume=_safe_float(getattr(ticker, "volume", None)),
        open=_safe_float(getattr(ticker, "open_", None)),
        high=_safe_float(getattr(ticker, "high", None)),
        low=_safe_float(getattr(ticker, "low", None)),
        close=_safe_float(getattr(ticker, "close", None)),
        vwap=_safe_float(getattr(ticker, "vwap", None)),
        mark_price=_safe_float(getattr(ticker, "markPrice", None)),
        halted=getattr(ticker, "halted", None),
    )


def fx_pair_parts(symbol: str, currency: str | None = None) -> tuple[str, str, str]:
    normalized = symbol.replace("/", "").strip().upper()
    if len(normalized) != 6:
        raise ValueError("FX option symbols must be six-character pairs such as EURUSD")
    base = normalized[:3]
    quote = currency.strip().upper() if currency else normalized[3:]
    return normalized, base, quote


def fx_option_contract_key(
    *,
    symbol: str,
    expiry: str,
    strike: float,
    right: str,
    exchange: str = "SMART",
    local_symbol: str | None = None,
    con_id: int | None = None,
) -> str:
    normalized_right = right.strip().upper()
    if normalized_right == "CALL":
        normalized_right = "C"
    elif normalized_right == "PUT":
        normalized_right = "P"
    components = [
        symbol.replace("/", "").strip().upper(),
        expiry.strip().upper(),
        f"{float(strike):g}",
        normalized_right,
        exchange.strip().upper(),
        (local_symbol or "").strip().upper(),
        str(con_id or ""),
    ]
    return ":".join(component.replace(" ", "_") for component in components if component)


def ticker_to_fx_option_snapshot(
    ticker: Any,
    contract: OptionContractSpec,
    *,
    symbol: str,
    timestamp: datetime | None = None,
) -> FXOptionSnapshot:
    return FXOptionSnapshot(
        symbol=symbol,
        underlying_symbol=contract.underlying_symbol,
        expiry=contract.expiry,
        strike=contract.strike,
        right=contract.right.value,
        exchange=contract.exchange,
        currency=contract.currency,
        multiplier=contract.multiplier,
        trading_class=contract.trading_class,
        local_symbol=contract.local_symbol,
        con_id=contract.con_id or getattr(getattr(ticker, "contract", None), "conId", None),
        timestamp=timestamp or datetime.now(timezone.utc),
        last=_safe_float(getattr(ticker, "last", None)),
        bid=_safe_float(getattr(ticker, "bid", None)),
        ask=_safe_float(getattr(ticker, "ask", None)),
        bid_size=_safe_float(getattr(ticker, "bidSize", None)),
        ask_size=_safe_float(getattr(ticker, "askSize", None)),
        last_size=_safe_float(getattr(ticker, "lastSize", None)),
        volume=_safe_float(getattr(ticker, "volume", None)),
        mark_price=_safe_float(getattr(ticker, "markPrice", None)),
        implied_volatility=_safe_float(getattr(ticker, "impliedVolatility", None)),
        historical_volatility=_safe_float(getattr(ticker, "histVolatility", None)),
        average_option_volume=_safe_float(getattr(ticker, "avOptionVolume", None)),
        call_open_interest=_safe_float(getattr(ticker, "callOpenInterest", None)),
        put_open_interest=_safe_float(getattr(ticker, "putOpenInterest", None)),
        call_volume=_safe_float(getattr(ticker, "callVolume", None)),
        put_volume=_safe_float(getattr(ticker, "putVolume", None)),
        open_interest=_sum_optional(
            _safe_float(getattr(ticker, "callOpenInterest", None)),
            _safe_float(getattr(ticker, "putOpenInterest", None)),
        ),
        option_volume=_safe_float(getattr(ticker, "volume", None))
        or _sum_optional(_safe_float(getattr(ticker, "callVolume", None)), _safe_float(getattr(ticker, "putVolume", None))),
        bid_greeks=_normalize_snapshot_greeks(getattr(ticker, "bidGreeks", None), OptionGreekSource.BID),
        ask_greeks=_normalize_snapshot_greeks(getattr(ticker, "askGreeks", None), OptionGreekSource.ASK),
        last_greeks=_normalize_snapshot_greeks(getattr(ticker, "lastGreeks", None), OptionGreekSource.LAST),
        model_greeks=_normalize_snapshot_greeks(getattr(ticker, "modelGreeks", None), OptionGreekSource.MODEL),
    )


def _sum_optional(*values: float | None) -> float | None:
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def _normalize_snapshot_greeks(value: Any, source: OptionGreekSource) -> OptionGreekSet | None:
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
