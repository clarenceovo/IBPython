"""Data models for tick-by-tick data, historical ticks, market rules, and option calculations."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TickType(StrEnum):
    """Tick-by-tick subscription types supported by IBKR."""

    LAST = "Last"
    BID_ASK = "BidAsk"
    MIDPOINT = "MidPoint"
    ALL_LAST = "AllLast"


class TickByTickData(BaseModel):
    """A single tick from tick-by-tick streaming or historical tick data."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    timestamp: datetime
    tick_type: TickType
    price: float | None = None
    size: float | None = None
    bid: float | None = None
    ask: float | None = None
    size_bid: float | None = None
    size_ask: float | None = None
    exchange: str | None = None
    special_conditions: str | None = None

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


class HistoricalTickRequest(BaseModel):
    """Request for historical tick-level data from IBKR."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    sec_type: str = Field(default="STK", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    start_date: datetime
    end_date: datetime
    what_to_show: str = Field(default="TRADES", min_length=1)  # TRADES, BID_ASK, MIDPOINT
    use_rth: bool = True
    max_ticks: int = Field(default=10_000, ge=1, le=100_000)

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def normalize_datetime_utc(cls, value: Any) -> datetime:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("datetime fields must be datetimes")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class HistoricalTickResponse(BaseModel):
    """Response containing historical tick data."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str
    ticks: list[TickByTickData]
    total_count: int
    truncated: bool


class HistogramDataPoint(BaseModel):
    """One IBKR price histogram bucket."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    price: float = Field(ge=0)
    size: float = Field(ge=0)


class HistogramDataRequest(BaseModel):
    """Request for IBKR price histogram data."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    sec_type: str = Field(default="STK", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    use_rth: bool = True
    period: str = Field(default="1 week", min_length=1)


class HistogramDataResponse(BaseModel):
    """Response containing IBKR price histogram data."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str
    period: str
    use_rth: bool
    data: list[HistogramDataPoint]
    total_count: int


class PriceIncrement(BaseModel):
    """A single price increment entry from a market rule."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    low_edge: float
    increment: float


class MarketRule(BaseModel):
    """Market rule defining minimum tick increments by price level."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    price_magnitude: int
    increments: list[PriceIncrement]


class SmartComponent(BaseModel):
    """A smart-routing component exchange."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    exchange: str
    con_id: int | None = None
    description: str | None = None


class MarketDepthLevel(BaseModel):
    """One price level from IBKR market depth."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    position: int = Field(ge=0)
    price: float = Field(ge=0)
    size: float = Field(ge=0)
    market_maker: str | None = None

    @field_validator("market_maker", mode="before")
    @classmethod
    def normalize_optional_market_maker(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


class MarketDepthSnapshot(BaseModel):
    """Short-lived DOM/L2 snapshot assembled from a live IBKR depth subscription."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    asset_class: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    currency: str = Field(min_length=1)
    primary_exchange: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    local_symbol: str | None = None
    sec_type: str | None = None
    num_rows: int = Field(ge=1)
    is_smart_depth: bool
    snapshot_wait_seconds: float = Field(gt=0)
    received_at: datetime
    bids: list[MarketDepthLevel]
    asks: list[MarketDepthLevel]
    source: str = Field(default="ibkr", min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol", "exchange", "currency", "primary_exchange", "local_symbol", "sec_type", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized:
            return None
        return normalized.upper()

    @field_validator("asset_class", mode="before")
    @classmethod
    def normalize_asset_class(cls, value: Any) -> str:
        if value is None:
            raise ValueError("asset_class is required")
        normalized = str(value).strip().lower()
        if not normalized:
            raise ValueError("asset_class cannot be empty")
        return normalized

    @field_validator("received_at", mode="before")
    @classmethod
    def normalize_received_at_utc(cls, value: Any) -> datetime:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("received_at must be a datetime")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class SymbolDescription(BaseModel):
    """A symbol match from fuzzy symbol search."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    con_id: int
    symbol: str
    name: str
    sec_type: str
    exchange: str | None = None
    listing_exchange: str | None = None
    industry: str | None = None
    category: str | None = None
    subcategory: str | None = None


class IVCalcRequest(BaseModel):
    """Request for implied volatility calculation."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    sec_type: str = Field(default="OPT", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    expiry: str = Field(min_length=1)
    strike: float = Field(gt=0)
    right: str = Field(min_length=1)  # "C" or "P"
    multiplier: str = Field(default="100")
    option_price: float = Field(gt=0)
    under_price: float = Field(gt=0)


class OptionPriceCalcRequest(BaseModel):
    """Request for option price calculation."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    sec_type: str = Field(default="OPT", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    expiry: str = Field(min_length=1)
    strike: float = Field(gt=0)
    right: str = Field(min_length=1)  # "C" or "P"
    multiplier: str = Field(default="100")
    volatility: float = Field(gt=0)
    under_price: float = Field(gt=0)


class HeadTimestampRequest(BaseModel):
    """Request for the earliest available data date per contract."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    sec_type: str = Field(default="STK", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True


class TickSubscribeRequest(BaseModel):
    """Request to start a tick-by-tick subscription."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    sec_type: str = Field(default="STK", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    tick_type: TickType = Field(default=TickType.ALL_LAST)
    max_ticks: int = Field(default=10_000, ge=100, le=1_000_000)


class TickUnsubscribeRequest(BaseModel):
    """Request to stop a tick-by-tick subscription."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    sec_type: str = Field(default="STK", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)


class HistogramDataPoint(BaseModel):
    """A single price/count bucket from IBKR histogram data."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    price: float
    count: int = Field(ge=0)


class HistogramRequest(BaseModel):
    """Request for IBKR histogram data."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    asset_class: str = Field(default="EQUITY", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    use_rth: bool = True
    time_period: str = Field(default="1 day", min_length=1)
