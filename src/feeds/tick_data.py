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

    symbol: str = Field(min_length=1, description="Instrument symbol, for example 'AAPL', 'ES', 'EUR.USD'.")
    sec_type: str = Field(default="STK", min_length=1, description="IBKR security type: 'STK', 'FUT', 'OPT', 'CASH', 'IND', 'BOND'.")
    exchange: str = Field(default="SMART", min_length=1, description="Exchange or routing destination, for example 'SMART', 'NASDAQ', 'CME', 'IDEALPRO'.")
    currency: str = Field(default="USD", min_length=1, description="IBKR currency code, for example 'USD', 'HKD', 'JPY'.")
    start_date: datetime = Field(description="Window start, UTC ISO-8601.")
    end_date: datetime = Field(description="Window end, UTC ISO-8601.")
    what_to_show: str = Field(
        default="TRADES",
        min_length=1,
        description="IBKR whatToShow value, for example 'TRADES', 'MIDPOINT', 'BID', 'ASK', 'BID_ASK'. Auto-converted to tick format.",
    )  # Bar-data style; auto-converted to tick format for reqHistoricalTicksAsync
    use_rth: bool = Field(default=True, description="True: regular trading hours only. False: include extended hours.")
    max_ticks: int = Field(default=10_000, ge=1, le=100_000, description="Stop after this many ticks. 0 means unlimited.")

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

    symbol: str = Field(min_length=1, description="Instrument symbol, for example 'AAPL', 'ES', 'EUR.USD'.")
    sec_type: str = Field(default="STK", min_length=1, description="IBKR security type: 'STK', 'FUT', 'OPT', 'CASH', 'IND', 'BOND'.")
    exchange: str = Field(default="SMART", min_length=1, description="Exchange or routing destination, for example 'SMART', 'NASDAQ', 'CME', 'IDEALPRO'.")
    currency: str = Field(default="USD", min_length=1, description="IBKR currency code, for example 'USD', 'HKD', 'JPY'.")
    use_rth: bool = Field(default=True, description="True: regular trading hours only. False: include extended hours.")
    period: str = Field(default="1 week", min_length=1, description="Lookback period, for example '1 D'.")


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

    symbol: str = Field(min_length=1, description="Instrument symbol, for example 'AAPL', 'ES', 'EUR.USD'.")
    sec_type: str = Field(default="OPT", min_length=1, description="IBKR security type: 'STK', 'FUT', 'OPT', 'CASH', 'IND', 'BOND'.")
    exchange: str = Field(default="SMART", min_length=1, description="Exchange or routing destination, for example 'SMART', 'NASDAQ', 'CME', 'IDEALPRO'.")
    currency: str = Field(default="USD", min_length=1, description="IBKR currency code, for example 'USD', 'HKD', 'JPY'.")
    expiry: str = Field(min_length=1, description="Option or future expiry date, format YYYYMMDD.")
    strike: float = Field(gt=0, description="Option strike price.")
    right: str = Field(min_length=1, description="Option right: 'C'/'CALL' or 'P'/'PUT' (case-insensitive).")  # "C" or "P"
    multiplier: str = Field(default="100", description="Contract multiplier, for example 100 for equity options.")
    option_price: float = Field(gt=0, description="Option market price used to solve implied volatility.")
    under_price: float = Field(gt=0, description="Underlying price used in the calculation.")


class OptionPriceCalcRequest(BaseModel):
    """Request for option price calculation."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, description="Instrument symbol, for example 'AAPL', 'ES', 'EUR.USD'.")
    sec_type: str = Field(default="OPT", min_length=1, description="IBKR security type: 'STK', 'FUT', 'OPT', 'CASH', 'IND', 'BOND'.")
    exchange: str = Field(default="SMART", min_length=1, description="Exchange or routing destination, for example 'SMART', 'NASDAQ', 'CME', 'IDEALPRO'.")
    currency: str = Field(default="USD", min_length=1, description="IBKR currency code, for example 'USD', 'HKD', 'JPY'.")
    expiry: str = Field(min_length=1, description="Option or future expiry date, format YYYYMMDD.")
    strike: float = Field(gt=0, description="Option strike price.")
    right: str = Field(min_length=1, description="Option right: 'C'/'CALL' or 'P'/'PUT' (case-insensitive).")  # "C" or "P"
    multiplier: str = Field(default="100", description="Contract multiplier, for example 100 for equity options.")
    volatility: float = Field(gt=0, description="Volatility as a decimal (0.20 = 20%) used to solve the option price.")
    under_price: float = Field(gt=0, description="Underlying price used in the calculation.")


class HeadTimestampRequest(BaseModel):
    """Request for the earliest available data date per contract."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    sec_type: str = Field(default="STK", min_length=1)
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1, description="IBKR whatToShow value, for example 'TRADES', 'MIDPOINT', 'BID', 'ASK', 'BID_ASK'.")
    use_rth: bool = True


class TickSubscribeRequest(BaseModel):
    """Request to start a tick-by-tick subscription."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, description="Instrument symbol, for example 'AAPL', 'ES', 'EUR.USD'.")
    sec_type: str = Field(default="STK", min_length=1, description="IBKR security type: 'STK', 'FUT', 'OPT', 'CASH', 'IND', 'BOND'.")
    exchange: str = Field(default="SMART", min_length=1, description="Exchange or routing destination, for example 'SMART', 'NASDAQ', 'CME', 'IDEALPRO'.")
    currency: str = Field(default="USD", min_length=1, description="IBKR currency code, for example 'USD', 'HKD', 'JPY'.")
    tick_type: TickType = Field(default=TickType.ALL_LAST, description="Tick stream type: 'Last', 'BidAsk', 'MidPoint', or 'AllLast'.")
    max_ticks: int = Field(default=10_000, ge=100, le=1_000_000, description="Stop after this many ticks. 0 means unlimited.")


class TickUnsubscribeRequest(BaseModel):
    """Request to stop a tick-by-tick subscription."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, description="Instrument symbol, for example 'AAPL', 'ES', 'EUR.USD'.")
    sec_type: str = Field(default="STK", min_length=1, description="IBKR security type: 'STK', 'FUT', 'OPT', 'CASH', 'IND', 'BOND'.")
    exchange: str = Field(default="SMART", min_length=1, description="Exchange or routing destination, for example 'SMART', 'NASDAQ', 'CME', 'IDEALPRO'.")

