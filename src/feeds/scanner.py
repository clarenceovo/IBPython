from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ContractSearchRequest(BaseModel):
    """Search for IBKR contracts by symbol pattern or conId."""

    model_config = ConfigDict(extra="forbid")

    symbol: str | None = Field(
        default=None,
        min_length=1,
        description="Symbol pattern to search. Supports prefix matching.",
    )
    sec_type: str | None = Field(
        default=None,
        description="Security type filter: STK, OPT, FUT, CASH, IND, BOND, CRYPTO",
    )
    exchange: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    con_id: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_has_search_criteria(self) -> ContractSearchRequest:
        if not (self.symbol or self.con_id):
            raise ValueError("Must provide symbol or con_id")
        return self


class ContractSearchResult(BaseModel):
    """A single contract match from the scanner."""

    model_config = ConfigDict(extra="forbid")

    con_id: int = Field(gt=0)
    symbol: str
    sec_type: str
    exchange: str
    currency: str
    primary_exchange: str = ""
    local_symbol: str = ""
    long_name: str = ""
    category: str = ""
    subcategory: str = ""
    industry: str = ""
    market_name: str = ""
    min_tick: float = 0.0
    trading_hours: str = ""
    liquid_hours: str = ""
    last_trading_day: str = ""
    multiplier: str = ""
    strike: float | None = None
    right: str = ""
    expiry: str = ""


class ContractScanRequest(BaseModel):
    """Scanner for finding contracts matching criteria across IBKR's database."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, description="Symbol to scan for")
    sec_type: str = Field(
        default="STK",
        description="Security type: STK, OPT, FUT, CASH, IND, BOND, CRYPTO",
    )
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    primary_exchange: str | None = None
    max_results: int = Field(default=20, ge=1, le=100)


class MarketScannerFilter(BaseModel):
    """IBKR market scanner filter tag/value pair."""

    model_config = ConfigDict(extra="forbid")

    code: str = Field(min_length=1, description="IBKR scanner filter code, e.g. priceAbove.")
    value: str | int | float | bool = Field(description="Filter value passed to IBKR as text.")


class MarketScannerRequest(BaseModel):
    """True IBKR market scanner request using ScannerSubscription."""

    model_config = ConfigDict(extra="forbid")

    instrument: str = Field(default="STK", min_length=1, description="Scanner instrument, e.g. STK.")
    location_code: str = Field(
        default="STK.US.MAJOR",
        min_length=1,
        description="Scanner locationCode, e.g. STK.US.MAJOR or STK.HK.",
    )
    scan_code: str = Field(default="HOT_BY_VOLUME", min_length=1, description="Scanner scanCode.")
    max_results: int = Field(default=20, ge=1, le=50, description="IBKR scanner rows; IBKR caps scans at 50.")
    filters: list[MarketScannerFilter] = Field(default_factory=list)


class MarketScannerRow(BaseModel):
    """One ranked row returned by the IBKR market scanner."""

    model_config = ConfigDict(extra="forbid")

    rank: int = Field(ge=0)
    con_id: int = Field(gt=0)
    symbol: str
    sec_type: str = ""
    exchange: str = ""
    currency: str = ""
    primary_exchange: str = ""
    local_symbol: str = ""
    long_name: str = ""
    market_name: str = ""
    distance: str = ""
    benchmark: str = ""
    projection: str = ""
    legs: str = ""
