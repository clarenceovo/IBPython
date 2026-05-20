from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.bonds import BondYieldBar, BondYieldHistoryRequest
from src.feeds.contracts import ContractSpec
from src.feeds.exchange_resolver import resolve_equity
from src.feeds.models import AssetClass, FXOHLCVBar, FutureOHLCVBar, OHLCVBar, OHLCVRequest, OptionOHLCVBar
from src.feeds.news import HistoricalNewsHeadline, HistoricalNewsRequest, NewsArticle, NewsArticleRequest
from src.feeds.options import OptionAnalyticsRequest, OptionAnalyticsSnapshot, OptionContractSpec, OptionRight, OptionSkewSurfaceRequest, OptionSkewSurfaceResponse
from src.feeds.snapshotter import fx_pair_parts
from src.feeds.tick_data import HeadTimestampRequest, HistoricalTickRequest, HistoricalTickResponse, MarketRule
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/market-data", tags=["market-data"])


class HistoricalOHLCVLoadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OHLCVRequest
    persist: bool = False
    cache_latest: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


class MinimalOHLCVLoadControls(BaseModel):
    """Common wrapper controls with production defaults."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    start_datetime: datetime | None = Field(
        default=None,
        description="Start of the date range (inclusive). When set with end_datetime, the wrapper paginates automatically.",
    )
    end_datetime: datetime | None = Field(
        default=None,
        description="End of the date range (inclusive). Defaults to now.",
    )
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True
    persist: bool = False
    cache_latest: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)
    metadata: dict[str, object] = Field(default_factory=dict)

    @field_validator("start_datetime", "end_datetime", mode="before")
    @classmethod
    def normalize_datetime_utc(cls, value: object) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("datetime fields must be datetimes")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("duration", "bar_size", "what_to_show", mode="before")
    @classmethod
    def normalize_required_text(cls, value: object) -> str:
        if value is None:
            raise ValueError("value is required")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("value cannot be empty")
        return normalized

    @model_validator(mode="after")
    def validate_datetime_range(self) -> "MinimalOHLCVLoadControls":
        if self.start_datetime is not None and self.end_datetime is not None and self.start_datetime >= self.end_datetime:
            raise ValueError("start_datetime must be before end_datetime")
        return self

    def to_ohlcv_request(self, asset_class: AssetClass, **overrides: object) -> OHLCVRequest:
        metadata = overrides.pop("metadata", self.metadata)
        return OHLCVRequest(
            asset_class=asset_class,
            duration=self.duration,
            bar_size=self.bar_size,
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
            what_to_show=self.what_to_show,
            use_rth=self.use_rth,
            metadata=metadata,
            **overrides,
        )


class EquityOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str = Field(
        min_length=1,
        examples=["SPY", "0700.HK", "7203.T", "TSLA"],
        description=(
            "Ticker symbol with optional exchange suffix. "
            "Supported suffixes: .HK (HKEX), .T (TSE), .L (LSE), .F/.DE (Xetra), "
            ".PA (Euronext Paris), .AS (Amsterdam), .MC (Madrid), .MI (Milan), "
            ".SW (SIX), .TO (TSX), .AX (ASX), .SI (SGX), .NS/.BO (India), "
            ".KS (Korea), .SS (Shanghai), .SZ (Shenzhen), .MX (Mexico), .SA (Brazil). "
            "No suffix defaults to SMART/USD (US equity)."
        ),
    )
    exchange: str | None = Field(
        default=None,
        min_length=1,
        description="Override auto-detected exchange. Leave empty to auto-resolve from symbol suffix.",
    )
    currency: str | None = Field(
        default=None,
        min_length=1,
        description="Override auto-detected currency. Leave empty to auto-resolve from symbol suffix.",
    )
    primary_exchange: str | None = Field(
        default=None,
        examples=["ARCA"],
        description="Override auto-detected primary exchange. Leave empty to auto-resolve.",
    )

    def to_request(self) -> OHLCVRequest:
        resolved = resolve_equity(self.symbol)
        exchange = self.exchange or resolved.exchange
        currency = self.currency or resolved.currency
        primary = self.primary_exchange or resolved.primary_exchange or None
        return self.to_ohlcv_request(
            AssetClass.EQUITY,
            symbol=resolved.symbol,
            exchange=exchange,
            currency=currency,
            primary_exchange=primary,
        )


class FutureOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str = Field(min_length=1, examples=["ES"])
    exchange: str = Field(
        default="CME",
        min_length=1,
        description=(
            "IBKR futures exchange code. Common codes: "
            "CME (ES, NQ, RTY), CBOT (YM), CFE (VX), HKFE (HSI, HTI), "
            "OSE.JPN (N225, N225M), SGX (XINA), KSE (K200), TAIFEX (TX, MTX), "
            "EUREX (DAX, ESTX50), ICEEU (FTSE 100), MONEP (FCE)."
        ),
    )
    currency: str = Field(default="USD", min_length=1)
    last_trade_date_or_contract_month: str | None = Field(default=None, examples=["202606"])
    multiplier: str | None = Field(default=None, examples=["50"])
    local_symbol: str | None = Field(default=None, examples=["ESM6"])
    con_id: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_future_identifier(self) -> "FutureOHLCVLoadRequest":
        if not (self.last_trade_date_or_contract_month or self.local_symbol or self.con_id):
            raise ValueError("futures wrapper requires last_trade_date_or_contract_month, local_symbol, or con_id")
        return self

    def to_request(self) -> OHLCVRequest:
        return self.to_ohlcv_request(
            AssetClass.FUTURE,
            symbol=self.symbol,
            exchange=self.exchange,
            currency=self.currency,
            last_trade_date_or_contract_month=self.last_trade_date_or_contract_month,
            multiplier=self.multiplier,
            local_symbol=self.local_symbol,
            con_id=self.con_id,
        )


_COMMODITY_FUTURES_PRESETS: dict[str, tuple[str, str]] = {
    "CL": ("NYMEX", "USD"),
    "NG": ("NYMEX", "USD"),
    "GC": ("COMEX", "USD"),
    "SI": ("COMEX", "USD"),
    "HG": ("COMEX", "USD"),
    "ZC": ("CBOT", "USD"),
    "ZS": ("CBOT", "USD"),
    "ZW": ("CBOT", "USD"),
    "ZL": ("CBOT", "USD"),
    "ZM": ("CBOT", "USD"),
}


def _resolve_commodity_future(symbol: str) -> dict[str, str]:
    upper = symbol.strip().upper()
    exchange, currency = _COMMODITY_FUTURES_PRESETS.get(upper, ("NYMEX", "USD"))
    return {"symbol": upper, "exchange": exchange, "currency": currency}


class CommodityOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str = Field(min_length=1, examples=["CL", "GC", "NG"])
    exchange: str | None = Field(default=None, min_length=1, description="Override the commodity preset exchange.")
    currency: str | None = Field(default=None, min_length=1, description="Override the commodity preset currency.")
    last_trade_date_or_contract_month: str | None = Field(default=None, examples=["202606"])
    multiplier: str | None = Field(default=None, examples=["1000"])
    local_symbol: str | None = Field(default=None, examples=["CLM6"])
    con_id: int | None = Field(default=None, gt=0)
    use_rth: bool = False

    @model_validator(mode="after")
    def validate_future_identifier(self) -> "CommodityOHLCVLoadRequest":
        if not (self.last_trade_date_or_contract_month or self.local_symbol or self.con_id):
            raise ValueError("commodity OHLCV requires last_trade_date_or_contract_month, local_symbol, or con_id")
        return self

    def to_request(self) -> OHLCVRequest:
        resolved = _resolve_commodity_future(self.symbol)
        return self.to_ohlcv_request(
            AssetClass.FUTURE,
            symbol=resolved["symbol"],
            exchange=self.exchange or resolved["exchange"],
            currency=self.currency or resolved["currency"],
            last_trade_date_or_contract_month=self.last_trade_date_or_contract_month,
            multiplier=self.multiplier,
            local_symbol=self.local_symbol,
            con_id=self.con_id,
            metadata={**self.metadata, "market": "commodity"},
        )


class CommodityOptionOHLCVLoadRequest(MinimalOHLCVLoadControls):
    underlying_symbol: str = Field(min_length=1, examples=["CL"])
    expiry: str = Field(min_length=6, examples=["20260617"])
    strike: float = Field(gt=0, examples=[80.0])
    right: str = Field(min_length=1, examples=["C"])
    exchange: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    multiplier: str | None = Field(default=None, examples=["1000"])
    trading_class: str | None = Field(default=None, examples=["LO"])
    local_symbol: str | None = Field(default=None)
    con_id: int | None = Field(default=None, gt=0)
    use_rth: bool = False

    @field_validator("right", mode="before")
    @classmethod
    def normalize_right(cls, value: object) -> str:
        normalized = str(value).strip().upper()
        if normalized in {"C", "CALL"}:
            return "C"
        if normalized in {"P", "PUT"}:
            return "P"
        raise ValueError("right must be C/CALL or P/PUT")

    def to_request(self) -> OHLCVRequest:
        resolved = _resolve_commodity_future(self.underlying_symbol)
        symbol = self.local_symbol or f"{resolved['symbol']} {self.expiry}{self.right}{self.strike:g}"
        return self.to_ohlcv_request(
            AssetClass.OPTION,
            symbol=symbol,
            exchange=self.exchange or resolved["exchange"],
            currency=self.currency or resolved["currency"],
            option_sec_type="FOP",
            underlying_symbol=resolved["symbol"],
            expiry=self.expiry,
            strike=self.strike,
            right=self.right,
            multiplier=self.multiplier,
            trading_class=self.trading_class,
            local_symbol=self.local_symbol,
            con_id=self.con_id,
            metadata={**self.metadata, "market": "commodity", "option_sec_type": "FOP"},
        )

    def to_option_contract_spec(self) -> OptionContractSpec:
        resolved = _resolve_commodity_future(self.underlying_symbol)
        return OptionContractSpec(
            sec_type="FOP",
            underlying_symbol=resolved["symbol"],
            expiry=self.expiry,
            strike=self.strike,
            right=OptionRight(self.right),
            exchange=self.exchange or resolved["exchange"],
            currency=self.currency or resolved["currency"],
            multiplier=self.multiplier or "100",
            trading_class=self.trading_class,
            local_symbol=self.local_symbol,
            con_id=self.con_id,
        )


class FXOptionOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str = Field(min_length=6, examples=["EURUSD"])
    expiry: str = Field(min_length=6, examples=["20260619"])
    strike: float = Field(gt=0, examples=[1.10])
    right: str = Field(min_length=1, examples=["C"])
    exchange: str = Field(default="SMART", min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    multiplier: str | None = Field(default=None, examples=["100000"])
    trading_class: str | None = None
    local_symbol: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    use_rth: bool = False

    @field_validator("right", mode="before")
    @classmethod
    def normalize_right(cls, value: object) -> str:
        normalized = str(value).strip().upper()
        if normalized in {"C", "CALL"}:
            return "C"
        if normalized in {"P", "PUT"}:
            return "P"
        raise ValueError("right must be C/CALL or P/PUT")

    def to_request(self) -> OHLCVRequest:
        pair, base, quote = fx_pair_parts(self.symbol, self.currency)
        symbol = self.local_symbol or f"{pair} {self.expiry}{self.right}{self.strike:g}"
        return self.to_ohlcv_request(
            AssetClass.OPTION,
            symbol=symbol,
            exchange=self.exchange,
            currency=quote,
            option_sec_type="OPT",
            underlying_symbol=base,
            expiry=self.expiry,
            strike=self.strike,
            right=self.right,
            multiplier=self.multiplier,
            trading_class=self.trading_class,
            local_symbol=self.local_symbol,
            con_id=self.con_id,
            metadata={**self.metadata, "market": "fx_option", "pair": pair, "option_sec_type": "OPT"},
        )


class FXOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str = Field(min_length=1, examples=["EURUSD"])
    exchange: str = Field(default="IDEALPRO", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    what_to_show: str = Field(default="MIDPOINT", min_length=1)
    use_rth: bool = False

    def to_request(self) -> OHLCVRequest:
        return self.to_ohlcv_request(
            AssetClass.FX,
            symbol=self.symbol,
            exchange=self.exchange,
            currency=self.currency,
        )


class BondOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str | None = Field(default=None, min_length=1, examples=["91282CJN2"])
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    sec_id_type: str | None = Field(default=None, examples=["CUSIP"])
    sec_id: str | None = Field(default=None, examples=["91282CJN2"])
    con_id: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_bond_identifier(self) -> "BondOHLCVLoadRequest":
        if not (self.symbol or self.sec_id or self.con_id):
            raise ValueError("bond wrapper requires symbol, sec_id, or con_id")
        return self

    def to_request(self) -> OHLCVRequest:
        symbol = self.symbol or self.sec_id or str(self.con_id)
        return self.to_ohlcv_request(
            AssetClass.BOND,
            symbol=symbol,
            exchange=self.exchange,
            currency=self.currency,
            sec_id_type=self.sec_id_type,
            sec_id=self.sec_id,
            con_id=self.con_id,
        )


class IndexOHLCVLoadRequest(MinimalOHLCVLoadControls):
    """Wrapper for index OHLCV — SPX, NDX, DAX, HSI, etc."""

    symbol: str = Field(
        min_length=1,
        examples=["SPX", "NDX", "HSI", "DAX"],
        description=(
            "Index symbol as recognized by IBKR. "
            "SPX → CBOE/USD, NDX → CBOE/USD, HSI → SEHK/HKD, DAX → EUREX/EUR. "
            "Use the exchange/currency overrides for less common indices."
        ),
    )
    exchange: str | None = Field(
        default=None,
        min_length=1,
        description="Override auto-detected exchange. Leave empty to auto-resolve from symbol.",
    )
    currency: str | None = Field(
        default=None,
        min_length=1,
        description="Override auto-detected currency. Leave empty to auto-resolve.",
    )

    def to_request(self) -> OHLCVRequest:
        resolved = _resolve_index(self.symbol)
        return self.to_ohlcv_request(
            AssetClass.INDEX,
            symbol=resolved["symbol"],
            exchange=self.exchange or resolved["exchange"],
            currency=self.currency or resolved["currency"],
        )


class CachedOptionAnalyticsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OptionAnalyticsRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


class CachedOptionSkewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OptionSkewSurfaceRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=60, ge=0)


class CachedBondYieldHistoryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: BondYieldHistoryRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


class CommodityOptionAnalyticsLoadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract: CommodityOptionOHLCVLoadRequest
    generic_ticks: tuple[str, ...] | list[str] = ("100", "101", "104", "105", "106")
    snapshot_wait_seconds: float = Field(default=2.0, gt=0)
    regulatory_snapshot: bool = False
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)

    def to_request(self) -> OptionAnalyticsRequest:
        return OptionAnalyticsRequest(
            contract=self.contract.to_option_contract_spec(),
            generic_ticks=tuple(self.generic_ticks),
            snapshot_wait_seconds=self.snapshot_wait_seconds,
            regulatory_snapshot=self.regulatory_snapshot,
        )


class CommodityMetadataRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, examples=["CL"])
    exchange: str | None = None
    currency: str | None = None
    last_trade_date_or_contract_month: str | None = Field(default=None, examples=["202606"])
    multiplier: str | None = None
    local_symbol: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = False
    include_head_timestamp: bool = True
    include_trading_schedule: bool = False
    include_market_rules: bool = True
    schedule_date: date | None = None
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)

    @model_validator(mode="after")
    def validate_future_identifier(self) -> "CommodityMetadataRequest":
        if not (self.last_trade_date_or_contract_month or self.local_symbol or self.con_id):
            raise ValueError("commodity metadata requires last_trade_date_or_contract_month, local_symbol, or con_id")
        return self

    def to_ohlcv_request(self) -> OHLCVRequest:
        resolved = _resolve_commodity_future(self.symbol)
        return OHLCVRequest(
            symbol=resolved["symbol"],
            asset_class=AssetClass.FUTURE,
            exchange=self.exchange or resolved["exchange"],
            currency=self.currency or resolved["currency"],
            last_trade_date_or_contract_month=self.last_trade_date_or_contract_month,
            multiplier=self.multiplier,
            local_symbol=self.local_symbol,
            con_id=self.con_id,
            what_to_show=self.what_to_show,
            use_rth=self.use_rth,
            metadata={"market": "commodity"},
        )

    def to_contract_spec(self) -> ContractSpec:
        return ContractSpec.from_ohlcv_request(self.to_ohlcv_request())


class CommodityMetadataResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    exchange: str
    currency: str
    con_id: int | None = None
    local_symbol: str | None = None
    trading_class: str | None = None
    min_tick: float | None = None
    market_rule_ids: tuple[int, ...] = ()
    head_timestamp: datetime | None = None
    trading_sessions: tuple[dict[str, Any], ...] = ()
    market_rules: tuple[MarketRule, ...] = ()
    source: str = "ibkr"


class CommodityHistoricalTicksRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, examples=["CL"])
    exchange: str | None = None
    currency: str | None = None
    last_trade_date_or_contract_month: str | None = Field(default=None, examples=["202606"])
    local_symbol: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    start_date: datetime
    end_date: datetime
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = False
    max_ticks: int = Field(default=10_000, ge=1, le=100_000)

    @model_validator(mode="after")
    def validate_future_identifier(self) -> "CommodityHistoricalTicksRequest":
        if not (self.last_trade_date_or_contract_month or self.local_symbol or self.con_id):
            raise ValueError("commodity historical ticks require last_trade_date_or_contract_month, local_symbol, or con_id")
        return self

    def to_request(self) -> HistoricalTickRequest:
        resolved = _resolve_commodity_future(self.symbol)
        return HistoricalTickRequest(
            symbol=resolved["symbol"],
            sec_type="FUT",
            exchange=self.exchange or resolved["exchange"],
            currency=self.currency or resolved["currency"],
            start_date=self.start_date,
            end_date=self.end_date,
            what_to_show=self.what_to_show,
            use_rth=self.use_rth,
            max_ticks=self.max_ticks,
        )


class CommodityNewsRequest(CommodityMetadataRequest):
    provider_codes: tuple[str, ...] | None = None
    start_datetime: datetime | None = None
    end_datetime: datetime | None = None
    max_results: int = Field(default=50, ge=1, le=300)
    include_articles: bool = False


class CommodityNewsHeadline(BaseModel):
    model_config = ConfigDict(extra="forbid")

    headline: HistoricalNewsHeadline
    article: NewsArticle | None = None


class CommodityNewsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    con_id: int
    provider_codes: tuple[str, ...]
    headlines: tuple[CommodityNewsHeadline, ...]
    source: str = "ibkr_news"


GENERIC_OHLCV_REQUEST_EXAMPLES = {
    "spy_equity_full_request": {
        "summary": "Generic SPY equity OHLCV",
        "description": "Advanced endpoint that accepts the full OHLCVRequest payload.",
        "value": {
            "request": {
                "symbol": "SPY",
                "asset_class": "equity",
                "exchange": "SMART",
                "currency": "USD",
                "start_datetime": "2026-05-01T13:30:00Z",
                "end_datetime": "2026-05-01T20:00:00Z",
                "duration": "1 D",
                "bar_size": "1 min",
                "what_to_show": "TRADES",
                "use_rth": True,
            },
            "persist": False,
            "cache_latest": True,
            "use_ttl_cache": True,
        },
    }
}


EQUITY_OHLCV_REQUEST_EXAMPLES = {
    "minimal_spy": {
        "summary": "Minimal SPY equity bars",
        "description": "Only symbol is required. Auto-resolves to SMART/USD/ARCA.",
        "value": {"symbol": "SPY"},
    },
    "tsla_nasdaq_auto": {
        "summary": "TSLA auto-resolved",
        "description": "TSLA auto-resolves to SMART/USD/NASDAQ via exchange resolver.",
        "value": {"symbol": "TSLA"},
    },
    "hk_stock_0700": {
        "summary": "0700.HK (Tencent)",
        "description": ".HK suffix auto-resolves to SEHK/HKD.",
        "value": {"symbol": "0700.HK"},
    },
    "jp_stock_toyota": {
        "summary": "7203.T (Toyota)",
        "description": ".T suffix auto-resolves to TSEJ/JPY.",
        "value": {"symbol": "7203.T"},
    },
    "nasdaq_equity_with_primaryExchange": {
        "summary": "NASDAQ-listed equity (explicit override)",
        "description": "Use primary_exchange when the auto-resolver doesn't match and SMART needs disambiguation.",
        "value": {
            "symbol": "TSLA",
            "primary_exchange": "NASDAQ",
            "start_datetime": "2026-05-01T13:30:00Z",
            "end_datetime": "2026-05-01T20:00:00Z",
            "duration": "1 D",
            "bar_size": "5 mins",
            "cache_latest": True,
        },
    },
    "london_stock": {
        "summary": "HSBA.L (HSBC London)",
        "description": ".L suffix auto-resolves to LSE/GBP.",
        "value": {"symbol": "HSBA.L"},
    },
    "shanghai_connect": {
        "summary": "600519.SS (Kweichow Moutai)",
        "description": ".SS suffix auto-resolves to SEHKNTL/CNH (Stock Connect).",
        "value": {"symbol": "600519.SS"},
    },
}


FUTURES_OHLCV_REQUEST_EXAMPLES = {
    "es_by_contract_month": {
        "summary": "ES future by contract month",
        "description": "Futures require an expiry/contract month, local_symbol, or con_id.",
        "value": {
            "symbol": "ES",
            "exchange": "CME",
            "currency": "USD",
            "last_trade_date_or_contract_month": "202606",
            "start_datetime": "2026-05-01T13:30:00Z",
            "end_datetime": "2026-05-01T20:00:00Z",
            "duration": "1 D",
            "bar_size": "1 min",
        },
    },
    "es_by_local_symbol": {
        "summary": "ES future by local symbol",
        "description": "Use local_symbol when that is how the contract is represented in TWS.",
        "value": {
            "symbol": "ES",
            "exchange": "CME",
            "local_symbol": "ESM6",
            "multiplier": "50",
        },
    },
    "hsi_hkfe_by_contract_month": {
        "summary": "Hang Seng Index future",
        "description": "HKEX Hang Seng Index futures use product code HSI. Use HKFE/HKD for IBKR routing.",
        "value": {
            "symbol": "HSI",
            "exchange": "HKFE",
            "currency": "HKD",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "hstech_hkfe_by_contract_month": {
        "summary": "Hang Seng TECH Index future",
        "description": "HKEX Hang Seng TECH Index futures use product code HTI. Use HKFE/HKD for IBKR routing.",
        "value": {
            "symbol": "HTI",
            "exchange": "HKFE",
            "currency": "HKD",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "n225m_ose_by_contract_month": {
        "summary": "Nikkei 225 Mini future",
        "description": "Nikkei 225 Mini futures on OSE.JPN. Use N225M/JPY.",
        "value": {
            "symbol": "N225M",
            "exchange": "OSE.JPN",
            "currency": "JPY",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "xina_sgx_by_contract_month": {
        "summary": "SGX FTSE China A50 future",
        "description": "SGX FTSE China A50 futures. Symbol XINA on SGX/USD.",
        "value": {
            "symbol": "XINA",
            "exchange": "SGX",
            "currency": "USD",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "k200_kse_by_contract_month": {
        "summary": "KOSPI 200 future",
        "description": "KOSPI 200 futures on KSE. Symbol K200/KRW.",
        "value": {
            "symbol": "K200",
            "exchange": "KSE",
            "currency": "KRW",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "tx_taifex_by_contract_month": {
        "summary": "TAIEX future (Taiwan)",
        "description": "TAIEX futures on TAIFEX. Symbol TX/TWD. Mini is MTX.",
        "value": {
            "symbol": "TX",
            "exchange": "TAIFEX",
            "currency": "TWD",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "nq_cme_by_contract_month": {
        "summary": "Nasdaq 100 E-mini future",
        "description": "Nasdaq 100 E-mini on CME. Symbol NQ/USD.",
        "value": {
            "symbol": "NQ",
            "exchange": "CME",
            "currency": "USD",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "ym_cbot_by_contract_month": {
        "summary": "Dow Jones E-mini future",
        "description": "Dow Jones E-mini on CBOT. Symbol YM/USD.",
        "value": {
            "symbol": "YM",
            "exchange": "CBOT",
            "currency": "USD",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "rty_cme_by_contract_month": {
        "summary": "Russell 2000 E-mini future",
        "description": "Russell 2000 E-mini on CME. Symbol RTY/USD, $50/pt multiplier.",
        "value": {
            "symbol": "RTY",
            "exchange": "CME",
            "currency": "USD",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "vx_cfe_by_contract_month": {
        "summary": "VIX future",
        "description": "CBOE VIX futures. Symbol VX/USD on CFE.",
        "value": {
            "symbol": "VX",
            "exchange": "CFE",
            "currency": "USD",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "z_iceeu_ftse100": {
        "summary": "FTSE 100 Index future (UK)",
        "description": "ICE Futures Europe FTSE 100. Symbol Z/GBP on ICEEU.",
        "value": {
            "symbol": "Z",
            "exchange": "ICEEU",
            "currency": "GBP",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "fce_monep_cac40": {
        "summary": "CAC 40 Index future (France)",
        "description": "Euronext Paris CAC 40 futures. Symbol FCE/EUR on MONEP.",
        "value": {
            "symbol": "FCE",
            "exchange": "MONEP",
            "currency": "EUR",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "dax_eurex_by_contract_month": {
        "summary": "DAX 40 future (Germany)",
        "description": "Eurex DAX 40 futures. Symbol DAX/EUR on EUREX.",
        "value": {
            "symbol": "DAX",
            "exchange": "EUREX",
            "currency": "EUR",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
    "estx50_eurex_by_contract_month": {
        "summary": "Euro Stoxx 50 future",
        "description": "Eurex Euro Stoxx 50 futures. Symbol ESTX50/EUR on EUREX.",
        "value": {
            "symbol": "ESTX50",
            "exchange": "EUREX",
            "currency": "EUR",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
        },
    },
}


COMMODITY_OHLCV_REQUEST_EXAMPLES = {
    "cl_crude_nymex": {
        "summary": "CL crude oil future",
        "description": "CL auto-resolves to NYMEX/USD and uses futures OHLCV under the hood.",
        "value": {
            "symbol": "CL",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "1 min",
        },
    },
    "gc_gold_comex": {
        "summary": "GC gold future",
        "description": "GC auto-resolves to COMEX/USD.",
        "value": {
            "symbol": "GC",
            "last_trade_date_or_contract_month": "202606",
            "duration": "1 D",
            "bar_size": "5 mins",
        },
    },
    "ng_by_local_symbol": {
        "summary": "NG natural gas by local symbol",
        "description": "Use local_symbol when that is how the contract is represented in TWS.",
        "value": {"symbol": "NG", "local_symbol": "NGM6"},
    },
}


COMMODITY_OPTION_OHLCV_REQUEST_EXAMPLES = {
    "cl_fop_call": {
        "summary": "CL futures option OHLCV",
        "description": "Commodity options use IBKR secType=FOP and return OptionOHLCVBar.",
        "value": {
            "underlying_symbol": "CL",
            "expiry": "20260617",
            "strike": 80.0,
            "right": "C",
            "exchange": "NYMEX",
            "multiplier": "1000",
            "duration": "1 D",
            "bar_size": "1 day",
        },
    }
}


COMMODITY_OPTION_ANALYTICS_EXAMPLES = {
    "cl_fop_greeks": {
        "summary": "CL futures option Greeks/OI",
        "description": "Uses a short-lived IBKR market-data subscription because generic ticks are requested.",
        "value": {
            "contract": {
                "underlying_symbol": "CL",
                "expiry": "20260617",
                "strike": 80.0,
                "right": "C",
                "exchange": "NYMEX",
                "multiplier": "1000",
            },
            "snapshot_wait_seconds": 2.0,
            "use_ttl_cache": True,
        },
    }
}


COMMODITY_METADATA_EXAMPLES = {
    "cl_metadata": {
        "summary": "CL contract metadata",
        "value": {
            "symbol": "CL",
            "last_trade_date_or_contract_month": "202606",
            "include_head_timestamp": True,
            "include_trading_schedule": True,
            "schedule_date": "2026-05-18",
        },
    }
}


COMMODITY_HISTORICAL_TICKS_EXAMPLES = {
    "cl_historical_ticks": {
        "summary": "CL historical trade ticks",
        "value": {
            "symbol": "CL",
            "last_trade_date_or_contract_month": "202606",
            "start_date": "2026-05-18T13:30:00Z",
            "end_date": "2026-05-18T14:30:00Z",
            "what_to_show": "TRADES",
            "max_ticks": 1000,
        },
    }
}


COMMODITY_NEWS_EXAMPLES = {
    "cl_news": {
        "summary": "CL historical news",
        "value": {
            "symbol": "CL",
            "last_trade_date_or_contract_month": "202606",
            "start_datetime": "2026-05-01T00:00:00Z",
            "end_datetime": "2026-05-18T00:00:00Z",
            "max_results": 25,
        },
    }
}


FX_OPTION_OHLCV_REQUEST_EXAMPLES = {
    "eurusd_call": {
        "summary": "EURUSD FX option call",
        "description": "Pair-style FX option OHLCV. EURUSD maps to underlying_symbol=EUR and currency=USD.",
        "value": {
            "symbol": "EURUSD",
            "expiry": "20260619",
            "strike": 1.10,
            "right": "C",
            "duration": "1 D",
            "bar_size": "1 day",
        },
    },
    "eurusd_put_with_local_symbol": {
        "summary": "EURUSD FX option by local symbol",
        "description": "Use local_symbol or con_id when IBKR needs exact contract disambiguation.",
        "value": {
            "symbol": "EURUSD",
            "expiry": "20260619",
            "strike": 1.05,
            "right": "P",
            "local_symbol": "EURUSD  260619P00001050",
            "duration": "1 D",
            "bar_size": "1 day",
        },
    },
}


FX_OHLCV_REQUEST_EXAMPLES = {
    "eurusd_minimal": {
        "summary": "EURUSD midpoint bars",
        "description": "FX wrapper presets asset_class=fx, exchange=IDEALPRO, what_to_show=MIDPOINT, and use_rth=false.",
        "value": {"symbol": "EURUSD"},
    },
    "usdjpy_hourly": {
        "summary": "USDJPY hourly midpoint bars",
        "value": {
            "symbol": "USDJPY",
            "currency": "JPY",
            "start_datetime": "2026-05-01T00:00:00Z",
            "end_datetime": "2026-05-05T00:00:00Z",
            "duration": "5 D",
            "bar_size": "1 hour",
        },
    },
}


BOND_OHLCV_REQUEST_EXAMPLES = {
    "treasury_by_cusip": {
        "summary": "Treasury bond by CUSIP",
        "description": "Bond wrapper presets asset_class=bond. Use sec_id_type/sec_id or con_id when available.",
        "value": {
            "sec_id_type": "CUSIP",
            "sec_id": "91282CJN2",
            "start_datetime": "2026-05-01T00:00:00Z",
            "end_datetime": "2026-05-31T00:00:00Z",
            "duration": "1 M",
            "bar_size": "1 day",
        },
    },
    "bond_by_con_id": {
        "summary": "Bond by IBKR conId",
        "description": "Use con_id when your upstream security master already stores IBKR identifiers.",
        "value": {
            "con_id": 123456789,
            "currency": "USD",
            "duration": "1 M",
            "bar_size": "1 day",
        },
    },
}


INDEX_OHLCV_REQUEST_EXAMPLES = {
    "spx_us": {
        "summary": "S&P 500 (SPX)",
        "description": "Auto-resolves to CBOE/USD.",
        "value": {"symbol": "SPX"},
    },
    "ndx_us": {
        "summary": "Nasdaq-100 (NDX)",
        "description": "Auto-resolves to CBOE/USD.",
        "value": {"symbol": "NDX"},
    },
    "vix_us": {
        "summary": "CBOE Volatility Index (VIX)",
        "description": "Auto-resolves to CBOE/USD.",
        "value": {"symbol": "VIX"},
    },
    "rut_us": {
        "summary": "Russell 2000 (RUT)",
        "description": "Auto-resolves to ICE/USD.",
        "value": {"symbol": "RUT"},
    },
    "dji_us": {
        "summary": "Dow Jones Industrial Average (DJI)",
        "description": "Auto-resolves to CBOE/USD.",
        "value": {"symbol": "DJI"},
    },
    "hsi_hk": {
        "summary": "Hang Seng Index (HSI)",
        "description": "Auto-resolves to SEHK/HKD.",
        "value": {"symbol": "HSI"},
    },
    "dax_de": {
        "summary": "DAX (DAX)",
        "description": "Auto-resolves to EUREX/EUR.",
        "value": {"symbol": "DAX"},
    },
    "nikkei_jp": {
        "summary": "Nikkei 225 (NIKKEI)",
        "description": "Auto-resolves to TSEJ/JPY.",
        "value": {"symbol": "NIKKEI"},
    },
    "ftse100_uk": {
        "summary": "FTSE 100 (UK)",
        "description": "Auto-resolves to LSE/GBP.",
        "value": {"symbol": "FTSE100"},
    },
    "cac40_fr": {
        "summary": "CAC 40 (France)",
        "description": "Auto-resolves to SBF/EUR.",
        "value": {"symbol": "CAC40"},
    },
    "taiex_tw": {
        "summary": "TAIEX (Taiwan)",
        "description": "Auto-resolves to TWSE/TWD.",
        "value": {"symbol": "TAIEX"},
    },
    "kospi200_kr": {
        "summary": "KOSPI 200 (Korea)",
        "description": "Auto-resolves to KSE/KRW.",
        "value": {"symbol": "KOSPI200"},
    },
    "smi_ch": {
        "summary": "SMI (Switzerland)",
        "description": "Auto-resolves to EBS/CHF.",
        "value": {"symbol": "SMI"},
    },
    "estx50_eu": {
        "summary": "Euro Stoxx 50",
        "description": "Auto-resolves to EUREX/EUR.",
        "value": {"symbol": "ESTX50"},
    },
}


# ---------------------------------------------------------------------------
# Index auto-resolver
# ---------------------------------------------------------------------------
_INDEX_EXCHANGE_MAP: dict[str, tuple[str, str]] = {
    # US indices
    "SPX": ("CBOE", "USD"),
    "NDX": ("CBOE", "USD"),
    "VIX": ("CBOE", "USD"),
    "RUT": ("ICE", "USD"),
    "DJI": ("CBOE", "USD"),
    "OEX": ("CBOE", "USD"),
    "NDXP": ("CBOE", "USD"),
    # Hong Kong
    "HSI": ("SEHK", "HKD"),
    "HSCEI": ("SEHK", "HKD"),
    "HSTECH": ("SEHK", "HKD"),
    # Japan
    "NIKKEI": ("TSEJ", "JPY"),
    "NKY": ("TSEJ", "JPY"),
    "TOPIX": ("TSEJ", "JPY"),
    # Europe
    "DAX": ("EUREX", "EUR"),
    "FDAX": ("EUREX", "EUR"),
    "ESTX50": ("EUREX", "EUR"),
    "SMI": ("EBS", "CHF"),
    "CAC40": ("SBF", "EUR"),
    "FTSE100": ("LSE", "GBP"),
    "FTSE250": ("LSE", "GBP"),
    # Australia
    "SPI": ("ASX", "AUD"),
    "XJO": ("ASX", "AUD"),
    # Korea
    "KOSPI": ("KSE", "KRW"),
    "KOSPI200": ("KSE", "KRW"),
    "KOSDQ150": ("KSE", "KRW"),
    # India
    "NIFTY": ("NSE", "INR"),
    "BANKNIFTY": ("NSE", "INR"),
    # Singapore
    "STI": ("SGX", "SGD"),
    # Taiwan
    "TAIEX": ("TWSE", "TWD"),
    # Canada
    "SPTSX": ("TSE", "CAD"),
}


def _resolve_index(symbol: str) -> dict[str, str]:
    """Look up IBKR exchange and currency for a known index symbol."""
    upper = symbol.strip().upper()
    if upper in _INDEX_EXCHANGE_MAP:
        exchange, currency = _INDEX_EXCHANGE_MAP[upper]
        return {"symbol": upper, "exchange": exchange, "currency": currency}
    return {"symbol": upper, "exchange": "CBOE", "currency": "USD"}


OPTION_SKEW_REQUEST_EXAMPLES = {
    "tsla_bounded_skew": {
        "summary": "TSLA per-maturity skew",
        "description": "Samples a bounded strike window around spot, computes put-minus-call IV skew, and reports max call/put OI per expiry.",
        "value": {
            "request": {
                "chain_request": {
                    "symbol": "TSLA",
                    "asset_class": "equity",
                    "exchange": "SMART",
                    "currency": "USD",
                    "primary_exchange": "NASDAQ",
                },
                "spot_price": 250.0,
                "strike_window_pct": 0.30,
                "max_expirations": 4,
                "max_strikes_per_expiry": 11,
                "target_abs_delta": 0.25,
                "max_concurrent_requests": 4,
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 60,
        },
    },
    "spx_bounded_skew": {
        "summary": "SPX index skew",
        "description": "For index options, specify the index exchange and optionally a trading class such as SPX or SPXW.",
        "value": {
            "request": {
                "chain_request": {
                    "symbol": "SPX",
                    "asset_class": "index",
                    "exchange": "CBOE",
                    "currency": "USD",
                },
                "chain_exchange": "CBOE",
                "trading_class": "SPX",
                "spot_price": 5200.0,
                "strike_window_pct": 0.20,
                "max_expirations": 4,
                "max_strikes_per_expiry": 11,
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 60,
        },
    },
}


async def _load_ohlcv_with_controls(
    *,
    request: OHLCVRequest,
    start_datetime: datetime | None = None,
    persist: bool,
    cache_latest: bool,
    use_ttl_cache: bool,
    cache_ttl_seconds: float | None,
    cache_namespace: str,
    state: IBKRRestAppState,
) -> list[OHLCVBar]:
    # When start_datetime is provided, use paginated range fetch.
    if start_datetime is not None and start_datetime != request.end_datetime:
        if use_ttl_cache and not persist:
            key = stable_cache_key(
                f"{cache_namespace}:range",
                {
                    "request": request.model_dump(mode="json"),
                    "start_datetime": start_datetime.isoformat(),
                },
            )
            cached = await state.market_data_cache.get(key)
            if cached is not None:
                return cached

        bars = await state.feed.load_historical_ohlcv_range(
            request,
            start_datetime=start_datetime,
            end_datetime=request.end_datetime,
        )

        if persist:
            await state.loader.persist_bars(bars)
        if cache_latest and bars:
            await state.loader.cache_latest_bar(bars[-1])

        if use_ttl_cache and not persist:
            await state.market_data_cache.set(key, bars, ttl_seconds=cache_ttl_seconds)
        return bars

    # Single-chunk fetch (original behavior).
    async def load() -> list[OHLCVBar]:
        return await state.loader.load(
            request,
            persist=persist,
            cache_latest=cache_latest,
        )

    if use_ttl_cache and not persist:
        key = stable_cache_key(
            cache_namespace,
            {
                "request": request.model_dump(mode="json"),
                "cache_latest": cache_latest,
            },
        )
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=cache_ttl_seconds)
    return await load()


def _contract_int(value: Any, *names: str) -> int | None:
    for name in names:
        raw = getattr(value, name, None)
        if raw not in (None, ""):
            try:
                return int(raw)
            except (TypeError, ValueError):
                return None
    return None


def _contract_text(value: Any, *names: str) -> str | None:
    for name in names:
        raw = getattr(value, name, None)
        if raw not in (None, ""):
            return str(raw).strip() or None
    return None


def _market_rule_ids(value: Any) -> tuple[int, ...]:
    raw = _contract_text(value, "marketRuleIds", "market_rule_ids") or ""
    ids: list[int] = []
    for item in raw.replace(";", ",").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.append(int(item))
        except ValueError:
            continue
    return tuple(ids)


def _session_to_dict(session: Any) -> dict[str, Any]:
    return {
        key: getattr(session, key, None)
        for key in ("startDateTime", "endDateTime", "refDate", "start", "end")
        if getattr(session, key, None) is not None
    }


async def _load_commodity_news(payload: CommodityNewsRequest, state: IBKRRestAppState) -> CommodityNewsResponse:
    contract = await state.feed.qualify_contract(payload.to_contract_spec())
    con_id = _contract_int(contract, "conId", "con_id")
    if con_id is None:
        raise HTTPException(status_code=404, detail=f"IBKR could not qualify commodity contract for {payload.symbol}")

    provider_codes = payload.provider_codes
    if provider_codes is None:
        providers = await state.feed.load_news_providers()
        provider_codes = tuple(provider.provider_code for provider in providers)
    if not provider_codes:
        raise HTTPException(status_code=503, detail="IBKR news providers are not available for this account")

    request = HistoricalNewsRequest(
        con_id=con_id,
        provider_codes=provider_codes,
        start_datetime=payload.start_datetime,
        end_datetime=payload.end_datetime,
        total_results=payload.max_results,
    )
    headlines = await state.feed.load_historical_news(request)
    items: list[CommodityNewsHeadline] = []
    for headline in headlines:
        article = None
        if payload.include_articles:
            article = await state.feed.load_news_article(
                NewsArticleRequest(provider_code=headline.provider_code, article_id=headline.article_id)
            )
        items.append(CommodityNewsHeadline(headline=headline, article=article))

    return CommodityNewsResponse(
        symbol=payload.symbol.strip().upper(),
        con_id=con_id,
        provider_codes=provider_codes,
        headlines=tuple(items),
    )


@router.post("/ohlcv", response_model=list[OHLCVBar])
async def load_ohlcv(
    payload: Annotated[HistoricalOHLCVLoadRequest, Body(openapi_examples=GENERIC_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.request,
        start_datetime=payload.request.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv",
        state=state,
    )


@router.post(
    "/ohlcv/equity",
    response_model=list[OHLCVBar],
    summary="Load equity OHLCV with preset asset_class",
)
async def load_equity_ohlcv(
    payload: Annotated[EquityOHLCVLoadRequest, Body(openapi_examples=EQUITY_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_equity",
        state=state,
    )


@router.post(
    "/ohlcv/futures",
    response_model=list[FutureOHLCVBar],
    summary="Load futures OHLCV with preset asset_class",
)
async def load_futures_ohlcv(
    payload: Annotated[FutureOHLCVLoadRequest, Body(openapi_examples=FUTURES_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[FutureOHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_futures",
        state=state,
    )


@router.post(
    "/ohlcv/commodities",
    response_model=list[FutureOHLCVBar],
    summary="Load commodity futures OHLCV with commodity presets",
)
async def load_commodity_ohlcv(
    payload: Annotated[CommodityOHLCVLoadRequest, Body(openapi_examples=COMMODITY_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[FutureOHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_commodities",
        state=state,
    )


@router.post(
    "/ohlcv/commodity-options",
    response_model=list[OptionOHLCVBar],
    summary="Load commodity futures option OHLCV",
)
async def load_commodity_option_ohlcv(
    payload: Annotated[CommodityOptionOHLCVLoadRequest, Body(openapi_examples=COMMODITY_OPTION_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OptionOHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_commodity_options",
        state=state,
    )


@router.post(
    "/ohlcv/fx-options",
    response_model=list[OptionOHLCVBar],
    summary="Load FX option OHLCV",
)
async def load_fx_option_ohlcv(
    payload: Annotated[FXOptionOHLCVLoadRequest, Body(openapi_examples=FX_OPTION_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OptionOHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_fx_options",
        state=state,
    )


@router.post(
    "/ohlcv/fx",
    response_model=list[FXOHLCVBar],
    summary="Load FX OHLCV with preset asset_class",
)
async def load_fx_ohlcv(
    payload: Annotated[FXOHLCVLoadRequest, Body(openapi_examples=FX_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[FXOHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_fx",
        state=state,
    )


@router.post(
    "/ohlcv/bond",
    response_model=list[OHLCVBar],
    summary="Load bond OHLCV with preset asset_class",
)
async def load_bond_ohlcv(
    payload: Annotated[BondOHLCVLoadRequest, Body(openapi_examples=BOND_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_bond",
        state=state,
    )


@router.post(
    "/ohlcv/index",
    response_model=list[OHLCVBar],
    summary="Load index OHLCV with auto-resolved exchange",
)
async def load_index_ohlcv(
    payload: Annotated[IndexOHLCVLoadRequest, Body(openapi_examples=INDEX_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    return await _load_ohlcv_with_controls(
        request=payload.to_request(),
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_index",
        state=state,
    )


@router.get(
    "/latest-bar",
    response_model=OHLCVBar | None,
    summary="Read the latest cached OHLCV bar",
    description=(
        "Reads Redis only; it does not call IBKR or QuestDB. "
        "Populate this cache by loading OHLCV with cache_latest=true or by running a scheduler snapshot job. "
        "Use symbol for the production symbol-scoped key. If symbol is omitted, the legacy asset-class latest key is used."
    ),
)
async def get_latest_bar(
    asset_class: Annotated[
        AssetClass,
        Query(
            description="Asset class namespace used in the Redis latest-bar key.",
            examples=["equity"],
        ),
    ],
    bar_size: Annotated[
        str,
        Query(
            min_length=1,
            description="Bar size exactly as used by OHLCV loading, for example '1 min'. Spaces are normalized to underscores in Redis keys.",
            examples=["1 min"],
        ),
    ],
    symbol: Annotated[
        str | None,
        Query(
            min_length=1,
            description="Optional symbol for the symbol-scoped latest-bar key. Omit only for the legacy asset-class latest key.",
            examples=["SPY"],
        ),
    ] = None,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OHLCVBar | None:
    return await state.redis.get_latest_bar(asset_class, bar_size, symbol=symbol)


@router.post("/options/analytics", response_model=OptionAnalyticsSnapshot)
async def load_option_analytics(
    payload: CachedOptionAnalyticsRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionAnalyticsSnapshot:
    async def load() -> OptionAnalyticsSnapshot:
        return await state.feed.load_option_analytics(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("option_analytics", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post(
    "/commodities/options/analytics",
    response_model=OptionAnalyticsSnapshot,
    summary="Load commodity futures option analytics",
)
async def load_commodity_option_analytics(
    payload: Annotated[CommodityOptionAnalyticsLoadRequest, Body(openapi_examples=COMMODITY_OPTION_ANALYTICS_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionAnalyticsSnapshot:
    request = payload.to_request()

    async def load() -> OptionAnalyticsSnapshot:
        return await state.feed.load_option_analytics(request)

    if payload.use_ttl_cache:
        key = stable_cache_key("commodity_option_analytics", request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post("/options/skew", response_model=OptionSkewSurfaceResponse)
async def load_option_skew_surface(
    payload: Annotated[CachedOptionSkewRequest, Body(openapi_examples=OPTION_SKEW_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionSkewSurfaceResponse:
    async def load() -> OptionSkewSurfaceResponse:
        return await state.feed.load_option_skew_surface(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("option_skew", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post(
    "/commodities/metadata",
    response_model=CommodityMetadataResponse,
    summary="Load IBKR-native commodity contract metadata",
)
async def load_commodity_metadata(
    payload: Annotated[CommodityMetadataRequest, Body(openapi_examples=COMMODITY_METADATA_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> CommodityMetadataResponse:
    async def load() -> CommodityMetadataResponse:
        request = payload.to_ohlcv_request()
        contract = await state.feed.qualify_contract(ContractSpec.from_ohlcv_request(request))
        market_rule_ids = _market_rule_ids(contract)
        market_rules: list[MarketRule] = []
        if payload.include_market_rules:
            for rule_id in market_rule_ids:
                market_rules.append(await state.feed.load_market_rule(rule_id))

        head_timestamp = None
        if payload.include_head_timestamp:
            head_timestamp = await state.feed.load_head_timestamp(
                HeadTimestampRequest(
                    symbol=request.symbol,
                    sec_type="FUT",
                    exchange=request.exchange,
                    currency=request.currency,
                    what_to_show=payload.what_to_show,
                    use_rth=payload.use_rth,
                )
            )

        trading_sessions: tuple[dict[str, Any], ...] = ()
        if payload.include_trading_schedule:
            sessions = await state.feed.load_trading_schedule(
                request,
                ref_date=payload.schedule_date or date.today(),
                use_rth=payload.use_rth,
            )
            trading_sessions = tuple(_session_to_dict(session) for session in sessions)

        return CommodityMetadataResponse(
            symbol=request.symbol,
            exchange=request.exchange,
            currency=request.currency,
            con_id=_contract_int(contract, "conId", "con_id"),
            local_symbol=_contract_text(contract, "localSymbol", "local_symbol"),
            trading_class=_contract_text(contract, "tradingClass", "trading_class"),
            min_tick=getattr(contract, "minTick", None),
            market_rule_ids=market_rule_ids,
            head_timestamp=head_timestamp,
            trading_sessions=trading_sessions,
            market_rules=tuple(market_rules),
        )

    if payload.use_ttl_cache:
        key = stable_cache_key("commodity_metadata", payload)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post(
    "/commodities/historical-ticks",
    response_model=HistoricalTickResponse,
    summary="Load commodity futures historical ticks",
)
async def load_commodity_historical_ticks(
    payload: Annotated[CommodityHistoricalTicksRequest, Body(openapi_examples=COMMODITY_HISTORICAL_TICKS_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> HistoricalTickResponse:
    return await state.feed.load_historical_ticks(payload.to_request())


@router.post(
    "/commodities/news",
    response_model=CommodityNewsResponse,
    summary="Load IBKR historical news for a commodity future",
)
async def load_commodity_news(
    payload: Annotated[CommodityNewsRequest, Body(openapi_examples=COMMODITY_NEWS_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> CommodityNewsResponse:
    if payload.use_ttl_cache:
        key = stable_cache_key("commodity_news", payload)
        return await state.market_data_cache.get_or_set(key, lambda: _load_commodity_news(payload, state), ttl_seconds=payload.cache_ttl_seconds)
    return await _load_commodity_news(payload, state)


@router.post("/bonds/yields/history", response_model=list[BondYieldBar])
async def load_bond_yield_history(
    payload: CachedBondYieldHistoryRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[BondYieldBar]:
    async def load() -> list[BondYieldBar]:
        return await state.feed.load_bond_yield_history(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("bond_yield_history", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()
