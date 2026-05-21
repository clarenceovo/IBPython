"""Futures and commodity endpoints and models."""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.config.reference_data import resolve_commodity_future as _resolve_commodity_future
from src.feeds.contracts import ContractSpec
from src.feeds.models import AssetClass, FutureOHLCVBar, OHLCVBar, OHLCVRequest, OptionOHLCVBar
from src.feeds.news import HistoricalNewsHeadline, HistoricalNewsRequest, NewsArticle, NewsArticleRequest
from src.feeds.options import OptionAnalyticsRequest, OptionAnalyticsSnapshot, OptionContractSpec, OptionRight
from src.feeds.tick_data import HeadTimestampRequest, HistoricalTickRequest, HistoricalTickResponse, MarketRule
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.routers.market_data_shared import (
    MinimalOHLCVLoadControls,
    contract_int,
    contract_text,
    load_ohlcv_with_controls,
    market_rule_ids,
    session_to_dict,
)

router = APIRouter(prefix="/market-data", tags=["market-data"])


# ---------------------------------------------------------------------------
# Futures models
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Commodity models
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Examples
# ---------------------------------------------------------------------------

FUTURES_OHLCV_REQUEST_EXAMPLES = {
    "es_by_contract_month": {
        "summary": "ES future by contract month",
        "description": "Futures require an expiry/contract month, local_symbol, or con_id.",
        "value": {"symbol": "ES", "exchange": "CME", "currency": "USD", "last_trade_date_or_contract_month": "202606", "start_datetime": "2026-05-01T13:30:00Z", "end_datetime": "2026-05-01T20:00:00Z", "duration": "1 D", "bar_size": "1 min"},
    },
    "es_by_local_symbol": {
        "summary": "ES future by local symbol",
        "description": "Use local_symbol when that is how the contract is represented in TWS.",
        "value": {"symbol": "ES", "exchange": "CME", "local_symbol": "ESM6", "multiplier": "50"},
    },
    "hsi_hkfe_by_contract_month": {
        "summary": "Hang Seng Index future",
        "description": "HKEX Hang Seng Index futures use product code HSI. Use HKFE/HKD for IBKR routing.",
        "value": {"symbol": "HSI", "exchange": "HKFE", "currency": "HKD", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "hstech_hkfe_by_contract_month": {
        "summary": "Hang Seng TECH Index future",
        "description": "HKEX Hang Seng TECH Index futures use product code HTI. Use HKFE/HKD for IBKR routing.",
        "value": {"symbol": "HTI", "exchange": "HKFE", "currency": "HKD", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "n225m_ose_by_contract_month": {
        "summary": "Nikkei 225 Mini future",
        "description": "Nikkei 225 Mini futures on OSE.JPN. Use N225M/JPY.",
        "value": {"symbol": "N225M", "exchange": "OSE.JPN", "currency": "JPY", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "xina_sgx_by_contract_month": {
        "summary": "SGX FTSE China A50 future",
        "description": "SGX FTSE China A50 futures. Symbol XINA on SGX/USD.",
        "value": {"symbol": "XINA", "exchange": "SGX", "currency": "USD", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "k200_kse_by_contract_month": {
        "summary": "KOSPI 200 future",
        "description": "KOSPI 200 futures on KSE. Symbol K200/KRW.",
        "value": {"symbol": "K200", "exchange": "KSE", "currency": "KRW", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "tx_taifex_by_contract_month": {
        "summary": "TAIEX future (Taiwan)",
        "description": "TAIEX futures on TAIFEX. Symbol TX/TWD. Mini is MTX.",
        "value": {"symbol": "TX", "exchange": "TAIFEX", "currency": "TWD", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "nq_cme_by_contract_month": {
        "summary": "Nasdaq 100 E-mini future",
        "description": "Nasdaq 100 E-mini on CME. Symbol NQ/USD.",
        "value": {"symbol": "NQ", "exchange": "CME", "currency": "USD", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "ym_cbot_by_contract_month": {
        "summary": "Dow Jones E-mini future",
        "description": "Dow Jones E-mini on CBOT. Symbol YM/USD.",
        "value": {"symbol": "YM", "exchange": "CBOT", "currency": "USD", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "rty_cme_by_contract_month": {
        "summary": "Russell 2000 E-mini future",
        "description": "Russell 2000 E-mini on CME. Symbol RTY/USD, $50/pt multiplier.",
        "value": {"symbol": "RTY", "exchange": "CME", "currency": "USD", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "vx_cfe_by_contract_month": {
        "summary": "VIX future",
        "description": "CBOE VIX futures. Symbol VX/USD on CFE.",
        "value": {"symbol": "VX", "exchange": "CFE", "currency": "USD", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "z_iceeu_ftse100": {
        "summary": "FTSE 100 Index future (UK)",
        "description": "ICE Futures Europe FTSE 100. Symbol Z/GBP on ICEEU.",
        "value": {"symbol": "Z", "exchange": "ICEEU", "currency": "GBP", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "fce_monep_cac40": {
        "summary": "CAC 40 Index future (France)",
        "description": "Euronext Paris CAC 40 futures. Symbol FCE/EUR on MONEP.",
        "value": {"symbol": "FCE", "exchange": "MONEP", "currency": "EUR", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "dax_eurex_by_contract_month": {
        "summary": "DAX 40 future (Germany)",
        "description": "Eurex DAX 40 futures. Symbol DAX/EUR on EUREX.",
        "value": {"symbol": "DAX", "exchange": "EUREX", "currency": "EUR", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
    "estx50_eurex_by_contract_month": {
        "summary": "Euro Stoxx 50 future",
        "description": "Eurex Euro Stoxx 50 futures. Symbol ESTX50/EUR on EUREX.",
        "value": {"symbol": "ESTX50", "exchange": "EUREX", "currency": "EUR", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min", "what_to_show": "TRADES"},
    },
}

COMMODITY_OHLCV_REQUEST_EXAMPLES = {
    "cl_crude_nymex": {
        "summary": "CL crude oil future",
        "description": "CL auto-resolves to NYMEX/USD and uses futures OHLCV under the hood.",
        "value": {"symbol": "CL", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "1 min"},
    },
    "gc_gold_comex": {
        "summary": "GC gold future",
        "description": "GC auto-resolves to COMEX/USD.",
        "value": {"symbol": "GC", "last_trade_date_or_contract_month": "202606", "duration": "1 D", "bar_size": "5 mins"},
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
        "value": {"underlying_symbol": "CL", "expiry": "20260617", "strike": 80.0, "right": "C", "exchange": "NYMEX", "multiplier": "1000", "duration": "1 D", "bar_size": "1 day"},
    }
}

COMMODITY_OPTION_ANALYTICS_EXAMPLES = {
    "cl_fop_greeks": {
        "summary": "CL futures option Greeks/OI",
        "description": "Uses a short-lived IBKR market-data subscription because generic ticks are requested.",
        "value": {"contract": {"underlying_symbol": "CL", "expiry": "20260617", "strike": 80.0, "right": "C", "exchange": "NYMEX", "multiplier": "1000"}, "snapshot_wait_seconds": 2.0, "use_ttl_cache": True},
    }
}

COMMODITY_METADATA_EXAMPLES = {
    "cl_metadata": {
        "summary": "CL contract metadata",
        "value": {"symbol": "CL", "last_trade_date_or_contract_month": "202606", "include_head_timestamp": True, "include_trading_schedule": True, "schedule_date": "2026-05-18"},
    }
}

COMMODITY_HISTORICAL_TICKS_EXAMPLES = {
    "cl_historical_ticks": {
        "summary": "CL historical trade ticks",
        "value": {"symbol": "CL", "last_trade_date_or_contract_month": "202606", "start_date": "2026-05-18T13:30:00Z", "end_date": "2026-05-18T14:30:00Z", "what_to_show": "TRADES", "max_ticks": 1000},
    }
}

COMMODITY_NEWS_EXAMPLES = {
    "cl_news": {
        "summary": "CL historical news",
        "value": {"symbol": "CL", "last_trade_date_or_contract_month": "202606", "start_datetime": "2026-05-01T00:00:00Z", "end_datetime": "2026-05-18T00:00:00Z", "max_results": 25},
    }
}


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

async def _load_commodity_news(payload: CommodityNewsRequest, state: IBKRRestAppState) -> CommodityNewsResponse:
    contract = await state.feed.qualify_contract(payload.to_contract_spec())
    con_id = contract_int(contract, "conId", "con_id")
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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post(
    "/ohlcv/futures",
    response_model=list[FutureOHLCVBar],
    summary="Load futures OHLCV with preset asset_class",
)
async def load_futures_ohlcv(
    payload: Annotated[FutureOHLCVLoadRequest, Body(openapi_examples=FUTURES_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[FutureOHLCVBar]:
    return await load_ohlcv_with_controls(
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
    return await load_ohlcv_with_controls(
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
    return await load_ohlcv_with_controls(
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
        rule_ids = market_rule_ids(contract)
        rules: list[MarketRule] = []
        if payload.include_market_rules:
            for rule_id in rule_ids:
                rules.append(await state.feed.load_market_rule(rule_id))

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
            trading_sessions = tuple(session_to_dict(session) for session in sessions)

        return CommodityMetadataResponse(
            symbol=request.symbol,
            exchange=request.exchange,
            currency=request.currency,
            con_id=contract_int(contract, "conId", "con_id"),
            local_symbol=contract_text(contract, "localSymbol", "local_symbol"),
            trading_class=contract_text(contract, "tradingClass", "trading_class"),
            min_tick=getattr(contract, "minTick", None),
            market_rule_ids=rule_ids,
            head_timestamp=head_timestamp,
            trading_sessions=trading_sessions,
            market_rules=tuple(rules),
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
