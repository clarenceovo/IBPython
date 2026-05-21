from __future__ import annotations

import asyncio
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.bond_curve import BondCurveRequest, BondCurveResponse, build_standard_bond_curve
from src.feeds.contracts import ContractSpec, OptionChainRequest
from src.feeds.exchange_resolver import resolve_equity
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.feeds.news import (
    HistoricalNewsHeadline,
    HistoricalNewsRequest,
    NewsArticle,
    NewsArticleRequest,
    NewsProvider,
)
from src.feeds.options import OptionSkewSurfaceRequest, OptionSkewSurfaceResponse
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.openapi_markdown import markdown_openapi_examples

router = APIRouter(prefix="/business", tags=["business"])


class BusinessCacheControls(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)


class BusinessDateRangeControls(BusinessCacheControls):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    start_datetime: datetime | None = None
    end_datetime: datetime | None = None

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

    @model_validator(mode="after")
    def validate_time_range(self) -> "BusinessDateRangeControls":
        if self.start_datetime and self.end_datetime and self.start_datetime >= self.end_datetime:
            raise ValueError("start_datetime must be before end_datetime")
        return self


class BusinessOHLCVSymbol(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    exchange: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    primary_exchange: str | None = Field(default=None, min_length=1)
    last_trade_date_or_contract_month: str | None = Field(default=None, min_length=1)
    multiplier: str | None = Field(default=None, min_length=1)
    local_symbol: str | None = Field(default=None, min_length=1)
    con_id: int | None = Field(default=None, gt=0)
    sec_id_type: str | None = Field(default=None, min_length=1)
    sec_id: str | None = Field(default=None, min_length=1)


class MarketPanelRequest(BusinessDateRangeControls):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbols: list[str | BusinessOHLCVSymbol] = Field(min_length=1)
    asset_class: AssetClass = AssetClass.EQUITY
    exchange: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True
    cache_latest: bool = False
    max_concurrent_requests: int = Field(default=4, ge=1, le=20)


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

_COMMODITY_FUTURES_MONTHS: dict[str, tuple[int, ...]] = {
    "CL": tuple(range(1, 13)),
    "NG": tuple(range(1, 13)),
    "GC": (2, 4, 6, 8, 10, 12),
    "SI": (3, 5, 7, 9, 12),
    "HG": (3, 5, 7, 9, 12),
    "ZC": (3, 5, 7, 9, 12),
    "ZS": (3, 5, 7, 8, 9, 11),
    "ZW": (3, 5, 7, 9, 12),
    "ZL": (1, 3, 5, 7, 8, 9, 10, 12),
    "ZM": (1, 3, 5, 7, 8, 9, 10, 12),
}

_COMMODITY_EXPIRY_RULES: dict[str, str] = {
    "CL": "nymex_crude_oil",
}


class CommodityFuturesRequest(BusinessDateRangeControls):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, examples=["CL", "GC", "NG"])
    as_of_date: date = Field(default_factory=date.today)
    forward_count: int = Field(default=1, ge=0, le=12)
    exchange: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    multiplier: str | None = Field(default=None, min_length=1)
    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = False
    cache_latest: bool = False
    max_concurrent_requests: int = Field(default=4, ge=1, le=20)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: object) -> str:
        if value is None:
            raise ValueError("symbol is required")
        normalized = str(value).strip().upper()
        if not normalized:
            raise ValueError("symbol cannot be empty")
        return normalized


class CommodityFuturePoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: str
    symbol: str
    contract_month: str
    exchange: str
    currency: str
    bar: OHLCVBar | None = None


class CommodityFuturesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    as_of_date: date
    contracts: tuple[CommodityFuturePoint, ...]
    source: str = "ibkr"


class UniverseBarsRequest(BusinessDateRangeControls):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    universe: str = Field(min_length=1)
    symbols: list[str] | None = Field(
        default=None,
        description="Optional explicit symbols. When omitted, the endpoint reads Redis index composition by universe.",
    )
    asset_class: AssetClass = AssetClass.EQUITY
    exchange: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True
    cache_latest: bool = False
    max_symbols: int = Field(default=100, ge=1, le=500)
    max_concurrent_requests: int = Field(default=4, ge=1, le=20)


class ReturnPoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    timestamp: datetime
    close: float
    previous_close: float
    simple_return: float
    log_return: float


class SymbolReturnSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    observations: int
    return_count: int
    cumulative_return: float | None
    realized_volatility: float | None
    first_timestamp: datetime | None
    last_timestamp: datetime | None
    points: list[ReturnPoint]


class ReturnsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    asset_class: AssetClass
    bar_size: str
    summaries: list[SymbolReturnSummary]
    warnings: list[str] = Field(default_factory=list)


class SymbolNewsRequest(BusinessDateRangeControls):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    asset_class: AssetClass = AssetClass.EQUITY
    exchange: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    primary_exchange: str | None = Field(default=None, min_length=1)
    con_id: int | None = Field(default=None, gt=0)
    provider_codes: tuple[str, ...] | None = None
    total_results: int = Field(default=20, ge=1, le=300)
    include_articles: bool = False
    max_concurrent_article_requests: int = Field(default=4, ge=1, le=10)

    @field_validator("provider_codes", mode="before")
    @classmethod
    def normalize_provider_codes(cls, value: object) -> tuple[str, ...] | None:
        if value is None or value == "":
            return None
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("provider_codes must be a sequence")
        normalized = tuple(str(item).strip().upper() for item in value if str(item).strip())
        return normalized or None


class BusinessNewsHeadline(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timestamp: datetime
    provider_code: str
    article_id: str
    headline: str
    article: NewsArticle | None = None
    source: str = "ibkr_news"


class SymbolNewsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    asset_class: AssetClass
    con_id: int
    provider_codes: tuple[str, ...]
    headlines: list[BusinessNewsHeadline]


class CachedNewsArticleRequest(BusinessCacheControls):
    model_config = ConfigDict(extra="forbid")

    provider_code: str = Field(min_length=1)
    article_id: str = Field(min_length=1)

    @field_validator("provider_code", mode="before")
    @classmethod
    def normalize_provider_code(cls, value: object) -> str:
        if value is None:
            raise ValueError("provider_code is required")
        return str(value).strip().upper()


class BusinessOptionSkewRequest(BusinessCacheControls):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    asset_class: AssetClass = AssetClass.EQUITY
    exchange: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)
    primary_exchange: str | None = Field(default=None, min_length=1)
    chain_exchange: str | None = Field(default=None, min_length=1)
    trading_class: str | None = Field(default=None, min_length=1)
    option_exchange: str | None = Field(default=None, min_length=1)
    spot_price: float | None = Field(default=None, gt=0)
    strike_window_pct: float = Field(default=0.30, gt=0, le=2.0)
    max_expirations: int = Field(default=4, ge=1, le=36)
    max_strikes_per_expiry: int = Field(default=11, ge=3, le=50)
    target_abs_delta: float = Field(default=0.25, gt=0, lt=1)
    max_concurrent_requests: int = Field(default=4, ge=1, le=20)
    snapshot_wait_seconds: float = Field(default=2.0, gt=0)


@router.get(
    "/getBondCurve",
    response_model=BondCurveResponse,
    operation_id="getBondCurve",
    summary="Get a standard-tenor sovereign bond curve",
)
async def get_bond_curve(
    market: Annotated[str, Query(min_length=1)],
    valuation_date: date | None = Query(default=None),
    coupon_frequency: int | None = Query(default=None, ge=1),
) -> BondCurveResponse:
    try:
        request = BondCurveRequest(
            market=market,
            valuation_date=valuation_date or datetime.now(timezone.utc).date(),
            coupon_frequency=coupon_frequency,
        )
        return build_standard_bond_curve(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get(
    "/getNewsProviders",
    response_model=list[NewsProvider],
    summary="Get entitled IBKR news providers",
)
async def get_news_providers(
    use_ttl_cache: bool = Query(default=True),
    cache_ttl_seconds: float | None = Query(default=300, ge=0),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[NewsProvider]:
    async def load() -> list[NewsProvider]:
        return await state.feed.load_news_providers()

    if use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_news_providers", {}),
            load,
            ttl_seconds=cache_ttl_seconds,
        )
    return await load()


@router.post(
    "/getSymbolNews",
    response_model=SymbolNewsResponse,
    summary="Get historical news for a symbol",
)
async def get_symbol_news(
    payload: Annotated[
        SymbolNewsRequest,
        Body(openapi_examples=markdown_openapi_examples("business.getSymbolNews")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> SymbolNewsResponse:
    async def load() -> SymbolNewsResponse:
        return await _load_symbol_news(payload, state)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_symbol_news", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


@router.post(
    "/getNewsArticle",
    response_model=NewsArticle,
    summary="Get an IBKR news article body",
)
async def get_news_article(
    payload: Annotated[
        CachedNewsArticleRequest,
        Body(openapi_examples=markdown_openapi_examples("business.getNewsArticle")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> NewsArticle:
    request = NewsArticleRequest(provider_code=payload.provider_code, article_id=payload.article_id)

    async def load() -> NewsArticle:
        return await state.feed.load_news_article(request)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_news_article", request),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


@router.post(
    "/getMarketPanel",
    response_model=list[OHLCVBar],
    summary="Load a multi-symbol OHLCV panel",
)
async def get_market_panel(
    payload: Annotated[
        MarketPanelRequest,
        Body(openapi_examples=markdown_openapi_examples("business.getMarketPanel")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    async def load() -> list[OHLCVBar]:
        requests = [_symbol_to_ohlcv_request(item, payload) for item in payload.symbols]
        return await _load_many_ohlcv(requests, payload, state)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_market_panel", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


@router.post(
    "/getUniverseBars",
    response_model=list[OHLCVBar],
    summary="Load OHLCV bars for a named universe",
)
async def get_universe_bars(
    payload: Annotated[
        UniverseBarsRequest,
        Body(openapi_examples=markdown_openapi_examples("business.getUniverseBars")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    async def load() -> list[OHLCVBar]:
        symbols = await _resolve_universe_symbols(payload, state)
        panel = MarketPanelRequest(
            symbols=symbols,
            asset_class=payload.asset_class,
            exchange=payload.exchange,
            currency=payload.currency,
            duration=payload.duration,
            bar_size=payload.bar_size,
            start_datetime=payload.start_datetime,
            end_datetime=payload.end_datetime,
            what_to_show=payload.what_to_show,
            use_rth=payload.use_rth,
            cache_latest=payload.cache_latest,
            max_concurrent_requests=payload.max_concurrent_requests,
            use_ttl_cache=False,
        )
        requests = [_symbol_to_ohlcv_request(item, panel) for item in panel.symbols[: payload.max_symbols]]
        return await _load_many_ohlcv(requests, panel, state)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_universe_bars", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


@router.post(
    "/commodities/getFutures",
    response_model=CommodityFuturesResponse,
    summary="Load front and forward commodity futures",
)
async def get_commodity_futures(
    payload: Annotated[
        CommodityFuturesRequest,
        Body(openapi_examples=markdown_openapi_examples("business.commodities.getFutures")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> CommodityFuturesResponse:
    async def load() -> CommodityFuturesResponse:
        root = payload.symbol
        exchange, currency = _resolve_commodity_market(root, payload.exchange, payload.currency)
        contract_months = _commodity_contract_months(root, payload.as_of_date, payload.forward_count + 1)
        semaphore = asyncio.Semaphore(payload.max_concurrent_requests)

        async def load_contract(index: int, contract_month: str) -> CommodityFuturePoint:
            role = "front" if index == 0 else f"forward_{index}"
            request = OHLCVRequest(
                symbol=root,
                asset_class=AssetClass.FUTURE,
                exchange=exchange,
                currency=currency,
                last_trade_date_or_contract_month=contract_month,
                multiplier=payload.multiplier,
                duration=payload.duration,
                bar_size=payload.bar_size,
                start_datetime=payload.start_datetime,
                end_datetime=payload.end_datetime,
                what_to_show=payload.what_to_show,
                use_rth=payload.use_rth,
                metadata={"market": "commodity", "role": role},
            )
            async with semaphore:
                if payload.start_datetime is not None:
                    bars = await state.feed.load_historical_ohlcv_range(
                        request,
                        start_datetime=payload.start_datetime,
                        end_datetime=payload.end_datetime,
                    )
                    if payload.cache_latest and bars:
                        await state.loader.cache_latest_bar(bars[-1])
                else:
                    bars = await state.loader.load(request, persist=False, cache_latest=payload.cache_latest)
            return CommodityFuturePoint(
                role=role,
                symbol=root,
                contract_month=contract_month,
                exchange=exchange,
                currency=currency,
                bar=bars[-1] if bars else None,
            )

        points = await asyncio.gather(*(load_contract(index, month) for index, month in enumerate(contract_months)))
        return CommodityFuturesResponse(symbol=root, as_of_date=payload.as_of_date, contracts=tuple(points))

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_commodity_futures", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


@router.post(
    "/getReturns",
    response_model=ReturnsResponse,
    summary="Load bars and compute close-to-close returns",
)
async def get_returns(
    payload: Annotated[
        MarketPanelRequest,
        Body(openapi_examples=markdown_openapi_examples("business.getReturns")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> ReturnsResponse:
    bars = await get_market_panel(payload, state)
    return _bars_to_returns(asset_class=payload.asset_class, bar_size=payload.bar_size, bars=bars)


@router.post(
    "/getOptionSkew",
    response_model=OptionSkewSurfaceResponse,
    summary="Get option skew from a minimal business payload",
)
async def get_option_skew(
    payload: Annotated[
        BusinessOptionSkewRequest,
        Body(openapi_examples=markdown_openapi_examples("business.getOptionSkew")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OptionSkewSurfaceResponse:
    resolved = _resolve_business_symbol(
        symbol=payload.symbol,
        asset_class=payload.asset_class,
        exchange=payload.exchange,
        currency=payload.currency,
        primary_exchange=payload.primary_exchange,
    )
    request = OptionSkewSurfaceRequest(
        chain_request=OptionChainRequest(
            symbol=resolved.symbol,
            asset_class=payload.asset_class,
            exchange=resolved.exchange,
            currency=resolved.currency,
            primary_exchange=resolved.primary_exchange,
        ),
        chain_exchange=payload.chain_exchange,
        trading_class=payload.trading_class,
        option_exchange=payload.option_exchange,
        spot_price=payload.spot_price,
        strike_window_pct=payload.strike_window_pct,
        max_expirations=payload.max_expirations,
        max_strikes_per_expiry=payload.max_strikes_per_expiry,
        target_abs_delta=payload.target_abs_delta,
        max_concurrent_requests=payload.max_concurrent_requests,
        snapshot_wait_seconds=payload.snapshot_wait_seconds,
    )

    async def load() -> OptionSkewSurfaceResponse:
        return await state.feed.load_option_skew_surface(request)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_option_skew", request),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


async def _load_symbol_news(payload: SymbolNewsRequest, state: IBKRRestAppState) -> SymbolNewsResponse:
    provider_codes = payload.provider_codes
    if provider_codes is None:
        providers = await get_news_providers(
            use_ttl_cache=payload.use_ttl_cache,
            cache_ttl_seconds=payload.cache_ttl_seconds,
            state=state,
        )
        provider_codes = tuple(provider.provider_code for provider in providers)
    if not provider_codes:
        raise HTTPException(status_code=503, detail="IBKR news providers are not available for this account")

    con_id = payload.con_id or await _qualify_symbol_con_id(payload, state)
    request = HistoricalNewsRequest(
        con_id=con_id,
        provider_codes=provider_codes,
        start_datetime=payload.start_datetime,
        end_datetime=payload.end_datetime,
        total_results=payload.total_results,
    )
    headlines = await state.feed.load_historical_news(request)
    articles: dict[tuple[str, str], NewsArticle] = {}
    if payload.include_articles and headlines:
        articles = await _load_articles_for_headlines(headlines, payload.max_concurrent_article_requests, state)

    return SymbolNewsResponse(
        symbol=payload.symbol.strip().upper(),
        asset_class=payload.asset_class,
        con_id=con_id,
        provider_codes=provider_codes,
        headlines=[
            BusinessNewsHeadline(
                timestamp=headline.timestamp,
                provider_code=headline.provider_code,
                article_id=headline.article_id,
                headline=headline.headline,
                article=articles.get((headline.provider_code, headline.article_id)),
                source=headline.source,
            )
            for headline in headlines
        ],
    )


async def _qualify_symbol_con_id(payload: SymbolNewsRequest, state: IBKRRestAppState) -> int:
    resolved = _resolve_business_symbol(
        symbol=payload.symbol,
        asset_class=payload.asset_class,
        exchange=payload.exchange,
        currency=payload.currency,
        primary_exchange=payload.primary_exchange,
    )
    contract = await state.feed.qualify_contract(
        ContractSpec(
            symbol=resolved.symbol,
            asset_class=payload.asset_class,
            exchange=resolved.exchange,
            currency=resolved.currency,
            primary_exchange=resolved.primary_exchange,
        )
    )
    con_id = int(getattr(contract, "conId", 0) or 0)
    if con_id <= 0:
        raise HTTPException(status_code=404, detail=f"IBKR could not qualify contract for {payload.symbol}")
    return con_id


async def _load_articles_for_headlines(
    headlines: list[HistoricalNewsHeadline],
    max_concurrency: int,
    state: IBKRRestAppState,
) -> dict[tuple[str, str], NewsArticle]:
    semaphore = asyncio.Semaphore(max_concurrency)

    async def load_one(headline: HistoricalNewsHeadline) -> tuple[tuple[str, str], NewsArticle]:
        async with semaphore:
            request = NewsArticleRequest(provider_code=headline.provider_code, article_id=headline.article_id)
            article = await state.feed.load_news_article(request)
            return (headline.provider_code, headline.article_id), article

    pairs = await asyncio.gather(*(load_one(headline) for headline in headlines))
    return dict(pairs)


async def _load_many_ohlcv(
    requests: list[OHLCVRequest],
    payload: MarketPanelRequest,
    state: IBKRRestAppState,
) -> list[OHLCVBar]:
    semaphore = asyncio.Semaphore(payload.max_concurrent_requests)

    async def load_one(request: OHLCVRequest) -> list[OHLCVBar]:
        async with semaphore:
            if payload.start_datetime is not None:
                bars = await state.feed.load_historical_ohlcv_range(
                    request,
                    start_datetime=payload.start_datetime,
                    end_datetime=payload.end_datetime,
                )
                if payload.cache_latest and bars:
                    await state.loader.cache_latest_bar(bars[-1])
                return bars
            return await state.loader.load(request, persist=False, cache_latest=payload.cache_latest)

    batches = await asyncio.gather(*(load_one(request) for request in requests))
    return sorted([bar for batch in batches for bar in batch], key=lambda bar: (bar.symbol, bar.timestamp))


def _resolve_commodity_market(symbol: str, exchange: str | None, currency: str | None) -> tuple[str, str]:
    preset_exchange, preset_currency = _COMMODITY_FUTURES_PRESETS.get(symbol.strip().upper(), ("NYMEX", "USD"))
    return exchange or preset_exchange, currency or preset_currency


def _commodity_contract_months(symbol: str, as_of_date: date, count: int) -> tuple[str, ...]:
    root = symbol.strip().upper()
    listed_months = _COMMODITY_FUTURES_MONTHS.get(root, tuple(range(1, 13)))
    months: list[str] = []
    year = as_of_date.year
    while len(months) < count:
        for listed_month in listed_months:
            contract_month = f"{year}{listed_month:02d}"
            if not _commodity_contract_available_on(root, contract_month, as_of_date):
                continue
            months.append(contract_month)
            if len(months) == count:
                break
        year += 1
    return tuple(months)


def _commodity_contract_available_on(symbol: str, contract_month: str, as_of_date: date) -> bool:
    rule = _COMMODITY_EXPIRY_RULES.get(symbol.strip().upper())
    if rule == "nymex_crude_oil":
        expiry = _nymex_crude_oil_last_trade_date(contract_month)
        return expiry is not None and as_of_date <= expiry
    return contract_month >= f"{as_of_date.year}{as_of_date.month:02d}"


def _nymex_crude_oil_last_trade_date(contract_month: str) -> date | None:
    try:
        year = int(contract_month[:4])
        month = int(contract_month[4:6])
    except (TypeError, ValueError):
        return None
    if month == 1:
        preceding_month = 12
        preceding_year = year - 1
    else:
        preceding_month = month - 1
        preceding_year = year

    twenty_fifth = date(preceding_year, preceding_month, 25)
    reference_day = _previous_nymex_business_day(twenty_fifth) if not _is_nymex_business_day(twenty_fifth) else twenty_fifth
    return _subtract_nymex_business_days(reference_day, 3)


def _subtract_nymex_business_days(value: date, count: int) -> date:
    current = value
    remaining = count
    while remaining > 0:
        current -= timedelta(days=1)
        if _is_nymex_business_day(current):
            remaining -= 1
    return current


def _previous_nymex_business_day(value: date) -> date:
    current = value
    while not _is_nymex_business_day(current):
        current -= timedelta(days=1)
    return current


def _is_nymex_business_day(value: date) -> bool:
    return value.weekday() < 5 and value not in _us_market_holidays(value.year)


def _us_market_holidays(year: int) -> set[date]:
    return {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _good_friday(year),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(year, 6, 19),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(year, 12, 25),
    }


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    current = date(year, month, 1)
    offset = (weekday - current.weekday()) % 7
    return current + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    current = date(year + int(month == 12), 1 if month == 12 else month + 1, 1) - timedelta(days=1)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _good_friday(year: int) -> date:
    # Anonymous Gregorian algorithm for Easter Sunday, then step back two days.
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    easter_month = (h + ell - 7 * m + 114) // 31
    easter_day = ((h + ell - 7 * m + 114) % 31) + 1
    return date(year, easter_month, easter_day) - timedelta(days=2)


async def _resolve_universe_symbols(payload: UniverseBarsRequest, state: IBKRRestAppState) -> list[str]:
    if payload.symbols:
        return [symbol.strip().upper() for symbol in payload.symbols if symbol.strip()][: payload.max_symbols]
    composition = await state.redis.get_index_composition(payload.universe)
    if composition is None:
        raise HTTPException(status_code=404, detail=f"universe {payload.universe!r} not found in Redis index composition")
    return [item.symbol for item in composition.constituents][: payload.max_symbols]


def _symbol_to_ohlcv_request(item: str | BusinessOHLCVSymbol, payload: MarketPanelRequest) -> OHLCVRequest:
    symbol = BusinessOHLCVSymbol(symbol=item) if isinstance(item, str) else item
    resolved = _resolve_business_symbol(
        symbol=symbol.symbol,
        asset_class=payload.asset_class,
        exchange=symbol.exchange or payload.exchange,
        currency=symbol.currency or payload.currency,
        primary_exchange=symbol.primary_exchange,
    )
    return OHLCVRequest(
        symbol=resolved.symbol,
        asset_class=payload.asset_class,
        exchange=resolved.exchange,
        currency=resolved.currency,
        primary_exchange=resolved.primary_exchange,
        duration=payload.duration,
        bar_size=payload.bar_size,
        start_datetime=payload.start_datetime,
        end_datetime=payload.end_datetime,
        what_to_show=payload.what_to_show,
        use_rth=payload.use_rth,
        last_trade_date_or_contract_month=symbol.last_trade_date_or_contract_month,
        multiplier=symbol.multiplier,
        local_symbol=symbol.local_symbol,
        con_id=symbol.con_id,
        sec_id_type=symbol.sec_id_type,
        sec_id=symbol.sec_id,
    )


def _resolve_business_symbol(
    *,
    symbol: str,
    asset_class: AssetClass,
    exchange: str | None = None,
    currency: str | None = None,
    primary_exchange: str | None = None,
) -> BusinessOHLCVSymbol:
    if asset_class is AssetClass.EQUITY:
        resolved = resolve_equity(symbol)
        return BusinessOHLCVSymbol(
            symbol=resolved.symbol,
            exchange=exchange or resolved.exchange,
            currency=currency or resolved.currency,
            primary_exchange=primary_exchange or resolved.primary_exchange or None,
        )
    if asset_class is AssetClass.FX:
        return BusinessOHLCVSymbol(
            symbol=symbol.strip().upper(),
            exchange=exchange or "IDEALPRO",
            currency=currency or symbol.strip().upper()[3:6] or "USD",
            primary_exchange=primary_exchange,
        )
    if asset_class is AssetClass.INDEX:
        return BusinessOHLCVSymbol(
            symbol=symbol.strip().upper(),
            exchange=exchange or "CBOE",
            currency=currency or "USD",
            primary_exchange=primary_exchange,
        )
    return BusinessOHLCVSymbol(
        symbol=symbol.strip().upper(),
        exchange=exchange or "SMART",
        currency=currency or "USD",
        primary_exchange=primary_exchange,
    )


def _bars_to_returns(*, asset_class: AssetClass, bar_size: str, bars: list[OHLCVBar]) -> ReturnsResponse:
    grouped: dict[str, list[OHLCVBar]] = defaultdict(list)
    for bar in bars:
        grouped[bar.symbol].append(bar)
    summaries: list[SymbolReturnSummary] = []
    warnings: list[str] = []
    for symbol, symbol_bars in sorted(grouped.items()):
        ordered = sorted(symbol_bars, key=lambda bar: bar.timestamp)
        points: list[ReturnPoint] = []
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if previous.close <= 0 or current.close <= 0:
                warnings.append(f"{symbol}: skipped non-positive close at {current.timestamp.isoformat()}")
                continue
            simple_return = current.close / previous.close - 1.0
            points.append(
                ReturnPoint(
                    symbol=symbol,
                    timestamp=current.timestamp,
                    close=current.close,
                    previous_close=previous.close,
                    simple_return=simple_return,
                    log_return=math.log(current.close / previous.close),
                )
            )
        cumulative_return = ordered[-1].close / ordered[0].close - 1.0 if len(ordered) >= 2 and ordered[0].close > 0 else None
        summaries.append(
            SymbolReturnSummary(
                symbol=symbol,
                observations=len(ordered),
                return_count=len(points),
                cumulative_return=cumulative_return,
                realized_volatility=_sample_volatility([point.log_return for point in points]),
                first_timestamp=ordered[0].timestamp if ordered else None,
                last_timestamp=ordered[-1].timestamp if ordered else None,
                points=points,
            )
        )
    return ReturnsResponse(asset_class=asset_class, bar_size=bar_size, summaries=summaries, warnings=warnings)


def _sample_volatility(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)
