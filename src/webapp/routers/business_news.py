"""News endpoints for the business domain."""

from __future__ import annotations

import asyncio
from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.feeds.contracts import ContractSpec
from src.feeds.models import AssetClass
from src.feeds.news import (
    HistoricalNewsHeadline,
    HistoricalNewsRequest,
    NewsArticle,
    NewsArticleRequest,
    NewsProvider,
)
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.openapi_markdown import markdown_openapi_examples
from src.webapp.routers.business_shared import (
    BusinessCacheControls,
    BusinessDateRangeControls,
    resolve_business_symbol,
)

router = APIRouter()


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

    timestamp: object
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


async def _load_symbol_news(payload: SymbolNewsRequest, state: IBKRRestAppState) -> SymbolNewsResponse:
    providers = await get_news_providers(
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        state=state,
    )
    entitled_provider_codes = tuple(provider.provider_code for provider in providers)
    if not entitled_provider_codes:
        raise HTTPException(status_code=503, detail="IBKR news providers are not available for this account")

    provider_codes = payload.provider_codes
    if provider_codes is None:
        provider_codes = entitled_provider_codes
    else:
        entitled_set = set(entitled_provider_codes)
        unsupported = tuple(code for code in provider_codes if code not in entitled_set)
        if unsupported:
            raise HTTPException(
                status_code=422,
                detail=(
                    "IBKR news provider code(s) not entitled for this account: "
                    f"{', '.join(unsupported)}. Entitled provider codes: {', '.join(entitled_provider_codes)}"
                ),
            )

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
    resolved = resolve_business_symbol(
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
