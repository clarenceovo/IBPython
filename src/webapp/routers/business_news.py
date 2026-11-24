"""News endpoints for the business domain."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
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
    """Request body for symbol news: historical headlines plus optional article bodies."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1, description="Underlying symbol in the notation of asset_class, e.g. AAPL (equity), 0700.HK (HK equity), EUR.USD (fx).")
    asset_class: AssetClass = Field(default=AssetClass.EQUITY, description="Asset class used to qualify the symbol: equity, fx, future, bond, index, crypto, option.")
    exchange: str | None = Field(default=None, min_length=1, description="Exchange override for qualification; defaults are resolved per asset class (SMART for equities, IDEALPRO for fx).")
    currency: str | None = Field(default=None, min_length=1, description="ISO 4217 currency override, e.g. USD, HKD.")
    primary_exchange: str | None = Field(default=None, min_length=1, description="Primary listing exchange for SMART-routed equities, e.g. NASDAQ.")
    con_id: int | None = Field(default=None, gt=0, description="IBKR contract id; skips symbol qualification when supplied.")
    provider_codes: tuple[str, ...] | None = Field(default=None, description="News provider codes to query (e.g. DJNL, Reuters); null queries all entitled providers. Codes are uppercased.")
    total_results: int = Field(default=20, ge=1, le=300, description="Maximum number of headlines IBKR returns.")
    include_articles: bool = Field(default=False, description="Also fetch full article bodies for the returned headlines (one request per headline, bounded by max_concurrent_article_requests).")
    max_concurrent_article_requests: int = Field(default=4, ge=1, le=10, description="Upper bound on concurrent article fetches when include_articles is true.")

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
    """One news headline returned by the business news endpoints."""

    model_config = ConfigDict(extra="forbid")

    timestamp: datetime = Field(description="UTC publication time of the headline.")
    provider_code: str = Field(description="IBKR news provider code, e.g. DJNL.")
    article_id: str = Field(description="Provider-side article identifier for getNewsArticle lookups.")
    headline: str = Field(description="Headline text.")
    article: NewsArticle | None = Field(default=None, description="Full article body when include_articles was set and the fetch succeeded; null otherwise.")
    source: str = Field(default="ibkr_news", description="Source identifier.")


class SymbolNewsResponse(BaseModel):
    """Response envelope for symbol news."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(description="Uppercase symbol the news was queried for.")
    asset_class: AssetClass = Field(description="Asset class used for qualification.")
    con_id: int = Field(description="IBKR contract id the news is bound to.")
    provider_codes: tuple[str, ...] = Field(description="Provider codes actually queried.")
    headlines: list[BusinessNewsHeadline] = Field(description="Headlines, newest first as IBKR returns them, bounded by total_results.")


class CachedNewsArticleRequest(BusinessCacheControls):
    """Request body for a single news article body."""

    model_config = ConfigDict(extra="forbid")

    provider_code: str = Field(min_length=1, description="IBKR news provider code, e.g. DJNL; uppercased.")
    article_id: str = Field(min_length=1, description="Provider-side article identifier from a headline.")

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
    use_ttl_cache: bool = Query(default=True, description="Serve from the shared TTL cache when possible."),
    cache_ttl_seconds: float | None = Query(default=300, ge=0, description="Cache entry lifetime in seconds."),
    limit: int | None = Query(default=None, ge=1, le=1000, description="Return at most the first N providers; omit for all."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[NewsProvider]:
    """Return the news providers this account is entitled to query.

    Each entry carries a provider_code and a display name. Use the codes in
    getSymbolNews provider_codes and getNewsArticle lookups. An empty list
    means the account has no news entitlement (symbol-news endpoints then
    fail with 503). Responses are cached for cache_ttl_seconds by default.
    """
    providers = await _load_news_providers(use_ttl_cache=use_ttl_cache, cache_ttl_seconds=cache_ttl_seconds, state=state)
    return providers[:limit] if isinstance(limit, int) else providers


async def _load_news_providers(
    *, use_ttl_cache: bool, cache_ttl_seconds: float | None, state: IBKRRestAppState
) -> list[NewsProvider]:
    """Load entitled news providers with optional TTL caching."""
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
    """Return historical news headlines for one symbol, with optional article bodies.

    Qualifies the symbol to a con_id when one is not supplied, then loads up
    to total_results headlines from the entitled providers. When
    include_articles is true, article bodies are fetched concurrently and
    attached per headline. The response carries the resolved con_id and the
    provider codes used. Errors: 503 when no news providers are entitled,
    422 for provider codes that are not entitled, 404 when IBKR cannot
    qualify the symbol.
    """
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
    """Return the full body of one IBKR news article.

    Identify the article by provider_code plus article_id, exactly as a
    getSymbolNews headline reports them. The response carries the article
    text, type, and UTC receive time. Cached for cache_ttl_seconds by
    default.
    """
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
    providers = await _load_news_providers(
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

    raw_pairs = await asyncio.gather(*(load_one(headline) for headline in headlines), return_exceptions=True)
    pairs: list[tuple[tuple[str, str], NewsArticle]] = []
    for headline, raw in zip(headlines, raw_pairs, strict=True):
        if isinstance(raw, Exception):
            logging.getLogger("business_news").warning("Failed to load article %s/%s: %s", headline.provider_code, headline.article_id, raw)
            continue
        pairs.append(raw)  # type: ignore[arg-type]
    return dict(pairs)
