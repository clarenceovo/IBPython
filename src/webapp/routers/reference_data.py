from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.contracts import OptionChain, OptionChainRequest
from src.feeds.fundamental_data import (
    FundamentalDataReport,
    FundamentalDataRequest,
    WSHEventDataReport,
    WSHEventDataRequest,
    WSHMetadataReport,
)
from src.feeds.news import (
    HistoricalNewsHeadline,
    HistoricalNewsRequest,
    NewsArticle,
    NewsArticleRequest,
    NewsProvider,
)
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/reference-data", tags=["reference-data"])


class CachedOptionChainRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OptionChainRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)


@router.post("/options/chains", response_model=list[OptionChain])
async def load_option_chains(
    payload: CachedOptionChainRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OptionChain]:
    async def load() -> list[OptionChain]:
        return await state.feed.load_option_chains(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("option_chains", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post("/fundamentals", response_model=FundamentalDataReport)
async def load_fundamental_data(
    request: FundamentalDataRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> FundamentalDataReport:
    return await state.feed.load_fundamental_data(request)


@router.get("/wsh/metadata", response_model=WSHMetadataReport)
async def load_wsh_metadata(state: IBKRRestAppState = Depends(get_rest_state)) -> WSHMetadataReport:
    return await state.feed.load_wsh_metadata()


@router.post("/wsh/events", response_model=WSHEventDataReport)
async def load_wsh_event_data(
    request: WSHEventDataRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> WSHEventDataReport:
    return await state.feed.load_wsh_event_data(request)


@router.get("/news/providers", response_model=list[NewsProvider])
async def load_news_providers(state: IBKRRestAppState = Depends(get_rest_state)) -> list[NewsProvider]:
    return await state.feed.load_news_providers()


@router.post("/news/historical", response_model=list[HistoricalNewsHeadline])
async def load_historical_news(
    request: HistoricalNewsRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[HistoricalNewsHeadline]:
    return await state.feed.load_historical_news(request)


@router.post("/news/article", response_model=NewsArticle)
async def load_news_article(
    request: NewsArticleRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> NewsArticle:
    return await state.feed.load_news_article(request)
