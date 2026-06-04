from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException
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


OPTION_CHAIN_REQUEST_EXAMPLES = {
    "tsla_equity_smart": {
        "summary": "TSLA equity option chain",
        "description": "SMART-routed US equities should include primary_exchange to remove IBKR contract ambiguity.",
        "value": {
            "request": {
                "symbol": "TSLA",
                "asset_class": "equity",
                "exchange": "SMART",
                "currency": "USD",
                "primary_exchange": "NASDAQ",
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 300,
        },
    },
    "spx_index_cboe": {
        "summary": "SPX index option chain",
        "description": "Index option chains use asset_class=index and the exchange where the index option complex is listed.",
        "value": {
            "request": {
                "symbol": "SPX",
                "asset_class": "index",
                "exchange": "CBOE",
                "currency": "USD",
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 300,
        },
    },
    "known_underlying_con_id": {
        "summary": "Known underlying conId",
        "description": "If you already know the IBKR underlying conId, pass it to skip underlying qualification. Verify conIds with your own IBKR session before relying on them operationally.",
        "value": {
            "request": {
                "symbol": "TSLA",
                "asset_class": "equity",
                "exchange": "SMART",
                "currency": "USD",
                "primary_exchange": "NASDAQ",
                "underlying_con_id": 76792991,
            },
            "use_ttl_cache": True,
            "cache_ttl_seconds": 300,
        },
    },
}


class CachedOptionChainRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OptionChainRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)


@router.post("/options/chains", response_model=list[OptionChain])
async def load_option_chains(
    payload: Annotated[CachedOptionChainRequest, Body(openapi_examples=OPTION_CHAIN_REQUEST_EXAMPLES)],
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
    await _ensure_entitled_news_provider_codes(request.provider_codes, state)
    return await state.feed.load_historical_news(request)


@router.post("/news/article", response_model=NewsArticle)
async def load_news_article(
    request: NewsArticleRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> NewsArticle:
    return await state.feed.load_news_article(request)


async def _ensure_entitled_news_provider_codes(provider_codes: tuple[str, ...], state: IBKRRestAppState) -> None:
    providers = await state.feed.load_news_providers()
    entitled_provider_codes = tuple(provider.provider_code for provider in providers)
    if not entitled_provider_codes:
        raise HTTPException(status_code=503, detail="IBKR news providers are not available for this account")
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
