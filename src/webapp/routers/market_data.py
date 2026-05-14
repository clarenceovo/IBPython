from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.bonds import BondYieldBar, BondYieldHistoryRequest
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.feeds.options import OptionAnalyticsRequest, OptionAnalyticsSnapshot
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


class CachedOptionAnalyticsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OptionAnalyticsRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


class CachedBondYieldHistoryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: BondYieldHistoryRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


@router.post("/ohlcv", response_model=list[OHLCVBar])
async def load_ohlcv(
    payload: HistoricalOHLCVLoadRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    async def load() -> list[OHLCVBar]:
        return await state.loader.load(
            payload.request,
            persist=payload.persist,
            cache_latest=payload.cache_latest,
        )

    if payload.use_ttl_cache and not payload.persist:
        key = stable_cache_key(
            "ohlcv",
            {
                "request": payload.request.model_dump(mode="json"),
                "cache_latest": payload.cache_latest,
            },
        )
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.get("/latest-bar", response_model=OHLCVBar | None)
async def get_latest_bar(
    asset_class: AssetClass,
    bar_size: str = Query(min_length=1),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> OHLCVBar | None:
    return await state.redis.get_latest_bar(asset_class, bar_size)


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
