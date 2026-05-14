from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict

from src.webapp.cache import CacheStats
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/system", tags=["system"])


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    app_name: str


@router.get("/health", response_model=HealthResponse)
async def health(state: IBKRRestAppState = Depends(get_rest_state)) -> HealthResponse:
    return HealthResponse(status="ok", app_name=state.settings.ibkr_rest_app_name)


@router.get("/cache/market-data", response_model=CacheStats)
async def market_data_cache_stats(state: IBKRRestAppState = Depends(get_rest_state)) -> CacheStats:
    return await state.market_data_cache.stats()


@router.delete("/cache/market-data", response_model=CacheStats)
async def clear_market_data_cache(state: IBKRRestAppState = Depends(get_rest_state)) -> CacheStats:
    await state.market_data_cache.clear()
    return await state.market_data_cache.stats()
