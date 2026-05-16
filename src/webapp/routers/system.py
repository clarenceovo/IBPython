from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict

from src.webapp.cache import CacheStats
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/system", tags=["system"])


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str
    app_name: str
    ibkr_connection: str | None = None


@router.get("/health", response_model=HealthResponse)
async def health(state: IBKRRestAppState = Depends(get_rest_state)) -> HealthResponse:
    ibkr_status = None
    feed = getattr(state, "feed", None)
    if feed is not None:
        if getattr(feed, "_connection_dead", False):
            ibkr_status = "down"
        elif hasattr(feed, "_ib") and feed._ib is not None and feed._ib.isConnected():
            ibkr_status = "connected"
        else:
            ibkr_status = "disconnected"
    return HealthResponse(
        status="ok",
        app_name=state.settings.ibkr_rest_app_name,
        ibkr_connection=ibkr_status,
    )


@router.get("/cache/market-data", response_model=CacheStats)
async def market_data_cache_stats(state: IBKRRestAppState = Depends(get_rest_state)) -> CacheStats:
    return await state.market_data_cache.stats()


@router.delete("/cache/market-data", response_model=CacheStats)
async def clear_market_data_cache(state: IBKRRestAppState = Depends(get_rest_state)) -> CacheStats:
    await state.market_data_cache.clear()
    return await state.market_data_cache.stats()


@router.get("/scheduler/health")
async def scheduler_health(state: IBKRRestAppState = Depends(get_rest_state)) -> dict:
    """Return scheduler health status for all tracked jobs.

    Shows each job's last status, consecutive failures, last success time,
    and last error. Requires a health monitor to be configured on the
    scheduler.
    """
    scheduler = getattr(state, "scheduler", None)
    if scheduler is None or getattr(scheduler, "_health_monitor", None) is None:
        return {"status": "not_configured", "jobs": {}}
    report = scheduler._health_monitor.get_health_status()
    return report.model_dump(mode="json")
