from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict

from src.webapp.cache import CacheStats
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/system", tags=["system"])


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str  # "ok", "degraded", "down"
    app_name: str
    ibkr_connection: str | None = None
    redis_connection: str | None = None


@router.get("/health", response_model=HealthResponse)
async def health(state: IBKRRestAppState = Depends(get_rest_state)) -> HealthResponse:
    # IBKR status
    ibkr_status = None
    feed = getattr(state, "feed", None)
    if feed is not None:
        if hasattr(feed, "connection_status"):
            ibkr_status = feed.connection_status()
        elif getattr(feed, "_connection_dead", False):
            ibkr_status = "down"
        elif hasattr(feed, "_ib") and feed._ib is not None and feed._ib.isConnected():
            ibkr_status = "connected"
        else:
            ibkr_status = "disconnected"

    # Redis status
    redis_status = None
    if state.redis is not None:
        redis_ok = await state.redis.health_check()
        redis_status = "connected" if redis_ok else "down"

    # Aggregate status
    statuses = [s for s in [ibkr_status, redis_status] if s is not None]
    if any(s == "down" for s in statuses):
        overall = "degraded"
    elif ibkr_status == "connected":
        overall = "ok"
    else:
        overall = "degraded"

    return HealthResponse(
        status=overall,
        app_name=state.settings.ibkr_rest_app_name,
        ibkr_connection=ibkr_status,
        redis_connection=redis_status,
    )


@router.get("/cache/market-data", response_model=CacheStats)
async def market_data_cache_stats(state: IBKRRestAppState = Depends(get_rest_state)) -> CacheStats:
    return await state.market_data_cache.stats()


@router.delete("/cache/market-data", response_model=CacheStats)
async def clear_market_data_cache(state: IBKRRestAppState = Depends(get_rest_state)) -> CacheStats:
    await state.market_data_cache.clear()
    return await state.market_data_cache.stats()


@router.get("/rate-limits")
async def ibkr_rate_limits(state: IBKRRestAppState = Depends(get_rest_state)) -> dict:
    """Return the internal IBKR pacing controller snapshot."""
    feed = getattr(state, "feed", None)
    connection = getattr(feed, "_connection", None)
    snapshot = getattr(connection, "rate_limit_snapshot", None) if connection is not None else None
    if callable(snapshot):
        return await snapshot()
    return {"enabled": False, "reason": "not_configured"}


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
