from __future__ import annotations

from fastapi import APIRouter, Depends, Response
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


class ReadinessResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str  # "ready" or "unavailable"
    app_name: str
    ibkr_connection: str | None = None
    redis_connection: str | None = None


@router.get("/health", response_model=HealthResponse)
async def health(state: IBKRRestAppState = Depends(get_rest_state)) -> HealthResponse:
    ibkr_status = _ibkr_connection_status(state)
    redis_status = await _redis_connection_status(state)

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


@router.get("/readiness", response_model=ReadinessResponse)
async def readiness(response: Response, state: IBKRRestAppState = Depends(get_rest_state)) -> ReadinessResponse:
    ibkr_status = _ibkr_connection_status(state)
    redis_status = await _redis_connection_status(state)
    ready = ibkr_status == "connected" and redis_status == "connected"
    if not ready:
        response.status_code = 503
    return ReadinessResponse(
        status="ready" if ready else "unavailable",
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


def _ibkr_connection_status(state: IBKRRestAppState) -> str | None:
    feed = getattr(state, "feed", None)
    if feed is None:
        return None
    if hasattr(feed, "connection_status"):
        return feed.connection_status()
    if getattr(feed, "_connection_dead", False):
        return "down"
    if hasattr(feed, "_ib") and feed._ib is not None and feed._ib.isConnected():
        return "connected"
    return "disconnected"


async def _redis_connection_status(state: IBKRRestAppState) -> str | None:
    if state.redis is None:
        return None
    redis_ok = await state.redis.health_check()
    return "connected" if redis_ok else "down"
