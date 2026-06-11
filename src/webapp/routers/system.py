"""System health, version, cache, IBKR rate-limits, and scheduler diagnostics.

NOTE (URL prefix consistency): Several market-data routers use legacy prefixes
that could be grouped under /market-data/ in a future v2 API:
  - /histogram          → /market-data/histogram
  - /realtime-bars      → /market-data/realtime-bars
  - /tick-data          → /market-data/tick-data
These are kept as-is for backward compatibility and will be migrated when a
v2 API versioning scheme is introduced.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Depends, Response, Query
from pydantic import BaseModel, ConfigDict, Field

from src.webapp.cache import CacheStats
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.feeds.exceptions import ConnectionStatus
from src.config.config_constant import APP_VERSION

router = APIRouter(prefix="/system", tags=["system"])
# Secondary router for endpoints outside the /system/ prefix (migrated from alerts).
_market_data_router = APIRouter(tags=["market-data"])

logger = logging.getLogger(__name__)

# Timeout for dependency checks in readiness probe.
_READINESS_CHECK_TIMEOUT_SECONDS = 2.0


class LivenessResponse(BaseModel):
    """Cheap liveness response — process is alive and event loop is responsive."""

    model_config = ConfigDict(extra="forbid")

    status: str  # always "alive"
    app_name: str


class HealthResponse(BaseModel):
    """Legacy health response (checks dependencies). Prefer /live for liveness."""

    model_config = ConfigDict(extra="forbid")

    status: str  # "ok", "degraded", "down"
    app_name: str
    ibkr_connection: str | None = None
    redis_connection: str | None = None


class ReadinessResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str  # "ready" or "unavailable"
    app_name: str
    ibkr_connection: str | None = None
    redis_connection: str | None = None


class MarketDataTypeResponse(BaseModel):
    """Response model for market data type switch."""

    market_data_type: int
    status: str


class ServerTimeResponse(BaseModel):
    """Response model for IBKR server time."""

    server_time: str
    timezone: str | None = None


class DepthExchangeInfo(BaseModel):
    """Single exchange entry in the depth exchanges list."""

    exchange: str
    sec_type: str | None = None
    listing_rules: list[Any] | None = None


class DepthExchangesResponse(BaseModel):
    """Response model for L2 depth exchange list."""

    total_count: int
    exchanges: list[Any]


@router.get("/live", response_model=LivenessResponse)
async def live(state: IBKRRestAppState = Depends(get_rest_state)) -> LivenessResponse:
    """Cheap liveness probe — no dependency checks, no I/O.

    Returns 200 as long as the process and event loop are responsive.
    Orchestrators should use this to decide whether to restart the container.
    """
    return LivenessResponse(
        status="alive",
        app_name=state.settings.ibkr_rest_app_name,
    )


@router.get("/version")
async def version() -> dict[str, str]:
    """Return the running application version."""
    return {"version": APP_VERSION}


@router.get("/health", response_model=HealthResponse)
async def health(state: IBKRRestAppState = Depends(get_rest_state)) -> HealthResponse:
    ibkr_status = _ibkr_connection_status(state)
    redis_status = await _redis_connection_status(state)

    # Aggregate status
    statuses = [s for s in [ibkr_status, redis_status] if s is not None]
    if any(s == ConnectionStatus.DOWN for s in statuses):
        overall = "degraded"
    elif ibkr_status == ConnectionStatus.CONNECTED:
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
    """Readiness probe — returns 503 when critical dependencies are unavailable.

    Each dependency check is bounded by a timeout so a slow dependency
    does not stall the readiness probe itself.
    """
    ibkr_status = await _ibkr_connection_status_with_timeout(state)
    redis_status = await _redis_connection_status(state)
    ready = ibkr_status == ConnectionStatus.CONNECTED and redis_status == "connected"
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


@router.get("/rate-limits", summary="Get IBKR pacing controller snapshot")
async def ibkr_rate_limits(state: IBKRRestAppState = Depends(get_rest_state)) -> dict:
    """Return the internal IBKR pacing controller snapshot."""
    feed = getattr(state, "feed", None)
    connection = getattr(feed, "_connection", None)
    snapshot = getattr(connection, "rate_limit_snapshot", None) if connection is not None else None
    if callable(snapshot):
        return await snapshot()
    return {"enabled": False, "reason": "not_configured"}


@router.get("/scheduler/health", summary="Get scheduler job health status")
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


# ------------------------------------------------------------------
# Migrated from alerts router (system-level IBKR endpoints)
# ------------------------------------------------------------------


class MarketDataTypeRequest(BaseModel):
    """Request body to switch market data type."""

    model_config = ConfigDict(extra="forbid")

    market_data_type: int = Field(
        ge=1,
        le=4,
        description="1=Live, 2=Delayed, 3=Delayed Frozen, 4=Delayed Off",
    )


@router.post(
    "/market-data-type",
    summary="Switch IBKR market data type",
    response_model=MarketDataTypeResponse,
)
async def set_market_data_type(
    payload: MarketDataTypeRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> MarketDataTypeResponse:
    """Switch the IBKR market data feed type.

    Values:
    - 1: Live (requires paid subscriptions)
    - 2: Delayed (15 min)
    - 3: Delayed Frozen
    - 4: Delayed Off
    """
    result = await state.feed.set_market_data_type(payload.market_data_type)
    if isinstance(result, dict):
        return MarketDataTypeResponse(
            market_data_type=payload.market_data_type,
            status=result.get("status", "ok"),
        )
    return MarketDataTypeResponse(
        market_data_type=payload.market_data_type,
        status="ok",
    )


@router.get(
    "/server-time",
    summary="Get IBKR server time",
    response_model=ServerTimeResponse,
)
async def get_server_time(
    state: IBKRRestAppState = Depends(get_rest_state),
) -> ServerTimeResponse:
    """Return the current IBKR server time."""
    result = await state.feed.get_server_time()
    if isinstance(result, dict):
        return ServerTimeResponse(
            server_time=str(result.get("server_time", result)),
            timezone=result.get("timezone"),
        )
    return ServerTimeResponse(
        server_time=str(result),
    )


# NOTE: This endpoint is under /market-data/ (outside the /system/ prefix).
# Defined on a separate router to avoid path prefix collision.
@_market_data_router.get(
    "/market-data/depth/exchanges",
    summary="Get L2 depth exchange list",
    response_model=DepthExchangesResponse,
    tags=["market-data"],
)
async def get_depth_exchanges(
    state: IBKRRestAppState = Depends(get_rest_state),
) -> DepthExchangesResponse:
    """Return exchanges supporting L2 market depth via ``reqMktDepthExchanges``."""
    exchanges = await state.feed.get_depth_exchanges()
    return DepthExchangesResponse(
        total_count=len(exchanges),
        exchanges=exchanges,
    )


# ------------------------------------------------------------------
# Dependency status helpers
# ------------------------------------------------------------------


def _ibkr_connection_status(state: IBKRRestAppState) -> str | None:
    """Return IBKR connection status (sync, no I/O)."""
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


async def _ibkr_connection_status_with_timeout(state: IBKRRestAppState) -> str | None:
    """IBKR connection status for readiness probe, bounded by timeout."""
    try:
        return await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _ibkr_connection_status, state),
            timeout=_READINESS_CHECK_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        logger.warning("IBKR readiness check timed out after %.1fs", _READINESS_CHECK_TIMEOUT_SECONDS)
        return ConnectionStatus.DOWN
    except Exception:
        return ConnectionStatus.DOWN


async def _redis_connection_status(state: IBKRRestAppState) -> str | None:
    """Redis connection status for health/readiness, bounded by timeout."""
    if state.redis is None:
        return None
    try:
        redis_ok = await asyncio.wait_for(
            state.redis.health_check(),
            timeout=_READINESS_CHECK_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        logger.warning("Redis readiness check timed out after %.1fs", _READINESS_CHECK_TIMEOUT_SECONDS)
        return ConnectionStatus.DOWN
    except Exception:
        return ConnectionStatus.DOWN
    return "connected" if redis_ok else "down"
