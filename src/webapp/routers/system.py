"""System health, version, cache, and IBKR rate-limits.

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

from fastapi import APIRouter, Depends, HTTPException, Response, Query
from pydantic import BaseModel, ConfigDict, Field

from src.webapp.cache import CacheStats
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.feeds.exceptions import ConnectionStatus
from src.feeds.capabilities import GatewayCapabilities
from src.feeds.ibkr_connection import IBKRConnectionManager, IBKRConnectionSwitch
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

    status: str = Field(description="Always 'alive'.")  # always "alive"
    app_name: str = Field(description="Configured REST app name.")


class HealthResponse(BaseModel):
    """Legacy health response (checks dependencies). Prefer /live for liveness."""

    model_config = ConfigDict(extra="forbid")

    status: str = Field(description="'ok' or 'degraded'.")  # "ok", "degraded", "down"
    app_name: str = Field(description="Configured REST app name.")
    ibkr_connection: str | None = Field(default=None, description="IBKR connection state or null when no feed is attached.")
    redis_connection: str | None = Field(default=None, description="Redis connection state or null when not configured.")


class ReadinessResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str = Field(description="'ready' or 'unavailable'; unavailable responses carry HTTP 503.")  # "ready" or "unavailable"
    app_name: str = Field(description="Configured REST app name.")
    ibkr_connection: str | None = Field(default=None, description="IBKR connection state or null when no feed is attached.")
    redis_connection: str | None = Field(default=None, description="Redis connection state or null when not configured.")


class MarketDataTypeResponse(BaseModel):
    """Response model for market data type switch."""

    market_data_type: int = Field(description="Applied market data type: 1=Live, 2=Frozen, 3=Delayed, 4=Delayed Frozen.")
    status: str = Field(description="'ok' or the status reported by the feed.")


class IBKRClientConnectionStatus(BaseModel):
    """A process's last report; stale socket and lifecycle states are unknown."""

    enabled: bool | None = None
    desired_enabled: bool | None = None
    connection_status: str = "unknown"
    socket_connected: bool | None = None
    lifecycle_state: str = "unknown"
    generation: int | None = None
    redis_key: str | None = None
    client_id: int | None = None
    instance_id: str
    role: str = "unknown"
    last_checked_at: str | None = None
    last_error: str | None = None
    updated_at: str | None = None
    poll_seconds: float | None = None
    freshness: str = "fresh"
    heartbeat_age_seconds: float | None = None


class IBKRConnectionControlResponse(IBKRClientConnectionStatus):
    enabled: bool
    connection_status: str
    redis_key: str
    client_id: int
    clients: list[IBKRClientConnectionStatus] = Field(default_factory=list)
    clients_status: str = "unavailable"
    clients_error: str | None = None


def _connection_control_manager(state: IBKRRestAppState) -> IBKRConnectionManager:
    connection = getattr(state.feed, "_connection", None)
    if connection is None or connection._control_redis is None:
        raise HTTPException(status_code=503, detail="Redis IBKR connection control is not configured")
    return connection


async def _connection_control_response(connection: IBKRConnectionManager) -> IBKRConnectionControlResponse:
    local = connection.control_status()
    clients = {local["instance_id"]: IBKRClientConnectionStatus.model_validate(local)}
    clients_status, clients_error = "unavailable", "Client status listing is not configured"
    listing = getattr(connection._control_redis, "list_ibkr_client_status", None)
    if callable(listing):
        try:
            async with asyncio.timeout(_READINESS_CHECK_TIMEOUT_SECONDS):
                records = await listing(connection.connection_control_key)
            for record in records:
                client = IBKRClientConnectionStatus.model_validate(record)
                if client.instance_id != local["instance_id"]:
                    clients[client.instance_id] = client
            clients_status, clients_error = "available", None
        except Exception as exc:
            clients_error = f"Client status listing unavailable ({type(exc).__name__})"
    return IBKRConnectionControlResponse(
        **local, clients=sorted(clients.values(), key=lambda client: (client.role, client.client_id or 0, client.instance_id)),
        clients_status=clients_status, clients_error=clients_error,
    )


@router.get("/ibkr/connection", response_model=IBKRConnectionControlResponse)
async def get_ibkr_connection_control(state: IBKRRestAppState = Depends(get_rest_state)) -> IBKRConnectionControlResponse:
    """Read the switch, local state and last reported client states.

    Expired clients disappear; stale heartbeats report unknown socket state.
    Client reports are observations, not an acknowledgement from every client.
    """
    connection = _connection_control_manager(state)
    await connection.refresh_connection_control()
    result = await _connection_control_response(connection)
    if connection.connection_control_error is not None:
        raise HTTPException(status_code=503, detail={
            "message": "Redis IBKR connection switch is unreadable; connection is disabled",
            "connection": result.model_dump(),
        })
    return result


@router.put("/ibkr/connection", response_model=IBKRConnectionControlResponse)
async def set_ibkr_connection_control(
    payload: IBKRConnectionSwitch, state: IBKRRestAppState = Depends(get_rest_state),
) -> IBKRConnectionControlResponse:
    """Persist {\"enabled\": false/true} and apply it locally.

    Remote clients observe the flag independently. This response does not
    confirm that every client has stopped or finished reconnecting.
    """
    connection = _connection_control_manager(state)
    try:
        await connection.write_connection_control(payload)
    except Exception as exc:
        logger.warning("Failed to persist IBKR connection switch: %s", type(exc).__name__)
        result = await _connection_control_response(connection)
        raise HTTPException(status_code=503, detail={
            "message": "Could not persist IBKR connection switch in Redis",
            "connection": result.model_dump(),
        }) from exc
    return await _connection_control_response(connection)


class ServerTimeResponse(BaseModel):
    """Response model for IBKR server time."""

    server_time: str = Field(description="Current IBKR server time as a string.")
    timezone: str | None = Field(default=None, description="Timezone name reported by IBKR, when available.")


class VersionResponse(BaseModel):
    """Running application version."""

    model_config = ConfigDict(extra="forbid")

    version: str = Field(description="Application version from APP_VERSION.")


class RateLimitsNotConfiguredResponse(BaseModel):
    """Placeholder returned when no pacing controller is attached to the connection."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(description="Always false in this variant.")
    reason: str = Field(description="Why pacing is disabled, e.g. 'not_configured'.")


class RateLimitSnapshotResponse(BaseModel):
    """Snapshot of the internal IBKR pacing controller."""

    enabled: bool = Field(description="Whether request pacing is enforced.")
    redis_backed: bool = Field(description="Whether the controller coordinates through Redis.")
    redis_available: bool = Field(description="Whether Redis answered the last coordination call.")
    global_messages_per_second: int = Field(description="Configured global IBKR message budget per second.")
    global_messages_in_current_second: int = Field(description="Messages counted in the current one-second window.")
    market_data_lines: int = Field(description="Configured concurrent market-data line budget.")
    market_data_line_reserve: int = Field(description="Lines reserved from general market-data traffic.")
    max_active_market_data_lines: int = Field(description="Maximum lines available to this process.")
    active_market_data_lines: int = Field(description="Lines currently held.")
    market_data_lease_ttl_seconds: float = Field(description="TTL of a market-data line lease in seconds.")
    calls_by_operation: dict[str, int] = Field(description="Guarded calls counted per operation name.")
    wait_seconds_by_operation: dict[str, float] = Field(description="Accumulated wait time in seconds per operation.")
    redis_errors: int = Field(description="Number of Redis coordination failures so far.")


class DepthExchangeInfo(BaseModel):
    """Single exchange entry in the depth exchanges list."""

    exchange: str = Field(description="Exchange code, e.g. SMART or CBOE.")
    sec_type: str | None = Field(default=None, description="Security type the exchange supports depth for.")
    listing_rules: list[Any] | None = Field(default=None, description="Exchange listing rules as reported by IBKR.")


class DepthExchangesResponse(BaseModel):
    """Response model for L2 depth exchange list."""

    total_count: int = Field(description="Number of exchanges supporting L2 depth.")
    exchanges: list[Any] = Field(description="Exchange entries as reported by reqMktDepthExchanges.")


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


@router.get("/version", response_model=VersionResponse, summary="Get the running application version")
async def version() -> VersionResponse:
    """Return the running application version.

    Reads the packaged APP_VERSION constant; it does not contact IBKR or
    Redis. Use it to sanity-check which build an agent is talking to.
    """
    return VersionResponse(version=APP_VERSION)


@router.get("/health", response_model=HealthResponse)
async def health(state: IBKRRestAppState = Depends(get_rest_state)) -> HealthResponse:
    """Report aggregate service health from the IBKR and Redis dependencies.

    Returns status 'ok' only when IBKR reports connected; any down
    dependency or disconnected IBKR yields 'degraded'. Prefer /live for
    process liveness and /readiness for traffic gating.
    """
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
    """Return hit/miss and size statistics for the market-data response cache.

    Counters cover the process-local TTL cache used by the OHLCV and depth
    endpoints. Reads are cheap and do not disturb the cache.
    """
    return await state.market_data_cache.stats()


@router.delete("/cache/market-data", response_model=CacheStats)
async def clear_market_data_cache(state: IBKRRestAppState = Depends(get_rest_state)) -> CacheStats:
    """Clear every entry from the market-data response cache.

    Drops all cached bar/depth/snapshot responses for this process and
    returns the (now empty) cache statistics. The next identical request
    re-fetches from IBKR.
    """
    await state.market_data_cache.clear()
    return await state.market_data_cache.stats()


@router.get(
    "/rate-limits",
    summary="Get IBKR pacing controller snapshot",
    response_model=RateLimitSnapshotResponse | RateLimitsNotConfiguredResponse,
)
async def ibkr_rate_limits(
    state: IBKRRestAppState = Depends(get_rest_state),
) -> RateLimitSnapshotResponse | RateLimitsNotConfiguredResponse:
    """Return the internal IBKR pacing controller snapshot.

    Reports the configured message-per-second budget, market-data line
    usage, per-operation call counts and waits, and Redis coordination
    health. Returns {enabled: false, reason: 'not_configured'} when the
    connection has no pacing controller attached.
    """
    feed = getattr(state, "feed", None)
    connection = getattr(feed, "_connection", None)
    snapshot = getattr(connection, "rate_limit_snapshot", None) if connection is not None else None
    if callable(snapshot):
        return await snapshot()
    return RateLimitsNotConfiguredResponse(enabled=False, reason="not_configured")


# ------------------------------------------------------------------
# Migrated from alerts router (system-level IBKR endpoints)
# ------------------------------------------------------------------


class MarketDataTypeRequest(BaseModel):
    """Request body to switch market data type."""

    model_config = ConfigDict(extra="forbid")

    market_data_type: int = Field(
        ge=1,
        le=4,
        description="1=Live, 2=Frozen, 3=Delayed, 4=Delayed Frozen",
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
            asyncio.to_thread(_ibkr_connection_status, state),
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


@router.get("/capabilities", response_model=GatewayCapabilities)
async def get_capabilities(state: IBKRRestAppState = Depends(get_rest_state)) -> GatewayCapabilities:
    """Report local capabilities without opening a brokerage connection."""
    return state.feed.get_capabilities()
