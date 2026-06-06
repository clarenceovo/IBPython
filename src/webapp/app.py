from __future__ import annotations

import logging
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Callable, Awaitable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.config.settings import Settings, load_settings
from src.config.config_constant import APP_VERSION
from src.transport.metrics import MetricsMiddleware, metrics
from src.webapp.dependencies import IBKRRestAppState, build_rest_app_state
from src.webapp.middleware.correlation import CorrelationIdFilter, CorrelationIdMiddleware
from src.webapp.middleware.request_timeout import RequestTimeoutMiddleware

# Maximum request body size (10 MB). Prevents memory exhaustion from
# unbounded payloads.
_MAX_REQUEST_BODY_BYTES = 10 * 1024 * 1024

from src.webapp.middleware.body_limit import RequestBodyLimitMiddleware
from src.webapp.routers import (
    account,
    business,
    fixed_income,
    market_data_bonds,
    market_data_depth,
    market_data_equity,
    market_data_fx,
    market_data_futures,
    market_data_options,
    orders,
    reference_data,
    scanner,
    snapshot,
    streaming,
    system,
    tick_data,
)
from src.feeds.ibkr_historical import HistoricalRequestTooLargeError
from src.feeds.exceptions import (
    IBKRConnectionError,
    IBKRCircuitOpenError,
    IBKRContractResolutionError,
    IBKRMarketDataUnavailableError,
    IBKRPacingError,
    IBKROrderError,
    QuestDBWriteError,
    QuestDBConnectionError,
)

logger = logging.getLogger(__name__)

# Inject correlation_id into all log records from this application.
logging.getLogger().addFilter(CorrelationIdFilter())


class APIBearerAuthMiddleware:
    """Pure ASGI middleware for optional API-wide bearer token auth.

    When ``resolved_settings.ibkr_api_bearer_token`` is empty (default),
    requests pass through without auth — fully backward compatible.
    When set to a non-empty value, every request must include a valid
    ``Authorization: Bearer <token>`` header.
    """

    def __init__(self, app: Any, *, expected_token: str) -> None:
        self.app = app
        self._expected_token = expected_token

    async def __call__(self, scope: dict[str, Any], receive: Callable[[], Awaitable[dict[str, Any]]], send: Callable[[dict[str, Any]], Awaitable[None]]) -> None:
        if scope["type"] != "http" or not self._expected_token:
            await self.app(scope, receive, send)
            return

        # Extract Authorization header from ASGI scope
        headers = dict(scope.get("headers", []))
        auth_header = headers.get(b"authorization", b"")
        if isinstance(auth_header, bytes):
            auth_header = auth_header.decode("latin-1")

        if not auth_header.lower().startswith("bearer "):
            await self._send_401(send, "API bearer token required")
            return

        token = auth_header[7:].strip()
        if not token or not secrets.compare_digest(token, self._expected_token.strip()):
            await self._send_401(send, "invalid API bearer token")
            return

        await self.app(scope, receive, send)

    @staticmethod
    async def _send_401(send: Callable[[dict[str, Any]], Awaitable[None]], detail: str) -> None:
        import json as _json
        body = _json.dumps({"detail": detail}).encode("utf-8")
        await send({"type": "http.response.start", "status": 401, "headers": [
            [b"content-type", b"application/json"],
            [b"www-authenticate", b"Bearer"],
            [b"content-length", str(len(body)).encode()],
        ]})
        await send({"type": "http.response.body", "body": body})


def create_app(
    *,
    settings: Settings | None = None,
    state: IBKRRestAppState | None = None,
) -> FastAPI:
    resolved_settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(fastapi_app: FastAPI) -> AsyncIterator[None]:
        app_state = state or build_rest_app_state(resolved_settings)
        fastapi_app.state.ibkr_rest_state = app_state
        logger.info(
            "IBKRRestApp state initialized on active event loop: ibkr=%s:%s clientId=%s",
            resolved_settings.ibkr_host,
            resolved_settings.ibkr_port,
            resolved_settings.ibkr_client_id,
        )
        if resolved_settings.ibkr_rest_connect_on_startup:
            try:
                await app_state.connect()
            except Exception:
                logger.exception("IBKRRestApp startup failed — exiting to allow orchestrator restart")
                # Re-raise so uvicorn/gunicorn exits with non-zero code.
                # This prevents the service from running half-alive.
                raise
        try:
            yield
        finally:
            logger.info("IBKRRestApp shutdown requested; closing IBKR and Redis transports")
            # Cancel SSE subscription cleanup task (single-process).
            streaming.stop_cleanup_task()
            await app_state.close()

    fastapi_app = FastAPI(
        title=resolved_settings.ibkr_rest_app_name,
        version=APP_VERSION,
        description=(
            "# IBKR REST API\n\n"
            "Async FastAPI bridge for Interactive Brokers TWS/Gateway.\n\n"
            "## Modules\n"
            "- **Business** — Research-friendly wrappers for curves, news, market panels, returns, option skew, commodity futures, portfolio risk, and Event Contracts\n"
            "- **Fixed Income** — IBKR bond futures prices, CTD analytics, and futures-implied curves\n"
            "- **Market Data** — OHLCV bars, DOM/L2 snapshots, FX/commodity options, option analytics/skew, bond yields, and latest bars\n"
            "- **Reference Data** — Option chains, fundamentals, WSH events, news, contract search\n"
            "- **Account** — Positions, portfolio, P&L snapshots\n"
            "- **Orders** — Place, cancel, modify orders; execution details; explicit what-if margin preview\n"
            "- **Streaming** — Real-time market data via SSE\n"
            "- **Scanner** — Contract search across IBKR's security database\n"
            "- **System** — Health check, rate-limit diagnostics, cache management\n\n"
            "## Authentication\n"
            "There are two independent auth layers:\n"
            "1. **API-wide bearer token** (optional, env var `IBKR_API_BEARER_TOKEN`)\n"
            "   — When set, ALL endpoints require `Authorization: Bearer <token>`.\n"
            "   — When empty (default), auth is disabled for backward compatibility.\n"
            "2. **Order-specific Redis bearer token** (env var `IBKR_ORDER_AUTH_REDIS_KEY`)\n"
            "   — Required on all `/orders/*` endpoints regardless of the API-wide setting.\n"
            "   — Token payload is read from Redis.\n\n"
            "| Endpoint group | Auth status |\n"
            "|---|---|\n"
            "| All endpoints (when `IBKR_API_BEARER_TOKEN` is set) | `Authorization: Bearer <api-token>` required |\n"
            "| Orders (place/cancel/modify/preview) | Requires additional `Authorization: Bearer <token>` via Redis key `IBKR_ORDER_AUTH_REDIS_KEY` |\n"
            "| All other endpoints (when `IBKR_API_BEARER_TOKEN` is empty/default) | **No authentication** — bind to trusted networks or add an upstream gateway before production |\n"
            "\n"
            "**Note:** The order bearer token payload is read from Redis using `IBKR_ORDER_AUTH_REDIS_KEY`\n"
            "(default `OrderAuth::bearer_token`). Order endpoints validate the token; all other\n"
            "endpoints are currently unauthenticated by default.\n\n"
            "## Order Contract Notes\n"
            "- `/orders/preview` is the explicit IBKR what-if endpoint for margin and commission checks.\n"
            "- `/orders/place` is the live submission endpoint and must not automatically run what-if for every order.\n"
            "- Trailing stop limit requests should include `trail_stop_price` and `limit_price_offset`.\n"
            "- In-place modify is limited to `price`, `quantity`, and `tif`.\n\n"
            "## Rate Limits\n"
            "IBKR pacing limits apply. The app uses an internal controller for historical pacing, "
            "global outgoing messages, and market-data-line leases. "
            "`GET /api/v1/system/rate-limits` exposes the current limiter snapshot."
        ),
        lifespan=lifespan,
        servers=[
            {"url": "http://localhost:8000", "description": "Local development"},
        ],
        openapi_tags=[
            {"name": "business", "description": "Research-friendly wrappers for curves, symbol news, market panels, returns, option skew, commodity futures, portfolio risk, and ForecastEx/CME Event Contracts"},
            {"name": "system", "description": "Health checks, rate-limit diagnostics, and cache management"},
            {"name": "market-data", "description": "OHLCV bars, DOM/L2 snapshots, FX/commodity options, option analytics/skew, bond yields, and latest bar queries"},
            {"name": "reference-data", "description": "Option chains, fundamental data, Wall Street Horizon events, news, and contract search"},
            {"name": "account", "description": "Account summary, positions, portfolio items, and P&L snapshots"},
            {"name": "orders", "description": "Order management — live place/cancel/modify, execution details, and explicit what-if preview"},
            {"name": "scanner", "description": "Contract search and scanning across IBKR's security database"},
            {"name": "streaming", "description": "Real-time market data streaming via Server-Sent Events (SSE)"},
        ],
        openapi_url="/api/v1/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
    )
    # Metrics middleware must wrap early (added last = wraps outermost)
    fastapi_app.add_middleware(MetricsMiddleware)
    fastapi_app.add_middleware(CorrelationIdMiddleware)
    # Request deadline — prevents slow clients from occupying workers forever.
    fastapi_app.add_middleware(RequestTimeoutMiddleware, timeout_seconds=resolved_settings.ibkr_rest_request_timeout_seconds)
    # Reject oversized request bodies.
    fastapi_app.add_middleware(RequestBodyLimitMiddleware, max_bytes=_MAX_REQUEST_BODY_BYTES)

    # API-wide bearer auth — wraps all endpoints when IBKR_API_BEARER_TOKEN is set
    if resolved_settings.ibkr_api_bearer_token:
        fastapi_app.add_middleware(APIBearerAuthMiddleware, expected_token=resolved_settings.ibkr_api_bearer_token)

    fastapi_app.include_router(business.router, prefix="/api/v1")
    fastapi_app.include_router(fixed_income.router, prefix="/api/v1")
    fastapi_app.include_router(system.router, prefix="/api/v1")
    fastapi_app.include_router(market_data_equity.router, prefix="/api/v1")
    fastapi_app.include_router(market_data_depth.router, prefix="/api/v1")
    fastapi_app.include_router(market_data_futures.router, prefix="/api/v1")
    fastapi_app.include_router(market_data_fx.router, prefix="/api/v1")
    fastapi_app.include_router(market_data_options.router, prefix="/api/v1")
    fastapi_app.include_router(market_data_bonds.router, prefix="/api/v1")
    fastapi_app.include_router(reference_data.router, prefix="/api/v1")
    fastapi_app.include_router(account.router, prefix="/api/v1")
    fastapi_app.include_router(orders.router, prefix="/api/v1")
    fastapi_app.include_router(scanner.router, prefix="/api/v1")
    fastapi_app.include_router(streaming.router, prefix="/api/v1")
    fastapi_app.include_router(snapshot.router, prefix="/api/v1")
    fastapi_app.include_router(tick_data.router, prefix="/api/v1")

    @fastapi_app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint() -> bytes:
        """Expose Prometheus-compatible metrics in text format."""
        return metrics.expose().encode("utf-8")

    @fastapi_app.exception_handler(IBKRConnectionError)
    async def ibkr_connection_error_handler(_request: Request, exc: IBKRConnectionError) -> JSONResponse:
        logger.warning("IBKR connection error: %s", exc)
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    @fastapi_app.exception_handler(IBKRCircuitOpenError)
    async def ibkr_circuit_open_error_handler(_request: Request, exc: IBKRCircuitOpenError) -> JSONResponse:
        logger.warning("IBKR circuit breaker open: %s", exc)
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    @fastapi_app.exception_handler(IBKRContractResolutionError)
    async def ibkr_contract_resolution_error_handler(_request: Request, exc: IBKRContractResolutionError) -> JSONResponse:
        logger.warning("IBKR contract resolution error: %s", exc)
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @fastapi_app.exception_handler(IBKRMarketDataUnavailableError)
    async def ibkr_market_data_unavailable_handler(_request: Request, exc: IBKRMarketDataUnavailableError) -> JSONResponse:
        logger.warning("IBKR market data unavailable: %s", exc)
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    @fastapi_app.exception_handler(IBKRPacingError)
    async def ibkr_pacing_error_handler(_request: Request, exc: IBKRPacingError) -> JSONResponse:
        logger.warning("IBKR pacing violation: %s", exc)
        return JSONResponse(status_code=429, content={"detail": str(exc)})

    @fastapi_app.exception_handler(IBKROrderError)
    async def ibkr_order_error_handler(_request: Request, exc: IBKROrderError) -> JSONResponse:
        logger.warning("IBKR order error: %s", exc)
        return JSONResponse(status_code=502, content={"detail": str(exc)})

    @fastapi_app.exception_handler(HistoricalRequestTooLargeError)
    async def historical_request_too_large_handler(_request: Request, exc: HistoricalRequestTooLargeError) -> JSONResponse:
        logger.warning("historical OHLCV request rejected: %s", exc)
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @fastapi_app.exception_handler(QuestDBWriteError)
    async def questdb_write_error_handler(_request: Request, exc: QuestDBWriteError) -> JSONResponse:
        logger.error("QuestDB write error: %s", exc)
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    @fastapi_app.exception_handler(QuestDBConnectionError)
    async def questdb_connection_error_handler(_request: Request, exc: QuestDBConnectionError) -> JSONResponse:
        logger.error("QuestDB connection error: %s", exc)
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    @fastapi_app.exception_handler(RuntimeError)
    async def runtime_error_handler(_request: Request, exc: RuntimeError) -> JSONResponse:
        msg = str(exc)
        if "IBKR not available" in msg:
            logger.warning("request failed: %s", msg)
            return JSONResponse(status_code=503, content={"detail": msg})
        logger.exception("unhandled RuntimeError in request: %s", exc)
        return JSONResponse(status_code=503, content={"detail": msg})

    return fastapi_app


def get_app() -> FastAPI:
    """Uvicorn factory entrypoint.

    Return a fresh FastAPI object and build transport state inside lifespan so
    ib_insync, locks, and socket futures are owned by uvicorn's active loop.
    """

    return create_app()


# NOTE: Do NOT instantiate app at module level. Use `get_app()` via
# uvicorn --factory or `python -m src.webapp` entrypoint.
# This avoids importing heavy dependencies and reading .env at import time.
