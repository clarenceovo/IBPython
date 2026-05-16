from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.config.settings import Settings, load_settings
from src.webapp.dependencies import IBKRRestAppState, build_rest_app_state
from src.webapp.routers import account, business, fixed_income, market_data, reference_data, scanner, snapshot, streaming, system

logger = logging.getLogger(__name__)


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
            await app_state.connect()
        try:
            yield
        finally:
            await app_state.close()

    fastapi_app = FastAPI(
        title=resolved_settings.ibkr_rest_app_name,
        version="0.1.0",
        description=(
            "# IBKR REST API\n\n"
            "Async FastAPI bridge for Interactive Brokers TWS/Gateway.\n\n"
            "## Modules\n"
            "- **Business** — Research-friendly wrappers for curves, news, market panels, returns, and option skew\n"
            "- **Fixed Income** — IBKR bond futures prices, CTD analytics, and futures-implied curves\n"
            "- **Market Data** — OHLCV bars, option analytics/skew, bond yields, latest bars, equity snapshots\n"
            "- **Reference Data** — Option chains, fundamentals, WSH events, news, contract search\n"
            "- **Account** — Positions, portfolio, P&L snapshots\n"
            "- **Streaming** — Real-time market data via SSE\n"
            "- **Scanner** — Contract search across IBKR's security database\n"
            "- **System** — Health check, cache management\n\n"
            "## Authentication\n"
            "Not implemented yet. All endpoints are open when the service is running.\n\n"
            "## Rate Limits\n"
            "IBKR pacing limits apply. Historical data: 60 requests per 10 min window. "
            "TTL cache reduces redundant calls."
        ),
        lifespan=lifespan,
        servers=[
            {"url": "http://localhost:8000", "description": "Local development"},
        ],
        openapi_tags=[
            {"name": "business", "description": "Research-friendly wrappers for curves, symbol news, market panels, returns, and option skew"},
            {"name": "system", "description": "Health checks and cache management"},
            {"name": "market-data", "description": "OHLCV bars, option analytics/skew, bond yields, and latest bar queries"},
            {"name": "reference-data", "description": "Option chains, fundamental data, Wall Street Horizon events, news, and contract search"},
            {"name": "account", "description": "Account summary, positions, portfolio items, and P&L snapshots"},
            {"name": "scanner", "description": "Contract search and scanning across IBKR's security database"},
            {"name": "streaming", "description": "Real-time market data streaming via Server-Sent Events (SSE)"},
        ],
        openapi_url="/api/v1/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
    )
    fastapi_app.include_router(business.router, prefix="/api/v1")
    fastapi_app.include_router(fixed_income.router, prefix="/api/v1")
    fastapi_app.include_router(system.router, prefix="/api/v1")
    fastapi_app.include_router(market_data.router, prefix="/api/v1")
    fastapi_app.include_router(reference_data.router, prefix="/api/v1")
    fastapi_app.include_router(account.router, prefix="/api/v1")
    fastapi_app.include_router(scanner.router, prefix="/api/v1")
    fastapi_app.include_router(streaming.router, prefix="/api/v1")
    fastapi_app.include_router(snapshot.router, prefix="/api/v1")

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
