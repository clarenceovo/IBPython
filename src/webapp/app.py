from __future__ import annotations

import logging
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.config.settings import Settings, load_settings
from src.webapp.dependencies import IBKRRestAppState, build_rest_app_state
from src.webapp.routers import account, market_data, reference_data, system

logger = logging.getLogger(__name__)

_app_instance: FastAPI | None = None
_app_lock = threading.Lock()


def create_app(
    *,
    settings: Settings | None = None,
    state: IBKRRestAppState | None = None,
) -> FastAPI:
    resolved_settings = settings or load_settings()
    app_state = state or build_rest_app_state(resolved_settings)

    @asynccontextmanager
    async def lifespan(fastapi_app: FastAPI) -> AsyncIterator[None]:
        fastapi_app.state.ibkr_rest_state = app_state
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
            "- **Market Data** — OHLCV bars, option analytics, bond yields, latest bars\n"
            "- **Reference Data** — Option chains, fundamentals, WSH events, news\n"
            "- **Account** — Positions, portfolio, P&L snapshots\n"
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
            {"name": "system", "description": "Health checks and cache management"},
            {"name": "market-data", "description": "OHLCV bars, option analytics, bond yields, and latest bar queries"},
            {"name": "reference-data", "description": "Option chains, fundamental data, Wall Street Horizon events, and news"},
            {"name": "account", "description": "Account summary, positions, portfolio items, and P&L snapshots"},
        ],
        openapi_url="/api/v1/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
    )
    fastapi_app.state.ibkr_rest_state = app_state
    fastapi_app.include_router(system.router, prefix="/api/v1")
    fastapi_app.include_router(market_data.router, prefix="/api/v1")
    fastapi_app.include_router(reference_data.router, prefix="/api/v1")
    fastapi_app.include_router(account.router, prefix="/api/v1")

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
    """Thread-safe lazy singleton for uvicorn --factory or module-level use."""
    global _app_instance
    if _app_instance is not None:
        return _app_instance
    with _app_lock:
        if _app_instance is None:
            _app_instance = create_app()
        return _app_instance
