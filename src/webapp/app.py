from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.config.settings import Settings
from src.webapp.dependencies import IBKRRestAppState, build_rest_app_state
from src.webapp.routers import account, market_data, reference_data, system


def create_app(
    *,
    settings: Settings | None = None,
    state: IBKRRestAppState | None = None,
) -> FastAPI:
    resolved_settings = settings or Settings()
    app_state = state or build_rest_app_state(resolved_settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.ibkr_rest_state = app_state
        if resolved_settings.ibkr_rest_connect_on_startup:
            await app_state.connect()
        try:
            yield
        finally:
            await app_state.close()

    app = FastAPI(
        title=resolved_settings.ibkr_rest_app_name,
        version="0.1.0",
        description="Async FastAPI bridge for IBKR market data, reference data, account, PnL, and position DTOs.",
        lifespan=lifespan,
    )
    app.state.ibkr_rest_state = app_state
    app.include_router(system.router, prefix="/api/v1")
    app.include_router(market_data.router, prefix="/api/v1")
    app.include_router(reference_data.router, prefix="/api/v1")
    app.include_router(account.router, prefix="/api/v1")

    @app.exception_handler(RuntimeError)
    async def runtime_error_handler(_request: Request, exc: RuntimeError) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    return app


app = create_app()
