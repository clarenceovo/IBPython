from __future__ import annotations

import asyncio

import pytest

from src.config.settings import load_settings
from src.feeds.models import AssetClass, OHLCVRequest
from src.webapp.dependencies import build_rest_app_state


def test_live_ibkr_redis_questdb_market_data_smoke() -> None:
    settings = load_settings()
    if not settings.ibpython_live_smoke:
        pytest.skip("set IBPYTHON_LIVE_SMOKE=1 to run live market-data smoke checks")

    async def run() -> None:
        state = build_rest_app_state(settings)
        await state.connect()
        try:
            bars = await state.feed.load_historical_ohlcv(
                OHLCVRequest(
                    symbol="SPY",
                    asset_class=AssetClass.EQUITY,
                    exchange="SMART",
                    currency="USD",
                    duration="1 D",
                    bar_size="1 day",
                    use_rth=True,
                ),
                max_chunks=settings.ibkr_historical_max_chunks,
            )
            assert bars
            await state.market_store.create_market_ohlcv_table()
        finally:
            await state.close()

    asyncio.run(run())
