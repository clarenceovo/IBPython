"""Regression tests for the shared IBKR market-data line controller."""

from __future__ import annotations

import pytest

from src.transport.ibkr_rate_limit import IBKRRateLimitController


@pytest.mark.asyncio
async def test_controller_acquires_and_releases_market_data_line() -> None:
    controller = IBKRRateLimitController(redis_client=None, market_data_lines=10, market_data_line_reserve=0)

    lease = await controller.acquire_market_data_line(
        contract_key="STK:SPY:SMART:USD",
        operation="snapshot:SPY",
    )
    snapshot = await controller.snapshot()

    assert snapshot["active_market_data_lines"] == 1

    await lease.release()
    snapshot = await controller.snapshot()
    assert snapshot["active_market_data_lines"] == 0


@pytest.mark.asyncio
async def test_controller_market_data_context_releases_after_exception() -> None:
    controller = IBKRRateLimitController(redis_client=None, market_data_lines=3, market_data_line_reserve=0)

    with pytest.raises(RuntimeError, match="subscription failed"):
        async with controller.market_data_line(contract_key="OPT:EURUSD:202606:C", operation="fx_option_snapshot"):
            snapshot = await controller.snapshot()
            assert snapshot["active_market_data_lines"] == 1
            raise RuntimeError("subscription failed")

    snapshot = await controller.snapshot()
    assert snapshot["active_market_data_lines"] == 0


@pytest.mark.asyncio
async def test_controller_market_data_release_is_idempotent() -> None:
    controller = IBKRRateLimitController(redis_client=None, market_data_lines=2, market_data_line_reserve=0)
    lease = await controller.acquire_market_data_line(
        contract_key="FUT:CL:202606:NYMEX:USD",
        operation="commodity_option_analytics",
    )

    await lease.release()
    await lease.release()

    snapshot = await controller.snapshot()
    assert snapshot["active_market_data_lines"] == 0


@pytest.mark.asyncio
async def test_controller_respects_market_data_reserve() -> None:
    controller = IBKRRateLimitController(redis_client=None, market_data_lines=10, market_data_line_reserve=2)

    snapshot = await controller.snapshot()

    assert snapshot["market_data_lines"] == 10
    assert snapshot["market_data_line_reserve"] == 2
    assert snapshot["max_active_market_data_lines"] == 8
