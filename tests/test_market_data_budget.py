"""Tests for MarketDataLineBudget."""

import asyncio
import pytest

from src.feeds.market_data_budget import MarketDataBudgetExhausted, MarketDataLineBudget


@pytest.mark.asyncio
async def test_budget_acquire_and_release() -> None:
    budget = MarketDataLineBudget(max_lines=10)
    assert budget.active == 0
    assert budget.available == 10

    async with await budget.acquire(3):
        assert budget.active == 3
        assert budget.available == 7

    assert budget.active == 0
    assert budget.available == 10


@pytest.mark.asyncio
async def test_budget_raises_when_exhausted() -> None:
    budget = MarketDataLineBudget(max_lines=5)

    async with await budget.acquire(5):
        assert budget.available == 0
        with pytest.raises(MarketDataBudgetExhausted):
            await budget.acquire(1)


@pytest.mark.asyncio
async def test_budget_status() -> None:
    budget = MarketDataLineBudget(max_lines=100)
    status = budget.status()
    assert status["max_lines"] == 100
    assert status["active"] == 0
    assert status["available"] == 100
    assert status["utilization_pct"] == 0.0


@pytest.mark.asyncio
async def test_budget_utilization_pct() -> None:
    budget = MarketDataLineBudget(max_lines=100)
    async with await budget.acquire(50):
        assert budget.utilization_pct == 50.0


@pytest.mark.asyncio
async def test_budget_rejects_non_positive_count() -> None:
    budget = MarketDataLineBudget(max_lines=10)
    with pytest.raises(ValueError):
        await budget.acquire(0)
    with pytest.raises(ValueError):
        await budget.acquire(-1)


def test_budget_rejects_non_positive_max_lines() -> None:
    with pytest.raises(ValueError):
        MarketDataLineBudget(max_lines=0)
    with pytest.raises(ValueError):
        MarketDataLineBudget(max_lines=-1)
