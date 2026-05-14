from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from src.feeds.account import (
    group_account_summary,
    normalize_account_pnl,
    normalize_account_values,
    normalize_portfolio_items,
    normalize_position_pnl,
    normalize_positions,
)
from src.feeds.ibkr_feed import IBKRFeedClient


def _contract() -> SimpleNamespace:
    return SimpleNamespace(conId=123, symbol="AAPL", secType="STK", exchange="NASDAQ", currency="USD")


def test_account_summary_normalization_groups_by_account() -> None:
    values = normalize_account_values(
        [
            SimpleNamespace(account="DU123", tag="NetLiquidation", value="100000", currency="USD", modelCode=""),
            SimpleNamespace(account="DU123", tag="BuyingPower", value="50000", currency="USD", modelCode=""),
        ]
    )

    summary = group_account_summary(values)

    assert summary[0].account == "DU123"
    assert summary[0].values["NetLiquidation"].value == "100000"


def test_position_and_portfolio_normalization() -> None:
    positions = normalize_positions(
        [SimpleNamespace(account="DU123", contract=_contract(), position=10, avgCost=150.25)]
    )
    portfolio = normalize_portfolio_items(
        [
            SimpleNamespace(
                account="DU123",
                contract=_contract(),
                position=10,
                marketPrice=155.0,
                marketValue=1550.0,
                averageCost=150.25,
                unrealizedPNL=47.5,
                realizedPNL=1e308,
            )
        ]
    )

    assert positions[0].symbol == "AAPL"
    assert portfolio[0].unrealized_pnl == pytest.approx(47.5)
    assert portfolio[0].realized_pnl is None


def test_pnl_normalization() -> None:
    account = normalize_account_pnl(
        SimpleNamespace(dailyPnL=10.5, unrealizedPnL=20.0, realizedPnL=-1.25),
        account="DU123",
    )
    position = normalize_position_pnl(
        SimpleNamespace(dailyPnL=2.5, unrealizedPnL=3.0, realizedPnL=1.0, position=4, value=620.0),
        account="DU123",
        con_id=123,
    )

    assert account.daily_pnl == pytest.approx(10.5)
    assert position.position == pytest.approx(4)
    assert position.value == pytest.approx(620.0)


def test_ibkr_feed_short_lived_pnl_snapshots_cancel_subscriptions() -> None:
    class FakeIB:
        def __init__(self) -> None:
            self.cancelled_account: tuple[str, str] | None = None
            self.cancelled_position: tuple[str, str, int] | None = None

        def isConnected(self) -> bool:
            return True

        def reqPnL(self, account: str, model_code: str) -> SimpleNamespace:
            return SimpleNamespace(dailyPnL=1.0, unrealizedPnL=2.0, realizedPnL=3.0)

        def cancelPnL(self, account: str, model_code: str) -> None:
            self.cancelled_account = (account, model_code)

        def reqPnLSingle(self, account: str, model_code: str, con_id: int) -> SimpleNamespace:
            return SimpleNamespace(dailyPnL=4.0, unrealizedPnL=5.0, realizedPnL=6.0, position=7, value=800.0)

        def cancelPnLSingle(self, account: str, model_code: str, con_id: int) -> None:
            self.cancelled_position = (account, model_code, con_id)

    fake_ib = FakeIB()
    client = IBKRFeedClient()
    client._ib = fake_ib

    async def run() -> tuple[object, object]:
        return (
            await client.load_account_pnl_snapshot("DU123", wait_seconds=0),
            await client.load_position_pnl_snapshot("DU123", 123, wait_seconds=0),
        )

    account, position = asyncio.run(run())

    assert account.daily_pnl == pytest.approx(1.0)
    assert position.position == pytest.approx(7)
    assert fake_ib.cancelled_account == ("DU123", "")
    assert fake_ib.cancelled_position == ("DU123", "", 123)


def test_ibkr_ensure_connected_preserves_root_cause_in_error_message() -> None:
    client = IBKRFeedClient(host="127.0.0.1", port=4001, client_id=44)

    async def failing_connect() -> None:
        raise ValueError("uvloop cannot be patched")

    client.connect = failing_connect  # type: ignore[method-assign]

    async def run() -> None:
        with pytest.raises(RuntimeError) as exc_info:
            await client._ensure_connected()
        message = str(exc_info.value)
        assert "127.0.0.1:4001" in message
        assert "clientId=44" in message
        assert "ValueError: uvloop cannot be patched" in message

    asyncio.run(run())
