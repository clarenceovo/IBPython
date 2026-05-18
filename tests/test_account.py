from __future__ import annotations

import asyncio
import sys
from types import ModuleType
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
from src.feeds import ibkr_feed
from src.feeds.ibkr_account_feed import IBKRAccountFeedClient


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


def test_account_pnl_snapshot_uses_rate_limiter_for_subscribe_and_cancel() -> None:
    class FakeIB:
        def __init__(self) -> None:
            self.cancelled: tuple[str, str] | None = None

        def reqPnL(self, account: str, model_code: str) -> SimpleNamespace:
            return SimpleNamespace(dailyPnL=1.0, unrealizedPnL=2.0, realizedPnL=3.0)

        def cancelPnL(self, account: str, model_code: str) -> None:
            self.cancelled = (account, model_code)

    class FakeConnection:
        def __init__(self) -> None:
            self.ib = FakeIB()
            self.rate_limit_calls: list[tuple[str, int]] = []

        async def ensure_connected(self) -> None:
            return None

        async def wait_for_ibkr_request(self, *, operation: str, weight: int = 1) -> None:
            self.rate_limit_calls.append((operation, weight))

    async def run() -> FakeConnection:
        connection = FakeConnection()
        client = IBKRAccountFeedClient(connection)  # type: ignore[arg-type]
        await client.load_account_pnl_snapshot("DU123", wait_seconds=0)
        return connection

    connection = asyncio.run(run())

    assert connection.rate_limit_calls == [
        ("account_pnl_subscribe:DU123", 1),
        ("account_pnl_cancel:DU123", 1),
    ]
    assert connection.ib.cancelled == ("DU123", "")


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


def test_ibkr_loop_getter_patch_uses_running_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    ib_pkg = ModuleType("ib_insync")
    ib_pkg.__path__ = []
    util = ModuleType("ib_insync.util")
    connection = ModuleType("ib_insync.connection")
    client = ModuleType("ib_insync.client")
    wrapper = ModuleType("ib_insync.wrapper")

    for module in (util, connection, client, wrapper):
        module.getLoop = lambda: object()

    monkeypatch.setitem(sys.modules, "ib_insync", ib_pkg)
    monkeypatch.setitem(sys.modules, "ib_insync.util", util)
    monkeypatch.setitem(sys.modules, "ib_insync.connection", connection)
    monkeypatch.setitem(sys.modules, "ib_insync.client", client)
    monkeypatch.setitem(sys.modules, "ib_insync.wrapper", wrapper)

    async def run() -> None:
        running_loop = asyncio.get_running_loop()
        ibkr_feed._patch_ib_insync_loop_getters()

        assert util.getLoop() is running_loop
        assert connection.getLoop() is running_loop
        assert client.getLoop() is running_loop
        assert wrapper.getLoop() is running_loop

    asyncio.run(run())
