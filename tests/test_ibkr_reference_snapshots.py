from __future__ import annotations

import asyncio
from types import SimpleNamespace

from src.feeds.ibkr_feed import IBKRFeedClient


def test_equity_snapshot_uses_true_ibkr_snapshot_request() -> None:
    async def run() -> None:
        captured: dict[str, object] = {}

        class FakeIB:
            def isConnected(self) -> bool:
                return True

            def reqMktData(self, contract: object, generic_tick_list: str, snapshot: bool, regulatory_snapshot: bool) -> object:
                captured["contract"] = contract
                captured["generic_tick_list"] = generic_tick_list
                captured["snapshot"] = snapshot
                captured["regulatory_snapshot"] = regulatory_snapshot
                return SimpleNamespace(contract=contract, last=100.5, bid=100, ask=101)

            def cancelMktData(self, contract: object) -> None:
                captured["cancelled"] = contract

        client = IBKRFeedClient()
        client._ib = FakeIB()
        results = await client.capture_equity_snapshots(
            [("SPY", "SMART", "USD", "ARCA", 0)],
            snapshot_wait_seconds=0,
        )
        try:
            assert captured["generic_tick_list"] == ""
            assert captured["snapshot"] is True
            assert captured["regulatory_snapshot"] is False
            assert results[0].success is True
            assert results[0].symbol == "SPY"
        finally:
            await client.cancel_equity_tickers([result.ticker for result in results if result.ticker is not None])

        assert captured["cancelled"] is captured["contract"]

    asyncio.run(run())


def test_equity_snapshot_partial_failure_preserves_result_identity() -> None:
    async def run() -> None:
        class FakeIB:
            def isConnected(self) -> bool:
                return True

            def reqMktData(self, contract: object, generic_tick_list: str, snapshot: bool, regulatory_snapshot: bool) -> object:
                if getattr(contract, "symbol") == "AAPL":
                    raise RuntimeError("no market data")
                return SimpleNamespace(contract=contract, last=400.0, bid=399.5, ask=400.5)

            def cancelMktData(self, contract: object) -> None:
                return None

        client = IBKRFeedClient()
        client._ib = FakeIB()
        results = await client.capture_equity_snapshots(
            [
                ("AAPL", "SMART", "USD", "NASDAQ", 0),
                ("MSFT", "SMART", "USD", "NASDAQ", 0),
            ],
            snapshot_wait_seconds=0,
        )
        try:
            assert len(results) == 2
            assert results[0].symbol == "AAPL"
            assert results[0].ticker is None
            assert "no market data" in (results[0].error or "")
            assert results[1].symbol == "MSFT"
            assert results[1].success is True
        finally:
            await client.cancel_equity_tickers([result.ticker for result in results if result.ticker is not None])

    asyncio.run(run())
