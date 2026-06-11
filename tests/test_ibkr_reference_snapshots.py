from __future__ import annotations

import asyncio
from types import SimpleNamespace

from src.feeds.ibkr_feed import IBKRFeedClient
from src.feeds.scanner import MarketScannerFilter, MarketScannerRequest


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


def test_market_scanner_uses_ibkr_scanner_subscription() -> None:
    async def run() -> None:
        captured: dict[str, object] = {}

        class FakeIB:
            def isConnected(self) -> bool:
                return True

            async def reqScannerDataAsync(self, subscription: object, options: object, filters: object) -> list[object]:
                captured["subscription"] = subscription
                captured["options"] = options
                captured["filters"] = filters
                return [
                    SimpleNamespace(
                        rank=0,
                        contractDetails=SimpleNamespace(
                            contract=SimpleNamespace(
                                conId=12345,
                                symbol="0005",
                                secType="STK",
                                exchange="SEHK",
                                currency="HKD",
                                primaryExchange="SEHK",
                                localSymbol="0005",
                            ),
                            longName="HSBC Holdings PLC",
                            marketName="SEHK",
                        ),
                        distance="",
                        benchmark="",
                        projection="",
                        legsStr="",
                    )
                ]

        client = IBKRFeedClient()
        client._ib = FakeIB()
        rows = await client.run_market_scanner(
            MarketScannerRequest(
                instrument="STK",
                location_code="STK.HK",
                scan_code="HOT_BY_VOLUME",
                max_results=50,
                filters=[MarketScannerFilter(code="priceAbove", value=1)],
            )
        )

        subscription = captured["subscription"]
        assert getattr(subscription, "numberOfRows") == 50
        assert getattr(subscription, "instrument") == "STK"
        assert getattr(subscription, "locationCode") == "STK.HK"
        assert getattr(subscription, "scanCode") == "HOT_BY_VOLUME"
        assert captured["options"] == []
        assert [(item.tag, item.value) for item in captured["filters"]] == [("priceAbove", "1")]
        assert rows[0].con_id == 12345
        assert rows[0].symbol == "0005"
        assert rows[0].rank == 0

    asyncio.run(run())
