from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.feeds.contracts import ContractSpec
from src.feeds.exceptions import IBKRConnectionError, IBKRMarketDataUnavailableError
from src.feeds.fundamental_data import WSHEventDataRequest
from src.feeds.ibkr_account_feed import IBKRAccountFeedClient
from src.feeds.ibkr_marketdata_ext import IBKRMarketDataExtClient
from src.feeds.ibkr_reference_feed import IBKRReferenceFeedClient
from src.feeds.streaming import StreamRequest
from src.feeds.tick_data import TickByTickData, TickType
from src.webapp.routers import streaming


class SessionConnection:
    """In-memory sessions with explicit generation changes; never opens a socket."""

    def __init__(self, ib: object) -> None:
        self.ib = ib
        self.generation = 0
        self.connection_enabled = True
        self.session_ended = asyncio.Event()
        self.market_data_type = 1
        self.callbacks = []
        self.cleanup = []
        self.leases = []
        self.wait_for_ibkr_request = AsyncMock()

    async def ensure_connected(self) -> None:
        assert self.ib is not None and self.connection_enabled

    async def with_retry(self, call, *, operation):
        return await call()

    async def acquire_market_data_line(self, **kwargs):
        lease = SimpleNamespace(release=AsyncMock())
        self.leases.append(lease)
        return lease

    def add_session_end_callback(self, callback) -> None:
        self.callbacks.append(callback)

    def retain_background_task(self, task) -> None:
        self.cleanup.append(task)

    def end_session(self, replacement=None, *, enabled=False) -> None:
        old = self.ib
        self.generation += 1
        self.connection_enabled = enabled
        self.session_ended.set()
        self.session_ended = asyncio.Event()
        for callback in self.callbacks:
            result = callback(old)
            if inspect.isawaitable(result):
                self.retain_background_task(asyncio.create_task(result))
        self.ib = replacement

    async def drain(self) -> None:
        if self.cleanup:
            await asyncio.gather(*self.cleanup)


def make_ib():
    subscribed = asyncio.Event()

    def market_data(contract, **kwargs):
        subscribed.set()
        return SimpleNamespace(contract=contract, last=100, bid=99, ask=101)

    return SimpleNamespace(
        subscribed=subscribed,
        reqMktData=Mock(side_effect=market_data),
        cancelMktData=Mock(),
        reqPnL=Mock(return_value=SimpleNamespace(dailyPnL=12, unrealizedPnL=3, realizedPnL=9)),
        reqPnLSingle=Mock(return_value=SimpleNamespace(dailyPnL=12, unrealizedPnL=3, realizedPnL=9)),
        cancelPnL=Mock(), cancelPnLSingle=Mock(),
        reqTickByTickData=Mock(return_value=SimpleNamespace()),
        cancelTickByTickData=Mock(),
        reqMarketDataType=Mock(),
        getWshMetaDataAsync=AsyncMock(return_value="{}"),
        getWshEventDataAsync=AsyncMock(return_value="{}"),
    )


@pytest.mark.parametrize("replacement", [False, True])
def test_ticker_cleanup_uses_owner_and_releases_without_current_ib(replacement):
    async def run():
        old, new = make_ib(), make_ib()
        connection = SessionConnection(old)
        reference = IBKRReferenceFeedClient(connection, None)
        captures = await reference.capture_equity_snapshots([("SPY", "SMART", "USD", "ARCA", 1)], snapshot_wait_seconds=0)
        ticker = captures[0].ticker
        # Reproduce cleanup after the IB field is cleared/replaced, independently of callbacks.
        connection.ib = new if replacement else None
        await reference.unsubscribe_ticker(ticker)
        await reference.cancel_equity_tickers([ticker])
        old.cancelMktData.assert_called_once_with(ticker.contract)
        new.cancelMktData.assert_not_called()
        connection.leases[0].release.assert_awaited_once()
        assert not reference._market_data_leases_by_ticker_id
        await connection.drain()

    asyncio.run(run())


def test_session_end_releases_tickers_and_rejects_inflight_snapshot():
    async def run():
        old, new = make_ib(), make_ib()
        connection = SessionConnection(old)
        reference = IBKRReferenceFeedClient(connection, None)
        pending = asyncio.create_task(reference.capture_equity_snapshots(
            [("SPY", "SMART", "USD", "ARCA", 1)], snapshot_wait_seconds=30,
        ))
        await old.subscribed.wait()
        connection.end_session(new, enabled=True)
        with pytest.raises(IBKRConnectionError):
            await asyncio.wait_for(pending, timeout=.2)
        await connection.drain()
        assert not reference._market_data_leases_by_ticker_id
        connection.leases[0].release.assert_awaited_once()
        new.cancelMktData.assert_not_called()

    asyncio.run(run())


def test_ticker_setup_interrupted_in_pacing_releases_lease_without_sending():
    async def run():
        old, new = make_ib(), make_ib()
        connection = SessionConnection(old)
        reference = IBKRReferenceFeedClient(connection, None)

        async def replace(**kwargs):
            connection.end_session(new, enabled=True)

        connection.wait_for_ibkr_request.side_effect = replace
        with pytest.raises(IBKRConnectionError):
            await reference.subscribe_ticker(ContractSpec(symbol="SPY", asset_class="equity"))
        old.reqMktData.assert_not_called()
        new.reqMktData.assert_not_called()
        connection.leases[0].release.assert_awaited_once()

    asyncio.run(run())


def test_wsh_metadata_is_requested_again_for_new_session():
    async def run():
        old, new = make_ib(), make_ib()
        connection = SessionConnection(old)
        reference = IBKRReferenceFeedClient(connection, None)
        await reference.load_wsh_event_data(WSHEventDataRequest(con_id=1))
        connection.end_session(new, enabled=True)
        assert not reference._wsh_metadata_loaded
        await reference.load_wsh_event_data(WSHEventDataRequest(con_id=1))
        old.getWshMetaDataAsync.assert_awaited_once()
        new.getWshMetaDataAsync.assert_awaited_once()

    asyncio.run(run())


@pytest.mark.parametrize("position", [False, True])
def test_pnl_rejects_old_session_values_and_cancels_only_its_owner(position):
    async def run():
        old, new = make_ib(), make_ib()
        connection = SessionConnection(old)
        accounts = IBKRAccountFeedClient(connection)
        started = asyncio.Event()
        request = old.reqPnLSingle if position else old.reqPnL
        value = request.return_value

        def subscribe(*args):
            started.set()
            return value

        request.side_effect = subscribe
        pending = asyncio.create_task(
            accounts.load_position_pnl_snapshot("DU1", 1, wait_seconds=30) if position
            else accounts.load_account_pnl_snapshot("DU1", wait_seconds=30)
        )
        await started.wait()
        connection.end_session(new, enabled=True)
        with pytest.raises(IBKRConnectionError):
            await asyncio.wait_for(pending, timeout=.2)
        cancel = old.cancelPnLSingle if position else old.cancelPnL
        cancel.assert_called_once()
        new.cancelPnL.assert_not_called()
        new.cancelPnLSingle.assert_not_called()

    asyncio.run(run())


def test_tick_buffers_and_subscriptions_clear_before_async_cleanup():
    async def run():
        old, new = make_ib(), make_ib()
        connection = SessionConnection(old)
        client = IBKRMarketDataExtClient(connection)
        await client.start_tick_by_tick("SPY")
        client._tick_buffers[("SPY", "STK", "SMART")].append(TickByTickData(
            timestamp=datetime.now(timezone.utc), tick_type=TickType.ALL_LAST, price=100,
        ))
        connection.end_session(new, enabled=True)
        assert client.get_latest_ticks("SPY") == []
        assert client.list_active_subscriptions() == []
        # A new subscription with the same key must survive the old owner's cleanup.
        await client.start_tick_by_tick("SPY")
        await connection.drain()
        assert len(client.list_active_subscriptions()) == 1
        old.cancelTickByTickData.assert_called_once()
        new.cancelTickByTickData.assert_not_called()
        connection.leases[0].release.assert_awaited_once()
        connection.leases[1].release.assert_not_awaited()
        await client.cancel_all_subscriptions()

    asyncio.run(run())


def test_market_data_type_persists_only_after_current_session_send():
    async def run():
        old, new = make_ib(), make_ib()
        connection = SessionConnection(old)
        client = IBKRMarketDataExtClient(connection)
        assert await client.set_market_data_type(3) == {"market_data_type": 3}
        assert connection.market_data_type == 3
        old.reqMarketDataType.assert_called_once_with(3)

        async def replace(**kwargs):
            connection.end_session(new, enabled=True)

        connection.wait_for_ibkr_request.side_effect = replace
        with pytest.raises(IBKRConnectionError):
            await client.set_market_data_type(4)
        assert connection.market_data_type == 3
        new.reqMarketDataType.assert_not_called()
        with pytest.raises(ValueError):
            await client.set_market_data_type(5)

    asyncio.run(run())


def test_market_depth_rejects_previous_session_and_cancels_owner():
    async def run():
        old, new = make_ib(), make_ib()
        started = asyncio.Event()
        ticker = SimpleNamespace(domBids=[SimpleNamespace(price=100, size=1)], domAsks=[])

        def subscribe(*args, **kwargs):
            started.set()
            return ticker

        old.reqMktDepth, old.cancelMktDepth = Mock(side_effect=subscribe), Mock()
        new.cancelMktDepth = Mock()
        connection = SessionConnection(old)
        client = IBKRMarketDataExtClient(connection)
        contract = SimpleNamespace(symbol="SPY", conId=1)
        pending = asyncio.create_task(client.load_market_depth_snapshot(
            contract=contract, spec=ContractSpec(symbol="SPY", asset_class="equity"),
            snapshot_wait_seconds=30, request_timeout_seconds=40,
        ))
        await started.wait()
        connection.end_session(new, enabled=True)
        with pytest.raises(IBKRMarketDataUnavailableError, match="connection changed"):
            await asyncio.wait_for(pending, timeout=.2)
        old.cancelMktDepth.assert_called_once_with(contract, isSmartDepth=False)
        new.cancelMktDepth.assert_not_called()
        connection.leases[0].release.assert_awaited_once()

    asyncio.run(run())


def test_realtime_bars_stop_on_session_change_and_cancel_owner():
    async def run():
        old, new = make_ib(), make_ib()
        bars = [SimpleNamespace(open=1, high=1, low=1, close=1, volume=1, wap=1, count=1)]
        old.reqRealTimeBars, old.cancelRealTimeBars = Mock(return_value=bars), Mock()
        new.cancelRealTimeBars = Mock()
        connection = SessionConnection(old)
        client = IBKRMarketDataExtClient(connection)
        stream = client.subscribe_realtime_bars("SPY")
        assert (await anext(stream))["close"] == 1
        connection.end_session(new, enabled=True)
        with pytest.raises(IBKRConnectionError):
            await asyncio.wait_for(anext(stream), timeout=.2)
        old.cancelRealTimeBars.assert_called_once_with(bars)
        new.cancelRealTimeBars.assert_not_called()

    asyncio.run(run())


@pytest.mark.parametrize("enabled,reason", [(False, "connection_disabled"), (True, "connection_lost")])
def test_sse_stops_promptly_without_emitting_old_ticks(enabled, reason):
    async def run():
        old, new = make_ib(), make_ib()
        connection = SessionConnection(old)
        ticker = SimpleNamespace(time=datetime.now(timezone.utc), last=100, bid=99, ask=101)
        feed = SimpleNamespace(
            _connection=connection, subscribe_ticker=AsyncMock(return_value=ticker), unsubscribe_ticker=AsyncMock(),
        )
        response = await streaming.stream_ticker(
            StreamRequest(symbol="SPY", update_interval_seconds=60), SimpleNamespace(feed=feed),
        )
        iterator = response.body_iterator
        try:
            assert (await anext(iterator)).startswith("data:")
            pending = asyncio.create_task(anext(iterator))
            await asyncio.sleep(0)
            connection.end_session(new if enabled else None, enabled=enabled)
            terminal = await asyncio.wait_for(pending, timeout=.2)
            assert terminal.startswith("event: done") and reason in terminal
            with pytest.raises(StopAsyncIteration):
                await anext(iterator)
            assert not streaming._active_subscriptions
            feed.unsubscribe_ticker.assert_awaited_once_with(ticker)
        finally:
            await iterator.aclose()
            streaming.stop_cleanup_task()

    asyncio.run(run())
