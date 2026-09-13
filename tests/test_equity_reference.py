"""Exercise the real ib_insync decoder with a fake socket transport."""
import asyncio
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError

from src.feeds.capabilities import gateway_capabilities
from src.feeds.equity_reference import EquityReferenceClient, EquityReferenceRequest
from src.feeds.exceptions import IBKRConnectionError, IBKRMarketDataUnavailableError, IBKRUnsupportedFeatureError
from src.feeds.fundamental_data import FundamentalDataRequest
from src.feeds.ibkr_connection import IBKRConnectionManager
from src.feeds.ibkr_reference_feed import IBKRReferenceFeedClient


def setup_client(deliver=None):
    from ib_insync import IB, Stock
    ib = IB()
    ib.client._apiReady = True
    ib.client.updateReqId(1)
    ib.client.cancelMktData = Mock()
    ib.client.reqMktData = Mock()
    if deliver:
        ib.client.reqMktData.side_effect = lambda req_id, *_: asyncio.get_running_loop().call_soon(deliver, ib, req_id)
    leases = []

    async def acquire(**kwargs):
        lease = SimpleNamespace(release=AsyncMock(), **kwargs)
        leases.append(lease)
        return lease

    connection = SimpleNamespace(
        ib=ib, ensure_connected=AsyncMock(), _background_tasks=set(),
        wait_for_ibkr_request=AsyncMock(), acquire_market_data_line=acquire,
    )
    connection.market_data_request_id = IBKRConnectionManager.market_data_request_id.__get__(connection)
    connection.forget_market_data_ticker = IBKRConnectionManager.forget_market_data_ticker.__get__(connection)
    connection.retain_background_task = IBKRConnectionManager.retain_background_task.__get__(connection)
    historical = SimpleNamespace(qualify_contract=AsyncMock(return_value=Stock("AAPL", "SMART", "USD", conId=265598)))
    client = EquityReferenceClient(connection, historical)
    return client, connection, ib, leases


def emit(ib, req_id):
    ib.wrapper.reqId2Ticker[req_id].updateEvent.emit(ib.wrapper.reqId2Ticker[req_id])


def assert_clean(ib, leases, count=1):
    assert ib.client.cancelMktData.call_count == count
    assert not ib.wrapper.reqId2Ticker
    assert not ib.wrapper.tickers
    assert not ib.wrapper.ticker2ReqId["mktData"]
    assert not ib.wrapper._reqId2Contract
    for lease in leases:
        lease.release.assert_awaited_once()


def test_shortability_real_decoder_preserves_zero_and_rating():
    def deliver(ib, req_id):
        ib.wrapper.marketDataType(req_id, 3)
        ib.wrapper.tickGeneric(req_id, 46, 1.5)
        ib.wrapper.tickSize(req_id, 89, 0)
        emit(ib, req_id)

    async def run():
        client, conn, ib, leases = setup_client(deliver)
        result = await client.load_shortability(EquityReferenceRequest(symbol="aapl", timeout_seconds=.2))
        assert result.shortable_shares == 0
        assert result.shortability_rating == 1.5
        assert result.status == "available" and not result.timed_out
        assert result.observed_at is not None
        assert result.market_data_type == 3
        args = ib.client.reqMktData.call_args.args
        assert args[2:5] == ("236", False, False)
        assert conn.wait_for_ibkr_request.await_count == 2
        assert_clean(ib, leases)
    asyncio.run(run())


def test_dividend_tick59_decodes_date_and_amounts():
    def deliver(ib, req_id):
        ib.wrapper.marketDataType(req_id, 4)
        ib.wrapper.tickString(req_id, 59, '0,0.92,20261019,0.23')
        emit(ib, req_id)

    async def run():
        client, _, ib, leases = setup_client(deliver)
        result = await client.load_dividends(EquityReferenceRequest(symbol="AAPL", timeout_seconds=.2))
        assert result.past_12_months == 0
        assert result.next_12_months == .92
        assert result.next_dividend_amount == .23
        assert result.next_dividend_date == date(2026, 10, 19)
        assert result.status == "available"
        assert result.market_data_type == 4
        assert ib.client.reqMktData.call_args.args[2] == "456"
        assert_clean(ib, leases)
    asyncio.run(run())


@pytest.mark.parametrize("partial", [False, True])
def test_deadline_returns_missing_or_partial_data(partial):
    def deliver(ib, req_id):
        if partial:
            ib.wrapper.tickGeneric(req_id, 46, 2.5)
            emit(ib, req_id)

    async def run():
        client, _, ib, leases = setup_client(deliver)
        result = await client.load_shortability(EquityReferenceRequest(symbol="AAPL", timeout_seconds=.02))
        assert result.shortable_shares is None
        assert result.status == ("partial" if partial else "unavailable")
        assert result.timed_out
        assert result.market_data_type == (1 if partial else None)
        assert_clean(ib, leases)
    asyncio.run(run())


def test_cancellation_cleans_subscription():
    async def run():
        client, _, ib, leases = setup_client()
        task = asyncio.create_task(client.load_shortability(EquityReferenceRequest(symbol="AAPL")))
        await asyncio.sleep(0)
        assert ib.client.reqMktData.called
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert_clean(ib, leases)
    asyncio.run(run())


def test_concurrent_identical_contracts_are_isolated():
    def deliver(ib, req_id):
        ib.wrapper.tickSize(req_id, 89, 100 + req_id)
        emit(ib, req_id)

    async def run():
        client, _, ib, leases = setup_client(deliver)
        results = await asyncio.gather(*(client.load_shortability(EquityReferenceRequest(symbol="AAPL")) for _ in range(2)))
        assert {result.shortable_shares for result in results} == {101, 102}
        calls = ib.client.reqMktData.call_args_list
        assert calls[0].args[1] is not calls[1].args[1]
        assert_clean(ib, leases, count=2)
    asyncio.run(run())


@pytest.mark.parametrize("disconnect", [False, True])
def test_broker_errors_do_not_turn_into_success(disconnect):
    def deliver(ib, req_id):
        if disconnect:
            ib.disconnectedEvent.emit()
        else:
            ib.errorEvent.emit(req_id + 9, 354, "unrelated", None)
            ib.errorEvent.emit(req_id, 354, "Not subscribed", None)

    async def run():
        client, _, ib, leases = setup_client(deliver)
        with pytest.raises(IBKRConnectionError if disconnect else IBKRMarketDataUnavailableError):
            await client.load_shortability(EquityReferenceRequest(symbol="AAPL"))
        assert_clean(ib, leases)
    asyncio.run(run())


@pytest.mark.parametrize("warning_code", [399, 2104, 10090, 10167])
def test_non_terminal_warning_can_be_followed_by_valid_data(warning_code):
    def deliver(ib, req_id):
        ib.errorEvent.emit(req_id, warning_code, "Non-terminal market data message", None)
        ib.wrapper.tickSize(req_id, 89, 250)
        emit(ib, req_id)

    async def run():
        client, _, ib, leases = setup_client(deliver)
        result = await client.load_shortability(EquityReferenceRequest(symbol="AAPL"))
        assert result.status == "available"
        assert result.shortable_shares == 250
        assert_clean(ib, leases)
    asyncio.run(run())


def test_pacing_failure_before_subscription_releases_lease():
    async def run():
        client, conn, ib, leases = setup_client()
        conn.wait_for_ibkr_request.side_effect = RuntimeError("pacing failed")
        with pytest.raises(RuntimeError, match="pacing failed"):
            await client.load_shortability(EquityReferenceRequest(symbol="AAPL"))
        assert not ib.client.reqMktData.called
        leases[0].release.assert_awaited_once()
    asyncio.run(run())


def test_cancellation_pacing_failure_still_sends_cancel():
    def deliver(ib, req_id):
        ib.wrapper.tickSize(req_id, 89, 100)
        emit(ib, req_id)

    async def run():
        client, conn, ib, leases = setup_client(deliver)
        conn.wait_for_ibkr_request.side_effect = [None, RuntimeError("pacing failed")]
        with pytest.raises(RuntimeError, match="pacing failed"):
            await client.load_shortability(EquityReferenceRequest(symbol="AAPL"))
        assert_clean(ib, leases)
    asyncio.run(run())


def test_setup_deadline_is_an_error():
    async def run():
        client, conn, ib, _ = setup_client()
        async def slow():
            await asyncio.sleep(10)
        conn.ensure_connected.side_effect = slow
        with pytest.raises(IBKRMarketDataUnavailableError, match="setup"):
            await client.load_shortability(EquityReferenceRequest(symbol="AAPL", timeout_seconds=.01))
        assert not ib.client.reqMktData.called
    asyncio.run(run())


@pytest.mark.parametrize("payload", [{"symbol": " "}, {"symbol": "AAPL", "timeout_seconds": 21},
                                     {"symbol": "AAPL", "timeout_seconds": float('nan')},
                                     {"symbol": "AAPL", "con_id": 0}])
def test_invalid_requests(payload):
    with pytest.raises(ValidationError):
        EquityReferenceRequest(**payload)


def test_fundamentals_fail_without_connecting():
    async def run():
        connection = SimpleNamespace(ensure_connected=AsyncMock())
        client = IBKRReferenceFeedClient(connection, None)
        with pytest.raises(IBKRUnsupportedFeatureError, match="10.47"):
            await client.load_fundamental_data(FundamentalDataRequest(symbol="AAPL"))
        connection.ensure_connected.assert_not_called()
    asyncio.run(run())


def test_capabilities_do_not_infer_live_support(monkeypatch):
    disconnected = gateway_capabilities(SimpleNamespace(ib=None, is_connected=False))
    assert disconnected.negotiated_server_protocol is None
    assert disconnected.features['shortability'].client_supported is None
    ib = SimpleNamespace(
        client=SimpleNamespace(serverVersion=lambda: 176),
        reqMktData=lambda: None,
        wrapper=SimpleNamespace(tickSize=lambda: None, tickString=lambda: None),
    )
    connected = gateway_capabilities(SimpleNamespace(ib=ib, is_connected=True))
    assert connected.negotiated_server_protocol == 176
    assert connected.features['shortability'].client_supported is True
    assert connected.features['shortability'].availability == 'unknown'
    assert connected.features['fundamental_reports'].availability == 'unsupported'
    monkeypatch.setattr(ib, "wrapper", None)
    missing_decoder = gateway_capabilities(SimpleNamespace(ib=ib, is_connected=True))
    assert missing_decoder.features['shortability'].client_supported is False
    assert missing_decoder.features['dividends'].client_supported is False


def test_socket_send_failure_cleans_registered_ticker():
    async def run():
        client, _, ib, leases = setup_client()
        ib.client.reqMktData.side_effect = RuntimeError("send failed")
        with pytest.raises(RuntimeError, match="send failed"):
            await client.load_shortability(EquityReferenceRequest(symbol="AAPL"))
        assert_clean(ib, leases)
    asyncio.run(run())
