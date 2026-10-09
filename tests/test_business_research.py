"""Research endpoints, browser access and cancellation contracts."""
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient
from ib_insync import IB, Stock

from src.feeds.equity_reference import DividendsResponse, ShortabilityResponse
from src.feeds.ibkr_feed import IBKRFeedClient
from src.webapp.app import create_app
from src.webapp.cache import AsyncTTLCache
from src.webapp.routers.business_equity import EquityOverviewRequest, _load_equity_overview
from test_webapp import FakeState


def research_state():
    state = FakeState()
    state.settings.redis_url = ""
    state.settings.ibkr_rest_connect_on_startup = False
    state.settings.ibkr_api_bearer_token = ""
    return state


def overview_state():
    state = research_state()
    state.settings.ibkr_equity_snapshot_wait_seconds = .01
    state.feed.load_equity_shortability = AsyncMock(return_value=ShortabilityResponse(
        symbol="SPY", con_id=8314, exchange="SMART", currency="USD", status="available", shortable_shares=0,
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc), market_data_type=3,
    ))
    state.feed.load_equity_dividends = AsyncMock(return_value=DividendsResponse(
        symbol="SPY", con_id=8314, exchange="SMART", currency="USD", status="available", past_12_months=0,
        next_12_months=0, next_dividend_amount=0, next_dividend_date="2026-10-19",
    ))
    return state


def test_batch_failures_empty_data_legacy_projection_and_returns():
    state = research_state()
    original = state.loader.load

    async def load(request, **kwargs):
        if request.symbol == "BAD":
            raise RuntimeError("not entitled")
        if request.symbol == "EMPTY":
            return []
        return await original(request, **kwargs)

    state.loader.load = load
    payload = {"symbols": ["SPY", "BAD", "EMPTY"], "use_ttl_cache": False}
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        detailed = client.post('/api/v1/business/getMarketPanelDetailed', json=payload).json()
        assert [result['input_index'] for result in detailed['results']] == [0, 1, 2]
        assert [result['status'] for result in detailed['results']] == ['success', 'error', 'empty']
        assert detailed['results'][1]['error'] == 'not entitled'
        legacy = client.post('/api/v1/business/getMarketPanel', json=payload).json()
        assert isinstance(legacy, list) and {bar['symbol'] for bar in legacy} == {'SPY'}
        returns = client.post('/api/v1/business/getReturns', json=payload).json()
        assert {summary['symbol'] for summary in returns['summaries']} == {'SPY'}
        assert any('BAD' in warning for warning in returns['warnings'])
        assert any('EMPTY' in warning for warning in returns['warnings'])


def test_batch_preserves_distinct_contracts_and_omits_ambiguous_returns():
    state = research_state()
    payload = {"asset_class": "future", "symbols": [
        {"symbol": "CL", "exchange": "NYMEX", "con_id": 11, "last_trade_date_or_contract_month": "202611"},
        {"symbol": "CL", "exchange": "NYMEX", "con_id": 12, "last_trade_date_or_contract_month": "202612"},
    ], "use_ttl_cache": False}
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        results = client.post('/api/v1/business/getMarketPanelDetailed', json=payload).json()['results']
        assert [item['request']['con_id'] for item in results] == [11, 12]
        assert [item['bars'][0]['contract_month'] for item in results] == ['202611', '202612']
        returns = client.post('/api/v1/business/getReturns', json=payload).json()
        assert returns['summaries'] == []
        assert 'ambiguous' in returns['warnings'][0]


def test_detailed_universe_and_shared_cache():
    state = research_state()
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        payload = {'universe': 'explicit', 'symbols': ['SPY', 'QQQ'], 'max_symbols': 1}
        first = client.post('/api/v1/business/getUniverseBarsDetailed', json=payload).json()
        assert first['results'][0]['request']['symbol'] == 'SPY'
        legacy = client.post('/api/v1/business/getUniverseBars', json=payload).json()
        assert legacy == first['results'][0]['bars']
        assert state.loader.calls == 1
        assert client.post('/api/v1/business/getUniverseBarsDetailed', json={'universe': 'missing'}).status_code == 404
        panel = {'symbols': ['QQQ']}
        first = client.post('/api/v1/business/getMarketPanelDetailed', json=panel).json()
        second = client.post('/api/v1/business/getMarketPanelDetailed', json=panel).json()
        assert first == second


def test_failed_batch_is_not_cached_and_can_recover():
    state = research_state()
    original = state.loader.load
    state.loader.load = AsyncMock(side_effect=RuntimeError('temporary failure'))
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        payload = {'symbols': ['SPY']}
        assert client.post('/api/v1/business/getMarketPanelDetailed', json=payload).json()['results'][0]['status'] == 'error'
        state.loader.load = original
        assert client.post('/api/v1/business/getMarketPanelDetailed', json=payload).json()['results'][0]['status'] == 'success'


def test_overview_defaults_zero_values_cached_times_and_contract_identity():
    state = overview_state()
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        first = client.post('/api/v1/business/getEquityOverview', json={'symbol': 'spy'}).json()
        assert first['quote']['con_id'] == first['con_id'] == 8314
        assert first['quote_observed_at'] == first['quote']['timestamp']
        assert first['shortability']['shortable_shares'] == 0
        assert first['dividends']['past_12_months'] == 0
        assert first['news'] is None and first['section_status']['news'] == 'not_requested'
        assert state.feed.provider_calls == 0
        assert len(state.feed.cancelled_equity_tickers) == 1
        second = client.post('/api/v1/business/getEquityOverview', json={'symbol': 'spy'}).json()
        assert first.pop('from_cache') is False
        assert second.pop('from_cache') is True
        assert second == first
        state.feed.load_equity_shortability.assert_awaited_once()
        assert state.feed.load_equity_shortability.call_args.args[0].con_id == 8314
        assert client.post('/api/v1/business/getEquityOverview', json={'symbol': 'spy', 'timeout_seconds': 21}).status_code == 422


def test_overview_missing_observation_time_remains_null():
    state = overview_state()
    capture = state.feed.capture_equity_snapshots

    async def missing_time(*args, **kwargs):
        results = await capture(*args, **kwargs)
        results[0].ticker.time = None
        return results

    state.feed.capture_equity_snapshots = missing_time
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        data = client.post('/api/v1/business/getEquityOverview', json={'symbol': 'SPY'}).json()
        assert data['quote']['last'] == 100.5
        assert data['quote_observed_at'] is None
        assert data['quote_received_at'] is not None


def test_overview_partial_reference_news_limits_and_disabled_sections():
    state = overview_state()
    state.feed.load_equity_dividends = AsyncMock(return_value=DividendsResponse(
        symbol='SPY', exchange='SMART', currency='USD', status='partial', next_dividend_amount=0, timed_out=True,
    ))
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        payload = {'symbol': 'SPY', 'include_news': True, 'news_limit': 1}
        data = client.post('/api/v1/business/getEquityOverview', json=payload).json()
        assert data['section_status']['dividends'] == 'partial'
        assert data['dividends']['next_dividend_amount'] == 0
        assert len(data['news']['headlines']) == 1
        assert state.feed.historical_news_requests[0].total_results == 1
        assert data['warnings']
        client.post('/api/v1/business/getEquityOverview', json=payload)
        assert state.feed.load_equity_dividends.await_count == 2
        data = client.post('/api/v1/business/getEquityOverview', json={
            'symbol': 'SPY', 'include_shortability': False, 'include_dividends': False,
        }).json()
        assert data['section_status']['shortability'] == data['section_status']['dividends'] == 'not_requested'


def test_overview_feed_failure_does_not_discard_other_sections():
    state = overview_state()
    state.feed.load_equity_dividends = AsyncMock(side_effect=RuntimeError('not entitled'))
    state.feed.news_providers = []
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        data = client.post('/api/v1/business/getEquityOverview', json={'symbol': 'SPY', 'include_news': True}).json()
        assert data['section_status']['quote'] == data['section_status']['shortability'] == 'available'
        assert data['dividends'] is None and data['section_status']['dividends'] == 'unavailable'
        assert data['news'] is None and data['section_status']['news'] == 'unavailable'
        assert len(data['warnings']) == 2


@pytest.mark.parametrize('cleanup_result', [1, RuntimeError('cleanup failed')])
def test_overview_incomplete_quote_cleanup_preserves_data_without_caching(cleanup_result):
    state = overview_state()
    state.feed.cancel_equity_tickers = AsyncMock()
    if isinstance(cleanup_result, Exception):
        state.feed.cancel_equity_tickers.side_effect = cleanup_result
    else:
        state.feed.cancel_equity_tickers.return_value = cleanup_result
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        payload = {'symbol': 'SPY'}
        first = client.post('/api/v1/business/getEquityOverview', json=payload).json()
        assert first['quote']['last'] == 100.5
        assert first['section_status']['quote'] == 'partial'
        assert first['warnings']
        client.post('/api/v1/business/getEquityOverview', json=payload)
        assert state.feed.cancel_equity_tickers.await_count == 2


def test_overview_news_deadline_retains_completed_quote():
    async def run():
        state = overview_state()
        cancelled = asyncio.Event()

        async def stalled_news(request):
            try:
                await asyncio.sleep(10)
            finally:
                cancelled.set()

        state.feed.load_historical_news = stalled_news
        result = await _load_equity_overview(EquityOverviewRequest(
            symbol='SPY', timeout_seconds=.03, include_news=True, include_dividends=False, include_shortability=False,
        ), state)
        assert result.quote is not None and result.section_status['news'] == 'unavailable'
        assert cancelled.is_set() and len(state.feed.cancelled_equity_tickers) == 1
    asyncio.run(run())


def test_overview_qualification_uses_deadline():
    state = overview_state()
    async def stall(_):
        await asyncio.sleep(10)

    state.feed.qualify_contract = stall
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        assert client.post('/api/v1/business/getEquityOverview', json={'symbol': 'SPY', 'timeout_seconds': .02}).status_code == 503
        state.feed.load_equity_dividends.assert_not_awaited()


def test_real_decoder_overview_cleans_all_three_subscriptions():
    async def run():
        state = overview_state()
        state.settings.ibkr_equity_snapshot_wait_seconds = 11.5
        feed = IBKRFeedClient()
        ib = IB()
        ib.client._apiReady = True
        ib.client.updateReqId(1)
        ib.client.cancelMktData = Mock()
        contract = Stock('SPY', 'SMART', 'USD', conId=8314)
        feed._ib = ib
        feed.qualify_contract = AsyncMock(return_value=contract)
        feed._historical.qualify_contract = AsyncMock(return_value=contract)
        leases = []

        async def acquire(**kwargs):
            lease = SimpleNamespace(release=AsyncMock())
            leases.append(lease)
            return lease

        feed._connection.acquire_market_data_line = acquire
        feed._connection.wait_for_ibkr_request = AsyncMock()

        def deliver(req_id, contract, generic_ticks, *_):
            def ticks():
                ib.wrapper.marketDataType(req_id, 3)
                if generic_ticks == '236':
                    ib.wrapper.tickSize(req_id, 89, 0)
                elif generic_ticks == '456':
                    ib.wrapper.tickString(req_id, 59, '0,0,20261019,0')
                else:
                    ticker = ib.wrapper.reqId2Ticker[req_id]
                    ticker.last, ticker.bid, ticker.ask, ticker.open = 100, 99, 101, 98
                    ticker.time = datetime.now(timezone.utc)
                ticker = ib.wrapper.reqId2Ticker[req_id]
                ticker.updateEvent.emit(ticker)
            asyncio.get_running_loop().call_later(.01, ticks)

        ib.client.reqMktData = Mock(side_effect=deliver)
        state.feed = feed
        result = await _load_equity_overview(EquityOverviewRequest(symbol='SPY', timeout_seconds=.1), state)
        assert all(result.section_status[name] == 'available' for name in ('quote', 'shortability', 'dividends'))
        assert result.quote_market_data_type == 3
        assert result.quote.open == 98 and result.quote.halted is None
        assert result.shortability.shortable_shares == 0
        assert ib.client.cancelMktData.call_count == 3
        assert not ib.wrapper.tickers and not ib.wrapper.reqId2Ticker
        assert not ib.wrapper.ticker2ReqId['mktData']
        for lease in leases:
            lease.release.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize('cancel_caller', [False, True])
def test_real_quote_wait_deadline_and_caller_cancellation_clean_up(cancel_caller):
    async def run():
        state = overview_state()
        feed = IBKRFeedClient()
        ib = IB()
        ib.client._apiReady = True
        ib.client.updateReqId(1)
        started = asyncio.Event()
        ib.client.reqMktData = Mock(side_effect=lambda *args: started.set())
        ib.client.cancelMktData = Mock()
        feed._ib = ib
        feed.qualify_contract = AsyncMock(return_value=Stock('SPY', 'SMART', 'USD', conId=8314))
        lease = SimpleNamespace(release=AsyncMock())
        feed._connection.acquire_market_data_line = AsyncMock(return_value=lease)
        feed._connection.wait_for_ibkr_request = AsyncMock()
        state.feed = feed
        task = asyncio.create_task(_load_equity_overview(EquityOverviewRequest(
            symbol='SPY', timeout_seconds=.03 if not cancel_caller else 10,
            include_dividends=False, include_shortability=False,
        ), state))
        await asyncio.wait_for(started.wait(), 2)
        ticker = next(iter(ib.wrapper.tickers.values()))
        if cancel_caller:
            # Allow capture setup to hand ownership to the quote worker.
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
        else:
            result = await asyncio.wait_for(task, 2)
            assert result.quote is None and result.section_status['quote'] == 'unavailable'
        assert not len(ticker.updateEvent)
        assert not ib.wrapper.tickers and not ib.wrapper.reqId2Ticker
        assert not feed._reference._market_data_leases_by_ticker_id
        ib.client.cancelMktData.assert_called_once()
        lease.release.assert_awaited_once()
    asyncio.run(run())


def test_conditional_cache_keeps_coalescing_and_recovers_after_failure():
    async def run():
        cache = AsyncTTLCache(ttl_seconds=5, max_size=4)
        factory = AsyncMock(return_value=False)
        assert await cache.get_or_set('result', factory, cache_if=bool) is False
        assert await cache.get('result') is None
        factory.return_value = True
        assert await asyncio.gather(*(cache.get_or_set('result', factory, cache_if=bool) for _ in range(4))) == [True] * 4
        assert factory.await_count == 2
    asyncio.run(run())


def test_dashboard_static_files_auth_and_api_route_precedence():
    state = overview_state()
    state.settings.ibkr_api_bearer_token = 'test-api-token'
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        for path in ('/dashboard', '/dashboard/', '/dashboard/index.html', '/dashboard/style.css', '/dashboard/app.js'):
            assert client.get(path).status_code == 200
            assert client.head(path).status_code == 200
        assert client.get('/api/v1/system/health').status_code == 401
        assert client.get('/api/v1/system/health', headers={'Authorization': 'Bearer bad'}).status_code == 401
        assert client.post('/dashboard/app.js').status_code == 401
        headers = {'Authorization': 'Bearer test-api-token', 'Accept': 'text/html'}
        for path in ('/dashboard/missing.js', '/dashboard/missing', '/api/v1/missing'):
            assert client.get(path, headers=headers).status_code == 404
        assert client.post('/dashboard/app.js', headers=headers).status_code in (404, 405)
        assert client.get('/api/v1/system/live', headers=headers).json()['status'] == 'alive'
        assert client.post('/api/v1/business/getEquityOverview', json={'symbol': 'SPY'}).status_code == 401
        schema = client.get('/api/v1/openapi.json', headers=headers).json()
        assert schema['servers'][0]['url'] == '/'
        for path in ('getMarketPanelDetailed', 'getUniverseBarsDetailed', 'getEquityOverview'):
            assert '/api/v1/business/' + path in schema['paths']


def test_overview_caller_cancellation_propagates_and_cancels_children():
    async def run():
        state = overview_state()
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def waiting(request):
            started.set()
            try:
                await asyncio.sleep(10)
            finally:
                cancelled.set()

        state.feed.load_equity_dividends = waiting
        task = asyncio.create_task(_load_equity_overview(EquityOverviewRequest(symbol='SPY'), state))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set()
    asyncio.run(run())


@pytest.mark.parametrize('phase', ['setup', 'wait', 'second_setup', 'send_failure'])
def test_snapshot_interrupted_setup_and_capture_release_all_resources(phase):
    async def run():
        feed = IBKRFeedClient()
        ib = IB()
        ib.client._apiReady = True
        ib.client.updateReqId(1)
        ib.client.cancelMktData = Mock()
        started = asyncio.Event()
        leases = []

        async def acquire(**kwargs):
            lease = SimpleNamespace(release=AsyncMock())
            leases.append(lease)
            return lease

        async def pace(*, operation, weight=1):
            if (phase == 'setup' and operation.endswith(':reqMktData')) or (phase == 'second_setup' and operation == 'equity_snapshot:QQQ:reqMktData'):
                started.set()
                await asyncio.sleep(10)

        def send(*args):
            if phase == 'wait':
                started.set()
            if phase == 'send_failure':
                raise RuntimeError('simulated send failure')

        ib.client.reqMktData = Mock(side_effect=send)
        feed._ib = ib
        feed._connection.acquire_market_data_line = acquire
        feed._connection.wait_for_ibkr_request = pace
        symbols = [('SPY', 'SMART', 'USD', '', 11)]
        if phase == 'second_setup':
            symbols.append(('QQQ', 'SMART', 'USD', '', 12))
        task = asyncio.create_task(feed.capture_equity_snapshots(symbols, snapshot_wait_seconds=10))
        if phase == 'send_failure':
            results = await asyncio.wait_for(task, 2)
            assert results[0].error == 'simulated send failure'
        else:
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
        assert not feed._reference._market_data_leases_by_ticker_id
        assert not ib.wrapper.tickers and not ib.wrapper.reqId2Ticker
        for lease in leases:
            lease.release.assert_awaited_once()
    asyncio.run(run())


def test_snapshot_cleanup_sends_cancellation_even_when_pacing_fails():
    async def run():
        feed = IBKRFeedClient()
        ib = IB()
        ib.client._apiReady = True
        ib.client.updateReqId(1)
        ib.client.reqMktData = Mock()
        ib.client.cancelMktData = Mock()
        feed._ib = ib
        lease = SimpleNamespace(release=AsyncMock())
        feed._connection.acquire_market_data_line = AsyncMock(return_value=lease)
        feed._connection.wait_for_ibkr_request = AsyncMock()
        results = await feed.capture_equity_snapshots([('SPY', 'SMART', 'USD', '', 11)], snapshot_wait_seconds=0)
        feed._connection.wait_for_ibkr_request.side_effect = RuntimeError('pacing failed')
        assert await feed.cancel_equity_tickers([results[0].ticker]) == 1
        ib.client.cancelMktData.assert_called_once()
        lease.release.assert_awaited_once()
        assert not ib.wrapper.tickers and not ib.wrapper.reqId2Ticker
    asyncio.run(run())
