"""REST authentication, error contracts and MCP parity for gateway additions."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fastapi.testclient import TestClient

from src.config.settings import Settings
from src.feeds.capabilities import gateway_capabilities
from src.feeds.equity_reference import DividendsResponse, ShortabilityResponse
from src.feeds.ibkr_feed import IBKRFeedClient
from src.feeds.orders import OpenOrder
from src.webapp.app import create_app


def make_app():
    settings = Settings(ibkr_api_bearer_token="", ibkr_rest_connect_on_startup=False)
    real_feed = IBKRFeedClient()
    shortability = ShortabilityResponse(symbol="AAPL", exchange="SMART", currency="USD", status="available", shortable_shares=0)
    dividends = DividendsResponse(symbol="AAPL", exchange="SMART", currency="USD", timed_out=True)
    orders = [OpenOrder(order_id=i, symbol="AAPL", sec_type="STK", action="BUY", order_type="LMT",
                        quantity=1, status="Submitted") for i in range(3)]
    feed = SimpleNamespace(
        get_capabilities=real_feed.get_capabilities,
        load_equity_shortability=AsyncMock(return_value=shortability),
        load_equity_dividends=AsyncMock(return_value=dividends),
        get_all_open_orders=AsyncMock(return_value=orders),
        load_fundamental_data=real_feed.load_fundamental_data,
    )
    state = SimpleNamespace(settings=settings, feed=feed, close=AsyncMock(),
                            redis=SimpleNamespace(get_raw=AsyncMock(return_value=b'order-token')))
    return create_app(settings=settings, state=state), feed


def test_rest_capabilities_and_removed_fundamentals():
    app, _ = make_app()
    with TestClient(app) as client:
        response = client.get('/api/v1/system/capabilities')
        assert response.status_code == 200
        assert response.json()['negotiated_server_protocol'] is None
        assert response.json()['features']['fundamental_reports']['availability'] == 'unsupported'
        response = client.post('/api/v1/reference-data/fundamentals', json={'symbol': 'AAPL'})
        assert response.status_code == 410
        assert '10.47' in response.json()['detail']
        schema = client.get('/api/v1/openapi.json').json()
        assert schema['paths']['/api/v1/reference-data/fundamentals']['post']['deprecated'] is True


def test_rest_reference_responses_and_validation():
    app, feed = make_app()
    with TestClient(app) as client:
        response = client.post('/api/v1/market-data/equity/shortability', json={'symbol': ' aapl '})
        assert response.status_code == 200
        assert response.json()['shortable_shares'] == 0
        assert feed.load_equity_shortability.call_args.args[0].symbol == 'AAPL'
        response = client.post('/api/v1/market-data/equity/dividends', json={'symbol': 'AAPL'})
        assert response.status_code == 200
        assert response.json()['next_dividend_amount'] is None
        assert response.json()['timed_out'] is True
        response = client.post('/api/v1/market-data/equity/dividends', json={'symbol': 'AAPL', 'timeout_seconds': 100})
        assert response.status_code == 422
        assert feed.load_equity_dividends.await_count == 1


def test_rest_all_open_orders_requires_existing_auth_and_paginates():
    app, feed = make_app()
    with TestClient(app) as client:
        assert client.get('/api/v1/orders/open/all').status_code == 401
        assert client.get('/api/v1/orders/open/all', headers={'Authorization': 'Bearer bad'}).status_code == 401
        feed.get_all_open_orders.assert_not_called()
        response = client.get('/api/v1/orders/open/all?offset=1&limit=1', headers={'Authorization': 'Bearer order-token'})
        assert response.status_code == 200
        assert [item['order_id'] for item in response.json()] == [1]
        feed.get_all_open_orders.assert_awaited_once()


def test_mcp_uses_same_models_and_propagates_errors(monkeypatch):
    import src.mcp_server as server
    from src.feeds.exceptions import IBKRUnsupportedFeatureError
    import pytest

    async def run():
        _, feed = make_app()
        monkeypatch.setattr(server, '_state', lambda _: SimpleNamespace(feed=feed))
        short = await server.load_equity_shortability(None, 'aapl')
        assert short['shortable_shares'] == 0
        request = feed.load_equity_shortability.call_args.args[0]
        assert request.symbol == 'AAPL'
        dividend = await server.load_equity_dividends(None, 'AAPL', exchange='NYSE')
        assert dividend['timed_out'] is True
        assert feed.load_equity_dividends.call_args.args[0].exchange == 'NYSE'
        assert (await server.get_gateway_capabilities(None))['connected'] is False
        with pytest.raises(IBKRUnsupportedFeatureError, match='10.47'):
            await server.load_fundamentals(None, 'AAPL')
    asyncio.run(run())
