"""Redis switch, shared clients, and socket lifecycle without a live broker."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from eventkit import Event
from fastapi.testclient import TestClient

from src.feeds.exceptions import IBKRConnectionDisabledError
from src.feeds.ibkr_connection import IBKRConnectionManager, IBKRConnectionSwitch
from src.feeds.ibkr_feed import IBKRFeedClient
from src.webapp.app import create_app
from src.webapp.dependencies import IBKRRestAppState, build_rest_app_state
from src.config.settings import Settings
from test_webapp import FakeState


class MemoryRedis:
    def __init__(self, raw=b'{"enabled":true}'):
        self.raw = raw
        self.error = None
        self.writes = []
        self.records = {}

    async def get_raw(self, key):
        if self.error:
            raise self.error
        return self.raw if key == "IBKR:ConnectionControl" else self.records.get(key)

    async def set_raw(self, key, value, *, ex=None):
        if self.error:
            raise self.error
        self.writes.append((key, value, ex))
        if key == "IBKR:ConnectionControl":
            self.raw = value
        else:
            self.records[key] = value

    async def connect(self):
        pass

    async def close(self):
        pass

    async def health_check(self):
        return self.error is None


@pytest.fixture
def sockets(monkeypatch):
    instances = []

    class Socket:
        def __init__(self):
            self.connected = False
            self.low_connected = False
            self.gate = None
            self.started = asyncio.Event()
            self.cancelled = False
            self.errorEvent, self.disconnectedEvent, self.timeoutEvent = Event(), Event(), Event()
            self.client = SimpleNamespace(
                isConnected=lambda: self.low_connected,
                disconnect=self.close_socket, reset=Mock(),
            )
            self.setTimeout = Mock()
            self.reqCurrentTimeAsync = AsyncMock(return_value='2026-10-08T00:00:00Z')
            instances.append(self)

        def isConnected(self):
            return self.connected

        async def connectAsync(self, *args, **kwargs):
            self.low_connected = True
            self.started.set()
            try:
                if self.gate is not None:
                    await self.gate.wait()
                self.connected = True
            except asyncio.CancelledError:
                self.cancelled = True
                raise

        def close_socket(self):
            self.low_connected = False

        def disconnect(self):
            self.connected = False
            self.close_socket()
            self.disconnectedEvent.emit()

    monkeypatch.setattr('ib_async.IB', Socket)
    return instances


async def until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(.001)


def manager(redis, client_id=101):
    return IBKRConnectionManager(redis=redis, client_id=client_id, connection_control_poll_seconds=.01)


def test_shared_redis_false_kills_clients_and_true_creates_fresh_sockets(sockets):
    async def run():
        redis = MemoryRedis()
        clients = [manager(redis, client_id) for client_id in (101, 201, 301)]
        try:
            await asyncio.gather(*(client.connect() for client in clients))
            old = [client.ib for client in clients]
            await clients[0].write_connection_control(IBKRConnectionSwitch(enabled=False))
            await until(lambda: all(not client.connection_enabled for client in clients))
            assert all(client.ib is None for client in clients)
            assert all(not socket.connected and not socket.low_connected for socket in old)
            for client in clients:
                for connect in (client.connect, client.ensure_connected):
                    with pytest.raises(IBKRConnectionDisabledError):
                        await connect()
                client._on_ibkr_disconnected()
                await client._reconnect()
                assert not client._background_tasks
            assert len(sockets) == 3
            assert ('IBKR:ConnectionControl', '{"enabled":false}', None) in redis.writes
            redis.raw = b'{"enabled":true}'
            await until(lambda: all(client.is_connected for client in clients))
            assert all(client.ib is not previous for client, previous in zip(clients, old, strict=True))
            assert len(sockets) == 6
        finally:
            await asyncio.gather(*(client.disconnect() for client in clients))
        redis.raw = '{"enabled":false}'
        await asyncio.sleep(.02)
        assert all(client._control_task is None and client.ib is None for client in clients)
    asyncio.run(run())


def test_false_at_startup_and_unreadable_redis_never_connect(sockets):
    async def run():
        redis = MemoryRedis(b'{"enabled":false}')
        client = manager(redis)
        try:
            await client.start_connection_control()
            for raw in (b'{"enabled":"false"}', b'{"enabled":0}', b'{', None):
                redis.raw = raw
                await client.refresh_connection_control()
                with pytest.raises(IBKRConnectionDisabledError):
                    await client.connect()
            redis.error = ConnectionError('Redis down')
            await client.refresh_connection_control()
            assert not client.connection_enabled and not sockets
            redis.error = None
            await client.write_connection_control(IBKRConnectionSwitch(enabled=True))
            await until(lambda: client.is_connected)
            assert len(sockets) == 1
        finally:
            await client.disconnect()
    asyncio.run(run())


def test_disable_during_connect_cancels_socket_setup_not_the_caller(sockets, monkeypatch):
    async def run():
        redis = MemoryRedis()
        client = manager(redis)
        import ib_async
        factory = ib_async.IB

        def blocked_socket():
            socket = factory()
            socket.gate = asyncio.Event()
            return socket

        monkeypatch.setattr(ib_async, 'IB', blocked_socket)
        try:
            pending = asyncio.create_task(client.connect())
            await until(lambda: bool(sockets) and sockets[0].started.is_set())
            await client.write_connection_control(IBKRConnectionSwitch(enabled=False))
            with pytest.raises(IBKRConnectionDisabledError):
                await asyncio.wait_for(pending, 2)
            assert sockets[0].cancelled and not sockets[0].low_connected
            assert client.ib is None
            monkeypatch.setattr(ib_async, 'IB', factory)
            await client.write_connection_control(IBKRConnectionSwitch(enabled=True))
            await until(lambda: client.is_connected)
            assert client.ib is sockets[1]
        finally:
            await client.disconnect()
    asyncio.run(run())


def test_concurrent_connects_share_one_socket(sockets):
    async def run():
        client = manager(MemoryRedis())
        try:
            await asyncio.gather(*(client.connect() for _ in range(8)))
            assert len(sockets) == 1 and client.is_connected
        finally:
            await client.disconnect()
    asyncio.run(run())


def test_explicit_true_starts_a_lazy_client_without_replacing_a_live_socket(sockets):
    async def run():
        client = manager(MemoryRedis())
        try:
            await client.start_connection_control()
            assert client.connection_enabled
            await client.write_connection_control(IBKRConnectionSwitch(enabled=True))
            await until(lambda: client.is_connected)
            original = client.ib
            await client.write_connection_control(IBKRConnectionSwitch(enabled=True))
            assert client.ib is original and len(sockets) == 1
        finally:
            await client.disconnect()
    asyncio.run(run())


def test_json_control_api_auth_validation_persistence_and_disabled_requests(sockets):
    state = FakeState()
    state.settings.redis_url = ''
    state.settings.ibkr_rest_connect_on_startup = False
    state.settings.ibkr_api_bearer_token = 'control-test-token'
    state.redis = MemoryRedis('{"enabled":false}')
    state.feed = IBKRFeedClient(
        redis=state.redis, rate_limiter=SimpleNamespace(wait_for_request=AsyncMock()),
        connection_control_poll_seconds=.01,
    )

    async def close():
        await state.feed.disconnect()

    state.close = close
    path = '/api/v1/system/ibkr/connection'
    auth = {'Authorization': 'Bearer control-test-token'}
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        assert client.put(path, json={'enabled': True}).status_code == 401
        assert client.get(path).status_code == 401
        assert client.get(path, headers=auth).json()['connection_status'] == 'disabled'
        assert client.get('/api/v1/system/live', headers=auth).status_code == 200
        assert client.get('/api/v1/system/server-time', headers=auth).status_code == 503
        for payload in ({}, {'enabled': 'false'}, {'enabled': 0}, {'enabled': True, 'unknown': True}):
            assert client.put(path, headers=auth, json=payload).status_code == 422
        assert client.put(path, headers=auth, json={'enabled': True}).json()['enabled'] is True
        for _ in range(100):
            if client.get(path, headers=auth).json()['connection_status'] == 'connected':
                break
        assert client.get('/api/v1/system/server-time', headers=auth).status_code == 200
        assert client.put(path, headers=auth, json={'enabled': False}).json()['connection_status'] == 'disabled'
        assert state.redis.raw == '{"enabled":false}'
        assert client.get('/api/v1/system/server-time', headers=auth).status_code == 503
        state.redis.error = ConnectionError('Redis down')
        assert client.put(path, headers=auth, json={'enabled': True}).status_code == 503
        assert client.get(path, headers=auth).status_code == 503
        assert not state.feed._connection.connection_enabled
    assert state.feed._connection._control_task is None


def test_disabled_rest_startup_keeps_control_api_available_and_stops_watcher(sockets):
    base = FakeState()
    base.settings.ibkr_rest_connect_on_startup = True
    base.settings.ibkr_api_bearer_token = ''
    base.settings.redis_url = ''
    redis = MemoryRedis('{"enabled":false}')
    feed = IBKRFeedClient(redis=redis, connection_control_poll_seconds=.01)
    state = IBKRRestAppState(
        settings=base.settings, redis=redis, feed=feed, loader=base.loader,
        market_data_cache=base.market_data_cache, event_contracts=base.event_contracts,
    )
    with TestClient(create_app(settings=state.settings, state=state)) as client:
        assert not sockets
        assert client.get('/api/v1/system/live').status_code == 200
        assert client.get('/api/v1/system/health').json()['ibkr_connection'] == 'disabled'
        assert client.get('/api/v1/system/readiness').status_code == 503
    assert feed._connection._control_task is None


def test_runtime_builders_use_the_same_redis_switch_for_api_and_mcp():
    settings = Settings(_env_file=None, ibkr_client_id=101, ibkr_mcp_client_id=301)
    api = build_rest_app_state(settings)
    mcp = build_rest_app_state(settings.model_copy(update={'ibkr_client_id': 301}))
    for state in (api, mcp):
        assert state.feed._connection._control_redis is state.redis
        assert state.feed._connection.connection_control_key == settings.ibkr_connection_control_redis_key


def test_mcp_checks_live_switch_state_after_startup(sockets):
    from src.mcp_server import _require_ibkr

    async def run():
        redis = MemoryRedis('{"enabled":false}')
        feed = IBKRFeedClient(
            redis=redis, connection_control_poll_seconds=.01,
            rate_limiter=SimpleNamespace(wait_for_request=AsyncMock()),
        )
        ctx = SimpleNamespace(request_context=SimpleNamespace(lifespan_context={
            'ibkr_state': SimpleNamespace(feed=feed), 'ibkr_connected': False,
        }))
        try:
            await feed.start_connection_control()
            assert 'disabled' in _require_ibkr(ctx)['error']
            for _ in range(5):
                await feed._circuit_breaker.record_failure()
            assert feed._circuit_breaker.is_open
            redis.raw = '{"enabled":true}'
            await until(lambda: feed._connection.is_connected)
            await until(lambda: not feed._circuit_breaker.is_open)
            await feed._ensure_connected()
            assert _require_ibkr(ctx) is None
            redis.raw = '{"enabled":false}'
            await until(lambda: not feed._connection.connection_enabled)
            assert 'disabled' in _require_ibkr(ctx)['error']
        finally:
            await feed.disconnect()
    asyncio.run(run())
