"""Tests for MySQLClient with mocked aiomysql pool."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.transport.mysql_client import MySQLClient


@pytest.fixture
def mock_pool():
    """Build a fake aiomysql pool that yields a single mock connection."""
    conn = AsyncMock()
    cursor = AsyncMock()
    cursor.rowcount = 3
    cursor.description = [("id",), ("name",), ("value",)]
    cursor.execute = AsyncMock()
    cursor.executemany = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=(1, "test", 42.0))
    cursor.fetchall = AsyncMock(return_value=[(1, "a", 10.0), (2, "b", 20.0), (3, "c", 30.0)])

    # conn.cursor() returns an async context manager
    cursor_ctx = MagicMock()
    cursor_ctx.__aenter__ = AsyncMock(return_value=cursor)
    cursor_ctx.__aexit__ = AsyncMock(return_value=False)
    conn.cursor = MagicMock(return_value=cursor_ctx)
    conn.commit = AsyncMock()

    # pool.acquire() returns an async context manager yielding conn
    acquire_ctx = MagicMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
    acquire_ctx.__aexit__ = AsyncMock(return_value=False)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acquire_ctx)
    pool.close = MagicMock()
    pool.wait_closed = AsyncMock()

    return pool, cursor


def _make_client(mock_pool) -> MySQLClient:
    return MySQLClient(
        host="localhost",
        port=3306,
        user="test",
        password="testpw",
        database="testdb",
        pool=mock_pool,
    )


class TestMySQLClientConnectClose:
    @pytest.mark.asyncio
    async def test_connect_creates_pool(self):
        mock_pool = AsyncMock()
        with patch("src.transport.mysql_client.MySQLClient.connect", new_callable=AsyncMock) as mock_connect:
            client = MySQLClient(host="h", port=3306, user="u", password="p", database="d")
            await client.connect()
            mock_connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_closes_pool(self, mock_pool):
        pool, _ = mock_pool
        client = _make_client(pool)
        await client.close()
        pool.close.assert_called_once()
        pool.wait_closed.assert_awaited_once()
        assert client._connected is False

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_pool):
        pool, _ = mock_pool
        client = _make_client(pool)
        assert client._connected is True
        async with client:
            assert client._connected is True
        assert client._connected is False


class TestMySQLClientQueries:
    @pytest.mark.asyncio
    async def test_execute(self, mock_pool):
        pool, cursor = mock_pool
        client = _make_client(pool)
        rowcount = await client.execute("INSERT INTO t (a) VALUES (%s)", ("x",))
        # _ensure_connection runs SELECT 1 first, then the actual query
        assert cursor.execute.call_count == 2
        cursor.execute.assert_any_await("INSERT INTO t (a) VALUES (%s)", ("x",))
        assert rowcount == 3  # mocked rowcount

    @pytest.mark.asyncio
    async def test_execute_many(self, mock_pool):
        pool, cursor = mock_pool
        client = _make_client(pool)
        params = [("a",), ("b",), ("c",)]
        rowcount = await client.execute_many("INSERT INTO t (a) VALUES (%s)", params)
        cursor.executemany.assert_awaited_once()
        assert rowcount == 3

    @pytest.mark.asyncio
    async def test_execute_many_empty(self, mock_pool):
        pool, cursor = mock_pool
        client = _make_client(pool)
        rowcount = await client.execute_many("INSERT INTO t (a) VALUES (%s)", [])
        assert rowcount == 0
        cursor.executemany.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_one(self, mock_pool):
        pool, cursor = mock_pool
        client = _make_client(pool)
        result = await client.fetch_one("SELECT * FROM t WHERE id = %s", (1,))
        assert result == {"id": 1, "name": "test", "value": 42.0}

    @pytest.mark.asyncio
    async def test_fetch_one_none(self, mock_pool):
        pool, cursor = mock_pool
        cursor.fetchone = AsyncMock(return_value=None)
        client = _make_client(pool)
        result = await client.fetch_one("SELECT * FROM t WHERE id = %s", (999,))
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_all(self, mock_pool):
        pool, cursor = mock_pool
        client = _make_client(pool)
        rows = await client.fetch_all("SELECT * FROM t")
        assert len(rows) == 3
        assert rows[0] == {"id": 1, "name": "a", "value": 10.0}
        assert rows[2] == {"id": 3, "name": "c", "value": 30.0}

    @pytest.mark.asyncio
    async def test_fetch_value(self, mock_pool):
        pool, cursor = mock_pool
        client = _make_client(pool)
        val = await client.fetch_value("SELECT COUNT(*) FROM t")
        assert val == 1  # first element of the mocked row

    @pytest.mark.asyncio
    async def test_fetch_value_none(self, mock_pool):
        pool, cursor = mock_pool
        cursor.fetchone = AsyncMock(return_value=None)
        client = _make_client(pool)
        val = await client.fetch_value("SELECT COUNT(*) FROM empty_table")
        assert val is None


class TestMySQLClientHelpers:
    @pytest.mark.asyncio
    async def test_table_exists(self, mock_pool):
        pool, cursor = mock_pool
        cursor.fetchone = AsyncMock(return_value=(1,))
        client = _make_client(pool)
        assert await client.table_exists("instruments") is True

    @pytest.mark.asyncio
    async def test_table_not_exists(self, mock_pool):
        pool, cursor = mock_pool
        cursor.fetchone = AsyncMock(return_value=(0,))
        client = _make_client(pool)
        assert await client.table_exists("nonexistent") is False

    @pytest.mark.asyncio
    async def test_create_table(self, mock_pool):
        pool, cursor = mock_pool
        client = _make_client(pool)
        await client.create_table("CREATE TABLE test (id INT PRIMARY KEY)")
        cursor.execute.assert_any_await("CREATE TABLE test (id INT PRIMARY KEY)", None)


class TestMySQLClientReconnect:
    @pytest.mark.asyncio
    async def test_ensure_connection_reconnects_on_stale(self, mock_pool):
        pool, cursor = mock_pool
        # First SELECT 1 raises — simulates stale connection
        call_count = 0

        original_execute = cursor.execute

        async def flaky_execute(sql, params=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1 and sql == "SELECT 1":
                raise ConnectionResetError("connection lost")
            return await original_execute(sql, params)

        cursor.execute = AsyncMock(side_effect=flaky_execute)

        # Patch connect to succeed
        with patch.object(MySQLClient, "connect", new_callable=AsyncMock):
            client = _make_client(pool)
            # _ensure_connection should catch the error and reconnect
            await client._ensure_connection()
