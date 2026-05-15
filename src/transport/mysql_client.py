"""Async MySQL client for persistent relational storage.

Uses ``aiomysql`` for async database operations.  Designed as a companion
to ``QuestDBClient`` (time-series) and ``RedisClient`` (cache / pub-sub) —
MySQL handles relational / reference data such as instrument masters, trade
logs, portfolio snapshots, and strategy configuration.

Usage::

    client = MySQLClient(host="127.0.0.1", port=3306, user="root", password="pw", database="trading")
    async with client:
        rows = await client.fetch_all("SELECT * FROM instruments WHERE exchange = %s", ("SEHK",))
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import Any

from src.config import config_constant as constants

logger = logging.getLogger(__name__)


class MySQLClient:
    """Async MySQL client built on ``aiomysql``.

    Parameters
    ----------
    host, port, user, password, database:
        Standard MySQL connection parameters.
    pool_min_size, pool_max_size:
        Connection-pool sizing.
    autocommit:
        If ``True`` (default) every statement auto-commits.
    connection:
        Pre-built pool/engine for testing (skips pool creation on ``connect()``).
    """

    def __init__(
        self,
        *,
        host: str = constants.DEFAULT_MYSQL_HOST,
        port: int = constants.DEFAULT_MYSQL_PORT,
        user: str = constants.DEFAULT_MYSQL_USER,
        password: str = constants.DEFAULT_MYSQL_PASSWORD,
        database: str = constants.DEFAULT_MYSQL_DATABASE,
        pool_min_size: int = constants.DEFAULT_MYSQL_POOL_MIN_SIZE,
        pool_max_size: int = constants.DEFAULT_MYSQL_POOL_MAX_SIZE,
        autocommit: bool = True,
        pool: Any | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.autocommit = autocommit
        self._pool: Any = pool
        self._lock = asyncio.Lock()
        self._connected = pool is not None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Create the connection pool."""
        if self._connected:
            return
        try:
            import aiomysql
        except ImportError as exc:
            raise RuntimeError("aiomysql is required for MySQLClient. Install with: pip install aiomysql") from exc

        logger.info("MySQL connecting to %s:%d db=%s (pool %d-%d)", self.host, self.port, self.database, self.pool_min_size, self.pool_max_size)
        self._pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
            minsize=self.pool_min_size,
            maxsize=self.pool_max_size,
            autocommit=self.autocommit,
            charset="utf8mb4",
        )
        self._connected = True
        logger.info("MySQL connected")

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool is not None:
            logger.info("MySQL closing connection pool")
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            self._connected = False

    async def __aenter__(self) -> MySQLClient:
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _ensure_connection(self) -> None:
        """Verify the pool is alive; reconnect if stale."""
        if self._pool is None or not self._connected:
            await self.connect()
            return
        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
        except Exception:
            logger.warning("MySQL pool stale; reconnecting")
            try:
                self._pool.close()
                await self._pool.wait_closed()
            except Exception:
                pass
            self._pool = None
            self._connected = False
            await self.connect()

    def _acquire(self):
        """Acquire a connection from the pool (context manager)."""
        return self._pool.acquire()

    # ------------------------------------------------------------------
    # Public query interface
    # ------------------------------------------------------------------

    async def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        """Execute a single statement. Returns the rowcount."""
        await self._ensure_connection()
        async with self._lock:
            async with self._acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                if not self.autocommit:
                    await conn.commit()
                return cur.rowcount

    async def execute_many(self, sql: str, params_seq: Sequence[Sequence[Any]]) -> int:
        """Execute a statement with multiple parameter sets. Returns total rowcount."""
        if not params_seq:
            return 0
        await self._ensure_connection()
        async with self._lock:
            async with self._acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(sql, params_seq)
                if not self.autocommit:
                    await conn.commit()
                return cur.rowcount

    async def fetch_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Fetch a single row as a dict."""
        await self._ensure_connection()
        async with self._lock:
            async with self._acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                    row = await cur.fetchone()
                    if row is None:
                        return None
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, row, strict=True))

    async def fetch_all(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Fetch all rows as a list of dicts."""
        await self._ensure_connection()
        async with self._lock:
            async with self._acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                    columns = [desc[0] for desc in cur.description]
                    rows = await cur.fetchall()
        return [dict(zip(columns, row, strict=True)) for row in rows]

    async def fetch_value(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        """Fetch a single scalar value."""
        await self._ensure_connection()
        async with self._lock:
            async with self._acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                    row = await cur.fetchone()
        if row is None:
            return None
        return row[0]

    # ------------------------------------------------------------------
    # Table helpers
    # ------------------------------------------------------------------

    async def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the current database."""
        result = await self.fetch_value(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (self.database, table_name),
        )
        return bool(result)

    async def create_table(self, sql: str) -> None:
        """Execute a CREATE TABLE statement."""
        await self.execute(sql)
