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
from datetime import datetime, timezone
from typing import Any

from src.config import config_constant as constants
from src.feeds.models import AssetClass, OHLCVBar
from src.transport.market_data_store import MarketOHLCVStore

logger = logging.getLogger(__name__)


CREATE_MYSQL_MARKET_OHLCV_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {constants.MARKET_OHLCV_TABLE} (
    symbol VARCHAR(64) NOT NULL,
    asset_class VARCHAR(32) NOT NULL,
    exchange VARCHAR(64) NOT NULL,
    currency VARCHAR(16) NOT NULL,
    timestamp DATETIME(6) NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    bar_size VARCHAR(32) NOT NULL,
    source VARCHAR(64) NOT NULL,
    metadata JSON NULL,
    PRIMARY KEY (symbol, asset_class, bar_size, timestamp),
    KEY idx_market_ohlcv_latest (asset_class, bar_size, symbol, timestamp),
    KEY idx_market_ohlcv_symbol_time (symbol, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""".strip()


INSERT_MYSQL_MARKET_OHLCV_SQL = f"""
INSERT INTO {constants.MARKET_OHLCV_TABLE}
(symbol, asset_class, exchange, currency, timestamp, open, high, low, close, volume, bar_size, source, metadata)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    exchange = VALUES(exchange),
    currency = VALUES(currency),
    open = VALUES(open),
    high = VALUES(high),
    low = VALUES(low),
    close = VALUES(close),
    volume = VALUES(volume),
    source = VALUES(source),
    metadata = VALUES(metadata)
""".strip()


class MySQLClient(MarketOHLCVStore):
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

    # ------------------------------------------------------------------
    # Market OHLCV store interface
    # ------------------------------------------------------------------

    async def create_market_ohlcv_table(self) -> None:
        await self.create_table(CREATE_MYSQL_MARKET_OHLCV_TABLE_SQL)

    async def insert_bars(self, bars: Sequence[OHLCVBar]) -> int:
        return await self.execute_many(INSERT_MYSQL_MARKET_OHLCV_SQL, [mysql_bar_to_row(bar) for bar in bars])

    async def query_historical_bars(
        self,
        *,
        symbol: str,
        asset_class: AssetClass | str | None = None,
        bar_size: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 10_000,
    ) -> list[dict[str, Any]]:
        sql, params = build_mysql_historical_query(
            symbol=symbol,
            asset_class=asset_class,
            bar_size=bar_size,
            start=start,
            end=end,
            limit=limit,
        )
        return await self.fetch_all(sql, params)

    async def query_latest_bars(
        self,
        *,
        asset_class: AssetClass | str | None = None,
        bar_size: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        sql, params = build_mysql_latest_query(asset_class=asset_class, bar_size=bar_size, limit=limit)
        return await self.fetch_all(sql, params)


def mysql_bar_to_row(bar: OHLCVBar) -> tuple[Any, ...]:
    return (
        bar.symbol,
        str(bar.asset_class),
        bar.exchange,
        bar.currency,
        _mysql_timestamp(bar.timestamp),
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.volume,
        bar.bar_size,
        bar.source,
        bar.metadata_json(),
    )


def build_mysql_historical_query(
    *,
    symbol: str,
    asset_class: AssetClass | str | None = None,
    bar_size: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 10_000,
) -> tuple[str, list[Any]]:
    clauses = ["symbol = %s"]
    params: list[Any] = [symbol.upper()]
    if asset_class is not None:
        clauses.append("asset_class = %s")
        params.append(str(asset_class))
    if bar_size is not None:
        clauses.append("bar_size = %s")
        params.append(bar_size)
    if start is not None:
        clauses.append("timestamp >= %s")
        params.append(_mysql_timestamp(start))
    if end is not None:
        clauses.append("timestamp < %s")
        params.append(_mysql_timestamp(end))
    params.append(limit)
    sql = (
        f"SELECT * FROM {constants.MARKET_OHLCV_TABLE} "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY timestamp ASC "
        "LIMIT %s"
    )
    return sql, params


def build_mysql_latest_query(
    *,
    asset_class: AssetClass | str | None = None,
    bar_size: str | None = None,
    limit: int = 100,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if asset_class is not None:
        clauses.append("asset_class = %s")
        params.append(str(asset_class))
    if bar_size is not None:
        clauses.append("bar_size = %s")
        params.append(bar_size)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    sql = (
        "SELECT symbol, asset_class, exchange, currency, timestamp, open, high, low, close, volume, bar_size, source, metadata "
        "FROM ("
        "SELECT symbol, asset_class, exchange, currency, timestamp, open, high, low, close, volume, bar_size, source, metadata, "
        f"ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn FROM {constants.MARKET_OHLCV_TABLE} {where}"
        ") ranked WHERE rn = 1 ORDER BY timestamp DESC LIMIT %s"
    )
    return sql, params


def _mysql_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(tzinfo=None)
