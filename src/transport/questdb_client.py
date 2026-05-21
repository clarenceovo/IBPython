from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from src.config import config_constant as constants
from src.feeds.models import AssetClass, OHLCVBar
from src.feeds.snapshotter import EquitySnapshot, FXOptionSnapshot
from src.transport.market_data_store import MarketOHLCVStore
from src.transport.questdb_queries import (
    CREATE_EQUITY_SNAPSHOT_TABLE_SQL,
    CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL,
    CREATE_MARKET_OHLCV_TABLE_SQL,
    INSERT_EQUITY_SNAPSHOT_SQL,
    INSERT_FX_OPTION_SNAPSHOT_SQL,
    INSERT_MARKET_OHLCV_SQL,
    MARKET_OHLCV_IDENTITY_ALTER_SQL,
    bar_to_row,
    build_historical_query,
    build_latest_query,
    fx_option_snapshot_to_row,
    snapshot_to_row,
    _questdb_timestamp,
)

logger = logging.getLogger(__name__)


class QuestDBClient(MarketOHLCVStore):
    """Async QuestDB client over the PostgreSQL wire protocol.

    Uses a connection pool (``psycopg_pool.AsyncConnectionPool``) for queries
    and retains a serialising lock for DDL / table-creation operations.
    """

    def __init__(
        self,
        *,
        host: str = constants.DEFAULT_QUESTDB_HOST,
        port: int = constants.DEFAULT_QUESTDB_PORT,
        user: str = constants.DEFAULT_QUESTDB_USER,
        password: str = constants.DEFAULT_QUESTDB_PASSWORD,
        database: str = constants.DEFAULT_QUESTDB_DATABASE,
        connection: Any | None = None,
        pool_size: int = 4,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool_size = pool_size
        self._pool: Any | None = None
        # DDL operations (table creation) must still serialize.
        self._ddl_lock = asyncio.Lock()
        # Legacy single-connection attribute: when a pre-built connection is
        # injected (tests), we skip pool creation and use it directly.
        self._connection = connection
        self._connected = connection is not None

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} "
            f"port={self.port} "
            f"user={self.user} "
            f"password={self.password} "
            f"dbname={self.database}"
        )

    async def connect(self) -> None:
        if self._connected:
            return
        try:
            from psycopg_pool import AsyncConnectionPool
        except ImportError as exc:
            raise RuntimeError(
                "psycopg-pool is required for QuestDBClient. "
                "Install with: pip install psycopg-pool"
            ) from exc
        logger.info(
            "QuestDB creating connection pool to %s:%d db=%s pool_size=%d",
            self.host, self.port, self.database, self.pool_size,
        )
        self._pool = AsyncConnectionPool(
            conninfo=self.dsn,
            min_size=1,
            max_size=self.pool_size,
            open=False,
        )
        await self._pool.open()
        self._connected = True
        logger.info("QuestDB connection pool ready")

    async def close(self) -> None:
        if self._pool is not None:
            logger.info("QuestDB closing connection pool")
            await self._pool.close()
            self._pool = None
            self._connected = False
        elif self._connection is not None:
            logger.info("QuestDB closing legacy connection")
            await self._connection.close()
            self._connection = None
            self._connected = False

    async def health_check(self) -> bool:
        """Return True if QuestDB is reachable, False otherwise."""
        try:
            if not self._connected:
                return False
            # Legacy path (pre-injected connection in tests)
            if self._connection is not None and self._pool is None:
                async with self._connection.cursor() as cur:
                    await cur.execute("SELECT 1")
                return True
            # Pool path
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1")
                return True
            return False
        except Exception:
            return False

    async def __aenter__(self) -> "QuestDBClient":
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def _ensure_connection(self) -> None:
        """Ensure the pool is open.  No-op for legacy pre-injected connections."""
        if self._connection is not None and self._pool is None:
            return  # Legacy path: connection is managed externally
        if not self._connected or self._pool is None:
            await self.connect()

    async def create_market_ohlcv_table(self) -> None:
        await self._ensure_connection()
        async with self._ddl_lock:
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(CREATE_MARKET_OHLCV_TABLE_SQL)
                        for sql in MARKET_OHLCV_IDENTITY_ALTER_SQL:
                            try:
                                await cur.execute(sql)
                            except Exception:
                                logger.debug("QuestDB identity column migration skipped/already applied: %s", sql, exc_info=True)
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.execute(CREATE_MARKET_OHLCV_TABLE_SQL)
                    for sql in MARKET_OHLCV_IDENTITY_ALTER_SQL:
                        try:
                            await cur.execute(sql)
                        except Exception:
                            logger.debug("QuestDB identity column migration skipped/already applied: %s", sql, exc_info=True)
                await self._connection.commit()

    async def create_equity_snapshot_table(self) -> None:
        await self._ensure_connection()
        async with self._ddl_lock:
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(CREATE_EQUITY_SNAPSHOT_TABLE_SQL)
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.execute(CREATE_EQUITY_SNAPSHOT_TABLE_SQL)
                await self._connection.commit()

    async def create_fx_option_snapshot_table(self) -> None:
        await self._ensure_connection()
        async with self._ddl_lock:
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL)
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.execute(CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL)
                await self._connection.commit()

    async def insert_snapshots(self, snapshots: Sequence[EquitySnapshot]) -> int:
        if not snapshots:
            return 0
        await self._ensure_connection()
        async with self._ddl_lock:
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(CREATE_EQUITY_SNAPSHOT_TABLE_SQL)
                        await cur.executemany(INSERT_EQUITY_SNAPSHOT_SQL, [snapshot_to_row(s) for s in snapshots])
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.execute(CREATE_EQUITY_SNAPSHOT_TABLE_SQL)
                    await cur.executemany(INSERT_EQUITY_SNAPSHOT_SQL, [snapshot_to_row(s) for s in snapshots])
                await self._connection.commit()
        return len(snapshots)

    async def insert_fx_option_snapshots(self, snapshots: Sequence[FXOptionSnapshot]) -> int:
        if not snapshots:
            return 0
        await self._ensure_connection()
        async with self._ddl_lock:
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL)
                        await cur.executemany(INSERT_FX_OPTION_SNAPSHOT_SQL, [fx_option_snapshot_to_row(s) for s in snapshots])
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.execute(CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL)
                    await cur.executemany(INSERT_FX_OPTION_SNAPSHOT_SQL, [fx_option_snapshot_to_row(s) for s in snapshots])
                await self._connection.commit()
        return len(snapshots)

    async def query_fx_option_snapshots(
        self,
        *,
        symbol: str | None = None,
        expiry: str | None = None,
        strike: float | None = None,
        right: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if symbol is not None:
            clauses.append("symbol = %s")
            params.append(symbol.replace("/", "").upper())
        if expiry is not None:
            clauses.append("expiry = %s")
            params.append(expiry.upper())
        if strike is not None:
            clauses.append("strike = %s")
            params.append(strike)
        if right is not None:
            clauses.append("right = %s")
            normalized_right = right.upper()
            params.append("C" if normalized_right == "CALL" else "P" if normalized_right == "PUT" else normalized_right)
        if start is not None:
            clauses.append("timestamp >= %s")
            params.append(_questdb_timestamp(start))
        if end is not None:
            clauses.append("timestamp < %s")
            params.append(_questdb_timestamp(end))
        params.append(limit)
        where_clause = f"WHERE {' AND '.join(clauses)} " if clauses else ""
        sql = (
            f"SELECT * FROM {constants.FX_OPTION_SNAPSHOT_TABLE} "
            f"{where_clause}"
            "ORDER BY timestamp DESC "
            "LIMIT %s"
        )
        return await self._fetch_dicts(sql, params)

    async def query_snapshots(
        self,
        *,
        symbol: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if symbol is not None:
            clauses.append("symbol = %s")
            params.append(symbol.upper())
        if start is not None:
            clauses.append("timestamp >= %s")
            params.append(_questdb_timestamp(start))
        if end is not None:
            clauses.append("timestamp < %s")
            params.append(_questdb_timestamp(end))
        params.append(limit)
        where_clause = f"WHERE {' AND '.join(clauses)} " if clauses else ""
        sql = (
            f"SELECT * FROM {constants.EQUITY_SNAPSHOT_TABLE} "
            f"{where_clause}"
            "ORDER BY timestamp DESC "
            "LIMIT %s"
        )
        return await self._fetch_dicts(sql, params)

    async def query_latest_snapshots(
        self,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        params: list[Any] = [limit]
        sql = (
            f"SELECT * FROM {constants.EQUITY_SNAPSHOT_TABLE} "
            "LATEST ON timestamp PARTITION BY symbol "
            "LIMIT %s"
        )
        return await self._fetch_dicts(sql, params)

    async def insert_bars(self, bars: Sequence[OHLCVBar]) -> int:
        if not bars:
            return 0
        await self._ensure_connection()
        async with self._ddl_lock:
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(CREATE_MARKET_OHLCV_TABLE_SQL)
                        await cur.executemany(INSERT_MARKET_OHLCV_SQL, [bar_to_row(bar) for bar in bars])
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.execute(CREATE_MARKET_OHLCV_TABLE_SQL)
                    await cur.executemany(INSERT_MARKET_OHLCV_SQL, [bar_to_row(bar) for bar in bars])
                await self._connection.commit()
        return len(bars)

    async def query_historical_bars(
        self,
        *,
        symbol: str,
        asset_class: AssetClass | str | None = None,
        bar_size: str | None = None,
        contract_key: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 10_000,
    ) -> list[dict[str, Any]]:
        sql, params = build_historical_query(
            symbol=symbol,
            asset_class=asset_class,
            bar_size=bar_size,
            contract_key=contract_key,
            start=start,
            end=end,
            limit=limit,
        )
        return await self._fetch_dicts(sql, params)

    async def query_latest_bars(
        self,
        *,
        asset_class: AssetClass | str | None = None,
        bar_size: str | None = None,
        contract_key: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        sql, params = build_latest_query(asset_class=asset_class, bar_size=bar_size, contract_key=contract_key, limit=limit)
        return await self._fetch_dicts(sql, params)

    async def _fetch_dicts(self, sql: str, params: Sequence[Any]) -> list[dict[str, Any]]:
        await self._ensure_connection()
        if self._pool is not None:
            async with self._pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                    columns = [col.name for col in cur.description]
                    rows = await cur.fetchall()
                return [dict(zip(columns, row, strict=True)) for row in rows]
        # Legacy path (pre-injected connection in tests)
        async with self._ddl_lock:
            async with self._connection.cursor() as cur:
                await cur.execute(sql, params)
                columns = [col.name for col in cur.description]
                rows = await cur.fetchall()
        return [dict(zip(columns, row, strict=True)) for row in rows]
