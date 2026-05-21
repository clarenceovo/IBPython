from __future__ import annotations

import asyncio
import json
import logging
import time as monotonic_time
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
        redis: Any | None = None,
        buffer_max_age_seconds: float = 86400.0,
        buffer_drain_interval_seconds: float = 30.0,
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
        # Track which tables have been created to avoid repeated DDL on every insert.
        self._created_tables: set[str] = set()
        # Legacy single-connection attribute: when a pre-built connection is
        # injected (tests), we skip pool creation and use it directly.
        self._connection = connection
        self._connected = connection is not None
        # Write-behind buffer
        self._redis = redis
        self._buffer_max_age_seconds = buffer_max_age_seconds
        self._buffer_drain_interval_seconds = buffer_drain_interval_seconds
        self._drain_task: asyncio.Task[None] | None = None

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
        self._start_drain_task()

    async def close(self) -> None:
        self._stop_drain_task()
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
            logger.debug("QuestDB health_check failed", exc_info=True)
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

    async def _ensure_table(self, table_key: str, create_sql: str, *, alter_sqls: Sequence[str] | None = None) -> None:
        """Create a table if it hasn't been created yet in this session."""
        if table_key in self._created_tables:
            return
        async with self._ddl_lock:
            # Double-check under lock
            if table_key in self._created_tables:
                return
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(create_sql)
                        if alter_sqls:
                            for sql in alter_sqls:
                                try:
                                    await cur.execute(sql)
                                except Exception:
                                    logger.warning("QuestDB identity column migration skipped/already applied: %s", sql, exc_info=True)
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.execute(create_sql)
                    if alter_sqls:
                        for sql in alter_sqls:
                            try:
                                await cur.execute(sql)
                            except Exception:
                                logger.warning("QuestDB identity column migration skipped/already applied: %s", sql, exc_info=True)
                await self._connection.commit()
            self._created_tables.add(table_key)
            logger.debug("QuestDB table created: %s", table_key)

    async def create_market_ohlcv_table(self) -> None:
        await self._ensure_connection()
        await self._ensure_table("market_ohlcv", CREATE_MARKET_OHLCV_TABLE_SQL, alter_sqls=MARKET_OHLCV_IDENTITY_ALTER_SQL)

    async def create_equity_snapshot_table(self) -> None:
        await self._ensure_connection()
        await self._ensure_table("equity_snapshots", CREATE_EQUITY_SNAPSHOT_TABLE_SQL)

    async def create_fx_option_snapshot_table(self) -> None:
        await self._ensure_connection()
        await self._ensure_table("fx_option_snapshots", CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL)

    async def insert_snapshots(self, snapshots: Sequence[EquitySnapshot]) -> int:
        if not snapshots:
            return 0
        try:
            await self._ensure_connection()
            await self._ensure_table("equity_snapshots", CREATE_EQUITY_SNAPSHOT_TABLE_SQL)
            rows = [snapshot_to_row(s) for s in snapshots]
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(INSERT_EQUITY_SNAPSHOT_SQL, rows)
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.executemany(INSERT_EQUITY_SNAPSHOT_SQL, rows)
                await self._connection.commit()
            return len(snapshots)
        except Exception:
            logger.error("insert_snapshots failed, buffering %d rows", len(snapshots), exc_info=True)
            await self._buffer_failed_rows("equity_snapshots", rows=None, snapshots=snapshots)
            return 0

    async def insert_fx_option_snapshots(self, snapshots: Sequence[FXOptionSnapshot]) -> int:
        if not snapshots:
            return 0
        await self._ensure_connection()
        await self._ensure_table("fx_option_snapshots", CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL)
        rows = [fx_option_snapshot_to_row(s) for s in snapshots]
        if self._pool is not None:
            async with self._pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(INSERT_FX_OPTION_SNAPSHOT_SQL, rows)
                await conn.commit()
        else:
            async with self._connection.cursor() as cur:
                await cur.executemany(INSERT_FX_OPTION_SNAPSHOT_SQL, rows)
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
        try:
            await self._ensure_connection()
            await self._ensure_table("market_ohlcv", CREATE_MARKET_OHLCV_TABLE_SQL, alter_sqls=MARKET_OHLCV_IDENTITY_ALTER_SQL)
            rows = [bar_to_row(bar) for bar in bars]
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(INSERT_MARKET_OHLCV_SQL, rows)
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.executemany(INSERT_MARKET_OHLCV_SQL, rows)
                await self._connection.commit()
            return len(bars)
        except Exception:
            logger.error("insert_bars failed, buffering %d rows", len(bars), exc_info=True)
            await self._buffer_failed_rows("market_ohlcv", rows=None, bars=bars)
            return 0

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
        """Execute a SQL query and return rows as list[dict].

        .. warning::
            This method executes SQL as-is.  Callers **must** ensure that *sql*
            has been validated (e.g. SELECT-only) before passing it here.
            For user-supplied SQL, use the MCP ``query_raw_sql`` tool which
            runs validation before delegating to this method.
        """
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

    # ------------------------------------------------------------------
    # Write-behind buffer
    # ------------------------------------------------------------------

    def _start_drain_task(self) -> None:
        """Start the background buffer drain task if Redis is available."""
        if self._redis is None or self._drain_task is not None:
            return
        self._drain_task = asyncio.create_task(self._drain_buffer_loop())
        logger.info("QuestDB write-behind buffer drain task started")

    def _stop_drain_task(self) -> None:
        """Cancel the background drain task."""
        if self._drain_task is not None:
            self._drain_task.cancel()
            self._drain_task = None
            logger.debug("QuestDB write-behind buffer drain task stopped")

    async def _drain_buffer_loop(self) -> None:
        """Background task that periodically drains buffered rows and retries inserts."""
        try:
            while True:
                await asyncio.sleep(self._buffer_drain_interval_seconds)
                try:
                    await self._drain_all_buffers()
                except Exception:
                    logger.warning("QuestDB buffer drain cycle failed", exc_info=True)
        except asyncio.CancelledError:
            pass

    async def _drain_all_buffers(self) -> None:
        """Drain all write buffers (equity_snapshots, market_ohlcv, fx_option_snapshots)."""
        for table in ("equity_snapshots", "market_ohlcv", "fx_option_snapshots"):
            await self._drain_table_buffer(table)

    async def _drain_table_buffer(self, table: str) -> int:
        """Drain the buffer for a specific table, retrying inserts."""
        if self._redis is None:
            return 0
        try:
            key = f"questdb_write_buffer:{table}"
            raw_client = await self._redis.raw_client()
            drained = 0
            while True:
                raw_entry = await raw_client.lpop(key)
                if raw_entry is None:
                    break
                entry = json.loads(raw_entry if isinstance(raw_entry, str) else raw_entry.decode("utf-8"))
                # Check max age
                enqueued_at = entry.get("enqueued_at", 0)
                if monotonic_time.monotonic() - enqueued_at > self._buffer_max_age_seconds:
                    logger.debug("Discarding expired buffer entry for table=%s (age=%.0fs)", table, monotonic_time.monotonic() - enqueued_at)
                    continue
                # Retry the insert
                try:
                    await self._replay_buffer_entry(table, entry)
                    drained += 1
                except Exception:
                    # Re-insert at head for next drain cycle
                    await raw_client.lpush(key, raw_entry if isinstance(raw_entry, str) else raw_entry.decode("utf-8"))
                    logger.warning("QuestDB buffer retry failed for table=%s, will retry next cycle", table, exc_info=True)
                    break
            if drained > 0:
                logger.info("QuestDB buffer drained %d entries from table=%s", drained, table)
            return drained
        except Exception:
            logger.warning("QuestDB drain_table_buffer failed for table=%s", table, exc_info=True)
            return 0

    async def _replay_buffer_entry(self, table: str, entry: dict[str, Any]) -> None:
        """Replay a single buffered entry."""
        await self._ensure_connection()
        if table == "equity_snapshots":
            rows = entry.get("rows", [])
            if not rows:
                return
            await self._ensure_table("equity_snapshots", CREATE_EQUITY_SNAPSHOT_TABLE_SQL)
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(INSERT_EQUITY_SNAPSHOT_SQL, rows)
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.executemany(INSERT_EQUITY_SNAPSHOT_SQL, rows)
                await self._connection.commit()
        elif table == "market_ohlcv":
            rows = entry.get("rows", [])
            if not rows:
                return
            await self._ensure_table("market_ohlcv", CREATE_MARKET_OHLCV_TABLE_SQL, alter_sqls=MARKET_OHLCV_IDENTITY_ALTER_SQL)
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(INSERT_MARKET_OHLCV_SQL, rows)
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.executemany(INSERT_MARKET_OHLCV_SQL, rows)
                await self._connection.commit()
        elif table == "fx_option_snapshots":
            rows = entry.get("rows", [])
            if not rows:
                return
            await self._ensure_table("fx_option_snapshots", CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL)
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(INSERT_FX_OPTION_SNAPSHOT_SQL, rows)
                    await conn.commit()
            else:
                async with self._connection.cursor() as cur:
                    await cur.executemany(INSERT_FX_OPTION_SNAPSHOT_SQL, rows)
                await self._connection.commit()

    async def _buffer_failed_rows(
        self,
        table: str,
        *,
        rows: Sequence[Any] | None = None,
        snapshots: Sequence[EquitySnapshot] | None = None,
        bars: Sequence[OHLCVBar] | None = None,
    ) -> None:
        """Serialize failed rows to Redis list for later retry."""
        if self._redis is None:
            return
        try:
            # Convert to raw rows if not already done
            if rows is None:
                if snapshots is not None:
                    rows = [snapshot_to_row(s) for s in snapshots]
                elif bars is not None:
                    rows = [bar_to_row(bar) for bar in bars]
                else:
                    return

            # Serialize rows (convert tuples to lists for JSON)
            serialized_rows = [list(r) if isinstance(r, tuple) else r for r in rows]
            entry = {
                "table": table,
                "rows": serialized_rows,
                "enqueued_at": monotonic_time.monotonic(),
            }
            key = f"questdb_write_buffer:{table}"
            raw_client = await self._redis.raw_client()
            await raw_client.rpush(key, json.dumps(entry))
            depth = await raw_client.llen(key)
            logger.info("Buffered %d rows to Redis key=%s (depth=%d)", len(serialized_rows), key, depth)
        except Exception:
            logger.warning("_buffer_failed_rows: failed for table=%s", table, exc_info=True)

    async def get_write_buffer_depth(self) -> dict[str, int]:
        """Return the depth of each write buffer for monitoring.

        Returns a dict mapping table name to buffered entry count.
        """
        if self._redis is None:
            return {}
        try:
            raw_client = await self._redis.raw_client()
            depths: dict[str, int] = {}
            for table in ("equity_snapshots", "market_ohlcv", "fx_option_snapshots"):
                key = f"questdb_write_buffer:{table}"
                depths[table] = await raw_client.llen(key)
            return depths
        except Exception:
            logger.warning("get_write_buffer_depth: failed", exc_info=True)
            return {}
