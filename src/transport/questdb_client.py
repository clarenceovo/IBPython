from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from src.config import config_constant as constants
from src.feeds.models import AssetClass, OHLCVBar
from src.feeds.snapshotter import EquitySnapshot

logger = logging.getLogger(__name__)


CREATE_MARKET_OHLCV_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {constants.MARKET_OHLCV_TABLE} (
    symbol SYMBOL,
    asset_class SYMBOL,
    exchange SYMBOL,
    currency SYMBOL,
    timestamp TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    bar_size SYMBOL,
    source SYMBOL,
    metadata STRING
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
""".strip()


INSERT_MARKET_OHLCV_SQL = f"""
INSERT INTO {constants.MARKET_OHLCV_TABLE}
(symbol, asset_class, exchange, currency, timestamp, open, high, low, close, volume, bar_size, source, metadata)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""".strip()


CREATE_EQUITY_SNAPSHOT_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {constants.EQUITY_SNAPSHOT_TABLE} (
    symbol SYMBOL,
    exchange SYMBOL,
    currency SYMBOL,
    primary_exchange SYMBOL,
    con_id LONG,
    timestamp TIMESTAMP,
    last DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    bid_size DOUBLE,
    ask_size DOUBLE,
    last_size DOUBLE,
    volume DOUBLE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    vwap DOUBLE,
    mark_price DOUBLE,
    mid_price DOUBLE,
    spread DOUBLE,
    spread_bps DOUBLE,
    halted BOOLEAN,
    source SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
""".strip()

INSERT_EQUITY_SNAPSHOT_SQL = f"""
INSERT INTO {constants.EQUITY_SNAPSHOT_TABLE}
(symbol, exchange, currency, primary_exchange, con_id, timestamp,
 last, bid, ask, bid_size, ask_size, last_size, volume,
 open, high, low, close, vwap, mark_price,
 mid_price, spread, spread_bps, halted, source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""".strip()


class QuestDBClient:
    """Async QuestDB client over the PostgreSQL wire protocol."""

    def __init__(
        self,
        *,
        host: str = constants.DEFAULT_QUESTDB_HOST,
        port: int = constants.DEFAULT_QUESTDB_PORT,
        user: str = constants.DEFAULT_QUESTDB_USER,
        password: str = constants.DEFAULT_QUESTDB_PASSWORD,
        database: str = constants.DEFAULT_QUESTDB_DATABASE,
        connection: Any | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._connection = connection
        self._lock = asyncio.Lock()
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
            from psycopg import AsyncConnection
        except ImportError as exc:
            raise RuntimeError("psycopg is required for QuestDBClient") from exc
        logger.info("QuestDB connecting to %s:%d db=%s", self.host, self.port, self.database)
        self._connection = await AsyncConnection.connect(self.dsn)
        self._connected = True
        logger.info("QuestDB connected")

    async def close(self) -> None:
        if self._connection is not None:
            logger.info("QuestDB closing connection")
            await self._connection.close()
            self._connection = None
            self._connected = False

    async def __aenter__(self) -> "QuestDBClient":
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def _ensure_connection(self) -> None:
        """Verify the connection is alive and reconnect if stale."""
        if self._connection is None or not self._connected:
            await self.connect()
            return
        try:
            async with self._connection.cursor() as cur:
                await cur.execute("SELECT 1")
        except Exception:
            logger.warning("QuestDB connection stale; reconnecting")
            try:
                await self._connection.close()
            except Exception:
                pass
            self._connection = None
            self._connected = False
            await self.connect()

    async def create_market_ohlcv_table(self) -> None:
        await self._ensure_connection()
        async with self._lock:
            async with self._connection.cursor() as cur:
                await cur.execute(CREATE_MARKET_OHLCV_TABLE_SQL)
            await self._connection.commit()

    async def create_equity_snapshot_table(self) -> None:
        await self._ensure_connection()
        async with self._lock:
            async with self._connection.cursor() as cur:
                await cur.execute(CREATE_EQUITY_SNAPSHOT_TABLE_SQL)
            await self._connection.commit()

    async def insert_snapshots(self, snapshots: Sequence[EquitySnapshot]) -> int:
        if not snapshots:
            return 0
        await self._ensure_connection()
        async with self._lock:
            async with self._connection.cursor() as cur:
                await cur.executemany(INSERT_EQUITY_SNAPSHOT_SQL, [snapshot_to_row(s) for s in snapshots])
            await self._connection.commit()
        return len(snapshots)

    async def query_snapshots(
        self,
        *,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        clauses = ["symbol = %s"]
        params: list[Any] = [symbol.upper()]
        if start is not None:
            clauses.append("timestamp >= %s")
            params.append(_questdb_timestamp(start))
        if end is not None:
            clauses.append("timestamp < %s")
            params.append(_questdb_timestamp(end))
        params.append(limit)
        sql = (
            f"SELECT * FROM {constants.EQUITY_SNAPSHOT_TABLE} "
            f"WHERE {' AND '.join(clauses)} "
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
        async with self._lock:
            async with self._connection.cursor() as cur:
                await cur.executemany(INSERT_MARKET_OHLCV_SQL, [bar_to_row(bar) for bar in bars])
            await self._connection.commit()
        return len(bars)

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
        sql, params = build_historical_query(
            symbol=symbol,
            asset_class=asset_class,
            bar_size=bar_size,
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
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        sql, params = build_latest_query(asset_class=asset_class, bar_size=bar_size, limit=limit)
        return await self._fetch_dicts(sql, params)

    async def _fetch_dicts(self, sql: str, params: Sequence[Any]) -> list[dict[str, Any]]:
        await self._ensure_connection()
        async with self._lock:
            async with self._connection.cursor() as cur:
                await cur.execute(sql, params)
                columns = [col.name for col in cur.description]
                rows = await cur.fetchall()
        return [dict(zip(columns, row, strict=True)) for row in rows]


def bar_to_row(bar: OHLCVBar) -> tuple[Any, ...]:
    return (
        bar.symbol,
        str(bar.asset_class),
        bar.exchange,
        bar.currency,
        bar.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.volume,
        bar.bar_size,
        bar.source,
        bar.metadata_json(),
    )


def build_historical_query(
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
        params.append(_questdb_timestamp(start))
    if end is not None:
        clauses.append("timestamp < %s")
        params.append(_questdb_timestamp(end))
    params.append(limit)
    sql = (
        f"SELECT * FROM {constants.MARKET_OHLCV_TABLE} "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY timestamp ASC "
        "LIMIT %s"
    )
    return sql, params


def build_latest_query(
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
    where = f"WHERE {' AND '.join(clauses)} " if clauses else ""
    params.append(limit)
    sql = (
        f"SELECT * FROM {constants.MARKET_OHLCV_TABLE} "
        f"{where}"
        "LATEST ON timestamp PARTITION BY symbol "
        "LIMIT %s"
    )
    return sql, params


def _questdb_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def snapshot_to_row(snapshot: EquitySnapshot) -> tuple[Any, ...]:
    return (
        snapshot.symbol,
        snapshot.exchange,
        snapshot.currency,
        snapshot.primary_exchange,
        snapshot.con_id,
        snapshot.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
        snapshot.last,
        snapshot.bid,
        snapshot.ask,
        snapshot.bid_size,
        snapshot.ask_size,
        snapshot.last_size,
        snapshot.volume,
        snapshot.open,
        snapshot.high,
        snapshot.low,
        snapshot.close,
        snapshot.vwap,
        snapshot.mark_price,
        snapshot.mid_price,
        snapshot.spread,
        snapshot.spread_bps,
        snapshot.halted,
        snapshot.source,
    )
