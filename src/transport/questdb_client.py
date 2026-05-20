from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from src.config import config_constant as constants
from src.feeds.models import AssetClass, OHLCVBar, ohlcv_contract_identity
from src.feeds.snapshotter import EquitySnapshot, FXOptionSnapshot
from src.transport.market_data_store import MarketOHLCVStore

logger = logging.getLogger(__name__)


CREATE_MARKET_OHLCV_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {constants.QUESTDB_MARKET_OHLCV_TABLE} (
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
    contract_key SYMBOL,
    con_id LONG,
    local_symbol SYMBOL,
    contract_month SYMBOL,
    expiry SYMBOL,
    strike DOUBLE,
    right SYMBOL,
    trading_class SYMBOL,
    what_to_show SYMBOL,
    use_rth BOOLEAN,
    metadata STRING
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
""".strip()


INSERT_MARKET_OHLCV_SQL = f"""
INSERT INTO {constants.QUESTDB_MARKET_OHLCV_TABLE}
(symbol, asset_class, exchange, currency, timestamp, open, high, low, close, volume, bar_size, source,
 contract_key, con_id, local_symbol, contract_month, expiry, strike, right, trading_class, what_to_show, use_rth, metadata)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""".strip()

MARKET_OHLCV_IDENTITY_ALTER_SQL = (
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN contract_key SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN con_id LONG",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN local_symbol SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN contract_month SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN expiry SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN strike DOUBLE",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN right SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN trading_class SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN what_to_show SYMBOL",
    f"ALTER TABLE {constants.QUESTDB_MARKET_OHLCV_TABLE} ADD COLUMN use_rth BOOLEAN",
)


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

CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {constants.FX_OPTION_SNAPSHOT_TABLE} (
    symbol SYMBOL,
    underlying_symbol SYMBOL,
    expiry SYMBOL,
    strike DOUBLE,
    right SYMBOL,
    exchange SYMBOL,
    currency SYMBOL,
    multiplier SYMBOL,
    trading_class SYMBOL,
    local_symbol SYMBOL,
    con_id LONG,
    timestamp TIMESTAMP,
    last DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    bid_size DOUBLE,
    ask_size DOUBLE,
    last_size DOUBLE,
    volume DOUBLE,
    mark_price DOUBLE,
    implied_volatility DOUBLE,
    historical_volatility DOUBLE,
    option_volume DOUBLE,
    average_option_volume DOUBLE,
    open_interest DOUBLE,
    call_open_interest DOUBLE,
    put_open_interest DOUBLE,
    call_volume DOUBLE,
    put_volume DOUBLE,
    bid_delta DOUBLE,
    ask_delta DOUBLE,
    last_delta DOUBLE,
    model_delta DOUBLE,
    model_gamma DOUBLE,
    model_theta DOUBLE,
    model_vega DOUBLE,
    mid_price DOUBLE,
    spread DOUBLE,
    spread_bps DOUBLE,
    source SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
""".strip()

INSERT_FX_OPTION_SNAPSHOT_SQL = f"""
INSERT INTO {constants.FX_OPTION_SNAPSHOT_TABLE}
(symbol, underlying_symbol, expiry, strike, right, exchange, currency,
 multiplier, trading_class, local_symbol, con_id, timestamp,
 last, bid, ask, bid_size, ask_size, last_size, volume, mark_price,
 implied_volatility, historical_volatility, option_volume, average_option_volume,
 open_interest, call_open_interest, put_open_interest, call_volume, put_volume,
 bid_delta, ask_delta, last_delta, model_delta, model_gamma, model_theta, model_vega,
 mid_price, spread, spread_bps, source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""".strip()


class QuestDBClient(MarketOHLCVStore):
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

    async def health_check(self) -> bool:
        """Return True if QuestDB is reachable, False otherwise."""
        try:
            if not self._connected or self._connection is None:
                return False
            async with self._connection.cursor() as cur:
                await cur.execute("SELECT 1")
                return True
        except Exception:
            return False

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
                for sql in MARKET_OHLCV_IDENTITY_ALTER_SQL:
                    try:
                        await cur.execute(sql)
                    except Exception:
                        logger.debug("QuestDB identity column migration skipped/already applied: %s", sql, exc_info=True)
            await self._connection.commit()

    async def create_equity_snapshot_table(self) -> None:
        await self._ensure_connection()
        async with self._lock:
            async with self._connection.cursor() as cur:
                await cur.execute(CREATE_EQUITY_SNAPSHOT_TABLE_SQL)
            await self._connection.commit()

    async def create_fx_option_snapshot_table(self) -> None:
        await self._ensure_connection()
        async with self._lock:
            async with self._connection.cursor() as cur:
                await cur.execute(CREATE_FX_OPTION_SNAPSHOT_TABLE_SQL)
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

    async def insert_fx_option_snapshots(self, snapshots: Sequence[FXOptionSnapshot]) -> int:
        if not snapshots:
            return 0
        await self._ensure_connection()
        async with self._lock:
            async with self._connection.cursor() as cur:
                await cur.executemany(INSERT_FX_OPTION_SNAPSHOT_SQL, [fx_option_snapshot_to_row(s) for s in snapshots])
            await self._connection.commit()
        return len(snapshots)

    async def query_fx_option_snapshots(
        self,
        *,
        symbol: str,
        expiry: str | None = None,
        strike: float | None = None,
        right: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        clauses = ["symbol = %s"]
        params: list[Any] = [symbol.replace("/", "").upper()]
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
        sql = (
            f"SELECT * FROM {constants.FX_OPTION_SNAPSHOT_TABLE} "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY timestamp DESC "
            "LIMIT %s"
        )
        return await self._fetch_dicts(sql, params)

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
        async with self._lock:
            async with self._connection.cursor() as cur:
                await cur.execute(sql, params)
                columns = [col.name for col in cur.description]
                rows = await cur.fetchall()
        return [dict(zip(columns, row, strict=True)) for row in rows]


def bar_to_row(bar: OHLCVBar) -> tuple[Any, ...]:
    identity = ohlcv_contract_identity(bar)
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
        identity["contract_key"],
        identity["con_id"],
        identity["local_symbol"],
        identity["contract_month"],
        identity["expiry"],
        identity["strike"],
        identity["right"],
        identity["trading_class"],
        identity["what_to_show"],
        identity["use_rth"],
        bar.metadata_json(),
    )


def build_historical_query(
    *,
    symbol: str,
    asset_class: AssetClass | str | None = None,
    bar_size: str | None = None,
    contract_key: str | None = None,
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
    if contract_key is not None:
        clauses.append("contract_key = %s")
        params.append(contract_key)
    if start is not None:
        clauses.append("timestamp >= %s")
        params.append(_questdb_timestamp(start))
    if end is not None:
        clauses.append("timestamp < %s")
        params.append(_questdb_timestamp(end))
    params.append(limit)
    sql = (
        f"SELECT * FROM {constants.QUESTDB_MARKET_OHLCV_TABLE} "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY timestamp ASC "
        "LIMIT %s"
    )
    return sql, params


def build_latest_query(
    *,
    asset_class: AssetClass | str | None = None,
    bar_size: str | None = None,
    contract_key: str | None = None,
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
    if contract_key is not None:
        clauses.append("contract_key = %s")
        params.append(contract_key)
    where = f"WHERE {' AND '.join(clauses)} " if clauses else ""
    params.append(limit)
    sql = (
        f"SELECT * FROM {constants.QUESTDB_MARKET_OHLCV_TABLE} "
        f"{where}"
        "LATEST ON timestamp PARTITION BY symbol, contract_key "
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


def fx_option_snapshot_to_row(snapshot: FXOptionSnapshot) -> tuple[Any, ...]:
    return (
        snapshot.symbol,
        snapshot.underlying_symbol,
        snapshot.expiry,
        snapshot.strike,
        snapshot.right,
        snapshot.exchange,
        snapshot.currency,
        snapshot.multiplier,
        snapshot.trading_class,
        snapshot.local_symbol,
        snapshot.con_id,
        snapshot.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
        snapshot.last,
        snapshot.bid,
        snapshot.ask,
        snapshot.bid_size,
        snapshot.ask_size,
        snapshot.last_size,
        snapshot.volume,
        snapshot.mark_price,
        snapshot.implied_volatility,
        snapshot.historical_volatility,
        snapshot.option_volume,
        snapshot.average_option_volume,
        snapshot.open_interest,
        snapshot.call_open_interest,
        snapshot.put_open_interest,
        snapshot.call_volume,
        snapshot.put_volume,
        snapshot.bid_greeks.delta if snapshot.bid_greeks else None,
        snapshot.ask_greeks.delta if snapshot.ask_greeks else None,
        snapshot.last_greeks.delta if snapshot.last_greeks else None,
        snapshot.model_greeks.delta if snapshot.model_greeks else None,
        snapshot.model_greeks.gamma if snapshot.model_greeks else None,
        snapshot.model_greeks.theta if snapshot.model_greeks else None,
        snapshot.model_greeks.vega if snapshot.model_greeks else None,
        snapshot.mid_price,
        snapshot.spread,
        snapshot.spread_bps,
        snapshot.source,
    )
