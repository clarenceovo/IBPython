"""Storage MCP tools: QuestDB historical/snapshot/SQL queries and Redis cache/watchlist reads."""

from __future__ import annotations

import re
import time as _time
from datetime import datetime
from typing import Any

from mcp.server.fastmcp import Context, FastMCP

from src.mcp_app.common import (
    MCP_DEFAULT_BAR_LIMIT,
    MCP_DEFAULT_SQL_ROWS,
    MCP_DEFAULT_TEXT_CHARS,
    MCP_MAX_BAR_LIMIT,
    MCP_MAX_SQL_ROWS,
    _AssetClassParam,
    _BarSizeParam,
    _READ_TOOL,
    _RightOptParam,
    _RightParam,
    _SENSITIVE_KEY_PATTERNS,
    _clamp,
    _error,
    _parse_asset_class,
    _require_questdb,
    _require_redis,
    _state,
    _tool_error,
    _truncate_items,
    _truncate_text,
)


async def query_historical_ohlcv(
    ctx: Context,
    symbol: str,
    asset_class: _AssetClassParam = None,
    bar_size: _BarSizeParam = None,
    contract_key: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = MCP_DEFAULT_BAR_LIMIT,
) -> dict[str, Any]:
    """Query historical OHLCV bars from QuestDB with bounded output.

    Fetches up to ``limit + 1`` rows to detect truncation without running a
    separate ``COUNT(*)`` scan, reporting ``returned_count``, ``total_count``
    (exact when untruncated), and ``truncated``.

    Args:
        symbol: Ticker symbol (e.g. 'BTCUSDT', 'ES', 'EUR.USD')
        asset_class: Filter by asset class (CRYPTO, FUTURES, INDEX, FX, EQUITY, BOND, OPTION)
        bar_size: Bar size (e.g. '1 min', '5 mins', '1 hour', '1 day')
        contract_key: Specific contract key for futures/options
        start: Start datetime in ISO format (e.g. '2026-05-01T00:00:00Z')
        end: End datetime in ISO format
        limit: Max rows to return (default 500, max 5000)
    """
    if err := _require_questdb(ctx):
        return err
    state = _state(ctx)
    from src.feeds.models import (
        OHLCVBar,
        OHLCVRequestMeta,
        OHLCVResponseEnvelope,
        compute_ohlcv_quality,
    )

    try:
        limit = _clamp(limit, low=1, high=MCP_MAX_BAR_LIMIT)
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None

        t0 = _time.monotonic()
        rows = await state.loader.questdb.query_historical_bars(
            symbol=symbol,
            asset_class=asset_class,
            bar_size=bar_size,
            contract_key=contract_key,
            start=start_dt,
            end=end_dt,
            limit=limit + 1,
        )
        latency_ms = (_time.monotonic() - t0) * 1000.0

        truncated = len(rows) > limit
        kept_rows = rows[:limit]
        bars = [OHLCVBar.model_validate(r) if isinstance(r, dict) else r for r in kept_rows]
        request_meta = OHLCVRequestMeta(
            symbol=symbol,
            asset_class=asset_class or "unknown",
            bar_size=bar_size or "unknown",
            what_to_show="unknown",
        )
        envelope = OHLCVResponseEnvelope(
            bars=bars,
            request=request_meta,
            quality=compute_ohlcv_quality(bars),
            latency_ms=latency_ms,
            cache_hit=False,
            chunk_count=1,
            source="questdb",
        )
        payload = envelope.model_dump(mode="json")
        payload["returned_count"] = len(bars)
        payload["total_count"] = len(rows)
        payload["truncated"] = truncated
        return payload
    except Exception as e:
        return _tool_error(e)


async def query_latest_bars(
    ctx: Context,
    asset_class: _AssetClassParam = None,
    bar_size: _BarSizeParam = None,
    contract_key: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Query latest OHLCV bars across all symbols from QuestDB.

    Uses QuestDB LATEST ON to get the most recent bar per symbol+contract_key.

    Args:
        asset_class: Filter by asset class
        bar_size: Filter by bar size
        contract_key: Filter by contract key
        limit: Max rows (default 100, max 1000)
    """
    state = _state(ctx)
    if err := _require_questdb(ctx):
        return err
    from src.feeds.models import (
        OHLCVBar,
        OHLCVRequestMeta,
        OHLCVResponseEnvelope,
        compute_ohlcv_quality,
    )

    try:
        limit = _clamp(limit, low=1, high=1000)
        t0 = _time.monotonic()
        rows = await state.loader.questdb.query_latest_bars(
            asset_class=asset_class,
            bar_size=bar_size,
            contract_key=contract_key,
            limit=limit + 1,
        )
        latency_ms = (_time.monotonic() - t0) * 1000.0

        truncated = len(rows) > limit
        kept_rows = rows[:limit]
        bars = [OHLCVBar.model_validate(r) if isinstance(r, dict) else r for r in kept_rows]
        request_meta = OHLCVRequestMeta(
            symbol="*",
            asset_class=asset_class or "unknown",
            bar_size=bar_size or "unknown",
            what_to_show="unknown",
        )
        envelope = OHLCVResponseEnvelope(
            bars=bars,
            request=request_meta,
            quality=compute_ohlcv_quality(bars),
            latency_ms=latency_ms,
            cache_hit=False,
            chunk_count=1,
            source="questdb",
        )
        payload = envelope.model_dump(mode="json")
        payload["returned_count"] = len(bars)
        payload["total_count"] = len(rows)
        payload["truncated"] = truncated
        return payload
    except Exception as e:
        return _tool_error(e)


async def query_raw_sql(
    ctx: Context,
    sql: str,
    max_rows: int = MCP_DEFAULT_SQL_ROWS,
) -> dict[str, Any]:
    """Execute a raw SQL query against QuestDB with bounded output.

    Use for ad-hoc analytics, aggregations, and JOINs that the structured tools don't cover.
    SELECT only — no INSERT/UPDATE/DELETE/CLEAR/ALTER/DROP. Returns ``rows``,
    ``returned_count``, ``total_count``, and ``truncated``.

    Args:
        sql: QuestDB SQL query (SELECT only)
        max_rows: Max rows returned (default 500, max 5000; add your own LIMIT for less)
    """
    state = _state(ctx)
    if err := _require_questdb(ctx):
        return err

    stripped = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL).strip()
    stripped = re.sub(r"--.*?$", " ", stripped, flags=re.MULTILINE).strip()
    normalized = re.sub(r"\s+", " ", stripped).strip()

    if "/*" in normalized or "*/" in normalized:
        return _error("Malformed SQL block comments are not permitted", code="QUERY_REJECTED")

    if re.search(r";\s*\S", normalized):
        return _error("Multi-statement queries are not permitted", code="QUERY_REJECTED")

    first_keyword = normalized.split()[0].upper() if normalized else ""
    if first_keyword not in ("SELECT", "WITH"):
        return _error("Only SELECT and WITH (CTE) queries are permitted", code="QUERY_REJECTED")

    dangerous_keywords = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|EXECUTE|INTO)\b",
        re.IGNORECASE,
    )
    match = dangerous_keywords.search(normalized)
    if match:
        return _error(f"Keyword '{match.group(1).upper()}' is not permitted in queries", code="QUERY_REJECTED")

    max_rows = _clamp(max_rows, low=1, high=MCP_MAX_SQL_ROWS)
    sql_to_run = stripped.rstrip().rstrip(";").rstrip()
    if not re.search(r"\bLIMIT\b", normalized, re.IGNORECASE):
        # Fetch max_rows + 1 so we accurately set truncated=True when more rows exist.
        sql_to_run = f"{sql_to_run} LIMIT {max_rows + 1}"

    try:
        rows = await state.loader.questdb.fetch_dicts(sql_to_run)
        if isinstance(rows, list):
            return _truncate_items(rows, max_rows, key="rows")
        return {"rows": rows, "returned_count": 0, "total_count": 0, "truncated": False}
    except Exception as e:
        return _tool_error(e)


async def query_equity_snapshots(
    ctx: Context,
    symbol: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Query recent equity snapshots from QuestDB.

    Args:
        symbol: Optional ticker symbol filter (e.g. 'AAPL')
        start: Optional start datetime in ISO format
        end: Optional end datetime in ISO format
        limit: Max snapshots to return (default 100, max 1000)
    """
    if err := _require_questdb(ctx):
        return err
    state = _state(ctx)
    try:
        limit = _clamp(limit, low=1, high=1000)
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        rows = await state.loader.questdb.query_snapshots(symbol=symbol, start=start_dt, end=end_dt, limit=limit + 1)
        return _truncate_items(rows, limit, key="snapshots")
    except Exception as e:
        return _tool_error(e)


async def query_fx_option_snapshots(
    ctx: Context,
    symbol: str | None = None,
    expiry: str | None = None,
    strike: float | None = None,
    right: _RightOptParam = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Query FX option snapshots from QuestDB.

    Args:
        symbol: Optional currency pair filter (e.g. 'EURUSD', 'EUR/USD')
        expiry: Optional expiry filter
        strike: Optional strike price filter
        right: Optional right filter ('C'/'CALL' or 'P'/'PUT')
        start: Optional start datetime in ISO format
        end: Optional end datetime in ISO format
        limit: Max snapshots to return (default 100, max 1000)
    """
    if err := _require_questdb(ctx):
        return err
    state = _state(ctx)
    try:
        limit = _clamp(limit, low=1, high=1000)
        start_dt = datetime.fromisoformat(start) if start else None
        end_dt = datetime.fromisoformat(end) if end else None
        rows = await state.loader.questdb.query_fx_option_snapshots(
            symbol=symbol,
            expiry=expiry,
            strike=strike,
            right=right,
            start=start_dt,
            end=end_dt,
            limit=limit + 1,
        )
        return _truncate_items(rows, limit, key="snapshots")
    except Exception as e:
        return _tool_error(e)


async def get_all_latest_equity_snapshots(
    ctx: Context,
    limit: int = 100,
) -> dict[str, Any]:
    """Get the latest snapshot for ALL tracked equity symbols from QuestDB.

    Uses QuestDB LATEST ON to return the most recent snapshot per symbol.
    This queries QuestDB directly, not Redis.

    Args:
        limit: Max snapshots to return (default 100, max 1000)
    """
    if err := _require_questdb(ctx):
        return err
    state = _state(ctx)
    try:
        limit = _clamp(limit, low=1, high=1000)
        rows = await state.loader.questdb.query_latest_snapshots(limit=limit + 1)
        return _truncate_items(rows, limit, key="snapshots")
    except Exception as e:
        return _tool_error(e)


async def get_latest_bar_from_cache(
    ctx: Context,
    asset_class: _AssetClassParam,
    bar_size: _BarSizeParam,
    symbol: str | None = None,
) -> dict[str, Any]:
    """Get the latest bar from Redis cache for a symbol or entire asset class.

    Args:
        asset_class: Asset class (CRYPTO, FUTURES, INDEX, FX, EQUITY, BOND)
        bar_size: Bar size (e.g. '1 min', '5 mins', '1 hour')
        symbol: Optional specific symbol. If omitted, returns all cached bars for the class+bar_size.
    """
    state = _state(ctx)
    if err := _require_redis(ctx):
        return err
    try:
        bar = await state.redis.get_latest_bar(
            asset_class=_parse_asset_class(asset_class),
            bar_size=bar_size,
            symbol=symbol,
        )
        if bar is None:
            return {"message": "No cached bar found"}
        return {
            "symbol": bar.symbol,
            "asset_class": str(bar.asset_class),
            "exchange": bar.exchange,
            "currency": bar.currency,
            "timestamp": bar.timestamp.isoformat(),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "bar_size": bar.bar_size,
            "source": bar.source,
        }
    except Exception as e:
        return _tool_error(e)


async def get_latest_equity_snapshots(
    ctx: Context,
    symbols: str,
) -> dict[str, Any]:
    """Get latest cached equity snapshots from Redis for one or more symbols.

    Reads from Redis cache (not QuestDB), so results are near real-time.

    Args:
        symbols: Comma-separated ticker symbols (e.g. 'AAPL,MSFT,SPY')
    """
    if err := _require_redis(ctx):
        return err
    state = _state(ctx)
    try:
        symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if not symbol_list:
            return _error("No valid symbols provided", code="BAD_REQUEST")
        cached = await state.redis.get_latest_equity_snapshots(symbol_list)
        return {sym: snapshot.model_dump(mode="json") for sym, snapshot in cached.items()}
    except Exception as e:
        return _tool_error(e)


async def get_latest_fx_option_snapshot(
    ctx: Context,
    symbol: str,
    expiry: str,
    strike: float,
    right: _RightParam,
    exchange: str = "SMART",
    local_symbol: str | None = None,
    con_id: int | None = None,
) -> dict[str, Any]:
    """Get latest cached FX option snapshot from Redis.

    Reads from Redis cache (not QuestDB), so results are near real-time.

    Args:
        symbol: Currency pair (e.g. 'EURUSD', min 6 chars)
        expiry: Expiry date string (e.g. '20260619', min 6 chars)
        strike: Strike price (e.g. 1.10)
        right: Option right — 'C' for Call or 'P' for Put
        exchange: Exchange (default 'SMART')
        local_symbol: Optional IBKR local symbol
        con_id: Optional IBKR contract ID
    """
    if err := _require_redis(ctx):
        return err
    state = _state(ctx)
    try:
        snapshot = await state.redis.get_latest_fx_option_snapshot(
            symbol=symbol,
            expiry=expiry,
            strike=strike,
            right=right,
            exchange=exchange,
            local_symbol=local_symbol,
            con_id=con_id,
        )
        if snapshot is None:
            return {"message": "No cached FX option snapshot found for the given parameters"}
        return snapshot.model_dump(mode="json")
    except Exception as e:
        return _tool_error(e)


async def list_snapshot_watchlists(ctx: Context) -> dict[str, Any]:
    """List all snapshot watchlist names stored in Redis.

    Returns a list of Redis key names for configured watchlists.
    """
    if err := _require_redis(ctx):
        return err
    state = _state(ctx)
    try:
        keys = await state.redis.scan_snapshot_watchlists()
        return {"watchlists": keys, "count": len(keys), "total_count": len(keys), "truncated": False}
    except Exception as e:
        return _tool_error(e)


async def get_snapshot_watchlist(ctx: Context, name: str) -> dict[str, Any]:
    """Get a specific snapshot watchlist by name from Redis.

    Args:
        name: Watchlist name (e.g. 'us_tech', 'hk_large_cap')
    """
    if err := _require_redis(ctx):
        return err
    state = _state(ctx)
    try:
        from src.config import config_constant as wl_constants
        from src.feeds.snapshot_models import SnapshotWatchlist

        key = wl_constants.REDIS_SNAPSHOT_WATCHLIST_KEY_TEMPLATE.format(name=name.strip().lower())
        payload = await state.redis.get_raw(key)
        if payload is None:
            return {"message": f"Watchlist '{name}' not found"}
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        watchlist = SnapshotWatchlist.model_validate_json(payload)
        return watchlist.model_dump(mode="json")
    except Exception as e:
        return _tool_error(e)


async def get_redis_key(
    ctx: Context,
    key: str,
    max_chars: int = MCP_DEFAULT_TEXT_CHARS,
) -> dict[str, Any]:
    """Read a raw Redis key. For debugging and inspection.

    Values longer than ``max_chars`` are truncated; ``truncated`` and
    ``total_count`` (character length) are reported.

    Args:
        key: Full Redis key to read
        max_chars: Max characters returned (default 20000, max 100000)
    """
    if _SENSITIVE_KEY_PATTERNS.search(key):
        return _error("Access to this key is restricted", code="FORBIDDEN")

    state = _state(ctx)
    if err := _require_redis(ctx):
        return err
    try:
        raw = await state.redis.get_raw(key)
        if raw is None:
            return {"message": f"Key '{key}' not found", "code": "NOT_FOUND"}
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                raw = raw.hex()
        return _truncate_text(str(raw), max_chars, key="value", extra={"key": key})
    except Exception as e:
        return _tool_error(e)


def register(mcp: FastMCP) -> None:
    for fn in (
        query_historical_ohlcv,
        query_latest_bars,
        query_raw_sql,
        query_equity_snapshots,
        query_fx_option_snapshots,
        get_all_latest_equity_snapshots,
        get_latest_bar_from_cache,
        get_latest_equity_snapshots,
        get_latest_fx_option_snapshot,
        list_snapshot_watchlists,
        get_snapshot_watchlist,
        get_redis_key,
    ):
        mcp.tool(annotations=_READ_TOOL)(fn)
