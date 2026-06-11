"""
MCP (Model Context Protocol) server for IBPython.

Exposes IBKR market data, QuestDB historical queries, Redis cache,
scheduler status, and live IBKR feed as MCP tools and resources
that any MCP-compatible AI agent can query.

Run:
    python -m src.mcp_server
    # or with Streamable HTTP (binds 127.0.0.1:9000 by default):
    MCP_HTTP_HOST=0.0.0.0 MCP_HTTP_PORT=9000 MCP_API_KEY=secret python -m src.mcp_server --http
    # or with uvicorn:
    uvicorn src.mcp_server:mcp.streamable_http_app --host 127.0.0.1 --port 9000
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import sys
import time as _time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Awaitable

from mcp.server.fastmcp import Context, FastMCP

from src.config import config_constant as constants
from src.config.settings import Settings, load_settings
from src.webapp.dependencies import IBKRRestAppState, build_rest_app_state

logger = logging.getLogger(__name__)


@dataclass
class _MCPSharedState:
    state: IBKRRestAppState
    redis_connected: bool
    questdb_connected: bool
    ibkr_connected: bool
    idle_disconnect_seconds: float
    ref_count: int = 0
    close_task: asyncio.Task[None] | None = None
    closing: bool = False
    cancel_idle_close_for_reuse: bool = False

    def context(self) -> dict[str, Any]:
        return {
            "ibkr_state": self.state,
            "redis_connected": self.redis_connected,
            "questdb_connected": self.questdb_connected,
            "ibkr_connected": self.ibkr_connected,
        }


_mcp_shared_state: _MCPSharedState | None = None
_mcp_shared_state_lock: asyncio.Lock | None = None
_mcp_shared_state_lock_loop: asyncio.AbstractEventLoop | None = None


def _get_mcp_shared_state_lock() -> asyncio.Lock:
    global _mcp_shared_state_lock
    global _mcp_shared_state_lock_loop
    loop = asyncio.get_running_loop()
    if _mcp_shared_state_lock is None or _mcp_shared_state_lock_loop is not loop:
        _mcp_shared_state_lock = asyncio.Lock()
        _mcp_shared_state_lock_loop = loop
    return _mcp_shared_state_lock


async def _build_mcp_shared_state(settings: Settings) -> _MCPSharedState:
    mcp_settings = settings.model_copy(update={"ibkr_client_id": settings.ibkr_mcp_client_id})
    state = build_rest_app_state(mcp_settings)

    redis_connected = False
    questdb_connected = False
    ibkr_connected = False

    try:
        await state.redis.connect()
        redis_connected = True
        logger.info("MCP server: Redis connected")
    except Exception:
        logger.exception("MCP server: Redis connection failed")

    try:
        questdb = getattr(state.loader, "questdb", None)
        if questdb is None:
            logger.warning("MCP server: QuestDB store is not configured")
        else:
            await questdb.connect()
            questdb_connected = True
            logger.info("MCP server: QuestDB connected")
    except Exception:
        logger.exception("MCP server: QuestDB connection failed")

    try:
        await state.feed.connect()
        ibkr_connected = True
        logger.info("MCP server: IBKR feed connected")
    except Exception:
        logger.exception("MCP server: IBKR feed connection failed (tools requiring live IBKR will reconnect lazily)")

    return _MCPSharedState(
        state=state,
        redis_connected=redis_connected,
        questdb_connected=questdb_connected,
        ibkr_connected=ibkr_connected,
        idle_disconnect_seconds=settings.mcp_ibkr_idle_disconnect_seconds,
    )


async def _cancel_mcp_idle_close(shared: _MCPSharedState) -> None:
    task = shared.close_task
    shared.close_task = None
    if task is None or task.done():
        return
    shared.cancel_idle_close_for_reuse = True
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.debug("MCP server: cancelled idle IBKR disconnect")


async def _acquire_mcp_shared_state(settings: Settings) -> _MCPSharedState:
    global _mcp_shared_state
    while True:
        close_task_to_wait: asyncio.Task[None] | None = None
        async with _get_mcp_shared_state_lock():
            if _mcp_shared_state is None:
                _mcp_shared_state = await _build_mcp_shared_state(settings)
                _mcp_shared_state.ref_count = 1
                return _mcp_shared_state
            if _mcp_shared_state.closing:
                close_task_to_wait = _mcp_shared_state.close_task
            else:
                _mcp_shared_state.ref_count += 1
                shared = _mcp_shared_state
                break
        if close_task_to_wait is not None:
            await close_task_to_wait
        else:
            await asyncio.sleep(0)

    await _cancel_mcp_idle_close(shared)
    return shared


async def _close_mcp_shared_state(shared: _MCPSharedState) -> None:
    try:
        await shared.state.close()
        questdb = getattr(shared.state.loader, "questdb", None)
        close = getattr(questdb, "close", None)
        if callable(close):
            await close()
    finally:
        logger.info("MCP server: all connections closed")


async def _close_mcp_shared_state_after_idle(shared: _MCPSharedState, delay_seconds: float) -> None:
    global _mcp_shared_state
    try:
        await asyncio.sleep(delay_seconds)
        async with _get_mcp_shared_state_lock():
            if _mcp_shared_state is not shared or shared.ref_count > 0:
                return
            shared.closing = True
        await _close_mcp_shared_state(shared)
        async with _get_mcp_shared_state_lock():
            if _mcp_shared_state is shared:
                _mcp_shared_state = None
    except asyncio.CancelledError:
        if shared.cancel_idle_close_for_reuse:
            shared.cancel_idle_close_for_reuse = False
            raise
        if shared.ref_count == 0:
            shared.closing = True
            await _close_mcp_shared_state(shared)
            async with _get_mcp_shared_state_lock():
                if _mcp_shared_state is shared:
                    _mcp_shared_state = None
        raise


async def _release_mcp_shared_state(shared: _MCPSharedState) -> None:
    global _mcp_shared_state
    close_now = False
    async with _get_mcp_shared_state_lock():
        if _mcp_shared_state is not shared:
            return
        shared.ref_count = max(0, shared.ref_count - 1)
        if shared.ref_count > 0:
            return
        delay_seconds = shared.idle_disconnect_seconds
        if delay_seconds <= 0:
            shared.closing = True
            _mcp_shared_state = None
            close_now = True
        else:
            logger.info("MCP server: idle; keeping IBKR clientId=%s for %.1fs", shared.state.feed.client_id, delay_seconds)
            shared.close_task = asyncio.create_task(_close_mcp_shared_state_after_idle(shared, delay_seconds))

    if close_now:
        await _close_mcp_shared_state(shared)


# ── Lifespan: manage connections for the MCP server ──────────────────────────

@asynccontextmanager
async def mcp_lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:
    """Connect to Redis, QuestDB, and IBKR on startup; tear down on shutdown."""
    settings = load_settings()
    shared = await _acquire_mcp_shared_state(settings)
    try:
        yield shared.context()
    finally:
        await _release_mcp_shared_state(shared)


mcp = FastMCP(
    "IBPython Market Data",
    lifespan=mcp_lifespan,
)


def _state(ctx: Context) -> IBKRRestAppState:
    return ctx.request_context.lifespan_context["ibkr_state"]


def _parse_asset_class(asset_class: str) -> Any:
    """Parse MCP asset_class text using AssetClass values, names, and documented aliases."""
    from src.feeds.models import AssetClass

    normalized = asset_class.strip().lower()
    aliases = {
        "futures": AssetClass.FUTURE.value,
        "stocks": AssetClass.EQUITY.value,
        "stock": AssetClass.EQUITY.value,
        "equities": AssetClass.EQUITY.value,
        "forex": AssetClass.FX.value,
    }
    normalized = aliases.get(normalized, normalized)
    try:
        return AssetClass(normalized)
    except ValueError:
        member = AssetClass.__members__.get(asset_class.strip().upper())
        if member is not None:
            return member
        supported = ", ".join(item.value for item in AssetClass)
        raise ValueError(f"unsupported asset_class '{asset_class}'; expected one of: {supported}")


def _require_questdb(ctx: Context) -> dict[str, Any] | None:
    """Return an error dict if QuestDB is not connected, else None."""
    if not ctx.request_context.lifespan_context.get("questdb_connected"):
        return {"error": "QuestDB is not available"}
    return None


def _require_redis(ctx: Context) -> dict[str, Any] | None:
    """Return an error dict if Redis is not connected, else None."""
    if not ctx.request_context.lifespan_context.get("redis_connected"):
        return {"error": "Redis is not available"}
    return None


def _require_ibkr(ctx: Context) -> dict[str, Any] | None:
    """Return an error dict if IBKR feed is not connected, else None."""
    if not ctx.request_context.lifespan_context.get("ibkr_connected"):
        return {"error": "IBKR feed is not available"}
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# QUESTDB — Historical Data Tools
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def query_historical_ohlcv(
    ctx: Context,
    symbol: str,
    asset_class: str | None = None,
    bar_size: str | None = None,
    contract_key: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = 5000,
) -> dict[str, Any]:
    """Query historical OHLCV bars from QuestDB.

    Args:
        symbol: Ticker symbol (e.g. 'BTCUSDT', 'ES', 'EUR.USD')
        asset_class: Filter by asset class (CRYPTO, FUTURES, INDEX, FX, EQUITY, BOND, OPTION)
        bar_size: Bar size (e.g. '1 min', '5 mins', '1 hour', '1 day')
        contract_key: Specific contract key for futures/options
        start: Start datetime in ISO format (e.g. '2026-05-01T00:00:00Z')
        end: End datetime in ISO format
        limit: Max rows to return (default 5000, max 50000)
    """
    if err := _require_questdb(ctx):
        return err
    state = _state(ctx)
    limit = min(limit, 50_000)
    start_dt = datetime.fromisoformat(start) if start else None
    end_dt = datetime.fromisoformat(end) if end else None

    from src.feeds.models import (
        OHLCVBar, OHLCVResponseEnvelope, OHLCVRequestMeta,
        compute_ohlcv_quality,
    )

    t0 = _time.monotonic()
    rows = await state.loader.questdb.query_historical_bars(
        symbol=symbol,
        asset_class=asset_class,
        bar_size=bar_size,
        contract_key=contract_key,
        start=start_dt,
        end=end_dt,
        limit=limit,
    )
    latency_ms = (_time.monotonic() - t0) * 1000.0

    bars = [OHLCVBar.model_validate(r) if isinstance(r, dict) else r for r in rows]
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
    return envelope.model_dump(mode="json")


@mcp.tool()
async def query_latest_bars(
    ctx: Context,
    asset_class: str | None = None,
    bar_size: str | None = None,
    contract_key: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Query latest OHLCV bars across all symbols from QuestDB.

    Uses QuestDB LATEST ON to get the most recent bar per symbol+contract_key.

    Args:
        asset_class: Filter by asset class
        bar_size: Filter by bar size
        contract_key: Filter by contract key
        limit: Max rows (default 100)
    """
    state = _state(ctx)
    from src.feeds.models import (
        OHLCVBar, OHLCVResponseEnvelope, OHLCVRequestMeta,
        compute_ohlcv_quality,
    )

    try:
        t0 = _time.monotonic()
        rows = await state.loader.questdb.query_latest_bars(
            asset_class=asset_class,
            bar_size=bar_size,
            contract_key=contract_key,
            limit=limit,
        )
        latency_ms = (_time.monotonic() - t0) * 1000.0

        bars = [OHLCVBar.model_validate(r) if isinstance(r, dict) else r for r in rows]
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
        return envelope.model_dump(mode="json")
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def query_raw_sql(
    ctx: Context,
    sql: str,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Execute a raw SQL query against QuestDB.

    Use for ad-hoc analytics, aggregations, and JOINs that the structured tools don't cover.
    SELECT only — no INSERT/UPDATE/DELETE/CLEAR/ALTER/DROP.

    Args:
        sql: QuestDB SQL query (SELECT only)
    """
    state = _state(ctx)

    # Strip block comments (/* ... */) and inline comments (--)
    stripped = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL).strip()
    stripped = re.sub(r"--.*?$", " ", stripped, flags=re.MULTILINE).strip()
    # Normalize whitespace for reliable keyword matching
    normalized = re.sub(r"\s+", " ", stripped).strip()

    # Block remaining comment markers (unmatched delimiters)
    if "/*" in normalized or "*/" in normalized:
        return {"error": "Malformed SQL block comments are not permitted"}

    # Block multi-statement injection: semicolons followed by non-whitespace
    if re.search(r";\s*\S", normalized):
        return {"error": "Multi-statement queries are not permitted"}

    # Must start with an allowed keyword (SELECT or WITH for CTEs)
    first_keyword = normalized.split()[0].upper() if normalized else ""
    if first_keyword not in ("SELECT", "WITH"):
        return {"error": "Only SELECT and WITH (CTE) queries are permitted"}

    # Block dangerous keywords anywhere in the query.
    # NOTE: This scans raw text including string literals, so queries like
    #   SELECT * FROM foo WHERE name = 'INSERT INTO bar'
    # will be falsely blocked. A full SQL parser would be needed to fix this.
    dangerous_keywords = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|EXECUTE|INTO)\b",
        re.IGNORECASE,
    )
    match = dangerous_keywords.search(normalized)
    if match:
        return {"error": f"Keyword '{match.group(1).upper()}' is not permitted in queries"}

    try:
        return await state.loader.questdb.fetch_dicts(stripped)
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# REDIS CACHE — Latest Bars & Scheduler Status
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def get_latest_bar_from_cache(
    ctx: Context,
    asset_class: str,
    bar_size: str,
    symbol: str | None = None,
) -> dict[str, Any]:
    """Get the latest bar from Redis cache for a symbol or entire asset class.

    Args:
        asset_class: Asset class (CRYPTO, FUTURES, INDEX, FX, EQUITY, BOND)
        bar_size: Bar size (e.g. '1 min', '5 mins', '1 hour')
        symbol: Optional specific symbol. If omitted, returns all cached bars for the class+bar_size.
    """
    state = _state(ctx)
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


@mcp.tool()
async def list_scheduler_jobs(
    ctx: Context,
) -> dict[str, Any]:
    """List all registered scheduler jobs from Redis.

    Returns job names that can be used with other scheduler tools.
    """
    state = _state(ctx)
    keys = await state.redis.scan_scheduler_jobs()
    return {"jobs": keys, "count": len(keys)}


_SENSITIVE_KEY_PATTERNS = re.compile(
    r"(auth|token|secret|password|bearer)",
    re.IGNORECASE,
)


@mcp.tool()
async def get_redis_key(
    ctx: Context,
    key: str,
) -> dict[str, Any]:
    """Read a raw Redis key. For debugging and inspection.

    Args:
        key: Full Redis key to read
    """
    if _SENSITIVE_KEY_PATTERNS.search(key):
        return {"error": "Access to this key is restricted"}

    state = _state(ctx)
    raw = await state.redis.get_raw(key)
    if raw is None:
        return {"message": f"Key '{key}' not found"}
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            raw = raw.hex()
    return {"key": key, "value": raw}


# ═══════════════════════════════════════════════════════════════════════════════
# IBKR LIVE FEED — Market Data & Account
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def load_option_chains(
    ctx: Context,
    symbol: str,
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
) -> list[dict[str, Any]]:
    """Load option chain for a symbol from IBKR.

    Returns all available expirations and strikes for the given underlying.

    Args:
        symbol: Underlying symbol (e.g. 'AAPL', 'SPY', 'SPX')
        asset_class: EQUITY or INDEX (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
    """
    state = _state(ctx)
    from src.feeds.contracts import OptionChainRequest

    try:
        request = OptionChainRequest(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
        )
        chains = await state.feed.load_option_chains(request)
        result = []
        for chain in chains:
            result.append({
                "symbol": chain.symbol,
                "exchange": chain.exchange,
                "expirations": chain.expirations,
                "strikes": chain.strikes,
                "trading_class": getattr(chain, "trading_class", None),
                "multiplier": getattr(chain, "multiplier", None),
            })
        return result
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def load_account_summary(
    ctx: Context,
    account: str = "",
) -> dict[str, dict[str, str]]:
    """Load IBKR account summary (NetLiquidation, AvailableFunds, etc).

    Args:
        account: Account ID (empty string for default account)
    """
    state = _state(ctx)
    try:
        rows = await state.feed.load_account_summary(account=account)
        return {row.tag: {"value": row.value, "currency": row.currency} for row in rows}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def load_live_positions(
    ctx: Context,
) -> list[dict[str, Any]]:
    """Load all live positions from IBKR."""
    state = _state(ctx)
    try:
        positions = await state.feed.load_live_positions()
        return [
            {
                "symbol": p.symbol,
                "sec_type": p.sec_type,
                "exchange": p.exchange,
                "currency": p.currency,
                "position": p.position,
                "average_cost": p.average_cost,
                "account": p.account,
                "con_id": p.con_id,
            }
            for p in positions
        ]
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def load_historical_ohlcv_live(
    ctx: Context,
    symbol: str,
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    bar_size: str = "1 day",
    duration: str = "1 M",
    what_to_show: str = "TRADES",
    use_rth: bool = True,
) -> dict[str, Any]:
    """Load historical OHLCV bars directly from IBKR (not QuestDB).

    Use this when you need data not yet persisted, or for real-time snapshots.

    Args:
        symbol: Ticker symbol
        asset_class: CRYPTO, FUTURES, INDEX, FX, EQUITY, BOND (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        bar_size: '1 min', '5 mins', '15 mins', '1 hour', '4 hours', '1 day', '1 week'
        duration: IBKR duration string: '30 D', '1 M', '1 Y', etc
        what_to_show: TRADES, MIDPOINT, BID, ASK, BID_ASK (default 'TRADES')
        use_rth: Use regular trading hours only (default True)
    """
    state = _state(ctx)
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import (
        OHLCVRequest,
        OHLCVResponseEnvelope, OHLCVRequestMeta, compute_ohlcv_quality,
    )
    from src.transport.metrics import metrics

    request = OHLCVRequest(
        symbol=symbol,
        asset_class=_parse_asset_class(asset_class),
        exchange=exchange,
        currency=currency,
        bar_size=bar_size,
        duration=duration,
        what_to_show=what_to_show,
        use_rth=use_rth,
    )

    t0 = _time.monotonic()
    auto_chunk_plan = plan_historical_auto_chunk(request)
    if auto_chunk_plan is not None:
        ensure_historical_chunk_limit(request, auto_chunk_plan, max_chunks=state.settings.ibkr_historical_max_chunks)
        metrics.market_data_historical_auto_chunks_total.inc(
            {"asset_class": request.asset_class.value, "operation": "mcp", "status": "planned"}
        )
        logger.info(
            "MCP historical OHLCV auto_chunking symbol=%s bar_size=%s duration=%s estimated_chunks=%d",
            request.symbol,
            request.bar_size,
            request.duration,
            auto_chunk_plan.estimated_chunks,
        )
        range_request = request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime})
        bars = await state.feed.load_historical_ohlcv_range(
            range_request,
            start_datetime=auto_chunk_plan.start_datetime,
            end_datetime=auto_chunk_plan.end_datetime,
            max_chunks=state.settings.ibkr_historical_max_chunks,
        )
    else:
        bars = await state.feed.load_historical_ohlcv(request, max_chunks=state.settings.ibkr_historical_max_chunks)
    latency_ms = (_time.monotonic() - t0) * 1000.0

    request_meta = OHLCVRequestMeta(
        symbol=request.symbol,
        asset_class=request.asset_class.value,
        exchange=request.exchange,
        currency=request.currency,
        bar_size=request.bar_size,
        what_to_show=request.what_to_show,
        use_rth=request.use_rth,
        duration=request.duration,
    )
    envelope = OHLCVResponseEnvelope(
        bars=bars,
        request=request_meta,
        quality=compute_ohlcv_quality(bars),
        latency_ms=latency_ms,
        cache_hit=False,
        chunk_count=1,
        source="ibkr",
    )
    return envelope.model_dump(mode="json")


@mcp.tool()
async def search_contracts(
    ctx: Context,
    pattern: str,
) -> dict[str, Any] | list[Any]:
    """Search IBKR contract database for matching symbols.

    Args:
        pattern: Search pattern (e.g. 'Apple', 'TESLA', 'ES futures')
    """
    state = _state(ctx)
    try:
        results = await state.feed.search_matching_symbols(pattern)
        if not results:
            return {"message": "No matching contracts found"}
        return results
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# REFERENCE DATA
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def load_fundamentals(
    ctx: Context,
    symbol: str,
    exchange: str = "SMART",
    report_type: str = "ReportSnapshot",
) -> dict[str, Any]:
    """Load fundamental data for a stock from IBKR.

    Returns raw XML report from IBKR. Report types: ReportSnapshot, ReportsFinSummary,
    ReportRatios, ReportsOwnership, ReportsFinStatements.

    Args:
        symbol: Stock symbol (e.g. 'AAPL', 'MSFT')
        exchange: Exchange (default 'SMART')
        report_type: Report type (default 'ReportSnapshot')
    """
    state = _state(ctx)
    from src.feeds.fundamental_data import FundamentalDataRequest

    request = FundamentalDataRequest(
        symbol=symbol,
        exchange=exchange,
        report_type=report_type,
    )
    report = await state.feed.load_fundamental_data(request)
    return {
        "symbol": report.symbol,
        "report_type": str(report.report_type),
        "received_at": report.received_at.isoformat(),
        "raw_xml": report.raw_xml[:5000],  # Truncate for MCP context limits
    }


@mcp.tool()
async def load_news(
    ctx: Context,
    symbol: str,
    provider_codes: list[str] | None = None,
    limit: int = 20,
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
) -> dict[str, Any] | list[dict[str, Any]]:
    """Load recent news headlines for a symbol from IBKR.

    Args:
        symbol: Ticker symbol (e.g. 'AAPL', 'BTC')
        provider_codes: News provider codes (default ['BRFG', 'BRFUPDN'])
        limit: Max articles to return (default 20, max 300)
        asset_class: Asset class (CRYPTO, FUTURES, INDEX, FX, EQUITY, BOND, OPTION; default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
    """
    state = _state(ctx)
    from src.feeds.news import HistoricalNewsRequest

    # First resolve the contract to get con_id
    try:
        from src.feeds.contracts import ContractSpec
        spec = ContractSpec(symbol=symbol, asset_class=_parse_asset_class(asset_class), exchange=exchange, currency=currency)
        contract = await state.feed.qualify_contract(spec)
        con_id = contract.conId
    except Exception:
        return {"error": f"Could not resolve contract for {symbol}"}

    providers = tuple(provider_codes or ["BRFG", "BRFUPDN"])
    request = HistoricalNewsRequest(
        con_id=con_id,
        provider_codes=providers,
        total_results=min(limit, 300),
    )
    articles = await state.feed.load_historical_news(request)
    return [a.model_dump() for a in articles]


# ═══════════════════════════════════════════════════════════════════════════════
# SNAPSHOTS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def query_equity_snapshots(
    ctx: Context,
    symbol: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Query recent equity snapshots from QuestDB.

    Args:
        symbol: Optional ticker symbol filter (e.g. 'AAPL')
        start: Optional start datetime in ISO format
        end: Optional end datetime in ISO format
        limit: Max snapshots to return (default 100)
    """
    state = _state(ctx)
    start_dt = datetime.fromisoformat(start) if start else None
    end_dt = datetime.fromisoformat(end) if end else None
    return await state.loader.questdb.query_snapshots(symbol=symbol, start=start_dt, end=end_dt, limit=limit)


@mcp.tool()
async def query_fx_option_snapshots(
    ctx: Context,
    symbol: str | None = None,
    expiry: str | None = None,
    strike: float | None = None,
    right: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Query FX option snapshots from QuestDB.

    Args:
        symbol: Optional currency pair filter (e.g. 'EURUSD', 'EUR/USD')
        expiry: Optional expiry filter
        strike: Optional strike price filter
        right: Optional right filter ('C'/'CALL' or 'P'/'PUT')
        start: Optional start datetime in ISO format
        end: Optional end datetime in ISO format
        limit: Max snapshots to return (default 100)
    """
    state = _state(ctx)
    start_dt = datetime.fromisoformat(start) if start else None
    end_dt = datetime.fromisoformat(end) if end else None
    return await state.loader.questdb.query_fx_option_snapshots(
        symbol=symbol, expiry=expiry, strike=strike, right=right,
        start=start_dt, end=end_dt, limit=limit,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ACCOUNT — Portfolio & PnL
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def load_portfolio_items(
    ctx: Context,
    account: str = "",
) -> dict[str, Any] | list[dict[str, Any]]:
    """Load portfolio items (positions with market data) from IBKR.

    Returns a list of portfolio items with symbol, position, market price,
    market value, average cost, unrealized PnL, realized PnL, etc.

    Args:
        account: Account ID (empty string for default account)
    """
    state = _state(ctx)
    try:
        items = await state.feed.load_portfolio_items(account=account)
        return [item.model_dump() for item in items]
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def load_account_pnl(
    ctx: Context,
    account: str = "",
    model_code: str = "",
    wait_seconds: float = 4.0,
) -> dict[str, Any]:
    """Load account-level PnL snapshot from IBKR.

    Subscribes to account PnL, waits for the first update, then returns
    daily PnL, unrealized PnL, and realized PnL.

    Args:
        account: Account ID (empty string for default account)
        model_code: Optional model/portfolio code
        wait_seconds: Seconds to wait for PnL subscription data (default 4.0)
    """
    state = _state(ctx)
    try:
        pnl = await state.feed.load_account_pnl_snapshot(
            account=account, model_code=model_code, wait_seconds=wait_seconds,
        )
        return pnl.model_dump()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def load_position_pnl(
    ctx: Context,
    account: str = "",
    con_id: int = 0,
    model_code: str = "",
) -> dict[str, Any]:
    """Load position-level PnL snapshot from IBKR.

    Subscribes to position PnL for a specific contract or all positions,
    waits for the first update, then returns position PnL data.

    Args:
        account: Account ID (empty string for default account)
        con_id: IBKR contract ID. Use 0 (default) for all positions
        model_code: Optional model/portfolio code
    """
    state = _state(ctx)
    try:
        pnl = await state.feed.load_position_pnl_snapshot(
            account=account, con_id=con_id, model_code=model_code,
        )
        return pnl.model_dump()
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# NEWS — Providers & Articles
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def load_news_providers(
    ctx: Context,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Load available IBKR news providers for the current account.

    Returns a list of news providers the account is entitled to access,
    each with a provider code and name.
    """
    state = _state(ctx)
    try:
        providers = await state.feed.load_news_providers()
        return [
            {"provider_code": p.provider_code, "provider_name": p.provider_name}
            for p in providers
        ]
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def load_news_article(
    ctx: Context,
    provider_code: str,
    article_id: str,
) -> dict[str, Any]:
    """Load the full text of a news article from IBKR.

    Use load_news first to get headlines and article IDs, then call this
    tool to retrieve the full article body.

    Args:
        provider_code: News provider code (e.g. 'BRFG', 'BRFUPDN')
        article_id: Article ID from a news headline
    """
    state = _state(ctx)
    from src.feeds.news import NewsArticleRequest

    try:
        request = NewsArticleRequest(provider_code=provider_code, article_id=article_id)
        article = await state.feed.load_news_article(request)
        return {
            "provider_code": article.provider_code,
            "article_id": article.article_id,
            "article_type": article.article_type,
            "article_text": article.article_text,
            "received_at": article.received_at.isoformat(),
        }
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# WSH — Wall Street Horizon
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def load_wsh_metadata(
    ctx: Context,
) -> dict[str, Any]:
    """Load Wall Street Horizon (WSH) metadata from IBKR.

    Returns the WSH metadata report including available event types,
    countries, and other configuration for WSH calendar data.
    """
    state = _state(ctx)
    try:
        report = await state.feed.load_wsh_metadata()
        return {
            "received_at": report.received_at.isoformat(),
            "source": report.source,
            "payload": report.payload,
            "metadata": report.metadata,
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def load_wsh_event_data(
    ctx: Context,
    con_id: int | None = None,
    con_ids: list[int] | None = None,
    country: str = "All",
    limit: int = 10,
    event_types: list[str] | None = None,
) -> dict[str, Any]:
    """Load Wall Street Horizon event data from IBKR.

    Queries corporate event calendar data (earnings, dividends, splits, etc.)
    for specific contracts or by country/region.

    Args:
        con_id: Single IBKR contract ID to query events for
        con_ids: List of IBKR contract IDs to query events for
        country: Country filter (default 'All')
        limit: Max events to return per region (default 10)
        event_types: WSH event type tags (e.g. ['EARN'], ['DIVIDEND'])
    """
    state = _state(ctx)
    from src.feeds.fundamental_data import WSHEventDataRequest

    try:
        request = WSHEventDataRequest(
            con_id=con_id,
            con_ids=con_ids or [],
            country=country,
            limit=limit,
            event_types=event_types or [],
        )
        report = await state.feed.load_wsh_event_data(request)
        return {
            "received_at": report.received_at.isoformat(),
            "source": report.source,
            "payload": report.payload,
            "metadata": report.metadata,
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def load_economic_calendar(
    ctx: Context,
    country: str = "US",
    limit: int = 20,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """Load economic calendar events from IBKR Wall Street Horizon.

    Returns economic events (e.g. GDP, employment, CPI) for a country.
    This is a convenience wrapper around WSH event data with economic
    event types pre-configured.

    Args:
        country: Country code (default 'US')
        limit: Max events to return (default 20)
        start_date: Optional start date in ISO format (e.g. '2026-06-01')
        end_date: Optional end date in ISO format (e.g. '2026-06-30')
    """
    state = _state(ctx)
    from src.feeds.fundamental_data import WSHEventDataRequest

    try:
        from datetime import date as date_type

        sd = date_type.fromisoformat(start_date) if start_date else None
        ed = date_type.fromisoformat(end_date) if end_date else None

        request = WSHEventDataRequest(
            country=country,
            limit=limit,
            start_date=sd,
            end_date=ed,
        )
        report = await state.feed.load_wsh_event_data(request)
        return {
            "received_at": report.received_at.isoformat(),
            "source": report.source,
            "payload": report.payload,
            "metadata": report.metadata,
        }
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# RESOURCES — Static reference data
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
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
            return {"error": "No valid symbols provided"}
        cached = await state.redis.get_latest_equity_snapshots(symbol_list)
        result = {}
        for sym, snapshot in cached.items():
            result[sym] = snapshot.model_dump(mode="json")
        return result
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def get_latest_fx_option_snapshot(
    ctx: Context,
    symbol: str,
    expiry: str,
    strike: float,
    right: str,
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
        return {"error": str(e)}


@mcp.tool()
async def get_all_latest_equity_snapshots(
    ctx: Context,
    limit: int = 100,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Get the latest snapshot for ALL tracked equity symbols from QuestDB.

    Uses QuestDB LATEST ON to return the most recent snapshot per symbol.
    This queries QuestDB directly, not Redis.

    Args:
        limit: Max snapshots to return (default 100, max 1000)
    """
    if err := _require_questdb(ctx):
        return [err]  # type: ignore[list-item]
    state = _state(ctx)
    try:
        limit = max(1, min(limit, 1000))
        return await state.loader.questdb.query_latest_snapshots(limit=limit)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def list_snapshot_watchlists(
    ctx: Context,
) -> dict[str, Any]:
    """List all snapshot watchlist names stored in Redis.

    Returns a list of Redis key names for configured watchlists.
    """
    if err := _require_redis(ctx):
        return err
    state = _state(ctx)
    try:
        keys = await state.redis.scan_snapshot_watchlists()
        return {"watchlists": keys, "count": len(keys)}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def get_snapshot_watchlist(
    ctx: Context,
    name: str,
) -> dict[str, Any]:
    """Get a specific snapshot watchlist by name from Redis.

    Args:
        name: Watchlist name (e.g. 'us_tech', 'hk_large_cap')
    """
    if err := _require_redis(ctx):
        return err
    state = _state(ctx)
    try:
        from src.config import config_constant as wl_constants
        key = wl_constants.REDIS_SNAPSHOT_WATCHLIST_KEY_TEMPLATE.format(name=name.strip().lower())
        payload = await state.redis.get_raw(key)
        if payload is None:
            return {"message": f"Watchlist '{name}' not found"}
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        from src.feeds.snapshot_models import SnapshotWatchlist
        watchlist = SnapshotWatchlist.model_validate_json(payload)
        return watchlist.model_dump(mode="json")
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# OPTIONS ANALYTICS & TICK DATA
# ═══════════════════════════════════════════════════════════════════════════════


@mcp.tool()
async def load_option_analytics(
    ctx: Context,
    symbol: str,
    expiry: str,
    strike: float,
    right: str = "C",
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    multiplier: str = "100",
) -> dict[str, Any]:
    """Load real-time option greeks/analytics from IBKR.

    Returns delta, gamma, theta, vega, implied volatility, OI, volume, etc.

    Args:
        symbol: Underlying symbol (e.g. 'AAPL', 'SPY')
        expiry: Option expiry (e.g. '20260619')
        strike: Strike price
        right: 'C' for Call or 'P' for Put (default 'C')
        asset_class: EQUITY or INDEX (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        multiplier: Option multiplier (default '100')
    """
    state = _state(ctx)
    from src.feeds.options_models import OptionAnalyticsRequest, OptionContractSpec, OptionRight

    try:
        spec = OptionContractSpec(
            underlying_symbol=symbol.upper(),
            expiry=expiry,
            strike=strike,
            right=OptionRight(right.upper()),
            exchange=exchange,
            currency=currency,
            multiplier=multiplier,
        )
        request = OptionAnalyticsRequest(contract=spec)
        snapshot = await state.feed.load_option_analytics(request)
        return snapshot.model_dump(mode="json")
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def load_option_skew_surface(
    ctx: Context,
    symbol: str,
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    max_expirations: int = 6,
    strike_window_pct: float = 0.30,
) -> dict[str, Any]:
    """Load option skew surface across expirations.

    Returns put-minus-call IV skew per expiry, max call/put OI, and per-strike greeks.

    Args:
        symbol: Underlying symbol (e.g. 'AAPL', 'SPY')
        asset_class: EQUITY or INDEX (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        max_expirations: Max expirations to sample (default 6)
        strike_window_pct: Strike window around spot as percentage (default 0.30)
    """
    state = _state(ctx)
    from src.feeds.contracts import OptionChainRequest
    from src.feeds.options_models import OptionSkewSurfaceRequest

    try:
        chain_request = OptionChainRequest(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
        )
        request = OptionSkewSurfaceRequest(
            chain_request=chain_request,
            max_expirations=max_expirations,
            strike_window_pct=strike_window_pct,
        )
        response = await state.feed.load_option_skew_surface(request)
        return response.model_dump(mode="json")
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def get_latest_ticks(
    ctx: Context,
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    n: int = 100,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Get latest N ticks from in-memory tick buffer.

    Requires an active tick-by-tick subscription (start_tick_by_tick) for the symbol.

    Args:
        symbol: Ticker symbol
        sec_type: Security type (default 'STK')
        exchange: Exchange (default 'SMART')
        n: Number of ticks to return (default 100)
    """
    state = _state(ctx)
    try:
        ticks = state.feed.get_latest_ticks(symbol, sec_type, exchange, n)
        return [t.model_dump(mode="json") for t in ticks]
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def load_historical_ticks(
    ctx: Context,
    symbol: str,
    start_date: str,
    end_date: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
    what_to_show: str = "TRADES",
    use_rth: bool = True,
    max_ticks: int = 10000,
) -> dict[str, Any]:
    """Load historical tick data from IBKR.

    Returns timestamped ticks with price, size, bid/ask, etc.

    Args:
        symbol: Ticker symbol
        start_date: Start datetime ISO format (e.g. '2026-06-01T09:30:00Z')
        end_date: End datetime ISO format (e.g. '2026-06-01T16:00:00Z')
        sec_type: Security type (default 'STK')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        what_to_show: TRADES, BID_ASK, or MIDPOINT (default 'TRADES')
        use_rth: Regular trading hours only (default True)
        max_ticks: Max ticks to return (default 10000)
    """
    state = _state(ctx)
    from src.feeds.tick_data import HistoricalTickRequest

    try:
        request = HistoricalTickRequest(
            symbol=symbol,
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
            start_date=datetime.fromisoformat(start_date.replace("Z", "+00:00")),  # type: ignore[arg-type]
            end_date=datetime.fromisoformat(end_date.replace("Z", "+00:00")),  # type: ignore[arg-type]
            what_to_show=what_to_show,
            use_rth=use_rth,
            max_ticks=max_ticks,
        )
        response = await state.feed.load_historical_ticks(request)
        return response.model_dump(mode="json")
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def calculate_iv(
    ctx: Context,
    symbol: str,
    strike: float,
    expiry: str,
    option_price: float,
    under_price: float,
    right: str = "C",
    exchange: str = "SMART",
    currency: str = "USD",
    multiplier: str = "100",
) -> dict[str, Any]:
    """Calculate implied volatility using IBKR's option pricing engine.

    Args:
        symbol: Underlying symbol
        strike: Strike price
        expiry: Option expiry (e.g. '20260619')
        option_price: Observed option price
        under_price: Current underlying price
        right: 'C' for Call or 'P' for Put (default 'C')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        multiplier: Option multiplier (default '100')
    """
    state = _state(ctx)
    from src.feeds.contracts import ContractSpec
    from src.feeds.models import AssetClass

    try:
        spec = ContractSpec(
            symbol=symbol,
            asset_class=AssetClass.OPTION,
            exchange=exchange,
            currency=currency,
            option_sec_type="OPT",
            underlying_symbol=symbol,
            expiry=expiry,
            last_trade_date_or_contract_month=expiry,
            strike=strike,
            right=right,
            multiplier=multiplier,
        )
        contract = await state.feed.qualify_contract(spec)
        iv = await state.feed.calculate_iv(contract, option_price, under_price)
        return {
            "symbol": symbol,
            "strike": strike,
            "right": right,
            "expiry": expiry,
            "option_price": option_price,
            "under_price": under_price,
            "implied_volatility": iv,
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def calculate_option_price(
    ctx: Context,
    symbol: str,
    strike: float,
    expiry: str,
    implied_vol: float,
    under_price: float,
    right: str = "C",
    exchange: str = "SMART",
    currency: str = "USD",
    multiplier: str = "100",
) -> dict[str, Any]:
    """Calculate option price from implied volatility using IBKR's pricing engine.

    Args:
        symbol: Underlying symbol
        strike: Strike price
        expiry: Option expiry (e.g. '20260619')
        implied_vol: Implied volatility (e.g. 0.25 for 25%)
        under_price: Current underlying price
        right: 'C' for Call or 'P' for Put (default 'C')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        multiplier: Option multiplier (default '100')
    """
    state = _state(ctx)
    from src.feeds.contracts import ContractSpec
    from src.feeds.models import AssetClass

    try:
        spec = ContractSpec(
            symbol=symbol,
            asset_class=AssetClass.OPTION,
            exchange=exchange,
            currency=currency,
            option_sec_type="OPT",
            underlying_symbol=symbol,
            expiry=expiry,
            last_trade_date_or_contract_month=expiry,
            strike=strike,
            right=right,
            multiplier=multiplier,
        )
        contract = await state.feed.qualify_contract(spec)
        price = await state.feed.calculate_option_price(contract, implied_vol, under_price)
        return {
            "symbol": symbol,
            "strike": strike,
            "right": right,
            "expiry": expiry,
            "implied_vol": implied_vol,
            "under_price": under_price,
            "option_price": price,
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def get_head_timestamp(
    ctx: Context,
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
    what_to_show: str = "TRADES",
    use_rth: bool = True,
) -> dict[str, Any]:
    """Get earliest available data date for a symbol from IBKR.

    Args:
        symbol: Ticker symbol
        sec_type: Security type (default 'STK')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        what_to_show: Data type (default 'TRADES')
        use_rth: Regular trading hours only (default True)
    """
    state = _state(ctx)
    from src.feeds.tick_data import HeadTimestampRequest

    try:
        request = HeadTimestampRequest(
            symbol=symbol,
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )
        ts = await state.feed.load_head_timestamp(request)
        return {
            "symbol": symbol,
            "what_to_show": what_to_show,
            "head_timestamp": ts.isoformat() if ts else None,
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def scan_contracts(
    ctx: Context,
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
    max_results: int = 20,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Scan IBKR database for contracts matching criteria.

    Args:
        symbol: Symbol to scan for
        sec_type: Security type: STK, OPT, FUT, CASH, IND, BOND, CRYPTO (default 'STK')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        max_results: Max results to return (default 20)
    """
    state = _state(ctx)
    from src.feeds.scanner import ContractScanRequest

    try:
        request = ContractScanRequest(
            symbol=symbol,
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
            max_results=max_results,
        )
        results = await state.feed.scan_contracts(request)
        if not results:
            return {"message": "No matching contracts found"}
        return [r.model_dump(mode="json") for r in results]
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def get_index_composition(
    ctx: Context,
    index_symbol: str,
    max_results: int = 50,
    instrument: str | None = None,
    location_code: str | None = None,
    scan_code: str | None = None,
    filters: list[dict[str, Any]] | None = None,
    use_ttl_cache: bool = True,
    cache_ttl_seconds: float | None = 300,
) -> dict[str, Any]:
    """Load an IBKR scanner-backed approximation of index constituents.

    This is not official index membership and does not include index weights.
    For HSI the default scanner preset is STK / STK.HK / HOT_BY_VOLUME.

    Args:
        index_symbol: Index symbol to approximate, e.g. HSI
        max_results: Max scanner rows to return (IBKR caps scanner rows at 50)
        instrument: Optional scanner instrument override
        location_code: Optional scanner locationCode override
        scan_code: Optional scanner scanCode override
        filters: Optional IBKR scanner filters as [{"code": "...", "value": "..."}]
        use_ttl_cache: Use local API-process TTL cache
        cache_ttl_seconds: Cache TTL in seconds
    """
    state = _state(ctx)
    from src.feeds.index_composition import (
        IndexCompositionScannerRequest,
        build_index_composition_from_scanner_rows,
        resolve_index_composition_scanner_request,
    )
    from src.feeds.scanner import MarketScannerFilter

    try:
        composition_request = IndexCompositionScannerRequest(
            index_symbol=index_symbol,
            max_results=min(max_results, 50),
            instrument=instrument,
            location_code=location_code,
            scan_code=scan_code,
            filters=[MarketScannerFilter.model_validate(item) for item in (filters or [])],
        )
        scanner_request = resolve_index_composition_scanner_request(composition_request)

        async def load():
            rows = await state.feed.run_market_scanner(scanner_request)
            return build_index_composition_from_scanner_rows(composition_request, scanner_request, rows)

        if use_ttl_cache:
            from src.webapp.cache import stable_cache_key

            key = stable_cache_key("mcp_index_composition_scanner", composition_request)
            payload = await state.market_data_cache.get_or_set(key, load, ttl_seconds=cache_ttl_seconds)
        else:
            payload = await load()
        return payload.model_dump(mode="json")
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def load_bond_yield_history(
    ctx: Context,
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
    duration: str = "1 D",
    bar_size: str = "1 day",
    use_rth: bool = True,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Load historical bond yields from IBKR.

    Args:
        symbol: Bond symbol, CUSIP, or identifier (e.g. '91282CJN2')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        duration: IBKR duration string (default '1 D')
        bar_size: Bar size (default '1 day')
        use_rth: Regular trading hours only (default True)
    """
    state = _state(ctx)
    from src.feeds.bonds import BondInstrument, BondYieldHistoryRequest

    try:
        bond = BondInstrument(symbol=symbol, exchange=exchange, currency=currency)
        request = BondYieldHistoryRequest(
            bond=bond,
            duration=duration,
            bar_size=bar_size,
            use_rth=use_rth,
        )
        bars = await state.feed.load_bond_yield_history(request)
        return [b.model_dump(mode="json") for b in bars]
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def get_market_depth(
    ctx: Context,
    symbol: str,
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    num_rows: int = 5,
    sec_type: str = "STK",
    contract_month: str | None = None,
    strike: float | None = None,
    right: str | None = None,
) -> dict[str, Any]:
    """Load order book depth (Level 2 DOM snapshot) from IBKR.

    Args:
        symbol: Ticker symbol
        asset_class: EQUITY, FX, INDEX, or FUTURE (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        num_rows: Depth levels per side, 1-5 (default 5)
        sec_type: IBKR secType override (default 'STK')
        contract_month: Futures contract month (required for futures)
        strike: Strike price (for options)
        right: Option right 'C'/'P' (for options)
    """
    state = _state(ctx)
    from src.feeds.contracts import ContractSpec

    try:
        spec = ContractSpec(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
            last_trade_date_or_contract_month=contract_month,
            strike=strike,
            right=right,
        )
        snapshot = await state.feed.load_market_depth_snapshot(
            spec,
            num_rows=num_rows,
        )
        return snapshot.model_dump(mode="json")
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def get_ibkr_rate_limits(
    ctx: Context,
) -> dict[str, Any]:
    """Get IBKR rate limit / pacing controller status."""
    state = _state(ctx)
    try:
        feed = getattr(state, "feed", None)
        connection = getattr(feed, "_connection", None)
        snapshot_fn = getattr(connection, "rate_limit_snapshot", None) if connection is not None else None
        if callable(snapshot_fn):
            return await snapshot_fn()  # type: ignore[misc]
        return {"enabled": False, "reason": "not_configured"}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def get_scheduler_health(
    ctx: Context,
) -> dict[str, Any]:
    """Get scheduler health status for all tracked jobs."""
    state = _state(ctx)
    try:
        scheduler = getattr(state, "scheduler", None)
        if scheduler is None or getattr(scheduler, "_health_monitor", None) is None:
            return {"status": "not_configured", "jobs": {}}
        report = scheduler._health_monitor.get_health_status()
        return report.model_dump(mode="json")
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def get_cache_stats(
    ctx: Context,
) -> dict[str, Any]:
    """Get market data TTL cache statistics."""
    state = _state(ctx)
    try:
        stats = await state.market_data_cache.stats()
        return {"size": stats.size, "max_size": stats.max_size, "default_ttl_seconds": stats.default_ttl_seconds}
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# HISTOGRAM, REALTIME BARS & MARKET DATA TYPE
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def request_histogram(
    ctx: Context,
    symbol: str,
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    use_rth: bool = True,
    time_period: str = "1 day",
) -> dict[str, Any]:
    """Request price histogram data from IBKR.

    Returns price distribution for execution analysis and microstructure research.

    Args:
        symbol: Ticker symbol
        asset_class: EQUITY, FUTURE, FX, INDEX, BOND, OPTION (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        use_rth: Use regular trading hours only (default True)
        time_period: IBKR time period string (e.g. '1 day', '1 week')
    """
    state = _state(ctx)
    try:
        buckets_raw = await state.feed.request_histogram(
            symbol=symbol,
            asset_class=asset_class,
            exchange=exchange,
            currency=currency,
            use_rth=use_rth,
            time_period=time_period,
        )
        buckets = [
            {"price": b["price"], "count": b["count"]} if isinstance(b, dict) else b
            for b in buckets_raw
        ]
        return {
            "symbol": symbol,
            "asset_class": asset_class,
            "exchange": exchange,
            "currency": currency,
            "time_period": time_period,
            "use_rth": use_rth,
            "buckets": buckets,
            "total_count": len(buckets),
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def subscribe_realtime_bars(
    ctx: Context,
    symbol: str,
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    what_to_show: str = "TRADES",
    use_rth: bool = True,
    duration_seconds: float = 60.0,
) -> list[dict[str, Any]]:
    """Subscribe to 5-second real-time bars from IBKR.

    Returns collected 5-second bars over the specified duration.
    Use for intraday signal generation and execution quality analysis.

    Args:
        symbol: Ticker symbol
        asset_class: EQUITY, FUTURE, FX, INDEX, BOND, OPTION
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        what_to_show: TRADES, MIDPOINT, BID, ASK, BID_ASK (default 'TRADES')
        use_rth: Regular trading hours only (default True)
        duration_seconds: How long to collect bars in seconds (default 60)
    """
    state = _state(ctx)
    bars: list[dict[str, Any]] = []
    try:
        bar_stream = await state.feed.subscribe_realtime_bars(
            symbol=symbol,
            asset_class=asset_class,
            exchange=exchange,
            currency=currency,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )
        async with asyncio.timeout(duration_seconds):
            async for raw_bar in bar_stream:
                bar = {
                    "symbol": symbol,
                    "timestamp": raw_bar.get("time") if isinstance(raw_bar, dict) else getattr(raw_bar, "time", None),
                    "open": raw_bar.get("open", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "open", 0),
                    "high": raw_bar.get("high", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "high", 0),
                    "low": raw_bar.get("low", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "low", 0),
                    "close": raw_bar.get("close", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "close", 0),
                    "volume": raw_bar.get("volume", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "volume", 0),
                    "vwap": raw_bar.get("wap", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "wap", 0),
                    "trade_count": raw_bar.get("count", 0) if isinstance(raw_bar, dict) else getattr(raw_bar, "count", 0),
                }
                bars.append(bar)
        return bars
    except TimeoutError:
        return bars if bars else [{"message": f"collected {len(bars)} bars in {duration_seconds}s"}]
    except Exception as e:
        return [{"error": str(e)}]


@mcp.tool()
async def get_depth_exchanges(ctx: Context) -> list[dict[str, Any]]:
    """Get available Level 2 market depth exchanges from IBKR."""
    state = _state(ctx)
    try:
        return await state.feed.get_depth_exchanges()
    except Exception as e:
        return [{"error": str(e)}]


@mcp.tool()
async def set_market_data_type(
    ctx: Context,
    market_data_type: int = 1,
) -> dict[str, Any]:
    """Switch IBKR market data type.

    1=Live, 2=Frozen, 3=Delayed(15min), 4=DelayedFrozen.
    Essential for pre/post-market data and when live subscriptions are unavailable.

    Args:
        market_data_type: 1=Live, 2=Frozen, 3=Delayed, 4=DelayedFrozen (default 1)
    """
    state = _state(ctx)
    try:
        return await state.feed.set_market_data_type(market_data_type)
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# SERVER TIME, ORDER MANAGEMENT & OPTION EXERCISE
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def get_server_time(ctx: Context) -> dict[str, Any]:
    """Get IBKR server time. Useful for latency measurement and clock sync."""
    state = _state(ctx)
    try:
        return await state.feed.get_server_time()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def cancel_all_orders(ctx: Context) -> dict[str, Any]:
    """Cancel ALL open orders across all accounts. Use with extreme caution."""
    state = _state(ctx)
    try:
        return await state.feed.cancel_all_orders()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def get_all_open_orders(ctx: Context) -> list[dict[str, Any]]:
    """Get all open orders across ALL API clients and TWS (not just current session).

    Unlike load_open_orders which returns only current client's orders,
    this returns orders from all API sessions and manually-placed TWS orders.
    """
    state = _state(ctx)
    try:
        orders = await state.feed.get_all_open_orders()
        return [o.model_dump(mode="json") if hasattr(o, "model_dump") else o for o in orders]
    except Exception as e:
        return [{"error": str(e)}]


@mcp.tool()
async def exercise_option(
    ctx: Context,
    symbol: str,
    right: str,
    strike: float,
    expiry: str,
    exercise_action: int,
    quantity: int,
    account: str,
    exchange: str = "SMART",
    currency: str = "USD",
    override: bool = False,
) -> dict[str, Any]:
    """Exercise or lapse an option position.

    Args:
        symbol: Underlying symbol
        right: C/CALL or P/PUT
        strike: Strike price
        expiry: Expiration date YYYYMMDD
        exercise_action: 1=exercise, 2=lapse
        quantity: Number of contracts
        account: IBKR account ID
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        override: Override exercise restrictions (default False)
    """
    state = _state(ctx)
    try:
        return await state.feed.exercise_option(
            symbol=symbol,
            right=right,
            strike=strike,
            expiry=expiry,
            exercise_action=exercise_action,
            quantity=quantity,
            account=account,
            exchange=exchange,
            currency=currency,
            override=override,
        )
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def modify_order(
    ctx: Context,
    order_id: int,
    account: str,
    price: float | None = None,
    quantity: float | None = None,
    tif: str | None = None,
) -> dict[str, Any]:
    """Modify an existing live order. Only pass fields you want to change.

    Args:
        order_id: The IBKR order ID to modify
        account: IBKR account ID
        price: New limit price (optional)
        quantity: New order quantity (optional)
        tif: New time in force (optional, e.g. DAY, GTC, IOC)
    """
    from src.feeds.orders import ModifyOrderRequest, TIF as TIFEnum

    state = _state(ctx)
    try:
        tif_enum = TIFEnum(tif) if tif is not None else None
        modifications = ModifyOrderRequest(price=price, quantity=quantity, tif=tif_enum)
        result = await state.feed.modify_order(
            account_id=account,
            order_id=order_id,
            modifications=modifications,
        )
        return result.model_dump(mode="json") if hasattr(result, "model_dump") else result
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def preview_order(
    ctx: Context,
    symbol: str,
    action: str = "BUY",
    order_type: str = "LMT",
    quantity: float = 1.0,
    price: float | None = None,
    aux_price: float | None = None,
    tif: str = "DAY",
    account: str | None = None,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
    outside_rth: bool = False,
) -> dict[str, Any]:
    """Preview an order without submitting (whatIfOrder). Returns margin and commission impact.

    Args:
        symbol: Ticker symbol
        action: BUY or SELL (default 'BUY')
        order_type: Order type — LMT, MKT, STP, STP_LMT, etc (default 'LMT')
        quantity: Order quantity (default 1.0)
        price: Limit price (required for LMT orders)
        aux_price: Auxiliary/stop price (for stop orders)
        tif: Time in force — DAY, GTC, IOC (default 'DAY')
        account: IBKR account ID (optional)
        sec_type: Security type — STK, OPT, FUT, CASH (default 'STK')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        outside_rth: Allow outside regular trading hours (default False)
    """
    from src.feeds.orders import OrderAction, OrderType as OrderTypeEnum, PlaceOrderRequest, TIF as TIFEnum

    state = _state(ctx)
    try:
        request = PlaceOrderRequest(
            symbol=symbol,
            action=OrderAction(action.upper()),
            order_type=OrderTypeEnum(order_type.upper()),
            quantity=quantity,
            price=price,
            aux_price=aux_price,
            tif=TIFEnum(tif.upper()),
            account_id=account,
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
            outside_rth=outside_rth,
        )
        result = await state.feed.preview_order(request)
        return result.model_dump(mode="json") if hasattr(result, "model_dump") else result
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# VOLATILITY, YIELD & TRADING SCHEDULE
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def get_historical_volatility(
    ctx: Context,
    symbol: str,
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    bar_size: str = "1 day",
    duration: str = "1 Y",
    use_rth: bool = True,
) -> list[dict[str, Any]]:
    """Get historical volatility time series from IBKR.

    Uses IBKR's built-in HISTORICAL_VOLATILITY whatToShow — no manual computation needed.

    Args:
        symbol: Ticker symbol
        asset_class: EQUITY, FUTURE, INDEX, etc (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        bar_size: '1 min', '5 mins', '1 hour', '1 day' (default '1 day')
        duration: IBKR duration string (default '1 Y')
        use_rth: Regular trading hours only (default True)
    """
    state = _state(ctx)
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import OHLCVRequest, normalize_bar_size
    from src.transport.metrics import metrics

    try:
        request = OHLCVRequest(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
            bar_size=normalize_bar_size(bar_size),
            duration=duration,
            what_to_show="HISTORICAL_VOLATILITY",
            use_rth=use_rth,
        )
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            ensure_historical_chunk_limit(request, auto_chunk_plan, max_chunks=state.settings.ibkr_historical_max_chunks)
            bars = await state.feed.load_historical_ohlcv_range(
                request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime}),
                start_datetime=auto_chunk_plan.start_datetime,
                end_datetime=auto_chunk_plan.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        else:
            bars = await state.feed.load_historical_ohlcv(request, max_chunks=state.settings.ibkr_historical_max_chunks)
        return [
            {
                "timestamp": bar.timestamp.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
            for bar in bars
        ]
    except Exception as e:
        return [{"error": str(e)}]


@mcp.tool()
async def get_option_implied_volatility_series(
    ctx: Context,
    symbol: str,
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    bar_size: str = "1 day",
    duration: str = "6 M",
    use_rth: bool = True,
) -> list[dict[str, Any]]:
    """Get option implied volatility time series from IBKR.

    Uses IBKR's built-in OPTION_IMPLIED_VOLATILITY whatToShow for the underlying.

    Args:
        symbol: Underlying ticker symbol
        asset_class: EQUITY, FUTURE, INDEX (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        bar_size: Bar size (default '1 day')
        duration: IBKR duration string (default '6 M')
        use_rth: Regular trading hours only (default True)
    """
    state = _state(ctx)
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import OHLCVRequest, normalize_bar_size
    from src.transport.metrics import metrics

    try:
        request = OHLCVRequest(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
            bar_size=normalize_bar_size(bar_size),
            duration=duration,
            what_to_show="OPTION_IMPLIED_VOLATILITY",
            use_rth=use_rth,
        )
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            ensure_historical_chunk_limit(request, auto_chunk_plan, max_chunks=state.settings.ibkr_historical_max_chunks)
            bars = await state.feed.load_historical_ohlcv_range(
                request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime}),
                start_datetime=auto_chunk_plan.start_datetime,
                end_datetime=auto_chunk_plan.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        else:
            bars = await state.feed.load_historical_ohlcv(request, max_chunks=state.settings.ibkr_historical_max_chunks)
        return [
            {
                "timestamp": bar.timestamp.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
            for bar in bars
        ]
    except Exception as e:
        return [{"error": str(e)}]


@mcp.tool()
async def get_yield_data(
    ctx: Context,
    symbol: str,
    what_to_show: str = "YIELD_LAST",
    bar_size: str = "1 day",
    duration: str = "1 Y",
    exchange: str = "SMART",
    currency: str = "USD",
    use_rth: bool = True,
) -> list[dict[str, Any]]:
    """Get bond yield time series from IBKR.

    Uses IBKR yield-specific whatToShow values.

    Args:
        symbol: Bond symbol or CUSIP
        what_to_show: YIELD_ASK, YIELD_BID, YIELD_BID_ASK, YIELD_LAST (default 'YIELD_LAST')
        bar_size: Bar size (default '1 day')
        duration: Duration string (default '1 Y')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        use_rth: Regular trading hours (default True)
    """
    state = _state(ctx)
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import OHLCVRequest, normalize_bar_size
    from src.transport.metrics import metrics

    try:
        request = OHLCVRequest(
            symbol=symbol,
            asset_class=_parse_asset_class("BOND"),
            exchange=exchange,
            currency=currency,
            bar_size=normalize_bar_size(bar_size),
            duration=duration,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            ensure_historical_chunk_limit(request, auto_chunk_plan, max_chunks=state.settings.ibkr_historical_max_chunks)
            bars = await state.feed.load_historical_ohlcv_range(
                request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime}),
                start_datetime=auto_chunk_plan.start_datetime,
                end_datetime=auto_chunk_plan.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        else:
            bars = await state.feed.load_historical_ohlcv(request, max_chunks=state.settings.ibkr_historical_max_chunks)
        return [
            {
                "timestamp": bar.timestamp.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
            for bar in bars
        ]
    except Exception as e:
        return [{"error": str(e)}]


@mcp.tool()
async def get_trading_schedule(
    ctx: Context,
    symbol: str,
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    end_date: str = "",
    num_days: int = 7,
) -> list[dict[str, Any]]:
    """Get trading schedule for any instrument from IBKR.

    Returns session open/close times, overnight flags, and trading status.

    Args:
        symbol: Ticker symbol
        asset_class: Asset class (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        end_date: End date YYYYMMDD (default today)
        num_days: Number of days to return (default 7)
    """
    state = _state(ctx)
    from src.feeds.models import OHLCVRequest
    from datetime import date as date_type

    try:
        ref_date = date_type.today()
        if end_date:
            ref_date = date_type.fromisoformat(end_date)

        request = OHLCVRequest(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
            bar_size="1 day",
            duration=f"{num_days} D",
            what_to_show="SCHEDULE",
            use_rth=True,
        )
        schedule = await state.feed.load_trading_schedule(request, ref_date=ref_date, use_rth=True)
        if isinstance(schedule, (list, tuple)):
            return [
                item.model_dump(mode="json") if hasattr(item, "model_dump") else item
                for item in schedule
            ]
        return [schedule.model_dump(mode="json")] if hasattr(schedule, "model_dump") else [schedule]
    except Exception as e:
        return [{"error": str(e)}]


# ═══════════════════════════════════════════════════════════════════════════════
# BRACKET ORDERS, OCA GROUPS & MARKET SCANNER
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def place_bracket_order(
    ctx: Context,
    symbol: str,
    action: str = "BUY",
    quantity: float = 1.0,
    limit_price: float | None = None,
    take_profit_price: float | None = None,
    stop_loss_price: float | None = None,
    order_type: str = "LMT",
    tif: str = "GTC",
    asset_class: str = "EQUITY",
    exchange: str = "SMART",
    currency: str = "USD",
    account: str = "",
) -> dict[str, Any]:
    """Place a bracket order (parent + take-profit + stop-loss).

    Creates three linked orders: a parent entry order, an attached take-profit,
    and an attached stop-loss. When one of the exit orders fills, the other is cancelled.

    Args:
        symbol: Ticker symbol
        action: BUY or SELL (default 'BUY')
        quantity: Order quantity (default 1.0)
        limit_price: Limit price for entry order (required for LMT orders)
        take_profit_price: Take-profit limit price
        stop_loss_price: Stop-loss trigger price
        order_type: MKT or LMT (default 'LMT')
        tif: Time in force: DAY, GTC, IOC, GTD (default 'GTC')
        asset_class: EQUITY, FUTURE, OPTION (default 'EQUITY')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        account: IBKR account ID
    """
    state = _state(ctx)
    try:
        ib = state.feed._connection.ib
        if ib is None:
            return {"error": "IBKR connection not available"}

        # Build the contract
        ac = _parse_asset_class(asset_class)
        sec_type_map = {"EQUITY": "STK", "FUTURE": "FUT", "OPTION": "OPT", "FX": "CASH", "INDEX": "IND", "BOND": "BOND"}
        sec_type = sec_type_map.get(ac.value if hasattr(ac, "value") else str(ac), "STK")

        from ib_insync import Contract
        contract = Contract(symbol=symbol, secType=sec_type, exchange=exchange, currency=currency)
        qualified = await ib.qualifyContractsAsync(contract)
        if not qualified:
            return {"error": f"Could not qualify contract for {symbol}"}
        contract = qualified[0]

        # Build bracket order
        bracket = ib.bracketOrder(
            action=action.upper(),
            quantity=quantity,
            limitPrice=limit_price or 0.0,
            takeProfitPrice=take_profit_price or 0.0,
            stopLossPrice=stop_loss_price or 0.0,
        )

        # Override order type and TIF on parent
        bracket.orders[0].orderType = order_type.upper()
        bracket.orders[0].tif = tif

        # Set account on all orders
        if account:
            for o in bracket.orders:
                o.account = account

        # Place all orders
        trades = []
        for o in bracket.orders:
            trade = ib.placeOrder(contract, o)
            trades.append(trade)

        return {
            "symbol": symbol,
            "action": action,
            "quantity": quantity,
            "orders_placed": len(trades),
            "parent_order_id": bracket.orders[0].orderId if bracket.orders else None,
            "take_profit_order_id": bracket.orders[1].orderId if len(bracket.orders) > 1 else None,
            "stop_loss_order_id": bracket.orders[2].orderId if len(bracket.orders) > 2 else None,
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
async def place_oca_group(
    ctx: Context,
    orders: list[dict[str, Any]],
    oca_group: str = "",
    oca_type: int = 1,
    account: str = "",
) -> list[dict[str, Any]]:
    """Place a One-Cancels-All group of orders.

    When any order in the group fills, all others are automatically cancelled.

    Args:
        orders: List of order dicts, each with: symbol, action, quantity, order_type, price?, asset_class?, exchange?, currency?
        oca_group: OCA group name (auto-generated if empty)
        oca_type: 1=CancelAll, 2=ReduceRemaining, 3=ReducePosition (default 1)
        account: IBKR account ID
    """
    state = _state(ctx)
    try:
        import uuid
        from ib_insync import Contract, Order

        ib = state.feed._connection.ib
        if ib is None:
            return [{"error": "IBKR connection not available"}]

        group_name = oca_group or f"oca_{uuid.uuid4().hex[:8]}"
        sec_type_map = {"EQUITY": "STK", "FUTURE": "FUT", "OPTION": "OPT", "FX": "CASH", "INDEX": "IND", "BOND": "BOND"}
        results = []

        for i, od in enumerate(orders):
            sym = od.get("symbol", "")
            act = od.get("action", "BUY").upper()
            qty = od.get("quantity", 1)
            ot = od.get("order_type", "LMT").upper()
            price = od.get("price")
            ac_str = od.get("asset_class", "EQUITY")
            exch = od.get("exchange", "SMART")
            curr = od.get("currency", "USD")

            ac = _parse_asset_class(ac_str)
            sec_type = sec_type_map.get(ac.value if hasattr(ac, "value") else str(ac), "STK")

            contract = Contract(symbol=sym, secType=sec_type, exchange=exch, currency=curr)
            qualified = await ib.qualifyContractsAsync(contract)
            if not qualified:
                results.append({"error": f"Could not qualify contract for {sym}", "symbol": sym})
                continue
            contract = qualified[0]

            order = Order()
            order.action = act
            order.quantity = qty
            order.orderType = ot
            order.tif = "GTC"
            order.ocaGroup = group_name
            order.ocaType = oca_type
            if price is not None:
                order.lmtPrice = price
            if account:
                order.account = account

            trade = ib.placeOrder(contract, order)
            results.append({
                "symbol": sym,
                "action": act,
                "quantity": qty,
                "order_id": trade.order.orderId,
                "oca_group": group_name,
            })

        return results
    except Exception as e:
        return [{"error": str(e)}]


@mcp.tool()
async def scan_market(
    ctx: Context,
    instrument: str = "STK",
    location: str = "STK.US",
    scan_code: str = "TOP_PERC_GAIN",
    above_price: float | None = None,
    below_price: float | None = None,
    above_volume: int | None = None,
    market_cap_above: float | None = None,
    market_cap_below: float | None = None,
    max_results: int = 50,
) -> list[dict[str, Any]]:
    """Run an IBKR market scanner with customizable filters.

    Uses IBKR's built-in scanner to find instruments matching criteria.

    Args:
        instrument: Instrument type: STK, FUT, OPT, etc (default 'STK')
        location: Scanner location: STK.US, STK.EU, FUT.US, etc (default 'STK.US')
        scan_code: Scanner code: TOP_PERC_GAIN, TOP_PERC_LOSS, MOST_ACTIVE, HIGH_VOLATILITY, etc
        above_price: Minimum price filter
        below_price: Maximum price filter
        above_volume: Minimum volume filter
        market_cap_above: Minimum market cap
        market_cap_below: Maximum market cap
        max_results: Maximum results (default 50)
    """
    state = _state(ctx)
    try:
        return await state.feed.scan_market(
            instrument=instrument,
            location=location,
            scan_code=scan_code,
            above_price=above_price,
            below_price=below_price,
            above_volume=above_volume,
            market_cap_above=market_cap_above,
            market_cap_below=market_cap_below,
            max_results=max_results,
        )
    except Exception as e:
        return [{"error": str(e)}]


@mcp.tool()
async def get_scanner_parameters(ctx: Context) -> dict[str, Any]:
    """Get available IBKR scanner parameters (instruments, filters, locations).

    Returns an XML document describing all valid scanner parameter values.
    Required before using scan_market to know valid filter values.
    """
    state = _state(ctx)
    try:
        return await state.feed.get_scanner_parameters()
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# NEWS BULLETINS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def get_news_bulletins(
    ctx: Context,
    all_messages: bool = True,
) -> list[dict[str, Any]]:
    """Get IBKR system news bulletins (exchange halts, margin changes, etc).

    Args:
        all_messages: Return all historical bulletins (default True)
    """
    state = _state(ctx)
    try:
        return await state.feed.get_news_bulletins(all_messages=all_messages)
    except Exception as e:
        return [{"error": str(e)}]


# ═══════════════════════════════════════════════════════════════════════════════
# RESOURCES — Static reference data
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.resource("ibpython://status")
def get_server_status() -> str:
    """IBPython MCP server status and available capabilities."""
    return json.dumps({
        "server": "IBPython Market Data MCP",
        "version": constants.APP_VERSION,
        "tools": [
            "query_historical_ohlcv — QuestDB historical OHLCV bars",
            "query_latest_bars — Latest bar per symbol from QuestDB",
            "query_raw_sql — Raw QuestDB SQL query (SELECT only)",
            "get_latest_bar_from_cache — Redis-cached latest bars",
            "list_scheduler_jobs — Registered OHLCV scheduler jobs",
            "get_redis_key — Read arbitrary Redis key",
            "load_option_chains — IBKR option chain discovery",
            "load_account_summary — IBKR account summary",
            "load_live_positions — IBKR live positions",
            "load_historical_ohlcv_live — Direct IBKR historical bars",
            "search_contracts — IBKR contract database search",
            "load_fundamentals — IBKR fundamental data",
            "load_news — IBKR news feed",
            "query_equity_snapshots — QuestDB equity snapshots",
            "query_fx_option_snapshots — QuestDB FX option snapshots",
            "load_portfolio_items — IBKR portfolio items with market data",
            "load_account_pnl — IBKR account-level PnL snapshot",
            "load_position_pnl — IBKR position-level PnL snapshot",
            "load_news_providers — IBKR available news providers",
            "load_news_article — IBKR full news article text",
            "load_wsh_metadata — Wall Street Horizon metadata",
            "load_wsh_event_data — Wall Street Horizon event data",
            "load_economic_calendar — Wall Street Horizon economic calendar",
            "get_latest_equity_snapshots — Redis-cached latest equity snapshots",
            "get_latest_fx_option_snapshot — Redis-cached latest FX option snapshot",
            "get_all_latest_equity_snapshots — QuestDB latest snapshot per symbol",
            "list_snapshot_watchlists — List snapshot watchlist names",
            "get_snapshot_watchlist — Get a snapshot watchlist by name",
            "load_option_analytics — Real-time option greeks/analytics",
            "load_option_skew_surface — Option skew surface across expirations",
            "get_latest_ticks — Latest N ticks from in-memory buffer",
            "load_historical_ticks — Historical tick data from IBKR",
            "calculate_iv — Calculate implied volatility via IBKR",
            "calculate_option_price — Calculate option price from IV via IBKR",
            "get_head_timestamp — Earliest available data date for symbol",
            "scan_contracts — Scan IBKR database for matching contracts",
            "get_index_composition — IBKR scanner-backed index constituent approximation",
            "load_bond_yield_history — Historical bond yields from IBKR",
            "get_market_depth — Order book depth (Level 2 DOM) snapshot",
            "get_ibkr_rate_limits — IBKR rate limit/pacing status",
            "get_scheduler_health — Scheduler health status for all jobs",
            "get_cache_stats — Market data TTL cache statistics",
            "request_histogram — Price histogram data for execution analysis",
            "subscribe_realtime_bars — 5-second real-time bars from IBKR",
            "get_depth_exchanges — Level 2 market depth exchanges",
            "set_market_data_type — Switch IBKR market data type (live/frozen/delayed)",
            "get_server_time — IBKR server time for latency/clock sync",
            "cancel_all_orders — Cancel ALL open orders globally",
            "get_all_open_orders — All open orders across all API clients and TWS",
            "exercise_option — Exercise or lapse an option position",
            "modify_order — Modify an existing live order",
            "preview_order — Preview order margin/commission impact (what-if)",
            "get_historical_volatility — Historical volatility time series",
            "get_option_implied_volatility_series — Option implied volatility time series",
            "get_yield_data — Bond yield time series",
            "get_trading_schedule — Trading schedule for any instrument",
            "place_bracket_order — Bracket order (entry + take-profit + stop-loss)",
            "place_oca_group — One-Cancels-All group of orders",
            "scan_market — IBKR market scanner with customizable filters",
            "get_scanner_parameters — IBKR scanner parameter options",
            "get_news_bulletins — IBKR system news bulletins",
        ],
        "databases": {
            "questdb": "Time-series OHLCV, snapshots, tick data",
            "redis": "Latest bars cache, scheduler state, pacing bookmarks",
            "ibkr": "Live TWS/Gateway connection for real-time data",
        },
    }, indent=2)


@mcp.resource("ibpython://schema/tables")
def get_table_schema() -> str:
    """Available QuestDB tables and their structure."""
    return json.dumps({
        "EquityOHLCV": {
            "columns": ["symbol", "asset_class", "exchange", "currency", "timestamp",
                        "open", "high", "low", "close", "volume", "bar_size", "source",
                        "contract_key", "con_id", "local_symbol", "contract_month", "expiry",
                        "strike", "right", "trading_class", "what_to_show", "use_rth", "metadata"],
            "partition": "DAY",
            "indexed": ["symbol", "asset_class", "exchange", "bar_size", "contract_key"],
        },
        "equity_snapshots": {
            "description": "Real-time equity snapshots from IBKR scanner",
            "key_fields": ["symbol", "exchange", "timestamp"],
        },
        "fx_option_snapshots": {
            "description": "FX option volatility surface snapshots",
            "key_fields": ["currency", "timestamp", "tenor", "delta"],
        },
    }, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINTS
# ═══════════════════════════════════════════════════════════════════════════════

def main_stdio() -> None:
    """Run MCP server over stdio (for Claude Desktop, Cursor, etc)."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    mcp.run(transport="stdio")


class MCPBearerAuthMiddleware:
    """Pure ASGI middleware for MCP HTTP bearer token auth.

    When ``api_key`` is empty (default for stdio transport), auth is skipped.
    When set, all HTTP requests must include ``Authorization: Bearer <token>``.
    """

    def __init__(self, app: Any, *, api_key: str) -> None:
        self.app = app
        self._api_key = api_key

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] != "http" or not self._api_key:
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        auth_header = headers.get(b"authorization", b"")
        if isinstance(auth_header, bytes):
            auth_header = auth_header.decode("latin-1")

        if not auth_header.lower().startswith("bearer "):
            await self._send_401(send, "MCP API key required")
            return

        token = auth_header[7:].strip()
        if not token or not secrets.compare_digest(token, self._api_key.strip()):
            await self._send_401(send, "invalid MCP API key")
            return

        await self.app(scope, receive, send)

    @staticmethod
    async def _send_401(send: Callable[[dict[str, Any]], Awaitable[None]], detail: str) -> None:
        body = json.dumps({"detail": detail}).encode("utf-8")
        await send({"type": "http.response.start", "status": 401, "headers": [
            [b"content-type", b"application/json"],
            [b"www-authenticate", b"Bearer"],
            [b"content-length", str(len(body)).encode()],
        ]})
        await send({"type": "http.response.body", "body": body})


def _get_mcp_http_config() -> tuple[str, int, str]:
    """Read MCP HTTP transport config from environment variables."""
    host = os.environ.get(constants.MCP_HTTP_HOST_ENV, constants.DEFAULT_MCP_HTTP_HOST)
    port = int(os.environ.get(constants.MCP_HTTP_PORT_ENV, str(constants.DEFAULT_MCP_HTTP_PORT)))
    api_key = os.environ.get(constants.MCP_API_KEY_ENV, constants.DEFAULT_MCP_API_KEY)
    return host, port, api_key


def main_streamable_http() -> None:
    """Run MCP server over Streamable HTTP (for remote access).

    Binds to ``MCP_HTTP_HOST`` (default ``127.0.0.1``) on ``MCP_HTTP_PORT``
    (default ``9000``).  When ``MCP_API_KEY`` is set, all requests must
    include ``Authorization: Bearer <key>``.
    """
    import uvicorn
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    host, port, api_key = _get_mcp_http_config()

    app = mcp.streamable_http_app()
    if api_key:
        app = MCPBearerAuthMiddleware(app, api_key=api_key)
        logger.info("MCP HTTP: auth enabled, binding to %s:%s", host, port)
    else:
        logger.warning("MCP HTTP: auth disabled (MCP_API_KEY not set), binding to %s:%s", host, port)

    uvicorn.run(app, host=host, port=port)


def main(argv: list[str] | None = None) -> None:
    """Run stdio by default, or Streamable HTTP when ``--http`` is passed."""
    argv = sys.argv[1:] if argv is None else argv
    if "--http" in argv:
        main_streamable_http()
    else:
        main_stdio()


if __name__ == "__main__":
    main()
