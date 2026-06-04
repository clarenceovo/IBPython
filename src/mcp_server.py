"""
MCP (Model Context Protocol) server for IBPython.

Exposes IBKR market data, QuestDB historical queries, Redis cache,
scheduler status, and live IBKR feed as MCP tools and resources
that any MCP-compatible AI agent can query.

Run:
    python -m src.mcp_server
    # or with Streamable HTTP (binds 127.0.0.1:9000 by default):
    MCP_HTTP_HOST=0.0.0.0 MCP_HTTP_PORT=9000 MCP_API_KEY=secret python -m src.mcp_server
    # or with uvicorn:
    uvicorn src.mcp_server:mcp.streamable_http_app --host 127.0.0.1 --port 9000
"""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Awaitable

from mcp.server.fastmcp import Context, FastMCP

from src.config import config_constant as constants
from src.config.settings import Settings, load_settings
from src.webapp.dependencies import IBKRRestAppState, build_rest_app_state

logger = logging.getLogger(__name__)


# ── Lifespan: manage connections for the MCP server ──────────────────────────

@asynccontextmanager
async def mcp_lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:
    """Connect to Redis, QuestDB, and IBKR on startup; tear down on shutdown."""
    settings = load_settings()
    state = build_rest_app_state(settings)

    try:
        await state.redis.connect()
        logger.info("MCP server: Redis connected")
    except Exception:
        logger.exception("MCP server: Redis connection failed")

    try:
        await state.questdb.connect()
        logger.info("MCP server: QuestDB connected")
    except Exception:
        logger.exception("MCP server: QuestDB connection failed")

    try:
        await state.feed.connect()
        logger.info("MCP server: IBKR feed connected")
    except Exception:
        logger.exception("MCP server: IBKR feed connection failed (tools requiring live IBKR will be unavailable)")

    yield {"ibkr_state": state}

    await state.close()
    logger.info("MCP server: all connections closed")


mcp = FastMCP(
    "IBPython Market Data",
    version="0.1.0",
    lifespan=mcp_lifespan,
)


def _state(ctx: Context) -> IBKRRestAppState:
    return ctx.request_context.lifespan_context["ibkr_state"]


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
) -> list[dict[str, Any]]:
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
    state = _state(ctx)
    limit = min(limit, 50_000)
    start_dt = datetime.fromisoformat(start) if start else None
    end_dt = datetime.fromisoformat(end) if end else None

    return await state.questdb.query_historical_bars(
        symbol=symbol,
        asset_class=asset_class,
        bar_size=bar_size,
        contract_key=contract_key,
        start=start_dt,
        end=end_dt,
        limit=limit,
    )


@mcp.tool()
async def query_latest_bars(
    ctx: Context,
    asset_class: str | None = None,
    bar_size: str | None = None,
    contract_key: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Query latest OHLCV bars across all symbols from QuestDB.

    Uses QuestDB LATEST ON to get the most recent bar per symbol+contract_key.

    Args:
        asset_class: Filter by asset class
        bar_size: Filter by bar size
        contract_key: Filter by contract key
        limit: Max rows (default 100)
    """
    state = _state(ctx)
    return await state.questdb.query_latest_bars(
        asset_class=asset_class,
        bar_size=bar_size,
        contract_key=contract_key,
        limit=limit,
    )


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

    # Block dangerous keywords anywhere in the query
    dangerous_keywords = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|EXECUTE)\b",
        re.IGNORECASE,
    )
    match = dangerous_keywords.search(normalized)
    if match:
        return {"error": f"Keyword '{match.group(1).upper()}' is not permitted in queries"}

    try:
        return await state.questdb._fetch_dicts(stripped, [])
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
    from src.feeds.models import AssetClass
    bar = await state.redis.get_latest_bar(
        asset_class=AssetClass(asset_class.upper()),
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


@mcp.tool()
async def get_redis_key(
    ctx: Context,
    key: str,
) -> dict[str, Any]:
    """Read a raw Redis key. For debugging and inspection.

    Args:
        key: Full Redis key to read
    """
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
    from src.feeds.models import AssetClass

    request = OptionChainRequest(
        symbol=symbol,
        asset_class=AssetClass(asset_class.upper()),
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
    rows = await state.feed.load_account_summary(account=account)
    return {row.tag: {"value": row.value, "currency": row.currency} for row in rows}


@mcp.tool()
async def load_live_positions(
    ctx: Context,
) -> list[dict[str, Any]]:
    """Load all live positions from IBKR."""
    state = _state(ctx)
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
) -> list[dict[str, Any]]:
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
    from src.feeds.models import AssetClass, OHLCVRequest
    from src.transport.metrics import metrics

    request = OHLCVRequest(
        symbol=symbol,
        asset_class=AssetClass(asset_class.upper()),
        exchange=exchange,
        currency=currency,
        bar_size=bar_size,
        duration=duration,
        what_to_show=what_to_show,
        use_rth=use_rth,
    )
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
    results = await state.feed.search_matching_symbols(pattern)
    if not results:
        return {"message": "No matching contracts found"}
    return results


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
) -> dict[str, Any] | list[dict[str, Any]]:
    """Load recent news headlines for a symbol from IBKR.

    Args:
        symbol: Ticker symbol (e.g. 'AAPL', 'BTC')
        provider_codes: News provider codes (default ['BRFG', 'BRFUPDN'])
        limit: Max articles to return (default 20, max 300)
    """
    state = _state(ctx)
    from src.feeds.news import HistoricalNewsRequest

    # First resolve the contract to get con_id
    try:
        from src.feeds.contracts import ContractSpec
        from src.feeds.models import AssetClass
        spec = ContractSpec(symbol=symbol, asset_class=AssetClass.EQUITY, exchange="SMART", currency="USD")
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
    return await state.questdb.query_snapshots(symbol=symbol, start=start_dt, end=end_dt, limit=limit)


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
    return await state.questdb.query_fx_option_snapshots(
        symbol=symbol, expiry=expiry, strike=strike, right=right,
        start=start_dt, end=end_dt, limit=limit,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# RESOURCES — Static reference data
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.resource("ibpython://status")
def get_server_status() -> str:
    """IBPython MCP server status and available capabilities."""
    return json.dumps({
        "server": "IBPython Market Data MCP",
        "version": "0.1.0",
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


if __name__ == "__main__":
    main_stdio()
