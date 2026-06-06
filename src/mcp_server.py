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
        await state.loader.questdb.connect()
        questdb_connected = True
        logger.info("MCP server: QuestDB connected")
    except Exception:
        logger.exception("MCP server: QuestDB connection failed")

    try:
        await state.feed.connect()
        ibkr_connected = True
        logger.info("MCP server: IBKR feed connected")
    except Exception:
        logger.exception("MCP server: IBKR feed connection failed (tools requiring live IBKR will be unavailable)")

    yield {
        "ibkr_state": state,
        "redis_connected": redis_connected,
        "questdb_connected": questdb_connected,
        "ibkr_connected": ibkr_connected,
    }

    await state.close()
    logger.info("MCP server: all connections closed")


mcp = FastMCP(
    "IBPython Market Data",
    version=constants.APP_VERSION,
    lifespan=mcp_lifespan,
)


def _state(ctx: Context) -> IBKRRestAppState:
    return ctx.request_context.lifespan_context["ibkr_state"]


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
    if err := _require_questdb(ctx):
        return [err]  # type: ignore[list-item]
    state = _state(ctx)
    limit = min(limit, 50_000)
    start_dt = datetime.fromisoformat(start) if start else None
    end_dt = datetime.fromisoformat(end) if end else None

    return await state.loader.questdb.query_historical_bars(
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
    try:
        return await state.loader.questdb.query_latest_bars(
            asset_class=asset_class,
            bar_size=bar_size,
            contract_key=contract_key,
            limit=limit,
        )
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
    from src.feeds.models import AssetClass

    try:
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
        from src.feeds.models import AssetClass
        spec = ContractSpec(symbol=symbol, asset_class=AssetClass(asset_class.upper()), exchange=exchange, currency=currency)
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
    from src.feeds.models import AssetClass
    from src.feeds.options_models import OptionSkewSurfaceRequest

    try:
        chain_request = OptionChainRequest(
            symbol=symbol,
            asset_class=AssetClass(asset_class.upper()),
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
    from src.feeds.models import AssetClass

    try:
        spec = ContractSpec(
            symbol=symbol,
            asset_class=AssetClass(asset_class.upper()),
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
            "load_bond_yield_history — Historical bond yields from IBKR",
            "get_market_depth — Order book depth (Level 2 DOM) snapshot",
            "get_ibkr_rate_limits — IBKR rate limit/pacing status",
            "get_scheduler_health — Scheduler health status for all jobs",
            "get_cache_stats — Market data TTL cache statistics",
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
