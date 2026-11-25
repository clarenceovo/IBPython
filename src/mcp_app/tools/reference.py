"""Reference-data MCP tools: news, bulletins, WSH events, economic calendar, shortability, dividends."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import Context, FastMCP

from src.mcp_app.common import (
    _AssetClassParam,
    _READ_TOOL,
    _error,
    _parse_asset_class,
    _state,
    _tool_error,
)


async def load_fundamentals(
    ctx: Context,
    symbol: str,
    exchange: str = "SMART",
    report_type: str = "ReportSnapshot",
) -> dict[str, Any]:
    """Unsupported legacy tool: IBKR removed fundamental reports in API 10.47.

    Always returns an explicit FEATURE_REMOVED error. Does not connect to IBKR.
    Use load_wsh_event_data for event calendars instead.
    """
    return _error(
        "fundamental reports were removed in TWS API 10.47; "
        "use load_wsh_event_data (WSH events) or scan_market instead",
        code="FEATURE_REMOVED",
    )


async def load_news(
    ctx: Context,
    symbol: str,
    provider_codes: list[str] | None = None,
    limit: int = 20,
    asset_class: _AssetClassParam = "EQUITY",
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
    from src.feeds.contracts import ContractSpec
    from src.feeds.news import HistoricalNewsRequest

    try:
        spec = ContractSpec(
            symbol=symbol,
            asset_class=_parse_asset_class(asset_class),
            exchange=exchange,
            currency=currency,
        )
        contract = await state.feed.qualify_contract(spec)
        con_id = contract.conId
    except Exception:
        return _error(f"Could not resolve contract for {symbol}", code="CONTRACT_RESOLUTION_FAILED")

    try:
        providers = tuple(provider_codes or ["BRFG", "BRFUPDN"])
        request = HistoricalNewsRequest(
            con_id=con_id,
            provider_codes=providers,
            total_results=min(limit, 300),
        )
        articles = await state.feed.load_historical_news(request)
        return [a.model_dump() for a in articles]
    except Exception as e:
        return _tool_error(e)


async def load_news_providers(ctx: Context) -> dict[str, Any] | list[dict[str, Any]]:
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
        return _tool_error(e)


async def load_news_article(
    ctx: Context,
    provider_code: str,
    article_id: str,
) -> dict[str, Any]:
    """Load the full text of a news article from IBKR.

    Use load_news first to get headlines and article IDs, then call this
    tool to retrieve the full article body. Bodies longer than 20,000
    characters are truncated; 'truncated' is true then.

    Args:
        provider_code: News provider code (e.g. 'BRFG', 'BRFUPDN')
        article_id: Article ID from a news headline
    """
    state = _state(ctx)
    from src.feeds.news import NewsArticleRequest

    try:
        request = NewsArticleRequest(provider_code=provider_code, article_id=article_id)
        article = await state.feed.load_news_article(request)
        text = article.article_text
        return {
            "provider_code": article.provider_code,
            "article_id": article.article_id,
            "article_type": article.article_type,
            "article_text": text[:20_000],
            "truncated": len(text) > 20_000,
            "received_at": article.received_at.isoformat(),
        }
    except Exception as e:
        return _tool_error(e)


async def get_news_bulletins(
    ctx: Context,
    all_messages: bool = True,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Get IBKR system news bulletins (exchange halts, margin changes, etc).

    Args:
        all_messages: Return all historical bulletins (default True)
    """
    state = _state(ctx)
    try:
        return await state.feed.get_news_bulletins(all_messages=all_messages)
    except Exception as e:
        return _tool_error(e)


async def load_wsh_metadata(ctx: Context) -> dict[str, Any]:
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
        return _tool_error(e)


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
        return _tool_error(e)


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
    from datetime import date as date_type
    from src.feeds.fundamental_data import WSHEventDataRequest

    try:
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
        return _tool_error(e)


async def load_equity_shortability(
    ctx: Context,
    symbol: str,
    exchange: str | None = None,
    currency: str | None = None,
    primary_exchange: str | None = None,
    con_id: int | None = None,
    timeout_seconds: float = 10,
) -> dict[str, Any]:
    """Get indicative shortable shares with a bounded subscription; null means unavailable, not zero."""
    from src.feeds.equity_reference import EquityReferenceRequest

    try:
        request = EquityReferenceRequest(
            symbol=symbol,
            exchange=exchange,
            currency=currency,
            primary_exchange=primary_exchange,
            con_id=con_id,
            timeout_seconds=timeout_seconds,
        )
        return (await _state(ctx).feed.load_equity_shortability(request)).model_dump(mode="json")
    except Exception as e:
        return _tool_error(e)


async def load_equity_dividends(
    ctx: Context,
    symbol: str,
    exchange: str | None = None,
    currency: str | None = None,
    primary_exchange: str | None = None,
    con_id: int | None = None,
    timeout_seconds: float = 10,
) -> dict[str, Any]:
    """Get past/next twelve-month dividends and the next date/amount; partial data is explicit."""
    from src.feeds.equity_reference import EquityReferenceRequest

    try:
        request = EquityReferenceRequest(
            symbol=symbol,
            exchange=exchange,
            currency=currency,
            primary_exchange=primary_exchange,
            con_id=con_id,
            timeout_seconds=timeout_seconds,
        )
        return (await _state(ctx).feed.load_equity_dividends(request)).model_dump(mode="json")
    except Exception as e:
        return _tool_error(e)


def register(mcp: FastMCP) -> None:
    for fn in (
        load_fundamentals,
        load_news,
        load_news_providers,
        load_news_article,
        get_news_bulletins,
        load_wsh_metadata,
        load_wsh_event_data,
        load_economic_calendar,
        load_equity_shortability,
        load_equity_dividends,
    ):
        mcp.tool(annotations=_READ_TOOL)(fn)
