"""Discovery MCP tools: contract search, contract scanning, market scanner, and index composition."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import Context, FastMCP

from src.mcp_app.common import _READ_TOOL, _state, _tool_error


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
        return _tool_error(e)


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
        return _tool_error(e)


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
        return _tool_error(e)


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
) -> list[dict[str, Any]] | dict[str, Any]:
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
        return _tool_error(e)


async def get_scanner_parameters(ctx: Context) -> dict[str, Any]:
    """Get available IBKR scanner parameters (instruments, filters, locations).

    Returns an XML document describing all valid scanner parameter values.
    Required before using scan_market to know valid filter values.
    """
    state = _state(ctx)
    try:
        return await state.feed.get_scanner_parameters()
    except Exception as e:
        return _tool_error(e)


def register(mcp: FastMCP) -> None:
    for fn in (
        search_contracts,
        scan_contracts,
        get_index_composition,
        scan_market,
        get_scanner_parameters,
    ):
        mcp.tool(annotations=_READ_TOOL)(fn)
