"""Discovery MCP tools: contract search, contract scanning, market scanner, and index composition."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

from mcp.server.fastmcp import Context, FastMCP

from src.mcp_app.common import (
    MCP_DEFAULT_TEXT_CHARS,
    _READ_TOOL,
    _clamp,
    _state,
    _tool_error,
    _truncate_items,
    _truncate_text,
)


def _parse_scanner_parameters_xml(
    xml_text: str,
    *,
    query: str | None,
    max_items: int,
) -> dict[str, Any]:
    """Extract compact instrument, location, scan_code, and filter lists from IBKR's ~1.5MB scanner XML."""
    needle = query.strip().lower() if query and query.strip() else None

    def _matches(*candidates: str | None) -> bool:
        if needle is None:
            return True
        return any(needle in (c or "").lower() for c in candidates)

    root = ET.fromstring(xml_text)

    instruments: list[dict[str, str]] = []
    seen_instruments: set[str] = set()
    for inst in root.findall(".//Instrument"):
        inst_type = (inst.findtext("type") or "").strip()
        name = (inst.findtext("name") or "").strip()
        if inst_type and inst_type not in seen_instruments and _matches(inst_type, name):
            seen_instruments.add(inst_type)
            instruments.append({"type": inst_type, "name": name})

    locations: list[dict[str, str]] = []
    seen_locations: set[str] = set()
    for loc in root.findall(".//Location"):
        code = (loc.findtext("locationCode") or "").strip()
        desc = (loc.findtext("displayName") or "").strip()
        instruments_csv = (loc.findtext("instruments") or "").strip()
        if code and code not in seen_locations and _matches(code, desc, instruments_csv):
            seen_locations.add(code)
            locations.append({
                "location_code": code,
                "display_name": desc,
                "instruments": instruments_csv,
            })

    scan_types: list[dict[str, str]] = []
    seen_scans: set[str] = set()
    for scan in root.findall(".//ScanType"):
        code = (scan.findtext("scanCode") or "").strip()
        name = (scan.findtext("displayName") or "").strip()
        instruments_csv = (scan.findtext("instruments") or "").strip()
        if code and code not in seen_scans and _matches(code, name, instruments_csv):
            seen_scans.add(code)
            scan_types.append({
                "scan_code": code,
                "display_name": name,
                "instruments": instruments_csv,
            })

    inst_env = _truncate_items(instruments, max_items, key="items")
    loc_env = _truncate_items(locations, max_items, key="items")
    scan_env = _truncate_items(scan_types, max_items, key="items")

    total_count = inst_env["total_count"] + loc_env["total_count"] + scan_env["total_count"]
    returned_count = inst_env["returned_count"] + loc_env["returned_count"] + scan_env["returned_count"]
    truncated = bool(inst_env["truncated"] or loc_env["truncated"] or scan_env["truncated"])

    return {
        "query": needle,
        "instruments": inst_env,
        "locations": loc_env,
        "scan_types": scan_env,
        "returned_count": returned_count,
        "total_count": total_count,
        "truncated": truncated,
        "xml_total_chars": len(xml_text),
    }


async def search_contracts(
    ctx: Context,
    pattern: str,
    max_results: int = 25,
) -> dict[str, Any]:
    """Search IBKR contract database for matching symbols.

    Args:
        pattern: Search pattern (e.g. 'Apple', 'TESLA', 'ES futures')
        max_results: Max matches to return (default 25, max 200)
    """
    state = _state(ctx)
    try:
        max_results = _clamp(max_results, low=1, high=200)
        results = await state.feed.search_matching_symbols(pattern)
        serialized = [r.model_dump(mode="json") if hasattr(r, "model_dump") else r for r in results]
        return _truncate_items(
            serialized,
            max_results,
            key="contracts",
            extra={"pattern": pattern},
        )
    except Exception as e:
        return _tool_error(e)


async def scan_contracts(
    ctx: Context,
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
    max_results: int = 20,
) -> dict[str, Any]:
    """Scan IBKR database for contracts matching criteria.

    Args:
        symbol: Symbol to scan for
        sec_type: Security type: STK, OPT, FUT, CASH, IND, BOND, CRYPTO (default 'STK')
        exchange: Exchange (default 'SMART')
        currency: Currency (default 'USD')
        max_results: Max results to return (default 20, max 100)
    """
    state = _state(ctx)
    from src.feeds.scanner import ContractScanRequest

    try:
        max_results = _clamp(max_results, low=1, high=100)
        request = ContractScanRequest(
            symbol=symbol,
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
            max_results=max_results,
        )
        results = await state.feed.scan_contracts(request)
        serialized = [r.model_dump(mode="json") for r in results]
        return _truncate_items(
            serialized,
            max_results,
            key="contracts",
            extra={"symbol": symbol, "sec_type": sec_type, "exchange": exchange},
        )
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
        capped = _clamp(max_results, low=1, high=50)
        composition_request = IndexCompositionScannerRequest(
            index_symbol=index_symbol,
            max_results=capped,
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
        dumped = payload.model_dump(mode="json")
        constituents = dumped.get("constituents") or []
        dumped.setdefault("returned_count", len(constituents))
        dumped.setdefault("total_count", len(constituents))
        dumped.setdefault("truncated", len(constituents) >= capped)
        return dumped
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
) -> dict[str, Any]:
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
        max_results: Maximum results (default 50, IBKR max 50)
    """
    state = _state(ctx)
    try:
        max_results = _clamp(max_results, low=1, high=50)
        rows = await state.feed.scan_market(
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
        return _truncate_items(
            rows,
            max_results,
            key="results",
            extra={"instrument": instrument, "location": location, "scan_code": scan_code},
        )
    except Exception as e:
        return _tool_error(e)


async def get_scanner_parameters(
    ctx: Context,
    query: str | None = None,
    max_items: int = 50,
    include_raw_xml: bool = False,
    max_xml_chars: int = MCP_DEFAULT_TEXT_CHARS,
) -> dict[str, Any]:
    """Get available IBKR scanner parameters (instruments, locations, scan codes).

    Parses IBKR's ~1.5 MB scanner XML into compact, filterable JSON lists so
    it never overflows agent context windows. Pass ``query`` (e.g. 'HK',
    'TOP_PERC', 'FUT') to filter matching entries, or ``include_raw_xml=True``
    for a bounded raw XML slice.

    Args:
        query: Optional case-insensitive substring filter across codes and display names
        max_items: Max entries per category (instruments, locations, scan_types; default 50, max 500)
        include_raw_xml: Include a bounded slice of raw IBKR XML (default False)
        max_xml_chars: Max characters when include_raw_xml is True (default 20000, max 100000)
    """
    state = _state(ctx)
    try:
        max_items = _clamp(max_items, low=1, high=500)
        raw = await state.feed.get_scanner_parameters()
        xml_text = raw.get("xml", "") if isinstance(raw, dict) else str(raw or "")
        if not xml_text:
            return {"instruments": [], "locations": [], "scan_types": [], "returned_count": 0, "total_count": 0, "truncated": False}

        try:
            parsed = _parse_scanner_parameters_xml(xml_text, query=query, max_items=max_items)
        except ET.ParseError:
            return _truncate_text(xml_text, max_xml_chars, key="xml")

        if include_raw_xml:
            xml_env = _truncate_text(xml_text, max_xml_chars, key="xml")
            parsed["raw_xml"] = xml_env
            parsed["truncated"] = bool(parsed["truncated"] or xml_env["truncated"])
        return parsed
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
