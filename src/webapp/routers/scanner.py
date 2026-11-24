from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, Query
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.index_composition import (
    IndexCompositionPayload,
    IndexCompositionScannerRequest,
    build_index_composition_from_scanner_rows,
    resolve_index_composition_scanner_request,
)
from src.feeds.scanner import ContractScanRequest, ContractSearchRequest, ContractSearchResult
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

router = APIRouter(prefix="/scanner", tags=["reference-data"])

CONTRACT_SEARCH_EXAMPLES = {
    "tsla_search": {
        "summary": "Search TSLA contracts",
        "value": {"symbol": "TSLA", "sec_type": "STK"},
    },
    "spx_index": {
        "summary": "Search SPX index",
        "value": {"symbol": "SPX", "sec_type": "IND"},
    },
    "es_futures": {
        "summary": "Search ES futures",
        "value": {"symbol": "ES", "sec_type": "FUT"},
    },
    "by_conid": {
        "summary": "Look up by conId",
        "value": {"con_id": 76792991},
    },
}

CONTRACT_SCAN_EXAMPLES = {
    "aapl_scan": {
        "summary": "Scan AAPL across exchanges",
        "value": {"symbol": "AAPL", "sec_type": "STK", "max_results": 10},
    },
    "eurusd_fx": {
        "summary": "Scan EURUSD forex",
        "value": {"symbol": "EURUSD", "sec_type": "CASH", "exchange": "IDEALPRO"},
    },
}

INDEX_COMPOSITION_SCAN_EXAMPLES = {
    "hsi_scanner_approximation": {
        "summary": "HSI scanner approximation",
        "description": "Uses the default HK stock scanner preset; results are not official index constituents or weights.",
        "value": {"index_symbol": "HSI", "max_results": 50, "use_ttl_cache": True},
    },
}


class CachedContractSearchRequest(BaseModel):
    """Cached contract-search request envelope."""

    model_config = ConfigDict(extra="forbid")

    request: ContractSearchRequest = Field(description="Search criteria: symbol pattern or con_id, with optional sec_type/exchange/currency filters.")
    use_ttl_cache: bool = Field(default=True, description="Serve the response from the shared TTL cache when possible.")
    cache_ttl_seconds: float | None = Field(default=300, ge=0, description="Cache entry lifetime in seconds; null uses the cache default.")


class CachedContractScanRequest(BaseModel):
    """Cached contract-scan request envelope."""

    model_config = ConfigDict(extra="forbid")

    request: ContractScanRequest = Field(description="Scan criteria: symbol plus optional sec_type/exchange/currency/primary_exchange and max_results (1-100).")
    use_ttl_cache: bool = Field(default=True, description="Serve the response from the shared TTL cache when possible.")
    cache_ttl_seconds: float | None = Field(default=300, ge=0, description="Cache entry lifetime in seconds; null uses the cache default.")


class CachedIndexCompositionScannerRequest(IndexCompositionScannerRequest):
    """Cached index-composition scanner request envelope."""

    use_ttl_cache: bool = Field(default=True, description="Serve the response from the shared TTL cache when possible.")
    cache_ttl_seconds: float | None = Field(default=300, ge=0, description="Cache entry lifetime in seconds; null uses the cache default.")

    def to_composition_request(self) -> IndexCompositionScannerRequest:
        return IndexCompositionScannerRequest.model_validate(
            self.model_dump(exclude={"use_ttl_cache", "cache_ttl_seconds"})
        )


@router.post("/search", response_model=list[ContractSearchResult])
async def search_contracts(
    payload: Annotated[CachedContractSearchRequest, Body(openapi_examples=CONTRACT_SEARCH_EXAMPLES)],
    limit: int | None = Query(default=None, ge=1, le=1000, description="Return at most the first N matches; omit for all."),
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[ContractSearchResult]:
    """Search IBKR contract definitions by symbol pattern or con_id.

    Returns full contract details per match: con_id, security type,
    exchange, currency, local symbol, multiplier, and for options the
    strike, right, and expiry. Supply symbol or con_id (one is required).
    Responses are cached for cache_ttl_seconds (default 300 s).
    """
    async def load() -> list[ContractSearchResult]:
        return await state.feed.search_contracts(payload.request)

    if payload.use_ttl_cache:
        from src.webapp.cache import stable_cache_key

        key = stable_cache_key("contract_search", payload.request)
        results = await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    else:
        results = await load()
    return results[:limit] if isinstance(limit, int) else results


@router.post("/scan", response_model=list[ContractSearchResult])
async def scan_contracts(
    payload: Annotated[CachedContractScanRequest, Body(openapi_examples=CONTRACT_SCAN_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[ContractSearchResult]:
    """Scan IBKR's contract database for matching contracts.

    Unlike /search (pattern match), the scan resolves the symbol on an
    exchange (default SMART) and returns every distinct contract found,
    bounded by request.max_results (default 20, at most 100). Results carry
    the same detail as /search. Cached for cache_ttl_seconds (default
    300 s).
    """
    async def load() -> list[ContractSearchResult]:
        return await state.feed.scan_contracts(payload.request)

    if payload.use_ttl_cache:
        from src.webapp.cache import stable_cache_key

        key = stable_cache_key("contract_scan", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post("/index-composition", response_model=IndexCompositionPayload)
async def get_index_composition(
    payload: Annotated[CachedIndexCompositionScannerRequest, Body(openapi_examples=INDEX_COMPOSITION_SCAN_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> IndexCompositionPayload:
    """Return an index composition approximated from the IBKR market scanner.

    Runs the scanner preset for the index symbol (for example the HK stock
    preset for HSI) and maps the rows to constituents with ranks. The
    result is an approximation: is_official_composition is false and
    weights are absent. Bounded by max_results (IBKR caps scans at 50).
    Cached for cache_ttl_seconds (default 300 s).
    """
    composition_request = payload.to_composition_request()
    scanner_request = resolve_index_composition_scanner_request(composition_request)

    async def load() -> IndexCompositionPayload:
        rows = await state.feed.run_market_scanner(scanner_request)
        return build_index_composition_from_scanner_rows(composition_request, scanner_request, rows)

    if payload.use_ttl_cache:
        from src.webapp.cache import stable_cache_key

        key = stable_cache_key("index_composition_scanner", composition_request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.get("/parameters", summary="Get IBKR scanner parameters")
async def get_scanner_parameters(state: IBKRRestAppState = Depends(get_rest_state)):
    """Returns available scanner instruments, filters, and locations from IBKR.

    Returns an XML document describing all valid scanner parameter values.
    Required before using scan_market to know valid filter values.
    """
    return await state.feed.get_scanner_parameters()
