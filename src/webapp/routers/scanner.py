from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends
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
    model_config = ConfigDict(extra="forbid")

    request: ContractSearchRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)


class CachedContractScanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: ContractScanRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)


class CachedIndexCompositionScannerRequest(IndexCompositionScannerRequest):
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)

    def to_composition_request(self) -> IndexCompositionScannerRequest:
        return IndexCompositionScannerRequest.model_validate(
            self.model_dump(exclude={"use_ttl_cache", "cache_ttl_seconds"})
        )


@router.post("/search", response_model=list[ContractSearchResult])
async def search_contracts(
    payload: Annotated[CachedContractSearchRequest, Body(openapi_examples=CONTRACT_SEARCH_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[ContractSearchResult]:
    async def load() -> list[ContractSearchResult]:
        return await state.feed.search_contracts(payload.request)

    if payload.use_ttl_cache:
        from src.webapp.cache import stable_cache_key

        key = stable_cache_key("contract_search", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


@router.post("/scan", response_model=list[ContractSearchResult])
async def scan_contracts(
    payload: Annotated[CachedContractScanRequest, Body(openapi_examples=CONTRACT_SCAN_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[ContractSearchResult]:
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
