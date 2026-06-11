from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from src.feeds.scanner import MarketScannerFilter, MarketScannerRequest, MarketScannerRow


IBKR_MARKET_SCANNER_PROVIDER = "ibkr_market_scanner"
_DEFAULT_INDEX_SCANNER_PRESET = {
    "instrument": "STK",
    "location_code": "STK.US.MAJOR",
    "scan_code": "HOT_BY_VOLUME",
}
_INDEX_SCANNER_PRESETS: dict[str, dict[str, str]] = {
    "HSI": {
        "instrument": "STK",
        "location_code": "STK.HK",
        "scan_code": "HOT_BY_VOLUME",
    },
}


class IndexConstituent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    name: str = Field(default="")
    weight: float | None = None
    sector: str | None = None
    exchange: str | None = None
    currency: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    rank: int | None = Field(default=None, ge=0)
    sec_type: str | None = None
    local_symbol: str | None = None
    primary_exchange: str | None = None


class IndexCompositionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index_symbol: str = Field(min_length=1)
    as_of: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    provider: str
    is_official_composition: bool = True
    constituents: list[IndexConstituent]
    metadata: dict[str, Any] = Field(default_factory=dict)


class IndexCompositionScannerRequest(BaseModel):
    """Request for a scanner-backed approximation of index constituents."""

    model_config = ConfigDict(extra="forbid")

    index_symbol: str = Field(min_length=1, examples=["HSI"])
    max_results: int = Field(default=50, ge=1, le=50)
    instrument: str | None = Field(default=None, min_length=1)
    location_code: str | None = Field(default=None, min_length=1)
    scan_code: str | None = Field(default=None, min_length=1)
    filters: list[MarketScannerFilter] = Field(default_factory=list)


def resolve_index_composition_scanner_request(request: IndexCompositionScannerRequest) -> MarketScannerRequest:
    """Resolve index-level defaults into a concrete IBKR market scanner request."""

    preset = _INDEX_SCANNER_PRESETS.get(request.index_symbol.strip().upper(), _DEFAULT_INDEX_SCANNER_PRESET)
    return MarketScannerRequest(
        instrument=request.instrument or preset["instrument"],
        location_code=request.location_code or preset["location_code"],
        scan_code=request.scan_code or preset["scan_code"],
        max_results=request.max_results,
        filters=request.filters,
    )


def build_index_composition_from_scanner_rows(
    request: IndexCompositionScannerRequest,
    scanner_request: MarketScannerRequest,
    rows: Iterable[MarketScannerRow],
) -> IndexCompositionPayload:
    """Convert ranked scanner rows into an explicitly non-official composition payload."""

    row_list = list(rows)
    constituents = [
        IndexConstituent(
            symbol=row.symbol,
            name=row.long_name,
            weight=None,
            exchange=row.exchange or None,
            currency=row.currency or None,
            con_id=row.con_id,
            rank=row.rank,
            sec_type=row.sec_type or None,
            local_symbol=row.local_symbol or None,
            primary_exchange=row.primary_exchange or None,
        )
        for row in row_list
    ]
    return IndexCompositionPayload(
        index_symbol=request.index_symbol.strip().upper(),
        provider=IBKR_MARKET_SCANNER_PROVIDER,
        is_official_composition=False,
        constituents=constituents,
        metadata={
            "source": "IBKR TWS market scanner",
            "scanner": scanner_request.model_dump(mode="json"),
            "result_count": len(row_list),
            "warning": (
                "Scanner results are ranked contracts, not official index constituents "
                "or index weights."
            ),
        },
    )


class IndexCompositionProvider(Protocol):
    name: str

    async def fetch(self, index_symbol: str) -> IndexCompositionPayload:
        ...


class StaticIndexCompositionProvider:
    """Small provider useful for tests, notebooks, and bootstrapping."""

    name = "static"

    def __init__(self, compositions: dict[str, Iterable[dict[str, Any] | IndexConstituent]]) -> None:
        self.compositions = compositions

    async def fetch(self, index_symbol: str) -> IndexCompositionPayload:
        constituents = [
            item if isinstance(item, IndexConstituent) else IndexConstituent.model_validate(item)
            for item in self.compositions.get(index_symbol.upper(), [])
        ]
        return IndexCompositionPayload(
            index_symbol=index_symbol.upper(),
            provider=self.name,
            constituents=constituents,
        )


class PlaceholderIndexCompositionProvider:
    """Explicit placeholder until a production constituent provider is configured."""

    name = "configured_provider"

    async def fetch(self, index_symbol: str) -> IndexCompositionPayload:
        raise RuntimeError(
            f"index composition provider is not configured for {index_symbol}; "
            "IBKR does not expose index constituents/weights via TWS API"
        )


class IndexCompositionService:
    def __init__(self, provider: IndexCompositionProvider, redis: object) -> None:
        self.provider = provider
        self.redis = redis

    async def sync_index(self, index_symbol: str) -> IndexCompositionPayload:
        payload = await self.provider.fetch(index_symbol)
        await self.redis.set_index_composition(payload.index_symbol, payload)
        return payload

    async def sync_many(self, index_symbols: Iterable[str]) -> list[IndexCompositionPayload]:
        results: list[IndexCompositionPayload] = []
        for index_symbol in index_symbols:
            results.append(await self.sync_index(index_symbol))
        return results
