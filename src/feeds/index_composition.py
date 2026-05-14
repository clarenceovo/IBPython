from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field


class IndexConstituent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1)
    name: str = Field(default="")
    weight: float | None = None
    sector: str | None = None
    exchange: str | None = None
    currency: str | None = None


class IndexCompositionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index_symbol: str = Field(min_length=1)
    as_of: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    provider: str
    constituents: list[IndexConstituent]
    metadata: dict[str, Any] = Field(default_factory=dict)


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
