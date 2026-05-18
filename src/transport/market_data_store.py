"""Protocol for market OHLCV persistence backends."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from src.feeds.models import AssetClass, OHLCVBar


@runtime_checkable
class MarketOHLCVStore(Protocol):
    """Async interface for OHLCV bar persistence and retrieval."""

    async def create_market_ohlcv_table(self) -> None: ...

    async def insert_bars(self, bars: Sequence[OHLCVBar]) -> int: ...

    async def query_historical_bars(
        self,
        *,
        symbol: str,
        asset_class: AssetClass | str | None = None,
        bar_size: str | None = None,
        contract_key: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 10_000,
    ) -> list[dict[str, Any]]: ...

    async def query_latest_bars(
        self,
        *,
        asset_class: AssetClass | str | None = None,
        bar_size: str | None = None,
        contract_key: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]: ...
