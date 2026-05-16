from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from src.feeds.models import AssetClass, OHLCVBar


class MarketOHLCVStore(ABC):
    """Base interface shared by QuestDB and MySQL OHLCV backends."""

    @abstractmethod
    async def connect(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def __aenter__(self) -> "MarketOHLCVStore":
        ...

    @abstractmethod
    async def __aexit__(self, *_: object) -> None:
        ...

    @abstractmethod
    async def create_market_ohlcv_table(self) -> None:
        ...

    @abstractmethod
    async def insert_bars(self, bars: Sequence[OHLCVBar]) -> int:
        ...

    @abstractmethod
    async def query_historical_bars(
        self,
        *,
        symbol: str,
        asset_class: AssetClass | str | None = None,
        bar_size: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 10_000,
    ) -> list[dict[str, Any]]:
        ...

    @abstractmethod
    async def query_latest_bars(
        self,
        *,
        asset_class: AssetClass | str | None = None,
        bar_size: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        ...
