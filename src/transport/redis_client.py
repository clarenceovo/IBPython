from __future__ import annotations

from typing import Any

from src.config import config_constant as constants
from src.feeds.index_composition import IndexCompositionPayload
from src.feeds.models import AssetClass, OHLCVBar


def latest_bar_key(asset_class: AssetClass | str, bar_size: str, symbol: str | None = None) -> str:
    bar_size_key = str(bar_size).strip().replace(" ", "_")
    asset_class_key = str(asset_class).strip()
    if symbol is None:
        return constants.REDIS_LATEST_BAR_KEY_TEMPLATE.format(
            asset_class=asset_class_key,
            bar_size=bar_size_key,
        )
    return constants.REDIS_SYMBOL_LATEST_BAR_KEY_TEMPLATE.format(
        asset_class=asset_class_key,
        symbol=str(symbol).strip().upper().replace(" ", "_"),
        bar_size=bar_size_key,
    )


def index_composition_key(index_symbol: str) -> str:
    return constants.REDIS_INDEX_COMPOSITION_KEY_TEMPLATE.format(index_symbol=index_symbol.upper())


def scheduler_job_key(job_name: str) -> str:
    return constants.REDIS_SCHEDULER_JOB_KEY_TEMPLATE.format(job_name=job_name)


class MarketDataRedisClient:
    """Async Redis transport for latest bars, index composition, and scheduler jobs."""

    def __init__(
        self,
        url: str = constants.DEFAULT_REDIS_URL,
        *,
        password: str = constants.DEFAULT_REDIS_PASSWORD,
        client: Any | None = None,
    ) -> None:
        self.url = url
        self.password = password
        self._client = client

    async def connect(self) -> None:
        if self._client is not None:
            return
        try:
            from redis import asyncio as redis_async
        except ImportError as exc:
            raise RuntimeError("redis is required for MarketDataRedisClient") from exc
        self._client = redis_async.from_url(
            self.url,
            decode_responses=False,
            password=self.password or None,
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def __aenter__(self) -> "MarketDataRedisClient":
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def set_latest_bar(self, bar: OHLCVBar) -> str:
        await self.connect()
        key = latest_bar_key(bar.asset_class, bar.bar_size, bar.symbol)
        await self._client.set(key, bar.to_redis_json())
        await self._client.set(latest_bar_key(bar.asset_class, bar.bar_size), bar.to_redis_json())
        return key

    async def get_latest_bar(self, asset_class: AssetClass | str, bar_size: str, symbol: str | None = None) -> OHLCVBar | None:
        await self.connect()
        payload = await self._client.get(latest_bar_key(asset_class, bar_size, symbol))
        if payload is None:
            return None
        return OHLCVBar.from_redis_json(payload)

    async def set_index_composition(self, index_symbol: str, payload: IndexCompositionPayload) -> str:
        await self.connect()
        key = index_composition_key(index_symbol)
        await self._client.set(key, payload.model_dump_json())
        return key

    async def get_index_composition(self, index_symbol: str) -> IndexCompositionPayload | None:
        await self.connect()
        payload = await self._client.get(index_composition_key(index_symbol))
        if payload is None:
            return None
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return IndexCompositionPayload.model_validate_json(payload)

    async def set_scheduler_job(self, job_name: str, payload_json: str) -> str:
        await self.connect()
        key = scheduler_job_key(job_name)
        await self._client.set(key, payload_json)
        return key

    async def scan_scheduler_jobs(self) -> list[str]:
        await self.connect()
        keys: list[str] = []
        async for key in self._client.scan_iter(constants.REDIS_SCHEDULER_JOB_SCAN_PATTERN):
            keys.append(key.decode("utf-8") if isinstance(key, bytes) else key)
        return keys

    async def get_raw(self, key: str) -> bytes | str | None:
        await self.connect()
        return await self._client.get(key)

    async def raw_client(self) -> Any:
        await self.connect()
        return self._client
