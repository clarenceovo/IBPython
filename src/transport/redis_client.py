from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from src.config import config_constant as constants
from src.feeds.index_composition import IndexCompositionPayload
from src.feeds.models import AssetClass, OHLCVBar
from src.feeds.snapshotter import EquitySnapshot


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


def scheduler_lease_key(job_name: str) -> str:
    return constants.REDIS_SCHEDULER_LEASE_KEY_TEMPLATE.format(job_name=_redis_token(job_name))


def scheduler_run_latest_key(job_name: str) -> str:
    return constants.REDIS_SCHEDULER_RUN_LATEST_KEY_TEMPLATE.format(job_name=_redis_token(job_name))


def scheduler_run_history_key(job_name: str) -> str:
    return constants.REDIS_SCHEDULER_RUN_HISTORY_KEY_TEMPLATE.format(job_name=_redis_token(job_name))


def ohlcv_snapshot_last_ts_key(job_name: str, symbol: str, bar_size: str) -> str:
    return constants.REDIS_OHLCV_SNAPSHOT_LAST_TS_KEY_TEMPLATE.format(
        job_name=_redis_token(job_name),
        symbol=_redis_token(symbol),
        bar_size=_redis_token(bar_size),
    )


def ohlcv_snapshot_status_key(job_name: str, symbol: str, bar_size: str) -> str:
    return constants.REDIS_OHLCV_SNAPSHOT_STATUS_KEY_TEMPLATE.format(
        job_name=_redis_token(job_name),
        symbol=_redis_token(symbol),
        bar_size=_redis_token(bar_size),
    )


def ohlcv_snapshot_calendar_key(
    *,
    asset_class: AssetClass | str,
    exchange: str,
    symbol: str,
    date_value: str,
    use_rth: bool,
    contract_fingerprint: str = "default",
) -> str:
    return constants.REDIS_OHLCV_SNAPSHOT_CALENDAR_KEY_TEMPLATE.format(
        asset_class=_redis_token(str(asset_class)),
        exchange=_redis_token(exchange),
        symbol=_redis_token(symbol),
        contract_fingerprint=_redis_token(contract_fingerprint),
        date=date_value,
        use_rth=str(use_rth).lower(),
    )


def _redis_token(value: str) -> str:
    return str(value).strip().upper().replace(" ", "_")


def order_envelope_key(order_uuid: str) -> str:
    return constants.REDIS_ORDER_ENVELOPE_KEY_TEMPLATE.format(order_uuid=order_uuid)


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

    async def health_check(self) -> bool:
        """Return True if Redis is reachable, False otherwise."""
        try:
            if self._client is None:
                return False
            return bool(await self._client.ping())
        except Exception:
            return False

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

    async def set_raw(self, key: str, value: str, *, ex: int | None = None) -> None:
        await self.connect()
        kwargs = {"ex": ex} if ex is not None else {}
        await self._client.set(key, value, **kwargs)

    async def acquire_scheduler_lease(self, job_name: str, owner_token: str, *, ttl_seconds: float) -> bool:
        await self.connect()
        key = scheduler_lease_key(job_name)
        ttl = max(1, int(ttl_seconds))
        return bool(await self._client.set(key, owner_token, nx=True, ex=ttl))

    async def release_scheduler_lease(self, job_name: str, owner_token: str) -> bool:
        await self.connect()
        key = scheduler_lease_key(job_name)
        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        end
        return 0
        """
        return bool(await self._client.eval(script, 1, key, owner_token))

    async def record_scheduler_run(self, job_name: str, payload: dict[str, Any]) -> None:
        await self.connect()
        encoded = json.dumps(payload, sort_keys=True, default=str)
        latest_key = scheduler_run_latest_key(job_name)
        history_key = scheduler_run_history_key(job_name)
        await self._client.set(latest_key, encoded)
        await self._client.lpush(history_key, encoded)
        await self._client.ltrim(history_key, 0, constants.REDIS_SCHEDULER_RUN_HISTORY_MAXLEN - 1)

    async def raw_client(self) -> Any:
        await self.connect()
        return self._client

    async def get_ohlcv_snapshot_last_ts(self, job_name: str, symbol: str, bar_size: str) -> datetime | None:
        payload = await self.get_raw(ohlcv_snapshot_last_ts_key(job_name, symbol, bar_size))
        if payload is None:
            return None
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        parsed = datetime.fromisoformat(payload.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    async def set_ohlcv_snapshot_last_ts(
        self,
        job_name: str,
        symbol: str,
        bar_size: str,
        timestamp: datetime,
    ) -> str:
        key = ohlcv_snapshot_last_ts_key(job_name, symbol, bar_size)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        await self.set_raw(key, timestamp.astimezone(timezone.utc).isoformat())
        return key

    async def set_ohlcv_snapshot_status(
        self,
        job_name: str,
        symbol: str,
        bar_size: str,
        status: dict[str, Any],
    ) -> str:
        key = ohlcv_snapshot_status_key(job_name, symbol, bar_size)
        await self.set_raw(key, json.dumps(status, sort_keys=True, default=str))
        return key

    # ------------------------------------------------------------------
    # Equity snapshot caching
    # ------------------------------------------------------------------

    async def set_latest_equity_snapshot(self, snapshot: EquitySnapshot) -> str:
        """Cache the latest snapshot for a symbol."""
        await self.connect()
        key = constants.REDIS_EQUITY_SNAPSHOT_KEY_TEMPLATE.format(symbol=snapshot.symbol.upper())
        await self._client.set(key, snapshot.to_redis_json())
        return key

    async def get_latest_equity_snapshot(self, symbol: str) -> EquitySnapshot | None:
        """Get the latest cached snapshot for a symbol."""
        await self.connect()
        key = constants.REDIS_EQUITY_SNAPSHOT_KEY_TEMPLATE.format(symbol=symbol.strip().upper())
        payload = await self._client.get(key)
        if payload is None:
            return None
        return EquitySnapshot.from_redis_json(payload)

    async def get_latest_equity_snapshots(self, symbols: list[str]) -> dict[str, EquitySnapshot]:
        """Batch-get latest cached snapshots for multiple symbols."""
        await self.connect()
        if not symbols:
            return {}
        keys = [constants.REDIS_EQUITY_SNAPSHOT_KEY_TEMPLATE.format(symbol=s.strip().upper()) for s in symbols]
        payloads = await self._client.mget(keys)
        result: dict[str, EquitySnapshot] = {}
        for symbol, payload in zip(symbols, payloads):
            if payload is not None:
                try:
                    result[symbol.strip().upper()] = EquitySnapshot.from_redis_json(payload)
                except Exception:
                    pass
        return result

    async def set_snapshot_watchlist(self, watchlist_name: str, payload_json: str) -> str:
        await self.connect()
        key = constants.REDIS_SNAPSHOT_WATCHLIST_KEY_TEMPLATE.format(name=watchlist_name)
        await self._client.set(key, payload_json)
        return key

    async def scan_snapshot_watchlists(self) -> list[str]:
        await self.connect()
        keys: list[str] = []
        pattern = constants.REDIS_SNAPSHOT_WATCHLIST_KEY_TEMPLATE.replace("{name}", "*")
        async for key in self._client.scan_iter(pattern):
            keys.append(key.decode("utf-8") if isinstance(key, bytes) else key)
        return keys

    # ------------------------------------------------------------------
    # Order envelope caching
    # ------------------------------------------------------------------

    async def cache_order_envelope(self, envelope_json: str, *, ttl: int | None = None) -> str:
        """Cache an OrderEnvelope JSON blob keyed by its UUID. Returns the Redis key."""
        await self.connect()
        data = json.loads(envelope_json)
        order_uuid = data["order_uuid"]
        key = order_envelope_key(order_uuid)
        effective_ttl = ttl if ttl is not None else constants.REDIS_ORDER_ENVELOPE_TTL_SECONDS
        await self._client.set(key, envelope_json, ex=effective_ttl)
        return key

    async def get_order_envelope(self, order_uuid: str) -> str | None:
        """Get cached OrderEnvelope JSON by UUID. Returns None if not found."""
        await self.connect()
        key = order_envelope_key(order_uuid)
        payload = await self._client.get(key)
        if payload is None:
            return None
        return payload.decode("utf-8") if isinstance(payload, bytes) else payload

    async def scan_order_envelopes(self) -> list[str]:
        """Scan all cached order envelope keys."""
        await self.connect()
        keys: list[str] = []
        async for key in self._client.scan_iter(constants.REDIS_ORDER_ENVELOPE_SCAN_PATTERN):
            keys.append(key.decode("utf-8") if isinstance(key, bytes) else key)
        return keys

    async def delete_order_envelope(self, order_uuid: str) -> bool:
        """Delete a cached order envelope by UUID. Returns True if deleted."""
        await self.connect()
        key = order_envelope_key(order_uuid)
        return bool(await self._client.delete(key))
