from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from fastapi import Request

from src.config.settings import Settings
from src.feeds.ibkr_feed import IBKRFeedClient
from src.feeds.ohlcv_loader import OHLCVLoader
from src.transport.ibkr_rate_limit import RedisIBKRHistoricalPacingGuard
from src.transport.market_data_store import MarketOHLCVStore
from src.transport.mysql_client import MySQLClient
from src.transport.questdb_client import QuestDBClient
from src.transport.redis_client import MarketDataRedisClient
from src.webapp.cache import AsyncTTLCache

logger = logging.getLogger(__name__)


@dataclass
class IBKRRestAppState:
    settings: Settings
    redis: MarketDataRedisClient
    questdb: QuestDBClient
    market_store: MarketOHLCVStore
    feed: IBKRFeedClient
    loader: OHLCVLoader
    market_data_cache: AsyncTTLCache

    async def connect(self) -> None:
        try:
            await self.redis.connect()
            logger.info("REST app connected Redis transport")
        except Exception:
            logger.exception("failed to connect Redis during startup")
            raise
        try:
            if self.market_store is not self.questdb:
                await self.market_store.connect()
                logger.info("REST app connected market OHLCV store: backend=%s", self.settings.market_data_db_backend)
            await self.questdb.connect()
            logger.info("REST app connected QuestDB transport")
            await self.market_store.create_market_ohlcv_table()
            logger.info("REST app ensured market OHLCV table: backend=%s", self.settings.market_data_db_backend)
        except Exception:
            logger.exception("failed to connect storage during startup; closing Redis")
            if self.market_store is not self.questdb:
                await self._safe_close(self.market_store.close)
            await self._safe_close(self.redis.close)
            raise
        try:
            await self.feed.connect()
        except Exception:
            logger.exception("failed to connect IBKR feed during startup; closing storage and Redis")
            if self.market_store is not self.questdb:
                await self._safe_close(self.market_store.close)
            await self._safe_close(self.questdb.close)
            await self._safe_close(self.redis.close)
            raise

    async def close(self) -> None:
        errors: list[BaseException] = []
        closers = [self.feed.disconnect]
        if self.market_store is not self.questdb:
            closers.append(self.market_store.close)
        closers.extend([self.questdb.close, self.redis.close])
        for closer in closers:
            try:
                await closer()
            except Exception as exc:
                errors.append(exc)
                logger.warning("error during shutdown: %s", exc)
        if errors:
            logger.warning("%d error(s) during shutdown", len(errors))

    @staticmethod
    async def _safe_close(coro_fn: Any) -> None:
        try:
            await coro_fn()
        except Exception:
            pass


def build_rest_app_state(settings: Settings) -> IBKRRestAppState:
    redis = MarketDataRedisClient(settings.redis_url, password=settings.redis_password)
    questdb = QuestDBClient(
        host=settings.questdb_host,
        port=settings.questdb_port,
        user=settings.questdb_user,
        password=settings.questdb_password,
        database=settings.questdb_database,
    )
    market_store = build_market_data_store(settings, questdb=questdb)
    pacing_guard = RedisIBKRHistoricalPacingGuard(redis)
    feed = IBKRFeedClient(
        host=settings.ibkr_host,
        port=settings.ibkr_port,
        client_id=settings.ibkr_client_id,
        pacing_guard=pacing_guard,
    )
    loader = OHLCVLoader(feed, store=market_store, redis=redis)
    market_data_cache = AsyncTTLCache(
        ttl_seconds=settings.ibkr_rest_market_data_ttl_seconds,
        max_size=settings.ibkr_rest_market_data_cache_maxsize,
    )
    return IBKRRestAppState(
        settings=settings,
        redis=redis,
        questdb=questdb,
        market_store=market_store,
        feed=feed,
        loader=loader,
        market_data_cache=market_data_cache,
    )


def get_rest_state(request: Request) -> IBKRRestAppState:
    return request.app.state.ibkr_rest_state


def build_market_data_store(settings: Settings, *, questdb: QuestDBClient) -> MarketOHLCVStore:
    backend = settings.market_data_db_backend.strip().lower()
    if backend == "questdb":
        return questdb
    if backend == "mysql":
        return MySQLClient(
            host=settings.mysql_host,
            port=settings.mysql_port,
            user=settings.mysql_user,
            password=settings.mysql_password,
            database=settings.mysql_database,
        )
    raise ValueError(f"unsupported MARKET_DATA_DB_BACKEND={settings.market_data_db_backend!r}; expected questdb or mysql")
