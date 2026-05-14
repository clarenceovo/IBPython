from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from fastapi import Request

from src.config.settings import Settings
from src.feeds.ibkr_feed import IBKRFeedClient
from src.feeds.ohlcv_loader import OHLCVLoader
from src.transport.ibkr_rate_limit import RedisIBKRHistoricalPacingGuard
from src.transport.questdb_client import QuestDBClient
from src.transport.redis_client import MarketDataRedisClient
from src.webapp.cache import AsyncTTLCache

logger = logging.getLogger(__name__)


@dataclass
class IBKRRestAppState:
    settings: Settings
    redis: MarketDataRedisClient
    questdb: QuestDBClient
    feed: IBKRFeedClient
    loader: OHLCVLoader
    market_data_cache: AsyncTTLCache

    async def connect(self) -> None:
        try:
            await self.redis.connect()
        except Exception:
            logger.exception("failed to connect Redis during startup")
            raise
        try:
            await self.questdb.connect()
        except Exception:
            logger.exception("failed to connect QuestDB during startup; closing Redis")
            await self._safe_close(self.redis.close)
            raise
        try:
            await self.feed.connect()
        except Exception:
            logger.exception("failed to connect IBKR feed during startup; closing QuestDB and Redis")
            await self._safe_close(self.questdb.close)
            await self._safe_close(self.redis.close)
            raise

    async def close(self) -> None:
        errors: list[BaseException] = []
        for closer in (self.feed.disconnect, self.questdb.close, self.redis.close):
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
    pacing_guard = RedisIBKRHistoricalPacingGuard(redis)
    feed = IBKRFeedClient(
        host=settings.ibkr_host,
        port=settings.ibkr_port,
        client_id=settings.ibkr_client_id,
        pacing_guard=pacing_guard,
    )
    loader = OHLCVLoader(feed, questdb=questdb, redis=redis)
    market_data_cache = AsyncTTLCache(
        ttl_seconds=settings.ibkr_rest_market_data_ttl_seconds,
        max_size=settings.ibkr_rest_market_data_cache_maxsize,
    )
    return IBKRRestAppState(
        settings=settings,
        redis=redis,
        questdb=questdb,
        feed=feed,
        loader=loader,
        market_data_cache=market_data_cache,
    )


def get_rest_state(request: Request) -> IBKRRestAppState:
    return request.app.state.ibkr_rest_state
