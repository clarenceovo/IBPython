from __future__ import annotations

import logging
import importlib
import inspect
from dataclasses import dataclass
from typing import Any

from fastapi import Request

from src.config.settings import Settings
from src.feeds.event_contracts import IBKRWebAPIClient
from src.feeds.fixed_income import FixedIncomeReferenceProvider
from src.feeds.ibkr_feed import IBKRFeedClient
from src.feeds.ohlcv_loader import OHLCVLoader
from src.transport.ibkr_rate_limit import IBKRRateLimitController
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
    event_contracts: IBKRWebAPIClient
    fixed_income_reference_provider: FixedIncomeReferenceProvider | None = None

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
            logger.debug("safe_close failed", exc_info=True)


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
    rate_limiter = IBKRRateLimitController(
        redis,
        enabled=settings.ibkr_rate_limit_enabled,
        global_messages_per_second=settings.ibkr_rate_limit_global_messages_per_second,
        market_data_lines=settings.ibkr_market_data_lines,
        market_data_line_reserve=settings.ibkr_rate_limit_market_data_reserve,
        market_data_lease_ttl_seconds=settings.ibkr_rate_limit_market_data_lease_ttl_seconds,
    )
    feed = IBKRFeedClient(
        host=settings.ibkr_host,
        port=settings.ibkr_port,
        client_id=settings.ibkr_client_id,
        pacing_guard=rate_limiter.pacing_guard,
        rate_limiter=rate_limiter,
    )
    loader = OHLCVLoader(feed, store=market_store, redis=redis)
    market_data_cache = AsyncTTLCache(
        ttl_seconds=settings.ibkr_rest_market_data_ttl_seconds,
        max_size=settings.ibkr_rest_market_data_cache_maxsize,
    )
    event_contracts = IBKRWebAPIClient(
        base_url=settings.ibkr_web_api_base_url,
        bearer_token=settings.ibkr_web_api_bearer_token,
        cookie=settings.ibkr_web_api_cookie,
        verify_ssl=settings.ibkr_web_api_verify_ssl,
    )
    fixed_income_reference_provider = build_fixed_income_reference_provider(settings.fixed_income_reference_provider)
    return IBKRRestAppState(
        settings=settings,
        redis=redis,
        questdb=questdb,
        market_store=market_store,
        feed=feed,
        loader=loader,
        market_data_cache=market_data_cache,
        event_contracts=event_contracts,
        fixed_income_reference_provider=fixed_income_reference_provider,
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


def build_fixed_income_reference_provider(import_path: str) -> FixedIncomeReferenceProvider | None:
    normalized = import_path.strip()
    if not normalized:
        logger.info("fixed income reference provider not configured; CTD business APIs will require one")
        return None
    module_name, separator, attribute_name = normalized.partition(":")
    if not separator or not module_name or not attribute_name:
        raise ValueError("FIXED_INCOME_REFERENCE_PROVIDER must be an import path like 'package.module:provider_or_factory'")
    module = importlib.import_module(module_name)
    target = getattr(module, attribute_name)
    provider = target if _looks_like_fixed_income_provider(target) else target()
    if not _looks_like_fixed_income_provider(provider):
        raise TypeError("FIXED_INCOME_REFERENCE_PROVIDER target must expose name and async get_deliverable_basket(request)")
    logger.info("loaded fixed income reference provider: name=%s source=%s", provider.name, normalized)
    return provider


def _looks_like_fixed_income_provider(provider: Any) -> bool:
    get_deliverable_basket = getattr(provider, "get_deliverable_basket", None)
    return bool(getattr(provider, "name", None)) and callable(get_deliverable_basket) and inspect.iscoroutinefunction(get_deliverable_basket)
