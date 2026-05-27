from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import signal
from contextlib import AsyncExitStack
from types import FrameType
from typing import Any

from src.config.settings import load_settings
from src.feeds.index_composition import IndexCompositionProvider, IndexCompositionService
from src.feeds.ibkr_feed import IBKRFeedClient
from src.feeds.ohlcv_loader import OHLCVLoader
from src.transport.ibkr_rate_limit import RedisIBKRHistoricalPacingGuard
from src.transport.market_data_store import MarketOHLCVStore
from src.transport.mysql_client import MySQLClient
from src.transport.questdb_client import QuestDBClient
from src.transport.redis_client import MarketDataRedisClient
from src.transport.scheduler import GenericScheduler, IndexCompositionReloadJobHandler, MarketSnapshotJobHandler, OHLCVSnapshotJobHandler

logger = logging.getLogger(__name__)

PLACEHOLDER_INDEX_PROVIDER_NAMES = {"", "configured_provider", "placeholder", "todo"}


def configure_logging(telegram_bot_token: str = "", telegram_chat_id: str = "", telegram_log_level: str = "WARNING") -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    # Attach Telegram handler if configured
    if telegram_bot_token and telegram_chat_id:
        import logging as _logging

        from src.transport.telegram_client import TelegramLogHandler

        level = getattr(_logging, telegram_log_level.upper(), _logging.WARNING)
        tg_handler = TelegramLogHandler(
            bot_token=telegram_bot_token,
            chat_id=telegram_chat_id,
            level=level,
        )
        _logging.getLogger().addHandler(tg_handler)
        logger.info("Telegram logging enabled: chat_id=%s level=%s", telegram_chat_id, telegram_log_level)


def build_index_composition_provider(provider_name: str) -> IndexCompositionProvider | None:
    """Build an index composition provider from INDEX_COMPOSITION_PROVIDER.

    Blank and placeholder values intentionally disable index reload jobs. A real
    provider can be supplied as an import path: ``module.path:provider_or_factory``.
    The target may be a provider instance, a provider class, or a zero-argument
    factory returning an object with ``name`` and async ``fetch(...)`` attributes.
    """

    normalized = provider_name.strip()
    if normalized.lower() in PLACEHOLDER_INDEX_PROVIDER_NAMES:
        logger.info("index composition provider not configured; index reload jobs will be skipped")
        return None

    try:
        provider = _load_provider_from_import_path(normalized)
    except Exception:
        logger.exception(
            "failed to load INDEX_COMPOSITION_PROVIDER=%r; index reload jobs will be skipped",
            provider_name,
        )
        return None

    logger.info("loaded index composition provider: name=%s source=%s", provider.name, normalized)
    return provider


async def main() -> None:
    # Load settings first so we can configure telegram logging
    settings = load_settings()
    configure_logging(
        telegram_bot_token=settings.telegram_bot_token,
        telegram_chat_id=settings.telegram_chat_id,
        telegram_log_level=settings.telegram_log_level,
    )
    logger.info(
        "starting scheduler worker: ibkr=%s:%s client_id=%s redis_url=%s redis_password_configured=%s "
        "market_store_backend=%s questdb=%s:%s/%s mysql=%s:%s/%s index_provider=%r",
        settings.ibkr_host,
        settings.ibkr_port,
        settings.ibkr_client_id,
        settings.redis_url,
        bool(settings.redis_password),
        settings.market_data_db_backend,
        settings.questdb_host,
        settings.questdb_port,
        settings.questdb_database,
        settings.mysql_host,
        settings.mysql_port,
        settings.mysql_database,
        settings.index_composition_provider,
    )

    redis = MarketDataRedisClient(settings.redis_url, password=settings.redis_password)
    store = build_market_data_store(settings)
    pacing_guard = RedisIBKRHistoricalPacingGuard(redis)
    ibkr = IBKRFeedClient(
        host=settings.ibkr_host,
        port=settings.ibkr_port,
        client_id=settings.ibkr_client_id,
        pacing_guard=pacing_guard,
    )

    scheduler = GenericScheduler(
        redis_client=redis,
        local_job_directory="schedulejob",
        job_reload_interval_seconds=60,
    )
    loader = OHLCVLoader(ibkr, store=store, redis=redis)
    snapshot_handler = MarketSnapshotJobHandler(loader)
    scheduler.register_handler(snapshot_handler.job_type, snapshot_handler)
    ohlcv_snapshot_handler = OHLCVSnapshotJobHandler(
        loader,
        redis=redis,
        api_base_url=settings.ibkr_rest_base_url,
    )
    scheduler.register_handler(ohlcv_snapshot_handler.job_type, ohlcv_snapshot_handler)

    index_provider = build_index_composition_provider(settings.index_composition_provider)
    if index_provider is not None:
        index_service = IndexCompositionService(index_provider, redis)
        index_handler = IndexCompositionReloadJobHandler(index_service, provider_name=index_provider.name)
        scheduler.register_handler(index_handler.job_type, index_handler)

    _install_signal_handlers(scheduler)

    try:
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(redis)
            logger.info("connected Redis transport")

            try:
                local_jobs = await scheduler.load_jobs_from_directory("schedulejob")
                redis_jobs = await scheduler.load_jobs_from_redis(redis)
                jobs = scheduler.jobs()
            except Exception:
                logger.exception("failed to load scheduler jobs")
                return

            if not jobs:
                logger.warning(
                    "no runnable scheduler jobs found. Add Redis keys like SchedulerJob::snapshot_spy_1m "
                    "or local schedulejob/*.json files. local_jobs=%d redis_jobs=%d",
                    len(local_jobs),
                    len(redis_jobs),
                )
                return

            needs_ibkr = _jobs_require_ibkr(jobs)
            needs_market_store = _jobs_require_market_store(jobs)
            logger.info(
                "scheduler dependency plan: jobs=%d needs_ibkr=%s needs_market_store=%s market_store_backend=%s job_types=%s",
                len(jobs),
                needs_ibkr,
                needs_market_store,
                settings.market_data_db_backend,
                ",".join(sorted({job.job_type for job in jobs})),
            )

            unknown_types = {job.job_type for job in jobs} - {"market_snapshot", "ohlcv_snapshot", "index_composition_reload"}
            if unknown_types:
                logger.warning(
                    "unknown job types detected that may not receive IBKR/storage connections: %s",
                    ",".join(sorted(unknown_types)),
                )

            if needs_market_store:
                await stack.enter_async_context(store)
                logger.info("connected market OHLCV store: backend=%s", settings.market_data_db_backend)
                await store.create_market_ohlcv_table()
                logger.info("ensured market OHLCV table exists: backend=%s", settings.market_data_db_backend)
            else:
                logger.info("market OHLCV store connection skipped; no runnable job requires persistence")

            if needs_ibkr:
                await stack.enter_async_context(ibkr)
                logger.info("connected IBKR feed")
            else:
                logger.info("IBKR connection skipped; no runnable job requires IBKR")

            logger.info("scheduler entering run loop")
            await scheduler.run_forever()
            logger.info("scheduler run loop exited")
    except Exception:
        logger.exception("fatal error in scheduler main loop")
        raise


def _load_provider_from_import_path(import_path: str) -> IndexCompositionProvider:
    module_name, separator, attribute_name = import_path.partition(":")
    if not separator or not module_name or not attribute_name:
        raise ValueError(
            "INDEX_COMPOSITION_PROVIDER must be an import path like 'package.module:provider_or_factory'"
        )

    module = importlib.import_module(module_name)
    target = getattr(module, attribute_name)
    provider = target if _looks_like_index_provider(target) else target()
    if not _looks_like_index_provider(provider):
        raise TypeError(
            "INDEX_COMPOSITION_PROVIDER target must expose a name attribute and async fetch(index_symbol)"
        )
    return provider


def build_market_data_store(settings: Any) -> MarketOHLCVStore:
    backend = settings.market_data_db_backend.strip().lower()
    if backend == "mysql":
        logger.info("using MySQL as market OHLCV store")
        return MySQLClient(
            host=settings.mysql_host,
            port=settings.mysql_port,
            user=settings.mysql_user,
            password=settings.mysql_password,
            database=settings.mysql_database,
        )
    if backend == "questdb":
        logger.info("using QuestDB as market OHLCV store")
        return QuestDBClient(
            host=settings.questdb_host,
            port=settings.questdb_port,
            write_port=settings.questdb_write_port,
            user=settings.questdb_user,
            password=settings.questdb_password,
            database=settings.questdb_database,
        )
    raise ValueError(f"unsupported MARKET_DATA_DB_BACKEND={settings.market_data_db_backend!r}; expected questdb or mysql")


def _looks_like_index_provider(provider: Any) -> bool:
    fetch = getattr(provider, "fetch", None)
    return bool(getattr(provider, "name", None)) and callable(fetch) and inspect.iscoroutinefunction(fetch)


def _jobs_require_ibkr(jobs: list[Any]) -> bool:
    return any(job.job_type == MarketSnapshotJobHandler.job_type for job in jobs)


def _jobs_require_market_store(jobs: list[Any]) -> bool:
    return any(
        job.job_type == MarketSnapshotJobHandler.job_type and _job_param_bool(job.params.get("persist"), default=True)
        for job in jobs
    )


def _jobs_require_questdb(jobs: list[Any]) -> bool:
    """Backward-compatible alias for older tests and scripts."""
    return _jobs_require_market_store(jobs)


def _ohlcv_snapshot_job_persists(job: Any) -> bool:
    defaults = job.params.get("defaults", {}) if isinstance(job.params, dict) else {}
    default_persist = _job_param_bool(defaults.get("persist"), default=True)
    for symbol in job.params.get("symbols", []):
        if isinstance(symbol, dict) and "persist" in symbol:
            if _job_param_bool(symbol.get("persist"), default=default_persist):
                return True
    return default_persist


def _job_param_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "f", "no", "n", "off"}:
            return False
    return bool(value)


def _install_signal_handlers(scheduler: GenericScheduler) -> None:
    loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                shutdown_signal,
                _request_scheduler_shutdown,
                scheduler,
                shutdown_signal,
            )
            logger.info("installed shutdown signal handler: signal=%s", shutdown_signal.name)
        except NotImplementedError:
            signal.signal(
                shutdown_signal,
                lambda sig, frame: _request_scheduler_shutdown(scheduler, signal.Signals(sig), frame),
            )
            logger.info("installed fallback shutdown signal handler: signal=%s", shutdown_signal.name)


def _request_scheduler_shutdown(
    scheduler: GenericScheduler,
    shutdown_signal: signal.Signals,
    _frame: FrameType | None = None,
) -> None:
    logger.info("received shutdown signal: signal=%s", shutdown_signal.name)
    scheduler.request_stop()


if __name__ == "__main__":
    asyncio.run(main())
