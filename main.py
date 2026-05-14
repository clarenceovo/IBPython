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
from src.transport.questdb_client import QuestDBClient
from src.transport.redis_client import MarketDataRedisClient
from src.transport.scheduler import GenericScheduler, IndexCompositionReloadJobHandler, MarketSnapshotJobHandler

logger = logging.getLogger(__name__)

PLACEHOLDER_INDEX_PROVIDER_NAMES = {"", "configured_provider", "placeholder", "todo"}


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


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
    configure_logging()
    settings = load_settings()
    logger.info(
        "starting scheduler worker: ibkr=%s:%s client_id=%s redis_url=%s redis_password_configured=%s "
        "questdb=%s:%s/%s index_provider=%r",
        settings.ibkr_host,
        settings.ibkr_port,
        settings.ibkr_client_id,
        settings.redis_url,
        bool(settings.redis_password),
        settings.questdb_host,
        settings.questdb_port,
        settings.questdb_database,
        settings.index_composition_provider,
    )

    redis = MarketDataRedisClient(settings.redis_url, password=settings.redis_password)
    questdb = QuestDBClient(
        host=settings.questdb_host,
        port=settings.questdb_port,
        user=settings.questdb_user,
        password=settings.questdb_password,
        database=settings.questdb_database,
    )
    pacing_guard = RedisIBKRHistoricalPacingGuard(redis)
    ibkr = IBKRFeedClient(
        host=settings.ibkr_host,
        port=settings.ibkr_port,
        client_id=settings.ibkr_client_id,
        pacing_guard=pacing_guard,
    )

    scheduler = GenericScheduler()
    loader = OHLCVLoader(ibkr, questdb=questdb, redis=redis)
    snapshot_handler = MarketSnapshotJobHandler(loader)
    scheduler.register_handler(snapshot_handler.job_type, snapshot_handler)

    index_provider = build_index_composition_provider(settings.index_composition_provider)
    if index_provider is not None:
        index_service = IndexCompositionService(index_provider, redis)
        index_handler = IndexCompositionReloadJobHandler(index_service, provider_name=index_provider.name)
        scheduler.register_handler(index_handler.job_type, index_handler)

    _install_signal_handlers(scheduler)

    async with redis:
        logger.info("connected Redis transport")
        jobs = await scheduler.load_jobs_from_redis(redis)
        if not jobs:
            logger.warning("no runnable Redis scheduler jobs found. Add keys like SchedulerJob::snapshot_spy_1m.")
            return

        needs_ibkr = _jobs_require_ibkr(jobs)
        needs_questdb = _jobs_require_questdb(jobs)
        logger.info(
            "scheduler dependency plan: jobs=%d needs_ibkr=%s needs_questdb=%s job_types=%s",
            len(jobs),
            needs_ibkr,
            needs_questdb,
            ",".join(sorted({job.job_type for job in jobs})),
        )

        async with AsyncExitStack() as stack:
            if needs_questdb:
                await stack.enter_async_context(questdb)
                logger.info("connected QuestDB transport")
                await questdb.create_market_ohlcv_table()
                logger.info("ensured QuestDB OHLCV table exists")
            else:
                logger.info("QuestDB connection skipped; no runnable job requires persistence")

            if needs_ibkr:
                await stack.enter_async_context(ibkr)
                logger.info("connected IBKR feed")
            else:
                logger.info("IBKR connection skipped; no runnable job requires IBKR")

            logger.info("scheduler entering run loop")
            await scheduler.run_forever()
            logger.info("scheduler run loop exited")


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


def _looks_like_index_provider(provider: Any) -> bool:
    fetch = getattr(provider, "fetch", None)
    return bool(getattr(provider, "name", None)) and callable(fetch) and inspect.iscoroutinefunction(fetch)


def _jobs_require_ibkr(jobs: list[Any]) -> bool:
    return any(job.job_type == MarketSnapshotJobHandler.job_type for job in jobs)


def _jobs_require_questdb(jobs: list[Any]) -> bool:
    return any(
        job.job_type == MarketSnapshotJobHandler.job_type and _job_param_bool(job.params.get("persist"), default=True)
        for job in jobs
    )


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
