from __future__ import annotations

import asyncio
import logging

from src.config.settings import Settings
from src.feeds.index_composition import (
    IndexCompositionProvider,
    IndexCompositionService,
    PlaceholderIndexCompositionProvider,
)
from src.feeds.ibkr_feed import IBKRFeedClient
from src.feeds.ohlcv_loader import OHLCVLoader
from src.transport.ibkr_rate_limit import RedisIBKRHistoricalPacingGuard
from src.transport.questdb_client import QuestDBClient
from src.transport.redis_client import MarketDataRedisClient
from src.transport.scheduler import GenericScheduler, IndexCompositionReloadJobHandler, MarketSnapshotJobHandler


def build_index_composition_provider(provider_name: str) -> IndexCompositionProvider:
    """Return a production index composition provider when one is configured."""

    if not provider_name.strip():
        return PlaceholderIndexCompositionProvider()
    raise RuntimeError(
        f"Unsupported INDEX_COMPOSITION_PROVIDER={provider_name!r}. "
        "IBKR does not expose index constituents/weights; configure a dedicated provider implementation."
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings()

    redis = MarketDataRedisClient(settings.redis_url)
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
    index_service = IndexCompositionService(index_provider, redis)
    index_handler = IndexCompositionReloadJobHandler(index_service, provider_name=index_provider.name)
    scheduler.register_handler(index_handler.job_type, index_handler)

    async with redis:
        jobs = await scheduler.load_jobs_from_redis(redis)
        if not jobs:
            logging.warning("No Redis scheduler jobs found. Add keys like SchedulerJob::snapshot_spy_1m.")
            return
        async with questdb:
            await questdb.create_market_ohlcv_table()
            async with ibkr:
                logging.info("Loaded %s scheduler job(s)", len(jobs))
                await scheduler.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
