from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.feeds.models import OHLCVRequest
from src.feeds.snapshotter import SnapshotWatchlist

logger = logging.getLogger(__name__)

JobHandler = Callable[["SchedulerJobDefinition"], Awaitable[None]]


class SchedulerJobDefinition(BaseModel):
    """Redis-serializable scheduler job definition."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    job_type: str = Field(min_length=1)
    interval_seconds: float = Field(gt=0)
    enabled: bool = True
    run_immediately: bool = True
    params: dict[str, Any] = Field(default_factory=dict)


class IndexCompositionReloadParams(BaseModel):
    """Parameters for Redis job_type='index_composition_reload' jobs."""

    model_config = ConfigDict(extra="forbid")

    index_symbols: tuple[str, ...] = Field(min_length=1)
    provider: str = Field(default="configured_provider", min_length=1)

    @field_validator("index_symbols", mode="before")
    @classmethod
    def normalize_index_symbols(cls, value: Any) -> tuple[str, ...]:
        if not isinstance(value, (list, tuple)):
            raise TypeError("index_symbols must be a list of symbols")
        normalized = tuple(str(symbol).strip().upper() for symbol in value if str(symbol).strip())
        if not normalized:
            raise ValueError("index_symbols must contain at least one symbol")
        return normalized


class GenericScheduler:
    """Async periodic scheduler with isolated job failures and graceful shutdown."""

    def __init__(self) -> None:
        self._handlers: dict[str, JobHandler] = {}
        self._jobs: dict[str, SchedulerJobDefinition] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._stop_event = asyncio.Event()

    def register_handler(self, job_type: str, handler: JobHandler) -> None:
        self._handlers[job_type] = handler
        logger.info("registered scheduler handler: job_type=%s", job_type)

    def add_job(self, job: SchedulerJobDefinition) -> None:
        if job.job_type not in self._handlers:
            raise KeyError(f"no handler registered for job_type={job.job_type!r}")
        self._jobs[job.name] = job
        logger.info(
            "registered scheduler job: name=%s job_type=%s interval_seconds=%s run_immediately=%s",
            job.name,
            job.job_type,
            job.interval_seconds,
            job.run_immediately,
        )

    def add_jobs(self, jobs: Iterable[SchedulerJobDefinition]) -> None:
        for job in jobs:
            self.add_job(job)

    async def load_jobs_from_redis(self, redis_client: object) -> list[SchedulerJobDefinition]:
        jobs: list[SchedulerJobDefinition] = []
        for key in await redis_client.scan_scheduler_jobs():
            payload = await redis_client.get_raw(key)
            if payload is None:
                logger.warning("scheduler job key disappeared before load: key=%s", key)
                continue
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            try:
                job = SchedulerJobDefinition.model_validate_json(payload)
            except Exception:
                logger.exception("invalid scheduler job payload skipped: key=%s", key)
                continue
            if not job.enabled:
                logger.info("disabled scheduler job skipped: name=%s job_type=%s", job.name, job.job_type)
                continue
            if job.job_type not in self._handlers:
                logger.warning(
                    "scheduler job skipped because no handler is registered: name=%s job_type=%s",
                    job.name,
                    job.job_type,
                )
                continue
            self.add_job(job)
            jobs.append(job)
        logger.info("loaded %d runnable scheduler job(s) from Redis", len(jobs))
        return jobs

    async def start(self) -> None:
        self._stop_event.clear()
        for job in self._jobs.values():
            if not job.enabled:
                continue
            task = asyncio.create_task(self._run_job_loop(job), name=f"scheduler:{job.name}")
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            logger.info("started scheduler task: name=%s job_type=%s", job.name, job.job_type)

    def request_stop(self) -> None:
        """Request a graceful shutdown from synchronous signal handlers."""

        logger.info("scheduler stop requested")
        self._stop_event.set()

    async def stop(self, *, drain_timeout: float = 10.0) -> None:
        """Signal all jobs to stop and wait for in-flight work to drain.

        Sets the stop event first so job loops exit naturally.  If any tasks
        are still running after *drain_timeout* seconds they are cancelled.
        """
        self._stop_event.set()
        if not self._tasks:
            return

        # Wait for tasks to finish naturally.
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._tasks, return_exceptions=True),
                timeout=drain_timeout,
            )
            logger.info("all scheduler tasks drained cleanly within %.1fs", drain_timeout)
        except TimeoutError:
            remaining = [t for t in self._tasks if not t.done()]
            if remaining:
                logger.warning(
                    "scheduler drain timed out after %.1fs; cancelling %d remaining tasks",
                    drain_timeout,
                    len(remaining),
                )
                for task in remaining:
                    task.cancel()
                await asyncio.gather(*remaining, return_exceptions=True)

        self._tasks.clear()

    async def run_forever(self) -> None:
        await self.start()
        try:
            await self._stop_event.wait()
        finally:
            await self.stop()

    async def _run_job_loop(self, job: SchedulerJobDefinition) -> None:
        if job.run_immediately:
            await self._run_once(job)

        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=job.interval_seconds)
            except TimeoutError:
                await self._run_once(job)

    async def _run_once(self, job: SchedulerJobDefinition) -> None:
        handler = self._handlers[job.job_type]
        try:
            logger.info("scheduled job starting: name=%s job_type=%s", job.name, job.job_type)
            await handler(job)
            logger.info("scheduled job finished: name=%s job_type=%s", job.name, job.job_type)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("scheduled job failed: %s", job.name)


class MarketSnapshotJobHandler:
    """Handler for Redis job_type='market_snapshot' jobs."""

    job_type = "market_snapshot"

    def __init__(self, loader: object, *, persist: bool = True, cache_latest: bool = True) -> None:
        self.loader = loader
        self.persist = persist
        self.cache_latest = cache_latest

    async def __call__(self, job: SchedulerJobDefinition) -> None:
        request_params = {key: value for key, value in job.params.items() if key not in {"persist", "cache_latest"}}
        request = OHLCVRequest.model_validate(request_params)
        persist = _coerce_bool(job.params.get("persist"), default=self.persist)
        cache_latest = _coerce_bool(job.params.get("cache_latest"), default=self.cache_latest)
        logger.info(
            "market snapshot loading: job=%s symbol=%s asset_class=%s bar_size=%s persist=%s cache_latest=%s",
            job.name,
            request.symbol,
            request.asset_class,
            request.bar_size,
            persist,
            cache_latest,
        )
        await self.loader.load(request, persist=persist, cache_latest=cache_latest)


class IndexCompositionReloadJobHandler:
    """Handler for Redis job_type='index_composition_reload' jobs."""

    job_type = "index_composition_reload"
    placeholder_provider_names = {"configured_provider", "placeholder", "todo"}

    def __init__(self, composition_service: object, *, provider_name: str | None = None) -> None:
        self.composition_service = composition_service
        service_provider = getattr(composition_service, "provider", None)
        self.provider_name = provider_name or getattr(service_provider, "name", None)

    async def __call__(self, job: SchedulerJobDefinition) -> None:
        params = IndexCompositionReloadParams.model_validate(job.params)
        if params.provider.lower() in self.placeholder_provider_names and (
            self.provider_name is None or self.provider_name.lower() in self.placeholder_provider_names
        ):
            raise RuntimeError(
                "index composition reload requires a configured production provider; "
                "IBKR does not expose index constituents/weights via TWS API"
            )
        logger.info(
            "index composition reload starting: job=%s provider=%s symbols=%s",
            job.name,
            params.provider,
            ",".join(params.index_symbols),
        )
        await self.composition_service.sync_many(params.index_symbols)


def _coerce_bool(value: Any, *, default: bool) -> bool:
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


class EquitySnapshotJobHandler:
    """Handler for Redis job_type='equity_snapshot' jobs.

    Periodically captures point-in-time snapshots for an equity watchlist
    and persists them to QuestDB + caches latest in Redis.

    Expected job params:
      - watchlist_name: str — name of a SnapshotWatchlist stored in Redis
      - persist: bool (default True)
      - cache_latest: bool (default True)
    """

    job_type = "equity_snapshot"

    def __init__(self, snapshot_router: Any) -> None:
        """
        Parameters
        ----------
        snapshot_router : module or object
            Must expose ``capture_snapshots(request, state)`` compatible with the
            FastAPI endpoint. In practice, import the router function directly.
        """
        self._capture = snapshot_router

    async def __call__(self, job: SchedulerJobDefinition) -> None:
        watchlist_name = job.params.get("watchlist_name", "")
        if not watchlist_name:
            raise ValueError("equity_snapshot job requires 'watchlist_name' param")
        # The handler needs access to feed/redis/questdb — it will be wired
        # at main.py level where the full app state is available.
        # For now, this handler is a placeholder that validates the job config.
        logger.info("equity snapshot job executed: job=%s watchlist=%s", job.name, watchlist_name)

        persist = _coerce_bool(job.params.get("persist"), default=True)
        cache_latest = _coerce_bool(job.params.get("cache_latest"), default=True)

        # Load watchlist from Redis (requires redis client to be injected)
        if not hasattr(self, "_redis") or self._redis is None:
            raise RuntimeError("EquitySnapshotJobHandler requires redis client (call wire_redis)")

        key_pattern = "SnapshotWatchlist::{name}"
        key = key_pattern.format(name=watchlist_name.strip().lower())
        payload = await self._redis.get_raw(key)
        if payload is None:
            raise ValueError(f"watchlist '{watchlist_name}' not found in Redis")
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        watchlist = SnapshotWatchlist.model_validate_json(payload)

        logger.info(
            "snapshotting watchlist: name=%s symbols=%d persist=%s cache=%s",
            watchlist.name,
            len(watchlist.symbols),
            persist,
            cache_latest,
        )

        # Delegate to the feed client directly for scheduler context
        if not hasattr(self, "_feed") or self._feed is None:
            raise RuntimeError("EquitySnapshotJobHandler requires feed client (call wire_feed)")

        from src.feeds.exchange_resolver import resolve_equity
        from src.feeds.snapshotter import ticker_to_snapshot, EquitySnapshot

        import time as _time
        t0 = _time.monotonic()
        snapshots: list[EquitySnapshot] = []
        failed: list[str] = []

        symbol_params = []
        for raw_sym in watchlist.symbols:
            resolved = resolve_equity(raw_sym)
            symbol_params.append((resolved.symbol, resolved.exchange, resolved.currency, resolved.primary_exchange, 0))

        tickers = await self._feed.capture_equity_snapshots(symbol_params)

        for i, ticker in enumerate(tickers):
            if i < len(symbol_params):
                s, ex, cur, pe, _ = symbol_params[i]
                try:
                    snap = ticker_to_snapshot(ticker, symbol=s, exchange=ex, currency=cur, primary_exchange=pe)
                    snapshots.append(snap)
                except Exception:
                    failed.append(s)

        await self._feed.cancel_equity_tickers(tickers)

        captured_symbols = {s.symbol for s in snapshots}
        for raw_sym in watchlist.symbols:
            resolved = resolve_equity(raw_sym)
            if resolved.symbol not in captured_symbols:
                failed.append(resolved.symbol)

        # Persist
        if persist and snapshots and hasattr(self, "_questdb") and self._questdb is not None:
            try:
                await self._questdb.insert_snapshots(snapshots)
                logger.info("persisted %d snapshots to QuestDB", len(snapshots))
            except Exception:
                logger.exception("failed to persist snapshots")

        # Cache
        if cache_latest and snapshots:
            for snap in snapshots:
                try:
                    await self._redis.set_latest_equity_snapshot(snap)
                except Exception:
                    pass

        duration = _time.monotonic() - t0
        logger.info(
            "equity snapshot complete: watchlist=%s captured=%d failed=%d duration=%.2fs",
            watchlist.name,
            len(snapshots),
            len(failed),
            duration,
        )

    def wire_feed(self, feed: Any) -> "EquitySnapshotJobHandler":
        self._feed = feed
        return self

    def wire_redis(self, redis: Any) -> "EquitySnapshotJobHandler":
        self._redis = redis
        return self

    def wire_questdb(self, questdb: Any) -> "EquitySnapshotJobHandler":
        self._questdb = questdb
        return self
