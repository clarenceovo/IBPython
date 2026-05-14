from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.feeds.models import OHLCVRequest

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

    def add_job(self, job: SchedulerJobDefinition) -> None:
        if job.job_type not in self._handlers:
            raise KeyError(f"no handler registered for job_type={job.job_type!r}")
        self._jobs[job.name] = job

    def add_jobs(self, jobs: Iterable[SchedulerJobDefinition]) -> None:
        for job in jobs:
            self.add_job(job)

    async def load_jobs_from_redis(self, redis_client: object) -> list[SchedulerJobDefinition]:
        jobs: list[SchedulerJobDefinition] = []
        for key in await redis_client.scan_scheduler_jobs():
            payload = await redis_client.get_raw(key)
            if payload is None:
                continue
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            job = SchedulerJobDefinition.model_validate_json(payload)
            if job.enabled:
                self.add_job(job)
                jobs.append(job)
        return jobs

    async def start(self) -> None:
        self._stop_event.clear()
        for job in self._jobs.values():
            if not job.enabled:
                continue
            task = asyncio.create_task(self._run_job_loop(job), name=f"scheduler:{job.name}")
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def stop(self) -> None:
        self._stop_event.set()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
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
            await handler(job)
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
        persist = bool(job.params.get("persist", self.persist))
        cache_latest = bool(job.params.get("cache_latest", self.cache_latest))
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
        await self.composition_service.sync_many(params.index_symbols)
