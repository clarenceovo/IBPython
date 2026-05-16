from __future__ import annotations

import asyncio
import contextvars
import hashlib
import json
import logging
import os
import random
import socket
import time as monotonic_time
import uuid
from collections.abc import Awaitable, Callable, Iterable
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.models import OHLCVRequest
from src.feeds.snapshotter import SnapshotWatchlist
from src.transport.redis_client import ohlcv_snapshot_calendar_key
from src.transport.scheduler_calendar import _parse_cron_expression, next_cron_run

logger = logging.getLogger(__name__)
execution_logger = logging.getLogger(f"{__name__}.execution")

JobHandler = Callable[["SchedulerJobDefinition"], Awaitable[Any]]

SCHEDULER_RUN_STATUSES = {
    "scheduled",
    "lease_skipped",
    "running",
    "skipped_window",
    "skipped_holiday",
    "success",
    "partial_success",
    "failed",
    "timeout",
    "cancelled",
    "disabled",
}


class SchedulerRunContext(BaseModel):
    """Context for one scheduler handler attempt."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    job_name: str
    job_type: str
    worker_id: str
    attempt: int
    job_payload_hash: str
    scheduled_at: datetime
    started_at: datetime


class SchedulerRunResult(BaseModel):
    """Structured result emitted by scheduler handlers and persisted to Redis."""

    model_config = ConfigDict(extra="allow")

    run_id: str | None = None
    job_name: str | None = None
    job_type: str | None = None
    worker_id: str | None = None
    scheduled_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: float | None = None
    status: str
    error: str | None = None
    attempts: int = 1
    metrics: dict[str, Any] = Field(default_factory=dict)
    next_run: datetime | None = None
    job_payload_hash: str | None = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in SCHEDULER_RUN_STATUSES:
            raise ValueError(f"unsupported scheduler run status: {value!r}")
        return normalized


_CURRENT_RUN_CONTEXT: contextvars.ContextVar[SchedulerRunContext | None] = contextvars.ContextVar(
    "scheduler_run_context",
    default=None,
)


def get_current_scheduler_run_context() -> SchedulerRunContext | None:
    """Return metadata for the currently executing scheduler run, if any."""

    return _CURRENT_RUN_CONTEXT.get()


class SchedulerJobDefinition(BaseModel):
    """Redis-serializable scheduler job definition."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    job_type: str = Field(min_length=1)
    interval_seconds: float | None = Field(default=None, gt=0)
    cron: str | None = Field(default=None, min_length=1)
    timezone: str | None = Field(default=None, min_length=1)
    enabled: bool = True
    run_immediately: bool = True
    timeout_seconds: float | None = Field(default=None, gt=0)
    max_attempts: int = Field(default=1, ge=1)
    retry_backoff_seconds: float = Field(default=0.0, ge=0)
    jitter_seconds: float = Field(default=0.0, ge=0)
    lease_ttl_seconds: float = Field(default=300.0, gt=0)
    misfire_policy: str = Field(default="run_next", min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, value: str | None) -> str | None:
        if value is None:
            return None
        _parse_cron_expression(value)
        return value.strip()

    @field_validator("timezone")
    @classmethod
    def validate_scheduler_timezone(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        ZoneInfo(normalized)
        return normalized

    @field_validator("misfire_policy")
    @classmethod
    def validate_misfire_policy(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"run_next", "skip"}:
            raise ValueError("misfire_policy must be 'run_next' or 'skip'")
        return normalized

    @model_validator(mode="after")
    def validate_schedule(self) -> "SchedulerJobDefinition":
        if self.interval_seconds is None and self.cron is None:
            raise ValueError("scheduler job requires interval_seconds or cron")
        return self


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


WEEKDAY_ALIASES = {
    "mon": 0,
    "monday": 0,
    "tue": 1,
    "tues": 1,
    "tuesday": 1,
    "wed": 2,
    "wednesday": 2,
    "thu": 3,
    "thur": 3,
    "thurs": 3,
    "thursday": 3,
    "fri": 4,
    "friday": 4,
    "sat": 5,
    "saturday": 5,
    "sun": 6,
    "sunday": 6,
}


class OHLCVSnapshotSymbol(BaseModel):
    """One symbol entry for job_type='ohlcv_snapshot' jobs."""

    model_config = ConfigDict(extra="allow")

    symbol: str = Field(min_length=1)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        if value is None:
            raise ValueError("symbol is required")
        return str(value).strip().upper()

    def request_overrides(self) -> dict[str, Any]:
        payload = self.model_dump(exclude_none=True)
        if self.model_extra:
            payload.update({key: value for key, value in self.model_extra.items() if value is not None})
        return payload


class OHLCVSnapshotParams(BaseModel):
    """Parameters for Redis/local job_type='ohlcv_snapshot' jobs."""

    model_config = ConfigDict(extra="forbid")

    start_time: time
    end_time: time
    timezone: str = Field(default="UTC", min_length=1)
    snap_interval_seconds: float = Field(gt=0)
    snap_days: tuple[int, ...] = (0, 1, 2, 3, 4)
    detect_holiday: bool = False
    capture_rth: bool = True
    defaults: dict[str, Any] = Field(default_factory=dict)
    symbols: tuple[OHLCVSnapshotSymbol, ...] = Field(min_length=1)

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def parse_wall_clock_time(cls, value: Any) -> time:
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            parts = value.strip().split(":")
            if len(parts) not in {2, 3}:
                raise ValueError("time must use HH:MM or HH:MM:SS")
            hour, minute = int(parts[0]), int(parts[1])
            second = int(parts[2]) if len(parts) == 3 else 0
            return time(hour=hour, minute=minute, second=second)
        raise TypeError("time fields must be time values or HH:MM strings")

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        normalized = value.strip()
        ZoneInfo(normalized)
        return normalized

    @field_validator("snap_days", mode="before")
    @classmethod
    def parse_snap_days(cls, value: Any) -> tuple[int, ...]:
        if value is None:
            return (0, 1, 2, 3, 4)
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("snap_days must be a list of weekday names or numbers")
        days: list[int] = []
        for item in value:
            if isinstance(item, int):
                day = item
            else:
                token = str(item).strip().lower()
                if token not in WEEKDAY_ALIASES:
                    raise ValueError(f"unsupported snap day: {item!r}")
                day = WEEKDAY_ALIASES[token]
            if day < 0 or day > 6:
                raise ValueError("snap day numbers must be between 0 and 6")
            if day not in days:
                days.append(day)
        if not days:
            raise ValueError("snap_days cannot be empty")
        return tuple(days)

    @field_validator("symbols", mode="before")
    @classmethod
    def normalize_symbols(cls, value: Any) -> tuple[OHLCVSnapshotSymbol, ...]:
        if not isinstance(value, (list, tuple)):
            raise TypeError("symbols must be a list")
        normalized = []
        for item in value:
            if isinstance(item, str):
                normalized.append({"symbol": item})
            else:
                normalized.append(item)
        return tuple(normalized)

    @model_validator(mode="after")
    def validate_defaults(self) -> "OHLCVSnapshotParams":
        if not self.defaults:
            raise ValueError("defaults cannot be empty")
        return self

    def validate_interval(self, job: "SchedulerJobDefinition") -> None:
        if job.interval_seconds is not None and abs(float(job.interval_seconds) - float(self.snap_interval_seconds)) > 1e-9:
            raise ValueError("snap_interval_seconds must match SchedulerJobDefinition.interval_seconds")


class GenericScheduler:
    """Async scheduler with isolated failures, Redis leases, and job reload support."""

    def __init__(
        self,
        *,
        redis_client: object | None = None,
        worker_id: str | None = None,
        local_job_directory: str | Path | None = None,
        job_reload_interval_seconds: float | None = None,
    ) -> None:
        self._handlers: dict[str, JobHandler] = {}
        self._jobs: dict[str, SchedulerJobDefinition] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._stop_event = asyncio.Event()
        self._running = False
        self._redis = redis_client
        self._worker_id = worker_id or _default_worker_id()
        self._local_job_directory = Path(local_job_directory) if local_job_directory is not None else None
        self._job_reload_interval_seconds = job_reload_interval_seconds
        self._reload_task: asyncio.Task[None] | None = None
        self.validation_warnings: list[str] = []

    def configure_job_sources(
        self,
        *,
        redis_client: object | None = None,
        local_job_directory: str | Path | None = None,
        job_reload_interval_seconds: float | None = None,
    ) -> None:
        """Configure sources used by the dynamic job reconciler."""

        if redis_client is not None:
            self._redis = redis_client
        if local_job_directory is not None:
            self._local_job_directory = Path(local_job_directory)
        if job_reload_interval_seconds is not None:
            if job_reload_interval_seconds <= 0:
                raise ValueError("job_reload_interval_seconds must be positive")
            self._job_reload_interval_seconds = job_reload_interval_seconds

    def register_handler(self, job_type: str, handler: JobHandler) -> None:
        self._handlers[job_type] = handler
        logger.info("registered scheduler handler: job_type=%s", job_type)

    def add_job(self, job: SchedulerJobDefinition) -> None:
        if job.job_type not in self._handlers:
            raise KeyError(f"no handler registered for job_type={job.job_type!r}")
        self._jobs[job.name] = job
        logger.info(
            "registered scheduler job: name=%s job_type=%s interval_seconds=%s cron=%s run_immediately=%s payload_hash=%s",
            job.name,
            job.job_type,
            job.interval_seconds,
            job.cron,
            job.run_immediately,
            _job_payload_hash(job),
        )
        if self._running:
            self._start_job_task(job)

    def add_jobs(self, jobs: Iterable[SchedulerJobDefinition]) -> None:
        for job in jobs:
            self.add_job(job)

    def jobs(self) -> list[SchedulerJobDefinition]:
        return list(self._jobs.values())

    async def load_jobs_from_directory(self, directory: str | Path) -> list[SchedulerJobDefinition]:
        jobs = await self._read_jobs_from_directory(directory)
        self.add_jobs(jobs)
        return jobs

    async def load_jobs_from_redis(self, redis_client: object) -> list[SchedulerJobDefinition]:
        jobs = await self._read_jobs_from_redis(redis_client)
        self.add_jobs(jobs)
        return jobs

    async def reload_jobs_from_sources(self) -> list[SchedulerJobDefinition]:
        """Reload configured local/Redis sources and reconcile running tasks.

        Local job files are deployable defaults. Redis jobs are live operational
        overrides and win when a job name exists in both sources.
        """

        self.validation_warnings.clear()
        merged: dict[str, SchedulerJobDefinition] = {}
        local_count = 0
        redis_count = 0
        if self._local_job_directory is not None:
            local_jobs = await self._read_jobs_from_directory(self._local_job_directory)
            local_count = len(local_jobs)
            merged.update({job.name: job for job in local_jobs})
        if self._redis is not None:
            redis_jobs = await self._read_jobs_from_redis(self._redis)
            redis_count = len(redis_jobs)
            merged.update({job.name: job for job in redis_jobs})

        await self._reconcile_jobs(merged)
        logger.info(
            "scheduler jobs reconciled: local_jobs=%d redis_jobs=%d active_jobs=%d",
            local_count,
            redis_count,
            len(self._jobs),
        )
        return list(self._jobs.values())

    async def _read_jobs_from_directory(self, directory: str | Path) -> list[SchedulerJobDefinition]:
        jobs: list[SchedulerJobDefinition] = []
        path = Path(directory)
        if not path.exists():
            logger.info("scheduler job directory not found: %s", path)
            return jobs
        for file_path in sorted(path.glob("*.json")):
            try:
                job = SchedulerJobDefinition.model_validate_json(file_path.read_text())
            except Exception:
                logger.exception("invalid local scheduler job payload skipped: path=%s", file_path)
                continue
            if not job.enabled:
                logger.info("disabled local scheduler job skipped: name=%s job_type=%s", job.name, job.job_type)
                continue
            if job.job_type not in self._handlers:
                warning = (
                    f"local scheduler job skipped because no handler is registered: "
                    f"name={job.name} job_type={job.job_type} path={file_path}"
                )
                self.validation_warnings.append(warning)
                logger.warning(warning)
                continue
            jobs.append(job)
        logger.info("loaded %d runnable scheduler job(s) from %s", len(jobs), path)
        return jobs

    async def _read_jobs_from_redis(self, redis_client: object) -> list[SchedulerJobDefinition]:
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
                warning = (
                    "scheduler job skipped because no handler is registered: "
                    f"name={job.name} job_type={job.job_type}"
                )
                self.validation_warnings.append(warning)
                logger.warning(warning)
                continue
            jobs.append(job)
        logger.info("loaded %d runnable scheduler job(s) from Redis", len(jobs))
        return jobs

    async def _reconcile_jobs(self, desired_jobs: dict[str, SchedulerJobDefinition]) -> None:
        for removed_name in sorted(set(self._jobs) - set(desired_jobs)):
            logger.info("scheduler job removed or disabled: name=%s", removed_name)
            await self._cancel_job_task(removed_name)
            self._jobs.pop(removed_name, None)

        for name, desired in desired_jobs.items():
            current = self._jobs.get(name)
            current_hash = _job_payload_hash(current) if current is not None else None
            desired_hash = _job_payload_hash(desired)
            if current is not None and current_hash == desired_hash:
                continue
            if current is not None:
                logger.info(
                    "scheduler job changed; restarting task: name=%s old_hash=%s new_hash=%s",
                    name,
                    current_hash,
                    desired_hash,
                )
                await self._cancel_job_task(name)
            self._jobs[name] = desired
            if self._running and desired.enabled:
                self._start_job_task(desired)

    async def start(self) -> None:
        self._running = True
        self._stop_event.clear()
        for job in self._jobs.values():
            if not job.enabled:
                continue
            self._start_job_task(job)
        if self._job_reload_interval_seconds is not None and (
            self._local_job_directory is not None or self._redis is not None
        ):
            self._reload_task = asyncio.create_task(self._reload_job_loop(), name="scheduler:job_reload")
            logger.info(
                "started scheduler job reload loop: interval_seconds=%.3f",
                self._job_reload_interval_seconds,
            )

    def _start_job_task(self, job: SchedulerJobDefinition) -> None:
        existing = self._tasks.get(job.name)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(self._run_job_loop(job), name=f"scheduler:{job.name}")
        self._tasks[job.name] = task

        def _discard(done_task: asyncio.Task[None], *, job_name: str = job.name) -> None:
            if self._tasks.get(job_name) is done_task:
                self._tasks.pop(job_name, None)
            if done_task.cancelled():
                return
            try:
                exc = done_task.exception()
            except asyncio.CancelledError:
                return
            if exc is not None:
                logger.error(
                    "scheduler task exited with error: name=%s",
                    job_name,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )

        task.add_done_callback(_discard)
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
        tasks = list(self._tasks.values())
        if self._reload_task is not None:
            self._reload_task.cancel()
            tasks.append(self._reload_task)
        if not tasks:
            self._running = False
            return

        # Wait for tasks to finish naturally.
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=drain_timeout,
            )
            logger.info("all scheduler tasks drained cleanly within %.1fs", drain_timeout)
        except TimeoutError:
            remaining = [t for t in tasks if not t.done()]
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
        self._reload_task = None
        self._running = False

    async def run_forever(self) -> None:
        await self.start()
        try:
            await self._stop_event.wait()
        finally:
            await self.stop()

    async def _reload_job_loop(self) -> None:
        assert self._job_reload_interval_seconds is not None
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._job_reload_interval_seconds)
            except TimeoutError:
                try:
                    await self.reload_jobs_from_sources()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("scheduler job reload failed")

    async def _run_job_loop(self, job: SchedulerJobDefinition) -> None:
        if job.run_immediately:
            await self._run_once(job)
        if job.cron is not None:
            await self._run_cron_loop(job)
            return
        if job.interval_seconds is None:
            raise ValueError(f"interval_seconds missing for interval job {job.name!r}")

        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=job.interval_seconds)
            except TimeoutError:
                await self._run_once(job)

    async def _run_cron_loop(self, job: SchedulerJobDefinition) -> None:
        if job.cron is None:
            return
        while not self._stop_event.is_set():
            now = _scheduler_now(job)
            next_run = next_cron_run(job.cron, now)
            wait_seconds = max(0.0, (next_run - now).total_seconds())
            execution_logger.info(
                "job_state=cron_wait job=%s cron=%s timezone=%s next_run=%s wait_seconds=%.3f",
                job.name,
                job.cron,
                job.timezone or job.params.get("timezone") or "UTC",
                next_run.isoformat(),
                wait_seconds,
            )
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait_seconds)
            except TimeoutError:
                await self._run_once(job)

    async def _run_once(self, job: SchedulerJobDefinition) -> None:
        handler = self._handlers[job.job_type]
        scheduled_at = datetime.now(timezone.utc)
        run_id = uuid.uuid4().hex
        payload_hash = _job_payload_hash(job)
        lease_token = f"{self._worker_id}:{run_id}"

        if job.jitter_seconds > 0:
            await asyncio.sleep(random.uniform(0.0, job.jitter_seconds))

        if not await self._acquire_job_lease(job, lease_token):
            result = SchedulerRunResult(
                run_id=run_id,
                job_name=job.name,
                job_type=job.job_type,
                worker_id=self._worker_id,
                scheduled_at=scheduled_at,
                started_at=scheduled_at,
                finished_at=datetime.now(timezone.utc),
                duration_ms=0.0,
                status="lease_skipped",
                attempts=0,
                metrics={"lease_ttl_seconds": job.lease_ttl_seconds},
                next_run=_next_run_hint(job),
                job_payload_hash=payload_hash,
            )
            await self._record_run_result(job, result)
            execution_logger.info(
                "job_state=lease_skipped job=%s job_type=%s run_id=%s worker_id=%s payload_hash=%s",
                job.name,
                job.job_type,
                run_id,
                self._worker_id,
                payload_hash,
            )
            return

        final_result: SchedulerRunResult | None = None
        try:
            for attempt in range(1, job.max_attempts + 1):
                started_at = datetime.now(timezone.utc)
                run_context = SchedulerRunContext(
                    run_id=run_id,
                    job_name=job.name,
                    job_type=job.job_type,
                    worker_id=self._worker_id,
                    attempt=attempt,
                    job_payload_hash=payload_hash,
                    scheduled_at=scheduled_at,
                    started_at=started_at,
                )
                running_result = SchedulerRunResult(
                    run_id=run_id,
                    job_name=job.name,
                    job_type=job.job_type,
                    worker_id=self._worker_id,
                    scheduled_at=scheduled_at,
                    started_at=started_at,
                    status="running",
                    attempts=attempt,
                    metrics={"attempt": attempt, "max_attempts": job.max_attempts},
                    next_run=_next_run_hint(job),
                    job_payload_hash=payload_hash,
                )
                await self._record_run_result(job, running_result)
                token = _CURRENT_RUN_CONTEXT.set(run_context)
                start_monotonic = monotonic_time.monotonic()
                try:
                    execution_logger.info(
                        "job_state=running job=%s job_type=%s run_id=%s worker_id=%s attempt=%d max_attempts=%d",
                        job.name,
                        job.job_type,
                        run_id,
                        self._worker_id,
                        attempt,
                        job.max_attempts,
                    )
                    raw_result = await asyncio.wait_for(handler(job), timeout=job.timeout_seconds)
                    final_result = _coerce_run_result(raw_result)
                    break
                except TimeoutError as exc:
                    final_result = SchedulerRunResult(status="timeout", error=str(exc) or "job timed out", attempts=attempt)
                    logger.exception("scheduled job timed out: name=%s attempt=%d", job.name, attempt)
                except asyncio.CancelledError:
                    final_result = SchedulerRunResult(status="cancelled", error="scheduler task cancelled", attempts=attempt)
                    final_result = _finalize_run_result(
                        final_result,
                        job=job,
                        run_id=run_id,
                        worker_id=self._worker_id,
                        scheduled_at=scheduled_at,
                        started_at=started_at,
                        duration_ms=(monotonic_time.monotonic() - start_monotonic) * 1000.0,
                        job_payload_hash=payload_hash,
                    )
                    await self._record_run_result(job, final_result)
                    raise
                except Exception as exc:
                    final_result = SchedulerRunResult(status="failed", error=f"{type(exc).__name__}: {exc}", attempts=attempt)
                    logger.exception("scheduled job failed: name=%s attempt=%d", job.name, attempt)
                finally:
                    _CURRENT_RUN_CONTEXT.reset(token)

                if final_result.status in {"timeout", "failed"} and attempt < job.max_attempts:
                    sleep_seconds = job.retry_backoff_seconds * attempt
                    execution_logger.warning(
                        "job_state=retry_wait job=%s run_id=%s attempt=%d next_attempt=%d sleep_seconds=%.3f",
                        job.name,
                        run_id,
                        attempt,
                        attempt + 1,
                        sleep_seconds,
                    )
                    if sleep_seconds > 0:
                        await asyncio.sleep(sleep_seconds)

            if final_result is None:
                final_result = SchedulerRunResult(status="success")
            finished_at = datetime.now(timezone.utc)
            started_at = final_result.started_at or scheduled_at
            duration_ms = (
                final_result.duration_ms
                if final_result.duration_ms is not None
                else max(0.0, (finished_at - started_at).total_seconds() * 1000.0)
            )
            final_result = _finalize_run_result(
                final_result,
                job=job,
                run_id=run_id,
                worker_id=self._worker_id,
                scheduled_at=scheduled_at,
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=duration_ms,
                job_payload_hash=payload_hash,
            )
            await self._record_run_result(job, final_result)
            execution_logger.info(
                "job_state=%s job=%s job_type=%s run_id=%s worker_id=%s attempts=%d duration_ms=%.3f metrics=%s error=%s",
                final_result.status,
                job.name,
                job.job_type,
                run_id,
                self._worker_id,
                final_result.attempts,
                final_result.duration_ms or 0.0,
                json.dumps(final_result.metrics, sort_keys=True, default=str),
                final_result.error,
            )
        finally:
            await self._release_job_lease(job, lease_token)

    async def _cancel_job_task(self, job_name: str) -> None:
        task = self._tasks.pop(job_name, None)
        if task is None or task.done():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _acquire_job_lease(self, job: SchedulerJobDefinition, token: str) -> bool:
        if self._redis is None:
            return True
        if hasattr(self._redis, "acquire_scheduler_lease"):
            try:
                return bool(await self._redis.acquire_scheduler_lease(job.name, token, ttl_seconds=job.lease_ttl_seconds))
            except Exception:
                logger.exception("failed to acquire scheduler lease; skipping run to avoid duplicate work: job=%s", job.name)
                return False
        return True

    async def _release_job_lease(self, job: SchedulerJobDefinition, token: str) -> None:
        if self._redis is None:
            return
        if hasattr(self._redis, "release_scheduler_lease"):
            try:
                await self._redis.release_scheduler_lease(job.name, token)
            except Exception:
                logger.exception("failed to release scheduler lease: job=%s", job.name)

    async def _record_run_result(self, job: SchedulerJobDefinition, result: SchedulerRunResult) -> None:
        if self._redis is None:
            return
        if hasattr(self._redis, "record_scheduler_run"):
            try:
                await self._redis.record_scheduler_run(job.name, result.model_dump(mode="json", exclude_none=True))
            except Exception:
                logger.exception(
                    "failed to record scheduler run: job=%s run_id=%s status=%s",
                    job.name,
                    result.run_id,
                    result.status,
                )


def _default_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _job_payload_hash(job: SchedulerJobDefinition | None) -> str:
    if job is None:
        return ""
    payload = job.model_dump(mode="json", exclude_none=True)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def _coerce_run_result(value: Any) -> SchedulerRunResult:
    if value is None:
        return SchedulerRunResult(status="success")
    if isinstance(value, SchedulerRunResult):
        return value
    if isinstance(value, dict):
        return SchedulerRunResult.model_validate(value)
    return SchedulerRunResult(status="success", metrics={"handler_result": str(value)})


def _finalize_run_result(
    result: SchedulerRunResult,
    *,
    job: SchedulerJobDefinition,
    run_id: str,
    worker_id: str,
    scheduled_at: datetime,
    started_at: datetime,
    job_payload_hash: str,
    finished_at: datetime | None = None,
    duration_ms: float | None = None,
) -> SchedulerRunResult:
    finished = finished_at or result.finished_at or datetime.now(timezone.utc)
    duration = duration_ms
    if duration is None:
        duration = result.duration_ms
    if duration is None:
        duration = max(0.0, (finished - started_at).total_seconds() * 1000.0)
    return result.model_copy(
        update={
            "run_id": result.run_id or run_id,
            "job_name": result.job_name or job.name,
            "job_type": result.job_type or job.job_type,
            "worker_id": result.worker_id or worker_id,
            "scheduled_at": result.scheduled_at or scheduled_at,
            "started_at": result.started_at or started_at,
            "finished_at": finished,
            "duration_ms": duration,
            "attempts": max(1, result.attempts),
            "next_run": result.next_run or _next_run_hint(job),
            "job_payload_hash": result.job_payload_hash or job_payload_hash,
        }
    )


def _next_run_hint(job: SchedulerJobDefinition) -> datetime | None:
    try:
        if job.cron is not None:
            return next_cron_run(job.cron, _scheduler_now(job))
        if job.interval_seconds is not None:
            return datetime.now(timezone.utc) + timedelta(seconds=job.interval_seconds)
    except Exception:
        return None
    return None


def _contract_fingerprint(request: OHLCVRequest) -> str:
    identity = {
        "asset_class": str(request.asset_class),
        "symbol": request.symbol,
        "exchange": request.exchange,
        "currency": request.currency,
        "primary_exchange": request.primary_exchange,
        "last_trade_date_or_contract_month": request.last_trade_date_or_contract_month,
        "multiplier": request.multiplier,
        "local_symbol": request.local_symbol,
        "sec_id_type": request.sec_id_type,
        "sec_id": request.sec_id,
        "con_id": request.con_id,
    }
    payload = json.dumps(identity, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class MarketSnapshotJobHandler:
    """Handler for Redis job_type='market_snapshot' jobs."""

    job_type = "market_snapshot"

    def __init__(self, loader: object, *, persist: bool = True, cache_latest: bool = True) -> None:
        self.loader = loader
        self.persist = persist
        self.cache_latest = cache_latest

    async def __call__(self, job: SchedulerJobDefinition) -> SchedulerRunResult:
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
        bars = await self.loader.load(request, persist=persist, cache_latest=cache_latest)
        return SchedulerRunResult(status="success", metrics={"bars_captured": len(bars or [])})


class OHLCVSnapshotJobHandler:
    """Handler for multi-symbol job_type='ohlcv_snapshot' jobs."""

    job_type = "ohlcv_snapshot"

    def __init__(
        self,
        loader: object,
        *,
        redis: object | None = None,
        feed: object | None = None,
        clock: Callable[[], datetime] | None = None,
        max_concurrency: int = 8,
    ) -> None:
        self.loader = loader
        self.redis = redis
        self.feed = feed or getattr(loader, "feed", None)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.max_concurrency = max(1, max_concurrency)

    async def __call__(self, job: SchedulerJobDefinition) -> SchedulerRunResult:
        params = OHLCVSnapshotParams.model_validate(job.params)
        params.validate_interval(job)
        now_local = self.clock().astimezone(ZoneInfo(params.timezone))
        run_context = get_current_scheduler_run_context()
        execution_logger.info(
            "job_state=evaluating job=%s job_type=%s run_id=%s now=%s timezone=%s symbols=%d",
            job.name,
            job.job_type,
            run_context.run_id if run_context else None,
            now_local.isoformat(),
            params.timezone,
            len(params.symbols),
        )
        if not _is_runnable_window(params, now_local):
            execution_logger.info(
                "job_state=skipped_window reason=outside_schedule job=%s job_type=%s run_id=%s now=%s timezone=%s",
                job.name,
                job.job_type,
                run_context.run_id if run_context else None,
                now_local.isoformat(),
                params.timezone,
            )
            return SchedulerRunResult(
                status="skipped_window",
                metrics={
                    "symbols_total": len(params.symbols),
                    "timezone": params.timezone,
                    "now_local": now_local.isoformat(),
                },
            )

        estimated_requests = len(params.symbols)
        if estimated_requests > self.max_concurrency:
            execution_logger.info(
                "job_state=pacing_estimate job=%s run_id=%s estimated_historical_requests=%d max_concurrency=%d",
                job.name,
                run_context.run_id if run_context else None,
                estimated_requests,
                self.max_concurrency,
            )

        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def run_one(symbol: OHLCVSnapshotSymbol) -> dict[str, Any]:
            async with semaphore:
                return await self._run_symbol(job, params, symbol, now_local)

        results = await asyncio.gather(*(run_one(symbol) for symbol in params.symbols))
        success_count = sum(1 for result in results if result["status"] == "success")
        skipped_count = sum(1 for result in results if result["status"] == "skipped_holiday")
        failed_count = sum(1 for result in results if result["status"] == "failed")
        bars_captured = sum(int(result.get("bars_captured", 0)) for result in results)
        if failed_count == len(results):
            status = "failed"
        elif failed_count > 0:
            status = "partial_success"
        elif skipped_count == len(results):
            status = "skipped_holiday"
        else:
            status = "success"
        return SchedulerRunResult(
            status=status,
            error=f"{failed_count} symbol(s) failed" if failed_count else None,
            metrics={
                "symbols_total": len(results),
                "symbols_success": success_count,
                "symbols_failed": failed_count,
                "symbols_skipped_holiday": skipped_count,
                "bars_captured": bars_captured,
                "estimated_historical_requests": estimated_requests,
                "max_concurrency": self.max_concurrency,
            },
        )

    async def _run_symbol(
        self,
        job: SchedulerJobDefinition,
        params: OHLCVSnapshotParams,
        symbol: OHLCVSnapshotSymbol,
        now_local: datetime,
    ) -> dict[str, Any]:
        started = monotonic_time.monotonic()
        run_context = get_current_scheduler_run_context()
        request_payload = {**params.defaults, **symbol.request_overrides()}
        persist = _coerce_bool(request_payload.pop("persist", None), default=True)
        cache_latest = _coerce_bool(request_payload.pop("cache_latest", None), default=True)
        request_payload["use_rth"] = params.capture_rth

        try:
            request = OHLCVRequest.model_validate(request_payload)
            bookmark = await self._read_bookmark(job, request)
            if bookmark is not None and request.start_datetime is None:
                request = request.model_copy(update={"start_datetime": bookmark})
                execution_logger.info(
                    "job_state=bookmark_loaded job=%s symbol=%s bar_size=%s start_datetime=%s",
                    job.name,
                    request.symbol,
                    request.bar_size,
                    bookmark.isoformat(),
                )

            if params.detect_holiday:
                execution_logger.info(
                    "job_state=calendar_check job=%s symbol=%s date=%s use_rth=%s",
                    job.name,
                    request.symbol,
                    now_local.date().isoformat(),
                    params.capture_rth,
                )
                has_session = await self._has_trading_session(request, now_local.date(), params.capture_rth)
                if not has_session:
                    await self._safe_write_status(
                        job,
                        request,
                        {
                            "status": "skipped_holiday",
                            "bars_captured": 0,
                            "last_run_at": self.clock().astimezone(timezone.utc).isoformat(),
                            "duration_ms": (monotonic_time.monotonic() - started) * 1000.0,
                        },
                    )
                    execution_logger.info(
                        "job_state=skipped_holiday reason=no_trading_session job=%s run_id=%s symbol=%s date=%s",
                        job.name,
                        run_context.run_id if run_context else None,
                        request.symbol,
                        now_local.date().isoformat(),
                    )
                    return {"status": "skipped_holiday", "symbol": request.symbol, "bars_captured": 0}

            execution_logger.info(
                "job_state=started job=%s run_id=%s symbol=%s asset_class=%s bar_size=%s persist=%s cache_latest=%s",
                job.name,
                run_context.run_id if run_context else None,
                request.symbol,
                request.asset_class,
                request.bar_size,
                persist,
                cache_latest,
            )
            bars = await self.loader.load(request, persist=persist, cache_latest=cache_latest)
            if bars:
                latest_timestamp = max(bar.timestamp for bar in bars)
                await self._write_bookmark(job, request, latest_timestamp)
                execution_logger.info(
                    "job_state=bookmark_updated job=%s symbol=%s bar_size=%s last_ts=%s",
                    job.name,
                    request.symbol,
                    request.bar_size,
                    latest_timestamp.isoformat(),
                )
            await self._safe_write_status(
                job,
                request,
                {
                    "status": "success",
                    "bars_captured": len(bars),
                    "last_run_at": self.clock().astimezone(timezone.utc).isoformat(),
                    "latest_bar_timestamp": max((bar.timestamp for bar in bars), default=None),
                    "duration_ms": (monotonic_time.monotonic() - started) * 1000.0,
                },
            )
            execution_logger.info(
                "job_state=success job=%s run_id=%s symbol=%s bar_size=%s bars_captured=%d",
                job.name,
                run_context.run_id if run_context else None,
                request.symbol,
                request.bar_size,
                len(bars),
            )
            return {"status": "success", "symbol": request.symbol, "bars_captured": len(bars)}
        except Exception as exc:
            execution_logger.exception(
                "job_state=failed job=%s run_id=%s symbol=%s error=%s",
                job.name,
                run_context.run_id if run_context else None,
                symbol.symbol,
                exc,
            )
            await self._safe_write_status(
                job,
                request_payload,
                {
                    "status": "failed",
                    "bars_captured": 0,
                    "last_run_at": self.clock().astimezone(timezone.utc).isoformat(),
                    "error": str(exc),
                    "duration_ms": (monotonic_time.monotonic() - started) * 1000.0,
                },
            )
            return {"status": "failed", "symbol": symbol.symbol, "bars_captured": 0, "error": str(exc)}

    async def _read_bookmark(self, job: SchedulerJobDefinition, request: OHLCVRequest) -> datetime | None:
        if self.redis is None or not hasattr(self.redis, "get_ohlcv_snapshot_last_ts"):
            return None
        return await self.redis.get_ohlcv_snapshot_last_ts(job.name, request.symbol, request.bar_size)

    async def _write_bookmark(self, job: SchedulerJobDefinition, request: OHLCVRequest, timestamp: datetime) -> None:
        if self.redis is None or not hasattr(self.redis, "set_ohlcv_snapshot_last_ts"):
            return
        await self.redis.set_ohlcv_snapshot_last_ts(job.name, request.symbol, request.bar_size, timestamp)

    async def _write_status(
        self,
        job: SchedulerJobDefinition,
        request: OHLCVRequest | dict[str, Any],
        status: dict[str, Any],
    ) -> None:
        if self.redis is None or not hasattr(self.redis, "set_ohlcv_snapshot_status"):
            return
        symbol = request.symbol if isinstance(request, OHLCVRequest) else str(request.get("symbol", "UNKNOWN"))
        bar_size = request.bar_size if isinstance(request, OHLCVRequest) else str(request.get("bar_size", "unknown"))
        asset_class = str(request.asset_class) if isinstance(request, OHLCVRequest) else str(request.get("asset_class", "unknown"))
        payload = {
            "job_name": job.name,
            "symbol": symbol,
            "asset_class": asset_class,
            "bar_size": bar_size,
            **status,
        }
        run_context = get_current_scheduler_run_context()
        if run_context is not None:
            payload.update(
                {
                    "run_id": run_context.run_id,
                    "worker_id": run_context.worker_id,
                    "attempt": run_context.attempt,
                    "job_payload_hash": run_context.job_payload_hash,
                }
            )
        await self.redis.set_ohlcv_snapshot_status(job.name, symbol, bar_size, payload)

    async def _safe_write_status(
        self,
        job: SchedulerJobDefinition,
        request: OHLCVRequest | dict[str, Any],
        status: dict[str, Any],
    ) -> None:
        try:
            await self._write_status(job, request, status)
        except Exception:
            execution_logger.exception(
                "job_state=status_write_failed job=%s status=%s",
                job.name,
                status.get("status"),
            )

    async def _has_trading_session(self, request: OHLCVRequest, ref_date: date, use_rth: bool) -> bool:
        cache_key = ohlcv_snapshot_calendar_key(
            asset_class=request.asset_class,
            exchange=request.exchange,
            symbol=request.symbol,
            date_value=ref_date.isoformat(),
            use_rth=use_rth,
            contract_fingerprint=_contract_fingerprint(request),
        )
        if self.redis is not None and hasattr(self.redis, "get_raw"):
            try:
                cached = await self.redis.get_raw(cache_key)
                if cached is not None:
                    if isinstance(cached, bytes):
                        cached = cached.decode("utf-8")
                    return str(cached).strip().lower() == "true"
            except Exception:
                execution_logger.exception(
                    "job_state=calendar_cache_read_failed symbol=%s cache_key=%s",
                    request.symbol,
                    cache_key,
                )

        if self.feed is None or not hasattr(self.feed, "load_trading_schedule"):
            logger.warning("holiday detection requested but feed has no load_trading_schedule; assuming session exists")
            return True

        sessions = await self.feed.load_trading_schedule(request, ref_date=ref_date, use_rth=use_rth)
        has_session = bool(sessions)
        if self.redis is not None and hasattr(self.redis, "set_raw"):
            try:
                await self.redis.set_raw(cache_key, str(has_session).lower(), ex=86_400)
            except Exception:
                execution_logger.exception(
                    "job_state=calendar_cache_write_failed symbol=%s cache_key=%s",
                    request.symbol,
                    cache_key,
                )
        return has_session


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


def _is_runnable_window(params: OHLCVSnapshotParams, now_local: datetime) -> bool:
    if now_local.weekday() not in params.snap_days:
        return False
    current = now_local.time().replace(tzinfo=None)
    if params.start_time <= params.end_time:
        return params.start_time <= current < params.end_time
    return current >= params.start_time or current < params.end_time


def _scheduler_now(job: SchedulerJobDefinition) -> datetime:
    tz_name = job.timezone or str(job.params.get("timezone") or "UTC")
    return datetime.now(ZoneInfo(tz_name))


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
