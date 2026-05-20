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

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.models import OHLCVRequest
from src.feeds.ohlcv_loader import estimate_expected_bars
from src.feeds.snapshotter import SnapshotWatchlist
from src.transport.redis_client import ohlcv_snapshot_calendar_key
from src.transport.scheduler_calendar import _parse_cron_expression, next_cron_run

logger = logging.getLogger(__name__)
execution_logger = logging.getLogger(f"{__name__}.execution")

JobHandler = Callable[["SchedulerJobDefinition"], Awaitable[Any]]

from src.transport.scheduler_models import (  # noqa: F401
    SCHEDULER_RUN_STATUSES,
    SchedulerJobDefinition,
    SchedulerRunContext,
    SchedulerRunResult,
    _CURRENT_RUN_CONTEXT,
    get_current_scheduler_run_context,
    IndexCompositionReloadParams,
    OHLCVSnapshotParams,
    OHLCVSnapshotSymbol,
    WEEKDAY_ALIASES,
)
from src.transport.scheduler_handlers import (  # noqa: F401
    MarketSnapshotJobHandler,
    OHLCVSnapshotJobHandler,
    IndexCompositionReloadJobHandler,
    EquitySnapshotJobHandler,
    _contract_fingerprint,
    _loader_quality_summary,
)


class GenericScheduler:
    """Async scheduler with isolated failures, Redis leases, and job reload support."""

    def __init__(
        self,
        *,
        redis_client: object | None = None,
        worker_id: str | None = None,
        local_job_directory: str | Path | None = None,
        job_reload_interval_seconds: float | None = None,
        health_monitor: object | None = None,
        redis_job_load_timeout_seconds: float = 5.0,
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
        self._health_monitor = health_monitor
        self._redis_job_load_timeout_seconds = max(0.1, float(redis_job_load_timeout_seconds))
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
        try:
            keys = await asyncio.wait_for(
                redis_client.scan_scheduler_jobs(),
                timeout=self._redis_job_load_timeout_seconds,
            )
        except TimeoutError:
            logger.warning(
                "scheduler Redis job scan timed out after %.1fs; continuing with already loaded/local jobs",
                self._redis_job_load_timeout_seconds,
            )
            return jobs

        for key in keys:
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
            if self._health_monitor is not None:
                try:
                    self._health_monitor.record_result(
                        job_name=job.name,
                        job_type=job.job_type,
                        status=final_result.status,
                        error=final_result.error,
                    )
                except Exception:
                    execution_logger.exception("health monitor update failed for job=%s", job.name)
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

