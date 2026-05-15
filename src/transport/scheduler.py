from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.models import OHLCVRequest
from src.feeds.snapshotter import SnapshotWatchlist
from src.transport.redis_client import ohlcv_snapshot_calendar_key

logger = logging.getLogger(__name__)
execution_logger = logging.getLogger(f"{__name__}.execution")

JobHandler = Callable[["SchedulerJobDefinition"], Awaitable[None]]


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
            "registered scheduler job: name=%s job_type=%s interval_seconds=%s cron=%s run_immediately=%s",
            job.name,
            job.job_type,
            job.interval_seconds,
            job.cron,
            job.run_immediately,
        )

    def add_jobs(self, jobs: Iterable[SchedulerJobDefinition]) -> None:
        for job in jobs:
            self.add_job(job)

    def jobs(self) -> list[SchedulerJobDefinition]:
        return list(self._jobs.values())

    async def load_jobs_from_directory(self, directory: str | Path) -> list[SchedulerJobDefinition]:
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
                logger.warning(
                    "local scheduler job skipped because no handler is registered: name=%s job_type=%s path=%s",
                    job.name,
                    job.job_type,
                    file_path,
                )
                continue
            self.add_job(job)
            jobs.append(job)
        logger.info("loaded %d runnable scheduler job(s) from %s", len(jobs), path)
        return jobs

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

    async def __call__(self, job: SchedulerJobDefinition) -> None:
        params = OHLCVSnapshotParams.model_validate(job.params)
        params.validate_interval(job)
        now_local = self.clock().astimezone(ZoneInfo(params.timezone))
        execution_logger.info(
            "job_state=evaluating job=%s job_type=%s now=%s timezone=%s symbols=%d",
            job.name,
            job.job_type,
            now_local.isoformat(),
            params.timezone,
            len(params.symbols),
        )
        if not _is_runnable_window(params, now_local):
            execution_logger.info(
                "job_state=skipped reason=outside_schedule job=%s job_type=%s now=%s timezone=%s",
                job.name,
                job.job_type,
                now_local.isoformat(),
                params.timezone,
            )
            return

        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def run_one(symbol: OHLCVSnapshotSymbol) -> None:
            async with semaphore:
                await self._run_symbol(job, params, symbol, now_local)

        await asyncio.gather(*(run_one(symbol) for symbol in params.symbols))

    async def _run_symbol(
        self,
        job: SchedulerJobDefinition,
        params: OHLCVSnapshotParams,
        symbol: OHLCVSnapshotSymbol,
        now_local: datetime,
    ) -> None:
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
                    await self._write_status(
                        job,
                        request,
                        {
                            "status": "skipped_holiday",
                            "bars_captured": 0,
                            "last_run_at": self.clock().astimezone(timezone.utc).isoformat(),
                        },
                    )
                    execution_logger.info(
                        "job_state=skipped reason=no_trading_session job=%s symbol=%s date=%s",
                        job.name,
                        request.symbol,
                        now_local.date().isoformat(),
                    )
                    return

            execution_logger.info(
                "job_state=started job=%s symbol=%s asset_class=%s bar_size=%s persist=%s cache_latest=%s",
                job.name,
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
            await self._write_status(
                job,
                request,
                {
                    "status": "ok",
                    "bars_captured": len(bars),
                    "last_run_at": self.clock().astimezone(timezone.utc).isoformat(),
                    "latest_bar_timestamp": max((bar.timestamp for bar in bars), default=None),
                },
            )
            execution_logger.info(
                "job_state=success job=%s symbol=%s bar_size=%s bars_captured=%d",
                job.name,
                request.symbol,
                request.bar_size,
                len(bars),
            )
        except Exception as exc:
            execution_logger.exception("job_state=error job=%s symbol=%s error=%s", job.name, symbol.symbol, exc)
            await self._write_status(
                job,
                request_payload,
                {
                    "status": "error",
                    "bars_captured": 0,
                    "last_run_at": self.clock().astimezone(timezone.utc).isoformat(),
                    "error": str(exc),
                },
            )

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
        await self.redis.set_ohlcv_snapshot_status(job.name, symbol, bar_size, payload)

    async def _has_trading_session(self, request: OHLCVRequest, ref_date: date, use_rth: bool) -> bool:
        cache_key = ohlcv_snapshot_calendar_key(
            asset_class=request.asset_class,
            exchange=request.exchange,
            symbol=request.symbol,
            date_value=ref_date.isoformat(),
            use_rth=use_rth,
        )
        if self.redis is not None and hasattr(self.redis, "get_raw"):
            cached = await self.redis.get_raw(cache_key)
            if cached is not None:
                if isinstance(cached, bytes):
                    cached = cached.decode("utf-8")
                return str(cached).strip().lower() == "true"

        if self.feed is None or not hasattr(self.feed, "load_trading_schedule"):
            logger.warning("holiday detection requested but feed has no load_trading_schedule; assuming session exists")
            return True

        sessions = await self.feed.load_trading_schedule(request, ref_date=ref_date, use_rth=use_rth)
        has_session = bool(sessions)
        if self.redis is not None and hasattr(self.redis, "set_raw"):
            await self.redis.set_raw(cache_key, str(has_session).lower(), ex=86_400)
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


def next_cron_run(expression: str, after: datetime) -> datetime:
    cron = _parse_cron_expression(expression)
    candidate = (after + _ONE_MINUTE).replace(second=0, microsecond=0)
    for _ in range(366 * 24 * 60):
        if _cron_matches(cron, candidate):
            return candidate
        candidate += _ONE_MINUTE
    raise ValueError(f"could not find next cron run within one year for {expression!r}")


_ONE_MINUTE = timedelta(minutes=1)


def _parse_cron_expression(expression: str) -> tuple[set[int], set[int], set[int], set[int], set[int], bool, bool]:
    fields = expression.strip().split()
    if len(fields) != 5:
        raise ValueError("cron must contain five fields: minute hour day_of_month month day_of_week")
    minutes = _parse_cron_field(fields[0], minimum=0, maximum=59)
    hours = _parse_cron_field(fields[1], minimum=0, maximum=23)
    days = _parse_cron_field(fields[2], minimum=1, maximum=31)
    months = _parse_cron_field(fields[3], minimum=1, maximum=12)
    weekdays = _parse_cron_weekday_field(fields[4])
    return minutes, hours, days, months, weekdays, fields[2].strip() == "*", fields[4].strip() == "*"


def _parse_cron_weekday_field(field: str) -> set[int]:
    normalized = field.lower()
    for name, number in {
        "sun": "0",
        "mon": "1",
        "tue": "2",
        "wed": "3",
        "thu": "4",
        "fri": "5",
        "sat": "6",
    }.items():
        normalized = normalized.replace(name, number)
    values = _parse_cron_field(normalized, minimum=0, maximum=7)
    return {0 if value == 7 else value for value in values}


def _parse_cron_field(field: str, *, minimum: int, maximum: int) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        token = part.strip()
        if not token:
            raise ValueError("empty cron field token")
        step = 1
        if "/" in token:
            token, step_text = token.split("/", 1)
            step = int(step_text)
            if step <= 0:
                raise ValueError("cron step must be positive")
        if token == "*":
            start, end = minimum, maximum
        elif "-" in token:
            start_text, end_text = token.split("-", 1)
            start, end = int(start_text), int(end_text)
        else:
            start = end = int(token)
        if start < minimum or end > maximum or start > end:
            raise ValueError(f"cron value out of range: {part!r}")
        values.update(range(start, end + 1, step))
    return values


def _cron_matches(cron: tuple[set[int], set[int], set[int], set[int], set[int], bool, bool], candidate: datetime) -> bool:
    minutes, hours, days, months, weekdays, day_wildcard, weekday_wildcard = cron
    if candidate.minute not in minutes or candidate.hour not in hours or candidate.month not in months:
        return False
    day_matches = candidate.day in days
    cron_weekday = (candidate.weekday() + 1) % 7
    weekday_matches = cron_weekday in weekdays
    if day_wildcard and weekday_wildcard:
        return True
    if day_wildcard:
        return weekday_matches
    if weekday_wildcard:
        return day_matches
    return day_matches or weekday_matches


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
