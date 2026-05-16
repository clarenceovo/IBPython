from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import load_settings
from src.transport.redis_client import MarketDataRedisClient
from src.transport.scheduler import (
    GenericScheduler,
    IndexCompositionReloadJobHandler,
    MarketSnapshotJobHandler,
    OHLCVSnapshotJobHandler,
    OHLCVSnapshotParams,
    SchedulerJobDefinition,
    next_cron_run,
)


async def _noop_handler(job: SchedulerJobDefinition) -> None:
    return None


def _register_validation_handlers(scheduler: GenericScheduler) -> None:
    scheduler.register_handler(MarketSnapshotJobHandler.job_type, _noop_handler)
    scheduler.register_handler(OHLCVSnapshotJobHandler.job_type, _noop_handler)
    scheduler.register_handler(IndexCompositionReloadJobHandler.job_type, _noop_handler)


def _dependencies(job: SchedulerJobDefinition) -> set[str]:
    dependencies = {"redis"}
    if job.job_type in {MarketSnapshotJobHandler.job_type, OHLCVSnapshotJobHandler.job_type}:
        dependencies.add("ibkr")
    if job.job_type == MarketSnapshotJobHandler.job_type:
        if _coerce_bool(job.params.get("persist"), default=True):
            dependencies.add("market_store")
    if job.job_type == OHLCVSnapshotJobHandler.job_type:
        defaults = job.params.get("defaults", {}) if isinstance(job.params, dict) else {}
        default_persist = _coerce_bool(defaults.get("persist"), default=True)
        persists = default_persist
        for symbol in job.params.get("symbols", []):
            if isinstance(symbol, dict) and "persist" in symbol:
                persists = persists or _coerce_bool(symbol.get("persist"), default=default_persist)
        if persists:
            dependencies.add("market_store")
    return dependencies


def _estimated_ibkr_requests(job: SchedulerJobDefinition) -> int:
    if job.job_type == MarketSnapshotJobHandler.job_type:
        return 1
    if job.job_type != OHLCVSnapshotJobHandler.job_type:
        return 0
    try:
        params = OHLCVSnapshotParams.model_validate(job.params)
        params.validate_interval(job)
        return len(params.symbols)
    except Exception:
        return -1


def _next_run(job: SchedulerJobDefinition) -> str:
    if job.cron:
        from src.transport.scheduler import _scheduler_now

        return next_cron_run(job.cron, _scheduler_now(job)).isoformat()
    if job.interval_seconds:
        return f"interval:{job.interval_seconds:g}s"
    return "unknown"


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


async def main() -> int:
    parser = argparse.ArgumentParser(description="Validate local and optional Redis scheduler job definitions.")
    parser.add_argument("--schedule-dir", default="schedulejob", help="Directory containing local scheduler JSON files.")
    parser.add_argument("--include-redis", action="store_true", help="Merge Redis SchedulerJob::* definitions.")
    args = parser.parse_args()

    settings = load_settings()
    redis: MarketDataRedisClient | None = None
    scheduler = GenericScheduler(local_job_directory=Path(args.schedule_dir))
    _register_validation_handlers(scheduler)

    if args.include_redis:
        redis = MarketDataRedisClient(settings.redis_url, password=settings.redis_password)
        scheduler.configure_job_sources(redis_client=redis)

    try:
        if redis is not None:
            async with redis:
                jobs = await scheduler.reload_jobs_from_sources()
        else:
            jobs = await scheduler.reload_jobs_from_sources()
    except Exception as exc:
        print(f"validation failed: {type(exc).__name__}: {exc}")
        return 1

    if scheduler.validation_warnings:
        print("warnings:")
        for warning in scheduler.validation_warnings:
            print(f"  - {warning}")

    if not jobs:
        print("no runnable scheduler jobs found")
        return 1

    print(f"validated {len(jobs)} runnable scheduler job(s)")
    for job in sorted(jobs, key=lambda item: item.name):
        deps = ",".join(sorted(_dependencies(job)))
        estimated = _estimated_ibkr_requests(job)
        estimate_text = "invalid" if estimated < 0 else str(estimated)
        print(
            f"- {job.name}: type={job.job_type} enabled={job.enabled} cron={job.cron or '-'} "
            f"interval={job.interval_seconds or '-'} next={_next_run(job)} deps={deps} "
            f"estimated_ibkr_requests={estimate_text}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
