"""Scheduler job handlers — pluggable run strategies for GenericScheduler.

Extracted from scheduler.py to keep the core scheduler loop separate from
handler implementations.
"""

from __future__ import annotations

import asyncio
import logging
import time as monotonic_time
from collections.abc import Awaitable, Callable
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from src.feeds.models import OHLCVRequest
from src.feeds.ohlcv_loader import estimate_expected_bars
from src.feeds.snapshotter import SnapshotWatchlist
from src.transport.redis_client import ohlcv_snapshot_calendar_key
from src.transport.scheduler_models import (
    IndexCompositionReloadParams,
    OHLCVSnapshotParams,
    OHLCVSnapshotSymbol,
    SchedulerJobDefinition,
    SchedulerRunResult,
    get_current_scheduler_run_context,
)

logger = logging.getLogger(__name__)
execution_logger = logging.getLogger(f"{__name__}.execution")


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
        bars_expected = estimate_expected_bars(request.duration, request.bar_size)
        metrics: dict[str, Any] = {"bars_captured": len(bars or []), "bars_expected": bars_expected}
        quality_summary = _loader_quality_summary(self.loader)
        if quality_summary is not None:
            metrics["data_quality"] = quality_summary
        return SchedulerRunResult(status="success", metrics=metrics)


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
        api_base_url: str | None = None,
        api_timeout_seconds: float = 60.0,
    ) -> None:
        self.loader = loader
        self.redis = redis
        self.feed = feed or getattr(loader, "feed", None)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.max_concurrency = max(1, max_concurrency)
        self.api_base_url = api_base_url.rstrip("/") if api_base_url else None
        self.api_timeout_seconds = api_timeout_seconds

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
        bars_expected = sum(int(result.get("bars_expected", 0)) for result in results)
        quality_reports = [result["data_quality"] for result in results if result.get("data_quality") is not None]
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
                "bars_expected": bars_expected,
                "estimated_historical_requests": estimated_requests,
                "max_concurrency": self.max_concurrency,
                "data_quality_reports": quality_reports,
                "data_quality_issue_symbols": sum(1 for report in quality_reports if report.get("issue_codes")),
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
            capture = await self._capture_ohlcv(request, persist=persist, cache_latest=cache_latest)
            quality_summary = _loader_quality_summary(self.loader)
            bars_captured = int(capture["bars_captured"])
            latest_timestamp = capture.get("latest_timestamp")
            if latest_timestamp is not None:
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
                    "bars_captured": bars_captured,
                    "last_run_at": self.clock().astimezone(timezone.utc).isoformat(),
                    "latest_bar_timestamp": latest_timestamp,
                    "duration_ms": (monotonic_time.monotonic() - started) * 1000.0,
                    "data_quality": quality_summary,
                },
            )
            execution_logger.info(
                "job_state=success job=%s run_id=%s symbol=%s bar_size=%s bars_captured=%d",
                job.name,
                run_context.run_id if run_context else None,
                request.symbol,
                request.bar_size,
                bars_captured,
            )
            bars_expected = estimate_expected_bars(request.duration, request.bar_size)
            return {
                "status": "success",
                "symbol": request.symbol,
                "bars_captured": bars_captured,
                "bars_expected": bars_expected,
                "data_quality": quality_summary,
            }
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

    async def _capture_ohlcv(self, request: OHLCVRequest, *, persist: bool, cache_latest: bool) -> dict[str, Any]:
        if self.api_base_url is None:
            bars = await self.loader.load(request, persist=persist, cache_latest=cache_latest)
            latest_timestamp = max((bar.timestamp for bar in bars), default=None)
            return {"bars_captured": len(bars), "latest_timestamp": latest_timestamp}

        url = f"{self.api_base_url}/api/v1/market-data/ohlcv"
        payload = {
            "request": request.model_dump(mode="json", exclude_none=True),
            "persist": persist,
            "cache_latest": cache_latest,
            "use_ttl_cache": False,
        }
        async with httpx.AsyncClient(timeout=self.api_timeout_seconds) as client:
            response = await client.post(url, json=payload)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(f"OHLCV API returned {response.status_code}: {response.text[:500]}") from exc

        bars_payload = response.json()
        if not isinstance(bars_payload, list):
            raise RuntimeError(f"OHLCV API returned unexpected payload type: {type(bars_payload).__name__}")
        latest_timestamp = max((_bar_timestamp(bar) for bar in bars_payload), default=None)
        return {"bars_captured": len(bars_payload), "latest_timestamp": latest_timestamp}

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
        if self.api_base_url is not None:
            execution_logger.info(
                "job_state=calendar_check_skipped_api_mode symbol=%s date=%s",
                request.symbol,
                ref_date.isoformat(),
            )
            return True

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


def _bar_timestamp(bar: Any) -> datetime:
    if isinstance(bar, dict):
        value = bar.get("timestamp") or bar.get("date")
    else:
        value = getattr(bar, "timestamp", None) or getattr(bar, "date", None)
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"unsupported OHLCV bar timestamp type: {type(value)!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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

    def __init__(
        self,
        snapshot_router: Any,
        *,
        feed: Any = None,
        redis: Any = None,
        questdb: Any = None,
    ) -> None:
        """
        Parameters
        ----------
        snapshot_router : module or object
            Must expose ``capture_snapshots(request, state)`` compatible with the
            FastAPI endpoint. In practice, import the router function directly.
        feed : IBKRFeedClient
            Required feed client for capturing equity snapshots.
        redis : MarketDataRedisClient
            Required Redis client for watchlist lookup and caching.
        questdb : QuestDBClient
            Optional QuestDB client for snapshot persistence.
        """
        self._capture = snapshot_router
        self._feed = feed
        self._redis = redis
        self._questdb = questdb

    async def __call__(self, job: SchedulerJobDefinition) -> None:
        watchlist_name = job.params.get("watchlist_name", "")
        if not watchlist_name:
            raise ValueError("equity_snapshot job requires 'watchlist_name' param")
        logger.info("equity snapshot job executed: job=%s watchlist=%s", job.name, watchlist_name)

        persist = _coerce_bool(job.params.get("persist"), default=True)
        cache_latest = _coerce_bool(job.params.get("cache_latest"), default=True)

        # Load watchlist from Redis
        if self._redis is None:
            raise RuntimeError("EquitySnapshotJobHandler requires redis client (pass redis to constructor)")

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
        if self._feed is None:
            raise RuntimeError("EquitySnapshotJobHandler requires feed client (pass feed to constructor)")

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
                    ticker_time = getattr(ticker, "time", None)
                    snap = ticker_to_snapshot(
                        ticker, symbol=s, exchange=ex, currency=cur, primary_exchange=pe,
                        timestamp=ticker_time if isinstance(ticker_time, datetime) else None,
                    )
                    snapshots.append(snap)
                except Exception:
                    logger.warning("failed to create snapshot for symbol=%s", s, exc_info=True)
                    failed.append(s)

        await self._feed.cancel_equity_tickers(tickers)

        captured_symbols = {s.symbol for s in snapshots}
        for raw_sym in watchlist.symbols:
            resolved = resolve_equity(raw_sym)
            if resolved.symbol not in captured_symbols:
                failed.append(resolved.symbol)

        # Persist to QuestDB first, cache to Redis only on success
        persist_ok = False
        if persist and snapshots and self._questdb is not None:
            try:
                await self._questdb.insert_snapshots(snapshots)
                logger.info("persisted %d snapshots to QuestDB", len(snapshots))
                persist_ok = True
            except Exception:
                logger.exception("failed to persist snapshots to QuestDB — skipping Redis cache")

        if cache_latest and snapshots and persist_ok:
            for snap in snapshots:
                try:
                    await self._redis.set_latest_equity_snapshot(snap)
                except Exception:
                    logger.debug("failed to cache snapshot for %s", snap.symbol, exc_info=True)
        elif cache_latest and snapshots and not persist_ok:
            logger.warning("skipping Redis cache because QuestDB persist failed for %d snapshots", len(snapshots))

        duration = _time.monotonic() - t0
        logger.info(
            "equity snapshot complete: watchlist=%s captured=%d failed=%d duration=%.2fs",
            watchlist.name,
            len(snapshots),
            len(failed),
            duration,
        )


def _contract_fingerprint(request: OHLCVRequest) -> str:
    import hashlib
    import json
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


def _loader_quality_summary(loader: object) -> dict[str, Any] | None:
    report = getattr(loader, "last_quality_report", None)
    if report is None or not hasattr(report, "summary"):
        return None
    return report.summary()
