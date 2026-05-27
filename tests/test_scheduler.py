import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from src.feeds.models import AssetClass, OHLCVBar
from src.feeds.snapshotter import EquitySnapshotCaptureResult, SnapshotWatchlist
from src.transport.scheduler import (
    EquitySnapshotJobHandler,
    GenericScheduler,
    MarketSnapshotJobHandler,
    OHLCVSnapshotJobHandler,
    OHLCVSnapshotParams,
    SchedulerRunResult,
    SchedulerJobDefinition,
    get_current_scheduler_run_context,
    next_cron_run,
)
import src.transport.scheduler as scheduler_module


class FakeLoader:
    def __init__(self) -> None:
        self.calls = []

    async def load(self, request, *, persist: bool, cache_latest: bool):
        self.calls.append((request, persist, cache_latest))
        return []


class FakeOHLCVLoader:
    def __init__(self, *, fail: bool = False, quality_summary: dict | None = None) -> None:
        self.calls = []
        self.persisted_batches = []
        self.cached_bars = []
        self.fail = fail
        self.last_quality_report = _FakeQualityReport(quality_summary) if quality_summary is not None else None

    async def load(self, request, *, persist: bool, cache_latest: bool):
        self.calls.append((request, persist, cache_latest))
        if self.fail:
            raise RuntimeError("load failed")
        return [
            OHLCVBar(
                symbol=request.symbol,
                asset_class=request.asset_class,
                exchange=request.exchange,
                currency=request.currency,
                timestamp=datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc),
                open=100,
                high=101,
                low=99,
                close=100.5,
                volume=1000,
                bar_size=request.bar_size,
            )
        ]

    async def persist_bars(self, bars):
        self.persisted_batches.append(list(bars))

    async def cache_latest_bar(self, bar):
        self.cached_bars.append(bar)


class _FakeQualityReport:
    def __init__(self, summary: dict) -> None:
        self._summary = summary

    def summary(self) -> dict:
        return dict(self._summary)


class FakeRedis:
    def __init__(self) -> None:
        self.last_ts: dict[tuple[str, str, str], datetime] = {}
        self.status: dict[tuple[str, str, str], dict] = {}
        self.raw: dict[str, str] = {}
        self.scheduler_payloads: dict[str, str] = {}
        self.leases: dict[str, tuple[str, float]] = {}
        self.now = 0.0
        self.runs: dict[str, list[dict]] = {}
        self.equity_snapshots: dict[str, object] = {}

    async def get_ohlcv_snapshot_last_ts(self, job_name: str, symbol: str, bar_size: str):
        return self.last_ts.get((job_name, symbol, bar_size))

    async def set_ohlcv_snapshot_last_ts(self, job_name: str, symbol: str, bar_size: str, timestamp: datetime):
        self.last_ts[(job_name, symbol, bar_size)] = timestamp
        return "last-ts-key"

    async def set_ohlcv_snapshot_status(self, job_name: str, symbol: str, bar_size: str, status: dict):
        self.status[(job_name, symbol, bar_size)] = status
        return "status-key"

    async def get_raw(self, key: str):
        return self.raw.get(key) or self.scheduler_payloads.get(key)

    async def set_raw(self, key: str, value: str, *, ex: int | None = None):
        self.raw[key] = value

    async def scan_scheduler_jobs(self):
        return list(self.scheduler_payloads)

    async def acquire_scheduler_lease(self, job_name: str, owner_token: str, *, ttl_seconds: float):
        current = self.leases.get(job_name)
        if current is not None and current[1] > self.now:
            return False
        self.leases[job_name] = (owner_token, self.now + ttl_seconds)
        return True

    async def release_scheduler_lease(self, job_name: str, owner_token: str):
        current = self.leases.get(job_name)
        if current is not None and current[0] == owner_token:
            self.leases.pop(job_name, None)
            return True
        return False

    async def record_scheduler_run(self, job_name: str, payload: dict):
        self.runs.setdefault(job_name, []).append(payload)

    async def set_latest_equity_snapshot(self, snapshot):
        self.equity_snapshots[snapshot.symbol] = snapshot
        return "equity-snapshot-key"

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeFeed:
    def __init__(self, sessions: tuple[object, ...]) -> None:
        self.sessions = sessions
        self.calls = []

    async def load_trading_schedule(self, request, *, ref_date, use_rth: bool):
        self.calls.append((request, ref_date, use_rth))
        return self.sessions


def _ohlcv_job(**overrides) -> SchedulerJobDefinition:
    params = {
        "start_time": "09:30",
        "end_time": "16:00",
        "timezone": "UTC",
        "snap_interval_seconds": 60,
        "snap_days": ["mon", "tue", "wed", "thu", "fri"],
        "detect_holiday": False,
        "capture_rth": True,
        "defaults": {
            "asset_class": "equity",
            "exchange": "SMART",
            "currency": "USD",
            "duration": "1 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
            "persist": True,
            "cache_latest": True,
        },
        "symbols": [{"symbol": "SPY", "primary_exchange": "ARCA"}],
    }
    params.update(overrides.pop("params", {}))
    return SchedulerJobDefinition(
        name=overrides.pop("name", "ohlcv_test"),
        job_type="ohlcv_snapshot",
        interval_seconds=overrides.pop("interval_seconds", 60),
        params=params,
        **overrides,
    )


def test_market_snapshot_handler_builds_request() -> None:
    async def run() -> None:
        loader = FakeLoader()
        handler = MarketSnapshotJobHandler(loader)
        job = SchedulerJobDefinition(
            name="snapshot_spy_1m",
            job_type="market_snapshot",
            interval_seconds=60,
            params={
                "symbol": "SPY",
                "asset_class": "equity",
                "exchange": "SMART",
                "currency": "USD",
                "duration": "1 D",
                "bar_size": "1 min",
                "persist": False,
            },
        )

        await handler(job)

        assert loader.calls[0][0].symbol == "SPY"
        assert loader.calls[0][1] is False
        assert loader.calls[0][2] is True

    asyncio.run(run())


def test_scheduler_isolates_job_failures() -> None:
    async def run() -> None:
        calls = 0

        async def failing_handler(job):
            nonlocal calls
            calls += 1
            raise RuntimeError("boom")

        scheduler = GenericScheduler()
        scheduler.register_handler("failing", failing_handler)
        scheduler.add_job(
            SchedulerJobDefinition(
                name="bad_job",
                job_type="failing",
                interval_seconds=3600,
                run_immediately=True,
            )
        )
        await scheduler.start()
        await asyncio.sleep(0.01)
        await scheduler.stop()

        assert calls == 1

    asyncio.run(run())


def test_scheduler_job_accepts_cron_expression_without_interval() -> None:
    job = SchedulerJobDefinition(
        name="cron_job",
        job_type="noop",
        cron="*/5 9-16 * * mon-fri",
        timezone="America/New_York",
    )

    assert job.interval_seconds is None
    assert job.cron == "*/5 9-16 * * mon-fri"


def test_next_cron_run_supports_steps_ranges_and_weekday_names() -> None:
    after = datetime(2026, 1, 5, 9, 31, 15, tzinfo=timezone.utc)

    assert next_cron_run("*/5 9-16 * * mon-fri", after) == datetime(2026, 1, 5, 9, 35, tzinfo=timezone.utc)


def test_scheduler_skips_redis_jobs_without_registered_handler() -> None:
    async def run() -> None:
        class FakeRedis:
            async def scan_scheduler_jobs(self):
                return ["SchedulerJob::reload_index"]

            async def get_raw(self, key):
                return b"""{
                    "name": "reload_index",
                    "job_type": "index_composition_reload",
                    "interval_seconds": 60,
                    "enabled": true,
                    "params": {"index_symbols": ["SPX"], "provider": "configured_provider"}
                }"""

        scheduler = GenericScheduler()
        jobs = await scheduler.load_jobs_from_redis(FakeRedis())

        assert jobs == []

    asyncio.run(run())


def test_scheduler_redis_job_scan_timeout_keeps_local_jobs_runnable() -> None:
    async def run() -> None:
        class SlowRedis:
            async def scan_scheduler_jobs(self):
                await asyncio.sleep(60)
                return []

        scheduler = GenericScheduler(redis_job_load_timeout_seconds=0.01)
        jobs = await scheduler.load_jobs_from_redis(SlowRedis())

        assert jobs == []

    asyncio.run(run())


def test_market_snapshot_handler_parses_string_boolean_flags() -> None:
    async def run() -> None:
        loader = FakeLoader()
        handler = MarketSnapshotJobHandler(loader)
        job = SchedulerJobDefinition(
            name="snapshot_spy_1m",
            job_type="market_snapshot",
            interval_seconds=60,
            params={
                "symbol": "SPY",
                "asset_class": "equity",
                "exchange": "SMART",
                "currency": "USD",
                "duration": "1 D",
                "bar_size": "1 min",
                "persist": "false",
                "cache_latest": "off",
            },
        )

        await handler(job)

        assert loader.calls[0][1] is False
        assert loader.calls[0][2] is False

    asyncio.run(run())


def test_ohlcv_snapshot_params_parse_schedule_fields() -> None:
    params = OHLCVSnapshotParams.model_validate(_ohlcv_job().params)

    assert params.start_time.hour == 9
    assert params.end_time.hour == 16
    assert params.snap_days == (0, 1, 2, 3, 4)
    params.validate_interval(_ohlcv_job())


def test_ohlcv_snapshot_handler_runs_inside_window_and_merges_symbols() -> None:
    async def run() -> None:
        loader = FakeOHLCVLoader()
        redis = FakeRedis()
        handler = OHLCVSnapshotJobHandler(
            loader,
            redis=redis,
            clock=lambda: datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc),
        )

        await handler(_ohlcv_job())

        request, persist, cache_latest = loader.calls[0]
        assert request.symbol == "SPY"
        assert request.primary_exchange == "ARCA"
        assert request.use_rth is True
        assert persist is True
        assert cache_latest is True
        assert redis.last_ts[("ohlcv_test", "SPY", "1 min")] == datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc)
        assert redis.status[("ohlcv_test", "SPY", "1 min")]["status"] == "success"

    asyncio.run(run())


def test_ohlcv_snapshot_handler_can_capture_through_fastapi() -> None:
    class FakeResponse:
        status_code = 200
        text = "ok"

        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict]:
            return [
                {
                    "symbol": "SPY",
                    "asset_class": "equity",
                    "exchange": "SMART",
                    "currency": "USD",
                    "timestamp": "2026-01-05T15:00:00Z",
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100.5,
                    "volume": 1000,
                    "bar_size": "1 min",
                    "source": "ibkr",
                    "metadata": {},
                }
            ]

    class FakeAsyncClient:
        calls: list[tuple[str, dict]] = []

        def __init__(self, *, timeout: float) -> None:
            self.timeout = timeout

        async def __aenter__(self) -> "FakeAsyncClient":
            return self

        async def __aexit__(self, *args) -> None:
            return None

        async def post(self, url: str, *, json: dict) -> FakeResponse:
            self.calls.append((url, json))
            return FakeResponse()

    async def run() -> None:
        loader = FakeOHLCVLoader()
        redis = FakeRedis()
        original_client = scheduler_module.httpx.AsyncClient
        scheduler_module.httpx.AsyncClient = FakeAsyncClient
        try:
            feed = FakeFeed((object(),))
            handler = OHLCVSnapshotJobHandler(
                loader,
                redis=redis,
                feed=feed,
                api_base_url="http://localhost:8000/",
                clock=lambda: datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc),
            )

            await handler(_ohlcv_job())
        finally:
            scheduler_module.httpx.AsyncClient = original_client

        assert loader.calls == []
        assert feed.calls == []
        url, payload = FakeAsyncClient.calls[0]
        assert url == "http://localhost:8000/api/v1/market-data/ohlcv"
        assert payload["persist"] is False
        assert payload["cache_latest"] is False
        assert payload["use_ttl_cache"] is False
        assert payload["request"]["symbol"] == "SPY"
        assert loader.persisted_batches[0][0].symbol == "SPY"
        assert loader.cached_bars[0].symbol == "SPY"
        assert redis.last_ts[("ohlcv_test", "SPY", "1 min")] == datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc)
        assert redis.status[("ohlcv_test", "SPY", "1 min")]["status"] == "success"

    asyncio.run(run())


def test_ohlcv_snapshot_handler_records_data_quality_summary() -> None:
    async def run() -> None:
        quality = {
            "symbol": "SPY",
            "bar_size": "1 min",
            "total_bars": 1,
            "fatal_count": 0,
            "error_count": 0,
            "warning_count": 1,
            "issue_codes": ["missing_interval_gap"],
        }
        loader = FakeOHLCVLoader(quality_summary=quality)
        redis = FakeRedis()
        handler = OHLCVSnapshotJobHandler(
            loader,
            redis=redis,
            clock=lambda: datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc),
        )

        result = await handler(_ohlcv_job())

        assert result.metrics["data_quality_reports"] == [quality]
        assert result.metrics["data_quality_issue_symbols"] == 1
        assert redis.status[("ohlcv_test", "SPY", "1 min")]["data_quality"] == quality

    asyncio.run(run())


def test_ohlcv_snapshot_handler_skips_outside_daily_window() -> None:
    async def run() -> None:
        loader = FakeOHLCVLoader()
        handler = OHLCVSnapshotJobHandler(
            loader,
            clock=lambda: datetime(2026, 1, 5, 8, 0, tzinfo=timezone.utc),
        )

        await handler(_ohlcv_job())

        assert loader.calls == []

    asyncio.run(run())


def test_ohlcv_snapshot_handler_uses_existing_bookmark_as_start_datetime() -> None:
    async def run() -> None:
        loader = FakeOHLCVLoader()
        redis = FakeRedis()
        redis.last_ts[("ohlcv_test", "SPY", "1 min")] = datetime(2026, 1, 5, 14, 0, tzinfo=timezone.utc)
        handler = OHLCVSnapshotJobHandler(
            loader,
            redis=redis,
            clock=lambda: datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc),
        )

        await handler(_ohlcv_job())

        assert loader.calls[0][0].start_datetime == datetime(2026, 1, 5, 14, 0, tzinfo=timezone.utc)
        assert redis.last_ts[("ohlcv_test", "SPY", "1 min")] == datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc)

    asyncio.run(run())


def test_ohlcv_snapshot_handler_detects_holiday_and_skips_symbol() -> None:
    async def run() -> None:
        loader = FakeOHLCVLoader()
        redis = FakeRedis()
        feed = FakeFeed(())
        handler = OHLCVSnapshotJobHandler(
            loader,
            redis=redis,
            feed=feed,
            clock=lambda: datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc),
        )

        await handler(_ohlcv_job(params={"detect_holiday": True}))

        assert loader.calls == []
        assert feed.calls[0][2] is True
        assert redis.status[("ohlcv_test", "SPY", "1 min")]["status"] == "skipped_holiday"

    asyncio.run(run())


def test_ohlcv_snapshot_handler_does_not_update_bookmark_on_failure() -> None:
    async def run() -> None:
        loader = FakeOHLCVLoader(fail=True)
        redis = FakeRedis()
        handler = OHLCVSnapshotJobHandler(
            loader,
            redis=redis,
            clock=lambda: datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc),
        )

        await handler(_ohlcv_job())

        assert redis.last_ts == {}
        assert redis.status[("ohlcv_test", "SPY", "1 min")]["status"] == "failed"

    asyncio.run(run())


def test_scheduler_loads_local_jobs_and_redis_overrides_duplicate_names(tmp_path) -> None:
    async def run() -> None:
        local_payload = _ohlcv_job(name="duplicate_job").model_dump_json()
        redis_payload = _ohlcv_job(
            name="duplicate_job",
            params={"symbols": [{"symbol": "TSLA", "primary_exchange": "NASDAQ"}]},
        ).model_dump_json()
        (tmp_path / "duplicate.json").write_text(local_payload)
        redis = FakeRedis()
        redis.scheduler_payloads["SchedulerJob::duplicate_job"] = redis_payload

        scheduler = GenericScheduler()
        scheduler.register_handler("ohlcv_snapshot", lambda job: None)
        await scheduler.load_jobs_from_directory(tmp_path)
        await scheduler.load_jobs_from_redis(redis)

        jobs = scheduler.jobs()
        assert len(jobs) == 1
        assert jobs[0].params["symbols"][0]["symbol"] == "TSLA"

    asyncio.run(run())


def test_scheduler_redis_lease_allows_only_one_worker_to_run_job() -> None:
    async def run() -> None:
        redis = FakeRedis()
        calls: list[str] = []

        async def handler(job):
            context = get_current_scheduler_run_context()
            calls.append(context.worker_id if context else "missing")
            await asyncio.sleep(0.02)

        job = SchedulerJobDefinition(
            name="leased_job",
            job_type="leased",
            interval_seconds=60,
            lease_ttl_seconds=30,
        )
        scheduler_a = GenericScheduler(redis_client=redis, worker_id="worker-a")
        scheduler_b = GenericScheduler(redis_client=redis, worker_id="worker-b")
        scheduler_a.register_handler("leased", handler)
        scheduler_b.register_handler("leased", handler)

        await asyncio.gather(scheduler_a._run_once(job), scheduler_b._run_once(job))

        assert len(calls) == 1
        statuses = [payload["status"] for payload in redis.runs["leased_job"]]
        assert "lease_skipped" in statuses
        assert "success" in statuses

    asyncio.run(run())


def test_scheduler_lease_expiry_permits_recovery() -> None:
    async def run() -> None:
        redis = FakeRedis()

        assert await redis.acquire_scheduler_lease("expiring", "owner-a", ttl_seconds=5)
        assert not await redis.acquire_scheduler_lease("expiring", "owner-b", ttl_seconds=5)
        redis.advance(6)
        assert await redis.acquire_scheduler_lease("expiring", "owner-b", ttl_seconds=5)

    asyncio.run(run())


def test_scheduler_reloads_jobs_from_sources_and_reconciles_changes(tmp_path) -> None:
    async def run() -> None:
        job_path = tmp_path / "job.json"
        job_path.write_text(_ohlcv_job(name="reloadable").model_dump_json())
        scheduler = GenericScheduler(local_job_directory=tmp_path)
        scheduler.register_handler("ohlcv_snapshot", lambda job: None)

        await scheduler.reload_jobs_from_sources()
        assert [job.name for job in scheduler.jobs()] == ["reloadable"]

        changed = _ohlcv_job(
            name="reloadable",
            params={"symbols": [{"symbol": "TSLA", "primary_exchange": "NASDAQ"}]},
        )
        job_path.write_text(changed.model_dump_json())
        await scheduler.reload_jobs_from_sources()
        assert scheduler.jobs()[0].params["symbols"][0]["symbol"] == "TSLA"

        disabled = changed.model_copy(update={"enabled": False})
        job_path.write_text(disabled.model_dump_json())
        await scheduler.reload_jobs_from_sources()
        assert scheduler.jobs() == []

    asyncio.run(run())


def test_scheduler_timeout_marks_run_timeout() -> None:
    async def run() -> None:
        redis = FakeRedis()

        async def slow_handler(job):
            await asyncio.sleep(0.05)

        scheduler = GenericScheduler(redis_client=redis, worker_id="worker-timeout")
        scheduler.register_handler("slow", slow_handler)
        job = SchedulerJobDefinition(
            name="slow_job",
            job_type="slow",
            interval_seconds=60,
            timeout_seconds=0.001,
        )

        await scheduler._run_once(job)

        assert redis.runs["slow_job"][-1]["status"] == "timeout"

    asyncio.run(run())


def test_ohlcv_snapshot_handler_returns_partial_success_for_symbol_failures() -> None:
    class PartiallyFailingLoader(FakeOHLCVLoader):
        async def load(self, request, *, persist: bool, cache_latest: bool):
            if request.symbol == "TSLA":
                raise RuntimeError("tsla failed")
            return await super().load(request, persist=persist, cache_latest=cache_latest)

    async def run() -> None:
        loader = PartiallyFailingLoader()
        redis = FakeRedis()
        handler = OHLCVSnapshotJobHandler(
            loader,
            redis=redis,
            clock=lambda: datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc),
        )
        job = _ohlcv_job(params={"symbols": [{"symbol": "SPY"}, {"symbol": "TSLA"}]})

        result = await handler(job)

        assert result.status == "partial_success"
        assert result.metrics["symbols_success"] == 1
        assert result.metrics["symbols_failed"] == 1
        assert redis.status[("ohlcv_test", "TSLA", "1 min")]["status"] == "failed"

    asyncio.run(run())


def test_ohlcv_snapshot_handler_returns_failed_when_all_symbols_fail() -> None:
    async def run() -> None:
        loader = FakeOHLCVLoader(fail=True)
        handler = OHLCVSnapshotJobHandler(
            loader,
            clock=lambda: datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc),
        )

        result = await handler(_ohlcv_job())

        assert result.status == "failed"
        assert result.metrics["symbols_failed"] == 1

    asyncio.run(run())


def test_equity_snapshot_scheduler_preserves_symbol_identity_and_cleans_up_tickers() -> None:
    class FakeEquityFeed:
        def __init__(self) -> None:
            self.requests = []
            self.cancelled_tickers = []

        async def capture_equity_snapshots(self, symbols):
            symbol_rows = tuple(symbols)
            self.requests.append(symbol_rows)
            results = [
                EquitySnapshotCaptureResult(
                    requested_symbol="AAPL",
                    symbol="AAPL",
                    exchange="SMART",
                    currency="USD",
                    primary_exchange="NASDAQ",
                    error="simulated subscription failure",
                )
            ]
            for symbol, exchange, currency, primary_exchange, con_id in symbol_rows[1:]:
                ticker = SimpleNamespace(
                    contract=SimpleNamespace(conId=con_id or 12345, symbol=symbol),
                    time=datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc),
                    last=100.5,
                    bid=100.0,
                    ask=101.0,
                    volume=1000,
                )
                results.append(
                    EquitySnapshotCaptureResult(
                        requested_symbol=symbol,
                        symbol=symbol,
                        exchange=exchange,
                        currency=currency,
                        primary_exchange=primary_exchange,
                        con_id=con_id or 12345,
                        ticker=ticker,
                    )
                )
            return results

        async def cancel_equity_tickers(self, tickers):
            self.cancelled_tickers.extend(tickers)
            return 0

    class FakeQuestDB:
        def __init__(self) -> None:
            self.snapshots = []

        async def insert_snapshots(self, snapshots):
            self.snapshots.extend(snapshots)
            return len(snapshots)

    async def run() -> None:
        redis = FakeRedis()
        watchlist = SnapshotWatchlist(name="core", symbols=("AAPL", "MSFT"))
        redis.raw["SnapshotWatchlist::core"] = watchlist.model_dump_json()
        feed = FakeEquityFeed()
        questdb = FakeQuestDB()
        handler = EquitySnapshotJobHandler(None, feed=feed, redis=redis, questdb=questdb)
        job = SchedulerJobDefinition(
            name="equity_core",
            job_type="equity_snapshot",
            interval_seconds=60,
            params={"watchlist_name": "core", "persist": True, "cache_latest": True},
        )

        result = await handler(job)

        assert feed.requests[0][0][0] == "AAPL"
        assert feed.requests[0][1][0] == "MSFT"
        assert [snapshot.symbol for snapshot in questdb.snapshots] == ["MSFT"]
        assert redis.equity_snapshots["MSFT"].symbol == "MSFT"
        assert len(feed.cancelled_tickers) == 1
        assert result.status == "partial_success"
        assert result.metrics["symbols_captured"] == 1
        assert result.metrics["symbols_failed"] == 1
        assert result.metrics["cleanup_failures"] == 0

    asyncio.run(run())


def test_equity_snapshot_scheduler_reports_cleanup_failures() -> None:
    class CleanupFailingFeed:
        async def capture_equity_snapshots(self, symbols):
            symbol, exchange, currency, primary_exchange, con_id = tuple(symbols)[0]
            ticker = SimpleNamespace(
                contract=SimpleNamespace(conId=con_id or 12345, symbol=symbol),
                time=datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc),
                last=100.5,
                bid=100.0,
                ask=101.0,
                volume=1000,
            )
            return [
                EquitySnapshotCaptureResult(
                    requested_symbol=symbol,
                    symbol=symbol,
                    exchange=exchange,
                    currency=currency,
                    primary_exchange=primary_exchange,
                    con_id=con_id or 12345,
                    ticker=ticker,
                )
            ]

        async def cancel_equity_tickers(self, tickers):
            raise RuntimeError("cleanup failed")

    class FakeQuestDB:
        async def insert_snapshots(self, snapshots):
            return len(snapshots)

    async def run() -> None:
        redis = FakeRedis()
        redis.raw["SnapshotWatchlist::core"] = SnapshotWatchlist(name="core", symbols=("MSFT",)).model_dump_json()
        handler = EquitySnapshotJobHandler(None, feed=CleanupFailingFeed(), redis=redis, questdb=FakeQuestDB())
        job = SchedulerJobDefinition(
            name="equity_core",
            job_type="equity_snapshot",
            interval_seconds=60,
            params={"watchlist_name": "core", "persist": True, "cache_latest": True},
        )

        result = await handler(job)

        assert result.status == "partial_success"
        assert result.metrics["symbols_captured"] == 1
        assert result.metrics["cleanup_failures"] == 1

    asyncio.run(run())


def test_scheduler_run_ledger_writes_latest_status_and_history() -> None:
    async def run() -> None:
        redis = FakeRedis()

        async def handler(job):
            return SchedulerRunResult(status="success", metrics={"bars_captured": 3})

        scheduler = GenericScheduler(redis_client=redis, worker_id="worker-ledger")
        scheduler.register_handler("ledger", handler)
        job = SchedulerJobDefinition(name="ledger_job", job_type="ledger", interval_seconds=60)

        await scheduler._run_once(job)

        assert redis.runs["ledger_job"][-1]["status"] == "success"
        assert redis.runs["ledger_job"][-1]["metrics"]["bars_captured"] == 3
        assert redis.runs["ledger_job"][-1]["worker_id"] == "worker-ledger"

    asyncio.run(run())
