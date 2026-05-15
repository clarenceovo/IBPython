import asyncio
from datetime import datetime, timezone

from src.feeds.models import AssetClass, OHLCVBar
from src.transport.scheduler import (
    GenericScheduler,
    MarketSnapshotJobHandler,
    OHLCVSnapshotJobHandler,
    OHLCVSnapshotParams,
    SchedulerJobDefinition,
    next_cron_run,
)


class FakeLoader:
    def __init__(self) -> None:
        self.calls = []

    async def load(self, request, *, persist: bool, cache_latest: bool):
        self.calls.append((request, persist, cache_latest))
        return []


class FakeOHLCVLoader:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls = []
        self.fail = fail

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


class FakeRedis:
    def __init__(self) -> None:
        self.last_ts: dict[tuple[str, str, str], datetime] = {}
        self.status: dict[tuple[str, str, str], dict] = {}
        self.raw: dict[str, str] = {}
        self.scheduler_payloads: dict[str, str] = {}

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
        assert redis.status[("ohlcv_test", "SPY", "1 min")]["status"] == "ok"

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
        assert redis.status[("ohlcv_test", "SPY", "1 min")]["status"] == "error"

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
