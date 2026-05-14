import asyncio

from src.transport.scheduler import GenericScheduler, MarketSnapshotJobHandler, SchedulerJobDefinition


class FakeLoader:
    def __init__(self) -> None:
        self.calls = []

    async def load(self, request, *, persist: bool, cache_latest: bool):
        self.calls.append((request, persist, cache_latest))
        return []


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
