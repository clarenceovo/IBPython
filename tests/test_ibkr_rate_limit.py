import asyncio

from src.feeds.models import OHLCVRequest
from src.transport.ibkr_rate_limit import IBKRRateLimitController, RedisIBKRHistoricalPacingGuard


class FakeRawRedis:
    def __init__(self) -> None:
        self.calls = []
        self.removed = []

    async def eval(self, script, numkeys, *args):
        self.calls.append((script, numkeys, args))
        return 0

    async def zrem(self, key, member):
        self.removed.append((key, member))
        return 1


def test_redis_rate_limiter_uses_weighted_bid_ask_request() -> None:
    async def run() -> None:
        redis = FakeRawRedis()
        guard = RedisIBKRHistoricalPacingGuard(redis)
        request = OHLCVRequest(
            symbol="SPY",
            asset_class="equity",
            exchange="SMART",
            currency="USD",
            what_to_show="BID_ASK",
        )

        await guard.acquire(request)
        guard.release()

        args = redis.calls[0][2]
        assert args[0] == "IBKRRateLimit:historical:window"
        assert args[6] == 2

    asyncio.run(run())


def test_controller_global_message_limiter_uses_redis_bucket() -> None:
    async def run() -> None:
        redis = FakeRawRedis()
        controller = IBKRRateLimitController(redis, pacing_guard=RedisIBKRHistoricalPacingGuard(redis))

        await controller.wait_for_request(operation="contract_details:SPY", weight=3)

        args = redis.calls[0][2]
        assert args[0] == "IBKRRateLimit:global:window"
        assert args[3] == 50
        assert args[4] == 3

    asyncio.run(run())


def test_controller_market_data_lease_releases_redis_member() -> None:
    async def run() -> None:
        redis = FakeRawRedis()
        controller = IBKRRateLimitController(
            redis,
            market_data_lines=10,
            market_data_line_reserve=2,
            pacing_guard=RedisIBKRHistoricalPacingGuard(redis),
        )

        lease = await controller.acquire_market_data_line(
            contract_key="conId:123",
            operation="option_snapshot:SPY",
            ttl_seconds=30,
        )
        await lease.release()

        args = redis.calls[0][2]
        assert args[0] == "IBKRRateLimit:market_data:leases"
        assert args[2] == 8
        assert redis.removed == [("IBKRRateLimit:market_data:leases", lease.lease_id)]

    asyncio.run(run())


def test_controller_snapshot_reports_local_active_market_data_lines() -> None:
    async def run() -> None:
        controller = IBKRRateLimitController(
            redis_client=None,
            market_data_lines=6,
            market_data_line_reserve=1,
        )
        lease = await controller.acquire_market_data_line(
            contract_key="STK:AAPL:SMART:USD",
            operation="equity_snapshot:AAPL",
            ttl_seconds=30,
        )
        snapshot = await controller.snapshot()
        await lease.release()

        assert snapshot["redis_backed"] is False
        assert snapshot["max_active_market_data_lines"] == 5
        assert snapshot["active_market_data_lines"] == 1

    asyncio.run(run())
