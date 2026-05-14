import asyncio

from src.feeds.models import OHLCVRequest
from src.transport.ibkr_rate_limit import RedisIBKRHistoricalPacingGuard


class FakeRawRedis:
    def __init__(self) -> None:
        self.calls = []

    async def eval(self, script, numkeys, *args):
        self.calls.append((script, numkeys, args))
        return 0


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
