from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.feeds.ibkr_historical import (
    HistoricalRequestTooLargeError,
    _ibkr_duration_between,
    ensure_historical_chunk_limit,
    plan_historical_auto_chunk,
)
from src.feeds.models import AssetClass, OHLCVRequest


def test_plan_historical_auto_chunk_for_oversized_minute_request() -> None:
    request = OHLCVRequest(
        symbol="SPY",
        asset_class=AssetClass.EQUITY,
        exchange="SMART",
        currency="USD",
        duration="2 D",
        bar_size="1 min",
    )
    now = datetime(2026, 1, 3, 21, 0, tzinfo=timezone.utc)

    plan = plan_historical_auto_chunk(request, now=now)

    assert plan is not None
    assert plan.max_duration == "1 D"
    assert plan.estimated_chunks == 2
    assert plan.start_datetime == datetime(2026, 1, 1, 21, 0, tzinfo=timezone.utc)
    assert plan.end_datetime == now


def test_plan_historical_auto_chunk_keeps_compliant_request_single_shot() -> None:
    request = OHLCVRequest(
        symbol="SPY",
        asset_class=AssetClass.EQUITY,
        exchange="SMART",
        currency="USD",
        duration="1 D",
        bar_size="1 min",
    )

    assert plan_historical_auto_chunk(request, now=datetime(2026, 1, 3, tzinfo=timezone.utc)) is None


def test_range_chunk_duration_does_not_exceed_exact_max_chunk() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 8, tzinfo=timezone.utc)

    assert _ibkr_duration_between(start, end) == "7 D"


def test_historical_auto_chunk_limit_rejects_oversized_request_before_ibkr() -> None:
    request = OHLCVRequest(
        symbol="SPY",
        asset_class=AssetClass.EQUITY,
        exchange="SMART",
        currency="USD",
        duration="3 D",
        bar_size="1 min",
    )
    plan = plan_historical_auto_chunk(request, now=datetime(2026, 1, 4, tzinfo=timezone.utc))

    with pytest.raises(HistoricalRequestTooLargeError, match="exceeding configured max 2"):
        ensure_historical_chunk_limit(request, plan, max_chunks=2)
