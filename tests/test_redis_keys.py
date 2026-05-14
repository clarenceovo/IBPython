from src.feeds.models import AssetClass
from src.transport.redis_client import index_composition_key, latest_bar_key, scheduler_job_key


def test_latest_bar_key_format() -> None:
    assert latest_bar_key(AssetClass.EQUITY, "1 min") == "MarketData::equity::1_min:latest"


def test_index_composition_key_format() -> None:
    assert index_composition_key("spx") == "GlobalIndex:SPX:composition"


def test_scheduler_job_key_format() -> None:
    assert scheduler_job_key("snapshot_spy_1m") == "SchedulerJob::snapshot_spy_1m"
