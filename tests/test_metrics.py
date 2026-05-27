from __future__ import annotations

from src.transport.metrics import MetricsCollector


def test_market_data_production_metrics_are_exposed() -> None:
    collector = MetricsCollector()

    empty_exposed = collector.expose()
    assert 'market_data_snapshot_total{asset_class="",status="",source=""} 0' in empty_exposed
    assert 'ibkr_request_duration_seconds_bucket{operation="",status="",le="+Inf"}' in empty_exposed

    collector.market_data_snapshot_total.inc({"asset_class": "equity", "status": "captured", "source": "fastapi"})
    collector.market_data_historical_auto_chunks_total.inc({"asset_class": "equity", "operation": "fastapi", "status": "planned"})
    collector.market_data_quality_failures_total.inc({"asset_class": "equity", "data_type": "snapshot", "severity": "error"})
    exposed = collector.expose()

    assert "market_data_snapshot_total" in exposed
    assert 'asset_class="equity"' in exposed
    assert "market_data_historical_auto_chunks_total" in exposed
    assert "market_data_quality_failures_total" in exposed
