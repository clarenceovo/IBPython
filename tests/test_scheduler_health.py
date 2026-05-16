"""Tests for SchedulerHealthMonitor."""

from src.transport.scheduler_health import (
    JobHealthStatus,
    SchedulerHealthMonitor,
    SchedulerHealthReport,
)


def test_health_monitor_tracks_success() -> None:
    monitor = SchedulerHealthMonitor(failure_threshold=3)
    monitor.record_result("test_job", "ohlcv_snapshot", "success")

    report = monitor.get_health_status()
    assert report.status == "healthy"
    assert "test_job" in report.jobs
    assert report.jobs["test_job"].last_status == "success"
    assert report.jobs["test_job"].consecutive_failures == 0
    assert report.jobs["test_job"].total_runs == 1


def test_health_monitor_tracks_failures() -> None:
    monitor = SchedulerHealthMonitor(failure_threshold=3)

    monitor.record_result("job_a", "ohlcv_snapshot", "failed", error="timeout")
    monitor.record_result("job_a", "ohlcv_snapshot", "failed", error="connection")

    report = monitor.get_health_status()
    assert report.status == "degraded"
    assert report.jobs["job_a"].consecutive_failures == 2
    assert report.jobs["job_a"].total_failures == 2


def test_health_monitor_goes_critical_at_threshold() -> None:
    monitor = SchedulerHealthMonitor(failure_threshold=3)

    for i in range(3):
        monitor.record_result("job_b", "market_snapshot", "failed", error=f"error_{i}")

    report = monitor.get_health_status()
    assert report.status == "critical"
    assert report.jobs["job_b"].consecutive_failures == 3


def test_health_monitor_resets_on_success() -> None:
    monitor = SchedulerHealthMonitor(failure_threshold=3)

    monitor.record_result("job_c", "ohlcv_snapshot", "failed", error="err")
    monitor.record_result("job_c", "ohlcv_snapshot", "success")

    report = monitor.get_health_status()
    assert report.status == "healthy"
    assert report.jobs["job_c"].consecutive_failures == 0


def test_health_monitor_partial_success_resets_failures() -> None:
    monitor = SchedulerHealthMonitor(failure_threshold=2)
    monitor.record_result("job_d", "ohlcv_snapshot", "failed", error="err")
    monitor.record_result("job_d", "ohlcv_snapshot", "partial_success")

    report = monitor.get_health_status()
    assert report.jobs["job_d"].consecutive_failures == 0


def test_health_monitor_rejects_non_positive_threshold() -> None:
    import pytest
    with pytest.raises(ValueError):
        SchedulerHealthMonitor(failure_threshold=0)
    with pytest.raises(ValueError):
        SchedulerHealthMonitor(failure_threshold=-1)


def test_health_report_model_serialization() -> None:
    report = SchedulerHealthReport(status="healthy", jobs={})
    data = report.model_dump(mode="json")
    assert data["status"] == "healthy"
    assert isinstance(data["jobs"], dict)


def test_job_health_status_model_forbids_extra() -> None:
    import pytest
    with pytest.raises(Exception):
        JobHealthStatus(job_name="x", job_type="y", unknown_field="bad")
