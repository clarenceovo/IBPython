"""Scheduler health monitoring and alerting.

Tracks consecutive failure counts per job, logs CRITICAL when a configurable
threshold is breached, and exposes a structured health status for system endpoints.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class JobHealthStatus(BaseModel):
    """Health status for a single scheduler job."""

    job_name: str
    job_type: str
    last_status: str | None = None
    consecutive_failures: int = 0
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_error: str | None = None
    total_runs: int = 0
    total_failures: int = 0

    class Config:
        extra = "forbid"


class SchedulerHealthReport(BaseModel):
    """Aggregated health report for all scheduler jobs."""

    status: str  # "healthy", "degraded", "critical"
    jobs: dict[str, JobHealthStatus] = Field(default_factory=dict)
    checked_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        extra = "forbid"


class SchedulerHealthMonitor:
    """Track consecutive failures and alert when thresholds are breached.

    Parameters
    ----------
    failure_threshold:
        Number of consecutive failures before logging CRITICAL and marking
        the job as unhealthy. Default: 3.
    notification_callback:
        Optional async callback invoked when a job breaches the failure threshold.
        Receives the job name and the health status dict.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        notification_callback: Any = None,
    ) -> None:
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be positive")
        self._failure_threshold = failure_threshold
        self._notification_callback = notification_callback
        self._jobs: dict[str, JobHealthStatus] = {}

    @property
    def failure_threshold(self) -> int:
        return self._failure_threshold

    def record_result(self, job_name: str, job_type: str, status: str, error: str | None = None) -> None:
        """Record a job run result and update health state.

        Parameters
        ----------
        job_name:
            The scheduler job name.
        job_type:
            The scheduler job type.
        status:
            The run status string (e.g. "success", "failed", "timeout", "partial_success").
        error:
            Optional error message from the run.
        """
        now = datetime.now(timezone.utc)
        health = self._jobs.get(job_name)
        if health is None:
            health = JobHealthStatus(job_name=job_name, job_type=job_type)
            self._jobs[job_name] = health

        health.last_status = status
        health.total_runs += 1

        is_failure = status in ("failed", "timeout", "cancelled")

        if is_failure:
            health.consecutive_failures += 1
            health.total_failures += 1
            health.last_failure_at = now
            health.last_error = error

            if health.consecutive_failures >= self._failure_threshold:
                logger.critical(
                    "scheduler job health CRITICAL: job=%s consecutive_failures=%d threshold=%d "
                    "last_error=%s last_success=%s total_runs=%d total_failures=%d",
                    job_name,
                    health.consecutive_failures,
                    self._failure_threshold,
                    error or "none",
                    health.last_success_at.isoformat() if health.last_success_at else "never",
                    health.total_runs,
                    health.total_failures,
                )
                self._fire_notification(job_name, health)
        else:
            health.consecutive_failures = 0
            if status == "success":
                health.last_success_at = now
            health.last_error = None

    def get_health_status(self) -> SchedulerHealthReport:
        """Return a structured health report for all tracked jobs."""
        now = datetime.now(timezone.utc)
        overall = "healthy"
        for health in self._jobs.values():
            if health.consecutive_failures >= self._failure_threshold:
                overall = "critical"
                break
            if health.consecutive_failures > 0:
                overall = "degraded"

        return SchedulerHealthReport(
            status=overall,
            jobs={name: health.model_copy() for name, health in self._jobs.items()},
            checked_at=now,
        )

    def _fire_notification(self, job_name: str, health: JobHealthStatus) -> None:
        """Fire the notification callback if configured."""
        if self._notification_callback is None:
            return
        try:
            import asyncio
            loop = asyncio.get_running_loop()
            loop.create_task(
                self._notification_callback(job_name, health.model_dump(mode="json"))
            )
        except RuntimeError:
            logger.debug("no event loop for notification callback; skipping alert for job=%s", job_name)
        except Exception:
            logger.exception("scheduler health notification callback failed for job=%s", job_name)
