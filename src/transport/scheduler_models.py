"""Pydantic models for the scheduler subsystem.

Extracted from scheduler.py so that other modules can import
job definitions and results without pulling in the entire scheduler.
"""

from __future__ import annotations

import contextvars
from datetime import datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.transport.scheduler_calendar import _parse_cron_expression

SCHEDULER_RUN_STATUSES = {
    "scheduled",
    "lease_skipped",
    "running",
    "skipped_window",
    "skipped_holiday",
    "success",
    "partial_success",
    "failed",
    "timeout",
    "cancelled",
    "disabled",
}


class SchedulerRunContext(BaseModel):
    """Context for one scheduler handler attempt."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    job_name: str
    job_type: str
    worker_id: str
    attempt: int
    job_payload_hash: str
    scheduled_at: datetime
    started_at: datetime


class SchedulerRunResult(BaseModel):
    """Structured result emitted by scheduler handlers and persisted to Redis."""

    model_config = ConfigDict(extra="allow")

    run_id: str | None = None
    job_name: str | None = None
    job_type: str | None = None
    worker_id: str | None = None
    scheduled_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: float | None = None
    status: str
    error: str | None = None
    attempts: int = 1
    metrics: dict[str, Any] = Field(default_factory=dict)
    next_run: datetime | None = None
    job_payload_hash: str | None = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in SCHEDULER_RUN_STATUSES:
            raise ValueError(f"unsupported scheduler run status: {value!r}")
        return normalized


_CURRENT_RUN_CONTEXT: contextvars.ContextVar[SchedulerRunContext | None] = contextvars.ContextVar(
    "scheduler_run_context",
    default=None,
)


def get_current_scheduler_run_context() -> SchedulerRunContext | None:
    """Return metadata for the currently executing scheduler run, if any."""

    return _CURRENT_RUN_CONTEXT.get()


class SchedulerJobDefinition(BaseModel):
    """Redis-serializable scheduler job definition."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    job_type: str = Field(min_length=1)
    interval_seconds: float | None = Field(default=None, gt=0)
    cron: str | None = Field(default=None, min_length=1)
    timezone: str | None = Field(default=None, min_length=1)
    enabled: bool = True
    run_immediately: bool = True
    timeout_seconds: float | None = Field(default=None, gt=0)
    max_attempts: int = Field(default=1, ge=1)
    retry_backoff_seconds: float = Field(default=0.0, ge=0)
    jitter_seconds: float = Field(default=0.0, ge=0)
    lease_ttl_seconds: float = Field(default=300.0, gt=0)
    misfire_policy: str = Field(default="run_next", min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, value: str | None) -> str | None:
        if value is None:
            return None
        _parse_cron_expression(value)
        return value.strip()

    @field_validator("timezone")
    @classmethod
    def validate_scheduler_timezone(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        ZoneInfo(normalized)
        return normalized

    @field_validator("misfire_policy")
    @classmethod
    def validate_misfire_policy(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"run_next", "skip"}:
            raise ValueError("misfire_policy must be 'run_next' or 'skip'")
        return normalized

    @model_validator(mode="after")
    def validate_schedule(self) -> "SchedulerJobDefinition":
        if self.interval_seconds is None and self.cron is None:
            raise ValueError("scheduler job requires interval_seconds or cron")
        return self


class IndexCompositionReloadParams(BaseModel):
    """Parameters for Redis job_type='index_composition_reload' jobs."""

    model_config = ConfigDict(extra="forbid")

    index_symbols: tuple[str, ...] = Field(min_length=1)
    provider: str = Field(default="configured_provider", min_length=1)

    @field_validator("index_symbols", mode="before")
    @classmethod
    def normalize_index_symbols(cls, value: Any) -> tuple[str, ...]:
        if not isinstance(value, (list, tuple)):
            raise TypeError("index_symbols must be a list of symbols")
        normalized = tuple(str(symbol).strip().upper() for symbol in value if str(symbol).strip())
        if not normalized:
            raise ValueError("index_symbols must contain at least one symbol")
        return normalized


WEEKDAY_ALIASES = {
    "mon": 0,
    "monday": 0,
    "tue": 1,
    "tues": 1,
    "tuesday": 1,
    "wed": 2,
    "wednesday": 2,
    "thu": 3,
    "thur": 3,
    "thurs": 3,
    "thursday": 3,
    "fri": 4,
    "friday": 4,
    "sat": 5,
    "saturday": 5,
    "sun": 6,
    "sunday": 6,
}


class OHLCVSnapshotSymbol(BaseModel):
    """One symbol entry for job_type='ohlcv_snapshot' jobs."""

    model_config = ConfigDict(extra="allow")

    symbol: str = Field(min_length=1)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        if value is None:
            raise ValueError("symbol is required")
        return str(value).strip().upper()

    def request_overrides(self) -> dict[str, Any]:
        payload = self.model_dump(exclude_none=True)
        if self.model_extra:
            payload.update({key: value for key, value in self.model_extra.items() if value is not None})
        return payload


class OHLCVSnapshotParams(BaseModel):
    """Parameters for Redis/local job_type='ohlcv_snapshot' jobs."""

    model_config = ConfigDict(extra="forbid")

    start_time: time
    end_time: time
    timezone: str = Field(default="UTC", min_length=1)
    snap_interval_seconds: float = Field(gt=0)
    snap_days: tuple[int, ...] = (0, 1, 2, 3, 4)
    detect_holiday: bool = False
    capture_rth: bool = True
    defaults: dict[str, Any] = Field(default_factory=dict)
    symbols: tuple[OHLCVSnapshotSymbol, ...] = Field(min_length=1)

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def parse_wall_clock_time(cls, value: Any) -> time:
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            parts = value.strip().split(":")
            if len(parts) not in {2, 3}:
                raise ValueError("time must use HH:MM or HH:MM:SS")
            hour, minute = int(parts[0]), int(parts[1])
            second = int(parts[2]) if len(parts) == 3 else 0
            return time(hour=hour, minute=minute, second=second)
        raise TypeError("time fields must be time values or HH:MM strings")

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        normalized = value.strip()
        ZoneInfo(normalized)
        return normalized

    @field_validator("snap_days", mode="before")
    @classmethod
    def parse_snap_days(cls, value: Any) -> tuple[int, ...]:
        if value is None:
            return (0, 1, 2, 3, 4)
        if not isinstance(value, (list, tuple, set)):
            raise TypeError("snap_days must be a list of weekday names or numbers")
        days: list[int] = []
        for item in value:
            if isinstance(item, int):
                day = item
            else:
                token = str(item).strip().lower()
                if token not in WEEKDAY_ALIASES:
                    raise ValueError(f"unsupported snap day: {item!r}")
                day = WEEKDAY_ALIASES[token]
            if day < 0 or day > 6:
                raise ValueError("snap day numbers must be between 0 and 6")
            if day not in days:
                days.append(day)
        if not days:
            raise ValueError("snap_days cannot be empty")
        return tuple(days)

    @field_validator("symbols", mode="before")
    @classmethod
    def normalize_symbols(cls, value: Any) -> tuple[OHLCVSnapshotSymbol, ...]:
        if not isinstance(value, (list, tuple)):
            raise TypeError("symbols must be a list")
        normalized = []
        for item in value:
            if isinstance(item, str):
                normalized.append({"symbol": item})
            else:
                normalized.append(item)
        return tuple(normalized)

    @model_validator(mode="after")
    def validate_defaults(self) -> "OHLCVSnapshotParams":
        if not self.defaults:
            raise ValueError("defaults cannot be empty")
        return self

    def validate_interval(self, job: "SchedulerJobDefinition") -> None:
        if job.interval_seconds is not None and abs(float(job.interval_seconds) - float(self.snap_interval_seconds)) > 1e-9:
            raise ValueError("snap_interval_seconds must match SchedulerJobDefinition.interval_seconds")

