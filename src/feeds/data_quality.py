from __future__ import annotations

import math
from collections import Counter
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from src.feeds.models import OHLCVBar, OHLCVRequest, ohlcv_contract_key


class DataQualitySeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    FATAL = "fatal"


class DataQualityIssue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str = Field(min_length=1)
    severity: DataQualitySeverity
    message: str = Field(min_length=1)
    symbol: str | None = None
    timestamp: datetime | None = None
    contract_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DataQualityReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    bar_size: str
    total_bars: int = Field(ge=0)
    checked_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    issues: tuple[DataQualityIssue, ...] = ()

    @property
    def fatal_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity is DataQualitySeverity.FATAL)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity is DataQualitySeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity is DataQualitySeverity.WARNING)

    @property
    def has_fatal(self) -> bool:
        return self.fatal_count > 0

    def summary(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "bar_size": self.bar_size,
            "total_bars": self.total_bars,
            "fatal_count": self.fatal_count,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "issue_codes": sorted({issue.code for issue in self.issues}),
        }


def validate_ohlcv_bars(
    bars: list[OHLCVBar],
    *,
    request: OHLCVRequest,
    stale_after_seconds: float | None = None,
    sessions: list[tuple[datetime, datetime]] | None = None,
    checked_at: datetime | None = None,
) -> DataQualityReport:
    checked_at = checked_at or datetime.now(timezone.utc)
    issues: list[DataQualityIssue] = []
    if not bars:
        # An empty known calendar can legitimately represent a closed market.
        if sessions != []:
            issues.append(DataQualityIssue(code="empty_dataset", severity=DataQualitySeverity.WARNING,
                                           message="no bars returned; coverage cannot be established", symbol=request.symbol))
        return DataQualityReport(symbol=request.symbol, bar_size=request.bar_size, total_bars=0,
                                 checked_at=checked_at, issues=tuple(issues))

    expected_seconds = _bar_size_to_seconds(request.bar_size)
    identity_counts = Counter((ohlcv_contract_key(bar), bar.timestamp) for bar in bars)
    for (contract_key, timestamp), count in identity_counts.items():
        if count > 1:
            issues.append(
                DataQualityIssue(
                    code="duplicate_contract_timestamp",
                    severity=DataQualitySeverity.FATAL,
                    message=f"duplicate OHLCV bars for contract/timestamp count={count}",
                    symbol=request.symbol,
                    timestamp=timestamp,
                    contract_key=contract_key,
                )
            )

    for previous, current in zip(bars, bars[1:], strict=False):
        if previous.timestamp.tzinfo is None or current.timestamp.tzinfo is None:
            continue  # Naive timestamps are reported below; do not mix time bases.
        if ohlcv_contract_key(previous) != ohlcv_contract_key(current):
            continue
        if current.timestamp < previous.timestamp:
            issues.append(
                DataQualityIssue(
                    code="non_monotonic_timestamp",
                    severity=DataQualitySeverity.FATAL,
                    message="bars are not sorted by non-decreasing timestamp",
                    symbol=current.symbol,
                    timestamp=current.timestamp,
                    contract_key=ohlcv_contract_key(current),
                )
            )
        if expected_seconds is not None and current.timestamp > previous.timestamp:
            gap_seconds = (current.timestamp - previous.timestamp).total_seconds()
            # Daily/weekly/monthly timestamps do not describe a fixed intraday
            # grid. Without a calendar, gaps are suspected rather than confirmed.
            missing = None
            if sessions is not None and expected_seconds < 86400:
                missing = _missing_session_slots(previous.timestamp, current.timestamp, expected_seconds, sessions)
            if gap_seconds > expected_seconds * 1.5 and (missing is None or missing > 0):
                issues.append(
                    DataQualityIssue(
                        code="missing_interval_gap" if missing is None else "missing_session_bars",
                        severity=DataQualitySeverity.WARNING,
                        message=f"gap {gap_seconds:g}s exceeds expected bar interval {expected_seconds:g}s",
                        symbol=current.symbol,
                        timestamp=current.timestamp,
                        contract_key=ohlcv_contract_key(current),
                        metadata={"gap_seconds": gap_seconds, "expected_seconds": expected_seconds,
                                  "calendar_verified": missing is not None, "missing_bars": missing},
                    )
                )

    for bar in bars:
        issues.extend(_bar_value_issues(bar))

    if sessions is not None and expected_seconds is not None and expected_seconds < 86400:
        for session_start, session_end in sessions:
            lower = max(session_start, request.start_datetime) if request.start_datetime else session_start
            upper = min(session_end, request.end_datetime) if request.end_datetime else session_end
            first_index = max(0, math.ceil((lower - session_start).total_seconds() / expected_seconds))
            stop_index = math.ceil((upper - session_start).total_seconds() / expected_seconds)
            observed = set()
            for bar in bars:
                if bar.timestamp.tzinfo is None or not lower <= bar.timestamp < upper:
                    continue
                index = (bar.timestamp - session_start).total_seconds() / expected_seconds
                if abs(index - round(index)) < 1e-6:
                    observed.add(round(index))
            expected = max(0, stop_index - first_index)
            missing = expected - len(observed)
            if missing > 0:
                issues.append(DataQualityIssue(code="incomplete_session_coverage", severity=DataQualitySeverity.WARNING,
                    message=f"session has {missing} missing bar starts including window boundaries", symbol=request.symbol,
                    metadata={"session_start": session_start.isoformat(), "session_end": session_end.isoformat(),
                              "expected_bars": expected, "observed_bars": len(observed), "missing_bars": missing,
                              "calendar_verified": True}))

    if stale_after_seconds is not None and bars:
        latest = max(bar.timestamp for bar in bars)
        age_seconds = (checked_at - latest).total_seconds() if latest.tzinfo else 0
        if age_seconds > stale_after_seconds:
            issues.append(
                DataQualityIssue(
                    code="stale_latest_bar",
                    severity=DataQualitySeverity.WARNING,
                    message=f"latest bar is stale by {age_seconds:g}s",
                    symbol=request.symbol,
                    timestamp=latest,
                    metadata={"age_seconds": age_seconds, "stale_after_seconds": stale_after_seconds},
                )
            )

    return DataQualityReport(
        symbol=request.symbol,
        bar_size=request.bar_size,
        total_bars=len(bars),
        checked_at=checked_at,
        issues=tuple(issues),
    )


def _missing_session_slots(previous: datetime, current: datetime, seconds: float,
                           sessions: list[tuple[datetime, datetime]]) -> int:
    """Count missing bar starts on each session's grid, excluding closed periods."""
    slots = 0
    for start, end in sessions:
        if end <= previous or start >= current:
            continue
        first = max(0, math.floor((previous - start).total_seconds() / seconds) + 1)
        stop = min(math.ceil((current - start).total_seconds() / seconds),
                   math.ceil((end - start).total_seconds() / seconds))
        slots += max(0, stop - first)
    return slots


def _bar_value_issues(bar: OHLCVBar) -> list[DataQualityIssue]:
    issues: list[DataQualityIssue] = []
    values = {"open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close, "volume": bar.volume}
    for field_name, value in values.items():
        if not math.isfinite(float(value)):
            issues.append(
                DataQualityIssue(
                    code="non_finite_value",
                    severity=DataQualitySeverity.FATAL,
                    message=f"{field_name} is not finite",
                    symbol=bar.symbol,
                    timestamp=bar.timestamp,
                    contract_key=ohlcv_contract_key(bar),
                    metadata={"field": field_name},
                )
            )
    if bar.high < bar.low or not (bar.low <= bar.open <= bar.high) or not (bar.low <= bar.close <= bar.high):
        issues.append(
            DataQualityIssue(
                code="invalid_ohlc_range",
                severity=DataQualitySeverity.FATAL,
                message="OHLC fields violate high/low bounds",
                symbol=bar.symbol,
                timestamp=bar.timestamp,
                contract_key=ohlcv_contract_key(bar),
            )
        )
    if bar.timestamp.tzinfo is None or bar.timestamp.utcoffset() is None:
        issues.append(
            DataQualityIssue(
                code="naive_timestamp",
                severity=DataQualitySeverity.FATAL,
                message="timestamp is not timezone-aware",
                symbol=bar.symbol,
                timestamp=bar.timestamp.replace(tzinfo=timezone.utc),
                contract_key=ohlcv_contract_key(bar),
            )
        )
    return issues


def _bar_size_to_seconds(bar_size: str) -> float | None:
    normalized = bar_size.strip().lower()
    table = {
        "1 sec": 1.0,
        "5 secs": 5.0,
        "10 secs": 10.0,
        "15 secs": 15.0,
        "30 secs": 30.0,
        "1 min": 60.0,
        "2 mins": 120.0,
        "3 mins": 180.0,
        "5 mins": 300.0,
        "10 mins": 600.0,
        "15 mins": 900.0,
        "20 mins": 1200.0,
        "30 mins": 1800.0,
        "1 hour": 3600.0,
        "1 day": 86400.0,
    }
    for key, seconds in table.items():
        if key == normalized or key.rstrip("s") == normalized.rstrip("s"):
            return seconds
    return None
