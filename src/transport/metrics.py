"""Lightweight Prometheus-compatible metrics collector.

Zero-dependency implementation that exposes metrics in Prometheus text exposition
format via a ``GET /metrics`` endpoint.  Thread-safe counters, gauges, and
histograms backed by simple dicts and ``threading.Lock``.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Sequence

# ---------------------------------------------------------------------------
# Metric primitives
# ---------------------------------------------------------------------------


class _Counter:
    """Monotonically increasing counter with optional label dimensions."""

    __slots__ = ("_name", "_help", "_label_names", "_values", "_lock")

    def __init__(self, name: str, help_text: str, label_names: Sequence[str] = ()) -> None:
        self._name = name
        self._help = help_text
        self._label_names = tuple(label_names)
        self._values: dict[tuple[str, ...], float] = {}
        self._lock = threading.Lock()

    def inc(self, labels: dict[str, str] | None = None, amount: float = 1.0) -> None:
        if amount < 0:
            raise ValueError("counter can only increase")
        key = self._label_key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + amount

    def _label_key(self, labels: dict[str, str] | None) -> tuple[str, ...]:
        if not self._label_names:
            return ()
        if labels is None:
            labels = {}
        return tuple(labels.get(k, "") for k in self._label_names)

    def expose(self) -> str:
        lines: list[str] = [
            f"# HELP {self._name} {self._help}",
            f"# TYPE {self._name} counter",
        ]
        with self._lock:
            snapshot = dict(self._values)
        if not self._label_names:
            val = snapshot.get((), 0.0)
            lines.append(f"{self._name} {val}")
        else:
            if not snapshot:
                label_str = ",".join(f'{k}=""' for k in self._label_names)
                lines.append(f"{self._name}{{{label_str}}} 0")
            else:
                for key, val in sorted(snapshot.items()):
                    label_parts = ",".join(f'{k}="{v}"' for k, v in zip(self._label_names, key))
                    lines.append(f"{self._name}{{{label_parts}}} {val}")
        return "\n".join(lines)


class _Gauge:
    """Value that can go up or down, with optional label dimensions."""

    __slots__ = ("_name", "_help", "_label_names", "_values", "_lock")

    def __init__(self, name: str, help_text: str, label_names: Sequence[str] = ()) -> None:
        self._name = name
        self._help = help_text
        self._label_names = tuple(label_names)
        self._values: dict[tuple[str, ...], float] = {}
        self._lock = threading.Lock()

    def set(self, value: float, labels: dict[str, str] | None = None) -> None:
        key = self._label_key(labels)
        with self._lock:
            self._values[key] = value

    def inc(self, labels: dict[str, str] | None = None, amount: float = 1.0) -> None:
        key = self._label_key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + amount

    def dec(self, labels: dict[str, str] | None = None, amount: float = 1.0) -> None:
        self.inc(labels, -amount)

    def _label_key(self, labels: dict[str, str] | None) -> tuple[str, ...]:
        if not self._label_names:
            return ()
        if labels is None:
            labels = {}
        return tuple(labels.get(k, "") for k in self._label_names)

    def expose(self) -> str:
        lines: list[str] = [
            f"# HELP {self._name} {self._help}",
            f"# TYPE {self._name} gauge",
        ]
        with self._lock:
            snapshot = dict(self._values)
        if not self._label_names:
            val = snapshot.get((), 0.0)
            lines.append(f"{self._name} {val}")
        else:
            if not snapshot:
                label_str = ",".join(f'{k}=""' for k in self._label_names)
                lines.append(f"{self._name}{{{label_str}}} 0")
            else:
                for key, val in sorted(snapshot.items()):
                    label_parts = ",".join(f'{k}="{v}"' for k, v in zip(self._label_names, key))
                    lines.append(f"{self._name}{{{label_parts}}} {val}")
        return "\n".join(lines)


# Default histogram bucket boundaries (seconds).
_DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)


class _Histogram:
    """Observation histogram with configurable buckets and optional labels."""

    __slots__ = ("_name", "_help", "_label_names", "_buckets", "_data", "_lock")

    def __init__(
        self,
        name: str,
        help_text: str,
        label_names: Sequence[str] = (),
        buckets: Sequence[float] = _DEFAULT_BUCKETS,
    ) -> None:
        self._name = name
        self._help = help_text
        self._label_names = tuple(label_names)
        self._buckets = tuple(sorted(set(buckets)))
        self._data: dict[tuple[str, ...], dict[str, float]] = {}
        self._lock = threading.Lock()

    def observe(self, value: float, labels: dict[str, str] | None = None) -> None:
        key = self._label_key(labels)
        with self._lock:
            entry = self._data.setdefault(key, {"_sum": 0.0, "_count": 0.0})
            entry["_sum"] += value
            entry["_count"] += 1
            for b in self._buckets:
                bucket_key = f"_le_{b}"
                if value <= b:
                    entry[bucket_key] = entry.get(bucket_key, 0.0) + 1
            # +Inf bucket always equals count
            entry["_le_inf"] = entry["_count"]

    def _label_key(self, labels: dict[str, str] | None) -> tuple[str, ...]:
        if not self._label_names:
            return ()
        if labels is None:
            labels = {}
        return tuple(labels.get(k, "") for k in self._label_names)

    def expose(self) -> str:
        lines: list[str] = [
            f"# HELP {self._name} {self._help}",
            f"# TYPE {self._name} histogram",
        ]
        with self._lock:
            snapshot = {k: dict(v) for k, v in self._data.items()}

        def _emit_for(key: tuple[str, ...], entry: dict[str, float]) -> None:
            label_prefix = ""
            if self._label_names:
                parts = ",".join(f'{k}="{v}"' for k, v in zip(self._label_names, key))
                label_prefix = f"{{{parts},"
            else:
                label_prefix = "{"

            for b in self._buckets:
                bucket_val = entry.get(f"_le_{b}", 0.0)
                le_str = f'{label_prefix}le="{b}"}} {bucket_val}'
                lines.append(f"{self._name}_bucket{le_str}")

            inf_val = entry.get("_le_inf", 0.0)
            s = entry.get("_sum", 0.0)
            c = entry.get("_count", 0.0)
            if self._label_names:
                parts = ",".join(f'{k}="{v}"' for k, v in zip(self._label_names, key))
                lines.append(f'{self._name}_bucket{{{parts},le="+Inf"}} {inf_val}')
                lines.append(f"{self._name}_sum{{{parts}}} {s}")
                lines.append(f"{self._name}_count{{{parts}}} {c}")
            else:
                lines.append(f'{self._name}_bucket{{le="+Inf"}} {inf_val}')
                lines.append(f"{self._name}_sum {s}")
                lines.append(f"{self._name}_count {c}")

        if not snapshot:
            empty_key = tuple("" for _ in self._label_names)
            _emit_for(empty_key, {"_sum": 0.0, "_count": 0.0})
        else:
            for key in sorted(snapshot):
                _emit_for(key, snapshot[key])

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Singleton collector
# ---------------------------------------------------------------------------


class MetricsCollector:
    """Central registry for all application metrics.

    Usage::

        from src.transport.metrics import metrics

        metrics.ibkr_request_duration.observe(0.42, {"operation": "qualify", "status": "ok"})
        metrics.cache_hit_total.inc({"cache_name": "market_data"})
    """

    def __init__(self) -> None:
        # IBKR request metrics
        self.ibkr_request_duration: _Histogram = _Histogram(
            "ibkr_request_duration_seconds",
            "Duration of IBKR API requests in seconds",
            label_names=("operation", "status"),
        )
        self.ibkr_request_errors: _Counter = _Counter(
            "ibkr_request_errors_total",
            "Total IBKR request errors",
            label_names=("operation", "error_type"),
        )

        # QuestDB metrics
        self.questdb_insert_duration: _Histogram = _Histogram(
            "questdb_insert_duration_seconds",
            "Duration of QuestDB insert operations in seconds",
            label_names=("table",),
        )
        self.questdb_insert_failures: _Counter = _Counter(
            "questdb_insert_failures_total",
            "Total QuestDB insert failures",
            label_names=("table",),
        )

        # Cache metrics
        self.cache_hit_total: _Counter = _Counter(
            "cache_hit_total",
            "Total cache hits",
            label_names=("cache_name",),
        )
        self.cache_miss_total: _Counter = _Counter(
            "cache_miss_total",
            "Total cache misses",
            label_names=("cache_name",),
        )

        # Streaming metrics
        self.streaming_subscriptions_active: _Gauge = _Gauge(
            "streaming_subscriptions_active",
            "Number of active SSE streaming subscriptions",
        )

        # Circuit breaker
        self.circuit_breaker_state: _Gauge = _Gauge(
            "circuit_breaker_state",
            "Circuit breaker state (0=closed, 1=open, 2=half_open)",
            label_names=("name",),
        )

        # Scheduler metrics
        self.scheduler_job_duration: _Histogram = _Histogram(
            "scheduler_job_duration_seconds",
            "Duration of scheduler job executions in seconds",
            label_names=("job_name",),
        )
        self.scheduler_job_failures: _Counter = _Counter(
            "scheduler_job_failures_total",
            "Total scheduler job failures",
            label_names=("job_name",),
        )

        # HTTP request metrics
        self.http_request_duration: _Histogram = _Histogram(
            "http_request_duration_seconds",
            "Duration of HTTP requests in seconds",
            label_names=("method", "path", "status"),
        )

        # Market data production controls
        self.market_data_snapshot_total: _Counter = _Counter(
            "market_data_snapshot_total",
            "Total equity snapshot outcomes",
            label_names=("asset_class", "status", "source"),
        )
        self.market_data_snapshot_cleanup_failures_total: _Counter = _Counter(
            "market_data_snapshot_cleanup_failures_total",
            "Total market data snapshot cleanup failures",
            label_names=("asset_class", "operation"),
        )
        self.market_data_historical_auto_chunks_total: _Counter = _Counter(
            "market_data_historical_auto_chunks_total",
            "Total historical OHLCV auto-chunking decisions",
            label_names=("asset_class", "operation", "status"),
        )
        self.market_data_historical_chunks_total: _Counter = _Counter(
            "market_data_historical_chunks_total",
            "Total historical OHLCV chunks fetched",
            label_names=("asset_class", "operation"),
        )
        self.market_data_historical_bars_total: _Counter = _Counter(
            "market_data_historical_bars_total",
            "Total historical OHLCV bars returned",
            label_names=("asset_class", "operation"),
        )
        self.market_data_quality_failures_total: _Counter = _Counter(
            "market_data_quality_failures_total",
            "Total market data quality failures",
            label_names=("asset_class", "data_type", "severity"),
        )

        # All metrics in a stable order for exposition
        self._all_metrics: list[_Counter | _Gauge | _Histogram] = [
            self.ibkr_request_duration,
            self.ibkr_request_errors,
            self.questdb_insert_duration,
            self.questdb_insert_failures,
            self.cache_hit_total,
            self.cache_miss_total,
            self.streaming_subscriptions_active,
            self.circuit_breaker_state,
            self.scheduler_job_duration,
            self.scheduler_job_failures,
            self.http_request_duration,
            self.market_data_snapshot_total,
            self.market_data_snapshot_cleanup_failures_total,
            self.market_data_historical_auto_chunks_total,
            self.market_data_historical_chunks_total,
            self.market_data_historical_bars_total,
            self.market_data_quality_failures_total,
        ]

    def expose(self) -> str:
        """Render all metrics in Prometheus text exposition format."""
        blocks: list[str] = []
        for metric in self._all_metrics:
            blocks.append(metric.expose())
        return "\n".join(blocks) + "\n"


# Module-level singleton
metrics = MetricsCollector()


# ---------------------------------------------------------------------------
# ASGI middleware
# ---------------------------------------------------------------------------

class MetricsMiddleware:
    """ASGI middleware that tracks request duration for all HTTP requests."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Any,
        send: Any,
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Skip the /metrics endpoint itself to avoid self-referential metrics.
        path = scope.get("path", "")
        if path == "/metrics":
            await self.app(scope, receive, send)
            return

        start = time.monotonic()
        status_code = "500"

        async def _send(message: dict[str, Any]) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                raw_status = message.get("status", 500)
                status_code = str(raw_status // 100) + "xx"
            await send(message)

        try:
            await self.app(scope, receive, _send)
        finally:
            method = scope.get("method", "GET")
            elapsed = time.monotonic() - start
            metrics.http_request_duration.observe(
                elapsed,
                {"method": method, "path": path, "status": status_code},
            )


# ---------------------------------------------------------------------------
# Helper: context manager for timing operations
# ---------------------------------------------------------------------------

class _Timer:
    """Context manager that records elapsed time into a histogram."""

    __slots__ = ("_histogram", "_labels", "_start")

    def __init__(self, histogram: _Histogram, labels: dict[str, str] | None = None) -> None:
        self._histogram = histogram
        self._labels = labels
        self._start: float = 0.0

    def __enter__(self) -> "_Timer":
        self._start = time.monotonic()
        return self

    def __exit__(self, *_: object) -> None:
        elapsed = time.monotonic() - self._start
        self._histogram.observe(elapsed, self._labels)


def timer(histogram: _Histogram, labels: dict[str, str] | None = None) -> _Timer:
    """Create a context-manager timer that records into a histogram."""
    return _Timer(histogram, labels)
