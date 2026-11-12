"""Redis Matrix publisher: service logs and metrics under one key.

Writes a JSON snapshot — recent log lines, the full Prometheus metrics
text, and the WebSocket pub/sub topic stats — to the Redis key
``Matrix:{service}`` on a fixed cadence. The key carries a TTL of about
three intervals, so a dead publisher disappears from Redis instead of
serving stale data.

Redis outages never take the app down: a failed write logs one warning
and the loop retries on the next tick.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable

from src.config import config_constant as constants
from src.transport.metrics import metrics

logger = logging.getLogger(__name__)

_STARTED_AT = time.time()
_HOST = socket.gethostname()
_PID = os.getpid()


def matrix_key(service: str) -> str:
    return constants.REDIS_MATRIX_KEY_TEMPLATE.format(service=service)


class MatrixLogBuffer(logging.Handler):
    """Bounded in-memory ring of recent log records, JSON-ready."""

    def __init__(self, capacity: int = constants.MATRIX_LOG_BUFFER_SIZE) -> None:
        super().__init__(level=logging.INFO)
        self.records: deque[dict[str, Any]] = deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
        except Exception:
            message = record.msg if isinstance(record.msg, str) else repr(record.msg)
        self.records.append(
            {
                "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": message,
            }
        )

    def snapshot(self) -> list[dict[str, Any]]:
        return list(self.records)


_log_buffer: MatrixLogBuffer | None = None


def install_log_buffer() -> MatrixLogBuffer:
    """Attach the ring buffer to the root logger once; return it."""
    global _log_buffer
    if _log_buffer is None:
        _log_buffer = MatrixLogBuffer()
        logging.getLogger().addHandler(_log_buffer)
    return _log_buffer


def build_matrix_payload(
    hub: Any,
    service: str,
    version: str,
    stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble the Matrix snapshot: identity, stats, topics, metrics, logs."""
    return {
        "service": service,
        "version": version,
        "host": _HOST,
        "pid": _PID,
        "started_at": datetime.fromtimestamp(_STARTED_AT, tz=timezone.utc).isoformat(),
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        "uptime_seconds": round(time.time() - _STARTED_AT, 1),
        "stats": dict(stats or {}),
        "topics": hub.topic_status() if hub is not None else [],
        "metrics": metrics.expose(),
        "logs": _log_buffer.snapshot() if _log_buffer is not None else [],
    }


async def matrix_publish_loop(
    redis: Any,
    hub: Any,
    *,
    service: str,
    version: str,
    interval_seconds: float,
    stats_provider: Callable[[], dict[str, Any]] | None = None,
) -> None:
    """Write the Matrix snapshot to Redis until cancelled."""
    install_log_buffer()
    key = matrix_key(service)
    ttl = int(interval_seconds * 3) + 30
    while True:
        try:
            payload = build_matrix_payload(
                hub, service, version, stats_provider() if stats_provider is not None else None
            )
            await redis.set_raw(key, json.dumps(payload), ex=ttl)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("matrix: publish failed key=%s", key, exc_info=True)
        await asyncio.sleep(interval_seconds)


def start_matrix_publisher(
    redis: Any,
    hub: Any,
    *,
    service: str,
    version: str,
    interval_seconds: float,
    stats_provider: Callable[[], dict[str, Any]] | None = None,
) -> asyncio.Task[None]:
    """Start the publish loop as a named background task."""
    task = asyncio.create_task(
        matrix_publish_loop(
            redis,
            hub,
            service=service,
            version=version,
            interval_seconds=interval_seconds,
            stats_provider=stats_provider,
        )
    )
    task.set_name("matrix-publisher")
    return task
