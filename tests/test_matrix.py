"""Matrix publisher tests: key format, log ring, payload shape, Redis loop."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import pytest
from fastapi import WebSocketDisconnect

from src.webapp import matrix
from src.webapp.routers import websocket as websocket_module
from src.webapp.routers.websocket import _Connection
from src.webapp.ws_broker import MarketDataBroker


def test_matrix_key_format() -> None:
    assert matrix.matrix_key("IBPython") == "Matrix:IBPython"
    assert matrix.matrix_key("other-service") == "Matrix:other-service"


def test_log_buffer_records_and_bounds() -> None:
    buffer = matrix.MatrixLogBuffer(capacity=3)
    test_logger = logging.getLogger("matrix-test-buffer")
    test_logger.addHandler(buffer)
    test_logger.setLevel(logging.INFO)
    try:
        test_logger.info("first")
        test_logger.warning("second %s", "formatted")
        test_logger.info("third")
        test_logger.info("fourth")
        test_logger.info("fifth")
    finally:
        test_logger.removeHandler(buffer)

    entries = buffer.snapshot()
    assert len(entries) == 3  # bounded ring: the two oldest records dropped
    assert [e["message"] for e in entries] == ["third", "fourth", "fifth"]
    assert entries[0]["level"] == "INFO"
    assert entries[0]["logger"] == "matrix-test-buffer"
    assert "ts" in entries[0]


@pytest.mark.asyncio
async def test_build_matrix_payload_shape() -> None:
    hub = MarketDataBroker()
    publishers: list[Any] = []

    async def start(publish: Any) -> Any:
        publishers.append(publish)

        async def cleanup() -> None:
            pass

        return cleanup

    await hub.attach(("ticks", "ES"), channel="ticks", start_upstream=start, deliver=lambda f: None)
    publishers[0]({"price": 1})

    payload = matrix.build_matrix_payload(
        hub, service="IBPython", version="9.9.9", connection_stats={"connections_active": 2, "frames_dropped_total": 1}
    )

    assert payload["service"] == "IBPython"
    assert payload["version"] == "9.9.9"
    assert payload["host"] and payload["pid"] > 0
    assert payload["uptime_seconds"] >= 0.0
    assert payload["ws"] == {"connections_active": 2, "frames_dropped_total": 1}
    assert payload["topics"][0]["topic"] == "ticks.ES"
    assert payload["topics"][0]["published"] == 1
    assert "ibkr_request_duration" in payload["metrics"]  # Prometheus text present
    assert isinstance(payload["logs"], list)
    json.dumps(payload)  # whole snapshot must be JSON-serializable


class _FakeRedis:
    def __init__(self) -> None:
        self.writes: list[tuple[str, str, int | None]] = []
        self.failures_remaining = 0

    async def set_raw(self, key: str, value: str, *, ex: int | None = None) -> None:
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise RuntimeError("redis down")
        self.writes.append((key, value, ex))


@pytest.mark.asyncio
async def test_publish_loop_writes_key_with_ttl_and_survives_failure() -> None:
    fake = _FakeRedis()
    fake.failures_remaining = 1  # first write fails; loop must retry
    hub = MarketDataBroker()

    task = asyncio.create_task(
        matrix.matrix_publish_loop(
            fake, hub, service="IBPython", version="9.9.9", interval_seconds=0.02
        )
    )
    await asyncio.sleep(0.1)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert len(fake.writes) >= 2  # loop kept running after the failure
    key, value, ex = fake.writes[0]
    assert key == "Matrix:IBPython"
    assert ex == int(0.02 * 3) + 30  # ~3 intervals of staleness headroom
    payload = json.loads(value)
    assert payload["service"] == "IBPython"
    assert payload["topics"] == []


class _DisconnectSocket:
    """Socket double that disconnects on the first receive."""

    async def receive_text(self) -> str:
        raise WebSocketDisconnect()

    async def send_text(self, data: str) -> None:
        pass

    async def close(self) -> None:
        pass


@pytest.mark.asyncio
async def test_connection_stats_return_to_baseline_after_disconnect() -> None:
    from types import SimpleNamespace

    hub = MarketDataBroker()
    state = SimpleNamespace(feed=None)
    baseline = websocket_module.connection_stats()["connections_active"]

    connection = _Connection(_DisconnectSocket(), state, hub)  # type: ignore[arg-type]
    await connection.run()

    assert websocket_module.connection_stats()["connections_active"] == baseline
