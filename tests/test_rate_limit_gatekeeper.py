"""Gatekeeper tests: fixed window, escalating blocks, decay, ASGI behavior."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import pytest

from src.webapp.middleware.rate_limit import RateLimiterMiddleware, RateLimitGatekeeper


@pytest.mark.asyncio
async def test_window_allows_up_to_limit_then_blocks() -> None:
    gatekeeper = RateLimitGatekeeper(rate_limit_per_minute=3)

    for expected_remaining in (2, 1, 0):
        decision = gatekeeper._check_local("1.2.3.4")
        assert decision.allowed
        assert decision.remaining == expected_remaining
        assert decision.retry_after == 0

    fourth = gatekeeper._check_local("1.2.3.4")
    assert not fourth.allowed
    assert fourth.retry_after == 60  # first strike: 60s block

    # Blocked IP is denied fast without window counting.
    fifth = gatekeeper._check_local("1.2.3.4")
    assert not fifth.allowed
    assert 0 < fifth.retry_after <= 60

    snap = gatekeeper.snapshot()
    assert snap["blocked_ips"] == {"1.2.3.4": fifth.retry_after}
    assert snap["allowed_total"] == 3
    assert snap["denied_total"] == 2
    assert snap["blocked_total"] == 1
    assert snap["mode"] == "in-process"


@pytest.mark.asyncio
async def test_block_durations_escalate_with_strikes() -> None:
    gatekeeper = RateLimitGatekeeper(rate_limit_per_minute=1)

    def trip_and_clear_block() -> int:
        gatekeeper._check_local("1.2.3.4")  # consume the window
        decision = gatekeeper._check_local("1.2.3.4")  # over limit: strike
        assert not decision.allowed
        gatekeeper._blocks.pop("1.2.3.4", None)  # white-box: block expires
        gatekeeper._windows.pop("1.2.3.4", None)  # white-box: fresh window
        return decision.retry_after

    assert trip_and_clear_block() == 60  # strike 1
    assert trip_and_clear_block() == 300  # strike 2
    assert trip_and_clear_block() == 900  # strike 3
    assert trip_and_clear_block() == 900  # capped


@pytest.mark.asyncio
async def test_strikes_decay_after_strike_window() -> None:
    gatekeeper = RateLimitGatekeeper(rate_limit_per_minute=1)
    gatekeeper._strikes["1.2.3.4"] = (2, time.monotonic() - 1)  # stale strikes

    gatekeeper._check_local("1.2.3.4")  # consume window
    decision = gatekeeper._check_local("1.2.3.4")  # over limit

    assert decision.retry_after == 60  # strikes reset to 1, not escalated to 900


@pytest.mark.asyncio
async def test_window_reset_after_expiry() -> None:
    gatekeeper = RateLimitGatekeeper(rate_limit_per_minute=2)
    gatekeeper._check_local("1.2.3.4")
    gatekeeper._check_local("1.2.3.4")
    # White-box: expire the window.
    count, _ = gatekeeper._windows["1.2.3.4"]
    gatekeeper._windows["1.2.3.4"] = (count, time.monotonic() - 1)

    decision = gatekeeper._check_local("1.2.3.4")
    assert decision.allowed  # fresh window
    assert decision.remaining == 1


# ---------------------------------------------------------------------------
# ASGI middleware behavior
# ---------------------------------------------------------------------------


class _Recorder:
    """Downstream ASGI app that records calls and returns 200 OK."""

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        self.calls += 1
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})


def _scope(ip: str = "1.2.3.4", path: str = "/api/v1/quotes") -> dict[str, Any]:
    return {"type": "http", "path": path, "client": (ip, 12345), "headers": []}


async def _capture_response(send_messages: list[dict[str, Any]]):
    async def send(message: dict[str, Any]) -> None:
        send_messages.append(message)

    return send


@pytest.mark.asyncio
async def test_middleware_allows_under_limit_with_ratelimit_headers() -> None:
    recorder = _Recorder()
    gatekeeper = RateLimitGatekeeper(rate_limit_per_minute=2)
    middleware = RateLimiterMiddleware(recorder, gatekeeper=gatekeeper)

    messages: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    await middleware(_scope(), _noop_receive, send)

    start = next(m for m in messages if m["type"] == "http.response.start")
    headers = {k.decode(): v.decode() for k, v in start["headers"]}
    assert start["status"] == 200
    assert headers["x-ratelimit-limit"] == "2"
    assert headers["x-ratelimit-remaining"] == "1"
    assert "x-ratelimit-reset" in headers
    assert recorder.calls == 1


@pytest.mark.asyncio
async def test_middleware_returns_429_over_limit_and_blocks_fast() -> None:
    recorder = _Recorder()
    gatekeeper = RateLimitGatekeeper(rate_limit_per_minute=1)
    middleware = RateLimiterMiddleware(recorder, gatekeeper=gatekeeper)

    async def send(message: dict[str, Any]) -> None:
        pass

    await middleware(_scope(), _noop_receive, send)  # allowed, window full

    messages: list[dict[str, Any]]

    async def send_capture(message: dict[str, Any]) -> None:
        messages.append(message)

    messages = []
    await middleware(_scope(), _noop_receive, send_capture)  # over limit: 429
    start = next(m for m in messages if m["type"] == "http.response.start")
    body = next(m for m in messages if m["type"] == "http.response.body")
    headers = {k.decode(): v.decode() for k, v in start["headers"]}
    assert start["status"] == 429
    assert headers["retry-after"] == "60"
    assert json.loads(body["body"])["detail"] == "Rate limit exceeded"
    assert recorder.calls == 1  # downstream never saw the denied request

    messages = []
    await middleware(_scope(), _noop_receive, send_capture)  # blocked: fast 429
    start = next(m for m in messages if m["type"] == "http.response.start")
    assert start["status"] == 429
    assert recorder.calls == 1


@pytest.mark.asyncio
async def test_middleware_skips_metrics_path_without_consuming_quota() -> None:
    recorder = _Recorder()
    gatekeeper = RateLimitGatekeeper(rate_limit_per_minute=1)
    middleware = RateLimiterMiddleware(recorder, gatekeeper=gatekeeper)

    async def send(message: dict[str, Any]) -> None:
        pass

    for _ in range(3):
        await middleware(_scope(path="/metrics"), _noop_receive, send)

    assert recorder.calls == 3
    assert gatekeeper.allowed_total == 0  # skipped requests never counted


async def _noop_receive() -> dict[str, Any]:
    return {"type": "http.request", "body": b"", "more_body": False}
