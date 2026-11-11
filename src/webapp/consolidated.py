"""Shared building blocks for consolidated endpoints.

A consolidated endpoint fans out to several service calls and returns every
section in one payload. Sections run concurrently. One failing section must
not fail the whole response: the runner records the error and the endpoint
reports status per section.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class ConsolidatedSection(BaseModel):
    """Per-section metadata: status, latency, and error text."""

    status: str = Field(description="ok | error | skipped")
    latency_ms: float
    error: str | None = None


async def run_consolidated_sections(
    sections: dict[str, Callable[[], Awaitable[Any]]],
) -> tuple[dict[str, Any], dict[str, ConsolidatedSection]]:
    """Run section factories concurrently and isolate failures.

    Returns ``(results, meta)``. ``results[key]`` is None when the section
    failed or was skipped. ``meta[key]`` carries status, latency in
    milliseconds, and the error message when a section failed.
    """

    async def _run_one(key: str, factory: Callable[[], Awaitable[Any]]) -> tuple[str, Any, ConsolidatedSection]:
        started = time.perf_counter()
        try:
            value = await factory()
            latency_ms = round((time.perf_counter() - started) * 1000.0, 3)
            return key, value, ConsolidatedSection(status="ok", latency_ms=latency_ms)
        except Exception as exc:  # noqa: BLE001 - consolidated payloads isolate section errors
            latency_ms = round((time.perf_counter() - started) * 1000.0, 3)
            return key, None, ConsolidatedSection(status="error", latency_ms=latency_ms, error=str(exc))

    outcomes = await asyncio.gather(*(_run_one(key, factory) for key, factory in sections.items()))
    results: dict[str, Any] = {key: value for key, value, _ in outcomes}
    meta: dict[str, ConsolidatedSection] = {key: section for key, _, section in outcomes}
    return results, meta


def utc_now() -> datetime:
    """Current UTC timestamp used by consolidated response envelopes."""
    return datetime.now(tz=timezone.utc)
