"""Shared building blocks for consolidated endpoints.

A consolidated endpoint fans out to several service calls and returns every
section in one payload. Sections run concurrently. One failing section must
not fail the whole response: the runner records structured error metadata
(``code``, ``retryable``, ``error``) and the endpoint reports status per section.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from src.feeds.exceptions import (
    IBKRCircuitOpenError,
    IBKRConnectionDisabledError,
    IBKRConnectionError,
    IBKRContractResolutionError,
    IBKRMarketDataUnavailableError,
    IBKRPacingError,
    IBKRUnsupportedFeatureError,
    QuestDBConnectionError,
    QuestDBWriteError,
)
from src.feeds.ibkr_historical import HistoricalRequestTooLargeError, HistoricalRequestUnsupportedError


class ConsolidatedSection(BaseModel):
    """Per-section metadata: status, latency, machine-readable error code, retry flag, and error text."""

    status: str = Field(description="ok | error | skipped")
    latency_ms: float
    code: str | None = Field(
        default=None,
        description="Machine-readable error code when status == 'error' (e.g. IBKR_PACING, SECTION_TIMEOUT).",
    )
    retryable: bool | None = Field(
        default=None,
        description="Whether retrying this section later can succeed when status == 'error'.",
    )
    error: str | None = None


def _classify_section_exception(exc: BaseException) -> tuple[str, bool]:
    """Map a section exception to ``(code, retryable)`` matching ``api_error`` conventions."""
    if isinstance(exc, IBKRPacingError):
        return "IBKR_PACING", True
    if isinstance(exc, IBKRConnectionDisabledError):
        return "IBKR_DISABLED", False
    if isinstance(exc, IBKRCircuitOpenError):
        return "IBKR_CIRCUIT_OPEN", True
    if isinstance(exc, (IBKRConnectionError, ConnectionError)):
        return "IBKR_UNAVAILABLE", True
    if isinstance(exc, IBKRMarketDataUnavailableError):
        return "MARKET_DATA_UNAVAILABLE", True
    if isinstance(exc, IBKRContractResolutionError):
        return "CONTRACT_RESOLUTION_FAILED", False
    if isinstance(exc, HistoricalRequestTooLargeError):
        return "REQUEST_TOO_LARGE", False
    if isinstance(exc, HistoricalRequestUnsupportedError):
        return "UNSUPPORTED_HISTORICAL_REQUEST", False
    if isinstance(exc, IBKRUnsupportedFeatureError):
        return "FEATURE_REMOVED", False
    if isinstance(exc, (QuestDBConnectionError, QuestDBWriteError)):
        return "QUESTDB_UNAVAILABLE", True
    if isinstance(exc, ValueError):
        return "BAD_REQUEST", False
    text = str(exc).lower()
    if "ibkr not available" in text:
        return "IBKR_UNAVAILABLE", True
    return "SECTION_ERROR", False


async def run_consolidated_sections(
    sections: dict[str, Callable[[], Awaitable[Any]]],
    *,
    section_timeout_seconds: float | None = None,
) -> tuple[dict[str, Any], dict[str, ConsolidatedSection]]:
    """Run section factories concurrently and isolate failures.

    Returns ``(results, meta)``. ``results[key]`` is None when the section
    failed or was skipped. ``meta[key]`` carries status, latency in
    milliseconds, ``code``, ``retryable``, and the error message when a
    section failed.

    When ``section_timeout_seconds`` is set, a section that exceeds the budget
    is cancelled and recorded as ``status="error"`` (``code="SECTION_TIMEOUT"``,
    ``retryable=True``) so one stuck provider can no longer gate the whole
    consolidated response.
    """

    async def _run_one(key: str, factory: Callable[[], Awaitable[Any]]) -> tuple[str, Any, ConsolidatedSection]:
        started = time.perf_counter()
        try:
            if section_timeout_seconds and section_timeout_seconds > 0:
                value = await asyncio.wait_for(factory(), timeout=section_timeout_seconds)
            else:
                value = await factory()
            latency_ms = round((time.perf_counter() - started) * 1000.0, 3)
            return key, value, ConsolidatedSection(status="ok", latency_ms=latency_ms)
        except asyncio.TimeoutError:
            latency_ms = round((time.perf_counter() - started) * 1000.0, 3)
            return key, None, ConsolidatedSection(
                status="error",
                latency_ms=latency_ms,
                code="SECTION_TIMEOUT",
                retryable=True,
                error=f"section timed out after {section_timeout_seconds:g}s",
            )
        except Exception as exc:  # noqa: BLE001 - consolidated payloads isolate section errors
            latency_ms = round((time.perf_counter() - started) * 1000.0, 3)
            code, retryable = _classify_section_exception(exc)
            return key, None, ConsolidatedSection(
                status="error",
                latency_ms=latency_ms,
                code=code,
                retryable=retryable,
                error=str(exc),
            )

    outcomes = await asyncio.gather(*(_run_one(key, factory) for key, factory in sections.items()))
    results: dict[str, Any] = {key: value for key, value, _ in outcomes}
    meta: dict[str, ConsolidatedSection] = {key: section for key, _, section in outcomes}
    return results, meta


def utc_now() -> datetime:
    """Current UTC timestamp used by consolidated response envelopes."""
    return datetime.now(tz=timezone.utc)
