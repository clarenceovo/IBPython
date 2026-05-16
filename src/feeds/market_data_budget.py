"""Market data line budget tracker for IBKR concurrent subscription limits.

IBKR imposes a limit on concurrent market data lines (subscriptions).
Default is ~100 lines, configurable via TWS/Gateway settings.
Option skew surfaces can consume dozens of lines per request.
If a skew request overlaps with snapshot jobs, you silently hit the limit.

This module tracks active subscriptions and provides an async context manager
for safe acquisition/release of market data lines.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class MarketDataBudgetExhausted(Exception):
    """Raised when a market data budget acquisition cannot be fulfilled."""


class MarketDataLineBudget:
    """Track active market data subscriptions against a configurable maximum.

    Usage::

        budget = MarketDataLineBudget(max_lines=100)
        async with budget.acquire(5):
            # subscribe to 5 market data lines
            ...
        # lines automatically released on context exit
    """

    def __init__(
        self,
        *,
        max_lines: int = 100,
        warning_threshold_pct: float = 0.80,
        error_threshold_pct: float = 0.95,
    ) -> None:
        if max_lines <= 0:
            raise ValueError("max_lines must be positive")
        self._max_lines = max_lines
        self._active = 0
        self._lock = asyncio.Lock()
        self._warning_threshold = int(max_lines * warning_threshold_pct)
        self._error_threshold = int(max_lines * error_threshold_pct)
        self._subscriptions: dict[int, int] = {}
        self._next_id = 0

    @property
    def max_lines(self) -> int:
        return self._max_lines

    @property
    def active(self) -> int:
        return self._active

    @property
    def available(self) -> int:
        return max(0, self._max_lines - self._active)

    @property
    def utilization_pct(self) -> float:
        if self._max_lines == 0:
            return 0.0
        return (self._active / self._max_lines) * 100.0

    def status(self) -> dict[str, Any]:
        return {
            "max_lines": self._max_lines,
            "active": self._active,
            "available": self.available,
            "utilization_pct": round(self.utilization_pct, 1),
            "active_subscriptions": len(self._subscriptions),
        }

    async def acquire(self, count: int = 1) -> "MarketDataLineBudget._Acquisition":
        """Acquire *count* market data lines and return a context manager.

        Raises ``MarketDataBudgetExhausted`` if not enough lines are available.
        Logs warnings at 80% capacity and errors at 95%.
        """
        if count <= 0:
            raise ValueError("count must be positive")

        async with self._lock:
            if self._active + count > self._max_lines:
                logger.error(
                    "market data budget exhausted: requested=%d active=%d max=%d available=%d",
                    count,
                    self._active,
                    self._max_lines,
                    self.available,
                )
                raise MarketDataBudgetExhausted(
                    f"cannot acquire {count} lines: {self._active}/{self._max_lines} in use "
                    f"({self.available} available)"
                )

            sub_id = self._next_id
            self._next_id += 1
            self._subscriptions[sub_id] = count
            self._active += count

            self._log_utilization("acquire", count)

        return MarketDataLineBudget._Acquisition(self, sub_id, count)

    async def _release(self, sub_id: int, count: int) -> None:
        async with self._lock:
            if sub_id in self._subscriptions:
                del self._subscriptions[sub_id]
                self._active = max(0, self._active - count)
                self._log_utilization("release", count)
            else:
                logger.warning("market data budget: unknown subscription id %d already released", sub_id)

    def _log_utilization(self, action: str, count: int) -> None:
        if self._active >= self._error_threshold:
            logger.error(
                "market data budget %s: +%d lines now active=%d/%d (%.0f%%)",
                action,
                count,
                self._active,
                self._max_lines,
                self.utilization_pct,
            )
        elif self._active >= self._warning_threshold:
            logger.warning(
                "market data budget %s: +%d lines now active=%d/%d (%.0f%%)",
                action,
                count,
                self._active,
                self._max_lines,
                self.utilization_pct,
            )
        else:
            logger.debug(
                "market data budget %s: +%d lines now active=%d/%d",
                action,
                count,
                self._active,
                self._max_lines,
            )

    class _Acquisition:
        """Async context manager for market data line budget."""

        def __init__(self, budget: "MarketDataLineBudget", sub_id: int, count: int) -> None:
            self._budget = budget
            self._sub_id = sub_id
            self._count = count

        async def __aenter__(self) -> "MarketDataLineBudget._Acquisition":
            return self

        async def __aexit__(self, *args: Any) -> None:
            await self._budget._release(self._sub_id, self._count)
