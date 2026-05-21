"""Circuit breaker for IBKR operations.

Trips after ``failure_threshold`` consecutive failures, then fast-fails
all calls for ``recovery_timeout_seconds``.  After the timeout elapses
the circuit moves to HALF_OPEN and allows one probe call.  A successful
probe resets the breaker; a failed probe reopens it.
"""

from __future__ import annotations

import asyncio
import logging
import time as monotonic_time
from enum import Enum
from typing import Any

from src.transport.metrics import metrics

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"       # Normal operation
    OPEN = "open"            # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreaker:
    """Simple circuit breaker for IBKR operations.

    Trips after ``failure_threshold`` consecutive failures, then fast-fails
    all calls for ``recovery_timeout_seconds``.  After the timeout elapses
    the circuit moves to HALF_OPEN and allows one probe call.  A successful
    probe resets the breaker; a failed probe reopens it.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 30.0,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self._consecutive_failures: int = 0
        self._state: CircuitState = CircuitState.CLOSED
        self._last_failure_time: float = 0.0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Current circuit breaker state (lock-protected read)."""
        if self._state == CircuitState.OPEN:
            elapsed = monotonic_time.monotonic() - self._last_failure_time
            if elapsed >= self.recovery_timeout_seconds:
                return CircuitState.HALF_OPEN
        return self._state

    @property
    def is_open(self) -> bool:
        """True when the circuit is tripped (fast-failing)."""
        return self.state == CircuitState.OPEN

    async def record_success(self) -> None:
        """Record a successful operation; resets the breaker."""
        async with self._lock:
            self._consecutive_failures = 0
            self._state = CircuitState.CLOSED
        metrics.circuit_breaker_state.set(0, {"name": "ibkr_feed"})

    async def record_failure(self) -> None:
        """Record a failed operation; trips the breaker if threshold is reached."""
        async with self._lock:
            self._consecutive_failures += 1
            self._last_failure_time = monotonic_time.monotonic()
            if self._consecutive_failures >= self.failure_threshold:
                self._state = CircuitState.OPEN
        if self._state == CircuitState.OPEN:
            logger.warning(
                "Circuit breaker TRIPPED: %d consecutive failures (threshold=%d). "
                "Fast-failing for %.0fs.",
                self._consecutive_failures,
                self.failure_threshold,
                self.recovery_timeout_seconds,
            )
        state_value = 1 if self._state == CircuitState.OPEN else 2 if self._state == CircuitState.HALF_OPEN else 0
        metrics.circuit_breaker_state.set(state_value, {"name": "ibkr_feed"})

    def get_state_dict(self) -> dict[str, Any]:
        """Return circuit breaker state for health checks."""
        return {
            "state": self.state.value,
            "consecutive_failures": self._consecutive_failures,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout_seconds": self.recovery_timeout_seconds,
            "last_failure_time": self._last_failure_time,
        }

    async def guard(self) -> None:
        """Raise RuntimeError if the circuit is open.  Thread-safe."""
        current_state = self.state
        if current_state == CircuitState.OPEN:
            raise RuntimeError(
                f"IBKR circuit breaker is OPEN (consecutive_failures={self._consecutive_failures}). "
                f"Fast-failing for {self.recovery_timeout_seconds:.0f}s. "
                f"Last failure was {monotonic_time.monotonic() - self._last_failure_time:.1f}s ago."
            )
