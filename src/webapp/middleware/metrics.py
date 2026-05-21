"""ASGI middleware for automatic HTTP request metrics.

Delegates to :pymod:`src.transport.metrics` for the actual metric recording.
This module provides the ASGI-layer integration only.
"""

from __future__ import annotations

# Re-export for convenience — the actual implementation lives in
# src.transport.metrics to keep the metrics collector transport-layer agnostic.
from src.transport.metrics import MetricsMiddleware  # noqa: F401

__all__ = ["MetricsMiddleware"]
