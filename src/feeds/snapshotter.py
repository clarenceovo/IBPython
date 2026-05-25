"""Generic equity snapshot collector — point-in-time market data for a universe of equities.

The snapshotter fetches real-time ticker snapshots from IBKR (bid/ask/last/volume/etc.)
for a configurable watchlist of equity symbols, then persists them to QuestDB and
caches the latest in Redis.

Models and converters have been split into focused sub-modules:
- ``snapshot_models`` — all Pydantic models
- ``snapshot_converters`` — converter functions

This module re-exports everything for backward compatibility.
"""

from __future__ import annotations

# Re-export all models
from src.feeds.snapshot_models import (
    EquitySnapshot,
    EquitySnapshotCaptureResult,
    EquitySnapshotQualityError,
    FXOptionSnapshot,
    FXOptionSnapshotQuery,
    SnapshotQuery,
    SnapshotResult,
    SnapshotWatchlist,
    equity_snapshot_quality_issues,
    validate_equity_snapshot_quality,
)

# Re-export all converters and helpers
from src.feeds.snapshot_converters import (
    fx_option_contract_key,
    fx_pair_parts,
    ticker_to_fx_option_snapshot,
    ticker_to_snapshot,
)

__all__ = [
    # Models
    "EquitySnapshot",
    "EquitySnapshotCaptureResult",
    "EquitySnapshotQualityError",
    "FXOptionSnapshot",
    "FXOptionSnapshotQuery",
    "SnapshotQuery",
    "SnapshotResult",
    "SnapshotWatchlist",
    "equity_snapshot_quality_issues",
    "validate_equity_snapshot_quality",
    # Converters
    "fx_option_contract_key",
    "fx_pair_parts",
    "ticker_to_fx_option_snapshot",
    "ticker_to_snapshot",
]
