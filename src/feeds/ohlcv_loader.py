from __future__ import annotations

import logging
from datetime import timezone
from typing import Protocol

from src.feeds.data_quality import DataQualityReport, validate_ohlcv_bars
from src.feeds.models import OHLCVBar, OHLCVRequest
from src.transport.market_data_store import MarketOHLCVStore
from src.feeds.research import OHLCVLoadResult, TradingSession, utc_now
from src.transport.analytics_store import AnalyticsStore

logger = logging.getLogger(__name__)


class HistoricalOHLCVFeed(Protocol):
    async def load_historical_ohlcv(self, request: OHLCVRequest) -> list[OHLCVBar]:
        ...


class OHLCVLoader:
    """Orchestrates feed loading with optional persistence and latest-bar caching."""

    def __init__(
        self,
        feed: HistoricalOHLCVFeed,
        *,
        store: MarketOHLCVStore | None = None,
        questdb: MarketOHLCVStore | None = None,
        redis: object | None = None,
        analytics_store: AnalyticsStore | None = None,
    ) -> None:
        self.feed = feed
        self.store = store or questdb
        self.questdb = self.store
        self.redis = redis
        self.analytics_store = analytics_store
        self.last_quality_report: DataQualityReport | None = None

    async def load(
        self,
        request: OHLCVRequest,
        *,
        persist: bool = True,
        cache_latest: bool = True,
    ) -> list[OHLCVBar]:
        result = await self.load_detailed(request, persist=persist, cache_latest=cache_latest)
        return result.bars

    async def load_detailed(
        self, request: OHLCVRequest, *, persist: bool = True, cache_latest: bool = True,
        sessions: list[TradingSession] | None = None, stale_after_seconds: float | None = None,
        enqueue_repair: bool = True,
    ) -> OHLCVLoadResult:
        if self.analytics_store is not None and request.end_datetime is None and not request.continuous:
            request = request.model_copy(update={"end_datetime": utc_now()})
        bars = await self.feed.load_historical_ohlcv(request)
        return await self.process_bars(bars, request, persist=persist, cache_latest=cache_latest,
                                       sessions=sessions, stale_after_seconds=stale_after_seconds,
                                       enqueue_repair=enqueue_repair)

    async def process_bars(
        self, bars: list[OHLCVBar], request: OHLCVRequest, *, persist: bool = False, cache_latest: bool = True,
        sessions: list[TradingSession] | None = None, stale_after_seconds: float | None = None,
        enqueue_repair: bool = True,
    ) -> OHLCVLoadResult:
        # Copy the list so validation and provenance do not mutate a feed's cache.
        bars = sorted(bars, key=lambda b: b.timestamp.replace(tzinfo=timezone.utc) if b.timestamp.tzinfo is None else b.timestamp)
        if sessions is None and "trading_sessions" in request.metadata:
            sessions = [TradingSession.model_validate(s) for s in request.metadata["trading_sessions"]]
        if sessions is not None:
            request = request.model_copy(update={"metadata": {**request.metadata,
                "trading_sessions": [s.model_dump(mode="json") for s in sessions]}})
        if stale_after_seconds is None:
            stale_after_seconds = request.metadata.get("stale_after_seconds")
        intervals = [(s.start, s.end) for s in sessions] if sessions is not None else None
        quality = validate_ohlcv_bars(bars, request=request, sessions=intervals, stale_after_seconds=stale_after_seconds)
        self.last_quality_report = quality  # Legacy scheduler compatibility; detailed results are per-call.
        if quality.issues:
            logger.warning("OHLCV data quality report: %s", quality.summary())
        status = "quarantined" if quality.has_fatal else ("no_store" if persist and self.store is None else "not_requested")
        error = None

        if not quality.has_fatal and persist and bars and self.store is not None:
            try:
                await self.store.insert_bars(bars)
                # QuestDB ILP/TCP submission is not a durable server acknowledgement.
                status = "submitted"
            except Exception as exc:
                logger.exception("OHLCV persistence failed for %s", request.symbol)
                status, error = "failed", str(exc)

        manifest = None
        if self.analytics_store is not None:
            manifest = await self.analytics_store.archive_dataset(bars, request, quality, status, error,
                                                                   enqueue_repair=enqueue_repair)
        if quality.has_fatal:
            raise RuntimeError(f"fatal OHLCV data quality issue(s): {quality.summary()}; dataset_id={manifest.dataset_id if manifest else None}")

        if manifest is not None:
            # Provenance travels with cached bars; never infer it from last_quality_report.
            bars = [b.model_copy(update={"metadata": {**b.metadata, "research_capture": {
                "dataset_id": manifest.dataset_id, "captured_at": manifest.captured_at.isoformat(),
                "series_key": manifest.series_key, "primary_persistence": status,
                "archive_persistence": "stored", "quality": quality.summary(),
            }}}) for b in bars]

        if status != "failed" and cache_latest and bars and self.redis is not None:
            await self.cache_latest_bar(bars[-1])

        return OHLCVLoadResult(bars=bars, quality=quality, manifest=manifest, primary_persistence=status,
                              archive_persistence="stored" if manifest else "disabled", persistence_error=error)

    async def persist_bars(self, bars: list[OHLCVBar]) -> None:
        """Persist a list of bars to the configured OHLCV store."""
        if bars and self.store is not None:
            await self.store.insert_bars(bars)

    async def cache_latest_bar(self, bar: OHLCVBar) -> None:
        """Cache the latest bar to Redis."""
        if self.redis is not None:
            try:
                await self.redis.set_latest_bar(bar)
            except Exception:
                logger.warning(
                    "Redis latest-bar cache failed for %s %s; returning loaded OHLCV bars anyway",
                    bar.symbol,
                    bar.bar_size,
                    exc_info=True,
                )


# ------------------------------------------------------------------
# Bar count estimation
# ------------------------------------------------------------------

# Mapping from bar_size string to approximate duration in seconds.
_BAR_SIZE_SECONDS: dict[str, float] = {
    "1 sec": 1,
    "5 secs": 5,
    "10 secs": 10,
    "15 secs": 15,
    "30 secs": 30,
    "1 min": 60,
    "2 mins": 120,
    "3 mins": 180,
    "5 mins": 300,
    "10 mins": 600,
    "15 mins": 900,
    "20 mins": 1200,
    "30 mins": 1800,
    "1 hour": 3600,
    "2 hours": 7200,
    "3 hours": 10800,
    "4 hours": 14400,
    "8 hours": 28800,
    "1 day": 86400,
    "1 week": 86400 * 7,
    "1 month": 86400 * 30,
}


def estimate_expected_bars(duration_str: str, bar_size_str: str) -> int:
    """Estimate the expected number of bars for a given duration and bar size.

    Parameters
    ----------
    duration_str : str
        IBKR duration string, e.g. "1 D", "3600 S", "18 M".
    bar_size_str : str
        IBKR bar size string, e.g. "1 min", "5 mins", "1 hour".

    Returns
    -------
    int
        Estimated bar count (rounded up). Returns 0 if inputs cannot be parsed.
    """
    duration_seconds = _parse_duration_to_seconds(duration_str)
    bar_seconds = _parse_bar_size_to_seconds(bar_size_str)
    if duration_seconds <= 0 or bar_seconds <= 0:
        return 0
    import math
    return math.ceil(duration_seconds / bar_seconds)


def _parse_duration_to_seconds(duration_str: str) -> float:
    """Convert an IBKR duration string to seconds."""
    parts = duration_str.strip().split()
    if len(parts) != 2:
        return 0.0
    try:
        amount = float(parts[0])
    except ValueError:
        return 0.0
    unit = parts[1].upper()
    if unit == "S":
        return amount
    if unit == "D":
        return amount * 86400
    if unit == "W":
        return amount * 86400 * 7
    if unit == "M":
        return amount * 86400 * 30
    if unit == "Y":
        return amount * 86400 * 365
    return 0.0


def _parse_bar_size_to_seconds(bar_size_str: str) -> float:
    """Convert a bar size string to seconds."""
    normalized = bar_size_str.strip().lower()
    # Direct lookup
    if normalized in _BAR_SIZE_SECONDS:
        return _BAR_SIZE_SECONDS[normalized]
    # Try normalizing "min" -> "mins" and vice versa
    for key, value in _BAR_SIZE_SECONDS.items():
        if key.rstrip("s") == normalized.rstrip("s"):
            return value
    return 0.0
