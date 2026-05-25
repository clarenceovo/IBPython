"""Shared models and helpers for market-data router modules."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.ibkr_historical import HistoricalRequestTooLargeError, ensure_historical_chunk_limit, plan_historical_auto_chunk
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.transport.metrics import metrics
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state

logger = logging.getLogger(__name__)


class HistoricalOHLCVLoadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: OHLCVRequest
    persist: bool = False
    cache_latest: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


class MinimalOHLCVLoadControls(BaseModel):
    """Common wrapper controls with production defaults."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    duration: str = Field(default="1 D", min_length=1)
    bar_size: str = Field(default="1 min", min_length=1)
    start_datetime: datetime | None = Field(
        default=None,
        description="Start of the date range (inclusive). When set with end_datetime, the wrapper paginates automatically.",
    )
    end_datetime: datetime | None = Field(
        default=None,
        description="End of the date range (inclusive). Defaults to now.",
    )
    what_to_show: str = Field(default="TRADES", min_length=1)
    use_rth: bool = True
    persist: bool = False
    cache_latest: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)
    metadata: dict[str, object] = Field(default_factory=dict)

    @field_validator("start_datetime", "end_datetime", mode="before")
    @classmethod
    def normalize_datetime_utc(cls, value: object) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("datetime fields must be datetimes")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("duration", "bar_size", "what_to_show", mode="before")
    @classmethod
    def normalize_required_text(cls, value: object) -> str:
        if value is None:
            raise ValueError("value is required")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("value cannot be empty")
        return normalized

    @model_validator(mode="after")
    def validate_datetime_range(self) -> "MinimalOHLCVLoadControls":
        if self.start_datetime is not None and self.end_datetime is not None and self.start_datetime >= self.end_datetime:
            raise ValueError("start_datetime must be before end_datetime")
        return self

    def to_ohlcv_request(self, asset_class: AssetClass, **overrides: object) -> OHLCVRequest:
        metadata = overrides.pop("metadata", self.metadata)
        return OHLCVRequest(
            asset_class=asset_class,
            duration=self.duration,
            bar_size=self.bar_size,
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
            what_to_show=self.what_to_show,
            use_rth=self.use_rth,
            metadata=metadata,
            **overrides,
        )


async def load_ohlcv_with_controls(
    *,
    request: OHLCVRequest,
    start_datetime: datetime | None = None,
    persist: bool,
    cache_latest: bool,
    use_ttl_cache: bool,
    cache_ttl_seconds: float | None,
    cache_namespace: str,
    state: IBKRRestAppState,
) -> list[OHLCVBar]:
    auto_chunk_plan = None
    if start_datetime is None:
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            try:
                ensure_historical_chunk_limit(
                    request,
                    auto_chunk_plan,
                    max_chunks=state.settings.ibkr_historical_max_chunks,
                )
            except HistoricalRequestTooLargeError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            metrics.market_data_historical_auto_chunks_total.inc(
                {"asset_class": request.asset_class.value, "operation": "fastapi", "status": "planned"}
            )
            logger.info(
                "FastAPI historical OHLCV auto_chunking symbol=%s bar_size=%s duration=%s "
                "max_duration=%s estimated_chunks=%d range=%s -> %s",
                request.symbol,
                request.bar_size,
                request.duration,
                auto_chunk_plan.max_duration,
                auto_chunk_plan.estimated_chunks,
                auto_chunk_plan.start_datetime.isoformat(),
                auto_chunk_plan.end_datetime.isoformat(),
            )
            start_datetime = auto_chunk_plan.start_datetime
            request = request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime})

    # When start_datetime is provided or inferred for an oversized request, use paginated range fetch.
    if start_datetime is not None and start_datetime != request.end_datetime:
        if use_ttl_cache and not persist:
            key = stable_cache_key(
                f"{cache_namespace}:range",
                {
                    "request": request.model_dump(mode="json"),
                    "start_datetime": start_datetime.isoformat(),
                    "auto_chunk": auto_chunk_plan is not None,
                },
            )
            cached = await state.market_data_cache.get(key)
            if cached is not None:
                return cached

        try:
            bars = await state.feed.load_historical_ohlcv_range(
                request,
                start_datetime=start_datetime,
                end_datetime=request.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        except HistoricalRequestTooLargeError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        if persist:
            await state.loader.persist_bars(bars)
        if cache_latest and bars:
            await state.loader.cache_latest_bar(bars[-1])

        if use_ttl_cache and not persist:
            await state.market_data_cache.set(key, bars, ttl_seconds=cache_ttl_seconds)
        return bars

    # Single-chunk fetch (original behavior).
    async def load() -> list[OHLCVBar]:
        return await state.loader.load(
            request,
            persist=persist,
            cache_latest=cache_latest,
        )

    if use_ttl_cache and not persist:
        key = stable_cache_key(
            cache_namespace,
            {
                "request": request.model_dump(mode="json"),
                "cache_latest": cache_latest,
            },
        )
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=cache_ttl_seconds)
    return await load()


def contract_int(value: Any, *names: str) -> int | None:
    for name in names:
        raw = getattr(value, name, None)
        if raw not in (None, ""):
            try:
                return int(str(raw))
            except (TypeError, ValueError):
                return None
    return None


def contract_text(value: Any, *names: str) -> str | None:
    for name in names:
        raw = getattr(value, name, None)
        if raw not in (None, ""):
            return str(raw).strip() or None
    return None


def market_rule_ids(value: Any) -> tuple[int, ...]:
    raw = contract_text(value, "marketRuleIds", "market_rule_ids") or ""
    ids: list[int] = []
    for item in raw.replace(";", ",").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.append(int(item))
        except ValueError:
            continue
    return tuple(ids)


def session_to_dict(session: Any) -> dict[str, Any]:
    return {
        key: getattr(session, key, None)
        for key in ("startDateTime", "endDateTime", "refDate", "start", "end")
        if getattr(session, key, None) is not None
    }
