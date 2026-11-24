"""Immutable captures, point-in-time reference data and data-quality repair controls."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.models import OHLCVBar, OHLCVRequest
from src.feeds.quality_repair import run_quality_repairs
from src.feeds.research import DatasetCapture, DatasetManifest, OHLCVLoadResult, ReferenceRecord, RepairJob, StoredReference, TradingSession, aware_utc
from src.transport.analytics_store import AnalyticsStore, research_series_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.routers.orders import require_order_bearer_token

router = APIRouter(prefix="/research", tags=["research"])


def archive(state: IBKRRestAppState) -> AnalyticsStore:
    if state.analytics_store is None:
        raise HTTPException(503, "analytics archive is not configured")
    return state.analytics_store


class CaptureRequest(BaseModel):
    """Request body for an immutable OHLCV capture into the archive."""

    model_config = ConfigDict(extra="forbid")
    request: OHLCVRequest = Field(description="The OHLCV request to load (symbol, asset_class, duration, bar_size, what_to_show, window).")
    sessions: list[TradingSession] | None = Field(default=None, description="Complete trading-session calendar for the request window; requires calendar_source. Sessions must not overlap.")
    calendar_source: str | None = Field(default=None, min_length=1, description="Provenance label for the supplied session calendar, e.g. 'manual'.")
    stale_after_seconds: float | None = Field(default=None, gt=0, description="Reject captures whose latest stored bar is older than this many seconds.")

    @model_validator(mode="after")
    def calendar_provenance(self) -> CaptureRequest:
        if self.sessions is not None and self.calendar_source is None:
            raise ValueError("sessions require calendar_source; supply the complete calendar for the request window")
        if self.sessions:
            ordered = sorted(self.sessions, key=lambda s: s.start)
            if any(b.start < a.end for a, b in zip(ordered, ordered[1:])):
                raise ValueError("trading sessions cannot overlap")
        return self


class PointInTimeRequest(BaseModel):
    """Request body for point-in-time bar reconstruction."""

    model_config = ConfigDict(extra="forbid")
    series_key: str = Field(description="Archive series key; build one with POST /research/series-key.")
    known_at: datetime = Field(description="Point-in-time cutoff (ISO 8601, UTC): only captures recorded at or before this instant are visible.")
    start: datetime | None = Field(default=None, description="Window start (ISO 8601, UTC); null uses the series start.")
    end: datetime | None = Field(default=None, description="Window end (ISO 8601, UTC); null uses the series end. Must follow start.")

    @field_validator("known_at", "start", "end")
    @classmethod
    def utc(cls, value: datetime | None) -> datetime | None:
        return aware_utc(value) if value else None

    @model_validator(mode="after")
    def window(self) -> PointInTimeRequest:
        if self.start and self.end and self.end <= self.start:
            raise ValueError("end must follow start")
        return self


@router.post("/capture", response_model=OHLCVLoadResult)
async def capture(payload: CaptureRequest, state: IBKRRestAppState = Depends(get_rest_state)) -> OHLCVLoadResult:
    """Load OHLCV bars now and return the capture result without persisting it.

    Runs the same load path as the market-data endpoints (with the optional
    session calendar and staleness check) and reports bars, a quality
    report, and the manifest that an archive write would use. Requires the
    analytics archive to be configured (503 otherwise); nothing is written
    by this endpoint.
    """
    archive(state)
    request = payload.request.model_copy(update={"metadata": {**payload.request.metadata,
        "calendar_source": payload.calendar_source, "stale_after_seconds": payload.stale_after_seconds}})
    return await state.loader.load_detailed(request, persist=False, cache_latest=False,
                                           sessions=payload.sessions, stale_after_seconds=payload.stale_after_seconds)


@router.get("/datasets", response_model=list[DatasetManifest])
async def datasets(series_key: str | None = Query(default=None, description="Filter by exact archive series key; omit to list all series."),
                   limit: int = Query(100, ge=1, le=1000, description="Maximum number of manifests to return."),
                   state: IBKRRestAppState = Depends(get_rest_state)) -> list[DatasetManifest]:
    """List dataset manifests recorded in the analytics archive.

    Each manifest identifies one capture: series key, content hash, bar
    count, adjustment and roll policy, and persistence state. Newest first;
    bounded by limit. Requires the analytics archive (503 otherwise).
    """
    return await archive(state).list_datasets(series_key=series_key, limit=limit)


@router.get("/datasets/{dataset_id}", response_model=DatasetCapture)
async def dataset(dataset_id: str = Path(description="Dataset identifier from the manifest list."),
                  state: IBKRRestAppState = Depends(get_rest_state)) -> DatasetCapture:
    """Return one archived dataset: manifest, bars, and quality report.

    The bars are the captured OHLCV rows; the manifest records provenance
    and policies. Returns 404 when the dataset id is unknown.
    """
    result = await archive(state).get_dataset(dataset_id)
    if result is None:
        raise HTTPException(404, "dataset not found")
    return result


@router.post("/bars/as-of", response_model=list[OHLCVBar])
async def point_in_time(payload: PointInTimeRequest,
                        limit: int | None = Query(default=None, ge=1, le=1000, description="Return at most the first N (earliest) bars of the reconstructed window; omit for all."),
                        state: IBKRRestAppState = Depends(get_rest_state)) -> list[OHLCVBar]:
    """Reconstruct the bar history visible at a past instant.

    Uses only captures recorded at or before known_at, so the result
    excludes later corrections and revisions. The optional start/end window
    bounds the reconstruction in UTC. Requires the analytics archive (503
    otherwise).
    """
    bars = await archive(state).point_in_time_bars(**payload.model_dump())
    return bars[:limit] if isinstance(limit, int) else bars


@router.post("/series-key", response_model=str)
async def series_key(request: OHLCVRequest) -> str:
    """Return the deterministic archive series key for an OHLCV request.

    The key is a pure function of the request identity (symbol, asset
    class, exchange, bar size, policies), so the same request always maps
    to the same series. Use it with /research/datasets and
    /research/bars/as-of. This endpoint is local only: no IBKR call.
    """
    return research_series_key(request)


@router.post("/references", response_model=StoredReference, status_code=201)
async def append_reference(payload: ReferenceRecord,
                           state: IBKRRestAppState = Depends(require_order_bearer_token)) -> StoredReference:
    """Append one immutable reference record to the archive.

    Records universe memberships, corporate actions, futures rolls, or
    trading sessions with their effective time and source. The archive
    assigns reference_id and recorded_at; corrections append a new revision
    under the same key. Requires the order bearer token (401 otherwise) and
    the analytics archive (503 otherwise).
    """
    return await archive(state).append_reference(payload)


@router.get("/references", response_model=list[StoredReference])
async def references(kind: Literal["universe", "corporate_action", "futures_roll", "trading_session"] = Query(description="Reference category to query."),
                     entity: str = Query(description="Entity the record is about, e.g. a universe name or symbol."),
                     known_at: datetime = Query(description="Point-in-time cutoff (ISO 8601, UTC); later revisions are invisible."),
                     effective_at: datetime = Query(description="Effectiveness instant (ISO 8601, UTC); the record in force at this time is returned."),
                     limit: int | None = Query(default=None, ge=1, le=1000, description="Return at most the first N references; omit for all."),
                     state: IBKRRestAppState = Depends(get_rest_state)) -> list[StoredReference]:
    """Return the reference records in force for one entity at one instant.

    Resolves the revision history with a known_at cutoff and an
    effective_at probe, so point-in-time reads are reproducible. Requires
    the analytics archive (503 otherwise); invalid queries fail with 422.
    """
    try:
        results = await archive(state).references_as_of(kind, entity, aware_utc(known_at), aware_utc(effective_at))
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    return results[:limit] if isinstance(limit, int) else results


@router.get("/repairs", response_model=list[RepairJob])
async def repairs(limit: int = Query(100, ge=1, le=1000, description="Maximum number of repair jobs to return."),
                  state: IBKRRestAppState = Depends(get_rest_state)) -> list[RepairJob]:
    """List pending and past data-quality repair jobs.

    Each job targets one dataset and carries state (pending, running,
    complete, exhausted), attempts, and the result dataset when finished.
    Requires the analytics archive (503 otherwise).
    """
    return await archive(state).list_repairs(limit)


@router.post("/repairs/run", response_model=list[RepairJob])
async def repair(limit: int = Query(1, ge=1, le=10, description="Maximum number of repair jobs to run in this call."),
                 state: IBKRRestAppState = Depends(require_order_bearer_token)) -> list[RepairJob]:
    """Run up to limit data-quality repair jobs and return their new state.

    A job reloads a quarantined or incomplete dataset without persisting
    the repaired bars. This is a mutating operation and requires the order
    bearer token (401 otherwise). Requires the analytics archive (503
    otherwise).
    """
    return await run_quality_repairs(archive(state), state.loader, limit=limit, persist=False)
