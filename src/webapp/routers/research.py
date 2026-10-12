"""Immutable captures, point-in-time reference data and data-quality repair controls."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
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
    model_config = ConfigDict(extra="forbid")
    request: OHLCVRequest
    sessions: list[TradingSession] | None = None
    calendar_source: str | None = Field(default=None, min_length=1)
    stale_after_seconds: float | None = Field(default=None, gt=0)

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
    model_config = ConfigDict(extra="forbid")
    series_key: str
    known_at: datetime
    start: datetime | None = None
    end: datetime | None = None

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
    archive(state)
    request = payload.request.model_copy(update={"metadata": {**payload.request.metadata,
        "calendar_source": payload.calendar_source, "stale_after_seconds": payload.stale_after_seconds}})
    return await state.loader.load_detailed(request, persist=False, cache_latest=False,
                                           sessions=payload.sessions, stale_after_seconds=payload.stale_after_seconds)


@router.get("/datasets", response_model=list[DatasetManifest])
async def datasets(series_key: str | None = None, limit: int = Query(100, ge=1, le=1000),
                   state: IBKRRestAppState = Depends(get_rest_state)) -> list[DatasetManifest]:
    return await archive(state).list_datasets(series_key=series_key, limit=limit)


@router.get("/datasets/{dataset_id}", response_model=DatasetCapture)
async def dataset(dataset_id: str, state: IBKRRestAppState = Depends(get_rest_state)) -> DatasetCapture:
    result = await archive(state).get_dataset(dataset_id)
    if result is None:
        raise HTTPException(404, "dataset not found")
    return result


@router.post("/bars/as-of", response_model=list[OHLCVBar])
async def point_in_time(payload: PointInTimeRequest, state: IBKRRestAppState = Depends(get_rest_state)) -> list[OHLCVBar]:
    return await archive(state).point_in_time_bars(**payload.model_dump())


@router.post("/series-key", response_model=str)
async def series_key(request: OHLCVRequest) -> str:
    return research_series_key(request)


@router.post("/references", response_model=StoredReference, status_code=201)
async def append_reference(payload: ReferenceRecord,
                           state: IBKRRestAppState = Depends(require_order_bearer_token)) -> StoredReference:
    return await archive(state).append_reference(payload)


@router.get("/references", response_model=list[StoredReference])
async def references(kind: Literal["universe", "corporate_action", "futures_roll", "trading_session"], entity: str,
                     known_at: datetime, effective_at: datetime,
                     state: IBKRRestAppState = Depends(get_rest_state)) -> list[StoredReference]:
    try:
        return await archive(state).references_as_of(kind, entity, aware_utc(known_at), aware_utc(effective_at))
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.get("/repairs", response_model=list[RepairJob])
async def repairs(limit: int = Query(100, ge=1, le=1000), state: IBKRRestAppState = Depends(get_rest_state)) -> list[RepairJob]:
    return await archive(state).list_repairs(limit)


@router.post("/repairs/run", response_model=list[RepairJob])
async def repair(limit: int = Query(1, ge=1, le=10),
                 state: IBKRRestAppState = Depends(require_order_bearer_token)) -> list[RepairJob]:
    return await run_quality_repairs(archive(state), state.loader, limit=limit, persist=False)
