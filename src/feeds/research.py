"""Immutable research captures and point-in-time reference records."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.data_quality import DataQualityReport
from src.feeds.models import OHLCVBar, OHLCVRequest


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return value.astimezone(timezone.utc)


class TradingSession(BaseModel):
    model_config = ConfigDict(extra="forbid")
    start: datetime
    end: datetime

    _utc = field_validator("start", "end")(aware_utc)

    @model_validator(mode="after")
    def ordered(self) -> TradingSession:
        if self.end <= self.start:
            raise ValueError("session end must follow start")
        return self


class DatasetManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dataset_id: str
    series_key: str
    captured_at: datetime
    content_sha256: str
    request: OHLCVRequest
    bar_count: int
    adjustment_policy: str
    roll_policy: str
    reference_ids: list[str] = Field(default_factory=list)
    quarantined: bool = False
    primary_persistence: Literal["not_requested", "no_store", "submitted", "failed", "quarantined"]
    persistence_error: str | None = None


class DatasetCapture(BaseModel):
    manifest: DatasetManifest
    bars: list[dict[str, Any]]
    quality: DataQualityReport


class OHLCVLoadResult(BaseModel):
    bars: list[OHLCVBar]
    quality: DataQualityReport
    manifest: DatasetManifest | None = None
    primary_persistence: str
    archive_persistence: Literal["stored", "disabled"]
    persistence_error: str | None = None


class ReferenceRecord(BaseModel):
    """An immutable revision. recorded_at is assigned by the archive, never by callers.

    Universe values are complete member lists. Corporate actions and roll events
    use stable event keys; corrections append revisions using the same key.
    """

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    kind: Literal["universe", "corporate_action", "futures_roll", "trading_session"]
    entity: str = Field(min_length=1)
    key: str = Field(min_length=1)
    effective_at: datetime
    source: str = Field(min_length=1)
    values: dict[str, Any]

    _utc = field_validator("effective_at")(aware_utc)

    @model_validator(mode="after")
    def validate_values(self) -> ReferenceRecord:
        if self.kind == "universe":
            members = self.values.get("members")
            if not isinstance(members, list) or any(not isinstance(s, str) or not s.strip() for s in members):
                raise ValueError("universe values require a complete members list")
            if len(members) != len(set(members)):
                raise ValueError("universe members must be unique")
        elif self.kind == "trading_session":
            TradingSession.model_validate(self.values)
        elif self.kind == "corporate_action":
            if self.values.get("action") not in {"split", "dividend", "symbol_change", "delisting"}:
                raise ValueError("corporate action requires action=split/dividend/symbol_change/delisting")
        elif self.kind == "futures_roll":
            if not all(self.values.get(k) for k in ("from_contract", "to_contract", "rule", "adjustment")):
                raise ValueError("futures roll requires from_contract, to_contract, rule, adjustment")
        return self


class StoredReference(ReferenceRecord):
    reference_id: str
    recorded_at: datetime


class RepairJob(BaseModel):
    job_id: str
    dataset_id: str
    request: OHLCVRequest
    state: Literal["pending", "running", "complete", "exhausted"]
    attempts: int
    due_at: datetime
    lease_token: str | None = None
    result_dataset_id: str | None = None
    last_error: str | None = None
