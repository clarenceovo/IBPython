"""Durable single-host analytics archive, separate from mutable latest-bar stores.

SQLite WAL supports the API and scheduler sharing a local disk. Operations run
off the event loop with separate connections. Do not place this file on NFS.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, TypeVar
from uuid import uuid4

from src.feeds.data_quality import DataQualityReport
from src.feeds.models import OHLCVBar, OHLCVRequest, ohlcv_contract_key
from src.feeds.orders import ExecutionDetail, OrderEnvelope
from src.feeds.research import (
    DatasetCapture, DatasetManifest, ReferenceRecord, RepairJob, StoredReference, aware_utc, utc_now,
)

T = TypeVar("T")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS datasets (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, dataset_id TEXT UNIQUE NOT NULL,
    series_key TEXT NOT NULL, captured_at TEXT NOT NULL, quarantined INTEGER NOT NULL,
    manifest TEXT NOT NULL, bars TEXT NOT NULL, quality TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS datasets_series_time ON datasets(series_key, captured_at);
CREATE TABLE IF NOT EXISTS reference_records (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, reference_id TEXT UNIQUE NOT NULL,
    kind TEXT NOT NULL, entity TEXT NOT NULL, event_key TEXT NOT NULL,
    effective_at TEXT NOT NULL, recorded_at TEXT NOT NULL, payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS references_lookup ON reference_records(kind, entity, recorded_at);
CREATE TABLE IF NOT EXISTS repairs (
    job_id TEXT PRIMARY KEY, dataset_id TEXT UNIQUE NOT NULL, request TEXT NOT NULL,
    state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, due_at TEXT NOT NULL,
    lease_token TEXT, result_dataset_id TEXT, last_error TEXT
);
CREATE TABLE IF NOT EXISTS order_contexts (
    order_uuid TEXT PRIMARY KEY, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS executions (
    account_id TEXT NOT NULL, exec_id TEXT NOT NULL, correction_key TEXT NOT NULL,
    revision INTEGER NOT NULL, recorded_at TEXT NOT NULL, payload TEXT NOT NULL,
    PRIMARY KEY(account_id, exec_id)
);
CREATE INDEX IF NOT EXISTS executions_corrections ON executions(account_id, correction_key, revision);
"""


def _json(value: Any) -> str:
    def safe(item: Any) -> Any:
        if isinstance(item, float) and not math.isfinite(item):
            return str(item)
        if isinstance(item, dict):
            return {key: safe(val) for key, val in item.items()}
        if isinstance(item, list):
            return [safe(val) for val in item]
        return item
    return json.dumps(safe(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def research_series_key(request: OHLCVRequest) -> str:
    """Series semantics exclude the fetch window; price basis and sessions remain distinct."""
    data = request.model_dump(mode="json", exclude={"duration", "start_datetime", "end_datetime", "metadata"})
    # Policies are part of series identity, so raw and transformed captures cannot mix.
    data["adjustment_policy"] = request.metadata.get("adjustment_policy", "vendor_adjusted" if request.what_to_show == "ADJUSTED_LAST" else "vendor_unspecified")
    data["roll_policy"] = request.metadata.get("roll_policy", "vendor_continuous_unspecified" if request.continuous else "individual_contract")
    return hashlib.sha256(_json(data).encode()).hexdigest()


class AnalyticsStore:
    def __init__(self, path: str | Path, *, clock: Callable[[], datetime] = utc_now) -> None:
        self.path = Path(path)
        self.clock = clock

    async def _run(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        def run() -> T:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self.path, timeout=30)
            try:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=FULL")
                conn.executescript(_SCHEMA)
                with conn:
                    return operation(conn)
            finally:
                conn.close()
        return await asyncio.to_thread(run)

    async def archive_dataset(self, bars: list[OHLCVBar], request: OHLCVRequest, quality: DataQualityReport,
                              primary_persistence: str, persistence_error: str | None = None,
                              *, enqueue_repair: bool = True) -> DatasetManifest:
        now = aware_utc(self.clock())
        raw_bars = [bar.model_dump(mode="json") for bar in bars]
        bars_json = _json(raw_bars)
        # This timestamp records local availability, not historical vendor availability.
        manifest = DatasetManifest(
            dataset_id=str(uuid4()), series_key=research_series_key(request), captured_at=now,
            content_sha256=hashlib.sha256(bars_json.encode()).hexdigest(), request=request,
            bar_count=len(bars), quarantined=quality.has_fatal,
            adjustment_policy=str(request.metadata.get("adjustment_policy", "vendor_adjusted" if request.what_to_show == "ADJUSTED_LAST" else "vendor_unspecified")),
            roll_policy=str(request.metadata.get("roll_policy", "vendor_continuous_unspecified" if request.continuous else "individual_contract")),
            primary_persistence=primary_persistence, persistence_error=persistence_error,
        )

        def save(conn: sqlite3.Connection) -> DatasetManifest:
            # Pin all revisions available for this symbol plus explicitly named universes.
            entities = [request.symbol, *request.metadata.get("research_universes", [])]
            refs = conn.execute("SELECT reference_id FROM reference_records WHERE recorded_at <= ? AND entity IN ("
                                + ",".join("?" for _ in entities) + ") ORDER BY seq", [now.isoformat(), *entities]).fetchall()
            manifest.reference_ids = [row["reference_id"] for row in refs]
            conn.execute("INSERT INTO datasets(dataset_id,series_key,captured_at,quarantined,manifest,bars,quality) VALUES(?,?,?,?,?,?,?)",
                         (manifest.dataset_id, manifest.series_key, now.isoformat(), int(manifest.quarantined),
                          manifest.model_dump_json(), bars_json, quality.model_dump_json()))
            if enqueue_repair and (quality.issues or primary_persistence == "failed"):
                # Freeze the original window: retrying 'latest 1 D' tomorrow is not a repair.
                frozen = request.model_copy(update={"end_datetime": request.end_datetime or now})
                conn.execute("INSERT INTO repairs(job_id,dataset_id,request,state,due_at) VALUES(?,?,?,?,?)",
                             (str(uuid4()), manifest.dataset_id, frozen.model_dump_json(), "pending", (now + timedelta(minutes=5)).isoformat()))
            return manifest
        return await self._run(save)

    async def get_dataset(self, dataset_id: str) -> DatasetCapture | None:
        def read(conn: sqlite3.Connection) -> DatasetCapture | None:
            row = conn.execute("SELECT * FROM datasets WHERE dataset_id=?", (dataset_id,)).fetchone()
            if row is None:
                return None
            bars_json = row["bars"]
            manifest = DatasetManifest.model_validate_json(row["manifest"])
            if hashlib.sha256(bars_json.encode()).hexdigest() != manifest.content_sha256:
                raise RuntimeError("dataset content hash mismatch")
            return DatasetCapture(manifest=manifest, bars=json.loads(bars_json),
                                  quality=DataQualityReport.model_validate_json(row["quality"]))
        return await self._run(read)

    async def list_datasets(self, *, series_key: str | None = None, limit: int = 100) -> list[DatasetManifest]:
        def read(conn: sqlite3.Connection) -> list[DatasetManifest]:
            rows = conn.execute("SELECT manifest FROM datasets WHERE (? IS NULL OR series_key=?) ORDER BY seq DESC LIMIT ?",
                                (series_key, series_key, limit)).fetchall()
            return [DatasetManifest.model_validate_json(row["manifest"]) for row in rows]
        return await self._run(read)

    async def point_in_time_bars(self, *, series_key: str, known_at: datetime,
                                 start: datetime | None = None, end: datetime | None = None) -> list[OHLCVBar]:
        cutoff = aware_utc(known_at)
        start = aware_utc(start) if start else None
        end = aware_utc(end) if end else None

        def read(conn: sqlite3.Connection) -> list[OHLCVBar]:
            rows = conn.execute("SELECT bars FROM datasets WHERE series_key=? AND captured_at<=? AND quarantined=0 ORDER BY captured_at,seq",
                                (series_key, cutoff.isoformat())).fetchall()
            latest: dict[tuple[str, datetime], OHLCVBar] = {}
            for row in rows:
                for raw in json.loads(row["bars"]):
                    bar = OHLCVBar.model_validate(raw)
                    if bar.timestamp > cutoff or (start and bar.timestamp < start) or (end and bar.timestamp >= end):
                        continue
                    latest[(ohlcv_contract_key(bar), bar.timestamp)] = bar
            return sorted(latest.values(), key=lambda b: (b.timestamp, ohlcv_contract_key(b)))
        return await self._run(read)

    async def append_reference(self, record: ReferenceRecord) -> StoredReference:
        stored = StoredReference(**record.model_dump(), reference_id=str(uuid4()), recorded_at=aware_utc(self.clock()))
        def save(conn: sqlite3.Connection) -> StoredReference:
            conn.execute("INSERT INTO reference_records(reference_id,kind,entity,event_key,effective_at,recorded_at,payload) VALUES(?,?,?,?,?,?,?)",
                         (stored.reference_id, stored.kind, stored.entity, stored.key, stored.effective_at.isoformat(),
                          stored.recorded_at.isoformat(), stored.model_dump_json()))
            return stored
        return await self._run(save)

    async def references_as_of(self, kind: str, entity: str, known_at: datetime, effective_at: datetime) -> list[StoredReference]:
        def read(conn: sqlite3.Connection) -> list[StoredReference]:
            # Resolve corrections before applying effective-time filtering: a corrected
            # future effective date must suppress the old earlier effective date.
            rows = conn.execute("SELECT * FROM reference_records WHERE kind=? AND entity=? AND recorded_at<=? ORDER BY recorded_at,seq",
                                (kind, entity, aware_utc(known_at).isoformat())).fetchall()
            latest = {row["event_key"]: StoredReference.model_validate_json(row["payload"]) for row in rows}
            eligible = [r for r in latest.values() if r.effective_at <= aware_utc(effective_at)]
            eligible.sort(key=lambda r: (r.effective_at, r.recorded_at))
            return eligible[-1:] if kind == "universe" else eligible
        return await self._run(read)

    async def list_repairs(self, limit: int = 100) -> list[RepairJob]:
        return await self._run(lambda conn: [self._repair(row) for row in conn.execute("SELECT * FROM repairs ORDER BY due_at LIMIT ?", (limit,))])

    @staticmethod
    def _repair(row: sqlite3.Row) -> RepairJob:
        data = dict(row)
        data["request"] = OHLCVRequest.model_validate_json(data["request"])
        return RepairJob.model_validate(data)

    async def claim_repair(self) -> RepairJob | None:
        now = aware_utc(self.clock())
        def claim(conn: sqlite3.Connection) -> RepairJob | None:
            conn.execute("BEGIN IMMEDIATE")
            # Expired leases are recoverable; retry count still bounds repeated crashes.
            conn.execute("UPDATE repairs SET state='exhausted',lease_token=NULL WHERE attempts>=3 AND state IN ('pending','running') AND due_at<=?", (now.isoformat(),))
            row = conn.execute("SELECT * FROM repairs WHERE state IN ('pending','running') AND due_at<=? AND attempts<3 ORDER BY due_at LIMIT 1", (now.isoformat(),)).fetchone()
            if row is None:
                return None
            token = str(uuid4())
            conn.execute("UPDATE repairs SET state='running',attempts=attempts+1,lease_token=?,due_at=? WHERE job_id=?",
                         (token, (now + timedelta(minutes=30)).isoformat(), row["job_id"]))
            return self._repair(conn.execute("SELECT * FROM repairs WHERE job_id=?", (row["job_id"],)).fetchone())
        return await self._run(claim)

    async def finish_repair(self, job: RepairJob, *, result_dataset_id: str | None, error: str | None) -> None:
        state = "complete" if error is None else ("exhausted" if job.attempts >= 3 else "pending")
        due = aware_utc(self.clock()) + timedelta(minutes=5 * 2 ** job.attempts)
        def finish(conn: sqlite3.Connection) -> None:
            cursor = conn.execute("UPDATE repairs SET state=?,due_at=?,result_dataset_id=?,last_error=?,lease_token=NULL WHERE job_id=? AND state='running' AND lease_token=?",
                                  (state, due.isoformat(), result_dataset_id, error, job.job_id, job.lease_token))
            if cursor.rowcount != 1:
                raise RuntimeError("repair lease lost")
        await self._run(finish)

    async def save_order_context(self, envelope: OrderEnvelope) -> None:
        await self._run(lambda conn: conn.execute("INSERT INTO order_contexts VALUES(?,?) ON CONFLICT(order_uuid) DO UPDATE SET payload=excluded.payload",
                                                 (envelope.order_uuid, envelope.model_dump_json())).rowcount)

    async def save_executions(self, executions: list[ExecutionDetail]) -> None:
        now = aware_utc(self.clock()).isoformat()
        def save(conn: sqlite3.Connection) -> None:
            for execution in executions:
                if not execution.exec_id:
                    continue
                base, separator, suffix = execution.exec_id.rpartition(".")
                correction_key = base if separator and suffix.isdigit() else execution.exec_id
                revision = int(suffix) if separator and suffix.isdigit() else 0
                existing = conn.execute("SELECT payload FROM executions WHERE account_id=? AND exec_id=?",
                                        (execution.account_id, execution.exec_id)).fetchone()
                if existing:
                    old = ExecutionDetail.model_validate_json(existing["payload"])
                    execution = execution.model_copy(update={name: getattr(old, name) for name in ("commission", "commission_currency", "realized_pnl") if getattr(execution, name) is None})
                conn.execute("INSERT INTO executions VALUES(?,?,?,?,?,?) ON CONFLICT(account_id,exec_id) DO UPDATE SET payload=excluded.payload,recorded_at=excluded.recorded_at",
                             (execution.account_id, execution.exec_id, correction_key, revision, now, execution.model_dump_json()))
        await self._run(save)

    async def execution_inputs(self, account_id: str) -> tuple[list[OrderEnvelope], list[ExecutionDetail]]:
        def read(conn: sqlite3.Connection) -> tuple[list[OrderEnvelope], list[ExecutionDetail]]:
            orders = [OrderEnvelope.model_validate_json(row[0]) for row in conn.execute("SELECT payload FROM order_contexts")]
            # All corrections remain archived, but only the latest revision contributes to TCA.
            rows = conn.execute("SELECT * FROM executions WHERE account_id=? ORDER BY revision,recorded_at", (account_id,)).fetchall()
            latest = {row["correction_key"]: ExecutionDetail.model_validate_json(row["payload"]) for row in rows}
            return orders, list(latest.values())
        return await self._run(read)
