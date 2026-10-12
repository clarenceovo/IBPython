"""Bounded historical-data repairs. This worker never places orders."""

from __future__ import annotations

from src.feeds.ohlcv_loader import OHLCVLoader
from src.feeds.research import RepairJob
from src.transport.analytics_store import AnalyticsStore


async def run_quality_repairs(store: AnalyticsStore, loader: OHLCVLoader, *, limit: int = 1,
                              persist: bool = False) -> list[RepairJob]:
    results: list[RepairJob] = []
    for _ in range(limit):
        job = await store.claim_repair()
        if job is None:
            break
        result_id = None
        error = None
        try:
            original = await store.get_dataset(job.dataset_id)
            if original and original.manifest.primary_persistence == "failed" and not persist:
                raise ValueError("primary persistence repair requires the scheduler with persist=true")
            if job.request.continuous:
                raise ValueError("bounded repair of vendor continuous futures is unsupported; capture dated contracts")
            result = await loader.load_detailed(job.request, persist=persist, cache_latest=False, enqueue_repair=False)
            result_id = result.manifest.dataset_id if result.manifest else None
            if result.archive_persistence != "stored":
                error = "repair result was not archived"
            elif result.primary_persistence in {"failed", "no_store"}:
                error = result.persistence_error or "primary store unavailable"
            elif result.quality.issues:
                error = "quality issues remain: " + ",".join(i.code for i in result.quality.issues)
        except Exception as exc:
            error = str(exc)
        await store.finish_repair(job, result_dataset_id=result_id, error=error)
        job.state = "complete" if error is None else ("exhausted" if job.attempts >= 3 else "pending")
        job.last_error = error
        job.result_dataset_id = result_id
        job.lease_token = None
        results.append(job)
    return results
