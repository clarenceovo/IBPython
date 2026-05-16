# IBPython Brutal Code Review — asyncio/FastAPI Anti-Patterns

> Reviewed against `asyncio-fastapi` skill best practices.
> Date: 2026-05-16

---

## P0 (Critical) — Must Fix

### P0-1: Hardcoded credentials in source code
**File:** `src/config/config_constant.py`, lines 17-18, 22-23
**Description:** `DEFAULT_REDIS_PASSWORD = "96854233"`, `DEFAULT_QUESTDB_PASSWORD = "96854233"` — real passwords committed to source. Anyone with repo access has production credentials.
**Fix:** Replace with empty strings. Require env vars or `.env` for all secrets.

### P0-2: Hardcoded Redis URL with public IP in source
**File:** `src/config/config_constant.py`, line 16
**Description:** `DEFAULT_REDIS_URL = "redis://18.180.162.113:6379/0"` — exposes internal infrastructure IP.
**Fix:** Default to `redis://localhost:6379/0`.

### P0-3: Missing module `market_data_store.py`
**File:** `src/transport/market_data_store.py` (does not exist)
**Description:** 9+ files import `MarketOHLCVStore` from `src.transport.market_data_store` but the file is missing. The entire project is broken — nothing can run or import.
**Fix:** Create the Protocol class defining the `MarketOHLCVStore` interface.

### P0-4: Fire-and-forget reconnection task
**File:** `src/feeds/ibkr_feed.py`, line ~305
**Description:** `_on_ibkr_disconnected` calls `asyncio.ensure_future(self._reconnect())` without storing the task reference. If reconnection fails after all attempts, the error is silently lost. Also, per Python docs, unawaited tasks from `ensure_future` can be GC'd.
**Fix:** Use the fire-and-forget pattern: store task in a set with a done-callback to discard, and log errors from the callback.

### P0-5: `asyncio.Lock()` / `asyncio.Semaphore()` at `__init__` time
**File:** `src/feeds/ibkr_feed.py`, line 264 (`self._reconnect_lock`), lines 1342-1343 (`IBKRHistoricalPacingGuard._lock` and `_concurrency`)
**Description:** Creating `asyncio.Lock()` and `asyncio.Semaphore()` in `__init__` binds them to whatever loop is running (or no loop). If the object is created on one loop and used on another (e.g., during testing or when uvicorn restarts), "Future attached to a different loop" errors occur. The `_reconnect_lock` is mitigated since IBKRFeedClient is typically created on the active loop, but `IBKRHistoricalPacingGuard` is often created at import/config time.
**Fix:** Create locks lazily or use a factory pattern. For the pacing guard, create locks on first `acquire()`.

### P0-6: SSE streaming — no cleanup on `stop_subscription` DELETE
**File:** `src/webapp/routers/streaming.py`, lines ~130-140
**Description:** `stop_subscription` just pops from `_active_subscriptions` dict. But the SSE `event_generator` is still running in the StreamingResponse — the `finally` block only runs when the generator is actually cancelled. The DELETE endpoint can't force-cancel the generator. The subscription is removed from tracking but the underlying IBKR ticker subscription leaks.
**Fix:** Track the asyncio.Task for each SSE stream in the subscription, and cancel it in `stop_subscription`.

### P0-7: Module-level `app = create_app()` in `app.py`
**File:** `src/webapp/app.py`, last line
**Description:** `app = create_app()` runs at import time. This calls `load_settings()` which reads `.env` / env vars, and may trigger imports of heavy dependencies. In test environments, this creates side effects. The `get_app()` factory is correct, but the module-level `app` instantiates eagerly.
**Fix:** Remove the module-level `app = create_app()`. Use `get_app()` in uvicorn entry or `--factory` flag.

### P0-8: `disconnect()` resets `_shutting_down` to False
**File:** `src/feeds/ibkr_feed.py`, lines ~320-325
**Description:** After `disconnect()`, `_shutting_down` is set back to `False`. If a disconnection event fires during shutdown, it will trigger `_reconnect()` because `_shutting_down` is already `False` again. This is a race condition: disconnect sets False, then the disconnected event fires and starts reconnection.
**Fix:** Don't reset `_shutting_down` to False in `disconnect()`. Reset it only in `connect()`.

---

## P1 (Important) — Should Fix

### P1-1: QuestDB `_lock` acquired *after* `_ensure_connection()` in all public methods
**File:** `src/transport/questdb_client.py`
**Description:** `_ensure_connection()` opens/closes connections, then `_lock` protects the query. But two concurrent calls both pass `_ensure_connection()` before either acquires the lock. If the first call closes the connection (in reconnect path), the second's cursor is stale. The lock should wrap the entire operation including the connection check.
**Fix:** Move `_ensure_connection()` inside the lock, or make `_ensure_connection()` lock-aware.

### P1-2: Redis `aclose()` in `close()` but no null guard
**File:** `src/transport/redis_client.py`
**Description:** `close()` sets `_client` to None only implicitly (no reconnection possible after close). If `close()` is called twice, the second `aclose()` on a closed client could error.
**Fix:** Add `self._client = None` after `aclose()`.

### P1-3: `_active_subscriptions` is a plain dict — not thread/task-safe for concurrent requests
**File:** `src/webapp/routers/streaming.py`
**Description:** Multiple concurrent SSE requests modify `_active_subscriptions` without synchronization. In CPython the GIL protects dict operations, but this is relying on an implementation detail and could break with alternative interpreters or heavy load.
**Fix:** Use an `asyncio.Lock` for modifications, or accept the GIL-guaranteed atomicity with a comment.

### P1-4: MySQL client uses global `_lock` for all queries — single bottleneck
**File:** `src/transport/mysql_client.py`
**Description:** Every query (read and write) goes through the same `_lock`. This serializes all MySQL access. Since `aiomysql` provides a connection pool, the lock is unnecessary for reads and limits throughput.
**Fix:** Use the pool's built-in concurrency. Only lock write operations if needed, or remove the lock entirely and rely on the pool.

### P1-5: No CORS middleware on the FastAPI app
**File:** `src/webapp/app.py`
**Description:** No CORS configuration. Browser-based clients (e.g., dashboards) will be blocked.
**Fix:** Add `CORSMiddleware` with configurable origins.

### P1-6: `EquitySnapshotJobHandler` uses `import time as _time` inside `__call__`
**File:** `src/transport/scheduler.py`, line ~435
**Description:** Imports `time` inside the method body. While functionally correct, this is unusual and could mask the intent (is it `time.monotonic` or something else?).
**Fix:** Move import to module top.

### P1-7: Snapshot router catches all exceptions silently in Redis caching
**File:** `src/webapp/routers/snapshot.py`, lines ~155-165
**Description:** When caching snapshots to Redis fails, exceptions are silently swallowed. This could mask persistent Redis failures.
**Fix:** Log at WARNING level, and consider a circuit-breaker pattern.

### P1-8: `build_rest_app_state` creates `asyncio.Lock` objects that bind to the calling loop
**File:** `src/webapp/dependencies.py`
**Description:** When `build_rest_app_state` creates QuestDBClient (with Lock) and MySQLClient (with Lock), those locks bind to whatever event loop is active. If called before uvicorn starts its loop, the locks are on the wrong loop.
**Fix:** The current code calls this inside the lifespan which runs on uvicorn's loop, so this is mitigated. Just be aware for testing.

---

## P2 (Nice-to-have)

### P2-1: Streaming model uses `datetime.now()` without timezone
**File:** `src/feeds/streaming.py`, line 36
**Description:** `timestamp: datetime = Field(default_factory=lambda: datetime.now())` — naive datetime. Should use `datetime.now(timezone.utc)`.

### P2-2: `_active_subscriptions` never cleaned up on app shutdown
**File:** `src/webapp/routers/streaming.py`
**Description:** In-memory dict persists across app lifecycle. Should be cleared in lifespan shutdown.

### P2-3: No structured logging context
**Description:** Log messages use manual string formatting. Consider using structlog or `logging.LoggerAdapter` for consistent request/correlation IDs.

### P2-4: No health check endpoint that verifies downstream connections
**Description:** A `/health` endpoint should verify Redis + QuestDB + IBKR connectivity, not just return 200.

### P2-5: Test coverage for async paths is limited
**Description:** Many tests use mocks but don't test actual async interactions (concurrent access, cancellation, timeout).

---

## Summary

| Severity | Count | Key Themes |
|----------|-------|------------|
| P0 | 8 | Hardcoded secrets, missing module, fire-and-forget, lock-loop binding, SSE resource leak, shutdown race |
| P1 | 8 | Lock ordering, connection management, CORS, silent failures |
| P2 | 5 | Timezone, cleanup, logging, testing |
