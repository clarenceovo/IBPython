"""IBKR connection management, reconnection, error handling, and retry infrastructure.

This module owns the IB instance lifecycle and exposes shared infrastructure
used by all domain-specific sub-clients (historical, options, account, reference).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time as monotonic_time
from datetime import datetime, timezone
from uuid import uuid4
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, ConfigDict, StrictBool

from src.config import config_constant as constants
from src.transport.metrics import metrics
from src.feeds.exceptions import IBKRConnectionDisabledError, IBKRConnectionError

if TYPE_CHECKING:
    from src.feeds.contracts import ContractSpec
    from src.feeds.ibkr_feed import IBKRHistoricalPacingGuard

logger = logging.getLogger(__name__)

DETECTION_TIMEOUT = 180  # seconds before IBKR reports idle timeout


class IBKRConnectionSwitch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: StrictBool


def _ib_insync_compatible_loop() -> asyncio.AbstractEventLoop:
    """Return the loop ib_insync should use for socket futures."""

    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.get_event_loop_policy().get_event_loop()


def _patch_ib_insync_loop_getters() -> None:
    """Force ib_insync internals to use the currently running loop.

    ib_insync 0.9.x uses ``asyncio.get_event_loop_policy().get_event_loop()``
    through module-level ``getLoop`` aliases. Under ASGI servers this can
    resolve to a policy/default loop while the request is running on another
    loop, producing "Future attached to a different loop". Rebinding these
    aliases keeps socket creation, throttling, and timeout callbacks on the
    active uvicorn request loop.
    """

    try:
        import ib_insync.client as ib_client
        import ib_insync.connection as ib_connection
        import ib_insync.util as ib_util
        import ib_insync.wrapper as ib_wrapper
    except ImportError:
        return

    ib_util.getLoop = _ib_insync_compatible_loop
    ib_connection.getLoop = _ib_insync_compatible_loop
    ib_client.getLoop = _ib_insync_compatible_loop
    ib_wrapper.getLoop = _ib_insync_compatible_loop


def _maybe_apply_nest_asyncio() -> None:
    """Patch nested event loops only when the current loop supports it.

    Uvicorn with ``uvicorn[standard]`` may default to uvloop, which
    ``nest_asyncio`` cannot patch.  The API runner forces ``--loop asyncio``,
    but this guard keeps connection attempts from failing before they even
    reach the IBKR socket if a different runner is used.
    """

    try:
        import nest_asyncio
    except ImportError:
        return
    try:
        nest_asyncio.apply()
    except ValueError as exc:
        logger.info("nest_asyncio not applied to current event loop: %s", exc)


def _root_cause_message(exc: BaseException) -> str:
    root: BaseException = exc
    while root.__cause__ is not None or root.__context__ is not None:
        root = root.__cause__ or root.__context__  # type: ignore[assignment]
    message = str(root).strip()
    if not message:
        message = root.__class__.__name__
    return f"{root.__class__.__name__}: {message}"


def _last_ibkr_error_message(value: tuple[int, str] | None) -> str:
    if value is None:
        return "last_ibkr_error=none."
    code, message = value
    hint = ""
    if code == 326:
        hint = " Hint: choose a unique IBKR_CLIENT_ID for the API; notebooks and TWS API clients cannot share one."
    elif code == 200:
        hint = " Hint: contract not found; check symbol, secType, exchange, currency, and conId."
    elif code in {162, 354, 10167}:
        hint = " Hint: market data is not subscribed or unavailable; direct index Level 1 data may require an additional IBKR subscription."
    return f"last_ibkr_error={code}: {message}.{hint}"


def _contract_text(contract: Any, *attribute_names: str) -> str:
    for attribute_name in attribute_names:
        value = getattr(contract, attribute_name, None)
        if value is not None and str(value).strip():
            return str(value).strip().upper()
    return ""


def _contract_int(contract: Any, attribute_name: str) -> int | None:
    value = getattr(contract, attribute_name, None)
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _qualification_hint(spec: "ContractSpec") -> str:
    # Avoid circular import — import inline.
    from src.feeds.models import AssetClass

    if spec.con_id:
        return f" Check that con_id={spec.con_id} is valid for {spec.symbol}."
    if spec.asset_class is AssetClass.EQUITY and spec.exchange.upper() == "SMART":
        if spec.symbol.upper() == "TSLA":
            return " Add primary_exchange='NASDAQ' for TSLA or pass underlying_con_id if you already know the IBKR conId."
        return " Add primary_exchange for SMART-routed equities, or pass underlying_con_id when available."
    if spec.asset_class is AssetClass.INDEX:
        return " Confirm the index exchange, for example CBOE for SPX."
    return " Confirm symbol, asset_class, exchange, currency, and contract-specific identifiers."


class IBKRConnectionManager:
    """Manages the IB instance lifecycle, reconnection, event handlers, and retry logic.

    This is the shared base that all domain clients compose via dependency injection.
    """

    def __init__(
        self,
        host: str = constants.DEFAULT_IBKR_HOST,
        port: int = constants.DEFAULT_IBKR_PORT,
        client_id: int = constants.DEFAULT_IBKR_CLIENT_ID,
        *,
        retry_attempts: int = 3,
        retry_base_delay_seconds: float = 0.5,
        pacing_guard: "IBKRHistoricalPacingGuard | None" = None,
        rate_limiter: Any | None = None,
        redis: Any | None = None,
        connection_control_key: str = constants.DEFAULT_IBKR_CONNECTION_CONTROL_REDIS_KEY,
        connection_control_poll_seconds: float = constants.DEFAULT_IBKR_CONNECTION_CONTROL_POLL_SECONDS,
        on_connected: Any | None = None,
        client_role: str = "notebook",
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.retry_attempts = retry_attempts
        self.retry_base_delay_seconds = retry_base_delay_seconds
        self._ib: Any | None = None
        self._pacing_guard = pacing_guard  # set later if not provided
        self._rate_limiter = rate_limiter
        self._shutting_down = False
        self._reconnect_lock = asyncio.Lock()
        self._last_ibkr_error: tuple[int, str] | None = None
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._connection_dead = False
        self._notification_callback: Any | None = None
        self._control_redis = redis
        self.connection_control_key = connection_control_key
        self._control_poll_seconds = connection_control_poll_seconds
        self._control_lock = asyncio.Lock()
        self._control_task: asyncio.Task[None] | None = None
        self._connect_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._cleanup_tasks: set[asyncio.Task[Any]] = set()
        self._session_end_callbacks: list[Any] = []
        self._session_start_callbacks: list[Any] = []
        self.generation = 0
        self.session_ended = asyncio.Event()
        self._closed = False
        self.connection_enabled = redis is None
        self.desired_enabled: bool | None = True if redis is None else None
        self.connection_control_error: str | None = None
        self._on_connected = on_connected
        self.market_data_type = 1
        self.client_role = client_role
        self.instance_id = str(uuid4())
        self.last_control_check: str | None = None
        self._connect_error: str | None = None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def ib(self) -> Any | None:
        return self._ib

    @property
    def is_connected(self) -> bool:
        """Whether the underlying ib_insync IB instance is connected."""
        return self._ib is not None and self._ib.isConnected()

    @property
    def pacing_guard(self) -> "IBKRHistoricalPacingGuard | None":
        return self._pacing_guard

    @pacing_guard.setter
    def pacing_guard(self, value: "IBKRHistoricalPacingGuard | None") -> None:
        self._pacing_guard = value

    @property
    def rate_limiter(self) -> Any | None:
        return self._rate_limiter

    @rate_limiter.setter
    def rate_limiter(self, value: Any | None) -> None:
        self._rate_limiter = value

    @property
    def shutting_down(self) -> bool:
        return self._shutting_down

    @property
    def last_ibkr_error(self) -> tuple[int, str] | None:
        return self._last_ibkr_error

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def capture_session(self) -> tuple[int, Any]:
        self.require_connection_enabled()
        if not self.is_connected or self._shutting_down:
            raise IBKRConnectionError("IBKR session is not connected")
        return self.generation, self._ib

    def require_session(self, session: tuple[int, Any]) -> None:
        self.require_connection_enabled()
        if self._shutting_down or session != (self.generation, self._ib) or self._ib is None:
            raise IBKRConnectionError("IBKR connection changed; submit a new request")

    def add_session_end_callback(self, callback: Any) -> None:
        self._session_end_callbacks.append(callback)

    def _end_session(self) -> None:
        self.generation += 1
        self.session_ended.set()
        self.session_ended = asyncio.Event()
        for callback in self._session_end_callbacks:
            try:
                result = callback(self._ib)
                if inspect.isawaitable(result):
                    self.retain_background_task(asyncio.create_task(result))
            except Exception:
                logger.exception("IBKR session cleanup callback failed")

    def _require_generation(self, generation: int) -> None:
        self.require_connection_enabled()
        if generation != self.generation or self._closed or self._shutting_down:
            raise IBKRConnectionError("IBKR connection attempt was interrupted")

    async def connect(self) -> None:
        # An explicit later connect may reopen a closed manager; older calls cannot.
        generation = self.generation
        self._closed = False
        self._shutting_down = False
        await self.start_connection_control()
        await self.refresh_connection_control()
        self._require_generation(generation)
        if self.is_connected:
            return
        if self._connect_task is None or self._connect_task.done():
            self._connect_task = asyncio.create_task(self._connect(generation))
            self._connect_task.add_done_callback(self._consume_task)
        task = self._connect_task
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            self._require_generation(generation)
            raise

    async def _connect(self, generation: int) -> None:
        self._require_generation(generation)
        self._connection_dead = False
        if self.is_connected:
            return
        self._disconnect_stale_client()
        from ib_insync import IB

        _patch_ib_insync_loop_getters()
        _maybe_apply_nest_asyncio()
        ib = IB()
        self._ib = ib
        ib.errorEvent += self._on_ibkr_error
        ib.disconnectedEvent += self._on_ibkr_disconnected
        ib.timeoutEvent += self._on_ibkr_timeout
        self._last_ibkr_error = None
        try:
            await self._with_retry(
                lambda: ib.connectAsync(self.host, self.port, clientId=self.client_id), operation="connect",
            )
            self._require_generation(generation)
            if self._ib is not ib or not ib.isConnected():
                raise IBKRConnectionError("IBKR socket setup did not complete")
            ib.setTimeout(DETECTION_TIMEOUT)
            # Minimal fake clients may omit this API; real IB always supplies it.
            set_mode = getattr(ib, "reqMarketDataType", None)
            if callable(set_mode):
                set_mode(self.market_data_type)
            for callback in self._session_start_callbacks:
                callback(ib)
            if self._on_connected is not None:
                await self._on_connected()
            self._require_generation(generation)
            self._connect_error = None
            logger.info("IBKR connected clientId=%s generation=%s", self.client_id, generation)
        except BaseException:
            if self._ib is ib:
                self._dispose_ib_client(reason="connection attempt ended")
            else:
                # A cancellation-resistant old attempt must only close its own socket.
                self._close_ib_client(ib, reason="obsolete connection attempt")
            raise

    @staticmethod
    def _consume_task(task: asyncio.Task[Any]) -> None:
        if not task.cancelled():
            task.exception()

    def add_session_start_callback(self, callback: Any) -> None:
        """Bind listeners to every replacement IB client, before trading resumes."""
        self._session_start_callbacks.append(callback)
        if self._ib is not None:
            callback(self._ib)

    def _stop_connection(self, *, stop_control: bool) -> set[asyncio.Task[Any]]:
        self._shutting_down = True
        if stop_control:
            self._closed = True
        self._end_session()
        # This happens before the first await, including when the caller is cancelled.
        self._dispose_ib_client(reason="disconnect")
        current = asyncio.current_task()
        cancelled = set(self._background_tasks)
        cancelled.update(task for task in (self._connect_task, self._reconnect_task) if task is not None)
        self._connect_task = self._reconnect_task = None
        if stop_control and self._control_task is not None:
            cancelled.add(self._control_task)
            self._control_task = None
        for task in cancelled:
            if task is not current and not task.done():
                task.cancel()
        return {task for task in cancelled | self._cleanup_tasks if task is not current and not task.done()}

    async def _drain_cleanup(self, tasks: set[asyncio.Task[Any]]) -> None:
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=5)
        self._background_tasks.difference_update(task for task in tasks if task.done())
        for task in pending:
            task.cancel()
        if pending:
            logger.warning("IBKR cleanup deadline reached: %s tasks; socket already closed", len(pending))

    async def disconnect(self, *, stop_control: bool = True) -> None:
        tasks = self._stop_connection(stop_control=stop_control)
        cleanup = asyncio.create_task(self._drain_cleanup(tasks))
        self.retain_background_task(cleanup)
        await asyncio.shield(cleanup)

    async def __aenter__(self) -> "IBKRConnectionManager":
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.disconnect()

    async def ensure_connected(self) -> None:
        """Ensure the IB connection is alive, reconnecting if necessary."""
        self.require_connection_enabled()
        if self._ib is not None and self._ib.isConnected():
            return
        generation = self.generation
        async with self._reconnect_lock:
            self._require_generation(generation)
            self.require_connection_enabled()
            # Double-check after acquiring lock.
            if self._ib is not None and self._ib.isConnected():
                return
            logger.info("connection stale or missing; reconnecting")
            try:
                await self.connect()
            except IBKRConnectionDisabledError:
                raise
            except Exception as exc:
                root_cause = _root_cause_message(exc)
                logger.exception(
                    "IBKR connection unavailable: host=%s port=%d clientId=%d root_cause=%s last_ibkr_error=%s",
                    self.host,
                    self.port,
                    self.client_id,
                    root_cause,
                    self._last_ibkr_error,
                )
                raise IBKRConnectionError(
                    f"IBKR not available at {self.host}:{self.port} — "
                    f"ensure TWS or IB Gateway is running and API connections are enabled. "
                    f"clientId={self.client_id}. root_cause={root_cause}. "
                    f"{_last_ibkr_error_message(self._last_ibkr_error)}"
                ) from exc

    def require_connection_enabled(self) -> None:
        if not self.connection_enabled:
            raise IBKRConnectionDisabledError("IBKR connection disabled by Redis control switch")

    async def start_connection_control(self) -> None:
        if self._control_redis is None or self._closed:
            return
        if self._control_task is None or self._control_task.done():
            self._control_task = asyncio.create_task(self._watch_connection_control())
            self._control_task.add_done_callback(self._consume_task)
        await self.refresh_connection_control()

    async def refresh_connection_control(self) -> None:
        if self._control_redis is None or self._closed:
            return
        generation = self.generation
        async with self._control_lock:
            if self._closed or generation != self.generation:
                return
            try:
                async with asyncio.timeout(2):
                    raw = await self._control_redis.get_raw(self.connection_control_key)
                desired = None if raw is None else IBKRConnectionSwitch.model_validate_json(raw).enabled
                error = "missing" if raw is None else None
            except Exception as exc:
                desired, error = None, type(exc).__name__
            # A read that began before shutdown cannot restart the manager.
            if self._closed or generation != self.generation:
                return
            self.last_control_check = datetime.now(timezone.utc).isoformat()
            self.desired_enabled = desired
            self.connection_control_error = error
            await self._apply_connection_switch(desired is True)

    async def write_connection_control(self, switch: IBKRConnectionSwitch) -> None:
        if self._control_redis is None:
            raise IBKRConnectionError("Redis connection control is not configured")
        # Own the write/application so cancellation of an HTTP request cannot split them.
        task = asyncio.create_task(self._write_connection_control(switch))
        self.retain_background_task(task)
        await asyncio.shield(task)

    async def _write_connection_control(self, switch: IBKRConnectionSwitch) -> None:
        generation = self.generation
        async with self._control_lock:
            try:
                async with asyncio.timeout(2):
                    await self._control_redis.set_raw(self.connection_control_key, switch.model_dump_json())
            except Exception as exc:
                self.connection_control_error = type(exc).__name__
                self.desired_enabled = None
                await self._apply_connection_switch(False)
                raise
            if self._closed or generation != self.generation:
                return
            self.last_control_check = datetime.now(timezone.utc).isoformat()
            self.desired_enabled = switch.enabled
            self.connection_control_error = None
            await self._apply_connection_switch(switch.enabled)

    async def _apply_connection_switch(self, enabled: bool, *, reconnect: bool = False) -> None:
        changed = self.connection_enabled != enabled
        self.connection_enabled = enabled
        if not enabled:
            self._connection_dead = False
            if changed or self._ib is not None or (self._connect_task is not None and not self._connect_task.done()):
                tasks = self._stop_connection(stop_control=False)
                self.retain_background_task(asyncio.create_task(self._drain_cleanup(tasks)))
        elif not self._closed:
            self._shutting_down = False
            self._schedule_reconnect()

    def _schedule_reconnect(self) -> None:
        if self._closed or self._shutting_down or not self.connection_enabled or self.is_connected:
            return
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect())
            self._reconnect_task.add_done_callback(self._consume_task)

    def control_status(self) -> dict[str, Any]:
        connected = self.is_connected
        connecting = self._connect_task is not None and not self._connect_task.done()
        retrying = self._reconnect_task is not None and not self._reconnect_task.done()
        status = "connected" if connected else "disabled" if not self.connection_enabled else "down" if self._connection_dead else "disconnected"
        lifecycle = "connected" if connected else "disabled" if not self.connection_enabled else "connecting" if connecting else "retrying" if retrying else "disconnected"
        return {
            "enabled": self.connection_enabled, "desired_enabled": self.desired_enabled,
            "connection_status": status, "socket_connected": connected, "lifecycle_state": lifecycle,
            "generation": self.generation, "redis_key": self.connection_control_key,
            "client_id": self.client_id, "instance_id": self.instance_id, "role": self.client_role,
            "last_checked_at": self.last_control_check,
            "last_error": self.connection_control_error or self._connect_error,
            "updated_at": datetime.now(timezone.utc).isoformat(), "poll_seconds": self._control_poll_seconds,
            "ttl_seconds": max(60, int(6 * self._control_poll_seconds)),
        }

    async def _publish_control_status(self) -> None:
        if self._control_redis is None:
            return
        status = self.control_status()
        async with asyncio.timeout(2):
            await self._control_redis.set_raw(
                f"{self.connection_control_key}:clients:{self.instance_id}", json.dumps(status), ex=status["ttl_seconds"],
            )

    async def _watch_connection_control(self) -> None:
        while not self._closed:
            try:
                await self.refresh_connection_control()
                await self._publish_control_status()
            except Exception:
                logger.warning("IBKR control watcher iteration failed", exc_info=True)
            await asyncio.sleep(self._control_poll_seconds)

    # ------------------------------------------------------------------
    # IBKR event handlers
    # ------------------------------------------------------------------

    _DATA_FARM_CODES: ClassVar[frozenset[int]] = frozenset({
        2103, 2104, 2105, 2106, 2107, 2108, 2158,
    })

    def _on_ibkr_error(self, req_id: int, error_code: int, error_string: str, contract: Any = None) -> None:
        """Log IBKR errors at appropriate severity levels."""
        self._last_ibkr_error = (error_code, error_string)
        if error_code in self._DATA_FARM_CODES:
            logger.info("IBKR data farm status [%s]: %s", error_code, error_string)
        elif error_code == 326:
            logger.error(
                "IBKR client id already in use [%s]: %s (reqId=%s). "
                "Use a different IBKR_CLIENT_ID for the API than notebooks/other clients.",
                error_code,
                error_string,
                req_id,
            )
        elif error_code in (404,):
            logger.warning("IBKR pacing violation [%s]: %s (reqId=%s)", error_code, error_string, req_id)
        elif error_code in (1100, 1101, 1102):
            logger.warning("IBKR connectivity event [%s]: %s (reqId=%s)", error_code, error_string, req_id)
        elif error_code in (200, 201, 321, 502, 504):
            logger.error("IBKR error [%s]: %s (reqId=%s)", error_code, error_string, req_id)
        elif error_code == 399:
            logger.debug("IBKR info [%s]: %s (reqId=%s)", error_code, error_string, req_id)
        else:
            logger.warning("IBKR unhandled error [%s]: %s (reqId=%s)", error_code, error_string, req_id)

    def _on_ibkr_disconnected(self, *_: object) -> None:
        """Handle IB disconnection events with automatic reconnection."""
        if self._shutting_down or not self.connection_enabled:
            logger.info("IBKR disconnected during shutdown – skipping reconnection")
            return
        if self._connect_task is not None and not self._connect_task.done():
            return  # The owning connection attempt handles setup failure.
        logger.warning("IBKR disconnected – attempting reconnection")
        self._end_session()
        self._schedule_reconnect()

    def _on_ibkr_timeout(self, *_: object) -> None:
        """Handle IB timeout events."""
        if self._shutting_down:
            return
        if self._ib is not None and self._ib.isConnected():
            logger.info("IBKR idle timeout (connection still alive)")
        else:
            logger.warning("IBKR connection timeout – connection lost")

    async def _reconnect(self) -> None:
        """Keep trying while enabled; stop immediately on disable or shutdown."""
        generation = self.generation
        attempt = 0
        while not self._shutting_down and not self._closed and self.connection_enabled and generation == self.generation:
            if self.is_connected:
                return
            try:
                async with self._reconnect_lock:
                    self._require_generation(generation)
                    await self.connect()
                return
            except IBKRConnectionDisabledError:
                return
            except Exception as exc:
                if generation != self.generation or self._closed:
                    return
                attempt += 1
                self._connect_error = str(exc)
                self._connection_dead = attempt >= 5
                if attempt == 5:
                    await self._fire_connection_dead_alert(exc)
                delay = min(30, 2 ** min(attempt, 5))
                logger.warning("IBKR reconnect failed clientId=%s attempt=%s; retry in %ss", self.client_id, attempt, delay)
                await asyncio.sleep(delay)

    # ------------------------------------------------------------------
    # Connection dead alerting
    # ------------------------------------------------------------------

    async def _fire_connection_dead_alert(self, last_error: Exception) -> None:
        """Fire notification callback when connection is dead after all retries exhausted."""
        if self._notification_callback is None:
            return
        try:
            import asyncio
            loop = asyncio.get_running_loop()
            task = loop.create_task(
                self._notification_callback(
                    event="ibkr_connection_dead",
                    host=self.host,
                    port=self.port,
                    client_id=self.client_id,
                    last_error=str(last_error),
                    last_ibkr_error=self._last_ibkr_error,
                )
            )
            # Prevent GC of fire-and-forget task
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        except RuntimeError:
            logger.debug("no event loop for connection dead notification")
        except Exception:
            logger.exception("connection dead notification callback failed")

    # ------------------------------------------------------------------
    # Retry infrastructure
    # ------------------------------------------------------------------

    # IBKR error codes that indicate transient / recoverable conditions.
    _TRANSIENT_IBKR_CODES: ClassVar[frozenset[int]] = frozenset({502, 504, 1100, 1101, 1102})

    def is_transient_error(self, exc: BaseException) -> bool:
        """Return True only for connection-level and IBKR transient errors."""
        if isinstance(exc, (ConnectionError, OSError)):
            return True
        # Walk the exception chain (e.g. RuntimeError wrapping the real cause).
        chain: BaseException | None = exc
        while chain is not None:
            if isinstance(chain, (ConnectionError, OSError)):
                return True
            ibkr_code = getattr(chain, "code", None)
            if isinstance(ibkr_code, int) and ibkr_code in self._TRANSIENT_IBKR_CODES:
                return True
            chain = chain.__cause__ or chain.__context__
        return False

    async def with_retry(self, call: Any, *, operation: str) -> Any:
        """Execute an async callable with retry logic for transient errors."""
        last_error: BaseException | None = None
        generation, owner = self.generation, self._ib
        t0 = monotonic_time.monotonic()
        for attempt in range(1, self.retry_attempts + 1):
            self._require_generation(generation)
            try:
                await self.wait_for_ibkr_request(operation=operation)
                self._require_generation(generation)
                if owner is not self._ib:
                    raise IBKRConnectionError("IBKR connection changed during request")
                result = await call()
                self._require_generation(generation)
                if owner is not self._ib:
                    raise IBKRConnectionError("IBKR connection changed during request")
                elapsed = monotonic_time.monotonic() - t0
                metrics.ibkr_request_duration.observe(elapsed, {"operation": operation, "status": "ok"})
                return result
            except IBKRConnectionError:
                raise
            except Exception as exc:  # pragma: no cover - exercised with live gateways.
                last_error = exc
                if not self.is_transient_error(exc) or attempt >= self.retry_attempts:
                    break
                delay = self.retry_base_delay_seconds * (2 ** (attempt - 1))
                logger.warning("IBKR %s failed on attempt %d/%d; retrying in %.2fs", operation, attempt, self.retry_attempts, delay)
                await asyncio.sleep(delay)
        elapsed = monotonic_time.monotonic() - t0
        error_type = type(last_error).__name__ if last_error else "Unknown"
        metrics.ibkr_request_duration.observe(elapsed, {"operation": operation, "status": "error"})
        metrics.ibkr_request_errors.inc({"operation": operation, "error_type": error_type})
        raise RuntimeError(f"IBKR operation failed after retries: {operation}") from last_error

    # Backwards-compatible alias used by facade
    async def _with_retry(self, call: Any, *, operation: str) -> Any:
        return await self.with_retry(call, operation=operation)

    # ------------------------------------------------------------------
    # Server time
    # ------------------------------------------------------------------

    async def get_server_time(self) -> dict[str, Any]:
        """Request the current IBKR server time.

        Uses ``reqCurrentTimeAsync()``.
        """
        await self.ensure_connected()
        logger.info("get_server_time: requesting")

        result = await self.with_retry(
            lambda: self._ib.reqCurrentTimeAsync(),
            operation="server_time",
        )

        server_time_str = str(result) if result is not None else ""
        logger.info("get_server_time: %s", server_time_str)
        return {"server_time": server_time_str}

    async def wait_for_ibkr_request(self, *, operation: str, weight: int = 1) -> None:
        if self._rate_limiter is None:
            return
        await self._rate_limiter.wait_for_request(operation=operation, weight=weight)

    async def acquire_market_data_line(
        self,
        *,
        contract_key: str,
        operation: str,
        ttl_seconds: float | None = None,
    ) -> Any:
        if self._rate_limiter is None:
            return _NoopMarketDataLease()
        return await self._rate_limiter.acquire_market_data_line(
            contract_key=contract_key,
            operation=operation,
            ttl_seconds=ttl_seconds,
        )

    async def rate_limit_snapshot(self) -> dict[str, Any]:
        if self._rate_limiter is None:
            return {"enabled": False, "reason": "not_configured"}
        return await self._rate_limiter.snapshot()

    def market_data_request_id(self, ib: Any, ticker: Any) -> int:
        """Return the request id for a live market-data ticker.

        ``ib_insync`` does not expose this id on ``Ticker``. Keep access to its
        request registry inside the connection boundary so feed clients do not
        depend on wrapper internals.
        """
        request_id = ib.wrapper.ticker2ReqId["mktData"].get(ticker)
        if request_id is None:
            raise IBKRConnectionError("IBKR did not register the market-data request")
        return int(request_id)

    def forget_market_data_ticker(self, ib: Any, contract: Any, ticker: Any, request_id: int | None) -> None:
        """Drop retained wrapper state for a request-owned cancelled ticker."""
        wrapper = ib.wrapper
        if request_id is not None:
            wrapper.reqId2Ticker.pop(request_id, None)
        wrapper.tickers.pop(id(contract), None)
        wrapper.pendingTickers.discard(ticker)

    def retain_background_task(self, task: asyncio.Task[None]) -> None:
        """Keep lifecycle cleanup alive if its initiating request is cancelled."""
        self._cleanup_tasks.add(task)
        task.add_done_callback(self._cleanup_tasks.discard)
        task.add_done_callback(self._consume_task)

    def _disconnect_stale_client(self) -> None:
        self._dispose_ib_client(reason="stale client")

    def _dispose_ib_client(self, *, reason: str) -> None:
        """Fully release the current ib_insync client and detach callbacks."""
        ib, self._ib = self._ib, None
        if ib is not None:
            self._close_ib_client(ib, reason=reason)

    def _close_ib_client(self, ib: Any, *, reason: str) -> None:
        self._detach_ib_event_handlers(ib)
        try:
            is_connected = getattr(ib, "isConnected", None)
            if callable(is_connected) and is_connected():
                logger.info("disconnecting from IBKR %s:%d clientId=%d (%s)", self.host, self.port, self.client_id, reason)
                ib.disconnect()
        except Exception:
            logger.debug("error disconnecting IBKR client", exc_info=True)

        client = getattr(ib, "client", None)
        try:
            client_connected = getattr(client, "isConnected", None)
            client_disconnect = getattr(client, "disconnect", None)
            if callable(client_disconnect) and (not callable(client_connected) or client_connected()):
                client_disconnect()
        except Exception:
            logger.debug("error disposing low-level IBKR client connection", exc_info=True)
        try:
            client_reset = getattr(client, "reset", None)
            if callable(client_reset):
                client_reset()
        except Exception:
            logger.debug("error resetting low-level IBKR client", exc_info=True)

    def _detach_ib_event_handlers(self, ib: Any) -> None:
        for event_name, handler in (
            ("errorEvent", self._on_ibkr_error),
            ("disconnectedEvent", self._on_ibkr_disconnected),
            ("timeoutEvent", self._on_ibkr_timeout),
        ):
            event = getattr(ib, event_name, None)
            try:
                if event is not None:
                    event -= handler
            except Exception:
                logger.debug("error detaching IBKR %s handler", event_name, exc_info=True)


class _NoopMarketDataLease:
    released = False

    async def release(self) -> None:
        self.released = True


def capture_ibkr_session(connection: Any) -> tuple[int, Any]:
    """Capture a session, including lightweight injected feed adapters."""
    capture = getattr(connection, "capture_session", None)
    if callable(capture):
        return capture()
    return getattr(connection, "generation", 0), connection.ib


def require_ibkr_session(connection: Any, session: tuple[int, Any]) -> None:
    require = getattr(connection, "require_session", None)
    if callable(require):
        require(session)
        return
    if not getattr(connection, "connection_enabled", True):
        raise IBKRConnectionDisabledError("IBKR connection disabled by Redis control switch")
    if session[0] != getattr(connection, "generation", 0) or session[1] is not connection.ib:
        raise IBKRConnectionError("IBKR connection changed; submit a new request")


async def wait_for_ibkr_request(connection: Any, *, operation: str, weight: int = 1) -> None:
    wait = getattr(connection, "wait_for_ibkr_request", None)
    if callable(wait):
        await wait(operation=operation, weight=weight)


async def acquire_market_data_line(
    connection: Any,
    *,
    contract_key: str,
    operation: str,
    ttl_seconds: float | None = None,
) -> Any:
    acquire = getattr(connection, "acquire_market_data_line", None)
    if callable(acquire):
        return await acquire(contract_key=contract_key, operation=operation, ttl_seconds=ttl_seconds)
    return _NoopMarketDataLease()
