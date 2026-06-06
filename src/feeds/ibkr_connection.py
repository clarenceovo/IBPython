"""IBKR connection management, reconnection, error handling, and retry infrastructure.

This module owns the IB instance lifecycle and exposes shared infrastructure
used by all domain-specific sub-clients (historical, options, account, reference).
"""

from __future__ import annotations

import asyncio
import logging
import time as monotonic_time
from typing import TYPE_CHECKING, Any, ClassVar

from src.config import config_constant as constants
from src.transport.metrics import metrics
from src.feeds.exceptions import IBKRConnectionError

if TYPE_CHECKING:
    from src.feeds.contracts import ContractSpec
    from src.feeds.ibkr_feed import IBKRHistoricalPacingGuard

logger = logging.getLogger(__name__)

DETECTION_TIMEOUT = 180  # seconds before IBKR reports idle timeout


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

    async def connect(self) -> None:
        self._shutting_down = False
        self._connection_dead = False
        if self._ib is not None and self._ib.isConnected():
            logger.debug("connect skipped – already connected to %s:%d clientId=%d", self.host, self.port, self.client_id)
            return

        logger.info("connecting to IBKR at %s:%d clientId=%d", self.host, self.port, self.client_id)

        try:
            from ib_insync import IB
        except ImportError as exc:
            raise RuntimeError("ib_insync is required to connect to IBKR") from exc

        _patch_ib_insync_loop_getters()
        _maybe_apply_nest_asyncio()

        # Always create a fresh IB() on the current running loop.
        self._ib = IB()
        self._ib.errorEvent += self._on_ibkr_error
        self._ib.disconnectedEvent += self._on_ibkr_disconnected
        self._ib.timeoutEvent += self._on_ibkr_timeout
        t0 = monotonic_time.monotonic()
        try:
            await self._with_retry(
                lambda: self._ib.connectAsync(self.host, self.port, clientId=self.client_id),
                operation="connect",
            )
            if not self._ib.isConnected():
                raise IBKRConnectionError("connectAsync returned but IBKR client is not connected")
            logger.info("connected to IBKR in %.2fs", monotonic_time.monotonic() - t0)
            self._ib.setTimeout(DETECTION_TIMEOUT)
        except Exception:
            logger.exception(
                "failed to connect to IBKR at %s:%d clientId=%d; last_ibkr_error=%s",
                self.host,
                self.port,
                self.client_id,
                self._last_ibkr_error,
            )
            self._disconnect_stale_client()
            raise

    async def disconnect(self) -> None:
        self._shutting_down = True
        current_task = asyncio.current_task()
        reconnect_tasks = [task for task in self._background_tasks if task is not current_task and not task.done()]
        for task in reconnect_tasks:
            task.cancel()
        if reconnect_tasks:
            await asyncio.gather(*reconnect_tasks, return_exceptions=True)
            self._background_tasks.difference_update(reconnect_tasks)
        if self._ib is not None and self._ib.isConnected():
            logger.info("disconnecting from IBKR %s:%d clientId=%d", self.host, self.port, self.client_id)
            self._ib.disconnect()

    async def __aenter__(self) -> "IBKRConnectionManager":
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.disconnect()

    async def ensure_connected(self) -> None:
        """Ensure the IB connection is alive, reconnecting if necessary."""
        if self._ib is not None and self._ib.isConnected():
            return
        async with self._reconnect_lock:
            # Double-check after acquiring lock.
            if self._ib is not None and self._ib.isConnected():
                return
            logger.info("connection stale or missing; reconnecting")
            try:
                await self.connect()
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
        if self._shutting_down:
            logger.info("IBKR disconnected during shutdown – skipping reconnection")
            return
        logger.warning("IBKR disconnected – attempting reconnection")
        task = asyncio.create_task(self._reconnect())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _on_ibkr_timeout(self, *_: object) -> None:
        """Handle IB timeout events."""
        if self._shutting_down:
            return
        if self._ib is not None and self._ib.isConnected():
            logger.info("IBKR idle timeout (connection still alive)")
        else:
            logger.warning("IBKR connection timeout – connection lost")

    async def _reconnect(self) -> None:
        """Reconnect with exponential backoff (max 5 attempts)."""
        async with self._reconnect_lock:
            # Another coroutine may have already reconnected.
            if self._ib is not None and self._ib.isConnected():
                logger.info("IBKR already reconnected by another coroutine")
                return
            max_attempts = 5
            for attempt in range(1, max_attempts + 1):
                if self._shutting_down:
                    logger.info("IBKR reconnect aborted – shutdown in progress")
                    return
                try:
                    logger.info("IBKR reconnect attempt %s/%s", attempt, max_attempts)
                    if self._ib is not None:
                        try:
                            self._ib.disconnect()
                        except Exception:
                            logger.debug("failed to disconnect stale IBKR instance", exc_info=True)
                    await self._with_retry(
                        lambda: self._ib.connectAsync(self.host, self.port, clientId=self.client_id),
                        operation="reconnect",
                    )
                    self._ib.setTimeout(DETECTION_TIMEOUT)
                    logger.info("IBKR reconnected successfully on attempt %s", attempt)
                    return
                except Exception as exc:
                    delay = 2 ** attempt
                    logger.error("IBKR reconnect attempt %s failed: %s; next retry in %ss", attempt, exc, delay)
                    if attempt >= max_attempts:
                        self._connection_dead = True
                        logger.critical(
                            "IBKR reconnection FAILED after %s attempts — connection is DEAD. "
                            "host=%s port=%d clientId=%d last_error=%s last_ibkr_error=%s "
                            "All market data requests will fail until connection is restored.",
                            max_attempts,
                            self.host,
                            self.port,
                            self.client_id,
                            str(exc),
                            self._last_ibkr_error,
                        )
                        await self._fire_connection_dead_alert(exc)
                        break
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
            loop.create_task(
                self._notification_callback(
                    event="ibkr_connection_dead",
                    host=self.host,
                    port=self.port,
                    client_id=self.client_id,
                    last_error=str(last_error),
                    last_ibkr_error=self._last_ibkr_error,
                )
            )
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
        t0 = monotonic_time.monotonic()
        for attempt in range(1, self.retry_attempts + 1):
            try:
                await self.wait_for_ibkr_request(operation=operation)
                result = await call()
                elapsed = monotonic_time.monotonic() - t0
                metrics.ibkr_request_duration.observe(elapsed, {"operation": operation, "status": "ok"})
                return result
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

    def _disconnect_stale_client(self) -> None:
        if self._ib is None:
            return
        try:
            self._ib.disconnect()
        except Exception:
            logger.debug("error disconnecting stale IBKR client", exc_info=True)
        finally:
            self._ib = None


class _NoopMarketDataLease:
    released = False

    async def release(self) -> None:
        self.released = True


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
