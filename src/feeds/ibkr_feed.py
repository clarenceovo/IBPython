from __future__ import annotations

import asyncio
import logging
import math
import time as monotonic_time
from collections import defaultdict, deque
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, ClassVar

from src.config import config_constant as constants
from src.feeds.contracts import ContractSpec, OptionChain, OptionChainRequest, build_ibkr_contract
from src.feeds.fundamental_data import (
    FundamentalDataReport,
    FundamentalDataRequest,
    WSHEventDataReport,
    WSHEventDataRequest,
    WSHMetadataReport,
)
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.feeds.account import (
    AccountPnLDTO,
    AccountSummaryDTO,
    LivePositionDTO,
    PortfolioItemDTO,
    PositionPnLDTO,
    group_account_summary,
    normalize_account_pnl,
    normalize_account_values,
    normalize_portfolio_items,
    normalize_position_pnl,
    normalize_positions,
)
from src.feeds.bonds import (
    BondYieldBar,
    BondYieldHistoryRequest,
    normalize_ibkr_bond_yield_bars,
)
from src.feeds.news import (
    HistoricalNewsHeadline,
    HistoricalNewsRequest,
    NewsArticle,
    NewsArticleRequest,
    NewsProvider,
    format_historical_news_datetime,
    normalize_historical_news,
    normalize_news_article,
    normalize_news_providers,
)
from src.feeds.options import (
    OptionAnalyticsRequest,
    OptionAnalyticsSnapshot,
    OptionSkewSurfaceRequest,
    OptionSkewSurfaceResponse,
    build_ibkr_option_contract,
    build_skew_option_contracts,
    calculate_maturity_skew,
    normalize_option_analytics_from_ticker,
    select_option_chain,
    select_skew_expirations,
    select_skew_strikes,
)

logger = logging.getLogger(__name__)

DETECTION_TIMEOUT = 180  # seconds before IBKR reports idle timeout
US_EQUITY_PRIMARY_EXCHANGE_PREFERENCE: tuple[str, ...] = ("NASDAQ", "NYSE", "ARCA", "AMEX", "BATS")


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


def _contract_details_contract(detail: Any) -> Any:
    return getattr(detail, "contract", detail)


def _is_contract_details_candidate(contract: Any, requested_contract: Any) -> bool:
    for attribute_name in ("secType", "symbol", "currency"):
        expected = _contract_text(requested_contract, attribute_name)
        actual = _contract_text(contract, attribute_name)
        if expected and actual and expected != actual:
            return False
    return True


def _contract_detail_score(contract: Any, spec: ContractSpec, requested_contract: Any) -> int:
    score = 0
    requested_exchange = _contract_text(requested_contract, "exchange")
    contract_exchange = _contract_text(contract, "exchange")
    primary_exchange = _contract_text(contract, "primaryExchange", "primaryExch")
    con_id = _contract_int(contract, "conId")

    if spec.con_id and con_id == spec.con_id:
        score += 10_000
    if _contract_text(contract, "symbol") == _contract_text(requested_contract, "symbol"):
        score += 100
    if _contract_text(contract, "secType") == _contract_text(requested_contract, "secType"):
        score += 80
    if _contract_text(contract, "currency") == _contract_text(requested_contract, "currency"):
        score += 60

    if spec.primary_exchange:
        target_primary = spec.primary_exchange.upper()
        if primary_exchange == target_primary:
            score += 500
        if contract_exchange == target_primary:
            score += 200
    elif spec.asset_class is AssetClass.EQUITY and primary_exchange in US_EQUITY_PRIMARY_EXCHANGE_PREFERENCE:
        score += 100 - US_EQUITY_PRIMARY_EXCHANGE_PREFERENCE.index(primary_exchange)

    if requested_exchange and requested_exchange != "SMART":
        if contract_exchange == requested_exchange:
            score += 300
        if primary_exchange == requested_exchange:
            score += 150
    elif requested_exchange == "SMART" and contract_exchange == "SMART":
        score += 20

    if con_id:
        score += 5
    return score


def _select_contract_from_details(details: Sequence[Any], spec: ContractSpec, requested_contract: Any) -> Any | None:
    if not details:
        return None
    candidates = [
        _contract_details_contract(detail)
        for detail in details
        if _is_contract_details_candidate(_contract_details_contract(detail), requested_contract)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda contract: _contract_detail_score(contract, spec, requested_contract))


def _qualification_hint(spec: ContractSpec) -> str:
    if spec.con_id:
        return f" Check that con_id={spec.con_id} is valid for {spec.symbol}."
    if spec.asset_class is AssetClass.EQUITY and spec.exchange.upper() == "SMART":
        if spec.symbol.upper() == "TSLA":
            return " Add primary_exchange='NASDAQ' for TSLA or pass underlying_con_id if you already know the IBKR conId."
        return " Add primary_exchange for SMART-routed equities, or pass underlying_con_id when available."
    if spec.asset_class is AssetClass.INDEX:
        return " Confirm the index exchange, for example CBOE for SPX."
    return " Confirm symbol, asset_class, exchange, currency, and contract-specific identifiers."


class IBKRFeedClient:
    """Async IBKR market data adapter backed by ib_insync."""

    def __init__(
        self,
        host: str = constants.DEFAULT_IBKR_HOST,
        port: int = constants.DEFAULT_IBKR_PORT,
        client_id: int = constants.DEFAULT_IBKR_CLIENT_ID,
        *,
        retry_attempts: int = 3,
        retry_base_delay_seconds: float = 0.5,
        pacing_guard: "IBKRHistoricalPacingGuard | None" = None,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.retry_attempts = retry_attempts
        self.retry_base_delay_seconds = retry_base_delay_seconds
        self._ib: Any | None = None
        self._pacing_guard = pacing_guard or IBKRHistoricalPacingGuard()
        self._wsh_metadata_loaded = False
        self._shutting_down = False
        self._reconnect_lock = asyncio.Lock()
        self._last_ibkr_error: tuple[int, str] | None = None

    async def connect(self) -> None:
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
                raise RuntimeError("connectAsync returned but IBKR client is not connected")
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
        asyncio.ensure_future(self._reconnect())

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
                            pass
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
                        logger.critical("IBKR reconnection failed after %s attempts", max_attempts)
                        break
                    await asyncio.sleep(delay)

    async def disconnect(self) -> None:
        self._shutting_down = True
        if self._ib is not None and self._ib.isConnected():
            logger.info("disconnecting from IBKR %s:%d clientId=%d", self.host, self.port, self.client_id)
            self._ib.disconnect()
        self._shutting_down = False

    async def __aenter__(self) -> "IBKRFeedClient":
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.disconnect()

    async def qualify_contract(self, spec: ContractSpec) -> Any:
        await self._ensure_connected()
        logger.info(
            "qualify_contract: symbol=%s asset_class=%s exchange=%s primary_exchange=%s con_id=%s",
            spec.symbol,
            spec.asset_class,
            spec.exchange,
            spec.primary_exchange,
            spec.con_id,
        )
        t0 = monotonic_time.monotonic()
        contract = build_ibkr_contract(spec)
        qualified = await self._with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_contract:{spec.symbol}",
        )
        if qualified:
            selected = qualified[0]
            logger.debug(
                "qualify_contract completed in %.2fs for %s con_id=%s primary_exchange=%s",
                monotonic_time.monotonic() - t0,
                spec.symbol,
                _contract_int(selected, "conId"),
                _contract_text(selected, "primaryExchange", "primaryExch"),
            )
            return selected

        logger.warning(
            "qualifyContractsAsync returned no contract for %s; requesting contract details fallback",
            spec.symbol,
        )
        try:
            selected = await self._resolve_contract_from_details(contract, spec)
        except Exception as exc:
            raise RuntimeError(
                f"IBKR could not qualify contract for {spec.symbol}.{_qualification_hint(spec)} "
                f"contract_details_root_cause={_root_cause_message(exc)}"
            ) from exc
        if selected is None:
            raise RuntimeError(f"IBKR could not qualify contract for {spec.symbol}.{_qualification_hint(spec)}")
        logger.info(
            "qualify_contract fallback selected %s con_id=%s exchange=%s primary_exchange=%s in %.2fs",
            spec.symbol,
            _contract_int(selected, "conId"),
            _contract_text(selected, "exchange"),
            _contract_text(selected, "primaryExchange", "primaryExch"),
            monotonic_time.monotonic() - t0,
        )
        return selected

    async def _resolve_contract_from_details(self, contract: Any, spec: ContractSpec) -> Any | None:
        details = await self._with_retry(
            lambda: self._ib.reqContractDetailsAsync(contract),
            operation=f"contract_details:{spec.symbol}",
        )
        selected = _select_contract_from_details(details, spec, contract)
        if (
            selected is not None
            and spec.asset_class is AssetClass.EQUITY
            and spec.exchange.upper() == "SMART"
            and _contract_text(selected, "exchange")
        ):
            setattr(selected, "exchange", "SMART")
        return selected

    async def load_historical_ohlcv_range(
        self,
        request: OHLCVRequest,
        *,
        start_datetime: datetime,
        end_datetime: datetime | None = None,
    ) -> list[OHLCVBar]:
        """Paginated historical OHLCV fetch across a date range.

        IBKR limits each reqHistoricalData call to a maximum duration that depends
        on bar_size (e.g. ~6 months for 1-day bars, ~30 days for 1-min bars).
        This method chunks the range into IBKR-compatible duration windows,
        respects pacing limits between requests, and concatenates the results.
        """
        await self._ensure_connected()

        if end_datetime is None:
            end_datetime = datetime.now(timezone.utc)
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(tzinfo=timezone.utc)
        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(tzinfo=timezone.utc)

        chunk_duration = _ibkr_max_duration_for_bar_size(request.bar_size)
        chunk_seconds = _ibkr_duration_to_seconds(chunk_duration)

        total_seconds = (end_datetime - start_datetime).total_seconds()
        if total_seconds <= 0:
            logger.info("load_historical_ohlcv_range: empty range, returning []")
            return []

        logger.info(
            "load_historical_ohlcv_range: symbol=%s bar_size=%s range=%s → %s (%.0f seconds, ~%d chunks)",
            request.symbol, request.bar_size, start_datetime.isoformat(), end_datetime.isoformat(),
            total_seconds, max(1, int(total_seconds / chunk_seconds)),
        )

        all_bars: list[OHLCVBar] = []
        chunk_end = end_datetime
        chunk_count = 0
        max_chunks = 60  # safety limit: 60 requests = full pacing window

        while chunk_end > start_datetime and chunk_count < max_chunks:
            chunk_start = max(start_datetime, chunk_end - _seconds_to_timedelta(chunk_seconds))
            chunk_duration_actual = _ibkr_duration_between(chunk_start, chunk_end)

            chunk_request = request.model_copy(update={
                "end_datetime": chunk_end,
                "duration": chunk_duration_actual,
                "start_datetime": None,
            })

            logger.info(
                "ohlcv_range chunk %d: fetching %s → %s (duration=%s)",
                chunk_count + 1, chunk_start.isoformat(), chunk_end.isoformat(), chunk_duration_actual,
            )

            bars = await self.load_historical_ohlcv(chunk_request)

            # Filter out bars before start_datetime
            bars = [b for b in bars if b.timestamp >= start_datetime]

            all_bars = bars + all_bars  # prepend older bars
            chunk_count += 1

            if not bars:
                # No data returned — either market closed or no trading in this window.
                # Move the window back and continue.
                chunk_end = chunk_start
                continue

            # Earliest bar timestamp tells us where data actually starts.
            earliest = bars[0].timestamp
            if earliest <= chunk_start:
                # We got data at or before chunk_start, done.
                break

            chunk_end = earliest

        # Deduplicate by timestamp (overlapping chunks may produce duplicates)
        seen: set[datetime] = set()
        unique_bars: list[OHLCVBar] = []
        for bar in all_bars:
            if bar.timestamp not in seen:
                seen.add(bar.timestamp)
                unique_bars.append(bar)
        unique_bars.sort(key=lambda b: b.timestamp)

        logger.info(
            "load_historical_ohlcv_range: %d bars for %s across %d chunks (range %s → %s)",
            len(unique_bars), request.symbol, chunk_count,
            start_datetime.date().isoformat(), end_datetime.date().isoformat(),
        )
        return unique_bars

    async def load_historical_ohlcv(self, request: OHLCVRequest) -> list[OHLCVBar]:
        await self._ensure_connected()
        logger.info("load_historical_ohlcv: symbol=%s bar_size=%s duration=%s", request.symbol, request.bar_size, request.duration)
        t0 = monotonic_time.monotonic()
        contract = await self.qualify_contract(ContractSpec.from_ohlcv_request(request))
        end_datetime = _format_ibkr_end_datetime(request.end_datetime)

        try:
            await self._pacing_guard.acquire(request)
            bars = await self._with_retry(
                lambda: self._ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=end_datetime,
                    durationStr=request.duration,
                    barSizeSetting=request.bar_size,
                    whatToShow=request.what_to_show,
                    useRTH=request.use_rth,
                    formatDate=2,
                    keepUpToDate=False,
                ),
                operation=f"historical_ohlcv:{request.symbol}:{request.bar_size}",
            )
        finally:
            self._pacing_guard.release()
        result = normalize_ibkr_bars(bars, request)
        logger.info("load_historical_ohlcv: %d bars for %s in %.2fs", len(result), request.symbol, monotonic_time.monotonic() - t0)
        return result

    async def load_option_chains(self, request: OptionChainRequest) -> list[OptionChain]:
        """Load option chain metadata for stock or index underlyings via reqSecDefOptParams."""

        await self._ensure_connected()
        logger.info(
            "load_option_chains: symbol=%s asset_class=%s exchange=%s primary_exchange=%s underlying_con_id=%s",
            request.symbol,
            request.asset_class,
            request.exchange,
            request.primary_exchange,
            request.underlying_con_id,
        )
        t0 = monotonic_time.monotonic()
        if request.underlying_con_id:
            underlying_con_id = request.underlying_con_id
            logger.info("load_option_chains: using provided underlying_con_id=%s for %s", underlying_con_id, request.symbol)
        else:
            underlying_contract = await self.qualify_contract(request.to_contract_spec())
            resolved_con_id = _contract_int(underlying_contract, "conId")
            if resolved_con_id is None:
                raise RuntimeError(f"IBKR qualified {request.symbol} but did not return an underlying conId")
            underlying_con_id = resolved_con_id
        chains = await self._with_retry(
            lambda: self._ib.reqSecDefOptParamsAsync(
                request.symbol,
                "",
                _ibkr_sec_type_for_option_underlying(request.asset_class),
                underlying_con_id,
            ),
            operation=f"option_chain:{request.symbol}",
        )
        result = normalize_ibkr_option_chains(chains, request, underlying_con_id)
        logger.info("load_option_chains: %d chains for %s in %.2fs", len(result), request.symbol, monotonic_time.monotonic() - t0)
        return result

    async def load_option_analytics(self, request: OptionAnalyticsRequest) -> OptionAnalyticsSnapshot:
        """Load short-lived option market data with Greeks, volume, OI, and volatility fields."""

        await self._ensure_connected()
        logger.info("load_option_analytics: underlying=%s expiry=%s", request.contract.underlying_symbol, request.contract.expiry)
        t0 = monotonic_time.monotonic()
        contract = build_ibkr_option_contract(request.contract)
        qualified = await self._with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_option:{request.contract.underlying_symbol}:{request.contract.expiry}",
        )
        if qualified:
            contract = qualified[0]
        generic_tick_list = request.generic_tick_list
        use_snapshot = not generic_tick_list
        if generic_tick_list and request.regulatory_snapshot:
            logger.warning(
                "load_option_analytics: regulatory_snapshot ignored because IBKR snapshot market data "
                "does not support generic ticks; using short-lived streaming subscription"
            )
        logger.debug(
            "load_option_analytics market data mode: snapshot=%s generic_ticks=%s",
            use_snapshot,
            generic_tick_list or "none",
        )
        ticker = self._ib.reqMktData(
            contract,
            genericTickList=generic_tick_list,
            snapshot=use_snapshot,
            regulatorySnapshot=request.regulatory_snapshot if use_snapshot else False,
            mktDataOptions=[],
        )
        try:
            await asyncio.sleep(request.snapshot_wait_seconds)
            result = normalize_option_analytics_from_ticker(ticker, request.contract)
            logger.debug("load_option_analytics completed in %.2fs for %s", monotonic_time.monotonic() - t0, request.contract.underlying_symbol)
            return result
        finally:
            try:
                self._ib.cancelMktData(contract)
            except Exception:
                logger.debug("Failed to cancel market data subscription for %s", request.contract.underlying_symbol, exc_info=True)

    async def load_option_skew_surface(self, request: OptionSkewSurfaceRequest) -> OptionSkewSurfaceResponse:
        """Load bounded per-maturity option skew and open-interest summaries."""

        await self._ensure_connected()
        logger.info(
            "load_option_skew_surface: symbol=%s max_expirations=%d max_strikes_per_expiry=%d",
            request.chain_request.symbol,
            request.max_expirations,
            request.max_strikes_per_expiry,
        )
        t0 = monotonic_time.monotonic()
        chains = await self.load_option_chains(request.chain_request)
        chain = select_option_chain(chains, request)
        spot_price = request.spot_price
        if spot_price is None:
            spot_price = await self._load_underlying_snapshot_price(
                request.chain_request,
                wait_seconds=request.snapshot_wait_seconds,
            )
        expirations = select_skew_expirations(chain, request)
        if not expirations:
            raise RuntimeError(f"IBKR returned no matching expirations for {request.chain_request.symbol}")

        semaphore = asyncio.Semaphore(request.max_concurrent_requests)
        maturities = []
        for expiry in expirations:
            strikes = select_skew_strikes(
                chain.strikes,
                spot_price=spot_price,
                window_pct=request.strike_window_pct,
                max_count=request.max_strikes_per_expiry,
            )
            contracts = build_skew_option_contracts(
                chain=chain,
                request=request,
                expiry=expiry,
                strikes=strikes,
            )
            snapshots, warnings = await self._load_skew_contract_snapshots(
                contracts,
                generic_ticks=request.generic_ticks,
                snapshot_wait_seconds=request.snapshot_wait_seconds,
                regulatory_snapshot=request.regulatory_snapshot,
                semaphore=semaphore,
            )
            maturities.append(
                calculate_maturity_skew(
                    underlying_symbol=request.chain_request.symbol,
                    expiry=expiry,
                    spot_price=spot_price,
                    target_abs_delta=request.target_abs_delta,
                    fallback_moneyness_pct=request.fallback_moneyness_pct,
                    snapshots=snapshots,
                    warnings=tuple(warnings),
                )
            )

        logger.info(
            "load_option_skew_surface: %d maturities for %s in %.2fs",
            len(maturities),
            request.chain_request.symbol,
            monotonic_time.monotonic() - t0,
        )
        return OptionSkewSurfaceResponse(
            underlying_symbol=request.chain_request.symbol,
            underlying_con_id=chain.underlying_con_id,
            underlying_asset_class=request.chain_request.asset_class.value,
            chain_exchange=chain.exchange,
            trading_class=chain.trading_class,
            multiplier=chain.multiplier,
            spot_price=spot_price,
            maturities=tuple(maturities),
            metadata={
                "strike_window_pct": request.strike_window_pct,
                "max_strikes_per_expiry": request.max_strikes_per_expiry,
                "target_abs_delta": request.target_abs_delta,
                "sampled_expirations": expirations,
            },
        )

    async def _load_skew_contract_snapshots(
        self,
        contracts: Sequence[Any],
        *,
        generic_ticks: tuple[str, ...],
        snapshot_wait_seconds: float,
        regulatory_snapshot: bool,
        semaphore: asyncio.Semaphore,
    ) -> tuple[list[OptionAnalyticsSnapshot], list[str]]:
        async def load_one(contract: Any) -> OptionAnalyticsSnapshot:
            async with semaphore:
                return await self.load_option_analytics(
                    OptionAnalyticsRequest(
                        contract=contract,
                        generic_ticks=generic_ticks,
                        snapshot_wait_seconds=snapshot_wait_seconds,
                        regulatory_snapshot=regulatory_snapshot,
                    )
                )

        results = await asyncio.gather(*(load_one(contract) for contract in contracts), return_exceptions=True)
        snapshots: list[OptionAnalyticsSnapshot] = []
        warnings: list[str] = []
        for contract, result in zip(contracts, results, strict=True):
            if isinstance(result, Exception):
                warnings.append(f"{contract.expiry}:{contract.right.value}:{contract.strike}: {_root_cause_message(result)}")
            else:
                snapshots.append(result)
        return snapshots, warnings

    async def _load_underlying_snapshot_price(
        self,
        request: OptionChainRequest,
        *,
        wait_seconds: float,
    ) -> float:
        spec = request.to_contract_spec()
        contract = build_ibkr_contract(spec) if request.underlying_con_id else await self.qualify_contract(spec)
        ticker = self._ib.reqMktData(
            contract,
            genericTickList="",
            snapshot=True,
            regulatorySnapshot=False,
            mktDataOptions=[],
        )
        try:
            await asyncio.sleep(wait_seconds)
            price = _ticker_snapshot_price(ticker)
            if price is None:
                raise RuntimeError(
                    f"IBKR did not return a finite underlying snapshot price for {request.symbol}; "
                    "pass spot_price in the option skew request"
                )
            return price
        finally:
            try:
                self._ib.cancelMktData(contract)
            except Exception:
                logger.debug("Failed to cancel underlying snapshot market data for %s", request.symbol, exc_info=True)

    async def load_bond_yield_history(self, request: BondYieldHistoryRequest) -> list[BondYieldBar]:
        """Load historical bond yield bars for bid, ask, and/or last yield fields."""

        await self._ensure_connected()
        logger.info("load_bond_yield_history: bond=%s fields=%s", request.bond.symbol, [f.value for f in request.yield_fields])
        t0 = monotonic_time.monotonic()
        contract = await self.qualify_contract(request.bond.to_contract_spec())
        normalized: list[BondYieldBar] = []
        for yield_field in request.yield_fields:
            pacing_request = request.to_pacing_request(yield_field)
            try:
                await self._pacing_guard.acquire(pacing_request)
                bars = await self._with_retry(
                    lambda yf=yield_field: self._ib.reqHistoricalDataAsync(
                        contract,
                        endDateTime=_format_ibkr_end_datetime(request.end_datetime),
                        durationStr=request.duration,
                        barSizeSetting=request.bar_size,
                        whatToShow=yf.value,
                        useRTH=request.use_rth,
                        formatDate=2,
                        keepUpToDate=False,
                    ),
                    operation=f"bond_yield:{request.bond.symbol}:{yield_field.value}",
                )
            finally:
                self._pacing_guard.release()
            normalized.extend(normalize_ibkr_bond_yield_bars(bars, request, yield_field))
        logger.info("load_bond_yield_history: %d bars for %s in %.2fs", len(normalized), request.bond.symbol, monotonic_time.monotonic() - t0)
        return normalized

    async def load_fundamental_data(self, request: FundamentalDataRequest) -> FundamentalDataReport:
        """Load an IBKR fundamental report as raw XML."""

        await self._ensure_connected()
        logger.info("load_fundamental_data: symbol=%s report_type=%s", request.symbol, request.report_type.value)
        t0 = monotonic_time.monotonic()
        contract = await self.qualify_contract(request.to_contract_spec())
        raw_xml = await self._with_retry(
            lambda: self._ib.reqFundamentalDataAsync(contract, request.report_type.value, []),
            operation=f"fundamental_data:{request.symbol}:{request.report_type.value}",
        )
        report = FundamentalDataReport(
            symbol=request.symbol,
            asset_class=request.asset_class,
            con_id=getattr(contract, "conId", None),
            report_type=request.report_type,
            raw_xml=raw_xml,
            source=request.source,
            metadata=request.metadata,
        )
        logger.info("load_fundamental_data: %d bytes XML for %s in %.2fs", len(raw_xml or ""), request.symbol, monotonic_time.monotonic() - t0)
        return report

    async def load_wsh_metadata(self) -> WSHMetadataReport:
        """Load Wall Street Horizon metadata as raw JSON."""

        await self._ensure_connected()
        logger.info("load_wsh_metadata: starting")
        t0 = monotonic_time.monotonic()
        raw_json = await self._with_retry(
            lambda: self._ib.getWshMetaDataAsync(),
            operation="wsh_metadata",
        )
        self._wsh_metadata_loaded = True
        logger.info("load_wsh_metadata: completed in %.2fs", monotonic_time.monotonic() - t0)
        return WSHMetadataReport.from_raw_json(raw_json)

    async def load_wsh_event_data(
        self,
        request: WSHEventDataRequest,
        *,
        ensure_metadata: bool = True,
    ) -> WSHEventDataReport:
        """Load Wall Street Horizon event data as raw JSON."""

        await self._ensure_connected()
        logger.info("load_wsh_event_data: starting")
        t0 = monotonic_time.monotonic()
        if ensure_metadata and not self._wsh_metadata_loaded:
            await self.load_wsh_metadata()
        request_filter_json = request.to_filter_json()
        wsh_event_data = _build_wsh_event_data(request_filter_json)
        raw_json = await self._with_retry(
            lambda: self._ib.getWshEventDataAsync(wsh_event_data),
            operation="wsh_event_data",
        )
        report = WSHEventDataReport.from_raw_json(
            raw_json=raw_json,
            request_filter_json=request_filter_json,
        )
        logger.info("load_wsh_event_data: completed in %.2fs", monotonic_time.monotonic() - t0)
        return report

    async def load_news_providers(self) -> list[NewsProvider]:
        """Load API-entitled IBKR news providers."""

        await self._ensure_connected()
        logger.info("load_news_providers: starting")
        providers = await self._with_retry(
            lambda: self._ib.reqNewsProvidersAsync(),
            operation="news_providers",
        )
        result = normalize_news_providers(providers)
        logger.info("load_news_providers: %d providers loaded", len(result))
        return result

    async def load_historical_news(self, request: HistoricalNewsRequest) -> list[HistoricalNewsHeadline]:
        """Load historical IBKR news headlines for a contract id."""

        await self._ensure_connected()
        logger.info("load_historical_news: con_id=%s provider=%s", request.con_id, request.provider_codes_param)
        headlines = await self._with_retry(
            lambda: self._ib.reqHistoricalNewsAsync(
                request.con_id,
                request.provider_codes_param,
                format_historical_news_datetime(request.start_datetime),
                format_historical_news_datetime(request.end_datetime),
                request.total_results,
                [],
            ),
            operation=f"historical_news:{request.con_id}:{request.provider_codes_param}",
        )
        result = normalize_historical_news(headlines)
        logger.info("load_historical_news: %d headlines for con_id=%s", len(result), request.con_id)
        return result

    async def load_news_article(self, request: NewsArticleRequest) -> NewsArticle:
        """Load the body of an IBKR news article by provider and article id."""

        await self._ensure_connected()
        logger.info("load_news_article: provider=%s article_id=%s", request.provider_code, request.article_id)
        article = await self._with_retry(
            lambda: self._ib.reqNewsArticleAsync(request.provider_code, request.article_id, []),
            operation=f"news_article:{request.provider_code}:{request.article_id}",
        )
        return normalize_news_article(article, request)

    async def load_account_summary(self, account: str = "") -> list[AccountSummaryDTO]:
        """Load account summary values grouped by account."""

        await self._ensure_connected()
        logger.info("load_account_summary: account=%s", account or "all")
        values = await self._with_retry(
            lambda: self._ib.accountSummaryAsync(account),
            operation=f"account_summary:{account or 'all'}",
        )
        result = group_account_summary(normalize_account_values(values))
        logger.info("load_account_summary: %d accounts for %s", len(result), account or "all")
        return result

    async def load_live_positions(self) -> list[LivePositionDTO]:
        """Load current live positions."""

        await self._ensure_connected()
        logger.info("load_live_positions: starting")
        positions = await self._with_retry(
            lambda: self._ib.reqPositionsAsync(),
            operation="positions",
        )
        result = normalize_positions(positions)
        logger.info("load_live_positions: %d positions loaded", len(result))
        return result

    async def load_portfolio_items(self, account: str = "") -> list[PortfolioItemDTO]:
        """Expose current portfolio items from ib_insync's local account cache."""

        await self._ensure_connected()
        logger.info("load_portfolio_items: account=%s", account or "all")
        result = normalize_portfolio_items(self._ib.portfolio(account))
        logger.info("load_portfolio_items: %d items for %s", len(result), account or "all")
        return result

    async def subscribe_account_pnl(self, account: str, model_code: str = "") -> object:
        """Start a live account PnL subscription and return the ib_insync PnL object."""

        await self._ensure_connected()
        logger.info("subscribe_account_pnl: account=%s model_code=%s", account, model_code)
        return self._ib.reqPnL(account, model_code)

    async def subscribe_position_pnl(self, account: str, con_id: int, model_code: str = "") -> object:
        """Start a live position PnL subscription and return the ib_insync PnLSingle object."""

        await self._ensure_connected()
        logger.info("subscribe_position_pnl: account=%s con_id=%d model_code=%s", account, con_id, model_code)
        return self._ib.reqPnLSingle(account, model_code, con_id)

    async def load_account_pnl_snapshot(
        self,
        account: str,
        model_code: str = "",
        *,
        wait_seconds: float = 1.2,
    ) -> AccountPnLDTO:
        """Open a short-lived account PnL subscription and return the latest values."""

        logger.info("load_account_pnl_snapshot: account=%s wait=%.1fs", account, wait_seconds)
        subscription = await self.subscribe_account_pnl(account, model_code)
        try:
            if wait_seconds:
                await asyncio.sleep(wait_seconds)
            return self.account_pnl_snapshot(subscription, account, model_code)
        finally:
            cancel = getattr(self._ib, "cancelPnL", None)
            if cancel is not None:
                cancel(account, model_code)

    async def load_position_pnl_snapshot(
        self,
        account: str,
        con_id: int,
        model_code: str = "",
        *,
        wait_seconds: float = 1.2,
    ) -> PositionPnLDTO:
        """Open a short-lived position PnL subscription and return the latest values."""

        logger.info("load_position_pnl_snapshot: account=%s con_id=%d wait=%.1fs", account, con_id, wait_seconds)
        subscription = await self.subscribe_position_pnl(account, con_id, model_code)
        try:
            if wait_seconds:
                await asyncio.sleep(wait_seconds)
            return self.position_pnl_snapshot(subscription, account, con_id, model_code)
        finally:
            cancel = getattr(self._ib, "cancelPnLSingle", None)
            if cancel is not None:
                cancel(account, model_code, con_id)

    def account_pnl_snapshot(self, pnl_subscription: object, account: str, model_code: str = "") -> AccountPnLDTO:
        return normalize_account_pnl(pnl_subscription, account, model_code)

    def position_pnl_snapshot(
        self,
        pnl_subscription: object,
        account: str,
        con_id: int,
        model_code: str = "",
    ) -> PositionPnLDTO:
        return normalize_position_pnl(pnl_subscription, account, con_id, model_code)

    async def _ensure_connected(self) -> None:
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
                raise RuntimeError(
                    f"IBKR not available at {self.host}:{self.port} — "
                    f"ensure TWS or IB Gateway is running and API connections are enabled. "
                    f"clientId={self.client_id}. root_cause={root_cause}. "
                    f"{_last_ibkr_error_message(self._last_ibkr_error)}"
                ) from exc

    # IBKR error codes that indicate transient / recoverable conditions.
    _TRANSIENT_IBKR_CODES: ClassVar[frozenset[int]] = frozenset({502, 504, 1100, 1101, 1102})

    def _is_transient_error(self, exc: BaseException) -> bool:
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

    async def _with_retry(self, call: Any, *, operation: str) -> Any:
        last_error: BaseException | None = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                return await call()
            except Exception as exc:  # pragma: no cover - exercised with live gateways.
                last_error = exc
                if not self._is_transient_error(exc) or attempt >= self.retry_attempts:
                    break
                delay = self.retry_base_delay_seconds * (2 ** (attempt - 1))
                logger.warning("IBKR %s failed on attempt %d/%d; retrying in %.2fs", operation, attempt, self.retry_attempts, delay)
                await asyncio.sleep(delay)
        raise RuntimeError(f"IBKR operation failed after retries: {operation}") from last_error

    def _disconnect_stale_client(self) -> None:
        if self._ib is None:
            return
        try:
            self._ib.disconnect()
        except Exception:
            logger.debug("error disconnecting stale IBKR client", exc_info=True)
        finally:
            self._ib = None


class IBKRHistoricalPacingGuard:
    """Conservative in-process pacing guard for IBKR historical data requests."""

    def __init__(
        self,
        *,
        max_requests_per_window: int = constants.IBKR_HISTORICAL_MAX_REQUESTS_PER_WINDOW,
        request_window_seconds: float = constants.IBKR_HISTORICAL_REQUEST_WINDOW_SECONDS,
        identical_cooldown_seconds: float = constants.IBKR_HISTORICAL_IDENTICAL_REQUEST_COOLDOWN_SECONDS,
        same_contract_window_seconds: float = constants.IBKR_HISTORICAL_SAME_CONTRACT_WINDOW_SECONDS,
        same_contract_max_requests: int = constants.IBKR_HISTORICAL_SAME_CONTRACT_MAX_REQUESTS,
        max_concurrent_requests: int = constants.IBKR_CONSERVATIVE_HISTORICAL_CONCURRENCY,
    ) -> None:
        self.max_requests_per_window = max_requests_per_window
        self.request_window_seconds = request_window_seconds
        self.identical_cooldown_seconds = identical_cooldown_seconds
        self.same_contract_window_seconds = same_contract_window_seconds
        self.same_contract_max_requests = same_contract_max_requests
        self._request_times: deque[float] = deque()
        self._identical_last_seen: dict[tuple[Any, ...], float] = {}
        self._same_contract_times: dict[tuple[Any, ...], deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()
        self._concurrency = asyncio.Semaphore(max_concurrent_requests)

    async def acquire(self, request: OHLCVRequest) -> None:
        await self._concurrency.acquire()
        try:
            await self._wait_for_slot(request)
        except Exception:
            self._concurrency.release()
            raise

    def release(self) -> None:
        self._concurrency.release()

    async def _wait_for_slot(self, request: OHLCVRequest) -> None:
        weight = 2 if request.what_to_show.upper() == "BID_ASK" else 1
        identical_key = _historical_identical_key(request)
        same_contract_key = _historical_same_contract_key(request)

        while True:
            async with self._lock:
                now = monotonic_time.monotonic()
                self._prune(now)
                wait_seconds = self._required_wait_seconds(now, identical_key, same_contract_key, weight)
                if wait_seconds <= 0:
                    for _ in range(weight):
                        self._request_times.append(now)
                    self._identical_last_seen[identical_key] = now
                    self._same_contract_times[same_contract_key].append(now)
                    return
            logger.warning(
                "pacing guard: waiting %.2fs for slot – symbol=%s what_to_show=%s",
                wait_seconds, request.symbol, request.what_to_show,
            )
            await asyncio.sleep(wait_seconds)

    def _prune(self, now: float) -> None:
        while self._request_times and now - self._request_times[0] >= self.request_window_seconds:
            self._request_times.popleft()
        for key, timestamps in list(self._same_contract_times.items()):
            while timestamps and now - timestamps[0] >= self.same_contract_window_seconds:
                timestamps.popleft()
            if not timestamps:
                del self._same_contract_times[key]
        for key, timestamp in list(self._identical_last_seen.items()):
            if now - timestamp >= self.identical_cooldown_seconds:
                del self._identical_last_seen[key]

    def _required_wait_seconds(
        self,
        now: float,
        identical_key: tuple[Any, ...],
        same_contract_key: tuple[Any, ...],
        weight: int,
    ) -> float:
        waits: list[float] = []

        if len(self._request_times) + weight > self.max_requests_per_window:
            waits.append(self.request_window_seconds - (now - self._request_times[0]))

        identical_seen = self._identical_last_seen.get(identical_key)
        if identical_seen is not None:
            waits.append(self.identical_cooldown_seconds - (now - identical_seen))

        same_contract_times = self._same_contract_times.get(same_contract_key)
        if same_contract_times and len(same_contract_times) >= self.same_contract_max_requests:
            waits.append(self.same_contract_window_seconds - (now - same_contract_times[0]))

        return max([0.0, *waits])


def normalize_ibkr_bars(bars: Sequence[Any], request: OHLCVRequest) -> list[OHLCVBar]:
    normalized: list[OHLCVBar] = []
    for bar in bars:
        normalized.append(
            OHLCVBar(
                symbol=request.symbol,
                asset_class=request.asset_class,
                exchange=request.exchange,
                currency=request.currency,
                timestamp=_parse_ibkr_timestamp(getattr(bar, "date")),
                open=float(getattr(bar, "open")),
                high=float(getattr(bar, "high")),
                low=float(getattr(bar, "low")),
                close=float(getattr(bar, "close")),
                volume=float(getattr(bar, "volume", 0) or 0),
                bar_size=request.bar_size,
                source=request.source,
                metadata={
                    **request.metadata,
                    "what_to_show": request.what_to_show,
                    "use_rth": request.use_rth,
                },
            )
        )
    return normalized


def normalize_ibkr_option_chains(
    chains: Sequence[Any],
    request: OptionChainRequest,
    underlying_con_id: int,
) -> list[OptionChain]:
    normalized: list[OptionChain] = []
    for chain in chains:
        expirations = tuple(getattr(chain, "expirations", ()) or ())
        strikes = tuple(getattr(chain, "strikes", ()) or ())
        if not expirations or not strikes:
            continue
        normalized.append(
            OptionChain(
                underlying_symbol=request.symbol,
                underlying_asset_class=request.asset_class,
                underlying_con_id=underlying_con_id,
                exchange=getattr(chain, "exchange", ""),
                trading_class=getattr(chain, "tradingClass", ""),
                multiplier=str(getattr(chain, "multiplier", "")),
                expirations=expirations,
                strikes=strikes,
            )
        )
    return normalized


def _ticker_snapshot_price(ticker: Any) -> float | None:
    market_price = getattr(ticker, "marketPrice", None)
    if callable(market_price):
        value = _finite_positive(market_price())
        if value is not None:
            return value

    bid = _finite_positive(getattr(ticker, "bid", None))
    ask = _finite_positive(getattr(ticker, "ask", None))
    if bid is not None and ask is not None:
        return (bid + ask) / 2

    for attribute_name in ("last", "close", "markPrice"):
        value = _finite_positive(getattr(ticker, attribute_name, None))
        if value is not None:
            return value
    return None


def _finite_positive(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _format_ibkr_end_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y%m%d %H:%M:%S UTC")


def _parse_ibkr_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    elif isinstance(value, str):
        parsed = _parse_timestamp_string(value)
    else:
        raise TypeError(f"unsupported IBKR timestamp type: {type(value)!r}")

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_timestamp_string(value: str) -> datetime:
    text = value.strip().replace("Z", "+00:00")
    for fmt in ("%Y%m%d %H:%M:%S %Z", "%Y%m%d %H:%M:%S", "%Y%m%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt.endswith("%Z") and text.endswith("UTC"):
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            continue
    return datetime.fromisoformat(text)


def _historical_identical_key(request: OHLCVRequest) -> tuple[Any, ...]:
    return (
        request.symbol,
        request.asset_class,
        request.exchange,
        request.currency,
        request.end_datetime,
        request.duration,
        request.bar_size,
        request.what_to_show.upper(),
        request.use_rth,
    )


def _ibkr_sec_type_for_option_underlying(asset_class: AssetClass) -> str:
    if asset_class is AssetClass.EQUITY:
        return "STK"
    if asset_class is AssetClass.INDEX:
        return "IND"
    raise ValueError(f"unsupported option underlying asset class: {asset_class}")


# IBKR maximum duration per bar_size for a single reqHistoricalData call.
# https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/#hd-duration
_IBKR_MAX_DURATION_BY_BAR_SIZE: dict[str, str] = {
    "1 sec": "1800 S",
    "5 secs": "3600 S",
    "10 secs": "7200 S",
    "15 secs": "14400 S",
    "30 secs": "28800 S",
    "1 min": "1 D",
    "2 mins": "2 D",
    "3 mins": "3 D",
    "5 mins": "7 D",
    "10 mins": "14 D",
    "15 mins": "30 D",
    "20 mins": "30 D",
    "30 mins": "60 D",
    "1 hour": "365 D",
    "2 hours": "365 D",
    "3 hours": "365 D",
    "4 hours": "365 D",
    "8 hours": "365 D",
    "1 day": "18 M",
    "1 week": "10 Y",
    "1 month": "10 Y",
}


def _ibkr_max_duration_for_bar_size(bar_size: str) -> str:
    """Return the maximum IBKR duration string for a given bar size."""
    normalized = bar_size.strip().lower()
    # Handle plural variants like "1 min" vs "1 mins"
    for key, value in _IBKR_MAX_DURATION_BY_BAR_SIZE.items():
        if key == normalized or key.rstrip("s") == normalized.rstrip("s"):
            return value
    # Default: 1 day for anything unknown
    return "365 D"


def _ibkr_duration_to_seconds(duration: str) -> float:
    """Convert an IBKR duration string to approximate seconds.

    IBKR duration strings: N S (seconds), N D (days), N W (weeks), N M (months), N Y (years).
    """
    duration = duration.strip()
    parts = duration.split()
    if len(parts) != 2:
        return 86400.0  # default 1 day
    try:
        amount = float(parts[0])
    except ValueError:
        return 86400.0
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
    return 86400.0


def _seconds_to_timedelta(seconds: float) -> timedelta:
    from datetime import timedelta as td
    return td(seconds=seconds)


def _ibkr_duration_between(start: datetime, end: datetime) -> str:
    """Compute an IBKR duration string that covers the interval from start to end."""
    total_seconds = (end - start).total_seconds()
    if total_seconds <= 0:
        return "1 D"

    days = total_seconds / 86400
    if days <= 1:
        # Use seconds for sub-day durations
        return f"{int(total_seconds)} S"
    if days <= 365:
        return f"{int(days) + 1} D"  # round up
    months = int(days / 30) + 1
    if months <= 18:
        return f"{months} M"
    years = int(days / 365) + 1
    return f"{years} Y"


def _build_wsh_event_data(filter_json: str) -> Any:
    try:
        from ib_insync import WshEventData
    except ImportError as exc:
        raise RuntimeError("ib_insync is required for WSH event data requests") from exc
    return WshEventData(filter=filter_json)


def _historical_same_contract_key(request: OHLCVRequest) -> tuple[Any, ...]:
    return (
        request.symbol,
        request.asset_class,
        request.exchange,
        request.what_to_show.upper(),
    )
