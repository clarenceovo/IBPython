from __future__ import annotations

import asyncio
import logging
import time as monotonic_time
from collections import defaultdict, deque
from collections.abc import Sequence
from datetime import date, datetime, time, timezone
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
    build_ibkr_option_contract,
    normalize_option_analytics_from_ticker,
)

logger = logging.getLogger(__name__)


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

    async def connect(self) -> None:
        if self._ib is not None and self._ib.isConnected():
            logger.debug("connect skipped – already connected to %s:%d clientId=%d", self.host, self.port, self.client_id)
            return

        logger.info("connecting to IBKR at %s:%d clientId=%d", self.host, self.port, self.client_id)

        try:
            from ib_insync import IB
        except ImportError as exc:
            raise RuntimeError("ib_insync is required to connect to IBKR") from exc

        self._ib = IB()
        self._ib.errorEvent += self._on_ibkr_error
        self._ib.disconnectedEvent += self._on_ibkr_disconnected
        self._ib.timeoutEvent += self._on_ibkr_timeout
        t0 = monotonic_time.monotonic()
        await self._with_retry(
            lambda: self._ib.connectAsync(self.host, self.port, clientId=self.client_id),
            operation="connect",
        )
        logger.info("connected to IBKR in %.2fs", monotonic_time.monotonic() - t0)
        self._ib.setTimeout(60)

    # ------------------------------------------------------------------
    # IBKR event handlers
    # ------------------------------------------------------------------

    def _on_ibkr_error(self, req_id: int, error_code: int, error_string: str, contract: Any = None) -> None:
        """Log IBKR errors at appropriate severity levels."""
        if error_code in (404,):
            logger.warning("IBKR pacing violation [%s]: %s (reqId=%s)", error_code, error_string, req_id)
        elif error_code in (1100, 1101, 1102):
            logger.warning("IBKR connectivity event [%s]: %s (reqId=%s)", error_code, error_string, req_id)
        elif error_code in (200, 201, 502, 504):
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
        logger.warning("IBKR connection timeout")

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
                    self._ib.setTimeout(60)
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
        logger.info("qualify_contract: symbol=%s asset_class=%s exchange=%s", spec.symbol, spec.asset_class, spec.exchange)
        t0 = monotonic_time.monotonic()
        contract = build_ibkr_contract(spec)
        qualified = await self._with_retry(
            lambda: self._ib.qualifyContractsAsync(contract),
            operation=f"qualify_contract:{spec.symbol}",
        )
        logger.debug("qualify_contract completed in %.2fs for %s", monotonic_time.monotonic() - t0, spec.symbol)
        if not qualified:
            raise RuntimeError(f"IBKR could not qualify contract for {spec.symbol}")
        return qualified[0]

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
        logger.info("load_option_chains: symbol=%s asset_class=%s", request.symbol, request.asset_class)
        t0 = monotonic_time.monotonic()
        underlying_contract = await self.qualify_contract(request.to_contract_spec())
        underlying_con_id = int(getattr(underlying_contract, "conId"))
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
        """Load a market-data snapshot with option Greeks, volume, OI, and volatility fields."""

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
        ticker = self._ib.reqMktData(
            contract,
            genericTickList=request.generic_tick_list,
            snapshot=True,
            regulatorySnapshot=request.regulatory_snapshot,
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
            await self.connect()

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
