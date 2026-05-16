"""Reference data: news, fundamentals, WSH events, contract search/scanner, bonds, streaming."""

from __future__ import annotations

import asyncio
import logging
import time as monotonic_time
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from src.feeds.bonds import (
    BondYieldBar,
    BondYieldHistoryRequest,
    normalize_ibkr_bond_yield_bars,
)
from src.feeds.contracts import ContractSpec, build_ibkr_contract
from src.feeds.fundamental_data import (
    FundamentalDataReport,
    FundamentalDataRequest,
    WSHEventDataReport,
    WSHEventDataRequest,
    WSHMetadataReport,
)
from src.feeds.ibkr_connection import IBKRConnectionManager
from src.feeds.ibkr_historical import _format_ibkr_end_datetime
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
from src.feeds.scanner import (
    ContractScanRequest,
    ContractSearchRequest,
    ContractSearchResult,
)

logger = logging.getLogger(__name__)


def _float_or_none(value: Any) -> float | None:
    try:
        v = float(value)
        return v if v != 0.0 else None
    except (TypeError, ValueError):
        return None


def _build_wsh_event_data(filter_json: str) -> Any:
    try:
        from ib_insync import WshEventData
    except ImportError as exc:
        raise RuntimeError("ib_insync is required for WSH event data requests") from exc
    return WshEventData(filter=filter_json)


class IBKRReferenceFeedClient:
    """News, fundamentals, WSH events, contract search/scanner, bonds, streaming."""

    def __init__(self, connection: IBKRConnectionManager, historical_client: "IBKRHistoricalClient") -> None:
        self._connection = connection
        self._historical = historical_client
        self._wsh_metadata_loaded = False

    @property
    def _ib(self) -> Any:
        return self._connection.ib

    # ------------------------------------------------------------------
    # Bonds
    # ------------------------------------------------------------------

    async def load_bond_yield_history(self, request: BondYieldHistoryRequest) -> list[BondYieldBar]:
        """Load historical bond yield bars for bid, ask, and/or last yield fields."""
        await self._connection.ensure_connected()
        logger.info("load_bond_yield_history: bond=%s fields=%s", request.bond.symbol, [f.value for f in request.yield_fields])
        t0 = monotonic_time.monotonic()
        contract = await self._historical.qualify_contract(request.bond.to_contract_spec())
        normalized: list[BondYieldBar] = []
        for yield_field in request.yield_fields:
            pacing_request = request.to_pacing_request(yield_field)
            try:
                await self._connection.pacing_guard.acquire(pacing_request)
                bars = await self._connection.with_retry(
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
                self._connection.pacing_guard.release()
            normalized.extend(normalize_ibkr_bond_yield_bars(bars, request, yield_field))
        logger.info("load_bond_yield_history: %d bars for %s in %.2fs", len(normalized), request.bond.symbol, monotonic_time.monotonic() - t0)
        return normalized

    # ------------------------------------------------------------------
    # Fundamentals & WSH
    # ------------------------------------------------------------------

    async def load_fundamental_data(self, request: FundamentalDataRequest) -> FundamentalDataReport:
        """Load an IBKR fundamental report as raw XML."""
        await self._connection.ensure_connected()
        logger.info("load_fundamental_data: symbol=%s report_type=%s", request.symbol, request.report_type.value)
        t0 = monotonic_time.monotonic()
        contract = await self._historical.qualify_contract(request.to_contract_spec())
        raw_xml = await self._connection.with_retry(
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
        await self._connection.ensure_connected()
        logger.info("load_wsh_metadata: starting")
        t0 = monotonic_time.monotonic()
        raw_json = await self._connection.with_retry(
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
        await self._connection.ensure_connected()
        logger.info("load_wsh_event_data: starting")
        t0 = monotonic_time.monotonic()
        if ensure_metadata and not self._wsh_metadata_loaded:
            await self.load_wsh_metadata()
        request_filter_json = request.to_filter_json()
        wsh_event_data = _build_wsh_event_data(request_filter_json)
        raw_json = await self._connection.with_retry(
            lambda: self._ib.getWshEventDataAsync(wsh_event_data),
            operation="wsh_event_data",
        )
        report = WSHEventDataReport.from_raw_json(
            raw_json=raw_json,
            request_filter_json=request_filter_json,
        )
        logger.info("load_wsh_event_data: completed in %.2fs", monotonic_time.monotonic() - t0)
        return report

    # ------------------------------------------------------------------
    # News
    # ------------------------------------------------------------------

    async def load_news_providers(self) -> list[NewsProvider]:
        """Load API-entitled IBKR news providers."""
        await self._connection.ensure_connected()
        logger.info("load_news_providers: starting")
        providers = await self._connection.with_retry(
            lambda: self._ib.reqNewsProvidersAsync(),
            operation="news_providers",
        )
        result = normalize_news_providers(providers)
        logger.info("load_news_providers: %d providers loaded", len(result))
        return result

    async def load_historical_news(self, request: HistoricalNewsRequest) -> list[HistoricalNewsHeadline]:
        """Load historical IBKR news headlines for a contract id."""
        await self._connection.ensure_connected()
        logger.info("load_historical_news: con_id=%s provider=%s", request.con_id, request.provider_codes_param)
        headlines = await self._connection.with_retry(
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
        await self._connection.ensure_connected()
        logger.info("load_news_article: provider=%s article_id=%s", request.provider_code, request.article_id)
        article = await self._connection.with_retry(
            lambda: self._ib.reqNewsArticleAsync(request.provider_code, request.article_id, []),
            operation=f"news_article:{request.provider_code}:{request.article_id}",
        )
        return normalize_news_article(article, request)

    # ------------------------------------------------------------------
    # Contract search & scanner
    # ------------------------------------------------------------------

    async def search_contracts(self, request: ContractSearchRequest) -> list[ContractSearchResult]:
        """Search IBKR contract database by symbol pattern or conId."""
        await self._connection.ensure_connected()

        try:
            from ib_insync import Contract
        except ImportError as exc:
            raise RuntimeError("ib_insync is required") from exc

        contract_kwargs: dict[str, Any] = {}
        if request.con_id:
            contract_kwargs["conId"] = request.con_id
        if request.symbol:
            contract_kwargs["symbol"] = request.symbol.upper()
        if request.sec_type:
            contract_kwargs["secType"] = request.sec_type.upper()
        if request.exchange:
            contract_kwargs["exchange"] = request.exchange.upper()
        if request.currency:
            contract_kwargs["currency"] = request.currency.upper()

        contract = Contract(**contract_kwargs)

        try:
            details = await self._connection.with_retry(
                lambda: self._ib.reqContractDetailsAsync(contract),
                operation="search_contracts",
            )
        except Exception as exc:
            if "200" in str(exc):
                return []
            raise

        if not details:
            return []

        results: list[ContractSearchResult] = []
        for detail in details:
            c = getattr(detail, "contract", detail)
            result = ContractSearchResult(
                con_id=int(getattr(c, "conId", 0) or 0),
                symbol=getattr(c, "symbol", "") or "",
                sec_type=getattr(c, "secType", "") or "",
                exchange=getattr(c, "exchange", "") or "",
                currency=getattr(c, "currency", "") or "",
                primary_exchange=getattr(c, "primaryExchange", "") or getattr(c, "primaryExch", "") or "",
                local_symbol=getattr(c, "localSymbol", "") or "",
                long_name=getattr(detail, "longName", "") or "",
                category=getattr(detail, "category", "") or "",
                subcategory=getattr(detail, "subcategory", "") or "",
                industry=getattr(detail, "industry", "") or "",
                market_name=getattr(detail, "marketName", "") or "",
                min_tick=float(getattr(detail, "minTick", 0) or 0),
                trading_hours=getattr(detail, "tradingHours", "") or "",
                liquid_hours=getattr(detail, "liquidHours", "") or "",
                last_trading_day=getattr(detail, "lastTradeDate", "") or getattr(detail, "contractMonth", "") or "",
                multiplier=getattr(detail, "multiplier", "") or getattr(c, "multiplier", "") or "",
                strike=_float_or_none(getattr(c, "strike", None)),
                right=getattr(c, "right", "") or "",
                expiry=getattr(c, "lastTradeDateOrContractMonth", "") or "",
            )
            if result.con_id > 0:
                results.append(result)

        return results[:100]

    async def scan_contracts(self, request: ContractScanRequest) -> list[ContractSearchResult]:
        """Scan for contracts matching a symbol across exchanges/types."""
        search_req = ContractSearchRequest(
            symbol=request.symbol,
            sec_type=request.sec_type,
            exchange=request.exchange,
            currency=request.currency,
        )
        results = await self.search_contracts(search_req)
        if request.primary_exchange:
            primary_upper = request.primary_exchange.upper()
            results = [r for r in results if r.primary_exchange.upper() == primary_upper or r.exchange.upper() == primary_upper]
        return results[:request.max_results]

    # ------------------------------------------------------------------
    # Real-time market data streaming
    # ------------------------------------------------------------------

    async def subscribe_ticker(self, spec: ContractSpec) -> Any:
        """Subscribe to real-time market data for a contract."""
        await self._connection.ensure_connected()
        contract = build_ibkr_contract(spec)
        ticker = self._ib.reqMktData(contract, "", False, False)
        await asyncio.sleep(0.5)
        return ticker

    async def unsubscribe_ticker(self, ticker: Any) -> None:
        """Cancel a market data subscription."""
        if self._ib is None:
            return
        try:
            self._ib.cancelMktData(ticker.contract)
        except Exception:
            logger.warning("error unsubscribing ticker", exc_info=True)

    async def capture_equity_snapshots(
        self,
        symbols: Sequence[tuple[str, str, str, str, int]],
    ) -> list[Any]:
        """Capture point-in-time snapshots for a list of equity contracts."""
        await self._connection.ensure_connected()
        tickers: list[Any] = []
        for symbol, exchange, currency, primary_exchange, con_id in symbols:
            try:
                from ib_insync import Contract
                kwargs: dict[str, Any] = {
                    "secType": "STK",
                    "symbol": symbol.upper(),
                    "exchange": exchange.upper(),
                    "currency": currency.upper(),
                }
                if primary_exchange:
                    kwargs["primaryExchange"] = primary_exchange.upper()
                if con_id and con_id > 0:
                    kwargs["conId"] = con_id
                contract = Contract(**kwargs)
                ticker = self._ib.reqMktData(contract, "", False, False)
                tickers.append(ticker)
            except Exception:
                logger.warning("failed to subscribe ticker for %s", symbol, exc_info=True)
        await asyncio.sleep(1.0)
        return tickers

    async def cancel_equity_tickers(self, tickers: Sequence[Any]) -> None:
        """Cancel market data subscriptions for a batch of tickers."""
        if self._ib is None:
            return
        for ticker in tickers:
            try:
                contract = getattr(ticker, "contract", None)
                if contract is not None:
                    self._ib.cancelMktData(contract)
            except Exception:
                logger.debug("error cancelling ticker", exc_info=True)
