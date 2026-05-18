"""Option chains, option analytics, skew surfaces, strike/expiry selection."""

from __future__ import annotations

import asyncio
import logging
import math
import time as monotonic_time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from src.feeds.contracts import OptionChain, OptionChainRequest, build_ibkr_contract
from src.feeds.ibkr_connection import (
    IBKRConnectionManager,
    acquire_market_data_line,
    _contract_int,
    _root_cause_message,
    wait_for_ibkr_request,
)
from src.feeds.models import AssetClass
from src.feeds.options import (
    DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS,
    OptionAnalyticsRequest,
    OptionAnalyticsSnapshot,
    OptionContractSpec,
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
from src.feeds.snapshotter import FXOptionSnapshot, ticker_to_fx_option_snapshot

if TYPE_CHECKING:
    from src.feeds.ibkr_historical import IBKRHistoricalClient

logger = logging.getLogger(__name__)


def _rate_limit_contract_key(contract: Any, fallback: str) -> str:
    con_id = getattr(contract, "conId", None)
    if con_id:
        return f"conId:{con_id}"
    local_symbol = getattr(contract, "localSymbol", None)
    if local_symbol:
        return f"localSymbol:{local_symbol}"
    symbol = getattr(contract, "symbol", fallback)
    sec_type = getattr(contract, "secType", "OPT")
    exchange = getattr(contract, "exchange", "")
    currency = getattr(contract, "currency", "")
    return f"{sec_type}:{symbol}:{exchange}:{currency}:{fallback}"


def _finite_positive(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


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


def _ibkr_sec_type_for_option_underlying(asset_class: AssetClass) -> str:
    if asset_class is AssetClass.EQUITY:
        return "STK"
    if asset_class is AssetClass.INDEX:
        return "IND"
    raise ValueError(f"unsupported option underlying asset class: {asset_class}")


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


class IBKROptionsFeedClient:
    """Option chains, analytics, skew surfaces."""

    def __init__(self, connection: IBKRConnectionManager, historical_client: "IBKRHistoricalClient") -> None:
        self._connection = connection
        self._historical = historical_client

    @property
    def _ib(self) -> Any:
        return self._connection.ib

    async def load_option_chains(self, request: OptionChainRequest) -> list[OptionChain]:
        """Load option chain metadata for stock or index underlyings via reqSecDefOptParams."""
        await self._connection.ensure_connected()
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
            underlying_contract = await self._historical.qualify_contract(request.to_contract_spec())
            resolved_con_id = _contract_int(underlying_contract, "conId")
            if resolved_con_id is None:
                raise RuntimeError(f"IBKR qualified {request.symbol} but did not return an underlying conId")
            underlying_con_id = resolved_con_id
        chains = await self._connection.with_retry(
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
        await self._connection.ensure_connected()
        logger.info("load_option_analytics: underlying=%s expiry=%s", request.contract.underlying_symbol, request.contract.expiry)
        t0 = monotonic_time.monotonic()
        contract = build_ibkr_option_contract(request.contract)
        qualified = await self._connection.with_retry(
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
        operation = f"option_analytics:{request.contract.underlying_symbol}:{request.contract.expiry}"
        lease = await acquire_market_data_line(
            self._connection,
            contract_key=_rate_limit_contract_key(contract, operation),
            operation=operation,
            ttl_seconds=max(30.0, request.snapshot_wait_seconds + 10.0),
        )
        await wait_for_ibkr_request(self._connection, operation=f"{operation}:reqMktData")
        try:
            ticker = self._ib.reqMktData(
                contract,
                genericTickList=generic_tick_list,
                snapshot=use_snapshot,
                regulatorySnapshot=request.regulatory_snapshot if use_snapshot else False,
                mktDataOptions=[],
            )
        except Exception:
            await lease.release()
            raise
        try:
            await asyncio.sleep(request.snapshot_wait_seconds)
            result = normalize_option_analytics_from_ticker(ticker, request.contract)
            logger.debug("load_option_analytics completed in %.2fs for %s", monotonic_time.monotonic() - t0, request.contract.underlying_symbol)
            return result
        finally:
            try:
                await wait_for_ibkr_request(self._connection, operation=f"{operation}:cancelMktData")
                self._ib.cancelMktData(contract)
            except Exception:
                logger.debug("Failed to cancel market data subscription for %s", request.contract.underlying_symbol, exc_info=True)
            await lease.release()

    async def capture_fx_option_snapshots(
        self,
        contracts: Sequence[OptionContractSpec],
        *,
        symbols: Sequence[str],
        generic_ticks: tuple[str, ...] = DEFAULT_OPTION_ANALYTICS_GENERIC_TICKS,
        snapshot_wait_seconds: float = 2.0,
    ) -> list[FXOptionSnapshot]:
        """Capture short-lived FX option market-data snapshots and cancel subscriptions."""
        await self._connection.ensure_connected()
        snapshots: list[FXOptionSnapshot] = []
        generic_tick_list = ",".join(generic_ticks)
        for contract_spec, pair_symbol in zip(contracts, symbols, strict=True):
            logger.info("capture_fx_option_snapshot: symbol=%s expiry=%s", pair_symbol, contract_spec.expiry)
            contract = build_ibkr_option_contract(contract_spec)
            qualified = await self._connection.with_retry(
                lambda: self._ib.qualifyContractsAsync(contract),
                operation=f"qualify_fx_option:{pair_symbol}:{contract_spec.expiry}",
            )
            if qualified:
                contract = qualified[0]
                if getattr(contract, "conId", None):
                    contract_spec = contract_spec.model_copy(update={"con_id": int(getattr(contract, "conId"))})
            operation = f"fx_option_snapshot:{pair_symbol}:{contract_spec.expiry}"
            lease = await acquire_market_data_line(
                self._connection,
                contract_key=_rate_limit_contract_key(contract, operation),
                operation=operation,
                ttl_seconds=max(30.0, snapshot_wait_seconds + 10.0),
            )
            await wait_for_ibkr_request(self._connection, operation=f"{operation}:reqMktData")
            try:
                ticker = self._ib.reqMktData(
                    contract,
                    genericTickList=generic_tick_list,
                    snapshot=False,
                    regulatorySnapshot=False,
                    mktDataOptions=[],
                )
            except Exception:
                await lease.release()
                raise
            try:
                await asyncio.sleep(snapshot_wait_seconds)
                snapshots.append(ticker_to_fx_option_snapshot(ticker, contract_spec, symbol=pair_symbol))
            finally:
                try:
                    await wait_for_ibkr_request(self._connection, operation=f"{operation}:cancelMktData")
                    self._ib.cancelMktData(contract)
                except Exception:
                    logger.debug("Failed to cancel FX option market data for %s", pair_symbol, exc_info=True)
                await lease.release()
        return snapshots

    async def load_option_skew_surface(self, request: OptionSkewSurfaceRequest) -> OptionSkewSurfaceResponse:
        """Load bounded per-maturity option skew and open-interest summaries."""
        await self._connection.ensure_connected()
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
        total_lines_planned = 0
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
            total_lines_planned += len(contracts)
            if total_lines_planned > request.max_total_lines:
                logger.warning(
                    "load_option_skew_surface: budget cap hit at expiry=%s — "
                    "planned=%d lines, max_total_lines=%d. Stopping surface scan.",
                    expiry,
                    total_lines_planned,
                    request.max_total_lines,
                )
                break
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
                "total_lines_used": total_lines_planned,
                "max_total_lines": request.max_total_lines,
                "budget_exhausted": total_lines_planned > request.max_total_lines,
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
        contract = build_ibkr_contract(spec) if request.underlying_con_id else await self._historical.qualify_contract(spec)
        operation = f"underlying_snapshot:{request.symbol}"
        lease = await acquire_market_data_line(
            self._connection,
            contract_key=_rate_limit_contract_key(contract, operation),
            operation=operation,
            ttl_seconds=max(30.0, wait_seconds + 10.0),
        )
        await wait_for_ibkr_request(self._connection, operation=f"{operation}:reqMktData")
        try:
            ticker = self._ib.reqMktData(
                contract,
                genericTickList="",
                snapshot=True,
                regulatorySnapshot=False,
                mktDataOptions=[],
            )
        except Exception:
            await lease.release()
            raise
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
                await wait_for_ibkr_request(self._connection, operation=f"{operation}:cancelMktData")
                self._ib.cancelMktData(contract)
            except Exception:
                logger.debug("Failed to cancel underlying snapshot market data for %s", request.symbol, exc_info=True)
            await lease.release()
