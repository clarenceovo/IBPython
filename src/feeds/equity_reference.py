"""Bounded IBKR generic-tick requests for equity reference data."""

from __future__ import annotations

import asyncio
import copy
import math
import sys
from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.feeds.contracts import ContractSpec
from src.feeds.exchange_resolver import resolve_equity
from src.feeds.exceptions import IBKRConnectionError, IBKRMarketDataUnavailableError
from src.feeds.ibkr_connection import acquire_market_data_line, wait_for_ibkr_request
from src.feeds.models import AssetClass


class EquityReferenceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    symbol: str = Field(min_length=1)
    exchange: str | None = None
    currency: str | None = None
    primary_exchange: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    timeout_seconds: float = Field(default=10, gt=0, le=20)

    @field_validator("symbol", "exchange", "currency", "primary_exchange", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> Any:
        if value is None:
            return None
        result = str(value).strip().upper()
        if not result:
            raise ValueError("contract fields must not be blank")
        return result

    def to_contract_spec(self) -> ContractSpec:
        resolved = resolve_equity(self.symbol)
        return ContractSpec(
            symbol=resolved.symbol, asset_class=AssetClass.EQUITY,
            exchange=self.exchange or resolved.exchange,
            currency=self.currency or resolved.currency,
            primary_exchange=self.primary_exchange or resolved.primary_exchange or None,
            con_id=self.con_id,
        )


class EquityReferenceResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    symbol: str
    con_id: int | None = None
    exchange: str
    currency: str
    observed_at: datetime | None = None
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: Literal["available", "partial", "unavailable"] = "unavailable"
    timed_out: bool = False
    market_data_type: int | None = None
    source: str = "ibkr"


class ShortabilityResponse(EquityReferenceResponse):
    shortable_shares: float | None = None
    shortability_rating: float | None = None


class DividendsResponse(EquityReferenceResponse):
    past_12_months: float | None = None
    next_12_months: float | None = None
    next_dividend_date: date | None = None
    next_dividend_amount: float | None = None


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and 0 <= number < sys.float_info.max else None


class EquityReferenceClient:
    def __init__(self, connection: Any, historical: Any) -> None:
        self._connection = connection
        self._historical = historical

    async def load_shortability(self, request: EquityReferenceRequest) -> ShortabilityResponse:
        return await self._load(request, "236", ShortabilityResponse)

    async def load_dividends(self, request: EquityReferenceRequest) -> DividendsResponse:
        return await self._load(request, "456", DividendsResponse)

    async def _load(self, request: EquityReferenceRequest, generic_tick: str, response_type: Any) -> Any:
        spec = request.to_contract_spec()
        result = response_type(symbol=spec.symbol, exchange=spec.exchange, currency=spec.currency)
        lease = ticker = ib = contract = req_id = None
        changed = asyncio.Event()
        error: Exception | None = None
        operation = f"equity_reference:{generic_tick}:{spec.symbol}"

        def update(value: Any) -> None:
            present: tuple[Any, ...]
            if generic_tick == "236":
                result.shortable_shares = _number(value.shortableShares)
                # ib_insync 0.9.86 preserves tick 46 in ticks, not a named field.
                for tick in value.ticks:
                    if tick.tickType == 46:
                        result.shortability_rating = _number(tick.price)
                present = (result.shortable_shares, result.shortability_rating)
                complete = result.shortable_shares is not None
            else:
                dividends = value.dividends
                if dividends is None:
                    return
                result.past_12_months = _number(dividends.past12Months)
                result.next_12_months = _number(dividends.next12Months)
                result.next_dividend_amount = _number(dividends.nextAmount)
                next_date = dividends.nextDate
                result.next_dividend_date = next_date.date() if isinstance(next_date, datetime) else next_date
                present = (result.past_12_months, result.next_12_months, result.next_dividend_amount, result.next_dividend_date)
                complete = all(item is not None for item in present)
            if any(item is not None for item in present):
                result.observed_at = datetime.now(timezone.utc)
                result.status = "available" if complete else "partial"
            result.market_data_type = value.marketDataType
            changed.set()

        def on_error(error_id: int, code: int, message: str, *_: Any) -> None:
            nonlocal error
            if error_id == req_id:
                error = IBKRMarketDataUnavailableError(f"IBKR {code}: {message}")
                changed.set()

        def disconnected(*_: Any) -> None:
            nonlocal error
            error = IBKRConnectionError("IBKR disconnected during equity reference request")
            changed.set()

        try:
            async with asyncio.timeout(request.timeout_seconds):
                await self._connection.ensure_connected()
                # Each request owns its contract identity: ib_insync keys tickers by id(contract).
                contract = copy.deepcopy(await self._historical.qualify_contract(spec))
                result.con_id = contract.conId or None
                ib = self._connection.ib
                lease = await acquire_market_data_line(
                    self._connection, contract_key=f"{contract.conId}:{spec.exchange}",
                    operation=operation, ttl_seconds=request.timeout_seconds + 10,
                )
                await wait_for_ibkr_request(self._connection, operation=operation)
                ticker = ib.reqMktData(contract, genericTickList=generic_tick, snapshot=False, regulatorySnapshot=False)
                req_id = ib.wrapper.ticker2ReqId["mktData"][ticker]
                ticker.updateEvent += update
                ib.errorEvent += on_error
                ib.disconnectedEvent += disconnected
                update(ticker)
                while result.status != "available":
                    await changed.wait()
                    changed.clear()
                    if error is not None:
                        raise error
                if error is not None:
                    raise error
        except TimeoutError as exc:
            if ticker is None:
                raise IBKRMarketDataUnavailableError("Equity reference setup exceeded the request deadline") from exc
            result.timed_out = True
        finally:
            # reqMktData registers a ticker before sending. Recover it if sending raised.
            if ticker is None and ib is not None and contract is not None:
                ticker = ib.wrapper.tickers.get(id(contract))
                if ticker is not None:
                    req_id = ib.wrapper.ticker2ReqId["mktData"].get(ticker)
            if ticker is not None:
                ticker.updateEvent -= update
                ib.errorEvent -= on_error
                ib.disconnectedEvent -= disconnected
            if lease is not None:
                async def cleanup() -> None:
                    try:
                        if ticker is not None:
                            try:
                                await wait_for_ibkr_request(self._connection, operation=f"{operation}:cancel")
                            finally:
                                # Cancellation must still be sent if pacing times out or the caller is cancelled.
                                try:
                                    ib.cancelMktData(contract)
                                finally:
                                    # ib_insync retains cancelled tickers; these identities belong to this request.
                                    ib.wrapper.reqId2Ticker.pop(req_id, None)
                                    ib.wrapper.tickers.pop(id(contract), None)
                                    ib.wrapper.pendingTickers.discard(ticker)
                    finally:
                        await lease.release()
                cleanup_task = asyncio.create_task(self._cleanup_with_deadline(cleanup))
                # Retain cleanup when HTTP/MCP cancellation interrupts the caller.
                self._connection._background_tasks.add(cleanup_task)
                cleanup_task.add_done_callback(self._connection._background_tasks.discard)
                await asyncio.shield(cleanup_task)
        result.received_at = datetime.now(timezone.utc)
        return result

    @staticmethod
    async def _cleanup_with_deadline(cleanup: Any) -> None:
        async with asyncio.timeout(5):
            await cleanup()
