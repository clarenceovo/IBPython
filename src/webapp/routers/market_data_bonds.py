"""Bond yield endpoints and models."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.feeds.bonds import BondYieldBar, BondYieldHistoryRequest
from src.feeds.ibkr_historical import HistoricalRequestTooLargeError, HistoricalRequestUnsupportedError
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.routers.market_data_shared import MinimalOHLCVLoadControls, load_ohlcv_with_controls

router = APIRouter(prefix="/market-data", tags=["market-data"])


class BondOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str | None = Field(
        default=None,
        min_length=1,
        examples=["91282CJN2"],
        description="Bond symbol or CUSIP, e.g. 91282CJN2. Optional when sec_id or con_id identifies the bond.",
    )
    exchange: str = Field(default="SMART", min_length=1, description="IBKR exchange; SMART routes bond data requests.")
    currency: str = Field(default="USD", min_length=1, description="IBKR settlement currency of the bond.")
    sec_id_type: str | None = Field(
        default=None,
        examples=["CUSIP"],
        description="Security identifier type matching sec_id, e.g. CUSIP or ISIN.",
    )
    sec_id: str | None = Field(
        default=None,
        examples=["91282CJN2"],
        description="Security identifier value for sec_id_type, e.g. the CUSIP.",
    )
    con_id: int | None = Field(default=None, gt=0, description="Optional IBKR contract id for exact bond identification.")

    @model_validator(mode="after")
    def validate_bond_identifier(self) -> "BondOHLCVLoadRequest":
        if not (self.symbol or self.sec_id or self.con_id):
            raise ValueError("bond wrapper requires symbol, sec_id, or con_id")
        return self

    def to_request(self) -> OHLCVRequest:
        symbol = self.symbol or self.sec_id or str(self.con_id)
        return self.to_ohlcv_request(
            AssetClass.BOND,
            symbol=symbol,
            exchange=self.exchange,
            currency=self.currency,
            sec_id_type=self.sec_id_type,
            sec_id=self.sec_id,
            con_id=self.con_id,
        )


class CachedBondYieldHistoryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: BondYieldHistoryRequest = Field(
        description="Bond yield history request: symbol/con_id plus bar_size, duration, and optional lookback window (UTC)."
    )
    use_ttl_cache: bool = Field(default=True, description="Serve identical repeated requests from the TTL cache.")
    cache_ttl_seconds: float | None = Field(
        default=None, ge=0, description="TTL override in seconds for the response cache. None uses the configured default."
    )


BOND_OHLCV_REQUEST_EXAMPLES = {
    "treasury_by_cusip": {
        "summary": "Treasury bond by CUSIP",
        "description": "Bond wrapper presets asset_class=bond. Use sec_id_type/sec_id or con_id when available.",
        "value": {
            "sec_id_type": "CUSIP",
            "sec_id": "91282CJN2",
            "start_datetime": "2026-05-01T00:00:00Z",
            "end_datetime": "2026-05-31T00:00:00Z",
            "duration": "1 M",
            "bar_size": "1 day",
        },
    },
    "bond_by_con_id": {
        "summary": "Bond by IBKR conId",
        "description": "Use con_id when your upstream security master already stores IBKR identifiers.",
        "value": {
            "con_id": 123456789,
            "currency": "USD",
            "duration": "1 M",
            "bar_size": "1 day",
        },
    },
}


@router.post(
    "/ohlcv/bond",
    response_model=list[OHLCVBar],
    summary="Load bond OHLCV with preset asset_class",
)
async def load_bond_ohlcv(
    payload: Annotated[BondOHLCVLoadRequest, Body(openapi_examples=BOND_OHLCV_REQUEST_EXAMPLES)],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[OHLCVBar]:
    """Load historical OHLCV bars for one bond with asset_class preset to bond.

    Accepts symbol, CUSIP/ISIN via sec_id_type/sec_id, or con_id; SMART is
    the default routing exchange. Returns OHLCVBar rows with UTC timestamps;
    oversized ranges are auto-chunked and rejected with 422 beyond the chunk
    cap. Use /market-data/bonds/yields for yield-valued series instead.
    """
    return await load_ohlcv_with_controls(
        request=payload.to_request(),
        start_datetime=payload.start_datetime,
        persist=payload.persist,
        cache_latest=payload.cache_latest,
        use_ttl_cache=payload.use_ttl_cache,
        cache_ttl_seconds=payload.cache_ttl_seconds,
        cache_namespace="ohlcv_bond",
        state=state,
    )


@router.post("/bonds/yields/history", response_model=list[BondYieldBar])
async def load_bond_yield_history(
    payload: CachedBondYieldHistoryRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[BondYieldBar]:
    """Load bid/ask/last yield history for one bond from IBKR.

    Requests the configured yield_fields (bid, ask, last by default) for the
    bond over the duration/bar_size window and returns BondYieldBar rows
    with UTC timestamps. Identical requests are served from a TTL cache when
    enabled. Contract qualification failures return 422; IBKR availability
    problems surface as 503 through the shared error envelope.
    """
    async def load() -> list[BondYieldBar]:
        return await state.feed.load_bond_yield_history(payload.request)

    if payload.use_ttl_cache:
        key = stable_cache_key("bond_yield_history", payload.request)
        return await state.market_data_cache.get_or_set(key, load, ttl_seconds=payload.cache_ttl_seconds)
    return await load()


# ------------------------------------------------------------------
# Bond yield data (YIELD_ASK/BID/LAST whatToShow)
# ------------------------------------------------------------------

class BondYieldDataRequest(BaseModel):
    """Request body for bond yield time series using IBKR yield-specific whatToShow values."""

    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(
        min_length=1,
        examples=["91282CJN2"],
        description="Bond symbol or CUSIP, e.g. 91282CJN2 for a US Treasury.",
    )
    what_to_show: str = Field(
        default="YIELD_LAST",
        examples=["YIELD_ASK", "YIELD_BID", "YIELD_BID_ASK", "YIELD_LAST"],
        description="IBKR yield data type: YIELD_ASK, YIELD_BID, YIELD_BID_ASK, or YIELD_LAST.",
    )
    bar_size: str = Field(
        default="1 day",
        examples=["1 min", "5 mins", "1 hour", "1 day"],
        description="IBKR bar size spelling, e.g. '1 min', '5 mins', '1 hour', '1 day'.",
    )
    duration: str = Field(
        default="1 Y",
        examples=["1 M", "6 M", "1 Y"],
        description="IBKR lookback window, e.g. '1 D', '1 W', '1 M', '6 M', '1 Y'.",
    )
    exchange: str = Field(default="SMART", description="IBKR exchange; SMART routes bond data requests.")
    currency: str = Field(default="USD", description="IBKR settlement currency of the bond.")
    use_rth: bool = Field(default=True, description="Restrict bars to regular trading hours.")
    use_ttl_cache: bool = Field(default=True, description="Serve identical repeated requests from the TTL cache.")
    cache_ttl_seconds: float | None = Field(
        default=300, ge=0, description="TTL override in seconds for the response cache."
    )


class BondYieldDataRow(BaseModel):
    """One bond yield bar; the yield value rides the bar's average price field."""

    model_config = ConfigDict(extra="forbid")

    timestamp: str = Field(description="Bar timestamp as an ISO-8601 UTC string.")
    open: float = Field(description="Bar open yield.")
    high: float = Field(description="Bar high yield.")
    low: float = Field(description="Bar low yield.")
    close: float = Field(description="Bar close yield.")
    volume: float = Field(description="Bar volume.")
    average_yield: float | None = Field(description="Average yield for the bar, mapped from IBKR's average price field.")


BOND_YIELD_DATA_EXAMPLES = {
    "treasury_yield": {
        "summary": "Treasury yield by CUSIP",
        "value": {
            "symbol": "91282CJN2",
            "what_to_show": "YIELD_LAST",
            "bar_size": "1 day",
            "duration": "1 Y",
        },
    },
}


@router.post(
    "/bonds/yields",
    summary="Get bond yield time series",
    response_model=list[BondYieldDataRow],
)
async def get_yield_data(
    payload: Annotated[
        BondYieldDataRequest,
        Body(openapi_examples=BOND_YIELD_DATA_EXAMPLES),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[BondYieldDataRow]:
    """Load a yield time series for one bond from IBKR.

    Uses IBKR yield-specific whatToShow values (YIELD_ASK, YIELD_BID,
    YIELD_BID_ASK, YIELD_LAST) instead of prices. Each row carries the bar's
    OHLCV fields plus `average_yield`; timestamps are ISO-8601 UTC strings.
    Oversized ranges are auto-chunked and rejected with 422 beyond the chunk
    cap. Pacing and connection problems surface as 429/503 through the
    shared error envelope.
    """
    from src.feeds.ibkr_historical import ensure_historical_chunk_limit, plan_historical_auto_chunk
    from src.feeds.models import AssetClass, OHLCVRequest, normalize_bar_size

    try:
        request = OHLCVRequest(
            symbol=payload.symbol,
            asset_class=AssetClass.BOND,
            exchange=payload.exchange,
            currency=payload.currency,
            bar_size=normalize_bar_size(payload.bar_size),
            duration=payload.duration,
            what_to_show=payload.what_to_show,
            use_rth=payload.use_rth,
        )
        auto_chunk_plan = plan_historical_auto_chunk(request)
        if auto_chunk_plan is not None:
            ensure_historical_chunk_limit(
                request, auto_chunk_plan,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
            bars = await state.feed.load_historical_ohlcv_range(
                request.model_copy(update={"end_datetime": auto_chunk_plan.end_datetime}),
                start_datetime=auto_chunk_plan.start_datetime,
                end_datetime=auto_chunk_plan.end_datetime,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )
        else:
            bars = await state.feed.load_historical_ohlcv(
                request,
                max_chunks=state.settings.ibkr_historical_max_chunks,
            )

        result = [
            BondYieldDataRow(
                timestamp=bar.timestamp.isoformat(),
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                average_yield=bar.vwap,
            )
            for bar in bars
        ]

        if payload.use_ttl_cache:
            from src.webapp.cache import stable_cache_key

            key = stable_cache_key("bond_yield_data", payload)
            async def _cached_result():
                return result
            return await state.market_data_cache.get_or_set(
                key, _cached_result, ttl_seconds=payload.cache_ttl_seconds,
            )
        return result
    except HistoricalRequestTooLargeError:
        raise
    except HistoricalRequestUnsupportedError:
        raise
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
