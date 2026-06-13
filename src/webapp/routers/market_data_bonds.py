"""Bond yield endpoints and models."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.feeds.bonds import BondYieldBar, BondYieldHistoryRequest
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.routers.market_data_shared import MinimalOHLCVLoadControls, load_ohlcv_with_controls

router = APIRouter(prefix="/market-data", tags=["market-data"])


class BondOHLCVLoadRequest(MinimalOHLCVLoadControls):
    symbol: str | None = Field(default=None, min_length=1, examples=["91282CJN2"])
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    sec_id_type: str | None = Field(default=None, examples=["CUSIP"])
    sec_id: str | None = Field(default=None, examples=["91282CJN2"])
    con_id: int | None = Field(default=None, gt=0)

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

    request: BondYieldHistoryRequest
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=None, ge=0)


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

    symbol: str = Field(min_length=1, examples=["91282CJN2"])
    what_to_show: str = Field(
        default="YIELD_LAST",
        examples=["YIELD_ASK", "YIELD_BID", "YIELD_BID_ASK", "YIELD_LAST"],
    )
    bar_size: str = Field(default="1 day", examples=["1 min", "5 mins", "1 hour", "1 day"])
    duration: str = Field(default="1 Y", examples=["1 M", "6 M", "1 Y"])
    exchange: str = Field(default="SMART")
    currency: str = Field(default="USD")
    use_rth: bool = True
    use_ttl_cache: bool = True
    cache_ttl_seconds: float | None = Field(default=300, ge=0)


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
)
async def get_yield_data(
    payload: Annotated[
        BondYieldDataRequest,
        Body(openapi_examples=BOND_YIELD_DATA_EXAMPLES),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> list[dict[str, Any]]:
    """Get bond yield time series from IBKR.

    Uses IBKR yield-specific whatToShow values (YIELD_ASK, YIELD_BID, YIELD_BID_ASK, YIELD_LAST).
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
            {
                "timestamp": bar.timestamp.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "average_yield": bar.vwap,
            }
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
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))
