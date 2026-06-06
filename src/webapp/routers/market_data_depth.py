"""Market-depth / DOM endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

from src.config.reference_data import is_known_index, resolve_future, resolve_index
from src.feeds.contracts import ContractSpec
from src.feeds.exchange_resolver import resolve_equity
from src.feeds.models import AssetClass
from src.feeds.tick_data import MarketDepthSnapshot
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.routers.market_data_shared import _looks_like_fx_pair

router = APIRouter(prefix="/market-data", tags=["market-data"])


class UnifiedMarketDepthRequest(BaseModel):
    """Compact DOM request with OHLCV-auto-style contract resolution."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(
        min_length=1,
        description="Ticker, FX pair, index symbol, or futures root.",
        examples=["TSLA", "EURUSD", "ES"],
    )
    asset_class: AssetClass | None = Field(
        default=None,
        description="Optional override when auto-detection is ambiguous. Supported values: equity, fx, index, future.",
    )
    exchange: str | None = Field(default=None, min_length=1, description="Optional IBKR exchange override.")
    currency: str | None = Field(default=None, min_length=1, description="Optional IBKR currency override.")
    primary_exchange: str | None = Field(
        default=None,
        min_length=1,
        description="Optional primary exchange for ambiguous SMART-routed equities.",
    )
    contract_month: str | None = Field(
        default=None,
        validation_alias=AliasChoices("contract_month", "contractMonth", "last_trade_date_or_contract_month"),
        description="Concrete futures contract month or last trade date. Required for futures unless local_symbol or con_id is supplied.",
        examples=["202606"],
    )
    multiplier: str | None = Field(default=None, min_length=1)
    local_symbol: str | None = Field(default=None, min_length=1, examples=["ESM6"])
    con_id: int | None = Field(default=None, gt=0)
    continuous: bool = Field(
        default=False,
        description="Not supported for live DOM; market depth requires a concrete live contract.",
    )
    num_rows: int = Field(
        default=5,
        ge=1,
        le=5,
        description="Depth levels per side. IBKR/ib_insync supports at most five rows per side.",
    )
    is_smart_depth: bool | None = Field(
        default=None,
        description="When omitted, defaults to true for SMART-routed contracts and false for direct-routed contracts.",
    )
    snapshot_wait_seconds: float = Field(
        default=1.5,
        gt=0,
        le=5,
        description="How long to keep the IBKR depth subscription open before returning the snapshot.",
    )
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol", "exchange", "currency", "primary_exchange", "contract_month", "multiplier", "local_symbol", mode="before")
    @classmethod
    def normalize_optional_upper_text(cls, value: object) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    @model_validator(mode="after")
    def validate_supported_contract(self) -> "UnifiedMarketDepthRequest":
        if self.asset_class is not None and self.asset_class not in {
            AssetClass.EQUITY,
            AssetClass.FX,
            AssetClass.INDEX,
            AssetClass.FUTURE,
        }:
            raise ValueError("market depth auto endpoint supports equity, fx, index, and concrete future requests")
        if self.continuous:
            raise ValueError("live market depth cannot use continuous futures; pass contract_month, local_symbol, or con_id")
        if self.resolved_asset_class is AssetClass.FUTURE and not (self.contract_month or self.local_symbol or self.con_id):
            raise ValueError("future market depth requests require contract_month, local_symbol, or con_id")
        return self

    @property
    def resolved_asset_class(self) -> AssetClass:
        if self.asset_class is not None:
            return self.asset_class
        if self.contract_month or self.local_symbol:
            return AssetClass.FUTURE
        if _looks_like_fx_pair(self.symbol):
            return AssetClass.FX
        if is_known_index(self.symbol):
            return AssetClass.INDEX
        return AssetClass.EQUITY

    def to_contract_spec(self) -> ContractSpec:
        asset_class = self.resolved_asset_class
        if asset_class is AssetClass.FUTURE:
            resolved = resolve_future(self.symbol)
            return ContractSpec(
                symbol=resolved["symbol"],
                asset_class=AssetClass.FUTURE,
                exchange=self.exchange or resolved["exchange"],
                currency=self.currency or resolved["currency"],
                last_trade_date_or_contract_month=self.contract_month,
                multiplier=self.multiplier,
                local_symbol=self.local_symbol,
                con_id=self.con_id,
                metadata=self.metadata,
            )
        if asset_class is AssetClass.FX:
            normalized_symbol = self.symbol.replace("/", "").upper()
            return ContractSpec(
                symbol=normalized_symbol,
                asset_class=AssetClass.FX,
                exchange=self.exchange or "IDEALPRO",
                currency=self.currency or normalized_symbol[3:6],
                con_id=self.con_id,
                metadata=self.metadata,
            )
        if asset_class is AssetClass.INDEX:
            resolved = resolve_index(self.symbol)
            return ContractSpec(
                symbol=resolved["symbol"],
                asset_class=AssetClass.INDEX,
                exchange=self.exchange or resolved["exchange"],
                currency=self.currency or resolved["currency"],
                con_id=self.con_id,
                metadata=self.metadata,
            )
        resolved = resolve_equity(self.symbol)
        return ContractSpec(
            symbol=resolved.symbol,
            asset_class=AssetClass.EQUITY,
            exchange=self.exchange or resolved.exchange,
            currency=self.currency or resolved.currency,
            primary_exchange=self.primary_exchange or resolved.primary_exchange or None,
            con_id=self.con_id,
            metadata=self.metadata,
        )

    def resolved_smart_depth(self, spec: ContractSpec) -> bool:
        if self.is_smart_depth is not None:
            return self.is_smart_depth
        return spec.exchange.upper() == "SMART"


@router.post(
    "/depth/auto",
    summary="Get Depth of Market with automatic contract resolution",
    response_model=MarketDepthSnapshot,
)
async def load_auto_market_depth(
    payload: UnifiedMarketDepthRequest,
    state: IBKRRestAppState = Depends(get_rest_state),
) -> MarketDepthSnapshot:
    """Return a short-lived IBKR DOM/L2 snapshot for a compact symbol request."""

    spec = payload.to_contract_spec()
    is_smart_depth = payload.resolved_smart_depth(spec)

    async def load_snapshot() -> MarketDepthSnapshot:
        return await state.feed.load_market_depth_snapshot(
            spec,
            num_rows=payload.num_rows,
            is_smart_depth=is_smart_depth,
            snapshot_wait_seconds=payload.snapshot_wait_seconds,
            request_timeout_seconds=state.settings.ibkr_market_depth_request_timeout_seconds,
            lease_wait_seconds=state.settings.ibkr_market_depth_lease_wait_seconds,
        )

    cache_ttl = state.settings.ibkr_market_depth_cache_ttl_seconds
    if cache_ttl <= 0:
        return await load_snapshot()
    cache_key = stable_cache_key(
        "market-depth:auto",
        {
            "spec": spec.model_dump(mode="json"),
            "num_rows": payload.num_rows,
            "is_smart_depth": is_smart_depth,
            "snapshot_wait_seconds": payload.snapshot_wait_seconds,
        },
    )
    return await state.market_data_cache.get_or_set(cache_key, load_snapshot, ttl_seconds=cache_ttl)
