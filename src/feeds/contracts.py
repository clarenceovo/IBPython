from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.feeds.models import AssetClass, OHLCVRequest


class ContractSpec(BaseModel):
    """Normalized description of an IBKR contract."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    asset_class: AssetClass
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    primary_exchange: str | None = None
    last_trade_date_or_contract_month: str | None = None
    multiplier: str | None = None
    local_symbol: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_asset_specific_fields(self) -> "ContractSpec":
        if self.asset_class is AssetClass.FUTURE and not self.last_trade_date_or_contract_month:
            raise ValueError("futures require last_trade_date_or_contract_month")
        if self.asset_class is AssetClass.FX and len(self.symbol) not in {3, 6}:
            raise ValueError("fx symbols must be a base currency or a six-character pair")
        return self

    @classmethod
    def from_ohlcv_request(cls, request: OHLCVRequest) -> "ContractSpec":
        return cls(
            symbol=request.symbol,
            asset_class=request.asset_class,
            exchange=request.exchange,
            currency=request.currency,
            metadata=request.metadata,
        )


class OptionChainRequest(BaseModel):
    """Request option chain metadata for a stock or index underlying."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(min_length=1)
    asset_class: AssetClass
    exchange: str = Field(default="SMART", min_length=1)
    currency: str = Field(default="USD", min_length=1)
    primary_exchange: str | None = None

    @field_validator("symbol", "exchange", "currency", mode="before")
    @classmethod
    def normalize_upper_tokens(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        return str(value).strip().upper()

    @model_validator(mode="after")
    def validate_supported_underlying(self) -> "OptionChainRequest":
        if self.asset_class not in {AssetClass.EQUITY, AssetClass.INDEX}:
            raise ValueError("option chain requests currently support equity and index underlyings")
        return self

    def to_contract_spec(self) -> ContractSpec:
        return ContractSpec(
            symbol=self.symbol,
            asset_class=self.asset_class,
            exchange=self.exchange,
            currency=self.currency,
            primary_exchange=self.primary_exchange,
        )


class OptionChain(BaseModel):
    """IBKR option chain metadata for one exchange/trading-class combination."""

    model_config = ConfigDict(extra="forbid")

    underlying_symbol: str
    underlying_asset_class: AssetClass
    underlying_con_id: int = Field(gt=0)
    exchange: str
    trading_class: str
    multiplier: str
    expirations: tuple[str, ...]
    strikes: tuple[float, ...]

    @field_validator("underlying_symbol", "exchange", "trading_class", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        if value is None:
            raise ValueError("value is required")
        return str(value).strip().upper()

    @field_validator("expirations", mode="before")
    @classmethod
    def normalize_expirations(cls, value: Any) -> tuple[str, ...]:
        return tuple(sorted(str(item).strip() for item in value if str(item).strip()))

    @field_validator("strikes", mode="before")
    @classmethod
    def normalize_strikes(cls, value: Any) -> tuple[float, ...]:
        return tuple(sorted(float(item) for item in value))


def ibkr_contract_kwargs(spec: ContractSpec) -> dict[str, Any]:
    """Return keyword arguments for ib_insync.Contract without importing ib_insync."""

    symbol = spec.symbol.upper()
    exchange = spec.exchange.upper()
    currency = spec.currency.upper()

    if spec.asset_class is AssetClass.EQUITY:
        kwargs: dict[str, Any] = {
            "secType": "STK",
            "symbol": symbol,
            "exchange": exchange,
            "currency": currency,
        }
        if spec.primary_exchange:
            kwargs["primaryExchange"] = spec.primary_exchange.upper()
    elif spec.asset_class is AssetClass.FX:
        base, quote = _split_fx_symbol(symbol, currency)
        kwargs = {
            "secType": "CASH",
            "symbol": base,
            "exchange": exchange if exchange != "SMART" else "IDEALPRO",
            "currency": quote,
        }
    elif spec.asset_class is AssetClass.FUTURE:
        kwargs = {
            "secType": "FUT",
            "symbol": symbol,
            "exchange": exchange,
            "currency": currency,
            "lastTradeDateOrContractMonth": spec.last_trade_date_or_contract_month,
        }
        if spec.multiplier:
            kwargs["multiplier"] = spec.multiplier
        if spec.local_symbol:
            kwargs["localSymbol"] = spec.local_symbol
    elif spec.asset_class is AssetClass.INDEX:
        kwargs = {
            "secType": "IND",
            "symbol": symbol,
            "exchange": exchange,
            "currency": currency,
        }
    elif spec.asset_class is AssetClass.BOND:
        kwargs = {
            "secType": "BOND",
            "symbol": symbol,
            "exchange": exchange,
            "currency": currency,
        }
    elif spec.asset_class is AssetClass.CRYPTO:
        kwargs = {
            "secType": "CRYPTO",
            "symbol": symbol,
            "exchange": exchange if exchange != "SMART" else "PAXOS",
            "currency": currency,
        }
    else:
        raise ValueError(f"unsupported asset class: {spec.asset_class}")

    if spec.con_id:
        kwargs["conId"] = spec.con_id
    return kwargs


def build_ibkr_contract(spec: ContractSpec) -> Any:
    """Build an ib_insync Contract, importing ib_insync only when the adapter is used."""

    try:
        from ib_insync import Contract
    except ImportError as exc:
        raise RuntimeError("ib_insync is required for IBKR contract creation") from exc
    return Contract(**ibkr_contract_kwargs(spec))


def _split_fx_symbol(symbol: str, currency: str) -> tuple[str, str]:
    if len(symbol) == 6:
        return symbol[:3], symbol[3:]
    return symbol, currency
