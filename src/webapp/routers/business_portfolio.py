"""Portfolio risk endpoints for the business domain."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel, ConfigDict, Field

from src.feeds.account import AccountSummaryDTO, AccountValueDTO, LivePositionDTO, PortfolioItemDTO
from src.webapp.cache import stable_cache_key
from src.webapp.dependencies import IBKRRestAppState, get_rest_state
from src.webapp.openapi_markdown import markdown_openapi_examples
from src.webapp.routers.business_shared import BusinessCacheControls

router = APIRouter(prefix="/portfolio")

_NET_LIQUIDATION_TAG = "NetLiquidation"
_TOTAL_CASH_VALUE_TAG = "TotalCashValue"
_AVAILABLE_FUNDS_TAG = "AvailableFunds"
_EXCESS_LIQUIDITY_TAG = "ExcessLiquidity"
_CUSHION_TAG = "Cushion"
_GROSS_POSITION_VALUE_TAG = "GrossPositionValue"


class BusinessPortfolioRiskRequest(BusinessCacheControls):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    account: str = ""
    model_code: str = ""
    include_account_pnl: bool = True
    include_positions: bool = True
    wait_seconds: float = Field(default=1.2, ge=0)
    cache_ttl_seconds: float | None = Field(default=5, ge=0)


class BusinessPortfolioPosition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: str
    con_id: int | None = None
    symbol: str
    sec_type: str
    currency: str
    position: float
    average_cost: float | None = None
    market_price: float | None = None
    market_value: float | None = None
    unrealized_pnl: float | None = None
    realized_pnl: float | None = None
    gross_exposure: float
    net_exposure: float
    weight_of_net_liquidation: float | None = None


class BusinessPortfolioExposure(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str
    gross_exposure: float
    net_exposure: float
    market_value: float
    weight_of_net_liquidation: float | None = None


class BusinessPortfolioRiskResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generated_at: datetime
    account: str
    base_currency: str | None = None
    net_liquidation: float | None = None
    total_cash_value: float | None = None
    available_funds: float | None = None
    excess_liquidity: float | None = None
    cushion: float | None = None
    gross_position_value: float | None = None
    leverage: float | None = None
    daily_pnl: float | None = None
    unrealized_pnl: float | None = None
    realized_pnl: float | None = None
    positions: list[BusinessPortfolioPosition] = Field(default_factory=list)
    exposures_by_asset_class: list[BusinessPortfolioExposure] = Field(default_factory=list)
    exposures_by_currency: list[BusinessPortfolioExposure] = Field(default_factory=list)
    top_concentrations: list[BusinessPortfolioPosition] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


@router.post(
    "/getRiskSnapshot",
    response_model=BusinessPortfolioRiskResponse,
    summary="Get a business-ready portfolio risk snapshot",
)
async def get_portfolio_risk_snapshot(
    payload: Annotated[
        BusinessPortfolioRiskRequest,
        Body(openapi_examples=markdown_openapi_examples("business.portfolio.getRiskSnapshot")),
    ],
    state: IBKRRestAppState = Depends(get_rest_state),
) -> BusinessPortfolioRiskResponse:
    async def load() -> BusinessPortfolioRiskResponse:
        return await _load_portfolio_risk_snapshot(payload, state)

    if payload.use_ttl_cache:
        return await state.market_data_cache.get_or_set(
            stable_cache_key("business_portfolio_risk", payload),
            load,
            ttl_seconds=payload.cache_ttl_seconds,
        )
    return await load()


async def _load_portfolio_risk_snapshot(
    payload: BusinessPortfolioRiskRequest,
    state: IBKRRestAppState,
) -> BusinessPortfolioRiskResponse:
    warnings: list[str] = []
    summaries = await state.feed.load_account_summary(payload.account)
    selected_summary = _select_account_summary(summaries, payload.account, warnings)

    portfolio_items: list[PortfolioItemDTO] = []
    live_positions: list[LivePositionDTO] = []
    if payload.include_positions:
        portfolio_items = await state.feed.load_portfolio_items(payload.account)
        live_positions = await state.feed.load_live_positions()

    account = _response_account(payload.account, selected_summary, summaries)
    net_liquidation = _summary_float(selected_summary, _NET_LIQUIDATION_TAG)
    total_cash_value = _summary_float(selected_summary, _TOTAL_CASH_VALUE_TAG)
    available_funds = _summary_float(selected_summary, _AVAILABLE_FUNDS_TAG)
    excess_liquidity = _summary_float(selected_summary, _EXCESS_LIQUIDITY_TAG)
    cushion = _summary_float(selected_summary, _CUSHION_TAG)
    gross_position_value = _summary_float(selected_summary, _GROSS_POSITION_VALUE_TAG)

    positions = _portfolio_positions(
        portfolio_items,
        live_positions,
        requested_account=payload.account,
        net_liquidation=net_liquidation,
    )
    computed_gross_position_value = sum(position.gross_exposure for position in positions)
    if gross_position_value is None and positions:
        gross_position_value = computed_gross_position_value
    if cushion is None and net_liquidation and excess_liquidity is not None and net_liquidation > 0:
        cushion = excess_liquidity / net_liquidation

    daily_pnl = unrealized_pnl = realized_pnl = None
    if payload.include_account_pnl:
        if not account or account == "ALL":
            warnings.append("account PnL skipped because an explicit single account was not selected")
        else:
            try:
                pnl = await state.feed.load_account_pnl_snapshot(
                    account=account,
                    model_code=payload.model_code,
                    wait_seconds=payload.wait_seconds,
                )
                daily_pnl = pnl.daily_pnl
                unrealized_pnl = pnl.unrealized_pnl
                realized_pnl = pnl.realized_pnl
            except Exception as exc:  # pragma: no cover - behavior covered through route tests
                warnings.append(f"account PnL unavailable: {exc}")

    return BusinessPortfolioRiskResponse(
        generated_at=datetime.now(timezone.utc),
        account=account or payload.account or "ALL",
        base_currency=_base_currency(selected_summary, positions),
        net_liquidation=net_liquidation,
        total_cash_value=total_cash_value,
        available_funds=available_funds,
        excess_liquidity=excess_liquidity,
        cushion=cushion,
        gross_position_value=gross_position_value,
        leverage=_safe_ratio(gross_position_value, net_liquidation),
        daily_pnl=daily_pnl,
        unrealized_pnl=unrealized_pnl,
        realized_pnl=realized_pnl,
        positions=positions,
        exposures_by_asset_class=_group_exposures(positions, lambda position: position.sec_type, net_liquidation),
        exposures_by_currency=_group_exposures(positions, lambda position: position.currency or "UNKNOWN", net_liquidation),
        top_concentrations=sorted(positions, key=lambda position: position.gross_exposure, reverse=True)[:10],
        warnings=warnings,
    )


def _select_account_summary(
    summaries: list[AccountSummaryDTO],
    requested_account: str,
    warnings: list[str],
) -> AccountSummaryDTO | None:
    if requested_account:
        for summary in summaries:
            if summary.account == requested_account:
                return summary
        warnings.append(f"account summary not found for {requested_account}")
        return None
    if len(summaries) == 1:
        return summaries[0]
    if len(summaries) > 1:
        accounts = ", ".join(summary.account for summary in summaries)
        warnings.append(f"multiple account summaries returned ({accounts}); account-level metrics were not aggregated")
    return None


def _response_account(
    requested_account: str,
    selected_summary: AccountSummaryDTO | None,
    summaries: list[AccountSummaryDTO],
) -> str:
    if requested_account:
        return requested_account
    if selected_summary is not None:
        return selected_summary.account
    if len(summaries) == 1:
        return summaries[0].account
    return "ALL"


def _summary_float(summary: AccountSummaryDTO | None, tag: str) -> float | None:
    if summary is None:
        return None
    value = summary.values.get(tag)
    return _account_value_float(value)


def _account_value_float(value: AccountValueDTO | None) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(str(value.value).replace(",", ""))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _base_currency(
    summary: AccountSummaryDTO | None,
    positions: list[BusinessPortfolioPosition],
) -> str | None:
    if summary is not None:
        for tag in (_NET_LIQUIDATION_TAG, _TOTAL_CASH_VALUE_TAG, _AVAILABLE_FUNDS_TAG):
            value = summary.values.get(tag)
            if value is not None and value.currency:
                return value.currency
    currencies = {position.currency for position in positions if position.currency}
    if len(currencies) == 1:
        return next(iter(currencies))
    return None


def _portfolio_positions(
    portfolio_items: list[PortfolioItemDTO],
    live_positions: list[LivePositionDTO],
    *,
    requested_account: str,
    net_liquidation: float | None,
) -> list[BusinessPortfolioPosition]:
    live_by_key = {
        (position.account, position.con_id, position.symbol, position.sec_type): position
        for position in live_positions
        if not requested_account or position.account == requested_account
    }
    rows: list[BusinessPortfolioPosition] = []
    seen_keys: set[tuple[str, int | None, str, str]] = set()
    for item in portfolio_items:
        if requested_account and item.account != requested_account:
            continue
        key = (item.account, item.con_id, item.symbol, item.sec_type)
        seen_keys.add(key)
        live = live_by_key.get(key)
        market_value = item.market_value
        if market_value is None and item.market_price is not None:
            market_value = item.position * item.market_price
        net_exposure = market_value if market_value is not None else 0.0
        gross_exposure = abs(net_exposure)
        rows.append(
            BusinessPortfolioPosition(
                account=item.account,
                con_id=item.con_id,
                symbol=item.symbol,
                sec_type=item.sec_type,
                currency=item.currency,
                position=item.position,
                average_cost=item.average_cost if item.average_cost is not None else (live.average_cost if live else None),
                market_price=item.market_price,
                market_value=market_value,
                unrealized_pnl=item.unrealized_pnl,
                realized_pnl=item.realized_pnl,
                gross_exposure=gross_exposure,
                net_exposure=net_exposure,
                weight_of_net_liquidation=_safe_ratio(gross_exposure, net_liquidation),
            )
        )
    for live in live_positions:
        if requested_account and live.account != requested_account:
            continue
        key = (live.account, live.con_id, live.symbol, live.sec_type)
        if key in seen_keys:
            continue
        rows.append(
            BusinessPortfolioPosition(
                account=live.account,
                con_id=live.con_id,
                symbol=live.symbol,
                sec_type=live.sec_type,
                currency=live.currency,
                position=live.position,
                average_cost=live.average_cost,
                gross_exposure=0.0,
                net_exposure=0.0,
                weight_of_net_liquidation=0.0 if net_liquidation else None,
            )
        )
    return sorted(rows, key=lambda row: (row.account, row.symbol, row.con_id or 0))


def _group_exposures(
    positions: list[BusinessPortfolioPosition],
    key_func: Callable[[BusinessPortfolioPosition], str],
    net_liquidation: float | None,
) -> list[BusinessPortfolioExposure]:
    groups: dict[str, dict[str, float]] = defaultdict(lambda: {"gross": 0.0, "net": 0.0, "market_value": 0.0})
    for position in positions:
        key = str(key_func(position))
        groups[key]["gross"] += position.gross_exposure
        groups[key]["net"] += position.net_exposure
        groups[key]["market_value"] += position.market_value or 0.0
    return [
        BusinessPortfolioExposure(
            key=key,
            gross_exposure=values["gross"],
            net_exposure=values["net"],
            market_value=values["market_value"],
            weight_of_net_liquidation=_safe_ratio(values["gross"], net_liquidation),
        )
        for key, values in sorted(groups.items())
    ]


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    result = numerator / denominator
    return result if math.isfinite(result) else None
