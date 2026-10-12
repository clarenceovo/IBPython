"""Fill-based transaction cost analysis. Positive slippage means adverse execution."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from src.feeds.orders import ExecutionAttribution, ExecutionDetail, OrderEnvelope


class OrderTCA(BaseModel):
    order_uuid: str
    account_id: str
    symbol: str
    currency: str | None = None
    strategy_id: str | None = None
    book_id: str | None = None
    decision_id: str | None = None
    parent_order_uuid: str | None = None
    side: str
    requested_quantity: float
    filled_quantity: float
    fill_ratio: float | None = None
    average_fill_price: float | None = None
    decision_slippage_bps: float | None = None
    arrival_slippage_bps: float | None = None
    spread_cost_bps: float | None = None
    decision_price_cost: float | None = None
    commission: float | None = None
    implementation_shortfall: float | None = None
    first_fill_latency_ms: float | None = None
    execution_count: int
    warnings: list[str] = Field(default_factory=list)


class TCAGroup(BaseModel):
    strategy_id: str | None
    symbol: str
    currency: str | None
    order_count: int
    filled_quantity: float
    decision_slippage_bps: float | None
    arrival_slippage_bps: float | None


class TCAReport(BaseModel):
    account_id: str
    cost_convention: Literal["positive_is_adverse"] = "positive_is_adverse"
    orders: list[OrderTCA]
    groups: list[TCAGroup]
    unattributed_execution_count: int


def build_tca_report(account_id: str, envelopes: list[OrderEnvelope], executions: list[ExecutionDetail], *,
                     strategy_id: str | None = None, start: datetime | None = None, end: datetime | None = None) -> TCAReport:
    contexts = {e.order_uuid: e for e in envelopes}
    grouped: dict[tuple[str, int], tuple[OrderEnvelope, dict, list[ExecutionDetail]]] = {}
    unmatched = 0
    for fill in executions:
        if fill.account_id != account_id or (start and (not fill.fill_time or fill.fill_time < start)) or (end and (not fill.fill_time or fill.fill_time >= end)):
            continue
        context = contexts.get(fill.order_ref or "")
        visited: set[str] = set()
        while context and context.action == "modify" and context.parent_uuid and context.order_uuid not in visited:
            visited.add(context.order_uuid)
            context = contexts.get(context.parent_uuid)
        if context is None or context.action not in {"place", "bracket", "oca"} or (context.account_id and context.account_id != account_id):
            unmatched += 1
            continue
        request = context.request
        if context.action == "bracket":
            # The parent benchmark is not an exit benchmark. Only the parent leg
            # shares that decision; stop/take-profit fills remain unmatched.
            if context.ibkr_order_id != fill.order_id:
                unmatched += 1
                continue
        if context.action == "oca":
            response_orders = (context.response or {}).get("orders", [])
            indices = [i for i, r in enumerate(response_orders) if r.get("order_id") == fill.order_id]
            if len(indices) != 1:
                unmatched += 1
                continue
            request = context.request["orders"][indices[0]]
        attribution = request.get("attribution") or {}
        if strategy_id is not None and attribution.get("strategy_id") != strategy_id:
            continue
        key = (context.order_uuid, fill.order_id)
        if key not in grouped:
            grouped[key] = (context, request, [])
        grouped[key][2].append(fill)

    results = [_order_tca(account_id, context, request, fills) for context, request, fills in grouped.values()]
    filled_contexts = {key[0] for key in grouped}
    for context in envelopes:
        if context.action != "place" or context.order_uuid in filled_contexts or context.account_id != account_id:
            continue
        if (start and context.created_at < start) or (end and context.created_at >= end):
            continue
        if strategy_id is not None and (context.request.get("attribution") or {}).get("strategy_id") != strategy_id:
            continue
        results.append(_order_tca(account_id, context, context.request, []))
    aggregates: dict[tuple[str | None, str, str | None], list[OrderTCA]] = {}
    for row in results:
        aggregates.setdefault((row.strategy_id, row.symbol, row.currency), []).append(row)

    def weighted(rows: list[OrderTCA], field: str) -> float | None:
        valid = [r for r in rows if getattr(r, field) is not None and r.average_fill_price is not None]
        # Price-notional weighting, with groups separated by symbol and currency.
        denominator = sum(r.filled_quantity * r.average_fill_price for r in valid)
        return sum(getattr(r, field) * r.filled_quantity * r.average_fill_price for r in valid) / denominator if denominator else None

    groups = [TCAGroup(strategy_id=key[0], symbol=key[1], currency=key[2], order_count=len(rows),
                       filled_quantity=sum(r.filled_quantity for r in rows),
                       decision_slippage_bps=weighted(rows, "decision_slippage_bps"),
                       arrival_slippage_bps=weighted(rows, "arrival_slippage_bps")) for key, rows in aggregates.items()]
    return TCAReport(account_id=account_id, orders=results, groups=groups, unattributed_execution_count=unmatched)


def _order_tca(account_id: str, context: OrderEnvelope, request: dict, fills: list[ExecutionDetail]) -> OrderTCA:
    attribution = ExecutionAttribution.model_validate(request["attribution"]) if request.get("attribution") else None
    warnings: list[str] = []
    side = str(request.get("action", ""))
    sign = 1 if side == "BUY" else -1
    quantity = sum(f.quantity for f in fills)
    avg = sum(f.quantity * f.price for f in fills) / quantity if quantity > 0 else None
    currencies = {f.currency for f in fills}
    currency = next(iter(currencies)) if len(currencies) == 1 else None
    valid = avg is not None and avg > 0 and all(f.quantity > 0 and f.price > 0 and f.side in ({"BOT", "BUY"} if side == "BUY" else {"SLD", "SELL"}) for f in fills)
    decision = arrival = spread = cost = fees = total = latency = None
    if not attribution:
        warnings.append("decision attribution unavailable")
    elif not valid or not currency or attribution.benchmark_currency.upper() != currency.upper():
        warnings.append("invalid fill or benchmark currency mismatch; cost calculations unavailable")
    else:
        if attribution.decision_price is not None:
            decision = sign * (avg / attribution.decision_price - 1) * 10000
            multipliers = {f.multiplier for f in fills}
            if len(multipliers) == 1 and None not in multipliers and next(iter(multipliers)) > 0:
                cost = sign * sum(f.quantity * (f.price - attribution.decision_price) * f.multiplier for f in fills)
            else:
                warnings.append("contract multiplier unavailable or inconsistent; cash cost unavailable")
        if attribution.arrival_bid is not None:
            midpoint = (attribution.arrival_bid + attribution.arrival_ask) / 2
            arrival = sign * (avg / midpoint - 1) * 10000
            spread = (attribution.arrival_ask - attribution.arrival_bid) / (2 * midpoint) * 10000
        if all(f.commission is not None and f.commission_currency and f.commission_currency.upper() == currency.upper() for f in fills):
            fees = sum(f.commission for f in fills)
            total = cost + fees if cost is not None else None
        else:
            warnings.append("fees incomplete or in another currency; total cost unavailable")
        times = [f.fill_time for f in fills if f.fill_time is not None]
        if attribution.decision_at and len(times) == len(fills):
            latency = (min(times) - attribution.decision_at).total_seconds() * 1000
            if latency < 0:
                latency = None
                warnings.append("fill precedes decision timestamp")
    requested = float(request.get("quantity", 0))
    if quantity > requested:
        warnings.append("fills exceed original requested quantity; inspect amendments")
    return OrderTCA(
        order_uuid=context.order_uuid, account_id=account_id, symbol=str(request.get("symbol", "")), currency=currency,
        strategy_id=attribution.strategy_id if attribution else None, book_id=attribution.book_id if attribution else None,
        decision_id=attribution.decision_id if attribution else None,
        parent_order_uuid=attribution.parent_order_uuid if attribution else None, side=side,
        requested_quantity=requested, filled_quantity=quantity, fill_ratio=quantity / requested if requested > 0 else None,
        average_fill_price=avg, decision_slippage_bps=decision, arrival_slippage_bps=arrival, spread_cost_bps=spread,
        decision_price_cost=cost, commission=fees, implementation_shortfall=total, first_fill_latency_ms=latency,
        execution_count=len(fills), warnings=warnings,
    )
