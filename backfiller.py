from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections.abc import Iterable, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from main import build_market_data_store, configure_logging
from src.config.settings import load_settings
from src.feeds.ibkr_historical import _ibkr_duration_to_seconds, _ibkr_max_duration_for_bar_size
from src.feeds.models import AssetClass, OHLCVBar, OHLCVRequest
from src.transport.redis_client import MarketDataRedisClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BackfillPlan:
    requests: tuple[OHLCVRequest, ...]
    start_datetime: datetime
    end_datetime: datetime
    chunk_seconds: float
    api_base_url: str
    persist: bool
    cache_latest: bool
    max_concurrency: int


@dataclass
class BackfillStats:
    symbols_total: int = 0
    chunks_total: int = 0
    chunks_success: int = 0
    chunks_failed: int = 0
    bars_loaded: int = 0
    bars_persisted: int = 0
    latest_cached: int = 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill OHLCV bars by calling the REST API, then persisting from this worker.",
    )
    parser.add_argument("--start", required=True, help="Inclusive start date/datetime, e.g. 2026-05-01 or 2026-05-01T09:30:00+08:00")
    parser.add_argument(
        "--end",
        required=True,
        help=(
            "End date/datetime. Date-only values include the full date; datetime values are treated "
            "as the exclusive end, e.g. 2026-05-28 or 2026-05-28T16:00:00+08:00"
        ),
    )
    parser.add_argument("--timezone", default="UTC", help="Timezone for date-only or naive datetime inputs; default UTC")
    parser.add_argument("--symbols", nargs="*", default=(), help="Symbols to backfill. Comma-separated values are also accepted.")
    parser.add_argument(
        "--symbols-file",
        type=Path,
        help=(
            "JSON symbol-control file. Supports {'defaults': {...}, 'symbols': [...]}, "
            "a plain list, or a scheduler-style payload containing params.defaults and params.symbols."
        ),
    )
    parser.add_argument("--asset-class", default="equity", choices=[item.value for item in AssetClass])
    parser.add_argument("--exchange", default="SMART")
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--bar-size", default="1 min")
    parser.add_argument("--duration", default="1 D", help="Fallback duration field for the API request")
    parser.add_argument("--what-to-show", default="TRADES")
    parser.add_argument("--use-rth", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--primary-exchange")
    parser.add_argument("--last-trade-date-or-contract-month")
    parser.add_argument("--local-symbol")
    parser.add_argument("--multiplier")
    parser.add_argument("--con-id", type=int)
    parser.add_argument("--api-base-url", help="REST API base URL. Defaults to IBKR_REST_BASE_URL from settings.")
    parser.add_argument("--chunk-seconds", type=float, help="Chunk size in seconds. Defaults to IBKR max duration for --bar-size.")
    parser.add_argument("--max-concurrency", type=int, default=1, help="Concurrent API chunks. Keep low for IBKR pacing; default 1.")
    parser.add_argument("--no-persist", action="store_true", help="Load from API but do not persist bars.")
    parser.add_argument("--no-cache-latest", action="store_true", help="Do not write latest bars to Redis.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned requests/chunks and exit without calling the API.")
    return parser.parse_args(argv)


def parse_datetime(value: str, tz: ZoneInfo, *, is_end: bool = False) -> datetime:
    text = value.strip()
    try:
        if len(text) == 10:
            parsed = datetime.combine(date.fromisoformat(text), time.min)
            if is_end:
                parsed += timedelta(days=1)
        else:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid datetime {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(timezone.utc)


def split_symbol_tokens(values: Iterable[str]) -> tuple[str, ...]:
    symbols: list[str] = []
    for value in values:
        for token in str(value).split(","):
            normalized = token.strip().upper()
            if normalized:
                symbols.append(normalized)
    return tuple(symbols)


def load_symbol_specs(args: argparse.Namespace) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    defaults = {
        "asset_class": args.asset_class,
        "exchange": args.exchange,
        "currency": args.currency,
        "bar_size": args.bar_size,
        "duration": args.duration,
        "what_to_show": args.what_to_show,
        "use_rth": args.use_rth,
        "primary_exchange": args.primary_exchange,
        "last_trade_date_or_contract_month": args.last_trade_date_or_contract_month,
        "local_symbol": args.local_symbol,
        "multiplier": args.multiplier,
        "con_id": args.con_id,
    }
    defaults = {key: value for key, value in defaults.items() if value is not None}

    symbol_specs: list[dict[str, Any]] = [{"symbol": symbol} for symbol in split_symbol_tokens(args.symbols)]
    if args.symbols_file is None:
        if not symbol_specs:
            raise ValueError("provide --symbols or --symbols-file")
        return defaults, symbol_specs

    payload = json.loads(args.symbols_file.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        params = payload["params"] if isinstance(payload.get("params"), dict) else payload
        file_defaults = params.get("defaults", {})
        if not isinstance(file_defaults, dict):
            raise ValueError("symbols file defaults must be an object")
        defaults.update({key: value for key, value in file_defaults.items() if key not in {"persist", "cache_latest"}})
        file_symbols = params.get("symbols", [])
    else:
        file_symbols = payload

    if not isinstance(file_symbols, list):
        raise ValueError("--symbols-file must contain a list or scheduler-style params.symbols list")
    for item in file_symbols:
        if isinstance(item, str):
            symbol_specs.append({"symbol": item.strip().upper()})
        elif isinstance(item, dict):
            symbol_specs.append({key: value for key, value in item.items() if value is not None})
        else:
            raise ValueError("symbol entries must be strings or objects")
    if not symbol_specs:
        raise ValueError("no symbols found")
    return defaults, symbol_specs


def build_requests(args: argparse.Namespace, start: datetime, end: datetime) -> tuple[OHLCVRequest, ...]:
    defaults, symbol_specs = load_symbol_specs(args)
    requests: list[OHLCVRequest] = []
    for symbol_spec in symbol_specs:
        payload = {
            **defaults,
            **symbol_spec,
            "start_datetime": start,
            "end_datetime": end,
        }
        requests.append(OHLCVRequest.model_validate(payload))
    return tuple(requests)


def chunk_ranges(start: datetime, end: datetime, chunk_seconds: float) -> tuple[tuple[datetime, datetime], ...]:
    if start >= end:
        raise ValueError("start must be before end")
    if chunk_seconds <= 0:
        raise ValueError("chunk_seconds must be positive")
    chunks: list[tuple[datetime, datetime]] = []
    cursor = start
    delta = timedelta(seconds=chunk_seconds)
    while cursor < end:
        chunk_end = min(end, cursor + delta)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end
    return tuple(chunks)


def api_bar_to_ohlcv_bar(payload: dict[str, Any], request: OHLCVRequest) -> OHLCVBar:
    data = dict(payload)
    metadata = dict(request.metadata or {})
    raw_metadata = data.get("metadata")
    if isinstance(raw_metadata, dict):
        metadata.update(raw_metadata)
    for key in (
        "con_id",
        "local_symbol",
        "last_trade_date_or_contract_month",
        "expiry",
        "strike",
        "right",
        "trading_class",
        "what_to_show",
        "use_rth",
        "multiplier",
    ):
        value = getattr(request, key, None)
        if value is not None:
            metadata.setdefault(key, value)
    data["metadata"] = metadata
    data.setdefault("asset_class", request.asset_class)
    data.setdefault("exchange", request.exchange)
    data.setdefault("currency", request.currency)
    data.setdefault("bar_size", request.bar_size)
    data.setdefault("source", request.source)
    allowed_fields = set(OHLCVBar.model_fields)
    return OHLCVBar.model_validate({key: value for key, value in data.items() if key in allowed_fields})


async def fetch_chunk(client: httpx.AsyncClient, request: OHLCVRequest, start: datetime, end: datetime) -> list[OHLCVBar]:
    chunk_request = request.model_copy(update={"start_datetime": start, "end_datetime": end})
    payload = {
        "request": chunk_request.model_dump(mode="json", exclude_none=True),
        "persist": False,
        "cache_latest": False,
        "use_ttl_cache": False,
    }
    response = await client.post("/api/v1/market-data/ohlcv", json=payload)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise RuntimeError(f"OHLCV API returned {response.status_code}: {response.text[:500]}") from exc
    body = response.json()
    if isinstance(body, dict) and "bars" in body:
        bars_data = body["bars"]
    elif isinstance(body, list):
        bars_data = body
    else:
        raise RuntimeError(f"OHLCV API returned unexpected payload type: {type(body).__name__}")
    return [api_bar_to_ohlcv_bar(item, chunk_request) for item in bars_data if isinstance(item, dict)]


async def run_backfill(plan: BackfillPlan) -> BackfillStats:
    settings = load_settings()
    redis = MarketDataRedisClient(settings.redis_url, password=settings.redis_password) if plan.cache_latest else None
    store = build_market_data_store(settings) if plan.persist else None
    stats = BackfillStats(symbols_total=len(plan.requests))
    chunks = chunk_ranges(plan.start_datetime, plan.end_datetime, plan.chunk_seconds)
    stats.chunks_total = len(plan.requests) * len(chunks)
    semaphore = asyncio.Semaphore(max(1, plan.max_concurrency))

    async with AsyncExitStack() as stack:
        if redis is not None:
            await stack.enter_async_context(redis)
        if store is not None:
            await stack.enter_async_context(store)
            await store.create_market_ohlcv_table()

        async with httpx.AsyncClient(base_url=plan.api_base_url, timeout=120.0) as client:
            async def run_one(request: OHLCVRequest, start: datetime, end: datetime) -> None:
                async with semaphore:
                    try:
                        bars = await fetch_chunk(client, request, start, end)
                        stats.bars_loaded += len(bars)
                        if store is not None and bars:
                            stats.bars_persisted += await store.insert_bars(bars)
                        if redis is not None and bars:
                            try:
                                await redis.set_latest_bar(bars[-1])
                                stats.latest_cached += 1
                            except Exception:
                                logger.warning(
                                    "backfill latest-cache write failed symbol=%s range=%s -> %s",
                                    request.symbol,
                                    start.isoformat(),
                                    end.isoformat(),
                                    exc_info=True,
                                )
                        stats.chunks_success += 1
                        logger.info(
                            "backfill chunk success symbol=%s bars=%d range=%s -> %s",
                            request.symbol,
                            len(bars),
                            start.isoformat(),
                            end.isoformat(),
                        )
                    except Exception:
                        stats.chunks_failed += 1
                        logger.exception(
                            "backfill chunk failed symbol=%s range=%s -> %s",
                            request.symbol,
                            start.isoformat(),
                            end.isoformat(),
                        )

            tasks = [run_one(request, start, end) for request in plan.requests for start, end in chunks]
            await asyncio.gather(*tasks)
    return stats


def build_plan(args: argparse.Namespace) -> BackfillPlan:
    settings = load_settings()
    tz = ZoneInfo(args.timezone)
    start = parse_datetime(args.start, tz)
    end = parse_datetime(args.end, tz, is_end=True)
    requests = build_requests(args, start, end)
    chunk_seconds = args.chunk_seconds
    if chunk_seconds is None:
        chunk_seconds = min(_ibkr_duration_to_seconds(_ibkr_max_duration_for_bar_size(request.bar_size)) for request in requests)
    return BackfillPlan(
        requests=requests,
        start_datetime=start,
        end_datetime=end,
        chunk_seconds=chunk_seconds,
        api_base_url=(args.api_base_url or settings.ibkr_rest_base_url).rstrip("/"),
        persist=not args.no_persist,
        cache_latest=not args.no_cache_latest,
        max_concurrency=max(1, args.max_concurrency),
    )


async def async_main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_settings()
    configure_logging(settings.telegram_bot_token, settings.telegram_chat_id, settings.telegram_log_level)
    plan = build_plan(args)
    logger.info(
        "backfill plan symbols=%d chunks_per_symbol=%d range=%s -> %s api=%s persist=%s cache_latest=%s",
        len(plan.requests),
        len(chunk_ranges(plan.start_datetime, plan.end_datetime, plan.chunk_seconds)),
        plan.start_datetime.isoformat(),
        plan.end_datetime.isoformat(),
        plan.api_base_url,
        plan.persist,
        plan.cache_latest,
    )
    if args.dry_run:
        for request in plan.requests:
            logger.info("planned symbol request: %s", request.model_dump(mode="json", exclude_none=True))
        return 0
    stats = await run_backfill(plan)
    logger.info("backfill complete: %s", stats)
    return 1 if stats.chunks_failed else 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
