"""Universal symbol resolver: one notation in, one IBKR contract out.

Accepts the common ticker notations used across data providers and converts
them to a normalized :class:`ResolvedSymbol` that carries the IBKR contract
fields. The resolver covers every asset class in ``AssetClass``:

- Equity: ``AAPL``, ``0700.HK``, ``7203.T``, ``BRK.B`` (suffix map + US
  primary-exchange table reused from ``exchange_resolver``).
- FX: ``EURUSD``, ``EUR.USD``, ``EUR-USD``, ``EUR/USD``.
- Crypto: ``BTC``, ``BTCUSD``, ``ETHUSD`` (IBKR Paxos spot).
- Future: ``ES1!`` / ``ES=F`` (continuous), ``ESZ2026``, ``ESZ26``, ``ESM6``,
  ``-ES`` (dashed root, continuous), and IBKR local symbols.
- Index: ``SPX``, ``NDX``, ``VIX``, ``HSI``, plus Yahoo aliases such as
  ``^GSPC``.
- Option / Bond: pass-through via explicit ``asset_class`` only. Option and
  bond contracts need extra fields that no notation reliably carries.

Detection precedence (first match wins): explicit asset class, crypto root,
FX pair, futures pattern, known index, equity fallback.

Every resolution records the ``matched_rule`` so callers can audit why a
symbol resolved the way it did.
"""

from __future__ import annotations

import datetime as _dt
import re
from dataclasses import dataclass, field

from src.config.reference_data import is_known_index, resolve_future, resolve_index
from src.feeds.contracts import ContractSpec
from src.feeds.exchange_resolver import resolve_equity
from src.feeds.models import AssetClass

# ---------------------------------------------------------------------------
# Month codes and tables
# ---------------------------------------------------------------------------

FUTURES_MONTH_CODES: dict[str, int] = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}
MONTH_CODE_LETTERS = frozenset(FUTURES_MONTH_CODES)

# IBKR Paxos-traded crypto roots (exchange PAXOS, quote currency USD).
CRYPTO_ROOTS: dict[str, str] = {
    "BTC": "BTC", "ETH": "ETH", "LTC": "LTC", "BCH": "BCH",
    "SOL": "SOL", "XRP": "XRP", "ADA": "ADA", "DOGE": "DOGE",
    "DOT": "DOT", "AVAX": "AVAX", "LINK": "LINK", "MATIC": "MATIC",
    "SHIB": "SHIB", "UNI": "UNI", "ATOM": "ATOM", "LINKUSD": "LINK",
}

# Yahoo-style index aliases mapped to IBKR symbols.
INDEX_ALIASES: dict[str, str] = {
    "^GSPC": "SPX", "^SPX": "SPX", "^DJI": "DJI", "^IXIC": "COMP", "^VIX": "VIX",
    "^RUT": "RUT", "^NDX": "NDX", "^HSI": "HSI", "^N225": "N225", "^DAX": "DAX",
    "^FTSE": "FTSE", "^STOXX50E": "ESTX50",
}

_CURRENCY_CODES: frozenset[str] = frozenset({
    "USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD", "CNH", "HKD", "SGD",
    "SEK", "NOK", "DKK", "MXN", "ZAR", "TRY", "BRL", "RUB", "INR", "KRW", "PLN",
    "CZK", "HUF", "ILS", "IDR", "PHP", "THB", "MYR", "CNY",
})

_FUTURES_LOCAL_RE = re.compile(
    r"^(?P<root>[A-Z0-9]{1,6}?)(?P<month>[FGHJKMNQUVXZ])(?P<year>\d{1,4})$"
)
_FX_RE = re.compile(r"^(?P<base>[A-Z]{3})[./-](?P<quote>[A-Z]{3})$")
_FUTURES_CONTINUOUS_SUFFIX_RE = re.compile(r"^(?P<root>[A-Z0-9]{1,6}?)(?P<slot>[12])!$")
_YAHOO_CONTINUOUS_RE = re.compile(r"^(?P<root>[A-Z0-9]{1,6}?)=F$")


@dataclass(frozen=True, slots=True)
class ResolvedSymbol:
    """Normalized IBKR contract fields derived from one input ticker."""

    input_symbol: str
    canonical_symbol: str
    asset_class: AssetClass
    exchange: str
    currency: str
    primary_exchange: str = ""
    local_symbol: str | None = None
    last_trade_date_or_contract_month: str | None = None
    continuous: bool = False
    con_id: int | None = None
    multiplier: str | None = None
    matched_rule: str = ""
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_contract_spec(self) -> ContractSpec:
        """Build a validated ContractSpec from the resolution."""
        return ContractSpec(
            symbol=self.canonical_symbol,
            asset_class=self.asset_class,
            exchange=self.exchange,
            currency=self.currency,
            primary_exchange=self.primary_exchange or None,
            last_trade_date_or_contract_month=self.last_trade_date_or_contract_month,
            multiplier=self.multiplier,
            local_symbol=self.local_symbol,
            continuous=self.continuous,
            con_id=self.con_id,
        )

    def describe(self) -> dict[str, object]:
        """Return a JSON-friendly summary used by debug endpoints."""
        return {
            "input": self.input_symbol,
            "symbol": self.canonical_symbol,
            "asset_class": self.asset_class.value,
            "exchange": self.exchange,
            "currency": self.currency,
            "primary_exchange": self.primary_exchange,
            "local_symbol": self.local_symbol,
            "contract_month": self.last_trade_date_or_contract_month,
            "continuous": self.continuous,
            "matched_rule": self.matched_rule,
            "notes": list(self.notes),
        }


class SymbolResolutionError(ValueError):
    """The ticker could not be resolved under the requested constraints."""


def _two_digit_year(year_text: str, today: _dt.date | None = None) -> int:
    """Expand a one- or two-digit futures year into a full year.

    One digit keeps the current decade unless the result lies more than
    seven years in the past, in which case it rolls to the next decade.
    Two digits use the current century with the same seven-year tolerance.
    """
    today = today or _dt.date.today()
    current_year = today.year
    digits = len(year_text)
    value = int(year_text)
    if digits <= 2:
        century = current_year // 100 * 100
        candidate = century + value
        while candidate < current_year - 7:
            candidate += 100 if digits == 2 else 10
        return candidate
    return value


def _contract_month(month_code: str, year: int) -> str:
    """Format the IBKR lastTradeDateOrContractMonth: YYYYMM."""
    return f"{year:04d}{FUTURES_MONTH_CODES[month_code]:02d}"


def _resolve_crypto(root: str, quote: str | None) -> ResolvedSymbol:
    base = CRYPTO_ROOTS[root]
    return ResolvedSymbol(
        input_symbol="",
        canonical_symbol=base,
        asset_class=AssetClass.CRYPTO,
        exchange="PAXOS",
        currency="USD",
        matched_rule=f"crypto_root:{root}",
        notes=(f"quote currency {quote or 'USD'} assumed USD on PAXOS",),
    )


def _resolve_fx(base: str, quote: str, separator: str) -> ResolvedSymbol:
    notes: tuple[str, ...] = ()
    if separator:
        notes = (f"separator '{separator}' removed; IBKR uses the 6-letter pair",)
    return ResolvedSymbol(
        input_symbol="",
        canonical_symbol=f"{base}{quote}",
        asset_class=AssetClass.FX,
        exchange="IDEALPRO",
        currency=quote,
        matched_rule="fx_pair",
        notes=notes,
    )


def _resolve_future(root: str, *, month_code: str | None, year_text: str | None,
                    continuous: bool, slot: int | None, local_symbol: str | None) -> ResolvedSymbol:
    preset = resolve_future(root)
    notes: tuple[str, ...] = ()
    contract_month: str | None = None
    if month_code is not None and year_text is not None:
        year = _two_digit_year(year_text)
        contract_month = _contract_month(month_code, year)
        if local_symbol is None:
            local_symbol = f"{root}{month_code}{year_text}"
        notes = (f"month code {month_code} = month {FUTURES_MONTH_CODES[month_code]}",)
    if continuous and slot is not None:
        notes = notes + (f"continuous slot {slot} mapped to IBKR CONTFUT",)
    return ResolvedSymbol(
        input_symbol="",
        canonical_symbol=root,
        asset_class=AssetClass.FUTURE,
        exchange=preset["exchange"],
        currency=preset["currency"],
        local_symbol=local_symbol,
        last_trade_date_or_contract_month=contract_month,
        continuous=continuous,
        matched_rule="future_pattern",
        notes=notes,
    )


def resolve_symbol(
    raw: str,
    *,
    asset_class: AssetClass | None = None,
    exchange: str | None = None,
    currency: str | None = None,
) -> ResolvedSymbol:
    """Resolve any supported ticker notation into IBKR contract fields.

    ``asset_class``, ``exchange``, and ``currency`` act as explicit overrides:
    when given, they pin the resolution and only the remaining fields are
    inferred. Options and bonds require the explicit class plus the extra
    contract fields; a bare notation cannot detect them.
    """
    text = raw.strip()
    if not text:
        raise SymbolResolutionError("empty symbol")
    upper = text.upper()

    resolved = _resolve_explicit(upper, asset_class) if asset_class is not None else _detect(upper)
    object.__setattr__(resolved, "input_symbol", raw)
    if exchange is not None:
        object.__setattr__(resolved, "exchange", exchange)
    if currency is not None:
        object.__setattr__(resolved, "currency", currency)
    return resolved


def _resolve_explicit(upper: str, asset_class: AssetClass) -> ResolvedSymbol:
    """Resolve a ticker under a pinned asset class."""
    if asset_class is AssetClass.EQUITY:
        equity = resolve_equity(upper)
        return ResolvedSymbol(
            input_symbol="", canonical_symbol=equity.symbol, asset_class=AssetClass.EQUITY,
            exchange=equity.exchange, currency=equity.currency,
            primary_exchange=equity.primary_exchange, matched_rule="explicit:equity",
        )
    if asset_class is AssetClass.INDEX:
        index = resolve_index(INDEX_ALIASES.get(upper, upper))
        return ResolvedSymbol(
            input_symbol="", canonical_symbol=index["symbol"], asset_class=AssetClass.INDEX,
            exchange=index["exchange"], currency=index["currency"], matched_rule="explicit:index",
        )
    if asset_class is AssetClass.FX:
        parts = _fx_parts(upper)
        if parts is None:
            raise SymbolResolutionError(f"{upper!r} is not a currency pair")
        return _resolve_fx(parts[0], parts[1], "")
    if asset_class is AssetClass.FUTURE:
        return _resolve_future_pattern(upper) or _resolve_future(
            upper, month_code=None, year_text=None, continuous=False, slot=None, local_symbol=None,
        )
    if asset_class is AssetClass.CRYPTO:
        parts = _crypto_parts(upper)
        if parts is None:
            raise SymbolResolutionError(f"{upper!r} is not a known crypto root")
        return _resolve_crypto(parts[0], parts[1])
    if asset_class is AssetClass.OPTION:
        return ResolvedSymbol(
            input_symbol="", canonical_symbol=upper, asset_class=AssetClass.OPTION,
            exchange="SMART", currency="USD", matched_rule="explicit:option",
            notes=("option contracts need con_id or local_symbol plus strike/expiry/right",),
        )
    if asset_class is AssetClass.BOND:
        return ResolvedSymbol(
            input_symbol="", canonical_symbol=upper, asset_class=AssetClass.BOND,
            exchange="SMART", currency="USD", matched_rule="explicit:bond",
            notes=("bond symbols are CUSIP/ISIN pass-through; IBKR resolves the bond",),
        )
    raise SymbolResolutionError(f"unsupported asset class {asset_class!r}")


def _detect(upper: str) -> ResolvedSymbol:
    """Auto-detect the asset class from the notation, most specific first."""
    crypto = _crypto_parts(upper)
    if crypto is not None:
        return _resolve_crypto(crypto[0], crypto[1])

    fx = _fx_parts(upper)
    if fx is not None:
        return _resolve_fx(fx[0], fx[1], _fx_separator(upper))

    future = _resolve_future_pattern(upper)
    if future is not None:
        return future

    alias = INDEX_ALIASES.get(upper)
    if alias is not None or is_known_index(upper):
        index = resolve_index(alias or upper)
        rule = "index_alias" if alias is not None else "known_index"
        return ResolvedSymbol(
            input_symbol="", canonical_symbol=index["symbol"], asset_class=AssetClass.INDEX,
            exchange=index["exchange"], currency=index["currency"], matched_rule=rule,
            notes=(f"alias {upper} -> {alias}",) if alias is not None else (),
        )

    equity = resolve_equity(upper)
    return ResolvedSymbol(
        input_symbol="", canonical_symbol=equity.symbol, asset_class=AssetClass.EQUITY,
        exchange=equity.exchange, currency=equity.currency,
        primary_exchange=equity.primary_exchange, matched_rule="equity_fallback",
    )


def _crypto_parts(upper: str) -> tuple[str, str | None] | None:
    """Return (root, quote) when the ticker is a known crypto notation."""
    if upper in CRYPTO_ROOTS:
        return upper, None
    if len(upper) >= 6:
        base = upper[:-3]
        quote = upper[-3:]
        if quote in {"USD", "USDT", "USDC"} and base in CRYPTO_ROOTS:
            return base, quote
    return None


def _fx_parts(upper: str) -> tuple[str, str] | None:
    """Return (base, quote) for a six-letter currency pair, separators removed."""
    compact = upper.replace("/", "").replace(".", "").replace("-", "")
    if len(compact) == 6 and compact[:3] in _CURRENCY_CODES and compact[3:] in _CURRENCY_CODES:
        return compact[:3], compact[3:]
    return None


def _fx_separator(upper: str) -> str:
    for separator in (".", "/", "-"):
        if separator in upper:
            return separator
    return ""


def _resolve_future_pattern(upper: str) -> ResolvedSymbol | None:
    """Match futures notations: continuous slots, dated contracts, dashed roots."""
    continuous_slot = _FUTURES_CONTINUOUS_SUFFIX_RE.match(upper)
    if continuous_slot is not None:
        return _resolve_future(
            continuous_slot.group("root"),
            month_code=None, year_text=None, continuous=True,
            slot=int(continuous_slot.group("slot")), local_symbol=None,
        )

    yahoo = _YAHOO_CONTINUOUS_RE.match(upper)
    if yahoo is not None:
        return _resolve_future(
            yahoo.group("root"),
            month_code=None, year_text=None, continuous=True, slot=1, local_symbol=None,
        )

    if upper.startswith("-") and len(upper) > 1:
        return _resolve_future(
            upper[1:], month_code=None, year_text=None, continuous=True,
            slot=None, local_symbol=None,
        )

    local = _FUTURES_LOCAL_RE.match(upper)
    if local is not None:
        root = local.group("root")
        if root and not root.isdigit():
            return _resolve_future(
                root, month_code=local.group("month"), year_text=local.group("year"),
                continuous=False, slot=None, local_symbol=upper,
            )
    return None
