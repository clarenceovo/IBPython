# Business API Examples

These payloads are the source of truth for the business API examples shown in
Swagger/OpenAPI. Keep examples here instead of embedding them in router code.

## News

<!-- openapi-example: business.getSymbolNews tsla_news -->
### TSLA headlines
Only `symbol` is required for many SMART-routed US equities. `primary_exchange`
helps remove IBKR ambiguity.

```json
{
  "symbol": "TSLA",
  "primary_exchange": "NASDAQ",
  "total_results": 20,
  "include_articles": false
}
```

<!-- openapi-example: business.getSymbolNews spx_news_with_provider -->
### SPX index news
Use explicit provider codes when the caller wants to control the entitled news
sources used for the headline query.

```json
{
  "symbol": "SPX",
  "asset_class": "index",
  "exchange": "CBOE",
  "currency": "USD",
  "provider_codes": ["BZ", "FLY"],
  "total_results": 10
}
```

<!-- openapi-example: business.getNewsArticle benzinga_article -->
### News article body
Fetch a body after receiving `provider_code` and `article_id` from
`getSymbolNews` or `/reference-data/news/historical`.

```json
{
  "provider_code": "BZ",
  "article_id": "BZ$1",
  "use_ttl_cache": true,
  "cache_ttl_seconds": 300
}
```

## Market Research

<!-- openapi-example: business.getMarketPanel us_equity_panel -->
### US equity panel
Load normalized long-form OHLCV bars for several symbols with one business
payload.

```json
{
  "symbols": ["SPY", "QQQ", "TSLA"],
  "asset_class": "equity",
  "start_datetime": "2026-05-01T13:30:00Z",
  "end_datetime": "2026-05-01T20:00:00Z",
  "bar_size": "5 mins"
}
```

## Fixed Income

<!-- openapi-example: business.fixedIncome.getBondFutureQuotes ust_futures_quotes -->
### UST futures quotes
Load the latest IBKR OHLCV close for the default US Treasury futures strip.

```json
{
  "market": "UST",
  "contract_month": "202606",
  "bar_size": "1 min",
  "duration": "1 D"
}
```

<!-- openapi-example: business.fixedIncome.getCTD zn_ctd -->
### 10Y Treasury note CTD
Calculate CTD analytics for one bond future. This requires a configured
fixed-income reference provider. For local demos, set
`FIXED_INCOME_REFERENCE_PROVIDER=src.feeds.fixed_income_reference:provider`;
replace it with an exchange/vendor provider for production use.

```json
{
  "future": {
    "market": "UST",
    "futures_symbol": "ZN",
    "exchange": "CBOT",
    "currency": "USD",
    "contract_month": "202606"
  },
  "valuation_date": "2026-05-16"
}
```

<!-- openapi-example: business.fixedIncome.getFuturesImpliedCurve ust_implied_curve -->
### UST futures-implied curve
Build a futures-implied curve from CTD selections across the default Treasury
future strip. Requires `FIXED_INCOME_REFERENCE_PROVIDER`; the bundled
`src.feeds.fixed_income_reference:provider` is indicative demo data only.

```json
{
  "market": "UST",
  "contract_month": "202606",
  "valuation_date": "2026-05-16",
  "bar_size": "1 min",
  "duration": "1 D"
}
```

<!-- openapi-example: business.fixedIncome.getCashBondCurve ust_cash_curve -->
### UST cash curve
Build the current cash-curve response. Without a configured provider this uses
the existing indicative static standard-tenor data.

```json
{
  "market": "UST",
  "valuation_date": "2026-05-16"
}
```

<!-- openapi-example: business.fixedIncome.getCurveComparison ust_curve_comparison -->
### Cash versus futures-implied curve
Compare the indicative cash curve against a provider-backed futures-implied CTD
curve. Requires `FIXED_INCOME_REFERENCE_PROVIDER`; for local demos use
`src.feeds.fixed_income_reference:provider`.

```json
{
  "market": "UST",
  "contract_month": "202606",
  "valuation_date": "2026-05-16"
}
```

## Commodities

<!-- openapi-example: business.commodities.getFutures cl_front_forward -->
### CL front and forward futures
Derive the current listed contract month and the next forward month from
`as_of_date`, then load the latest IBKR OHLCV bar for each contract.

```json
{
  "symbol": "CL",
  "as_of_date": "2026-05-18",
  "forward_count": 1,
  "duration": "1 D",
  "bar_size": "1 min"
}
```

<!-- openapi-example: business.commodities.getFutures gc_front_forward -->
### GC front and forward futures
Gold uses the common COMEX listed-month cycle. For May 2026 the endpoint selects
June 2026 as front and August 2026 as the first forward.

```json
{
  "symbol": "GC",
  "as_of_date": "2026-05-18",
  "forward_count": 1,
  "duration": "1 D",
  "bar_size": "5 mins"
}
```

## Event Contracts

### ForecastEx product search
Find the artificial underlier record for a ForecastEx market such as Fed Funds.

```json
{
  "symbol": "FF"
}
```

### ForecastEx tradable contracts
Resolve YES/NO contracts for one underlier, month, and strike.

```json
{
  "underlying_con_id": 658663572,
  "exchange": "FORECASTX",
  "sec_type": "OPT",
  "month": "JUN26",
  "strike": 4.875
}
```

### Event Contract snapshot
Load top-of-book fields after the IBKR Web API market-data preflight.

```json
{
  "con_ids": [713921696, 713921701],
  "fields": ["31", "84", "85", "86", "88", "7059"]
}
```

### Guarded Event Contract order ticket
Build or submit a ForecastEx order. Live submission additionally requires the
order bearer token, `confirm_live_order=true`, and
`IBKR_EVENT_CONTRACTS_LIVE_ORDERS_ENABLED=true`.

```json
{
  "account_id": "DU123456",
  "con_id": 713921696,
  "side": "BUY",
  "order_type": "LMT",
  "quantity": 1,
  "price": 0.81,
  "tif": "DAY",
  "exchange": "FORECASTX",
  "confirm_live_order": true
}
```

<!-- openapi-example: business.getMarketPanel fx_panel -->
### FX midpoint panel
Use FX-friendly defaults for intraday currency-pair research.

```json
{
  "symbols": ["EURUSD", "USDJPY"],
  "asset_class": "fx",
  "exchange": "IDEALPRO",
  "what_to_show": "MIDPOINT",
  "use_rth": false,
  "bar_size": "1 hour"
}
```

<!-- openapi-example: business.getUniverseBars spx_composition -->
### Redis SPX composition bars
Read an index composition from Redis and load bars for its constituents.

```json
{
  "universe": "SPX",
  "bar_size": "1 day",
  "duration": "1 M",
  "max_symbols": 100
}
```

<!-- openapi-example: business.getUniverseBars adhoc_universe -->
### Ad-hoc explicit universe
Bypass Redis composition lookup and provide symbols directly.

```json
{
  "universe": "my_watchlist",
  "symbols": ["AAPL", "MSFT", "NVDA"],
  "bar_size": "1 day"
}
```

<!-- openapi-example: business.getReturns us_equity_returns -->
### US equity returns
Load bars and compute close-to-close simple/log returns.

```json
{
  "symbols": ["SPY", "QQQ", "TSLA"],
  "asset_class": "equity",
  "start_datetime": "2026-05-01T13:30:00Z",
  "end_datetime": "2026-05-01T20:00:00Z",
  "bar_size": "5 mins"
}
```

<!-- openapi-example: business.getOptionSkew tsla_skew -->
### TSLA option skew
Minimal business payload over the bounded option skew engine.

```json
{
  "symbol": "TSLA",
  "primary_exchange": "NASDAQ",
  "spot_price": 250.0,
  "max_expirations": 4,
  "max_strikes_per_expiry": 11
}
```
