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
fixed-income reference provider.

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
future strip.

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
curve.

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
