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

## Portfolio Risk

<!-- openapi-example: business.portfolio.getRiskSnapshot account_risk -->
### Account risk snapshot
Summarize liquidity, leverage, PnL, exposures, and position concentration from
the existing IBKR account and portfolio feeds.

```json
{
  "account": "DU123456",
  "include_account_pnl": true,
  "include_positions": true,
  "wait_seconds": 1.2,
  "use_ttl_cache": true,
  "cache_ttl_seconds": 5
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

## Histogram

<!-- openapi-example: business.histogram aapl_histogram -->
### AAPL equity histogram
Request a price histogram (frequency distribution) for an equity. The returned
data shows how many trades occurred at each price level over the requested time
period. Useful for identifying support/resistance clusters and volume profiles.

Ref: [IBKR Histogram API](https://interactivebrokers.github.io/tws-api/histograms.html)

```json
{
  "symbol": "AAPL",
  "asset_class": "EQUITY",
  "exchange": "SMART",
  "currency": "USD",
  "use_rth": true,
  "time_period": "1 day"
}
```

<!-- openapi-example: business.histogram es_futures_histogram -->
### ES futures histogram
Request a histogram for CME E-mini S&P 500 futures. Use `time_period: "1 week"`
for a broader view of the price distribution across multiple sessions.

```json
{
  "symbol": "ES",
  "asset_class": "FUTURE",
  "exchange": "CME",
  "currency": "USD",
  "use_rth": true,
  "time_period": "1 week"
}
```

## Real-Time Bars

<!-- openapi-example: business.realtimeBars.start aapl_realtime_bars -->
### AAPL real-time 5-second bars
Start a real-time bar subscription for AAPL. The server emits 5-second OHLCV
bars via the streaming connection. `what_to_show: "TRADES"` is the most common
choice for equities. The subscription remains active until explicitly cancelled
or until `duration_seconds` elapses.

Ref: [IBKR Real-Time Bars API](https://interactivebrokers.github.io/tws-api/realtime_bars.html)

```json
{
  "symbol": "AAPL",
  "asset_class": "EQUITY",
  "exchange": "SMART",
  "currency": "USD",
  "what_to_show": "TRADES",
  "use_rth": true
}
```

## System

<!-- openapi-example: business.system.setMarketDataType live_data -->
### Set market data type to live
Switch the IBKR connection to live (real-time) market data. Use this before
requesting quotes or bars to ensure the data feed returns real-time values
rather than delayed or frozen data.

- `1` — Live (requires paid data subscriptions)
- `2` — Frozen (last known value when market is closed)
- `3` — Delayed (free, typically 15-minute lag)
- `4` — Delayed frozen

Ref: [IBKR Market Data Types](https://interactivebrokers.github.io/tws-api/market_data_type.html)

```json
{
  "market_data_type": 1
}
```

<!-- openapi-example: business.system.getServerTime server_time -->
### Get server time
Retrieve the current IBKR server time. This is a GET endpoint with no request
body. Useful for verifying connectivity and checking for clock drift before
placing time-sensitive orders.

```json
{"server_time": "2025-06-11T14:30:00.000000-04:00", "timezone": "America/New_York"}
```

<!-- openapi-example: business.marketData.getDepthExchanges depth_exchanges -->
### Depth exchanges
Retrieve the list of exchanges that support market-depth (order-book) data.
This is a GET endpoint with no request body. Use the returned exchange codes
when requesting Level II data.

```json
{"exchanges": ["SMART", "ISLAND", "ARCA", "NYSE", "BATS", "DRCTEDGE", "BEX", "EDGEA", "CHX", "NSDQ"]}
```

## Reference: WhatToShow Values

All endpoints that accept a `what_to_show` parameter support these 15 values:

- **TRADES** — Trade prices (most common for equities, futures, forex)
- **MIDPOINT** — Midpoint between bid and ask (common for FX)
- **BID** — Bid price only
- **ASK** — Ask price only
- **BID_ASK** — Combined bid/ask bars
- **ADJUSTED_LAST** — Adjusted last price (corporate actions)
- **HISTORICAL_VOLATILITY** — Historical volatility calculation
- **OPTION_IMPLIED_VOLATILITY** — Option implied volatility series
- **AGGTRADES** — Aggregated trades (for US equities)
- **FEE_RATE** — Fee rate data
- **SCHEDULE** — Trading schedule information
- **YIELD_ASK** — Yield based on ask price
- **YIELD_BID** — Yield based on bid price
- **YIELD_BID_ASK** — Combined yield bid/ask
- **YIELD_LAST** — Yield based on last price

Ref: [IBKR Historical Data WhatToShow](https://interactivebrokers.github.io/tws-api/historical_data.html)

## Reference: Bar Size Aliases

Short aliases can be used in place of the full IBKR bar-size strings:

- `5m` → `5 mins`
- `1h` → `1 hour`
- `1d` → `1 day`
- `1w` → `1 week`
- `1mo` → `1 month`

## Reference: HTTP Status Codes

### POST endpoints — 201 Created

All POST endpoints that create resources return **201 Created** on success:

- `POST /api/v1/orders/place` — new order submitted
- `POST /api/v1/orders/bracket` — bracket order group submitted
- `POST /api/v1/orders/oca` — OCA order group submitted
- `POST /api/v1/subscribe/realtime-bars` — new subscription started
- `POST /api/v1/watchlist` — new watchlist created

```json
HTTP/1.1 201 Created
Content-Type: application/json
{
  "order_id": 12345,
  "status": "Submitted"
}
```

### DELETE endpoints — 200 OK (intentional)

> **Note:** DELETE endpoints return **200 OK** with a response body confirming
> the cancelled/deleted resource. This is intentional — the body contains useful
> metadata (order ID, cancellation status) that callers need for reconciliation.

```json
HTTP/1.1 200 OK
Content-Type: application/json
{
  "order_id": 12345,
  "status": "Cancelled"
}
```

## Reference: Idempotency-Key

POST `/api/v1/orders/place` supports idempotent retries via the
`Idempotency-Key` request header. If a request with the same key is received
within the server-side retention window, the original response is replayed
without creating a duplicate order.

### Request

```
POST /api/v1/orders/place HTTP/1.1
Idempotency-Key: unique-key-123
Authorization: Bearer <token>
Content-Type: application/json

{
  "account_id": "DU123456",
  "con_id": 713921696,
  "side": "BUY",
  "order_type": "LMT",
  "quantity": 1,
  "price": 0.81,
  "tif": "DAY",
  "exchange": "FORECASTX"
}
```

### Replay response

When the same `Idempotency-Key` is reused, the server returns the cached
response with an additional header:

```
HTTP/1.1 201 Created
Idempotency-Replayed: true
Content-Type: application/json

{
  "order_id": 12345,
  "status": "Submitted"
}
```

## Reference: Pagination

All GET endpoints that return lists support `limit` and `offset` query
parameters for cursor-style pagination.

### Example request

```
GET /api/v1/orders/open?limit=10&offset=0
Authorization: Bearer <token>
```

### Paginated response format

```json
{
  "items": [
    { "order_id": 10001, "symbol": "AAPL", "status": "Submitted" },
    { "order_id": 10002, "symbol": "TSLA", "status": "Submitted" }
  ],
  "total": 42,
  "limit": 10,
  "offset": 0
}
```

- `items` — array of results for the current page
- `total` — total number of records matching the query
- `limit` — page size used (mirrors request or server default)
- `offset` — zero-based offset into the full result set

## Reference: Rate Limiting

The API enforces per-client rate limiting. When the limit is exceeded the
server returns a **429 Too Many Requests** response with a `Retry-After`
header indicating how many seconds to wait before retrying.

```
HTTP/1.1 429 Too Many Requests
Retry-After: 60
Content-Type: application/json

{
  "detail": "Rate limit exceeded. Please retry after 60 seconds."
}
```

The rate limit is configurable via the `IBKR_REST_RATE_LIMIT_PER_MINUTE`
environment variable (see Configuration section below).

## Reference: CORS Configuration

Cross-Origin Resource Sharing (CORS) is controlled via the
`IBKR_REST_CORS_ORIGINS` environment variable. Set it to a comma-separated
list of allowed origins:

```bash
# Allow a single origin
IBKR_REST_CORS_ORIGINS=https://dashboard.example.com

# Allow multiple origins
IBKR_REST_CORS_ORIGINS=https://dashboard.example.com,https://admin.example.com

# Disable CORS (default — no origins allowed)
IBKR_REST_CORS_ORIGINS=
```

CORS headers are only added when the incoming `Origin` header matches one of
the configured values.

## Reference: Security Headers

All API responses include the following security headers:

```
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Referrer-Policy: strict-origin-when-cross-origin
```

These headers are applied globally and cannot be overridden by individual
endpoints.

## Reference: Error Handling

### Validation errors (422)

Request-body validation errors return structured field-level detail:

```json
HTTP/1.1 422 Unprocessable Entity
Content-Type: application/json

{
  "detail": [
    {
      "loc": ["body", "quantity"],
      "msg": "ensure this value is greater than 0",
      "type": "value_error.number.not_gt"
    }
  ]
}
```

### Runtime errors (500)

Unexpected server-side errors return a generic message. Internal details
(stack traces, variable names, file paths) are **never** leaked to the client:

```json
HTTP/1.1 500 Internal Server Error
Content-Type: application/json

{
  "detail": "An unexpected error occurred. Please try again later."
}
```

> **Note:** Detailed error information is written to server-side logs only.

## Reference: Configuration

Key environment variables for the REST API server:

- **`IBKR_REST_CORS_ORIGINS`** — Comma-separated list of allowed CORS origins.
  Default: empty (CORS disabled).
- **`IBKR_REST_RATE_LIMIT_PER_MINUTE`** — Maximum number of requests per client
  per minute before the server returns 429. Default: `60`.

```bash
# Example .env
IBKR_REST_CORS_ORIGINS=https://dashboard.example.com
IBKR_REST_RATE_LIMIT_PER_MINUTE=120
```

---

## MCP Tool Examples

The following tools are available through the Model Context Protocol (MCP)
interface. These wrap common IBKR TWS API operations into tool calls suitable
for LLM-driven trading workflows.

### request_histogram

Request a price histogram showing trade frequency at each price level.

**Parameters:**

- `symbol` (string, required) — Ticker symbol, e.g. `"AAPL"`
- `asset_class` (string, required) — Asset class: `"EQUITY"`, `"FUTURE"`, etc.
- `exchange` (string, required) — Exchange: `"SMART"`, `"CME"`, etc.
- `currency` (string, required) — Currency code: `"USD"`
- `use_rth` (boolean) — Use regular trading hours only. Default `true`
- `time_period` (string) — Aggregation period: `"1 day"`, `"1 week"`, etc.

**Example:**

```json
{
  "symbol": "AAPL",
  "asset_class": "EQUITY",
  "exchange": "SMART",
  "currency": "USD",
  "use_rth": true,
  "time_period": "1 day"
}
```

### subscribe_realtime_bars

Subscribe to real-time 5-second OHLCV bars for a contract.

**Parameters:**

- `symbol` (string, required) — Ticker symbol
- `asset_class` (string, required) — Asset class
- `exchange` (string, required) — Exchange
- `currency` (string, required) — Currency code
- `what_to_show` (string) — Data type: `"TRADES"`, `"MIDPOINT"`, etc. Default `"TRADES"`
- `use_rth` (boolean) — Use regular trading hours. Default `true`
- `duration_seconds` (integer) — Auto-unsubscribe after this many seconds. Default `300`

**Example:**

```json
{
  "symbol": "AAPL",
  "asset_class": "EQUITY",
  "exchange": "SMART",
  "currency": "USD",
  "what_to_show": "TRADES",
  "use_rth": true,
  "duration_seconds": 300
}
```

### get_depth_exchanges

List all exchanges that support market-depth (Level II) data.

**Parameters:** None

**Example:**

```
get_depth_exchanges()
```

### set_market_data_type

Switch the IBKR connection market data type.

**Parameters:**

- `market_data_type` (integer, required) — Data type:
  - `1` = Live (requires paid subscription)
  - `2` = Frozen
  - `3` = Delayed
  - `4` = DelayedFrozen

**Example:**

```json
{
  "market_data_type": 1
}
```

### get_server_time

Retrieve the current IBKR server timestamp.

**Parameters:** None

**Example:**

```
get_server_time()
```

### cancel_all_orders

Cancel all open orders across all accounts on the connected IBKR session.

**Parameters:** None

**Example:**

```
cancel_all_orders()
```

### get_all_open_orders

Retrieve all currently open (working) orders.

**Parameters:** None

**Example:**

```
get_all_open_orders()
```

### exercise_option

Exercise or lapse an option contract.

**Parameters:**

- `symbol` (string, required) — Underlying symbol, e.g. `"AAPL"`
- `right` (string, required) — `"C"` for call, `"P"` for put
- `strike` (float, required) — Strike price
- `expiry` (string, required) — Expiration date, e.g. `"20260619"`
- `exercise_action` (integer, required) — `1` = exercise, `2` = lapse
- `quantity` (integer, required) — Number of contracts
- `account` (string, required) — IBKR account ID
- `exchange` (string) — Exchange. Default `"SMART"`
- `currency` (string) — Currency. Default `"USD"`
- `override` (integer) — `1` = override system exercise, `0` = no override. Default `0`

**Example:**

```json
{
  "symbol": "AAPL",
  "right": "C",
  "strike": 200.0,
  "expiry": "20260619",
  "exercise_action": 1,
  "quantity": 1,
  "account": "DU123456",
  "exchange": "SMART",
  "currency": "USD",
  "override": 0
}
```

### get_historical_volatility

Retrieve historical volatility data for a contract.

**Parameters:**

- `symbol` (string, required) — Ticker symbol
- `asset_class` (string, required) — Asset class
- `exchange` (string) — Exchange. Default `"SMART"`
- `currency` (string) — Currency. Default `"USD"`
- `bar_size` (string) — Bar size, e.g. `"1 day"`. Default `"1 day"`
- `duration` (string) — Lookback period, e.g. `"1 Y"`. Default `"1 Y"`
- `use_rth` (boolean) — Use regular trading hours. Default `true`

**Example:**

```json
{
  "symbol": "AAPL",
  "asset_class": "EQUITY",
  "exchange": "SMART",
  "currency": "USD",
  "bar_size": "1 day",
  "duration": "1 Y",
  "use_rth": true
}
```

### get_option_implied_volatility_series

Retrieve option implied volatility data for a contract.

**Parameters:**

- `symbol` (string, required) — Ticker symbol
- `asset_class` (string, required) — Asset class
- `exchange` (string) — Exchange. Default `"SMART"`
- `currency` (string) — Currency. Default `"USD"`
- `bar_size` (string) — Bar size. Default `"1 day"`
- `duration` (string) — Lookback period. Default `"1 Y"`
- `use_rth` (boolean) — Use regular trading hours. Default `true`

**Example:**

```json
{
  "symbol": "SPY",
  "asset_class": "EQUITY",
  "exchange": "SMART",
  "currency": "USD",
  "bar_size": "1 day",
  "duration": "6 M",
  "use_rth": true
}
```

### get_yield_data

Retrieve yield-based data (bid, ask, or last yield) for a fixed-income or
index contract.

**Parameters:**

- `symbol` (string, required) — Ticker or contract symbol
- `what_to_show` (string, required) — One of `"YIELD_ASK"`, `"YIELD_BID"`, `"YIELD_BID_ASK"`, `"YIELD_LAST"`
- `bar_size` (string) — Bar size. Default `"1 day"`
- `duration` (string) — Lookback period. Default `"1 M"`
- `exchange` (string) — Exchange. Default `"SMART"`
- `currency` (string) — Currency. Default `"USD"`
- `use_rth` (boolean) — Use regular trading hours. Default `true`

**Example:**

```json
{
  "symbol": "TLT",
  "what_to_show": "YIELD_LAST",
  "bar_size": "1 day",
  "duration": "1 M",
  "exchange": "SMART",
  "currency": "USD",
  "use_rth": true
}
```

### get_trading_schedule

Retrieve trading schedule (session open/close times) for a contract.

**Parameters:**

- `symbol` (string, required) — Ticker symbol
- `asset_class` (string, required) — Asset class
- `exchange` (string) — Exchange. Default `"SMART"`
- `currency` (string) — Currency. Default `"USD"`
- `end_date` (string) — End date for the schedule range, e.g. `"20260620"`
- `num_days` (integer) — Number of days to include. Default `5`

**Example:**

```json
{
  "symbol": "AAPL",
  "asset_class": "EQUITY",
  "exchange": "SMART",
  "currency": "USD",
  "end_date": "20260620",
  "num_days": 5
}
```

### place_bracket_order

Place a bracket order (entry + take-profit + stop-loss) as a single atomic
group.

**Parameters:**

- `symbol` (string, required) — Ticker symbol
- `action` (string, required) — `"BUY"` or `"SELL"`
- `quantity` (float, required) — Order quantity
- `limit_price` (float, required) — Entry limit price
- `take_profit_price` (float, required) — Take-profit limit price
- `stop_loss_price` (float, required) — Stop-loss trigger price
- `order_type` (string) — Order type. Default `"LMT"`
- `tif` (string) — Time in force. Default `"GTC"`
- `asset_class` (string) — Asset class. Default `"STK"`
- `exchange` (string) — Exchange. Default `"SMART"`
- `currency` (string) — Currency. Default `"USD"`
- `account` (string, required) — IBKR account ID

**Example:**

```json
{
  "symbol": "AAPL",
  "action": "BUY",
  "quantity": 100,
  "limit_price": 195.00,
  "take_profit_price": 210.00,
  "stop_loss_price": 190.00,
  "order_type": "LMT",
  "tif": "GTC",
  "asset_class": "STK",
  "exchange": "SMART",
  "currency": "USD",
  "account": "DU123456"
}
```

### place_oca_group

Place multiple orders grouped as a One-Cancels-All (OCA) group. When one order
in the group fills, the remaining orders are automatically cancelled.

**Parameters:**

- `orders` (array of objects, required) — List of order dicts, each containing
  `symbol`, `action`, `quantity`, `order_type`, `price`, `tif`, `asset_class`,
  `exchange`, `currency`
- `oca_group` (string, required) — User-defined OCA group name, e.g. `"my_oca_1"`
- `oca_type` (integer, required) — OCA cancellation type:
  - `1` = Cancel all remaining orders with block
  - `2` = Remaining orders are proportionately reduced
  - `3` = Remaining orders are reduced with no block
- `account` (string, required) — IBKR account ID

**Example:**

```json
{
  "orders": [
    {
      "symbol": "AAPL",
      "action": "BUY",
      "quantity": 100,
      "order_type": "LMT",
      "price": 195.00,
      "tif": "DAY"
    },
    {
      "symbol": "MSFT",
      "action": "BUY",
      "quantity": 50,
      "order_type": "LMT",
      "price": 420.00,
      "tif": "DAY"
    }
  ],
  "oca_group": "tech_buy_20260611",
  "oca_type": 1,
  "account": "DU123456"
}
```

### scan_market

Run a market scanner using IBKR's predefined scan parameters.

**Parameters:**

- `instrument` (string, required) — Instrument type: `"STK"`, `"FUT"`, `"OPT"`, etc.
- `location` (string) — Exchange/location code: `"STK.US"`, `"FUT.US"`, etc.
- `scan_code` (string, required) — Scanner code, e.g. `"TOP_PERC_GAIN"`, `"HIGH_OPT_IMP_VOLAT"`
- `above_price` (float) — Minimum price filter
- `below_price` (float) — Maximum price filter
- `above_volume` (integer) — Minimum volume filter
- `market_cap_above` (float) — Minimum market cap filter (millions)
- `market_cap_below` (float) — Maximum market cap filter (millions)
- `max_results` (integer) — Maximum number of results. Default `50`

**Example:**

```json
{
  "instrument": "STK",
  "location": "STK.US",
  "scan_code": "TOP_PERC_GAIN",
  "above_price": 5.0,
  "above_volume": 1000000,
  "max_results": 25
}
```

### get_news_bulletins

Retrieve IBKR news bulletins. These are brief regulatory or corporate-action
messages broadcast by exchanges and regulators.

**Parameters:**

- `all_messages` (boolean) — If `true`, return all historical bulletins; if `false`, return only new/unread ones. Default `false`

**Example:**

```json
{
  "all_messages": false
}
```
