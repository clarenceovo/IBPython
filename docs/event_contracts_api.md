# Event Contracts API

Endpoint reference for ForecastEx and CME Event Contract workflows exposed by
`IBKRRestApp`.

These endpoints wrap IBKR Web API Event Contract discovery, market data,
historical bars, websocket message construction, and guarded order-ticket
submission.

## Base Path

```text
/api/v1/business/event-contracts
```

Local default:

```text
http://localhost:8000/api/v1/business/event-contracts
```

## Prerequisites

- IBKR Client Portal Gateway or OAuth access must be configured for IBKR Web API.
- The IBKR Web API session must be authorized and, for `/iserver/*` operations,
  brokerage-session enabled.
- Market data endpoints require the relevant IBKR permissions and market data
  subscriptions.
- Live order submission is disabled by default and requires an explicit runtime
  switch plus order bearer-token auth.

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| `IBKR_WEB_API_BASE_URL` | `https://localhost:5000/v1/api` | IBKR Web API base URL. Use local Client Portal Gateway or direct OAuth URL. |
| `IBKR_WEB_API_BEARER_TOKEN` | empty | Optional OAuth bearer token for direct IBKR Web API calls. |
| `IBKR_WEB_API_COOKIE` | empty | Optional raw `Cookie` header for authenticated Client Portal Gateway sessions. |
| `IBKR_WEB_API_VERIFY_SSL` | `false` | TLS verification. Local CPGW commonly uses a self-signed certificate. |
| `IBKR_EVENT_CONTRACTS_LIVE_ORDERS_ENABLED` | `false` | Enables live Event Contract order submission. |
| `IBKR_ORDER_AUTH_REDIS_KEY` | `OrderAuth::bearer_token` | Redis key containing the bearer token required by `/orders/place`. |

## Event Contract Concepts

- ForecastEx Event Contracts are modeled as options (`OPT`) on artificial
  index underliers at exchange `FORECASTX`.
- CME Group Event Contracts are modeled as futures options (`FOP`) and are
  usually filtered by trading classes such as `ECNQ`.
- `right = C` means YES; `right = P` means NO.
- ForecastEx instruments cannot be sold. To reduce or flip exposure, submit a
  buy order for the opposing YES/NO contract.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/categories` | Load ForecastEx category tree. |
| `POST` | `/discover/search` | Search Event Contract underliers by product code. |
| `POST` | `/discover/strikes` | Load valid strikes for an underlier/month. |
| `POST` | `/discover/info` | Resolve tradable Event Contract instruments and conIds. |
| `POST` | `/market-data/snapshot` | Load top-of-book snapshot fields. |
| `POST` | `/market-data/history` | Load historical OHLC bars. |
| `POST` | `/market-data/streaming-messages` | Build Web API websocket subscribe/unsubscribe messages. |
| `POST` | `/orders/build` | Validate and build an order ticket without submitting. |
| `POST` | `/orders/place` | Submit a guarded live Event Contract order. |

## Discovery Workflow

### 1. Search Product Code

```http
POST /api/v1/business/event-contracts/discover/search
Content-Type: application/json
```

```json
{
  "symbol": "FF"
}
```

Response:

```json
[
  {
    "con_id": 658663572,
    "symbol": "FF",
    "description": "FORECASTX",
    "company_name": "US Fed Funds Target Rate",
    "company_header": "US Fed Funds Target Rate - FORECASTX",
    "opt_expirations": ["20260616", "20260728"],
    "fop_expirations": [],
    "sections": [],
    "raw": {}
  }
]
```

For CME underliers, pass `sec_type = "IND"` when needed:

```json
{
  "symbol": "NQ",
  "sec_type": "IND"
}
```

### 2. Load Valid Strikes

```http
POST /api/v1/business/event-contracts/discover/strikes
Content-Type: application/json
```

```json
{
  "underlying_con_id": 658663572,
  "exchange": "FORECASTX",
  "sec_type": "OPT",
  "month": "JUN26"
}
```

Response:

```json
{
  "call": [4.875, 5.125],
  "put": [4.875, 5.125],
  "all_strikes": [4.875, 5.125],
  "raw": {}
}
```

### 3. Resolve Tradable Instruments

```http
POST /api/v1/business/event-contracts/discover/info
Content-Type: application/json
```

```json
{
  "underlying_con_id": 658663572,
  "exchange": "FORECASTX",
  "sec_type": "OPT",
  "month": "JUN26",
  "strike": 4.875
}
```

Response:

```json
[
  {
    "con_id": 713921696,
    "symbol": "FF",
    "sec_type": "OPT",
    "exchange": "FORECASTX",
    "right": "C",
    "yes_no": "YES",
    "strike": 4.875,
    "currency": "USD",
    "desc1": "FF",
    "desc2": "JUN 16 '26 4.88 Call @FORECASTX",
    "maturity_date": "20260616",
    "multiplier": "1",
    "trading_class": "FF",
    "valid_exchanges": "FORECASTX",
    "raw": {}
  }
]
```

For CME Event Contracts, use `sec_type = "FOP"` and optionally filter by
`trading_class_prefix`:

```json
{
  "underlying_con_id": 11004958,
  "exchange": "CME",
  "sec_type": "FOP",
  "month": "AUG24",
  "trading_class_prefix": "EC"
}
```

## Market Data

### Snapshot

```http
POST /api/v1/business/event-contracts/market-data/snapshot
Content-Type: application/json
```

```json
{
  "con_ids": [713921696, 713921701],
  "fields": ["31", "84", "85", "86", "88", "7059"]
}
```

Default fields:

| Field | Meaning |
|---|---|
| `31` | Last price |
| `84` | Bid price |
| `85` | Ask size |
| `86` | Ask price |
| `88` | Bid size |
| `7059` | Last size |

Response:

```json
[
  {
    "con_id": 713921696,
    "con_id_ex": "713921696",
    "updated_at": "2026-05-28T05:46:40Z",
    "last": 0.81,
    "bid": 0.79,
    "bid_size": 14,
    "ask": 0.82,
    "ask_size": 11,
    "last_size": 6,
    "raw": {}
  }
]
```

IBKR Web API snapshots may require a first preflight call before values appear.
If the response only contains `conid`/`conidEx`, repeat the request after the
stream has initialized.

### Historical Bars

```http
POST /api/v1/business/event-contracts/market-data/history
Content-Type: application/json
```

```json
{
  "con_id": 721095500,
  "period": "2d",
  "bar": "1h",
  "start_time": "2026-06-16T17:00:00Z"
}
```

Response:

```json
{
  "con_id": 721095500,
  "symbol": "FF",
  "text": "US Fed Funds Target Rate",
  "period": "2d",
  "bar_length_seconds": 3600,
  "bars": [
    {
      "timestamp": "2026-06-16T17:00:00Z",
      "open": 0.2,
      "high": 0.2,
      "low": 0.19,
      "close": 0.2,
      "volume": 0,
      "raw": {}
    }
  ],
  "raw": {}
}
```

### Websocket Message Helper

This endpoint does not open a websocket. It builds the IBKR Web API websocket
messages the client should send.

```http
POST /api/v1/business/event-contracts/market-data/streaming-messages
Content-Type: application/json
```

```json
{
  "con_id": 721095500,
  "fields": ["31", "84", "86"]
}
```

Response:

```json
{
  "subscribe": "smd+721095500+{\"fields\":[\"31\",\"84\",\"86\"]}",
  "unsubscribe": "umd+721095500+{}"
}
```

## Orders

### Build Order Ticket

Validates the local Event Contract order rules and returns the Web API ticket.
No order is submitted.

```http
POST /api/v1/business/event-contracts/orders/build
Content-Type: application/json
```

```json
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

Response:

```json
{
  "account_id": "DU123456",
  "live_order_enabled": false,
  "ticket": {
    "conid": 713921696,
    "side": "BUY",
    "orderType": "LMT",
    "quantity": 1,
    "tif": "DAY",
    "price": 0.81
  },
  "warnings": [
    "ForecastEx positions are reduced by buying the opposing YES/NO contract; SELL is blocked for FORECASTX.",
    "This endpoint only builds the Web API ticket; it does not submit an order."
  ]
}
```

### Place Live Order

Live order submission requires all of the following:

- `Authorization: Bearer <order-token>`
- Redis key `IBKR_ORDER_AUTH_REDIS_KEY` must contain the matching token
- `IBKR_EVENT_CONTRACTS_LIVE_ORDERS_ENABLED=true`
- Request body must include `confirm_live_order=true`

```http
POST /api/v1/business/event-contracts/orders/place
Authorization: Bearer test-order-token
Content-Type: application/json
```

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

Response:

```json
{
  "account_id": "DU123456",
  "submitted": true,
  "response": {
    "order_id": "987654",
    "order_status": "Submitted"
  },
  "warnings": [
    "IBKR order reply messages are returned raw; this client does not auto-confirm /iserver/reply prompts."
  ]
}
```

## Error Behavior

| Status | Source | Meaning |
|---|---|---|
| `401` | `/orders/place` | Missing or invalid order bearer token. |
| `403` | `/orders/place` | Live Event Contract orders are disabled. |
| `422` | Request validation | Invalid payload, ForecastEx sell attempt, missing limit price, or missing `confirm_live_order`. |
| `502` | IBKR Web API parser | Upstream response did not match the expected shape. |
| `503` | IBKR Web API transport | Gateway/OAuth endpoint was unreachable or returned a transport error. |
| Upstream status | IBKR Web API | IBKR returned an HTTP error; the route forwards that status and response text. |

## Curl Examples

Search ForecastEx Fed Funds:

```bash
curl -s http://localhost:8000/api/v1/business/event-contracts/discover/search \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"FF"}'
```

Build an order ticket:

```bash
curl -s http://localhost:8000/api/v1/business/event-contracts/orders/build \
  -H 'Content-Type: application/json' \
  -d '{"account_id":"DU123456","con_id":713921696,"quantity":1,"price":0.81}'
```

Submit a live order after enabling the safety switch:

```bash
curl -s http://localhost:8000/api/v1/business/event-contracts/orders/place \
  -H 'Authorization: Bearer test-order-token' \
  -H 'Content-Type: application/json' \
  -d '{"account_id":"DU123456","con_id":713921696,"quantity":1,"price":0.81,"confirm_live_order":true}'
```

## Source Notes

This project follows IBKR's documented Event Contract workflow:

- category tree: `/trsrv/event/category-tree`
- discovery: `/iserver/secdef/search`, `/iserver/secdef/strikes`, `/iserver/secdef/info`
- market data: `/iserver/marketdata/snapshot`, `/iserver/marketdata/history`
- order submission: `/iserver/account/{accountId}/orders`

IBKR docs: <https://ibkrcampus.com/campus/ibkr-api-page/event-contracts/>
