# Equity reference data and gateway capabilities

The first delivery from the September 2026 IBKR comparison adds four REST endpoints and three MCP tools. It uses the existing `ib_insync` transport. No new broker credentials or service are required.

| REST endpoint | MCP tool | Result |
| --- | --- | --- |
| `GET /api/v1/system/capabilities` | `get_gateway_capabilities` | Installed client version, connection state, negotiated protocol and feature implementation status |
| `POST /api/v1/market-data/equity/shortability` | `load_equity_shortability` | Indicative available shares and optional shortability rating |
| `POST /api/v1/market-data/equity/dividends` | `load_equity_dividends` | Past/next twelve-month dividends and next dividend date/amount |
| `GET /api/v1/orders/open/all?offset=0&limit=100` | Existing `get_all_open_orders` | One-time open-order snapshot across API clients in associated accounts |

The capabilities endpoint performs no network request. `client_supported` is null when no client instance is available. `availability: unknown` means that a feature's live support, entitlements and instrument availability have not been verified; it does not mean an account has access. The negotiated protocol is not the installed TWS/IB Gateway application version.

## Running

Use the project's existing environment and settings:

```sh
.venv/bin/python -m pip install -r requirements.txt
make run-api
# Or run the MCP server over stdio:
.venv/bin/python -m src.mcp_server
```

The existing `IBKR_HOST`, `IBKR_PORT` and `IBKR_CLIENT_ID` settings configure REST's broker connection. MCP retains its separate configured client ID. Redis remains the existing pacing/auth backend. The MCP dependency is constrained to `>=1.0,<2` because this server uses the v1 `FastMCP` interface, removed in MCP v2.

Example bodies for both equity endpoints:

```json
{"symbol":"AAPL","timeout_seconds":10}
```

Optional fields are `exchange`, `currency`, `primary_exchange`, and positive `con_id`. Symbol suffixes use the same equity resolver as other gateway endpoints. For example, `0700.HK` resolves the Hong Kong exchange and currency. Explicit exchange/currency fields override those defaults.

```sh
curl -X POST http://localhost:8000/api/v1/market-data/equity/shortability \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"AAPL","timeout_seconds":10}'
curl -X POST http://localhost:8000/api/v1/market-data/equity/dividends \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"AAPL","exchange":"NASDAQ","timeout_seconds":10}'
```

Supply the existing API-wide bearer token if enabled. The all-orders endpoint additionally follows the existing order router's Redis bearer-token check; unauthenticated requests return 401. Its pagination defaults to 100 records and permits up to 1,000 per response. It does not bind orders or grant permission to modify orders from another client.

## Request outcomes

Equity reference requests accept `0 < timeout_seconds <= 20` (default 10). The deadline includes connection, qualification, pacing and data collection; cancellation cleanup has an additional five-second budget. IBKR generic ticks 236 and 456 are collected through temporary streaming subscriptions, not one-shot market-data snapshots. Each request owns a separate contract identity, so cancelling one request does not cancel another request for the same symbol.

- `status: available`: share count received, or all four dividend fields received.
- `status: partial`: some requested information arrived, but completeness was not reached before the deadline. The shortability rating alone is partial.
- `status: unavailable`: no requested values arrived.
- `timed_out: true`: collection ended at its deadline. Null fields remain null; zero is preserved as a valid observation.
- `observed_at`: local UTC observation time, null if no requested data arrived. It is not an exchange timestamp.
- `received_at`: local UTC response creation time. `market_data_type` is the client's reported data type when observed.

Partial/unavailable data after successful subscription returns HTTP 200 with explicit status. Setup timeouts and broker subscription errors return HTTP 503; validation errors return 422. Cancellation does not become a successful result. The application reuses its global pacing and line leases and removes the request's retained client ticker entries on cleanup.

Shortable shares are indicative, not reserved. Dividend values are estimates and some instruments require direct routing. [IBKR shortability](https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/available-tick-types/shortable), [IBKR dividends](https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/available-tick-types/ib-dividends), [snapshot limitations](https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/top-of-book-l-1/streaming-data-snapshots).

## Removed fundamental reports

`POST /api/v1/reference-data/fundamentals` is deprecated in OpenAPI and returns **410 Gone**, without connecting or retrying. The legacy `load_fundamentals` MCP tool raises an explicit unsupported-feature error. This is a gateway policy for the upstream removal, including when an older client still has the method; no release number is guessed from the negotiated protocol. WSH events and dividend ticks continue as separate data products. [IBKR 10.47 removal](https://www.ibkrguides.com/releasenotes/prod-2026.htm).

## Verification

Focused tests run without broker access:

```sh
.venv/bin/python -m pytest tests/test_equity_reference.py tests/test_gateway_additions.py tests/test_mcp_server.py -q
```

They use the installed ib_insync decoder with fake socket calls to verify generic ticks, zero/missing values, deadlines, concurrent isolation, broker errors, and cleanup. REST checks cover validation, deprecation, authentication and pagination; MCP checks exercise the shared request/response models. Live IBKR entitlements and delivery have not been tested.

Bulletin completion, Adaptive/combo orders, and newer protocol features remain later work from the comparison.
