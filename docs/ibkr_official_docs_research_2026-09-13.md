# IBKR documentation comparison — 13 September 2026

Implementation follow-up: the recommended first delivery is now implemented. See [gateway additions](gateway_equity_reference.md). The comparison below records the pre-implementation assessment.

## Verified upstream baseline

The official SDK download page lists **Latest API 10.50 (9 September 2026)** and **Stable API 10.45 (30 March 2026)**. These are SDK versions; this research did not establish the running IB Gateway build. The download page recommends TWS/IB Gateway 10.45 or higher for comprehensive support, but individual newer features have higher requirements. [Official SDK downloads](https://interactivebrokers.github.io/)

The separate documentation changelog still leads with the August 3 API 10.49 announcement, so it is not sufficient alone to establish the latest release. [Documentation changelog](https://www.interactivebrokers.com/docs/tws-api/changelog)

## Recent changes relevant to this gateway

| API release | Verified change | Gateway implication |
| --- | --- | --- |
| 10.50 | `conditionsIncludeOvernight` order field | Add only after condition support and serializer capability checks. |
| 10.49 | Settlement type in contract details | Extend contract metadata after decoder support. |
| 10.48 | Open-order responses include deactivated orders | Revisit status normalization. |
| 10.47 | Fundamental-data methods/callbacks and tick 47 removed | Existing fundamental-data routes need a compatibility decision. |
| 10.47 | Optional `$LEDGER-` prefix distinguishes per-currency account values; defaults differ for new/upgrading users | Preserve currency scope when normalizing account keys. |
| 10.46 | Odd-lot ticks and generic tick 787 | New quote fields require client support. |
| 10.45 | `hedgeMaxSize`; upgrade may clear socket API settings | Review order schema and deployment checks. |
| 10.44 | Fractional last sizes and update-config support | Preserve fractional quantities. |
| 10.43 | Mobile order staging and get-config support | Possible later operational features. |

Source for the table: [2026 API production release notes](https://www.ibkrguides.com/releasenotes/prod-2026.htm), updated September 4, 2026. These observations establish documented features, not compatibility of this repository's client.

API 10.34.01 introduced `reqCurrentTimeInMillis` / `currentTimeInMillis`; this is distinct from the gateway's existing seconds-based server-time endpoint. API 10.33 changed error callbacks, cancellation arguments and commission naming, making a client upgrade broader than adding DTO fields. [Documentation changelog](https://www.interactivebrokers.com/docs/tws-api/changelog)

## Practical additions with current documented support

| Candidate | IBKR interface and verified detail | Assessment |
| --- | --- | --- |
| Short availability | `reqMktData` generic 236 returns difficulty through tick 46 and available shares through tick 89; shares require TWS 974+ | Useful extension to equity snapshots or a bounded subscription endpoint. |
| Dividends | Generic 456 returns tick 59 through `tickString` | Expose structured dividend information with missing-data handling. |
| Odd-lot quote data | Generic 787 returns bid/ask prices, sizes and exchange identifiers through ticks 105–110; both TWS and API 10.46+ required | Useful modern addition, gated on client migration. |

The generic-tick mappings and requirements come from the current [available tick types table](https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/available-tick-types/introduction). Generic request codes differ from callback tick IDs. These are data capabilities, not guarantees that any particular account or instrument will return values.

Adaptive orders use `algoStrategy="Adaptive"` and the `adaptivePriority` parameter with `Urgent`, `Normal` or `Patient`. This offers a bounded first algorithmic-order extension rather than an unrestricted parameter pass-through. [Current Adaptive API documentation](https://www.interactivebrokers.com/docs/general/order-types/algorithmic-orders/ib-algorithms/adaptive-algo)

Combo orders are another extension area for multi-leg strategies. Their contract and order-leg representation must be modeled explicitly, including routing semantics, rather than treating a strategy as several unrelated orders. [Current combo-order documentation](https://www.interactivebrokers.com/docs/tws-api/doc/orders/place-order/combo-orders)

Historical trading schedules already exist in this repository through `load_trading_schedule` and commodity metadata. A general schedule endpoint would be an exposure improvement. [Current SCHEDULE documentation](https://www.interactivebrokers.com/docs/tws-api/doc/market-data-historical/historical-bar-what-to-show/schedule)

## Repository observations and limits

`requirements.txt` and `pyproject.toml` declare `ib-insync>=0.9.86`. Runtime imports in `src/feeds/ibkr_connection.py` use `ib_insync`; comments mentioning `ib_async` are not evidence of a migration. New SDK features cannot be assumed available merely because the IB Gateway application is upgraded.

Existing feed code already calls historical ticks, histogram data, market rules, SMART components, head timestamps, matching symbols, market-depth exchanges and scanner APIs. These should not be presented as entirely new backend capabilities without checking their REST/MCP exposure. The current reference feed also still calls `reqFundamentalDataAsync`, making the 10.47 removal directly relevant.

This is a documentation and source comparison. No SDK installation, runtime migration, connection to a brokerage account, or trading action was performed. New-feature support still requires installed-client inspection, negotiated protocol checks and paper-account validation.

## Recommended implementation order

The following priorities and effort ratings are engineering judgments from the local source at commit `c0182c1`. Paths below are proposed, under `/api/v1`; they do not represent implemented additions. Scope is this project's FastAPI/MCP gateway over the TWS socket API. IBKR Web API would require a separate authentication and transport integration.

| Priority | Proposed change | Current implementation and work required | Relative effort |
| --- | --- | --- | --- |
| First | `GET /system/capabilities` | `/system/version` returns only the application version. Report client package/version, negotiated server protocol and supported features, with unknown values while disconnected. Do not equate protocol number with Gateway application build. | Small |
| First | Resolve legacy fundamentals behavior | `reference_data.py:222` and `ibkr_reference_feed.py:181` expose the report request. Mark the documented removal and return an explicit unsupported result where appropriate; assess replacement data separately. | Small–medium |
| Next | `POST /market-data/equity/shortability` | Equity snapshot DTO lacks shortability fields; reference feed uses an empty generic tick list. Add bounded generic-data collection and typed availability responses. | Medium |
| Next | `POST /market-data/equity/dividends` | Add past/next twelve-month totals, next dividend date and amount. Existing WSH events and option present-value dividends serve different purposes. | Medium |
| Next | `GET /orders/open/all` | Reuse `IBKROrderClient.get_all_open_orders` at line 981 and existing MCP exposure. Current REST `/orders/open` calls the client-scoped method. Preserve order bearer authentication and pagination. | Small |
| Next | Complete bulletins, then `GET /reference-data/news/bulletins` | `ibkr_marketdata_ext.py:1186` subscribes but unconditionally returns `[]`; the MCP tool therefore does not deliver bulletins. Collect callbacks/cache, define subscription ownership and cancellation, then expose results. | Medium |
| Later | Adaptive order extension | `PlaceOrderRequest` has no algorithm strategy/parameters. Add a typed Adaptive configuration and verify placement, modification and preview serialization. | Medium |
| Later | `POST /orders/combo` | Order and contract models have no combo-leg schema. Define legs, ratios, actions, exchange and whole-order price semantics; reuse audit/idempotency handling and validate previews and fills. | Large |
| After transport work | Odd-lot quotes, settlement metadata, overnight conditions | Extend decoder/encoder support first, then DTOs, REST and MCP. A new Pydantic field alone cannot enable these features. | Large/shared prerequisite |

The all-orders request is a one-time snapshot of orders in associated accounts, not a subscription or authorization to modify every returned order. [IBKR all submitted orders](https://www.interactivebrokers.com/docs/tws-api/doc/order-management/requesting-currently-active-orders/all-submitted-orders)

Shortability and dividends must use a temporary streaming request with `snapshot=False`, a deadline and cleanup. IBKR's one-shot snapshots reject generic tick lists. Preserve the current equity snapshot behavior; share existing pacing and market-data-line accounting. Return unavailable fields as null with status/timestamp metadata. [IBKR snapshot semantics](https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/top-of-book-l-1/streaming-data-snapshots)

Shortability is indicative availability, not a share reservation. Dividend data may require direct routing. [Shortable](https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/available-tick-types/shortable), [IB Dividends](https://www.interactivebrokers.com/docs/tws-api/doc/market-data-live/available-tick-types/ib-dividends)

## Local compatibility evidence and validation plan

The installed `.venv` contains `ib_insync` **0.9.86**, whose `client.py` declares `MaxClientVersion = 176`. This is a protocol ceiling, not API release 10.176. The upstream repository was archived in March 2024. Installing a current `ibapi` package alongside it would not change this application's imports or serializers. Assess a maintained compatible client or an official SDK adapter, then test the actual required messages before choosing a migration. [Upstream ib_insync repository](https://github.com/erdewit/ib_insync)

Other existing capabilities include historical OHLCV, options chains/Greeks/skew, snapshots and SSE, depth, ticks, scanners, WSH/news, account/P&L, bracket/OCA orders, preview, executions and completed orders. `/system/server-time` is already REST-accessible. Contract search already returns trading/liquid hours and minimum tick; richer contract metadata is an expansion. Global cancel and exercise/lapse already exist in feed and MCP, so REST parity is possible, but exercise currently logs and ignores unsupported `manual_order_time`; that should be resolved before expanding exposure.

Recommended first delivery: capability reporting and fundamentals compatibility handling, followed by shortability, dividends and REST all-open-orders. Validate generic tick mapping, null versus zero, timeout/cancellation cleanup, concurrent subscription isolation, REST authentication and MCP parity. For transport upgrades, add connection/reconnect and order-event regression checks, then explicit paper-account checks. The comparison itself used static source inspection and official documentation; no runtime tests were necessary for this documentation-only change, and live support remains unverified.
