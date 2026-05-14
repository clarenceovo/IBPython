# IBKR API Deep Research

> Comprehensive IBKR API reference for NovaTradingTech.  
> Compiled from official IBKR documentation (ibkrcampus.com, interactivebrokers.github.io), GitHub community libraries, and production experience.  
> Date: 2025-05-14

---

## 1. Architecture Overview

### 1.1 TWS (Trader Workstation)

- **What**: Full-featured Java trading application with built-in charting, order management, and API access.
- **Ports**:
  - Live: `7496` (TWS paper trading: `7497`)
  - Only one API client can connect per TWS instance (but multiple clientId connections allowed).
- **Auth**: None at API level — authentication is handled by the TWS login. The API connection is localhost-only by default.
- **Deployment**: Desktop application, must be running for API to work. Not suitable for headless production.
- **Limitation**: Auto-logoff daily; requires manual restart or IBC automation.

### 1.2 IB Gateway (IB Gateway)

- **What**: Headless version of TWS — no GUI, only API access. Same API protocol as TWS.
- **Ports**:
  - Live: `4001` (Gateway paper trading: `4002`)
- **Auth**: Same as TWS — login credentials required at startup.
- **Deployment**: Suitable for servers/Docker. Requires IBC (IBController) for automated startup/login.
- **Advantage over TWS**: Lower resource usage, no GUI overhead, better for production.
- **Sessions**: Auto-logoff configurable. Gateway needs to be restarted after logoff.
- **Memory**: Recommended to set Java heap to 4096 MB minimum to prevent crashes during bulk data loading.
- **Stable vs Latest gateway**:
  - Stable: Updated every few months
  - Latest: Updated weekly

### 1.3 TWS API (Binary Protocol)

- **What**: TCP socket protocol using a proprietary binary format. The Python `ibapi` library implements this.
- **Connection**: Direct TCP socket to TWS/Gateway. No HTTP involved.
- **Threading model (official ibapi)**: Uses a dedicated reader thread + callback-based event model via `EWrapper`.
- **clientId**: Each connection specifies a `clientId` (integer). Multiple connections with different `clientId` values can connect simultaneously.
- **Session management**: On connect, TWS sends `nextValidId` callback with the next valid order ID. Must wait for this before placing orders.
- **Data flow**: Request-response + streaming. Market data is push-based via callbacks.

### 1.4 Web API (REST)

- **What**: RESTful HTTP API served by the Client Portal Gateway (CPG).
- **Base URL**: `https://localhost:5000/v1/api/` (default)
- **Auth**: Session-based. Requires login via `/iserver/auth/status` endpoint.
- **Note**: The IBKR Campus reference pages are JavaScript-heavy SPAs — direct content scraping is not feasible. Use the Swagger/OpenAPI spec or the interactive docs.
- **Endpoints documented at**: `ibkrcampus.com/campus/ibkr-api-page/webapi-ref/`
- **Web API v1.0**: Legacy REST API (being deprecated in favor of current Web API).

### 1.5 WebSocket API

- **What**: Part of the Web API ecosystem for streaming market data.
- **Protocol**: WebSocket over the Client Portal Gateway.
- **Use case**: Real-time market data subscriptions without TWS API binary protocol.

### 1.6 Key Architecture Decisions

| Feature | TWS API (Binary) | Web API (REST) |
|---------|-----------------|----------------|
| Protocol | TCP binary | HTTP REST + WebSocket |
| Auth | TWS/Gateway login | Session-based |
| Market Data | Full (streaming ticks) | Limited (polling + WS) |
| Order Management | Full | Full |
| Speed | Fastest (direct) | Slower (HTTP overhead) |
| Headless | Requires Gateway | Requires CPG |
| Python lib | ibapi / ib_async | requests / httpx |

---

## 2. Complete Endpoint/Function Inventory

### 2.1 TWS API — EClient Request Methods (Python ibapi)

All methods are on `IBApi.EClient` (or `self` when subclassing in Python).

#### Connection & Session

| Method | Signature | Description |
|--------|-----------|-------------|
| `connect` | `connect(host: str, port: int, clientId: int)` | Establish TCP connection to TWS/Gateway |
| `disconnect` | `disconnect()` | Close connection |
| `run` | `run()` | Start the message processing loop (blocking) |
| `isConnected` | `isConnected() -> bool` | Check connection status |
| `setServerLogLevel` | `setServerLogLevel(logLevel: int)` | Set TWS/Gateway log verbosity |

#### Market Data

| Method | Signature | Description |
|--------|-----------|-------------|
| `reqMktData` | `reqMktData(reqId: int, contract: Contract, genericTickList: str, snapshot: bool, regulatorySnapshot: bool, mktDataOptions: list)` | Subscribe to market data. `genericTickList` is comma-separated generic tick types (e.g., "100,101,104"). `snapshot=True` for one-time snapshot. |
| `cancelMktData` | `cancelMktData(reqId: int)` | Unsubscribe from market data |
| `reqMarketDataType` | `reqMarketDataType(marketDataType: int)` | Set market data type: 1=Live, 2=Frozen, 3=Delayed, 4=DelayedFrozen |
| `reqSmartComponents` | `reqSmartComponents(reqId: int, bboExchange: str)` | Request smart components for BBO exchange |
| `reqMktDepthEx` | `reqMktDepthEx()` | Request exchanges supporting market depth |
| `reqMktDepth` | `reqMktDepth(reqId: int, contract: Contract, numRows: int, isSmartDepth: bool, mktDepthOptions: list)` | Subscribe to market depth (order book). `numRows` = depth levels (max 5 by default). |
| `cancelMktDepth` | `cancelMktDepth(reqId: int, isSmartDepth: bool)` | Unsubscribe from market depth |
| `reqTickByTickData` | `reqTickByTickData(reqId: int, contract: Contract, tickType: str, numberOfTicks: int, ignoreSize: bool)` | Subscribe to tick-by-tick data. `tickType`: "Last", "BidAsk", "MidPoint". `numberOfTicks`: 0 = all. Available from API v973.04+ / TWS v969+. |
| `cancelTickByTickData` | `cancelTickByTickData(reqId: int)` | Cancel tick-by-tick subscription |
| `reqRealTimeBars` | `reqRealTimeBars(reqId: int, contract: Contract, barSize: int, whatToShow: str, useRTH: bool, realTimeBarsOptions: list)` | Subscribe to 5-second real-time bars. `barSize` must be 5. `whatToShow`: "TRADES", "MIDPOINT", "BID", "ASK". |
| `cancelRealTimeBars` | `cancelRealTimeBars(reqId: int)` | Cancel real-time bars subscription |
| `reqHistoricalData` | `reqHistoricalData(reqId: int, contract: Contract, endDateTime: str, durationStr: str, barSizeSetting: str, whatToShow: str, useRTH: bool, formatDate: int, keepUpToDate: bool, chartOptions: list)` | Request historical bars. `formatDate`: 1=yyyyMMdd HH:mm:ss, 2=Unix timestamp. `keepUpToDate=True` for live updates. |
| `cancelHistoricalData` | `cancelHistoricalData(reqId: int)` | Cancel historical data subscription |
| `reqHeadTimeStamp` | `reqHeadTimeStamp(reqId: int, contract: Contract, whatToShow: str, useRTH: bool, formatDate: int)` | Get earliest available data timestamp |
| `cancelHeadTimeStamp` | `cancelHeadTimeStamp(reqId: int)` | Cancel head timestamp request |
| `reqHistogramData` | `reqHistogramData(reqId: int, contract: Contract, useRTH: bool, timePeriod: str)` | Request price histogram data |
| `cancelHistogramData` | `cancelHistogramData(reqId: int)` | Cancel histogram request |
| `reqHistoricalTicks` | `reqHistoricalTicks(reqId: int, contract: Contract, startDateTime: str, endDateTime: str, numberOfTicks: int, whatToShow: str, useRTH: bool, ignoreSize: bool, miscOptions: list)` | Request historical tick data. `whatToShow`: "TRADES", "BID_ASK", "MIDPOINT". |
| `reqFundamentalData` | `reqFundamentalData(reqId: int, contract: Contract, reportType: str, fundamentalDataOptions: list)` | Request fundamental data (Reuters, etc.) |
| `cancelFundamentalData` | `cancelFundamentalData(reqId: int)` | Cancel fundamental data |

#### Contract & Instrument

| Method | Signature | Description |
|--------|-----------|-------------|
| `reqContractDetails` | `reqContractDetails(reqId: int, contract: Contract)` | Get full contract details. Returns all matching contracts. |
| `reqSecDefOptParams` | `reqSecDefOptParams(reqId: int, underlyingSymbol: str, futFopExchange: str, underlyingSecType: str, underlyingConId: int)` | Get option chain parameters (strikes, expirations). No throttling unlike reqContractDetails. API v9.72+. |
| `reqMatchingSymbols` | `reqMatchingSymbols(reqId: int, pattern: str)` | Search for symbols matching pattern |
| `reqFamilyCodes` | `reqFamilyCodes()` | Get family codes for FA accounts |
| `reqSymbolSamples` | `reqSymbolSamples(reqId: int, pattern: str)` | Get symbol samples matching pattern |

#### Order Management

| Method | Signature | Description |
|--------|-----------|-------------|
| `placeOrder` | `placeOrder(orderId: int, contract: Contract, order: Order)` | Place or modify an order. Uses orderId to identify. |
| `cancelOrder` | `cancelOrder(orderId: int, orderCancel: OrderCancel)` | Cancel an order |
| `reqOpenOrders` | `reqOpenOrders()` | Request all open orders |
| `reqAllOpenOrders` | `reqAllOpenOrders()` | Request all open orders (including from other sessions) |
| `reqAutoOpenOrders` | `reqAutoOpenOrders(bAutoBind: bool)` | Request TWS to associate manually entered orders with API client |
| `reqIds` | `reqIds(numIds: int)` | Request next valid order ID. Response via `nextValidId` callback. |
| `reqGlobalCancel` | `reqGlobalCancel()` | Cancel all open orders |
| `exerciseOptions` | `exerciseOptions(reqId: int, contract: Contract, exerciseAction: int, exerciseQuantity: int, account: str, override: int, manualOrderTime: str)` | Exercise or lapse options. `exerciseAction`: 1=Exercise, 2=Lapse. `override`: 1=Override, 0=No override. |

#### Account & Portfolio

| Method | Signature | Description |
|--------|-----------|-------------|
| `reqAccountSummary` | `reqAccountSummary(reqId: int, groupName: str, tags: str)` | Request account summary. `tags` is comma-separated (e.g., "NetLiquidation,AvailableFunds"). `groupName` = "All" for all accounts. |
| `cancelAccountSummary` | `cancelAccountSummary(reqId: int)` | Cancel account summary subscription |
| `reqAccountUpdates` | `reqAccountUpdates(subscribe: bool, acctCode: str)` | Subscribe to account value updates. Delivers via `updateAccountValue`, `updatePortfolio`, `updateAccountTime`. |
| `reqAccountUpdatesMulti` | `reqAccountUpdatesMulti(reqId: int, acctCode: str, modelName: str, ledgerAndNLV: bool)` | Multi-account updates |
| `cancelAccountUpdatesMulti` | `cancelAccountUpdatesMulti(reqId: int)` | Cancel multi-account updates |
| `reqPositions` | `reqPositions()` | Request all positions. Response via `position` callback. |
| `cancelPositions` | `cancelPositions()` | Cancel position subscription |
| `reqPositionsMulti` | `reqPositionsMulti(reqId: int, account: str, modelCode: str)` | Request positions for specific account/model |
| `cancelPositionsMulti` | `cancelPositionsMulti(reqId: int)` | Cancel multi-position subscription |
| `reqPnL` | `reqPnL(reqId: int, account: str, modelCode: str)` | Subscribe to account-level P&L. API v973.03+. Response ~1/sec. |
| `cancelPnL` | `cancelPnL(reqId: int)` | Cancel account P&L subscription |
| `reqPnLSingle` | `reqPnLSingle(reqId: int, account: str, modelCode: str, conId: int)` | Subscribe to single position P&L. API v973.03+. Response ~1/sec. |
| `cancelPnLSingle` | `cancelPnLSingle(reqId: int)` | Cancel single position P&L subscription |

#### Scanner

| Method | Signature | Description |
|--------|-----------|-------------|
| `reqScannerParameters` | `reqScannerParameters()` | Get available scanner parameters (XML) |
| `reqScannerSubscription` | `reqScannerSubscription(reqId: int, subscription: ScannerSubscription, scannerSubscriptionOptions: list, scannerSubscriptionFilterOptions: list)` | Subscribe to scanner results |
| `cancelScannerSubscription` | `cancelScannerSubscription(reqId: int)` | Cancel scanner subscription |

#### News

| Method | Signature | Description |
|--------|-----------|-------------|
| `reqNewsProviders` | `reqNewsProviders()` | Get available news providers |
| `reqNewsArticle` | `reqNewsArticle(reqId: int, providerCode: str, articleId: str, newsArticleOptions: list)` | Get news article content |
| `reqHistoricalNews` | `reqHistoricalNews(reqId: int, conId: int, providerCodes: str, startDateTime: str, endDateTime: str, totalResults: int, historicalNewsOptions: list)` | Request historical news |

#### Financial Advisor

| Method | Signature | Description |
|--------|-----------|-------------|
| `requestFA` | `requestFA(faDataType: int)` | Request FA allocation data. `faDataType`: 1=Aliases, 2=Groups, 3=Profiles. Response via `receiveFA` callback. |
| `replaceFA` | `replaceFA(reqId: int, faDataType: int, xml: str)` | Replace FA allocation configuration. Must pass FULL XML. Response via `replaceFAEnd` callback. |

#### Misc

| Method | Signature | Description |
|--------|-----------|-------------|
| `reqCurrentTime` | `reqCurrentTime()` | Request current server time |
| `reqManagedAccts` | `reqManagedAccts()` | Request list of managed accounts. Response via `managedAccounts` callback. |
| `queryDisplayGroups` | `queryDisplayGroups(reqId: int)` | Query display groups in TWS |
| `subscribeToGroupEvents` | `subscribeToGroupEvents(reqId: int, groupId: int)` | Subscribe to group events |
| `updateDisplayGroup` | `updateDisplayGroup(reqId: int, contractInfo: str)` | Update display group |
| `unsubscribeFromGroupEvents` | `unsubscribeFromGroupEvents(reqId: int)` | Unsubscribe from group events |

### 2.2 TWS API — EWrapper Callback Methods (Python ibapi)

All callbacks must be implemented in a class subclassing `wrapper.EWrapper`.

#### Connection & Session

| Callback | Signature | Description |
|----------|-----------|-------------|
| `connectAck` | `connectAck()` | Acknowledges successful connection |
| `nextValidId` | `nextValidId(orderId: int)` | Receives next valid order ID. Critical — must wait for this before placing orders. |
| `managedAccounts` | `managedAccounts(accountsList: str)` | Comma-separated list of account IDs |
| `error` | `error(reqId: int, errorCode: int, errorString: str)` | Error callback. Also called for informational messages. |
| `connectionClosed` | `connectionClosed()` | Called when connection is closed |

#### Market Data

| Callback | Signature | Description |
|----------|-----------|-------------|
| `tickPrice` | `tickPrice(reqId: int, tickType: int, price: float, attribs: TickAttrib)` | Receives price tick updates. `tickType` enum: 1=Bid, 2=Ask, 4=Last, 6=High, 7=Low, 9=Close, 14=Open, etc. |
| `tickSize` | `tickSize(reqId: int, tickType: int, size: Decimal)` | Receives size tick updates. `tickType` enum: 0=BidSize, 3=AskSize, 5=LastSize, 8=Volume, etc. |
| `tickString` | `tickString(reqId: int, tickType: int, value: str)` | String tick data. `tickType`: 45=LastTimestamp, 48=RTVolume, 59=Dividends, etc. |
| `tickGeneric` | `tickGeneric(reqId: int, tickType: int, value: float)` | Generic tick data. Covers values not in tickPrice/tickSize. |
| `tickEFP` | `tickEFP(reqId: int, tickType: int, basisPoints: float, formattedBasisPoints: str, totalDividends: float, holdDays: int, futureLastTradeDate: str, dividendImpact: float, dividendsToLastTradeDate: float)` | Exchange for Physical tick data |
| `tickSnapshotEnd` | `tickSnapshotEnd(reqId: int)` | Marks end of snapshot data |
| `tickOptionComputation` | `tickOptionComputation(reqId: int, tickType: int, tickAttrib: int, impliedVol: float, delta: float, gamma: float, vega: float, theta: float, undPrice: float)` | Option Greeks and implied volatility |
| `tickReqParams` | `tickReqParams(tickerId: int, minTick: float, bboExchange: str, snapshotPermissions: int)` | Tick request parameters |

#### Market Depth

| Callback | Signature | Description |
|----------|-----------|-------------|
| `updateMktDepth` | `updateMktDepth(reqId: int, position: int, operation: int, side: int, price: float, size: Decimal)` | Market depth update. `operation`: 0=Insert, 1=Update, 2=Delete. `side`: 0=Ask, 1=Bid. |
| `updateMktDepthL2` | `updateMktDepthL2(reqId: int, position: int, marketMaker: str, operation: int, side: int, price: float, size: Decimal, isSmartDepth: bool)` | Level 2 market depth update with market maker info. |
| `mktDepthExchanges` | `mktDepthExchanges(depthMktDataDescriptions: list)` | Response from reqMktDepthEx with available exchanges. |

#### Tick-by-Tick

| Callback | Signature | Description |
|----------|-----------|-------------|
| `tickByTickAllLast` | `tickByTickAllLast(reqId: int, tickType: int, time: int, price: float, size: Decimal, tickAttribLast: TickAttribLast, exchange: str, specialConditions: str)` | All last tick-by-tick data. `tickType`: 0=Last, 1=AllLast. |
| `tickByTickBidAsk` | `tickByTickBidAsk(reqId: int, time: int, bidPrice: float, askPrice: float, bidSize: Decimal, askSize: Decimal, tickAttribBidAsk: TickAttribBidAsk)` | Bid/Ask tick-by-tick data |
| `tickByTickMidPoint` | `tickByTickMidPoint(reqId: int, time: int, midPoint: float)` | Midpoint tick-by-tick data |

#### Historical Data

| Callback | Signature | Description |
|----------|-----------|-------------|
| `historicalData` | `historicalData(reqId: int, bar: BarData)` | Historical bar data. `BarData` has: date, open, high, low, close, volume, barCount, average. |
| `historicalDataEnd` | `historicalDataEnd(reqId: int, start: str, end: str)` | Marks end of historical data |
| `historicalDataUpdate` | `historicalDataUpdate(reqId: int, bar: BarData)` | Live update for historical data with `keepUpToDate=True` |
| `historicalTicks` | `historicalTicks(reqId: int, ticks: list, done: bool)` | Historical tick data (TRADES type) |
| `historicalTicksBidAsk` | `historicalTicksBidAsk(reqId: int, ticks: list, done: bool)` | Historical tick data (BID_ASK type) |
| `historicalTicksLast` | `historicalTicksLast(reqId: int, ticks: list, done: bool)` | Historical tick data (LAST type) |
| `headTimestamp` | `headTimestamp(reqId: int, headTimestamp: str)` | Earliest available data timestamp |
| `histogramData` | `histogramData(reqId: int, items: list)` | Price histogram data |

#### Real-time Bars

| Callback | Signature | Description |
|----------|-----------|-------------|
| `realtimeBar` | `realtimeBar(reqId: int, time: int, open: float, high: float, low: float, close: float, volume: Decimal, wap: Decimal, count: int)` | 5-second real-time bar data |

#### Contract Details

| Callback | Signature | Description |
|----------|-----------|-------------|
| `contractDetails` | `contractDetails(reqId: int, contractDetails: ContractDetails)` | Full contract information |
| `contractDetailsEnd` | `contractDetailsEnd(reqId: int)` | End of contract details |
| `bondContractDetails` | `bondContractDetails(reqId: int, contractDetails: ContractDetails)` | Bond-specific contract details |
| `securityDefinitionOptionParameter` | `securityDefinitionOptionParameter(reqId: int, exchange: str, underlyingConId: int, tradingClass: str, multiplier: str, expirations: set, strikes: set)` | Option chain parameters from reqSecDefOptParams |
| `securityDefinitionOptionParameterEnd` | `securityDefinitionOptionParameterEnd(reqId: int)` | End of option parameters |
| `symbolSamples` | `symbolSamples(reqId: int, contractDescriptions: list)` | Symbol search results |

#### Order Management

| Callback | Signature | Description |
|----------|-----------|-------------|
| `openOrder` | `openOrder(orderId: int, contract: Contract, order: Order, orderState: OrderState)` | Open order details. Called on connect (if "Download open orders on connection" checked) and after placeOrder. |
| `openOrderEnd` | `openOrderEnd()` | End of open orders |
| `orderStatus` | `orderStatus(orderId: int, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float)` | Order status updates. `status` values: PendingSubmit, PendingCancel, PreSubmitted, Submitted, ApiCancelled, Cancelled, Filled, Inactive, PendingReplace. |
| `execDetails` | `execDetails(reqId: int, contract: Contract, execution: Execution)` | Execution details |
| `execDetailsEnd` | `execDetailsEnd(reqId: int)` | End of execution details |
| `commissionReport` | `commissionReport(commissionReport: CommissionReport)` | Commission report for execution |
| `orderBound` | `orderBound(orderId: int, apiClientId: int, apiOrderId: int)` | Order binding confirmation |

#### Account & Portfolio

| Callback | Signature | Description |
|----------|-----------|-------------|
| `updateAccountValue` | `updateAccountValue(key: str, val: str, currency: str, accountName: str)` | Account value updates (e.g., NetLiquidation, AvailableFunds, UnrealizedPnL) |
| `updatePortfolio` | `updatePortfolio(contract: Contract, position: Decimal, marketPrice: float, marketValue: float, averageCost: float, unrealizedPNL: float, realizedPNL: float, accountName: str)` | Portfolio position updates |
| `updateAccountTime` | `updateAccountTime(timeStamp: str)` | Account update timestamp |
| `accountDownloadEnd` | `accountDownloadEnd(accountName: str)` | End of account download |
| `accountSummary` | `accountSummary(reqId: int, account: str, tag: str, value: str, currency: str)` | Account summary response |
| `accountSummaryEnd` | `accountSummaryEnd(reqId: int)` | End of account summary |
| `position` | `position(account: str, contract: Contract, position: Decimal, avgCost: float)` | Position data |
| `positionEnd` | `positionEnd()` | End of position data |
| `positionMulti` | `positionMulti(reqId: int, account: str, modelCode: str, contract: Contract, pos: Decimal, avgCost: float)` | Multi-account position |
| `positionMultiEnd` | `positionMultiEnd(reqId: int)` | End of multi-account positions |
| `pnl` | `pnl(reqId: int, dailyPnL: float, unrealizedPnL: float, realizedPnL: float)` | Account-level P&L. ~1 update/sec. API v973.03+. |
| `pnlSingle` | `pnlSingle(reqId: int, pos: Decimal, dailyPnL: float, unrealizedPnL: float, realizedPnL: float, value: float)` | Single position P&L. ~1 update/sec. API v973.05+ adds realizedPnL. |
| `accountUpdateMulti` | `accountUpdateMulti(reqId: int, account: str, modelCode: str, key: str, value: str, currency: str)` | Multi-account value updates |

#### Scanner

| Callback | Signature | Description |
|----------|-----------|-------------|
| `scannerParameters` | `scannerParameters(xml: str)` | Available scanner parameters (XML) |
| `scannerData` | `scannerData(reqId: int, rank: int, contractDetails: ContractDetails, distance: str, benchmark: str, projection: str, legsStr: str)` | Scanner result |
| `scannerDataEnd` | `scannerDataEnd(reqId: int)` | End of scanner data |

#### News

| Callback | Signature | Description |
|----------|-----------|-------------|
| `newsProviders` | `newsProviders(newsProviders: list)` | Available news providers |
| `newsArticle` | `newsArticle(reqId: int, articleType: int, articleText: str)` | News article content. `articleType`: 0=plain text, 1=HTML, 2=Markdown. |
| `historicalNews` | `historicalNews(reqId: int, time: str, providerCode: str, articleId: str, headline: str)` | Historical news headlines |
| `historicalNewsEnd` | `historicalNewsEnd(reqId: int, hasMore: bool)` | End of historical news |
| `updateNewsBulletin` | `updateNewsBulletin(msgId: int, msgType: int, message: str, origExchange: str)` | News bulletin updates |

#### Financial Advisor

| Callback | Signature | Description |
|----------|-----------|-------------|
| `receiveFA` | `receiveFA(faDataType: int, faXmlData: str)` | FA allocation data response |
| `replaceFAEnd` | `replaceFAEnd(reqId: int, text: str)` | Confirmation of FA config update |

#### Misc

| Callback | Signature | Description |
|----------|-----------|-------------|
| `currentTime` | `currentTime(time: int)` | Server time (Unix timestamp) |
| `displayGroupList` | `displayGroupList(reqId: int, groups: str)` | Display group list |
| `displayGroupUpdated` | `displayGroupUpdated(reqId: int, contractInfo: str)` | Display group update |
| `verifyMessageApi` | `verifyMessageApi(apiData: str)` | Verification message |
| `verifyCompleted` | `verifyCompleted(isSuccessful: bool, errorText: str)` | Verification completed |
| `verifyAndAuthMessageApi` | `verifyAndAuthMessageApi(apiData: str, xyzChallenge: str)` | Auth verification message |
| `verifyAndAuthCompleted` | `verifyAndAuthCompleted(isSuccessful: bool, errorText: str)` | Auth verification completed |
| `softDollarTiers` | `softDollarTiers(reqId: int, tiers: list)` | Soft dollar tier information |
| `familyCodes` | `familyCodes(familyCodes: list)` | Family codes |
| `smartComponents` | `smartComponents(reqId: int, smartComponents: list)` | Smart components |
| `rerouteCfmData` | `rerouteCfmData(reqId: int, conId: int, exchange: str)` | Reroute confirmation |
| `marketRule` | `marketRule(marketRuleId: int, priceIncrements: list)` | Market rule price increments |
| `pnl` | `pnl(reqId: int, dailyPnL: float, unrealizedPnL: float, realizedPnL: float)` | Account P&L update |
| `pnlSingle` | `pnlSingle(reqId: int, pos: Decimal, dailyPnL: float, unrealizedPnL: float, realizedPnL: float, value: float)` | Single position P&L |

### 2.3 Web API (REST) — Key Endpoints

The IBKR Campus Web API docs are JavaScript-rendered SPAs. The key endpoint groups documented are:

#### Authentication
- `POST /iserver/auth/status` — Check authentication status
- `POST /iserver/auth/ssodh/init` — Initialize SSO
- `POST /iserver/auth/ssodh/login` — SSO login
- `POST /iserver/auth/logout` — Logout

#### Market Data
- `GET /iserver/marketdata/snapshot` — Get market data snapshot for conids
- `GET /iserver/marketdata/history` — Historical price data
- `GET /iserver/marketdata/urgapt` — Unsubscribe from market data

#### Orders
- `POST /iserver/account/{accountId}/orders` — Place order
- `DELETE /iserver/account/{accountId}/order/{orderId}` — Cancel order
- `POST /iserver/account/{accountId}/orders/whatif` — Order preview (margin impact)
- `GET /iserver/account/orders` — Get open orders
- `POST /iserver/reply/{replyId}` — Reply to order confirmation

#### Account
- `GET /portfolio/{accountId}/positions` — Get positions
- `GET /portfolio/{accountId}/positions/{conId}` — Get specific position
- `GET /iserver/account/summary` — Account summary
- `GET /iserver/account/pnl/partitioned` — P&L data

#### Contracts
- `GET /iserver/contract/{conId}/info` — Contract info by conId
- `GET /iserver/secdef/search` — Search for securities
- `GET /trsrv/allConids` — Get all contract IDs
- `GET /trsrv/stocks` — Stock contract search
- `GET /trsrv/futures` — Futures contract search

#### Scanner
- `POST /iserver/scanner/run` — Run scanner

---

## 3. Data Structures

### 3.1 Contract Object

```python
Contract(
    conId: int,              # Unique contract identifier (assigned by IB)
    symbol: str,             # Ticker symbol (e.g., "AAPL", "EUR")
    secType: str,            # Security type: STK, OPT, FUT, FOREX, BOND, IND, CFD, CRYPTO, FUND, etc.
    lastTradeDateOrContractMonth: str,  # Expiry for options/futures (yyyyMMdd or yyyyMMddHHmmss)
    strike: float,           # Options: strike price
    right: str,              # Options: "PUT" or "CALL"
    multiplier: str,         # Options/Futures: contract multiplier (e.g., "100")
    exchange: str,           # Exchange (e.g., "SMART", "NYSE", "CBOE")
    primaryExchange: str,    # Primary exchange for disambiguation
    currency: str,           # Currency (e.g., "USD", "EUR")
    localSymbol: str,        # Local exchange symbol (varies by exchange)
    tradingClass: str,       # Trading class (e.g., "SPY" for SPY options)
    includeExpired: bool,    # Include expired contracts in search
    secIdType: str,          # Security ID type: "CUSIP", "SEDOL", "ISIN", "RIC"
    secId: str,              # Security ID value
    description: str,        # Contract description
    issuerId: str,           # Issuer ID (for bonds)
    comboLegsDescrip: str,   # Combo legs description
    comboLegs: list,         # List of ComboLeg objects
    deltaNeutralContract: DeltaNeutralContract,  # Delta neutral contract for combos
)
```

#### secType Values
| Value | Security Type |
|-------|--------------|
| `STK` | Stock / Equity |
| `OPT` | Option |
| `FUT` | Future |
| `FOREX` | Forex (currency pair) |
| `BOND` | Bond |
| `IND` | Index |
| `CFD` | Contract for Difference |
| `CRYPTO` | Cryptocurrency |
| `FUND` | Mutual Fund / ETF |
| `FOP` | Futures Option |
| `WAR` | Warrant |
| `IOPT` | Index Option |
| `BAG` | Combo / Spread |
| `NEWS` | News |

### 3.2 ContractDetails Object

```python
ContractDetails(
    contract: Contract,
    marketName: str,
    minTick: float,
    orderTypes: str,          # Comma-separated list of valid order types
    validExchanges: str,      # Comma-separated list of valid exchanges
    priceMagnifier: int,      # Price multiplier (e.g., 100 for prices in cents)
    underConId: int,          # ConId of underlying
    longName: str,            # Full company name
    contractMonth: str,       # Contract month (futures)
    industry: str,            # Industry category
    category: str,            # Category
    subcategory: str,         # Subcategory
    timeZoneId: str,          # Time zone ID
    tradingHours: str,        # Trading hours (semicolon-separated sessions)
    liquidHours: str,         # Liquid trading hours
    evRule: str,              # EV rule
    evMultiplier: int,        # EV multiplier
    mdSizeMultiplier: int,    # Market data size multiplier
    aggGroup: int,            # Aggregation group
    underSymbol: str,         # Underlying symbol
    underSecType: str,        # Underlying security type
    marketRuleIds: str,       # Market rule IDs
    secIdList: list,          # Security ID list
    realExpirationDate: str,  # Real expiration date
    lastTradeTime: str,       # Last trade time
    stockType: str,           # Stock type (e.g., "COMMON", "ADR")
    # Bond-specific:
    cusip: str,
    ratings: str,
    descAppend: str,
    bondType: str,
    couponType: str,
    callable: bool,
    putable: bool,
    coupon: float,
    convertible: bool,
    maturity: str,
    issueDate: str,
    nextOptionDate: str,
    nextOptionType: str,
    nextOptionPartial: bool,
    notes: str,
)
```

### 3.3 Order Object

```python
Order(
    orderId: int,              # Order ID (must be >= nextValidId)
    clientOrderId: int,        # Client-assigned order ID
    permId: int,               # Permanent order ID (assigned by IB)
    action: str,               # "BUY" or "SELL"
    totalQuantity: Decimal,    # Total order quantity
    orderType: str,            # Order type (see Order Types section)
    lmtPrice: float,           # Limit price
    auxPrice: float,           # Stop/auxiliary price
    tif: str,                  # Time in force: "DAY", "GTC", "IOC", "OPG", "GTD", "GAT"
    activeStartTime: str,      # Active period start (for GTC/GTD)
    activeStopTime: str,       # Active period stop
    ocaGroup: str,             # OCA (One Cancels All) group name
    ocaType: int,              # OCA type: 1=CANCEL_WITH_BLOCK, 2=REDUCE_WITH_BLOCK, 3=REDUCE_NON_BLOCK
    orderRef: str,             # User-defined order reference string
    transmit: bool,            # Auto-transmit (default True). Set False for bracket order children.
    parentId: int,             # Parent order ID (for bracket/attached orders)
    blockOrder: bool,          # Block order
    sweepToFill: bool,         # Sweep to fill
    displaySize: int,          # Display size (iceberg orders)
    triggerMethod: int,        # Trigger method: 0=Default, 1=DoubleBidAsk, 2=Last, 3=DoubleLast, 4=BidAsk, 7=LastOrBidAsk, 8=MidPoint
    outsideRth: bool,          # Allow outside regular trading hours
    hidden: bool,              # Hidden order
    goodAfterTime: str,        # Good after time (yyyyMMdd HH:mm:ss)
    goodTillDate: str,         # Good till date (yyyyMMdd HH:mm:ss)
    rule80A: str,              # Rule 80A (institutional)
    allOrNone: bool,           # All or none
    minQty: int,               # Minimum fill quantity
    percentOffset: float,      # Percent offset (REL orders)
    overridePercentageConstraints: bool,
    trailStopPrice: float,     # Trailing stop price
    trailingPercent: float,    # Trailing stop percentage
    
    # FA allocation
    faGroup: str,              # FA group name
    faProfile: str,            # FA profile name
    faMethod: str,             # FA method: "EqualQuantity", "NetLiq", "AvailableEquity", "PctChange", "Percentages", "FinancialRatios", "Shares"
    faPercentage: str,         # FA percentage
    
    # Short sale
    designatedLocation: str,   # Short sale location
    openClose: str,            # "O"=Open, "C"=Close
    origin: int,               # 0=Customer, 1=Firm
    shortSaleSlot: int,        # Short sale slot
    exemptCode: int,           # Exempt code
    
    # Discretionary
    discretionaryAmt: float,   # Discretionary amount
    
    # E-Trade only
    eTradeOnly: bool,
    firmQuoteOnly: bool,
    nbboPriceCap: float,
    
    # Auction
    optOutSmartRouting: bool,
    auctionStrategy: int,      # 1=UNSET, 2=PRIMARY, 3=SIMULATION, 4=AUCTION
    
    # Algo orders
    algoStrategy: str,         # Algorithm strategy name
    algoParams: list,          # List of TagValue pairs for algo parameters
    
    # Smart routing
    smartComboRoutingParams: list,  # Smart combo routing params
    
    # Scale orders
    scaleInitLevelSize: int,
    scaleSubsLevelSize: int,
    scalePriceIncrement: float,
    scalePriceAdjustValue: float,
    scalePriceAdjustInterval: int,
    scaleProfitOffset: float,
    scaleAutoReset: bool,
    scaleInitPosition: int,
    scaleInitFillQty: int,
    scaleRandomPercent: bool,
    scaleTable: str,
    
    # Hedge
    hedgeType: str,            # "D"=Delta, "B"=Beta, "F"=Fx, "P"=Pair
    hedgeParam: str,           # Hedge parameter value
    
    # Account
    account: str,              # Account ID
    settlingFirm: str,         # Settling firm
    clearingAccount: str,      # Clearing account
    clearingIntent: str,       # Clearing intent: "IB", "Away", "PTA"
    
    # Combo
    algoId: str,
    whatIf: bool,              # True for margin preview (what-if)
    notHeld: bool,             # Not held order
    
    # Solicited
    solicited: bool,
    
    # Model code
    modelCode: str,
    
    # Sec type
    secType: str,
    exchange: str,
    
    # Randomize
    randomizeSize: bool,
    randomizePrice: bool,
    
    # Conditions
    conditions: list,          # List of OrderCondition objects
    conditionsCancelOrder: bool,
    conditionsIgnoreRth: bool,
    
    # Soft dollar
    softDollarTierName: str,
    softDollarTierValue: str,
    
    # Cash qty
    cashQty: float,
    mifid2DecisionMaker: str,
    mifid2DecisionAlgo: str,
    mifid2ExecutionTrader: str,
    mifid2ExecutionAlgo: str,
    
    # PEG
    referencePriceType: int,   # 1=Midpoint, 2=Primary
    
    # Manual order time
    manualOrderTime: str,
)
```

### 3.4 Execution Object

```python
Execution(
    execId: str,               # Unique execution ID
    time: str,                 # Execution time
    acctNumber: str,           # Account number
    exchange: str,             # Exchange
    side: str,                 # "BOT" or "SLD"
    shares: Decimal,           # Number of shares filled
    price: float,              # Fill price
    permId: int,               # Permanent ID
    clientId: int,             # Client ID
    orderId: int,              # Order ID
    liquidation: int,          # Liquidation flag
    cumQty: Decimal,           # Cumulative quantity
    avgPrice: float,           # Average fill price
    orderRef: str,             # Order reference
    evRule: str,               # EV rule
    evMultiplier: int,         # EV multiplier
    modelCode: str,            # Model code
    lastLiquidity: int,        # Last liquidity: 1=Added, 2=Removed, 3=Rounded, 4=Unknown
    pendingPriceRevision: bool,
)
```

### 3.5 CommissionReport Object

```python
CommissionReport(
    execId: str,               # Execution ID
    commission: float,         # Commission amount
    currency: str,             # Commission currency
    realizedPNL: float,        # Realized P&L
    yield_: float,             # Yield
    yieldRedemptionDate: int,  # Yield redemption date (yyyyMMdd)
)
```

### 3.6 BarData Object

```python
BarData(
    date: str,                 # Date/time (format depends on formatDate param)
    open: float,
    high: float,
    low: float,
    close: float,
    volume: Decimal,
    barCount: int,             # Number of trades in bar
    average: float,            # VWAP for the bar
)
```

### 3.7 OrderState Object

```python
OrderState(
    status: str,               # Order status
    initMarginBefore: str,     # Initial margin before order
    maintMarginBefore: str,    # Maintenance margin before order
    equityWithLoanBefore: str, # Equity with loan before order
    initMarginChange: str,     # Initial margin change
    maintMarginChange: str,    # Maintenance margin change
    equityWithLoanChange: str, # Equity with loan change
    initMarginAfter: str,      # Initial margin after order
    maintMarginAfter: str,     # Maintenance margin after order
    equityWithLoanAfter: str,  # Equity with loan after order
)
```

### 3.8 TickType Enum Values

#### Price Tick Types (tickPrice callback)
| Value | Name | Description |
|-------|------|-------------|
| 0 | BID_SIZE | Bid size |
| 1 | BID | Bid price |
| 2 | ASK | Ask price |
| 3 | ASK_SIZE | Ask size |
| 4 | LAST | Last price |
| 5 | LAST_SIZE | Last size |
| 6 | HIGH | Day high |
| 7 | LOW | Day low |
| 8 | VOLUME | Volume |
| 9 | CLOSE | Previous close |
| 10 | BID_OPTION | Bid implied volatility (options) |
| 11 | ASK_OPTION | Ask implied volatility (options) |
| 12 | LAST_OPTION | Last implied volatility (options) |
| 13 | MODEL_OPTION | Model option price |
| 14 | OPEN | Day open |
| 15 | LOW_13_WEEK | 13-week low |
| 16 | HIGH_13_WEEK | 13-week high |
| 17 | LOW_26_WEEK | 26-week low |
| 18 | HIGH_26_WEEK | 26-week high |
| 19 | LOW_52_WEEK | 52-week low |
| 20 | HIGH_52_WEEK | 52-week high |
| 21 | AVG_VOLUME | 30-day average volume |
| 22 | OPEN_INTEREST | Open interest |
| 23 | OPTION_HISTORICAL_VOL | Option historical volatility |
| 24 | OPTION_IMPLIED_VOL | Option implied volatility |
| 25 | OPTION_CALL_OPEN_INTEREST | Call open interest |
| 26 | OPTION_PUT_OPEN_INTEREST | Put open interest |
| 27 | OPTION_CALL_VOLUME | Call volume |
| 28 | OPTION_PUT_VOLUME | Put volume |
| 29 | INDEX_FUTURE_PREMIUM | Index future premium |
| 30 | BID_EXCH | Bid exchange |
| 31 | ASK_EXCH | Ask exchange |
| 32 | AUCTION_VOLUME | Auction volume |
| 33 | AUCTION_PRICE | Auction price |
| 34 | AUCTION_IMBALANCE | Auction imbalance |
| 35 | MARK_PRICE | Mark price |
| 36 | BID_EFP_COMPUTATION | Bid EFP computation |
| 37 | ASK_EFP_COMPUTATION | Ask EFP computation |
| 38 | LAST_EFP_COMPUTATION | Last EFP computation |
| 39 | OPEN_EFP_COMPUTATION | Open EFP computation |
| 40 | HIGH_EFP_COMPUTATION | High EFP computation |
| 41 | LOW_EFP_COMPUTATION | Low EFP computation |
| 42 | CLOSE_EFP_COMPUTATION | Close EFP computation |
| 43 | LAST_TIMESTAMP | Last trade timestamp |
| 44 | SHORTABLE | Shortable shares (float: -1=unknown, 0=not shortable, >0=shares available) |
| 45 | FUNDAMENTAL_RATIOS | Fundamental ratios |
| 46 | RT_VOLUME | Real-time volume (string format: volume;VWAP;singleTradeFlag) |
| 47 | HALTED | Halted status (0=not halted, 1=halted, 2=halted by volatility) |
| 48 | BID_YIELD | Bid yield |
| 49 | ASK_YIELD | Ask yield |
| 50 | LAST_YIELD | Last yield |
| 51 | CUST_OPTION_COMPUTATION | Custom option computation |
| 52 | TRADE_COUNT | Trade count |
| 53 | TRADE_COUNT_PER_MINUTE | Trade count per minute |
| 54 | VOLUME_RATE | Volume rate |
| 55 | LAST_RTH_TRADE | Last regular trading hours trade |
| 56 | RT_HISTORICAL_VOL | Real-time historical volatility |
| 57 | IB_DIVIDENDS | IB dividend information |
| 58 | BOND_FACTOR_MULTIPLIER | Bond factor multiplier |
| 59 | REGULATORY_IMBALANCE | Regulatory imbalance |
| 60 | NEWS_TICK | News tick |
| 61 | SHORT_TERM_VOLUME_3MIN | 3-minute volume |
| 62 | SHORT_TERM_VOLUME_5MIN | 5-minute volume |
| 63 | SHORT_TERM_VOLUME_10MIN | 10-minute volume |
| 64 | DELAYED_BID | Delayed bid |
| 65 | DELAYED_ASK | Delayed ask |
| 66 | DELAYED_LAST | Delayed last |
| 67 | DELAYED_BID_SIZE | Delayed bid size |
| 68 | DELAYED_ASK_SIZE | Delayed ask size |
| 69 | DELAYED_LAST_SIZE | Delayed last size |
| 70 | DELAYED_HIGH | Delayed high |
| 71 | DELAYED_LOW | Delayed low |
| 72 | DELAYED_VOLUME | Delayed volume |
| 73 | DELAYED_CLOSE | Delayed close |
| 74 | DELAYED_OPEN | Delayed open |
| 75 | RT_TRD_VOLUME | RT trade volume |
| 76 | CREDITMAN_MARK_PRICE | Creditman mark price |
| 77 | CREDITMAN_SLOW_MARK_PRICE | Creditman slow mark price |
| 78 | DELAYED_BID_OPTION | Delayed bid option computation |
| 79 | DELAYED_ASK_OPTION | Delayed ask option computation |
| 80 | DELAYED_LAST_OPTION | Delayed last option computation |
| 81 | DELAYED_MODEL_OPTION | Delayed model option computation |
| 82 | LAST_EXCH | Last exchange |
| 83 | LAST_REG_TIME | Last regular trading time |
| 84 | FUTURES_OPEN_INTEREST | Futures open interest |
| 85 | AVG_OPT_VOLUME | Average option volume |
| 86 | DELAYED_LAST_TIMESTAMP | Delayed last timestamp |
| 87 | SHORTABLE_SHARES | Shortable shares |
| 88 | DELAYED_HALTED | Delayed halted |
| 89 | REUTERS_2_MUTUAL_FUNDS | Reuters mutual fund data |
| 90 | ETF_NAV_CLOSE | ETF NAV close |
| 91 | ETF_NAV_PRIOR_CLOSE | ETF NAV prior close |
| 92 | ETF_NAV_LAST | ETF NAV last |
| 93 | ETF_FROZEN_NAV_LAST | ETF frozen NAV last |
| 94 | ETF_NAV_HIGH | ETF NAV high |
| 95 | ETF_NAV_LOW | ETF NAV low |

### 3.9 OrderType Values

| Type | Description |
|------|-------------|
| `MKT` | Market order |
| `LMT` | Limit order |
| `STP` | Stop order |
| `STP_LMT` | Stop-limit order |
| `MIDPRICE` | Mid-price order |
| `MOC` | Market-on-Close |
| `LOC` | Limit-on-Close |
| `MOO` | Market-on-Open |
| `LOO` | Limit-on-Open |
| `MKT PRT` | Market if touched (participate) |
| `REL` | Relative/Pegged to primary |
| `TRAIL` | Trailing stop |
| `TRAIL LIMIT` | Trailing stop limit |
| `VWAP` | Volume-weighted average price |
| `MTL` | Market-to-Limit |
| `PEG BST` | Peg to best bid |
| `PEG BBO` | Peg to best bid/offer |
| `PEG MID` | Peg to midpoint |
| `PEG MKT` | Peg to market |
| `PEG PRIM` | Peg to primary |
| `PEG STK` | Peg to stock |
| `SNAP MKT` | Snapshot market |
| `SNAP MID` | Snapshot midpoint |
| `SNAP PRIM` | Snapshot primary |
| `LIT` | Limit if Touched |
| `MIT` | Market if Touched |
| `ADJUST` | Adjust order |
| `ALGO` | Algorithmic order |
| `BID` | Bid |
| `ASK` | Ask |
| `CRON` | Conditional order |
| `SWEEP` | Sweep order |
| `STATE` | State order |

### 3.10 OrderStatus Values

| Status | Description |
|--------|-------------|
| `PendingSubmit` | Order sent but not yet acknowledged |
| `PendingCancel` | Cancel request sent but not confirmed |
| `PreSubmitted` | Simulated order transmitted but not yet acknowledged |
| `Submitted` | Order acknowledged and working |
| `ApiCancelled` | Cancelled via API |
| `Cancelled` | Order cancelled |
| `Filled` | Order completely filled |
| `Inactive` | Order not working (e.g., invalid) |
| `PendingReplace` | Replace request sent but not confirmed |

### 3.11 Time-in-Force (TIF) Values

| Value | Description |
|-------|-------------|
| `DAY` | Good for the day |
| `GTC` | Good till cancelled |
| `IOC` | Immediate or cancel |
| `OPG` | At the open |
| `GTD` | Good till date |
| `GAT` | Good after time |

---

## 4. Market Data System

### 4.1 reqMktData — Streaming Market Data

```python
self.reqMktData(reqId, contract, genericTickList, snapshot, regulatorySnapshot, mktDataOptions)
```

**Parameters:**
- `reqId`: Unique integer identifier for this request
- `contract`: Contract object (must be fully resolved)
- `genericTickList`: Comma-separated list of generic tick types. Empty string for standard ticks (Bid, Ask, Last, Volume, etc.)
- `snapshot`: `True` for one-time snapshot (returns current values, no streaming). `False` for streaming.
- `regulatorySnapshot`: `True` for regulatory snapshot (costs money)
- `mktDataOptions`: `[]` (reserved, pass empty list)

**Generic Tick Types (for genericTickList parameter):**
| Value | Description |
|-------|-------------|
| `100` | Option Volume (call/put) |
| `101` | Option Open Interest (call/put) |
| `104` | Historical Volatility (30/60/90 day) |
| `105` | Average Option Volume |
| `106` | Option Implied Volatility |
| `162` | Index Future Premium |
| `165` | Miscellaneous Stats |
| `221` | Mark Price |
| `225` | Auction Values |
| `233` | RT Trade Volume |
| `236` | Shortable |
| `258` | Fundamental Ratios |
| `293` | Trade Count |
| `294` | Trade Count per Minute |
| `295` | Volume Rate |
| `311` | Short-Term Volume (3/5/10 min) |
| `375` | RT Historical Volatility |
| `411` | ETF NAV Data |
| `456` | Last RTH Trade |
| `588` | Reuters Mutual Fund |
| `595` | Bond Factor Multiplier |
| `596` | Regulatory Imbalance |
| `597` | News |
| `598` | Shortable Shares |

**Callbacks:**
- `tickPrice(reqId, tickType, price, attribs)` — Price updates
- `tickSize(reqId, tickType, size)` — Size updates
- `tickString(reqId, tickType, value)` — String data (timestamps, RT volume)
- `tickGeneric(reqId, tickType, value)` — Generic numeric data
- `tickEFP(reqId, tickType, ...)` — EFP data
- `tickOptionComputation(reqId, tickType, ...)` — Option Greeks

**TickAttrib fields:**
- `canAutoExecute`: Whether the tick can trigger auto-execution
- `pastLimit`: Whether tick price is past limit
- `preOpen`: Whether tick is from pre-open

### 4.2 reqTickByTickData — Tick-by-Tick Data

```python
self.reqTickByTickData(reqId, contract, tickType, numberOfTicks, ignoreSize)
```

**Parameters:**
- `tickType`: `"Last"`, `"BidAsk"`, or `"MidPoint"`
- `numberOfTicks`: Number of ticks to return. `0` = all available ticks.
- `ignoreSize`: Ignore bid/ask sizes in filtering

**Available from**: API v973.04+ / TWS v969+

**Callbacks:**
- `tickByTickAllLast(reqId, tickType, time, price, size, tickAttribLast, exchange, specialConditions)` — For "Last" and "AllLast"
- `tickByTickBidAsk(reqId, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk)` — For "BidAsk"
- `tickByTickMidPoint(reqId, time, midPoint)` — For "MidPoint"

### 4.3 reqRealTimeBars — 5-Second Bars

```python
self.reqRealTimeBars(reqId, contract, barSize, whatToShow, useRTH, realTimeBarsOptions)
```

**Parameters:**
- `barSize`: Must be `5` (5 seconds is the only supported bar size)
- `whatToShow`: `"TRADES"`, `"MIDPOINT"`, `"BID"`, `"ASK"`
- `useRTH`: `True` = regular trading hours only, `False` = all hours
- `realTimeBarsOptions`: `[]` (reserved)

**Callback:**
```python
realtimeBar(reqId, time, open, high, low, close, volume, wap, count)
```

### 4.4 reqHistoricalData — Historical Bars

```python
self.reqHistoricalData(reqId, contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate, keepUpToDate, chartOptions)
```

**Parameters:**
- `endDateTime`: Empty string = current time. Format: `"yyyyMMdd HH:mm:ss"` or `""` for now.
- `durationStr`: Duration of data. Examples: `"60 S"`, `"300 S"`, `"1 D"`, `"5 D"`, `"1 W"`, `"1 M"`, `"1 Y"`, `"10 Y"`
- `barSizeSetting`: Bar size. Valid values:
  - `"1 secs"`, `"5 secs"`, `"10 secs"`, `"15 secs"`, `"30 secs"`
  - `"1 min"`, `"2 mins"`, `"3 mins"`, `"5 mins"`, `"10 mins"`, `"15 mins"`, `"20 mins"`, `"30 mins"`
  - `"1 hour"`, `"2 hours"`, `"3 hours"`, `"4 hours"`, `"8 hours"`
  - `"1 day"`, `"1 week"`, `"1 month"`
- `whatToShow`: Data type. Values:
  - `"TRADES"`, `"MIDPOINT"`, `"BID"`, `"ASK"`
  - `"BID_ASK"` (bid/ask bars — only for specific bar sizes)
  - `"HISTORICAL_VOLATILITY"`, `"OPTION_IMPLIED_VOLATILITY"`
  - `"AGGTRADES"` (aggregated trades)
- `useRTH`: `True` = regular trading hours only
- `formatDate`: `1` = `"yyyyMMdd HH:mm:ss"`, `2` = Unix timestamp (integer)
- `keepUpToDate`: `True` = receive live updates via `historicalDataUpdate` callback

**Duration/Bar Size Constraints:**
- Not all combinations are valid. Smaller bar sizes have shorter maximum durations.
- 1-second bars: max ~30 minutes
- 5-second bars: max ~8 hours
- 1-minute bars: max ~10 days
- 5-minute bars: max ~10 days
- 1-hour bars: max ~1 year
- 1-day bars: max ~10 years

**Callbacks:**
- `historicalData(reqId, bar)` — Each bar
- `historicalDataEnd(reqId, start, end)` — End of data
- `historicalDataUpdate(reqId, bar)` — Live update (if `keepUpToDate=True`)

### 4.5 reqHeadTimestamp

```python
self.reqHeadTimestamp(reqId, contract, whatToShow, useRTH, formatDate)
```

Returns the earliest available data timestamp. Useful for checking how far back data goes.

**Callback:** `headTimestamp(reqId, headTimestamp)`

### 4.6 reqHistogramData

```python
self.reqHistogramData(reqId, contract, useRTH, timePeriod)
```

Returns price histogram showing distribution of trading volume at price levels.

**Callback:** `histogramData(reqId, items)` — list of HistogramData objects (price, count)

### 4.7 reqHistoricalTicks

```python
self.reqHistoricalTicks(reqId, contract, startDateTime, endDateTime, numberOfTicks, whatToShow, useRTH, ignoreSize, miscOptions)
```

**Parameters:**
- `whatToShow`: `"TRADES"`, `"BID_ASK"`, `"MIDPOINT"`
- `numberOfTicks`: Max number of ticks (0 = no limit within time range)
- `startDateTime`/`endDateTime`: `"yyyyMMdd HH:mm:ss"` format

**Callbacks:**
- `historicalTicks(reqId, ticks, done)` — TRADES ticks
- `historicalTicksBidAsk(reqId, ticks, done)` — BID_ASK ticks
- `historicalTicksLast(reqId, ticks, done)` — LAST ticks

### 4.8 Market Data Types

Set via `reqMarketDataType(marketDataType)`:

| Value | Type | Description |
|-------|------|-------------|
| 1 | Live | Real-time streaming data (requires paid subscriptions) |
| 2 | Frozen | Last known values when market is closed |
| 3 | Delayed | 10-20 minute delayed data (free) |
| 4 | DelayedFrozen | Delayed data that freezes when market closes |

### 4.9 Market Depth (Order Book)

```python
self.reqMktDepth(reqId, contract, numRows, isSmartDepth, mktDepthOptions)
```

**Parameters:**
- `numRows`: Number of depth levels (default max 5)
- `isSmartDepth`: `True` = aggregate across exchanges via SMART routing

**Callbacks:**
- `updateMktDepth(reqId, position, operation, side, price, size)`
- `updateMktDepthL2(reqId, position, marketMaker, operation, side, price, size, isSmartDepth)`

**Operation values:** 0=Insert, 1=Update, 2=Delete  
**Side values:** 0=Ask, 1=Bid

### 4.10 Pacing Violations

Historical data requests are subject to pacing restrictions:
- **No more than 60 requests in any 10-minute period** (for the same contract)
- **Cannot make identical requests within 15 seconds**
- Small bar sizes (<= 5 seconds) are subject to more restrictive pacing
- Pacing is per connection, not per clientId
- Multiple requests for different contracts can be made in parallel
- **How to avoid**: Use `keepUpToDate=True` for live bars instead of re-requesting; batch data downloads with appropriate delays; use different reqIds for different contracts.

---

## 5. Contract System

### 5.1 Contract Resolution Workflow

1. Create a `Contract` object with known fields
2. Call `reqContractDetails(reqId, contract)` 
3. Receive `contractDetails(reqId, contractDetails)` callback
4. Extract the fully resolved `Contract` from `contractDetails.contract`

**Important**: IB uses a combination of fields to uniquely identify a contract. Ambiguous contracts will return multiple results.

### 5.2 Contract Fields by Asset Class

#### Stocks (STK)
```python
contract = Contract()
contract.symbol = "AAPL"
contract.secType = "STK"
contract.exchange = "SMART"      # or "NYSE", "NASDAQ", "AMEX"
contract.currency = "USD"
# primaryExchange needed if exchange="SMART" and symbol exists on multiple exchanges
```

#### Options (OPT)
```python
contract = Contract()
contract.symbol = "AAPL"
contract.secType = "OPT"
contract.exchange = "SMART"
contract.currency = "USD"
contract.lastTradeDateOrContractMonth = "20240119"  # Expiry (yyyyMMdd)
contract.strike = 150.0
contract.right = "CALL"          # "CALL" or "PUT"
contract.multiplier = "100"      # Usually 100 for US options
# tradingClass may be needed for some options
```

**Option Chain via reqSecDefOptParams (recommended):**
```python
self.reqSecDefOptParams(0, "AAPL", "", "STK", 265598)
# Response: securityDefinitionOptionParameter(reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes)
```

**Exercise Options:**
```python
self.exerciseOptions(reqId, contract, exerciseAction, exerciseQuantity, account, override, manualOrderTime)
# exerciseAction: 1=Exercise, 2=Lapse
# override: 1=Override, 0=No override
```

#### Futures (FUT)
```python
contract = Contract()
contract.symbol = "ES"
contract.secType = "FUT"
contract.exchange = "CME"
contract.currency = "USD"
contract.lastTradeDateOrContractMonth = "202403"  # Contract month (yyyyMM)
contract.multiplier = "50"
# localSymbol often more reliable: "ESZ3", "ESH4", etc.
# tradingClass: "ES"
```

**Continuous Futures:**
Use `localSymbol` with specific continuous futures conventions. The conId changes with each roll.

#### Forex (FOREX)
```python
contract = Contract()
contract.symbol = "EUR"          # Base currency
contract.secType = "FOREX"       # or "CASH"
contract.exchange = "IDEALPRO"   # Primary FX exchange
contract.currency = "USD"        # Quote currency
# The pair is defined as symbol/currency (EUR/USD)
```

**Convention**: `symbol` = base currency, `currency` = quote currency

#### Bonds (BOND)
```python
contract = Contract()
contract.secType = "BOND"
contract.symbol = ""             # Usually empty
contract.exchange = "SMART"
contract.currency = "USD"
# Identify by: cusip, issueDate, maturityDate, or conId
contract.cusip = "912810QT0"
```

#### Index (IND)
```python
contract = Contract()
contract.symbol = "SPX"
contract.secType = "IND"
contract.exchange = "CBOE"
contract.currency = "USD"
```

#### CFD (CFD)
```python
contract = Contract()
contract.symbol = "IBUS500"
contract.secType = "CFD"
contract.exchange = "SMART"
contract.currency = "USD"
```

#### Crypto (CRYPTO)
```python
contract = Contract()
contract.symbol = "BTC"
contract.secType = "CRYPTO"
contract.exchange = "PAXOS"     # or other crypto exchanges
contract.currency = "USD"
```

#### Combos / Spreads (BAG)
```python
contract = Contract()
contract.secType = "BAG"
contract.exchange = "SMART"
contract.currency = "USD"
contract.comboLegs = [
    ComboLeg(conId=265598, ratio=1, action="BUY", exchange="SMART"),
    ComboLeg(conId=..., ratio=1, action="SELL", exchange="SMART"),
]
```

### 5.3 ConId (Contract Identifier)

- Each contract has a unique integer `conId` assigned by IB
- Using `conId` alone (with exchange) is sufficient to identify a contract
- `conId` can be found via `reqContractDetails` or IB's Contract Description tool
- **Best practice**: Resolve contracts once, cache the `conId`, then use it for subsequent requests

---

## 6. Order System

### 6.1 Order Lifecycle (State Machine)

```
[Create Order]
     │
     ▼
PendingSubmit ──► Submitted ──► Filled
     │                │              │
     │                │              ▼
     │           PreSubmitted   [Complete]
     │                │
     │         PendingReplace
     │                │
     ▼                ▼
  Cancelled       Cancelled
     │
  ApiCancelled

Inactive ← (order rejected/invalid)
```

**Status transitions:**
1. `placeOrder()` → `PendingSubmit` (order sent to TWS)
2. TWS acknowledges → `Submitted` (order working)
3. Partial/complete fill → `Filled` (when totalQuantity reached)
4. `cancelOrder()` → `PendingCancel` → `Cancelled`
5. Invalid order → `Inactive` (with error message)

### 6.2 Bracket Orders

Bracket orders consist of a parent order + child orders (stop loss, take profit):

```python
# Parent (entry order)
parent = Order()
parent.orderId = ib.client.getReqId()
parent.action = "BUY"
parent.orderType = "LMT"
parent.totalQuantity = 100
parent.lmtPrice = 150.0
parent.transmit = False  # Don't transmit yet

# Stop loss child
stop = Order()
stop.orderId = ib.client.getReqId()
stop.action = "SELL"
stop.orderType = "STP"
stop.totalQuantity = 100
stop.auxPrice = 145.0
stop.parentId = parent.orderId
stop.transmit = False

# Take profit child
tp = Order()
tp.orderId = ib.client.getReqId()
tp.action = "SELL"
tp.orderType = "LMT"
tp.totalQuantity = 100
tp.lmtPrice = 160.0
tp.parentId = parent.orderId
tp.transmit = True  # Transmit all orders now

# Place all three (in order)
self.placeOrder(parent.orderId, contract, parent)
self.placeOrder(stop.orderId, contract, stop)
self.placeOrder(tp.orderId, contract, tp)
```

**Key:** `transmit=False` on parent and intermediate children. `transmit=True` on the last child to send all orders.

### 6.3 OCA Groups (One Cancels All)

```python
order1.ocaGroup = "myOcaGroup"
order1.ocaType = 1  # CANCEL_WITH_BLOCK

order2.ocaGroup = "myOcaGroup"
order2.ocaType = 1

order3.ocaGroup = "myOcaGroup"
order3.ocaType = 1
```

**ocaType values:**
- 1: CANCEL_WITH_BLOCK — cancel all remaining orders when one fills
- 2: REDUCE_WITH_BLOCK — reduce remaining orders proportionally
- 3: REDUCE_NON_BLOCK — reduce without blocking

### 6.4 Conditional Orders

Orders can have conditions that must be met before they become active:

```python
# PriceCondition
condition = PriceCondition(
    PriceCondition.TriggerMethodEnum.DEFAULT,
    conId,
    exchange,
    price,
    isMore=True,  # True=above, False=below
    conIdExchSrv=False
)
order.conditions.append(condition)
```

Condition types: `PriceCondition`, `TimeCondition`, `MarginCondition`, `ExecutionCondition`, `VolumeCondition`, `PercentChangeCondition`

### 6.5 Algo Orders

```python
order.algoStrategy = "VWAP"
order.algoParams = [
    TagValue("startTime", "09:30:00 US/Eastern"),
    TagValue("endTime", "16:00:00 US/Eastern"),
    TagValue("noTakeLiq", "1"),
    TagValue("allowPastEndTime", "0"),
    TagValue("getDone", "0"),
]
```

**Available algo strategies:**
| Strategy | Description | Key Parameters |
|----------|-------------|----------------|
| `VWAP` | Volume-weighted average price | startTime, endTime, noTakeLiq, allowPastEndTime, getDone |
| `TWAP` | Time-weighted average price | startTime, endTime, noTakeLiq, allowPastEndTime |
| `Arrival` | Arrival price | startTime, endTime, allowPastEndTime, maxPctVol |
| `DarkIce` | Dark Ice (hidden) | startTime, endTime, allowPastEndTime, displaySize |
| `AD` | Accumulate/Distribute | startTime, endTime, noTakeLiq, allowPastEndTime, maxPctVol, randomizeSize20, randomizeTime20 |
| `PercentVolume` | Percentage of volume | startTime, endTime, noTakeLiq, allowPastEndTime, pctVol |
| `MinMax` | Min/Max participation | startTime, endTime, allowPastEndTime |
| `Balance` | Balance impact/urgency | startTime, endTime, allowPastEndTime |
| `Scale` | Scale trading | startTime, endTime |

### 6.6 Adaptive Orders

```python
order.algoStrategy = "Adaptive"
order.algoParams = [
    TagValue("adaptivePriority", "Patient"),  # Patient, Normal, Urgent
]
```

### 6.7 What-If (Margin Preview)

```python
order.whatIf = True
self.placeOrder(orderId, contract, order)
# Response comes via orderStatus callback with margin information
```

### 6.8 Order ID Management

```python
# On connect, receive next valid order ID
def nextValidId(self, orderId: int):
    self.nextOrderId = orderId

# Request more IDs if needed
self.reqIds(-1)

# Use sequential IDs
self.placeOrder(self.nextOrderId, contract, order)
self.nextOrderId += 1
```

**Critical**: Never reuse an orderId. Always use sequential IDs starting from `nextValidId`.

---

## 7. Portfolio & Account System

### 7.1 Account Summary

```python
# Subscribe to account summary
self.reqAccountSummary(reqId, "All", "NetLiquidation,AvailableFunds,GrossPositionValue,UnrealizedPnL,RealizedPnL")
```

**Common Account Summary Tags:**
| Tag | Description |
|-----|-------------|
| `AccountType` | Account type (INDIVIDUAL, JOINT, etc.) |
| `NetLiquidation` | Net liquidation value |
| `TotalCashValue` | Total cash value |
| `GrossPositionValue` | Gross position value |
| `EquityWithLoanValue` | Equity with loan value |
| `AvailableFunds` | Available funds |
| `ExcessLiquidity` | Excess liquidity |
| `BuyingPower` | Buying power |
| `Leverage` | Leverage factor |
| `FullAvailableFunds` | Full available funds |
| `FullInitMarginReq` | Full initial margin requirement |
| `FullMaintMarginReq` | Full maintenance margin requirement |
| `UnrealizedPnL` | Total unrealized P&L |
| `RealizedPnL` | Total realized P&L |
| `LookAheadNextChange` | Next margin change time |
| `LookAheadInitMarginReq` | Lookahead initial margin |
| `LookAheadMaintMarginReq` | Lookahead maintenance margin |
| `LookAheadAvailableFunds` | Lookahead available funds |
| `LookAheadExcessLiquidity` | Lookahead excess liquidity |
| `HighestSeverity` | Highest severity level |

**Callback:** `accountSummary(reqId, account, tag, value, currency)`

### 7.2 Account Updates

```python
self.reqAccountUpdates(True, "U1234567")  # Subscribe
# Callbacks: updateAccountValue, updatePortfolio, updateAccountTime
self.reqAccountUpdates(False, "U1234567")  # Unsubscribe
```

**updatePortfolio callback fields:**
- `contract`: Contract object
- `position`: Decimal position size
- `marketPrice`: Current market price
- `marketValue`: Position market value
- `averageCost`: Average cost basis
- `unrealizedPNL`: Unrealized P&L
- `realizedPNL`: Realized P&L
- `accountName`: Account identifier

### 7.3 Positions

```python
self.reqPositions()
# Callback: position(account, contract, position, avgCost)
# Terminal: positionEnd()

# Multi-account:
self.reqPositionsMulti(reqId, account, modelCode)
# Callback: positionMulti(reqId, account, modelCode, contract, pos, avgCost)
```

### 7.4 P&L — Account Level

```python
self.reqPnL(reqId, "U1234567", "")
# Callback: pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)
# Updates approximately once per second
# Cancel: self.cancelPnL(reqId)
```

### 7.5 P&L — Single Position

```python
self.reqPnLSingle(reqId, "U1234567", "", 265598)  # conId
# Callback: pnlSingle(reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value)
# Updates approximately once per second
# Cancel: self.cancelPnLSingle(reqId)
```

**P&L Data Sources:**
- **Account Window P&L** (via `reqAccountUpdates`): Updates on trade or every 3 minutes. Realized P&L reset daily.
- **Portfolio Window P&L** (via `reqPnL`/`reqPnLSingle`): More granular, resets based on TWS Global Configuration settings.

**API version requirements:**
- `reqPnL`: API v973.03+
- `reqPnLSingle`: API v973.03+
- `realizedPnL` in `pnlSingle`: API v973.05+ / TWS v968+
- Python P&L functions: API v973.06+

### 7.6 FA (Financial Advisor) Structure

**Allocation Methods by Group:**
| Method | Description |
|--------|-------------|
| `EqualQuantity` | Equal shares across accounts |
| `NetLiq` | Proportional to net liquidation |
| `AvailableEquity` | Proportional to available equity |
| `PctChange` | Percentage change of existing position |

**Allocation Methods by Profile:**
| Method | Type Number | Description |
|--------|-------------|-------------|
| `Percentages` | 1 | Explicit percentages |
| `Financial Ratios` | 2 | Explicit ratios |
| `Shares` | 3 | Explicit share counts |

**FA API Functions:**
```python
self.requestFA(FaDataTypeEnum.GROUPS)     # Request groups
self.requestFA(FaDataTypeEnum.PROFILES)   # Request profiles
self.requestFA(FaDataTypeEnum.ALIASES)    # Request aliases
# Callback: receiveFA(faDataType, faXmlData)

self.replaceFA(reqId, FaDataTypeEnum.GROUPS, xmlString)
# Callback: replaceFAEnd(reqId, text)
```

---

## 8. Pacing & Rate Limits

### 8.1 Historical Data Pacing

- **Max 60 requests per 10-minute window** for the same contract
- **15-second minimum between identical requests**
- Small bar sizes (≤ 5 secs): Additional restrictions apply
- **Workarounds**:
  - Use `keepUpToDate=True` for live bars
  - Batch different contracts in parallel
  - Cache historical data locally
  - Use `reqHeadTimestamp` to check data availability before large requests

### 8.2 Market Data Subscription Limits

- Default max ~100 concurrent `reqMktData` subscriptions (configurable in TWS settings up to ~500+)
- Default max 3 concurrent `reqMktDepth` subscriptions
- Default max 1 concurrent `reqRealTimeBars` subscription
- These limits are configurable in TWS/Gateway: Edit → Global Configuration → API → Settings

### 8.3 Scanner Pacing

- Scanner results update frequency is limited
- Avoid repeated scanner calls with identical parameters within seconds

### 8.4 Web API Rate Limits

- No explicit documented rate limit, but reasonable usage expected
- Session timeout after period of inactivity
- Re-authentication required after session expiry

### 8.5 General API Limits

- **Message rate**: TWS processes API messages sequentially. Sending too many requests too fast can cause queue buildup.
- **Connection limit**: Only one TWS instance can run per machine (multiple clientId connections per instance OK)
- **Order rate**: No explicit limit, but rapid-fire orders may trigger risk controls

---

## 9. Reconnection & Error Handling

### 9.1 Reconnection Behavior

- TWS/Gateway does NOT auto-reconnect API clients
- If TWS/Gateway restarts, the API client must detect the disconnect and reconnect
- Use `isConnected()` to check connection status
- `connectionClosed()` callback fires when connection drops

### 9.2 Common Error Codes

| Code | Description | Action |
|------|-------------|--------|
| 100 | Max number of market data subscriptions exceeded | Cancel existing subscriptions or increase limit in TWS settings |
| 101 | Max number of market depth subscriptions exceeded | Cancel existing depth subscriptions |
| 102 | Duplicate market data request | Use a different reqId |
| 103 | Duplicate order ID | Use next valid order ID |
| 104 | Cannot modify a filled order | Only pending orders can be modified |
| 105 | Order being modified | Wait for modification to complete |
| 110 | The price does not conform to the minimum price variation | Adjust price to valid increment |
| 111 | TIF type is invalid for this order | Change TIF |
| 113 | Invalid order | Check order parameters |
| 117 | Invalid exchange | Check exchange field |
| 119 | Not connected | Reconnect to TWS |
| 128 | Not available for this combination | Check contract validity |
| 130 | Account is not available | Check account code |
| 131 | Position does not exist | Verify position |
| 135 | Cannot find order | Verify order ID |
| 136 | No security definition found | Contract is invalid or not available |
| 200 | No security definition has been found for the request | Contract specification error |
| 201 | Order rejected — reason | Read error string for details |
| 202 | Order cancelled — reason | Read error string for details |
| 203 | The security is not available for trading | Check trading hours/permissions |
| 210 | Event notification (not an error) | Informational |
| 211 | Event notification: trade report | Informational |
| 300 | Can't find EId with ticker: | Invalid ticker ID |
| 301 | Error in margin calculation | Check what-if order |
| 302 | This contract is not available in this simulation | Paper trading limitation |
| 303 | There was a problem with the market data farm connection | Temporary, will auto-reconnect |
| 304 | Market data farm connection is OK | Informational |
| 305 | Market data for this contract is not available | Check subscription permissions |
| 316 | Requested market data is not subscribed | Subscribe to market data feed |
| 317 | Market data server error | Temporary |
| 319 | No market data permissions | Check account permissions |
| 320 | Server error when reading | Temporary |
| 321 | Server error: reqId | Request ID issue |
| 326 | Unable to connect to market data farm | Temporary |
| 354 | Not subscribed to requested market data | Subscribe |
| 401 | Login failed | Check credentials |
| 431 | Problem with data from order book | Temporary |
| 501 | Already connected | Don't reconnect if already connected |
| 502 | Couldn't connect to TWS | Check TWS is running and port |
| 503 | The TWS is out of date and must be upgraded | Upgrade TWS |
| 504 | Not connected | Reconnect |
| 505 | Fatal error: Connection to TWS has been lost | Reconnect |
| 506 | Server version is too old | Upgrade TWS |
| 507 | Bad message length | Protocol error |
| 508 | Bad 2nd message | Protocol error |
| 509 | Bad 3rd message | Protocol error |
| 510 | Server error: bad message | Protocol error |
| 511 | Server error: bad message 2 | Protocol error |
| 512 | Server error: invalid symbol | Check contract |
| 513 | Error validating position to a closing order | Check position |
| 514 | Error modifying order | Check order parameters |
| 515 | Error cancelling order | Check order ID |
| 516 | Cannot send circular order | Fix order chain |
| 10009 | Order rejected: hedging order must be same as original | Fix hedge params |
| 10013 | Order rejected: reason | Read error string |
| 10014 | Order cancelled: reason | Read error string |
| 10015 | Order placed, but not transmitted | Transmit=True on last child |
| 10016 | Order submitted to exchange | Informational |
| 10017 | Order is in inactive state | Check order validity |
| 10018 | No account found | Check account ID |
| 10019 | Max number of orders exceeded | System limit |
| 10020 | Invalid order action | Must be BUY or SELL |
| 10021 | Invalid order quantity | Must be positive |
| 10022 | Invalid order type | Check order type string |
| 10023 | Invalid TIF | Check TIF string |
| 10024 | Invalid price | Check price conforming to min tick |
| 10025 | Order size does not conform | Check quantity rules |
| 10026 | Order exceeds position | Cannot close more than position size |
| 10027 | Pacing violation | Slow down historical data requests |
| 10028 | Not subscribed to market data | Check data subscriptions |
| 10029 | Market data is frozen | Data is frozen (no live data available) |
| 10030 | Market data is delayed | Using delayed data |
| 10031 | Market data is stale | Data hasn't updated |
| 10032 | Market data is halted | Trading halted |
| 10033 | Market data is unsubscribed | Subscription lapsed |
| 10034 | Market data subscription error | Check subscription status |
| 10035 | Market data connection lost | Temporary |
| 10036 | Market data farm is connected | Informational |
| 10037 | Market data farm is disconnected | Temporary |
| 10038 | Market data farm is not available | Temporary |
| 10039 | Market data farm subscription status | Informational |

### 9.3 Error Classification

**Transient (retry-able):**
- 303, 304: Market data farm connection issues — will auto-reconnect
- 320: Server read error — retry
- 326: Connection to farm — retry
- 502: Couldn't connect — TWS might be restarting

**Fatal (need action):**
- 119, 504: Not connected — must reconnect
- 505: Connection lost — must reconnect
- 136, 200: Invalid contract — fix contract specification
- 100-102: Subscription limits — cancel subscriptions or increase limits

**Informational (not errors):**
- 210, 211: Event notifications
- 304, 10036: Connection OK notifications
- 10030, 10031: Delayed/stale data notifications

### 9.4 Session Management

- TWS auto-logoff: Daily restart required (can be configured)
- Gateway: Similar auto-logoff behavior
- Weekend: TWS/Gateway may auto-shutdown; requires restart on Monday
- IBC (IBController): Automates startup, login, and restart

### 9.5 Order/Position Synchronization After Reconnect

On reconnect:
1. `nextValidId` callback receives next valid order ID
2. `managedAccounts` callback receives account list
3. If "Download open orders on connection" is checked: `openOrder` callbacks for all open orders
4. If "Download positions on connection" is checked: `position` callbacks for all positions
5. `openOrderEnd` and `positionEnd` mark completion

---

## 10. Python Library Comparison

### 10.1 ib_async (Recommended)

**Repository**: https://github.com/ib-api-reloaded/ib_async  
**License**: BSD-2-Clause  
**Python**: 3.10+  
**Install**: `pip install ib_async`

**Architecture:**
- asyncio-native, no threading
- Implements the full IBKR binary protocol internally (does not depend on ibapi)
- Bridges EWrapper callbacks to eventkit events
- Single `IB` class is the main interface

**Key Classes:**
- `IB` — Main connection and request interface
- `Contract` (and subclasses: `Stock`, `Option`, `Future`, `Forex`, `Index`, `Bond`)
- `Order` (and subclasses: `MarketOrder`, `LimitOrder`, `StopOrder`, `StopLimitOrder`)
- `Trade` — Tracks complete order lifecycle
- `Ticker` — Live market data container
- `Position` — Portfolio position
- `BarData` — Historical bar
- `util` — Utility functions (df(), startLoop(), etc.)

**Event System:**
```python
from ib_async import *

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)

# Event-driven pattern
ticker = ib.reqMktData(contract, '', False, False)
ticker.updateEvent += lambda t: print(t.last, t.bid, t.ask)

# Wait pattern
ib.sleep(5)  # or ib.run() for infinite loop

# Synchronous request pattern
bars = ib.reqHistoricalData(contract, '', '1 D', '5 mins', 'TRADES', True)
df = util.df(bars)
```

**Core IB Methods:**
- `connect(host, port, clientId)` / `disconnect()`
- `reqMktData(contract, genericTickList, snapshot, regulatorySnapshot)` → `Ticker`
- `reqHistoricalData(contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH)` → list of `BarData`
- `placeOrder(contract, order)` → `Trade`
- `cancelOrder(order)` → `Trade`
- `positions()` → list of `Position`
- `accountSummary()` → list of `AccountValue`
- `reqPnL(account)` → `PnL` (with updateEvent)
- `reqPnLSingle(account, conId)` → `PnLSingle`
- `qualifyContracts(*contracts)` → resolves contracts (adds conId, exchange, etc.)
- `whatIfOrder(contract, order)` → `OrderState` (margin preview)

**Trade Object:**
- Tracks order lifecycle from submission to fill
- `trade.orderStatus` — current status
- `trade.fills` — list of fills
- `trade.log` — list of trade log messages
- `trade.statusEvent` — fires on status change
- `trade.fillEvent` — fires on each fill
- `trade.commissionReportEvent` — fires on commission report

**Ticker Object:**
- `ticker.last`, `ticker.bid`, `ticker.ask`, `ticker.lastSize`, `ticker.bidSize`, `ticker.askSize`
- `ticker.volume`, `ticker.high`, `ticker.low`, `ticker.close`
- `ticker.updateEvent` — fires on any field change

**Utility Functions:**
- `util.df(data)` — Convert list of objects to pandas DataFrame
- `util.startLoop()` — Enable asyncio in Jupyter notebooks
- `util.schedule(time, callback)` — Schedule callback at specific time
- `util.waitFn(fn, timeout)` — Wait for condition

**Wait/Queue Patterns:**
```python
# Wait for specific event
ib.waitOnUpdate(timeout=10)

# Wait for condition
filled = ib.waitOnUpdate(lambda: trade.orderStatus.status == 'Filled')

# Async
bars = await ib.reqHistoricalDataAsync(...)

# Event handler
def onStatus(trade):
    if trade.orderStatus.status == 'Filled':
        print(f"Filled at {trade.orderStatus.avgFillPrice}")

trade.statusEvent += onStatus
```

**Known Issues:**
- Must use `util.startLoop()` in Jupyter for asyncio compatibility
- TWS/Gateway must have "Download open orders on connection" checked
- Large option chains can be slow — use `reqSecDefOptParams` instead

### 10.2 ib_insync (Archived)

**Repository**: https://github.com/pilotech/ib_insync (404 — archived/moved)  
**Status**: Archived. Migration path: switch to `ib_async`.

**Migration from ib_insync to ib_async:**
- Replace `from ib_insync import *` with `from ib_async import *`
- Nearly identical API surface
- ib_async is a fork of ib_insync with ongoing maintenance
- Key difference: ib_async uses newer Python features (3.10+)

### 10.3 Official ibapi

**Architecture:**
- Threading-based: Dedicated reader thread consumes TCP messages
- Callback-based: All data delivered via `EWrapper` callbacks
- No built-in wait/sync mechanisms — user must implement threading primitives
- Boilerplate-heavy: Must subclass both `EClient` and `EWrapper`

**Why it's harder:**
- No automatic contract resolution
- No built-in event system
- Manual thread synchronization required
- No pandas integration
- Verbose callback handling
- Error-prone message parsing

**When you might need it:**
- Maximum low-level control
- Debugging protocol issues
- Custom protocol extensions
- Educational purposes

---

## 11. Production Deployment

### 11.1 TWS vs IB Gateway for Production

| Aspect | TWS | IB Gateway |
|--------|-----|------------|
| GUI | Full GUI | Headless |
| Resources | High (~1-2 GB) | Lower (~512 MB-1 GB) |
| Stability | Same | Same |
| API | Same | Same |
| Docker | Difficult | Supported |
| Recommended | No | **Yes** |

**Recommendation**: Use IB Gateway for all production deployments.

### 11.2 Docker Deployment of IB Gateway

```dockerfile
# Use IBC (IBController) Docker image
FROM ghcr.io/extrange/ibc:latest

# Configure environment
ENV TRADING_MODE=paper  # or live
ENV TWSUSERID=your_username
ENV TWSPASSWORD=your_password
ENV GATEWAY=yes
ENV TWOFA_TIMEOUT=300

EXPOSE 4001 4002 7496 7497
```

Popular Docker images:
- `ghcr.io/extrange/ibc` — IBC + IB Gateway
- `waytrade/ib-gateway-docker` — Alternative

### 11.3 IBC (IBController) for Automated Startup

IBC automates:
- Gateway startup and login
- Two-factor authentication acceptance
- Gateway restart after daily logoff
- Crash recovery

**Configuration:**
```ini
# IBCConfig.ini
TradingMode=paper
IbLoginId=your_username
IbPassword=your_password
Gateway=true
TwsPort=4002
OverrideTwsApiPort=true
SecondFactorDevice=ib_key
AcceptNonBrokerageAccountWarning=yes
```

### 11.4 Session Management

- **Daily logoff**: Gateway auto-logoffs daily. IBC handles restart.
- **Weekend**: Gateway stays down. IBC will restart on Monday pre-market.
- **Two-factor auth**: IBC can auto-accept IB Key challenges. Configure timeout.
- **Health monitoring**: Monitor TCP port availability (4001/4002) for health checks.

### 11.5 Health Monitoring & Watchdog

```python
import socket

def check_gateway_health(host='127.0.0.1', port=4001, timeout=5):
    """Check if IB Gateway is accepting connections"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.close()
        return True
    except:
        return False
```

**Watchdog pattern:**
1. Cron job every 5 minutes checks port availability
2. If port unavailable, check if IBC is running
3. If IBC not running, start IBC
4. If IBC running but port unavailable, restart IBC
5. Alert on repeated failures

### 11.6 Java Memory Configuration

```bash
# In Gateway settings or IBC config
# Set Java heap to at least 4096 MB
JAVA_HEAP_SIZE=4096
# Or in Gateway: Configure → Settings → Memory Allocation → 4096 MB
```

---

## 12. Production Architecture Recommendations

### 12.1 Service Decomposition

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────┐
│  IB Gateway  │◄──►│  Market Data     │───►│  QuestDB     │
│  (IBC)       │    │  Service         │    │  (ticks/bars)│
└──────┬───────┘    └──────┬───────────┘    └──────┬──────┘
       │                   │                        │
       │            ┌──────▼───────────┐            │
       │            │  Order Manager   │◄───────────┤
       │            │  Service         │            │
       │            └──────┬───────────┘    ┌──────▼──────┐
       │                   │                │  Redis       │
       │            ┌──────▼───────────┐    │  (pub/sub)   │
       └───────────►│  Strategy Engine  │◄──►│              │
                    │  (asyncio)        │    └─────────────┘
                    └──────┬───────────┘
                           │
                    ┌──────▼───────────┐
                    │  Risk Manager    │
                    │  (circuit break) │
                    └──────────────────┘
```

### 12.2 Event-Driven Architecture

- **Market Data Service**: Single connection to IB Gateway, receives all market data
- **Redis Pub/Sub**: Fan out market data to multiple consumers
- **Order Manager**: Dedicated service for order lifecycle management
- **Strategy Engine**: Multiple strategy instances consuming market data from Redis

### 12.3 Async Architecture with asyncio (ib_async)

```python
import asyncio
from ib_async import *

class TradingService:
    def __init__(self):
        self.ib = IB()
        self.connected = False
        
    async def connect(self):
        try:
            self.ib.connect('127.0.0.1', 4001, clientId=1)
            self.connected = True
            self.ib.errorEvent += self.on_error
        except Exception as e:
            print(f"Connection failed: {e}")
            await asyncio.sleep(30)
            await self.connect()
    
    def on_error(self, reqId, errorCode, errorString, contract):
        if errorCode in (504, 505):  # Connection lost
            self.connected = False
            asyncio.create_task(self.reconnect())
    
    async def reconnect(self):
        while not self.connected:
            try:
                await self.connect()
            except:
                await asyncio.sleep(30)
    
    async def run(self):
        await self.connect()
        # Start strategy loops
        await asyncio.gather(
            self.market_data_loop(),
            self.order_management_loop(),
            self.risk_monitor_loop(),
        )
```

### 12.4 Redis Pub/Sub for Market Data Fanout

```python
import redis
import json

r = redis.Redis()

# Publisher (Market Data Service)
def on_ticker_update(ticker):
    data = {
        'symbol': ticker.contract.symbol,
        'last': ticker.last,
        'bid': ticker.bid,
        'ask': ticker.ask,
        'volume': ticker.volume,
        'time': datetime.now().isoformat(),
    }
    r.publish(f'marketdata:{ticker.contract.conId}', json.dumps(data))

# Subscriber (Strategy Engine)
pubsub = r.pubsub()
pubsub.subscribe('marketdata:265598')  # AAPL
for message in pubsub.listen():
    data = json.loads(message['data'])
    strategy.on_tick(data)
```

### 12.5 QuestDB Schema for Tick/Bar Storage

```sql
-- Tick data
CREATE TABLE IF NOT EXISTS ticks (
    symbol SYMBOL,
    conId INT,
    timestamp TIMESTAMP,
    price DOUBLE,
    size LONG,
    exchange SYMBOL,
    conditions STRING,
    tick_type SYMBOL  -- LAST, BID, ASK
) TIMESTAMP(timestamp) PARTITION BY DAY WAL;

-- Bar data
CREATE TABLE IF NOT EXISTS bars (
    symbol SYMBOL,
    conId INT,
    bar_size SYMBOL,  -- 1s, 5s, 1m, 5m, 1h, 1d
    timestamp TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume LONG,
    vap DOUBLE,       -- Volume-weighted average price
    trade_count INT
) TIMESTAMP(timestamp) PARTITION BY DAY WAL;

-- Order events
CREATE TABLE IF NOT EXISTS order_events (
    order_id INT,
    perm_id LONG,
    symbol SYMBOL,
    conId INT,
    action SYMBOL,
    order_type SYMBOL,
    quantity DOUBLE,
    limit_price DOUBLE,
    stop_price DOUBLE,
    status SYMBOL,
    filled DOUBLE,
    avg_fill_price DOUBLE,
    commission DOUBLE,
    timestamp TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
```

### 12.6 Connection Pooling / Multiplexing

- **Single IB connection per Gateway instance** — the binary protocol handles multiplexing via reqId
- Use different `clientId` values for different services if needed
- **Recommended**: One Market Data Service (clientId=1), one Order Manager (clientId=2)
- Avoid multiple connections requesting the same market data — use Redis fanout instead

### 12.7 Error Recovery Patterns

```python
class RobustIBConnection:
    MAX_RETRIES = 10
    RETRY_DELAY = 30  # seconds
    
    async def connect_with_retry(self):
        for attempt in range(self.MAX_RETRIES):
            try:
                self.ib = IB()
                self.ib.connect(self.host, self.port, clientId=self.client_id)
                self.ib.errorEvent += self._handle_error
                self._sync_state()
                return True
            except Exception as e:
                wait = min(self.RETRY_DELAY * (2 ** attempt), 300)
                await asyncio.sleep(wait)
        raise ConnectionError(f"Failed after {self.MAX_RETRIES} attempts")
    
    def _sync_state(self):
        """Re-sync after reconnect"""
        # Re-download open orders
        # Re-download positions
        # Re-subscribe to market data
        # Re-subscribe to P&L
        pass
    
    def _handle_error(self, reqId, errorCode, errorString, contract):
        if errorCode == 505:  # Connection lost
            asyncio.create_task(self.connect_with_retry())
        elif errorCode == 100:  # Max subscriptions
            self._cancel_oldest_subscription()
```

### 12.8 Graceful Degradation

1. **Market data unavailable**: Fall back to last known prices. Log warnings. Continue order management.
2. **Order submission fails**: Queue orders locally. Retry with exponential backoff.
3. **Gateway down**: Activate backup gateway (if available). Pause trading. Alert immediately.
4. **Partial fills**: Track fill state. On reconnect, reconcile with TWS state.
5. **Stale data detection**: Track time since last market data update. Flag instruments with stale data.

### 12.9 Monitoring & Alerting

```python
class HealthMonitor:
    def __init__(self, ib):
        self.ib = ib
        self.last_tick_time = {}
        
    def check_health(self):
        checks = {
            'gateway_connected': self.ib.isConnected(),
            'last_tick_age': self._max_tick_age(),
            'open_orders': len(self.ib.openTrades()),
            'account_margin': self._check_margin(),
        }
        return checks
    
    def _max_tick_age(self):
        """Check if any subscribed instrument hasn't received ticks recently"""
        now = time.time()
        ages = {sym: now - t for sym, t in self.last_tick_time.items()}
        return max(ages.values()) if ages else 0
```

---

## Appendix A: TWS API Version History (Key Milestones)

| Version | Feature |
|---------|---------|
| v9.72+ | Initial Python API, reqSecDefOptParams |
| v973.03 | P&L functions (reqPnL, reqPnLSingle) |
| v973.04 | Tick-by-tick data (reqTickByTickData), TWS v969+ |
| v973.05 | Realized P&L at position level, TWS v968+ |
| v973.06 | Python P&L functions |
| v983+ | Unified Groups/Profiles for FA accounts |
| Latest | ProtoBuf support, enhanced market data |

## Appendix B: Port Reference

| Service | Live Port | Paper Port |
|---------|-----------|------------|
| TWS | 7496 | 7497 |
| IB Gateway | 4001 | 4002 |
| Web API (CPG) | 5000 (HTTPS) | 5000 (HTTPS) |

## Appendix C: Useful URL Reference

| Resource | URL |
|----------|-----|
| IBKR API Home | https://ibkrcampus.com/campus/ibkr-api-page/ibkr-api-home/ |
| TWS API Doc | https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/ |
| TWS API Ref | https://ibkrcampus.com/campus/ibkr-api-page/twsapi-ref/ |
| Web API Doc | https://ibkrcampus.com/campus/ibkr-api-page/web-api/ |
| Web API Ref | https://ibkrcampus.com/campus/ibkr-api-page/webapi-ref/ |
| ib_async | https://github.com/ib-api-reloaded/ib_async |
| ib_async API docs | https://ib-api-reloaded.github.io/ib_async/api.html |
| ib_async recipes | https://ib-api-reloaded.github.io/ib_async/recipes.html |
| ib_async notebooks | https://ib-api-reloaded.github.io/ib_async/notebooks.html |
| IBC (IBController) | https://github.com/IbcAlpha/IBC |
| Stable Gateway | https://www.interactivebrokers.com/en/trading/ibgateway-stable.php |
| Latest Gateway | https://www.interactivebrokers.com/en/trading/ibgateway-latest.php |

---

*Document compiled from IBKR official documentation (ibkrcampus.com, interactivebrokers.github.io/deprecated), ib_async GitHub repository, and production experience. Many of the old GitHub Pages TWS API URLs (interactivebrokers.github.io/tws-api/) are now returning 404 and have been migrated to ibkrcampus.com. The campus pages are JavaScript-heavy SPAs that don't render via simple web fetch — use the interactive docs directly.*
