"use strict";
const $ = id => document.getElementById(id);
const number = value => value == null ? "—" : new Intl.NumberFormat(undefined, { maximumFractionDigits: 4 }).format(value);
const time = value => value ? new Date(value).toLocaleString(undefined, { timeZone: "UTC", hour12: false }) + " UTC" : "Not observed";
let token = ""; // Deliberately never persisted or sent in a URL.

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text != null) node.textContent = text;
  if (className) node.className = className;
  return node;
}
function status(value) { return element("span", value.replaceAll("_", " "), "status " + value); }
function table(rows, headers) {
  const node = element("table", null, "data-table");
  if (headers) {
    const head = node.createTHead().insertRow();
    headers.forEach(value => { const cell = element("th", value); cell.scope = "col"; head.append(cell); });
  }
  const body = node.createTBody();
  rows.forEach(values => { const row = body.insertRow(); values.forEach((value, index) => {
    const cell = element(!headers && index === 0 ? "th" : "td", value);
    if (!headers && index === 0) cell.scope = "row";
    row.append(cell);
  }); });
  return node;
}
async function request(path, payload) {
  const headers = { Accept: "application/json" };
  if (token) headers.Authorization = "Bearer " + token;
  if (payload) headers["Content-Type"] = "application/json";
  const response = await fetch(path, { method: payload ? "POST" : "GET", headers, body: payload ? JSON.stringify(payload) : undefined, signal: AbortSignal.timeout(65000) });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (response.status === 401) throw new Error("API token required or invalid. Enter the token in Connection and check again.");
    const detail = typeof data.detail === "string" ? data.detail : data.detail?.message || JSON.stringify(data.detail || "Request failed");
    const error = new Error(response.status + " · " + detail);
    error.connection = data.detail?.connection;
    throw error;
  }
  return data;
}
async function submit(form, target, action) {
  const button = form.querySelector("button[type=submit]");
  button.disabled = true;
  target.setAttribute("aria-busy", "true");
  target.replaceChildren(element("p", "Requesting data…", "loading"));
  try { await action(); }
  catch (error) { target.replaceChildren(element("p", error.message, "error-message")); }
  finally { button.disabled = false; target.setAttribute("aria-busy", "false"); }
}
function feed(parent, name, availability) {
  const node = element("section", null, "feed");
  const title = element("div", null, "feed-title");
  title.append(element("h4", name), status(availability));
  node.append(title); parent.append(node); return node;
}
function renderEquity(data) {
  const target = $("equity-result"); target.replaceChildren();
  const heading = element("div", null, "result-heading");
  heading.append(element("h3", data.symbol), element("span", time(data.generated_at), "timestamp")); target.append(heading);
  target.append(element("p", [data.exchange, data.currency, "CONID " + data.con_id].join(" / "), "timestamp"));
  let node = feed(target, "Quote", data.section_status.quote);
  if (data.quote) {
    const quote = data.quote;
    node.append(table([["Last", number(quote.last)], ["Bid / Ask", number(quote.bid) + " / " + number(quote.ask)], ["Spread (bps)", number(quote.spread_bps)], ["Volume", number(quote.volume)]]));
    const types = { 1: "Live", 2: "Frozen", 3: "Delayed", 4: "Delayed frozen" };
    node.append(element("span", (types[data.quote_market_data_type] || "Market-data type unknown") + " · Observed " + time(data.quote_observed_at), "timestamp"));
    node.append(element("span", "Received " + time(data.quote_received_at), "timestamp"));
  } else node.append(element("p", "No usable quote returned.", "timestamp"));
  node = feed(target, "Dividends", data.section_status.dividends);
  if (data.dividends) {
    const dividends = data.dividends;
    node.append(table([["Past 12 months", number(dividends.past_12_months)], ["Next 12 months", number(dividends.next_12_months)], ["Next date", dividends.next_dividend_date || "—"], ["Next amount", number(dividends.next_dividend_amount)]]));
    node.append(element("span", "Observed " + time(dividends.observed_at) + " · Received " + time(dividends.received_at), "timestamp"));
  }
  node = feed(target, "Shortability · indicative", data.section_status.shortability);
  if (data.shortability) {
    const short = data.shortability;
    node.append(table([["Available shares", number(short.shortable_shares)], ["Rating", number(short.shortability_rating)]]));
    node.append(element("span", "Observed " + time(short.observed_at) + " · Received " + time(short.received_at), "timestamp"));
  }
  node = feed(target, "News", data.section_status.news);
  if (data.news) {
    if (!data.news.headlines.length) node.append(element("p", "No headlines returned.", "timestamp"));
    data.news.headlines.forEach(item => { node.append(element("p", item.headline, "headline"), element("span", item.provider_code + " · " + time(item.timestamp), "timestamp")); });
  } else node.append(element("p", data.section_status.news === "not_requested" ? "Enable headlines to include news." : "News unavailable.", "timestamp"));
  data.warnings.forEach(warning => target.append(element("p", warning, "warning")));
}
function renderBatch(data) {
  const target = $("batch-result"); target.replaceChildren();
  const counts = { success: 0, empty: 0, error: 0 };
  data.results.forEach(result => counts[result.status]++);
  const heading = element("div", null, "result-heading");
  heading.append(element("h3", data.results.length + " instruments"), element("span", time(data.generated_at), "timestamp"));
  target.append(heading, element("p", `${counts.success} success / ${counts.empty} empty / ${counts.error} failed`, "timestamp"));
  if (!data.results.length) target.append(element("p", "The universe contains no instruments.", "warning"));
  data.results.forEach(result => {
    const row = element("details", null, "batch-row"); const summary = element("summary");
    summary.append(element("strong", result.requested_symbol), status(result.status), element("span", result.bars.length + (result.bars.length === 1 ? " bar" : " bars"), "summary-count"));
    const identity = result.request;
    row.append(summary, element("p", [identity.symbol, identity.exchange, identity.currency, identity.con_id ? "CONID " + identity.con_id : null, identity.last_trade_date_or_contract_month, identity.what_to_show].filter(Boolean).join(" / "), "timestamp"));
    if (result.error) row.append(element("p", result.error, "error-message"));
    else if (!result.bars.length) row.append(element("p", "No historical bars returned.", "warning"));
    else {
      // Render bars only when expanded, keeping large panels responsive.
      row.addEventListener("toggle", () => {
        if (!row.open || row.dataset.rendered) return;
        const scroll = element("div", null, "table-scroll"); const bars = table(result.bars.map(bar => [time(bar.timestamp), number(bar.open), number(bar.high), number(bar.low), number(bar.close), number(bar.volume)]), ["Time (UTC)", "Open", "High", "Low", "Close", "Volume"]);
        bars.classList.add("bars-table"); scroll.append(bars); row.append(scroll); row.dataset.rendered = "true";
      });
    }
    target.append(row);
  });
}
function renderConnection(data) {
  const target = $("connection-result"); target.replaceChildren(element("h3", "IBKR clients"));
  const desired = data.desired_enabled == null ? "unreadable" : data.desired_enabled ? "enabled" : "disabled";
  target.append(element("p", "Shared switch / " + desired + " · Local connection / " + data.lifecycle_state, "timestamp"));
  target.append(element("p", "Status snapshot " + time(data.updated_at) + " · Refresh manually with Check connection.", "timestamp"));
  if (data.last_error) target.append(element("p", "Local / " + data.last_error, "warning"));
  if (data.clients_status !== "available") target.append(element("p", data.clients_error || "Client reports unavailable. Only local state is shown.", "warning"));
  const reports = data.clients || [];
  const rows = reports.map(client => {
    const fresh = client.freshness === "fresh";
    return [client.role, (client.client_id ?? "—") + " / " + client.instance_id.slice(0, 8),
      client.desired_enabled == null ? "Unknown" : client.desired_enabled ? "Enabled" : "Disabled",
      !fresh || client.socket_connected == null ? "Unknown" : client.socket_connected ? "Connected" : "Disconnected",
      fresh ? client.lifecycle_state : "unknown", client.freshness + (client.heartbeat_age_seconds == null ? "" : " · " + number(client.heartbeat_age_seconds) + "s"),
      time(client.last_checked_at), client.generation ?? "—"];
  });
  const scroll = element("div", null, "table-scroll");
  scroll.tabIndex = 0; scroll.setAttribute("role", "region"); scroll.setAttribute("aria-label", "IBKR client reports; scroll horizontally for more columns");
  const clients = table(rows, ["Role", "Client / instance", "Reported flag", "Socket", "Lifecycle", "Report age", "Checked (UTC)", "Generation"]);
  clients.classList.add("clients-table");
  Array.from(clients.tBodies[0].rows).forEach((row, index) => { row.cells[1].title = reports[index].instance_id; });
  scroll.append(clients); target.append(scroll);
  reports.filter(client => client.last_error && client.instance_id !== data.instance_id).forEach(client => {
    target.append(element("p", client.role + " / " + (client.client_id ?? client.instance_id) + " · " + client.last_error, "warning"));
  });
  target.append(element("p", "Reports cover participating clients sharing this Redis key. Stale reports show unknown socket state; expired reports disappear. This snapshot does not confirm that every client has stopped.", "hint"));
}
$("connection-form").addEventListener("submit", event => {
  event.preventDefault(); token = $("api-token").value.trim();
  submit(event.currentTarget, $("health-result"), async () => {
    const target = $("connection-result"); target.setAttribute("aria-busy", "true"); target.replaceChildren(element("p", "Checking client reports…", "loading"));
    const [health, connection] = await Promise.allSettled([request("/api/v1/system/health"), request("/api/v1/system/ibkr/connection")]);
    if (health.status === "fulfilled") {
      const data = health.value;
      const label = status(data.status); label.id = "health-status"; $("health-status").replaceWith(label);
      $("health-result").replaceChildren(...[["API", data.app_name], ["IBKR", data.ibkr_connection || "unknown"], ["Redis", data.redis_connection || "unknown"]].map(([name, value]) => { const node = element("span", name + " / "); node.append(element("strong", value)); return node; }));
    } else {
      const label = status("unavailable"); label.id = "health-status"; $("health-status").replaceWith(label);
      $("health-result").replaceChildren(element("p", health.reason.message, "error-message"));
    }
    if (connection.status === "fulfilled") renderConnection(connection.value);
    else if (connection.reason.connection) {
      renderConnection(connection.reason.connection);
      target.prepend(element("p", connection.reason.message, "error-message"));
    } else target.replaceChildren(element("p", connection.reason.message, "error-message"));
    target.setAttribute("aria-busy", "false");
  });
});
$("equity-form").addEventListener("submit", event => {
  event.preventDefault();
  const payload = { symbol: $("equity-symbol").value.trim(), include_news: $("include-news").checked };
  if (!payload.symbol) { $("equity-result").replaceChildren(element("p", "Enter a symbol.", "error-message")); return; }
  ["exchange", "currency"].forEach(key => { const value = $("equity-" + key).value.trim(); if (value) payload[key] = value; });
  submit(event.currentTarget, $("equity-result"), async () => renderEquity(await request("/api/v1/business/getEquityOverview", payload)));
});
$("batch-source").addEventListener("change", () => {
  const universe = $("batch-source").value === "universe";
  $("symbols-field").hidden = universe; $("universe-field").hidden = !universe;
  $("batch-symbols").disabled = universe; $("batch-symbols").required = !universe;
  $("batch-universe").disabled = !universe; $("batch-universe").required = universe;
});
$("batch-form").addEventListener("submit", event => {
  event.preventDefault();
  const universe = $("batch-source").value === "universe";
  const payload = { asset_class: "equity", duration: $("batch-duration").value, bar_size: $("batch-size").value, what_to_show: $("price-basis").value };
  if (universe) payload.universe = $("batch-universe").value.trim();
  else payload.symbols = $("batch-symbols").value.split(/[\s,]+/).filter(Boolean);
  if (universe ? !payload.universe : !payload.symbols.length) { $("batch-result").replaceChildren(element("p", "Enter symbols or a universe.", "error-message")); return; }
  const endpoint = universe ? "getUniverseBarsDetailed" : "getMarketPanelDetailed";
  submit(event.currentTarget, $("batch-result"), async () => renderBatch(await request("/api/v1/business/" + endpoint, payload)));
});
