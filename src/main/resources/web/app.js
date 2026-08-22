/* Thin renderer over the server's snapshot JSON: every number is computed server-side by
   risk-engine's calculators; this file only formats and signs what /api/stream pushes. */

// Embedded as a trading-desk tab (?embed=1): the desk supplies the outer chrome, so hide this
// app's own topbar/status bar (see app.css .embedded). Standalone, the class is never added.
if (new URLSearchParams(location.search).has("embed")) {
  document.body.classList.add("embedded");
}

const $ = (id) => document.getElementById(id);

const money = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const qty = new Intl.NumberFormat("en-US");

const fmt = (n, dp = 4) => (n == null ? "—" : n.toFixed(dp));

function signed(value, text) {
  const cls = value > 0 ? "pos" : value < 0 ? "neg" : "";
  return `<span class="${cls}">${text}</span>`;
}

function arrow(el, value, text) {
  el.textContent = text;
  el.classList.toggle("up", value > 0);
  el.classList.toggle("down", value < 0);
  el.classList.toggle("pos", value > 0);
  el.classList.toggle("neg", value < 0);
}

function renderStats(snapshot) {
  const book = snapshot.book;
  if (!book) return;
  $("st-instruments").textContent = String(book.symbols.length);
  $("st-gross").textContent = money.format(book.grossNotional);
  arrow($("st-valuation"), book.valuation, money.format(book.valuation));
  if (book.dayPnl != null)
    arrow($("st-pnl"), book.dayPnl, money.format(book.dayPnl));
}

function renderPositions(positions) {
  if (positions.length === 0) {
    $("positions").innerHTML =
      '<p class="empty">awaiting the first fill&hellip;</p>';
    return;
  }
  const rows = positions
    .map(
      (p) => `<tr>
        <td>${p.symbol}</td>
        <td>${signed(p.quantity, qty.format(p.quantity))}</td>
        <td>${money.format(p.lastPrice)}</td>
        <td>${new Date(p.lastTimeMillis).toLocaleTimeString()}</td>
      </tr>`,
    )
    .join("");
  $("positions").innerHTML = `<div class="report-block"><table class="risk">
      <thead><tr><th>Symbol</th><th>Net Qty</th><th>Last</th><th>Updated</th></tr></thead>
      <tbody>${rows}</tbody>
    </table></div>`;
}

/* One block per symbol: each report is priced in that symbol's own market, so the sums the
   book strip shows are the only cross-symbol numbers — VaR and Greeks stay per symbol. */
function renderReport(book) {
  if (!book) {
    $("report").innerHTML =
      '<p class="empty">no marks yet &mdash; the report prices off the first fill</p>';
    return;
  }
  $("report").innerHTML = book.symbols.map(symbolReport).join("");
}

function symbolReport(s) {
  const report = s.report;
  const g = report.greeks;
  const pct = Math.round(report.confidence * 100);
  const varBlock = (label, m) => `<tr>
      <td>${label}</td>
      <td>${money.format(m.valueAtRisk)}</td>
      <td>${money.format(m.expectedShortfall)}</td>
    </tr>`;
  const open =
    s.openPrice == null ? "no fill today" : `open ${money.format(s.openPrice)}`;
  let html = `<div class="report-block">
      <h3>${s.symbol} <span class="sub">mark-to-market &middot; ${open}</span></h3>
      <div class="valuation">${money.format(report.valuation)}</div>
    </div>
    <div class="report-block">
      <h3>Greeks <span class="sub">${s.symbol} &middot; bump-and-reprice</span></h3>
      <table class="risk"><tbody>
        <tr><td>Delta</td><td>${signed(g.delta, fmt(g.delta, 2))}</td></tr>
        <tr><td>Gamma</td><td>${fmt(g.gamma)}</td></tr>
        <tr><td>Vega</td><td>${fmt(g.vega, 2)}</td></tr>
        <tr><td>Theta</td><td>${signed(g.theta, fmt(g.theta, 2))}</td></tr>
        <tr><td>Rho</td><td>${fmt(g.rho, 2)}</td></tr>
      </tbody></table>
    </div>
    <div class="report-block">
      <h3>VaR / Expected Shortfall <span class="sub">${s.symbol} &middot; ${pct}% &middot; 1-day</span></h3>
      <table class="risk">
        <thead><tr><th>Method</th><th>VaR</th><th>ES</th></tr></thead>
        <tbody>
          ${varBlock("Parametric", report.var.parametric)}
          ${varBlock("Historical", report.var.historical)}
        </tbody>
      </table>
    </div>`;
  if (report.pnl) {
    const p = report.pnl;
    const row = (label, v) =>
      `<tr><td>${label}</td><td>${signed(v, money.format(v))}</td></tr>`;
    html += `<div class="report-block">
      <h3>Day PnL <span class="sub">${s.symbol} &middot; attribution vs session open</span></h3>
      <table class="risk"><tbody>
        ${row("Actual", p.actual)}
        ${row("Delta", p.delta)}
        ${row("Gamma", p.gamma)}
        ${row("Vega", p.vega)}
        ${row("Theta", p.theta)}
        ${row("Rho", p.rho)}
        ${row("Explained", p.explained)}
        ${row("Residual", p.residual)}
      </tbody></table>
    </div>`;
  }
  return html;
}

function renderExposure(exposure) {
  if (exposure.symbols.length === 0) {
    $("exposure").innerHTML =
      '<p class="empty">awaiting the first fill&hellip;</p>';
    return;
  }
  const pct = (u) => `${Math.round(u * 100)}%`;
  const rows = exposure.symbols
    .map(
      (s) => `<tr>
        <td>${s.symbol}</td>
        <td>${qty.format(Math.abs(s.netQuantity))} / ${qty.format(exposure.maxPosition)}</td>
        <td>${money.format(s.notional)} / ${money.format(exposure.maxNotional)}</td>
        <td>${pct(Math.max(s.positionUtilisation, s.notionalUtilisation))}</td>
        <td>${s.breached ? '<span class="neg">BREACH</span>' : '<span class="pos">OK</span>'}</td>
      </tr>`,
    )
    .join("");
  let html = `<div class="report-block"><table class="risk">
      <thead><tr><th>Symbol</th><th>Net</th><th>Notional</th><th>Util</th><th>Status</th></tr></thead>
      <tbody>${rows}</tbody>
    </table></div>`;
  if (exposure.events.length > 0) {
    const events = exposure.events
      .map(
        (e) => `<tr>
          <td>${new Date(e.ts).toLocaleTimeString()}</td>
          <td>${e.symbol}</td>
          <td>${e.kind}</td>
          <td>${e.breached ? '<span class="neg">breach</span>' : '<span class="pos">cleared</span>'}</td>
          <td>${money.format(e.value)} / ${money.format(e.limit)}</td>
        </tr>`,
      )
      .join("");
    html += `<div class="report-block">
      <h3>Breach events <span class="sub">newest first</span></h3>
      <table class="risk">
        <thead><tr><th>Time</th><th>Symbol</th><th>Limit</th><th>Event</th><th>Value</th></tr></thead>
        <tbody>${events}</tbody>
      </table></div>`;
  }
  if (exposure.malformed > 0) {
    html += `<p class="empty">malformed records skipped: ${qty.format(exposure.malformed)}</p>`;
  }
  $("exposure").innerHTML = html;
}

function renderSession(snapshot) {
  const sync = snapshot.sync;
  const offset = (p) => (p == null ? "—" : `@${p.offset}`);
  const rows = [
    ["Instruments", String(snapshot.positions.length)],
    ["Positions view", offset(sync.positions)],
    ["Exposure view", offset(sync.exposure)],
    ["Replays dropped", qty.format(sync.duplicatesDropped)],
    ["Dead letters", qty.format(sync.deadLetters)],
  ]
    .map(([k, v]) => `<tr><td>${k}</td><td>${v}</td></tr>`)
    .join("");
  $("session").innerHTML =
    `<div class="report-block"><table class="risk"><tbody>${rows}</tbody></table></div>`;
}

/* The two consumer paths are independent; say when they describe different stream positions
   instead of letting a half-updated screen pass as consistent. */
function renderSync(sync) {
  const el = $("sync");
  if (sync.coherent) {
    el.textContent = "in sync";
    el.className = "pos";
  } else {
    el.textContent = "catching up";
    el.className = "neg";
  }
}

/* Dead letters are rare and demand an operator's eye, so the flag exists only when nonzero. */
function renderDeadLetters(sync) {
  const item = $("dlt-item");
  item.hidden = !(sync.deadLetters > 0);
  $("dlt").textContent = String(sync.deadLetters);
}

const MARK_STALE_MILLIS = 10 * 60 * 1000;

function renderMarkAge() {
  const el = $("mark-age");
  const ts = lastSnapshot?.positions[0]?.lastTimeMillis;
  if (ts == null) {
    el.textContent = "—";
    el.className = "";
    return;
  }
  const age = Date.now() - ts;
  const minutes = Math.floor(age / 60000);
  el.textContent =
    minutes < 1
      ? "fresh"
      : minutes < 60
        ? `${minutes}m old`
        : `${Math.floor(minutes / 60)}h old`;
  el.className = age > MARK_STALE_MILLIS ? "neg" : "pos";
}

let updates = 0;
let lastSnapshot = null;

function render(snapshot) {
  updates++;
  lastSnapshot = snapshot;
  $("updates").textContent = String(updates);
  renderStats(snapshot);
  renderPositions(snapshot.positions);
  renderReport(snapshot.book);
  renderExposure(snapshot.exposure);
  renderSession(snapshot);
  renderSync(snapshot.sync);
  renderDeadLetters(snapshot.sync);
  renderMarkAge();
}

/* The mark ages between fills; keep its age truthful even when the stream is quiet. */
setInterval(renderMarkAge, 30000);

function connect() {
  const stream = new EventSource("api/stream");
  stream.onopen = () => {
    $("stream").textContent = "live";
  };
  stream.onerror = () => {
    $("stream").textContent = "reconnecting";
  };
  stream.onmessage = (event) => render(JSON.parse(event.data));
}

connect();
