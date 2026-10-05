"use strict";
const API = window.HOOKRELAY_API, $ = (s) => document.querySelector(s);
let token = sessionStorage.getItem("hr_token"), timer = null, route = "";
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const badge = (s) => `<span class="badge b-${esc(s)}">${esc(s)}</span>`;
const fmtT = (t) => (t ? new Date(t).toLocaleTimeString([], { hour12: false }) + "." + String(new Date(t).getMilliseconds()).padStart(3, "0") : "—");
const ms = (v) => (v == null ? "—" : v >= 1000 ? (v / 1000).toFixed(2) + "s" : Math.round(v) + "ms");
const pct = (v) => (v == null ? "—" : (v * 100).toFixed(1) + "%");
const short = (id) => esc(String(id).slice(0, 8));

async function api(path, opts = {}) {
  const r = await fetch(API + path, { ...opts, headers: { "Content-Type": "application/json", ...(token ? { Authorization: "Bearer " + token } : {}), ...(opts.headers || {}) } });
  if (r.status === 401 && path !== "/auth/login") { logout(); throw new Error("session expired"); }
  const ct = r.headers.get("content-type") || "";
  const body = r.status === 204 ? null : ct.includes("json") ? await r.json() : await r.text();
  if (!r.ok) throw new Error(body && body.error ? `${body.error.message} (${body.error.code})` : "HTTP " + r.status);
  return body;
}
function logout() { token = null; sessionStorage.removeItem("hr_token"); show(); }
function show() { $("#login").classList.toggle("hidden", !!token); $("#app").classList.toggle("hidden", !token); if (token) render(); }
$("#loginForm").addEventListener("submit", async (e) => {
  e.preventDefault(); $("#loginErr").textContent = "";
  try { const t = await api("/auth/login", { method: "POST", body: JSON.stringify({ email: $("#email").value, password: $("#password").value }) });
    token = t.access_token; sessionStorage.setItem("hr_token", token); show(); } catch (err) { $("#loginErr").textContent = err.message; }
});
$("#logout").addEventListener("click", logout);
window.addEventListener("hashchange", render);

function setView(html) { $("#view").innerHTML = html; }
function loop(fn, every) { clearInterval(timer); fn(); if (every) timer = setInterval(fn, every); }
function nav() { document.querySelectorAll("nav a").forEach((a) => a.classList.toggle("active", route.startsWith(a.getAttribute("href").slice(2)))); }
async function ready() {
  try { const r = await fetch(API + "/ready"); const j = await r.json(); $("#conn").className = "badge " + (r.ok ? "b-HEALTHY" : "b-UNHEALTHY");
    $("#conn").textContent = r.ok ? "API ready" : "not ready: " + Object.entries(j.checks).filter(([, v]) => v !== "ok").map(([k]) => k).join(",");
  } catch { $("#conn").className = "badge b-UNHEALTHY"; $("#conn").textContent = "API unreachable"; }
}

async function render() {
  if (!token) return;
  const [r, a] = (location.hash.slice(2) || "overview").split("/"); route = r; nav(); ready();
  clearInterval(timer);
  const pages = { overview, events, deliveries: () => (a ? deliveryDetail(a) : deliveries()), endpoints, lab, experiments: () => (a ? experimentDetail(a) : experiments()), tenants, metrics };
  try { await (pages[r] || overview)(); } catch (e) { setView(`<div class="notice">${esc(e.message)}</div>`); }
}

function chart(rows) {
  if (!rows.length) return `<p class="muted">No traffic in the last 30 minutes.</p>`;
  const W = 700, H = 120, max = Math.max(1, ...rows.map((r) => Math.max(r.events, r.attempts_ok + r.attempts_failed))), bw = W / rows.length;
  const bars = rows.map((r, i) => { const x = i * bw, h1 = (r.attempts_ok / max) * (H - 18), h2 = (r.attempts_failed / max) * (H - 18), h3 = (r.events / max) * (H - 18);
    return `<rect x="${x + 1}" y="${H - 14 - h1}" width="${bw * .4}" height="${h1}" fill="var(--ok)"/><rect x="${x + 1}" y="${H - 14 - h1 - h2}" width="${bw * .4}" height="${h2}" fill="var(--bad)"/><rect x="${x + 1 + bw * .45}" y="${H - 14 - h3}" width="${bw * .4}" height="${h3}" fill="var(--accent)"/>`; }).join("");
  return `<svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}">${bars}<text x="0" y="${H - 2}">${fmtT(rows[0].minute).slice(0, 5)}</text><text x="${W - 30}" y="${H - 2}">${fmtT(rows[rows.length - 1].minute).slice(0, 5)}</text></svg>
  <div class="small muted"><span class="ok">■</span> successful attempts &nbsp;<span class="bad">■</span> failed attempts &nbsp;<span style="color:var(--accent)">■</span> events ingested (per minute)</div>`;
}

async function overview() {
  loop(async () => {
    const [o, ts] = await Promise.all([api("/stats/overview"), api("/stats/timeseries")]);
    const d = o.deliveries_by_status, tile = (k, v, c = "") => `<div class="tile"><div class="k">${k}</div><div class="v ${c}">${v}</div></div>`;
    setView(`<h1>Overview</h1><p class="muted">Delivery is at-least-once: receivers must deduplicate on the event id. PostgreSQL is the source of truth.</p>
    <div class="grid tiles">${tile("events / sec (1m)", o.events_per_sec)}${tile("events (1h)", o.events_last_hour)}${tile("attempt success (1h)", pct(o.delivery_success_rate))}
    ${tile("retry rate (1h)", pct(o.retry_rate))}${tile("p50 / p95 / p99", `${ms(o.latency_ms.p50)} / ${ms(o.latency_ms.p95)} / ${ms(o.latency_ms.p99)}`).replace('class="v "', 'class="v" style="font-size:15px"')}
    ${tile("dead deliveries", o.dead, o.dead ? "bad" : "")}${tile("redis queue depth", o.queue_depth ?? "n/a")}${tile("backlog in postgres", o.backlog_in_postgres)}${tile("active workers", o.active_workers, o.active_workers ? "" : "bad")}</div>
    <h2>Throughput (last 30 min)</h2><div class="card">${chart(ts)}</div>
    <h2>Deliveries by status</h2><div class="row">${["PENDING", "QUEUED", "PROCESSING", "RETRYING", "DELIVERED", "DEAD"].map((s) => `${badge(s)} ${d[s] || 0}`).join(" &nbsp; ")}</div>
    <h2>Endpoint health &amp; circuit breakers</h2><table><tr><th>endpoint</th><th>health</th><th>circuit</th><th>consecutive failures</th></tr>
    ${o.endpoints.map((e) => `<tr><td class="mono">${esc(e.url)}</td><td>${badge(e.health)}</td><td>${badge(e.circuit.state)}</td><td>${e.circuit.failures}</td></tr>`).join("") || `<tr><td colspan=4 class="muted">No endpoints</td></tr>`}</table>
    <h2>Recent failed attempts</h2><table><tr><th>time</th><th>delivery</th><th>attempt</th><th>detail</th></tr>
    ${o.recent_failures.map((f) => `<tr class="click" onclick="location.hash='#/deliveries/${f.delivery_id}'"><td>${fmtT(f.at)}</td><td class="mono">${short(f.delivery_id)}</td><td>#${f.attempt}</td><td>${esc(f.detail)}</td></tr>`).join("") || `<tr><td colspan=4 class="muted">None</td></tr>`}</table>`);
  }, 5000);
}

async function events() {
  const e = await api("/events?limit=100");
  setView(`<h1>Events</h1><table><tr><th>created</th><th>event</th><th>type</th><th>idempotency key</th><th>deliveries</th></tr>${e.items.map((x) => `<tr><td>${fmtT(x.created_at)}</td><td class="mono">${short(x.id)}</td><td>${esc(x.type)}</td><td class="mono">${esc(x.idempotency_key)}</td>
  <td>${Object.entries(x.deliveries).map(([s, n]) => badge(s) + " " + n).join(" ") || '<span class="muted">none (no matching endpoint)</span>'}</td></tr>`).join("")}</table>`);
}

async function deliveries() {
  const f = new URLSearchParams(location.search).get("s") || "";
  const d = await api("/deliveries?limit=100" + (f ? "&status=" + f : ""));
  setView(`<h1>Deliveries</h1><div class="row">${["", "PENDING", "QUEUED", "PROCESSING", "RETRYING", "DELIVERED", "DEAD"].map((s) => `<a class="btn small" href="?s=${s}#/deliveries" style="text-decoration:none">${s || "ALL"}</a>`).join("")}</div><br>
  <table><tr><th>created</th><th>delivery</th><th>status</th><th>attempts</th><th>last status</th><th>last error</th><th>next attempt</th></tr>
  ${d.items.map((x) => `<tr class="click" onclick="location.hash='#/deliveries/${x.id}'"><td>${fmtT(x.created_at)}</td><td class="mono">${short(x.id)}${x.replay_of ? " ↻" : ""}</td><td>${badge(x.status)}</td><td>${x.attempt_count}/${x.max_attempts}</td><td>${x.last_status_code ?? "—"}</td><td>${esc(x.last_error || "")}</td><td>${fmtT(x.next_attempt_at)}</td></tr>`).join("")}</table>`);
}

async function deliveryDetail(id) {
  loop(async () => {
    const d = await api("/deliveries/" + id), steps = [];
    steps.push(["ok", "Event created", d.event.created_at, `${esc(d.event.type)} · event ${short(d.event.id)}`]);
    steps.push(["info", d.replay_of ? "Replay delivery created" : "Delivery created (PENDING)", d.created_at, d.replay_of ? `replay of <a href="#/deliveries/${d.replay_of}">${short(d.replay_of)}</a>` : ""]);
    d.attempts.forEach((a) => {
      steps.push(["info", `Attempt #${a.attempt_number}`, a.started_at, `timeout ${a.timeout_s}s`]);
      steps.push([a.error_class === "SUCCESS" ? "ok" : a.error_class === "PERMANENT" ? "bad" : "warn", a.http_status ? `HTTP ${a.http_status}` : a.error_detail, a.completed_at, `${ms(a.duration_ms)} · ${a.error_class} · ${esc(a.retry_decision)}`]);
      if (a.will_retry) steps.push(["warn", "Retry scheduled", a.completed_at, `in ${a.retry_delay_s.toFixed(1)}s (exponential backoff + jitter)`]);
    });
    d.audit.forEach((a) => steps.push(["info", a.action, a.at, esc(JSON.stringify(a.data || {}))]));
    const term = { DELIVERED: ["ok", "Delivered"], DEAD: ["bad", "Dead-lettered"] }[d.status];
    if (term) steps.push([term[0], term[1], d.delivered_at || d.attempts.at(-1)?.completed_at, d.last_error ? esc(d.last_error) : ""]);
    else steps.push(["info", `Currently ${d.status}`, null, d.next_attempt_at ? "next attempt " + fmtT(d.next_attempt_at) : esc(d.last_error || "")]);
    const t0 = new Date(d.event.created_at).getTime();
    setView(`<h1>Delivery <span class="mono">${short(d.id)}</span> ${badge(d.status)}</h1><p class="muted mono">${esc(d.id)} → endpoint ${short(d.endpoint_id)} · attempts ${d.attempt_count}/${d.max_attempts}</p>
    ${d.status === "DEAD" ? `<button class="btn primary" id="replay">Replay (creates a new delivery; history is preserved)</button>` : ""}
    ${d.replays.map((r) => `<p>Replayed as <a href="#/deliveries/${r.id}">${short(r.id)}</a> ${badge(r.status)}</p>`).join("")}
    <h2>Delivery trace</h2><div class="trace">${steps.map(([c, t, at, det]) => `<div class="step ${c}"><b>${esc(t)}</b><span class="t">${at ? fmtT(at) + " (+" + ms(new Date(at).getTime() - t0) + ")" : ""}</span><div class="d">${det}</div></div>`).join("")}</div>`);
    const b = $("#replay"); if (b) b.onclick = async () => { const r = await api(`/deliveries/${id}/replay`, { method: "POST" }); location.hash = "#/deliveries/" + r.new_delivery_id; };
  }, 3000);
}

async function endpoints() {
  const e = await api("/endpoints");
  setView(`<h1>Endpoints</h1><table><tr><th>url</th><th>events</th><th>health</th><th>circuit</th><th>rate limit</th><th></th></tr>${e.items.map((x) => `<tr><td class="mono">${esc(x.url)}<div class="muted">${esc(x.description || "")}</div></td><td>${esc(x.event_types.join(", "))}</td><td>${badge(x.health)}</td><td>${badge(x.circuit.state)} <span class="muted">${x.circuit.failures} fails</span></td><td>${x.rate_limit_per_sec ? x.rate_limit_per_sec + "/s" : "—"}</td>
  <td><button class="btn small" data-reset="${x.id}">Reset circuit</button> <button class="btn small danger" data-del="${x.id}">Delete</button></td></tr>`).join("")}</table>
  <h2>Register endpoint</h2><form id="epForm" class="card row"><input id="epUrl" placeholder="https://example.com/webhooks" size="40" required><input id="epRate" placeholder="rate limit /s (optional)" size="22"><button class="btn primary">Create</button></form>
  <div id="epMsg"></div><p class="muted small">Destinations are SSRF-checked: loopback, private, link-local and metadata addresses are rejected (the demo stack relaxes this only for the mock receiver).</p>`);
  document.querySelectorAll("[data-reset]").forEach((b) => (b.onclick = async () => { await api("/endpoints/" + b.dataset.reset, { method: "PATCH", body: JSON.stringify({ reset_circuit: true }) }); endpoints(); }));
  document.querySelectorAll("[data-del]").forEach((b) => (b.onclick = async () => { await api("/endpoints/" + b.dataset.del, { method: "DELETE" }); endpoints(); }));
  $("#epForm").onsubmit = async (ev) => { ev.preventDefault();
    try { const body = { url: $("#epUrl").value }; if ($("#epRate").value) body.rate_limit_per_sec = parseFloat($("#epRate").value);
      const c = await api("/endpoints", { method: "POST", body: JSON.stringify(body) });
      $("#epMsg").innerHTML = `<div class="notice">Signing secret (shown once): <span class="mono">${esc(c.signing_secret)}</span></div>`; } catch (err) { $("#epMsg").innerHTML = `<div class="notice bad">${esc(err.message)}</div>`; } };
}

async function lab() {
  const t = await api("/experiments/types");
  setView(`<h1>Reliability Laboratory</h1><p class="muted">Each experiment injects a controlled, scoped fault, records a timeline, then verifies system invariants from real data. Only one runs at a time. Experiments use isolated lab tenants.</p>
  <div class="grid exp-grid">${t.map((x) => `<div class="card"><b>${esc(x.type)}</b><p>${esc(x.description)}</p><button class="btn primary small" data-run="${x.type}">Run experiment</button></div>`).join("")}</div><div id="labMsg"></div>`);
  document.querySelectorAll("[data-run]").forEach((b) => (b.onclick = async () => { b.disabled = true;
    try { const e = await api("/experiments", { method: "POST", body: JSON.stringify({ type: b.dataset.run }) }); await api(`/experiments/${e.id}/start`, { method: "POST" }); location.hash = "#/experiments/" + e.id; }
    catch (err) { $("#labMsg").innerHTML = `<div class="notice">${esc(err.message)}</div>`; b.disabled = false; } }));
}

async function experiments() {
  loop(async () => { const e = await api("/experiments?limit=100");
    setView(`<h1>Experiments</h1><table><tr><th>started</th><th>type</th><th>status</th><th>verdict</th><th>id</th></tr>${e.items.map((x) => `<tr class="click" onclick="location.hash='#/experiments/${x.id}'"><td>${fmtT(x.started_at || x.created_at)}</td><td>${esc(x.type)}</td><td>${badge(x.status)}</td><td>${x.verdict ? badge(x.verdict) : "—"}</td><td class="mono">${short(x.id)}</td></tr>`).join("") || `<tr><td colspan=5 class="muted">No experiments yet — run one from the Reliability Lab.</td></tr>`}</table>`); }, 4000);
}

async function experimentDetail(id) {
  loop(async () => {
    const e = await api("/experiments/" + id), r = e.result, t0 = e.timeline[0] ? new Date(e.timeline[0].ts).getTime() : 0;
    const kindc = { fault: "bad", recovered: "ok", recovery: "ok", end: "ok", timeout: "bad", circuit: "warn" };
    setView(`<h1>${esc(e.type)} ${badge(e.status)} ${e.verdict ? badge(e.verdict) : ""}</h1><p class="muted">${esc(e.description)}</p>
    <h2>Failure timeline</h2><div class="trace">${e.timeline.map((x) => `<div class="step ${kindc[x.kind] || "info"}"><b>${esc(x.message)}</b><span class="t">${fmtT(x.ts)} (+${((new Date(x.ts).getTime() - t0) / 1000).toFixed(1)}s)</span></div>`).join("") || '<span class="muted">waiting…</span>'}</div>
    ${r && r.invariants ? `<h2>Invariants</h2><table><tr><th>inv</th><th>check</th><th>result</th><th>evidence (measured)</th></tr>${r.invariants.map((i) => `<tr><td>${esc(i.invariant)}</td><td>${esc(i.name)}</td><td>${badge(i.passed ? "PASS" : "FAIL")}</td><td>${esc(i.detail)}</td></tr>`).join("")}</table>
    <h2>Metrics (read back from PostgreSQL after the run)</h2><div class="grid tiles">${Object.entries(r.metrics).filter(([, v]) => typeof v !== "object").map(([k, v]) => `<div class="tile"><div class="k">${esc(k.replaceAll("_", " "))}</div><div class="v">${esc(v)}</div></div>`).join("")}
    <div class="tile"><div class="k">latency p50 / p95 / max</div><div class="v" style="font-size:15px">${ms(r.metrics.latency_ms?.p50)} / ${ms(r.metrics.latency_ms?.p95)} / ${ms(r.metrics.latency_ms?.max)}</div></div></div>
    <p class="muted small">Injected: ${esc(r.injected_failure)} · duration ${r.duration_s}s · ${esc(r.started_at)}</p>` : r && r.error ? `<div class="notice">Experiment failed to execute: ${esc(r.error)} ${esc(r.message || "")}</div>` : ""}`);
    if (e.status !== "RUNNING" && e.status !== "CREATED") clearInterval(timer);
  }, 2000);
}

async function tenants() {
  loop(async () => { const t = await api("/tenants"), mx = Math.max(1, ...t.items.map((x) => x.queue_depth));
    setView(`<h1>Tenants &amp; fairness</h1><p class="muted">Fairness: a per-tenant cap on concurrent in-flight deliveries (Redis counter) keeps one noisy tenant from monopolising the worker pool.</p>
    <table><tr><th>tenant</th><th>queue depth</th><th></th><th>in flight</th><th>events/s</th><th>delivered (1h)</th><th>dead</th><th>throttled (total)</th></tr>${t.items.map((x) => `<tr><td>${esc(x.name)} ${x.is_lab ? badge("LAB") : ""}</td><td>${x.queue_depth}</td><td style="width:140px"><div class="bar"><i style="width:${(x.queue_depth / mx) * 100}%"></i></div></td><td>${x.active_now ?? x.processing}</td><td>${x.events_per_sec}</td><td>${x.delivered_last_hour}</td><td>${x.dead}</td><td>${x.throttled_total ?? "—"}</td></tr>`).join("")}</table>`); }, 4000);
}

async function metrics() {
  loop(async () => { const txt = await (await fetch(API + "/metrics")).text();
    const rows = txt.split("\n").filter((l) => l.startsWith("hookrelay_") && !l.includes("_bucket") && !l.includes("_created")).map((l) => { const i = l.lastIndexOf(" "); return [l.slice(0, i), l.slice(i + 1)]; });
    setView(`<h1>Metrics</h1><p class="muted">API-process metrics from <span class="mono">/metrics</span>. Worker metrics are exposed on :9101 and both are scraped by Prometheus (<a href="${window.HOOKRELAY_PROM}" target="_blank">open Prometheus</a>).</p>
    <table><tr><th>series</th><th>value</th></tr>${rows.map(([k, v]) => `<tr><td class="mono">${esc(k)}</td><td>${esc(v)}</td></tr>`).join("")}</table>`); }, 5000);
}
show();
