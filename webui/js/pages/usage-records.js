// Page: 使用记录 — Beijing-aligned token usage ranges (GET /api/usage) with the
// model/endpoint mix and the per-request ledger.
//
// Ported from the local fork's single-file page (app.js:585-1406). Three display
// conventions are kept verbatim:
//   * compact 万/亿 numbers everywhere, with the exact value in the tooltip;
//   * a hover breakdown on the Token cell instead of four numeric columns;
//   * the effort / endpoint / session / TTFT columns that overview's one-hour
//     window cannot produce.
//
// Two hazards shaped the code:
//   * every DOM id is prefixed uq- — the unprefixed names (u-total, usage-trend,
//     usage-models, usage-tiers) already exist inside #page-overview, and
//     querySelector returns the first match in document order, so reusing them
//     would leave this page reading "—" *and* corrupt overview.js;
//   * the admin CSP is style-src 'self' with no 'unsafe-inline', so no node is
//     ever given a style attribute: colours and geometry go through the CSSOM
//     (node.style.foo = …), which CSP does not cover.
import { api } from "../api.js";
import * as store from "../store.js";
import { $, $$, el, emptyRow, formatNumber, localTime, toast } from "../ui.js";
import { onChange, t } from "../i18n.js";

const ROOT = "#page-usage-records";
const REFRESH_MS = 30000;
const SVG_NS = "http://www.w3.org/2000/svg";

let granularity = "day";
let rangeOffset = 0;
let usageData = null;
let loading = false;
// Non-empty once a request has failed: separates "not here yet" from "not
// coming", so the page never falls back to a mismatched window to look alive.
let loadError = "";
let lastFetch = 0;
let metricMode = "tokens";
let requestPage = 1;
let requestPageSize = 20;

const USAGE_COUNTS = { hour: 24, day: 30, week: 12, month: 12 };
const GRAN_LABELS = { hour: "小时", day: "天", week: "周", month: "月" };
const ENDPOINTS_OF_INTEREST = new Set([
  "/v1/chat/completions",
  "/v1/responses",
  "/v1/messages",
]);
const RANGE_LABELS = { "1h": "最近1小时", "6h": "最近6小时" };
const RANGE_DEFAULT = "最近24小时";

const DONUT_PALETTE = [
  "#153c34",
  "#ed5a2a",
  "#e7b94f",
  "#204d43",
  "#68716c",
  "#8a9a93",
  "#b6382f",
  "#237056",
];
const SESSION_PALETTE = [
  "#c2410c",
  "#6d28d9",
  "#1d4ed8",
  "#0f766e",
  "#a21caf",
  "#92400e",
  "#4d7c0f",
  "#be123c",
];

// --- formatting -------------------------------------------------------------
// Compact numbers only serve this page; they keep one decimal so the bar-top
// label still fits the bar width, and formatTokensExact backs them up in the
// cell title and the hover breakdown.
const compactTokens = (value) => {
  let n = Number(value || 0);
  if (Number.isNaN(n)) return "0";
  if (!isFinite(n)) return "∞";
  n = Math.round(n);
  if (n >= 1e8 || Number((n / 1e4).toFixed(1)) >= 10000) return `${(n / 1e8).toFixed(1)}亿`;
  if (n >= 1e4) return `${(n / 1e4).toFixed(1)}万`;
  return n.toLocaleString();
};

// Token cell: 1.0k → 1k, 12.0k → 12k, so at most one decimal is shown.
const kTokens = (value) => {
  let n = Number(value || 0);
  if (Number.isNaN(n)) return "0";
  if (!isFinite(n)) return "∞";
  n = Math.round(n);
  const sign = n < 0 ? "-" : "";
  const a = Math.abs(n);
  if (a >= 1000) return `${sign}${(a / 1000).toFixed(1).replace(/\.?0+$/, "")}k`;
  return sign + a.toLocaleString();
};

const exactTokens = (value) => {
  const n = Number(value || 0);
  return Number.isFinite(n) ? n.toLocaleString() : "—";
};

const hitRate = (cached, input) => {
  const c = Number(cached || 0);
  const i = Number(input || 0);
  if (!(i > 0) || !Number.isFinite(c) || !Number.isFinite(i)) return "—";
  return `${Math.min(100, Math.max(0, (c / i) * 100)).toFixed(1)}%`;
};

const formatUSD = (value) => {
  const number = Number(value || 0);
  if (!isFinite(number) || number === 0) return "$0";
  if (Math.abs(number) < 0.01) return `$${number.toFixed(4)}`;
  return (
    "$" + number.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
  );
};

// Known-free ($0) versus unknown-price: unknown models record $0 upstream and
// set unknown_cost, so a bare $0 would conflate the two.
const formatCost = (value, unknown) => {
  if (unknown && !(Number(value) > 0)) return "—";
  return formatUSD(value);
};

const costTitle = (value, unknown) => {
  const base = `预估成本 ${formatUSD(value)}（按记录时 models.dev 价格）`;
  return unknown ? `${base}；含未知价格模型，实际可能更高` : base;
};

// Durations render in seconds (m/s past a minute) instead of raw milliseconds.
const formatDurationMS = (value) => {
  if (value == null) return "—";
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return "—";
  const ds = Math.round(n / 100);
  if (ds >= 36000) {
    const h = Math.floor(ds / 36000);
    const m = Math.floor((ds - h * 36000) / 600);
    return `${h}h${m}m${(ds - h * 36000 - m * 600) / 10}s`;
  }
  if (ds >= 600) {
    const m = Math.floor(ds / 600);
    return `${m}m${(ds - m * 600) / 10}s`;
  }
  return `${ds / 10}s`;
};

// Display-only alias beautifying; filtering still compares the raw lower-case
// value the server reported.
const formatEffort = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "—";
  const n = raw.toLowerCase().replace(/[-_\s]/g, "");
  const known = {
    low: "Low",
    medium: "Medium",
    high: "High",
    xhigh: "XHigh",
    extrahigh: "XHigh",
    max: "Max",
    minimal: "Minimal",
  };
  if (known[n]) return known[n];
  if (n === "none") return "—";
  return raw.length > 1 ? raw[0].toUpperCase() + raw.slice(1) : raw.toUpperCase();
};

const formatFirstByte = (value) => {
  const n = Number(value || 0);
  if (!(n > 0)) return "—";
  return formatDurationMS(n);
};

// The duration column shows total − first byte. Non-streaming requests keep the
// total: their first byte and completion coincide, so subtracting gives a
// meaningless zero.
const durationExcludingFirstByte = (total, first, stream) => {
  if (total == null) return null;
  const t = Number(total);
  const f = Number(first || 0);
  if (!Number.isFinite(t) || t < 0) return null;
  if (!stream || !(f > 0)) return t;
  return Math.max(0, t - f);
};

const durationExcludingFirstByteTitle = (total, first, stream) => {
  if (total == null) return "";
  const t = Number(total);
  const f = Number(first || 0);
  if (!Number.isFinite(t) || t < 0) return "";
  if (!stream || !(f > 0)) return `总耗时 ${formatDurationMS(t)}（非流式无 TTFT）`;
  return `总耗时 ${formatDurationMS(t)} · 首字 ${formatDurationMS(f)} · 已减首字`;
};

const beijingDay = (iso) =>
  new Date(iso).toLocaleDateString("zh-CN", {
    timeZone: "Asia/Shanghai",
    month: "2-digit",
    day: "2-digit",
  });

const beijingHour = (iso) =>
  new Date(iso).toLocaleString("zh-CN", {
    timeZone: "Asia/Shanghai",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    hour12: false,
  });

const pointLabel = (iso) => (granularity === "hour" ? beijingHour(iso) : beijingDay(iso));

const axisTickIndex = (n, max) => {
  const cap = max || 12;
  const count = Math.min(Math.max(n || 0, 0), cap);
  const set = new Set();
  if (n <= 0) return set;
  if (count <= 1) {
    set.add(0);
    return set;
  }
  for (let k = 0; k < count; k++) set.add(Math.round((k * (n - 1)) / (count - 1)));
  return set;
};

const axisLabel = (iso) =>
  granularity === "hour"
    ? new Date(iso).toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai",
        hour: "2-digit",
        hour12: false,
      })
    : beijingDay(iso);

const rangeTotal = () => (usageData && usageData.total) || null;
const sessionIDOf = (request) => (request && request.session) || "";

// --- small DOM helpers ------------------------------------------------------
function appendCell(row, value, className = "", title = "") {
  const props = { text: String(value) };
  if (className) props.class = className;
  if (title) props.title = title;
  row.append(el("td", props));
}

function cacheIcon() {
  const svg = document.createElementNS(SVG_NS, "svg");
  svg.setAttribute("viewBox", "0 0 16 16");
  svg.setAttribute("fill", "none");
  svg.setAttribute("stroke", "currentColor");
  svg.setAttribute("stroke-width", "1.6");
  svg.setAttribute("aria-hidden", "true");
  const cap = document.createElementNS(SVG_NS, "ellipse");
  cap.setAttribute("cx", "8");
  cap.setAttribute("cy", "3.6");
  cap.setAttribute("rx", "5.6");
  cap.setAttribute("ry", "2.2");
  const wall = document.createElementNS(SVG_NS, "path");
  wall.setAttribute("d", "M2.4 3.6v8.8c0 1.2 2.5 2.2 5.6 2.2s5.6-1 5.6-2.2V3.6");
  const seam = document.createElementNS(SVG_NS, "path");
  seam.setAttribute("d", "M2.4 8c0 1.2 2.5 2.2 5.6 2.2s5.6-1 5.6-2.2");
  svg.append(cap, wall, seam);
  return svg;
}

const tipRow = (label, value) =>
  el("span", {}, [el("span", { text: label }), el("span", { text: value })]);

// Token cell: fresh input ↓ / output ↑ on the first line, cache read + hit rate
// on the second, exact breakdown in the hover tip.
function appendTokenCell(row, tokens) {
  const cell = el("td");
  const total = Number(tokens.total_tokens || 0);
  if (!(total > 0)) {
    cell.textContent = "—";
    row.append(cell);
    return;
  }
  const input = Number(tokens.input_tokens || 0);
  const output = Number(tokens.output_tokens || 0);
  const cached = Number(tokens.cached_tokens || 0);
  const reasoning = Number(tokens.reasoning_tokens || 0);
  const fresh = Math.max(0, input - cached);
  const hit = input > 0 ? Math.min(100, Math.max(0, (cached / input) * 100)) : null;
  const tone =
    hit == null
      ? ""
      : hit >= 95
        ? "hit95"
        : hit >= 85
          ? "hit85"
          : hit >= 70
            ? "hit70"
            : hit >= 50
              ? "hit50"
              : "";
  const line1 = el("span", { class: "tok-l1" }, [
    el("i", { class: "a-dn", text: "↓" }),
    kTokens(fresh),
    " ",
    el("i", { class: "a-up", text: "↑" }),
    kTokens(output),
  ]);
  const line2 = el("span", { class: "tok-l2" }, [cacheIcon(), kTokens(cached)]);
  if (hit != null) {
    line2.append(" ", el("span", { class: `tok-hit ${tone}`.trim(), text: `${hit.toFixed(1)}%` }));
  }
  const tip = el("span", { class: "tok-tip" }, [
    el("b", { text: "Token 明细" }),
    tipRow("输入 Token", exactTokens(fresh)),
    tipRow("输出 Token", exactTokens(output)),
    tipRow("缓存读取 Token", exactTokens(cached)),
    tipRow("推理 Token", exactTokens(reasoning)),
    el("span", { class: "tt-total" }, [
      el("span", { text: "总 Token" }),
      el("span", { text: exactTokens(total) }),
    ]),
    tipRow("缓存命中率", hit == null ? "—" : `${hit.toFixed(1)}%`),
  ]);
  cell.append(el("span", { class: "tok" }, [line1, line2, tip]));
  row.append(cell);
}

// Attempts: 0 means the request never left (no node, or rejected locally).
function attemptsCell(request) {
  const attempts = Number(request.attempts);
  if (!Number.isFinite(attempts) || attempts < 0) return { text: "—", tone: "", title: "" };
  if (attempts === 0) {
    return { text: "0", tone: "", title: "未发出上游尝试：无可用节点，或请求在本地被拒" };
  }
  if (attempts > 1) {
    return {
      text: String(attempts),
      tone: "warn",
      title: "上游尝试次数（含匿名 lane 与 key 回退；>1 表示触发过重试/回退）",
    };
  }
  return {
    text: String(attempts),
    tone: "",
    title: "上游尝试次数（含匿名 lane 与 key 回退；>1 表示触发过重试/回退）",
  };
}

// --- data -------------------------------------------------------------------
const isVisible = () => {
  const root = $(ROOT);
  return !!root && !root.classList.contains("hidden") && !document.hidden;
};

async function fetchUsage() {
  if (loading) return;
  loading = true;
  try {
    const count = USAGE_COUNTS[granularity] || 30;
    usageData = await api(
      `/api/usage?granularity=${granularity}&count=${count}&offset=${rangeOffset}`,
    );
    loadError = "";
    if (isVisible()) render();
  } catch (error) {
    loadError = error.message || String(error);
    // Clear the payload too: after a granularity switch a failure would leave
    // the previous window's numbers under the new window's heading.
    usageData = null;
    toast(error.message, true);
    if (isVisible()) render();
  } finally {
    loading = false;
    // Stamp on both paths — a permanently failing endpoint would otherwise be
    // retried on every 3s monitor tick.
    lastFetch = Date.now();
  }
}

// --- render -----------------------------------------------------------------
function render() {
  $$("[data-uq-gran]", $(ROOT)).forEach((button) =>
    button.classList.toggle("btn-primary", button.dataset.uqGran === granularity),
  );
  const title = $("#uq-trend-title");
  if (title) title.textContent = `按${GRAN_LABELS[granularity] || "天"} Token 趋势`;
  // When history persistence is disabled every number on this page is memory
  // only and dies with the process — say so instead of looking healthy.
  const warning = $("#uq-history-warning");
  if (warning) warning.classList.toggle("hidden", !(usageData && usageData.history_disabled));
  if (!usageData || !usageData.points) {
    showUnavailable();
    return;
  }
  const total = usageData.total || {};
  const tokens = total.tokens || {};
  $("#uq-coverage").textContent = `${((total.coverage || 0) * 100).toFixed(1)}%`;
  const totalTokens = $("#uq-total");
  totalTokens.textContent = compactTokens(tokens.total_tokens);
  totalTokens.title = exactTokens(tokens.total_tokens);
  $("#uq-total-note").textContent = `当前范围 · ${GRAN_LABELS[granularity] || ""}视图`;
  const cost = $("#uq-cost");
  cost.textContent = formatCost(total.cost_usd, total.unknown_cost);
  cost.title = costTitle(total.cost_usd, total.unknown_cost);
  $("#uq-requests").textContent = formatNumber(total.requests);
  const cached = $("#uq-cached");
  cached.textContent = compactTokens(tokens.cached_tokens);
  cached.title = exactTokens(tokens.cached_tokens);
  const reasoning = $("#uq-reasoning");
  reasoning.textContent = compactTokens(tokens.reasoning_tokens);
  reasoning.title = exactTokens(tokens.reasoning_tokens);
  $("#uq-hitrate").textContent = hitRate(tokens.cached_tokens, tokens.input_tokens);

  const label = $("#uq-range-label");
  if (label && usageData.from && usageData.to) {
    const beijingDate = (iso) =>
      new Date(iso).toLocaleDateString("zh-CN", { timeZone: "Asia/Shanghai" });
    const sameDay = granularity === "hour" && beijingDate(usageData.from) === beijingDate(usageData.to);
    const span = sameDay
      ? `${beijingHour(usageData.from)} ~ ${beijingHour(usageData.to)}`
      : `${beijingDay(usageData.from)} ~ ${beijingDay(usageData.to)}`;
    label.textContent =
      `${span} · 北京时间 · 共${usageData.points.length}个${GRAN_LABELS[granularity] || "点"}`;
  }
  const caption = $("#uq-trend-caption");
  if (caption) {
    caption.textContent =
      "北京时间对齐，柱顶数字为总量，下方时间轴对应各柱时间；悬停查看输入、输出、缓存、命中率与预估成本。";
  }
  renderTrend(usageData.points);
  renderModels(total);
  renderTiers(total);
  renderExtras();
}

function renderTrend(points) {
  const trend = $("#uq-trend");
  const maximum = Math.max(1, ...points.map((point) => point.tokens?.total_tokens || 0));
  const bars = el("div", { class: "trend-bars" });
  const tickIdx = axisTickIndex(
    points.length,
    granularity === "hour" || granularity === "day" ? points.length : 12,
  );
  points.forEach((point) => {
    const value = point.tokens?.total_tokens || 0;
    const bar = el("div", {
      class: "trend-bar",
      // Compact on the bar, exact in the tooltip: this pairing is the page's
      // rule everywhere else, so the trend is no longer the one exception that
      // makes the original value unrecoverable.
      title:
        `${pointLabel(point.start)} · input ${compactTokens(point.tokens?.input_tokens)}（${exactTokens(point.tokens?.input_tokens)}）` +
        ` · output ${compactTokens(point.tokens?.output_tokens)}（${exactTokens(point.tokens?.output_tokens)}）` +
        ` · cached ${compactTokens(point.tokens?.cached_tokens)}（${exactTokens(point.tokens?.cached_tokens)}）` +
        ` · 命中率 ${hitRate(point.tokens?.cached_tokens, point.tokens?.input_tokens)}` +
        ` · total ${compactTokens(value)}（${exactTokens(value)}） · ${formatCost(point.cost_usd, point.unknown_cost)}` +
        (point.unknown_cost ? "（含未知价格模型）" : ""),
    });
    bar.style.height = `${Math.max(2, Math.round((value / maximum) * 100))}%`;
    if (value > 0) bar.append(el("span", { class: "trend-value", text: compactTokens(value) }));
    bars.append(bar);
  });
  const axis = el("div", { class: "trend-axis" });
  points.forEach((point, index) => {
    axis.append(
      el("span", {
        class: "trend-tick",
        text: tickIdx.has(index) ? axisLabel(point.start) : "",
      }),
    );
  });
  trend.replaceChildren(bars, axis);
}

function renderModels(total) {
  const host = $("#uq-models");
  host.replaceChildren();
  const costs = total.model_costs || {};
  Object.entries(total.models || {})
    .sort((a, b) => (b[1].total_tokens || 0) - (a[1].total_tokens || 0))
    .slice(0, 15)
    .forEach(([model, value]) => {
      const row = el("tr");
      appendCell(row, model, "mono");
      appendCell(row, compactTokens(value.input_tokens), "num", exactTokens(value.input_tokens));
      appendCell(row, compactTokens(value.output_tokens), "num", exactTokens(value.output_tokens));
      appendCell(row, compactTokens(value.total_tokens), "num", exactTokens(value.total_tokens));
      appendCell(row, formatUSD(costs[model]), "cost");
      host.append(row);
    });
  if (!host.children.length) emptyRow(host, 5, "该范围内尚无 Token 用量");
}

function renderTiers(total) {
  const host = $("#uq-tiers");
  const costs = total.tier_costs || {};
  const entries = Object.entries(total.tiers || {}).sort(
    (a, b) => (b[1].total_tokens || 0) - (a[1].total_tokens || 0),
  );
  const tierTotal = Math.max(1, ...entries.map(([, value]) => value.total_tokens || 0));
  host.replaceChildren();
  entries.forEach(([tier, value]) => {
    const fill = el("div", { class: "tier-fill" });
    fill.style.width = `${Math.round(((value.total_tokens || 0) / tierTotal) * 100)}%`;
    const amount = el("span", {
      class: "mono",
      text: `${compactTokens(value.total_tokens)} · ${formatUSD(costs[tier])}`,
      title:
        `input ${compactTokens(value.input_tokens)}（${exactTokens(value.input_tokens)}）` +
        ` · output ${compactTokens(value.output_tokens)}（${exactTokens(value.output_tokens)}）` +
        ` · cached ${compactTokens(value.cached_tokens)}` +
        ` · 命中率 ${hitRate(value.cached_tokens, value.input_tokens)}` +
        ` · total ${compactTokens(value.total_tokens)}（${exactTokens(value.total_tokens)}）`,
    });
    host.append(
      el("div", { class: "tier-row" }, [
        el("strong", { text: tier }),
        el("div", { class: "tier-track" }, [fill]),
        amount,
      ]),
    );
  });
  if (!entries.length) host.append(el("div", { class: "empty", text: "该范围内尚无 Tier 用量" }));
}

// Only the slots /api/usage drives are cleared. Endpoint / channel / request
// sections come from the monitor payload and used to be wiped too, which made a
// usage-endpoint outage permanently mislabel healthy data as failed.
function showUnavailable() {
  const failed = !!loadError;
  const hint = failed ? "加载失败" : "加载中…";
  const body = failed ? "用量数据加载失败" : "正在加载用量数据…";
  for (const id of [
    "uq-coverage",
    "uq-total",
    "uq-cost",
    "uq-requests",
    "uq-cached",
    "uq-reasoning",
    "uq-hitrate",
  ]) {
    const node = $(`#${id}`);
    if (!node) continue;
    node.textContent = "—";
    node.title = "";
  }
  $("#uq-total-note").textContent = hint;
  const label = $("#uq-range-label");
  if (label) label.textContent = hint;
  const caption = $("#uq-trend-caption");
  if (caption) caption.textContent = body;
  $("#uq-trend").replaceChildren();
  // emptyRow only appends, so the host must be cleared first or every repaint
  // stacks one more row.
  const models = $("#uq-models");
  models.replaceChildren();
  emptyRow(models, 5, body);
  const tiers = $("#uq-tiers");
  if (tiers) {
    tiers.replaceChildren();
    tiers.append(el("div", { class: "empty", text: body }));
  }
  // renderExtras redraws the monitor-driven sections and lets paintDonut take
  // its empty branch. Do not clear the donut here: the gradient lives on
  // style.background, and dropping children would not reset it.
  renderExtras();
}

// --- donut / endpoint / channel / requests ----------------------------------
function paintDonut(donutId, legendId, rows) {
  const donut = $(donutId);
  const legend = $(legendId);
  if (!donut || !legend) return;
  const total = rows.reduce((sum, row) => sum + (Number(row.value) || 0), 0);
  if (!(total > 0)) {
    donut.style.background = "var(--surface-2)";
    legend.replaceChildren(el("div", { class: "empty", text: "暂无数据" }));
    return;
  }
  let acc = 0;
  const parts = rows.map((row, index) => {
    const start = (acc / total) * 360;
    acc += Number(row.value) || 0;
    const end = (acc / total) * 360;
    const color = DONUT_PALETTE[index % DONUT_PALETTE.length];
    return `${color} ${start.toFixed(1)}deg ${end.toFixed(1)}deg`;
  });
  donut.style.background = `conic-gradient(${parts.join(",")})`;
  legend.replaceChildren(
    ...rows.map((row, index) => {
      const dot = el("i", { class: "dot" });
      dot.style.background = DONUT_PALETTE[index % DONUT_PALETTE.length];
      return el("div", {}, [
        el("span", {}, [dot, row.label]),
        el("span", {
          class: "mono",
          text: `${((Number(row.value) / total) * 100).toFixed(1)}%`,
          title: String(row.hint || row.value),
        }),
      ]);
    }),
  );
}

function renderExtras() {
  try {
    renderModelDonut();
    renderEndpoints();
    renderChannels();
    renderRequests();
  } catch (error) {
    console.warn("renderUsageExtras failed", error);
  }
}

function renderModelDonut() {
  const tokenMode = $("#uq-metric-token");
  const costMode = $("#uq-metric-cost");
  if (tokenMode) tokenMode.classList.toggle("btn-primary", metricMode === "tokens");
  if (costMode) costMode.classList.toggle("btn-primary", metricMode === "cost");
  const total = rangeTotal();
  let rows = [];
  if (total && total.models) {
    const costs = total.model_costs || {};
    const mode = metricMode === "cost" && Object.keys(costs).length ? "cost" : "tokens";
    rows = Object.entries(total.models).map(([model, value]) => ({
      label: model,
      value: mode === "cost" ? Number(costs[model]) || 0 : Number(value.total_tokens) || 0,
    }));
  }
  // No fallback to the monitor's "last 60 minutes" usage: that window does not
  // match the granularity this page advertises, so it would draw a mislead.
  rows.sort((a, b) => b.value - a.value);
  paintDonut("#uq-model-donut", "#uq-model-legend", rows.slice(0, 8));
}

function renderEndpoints() {
  const body = $("#uq-endpoints");
  if (!body) return;
  const total = rangeTotal();
  body.replaceChildren();
  let rows = [];
  if (total && total.endpoints && Object.keys(total.endpoints).length) {
    const tokens = total.endpoint_tokens || {};
    const costs = total.endpoint_costs || {};
    rows = Object.entries(total.endpoints).map(([endpoint, count]) => ({
      endpoint,
      count,
      tokens: tokens[endpoint] || null,
      cost: costs[endpoint],
    }));
  } else {
    const metrics = store.get("monitor")?.metrics;
    if (metrics && metrics.endpoints) {
      rows = Object.entries(metrics.endpoints)
        .filter(([endpoint]) => ENDPOINTS_OF_INTEREST.has(endpoint))
        .map(([endpoint, count]) => ({ endpoint, count, tokens: null, cost: null }));
    }
  }
  rows.sort((a, b) => b.count - a.count);
  paintDonut(
    "#uq-endpoint-donut",
    "#uq-endpoint-legend",
    rows.map((row) => ({ label: row.endpoint, value: row.count })),
  );
  rows.slice(0, 15).forEach((row) => {
    const tr = el("tr");
    appendCell(tr, row.endpoint, "mono");
    appendCell(tr, formatNumber(row.count), "num");
    if (row.tokens) {
      appendCell(
        tr,
        compactTokens(row.tokens.total_tokens),
        "num",
        `${exactTokens(row.tokens.total_tokens)} · input ${exactTokens(row.tokens.input_tokens)} · output ${exactTokens(row.tokens.output_tokens)} · cached ${exactTokens(row.tokens.cached_tokens)}`,
      );
      appendCell(tr, row.cost == null ? "—" : formatUSD(row.cost), "cost");
    } else {
      appendCell(tr, "—", "num");
      appendCell(tr, "—", "cost");
    }
    body.append(tr);
  });
  if (!rows.length) emptyRow(body, 4, "该范围内尚无端点用量");
}

function renderChannels() {
  const host = $("#uq-channels");
  const monitor = store.get("monitor");
  if (!host || !monitor) return;
  const upstream = monitor.upstream || {};
  const recent = upstream.last_hour || upstream.window || {};
  const statuses = (monitor.metrics && monitor.metrics.statuses) || {};
  const lines = Object.entries(recent.channels || {})
    .sort((a, b) => b[1].total - a[1].total)
    .map(([channel, value]) => {
      const rate = value.total
        ? ` · 成功率 ${((value.success / value.total) * 100).toFixed(1)}%`
        : "";
      return `${channel}：${formatNumber(value.total)} 次${rate}`;
    });
  const statusLine = Object.entries(statuses)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)
    .map(([status, count]) => `${status} ${formatNumber(count)}`)
    .join(" · ");
  const pre = el("div", {
    text:
      (lines.length ? lines.join("\n") : "暂无通道数据") +
      (statusLine ? `\n状态码：${statusLine}` : ""),
  });
  pre.style.whiteSpace = "pre-wrap";
  host.replaceChildren(pre);
}

// Filters read the request detail that /api/usage ships with the queried range
// (newest first) — the monitor poll only ever carries one hour.
function filteredRequests() {
  const source = (usageData && usageData.requests) || [];
  const model = $("#uq-f-model")?.value || "";
  const tier = $("#uq-f-tier")?.value || "";
  const channel = $("#uq-f-channel")?.value || "";
  const status = $("#uq-f-status")?.value || "";
  const effort = $("#uq-f-effort")?.value || "";
  const query = ($("#uq-f-q")?.value || "").trim().toLowerCase();
  const range = $("#uq-f-range")?.value || "24h";
  const cutoff = Date.now() - (range === "1h" ? 3600000 : range === "6h" ? 21600000 : 86400000);
  return source
    .filter((request) => {
      const ts = Date.parse(request.time || "");
      if (!Number.isNaN(ts) && ts < cutoff) return false;
      if (model && request.model !== model) return false;
      if (effort && (request.reasoning_effort || "") !== effort) return false;
      if (tier && (request.tier || "") !== tier) return false;
      if (channel) {
        if (channel === "anonymous") {
          if (!request.anonymous && request.channel !== "anonymous") return false;
        } else if (channel === "key") {
          if (request.anonymous || request.channel === "anonymous" || request.channel === "not_routed")
            return false;
        } else if (request.channel !== channel) return false;
      }
      const ok = request.success || (request.status >= 200 && request.status < 400);
      if (status === "ok" && !ok) return false;
      if (status === "fail" && ok) return false;
      if (
        query &&
        !(
          (request.request_id || "").toLowerCase().includes(query) ||
          sessionIDOf(request).toLowerCase().includes(query) ||
          (request.model || "").toLowerCase().includes(query)
        )
      )
        return false;
      return true;
    })
    .slice(0, 500);
}

function syncSelect(id, values, allLabel, labelOf) {
  const select = $(`#${id}`);
  if (!select) return;
  const signature = values.join(" ");
  if (select.dataset.sig === signature) return;
  select.dataset.sig = signature;
  const current = select.value;
  select.replaceChildren(el("option", { value: "", text: allLabel }));
  for (const value of values) {
    select.append(el("option", { value, text: labelOf ? labelOf(value) : value }));
  }
  if (current && values.includes(current)) select.value = current;
}

function renderRequests() {
  const body = $("#uq-requests-body");
  if (!body) return;
  const requests = (usageData && usageData.requests) || [];
  syncSelect(
    "uq-f-model",
    [...new Set(requests.map((request) => request.model).filter(Boolean))].sort(),
    t("全部模型", "usage-records.filter.allModels"),
  );
  syncSelect(
    "uq-f-effort",
    [...new Set(requests.map((request) => request.reasoning_effort).filter(Boolean))].sort(),
    t("全部强度", "usage-records.filter.allEffort"),
    formatEffort,
  );
  const rows = filteredRequests();
  const sessionColor = new Map();
  let sessionNext = 0;
  for (const item of rows) {
    const key = sessionIDOf(item);
    if (key && !sessionColor.has(key)) sessionColor.set(key, sessionNext++ % SESSION_PALETTE.length);
  }
  body.replaceChildren();
  const total = rows.length;
  const pages = Math.max(1, Math.ceil(total / requestPageSize));
  if (requestPage > pages) requestPage = pages;
  const start = total === 0 ? 0 : (requestPage - 1) * requestPageSize + 1;
  const end = Math.min(total, requestPage * requestPageSize);
  rows
    .slice((requestPage - 1) * requestPageSize, requestPage * requestPageSize)
    .forEach((request) => {
      body.append(requestRow(request, sessionColor));
    });
  if (!rows.length) emptyRow(body, 15, "暂无匹配的请求路由");
  const range = $("#uq-f-range")?.value || "24h";
  const rangeLabel = RANGE_LABELS[range] || RANGE_DEFAULT;
  const count = $("#uq-req-count");
  if (count) count.textContent = `显示 ${start} 至 ${end} 共 ${total} 条结果（${rangeLabel}内最新 500 条）`;
  renderPager(pages);
}

function requestRow(request, sessionColor) {
  const row = el("tr");
  const rid = request.request_id || "";
  const tokens = request.tokens || {};
  const totalTokens = Number(tokens.total_tokens || 0);
  const ok = request.success || (request.status >= 200 && request.status < 400);
  appendCell(row, localTime(request.time));
  appendCell(row, rid.length > 6 ? `…${rid.slice(-6)}` : rid || "—", "mono", rid);
  appendCell(row, request.model || "—");
  appendCell(row, formatEffort(request.reasoning_effort), "", request.reasoning_effort || "");
  appendCell(row, request.endpoint || "—");
  appendCell(row, `${request.tier || "—"} / ${request.channel || "—"}`);
  appendCell(row, request.key_id || "—", "mono");
  // The stream column is three-state: the field is not omitempty, so "key
  // absent" uniquely means the record predates the field. Reading that as
  // "non-streaming" would mislabel historic streams, so it shows — instead.
  appendCell(
    row,
    request.stream === true ? "流式" : request.stream === false ? "同步" : "—",
    request.stream === true ? "ok" : "",
  );
  const sid = sessionIDOf(request);
  const conv = el("td", {
    class: sid ? "conv" : "",
    text: sid.length > 6 ? `…${sid.slice(-6)}` : sid || "—",
  });
  if (sid) {
    conv.style.cursor = "pointer";
    const colorIndex = sessionColor.get(sid);
    if (colorIndex != null) conv.style.color = SESSION_PALETTE[colorIndex];
    conv.title = `${sid}（点击筛选同对话）`;
    conv.addEventListener("click", () => {
      const query = $("#uq-f-q");
      if (!query) return;
      query.value = sid;
      requestPage = 1;
      renderRequests();
    });
  }
  row.append(conv);
  appendTokenCell(row, tokens);
  if (totalTokens > 0) {
    appendCell(row, request.cost_usd == null ? "—" : formatUSD(request.cost_usd), "cost");
  } else {
    appendCell(row, request.cost_usd ? formatUSD(request.cost_usd) : "—", "cost");
  }
  appendCell(row, request.status || request.outcome || "—", ok ? "ok" : "bad");
  const attempts = attemptsCell(request);
  appendCell(row, attempts.text, attempts.tone, attempts.title);
  appendCell(
    row,
    formatFirstByte(request.stream === true ? request.first_byte_ms : 0),
    "num",
    "客户端首字节耗时 · 仅流式有效（TTFT），非流式显示—",
  );
  appendCell(
    row,
    formatDurationMS(
      durationExcludingFirstByte(request.duration_ms, request.first_byte_ms, request.stream === true),
    ),
    "num",
    durationExcludingFirstByteTitle(
      request.duration_ms,
      request.first_byte_ms,
      request.stream === true,
    ),
  );
  return row;
}

function pagerList(current, pages) {
  const keep = new Set([1, pages, current - 1, current, current + 1]);
  const sorted = [...keep].filter((page) => page >= 1 && page <= pages).sort((a, b) => a - b);
  const out = [];
  let prev = 0;
  for (const page of sorted) {
    if (page - prev > 1) out.push(0);
    out.push(page);
    prev = page;
  }
  return out;
}

function renderPager(pages) {
  const host = $("#uq-pager");
  if (!host) return;
  const mk = (label, page, disabled, active) => {
    const button = el("button", {
      type: "button",
      text: label,
      disabled: !!disabled,
      class: active ? "on" : "",
    });
    button.addEventListener("click", () => {
      if (requestPage !== page) {
        requestPage = page;
        renderRequests();
      }
    });
    return button;
  };
  const nodes = [mk("<", Math.max(1, requestPage - 1), requestPage <= 1)];
  for (const page of pagerList(requestPage, pages)) {
    nodes.push(
      page === 0
        ? el("span", { class: "ell", text: "…" })
        : mk(String(page), page, false, page === requestPage),
    );
  }
  nodes.push(mk(">", Math.min(pages, requestPage + 1), requestPage >= pages));
  host.replaceChildren(...nodes);
}

// --- wiring -----------------------------------------------------------------
function resetFilters() {
  for (const id of ["uq-f-model", "uq-f-tier", "uq-f-channel", "uq-f-status", "uq-f-effort", "uq-f-q"]) {
    const node = $(`#${id}`);
    if (node) node.value = "";
  }
  const range = $("#uq-f-range");
  if (range) range.value = "24h";
  requestPage = 1;
  renderRequests();
}

export default {
  id: "usage-records",
  title: "使用记录",
  sub: "按小时 / 天 / 周 / 月的 Token 趋势、模型与端点分布，以及请求明细。",
  init() {
    const root = $(ROOT);
    $$("[data-uq-gran]", root).forEach((button) => {
      button.addEventListener("click", () => {
        granularity = button.dataset.uqGran;
        rangeOffset = 0;
        fetchUsage();
      });
    });
    $("#uq-prev").addEventListener("click", () => {
      rangeOffset++;
      fetchUsage();
    });
    $("#uq-next").addEventListener("click", () => {
      if (rangeOffset > 0) rangeOffset--;
      fetchUsage();
    });
    $("#uq-metric-token").addEventListener("click", () => {
      metricMode = "tokens";
      renderExtras();
    });
    $("#uq-metric-cost").addEventListener("click", () => {
      metricMode = "cost";
      renderExtras();
    });
    $("#uq-req-refresh").addEventListener("click", fetchUsage);
    $("#uq-req-reset").addEventListener("click", resetFilters);
    for (const id of [
      "uq-f-model",
      "uq-f-tier",
      "uq-f-channel",
      "uq-f-status",
      "uq-f-effort",
      "uq-f-range",
    ]) {
      $(`#${id}`).addEventListener("change", () => {
        requestPage = 1;
        renderRequests();
      });
    }
    $("#uq-f-q").addEventListener("input", () => {
      requestPage = 1;
      renderRequests();
    });
    $("#uq-f-pagesize").addEventListener("change", (event) => {
      requestPageSize = Math.max(1, Number(event.target.value) || 20);
      requestPage = 1;
      renderRequests();
    });
    // The 3s monitor poll repaints the donut and request table, and refetches
    // the range at most every 30s. Hidden pages stay untouched, so other pages
    // never pay for rebuilding a few hundred rows.
    store.on("monitor", () => {
      if (!isVisible()) return;
      renderExtras();
      if (Date.now() - lastFetch > REFRESH_MS) fetchUsage();
    });
    onChange(() => {
      // Force the two dynamic option lists to rebuild so their labels follow.
      const model = $("#uq-f-model");
      const effort = $("#uq-f-effort");
      if (model) delete model.dataset.sig;
      if (effort) delete effort.dataset.sig;
      render();
    });
  },
  show() {
    fetchUsage();
  },
  refresh() {
    return fetchUsage();
  },
  reset() {
    usageData = null;
    loadError = "";
    rangeOffset = 0;
    requestPage = 1;
    lastFetch = 0;
  },
};