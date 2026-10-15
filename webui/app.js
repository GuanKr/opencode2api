const $ = (selector) => document.querySelector(selector),
  $$ = (selector) => [...document.querySelectorAll(selector)];
let csrf = "",
  config = null,
  monitorData = null,
  debugData = null,
  lastPlayground = null,
  logEvents = [],
  eventSource = null,
  paused = false,
  monitorTimer = null,
  logRenderPending = false,
  toastTimer = null;
const pages = {
  availability: ["13", "免费模型可用性", "每小时探测，手动控制的模型不受影响。"],
  rotation: ["12", "模型轮转", "共享当前模型、故障切换与尝试记录。"],
  overview: ["01", "运行桌面", "最近一小时、进程累计与当前资源状态。"],
  guide: ["02", "首次运行", "用六个检查点完成从配置到首个请求。"],
  access: ["03", "接入手册", "Chat、Responses、Anthropic 与 SDK 示例。"],
  usage: ["04", "Token 用量", "按小时/天/周/月查看 Token 与预估成本"],
  playground: ["05", "Playground", "三种入口协议共用真实 Gateway 路由与诊断。"],
  diagnostics: ["06", "路由诊断", "Metadata、模型资格与每一次上游尝试。"],
  config: ["07", "配置中心", "验证后原子保存，并热应用可变字段。"],
  logs: ["08", "事件日志", "来自进程内 Ring Buffer 的结构化 SSE。"],
  account: ["09", "账号安全", "更新后撤销全部管理 Session。"],
  "model-quotas": ["10", "模型与额度", "可用模型、共享套餐剩余额度与重置时间。"],
  "api-keys": ["11", "API Key 管理", "给每个调用方独立的访问密钥。"],
};
async function api(path, options = {}) {
  options.headers = { ...(options.headers || {}), Accept: "application/json" };
  if (options.body) options.headers["Content-Type"] = "application/json";
  if (csrf && !["GET", "HEAD"].includes(options.method || "GET"))
    options.headers["X-CSRF-Token"] = csrf;
  const response = await fetch(path, options);
  if (response.status === 401) {
    showLogin();
    throw new Error("登录已失效");
  }
  if (response.status === 204) return null;
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error?.message || `HTTP ${response.status}`);
  return data;
}
function toast(message, bad = false) {
  const node = $("#toast");
  node.textContent = message;
  node.classList.toggle("bad-toast", bad);
  node.classList.remove("hidden");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => node.classList.add("hidden"), 3600);
}
function showLogin() {
  csrf = "";
  $("#app").classList.add("hidden");
  $("#login").classList.remove("hidden");
  if (eventSource) eventSource.close();
  eventSource = null;
  clearInterval(monitorTimer);
  clearInterval(quotaTimer);
  clearCreatedKey();
  $("#api-key-value").value = "";
}
async function enterConsole(session) {
  csrf = session.csrf_token;
  $("#login").classList.add("hidden");
  $("#app").classList.remove("hidden");
  await loadConfig();
  await Promise.all([refreshMonitor(), loadDebugModels()]);
  clearInterval(monitorTimer);
  monitorTimer = setInterval(refreshMonitor, 3000);
  connectLogs();
  openPage(pages[location.hash.slice(1)] ? location.hash.slice(1) : "overview");
}
async function boot() {
  try {
    await enterConsole(await api("/api/auth/session"));
  } catch (_) {
    showLogin();
  }
}
$("#login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  $("#login-error").textContent = "";
  try {
    const session = await api("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({ username: $("#login-user").value, password: $("#login-pass").value }),
    });
    $("#login-pass").value = "";
    await enterConsole(session);
  } catch (error) {
    $("#login-error").textContent = error.message;
  }
});
$("#logout").onclick = async () => {
  try {
    await api("/api/auth/logout", { method: "POST", body: "{}" });
  } finally {
    showLogin();
  }
};
function openPage(page) {
  if (!pages[page]) return;
  history.replaceState(null, "", `#${page}`);
  clearInterval(quotaTimer);
  if (page !== "api-keys") clearCreatedKey();
  $$(".nav button").forEach((button) =>
    button.classList.toggle("active", button.dataset.page === page),
  );
  $$(".page").forEach((node) => node.classList.toggle("hidden", node.id !== `page-${page}`));
  const meta = pages[page];
  $("#page-index").textContent = `Section ${meta[0]}`;
  $("#page-title").textContent = meta[1];
  $("#page-sub").textContent = meta[2];
  if (page === "access") renderAccessExamples();
  if (page === "usage") fetchUsage();
  if (page === "model-quotas") {
    loadModelQuotas();
    quotaTimer = setInterval(() => {
      if (!document.hidden) loadModelQuotas();
    }, 60000);
  }
  if (page === "rotation") loadRotation();
  if (page === "availability") loadAvailability();
  if (page === "api-keys") loadAPIKeys();
  if (page === "playground" && !debugData) loadDebugModels();
  if (page === "diagnostics") {
    loadDebugModels();
    renderDiagnostics();
  }
}
$$(".nav button").forEach((button) => (button.onclick = () => openPage(button.dataset.page)));
async function loadConfig() {
  config = await api("/api/config");
  fillConfig(config);
  renderAccessExamples();
}
function fillConfig(value) {
  $("#c-listen").value = value.listen;
  $("#c-prefer").value = value.prefer;
  $("#c-web-listen").value = value.webui.listen;
  $("#c-session").value = value.webui.session_ttl_minutes;
  $("#c-web-enabled").checked = value.webui.enabled;
  $("#c-anonymous").checked = !!value.anonymous;
  $("#c-proxyfile").value = value.proxyfile;
  $("#c-up-zen").value = value.upstream.zen;
  $("#c-up-go").value = value.upstream.go;
  $("#c-attempts").value = value.retry.max_attempts;
  $("#c-timeout").value = value.retry.timeout_seconds;
  $("#c-refresh").value = value.models.refresh_seconds;
  $("#c-protocols").value = JSON.stringify(value.models.protocols || {}, null, 2);
  $("#c-free-only").checked = !!value.models.free_only;
  const reasoning = value.reasoning || {};
  $("#c-reasoning-effort").value = reasoning.effort || "";
  $("#c-reasoning-by-model").value = JSON.stringify(reasoning.effort_by_model || {}, null, 2);
  $("#c-idle").value = value.performance.max_idle_conns;
  $("#c-idle-host").value = value.performance.max_idle_conns_per_host;
  $("#c-max-host").value = value.performance.max_conns_per_host;
  $("#c-idle-timeout").value = value.performance.idle_conn_timeout_seconds;
  $("#c-connect").value = value.performance.connect_timeout_seconds;
  $("#c-cooldown").value = value.performance.failure_cooldown_seconds;
  $("#c-attempt-timeout").value = value.performance.attempt_timeout_seconds;
  $("#c-first-event-timeout").value = value.performance.first_event_timeout_seconds || 0;
  $("#c-level").value = value.logging.level;
  $("#c-ring").value = value.logging.ring_size;
  $("#c-dump-bodies").checked = !!value.logging.dump_request_bodies;
  ["server_keys", "zen_keys", "go_keys", "proxies"].forEach(renderChips);
  const notice = $("#restart-notice");
  if (value.restart_required_fields?.length) {
    notice.textContent = `已保存但尚未生效：${value.restart_required_fields.join(", ")}。当前 API ${value.effective.listen}、WebUI ${value.effective.webui_listen}。请重启进程。`;
    notice.classList.remove("hidden");
  } else notice.classList.add("hidden");
}
function renderChips(name) {
  const host = $(`#${name}-chips`);
  host.replaceChildren();
  (config[name] || []).forEach((item) => {
    const chip = document.createElement("span"),
      text = document.createElement("span"),
      remove = document.createElement("button");
    chip.className = "chip";
    chip.dataset.id = item.id;
    text.textContent = item.display;
    remove.type = "button";
    remove.title = "移除";
    remove.textContent = "×";
    remove.onclick = () => chip.remove();
    chip.append(text, remove);
    host.appendChild(chip);
  });
}
function secretInputs(name) {
  const kept = [...$(`#${name}-chips`).children].map((node) => ({ id: node.dataset.id }));
  const added = $(`#${name}-new`)
    .value.split(/\r?\n/)
    .map((value) => value.trim())
    .filter(Boolean)
    .map((value) => ({ value }));
  return kept.concat(added);
}
const number = (id) => Number($(id).value);
$("#config-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    const update = {
      listen: $("#c-listen").value,
      server_keys: secretInputs("server_keys"),
      zen_keys: secretInputs("zen_keys"),
      go_keys: secretInputs("go_keys"),
      anonymous: $("#c-anonymous").checked,
      proxies: secretInputs("proxies"),
      proxyfile: $("#c-proxyfile").value,
      prefer: $("#c-prefer").value,
      upstream: { zen: $("#c-up-zen").value, go: $("#c-up-go").value },
      retry: { max_attempts: number("#c-attempts"), timeout_seconds: number("#c-timeout") },
      models: {
        refresh_seconds: number("#c-refresh"),
        protocols: JSON.parse($("#c-protocols").value || "{}"),
        free_only: $("#c-free-only").checked,
      },
      reasoning: {
        effort: $("#c-reasoning-effort").value,
        effort_by_model: JSON.parse($("#c-reasoning-by-model").value || "{}"),
      },
      performance: {
        max_idle_conns: number("#c-idle"),
        max_idle_conns_per_host: number("#c-idle-host"),
        max_conns_per_host: number("#c-max-host"),
        idle_conn_timeout_seconds: number("#c-idle-timeout"),
        connect_timeout_seconds: number("#c-connect"),
        failure_cooldown_seconds: number("#c-cooldown"),
        attempt_timeout_seconds: number("#c-attempt-timeout"),
        first_event_timeout_seconds: number("#c-first-event-timeout"),
      },
      logging: {
        level: $("#c-level").value,
        ring_size: number("#c-ring"),
        dump_request_bodies: $("#c-dump-bodies").checked,
      },
      webui: {
        enabled: $("#c-web-enabled").checked,
        listen: $("#c-web-listen").value,
        username: config.webui.username,
        session_ttl_minutes: number("#c-session"),
      },
    };
    const result = await api("/api/config", { method: "PUT", body: JSON.stringify(update) });
    config = result.config;
    ["server_keys", "zen_keys", "go_keys", "proxies"].forEach(
      (name) => ($(`#${name}-new`).value = ""),
    );
    fillConfig(config);
    await loadDebugModels();
    toast(
      result.result.restart_required
        ? `已应用；${result.result.restart_fields.join(", ")} 需重启`
        : "配置已保存并立即应用",
    );
  } catch (error) {
    toast(error.message, true);
  }
});
$("#reload-config").onclick = async () => {
  try {
    const result = await api("/api/config/reload", { method: "POST", body: "{}" });
    await loadConfig();
    await loadDebugModels();
    toast(result.restart_required ? "磁盘配置已重载，监听字段需重启" : "磁盘配置已重载");
  } catch (error) {
    toast(error.message, true);
  }
};
$("#reveal").onclick = async () => {
  const password = prompt("请再次输入管理密码");
  if (password === null) return;
  try {
    const values = await api("/api/config/reveal", {
      method: "POST",
      body: JSON.stringify({ password }),
    });
    $("#modal-content").textContent = JSON.stringify(values, null, 2);
    $("#modal").classList.remove("hidden");
  } catch (error) {
    toast(error.message, true);
  }
};
$("#modal-close").onclick = () => {
  $("#modal").classList.add("hidden");
  $("#modal-content").textContent = "";
};
$("#account-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await api("/api/account", {
      method: "PUT",
      body: JSON.stringify({
        current_password: $("#a-current").value,
        username: $("#a-user").value,
        new_password: $("#a-new").value,
      }),
    });
    toast("账号已更新，请重新登录");
    setTimeout(showLogin, 800);
  } catch (error) {
    toast(error.message, true);
  }
});
function appendCell(row, value, className = "", title = "") {
  const cell = document.createElement("td");
  cell.textContent = String(value);
  if (className) cell.className = className;
  if (title) cell.title = title;
  row.appendChild(cell);
}
// Local: 请求明细的 Token 紧凑格（新输入↓/输出↑ + 缓存读与命中率 + 悬停明细）。
function appendTokenCell(row, tokens) {
  const cell = document.createElement("td");
  row.appendChild(cell);
  const total = Number(tokens.total_tokens || 0);
  if (!(total > 0)) {
    cell.textContent = "—";
    return;
  }
  const input = Number(tokens.input_tokens || 0),
    output = Number(tokens.output_tokens || 0),
    cached = Number(tokens.cached_tokens || 0),
    reasoning = Number(tokens.reasoning_tokens || 0),
    fresh = Math.max(0, input - cached),
    hit = input > 0 ? Math.min(100, Math.max(0, (cached / input) * 100)) : null;
  const hitTone = hit == null ? "" : hit >= 95 ? "hit95" : hit >= 85 ? "hit85" : hit >= 70 ? "hit70" : hit >= 50 ? "hit50" : "";
  const esc = (value) => String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;");
  cell.innerHTML =
    `<span class="tok"><span class="tok-l1"><i class="a-dn">↓</i>${esc(formatTokensK(fresh))} <i class="a-up">↑</i>${esc(formatTokensK(output))}</span>` +
    `<span class="tok-l2"><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><ellipse cx="8" cy="3.6" rx="5.6" ry="2.2"/><path d="M2.4 3.6v8.8c0 1.2 2.5 2.2 5.6 2.2s5.6-1 5.6-2.2V3.6"/><path d="M2.4 8c0 1.2 2.5 2.2 5.6 2.2s5.6-1 5.6-2.2"/></svg>${esc(formatTokensK(cached))}` +
    `${hit == null ? "" : ' <span class="tok-hit ' + hitTone + '">' + hit.toFixed(1) + "%</span>"}</span>` +
    `<span class="tok-tip"><b>Token 明细</b>` +
    `<span><span>输入 Token</span><span>${esc(formatTokensExact(fresh))}</span></span>` +
    `<span><span>输出 Token</span><span>${esc(formatTokensExact(output))}</span></span>` +
    `<span><span>缓存读取 Token</span><span>${esc(formatTokensExact(cached))}</span></span>` +
    `<span><span>推理 Token</span><span>${esc(formatTokensExact(reasoning))}</span></span>` +
    `<span class="tt-total"><span>总 Token</span><span>${esc(formatTokensExact(total))}</span></span>` +
    `<span><span>缓存命中率</span><span>${hit == null ? "—" : hit.toFixed(1) + "%"}</span></span></span></span>`;
}
function emptyRow(host, columns, message = "暂无数据") {
  const row = document.createElement("tr"),
    cell = document.createElement("td");
  cell.colSpan = columns;
  cell.className = "empty";
  cell.textContent = message;
  row.appendChild(cell);
  host.appendChild(row);
}
function renderResources(resources) {
  const proxies = $("#proxy-table");
  proxies.replaceChildren();
  (resources.proxies || []).forEach((proxy) => {
    const row = document.createElement("tr");
    appendCell(row, proxy.address);
    appendCell(row, proxy.healthy ? "正常" : "不可用", `status ${proxy.healthy ? "ok" : "bad"}`);
    appendCell(
      row,
      `Z ${proxy.zen_keys} · G ${proxy.go_keys} · A ${proxy.anonymous ? "开" : "关"}`,
    );
    proxies.appendChild(row);
  });
  if (!proxies.children.length) emptyRow(proxies, 3);
  const keys = $("#key-table");
  keys.replaceChildren();
  if (resources.anonymous) {
    const row = document.createElement("tr");
    appendCell(row, "anonymous");
    appendCell(row, "zen");
    appendCell(row, "已启用", "status ok");
    keys.appendChild(row);
  }
  (resources.keys || []).forEach((key) => {
    const row = document.createElement("tr");
    appendCell(row, key.id);
    appendCell(row, key.tier);
    appendCell(
      row,
      key.cooldown_until ? "冷却中" : "可用",
      `status ${key.cooldown_until ? "warn" : "ok"}`,
    );
    keys.appendChild(row);
  });
  if (!keys.children.length) emptyRow(keys, 3);
}
function renderBreakdowns(metrics) {
  const host = $("#breakdowns");
  host.replaceChildren();
  [
    ["端点", metrics.endpoints],
    ["状态码", metrics.statuses],
    ["模型", metrics.models],
    ["Tier", metrics.tiers],
  ].forEach(([title, data]) => {
    const block = document.createElement("div"),
      heading = document.createElement("h3"),
      table = document.createElement("table"),
      body = document.createElement("tbody");
    heading.textContent = title;
    table.className = "table";
    Object.entries(data || {})
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .forEach(([key, value]) => {
        const row = document.createElement("tr");
        appendCell(row, key);
        appendCell(row, value);
        body.appendChild(row);
      });
    if (!body.children.length) emptyRow(body, 2, "暂无请求");
    table.appendChild(body);
    block.append(heading, table);
    host.appendChild(block);
  });
}
function renderGuide() {
  if (!config || !monitorData) return;
  const resources = monitorData.resources,
    models = resources.models || {},
    modelDescription = models.stale
      ? "目录来自" + (models.cache_source || "磁盘") + "快取，等待背景刷新。"
      : "上游目录已完成首次刷新且有可暴露模型。";
  const checks = [
    ["登录管理端", "当前 Session 已通过服务端验证。", true],
    ["配置本地 Key", "客户端使用 Server Key 访问 Gateway。", (config.server_keys || []).length > 0],
    [
      "选择上游通道",
      "至少有 Zen / Go Key，或启用匿名通道。",
      (config.zen_keys || []).length + (config.go_keys || []).length > 0 || config.anonymous,
    ],
    [
      "检查 Proxy",
      "至少一个代理节点健康。",
      (resources.proxies || []).some((proxy) => proxy.healthy),
    ],
    ["等待模型目录", modelDescription, models.exposed > 0 && !!models.updated_at],
    ["复制接入示例", "进入接入手册，替换模型与 YOUR_API_KEY。", true],
  ];
  const host = $("#guide-steps");
  host.replaceChildren();
  checks.forEach(([title, description, done]) => {
    const node = document.createElement("article"),
      heading = document.createElement("h3"),
      copy = document.createElement("p"),
      state = document.createElement("div");
    node.className = "guide-step";
    heading.textContent = title;
    copy.textContent = description;
    state.className = `step-state ${done ? "ok" : "warn"}`;
    state.textContent = done ? "✓ 已就绪" : "○ 待处理";
    node.append(heading, copy, state);
    host.appendChild(node);
  });
}
const formatTokens = (value) => Number(value || 0).toLocaleString();
// Local: Token 用量页 —— 紧凑数字/命中率/成本/耗时格式化分区。
// 紧凑口径只服务本页：上面的 formatTokens 保持上游的纯千分位写法，
// 元数据与 API Key 页的渲染不受影响。
const formatTokensCompact = (value) => {
  let n = Number(value || 0);
  if (Number.isNaN(n)) return "0";
  if (!isFinite(n)) return "∞";
  n = Math.round(n);
  if (n >= 1e8 || Number((n / 1e4).toFixed(2)) >= 10000) return (n / 1e8).toFixed(2) + "亿";
  if (n >= 1e4) return (n / 1e4).toFixed(2) + "万";
  return n.toLocaleString();
};
const formatTokensK = (value) => {
  let n = Number(value || 0);
  if (Number.isNaN(n)) return "0";
  if (!isFinite(n)) return "∞";
  n = Math.round(n);
  const sign = n < 0 ? "-" : "";
  const a = Math.abs(n);
  if (a >= 1000) return sign + (a / 1000).toFixed(2).replace(/\.?0+$/, "") + "k";
  return sign + a.toLocaleString();
};
const formatTokensExact = (value) => {
  const n = Number(value || 0);
  return Number.isFinite(n) ? n.toLocaleString() : "—";
};
const formatHitRate = (cached, input) => {
  const c = Number(cached || 0),
    i = Number(input || 0);
  if (!(i > 0) || !Number.isFinite(c) || !Number.isFinite(i)) return "—";
  return Math.min(100, Math.max(0, (c / i) * 100)).toFixed(1) + "%";
};
const formatUSD = (value) => {
  const number = Number(value || 0);
  if (!isFinite(number) || number === 0) return "$0";
  if (Math.abs(number) < 0.01) return "$" + number.toFixed(4);
  return "$" + number.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};
// Three-state cost: known-free ($0) vs unknown-price. Unknown models record
// $0 cost server-side and set unknown_cost on the point; showing a bare $0
// would conflate the two, so unknown renders "—" (or keeps the partial sum
// with a note when other models in range did price).
const formatCost = (value, unknown) => {
  if (unknown && !(Number(value) > 0)) return "—";
  return formatUSD(value);
};
const costTitle = (value, unknown) => {
  const base = "预估成本 " + formatUSD(value) + "（按记录时 models.dev 价格）";
  return unknown ? base + "；含未知价格模型，实际可能更高" : base;
};
// Durations render in seconds (m/s once past a minute) instead of raw ms.
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
// Local: 请求明细 —— 推理强度显示。空值（客户端未设推理，或显式 none）显示 —；
// 归一化（小写、去 -/_/空格）与服务端 ReasoningEffortLabel 同口径，但这一层额外
// 做纯显示的别名美化（extrahigh→XHigh），筛选仍按服务端的原始小写值比对。
const formatEffort = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "—";
  const n = raw.toLowerCase().replace(/[-_\s]/g, ""),
    known = {
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
// Local: 请求明细 —— 首字节耗时。0/负数一律显示 —（非流式无 TTFT，或后端未记录）。
const formatFirstByte = (value) => {
  const n = Number(value || 0);
  if (!(n > 0)) return "—";
  return formatDurationMS(n);
};
// Local: 请求明细 —— 耗时列显示「总耗时 − 首字」。仅流式且首字 >0 时相减；
// 非流式直接显示总耗时（其首字与完成时刻重合，相减会得到无意义的 0）。
const durationExcludingFirstByte = (total, first, stream) => {
  if (total == null) return null;
  const t = Number(total),
    f = Number(first || 0);
  if (!Number.isFinite(t) || t < 0) return null;
  if (!stream || !(f > 0)) return t;
  return Math.max(0, t - f);
};
const durationExcludingFirstByteTitle = (total, first, stream) => {
  if (total == null) return "";
  const t = Number(total),
    f = Number(first || 0);
  if (!Number.isFinite(t) || t < 0) return "";
  if (!stream || !(f > 0)) return "总耗时 " + formatDurationMS(t) + (stream ? "" : "（非流式无 TTFT）");
  return "总耗时 " + formatDurationMS(t) + " · 首字 " + formatDurationMS(f) + " · 已减首字";
};
let usageGranularity = "day",
  usageOffset = 0,
  usageData = null,
  usageLoading = false,
  // Local: /api/usage 失败原因。空字符串表示还没失败过（"加载中"），非空表示
  // 已失败——用于区分"数据还没到"和"数据不会来了"，避免拿旧口径兜底冒充新口径。
  usageLoadError = "",
  lastUsageFetch = 0,
  usageMetricMode = "tokens",
  usageReqPage = 1,
  usageReqPageSize = 20;
const usageCounts = { hour: 24, day: 30, week: 12, month: 12 },
  granLabels = { hour: "小时", day: "天", week: "周", month: "月" };
const formatBeijingDay = (iso) =>
  new Date(iso).toLocaleDateString("zh-CN", { timeZone: "Asia/Shanghai", month: "2-digit", day: "2-digit" });
const formatBeijingHour = (iso) =>
  new Date(iso).toLocaleString("zh-CN", {
    timeZone: "Asia/Shanghai",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    hour12: false,
  });
// Local: Token 用量页 —— 数据获取 + 北京时间标签/趋势轴分区。
async function fetchUsage() {
  if (usageLoading) return;
  usageLoading = true;
  try {
    const count = usageCounts[usageGranularity] || 30;
    usageData = await api(`/api/usage?granularity=${usageGranularity}&count=${count}&offset=${usageOffset}`);
    usageLoadError = "";
    if (location.hash === "#usage") renderUsage();
  } catch (error) {
    usageLoadError = error.message || String(error);
    // Local: 必须同时清空 usageData。否则切换粒度后接口报错，旧粒度的数据会
    // 留在标题已改成新粒度的页面上（实测出现"按小时"标题配 30 根按天柱）。
    usageData = null;
    toast(error.message, true);
    // Local: 失败后重画一次，让页面从"加载中"切到"加载失败"，
    // 否则会一直停在无数据的空壳上看不出是失败了还是没数据。
    if (location.hash === "#usage") renderUsage();
  } finally {
    usageLoading = false;
    // Local: 成功失败都记时间戳。之前只在成功时记，接口持续失败时自动重试
    // 会退化成每个 3s 轮询周期一次。
    lastUsageFetch = Date.now();
  }
}
function usagePointLabel(iso) {
  return usageGranularity === "hour" ? formatBeijingHour(iso) : formatBeijingDay(iso);
}
function axisTickIndex(n, max) {
  const cap = max || 12,
    count = Math.min(Math.max(n || 0, 0), cap),
    set = new Set();
  if (n <= 0) return set;
  if (count <= 1) {
    set.add(0);
    return set;
  }
  for (let k = 0; k < count; k++) set.add(Math.round((k * (n - 1)) / (count - 1)));
  return set;
}
function formatAxisLabel(iso) {
  return usageGranularity === "hour"
    ? new Date(iso).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour: "2-digit", hour12: false })
    : formatBeijingDay(iso);
}
function buildAxisTicks(labels, idx) {
  const axis = document.createElement("div");
  axis.className = "trend-axis";
  labels.forEach((text, i) => {
    const tick = document.createElement("span");
    tick.className = "trend-tick";
    if (idx.has(i)) tick.textContent = text;
    axis.appendChild(tick);
  });
  return axis;
}
// Local: Token 用量页 —— 主趋势图（北京时间柱 + 轴刻度）。
function renderUsage() {
  const title = $("#usage-trend-title"),
    caption = $("#usage-trend-caption"),
    warning = $("#usage-history-warning");
  $$("[data-gran]").forEach((button) =>
    button.classList.toggle("primary", button.dataset.gran === usageGranularity),
  );
  if (title) title.textContent = "按" + (granLabels[usageGranularity] || "天") + " Token 趋势";
  // 后端落盘被禁用（历史读回失败）时，页面上的一切都只在内存里，重启即丢：
  // 与其让它看起来一切正常，不如直接说清楚。
  if (warning) warning.classList.toggle("hidden", !(usageData && usageData.history_disabled));
  if (usageData && usageData.points) {
    const total = usageData.total || {},
      tokens = total.tokens || {};
    $("#u-coverage").textContent = ((total.coverage || 0) * 100).toFixed(1) + "%";
    $("#u-total").textContent = formatTokensCompact(tokens.total_tokens);
    $("#u-total").title = formatTokensExact(tokens.total_tokens);
    $("#u-total-note").textContent = "当前范围 · " + (granLabels[usageGranularity] || "") + "视图";
    $("#u-cost").textContent = formatCost(total.cost_usd, total.unknown_cost);
    $("#u-cost").title = costTitle(total.cost_usd, total.unknown_cost);
    $("#u-requests").textContent = Number(total.requests || 0).toLocaleString();
    $("#u-cached").textContent = formatTokensCompact(tokens.cached_tokens);
    $("#u-cached").title = formatTokensExact(tokens.cached_tokens);
    $("#u-reasoning").textContent = formatTokensCompact(tokens.reasoning_tokens);
    $("#u-reasoning").title = formatTokensExact(tokens.reasoning_tokens);
    $("#u-hitrate").textContent = formatHitRate(tokens.cached_tokens, tokens.input_tokens);
    const label = $("#usage-range-label");
    if (label && usageData.from && usageData.to) {
      const sameDay =
        usageGranularity === "hour" &&
        new Date(usageData.from).toLocaleDateString("zh-CN", { timeZone: "Asia/Shanghai" }) ===
          new Date(usageData.to).toLocaleDateString("zh-CN", { timeZone: "Asia/Shanghai" });
      label.textContent =
        (sameDay
          ? formatBeijingHour(usageData.from) + " ~ " + formatBeijingHour(usageData.to)
          : formatBeijingDay(usageData.from) + " ~ " + formatBeijingDay(usageData.to)) +
        " · 北京时间 · 共" +
        usageData.points.length +
        "个" +
        (granLabels[usageGranularity] || "点");
    }
    if (caption)
      caption.textContent =
        "北京时间对齐，柱顶数字为总量，下方时间轴对应各柱时间；悬停查看输入、输出、缓存、命中率与预估成本。";
    const trend = $("#usage-trend"),
      points = usageData.points,
      maximum = Math.max(1, ...points.map((point) => point.tokens?.total_tokens || 0)),
      bars = document.createElement("div"),
      tickIdx = axisTickIndex(points.length, usageGranularity === "hour" || usageGranularity === "day" ? points.length : 12);
    bars.className = "trend-bars";
    trend.replaceChildren();
    points.forEach((point) => {
      const bar = document.createElement("div");
      bar.className = "trend-bar";
      const value = point.tokens?.total_tokens || 0;
      bar.style.height = Math.max(2, Math.round((value / maximum) * 100)) + "%";
      bar.title =
        usagePointLabel(point.start) +
        " · input " +
        formatTokensCompact(point.tokens?.input_tokens) +
        " · output " +
        formatTokensCompact(point.tokens?.output_tokens) +
        " · cached " +
        formatTokensCompact(point.tokens?.cached_tokens) +
        " · 命中率 " +
        formatHitRate(point.tokens?.cached_tokens, point.tokens?.input_tokens) +
        " · total " +
        formatTokensCompact(value) +
        " · " +
        formatCost(point.cost_usd, point.unknown_cost) +
        (point.unknown_cost ? "（含未知价格模型）" : "");
      const tag = document.createElement("span");
      tag.className = "trend-value";
      if (value > 0) tag.textContent = formatTokensCompact(value);
      bar.appendChild(tag);
      bars.appendChild(bar);
    });
    trend.append(
      bars,
      buildAxisTicks(
        points.map((point) => formatAxisLabel(point.start)),
        tickIdx,
      ),
    );
    const models = $("#usage-models");
    models.replaceChildren();
    const modelCosts = total.model_costs || {};
    Object.entries(total.models || {})
      .sort((a, b) => (b[1].total_tokens || 0) - (a[1].total_tokens || 0))
      .slice(0, 15)
      .forEach(([model, value]) => {
        const row = document.createElement("tr");
        appendCell(row, model);
        appendCell(row, formatTokensCompact(value.input_tokens), "", formatTokensExact(value.input_tokens));
        appendCell(row, formatTokensCompact(value.output_tokens), "", formatTokensExact(value.output_tokens));
        appendCell(row, formatTokensCompact(value.total_tokens), "", formatTokensExact(value.total_tokens));
        appendCell(row, formatUSD(modelCosts[model]), "cost");
        models.appendChild(row);
      });
    if (!models.children.length) emptyRow(models, 5, "该范围内尚无 Token 用量");
    const tiers = $("#usage-tiers"),
      tierCosts = total.tier_costs || {},
      tierEntries = Object.entries(total.tiers || {}).sort(
        (a, b) => (b[1].total_tokens || 0) - (a[1].total_tokens || 0),
      ),
      tierTotal = Math.max(1, ...tierEntries.map(([, value]) => value.total_tokens || 0));
    tiers.replaceChildren();
    tierEntries.forEach(([tier, value]) => {
      const row = document.createElement("div"),
        name = document.createElement("strong"),
        track = document.createElement("div"),
        fill = document.createElement("div"),
        amount = document.createElement("span");
      row.className = "tier-row";
      name.textContent = tier;
      track.className = "tier-track";
      fill.className = "tier-fill";
      fill.style.width = Math.round(((value.total_tokens || 0) / tierTotal) * 100) + "%";
      amount.className = "mono";
      amount.textContent = formatTokensCompact(value.total_tokens) + " · " + formatUSD(tierCosts[tier]);
      amount.title =
        "input " +
        formatTokensCompact(value.input_tokens) +
        "（" +
        formatTokensExact(value.input_tokens) +
        "） · output " +
        formatTokensCompact(value.output_tokens) +
        "（" +
        formatTokensExact(value.output_tokens) +
        "） · cached " +
        formatTokensCompact(value.cached_tokens) +
        " · 命中率 " +
        formatHitRate(value.cached_tokens, value.input_tokens) +
        " · total " +
        formatTokensCompact(value.total_tokens) +
        "（" +
        formatTokensExact(value.total_tokens) +
        "）";
      track.appendChild(fill);
      row.append(name, track, amount);
      tiers.appendChild(row);
    });
    if (!tierEntries.length) {
      const empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent = "该范围内尚无 Tier 用量";
      tiers.appendChild(empty);
    }
    renderUsageExtras();
    return;
  }
  // Local: /api/usage 是本页唯一数据源。不回退到 monitorData.usage.last_hour
  // 兜底：接口报错时它会把"最近 60 分钟"的数字填进按天/周/月标题下方，
  // 量纲完全对不上（实测强制 500 后持续显示十余秒），比老实显示"无数据"更糟。
  showUsageUnavailable();
}
// Local: 用量数据不可用时的显式空状态，区分"还没加载到"与"加载失败"。
//
// 只清由 /api/usage 驱动的槽位（指标、趋势、模型表、Tier 分布）。
// 使用记录三节（端点 / 通道 / 请求表）来自 /api/monitor，与用量接口无关：
// 之前一并清掉，结果 /api/usage 一挂，有健康监控数据的区段被永久标成
// "用量数据加载失败" —— 那正是本次修复要消除的错误闪烁，只是换了位置。
function showUsageUnavailable() {
  const failed = !!usageLoadError;
  const dash = "—",
    hint = failed ? "加载失败" : "加载中…",
    body = failed ? "用量数据加载失败" : "正在加载用量数据…";
  const warning = $("#usage-history-warning");
  if (warning) warning.classList.toggle("hidden", !(usageData && usageData.history_disabled));
  for (const id of ["u-coverage", "u-total", "u-cost", "u-requests", "u-cached", "u-reasoning", "u-hitrate"]) {
    const node = $("#" + id);
    if (!node) continue;
    node.textContent = dash;
    node.title = "";
  }
  $("#u-total-note").textContent = hint;
  const label = $("#usage-range-label");
  if (label) label.textContent = hint;
  const caption = $("#usage-trend-caption");
  if (caption) caption.textContent = body;
  const trend = $("#usage-trend");
  if (trend) trend.replaceChildren();
  // emptyRow 只 append，必须先清空：否则每次重画多堆一行（实测约 40 行/分钟）。
  const models = $("#usage-models");
  models.replaceChildren();
  emptyRow(models, 5, body);
  const tiers = $("#usage-tiers");
  if (tiers) {
    tiers.replaceChildren();
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = body;
    tiers.appendChild(empty);
  }
  // renderUsageExtras 画的是 monitor 驱动的三节；顺带让 donut 走 paintDonut 的
  // 空分支（渐变复位 + 图例"暂无数据"）。不要手动 replaceChildren：渐变写在
  // style.background 上，清子节点对它无效。
  renderUsageExtras();
}
const usageDonutPalette = [
  "#153c34",
  "#ed5a2a",
  "#e7b94f",
  "#204d43",
  "#68716c",
  "#8a9a93",
  "#b6382f",
  "#237056",
];
const sessionPalette = [
  "#c2410c",
  "#6d28d9",
  "#1d4ed8",
  "#0f766e",
  "#a21caf",
  "#92400e",
  "#4d7c0f",
  "#be123c",
];
// Local: Token 用量页 —— donut 通用绘制（模型/端点共用）。
function paintDonut(donutId, legendId, rows) {
  const donut = $(donutId),
    legend = $(legendId);
  if (!donut || !legend) return;
  const total = rows.reduce((sum, row) => sum + (Number(row.value) || 0), 0);
  if (!(total > 0)) {
    donut.style.background = "var(--paper2)";
    legend.replaceChildren();
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = "暂无数据";
    legend.appendChild(empty);
    return;
  }
  let acc = 0;
  const parts = rows.map((row, index) => {
    const start = (acc / total) * 360;
    acc += Number(row.value) || 0;
    const end = (acc / total) * 360;
    return `${usageDonutPalette[index % usageDonutPalette.length]} ${start.toFixed(1)}deg ${end.toFixed(1)}deg`;
  });
  donut.style.background = `conic-gradient(${parts.join(",")})`;
  legend.replaceChildren();
  rows.forEach((row, index) => {
    const line = document.createElement("div"),
      left = document.createElement("span"),
      right = document.createElement("span");
    left.innerHTML = `<i class="dot" style="background:${usageDonutPalette[index % usageDonutPalette.length]}"></i>`;
    left.appendChild(document.createTextNode(row.label));
    right.className = "mono";
    right.textContent = `${((Number(row.value) / total) * 100).toFixed(1)}%`;
    right.title = String(row.hint || row.value);
    line.append(left, right);
    legend.appendChild(line);
  });
}
// Local: Token 用量页 —— donut/端点/通道/请求表聚合入口。
function renderUsageExtras() {
  try {
    renderUsageDonuts();
    renderUsageEndpoints();
    renderUsageChannels();
    renderUsageRequests();
  } catch (error) {
    if (window.console && console.warn) console.warn("renderUsageExtras failed", error);
  }
}
// Local: Token 用量页 —— 请求表分页（ ellipsis 压缩）。
function pagerList(c, P) {
  const keep = new Set([1, P, c - 1, c, c + 1]);
  const arr = [...keep].filter((p) => p >= 1 && p <= P).sort((a, b) => a - b);
  const out = [];
  let prev = 0;
  for (const p of arr) {
    if (p - prev > 1) out.push(0);
    out.push(p);
    prev = p;
  }
  return out;
}
function renderUsagePager(pages) {
  const host = $("#usage-pager");
  if (!host) return;
  host.replaceChildren();
  const mk = (label, page, disabled, on) => {
    const b = document.createElement("button");
    b.textContent = label;
    b.disabled = !!disabled;
    if (on) b.className = "on";
    b.onclick = () => {
      if (usageReqPage !== page) {
        usageReqPage = page;
        renderUsageRequests();
      }
    };
    return b;
  };
  host.appendChild(mk("<", Math.max(1, usageReqPage - 1), usageReqPage <= 1));
  pagerList(usageReqPage, pages).forEach((p) => {
    if (p === 0) {
      const s = document.createElement("span");
      s.className = "ell";
      s.textContent = "…";
      host.appendChild(s);
    } else host.appendChild(mk(String(p), p, false, p === usageReqPage));
  });
  host.appendChild(mk(">", Math.min(pages, usageReqPage + 1), usageReqPage >= pages));
}
function usageRangeTotal() {
  return (usageData && usageData.total) || null;
}
// Local: Token 用量页 —— 会话取值（上游 JSON 键为 session）。
function sessionIDOf(request) {
  return (request && request.session) || "";
}
// Local: Token 用量页 —— 模型 donut（tokens/cost 双模式）。
function renderUsageDonuts() {
  const total = usageRangeTotal();
  const toggleToken = $("#usage-metric-token"),
    toggleCost = $("#usage-metric-cost");
  if (toggleToken) toggleToken.classList.toggle("primary", usageMetricMode === "tokens");
  if (toggleCost) toggleCost.classList.toggle("primary", usageMetricMode === "cost");
  let rows = [];
  if (total && total.models) {
    const costs = total.model_costs || {};
    const mode = usageMetricMode === "cost" && Object.keys(costs).length ? "cost" : "tokens";
    rows = Object.entries(total.models).map(([model, value]) => ({
      label: model,
      value: mode === "cost" ? Number(costs[model]) || 0 : Number(value.total_tokens) || 0,
    }));
  }
  // Local: 无 usageData 时不再回退到 monitorData.usage.last_hour —— 那口径是
  // "最近 60 分钟"，和本页标题声明的粒度对不上，画出来的是误导而不是信息。
  rows.sort((a, b) => b.value - a.value);
  paintDonut("#usage-model-donut", "#usage-model-legend", rows.slice(0, 8));
}
// Local: Token 用量页 —— 端点分布表。
function renderUsageEndpoints() {
  const body = $("#usage-endpoints");
  if (!body) return;
  const total = usageRangeTotal();
  body.replaceChildren();
  let rows = [];
  if (total && total.endpoints && Object.keys(total.endpoints).length) {
    const tokens = total.endpoint_tokens || {},
      costs = total.endpoint_costs || {};
    rows = Object.entries(total.endpoints).map(([endpoint, count]) => ({
      endpoint,
      count,
      tokens: tokens[endpoint] || null,
      cost: costs[endpoint],
    }));
  } else if (monitorData && monitorData.metrics && monitorData.metrics.endpoints) {
    rows = Object.entries(monitorData.metrics.endpoints)
      .filter(
        ([endpoint]) =>
          endpoint === "/v1/chat/completions" || endpoint === "/v1/responses" || endpoint === "/v1/messages",
      )
      .map(([endpoint, count]) => ({ endpoint, count, tokens: null, cost: null }));
  }
  rows.sort((a, b) => b.count - a.count);
  paintDonut(
    "#usage-endpoint-donut",
    "#usage-endpoint-legend",
    rows.map((row) => ({ label: row.endpoint, value: row.count })),
  );
  rows.slice(0, 15).forEach((row) => {
    const tr = document.createElement("tr");
    appendCell(tr, row.endpoint);
    appendCell(tr, Number(row.count || 0).toLocaleString());
    if (row.tokens) {
      appendCell(
        tr,
        formatTokensCompact(row.tokens.total_tokens),
        "",
        formatTokensExact(row.tokens.total_tokens) +
          " · input " +
          formatTokensExact(row.tokens.input_tokens) +
          " · output " +
          formatTokensExact(row.tokens.output_tokens) +
          " · cached " +
          formatTokensExact(row.tokens.cached_tokens),
      );
      appendCell(tr, row.cost == null ? "—" : formatUSD(row.cost), "cost");
    } else {
      appendCell(tr, "—");
      appendCell(tr, "—", "cost");
    }
    body.appendChild(tr);
  });
  if (!rows.length) emptyRow(body, 4, "该范围内尚无端点用量");
}
// Local: Token 用量页 —— 通道/状态码摘要。
function renderUsageChannels() {
  const host = $("#usage-channels");
  if (!host || !monitorData) return;
  const upstream = monitorData.upstream || {},
    window = upstream.last_hour || upstream.window || {},
    statuses = (monitorData.metrics && monitorData.metrics.statuses) || {};
  const channels = window.channels || {};
  const lines = [];
  Object.entries(channels)
    .sort((a, b) => b[1].total - a[1].total)
    .forEach(([channel, value]) => {
      const rate = value.total ? ` · 成功率 ${((value.success / value.total) * 100).toFixed(1)}%` : "";
      lines.push(`${channel}：${Number(value.total || 0).toLocaleString()} 次${rate}`);
    });
  const statusLine = Object.entries(statuses)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)
    .map(([status, count]) => `${status} ${Number(count || 0).toLocaleString()}`)
    .join(" · ");
  host.replaceChildren();
  const pre = document.createElement("div");
  pre.style.whiteSpace = "pre-wrap";
  pre.textContent = (lines.length ? lines.join("\n") : "暂无通道数据") + (statusLine ? `\n状态码：${statusLine}` : "");
  host.appendChild(pre);
}
// Local: Token 用量页 —— 请求表筛选（模型/tier/通道/状态+会话+时间窗）。
// 数据源是 /api/usage 自带的明细（覆盖本次查询区间、最新在前），
// 不再依赖 /api/monitor 的轮询：monitor 只带最近一小时。
function usageFilteredRequests() {
  const source = (usageData && usageData.requests) || [];
  const model = $("#usage-f-model")?.value || "",
    tier = $("#usage-f-tier")?.value || "",
    channel = $("#usage-f-channel")?.value || "",
    status = $("#usage-f-status")?.value || "",
    effort = $("#usage-f-effort")?.value || "",
    query = ($("#usage-f-q")?.value || "").trim().toLowerCase(),
    range = $("#usage-f-range")?.value || "24h",
    cutoffMs = Date.now() - (range === "1h" ? 3600000 : range === "6h" ? 21600000 : 86400000);
  return source
    .filter((request) => {
      const ts = Date.parse(request.time || "");
      if (!Number.isNaN(ts) && ts < cutoffMs) return false;
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
// Local: Token 用量页 —— 请求表明细（Token 格/耗时/会话着色/点击筛同对话）。
// 尝试数单元格：attempts=0（无可用节点或本地拒绝，无形状可标）只显示裸 0；
// 预剥离首跳用后缀区分选择剥离(s)/全量剥离(*)，>1 一律 warn。
// 未知 prestrip_mode 按全量处理。
function attemptsCell(request) {
  const attempts = Number(request.attempts);
  if (!Number.isFinite(attempts) || attempts < 0) return { text: "—", tone: "", title: "" };
  // Allowlist：只有明确的 selective 才走选择分支，其余（含老数据缺字段、
  // 脏数据）有 prestripped 即按全量处理。
  const mode = request.prestrip_mode === "selective" ? "selective" : request.prestripped ? "full" : "";
  // 脏数据防护：只接受正整数（小数/负数/NaN 归零），并以上限挡掉指数记数
  // 显示（后端黑名单上限 1024，此处留余量到 100000，超限视为脏数据隐藏）。
  const rawBlacklist = Number(request.prestrip_ids);
  const blacklist =
    Number.isInteger(rawBlacklist) && rawBlacklist > 0 && rawBlacklist <= 100000 ? rawBlacklist : 0;
  const blacklistSuffix = blacklist > 0 ? `；黑名单 ${blacklist} 个推理 ID` : "";
  if (attempts === 0) {
    return {
      text: "0",
      tone: "",
      title: "未发出上游尝试：无可用节点，或请求在本地被拒",
    };
  }
  if (attempts > 1 && mode) {
    return {
      text: String(attempts) + (mode === "selective" ? "s" : "*"),
      tone: "warn",
      title:
        (mode === "selective"
          ? "命中中毒历史快照：首跳为选择剥离（仅剥黑名单推理ID，s=选择剥离），之后又触发了重试/回退（含全量回退）"
          : "命中中毒历史记忆（全量剥离）：首跳为剥离体，之后又触发了重试/回退") + blacklistSuffix,
    };
  }
  if (attempts > 1) {
    return {
      text: String(attempts),
      tone: "warn",
      title:
        "上游尝试次数（含匿名 lane 与 key 回退；>1 表示触发过重试/回退，含自愈）" + blacklistSuffix,
    };
  }
  if (mode === "selective") {
    return {
      text: String(attempts) + "s",
      tone: "prestrip-sel",
      title:
        "命中中毒历史快照：首跳仅剥离黑名单推理ID（标记前中毒段，s=选择剥离），保留其后健康推理；未消耗 400 往返" +
        (blacklistSuffix || "；黑名单无 ID（本轮仅剥离无 ID 推理块，命中死链一并剥离）"),
    };
  }
  if (mode) {
    return {
      text: String(attempts) + "*",
      tone: "prestrip",
      title: "命中中毒历史记忆（全量剥离）：首跳直接发送剥离体（等价于自愈重试体），未消耗 400 往返；命中期间连续多轮均为 *",
    };
  }
  return {
    text: String(attempts),
    tone: "",
    title: "上游尝试次数（含匿名 lane 与 key 回退；>1 表示触发过重试/回退，含自愈）",
  };
}
function renderUsageRequests() {
  const body = $("#usage-requests");
  if (!body) return;
  const select = $("#usage-f-model");
  if (select) {
    const models = [
      ...new Set(
        ((usageData && usageData.requests) || []).map((request) => request.model).filter(Boolean),
      ),
    ].sort();
    const sig = models.join(" ");
    if (select.dataset.sig !== sig) {
      select.dataset.sig = sig;
      const current = select.value;
      select.replaceChildren();
      const all = document.createElement("option");
      all.value = "";
      all.textContent = "全部模型";
      select.appendChild(all);
      models.forEach((model) => {
        const option = document.createElement("option");
        option.value = model;
        option.textContent = model;
        select.appendChild(option);
      });
      if (current && models.includes(current)) select.value = current;
    }
  }
  const effortSelect = $("#usage-f-effort");
  if (effortSelect) {
    const efforts = [
      ...new Set(
        ((usageData && usageData.requests) || [])
          .map((request) => request.reasoning_effort)
          .filter(Boolean),
      ),
    ].sort();
    const sig = efforts.join(" ");
    if (effortSelect.dataset.sig !== sig) {
      effortSelect.dataset.sig = sig;
      const current = effortSelect.value;
      effortSelect.replaceChildren();
      const all = document.createElement("option");
      all.value = "";
      all.textContent = "全部强度";
      effortSelect.appendChild(all);
      efforts.forEach((effort) => {
        const option = document.createElement("option");
        option.value = effort;
        option.textContent = formatEffort(effort);
        effortSelect.appendChild(option);
      });
      if (current && efforts.includes(current)) effortSelect.value = current;
    }
  }
  const rows = usageFilteredRequests();
  const sessionColor = new Map();
  let sessionNext = 0;
  for (const item of rows) {
    const key = sessionIDOf(item);
    if (key && !sessionColor.has(key)) sessionColor.set(key, sessionNext++ % sessionPalette.length);
  }
  body.replaceChildren();
  const total = rows.length,
    pages = Math.max(1, Math.ceil(total / usageReqPageSize));
  if (usageReqPage > pages) usageReqPage = pages;
  const start = total === 0 ? 0 : (usageReqPage - 1) * usageReqPageSize + 1,
    end = Math.min(total, usageReqPage * usageReqPageSize);
  rows.slice((usageReqPage - 1) * usageReqPageSize, usageReqPage * usageReqPageSize).forEach((request) => {
    const row = document.createElement("tr");
    const rid = request.request_id || "";
    const tokens = request.tokens || {};
    const totalTokens = Number(tokens.total_tokens || 0);
    const ok = request.success || (request.status >= 200 && request.status < 400);
    appendCell(row, request.time ? new Date(request.time).toLocaleString() : "—");
    appendCell(row, rid.length > 6 ? "…" + rid.slice(-6) : rid || "—", "", rid);
    appendCell(row, request.model || "—");
    appendCell(row, formatEffort(request.reasoning_effort), "", request.reasoning_effort || "");
    appendCell(row, request.endpoint || "—");
    appendCell(row, `${request.tier || "—"} / ${request.channel || "—"}`);
    appendCell(row, request.key_id || "—");
    // 流式列是三态而不是两态：`stream` 没有 omitempty，所以"键不存在"唯一地表示
// 这条记录产自该字段加入之前（本机最长 24h 的历史）。把它读成"同步"会把当时的
// 流式请求错标；宁可显示 —，让"没记录"和"记录为同步"分得开。
const streamLabel = request.stream === true ? "流式" : request.stream === false ? "同步" : "—";
appendCell(row, streamLabel, request.stream === true ? "ok" : "");
    const sid = sessionIDOf(request);
    appendCell(row, sid.length > 6 ? "…" + sid.slice(-6) : sid || "—", sid ? "conv" : "", sid ? sid + "（点击筛选同对话）" : "");
    if (sid) {
      const convCell = row.lastChild;
      convCell.style.cursor = "pointer";
      const colorIndex = sessionColor.get(sid);
      if (colorIndex != null) convCell.style.color = sessionPalette[colorIndex];
      convCell.onclick = () => {
        const q = $("#usage-f-q");
        if (q) {
          q.value = sid;
          usageReqPage = 1;
          renderUsageRequests();
        }
      };
    }
    appendTokenCell(row, tokens);
    if (totalTokens > 0) {
      appendCell(row, request.cost_usd == null ? "—" : formatUSD(request.cost_usd), "cost");
    } else {
      appendCell(row, request.cost_usd ? formatUSD(request.cost_usd) : "—", "cost");
    }
    appendCell(row, request.status || request.outcome || "—", ok ? "ok" : "bad");
    const ac = attemptsCell(request);
    appendCell(row, ac.text, ac.tone, ac.title);
    appendCell(
      row,
      formatFirstByte(request.stream === true ? request.first_byte_ms : 0),
      "",
      "客户端首字节耗时 · 仅流式有效（TTFT），非流式显示—",
    );
    appendCell(
      row,
      formatDurationMS(
        durationExcludingFirstByte(request.duration_ms, request.first_byte_ms, request.stream === true),
      ),
      "",
      durationExcludingFirstByteTitle(request.duration_ms, request.first_byte_ms, request.stream === true),
    );
    body.appendChild(row);
  });
  if (!rows.length) emptyRow(body, 15, "暂无匹配的请求路由");
  const rangeLabel = { "1h": "最近1小时", "6h": "最近6小时" }[$("#usage-f-range")?.value || "24h"] || "最近24小时";
  const count = $("#usage-req-count");
  if (count)
    count.textContent = `显示 ${start} 至 ${end} 共 ${total} 条结果（${rangeLabel}内最新 500 条）`;
  renderUsagePager(pages);
}
// Local: Token 用量页 —— 控件绑定。
$$("[data-gran]").forEach(
  (button) =>
    (button.onclick = () => {
      usageGranularity = button.dataset.gran;
      usageOffset = 0;
      fetchUsage();
    }),
);
$("#usage-prev").onclick = () => {
  usageOffset++;
  fetchUsage();
};
$("#usage-next").onclick = () => {
  if (usageOffset > 0) usageOffset--;
  fetchUsage();
};
$("#usage-metric-token").onclick = () => {
  usageMetricMode = "tokens";
  renderUsageExtras();
};
$("#usage-metric-cost").onclick = () => {
  usageMetricMode = "cost";
  renderUsageExtras();
};
$("#usage-req-reset").onclick = () => {
  $("#usage-f-model").value = "";
  $("#usage-f-tier").value = "";
  $("#usage-f-channel").value = "";
  $("#usage-f-status").value = "";
  $("#usage-f-effort").value = "";
  $("#usage-f-q").value = "";
  $("#usage-f-range").value = "24h";
  usageReqPage = 1;
  renderUsageRequests();
};
$("#usage-req-refresh").onclick = async () => {
  await fetchUsage();
};
["usage-f-model", "usage-f-tier", "usage-f-channel", "usage-f-status", "usage-f-effort", "usage-f-range"].forEach((id) => {
  const node = document.getElementById(id);
  if (node)
    node.onchange = () => {
      usageReqPage = 1;
      renderUsageRequests();
    };
});
$("#usage-f-q").oninput = () => {
  usageReqPage = 1;
  renderUsageRequests();
};
$("#usage-f-pagesize").onchange = (event) => {
  usageReqPageSize = Math.max(1, Number(event.target.value) || 20);
  usageReqPage = 1;
  renderUsageRequests();
};
function renderFacts(selector, entries) {
  const host = $(selector);
  host.replaceChildren();
  entries.forEach(([label, value, tone]) => {
    const fact = document.createElement("dl"),
      term = document.createElement("dt"),
      detail = document.createElement("dd");
    fact.className = "fact";
    term.textContent = label;
    detail.textContent = value ?? "—";
    if (tone) detail.className = tone;
    fact.append(term, detail);
    host.appendChild(fact);
  });
}
function renderPlayKeys() {
  const select = $("#play-key");
  if (!select) return;
  const selected = select.value;
  select.replaceChildren();
  const auto = document.createElement("option");
  auto.value = "auto";
  auto.textContent = "自动路由（匿名 / Tier 回退 / Key 轮询）";
  select.appendChild(auto);
  const keys = debugData?.keys || {};
  [
    ["zen", "Zen Keys"],
    ["go", "Go Keys"],
  ].forEach(([tier, label]) => {
    const items = keys[tier] || [];
    if (!items.length) return;
    const group = document.createElement("optgroup");
    group.label = label;
    items.forEach((item) => {
      const option = document.createElement("option");
      option.value = `${tier}:${item.id}`;
      const cooling = item.cooldown_until
        ? ` · 冷却至 ${new Date(item.cooldown_until).toLocaleTimeString()}`
        : "";
      option.textContent = `${tier.toUpperCase()} · ${item.display} · ${item.id}${cooling}`;
      group.appendChild(option);
    });
    select.appendChild(group);
  });
  if (selected && [...select.options].some((option) => option.value === selected))
    select.value = selected;
  else select.value = "auto";
}
async function loadDebugModels() {
  try {
    debugData = await api("/api/debug/models");
    if (debugData.last_inference) lastPlayground = debugData.last_inference;
    const select = $("#play-model"),
      selected = select.value;
    select.replaceChildren();
    (debugData.models || []).forEach((item) => {
      const option = document.createElement("option");
      option.value = item.model;
      option.textContent = item.model;
      select.appendChild(option);
    });
    if (selected && [...select.options].some((option) => option.value === selected))
      select.value = selected;
    renderPlayKeys();
    if (!$("#play-request").value.trim()) resetPlayTemplate();
    renderDiagnostics();
  } catch (error) {
    toast(error.message, true);
  }
}
function playgroundTemplate() {
  const protocol = $("#play-protocol").value,
    model = $("#play-model").value || "YOUR_MODEL";
  if (protocol === "responses") return { model, input: "用一句话说明这个模型的能力。" };
  if (protocol === "anthropic")
    return {
      model,
      max_tokens: 512,
      messages: [{ role: "user", content: "用一句话说明这个模型的能力。" }],
    };
  return { model, messages: [{ role: "user", content: "用一句话说明这个模型的能力。" }] };
}
function resetPlayTemplate() {
  $("#play-request").value = JSON.stringify(playgroundTemplate(), null, 2);
}
$("#play-protocol").addEventListener("change", resetPlayTemplate);
$("#play-model").addEventListener("change", () => {
  try {
    const request = JSON.parse($("#play-request").value);
    request.model = $("#play-model").value;
    $("#play-request").value = JSON.stringify(request, null, 2);
  } catch (_) {
    resetPlayTemplate();
  }
});
$("#play-reset").onclick = resetPlayTemplate;
function displayPlayground(result) {
  lastPlayground = result;
  const keyTest = result.key_test
    ? {
        usable: "Key 可用",
        rejected: "Key 被拒绝",
        rate_limited: "Key 已被接受但受到限流",
        upstream_error: "上游异常，无法判定 Key",
        transport_error: "网络或代理异常，无法判定 Key",
        unavailable: "Key 或运行时不可用",
        request_error: "请求未成功",
      }[result.key_test] || result.key_test
    : "";
  $("#play-state").textContent = result.ok
    ? `请求成功 · HTTP ${result.http_status}`
    : `请求失败 · HTTP ${result.http_status}`;
  $("#play-state").className = `section-caption ${result.ok ? "ok" : "bad"}`;
  const route = result.route || {},
    eligibility = route.anonymous_eligibility || {},
    selected = result.selected_key,
    routeLabel = route.tier
      ? `${route.tier} / ${route.anonymous ? "anonymous" : route.key_id || "key"}`
      : "未路由";
  renderFacts("#play-facts", [
    ["HTTP 状态", result.http_status, result.ok ? "ok" : "bad"],
    ["耗时", `${result.duration_ms} ms`],
    ["Request ID", result.request_id || "—"],
    ["测试模式", selected ? `指定 ${selected.tier.toUpperCase()} Key` : "自动路由"],
    ["测试 Key", selected ? `${selected.display} · ${selected.id}` : "—"],
    [
      "Key 判断",
      keyTest || "自动路由未单独判定",
      result.key_test === "usable" ? "ok" : result.key_test ? "bad" : "",
    ],
    ["实际路由", routeLabel],
    ["原生协议", route.native_protocol || "—"],
    [
      "匿名判断",
      `${eligibility.allowed ? "允许" : "拒绝"} · ${eligibility.source || "—"}`,
      eligibility.allowed ? "ok" : "warn",
    ],
  ]);
  $("#play-output").textContent = JSON.stringify(result.response, null, 2);
  renderDiagnostics();
}
$("#play-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  let request;
  try {
    request = JSON.parse($("#play-request").value);
  } catch (error) {
    toast(`请求 JSON 无效：${error.message}`, true);
    return;
  }
  const keyValue = $("#play-key").value,
    key =
      keyValue === "auto"
        ? { mode: "auto" }
        : {
            mode: "selected",
            tier: keyValue.split(":")[0],
            id: keyValue.split(":").slice(1).join(":"),
          };
  $("#play-run").disabled = true;
  $("#play-state").textContent =
    key.mode === "selected" ? "指定 Key 测试进行中…" : "Gateway 请求进行中…";
  try {
    const result = await api("/api/debug/inference", {
      method: "POST",
      body: JSON.stringify({ protocol: $("#play-protocol").value, key, request }),
    });
    displayPlayground(result);
  } catch (error) {
    toast(error.message, true);
    $("#play-state").textContent = "诊断入口请求失败";
  } finally {
    $("#play-run").disabled = false;
  }
});
$("#play-copy").onclick = async () => {
  try {
    await navigator.clipboard.writeText($("#play-output").textContent);
    toast("原始响应已复制");
  } catch (_) {
    toast("浏览器不允许剪贴板访问", true);
  }
};
function renderDiagnostics() {
  if (!monitorData && !debugData) return;
  const metadata = debugData?.metadata || monitorData?.resources?.metadata || {},
    last = lastPlayground || debugData?.last_inference;
  renderFacts("#metadata-facts", [
    ["就绪", metadata.ready ? "是" : "否", metadata.ready ? "ok" : "warn"],
    ["模型数", formatTokens(metadata.models)],
    ["更新时间", metadata.updated_at ? new Date(metadata.updated_at).toLocaleString() : "尚未更新"],
    ["是否过期", metadata.stale ? "是" : "否", metadata.stale ? "warn" : "ok"],
    ["下次刷新", metadata.next_refresh ? new Date(metadata.next_refresh).toLocaleString() : "—"],
    ["最近错误", metadata.last_error || "无", metadata.last_error ? "bad" : "ok"],
  ]);
  renderFacts(
    "#last-playground",
    last
      ? [
          ["结果", last.ok ? "成功" : "失败", last.ok ? "ok" : "bad"],
          ["HTTP", last.http_status],
          ["耗时", `${last.duration_ms} ms`],
          ["Request ID", last.request_id || "—"],
          ["模型", last.route?.model || "—"],
          [
            "路由",
            last.route?.tier
              ? `${last.route.tier} / ${last.route.anonymous ? "anonymous" : last.route.key_id || "key"}`
              : "未路由",
          ],
        ]
      : [["状态", "尚无 Playground 追踪"]],
  );
  renderDiagnosticModels();
  const requests = $("#upstream-requests");
  requests.replaceChildren();
  (monitorData?.upstream?.requests || [])
    .slice(-100)
    .reverse()
    .forEach((request) => {
      const row = document.createElement("tr");
      appendCell(row, new Date(request.time).toLocaleTimeString());
      appendCell(row, request.request_id);
      appendCell(row, request.model);
      appendCell(row, request.tier || "—");
      appendCell(row, request.channel);
      appendCell(row, request.key_id || "—");
      const dac = attemptsCell(request);
      appendCell(row, dac.text, dac.tone, dac.title);
      const terminalResults = {
        stream_error: "流式响应失败",
        client_canceled: "客户端已取消",
      };
      const result = terminalResults[request.outcome]
        ? `HTTP ${request.status} · ${terminalResults[request.outcome]}`
        : request.status || request.outcome;
      appendCell(row, result, request.success ? "ok" : "bad");
      appendCell(row, `${request.duration_ms} ms`);
      requests.appendChild(row);
    });
  if (!requests.children.length) emptyRow(requests, 9, "尚无请求路由");
  const attempts = $("#upstream-attempts");
  attempts.replaceChildren();
  (monitorData?.upstream?.recent || [])
    .slice(-100)
    .reverse()
    .forEach((attempt) => {
      const row = document.createElement("tr");
      appendCell(row, new Date(attempt.time).toLocaleTimeString());
      appendCell(row, attempt.request_id);
      appendCell(row, attempt.model);
      appendCell(row, `${attempt.tier} / ${attempt.channel}`);
      appendCell(row, attempt.key_id);
      appendCell(row, attempt.proxy_node);
      appendCell(row, attempt.status || attempt.outcome, attempt.success ? "ok" : "bad");
      appendCell(row, `${attempt.duration_ms} ms`);
      attempts.appendChild(row);
    });
  if (!attempts.children.length) emptyRow(attempts, 8, "尚无上游尝试");
}
function renderDiagnosticModels() {
  const host = $("#diagnostic-models");
  if (!host || !debugData) return;
  const query = $("#diagnostic-model-search").value.trim().toLowerCase();
  host.replaceChildren();
  (debugData.models || [])
    .filter((item) => !query || item.model.toLowerCase().includes(query))
    .forEach((item) => {
      const eligibility = item.anonymous_eligibility || {},
        row = document.createElement("tr"),
        input = eligibility.input_cost,
        output = eligibility.output_cost,
        plan = [...(item.anonymous ? ["anonymous"] : []), ...(item.key_tiers || [])];
      appendCell(row, item.model);
      appendCell(row, `${item.native_protocol} · ${item.protocol_source}`);
      appendCell(
        row,
        item.route_error || (plan.length ? plan.join(" → ") : "—"),
        item.route_error ? "bad" : "",
      );
      appendCell(row, eligibility.allowed ? "允许" : "拒绝", eligibility.allowed ? "ok" : "warn");
      appendCell(row, eligibility.source || "—");
      appendCell(row, `${input == null ? "?" : input} / ${output == null ? "?" : output}`, "cost");
      host.appendChild(row);
    });
  if (!host.children.length) emptyRow(host, 6, "没有匹配模型");
}
$("#diagnostic-model-search").addEventListener("input", renderDiagnosticModels);
async function refreshMonitor() {
  try {
    monitorData = await api("/api/monitor");
    const metrics = monitorData.metrics;
    $("#version").textContent = `version ${monitorData.version}`;
    $("#m-total").textContent = Number(metrics.last_hour.total || 0).toLocaleString();
    $("#m-rate").textContent = `${((metrics.last_hour.success_rate || 0) * 100).toFixed(1)}%`;
    $("#m-p95").textContent = `${metrics.last_hour.p95_ms || 0} ms`;
    $("#m-active").textContent = `${metrics.active_requests || 0} / ${metrics.active_streams || 0}`;
    renderResources(monitorData.resources);
    renderBreakdowns(metrics);
    renderGuide();
    // Local: 用量页的范围/趋势/明细都由 /api/usage 驱动（按粒度取，最快 30s
    // 一次）；只有停在这一页时才重画 donut 与请求表——每 3 秒无条件重建
    // 几百行 DOM 与停在哪一页无关，配置中心页不该为此付代价。
    if (location.hash === "#usage") {
      renderUsageExtras();
      if (Date.now() - lastUsageFetch > 30000) fetchUsage();
    }
    renderDiagnostics();
  } catch (_) {}
}
$("#refresh-now").onclick = async () => {
  await Promise.all([refreshMonitor(), loadConfig(), loadDebugModels(), fetchUsage()]);
  if (location.hash === "#model-quotas") await loadModelQuotas();
  if (location.hash === "#api-keys") await loadAPIKeys();
  toast("状态已刷新");
};
function derivedAPIBase() {
  if (!config) return `${location.protocol}//${location.hostname}:8080`;
  const listen = config.effective?.listen || config.listen || ":8080";
  const bracket = listen.match(/^\[.*\]:(\d+)$/),
    plain = listen.match(/:(\d+)$/),
    port = (bracket || plain)?.[1] || "8080";
  return `${location.protocol}//${location.hostname}:${port}`;
}
function codeCard(label, text) {
  const card = document.createElement("div"),
    head = document.createElement("div"),
    title = document.createElement("span"),
    copy = document.createElement("button"),
    pre = document.createElement("pre"),
    code = document.createElement("code");
  card.className = "code-card";
  head.className = "code-head";
  title.textContent = label;
  copy.type = "button";
  copy.className = "copy";
  copy.textContent = "复制";
  copy.onclick = async () => {
    try {
      await navigator.clipboard.writeText(text);
      toast("示例已复制");
    } catch (_) {
      toast("浏览器不允许剪贴板访问", true);
    }
  };
  head.append(title, copy);
  code.textContent = text;
  pre.appendChild(code);
  card.append(head, pre);
  return card;
}
function renderAccessExamples() {
  if (!config) return;
  const base = derivedAPIBase(),
    model = $("#access-model")?.value.trim() || "YOUR_MODEL";
  $("#access-base-value").value = base;
  const protocols = $("#access-protocol-examples"),
    sdks = $("#access-sdk-examples");
  protocols.replaceChildren();
  sdks.replaceChildren();
  const chat = `curl ${base}/v1/chat/completions \\\n  -H "Authorization: Bearer YOUR_API_KEY" \\\n  -H "Content-Type: application/json" \\\n  -d '{"model":"${model}","messages":[{"role":"user","content":"Hello"}]}'`;
  const responses = `curl ${base}/v1/responses \\\n  -H "Authorization: Bearer YOUR_API_KEY" \\\n  -H "Content-Type: application/json" \\\n  -d '{"model":"${model}","input":"Hello"}'`;
  const anthropic = `curl ${base}/v1/messages \\\n  -H "x-api-key: YOUR_API_KEY" \\\n  -H "anthropic-version: 2023-06-01" \\\n  -H "Content-Type: application/json" \\\n  -d '{"model":"${model}","max_tokens":512,"messages":[{"role":"user","content":"Hello"}]}'`;
  protocols.append(
    codeCard("Chat Completions", chat),
    codeCard("Responses", responses),
    codeCard("Anthropic Messages", anthropic),
  );
  const python = `from openai import OpenAI\n\nclient = OpenAI(base_url="${base}/v1", api_key="YOUR_API_KEY")\nresponse = client.chat.completions.create(\n    model="${model}",\n    messages=[{"role": "user", "content": "Hello"}],\n)\nprint(response.choices[0].message.content)`;
  const javascript = `import OpenAI from "openai";\n\nconst client = new OpenAI({\n  baseURL: "${base}/v1",\n  apiKey: "YOUR_API_KEY",\n});\nconst response = await client.responses.create({\n  model: "${model}",\n  input: "Hello",\n});\nconsole.log(response.output_text);`;
  sdks.append(
    codeCard("Python · OpenAI SDK", python),
    codeCard("JavaScript · OpenAI SDK", javascript),
  );
}
$("#access-model").addEventListener("input", renderAccessExamples);
$$("[data-scroll]").forEach(
  (button) =>
    (button.onclick = () =>
      document.getElementById(button.dataset.scroll).scrollIntoView({ behavior: "smooth" })),
);
function connectLogs() {
  if (eventSource) eventSource.close();
  eventSource = new EventSource("/api/logs/stream");
  eventSource.addEventListener("log", (event) => {
    const item = JSON.parse(event.data);
    logEvents.push(item);
    if (logEvents.length > 2000) logEvents.shift();
    if (!paused && !logRenderPending) {
      logRenderPending = true;
      requestAnimationFrame(() => {
        logRenderPending = false;
        renderLogs();
      });
    }
  });
  eventSource.addEventListener("gap", () => toast("较早日志已从内存清除", true));
}
function renderLogs() {
  const level = $("#log-level").value,
    component = $("#log-component").value.toLowerCase(),
    query = $("#log-search").value.toLowerCase(),
    rows = logEvents
      .filter(
        (item) =>
          (!level || item.level === level) &&
          (!component || (item.component || "").toLowerCase().includes(component)) &&
          (!query || JSON.stringify(item).toLowerCase().includes(query)),
      )
      .slice(-500),
    host = $("#log-list");
  host.replaceChildren();
  rows.forEach((item) => {
    const row = document.createElement("div");
    row.className = "log";
    const timeNode = document.createElement("span"),
      levelNode = document.createElement("span"),
      componentNode = document.createElement("span"),
      messageNode = document.createElement("span"),
      fieldsNode = document.createElement("span");
    timeNode.className = "log-time";
    timeNode.textContent = new Date(item.time).toLocaleTimeString();
    levelNode.className =
      item.level === "error"
        ? "bad"
        : item.level === "warn"
          ? "warn"
          : item.level === "info"
            ? "ok"
            : "";
    levelNode.textContent = item.level;
    componentNode.className = "log-component";
    componentNode.textContent = item.component || "core";
    messageNode.textContent = item.message;
    fieldsNode.className = "log-fields";
    fieldsNode.textContent = JSON.stringify(item.fields || {});
    row.append(timeNode, levelNode, componentNode, messageNode, fieldsNode);
    host.appendChild(row);
  });
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = "等待日志事件…";
    host.appendChild(empty);
  }
  host.scrollTop = host.scrollHeight;
}
["#log-level", "#log-component", "#log-search"].forEach((id) =>
  $(id).addEventListener("input", renderLogs),
);
$("#log-pause").onclick = () => {
  paused = !paused;
  $("#log-pause").textContent = paused ? "继续" : "暂停";
  if (!paused) renderLogs();
};
let quotaData = null,
  quotaTimer = null,
  quotaLoading = false,
  createdKey = "",
  apiKeysLoading = false;
const localTime = (value) =>
  value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "—";

function clearCreatedKey() {
  createdKey = "";
  $("#api-key-env").textContent = "";
  $("#api-key-created").classList.add("hidden");
}

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    toast("已复制");
  } catch {
    toast("浏览器未允许复制，请手动选中复制", true);
  }
}

async function loadModelQuotas() {
  if (quotaLoading) return;
  quotaLoading = true;
  $("#quota-refresh").disabled = true;
  $("#quota-refresh").textContent = "读取中…";
  try {
    const value = await api("/api/model-quotas");
    if (!csrf) return;
    quotaData = value;
    const select = $("#quota-account"),
      selected = select.value;
    select.replaceChildren();
    for (const account of value.accounts || []) {
      const option = document.createElement("option");
      option.value = account.id;
      option.textContent = `Go Key ${account.display}`;
      select.appendChild(option);
    }
    if ([...select.options].some((option) => option.value === selected)) select.value = selected;
    if (!select.options.length) select.appendChild(new Option("尚未配置 Go Key", ""));
    renderModelQuotas();
  } catch (error) {
    $("#quota-status").textContent = `查询失败：${error.message}。已有显示仅为上次查询结果。`;
  } finally {
    quotaLoading = false;
    $("#quota-refresh").disabled = false;
    $("#quota-refresh").textContent = "刷新额度";
  }
}

function renderModelQuotas() {
  if (!quotaData) return;
  const account = quotaData.accounts.find((item) => item.id === $("#quota-account").value);
  const stale = account?.status === "stale";
  const cards = $("#quota-cards");
  cards.replaceChildren();
  $("#quota-status").textContent = !account
    ? "请先在配置中心添加 OpenCode Go Key。"
    : account.error
      ? `${account.error}${stale ? "；下方显示上次成功数据，当前额度未知。" : "；当前额度未知。"}`
      : "已连接 OpenCode · 共享套餐额度。多个 Key 的额度不相加。";
  $("#quota-updated").textContent = account?.fetched_at
    ? `${stale ? "上次成功查询" : "额度更新时间"}：${localTime(account.fetched_at)} · 缓存 60 秒 · 来源：OpenCode Go /v1/usage`
    : "";
  const windows = [
    ["rolling", "5 小时窗口"],
    ["weekly", "每周窗口"],
    ["monthly", "每月窗口"],
  ];
  for (const [id, label] of windows) {
    const item = account?.windows?.[id];
    const card = document.createElement("div"),
      heading = document.createElement("h3"),
      value = document.createElement("div"),
      progress = document.createElement("progress"),
      reset = document.createElement("div");
    card.className = `quota-card${item?.remaining_percent < 20 ? " quota-low" : ""}`;
    heading.textContent = label;
    value.className = "quota-value";
    value.textContent = item ? `≈ ${item.remaining_percent}%` : "未知";
    progress.max = 100;
    if (item) progress.value = item.remaining_percent;
    else progress.value = 0;
    progress.setAttribute("aria-label", `${label}剩余额度`);
    reset.className = "quota-reset";
    reset.textContent = item
      ? `${stale ? "历史剩余" : "剩余"} · ${item.status === "rate-limited" ? "已限额 · " : ""}重置于 ${localTime(item.resets_at)}`
      : "等待上游额度数据";
    card.append(heading, value, progress, reset);
    cards.appendChild(card);
  }
  const query = $("#quota-model-search").value.trim().toLowerCase();
  const models = quotaData.models.filter((model) => model.id.toLowerCase().includes(query));
  $("#quota-model-count").textContent = `${models.length} / ${quotaData.models.length}`;
  const catalog = quotaData.catalog;
  $("#quota-catalog-status").textContent =
    `${catalog?.stale ? "模型目录暂未更新，显示缓存 · " : ""}目录更新时间：${localTime(catalog?.updated_at)}`;
  const host = $("#quota-models");
  host.replaceChildren();
  for (const model of models) {
    const row = document.createElement("tr");
    appendCell(row, model.id, "mono");
    appendCell(
      row,
      { chat: "Chat Completions", responses: "Responses", anthropic: "Messages" }[model.protocol] ||
        model.protocol,
    );
    for (const [id] of windows) {
      const item = account?.windows?.[id];
      appendCell(row, item ? `≈ ${item.remaining_percent}%${stale ? "（历史）" : ""}` : "未知");
    }
    const cell = document.createElement("td"),
      button = document.createElement("button");
    button.className = "ghost";
    button.textContent = "复制名称";
    button.onclick = () => copyText(model.id);
    cell.appendChild(button);
    row.appendChild(cell);
    host.appendChild(row);
  }
  if (!models.length)
    emptyRow(host, 6, quotaData.models.length ? "没有匹配的模型" : "模型目录尚未就绪，请稍后刷新");
}
$("#quota-refresh").onclick = loadModelQuotas;
$("#quota-account").onchange = renderModelQuotas;
$("#quota-model-search").oninput = renderModelQuotas;

async function loadAPIKeys() {
  if (apiKeysLoading) return;
  apiKeysLoading = true;
  $("#api-key-refresh").disabled = true;
  try {
    const data = await api("/api/api-keys");
    if (csrf) renderAPIKeys(data.keys);
  } catch (error) {
    $("#api-key-status").textContent = `读取失败：${error.message}`;
  } finally {
    apiKeysLoading = false;
    $("#api-key-refresh").disabled = false;
  }
}

async function mutateAPIKey(id, method, body, button) {
  button.disabled = true;
  try {
    const data = await api(`/api/api-keys/${encodeURIComponent(id)}`, {
      method,
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    if (!csrf) return;
    renderAPIKeys(data.keys);
    await loadConfig();
    toast("已保存并立即生效");
  } catch (error) {
    toast(error.message, true);
  } finally {
    button.disabled = false;
  }
}

function renderAPIKeys(keys) {
  $("#api-key-count").textContent = `${keys.length} 个`;
  $("#api-key-status").textContent = `${keys.filter((key) => key.enabled).length} 个已启用`;
  const host = $("#api-key-list");
  host.replaceChildren();
  for (const key of keys) {
    const row = document.createElement("tr");
    appendCell(row, key.name);
    appendCell(row, key.display, "mono");
    appendCell(row, key.enabled ? "已启用" : "已停用", key.enabled ? "status ok" : "status warn");
    const usage = key.usage || {},
      summary = (value = {}) => `${formatTokens(value.requests)} 次 · ${formatTokens(value.total_tokens)} tokens`;
    appendCell(row, `${summary(usage.last_hour)} / ${summary(usage.lifetime)}`);
    appendCell(row, key.created_at ? localTime(key.created_at) : "已有配置");
    const cell = document.createElement("td"),
      actions = document.createElement("div");
    actions.className = "key-actions";
    for (const [label, action] of [
      [
        "重命名",
        (button) => {
          const name = prompt("密钥名称", key.name);
          if (name !== null) mutateAPIKey(key.id, "PATCH", { name }, button);
        },
      ],
      [
        key.enabled ? "停用" : "启用",
        (button) => mutateAPIKey(key.id, "PATCH", { enabled: !key.enabled }, button),
      ],
      [
        "删除",
        (button) => {
          if (confirm(`删除“${key.name}”后，使用它的应用将无法继续访问。确定删除？`))
            mutateAPIKey(key.id, "DELETE", null, button);
        },
      ],
    ]) {
      const button = document.createElement("button");
      button.className = "ghost";
      button.textContent = label;
      button.onclick = () => action(button);
      actions.appendChild(button);
    }
    cell.appendChild(actions);
    row.appendChild(cell);
    host.appendChild(row);
  }
  if (!keys.length) emptyRow(host, 6, "暂无 API Key");
}
$("#api-key-form").onsubmit = async (event) => {
  event.preventDefault();
  const button = $("#api-key-create");
  button.disabled = true;
  try {
    const data = await api("/api/api-keys", {
      method: "POST",
      body: JSON.stringify({ name: $("#api-key-name").value, value: $("#api-key-value").value }),
    });
    if (!csrf) return;
    createdKey = data.key;
    const goModels = debugData?.models?.filter((item) => item.available_go) || [];
    const model =
      goModels.find((item) => item.model === "glm-5.3-flash")?.model ||
      goModels[0]?.model ||
      "YOUR_MODEL";
    $("#api-key-env").textContent =
      `OPENAI_API_KEY=${data.key}\nOPENAI_BASE_URL=${derivedAPIBase()}/v1\nOPENAI_MODEL=${model}`;
    $("#api-key-created").classList.remove("hidden");
    $("#api-key-form").reset();
    renderAPIKeys(data.keys);
    await loadConfig();
    toast("API Key 已创建");
  } catch (error) {
    toast(error.message, true);
  } finally {
    button.disabled = false;
  }
};
$("#api-key-refresh").onclick = loadAPIKeys;
$("#api-key-dismiss").onclick = clearCreatedKey;
$("#api-key-copy").onclick = () => copyText(createdKey);
$("#api-key-copy-env").onclick = () => copyText($("#api-key-env").textContent);
let rotationData = null,
  rotationDirty = false;
const rotationReasons = {
  success: "成功",
  client_canceled: "调用方取消",
  quota_or_rate_limit: "额度不足 / 限流",
  transport_error: "连接错误",
  first_output_timeout: "首个输出超时",
  stream_error: "输出中断",
  response_read_error: "读取响应失败",
  response_conversion_error: "响应解析或转换失败",
  request_conversion_error: "请求转换失败",
  upstream_error: "上游错误",
};
async function loadRotation() {
  try {
    const value = await api("/api/rotation");
    if (!csrf) return;
    rotationData = value;
    rotationDirty = false;
    renderRotation();
  } catch (error) {
    toast(error.message, true);
  }
}
function renderRotation() {
  const state = rotationData;
  $("#rotation-status").textContent =
    state.persistence_error || "状态已更新 · 两组独立轮转，所有调用方共享当前位置。";
  $("#rotation-timeout").value = state.first_output_seconds;
  const host = $("#rotation-groups");
  host.replaceChildren();
  for (const group of state.groups) {
    const section = document.createElement("div");
    section.className = "section";
    const title = document.createElement("h2");
    title.textContent = group.id === "sota" ? "SOTA" : "甜点";
    const label = document.createElement("label");
    label.textContent = "调用别名";
    label.htmlFor = `rotation-alias-${group.id}`;
    const input = document.createElement("input");
    input.id = label.htmlFor;
    input.value = group.alias;
    input.required = true;
    input.maxLength = 64;
    input.oninput = () => {
      group.alias = input.value;
      rotationDirty = true;
    };
    const field = document.createElement("div");
    field.className = "field";
    field.append(label, input);
    const current = document.createElement("p");
    current.className = "mono";
    current.textContent = `当前模型：${group.current || "等待可用模型"}`;
    const table = document.createElement("table");
    table.className = "table";
    const header = document.createElement("tr");
    for (const text of ["顺序", "模型", "状态 / 恢复时间", "操作"]) {
      const th = document.createElement("th");
      th.textContent = text;
      header.append(th);
    }
    const thead = document.createElement("thead");
    thead.append(header);
    table.append(thead);
    const tbody = document.createElement("tbody");
    table.append(tbody);
    group.order.forEach((model, index) => {
      const row = document.createElement("tr");
      appendCell(row, String(index + 1));
      appendCell(row, model, "mono");
      const until = group.paused[model],
        paused = until && new Date(until) > new Date();
      appendCell(
        row,
        !group.available[model]
          ? "已下架 / 暂不可用"
          : paused
            ? `暂停至 ${localTime(until)}`
            : model === group.current
              ? "当前使用"
              : "可用",
      );
      const actions = document.createElement("td");
      actions.className = "rotation-actions";
      for (const [text, delta] of [
        ["上移", -1],
        ["下移", 1],
      ]) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "ghost";
        button.textContent = text;
        button.setAttribute("aria-label", `${model} ${text}`);
        button.disabled = index + delta < 0 || index + delta >= group.order.length;
        button.onclick = () => {
          [group.order[index], group.order[index + delta]] = [
            group.order[index + delta],
            group.order[index],
          ];
          state.first_output_seconds = Number($("#rotation-timeout").value);
          rotationDirty = true;
          renderRotation();
        };
        actions.append(button);
      }
      const select = document.createElement("button");
      select.type = "button";
      select.className = "ghost";
      select.textContent = "设为当前";
      select.disabled = !group.available[model];
      select.setAttribute("aria-label", `使用 ${model}`);
      select.onclick = async () => {
        select.disabled = true;
        try {
          const result = await api("/api/rotation/current", {
            method: "POST",
            body: JSON.stringify({ group: group.id, model }),
          });
          // Keep unsaved alias/order edits while refreshing operational state.
          for (const item of state.groups) {
            const live = result.groups.find((x) => x.id === item.id);
            Object.assign(item, {
              current: live.current,
              paused: live.paused,
              switches: live.switches,
            });
          }
          state.persistence_error = result.persistence_error;
          renderRotation();
          toast("当前模型已切换");
        } catch (error) {
          select.disabled = false;
          toast(error.message, true);
        }
      };
      actions.append(select);
      row.append(actions);
      tbody.append(row);
    });
    if (!group.order.length) emptyRow(tbody, 4, "等待 Go 模型目录");
    const wrap = document.createElement("div");
    wrap.className = "table-wrap";
    wrap.append(table);
    const recent = document.createElement("div");
    recent.className = "rotation-switches";
    const caption = document.createElement("h3");
    caption.textContent = "最近切换";
    recent.append(caption);
    for (const item of (group.switches || []).slice(-5).reverse()) {
      const line = document.createElement("p");
      line.textContent = `${localTime(item.time)} · ${item.from} → ${item.to} · ${rotationReasons[item.reason] || item.reason}`;
      recent.append(line);
    }
    if (!group.switches?.length) {
      const line = document.createElement("p");
      line.textContent = "尚无切换记录";
      recent.append(line);
    }
    section.append(title, field, current, wrap, recent);
    host.append(section);
  }
  const attempts = $("#rotation-attempts");
  attempts.replaceChildren();
  for (const item of (state.attempts || []).slice().reverse()) {
    const row = document.createElement("tr");
    for (const value of [
      localTime(item.time),
      item.request,
      item.alias,
      item.model,
      String(item.number),
      rotationReasons[item.outcome] || item.outcome,
    ])
      appendCell(row, value);
    attempts.append(row);
  }
  if (!state.attempts?.length) emptyRow(attempts, 6, "调用分组别名后显示最近 100 次尝试");
}
$("#rotation-refresh").onclick = () => {
  if (rotationDirty) {
    toast("有未保存的修改，请先保存后刷新", true);
    return;
  }
  loadRotation();
};
$("#rotation-timeout").oninput = () => {
  rotationDirty = true;
};
$("#rotation-form").onsubmit = async (event) => {
  event.preventDefault();
  if (!rotationData) return;
  const button = $("#rotation-save");
  button.disabled = true;
  try {
    const body = { first_output_seconds: Number($("#rotation-timeout").value) };
    for (const group of rotationData.groups)
      body[group.id] = { alias: group.alias.trim(), order: group.order };
    rotationData = await api("/api/rotation", { method: "PUT", body: JSON.stringify(body) });
    rotationDirty = false;
    renderRotation();
    toast("模型轮转配置已保存并生效");
  } catch (error) {
    toast(error.message, true);
  } finally {
    button.disabled = false;
  }
};
boot();

async function loadAvailability() {
  try {
    const data = await api("/api/models/availability");
    $("#availability-error").textContent = "";
    $("#availability-models").replaceChildren();
    for (const item of data.models) {
      const row = document.createElement("tr");
      const date = (value) =>
        value && !value.startsWith("0001-") ? new Date(value).toLocaleString() : "待探测";
      for (const value of [
        item.model,
        item.disabled ? "已禁用" : "已启用",
        [item.channel, item.reason].filter(Boolean).join(" / ") || "待探测",
        item.manual ? "手动" : "自动",
        item.auto_effort?.effort
          ? `${item.auto_effort.effort}（${date(item.effort_checked_at)}）`
          : "未验证",
        date(item.checked_at),
        item.manual ? "手动控制，不自动探测" : date(item.next_check),
      ]) {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      }
      const cell = document.createElement("td");
      const button = document.createElement("button");
      button.textContent = "恢复启用";
      button.disabled = !item.disabled;
      button.onclick = async () => {
        button.disabled = true;
        toggle.disabled = true;
        try {
          await api("/api/models/restore", {
            method: "POST",
            body: JSON.stringify({ model: item.model }),
          });
          await loadAvailability();
          await loadDebugModels();
        } catch (error) {
          $("#availability-error").textContent = error.message;
          button.disabled = !item.disabled;
          toggle.disabled = false;
        }
      };
      cell.append(button);
      const toggle = document.createElement("button");
      toggle.className = "ghost";
      toggle.textContent = item.manual ? "切回自动" : "切为手动";
      toggle.onclick = async () => {
        button.disabled = true;
        toggle.disabled = true;
        try {
          await api("/api/models/manual", {
            method: "POST",
            body: JSON.stringify({ model: item.model, manual: !item.manual }),
          });
          await loadAvailability();
          await loadDebugModels();
        } catch (error) {
          $("#availability-error").textContent = error.message;
          button.disabled = !item.disabled;
          toggle.disabled = false;
        }
      };
      cell.append(toggle);
      row.append(cell);
      $("#availability-models").append(row);
    }
    if (!data.models.length)
      $("#availability-error").textContent = "当前模型目录中没有符合免费资格的模型。";
  } catch (error) {
    $("#availability-error").textContent = error.message;
  }
}
$("#availability-refresh").onclick = loadAvailability;
