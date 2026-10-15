# AGENTS.md

> 本地运维契约（上游仓库没有 AGENTS.md，本文件由 `local` 分支维护）。
> 2026-10-15 起基线 = 上游 `main`（含其自带的免费层修复），本地保留字体、启动脚本与本文件，
> 外加三件按基线重新接过的本地特性（使用记录页 / staleMemo 中毒治理 / 看门狗决策通道，见末节）。

多包 Go 网关：把 OpenCode Zen / Zen Go 转为 OpenAI（Chat / Responses / Models）与 Anthropic Messages 协议。后端在 `cmd/opencode2api`（入口）与 `internal/*` 包（`gateway` 路由/转发/冷却、`protocol` 转换、`models` 目录与价格、`admin` 管理 API、`telemetry` 内存指标、`config` 配置、`identity` 请求身份、`httpx`/`jsonutil`/`buildinfo` 基础件），前端为 `webui/index.html` + `webui/app.js` + `webui/styles.css`（经 `webui/embed.go` 中 `//go:embed` 打包，改完必须重新构建）。改完 WebUI 先跑 `node --check webui/app.js`，通过再构建。

## 构建与验证

- 需 Go 1.24+：`go build -o opencode2api.exe ./cmd/opencode2api`（`start.bat`/`start.ps1` 认这个文件名）。
- 校验：`go vet ./...`；CI（`v*` tag / 手动触发）跑 `go test ./...`。
- **上游 `main` 本身零 `_test.go`**（2026-10-15 核实）；本地已补测试集中在 `internal/admin`、`internal/gateway`、`internal/telemetry` 三处，覆盖使用记录页、staleMemo 预剥离、看门狗决策。`go test ./...` 通过不代表行为被全锁定——尤其是网关热路径，改语义时仍要自己补最小内存用例，并确认新用例**能证伪**（改坏→红→恢复→绿）。
- 上游已删掉 npm/Prettier 工具链（无 `package.json`），WebUI 只能靠 `node --check webui/app.js` 做语法检查。
- 跨平台 / 镜像构建以 `.github/workflows/release.yml` 与 `Dockerfile` 为准：`CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=$TAG"`（入口包保留 `var version` 作注入点）；不要改构建 flag。
- 无 lint / formatter 配置，不要整仓 `gofmt -w`；换行跟随 `.gitattributes`（`text=auto eol=lf`）。

## 配置（最易踩坑）

- 入口：`cp config.example.json config.json`，启动参数 `-config`、`-listen`、`-web-listen` 覆盖文件值；Docker 下另有 `CONFIG_PATH` / `CONFIG_SEED_PATH` / `LISTEN_ADDRESS` / `WEBUI_LISTEN_ADDRESS`（见 `docker-entrypoint.sh`）。
- `config.json` 支持 `//` 与 `/* */` 注释（`internal/config:stripJSONComments`，字符串内的 `https://` 不受影响）；`LoadConfig` 用 `DisallowUnknownFields`，未知字段直接启动失败，增删字段必须同步 `Config` 结构体。
- 校验集中在 `NormalizeConfig`（`internal/config`）：`server_keys` 必填；`anonymous=false` 时 zen/go 至少一池非空；`prefer` 只能 `zen`/`go`；代理仅支持 `direct,http,https,socks5,socks5h`。
- **三个上游 base 要分清**：`upstream.zen`（带 key 的 Zen 通道）、`upstream.go`、`upstream.anonymous`（默认 `https://opencode.ai/inference`，免费层专用，2026-10-14 上游把免费层从 `/zen/v1/*` 搬到了这里）。改上游地址时只改 `upstream.zen` 不会影响匿名流量——这点曾经浪费一整天排查。
- `proxyfile` 相对路径以 `config.json` 所在目录为基准，与 `proxies` 合并去重；合并结果只存 `effectiveProxies`（内存），`PasswordForSave` 会清掉它，落盘永远不写合并结果，不要「修复」此行为。
- 热重载：`listen`、`webui.listen`、`webui.enabled` 保存后需重启进程，其余（keys/代理/上游/重试/模型/性能/日志级别）经 `RuntimeManager.Apply`（`internal/gateway/runtime.go`）立即生效：先验证候选配置并构建新 Gateway，失败则保留旧实例，成功后原子切换，老请求在旧池上继续。
- 本地生成文件勿提交、勿手改：`config.json`、`watchdog.json`、`*.bak`、`*~`、`*.models.*.json`、`opencode2api.exe*`、`.codegraph` 下的运行时文件。
- 首次启动会把 `webui.password`（≥10 字符）转 Argon2id 存入 `password_hash` 并删除明文及 `.bak`。

## 运行时约定（已对当前基线核实）

- 健康检查 `GET /healthz` 免鉴权；刚启动模型目录未就绪时短暂 `503 starting` 属正常。
- 模型 ID 永不硬编码：Zen/Go `/v1/models` + `models.opencode.ai/api.json`（协议）+ `models.dev`（成本/弃用）合并；`models.protocols` 仅用于覆盖实验模型。
- 路由顺序：免费模型先走匿名（`Bearer public` → `upstream.anonymous`），失败后按 `prefer` 在 Go/Zen key 间回退；带 key 的免费模型同样会被 `shapeKeyBody` 套上 agent 形状（`stream:true` + 工具）。
- **免费层身份形状（2026-10-14/15 抓包实测）**：打 `/inference` 时
  - `User-Agent: opencode/<channel>/<version>/<artifact>`（如 `opencode/latest/2.0.29/cli`，原生四段式，别退回裸 `opencode/<版本>`）
  - `x-opencode-client: cli`、`x-opencode-project: <40 位 hex>`（不是 `prj_…`）
  - 会话同值四连：`x-opencode-session` / `x-opencode-session-id` / `x-session-affinity` / `X-Session-Id`
  - **不发 `x-opencode-request`**（上游对这个遗留头直接 403）
  - 工具用 `internal/gateway/opencode_tools.json` 里的**真实定义**（v2 名字：`shell`/`edit`/`glob`/`grep`/`read`…，社区那套 `bash`/`task`/`todowrite` 是 v1 旧名），`stream:true`，非流式客户端由网关把 SSE 聚合回 JSON
  - 上游对**所有 tier** 都看客户端身份（头 + 版本），工具注入则是**免费模型**在匿名与带 key 两条 lane 上都要加（`shapeKeyBody`）；别为了省体积把它改成空壳声明
- 会话 ID：`identity.CanonicalSessionID` 对非规范信号做确定性重编码（`descendingTimePart`：hash 的 36 位移入高 36 位、`| counter=1`、整体取反 → 12 位 hex 尾部 `ffe`）。实测门只接受 counter 1–63（低 12 位 `0xFC0–0xFFF`），**别改回摘要直出、也别改计数器窗口**；已有规范形状的原样透传以保住上游 prompt-cache 亲和。
- 冷却：按 proxy 指数退避（`1x/2x/4x/8x` 封顶），上游 `Retry-After` 被 `maxAnonymousRetryAfterMultiplier=8` 夹住（匿名 lane 没有「最早到期节点」兜底，挂几个小时会把出口跳过后不再回来）；key 通道仍全量尊重 `Retry-After`。代理恢复时 `RestoreProxy` 会清匿名节点的冷却残留。
- 上游透传错误保持原样（`error.type` 前缀）；探测/健康接口只允许出现 key 末 5 位（`config.KeyDisplayID`）与脱敏代理 URL（`RedactURL`），禁止输出完整 key、`Authorization`、`Cookie`、代理密码、请求正文。
- 流一旦向客户端输出即不再切换节点重试；未知 content block 类型必须返回明确转换错误，禁止静默丢弃（`protocol:decodeOpenAIBlocksChecked` / `decodeAnthropicBlocksChecked`）。上游 reasoning 回放走 `encodeResponsesReasoningInput`：无上游 ID 且无 `encrypted_content` 的历史项整体丢弃、只保留文本上下文，否则多轮第二轮必被上游 `400 Referenced reasoning item … not found` 拒绝。

## 2026-10-14 排查结论（别重跑）

- 老匿名路径 `https://opencode.ai/zen/v1/*` 现在对**所有第三方客户端**一律 403 `FreeTierError`，而真 opencode CLI 同时刻正常。当时做过一整轮二分：把真客户端的请求头逐条 + 43KB 请求体原样重放（Node fetch 与 Go 两种栈）仍 403；UA 三形态、补三个会话头、工具集 5/10/12 个、`stream:false`、两条 endpoint、直连与代理两种出口 IP，全 403。
- 当时我据此写下「门在头/体可达范围之外、像是 TLS 指纹」——**这个结论是错的**。真因是免费层端点搬到了 `/inference`，请求头与工具定义同时换了一套。在新端点上，上面的形状一次就通。
- 教训：社区 #4836、fork commit `410536d`/`236e74b` 的「10 工具指纹补丁」是在老端点上试出来的，照抄进我们这条路径无效；遇到整族 403 先确认请求打到哪个 base，再谈头和体。

## 改代码时

- 包布局（上游现状）：`internal/gateway`（`gateway.go` 路由入口、`upstream.go` 转发/重试/agent 形状、`pool.go` key 与匿名冷却（含看门狗 cause/年龄记忆）、`probe.go` 免费模型探测、`health.go` 健康与模型列表、`refresh.go` 目录刷新、`rotation.go`、`runtime.go` 热重载、`availability.go`、`first_event.go`、`opencode_tools.json`；本地加的：`stale_memo.go` 预剥离记忆、`watchdog.go` 只读决策端点与 verdict 分类）、`internal/protocol`（转换与流）、`internal/models`（目录与价格）、`internal/admin`（管理 API：除登录外均需 session，POST/PUT 另需 CSRF；`usage.go` 是 `/api/usage`；诊断请求经 `WithDiagnosticRequest` 隔离，永不写生产池状态）、`internal/telemetry`（指标本身纯内存，但使用记录页的小时桶与请求明细会落盘到 `*.usage.history.json`，重启读回；其余指标重启清空；`usage_history.go`/`usage_range.go`/`usage_cost.go` 是使用记录页的存储与聚合）、`internal/identity/request.go`（会话与请求身份）。改动前先读对应文件的现有注释。
- 动管理 API 时同步检查 session/CSRF/限速与敏感字段脱敏；动模型路由用 `GET /api/debug/models` 与 `POST /api/debug/inference`（强制非流式）验证；动免费层先看本文「免费层身份形状」。
- WebUI 字体在 `webui/styles.css`：正文 `body font` 与等宽主组（`.mono,code,pre,.eyebrow,.nav-no,.metric,.table th,.log`）首选 JetBrains Mono，中文靠 `Microsoft YaHei` 等系统回退，无外部字体依赖。改完 `node --check webui/app.js` 再 `go build`。

## 本地维护（上游 clone，不推回）

- 本仓库是上游项目的本地 clone，修改只在 `local` 分支维护，`main` 永远干净跟随 `origin/main`，禁止 push。
- 提交范围：源码（`cmd/**`、`internal/**`、`webui/*`、`AGENTS.md`、`.gitattributes`、`.gitignore`）＋个人脚本（`start.bat`/`start.ps1`、`watchdog-clash.ps1`、`watchdog.example.json`）＋ `.codegraph/.gitignore`；`.codegraph/codegraph.db` 不收；其余（`config.json`、`watchdog.json`、`*.usage.history.json*`、`.usage-history-*.tmp`、`*.bak`、`*~`、`*.models.*.json`、`opencode2api.exe*`）永不 `git add`。
- **当前合并状态（2026-10-15，已完成）**：上游 `main` 35 个提交并入（旧基线 `1e2702f` → `25afadd`）。本地原有 12 个主题提交与上游同期实现高度重叠，逐 hunk rebase 冲突过大，已按「移植分支」路线重建：`local` = 上游 `main` + 字体/启动脚本 + 本文件 + 三件按基线重新接过的本地特性，每件一个提交：
  1. `9aa0c36` 使用记录页：`GET /api/usage`（小时/天/周/月区间、成本、缓存命中率、最近 24h 请求明细）+ 小时桶落盘（`*.usage.history.json`、0600、原子写、仅变更时写、400 天）+ webui 用量页。明细放 `/api/usage`，`/api/monitor` 保持上游 1 小时窗口（环内存仍留 24h）。
  2. `f311f6d` staleMemo 中毒治理：`internal/gateway/stale_memo.go` 按会话记忆被剥掉的 reasoning id / 失效 prev，下一轮**发出请求前**预剥离（不再吃一次 400），选择剥离保住新增的健康推理；telemetry 落 `prestripped`/`prestrip_mode`/`prestrip_ids`（能活过落盘往返）。
  3. `c417d07` 看门狗决策通道：`GET /v1/watchdog/status`（server key 鉴权、只读）+ `watchdog-clash.ps1` / `watchdog.example.json`（与旧版字节一致）。cause 与成功/失败年龄挂在 `anonymousPool` 冷却状态旁，只由**用户流量**推进；后台目录刷新不算恢复证据；冷却退避语义与上游透传错误**一字未改**。`watchdog.json` 是脚本只读配置（`exclude_contains`/`deprioritize`，支持注释）。
  - 移植时每件单独一个提交并跑 `go vet ./... && go build ./cmd/opencode2api && go test -count=1 ./...`；本轮还做了每件的变异证据（改坏→红→恢复→绿）与独立审查。
- **故意不移植的旧本地子系统**（上游已自研实现取代，别在后续移植里复活）：`internal/gateway/anonymous_cooling.go`、`anonymous_errors.go`（指纹作用域冷却 + `opencode2api:` 合成错误——上游把上游错误原样透传，这条是基线约定）、`internal/identity/{canonical,fingerprint,ua_probe}.go`、`internal/protocol/{collect,fingerprint,reasoning_labels}.go`、大请求零拷贝懒编码。代码只存在于 `local-pre-upstream-20261015`；该分支与 `try-rebase`/`bak_0915` 现在可删，但删之前确认你不需要回看这些子系统。
- 本地 opencode 源码在 `F:\go\src\opencode`（clone 可能落后于线上，如 2026-10-15 本机 CLI 已是 v2.0.29 而 clone 停在 10-10）：对齐原生行为以该目录为准，但版本号与端点这类「线上才变」的事实，以上游 `main` 的实现与抓包为准。
- 上游更新流程：`fetch` 后先 `log/stat` 看量；默认先在一次性分支试 rebase，冲突过多就退到移植分支（从 `main` 新开分支逐件移植），验证通过再替换 `local`，**替换前先留 `local-pre-upstream-<日期>` 备份分支**。

```bash
git fetch origin
git log --oneline main..origin/main
git diff --stat main...origin/main
git checkout main && git pull --ff-only
# 首选：试 rebase（先在一次性分支上试，成了再动 local）
git checkout -b try-rebase local && git rebase main
# 冲突过多：git rebase --abort，改走移植分支
git checkout -b local-rebase main
# ...逐件移植并提交...
go vet ./... && go build ./cmd/opencode2api && go test ./...
```
