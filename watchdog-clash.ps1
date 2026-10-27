<#
.SYNOPSIS
  Clash Verge (verge-mihomo) 节点自动切换看门狗 — 被动式(A 跑法)。

.DESCRIPTION
  每隔 PollSeconds 秒做一次只读巡检，只在命中以下信号时才动作：
    1) 当前 Selector 节点在 Mihomo 侧 alive=false / 消失        → 刷新后原地复测一次，仍坏才切换
    2) 网关 /v1/watchdog/status 报 proxies.all_down=true（传输全坏） → 刷新后原地复测一次，仍坏才切换
    3) 网关 /v1/watchdog/status 报 anonymous.verdict=transport（传输坏，常见为 cause=transport (connection failed) 网络抖动） → 累计 TransportConfirmRounds 轮（默认 2，可用 watchdog.json 的 transport_confirm_rounds 或命令行 -TransportConfirmRounds 改）仍坏才刷新，刷新后原地复测一次，仍坏才切换（单个 ok/限流轮不再清零，连续 4 轮非 transport 才衰减；网关冷却过期后 5 分钟内仍报 transport，防“两次失败→过期→误报恢复”死循环）
    4) 网关 /v1/watchdog/status 报 anonymous.verdict=quota（额度尽） → 与上面三类同样先刷新，刷新后原地复测一次（刷新换的是同名节点背后的出口 IP，额度可能已恢复），仍坏才切换；探针走网关的 /v1/watchdog/probe 时只测匿名通道、不回退付费 key，复测 200 就等于这条出口 IP 的匿名通道真恢复了（旧探针 legacy 才需要再复查 verdict 翻离 quota）
    5) WARP 模式：Clash SOCKS5 中继健康、但网关 /healthz 的 warp.failure_streak 连续达到
       阈值（或出口池持续 unavailable/active=0） → 按 watchdog.json 的 warp_exit_failure.action
       刷新订阅/切换节点，并 POST /v1/watchdog/warp/reset 让旧 WARP 出口退役（默认关闭）
  verdict=ratelimit/cooling（纯限流/普通冷却）只记日志，不刷新、不切换、不验证；verdict=ok 且有 transport 前科未验证时只打一次节流验证探针（5 分钟一次），无前科的 ok 轮不验证。
  切换前不再为确认 verdict 打探针：verdict 已由网关内部分类给出原因，不花配额；
  仅订阅刷新成功后打一次恢复复测（见下），恢复则跳过切换。
  正常巡检降噪：一切正常时每 NormalLogMinutes 分钟（默认 20）只打一条心跳，
  其余轮次静默计数；异常/切换/限流/冷却不受影响仍每轮打印，恢复正常后立即补一条。
  节流只对常驻循环有效（-Once 每次新进程首轮必打）；若超过 NormalLogMinutes 仍无任何日志
  且无“进入故障切换”行，即视为看门狗异常（正常静默不会超过心跳周期）。
  切换后打一次 big-pickle 探针，默认走网关的 POST /v1/watchdog/probe：它保证请求真的
  发到上游（忽略代理传输健康闸门与冷却——这两样挂在新节点上时，旧探针会拿到本地合成的
  502「all upstream attempts failed」，而 502 又被判成 transport，整池候选就被烧在一个
  换节点根本解不开的闸门上）、不写任何池状态（不自造退避、不伪造恢复证据）、并直接回
  机器可读的 verdict（quota/transport/ratelimit/fingerprint/config/ok）。
  网关没有该端点（404）时自动回落旧的 /v1/chat/completions 探针，也可用 -ProbeEndpoint legacy 强制。
  探针通过即结束，失败则按延迟从低到高试下一个候选，最多 MaxCandidates 个(默认120，可用
  watchdog.json 的 max_candidates 或命令行 -MaxCandidates 改，上限 300)。
  注意最坏单轮耗时：MaxCandidates(默认120) × VerifyTimeoutSeconds(默认120s) + SwitchGapSeconds ≈ 4 小时，
  外加每个候选最多 ProxyReadySeconds(默认5s)+10s（闸门等待里最后一次 status 调用自身带 10s 超时）与候选间 2s 收敛等待：三笔合计真最坏 ≈ 4.5 小时；
  刷新后的原地复测另加最多 1 × VerifyTimeoutSeconds（quota 轮现在也计入），刷新本身还有下载+校验+热重载，
  同步阻塞巡检循环属设计如此（不是卡死），调参前请知悉。真实故障里失败候选往往秒回错误，不会真跑满这个上界。
  进入切换前先刷新一次订阅(受 RefreshCooldownMinutes 节流，每轮最多 1 次)，
  再基于最新节点列表選取，避免旧列表里只剩高延迟节点可换，
  transport/all_down/节点失效/quota 四类在刷新成功后先等 RefreshSettleSeconds 秒（默认 3），
  再原地复测一次当前节点：恢复则跳过切换（机场常在同名节点背后换 IP/凭证，
  刷新即可恢复；transport 需先连续 TransportConfirmRounds 轮确认才到这一步，见 3)；
  仍失败才进入切换循环。quota 同样走这条快捷路径：额度按出口 IP 计，
  而刷新换的正是同名节点背后的出口 IP，原 IP 额度耗尽不代表新 IP 也用尽，
  复测一次比直接换节点（掐连接+再探针）更省；复测若报网关配置错误（key/模型名不对），
  四类一律停止本轮不切换——quota 走快捷路径后新增的行为，配置坏了就别再换节点了。
  刷新 = 重新下载远端 profile → 校验 → 备份 → 写回 profile 文件 → 同步运行配置 → 校验 → 热重载内核。
  注意（9-18 日志实证，请勿“优化”掉）：该机场节点同名背后会换 IP/凭证，
  订阅差异即使只剩流量占位节点的改名（如 旧 50 → 新 50，仅“剩余流量”数值变化）
  也必须照常刷新与重载——多次 transport 都是刷新后原地复测即恢复，跳过即误判。
  同理 quota 失败记忆必须带过期时间（-QuotaFailMemoryMinutes），绝不能永久拉黑同名节点。
  10-14 更正（勿再按旧文案“优化”回去）：早期版本按“quota 刷新不变 IP，复测必败”跳过了刷新后的原地复测，
  与本段实证直接矛盾——刷新换的正是同名节点背后的出口 IP，原 IP 额度烧尽不代表新 IP 也用尽；
  已改为四类触发统一走刷新 + 原地复测。
  网关侧新增三个脚本专用接口（server_keys 鉴权，不动路由与配置结构）：
  GET /v1/watchdog/status 是只读 decision 快照；POST /v1/watchdog/probe 是强制真发上游的探针
  （忽略代理健康闸门与冷却、只测匿名通道、不写任何池状态）。切换 Selector 即换出口 IP，网关零重启生效。
  2026-10-26 新增 POST /v1/watchdog/warp/reset（同一鉴权）：WARP 出口失败触发切换成功后调用，
  退役全部旧 WARP 出口（旧请求失败、新注册走新 Clash 节点）；旧网关 404/405 时忽略。

  并发与密钥说明：脚本用全局命名互斥保证单实例（双开时后者直接退出），避免并发写三文件与重复切换；
  网关 server key 与推理 key 同权（可打全量推理），脚本只放网关本机、config.json 用文件 ACL 收紧，长期考虑只读 token 或回环绑定。
  每次切换成功后 DELETE /connections 是有意的：网关到 Clash 的 keep-alive 连接会钉住旧出口路由，
  不掐断则 big-pickle 验证可能走旧路由，把可用的新节点误判为坏节点；代价是每次切换掐断全部连接（含非网关流量）。

  筛选规则(对应截图要求，按最新确认)：
    只排除：中国大陆节点 + 流量/到期等信息占位节点 + 自动选择/DIRECT/故障转移这类非叶子；
    非叶子组现在按 /proxies 里的 type 判定（Selector/URLTest/Fallback/LoadBalance/Relay/
    Direct/Reject/Compatible/Pass），不再依赖组名恰好等于「自动选择」——带 emoji 的
    「♻️ 自动选择」以前会漏进候选池，被当成延迟未知的兜底节点试一遍。
    香港/台湾节点已按要求移出排除表——出口 IP 同样是境外 IP，匿名额度按出口 IP 计，
    不构成必须避开的理由；要临时排除见下方 watchdog.json；
    红框内两个占位节点（节点超时…/Gemini解锁请使用VLESS节点）用精确匹配排除，
    避免子串误杀 Gemini解锁类真实节点；
    日本直连 / 日本01 境外中转这类保留——它们仍是代理节点，出口 IP 仍是所标国家，
    “直连”指机场直连服务器，“境外中转”指经中转入口再出境，只是接入路径不同；
    无国家名的其余节点作为兜底候选排最后（默认选国家节点，不会无声丢弃）；
    地区降权（默认新加坡 +500ms）：延迟低但不稳定的地区只降排序、不排除，保底仍可选；
    排序：延迟从低到高（全部候选都没有测速数据时退化为名字序，日志会单独 WARN）；
    当前节点永不复选(换 IP 才有意义)。

  订阅刷新与本地配置（2026-10-23，selector 候选列表 2026-10-24）：
    Selector 是 Clash 里的「代理组名」，UID 是 Clash Verge profiles.yaml 里的 profile id。
    两者都可以写进 watchdog.json（selector / subscription_uid），不必改源码；
    selector 支持字符串或字符串数组：数组是候选列表，启动与每轮 404 时都按顺序取第一个
    在当前配置里存在、且 type=Selector 的组。默认候选是「🚀 节点选择 → 渔云CloudFisher」，
    本地节点池与渔云订阅来回切时不用改配置。只认 type=Selector：url-test/fallback 组的
    now 由内核测速决定，PUT 手动选择会被拒，不能当切换目标。
    -SubscriptionUid 留空时自动取 profiles.yaml 的 current:（当前激活的那份配置），
    所以每次重新导入「opencode2api-节点.yaml」（Verge 每次导入都会生成新 uid）都不用改参数；
    进程运行中重新导入时，下一次故障切换会重新解析 current: 并自动跟随新 uid。
    当前配置若是 type: local（没有 url: 行，例如本地生成后拖进 Verge 的 yaml），
    刷新动作直接跳过并记 INFO：本地文件不会自己变，逐节点切换照常走内核的当前节点列表。

  看门狗专用配置文件（-WatchdogConfig，默认脚本同目录 watchdog.json，只读从不写回）：
  这些项都不需要改脚本源码，直接在配置文件里增删，支持 // 与 /* */ 注释：
    {
      "selector": ["🚀 节点选择", "渔云CloudFisher"],
      "subscription_uid": "",
      "max_candidates": 120,
      "exclude_contains": ["剩余","到期","重置","流量","套餐","官网","中国"],
      "deprioritize": { "contains": ["新加坡"], "penalty_ms": 500 },
      "transport_confirm_rounds": 2,
      "warp_exit_failure": {
        "enabled": true, "threshold": 9, "cooldown_minutes": 15,
        "reset_rounds": 2, "action": "refresh_then_switch"
      }
    }
  语义：exclude_contains / deprioritize.contains 是整体替换内置默认值（不是追加），
  [] 即关闭该项；省略的键仍用内置默认值。deprioritize.penalty_ms 省略即用 500。
  selector 是 Clash 里的代理组名，或组名数组（候选列表，按顺序取第一个存在的 Selector 组；
  省略 = 用命令行默认值）；subscription_uid 留空/省略 =
  自动取 profiles.yaml 的 current:（重新导入配置后不需要改参数）。
  max_candidates 是单轮最多试多少个候选节点（1-300，省略即 120），等价于命令行 -MaxCandidates。
  transport_confirm_rounds 省略即用 2（1-10 的整数）：网关报 verdict=transport 时需连续
  N 轮仍坏才刷新切换，等价于命令行 -TransportConfirmRounds，-Once 单轮模式忽略它。
  warp_exit_failure 是 WARP 出口失败触发（省略即 enabled=false）：WARP 中继检查通过后，
  脚本再只读 GET 网关 /healthz（5s 超时；失败后 60s 退避，网关没起时不拖慢每轮巡检），
  读 warp.failure_streak（主信号）与 warp.status/active。failure_streak 达到 threshold
  （1-100，默认 9）时按 action 动作：refresh_only = 只刷新订阅；refresh_then_switch
  （默认）= 先刷新（节流/失败只记日志、继续）再按 Get-RankedCandidates 的 WARP 排序
  切到第一个可切换候选；switch_only = 不刷新直接切。failure_streak 缺失或为 0、但
  warp.status=unavailable 或 warp.active=0 连续达到 threshold 轮（本地计数，日志单列
  原因）同样触发，覆盖“出口池建不出任何出口”的故障；主信号有数时不再重复累计。
  动作成功后 POST /v1/watchdog/warp/reset（server_keys 鉴权；404/405 视为旧网关忽略）
  让旧出口退役，然后进入 cooldown_minutes（0-120，默认 15）冷却并把当前连击记为基线，
  连击需再涨 threshold 才会重复触发；连击连续归零 reset_rounds（1-10，默认 2）轮才
  清除基线。动作后的前 reset_rounds 轮不累计“出口池不可用”本地计数，给 WARP 重新
  注册留时间。网关重启（started_at 变化）、last_success_at 前进、或检测到任何节点切换
  （$script:LastSwitch 变化，含中继 failover）都会清零基线与冷却。last_success_at
  成功日志默认每 5 分钟最多一条；若本次成功实际清掉了连击基线、冷却或本地不可用计数
  则立即记录，便于确认恢复。成功时间本身仍每轮更新。此触发只挂在 WARP
  中继检查通过的分支，不影响中继失败路径，也不会调用 legacy Invoke-Failover（其探针
  在 WARP 下无意义）。阈值/连续轮数只靠常驻循环推进：-Once 单轮不做本地累计，只按
  网关当前 failure_streak 判断（已达标仍会动作，返回 Acted/ActionFailed 供退出码使用）。
  F2 注意（2026-10-26）：进程启动或网关重启后的首次观察，若出口池已满（active >= size、
  status=ready）且历史 failure_streak 已达阈值，先把它记为基线、不立即切换（重启后看到的
  高连击可能是上一个进程遗留的历史，尚未发生真正的 2xx）；代价是需要再新增 threshold 次
  失败才会动作，基线之后的增量失败仍会触发。若 active=0/unavailable 则照常动作。
  F1 注意：旧网关 /healthz 没有 failure_streak/failures_total 时只打一次兼容警告并禁用
  主信号，保留 active=0/unavailable 副信号；升级并重启网关后恢复主信号。
  优先级：显式命令行参数 > 配置文件 > 内置默认值——所以临时 -DeprioritizeContains
  试一把仍然压过文件，不必先改文件。exclude_contains 没有对应的命令行参数，
  它写错了只能改文件或把 -WatchdogConfig 指向别的路径。
  校验从严：未知键、类型不对（该给数组却给字符串/null、轮数给成字符串或小数）、
  空文件一律报错中止启动，不静默回退默认值——悄悄丢掉排除规则会把节点切到本该避开的
  地区，比直接报错危险。selector / subscription_uid / max_candidates /
  transport_confirm_rounds 即使被显式命令行参数覆盖也照样校验（只是不生效），
  免得哪天去掉命令行参数、看门狗才突然起不来；deprioritize.* 被覆盖时跳过校验
  （既有行为，别照上面那句推断）。
  只在启动时读一次，改完 watchdog.json 必须重启看门狗才生效。
  新建文件请从 watchdog.example.json 复制；未创建则全部走内置默认值，行为与从前一致。
  升级/回滚注意（2026-10-26）：warp_exit_failure 是新增顶层键。旧版 watchdog-clash.ps1
  读到含该键的新 watchdog.json 会按未知键直接启动失败——回滚必须把脚本与 watchdog.json
  一起回滚，不能只换其中一个。网关的新 /healthz 计数字段与 /v1/watchdog/warp/reset 也需
  网关与看门狗同批升级/重启：旧网关缺计数时主信号禁用（仅副信号），reset 404/405 会被
  忽略；新网关配旧看门狗则不会读取这些字段。

  本脚本会写的文件(试运行 -DryRun 除外，-DryRun 除运行日志外零写入)：
    - profiles/<uid>.yaml        订阅刷新时覆盖(先备份 .bak-yyyyMMdd-HHmmss)
    - profiles.yaml              订阅刷新时更新 extra/updated；每次切换成功后更新 selected.now
                                 （首次切换时该 profile 还没有 selected 块，会补写一块）
    - clash-verge.yaml           订阅刷新时同步 proxies/proxy-groups/rules 等节(先备份)
    - .tmp/clash-watchdog.log    运行日志(密钥/订阅 token 自动脱敏)
  只读不写的文件：
    - watchdog.json              看门狗专用配置(selector/uid/候选上限/排除/降权/transport 轮数/WARP 出口失败触发)，
                                 默认取脚本同目录，-WatchdogConfig 可改路径
    - config.json                只读 server_keys[0]

  密钥来源(绝不落盘、不打印)：Clash secret 从 clash-verge.yaml 读取；
  网关 server key 从本目录 config.json 的 server_keys[0] 读取；也可用参数显式传入。

.PARAMETER DryRun
  只读试运行：打印看门狗快照（verdict/cause）、候选排名 Top10、订阅差异预览，除运行日志外不写任何配置文件、不切换、不验证(零网关配额消耗，订阅下载仍耗机场流量)。
  当前配置若是本地文件（type: local，无 url:），订阅差异预览自动跳过，只打印候选与 selected 检查。

.PARAMETER Selector
  代理组候选（默认 @('🚀 节点选择','渔云CloudFisher')）。看门狗只在这些组里换节点，
  启动时按顺序取第一个在当前配置里存在、且 type=Selector 的组；每轮 404 也会按同一列表
  重挑一次，所以本地节点池 ↔ 渔云订阅来回切不用改参数。
  也可用 watchdog.json 的 selector 覆盖（字符串或数组都行；命令行显式传参优先）。

.PARAMETER SubscriptionUid
  Clash Verge profiles.yaml 里的 profile uid（决定刷新哪份配置、selected.now 写到哪）。
  留空（默认）= 自动取 profiles.yaml 的 current:，即当前激活的那份；
  每次重新导入本地 yaml 都会生成新 uid，留空就不必改参数；运行中重新导入时，
  下一次故障切换会重新解析 current: 并跟随新 uid（不必重启看门狗）。
  解析不出来时只跳过「刷新 + selected 记录」，逐节点切换照常。

.PARAMETER MaxCandidates
  单轮最多试多少个候选节点（默认 120，范围 1-300）。节点池比它小就整池试一遍。
  也可用 watchdog.json 的 max_candidates 覆盖（命令行显式传参优先）。
  注意最坏耗时随它线性增长（见 .DESCRIPTION）。

.PARAMETER WatchdogConfig
  看门狗专用配置文件路径，默认脚本同目录 watchdog.json。文件不存在即全部用内置默认值；
  存在则用其中的 selector（代理组候选，字符串或数组）、subscription_uid（profile uid，空=自动）、
  max_candidates（候选上限）、exclude_contains（子串排除）、deprioritize（地区降权）、
  transport_confirm_rounds（transport 连续确认轮数）与 warp_exit_failure（WARP 出口失败触发）
  覆盖对应内置默认值，省略的键不受影响。除 exclude_contains 外，其余键在显式传同名命令行参数时以命令行优先
  （selector / subscription_uid / max_candidates / transport_confirm_rounds 被覆盖也照样
  先校验，写错会中止启动；deprioritize.* 被覆盖时跳过校验，属既有行为）。
  只在启动时读一次，改完需重启看门狗。

.PARAMETER ProbeEndpoint
  验证探针走哪条通道。auto（默认）= 打网关的 POST /v1/watchdog/probe：它保证请求真的发到上游
  （忽略代理传输健康闸门与冷却）、不写任何池状态、并直接给出机器可读的 verdict；网关还没有这个
  端点时本进程自动回落旧的 /v1/chat/completions 探针并打一条 WARN。legacy = 强制旧探针。
  旧探针拿到的 502「all upstream attempts failed」无法区分「网关根本没发」和「发了被拒」，
  新探针则两种情况都能如实分类。
  已知副作用（探针通道独有）：网关自撰的冷却 429 永远不出现，所以「网关在退避 → 贴 Retry-After
  早退」这条路在探针通道下不可达；全候选失败只能退回 RoundCooldownMinutes(默认2分钟) 静默，
  而匿名冷却封顶才 15s。这是刻意的——探针的 429 一定是上游的，换出口 IP 才是唯一解法。

.PARAMETER ProxyReadySeconds
  切完节点、真正打探针之前，最多等多少秒让网关恢复代理传输健康闸门
  （/v1/watchdog/status 的 proxies.healthy 回到 proxies.total 就立刻继续）。只在闸门确实关着时
  花时间，默认 5，0 = 不等。注意这不是「探针能不能发出去」的前提——探针会穿透闸门；它只是少让
  「切换后首次拨号超时被判坏」的瞬时状态白烧一个候选和一次额度。

.PARAMETER TransportConfirmRounds
  transport 连续确认轮数（1-10，默认 2）：网关报 anonymous.verdict=transport（多为网络抖动）
  时，需连续 N 轮仍为 transport 才刷新/切换，单轮抖动只观察不动作。quota / all_down / 节点失效
  走立即路径，不受此值影响。也可以写进 watchdog.json 的 transport_confirm_rounds；
  两者同时存在时命令行显式传参优先。计数只存在进程内存（重启清零），且 -Once 单轮模式忽略此值。

.PARAMETER WarpExitFailureEnabled
  WARP 出口失败触发总开关（默认 false）。开启后，WARP 中继检查通过的轮次会只读
  GET 网关 /healthz，按 warp.failure_streak（主信号）或出口池持续 unavailable/active=0
  （本地连续轮数）触发订阅刷新/节点切换。等价于 watchdog.json 的
  warp_exit_failure.enabled；命令行显式传参优先。
  注意 [bool] 参数经 pwsh -File 必须带冒号传值：-WarpExitFailureEnabled:$true 或
  -WarpExitFailureEnabled:$false；单独写 -WarpExitFailureEnabled 会被当成缺参数。

.PARAMETER WarpExitFailureThreshold
  WARP 出口失败阈值（1-100，默认 9）：failure_streak 达到它才动作；动作后以当时的
  连击为新基线，需再涨同样多才会重复触发。出口池不可用的本地计数也用同一阈值。
  等价于 watchdog.json 的 warp_exit_failure.threshold。

.PARAMETER WarpExitFailureCooldownMinutes
  动作后的冷却分钟数（0-120，默认 15）：冷却期内即使连击再次达标也不动作。
  只作用于本触发器，不碰 $script:RoundCooldownUntil。等价于
  watchdog.json 的 warp_exit_failure.cooldown_minutes。

.PARAMETER WarpExitFailureResetRounds
  连击归零后清除动作基线的连续轮数（1-10，默认 2）；动作后的前 N 轮也不累计
  “出口池不可用”本地计数，给 WARP 重新注册留时间。等价于 watchdog.json 的
  warp_exit_failure.reset_rounds。

.PARAMETER WarpExitFailureAction
  触发后的动作：refresh_only = 只刷新订阅；refresh_then_switch（默认）= 先刷新
  （节流/失败只记日志）再切换；switch_only = 不刷新直接切换。成功切换后还会
  POST /v1/watchdog/warp/reset 让网关退役旧出口。等价于 watchdog.json 的
  warp_exit_failure.action。

.EXAMPLE
  pwsh -NoProfile -ExecutionPolicy Bypass -File watchdog-clash.ps1 -DryRun
  pwsh -NoProfile -ExecutionPolicy Bypass -File watchdog-clash.ps1
#>
#Requires -Version 7.0
# 运行要求：pwsh 7+（start.ps1 只找 pwsh，不再回退 powershell 5.1）。
# powershell 5.1 会把无 BOM 的中文按 ANSI 解码，导致节点过滤与 selector 匹配失效。
# 看门狗调参请直调本脚本 -PollSeconds/-MaxCandidates/-VerifyTimeoutSeconds 等；
# start.ps1 只消费 -Watchdog 开关，不透传任何看门狗/网关参数。
param(
  [string]$ClashBase = 'http://127.0.0.1:9097',
  [string]$ClashSecret = '',
  # 代理组候选：按顺序取第一个在当前 Clash 配置里存在的 Selector 组。
  # 换 profile（本地节点池 ↔ 渔云）时组名会变，写死单个名字会在换回另一端时 404 空转。
  [string[]]$Selector = @('🚀 节点选择', '渔云CloudFisher'),
  [string]$GatewayBase = 'http://127.0.0.1:19880',
  [string]$GatewayKey = '',
  [string]$VerifyModel = 'big-pickle',
  [ValidateRange(5, 300)][int]$PollSeconds = 15,
  # 候选上限：默认 120（2026-10-23 由 40 上调；当前本地配置 100 个节点 + 组名等，
  # 120 覆盖整池还留余量）。上限 300 只挡明显笔误；也可用 watchdog.json 的
  # max_candidates 覆盖，命令行显式传参优先。
  [ValidateRange(1, 300)][int]$MaxCandidates = 120,
  [ValidateRange(0, 300)][int]$SwitchGapSeconds = 15,
  [ValidateRange(5, 600)][int]$VerifyTimeoutSeconds = 120,
  # 探针通道。auto = 优先打网关的 /v1/watchdog/probe：它保证请求真的发到上游
  # （忽略代理健康闸门与冷却）、不写任何池状态、并直接回机器可读的 verdict；
  # 网关还没有这个端点（404）时本进程自动回落旧的 /v1/chat/completions 探针。
  # legacy = 强制旧探针（旧行为：可能被网关本地短路，且 502 无法区分原因）。
  [ValidateSet('auto', 'legacy')][string]$ProbeEndpoint = 'auto',
  # 切完节点、真正打探针之前，最多等多少秒让网关把代理传输健康闸门恢复
  # （/v1/watchdog/status 的 proxies.healthy 回到 proxies.total 就立刻继续，不等满）。
  # 只在闸门确实关着时才花时间，0 = 不等。探针本身已经能穿透闸门，所以这一项
  # 只是少误伤、少烧配额，不是探针能发出去的前提。
  [ValidateRange(0, 30)][int]$ProxyReadySeconds = 5,
  # profile uid：决定刷新哪份 profile、selected.now 写到哪个条目。
  # 留空 = 启动时自动取 profiles.yaml 的 current:（重新导入本地 yaml 会换 uid，
  # 留空就不必每次改参数）；显式传入则按传入值。
  [ValidatePattern('^[A-Za-z0-9_-]*$')][string]$SubscriptionUid = '',
  [string]$SubscriptionUA = 'clash-verge/v2.5.2',
  [int]$RefreshCooldownMinutes = 5,
  [int]$RoundCooldownMinutes = 2,
  # 近期 quota 失败记忆窗口（分钟）：验证时返回 quota 的节点在此窗口内候选降权，
  # 过期自动恢复——该机场同名背后会换 IP，永久拉黑会把已恢复的新 IP 挡在门外。
  # 0 = 关闭降权（不记录不降权）。
  [ValidateRange(0, 120)][int]$QuotaFailMemoryMinutes = 20,
  # 订阅刷新成功后、原地复测当前节点前的等待秒数（等内核把新 profile 热重载生效）；
  # 0 = 不等直接复测。与 ProxyReadySeconds 无关，别混用。
  [ValidateRange(0, 60)][int]$RefreshSettleSeconds = 3,
  # transport 连续确认轮数：verdict=transport（多为网络抖动，如 transport (connection failed)）需连续 N 轮仍坏才刷新切换；
  # quota/all_down/节点失效不受此限制，仍立即动作。
  # 未显式传本参数时由 watchdog.json 的 transport_confirm_rounds 接管（见 Read-WatchdogConfig）。
  [ValidateRange(1, 10)][int]$TransportConfirmRounds = 2,
  # 看门狗专用配置文件路径（默认脚本同目录 watchdog.json）。只读、从不写回。
  # 存 exclude_contains（子串排除）与 deprioritize（地区降权）两项，见脚本头 .DESCRIPTION。
  # 文件不存在 = 全部用内置默认值；解析失败直接报错（静默回退等于悄悄丢掉排除规则）。
  [string]$WatchdogConfig = '',
  # 地区降权：名字命中任一子串的候选排序时加罚分（毫秒），用于“延迟低但不稳定”的地区（如新加坡）；
  # 只降排序、不排除，保底仍可选；与 quota 降权可叠加；0 / 空列表 / 全空白条目 = 关闭。
  # 未显式传本参数时由 watchdog.json 的 deprioritize.contains 接管（见 Read-WatchdogConfig）。
  [string[]]$DeprioritizeContains = @('新加坡'),
  # 未显式传本参数时由 watchdog.json 的 deprioritize.penalty_ms 接管。
  [ValidateRange(0, 60000)][int]$DeprioritizePenaltyMs = 500,
  [ValidateRange(1, 1440)][int]$NormalLogMinutes = 20,
  # 为空时只靠 verge-mihomo 进程路径探测（找不到则跳过本地校验），不假设安装盘符。
  [string]$MihomoExe = '',
  [string]$LogPath = '',
  # WARP proxy 检查（Phase 1）：auto = warp.enabled 且有 warp.proxy 时进入 proxy-check 模式；
  # off = 完全 legacy（不读 config.json 的 warp 字段、不建 SOCKS5 连接）；force = 即使
  # warp.enabled=false，只要 warp.proxy 有值也检查。命令行显式传参优先于 watchdog.json。
  [ValidateSet('auto', 'off', 'force')][string]$ProxyCheckMode = 'auto',
  # WARP 模式的检查间隔（秒）：健康路径按它轮询；失败路径还要多等 proxy_check_recheck_seconds。
  [ValidateRange(5, 300)][int]$ProxyCheckIntervalSeconds = 15,
  # 第一次检查失败后的复检等待（秒）；0 = 跳过复检，直接确认失败（仍要等切换条件）。
  [ValidateRange(0, 30)][int]$ProxyCheckRecheckSeconds = 5,
  # 单次完整检查（TCP + UDP）的总预算（秒）；不是每步预算。
  [ValidateRange(3, 30)][int]$ProxyCheckTimeoutSeconds = 8,
  # UDP DNS 检查目标（host:port / [v6]:port）；失败后自动回退 8.8.8.8:53。
  [string]$ProxyCheckUdpTarget = '1.1.1.1:53',
  # 是否要求 UDP 检查也通过；false = 只测 TCP（WARP 可能仍不可用，属降级观察）。
  [bool]$ProxyCheckRequireUdp = $true,
  # 是否允许切 Clash 节点：auto 只在本机 Clash 的 mixed-port/socks-port 有正向证据时切；
  # on 强制；off 只监控。详见 watchdog-warp-proxy-design.md §7.1。
  [ValidateSet('auto', 'on', 'off')][string]$ProxyCheckFailover = 'auto',
  # 切到候选后、验证前等待的秒数（等 Clash 收敛）。
  [ValidateRange(0, 30)][int]$ProxySwitchGapSeconds = 2,
  # 单轮 WARP failover 的总时间预算（秒）；超时停止候选循环并回滚，避免 120 候选择磨死。
  [ValidateRange(30, 600)][int]$ProxyFailoverBudgetSeconds = 120,
  # 诊断专用：覆盖 config.json 的 warp.proxy；只在 -ProxyCheckProbeOnly / -DryRun 下生效，
  # 正常常驻循环忽略并 WARN（防止误把诊断目标当成生产切换依据）。
  [string]$ProxyCheckEndpoint = '',
  # WARP 出口失败触发（2026-10-26）：Clash SOCKS5 中继检查通过、但网关 /healthz 的
  # warp.failure_streak 连续达到阈值（或出口池持续 unavailable/active=0）时，按 action
  # 刷新订阅/切换节点，并 POST /v1/watchdog/warp/reset 让旧出口退役。默认关闭；
  # watchdog.json 的 warp_exit_failure 块可覆盖，命令行显式传参优先。
  [bool]$WarpExitFailureEnabled = $false,
  [ValidateRange(1, 100)][int]$WarpExitFailureThreshold = 9,
  [ValidateRange(0, 120)][int]$WarpExitFailureCooldownMinutes = 15,
  [ValidateRange(1, 10)][int]$WarpExitFailureResetRounds = 2,
  [ValidateSet('refresh_only', 'refresh_then_switch', 'switch_only')][string]$WarpExitFailureAction = 'refresh_then_switch',
  # 独立诊断入口：只做一次 proxy 检查、打印结果并按 0/1/2 退出；早于实例互斥与所有 Resolve-*，
  # 因此网关看门狗常驻运行时也能执行。不切换、不写配置/状态文件（运行日志除外）。
  [switch]$ProxyCheckProbeOnly,
  [switch]$DryRun,
  [switch]$Once
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($LogPath)) {
  $LogPath = Join-Path $PSScriptRoot '.tmp\clash-watchdog.log'
}
if ([string]::IsNullOrWhiteSpace($WatchdogConfig)) {
  $WatchdogConfig = Join-Path $PSScriptRoot 'watchdog.json'
}
# 命令行显式传过的参数名（脚本作用域快照）：配置文件只接管没显式传的项，
# 保证临时 -DeprioritizeContains 试一把仍然优先于文件。
$script:ExplicitParams = @($PSBoundParameters.Keys)
$script:WatchdogConfigPath = $WatchdogConfig
$script:ClashBase = $ClashBase.TrimEnd('/')
$script:ClashSecret = $ClashSecret
# selector 是「候选列表」：启动时按顺序取第一个在 /proxies 里存在的 Selector 组，
# 404 时也会按同一列表重新挑（见 Resolve-SelectorCandidate）。
# 兼容两种传法：数组（watchdog.json 的 JSON 数组 / 直接调用脚本时传数组）与
# `pwsh -File ... -Selector a,b` —— 后者会被 -File 当成单个字符串，所以按逗号再拆一次。
# Clash 组名里出现逗号属于病态命名（YAML flow 写法本身就会坏），拆开是安全的。
$script:SelectorCandidates = @($Selector |
  ForEach-Object { ([string]$_) -split ',' } |
  ForEach-Object { $_.Trim() } |
  Where-Object { $_ -ne '' })
if (@($script:SelectorCandidates).Count -eq 0) { throw '-Selector 不能为空：至少给一个代理组名' }
$script:Selector = $script:SelectorCandidates[0]
$script:GatewayBase = $GatewayBase.TrimEnd('/')
$script:GatewayKey = $GatewayKey
$script:VerifyModel = $VerifyModel
$script:ProbeEndpoint = $ProbeEndpoint
# 候选上限与 profile uid：param 绑定值起步，watchdog.json 可覆盖（命令行显式传参优先）。
# uid 为空时在启动块里自动解析为 profiles.yaml 的 current:。
$script:MaxCandidates = $MaxCandidates
$script:SubscriptionUid = $SubscriptionUid
# uid 是否来自「自动跟随 current:」。启动块里会按生效 uid（命令行/watchdog.json）重算：
# 显式钉死的 uid 失效时必须报错，不能悄悄改成去刷新/覆盖当前激活 profile。
$script:UidAuto = $true
# 运行时可降级：探针端点 404 一次就把本进程永久切回旧探针，不必每轮再试。
$script:ProbeSupported = ($ProbeEndpoint -ne 'legacy')
$script:ProxyReadySeconds = $ProxyReadySeconds
$script:QuotaFailMemoryMinutes = $QuotaFailMemoryMinutes
$script:DeprioritizeContains = $DeprioritizeContains
$script:DeprioritizePenaltyMs = $DeprioritizePenaltyMs
# transport 连续确认轮数：param 绑定值起步，配置文件可覆盖（命令行显式传参时以命令行为准）。
# param 变量本身就在脚本作用域，所以 $script:TransportConfirmRounds 与 $TransportConfirmRounds
# 是同一个存储；统一走 $script: 读写，来源单一、不会出现两处读出不同值。
$script:TransportConfirmRounds = $TransportConfirmRounds
$script:LogPath = $LogPath
$script:VergeRoot = Join-Path $env:APPDATA 'io.github.clash-verge-rev.clash-verge-rev'
# WARP proxy 检查（Phase 1）。param 绑定值起步，watchdog.json 可覆盖（命令行显式传参优先）。
$script:ProxyCheckMode = $ProxyCheckMode
$script:ProxyCheckIntervalSeconds = $ProxyCheckIntervalSeconds
$script:ProxyCheckRecheckSeconds = $ProxyCheckRecheckSeconds
$script:ProxyCheckTimeoutSeconds = $ProxyCheckTimeoutSeconds
$script:ProxyCheckUdpTarget = $ProxyCheckUdpTarget
$script:ProxyCheckRequireUdp = $ProxyCheckRequireUdp
$script:ProxyCheckFailover = $ProxyCheckFailover
$script:ProxySwitchGapSeconds = $ProxySwitchGapSeconds
$script:ProxyFailoverBudgetSeconds = $ProxyFailoverBudgetSeconds
$script:ProxyCheckEndpoint = $ProxyCheckEndpoint
# WARP 出口失败触发：param 绑定值起步，watchdog.json 的 warp_exit_failure 可覆盖
# （命令行显式传参优先）。运行状态在启动块统一初始化（进程内存，重启清零）。
$script:WarpExitFailureEnabled = $WarpExitFailureEnabled
$script:WarpExitFailureThreshold = $WarpExitFailureThreshold
$script:WarpExitFailureCooldownMinutes = $WarpExitFailureCooldownMinutes
$script:WarpExitFailureResetRounds = $WarpExitFailureResetRounds
$script:WarpExitFailureAction = $WarpExitFailureAction
# 运行状态：主循环按 WarpProxyActive 选择 sleep 间隔；LastState/LastLogAt 做心跳与状态切换
# 去重；ConfigErrSig 让同一配置错误只打一次；DirectWarned 让直连 WARP 告警只打一次。
$script:WarpProxyActive = $false
$script:WarpProxyLastMode = ''
$script:WarpProxyLastState = ''
$script:WarpProxyLastLogAt = [DateTime]::MinValue
$script:WarpProxyConfigErrSig = ''
$script:WarpDirectWarned = $false
$script:ProxyCheckEndpointWarned = $false
$script:WarpFailoverOnWarned = $false
# config.json 的 warp 设置缓存（mtime + length）与最近一份成功解析的设置：
# 解析失败时保留旧值并只告警一次；从未成功过才退回 legacy。
$script:GatewayWarpCache = $null
$script:GatewayWarpLastGood = $null

$script:ExcludeExact = @('自动选择', '故障转移', 'DIRECT', 'COMPATIBLE', 'GLOBAL', 'PASS', 'REJECT', 'REJECT-DROP', 'PASS-RULE',
  # 以下两个是红框内的信息占位节点（与真实节点同服同口，靠名字区分），必须精确排除：
  # 用精确匹配而不用子串，是为了不误杀 VLESS-美国01 / Gemini解锁类真实命名。
  '节点超时请手动更新订阅', 'Gemini解锁请使用VLESS节点')
# 非叶子组的 mihomo type（/proxies 里每个条目的 type 字段）。组名不可靠——本地这份
# 「🚀 节点选择」的候选里就带着「♻️ 自动选择」，精确匹配 '自动选择' 命不中它，
# 它会以兜底候选身份被切换循环试一遍。按 type 排除才是稳的。
$script:GroupTypes = @('Selector', 'URLTest', 'Fallback', 'LoadBalance', 'Relay', 'Smart',
  'Direct', 'Reject', 'RejectDrop', 'Compatible', 'Pass')
# 只排除流量/到期/官网类信息占位节点 + 中国大陆节点。
# 注意：香港/台湾节点已按要求移出排除表（它们的出口 IP 同样是境外 IP，匿名额度按出口
# 计，不构成必须避开的理由）；要临时排除请用 watchdog.json 的 exclude_contains 追加。
# 日本直连 / 日本01 境外中转这类保留：它们仍是代理节点（hysteria2 连机场服务器），
# 出口 IP 仍是所标国家，“直连”=“直连该国服务器”，“境外中转”=“经中转入口再出境”，
# 与匿名额度/出口 IP 切换的目标完全一致。
# 注意：VLESS/Gemini/解锁/超时/手动是节点命名常用词（如 VLESS-美国01、Gemini解锁节点），
# 不是占位标记，故不进排除表；占位节点用 剩余/到期/流量/套餐 等词覆盖。
$script:ExcludeContains = @('剩余', '到期', '重置', '流量', '套餐', '官网', '中国')
# CountryHints 只决定“算不算有国家名”（Test-NodeEligible 的提前放行 + Get-RankedCandidates
# 里避开 900000+ 兜底桶），不参与排除。香港/台湾显式列进来：它们已移出排除表，若还只靠
# 名字前的 🇭🇰/🇨🇳 旗子 emoji 识别，机场把节点改名成不带旗子的“台湾04”时会静默掉进兜底桶，
# 排到所有存活国家节点之后（≈ 加了但不选）且无日志。
$script:CountryHints = @('日本', '新加坡', '美国', '德国', '英国', '法国', '西班牙', '加拿大', '瑞士',
  '澳大利亚', '墨西哥', '以色列', '南非', '阿联酋', '迪拜', '印度', '孟买', '沙特',
  '巴西', '智利', '哥伦比亚', '菲律宾', '泰国', '韩国', '马来', '印尼', '越南',
  '俄罗斯', '荷兰', '意大利', '瑞典', '挪威', '芬兰', '土耳其', '阿根廷', '新西兰', '爱尔兰',
  '香港', '台湾',
  # 2026-10-23 补齐：本地 opencode2api 节点池实际用到的国家词。缺一个就会让整批节点
  # 静默掉进 900000+ 兜底桶（排在所有国家节点之后，约等于「加了但不优先选」）——
  # 黑山 7 个、波兰 6 个、拉脱维亚 3 个、亚美尼亚 1 个曾整批被压后。
  '黑山', '波兰', '拉脱维亚', '亚美尼亚',
  # 常见但此前未列出的，避免换一份订阅又要来补：欧洲/中亚/南亚/东南亚。
  '立陶宛', '爱沙尼亚', '捷克', '奥地利', '罗马尼亚', '保加利亚', '乌克兰', '塞尔维亚',
  '克罗地亚', '斯洛文尼亚', '匈牙利', '希腊', '葡萄牙', '比利时', '丹麦', '卢森堡', '冰岛',
  '摩尔多瓦', '白俄罗斯', '哈萨克斯坦', '巴基斯坦', '孟加拉', '斯里兰卡', '尼泊尔',
  '柬埔寨', '缅甸', '老挝', '蒙古', '埃及', '摩洛哥', '尼日利亚', '肯尼亚')
$script:SyncKeys = @('proxies', 'proxy-providers', 'proxy-groups', 'rules', 'rule-providers')

function Write-WdLog {
  param([string]$Message, [string]$Level = 'INFO')
  $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  $line = "[$ts][$Level] $Message"
  $color = 'Gray'
  if ($Level -eq 'WARN') { $color = 'Yellow' }
  elseif ($Level -eq 'ERROR') { $color = 'Red' }
  elseif ($Level -eq 'OK') { $color = 'Green' }
  Write-Host $line -ForegroundColor $color
  try {
    $logDir = Split-Path -Parent $script:LogPath
    if ($logDir -ne '' -and -not (Test-Path -LiteralPath $logDir)) {
      New-Item -ItemType Directory -Path $logDir -Force | Out-Null
    }
    Add-Content -LiteralPath $script:LogPath -Value $line -Encoding utf8
    $fi = Get-Item -LiteralPath $script:LogPath -ErrorAction SilentlyContinue
    if ($fi -ne $null -and $fi.Length -gt 5MB) {
      $rot = "$($script:LogPath).1"
      if (Test-Path -LiteralPath $rot) { Remove-Item -LiteralPath $rot -Force }
      Rename-Item -LiteralPath $script:LogPath -NewName ([IO.Path]::GetFileName($rot)) -Force
    }
  } catch { }
}

function Mask-Url {
  param([string]$Url)
  if ($Url -eq $null -or $Url -eq '') { return '' }
  try {
    $u = [uri]$Url
    # 只留 host，路径/query 里的 token 一律不打印（机场多为路径式 token，如 /s/<token>）。
    return ($u.Host + ' (token已脱敏)')
  } catch {
    return '(订阅地址已脱敏)'
  }
}

function Shorten {
  param([string]$Text, [int]$Max = 300)
  if ($Text -eq $null) { return '' }
  $t = $Text -replace '\s+', ' '
  if ($t.Length -gt $Max) { return $t.Substring(0, $Max) + '…' }
  return $t
}

function Mask-Text {
  # 异常文本可能原样包含请求 URI（含订阅 token / SOCKS5 userinfo）：记日志前先抹掉 URL。
  # 2026-10-26 起 URL 正则扩展到 socks5/socks5h（warp.proxy 异常文本可能带 userinfo）。
  param([string]$Text)
  if ([string]::IsNullOrEmpty($Text)) { return '' }
  return ([regex]::Replace($Text, '(?i)(?:https?|socks5h?)://\S+', '(url已脱敏)'))
}

function Mask-Proxy {
  # 代理相关异常文本专用：只保留非 URL 文本，URL（可能含 userinfo/路径 token）整体替换。
  param([string]$Text)
  if ([string]::IsNullOrEmpty($Text)) { return '' }
  return ([regex]::Replace($Text, '(?i)\bsocks5h?://\S+', '(proxy已脱敏)'))
}

# ==== WARP proxy 检查（Phase 1）辅助函数 =====================================
# 设计依据：watchdog-warp-proxy-design.md §5/§8。检查只证明 SOCKS5 中继的 TCP 可达与
# UDP DNS 往返，不证明 WARP 注册/外层 UDP 2408/egress 可用；后者仍看 /healthz 与池日志。

function Get-RemainingMs {
  param([datetime]$Deadline)
  return [int][Math]::Floor(($Deadline - (Get-Date)).TotalMilliseconds)
}

function Test-HostPortTarget {
  # 解析无 scheme 的 host:port（UDP DNS 目标 / SOCKS5 目标用）。支持域名、IPv4、[IPv6]。
  # 严格拒绝 scheme、userinfo、path/query/fragment、空白与非法端口。
  param([string]$Text)
  $res = @{ Ok = $false; Host = ''; Port = 0; Kind = ''; Error = '' }
  if ([string]::IsNullOrWhiteSpace($Text)) { $res.Error = '目标为空'; return $res }
  $t = $Text.Trim()
  if ($t -match '\s') { $res.Error = '目标含空白'; return $res }
  if ($t -match '^[A-Za-z][A-Za-z0-9+.-]*://') { $res.Error = '目标不能带 scheme'; return $res }
  if ($t.Contains('@') -or $t.Contains('/') -or $t.Contains('?') -or $t.Contains('#')) {
    $res.Error = '目标不能带 userinfo/path/query/fragment'; return $res
  }
  $hostPart = ''
  $portPart = ''
  if ($t.StartsWith('[')) {
    if ($t -notmatch '^\[([^\]]+)\]:(\d+)$') { $res.Error = 'IPv6 目标格式应为 [v6]:port'; return $res }
    $hostPart = $Matches[1]
    $portPart = $Matches[2]
    $ip = $null
    if (-not [System.Net.IPAddress]::TryParse($hostPart, [ref]$ip) -or
      $ip.AddressFamily -ne [System.Net.Sockets.AddressFamily]::InterNetworkV6) {
      $res.Error = '方括号内不是合法 IPv6 地址'; return $res
    }
    $res.Kind = 'ipv6'
  } else {
    if ($t -notmatch '^([^:]+):(\d+)$') { $res.Error = '目标格式应为 host:port'; return $res }
    $hostPart = $Matches[1]
    $portPart = $Matches[2]
    if ($hostPart.Contains(':')) { $res.Error = 'IPv6 目标必须写成 [v6]:port'; return $res }
    $ip = $null
    if ([System.Net.IPAddress]::TryParse($hostPart, [ref]$ip)) {
      if ($ip.AddressFamily -ne [System.Net.Sockets.AddressFamily]::InterNetwork) {
        $res.Error = '地址族不受支持'; return $res
      }
      $res.Kind = 'ipv4'
    } else {
      if ($hostPart -notmatch '^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$') { $res.Error = '域名非法'; return $res }
      $res.Kind = 'domain'
    }
  }
  $n = 0
  if (-not [int]::TryParse($portPart, [ref]$n) -or $n -lt 1 -or $n -gt 65535) {
    $res.Error = '端口必须是 1-65535'; return $res
  }
  $res.Ok = $true
  $res.Host = $hostPart
  $res.Port = $n
  return $res
}

function Test-ProxyEndpointFormat {
  # warp.proxy 格式校验：只收 socks5/socks5h://host:port（域名/IPv4/[IPv6]）。
  # 拒绝 http(s)、userinfo、缺端口、path/query/fragment、非法端口。返回值里的 Host 不带方括号。
  param([string]$Url)
  $res = @{ Ok = $false; Scheme = ''; Host = ''; Port = 0; Kind = ''; IsLoopback = $false; Error = '' }
  if ([string]::IsNullOrWhiteSpace($Url)) { $res.Error = '代理地址为空'; return $res }
  $raw = $Url.Trim()
  if ($raw -match '\s') { $res.Error = '代理地址含空白'; return $res }
  if ($raw -notmatch '^(?i:socks5h?)://') {
    if ($raw -match '^(?i)https?://') { $res.Error = '只支持 socks5 / socks5h scheme（http(s) 无法承载 UDP）' }
    else { $res.Error = 'scheme 必须是 socks5 或 socks5h' }
    return $res
  }
  # 设计 §5.1：只允许裸 scheme://host:port。Uri 会把无 path 的 URL 规范化成 "/"，
  # 无法区分 "host:port" 与 "host:port/"，所以对 scheme 之后的原始文本再查一次。
  $afterScheme = $raw.Substring($raw.IndexOf('://') + 3)
  if ($afterScheme.Contains('/') -or $afterScheme.Contains('?') -or $afterScheme.Contains('#')) {
    $res.Error = '不允许 path/query/fragment'; return $res
  }
  try { $u = [uri]$raw } catch { $res.Error = 'URL 解析失败'; return $res }
  if (@('socks5', 'socks5h') -notcontains ([string]$u.Scheme).ToLowerInvariant()) {
    $res.Error = 'scheme 必须是 socks5 或 socks5h'; return $res
  }
  if (-not [string]::IsNullOrEmpty($u.UserInfo)) { $res.Error = '不支持 userinfo（socks5Bind 只支持 no-auth）'; return $res }
  if ($u.Query -ne '' -or $u.Fragment -ne '') { $res.Error = '不允许 path/query/fragment'; return $res }
  if ($u.AbsolutePath -ne '' -and $u.AbsolutePath -ne '/') { $res.Error = '不允许 path/query/fragment'; return $res }
  if ($u.IsDefaultPort -or $u.Port -lt 1 -or $u.Port -gt 65535) { $res.Error = '端口缺失或非法（应为 1-65535）'; return $res }
  $h = [string]$u.DnsSafeHost
  if ([string]::IsNullOrWhiteSpace($h)) { $res.Error = '主机为空'; return $res }
  $ip = $null
  if ([System.Net.IPAddress]::TryParse($h, [ref]$ip)) {
    if ($ip.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork) {
      $res.Kind = 'ipv4'
      $b = $ip.GetAddressBytes()
      $res.IsLoopback = ($b[0] -eq 127)
    } elseif ($ip.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetworkV6) {
      $res.Kind = 'ipv6'
      $res.IsLoopback = $ip.Equals([System.Net.IPAddress]::IPv6Loopback)
    } else {
      $res.Error = '地址族不受支持'; return $res
    }
  } else {
    $res.Kind = 'domain'
    $res.IsLoopback = ($h -ieq 'localhost')
  }
  $res.Ok = $true
  $res.Scheme = ([string]$u.Scheme).ToLowerInvariant()
  $res.Host = $h
  $res.Port = [int]$u.Port
  return $res
}

function Format-ProxyEndpoint {
  # 日志统一输出 scheme://host:port（IPv6 保留方括号）；绝不输出 userinfo/path/query。
  param([string]$Url)
  $f = Test-ProxyEndpointFormat -Url $Url
  if (-not $f.Ok) { return '(无效代理地址)' }
  $hostTxt = if ($f.Kind -eq 'ipv6') { '[' + $f.Host + ']' } else { $f.Host }
  return ($f.Scheme + '://' + $hostTxt + ':' + $f.Port)
}

function Resolve-Socks5Host {
  # 有界域名解析：IP 字面量直接返回；域名用 GetHostAddressesAsync().Wait(remaining)。
  # 超时只放弃本次检查，后台任务可能继续但不会阻塞调用方（设计 §5.5）。
  param([string]$HostName, [int]$TimeoutMs)
  $res = @{ Ok = $false; IP = $null; Error = '' }
  if ([string]::IsNullOrWhiteSpace($HostName)) { $res.Error = '主机为空'; return $res }
  $ip = $null
  if ([System.Net.IPAddress]::TryParse($HostName, [ref]$ip)) { $res.Ok = $true; $res.IP = $ip; return $res }
  if ($TimeoutMs -lt 1) { $res.Error = '域名解析预算耗尽'; return $res }
  try {
    $task = [System.Net.Dns]::GetHostAddressesAsync($HostName)
    if (-not $task.Wait($TimeoutMs)) { $res.Error = '域名解析超时'; return $res }
    $ips = @($task.Result)
    if (@($ips).Count -eq 0) { $res.Error = '域名无解析结果'; return $res }
    $v4 = @($ips | Where-Object { $_.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork })
    $pick = if (@($v4).Count -gt 0) { $v4[0] } else { $ips[0] }
    $res.Ok = $true
    $res.IP = $pick
  } catch {
    $res.Error = '域名解析失败'
  }
  return $res
}

function Read-Socks5Exact {
  # 按绝对 deadline 读满 N 字节；每次 Read 前重算剩余，<=0 立即失败。
  # 不能只在进入函数时设一次 ReadTimeout：对方逐字节慢发时每次 Read 都会重置超时。
  param($Stream, [int]$Count, [int]$TimeoutMs)
  if ($TimeoutMs -lt 1 -or $Count -lt 1) { return $null }
  $deadline = (Get-Date).AddMilliseconds([Math]::Max(1, $TimeoutMs))
  $buf = New-Object byte[] $Count
  $off = 0
  while ($off -lt $Count) {
    $rem = Get-RemainingMs $deadline
    if ($rem -lt 1) { return $null }
    try { $Stream.ReadTimeout = $rem } catch { return $null }
    try { $n = $Stream.Read($buf, $off, $Count - $off) } catch { return $null }
    if ($n -le 0) { return $null }
    $off += $n
  }
  return , $buf
}

function Write-Socks5Bytes {
  param($Stream, [byte[]]$Bytes, [int]$TimeoutMs)
  if ($TimeoutMs -lt 1) { return $false }
  try {
    $Stream.WriteTimeout = $TimeoutMs
    $Stream.Write($Bytes, 0, $Bytes.Length)
    $Stream.Flush()
    return $true
  } catch { return $false }
}

function Connect-Socks5Control {
  # 建立 SOCKS5 TCP 控制连接并完成 no-auth greeting。05 FF = config（换节点无解）。
  # 成功时把 Client/Stream 交给调用方释放；失败在函数内释放。
  param($Endpoint, [int]$TimeoutMs)
  $start = Get-Date
  $deadline = $start.AddMilliseconds([Math]::Max(1, $TimeoutMs))
  $res = @{ Ok = $false; Client = $null; Stream = $null; Class = 'unavailable'; Kind = 'unreachable';
    Stage = 'tcp_connect'; Reason = ''; RemoteIP = $null; DurationMs = 0 }
  if ($Endpoint -eq $null -or -not $Endpoint.Ok) {
    $res.Class = 'config'; $res.Kind = 'format'; $res.Stage = 'format'
    $res.Reason = if ($Endpoint -ne $null -and $Endpoint.Error -ne '') { [string]$Endpoint.Error } else { '代理地址非法' }
    return $res
  }
  $client = $null
  $stream = $null
  try {
    $resolved = Resolve-Socks5Host -HostName $Endpoint.Host -TimeoutMs (Get-RemainingMs $deadline)
    if (-not $resolved.Ok) {
      $res.Kind = 'timeout'; $res.Reason = [string]$resolved.Error; return $res
    }
    $rem = Get-RemainingMs $deadline
    if ($rem -lt 1) { $res.Kind = 'timeout'; $res.Reason = '连接预算耗尽'; return $res }
    $client = [System.Net.Sockets.TcpClient]::new($resolved.IP.AddressFamily)
    $iar = $client.BeginConnect($resolved.IP, [int]$Endpoint.Port, $null, $null)
    if (-not $iar.AsyncWaitHandle.WaitOne($rem)) {
      $res.Kind = 'timeout'; $res.Reason = '连接 SOCKS5 入口超时'; return $res
    }
    try { $client.EndConnect($iar) } catch { $res.Kind = 'unreachable'; $res.Reason = '连接 SOCKS5 入口失败'; return $res }
    $client.NoDelay = $true
    $stream = $client.GetStream()
    try { $res.RemoteIP = $client.Client.RemoteEndPoint.Address } catch { $res.RemoteIP = $null }
    $rem = Get-RemainingMs $deadline
    if (-not (Write-Socks5Bytes -Stream $stream -Bytes ([byte[]]@(5, 1, 0)) -TimeoutMs $rem)) {
      $res.Stage = 'socks5_greeting'; $res.Kind = 'timeout'; $res.Reason = '发送 SOCKS5 greeting 超时'; return $res
    }
    $gr = Read-Socks5Exact -Stream $stream -Count 2 -TimeoutMs (Get-RemainingMs $deadline)
    if ($null -eq $gr) {
      $res.Stage = 'socks5_greeting'; $res.Kind = 'timeout'; $res.Reason = '等待 SOCKS5 greeting 回复超时'; return $res
    }
    if ($gr[0] -ne 5) {
      $res.Stage = 'socks5_greeting'; $res.Kind = 'protocol'; $res.Reason = 'SOCKS5 版本异常'; return $res
    }
    if ($gr[1] -eq 0xFF) {
      $res.Class = 'config'; $res.Kind = 'format'; $res.Stage = 'socks5_greeting'
      $res.Reason = 'SOCKS5 入口要求认证（05 FF），换 Clash 节点无效'; return $res
    }
    if ($gr[1] -ne 0) {
      $res.Stage = 'socks5_greeting'; $res.Kind = 'protocol'
      $res.Reason = ('SOCKS5 不接受 no-auth（方法 0x{0:X2}）' -f $gr[1]); return $res
    }
    $res.Ok = $true
    $res.Class = 'ok'
    $res.Kind = 'ok'
    $res.Client = $client
    $res.Stream = $stream
    return $res
  } catch {
    $res.Kind = 'unreachable'
    $res.Reason = 'TCP 检查异常：' + (Mask-Proxy (Mask-Text $_.Exception.Message))
    return $res
  } finally {
    if (-not $res.Ok) {
      if ($stream -ne $null) { try { $stream.Dispose() } catch { } }
      if ($client -ne $null) { try { $client.Dispose() } catch { } }
      $res.Client = $null
      $res.Stream = $null
    }
    $res.DurationMs = [int]((Get-Date) - $start).TotalMilliseconds
  }
}

function New-Socks5ConnectRequest {
  # 构造 SOCKS5 CONNECT 请求；按目标类型用 ATYP 0x01/0x03/0x04。
  param($Target)
  $ms = [System.IO.MemoryStream]::new()
  $ms.WriteByte(5); $ms.WriteByte(1); $ms.WriteByte(0)
  switch ($Target.Kind) {
    'ipv4' {
      $ms.WriteByte(1)
      $b = ([System.Net.IPAddress]::Parse($Target.Host)).GetAddressBytes()
      $ms.Write($b, 0, $b.Length)
    }
    'ipv6' {
      $ms.WriteByte(4)
      $b = ([System.Net.IPAddress]::Parse($Target.Host)).GetAddressBytes()
      $ms.Write($b, 0, $b.Length)
    }
    'domain' {
      $ms.WriteByte(3)
      $nb = [System.Text.Encoding]::ASCII.GetBytes([string]$Target.Host)
      $ms.WriteByte([byte]$nb.Length)
      $ms.Write($nb, 0, $nb.Length)
    }
    default { throw '不支持的目标类型' }
  }
  $p = [BitConverter]::GetBytes([uint16]$Target.Port)
  if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($p) }
  $ms.Write($p, 0, 2)
  $out = $ms.ToArray()
  $ms.Dispose()
  return , $out
}

function Read-Socks5BoundAddress {
  # 读 UDP ASSOCIATE 回复中的 BND.ADDR/BND.PORT（ATYP 0x01/0x03/0x04 显式解析）。
  # 使用绝对 Deadline：ATYP=3 先读长度再读域名，多段读取不得各自重置超时预算。
  param($Stream, [int]$Atyp, [datetime]$Deadline)
  $res = @{ Ok = $false; Address = ''; Port = 0; Kind = ''; Error = ''; ErrorKind = '' }
  switch ($Atyp) {
    1 {
      $rem = Get-RemainingMs $Deadline
      if ($rem -lt 1) { $res.Error = '读取 BND.ADDR 超时'; $res.ErrorKind = 'timeout'; return $res }
      $b = Read-Socks5Exact -Stream $Stream -Count 6 -TimeoutMs $rem
      if ($null -eq $b) { $res.Error = '读取 BND.ADDR 超时'; $res.ErrorKind = 'timeout'; return $res }
      $res.Address = ([System.Net.IPAddress]::new([byte[]]@($b[0], $b[1], $b[2], $b[3]))).ToString()
      $res.Port = ([int]$b[4] -shl 8) -bor [int]$b[5]
      $res.Kind = 'ipv4'
    }
    4 {
      $rem = Get-RemainingMs $Deadline
      if ($rem -lt 1) { $res.Error = '读取 BND.ADDR 超时'; $res.ErrorKind = 'timeout'; return $res }
      $b = Read-Socks5Exact -Stream $Stream -Count 18 -TimeoutMs $rem
      if ($null -eq $b) { $res.Error = '读取 BND.ADDR 超时'; $res.ErrorKind = 'timeout'; return $res }
      $ab = New-Object byte[] 16
      [Array]::Copy($b, 0, $ab, 0, 16)
      $res.Address = ([System.Net.IPAddress]::new($ab)).ToString()
      $res.Port = ([int]$b[16] -shl 8) -bor [int]$b[17]
      $res.Kind = 'ipv6'
    }
    3 {
      $rem = Get-RemainingMs $Deadline
      if ($rem -lt 1) { $res.Error = '读取 BND.ADDR 长度超时'; $res.ErrorKind = 'timeout'; return $res }
      $lb = Read-Socks5Exact -Stream $Stream -Count 1 -TimeoutMs $rem
      if ($null -eq $lb) { $res.Error = '读取 BND.ADDR 长度超时'; $res.ErrorKind = 'timeout'; return $res }
      $len = [int]$lb[0]
      if ($len -lt 1) { $res.Error = 'BND.ADDR 域名长度为 0'; $res.ErrorKind = 'protocol'; return $res }
      $rem = Get-RemainingMs $Deadline
      if ($rem -lt 1) { $res.Error = '读取 BND.ADDR 超时'; $res.ErrorKind = 'timeout'; return $res }
      $b = Read-Socks5Exact -Stream $Stream -Count ($len + 2) -TimeoutMs $rem
      if ($null -eq $b) { $res.Error = '读取 BND.ADDR 超时'; $res.ErrorKind = 'timeout'; return $res }
      $res.Address = [System.Text.Encoding]::ASCII.GetString($b, 0, $len)
      $res.Port = ([int]$b[$len] -shl 8) -bor [int]$b[$len + 1]
      $res.Kind = 'domain'
    }
    default { $res.Error = 'BND.ATYP 不受支持'; $res.ErrorKind = 'protocol'; return $res }
  }
  if ($res.Port -lt 1 -or $res.Port -gt 65535) { $res.Error = 'BND.PORT 为 0 或非法'; $res.ErrorKind = 'protocol'; return $res }
  $res.Ok = $true
  return $res
}

function New-Socks5UdpPacket {
  # 构造 SOCKS5 UDP 数据报：RSV=0000、FRAG=00、DST.ADDR/PORT、payload。
  param($Target, [byte[]]$Payload)
  $ms = [System.IO.MemoryStream]::new()
  $ms.WriteByte(0); $ms.WriteByte(0); $ms.WriteByte(0)
  switch ($Target.Kind) {
    'ipv4' {
      $ms.WriteByte(1)
      $b = ([System.Net.IPAddress]::Parse($Target.Host)).GetAddressBytes()
      $ms.Write($b, 0, $b.Length)
    }
    'ipv6' {
      $ms.WriteByte(4)
      $b = ([System.Net.IPAddress]::Parse($Target.Host)).GetAddressBytes()
      $ms.Write($b, 0, $b.Length)
    }
    'domain' {
      $ms.WriteByte(3)
      $nb = [System.Text.Encoding]::ASCII.GetBytes([string]$Target.Host)
      $ms.WriteByte([byte]$nb.Length)
      $ms.Write($nb, 0, $nb.Length)
    }
    default { throw '不支持的目标类型' }
  }
  $p = [BitConverter]::GetBytes([uint16]$Target.Port)
  if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($p) }
  $ms.Write($p, 0, 2)
  $ms.Write($Payload, 0, $Payload.Length)
  $out = $ms.ToArray()
  $ms.Dispose()
  return , $out
}

function New-DnsQueryPayload {
  # 标准 DNS A 查询（随机 TXID + example.com）。只用于证明 UDP 往返，不解析内容、不写日志。
  param([int]$TxId)
  $ms = [System.IO.MemoryStream]::new()
  $ms.WriteByte([byte](($TxId -shr 8) -band 0xFF))
  $ms.WriteByte([byte]($TxId -band 0xFF))
  foreach ($b in @(1, 0, 0, 1, 0, 0, 0, 0, 0, 0)) { $ms.WriteByte([byte]$b) }
  foreach ($label in @('example', 'com')) {
    $nb = [System.Text.Encoding]::ASCII.GetBytes($label)
    $ms.WriteByte([byte]$nb.Length)
    $ms.Write($nb, 0, $nb.Length)
  }
  $ms.WriteByte(0)
  foreach ($b in @(0, 1, 0, 1)) { $ms.WriteByte([byte]$b) }
  $out = $ms.ToArray()
  $ms.Dispose()
  return , $out
}

function Test-Socks5TcpConnect {
  # TCP CONNECT 检查：目标顺序由调用方给出；每个目标新建控制连接，最多 min(2s, remaining)。
  # 只证明 TCP 可达，不证明 TLS/HTTP/注册成功。
  param(
    [string]$ProxyUrl,
    [int]$TimeoutMs = 8000,
    [string[]]$Targets = @('api.cloudflareclient.com:443', 'cloudflare.com:443', '1.1.1.1:443')
  )
  $start = Get-Date
  $res = @{ Ok = $false; Class = 'unavailable'; Stage = 'tcp_connect'; Kind = 'unreachable'; Reason = ''; TcpMs = 0; DurationMs = 0 }
  $f = Test-ProxyEndpointFormat -Url $ProxyUrl
  if (-not $f.Ok) {
    $res.Class = 'config'; $res.Kind = 'format'; $res.Stage = 'format'; $res.Reason = $f.Error
    $res.DurationMs = [int]((Get-Date) - $start).TotalMilliseconds
    return $res
  }
  $deadline = $start.AddMilliseconds([Math]::Max(1000, $TimeoutMs))
  $lastReason = ''
  $lastStage = 'tcp_connect'
  $lastKind = 'unreachable'
  foreach ($targetText in $Targets) {
    $rem = Get-RemainingMs $deadline
    if ($rem -lt 1000) {
      $lastReason = if ($lastReason -eq '') { 'TCP 检查总预算耗尽' } else { $lastReason + '（TCP 检查总预算耗尽）' }
      $lastStage = 'tcp_connect'
      $lastKind = 'timeout'
      break
    }
    $target = Test-HostPortTarget -Text $targetText
    if (-not $target.Ok) { $lastReason = "内置 TCP 目标非法：$($target.Error)"; continue }
    $per = [Math]::Min(2000, $rem)
    $targetDeadline = (Get-Date).AddMilliseconds($per)
    $conn = Connect-Socks5Control -Endpoint $f -TimeoutMs $per
    if ($conn.Class -eq 'config') {
      $res.Class = 'config'; $res.Kind = $conn.Kind; $res.Stage = $conn.Stage; $res.Reason = $conn.Reason
      $res.DurationMs = [int]((Get-Date) - $start).TotalMilliseconds
      $res.TcpMs = $res.DurationMs
      return $res
    }
    if (-not $conn.Ok) {
      $lastReason = "连接 SOCKS5 入口失败：$($conn.Reason)"
      $lastStage = $conn.Stage
      $lastKind = $conn.Kind
      continue
    }
    try {
      $req = New-Socks5ConnectRequest -Target $target
      $wrem = [Math]::Min((Get-RemainingMs $targetDeadline), (Get-RemainingMs $deadline))
      if (-not (Write-Socks5Bytes -Stream $conn.Stream -Bytes $req -TimeoutMs $wrem)) {
        $lastReason = "CONNECT $targetText 发送超时"
        $lastStage = 'tcp_connect_reply'; $lastKind = 'timeout'
        continue
      }
      $rrem = [Math]::Min((Get-RemainingMs $targetDeadline), (Get-RemainingMs $deadline))
      $cr = Read-Socks5Exact -Stream $conn.Stream -Count 4 -TimeoutMs $rrem
      if ($null -eq $cr) {
        $lastReason = "CONNECT $targetText 等待回复超时"
        $lastStage = 'tcp_connect_reply'; $lastKind = 'timeout'
        continue
      }
      if ($cr[0] -ne 5) {
        $lastReason = "CONNECT $targetText 回复版本异常"
        $lastStage = 'tcp_connect_reply'; $lastKind = 'protocol'
        continue
      }
      if ($cr[1] -eq 0) {
        $res.Ok = $true; $res.Class = 'ok'; $res.Kind = 'ok'; $res.Stage = 'tcp_connect_reply'
        $res.Reason = "TCP CONNECT $targetText ok"
        $res.DurationMs = [int]((Get-Date) - $start).TotalMilliseconds
        $res.TcpMs = $res.DurationMs
        return $res
      }
      $lastReason = ('CONNECT {0} 被拒（REP=0x{1:X2}）' -f $targetText, $cr[1])
      $lastStage = 'tcp_connect_reply'; $lastKind = 'unreachable'
    } finally {
      if ($conn.Stream -ne $null) { try { $conn.Stream.Dispose() } catch { } }
      if ($conn.Client -ne $null) { try { $conn.Client.Dispose() } catch { } }
    }
  }
  $res.Stage = $lastStage
  $res.Kind = $lastKind
  $res.Reason = if ($lastReason -ne '') { $lastReason } else { '所有 TCP 目标均失败' }
  $res.DurationMs = [int]((Get-Date) - $start).TotalMilliseconds
  $res.TcpMs = $res.DurationMs
  return $res
}

function Test-Socks5UdpDns {
  # UDP ASSOCIATE + DNS 往返。只证明 SOCKS5 UDP relay；不证明 WARP 外层 UDP 2408 可用。
  param([string]$ProxyUrl, [string]$Target, [int]$TimeoutMs = 3000)
  $start = Get-Date
  $res = @{ Ok = $false; Class = 'unavailable'; Stage = 'udp_associate'; Kind = 'unreachable';
    Reason = ''; UdpMs = 0; UdpTarget = $Target; DurationMs = 0 }
  $f = Test-ProxyEndpointFormat -Url $ProxyUrl
  if (-not $f.Ok) {
    $res.Class = 'config'; $res.Kind = 'format'; $res.Stage = 'format'; $res.Reason = $f.Error
    $res.DurationMs = [int]((Get-Date) - $start).TotalMilliseconds
    return $res
  }
  $tgt = Test-HostPortTarget -Text $Target
  if (-not $tgt.Ok) {
    $res.Class = 'config'; $res.Kind = 'format'; $res.Stage = 'format'
    $res.Reason = 'UDP 目标非法：' + $tgt.Error
    $res.DurationMs = [int]((Get-Date) - $start).TotalMilliseconds
    return $res
  }
  $deadline = $start.AddMilliseconds([Math]::Max(1000, $TimeoutMs))
  $conn = $null
  $udp = $null
  try {
    $conn = Connect-Socks5Control -Endpoint $f -TimeoutMs ([Math]::Max(1, (Get-RemainingMs $deadline)))
    if ($conn.Class -eq 'config') {
      $res.Class = 'config'; $res.Kind = $conn.Kind; $res.Stage = $conn.Stage; $res.Reason = $conn.Reason
      return $res
    }
    if (-not $conn.Ok) {
      $res.Kind = $conn.Kind; $res.Stage = $conn.Stage; $res.Reason = $conn.Reason; return $res
    }
    $assoc = [byte[]]@(5, 3, 0, 1, 0, 0, 0, 0, 0, 0)
    $wrem = Get-RemainingMs $deadline
    if (-not (Write-Socks5Bytes -Stream $conn.Stream -Bytes $assoc -TimeoutMs $wrem)) {
      $res.Kind = 'timeout'; $res.Reason = '发送 UDP ASSOCIATE 超时'; return $res
    }
    $hdr = Read-Socks5Exact -Stream $conn.Stream -Count 4 -TimeoutMs (Get-RemainingMs $deadline)
    if ($null -eq $hdr) { $res.Kind = 'timeout'; $res.Reason = '等待 UDP ASSOCIATE 回复超时'; return $res }
    if ($hdr[0] -ne 5) { $res.Kind = 'protocol'; $res.Reason = 'UDP ASSOCIATE 回复版本异常'; return $res }
    if ($hdr[1] -ne 0) {
      $res.Kind = 'protocol'
      $res.Reason = ('UDP ASSOCIATE 被拒（REP=0x{0:X2}）' -f $hdr[1]); return $res
    }
    # ASSOCIATE 已成功：从这里起都属于 DNS 数据路径，Stage 固定 udp_dns（review M4）。
    $res.Stage = 'udp_dns'
    $bound = Read-Socks5BoundAddress -Stream $conn.Stream -Atyp ([int]$hdr[3]) -Deadline $deadline
    if (-not $bound.Ok) {
      $res.Kind = if ([string]$bound.ErrorKind -ne '') { [string]$bound.ErrorKind } else { 'protocol' }
      $res.Reason = $bound.Error
      return $res
    }
    $relayIp = $null
    if ($bound.Kind -eq 'domain') {
      $rr = Resolve-Socks5Host -HostName ([string]$bound.Address) -TimeoutMs ([Math]::Max(1, (Get-RemainingMs $deadline)))
      if ($rr.Ok) { $relayIp = $rr.IP }
    } else {
      $relayIp = [System.Net.IPAddress]::Parse([string]$bound.Address)
    }
    if ($relayIp -eq $null) {
      # ATYP=0x03 解析失败 / BND.ADDR 非数字：按设计退回控制连接远端 IP，端口仍用 BND.PORT。
      if ($conn.RemoteIP -ne $null) { $relayIp = $conn.RemoteIP }
      else { $res.Kind = 'protocol'; $res.Reason = '无法确定 UDP relay 地址'; return $res }
    }
    if ($relayIp.Equals([System.Net.IPAddress]::Any) -or $relayIp.Equals([System.Net.IPAddress]::IPv6Any)) {
      if ($conn.RemoteIP -eq $null) { $res.Kind = 'protocol'; $res.Reason = 'BND.ADDR 为 any 且读不到控制连接远端地址'; return $res }
      $relayIp = $conn.RemoteIP
    }
    $relayPort = [int]$bound.Port
    $udp = [System.Net.Sockets.UdpClient]::new($relayIp.AddressFamily)
    $urem = Get-RemainingMs $deadline
    if ($urem -lt 1) { $res.Kind = 'timeout'; $res.Reason = 'UDP 预算耗尽'; return $res }
    try { $udp.Connect($relayIp, $relayPort) } catch {
      $res.Kind = 'unreachable'; $res.Reason = 'UDP relay 连接失败'; return $res
    }
    $txid = Get-Random -Minimum 0 -Maximum 65536
    $payload = New-DnsQueryPayload -TxId $txid
    $dgram = New-Socks5UdpPacket -Target $tgt -Payload $payload
    $urem = Get-RemainingMs $deadline
    try { [void]$udp.Send($dgram, $dgram.Length) } catch {
      $res.Kind = 'unreachable'; $res.Reason = '发送 UDP 数据报失败'; return $res
    }
    $urem = Get-RemainingMs $deadline
    if ($urem -lt 1) { $res.Kind = 'timeout'; $res.Reason = '等待 UDP 回复预算耗尽'; return $res }
    try { $udp.Client.ReceiveTimeout = [int]$urem } catch { }
    $remote = [System.Net.IPEndPoint]::new([System.Net.IPAddress]::Any, 0)
    $data = $null
    try { $data = $udp.Receive([ref]$remote) } catch {
      $res.Kind = 'timeout'; $res.Reason = '等待 UDP 回复超时'; return $res
    }
    $urem = Get-RemainingMs $deadline
    $res.UdpMs = [int]((Get-Date) - $start).TotalMilliseconds
    $res.DurationMs = $res.UdpMs
    if ($data -eq $null -or $data.Length -lt 10) { $res.Kind = 'protocol'; $res.Reason = 'UDP 回复过短'; return $res }
    if ($data[0] -ne 0 -or $data[1] -ne 0) { $res.Kind = 'protocol'; $res.Reason = 'UDP RSV 非 0'; return $res }
    if ($data[2] -ne 0) { $res.Kind = 'protocol'; $res.Reason = 'UDP FRAG 非 0'; return $res }
    $off = 4
    switch ([int]$data[3]) {
      1 { $off += 4 }
      4 { $off += 16 }
      3 {
        $len = [int]$data[$off]
        $off += 1 + $len
      }
      default { $res.Kind = 'protocol'; $res.Reason = 'UDP 回复 ATYP 不支持'; return $res }
    }
    $off += 2
    if ($off + 2 -gt $data.Length) { $res.Kind = 'protocol'; $res.Reason = 'UDP 回复地址越界'; return $res }
    $respTx = ([int]$data[$off] -shl 8) -bor [int]$data[$off + 1]
    if ($respTx -ne $txid) { $res.Kind = 'protocol'; $res.Reason = 'DNS TXID 不匹配'; return $res }
    $res.Ok = $true
    $res.Class = 'ok'
    $res.Kind = 'ok'
    $res.Stage = 'udp_dns'
    $res.Reason = "UDP DNS $Target ok"
    return $res
  } catch {
    $res.Kind = 'unreachable'
    $res.Reason = 'UDP 检查异常：' + (Mask-Proxy (Mask-Text $_.Exception.Message))
    return $res
  } finally {
    if ($udp -ne $null) { try { $udp.Dispose() } catch { } }
    if ($conn -ne $null) {
      if ($conn.Stream -ne $null) { try { $conn.Stream.Dispose() } catch { } }
      if ($conn.Client -ne $null) { try { $conn.Client.Dispose() } catch { } }
    }
    $res.DurationMs = [int]((Get-Date) - $start).TotalMilliseconds
    if ($res.UdpMs -eq 0) { $res.UdpMs = $res.DurationMs }
  }
}

function Test-WarpProxyCheck {
  # 一次完整检查：TCP（api.cloudflareclient.com:443 → cloudflare.com:443 → 1.1.1.1:443）
  # 然后（require_udp 时）UDP DNS（默认 1.1.1.1:53 → 8.8.8.8:53）。总预算 TimeoutSeconds。
  param(
    [string]$ProxyUrl,
    [int]$TimeoutSeconds = 8,
    [bool]$RequireUdp = $true,
    [string]$UdpTarget = '1.1.1.1:53'
  )
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  $res = @{ Ok = $false; Class = 'unavailable'; Kind = 'timeout'; Stage = 'format'; Reason = '';
    DurationMs = 0; TcpMs = 0; UdpMs = 0; TcpOk = $false; UdpOk = $false; UdpTarget = $UdpTarget }
  $f = Test-ProxyEndpointFormat -Url $ProxyUrl
  if (-not $f.Ok) {
    $res.Class = 'config'; $res.Kind = 'format'; $res.Stage = 'format'; $res.Reason = $f.Error
    $res.DurationMs = [int]$sw.ElapsedMilliseconds
    return $res
  }
  $ut = Test-HostPortTarget -Text $UdpTarget
  if (-not $ut.Ok) {
    $res.Class = 'config'; $res.Kind = 'format'; $res.Stage = 'format'
    $res.Reason = 'UDP 目标非法：' + $ut.Error
    $res.DurationMs = [int]$sw.ElapsedMilliseconds
    return $res
  }
  $totalMs = [Math]::Max(1000, $TimeoutSeconds * 1000)
  $tcp = Test-Socks5TcpConnect -ProxyUrl $ProxyUrl -TimeoutMs $totalMs
  $res.TcpOk = [bool]$tcp.Ok
  $res.TcpMs = [int]$tcp.DurationMs
  $res.Stage = $tcp.Stage
  $res.Kind = $tcp.Kind
  $res.Reason = $tcp.Reason
  if ($tcp.Class -eq 'config') {
    $res.Class = 'config'; $res.DurationMs = [int]$sw.ElapsedMilliseconds
    return $res
  }
  if (-not $tcp.Ok) {
    $res.DurationMs = [int]$sw.ElapsedMilliseconds
    return $res
  }
  if (-not $RequireUdp) {
    $res.Ok = $true; $res.Class = 'ok'; $res.Kind = 'ok'; $res.Stage = 'ok'
    $res.Reason = 'TCP ok（未要求 UDP）'
    $res.DurationMs = [int]$sw.ElapsedMilliseconds
    return $res
  }
  $udpTargets = @($UdpTarget)
  if ($UdpTarget -ne '8.8.8.8:53') { $udpTargets += '8.8.8.8:53' }
  $lastUdp = $null
  $udpTotalMs = 0
  foreach ($item in $udpTargets) {
    $rem = $totalMs - [int]$sw.ElapsedMilliseconds
    if ($rem -lt 1000) { break }
    $per = [Math]::Min(3000, $rem)
    $udp = Test-Socks5UdpDns -ProxyUrl $ProxyUrl -Target $item -TimeoutMs $per
    $lastUdp = $udp
    $udpTotalMs += [int]$udp.DurationMs
    $res.UdpTarget = [string]$udp.UdpTarget
    if ($udp.Class -eq 'config') {
      $res.Class = 'config'; $res.Kind = $udp.Kind; $res.Stage = $udp.Stage; $res.Reason = $udp.Reason
      $res.UdpMs = $udpTotalMs; $res.DurationMs = [int]$sw.ElapsedMilliseconds
      return $res
    }
    if ($udp.Ok) {
      $res.Ok = $true; $res.Class = 'ok'; $res.Kind = 'ok'; $res.Stage = 'udp_dns'
      $res.Reason = $udp.Reason; $res.UdpOk = $true; $res.UdpMs = $udpTotalMs
      $res.DurationMs = [int]$sw.ElapsedMilliseconds
      return $res
    }
  }
  $res.Class = 'unavailable'
  $res.UdpMs = $udpTotalMs
  if ($lastUdp -ne $null) {
    $res.Kind = $lastUdp.Kind
    $res.Stage = $lastUdp.Stage
    $res.Reason = $lastUdp.Reason
  } elseif ($res.Kind -eq 'ok') {
    $res.Kind = 'timeout'; $res.Stage = 'udp_dns'; $res.Reason = 'TCP 通过后 UDP 总预算耗尽'
  }
  $res.DurationMs = [int]$sw.ElapsedMilliseconds
  return $res
}

function Invoke-WarpProxyCheckWithRecheck {
  # r1 + （recheck>0 时）sleep 后 r2。recheck=0 跳过 r2 直接确认失败。
  # 返回值里 ConfigError/Recovered/ConfirmedDown 供状态机固定分类；日志由调用方打。
  param(
    [string]$ProxyUrl,
    [int]$TimeoutSeconds = 8,
    [bool]$RequireUdp = $true,
    [string]$UdpTarget = '1.1.1.1:53',
    [int]$RecheckSeconds = 5
  )
  $r1 = Test-WarpProxyCheck -ProxyUrl $ProxyUrl -TimeoutSeconds $TimeoutSeconds -RequireUdp $RequireUdp -UdpTarget $UdpTarget
  $out = @{ Ok = $false; Class = 'unavailable'; Kind = $r1.Kind; Stage = $r1.Stage; Reason = $r1.Reason;
    DurationMs = $r1.DurationMs; TcpMs = $r1.TcpMs; UdpMs = $r1.UdpMs; TcpOk = $r1.TcpOk; UdpOk = $r1.UdpOk;
    UdpTarget = $r1.UdpTarget; R1 = $r1; R2 = $null; Rechecked = $false; Recovered = $false;
    ConfigError = $false; ConfirmedDown = $false }
  if ($r1.Class -eq 'config') {
    $out.Class = 'config'; $out.ConfigError = $true
    return $out
  }
  if ($r1.Ok) {
    $out.Ok = $true; $out.Class = 'ok'
    return $out
  }
  $r2 = $null
  if ($RecheckSeconds -gt 0) {
    if ($RecheckSeconds -gt 0) { Start-Sleep -Seconds $RecheckSeconds }
    $r2 = Test-WarpProxyCheck -ProxyUrl $ProxyUrl -TimeoutSeconds $TimeoutSeconds -RequireUdp $RequireUdp -UdpTarget $UdpTarget
    $out.R2 = $r2
    $out.Rechecked = $true
    $out.Kind = $r2.Kind; $out.Stage = $r2.Stage; $out.Reason = $r2.Reason
    $out.DurationMs = [int]$r1.DurationMs + [int]$r2.DurationMs + ($RecheckSeconds * 1000)
    $out.TcpOk = $r2.TcpOk; $out.UdpOk = $r2.UdpOk; $out.TcpMs = $r2.TcpMs; $out.UdpMs = $r2.UdpMs
    $out.UdpTarget = $r2.UdpTarget
    if ($r2.Class -eq 'config') {
      $out.Class = 'config'; $out.ConfigError = $true
      return $out
    }
    if ($r2.Ok) {
      $out.Ok = $true; $out.Class = 'ok'; $out.Recovered = $true
      return $out
    }
  }
  $out.Class = 'unavailable'
  $out.ConfirmedDown = $true
  return $out
}

function Read-GatewayWarpSettings {
  # 只读 config.json 的 warp.enabled / warp.proxy（外加日志用的 proxies/proxyfile/key 数量）。
  # 复用 Strip-JsonComments；按 LastWriteTimeUtc + 文件长度缓存。解析失败时保留上一份成功
  # 解析的设置（last-good）；从未成功过才让调用方退回 legacy。不写任何文件、不打印 key。
  param([string]$Path)
  $res = @{ Ok = $false; ConfigError = $false; ReadFailed = $false; Stale = $false; Error = '';
    Enabled = $false; Proxy = ''; ProxyOk = $true; ProxyError = ''; ProxyCount = 0; ProxyFile = '';
    KeyCount = 0; Path = $Path }
  if ([string]::IsNullOrWhiteSpace($Path)) { $Path = Join-Path $PSScriptRoot 'config.json'; $res.Path = $Path }
  $copyFrom = {
    param($src)
    foreach ($k in @($src.Keys)) { $res[$k] = $src[$k] }
    $res.Path = $Path
  }
  if (-not (Test-Path -LiteralPath $Path)) {
    if ($script:GatewayWarpLastGood -ne $null) {
      & $copyFrom $script:GatewayWarpLastGood
      $res.Stale = $true; $res.ReadFailed = $true
      $res.Error = "找不到网关 config.json：$Path（沿用最近一份成功设置）"
      return $res
    }
    $res.Error = "找不到网关 config.json：$Path"
    return $res
  }
  $fi = $null
  $mtime = 0
  $len = 0
  try { $fi = Get-Item -LiteralPath $Path; $mtime = $fi.LastWriteTimeUtc.Ticks; $len = $fi.Length } catch { }
  if ($script:GatewayWarpCache -ne $null -and $script:GatewayWarpCache.Mtime -eq $mtime -and
    $script:GatewayWarpCache.Length -eq $len -and $script:GatewayWarpCache.Result -ne $null) {
    & $copyFrom $script:GatewayWarpCache.Result
    return $res
  }
  $cfg = $null
  try {
    $raw = Get-Content -LiteralPath $Path -Raw -Encoding utf8
    try { $cfg = $raw | ConvertFrom-Json -Depth 20 }
    catch { $cfg = Strip-JsonComments -Text $raw | ConvertFrom-Json -Depth 20 }
  } catch {
    if ($script:GatewayWarpLastGood -ne $null) {
      & $copyFrom $script:GatewayWarpLastGood
      $res.Stale = $true; $res.ReadFailed = $true
      $res.Error = 'config.json 解析失败：' + (Mask-Proxy (Mask-Text $_.Exception.Message))
      return $res
    }
    $res.ReadFailed = $true
    $res.Error = 'config.json 解析失败：' + (Mask-Proxy (Mask-Text $_.Exception.Message))
    return $res
  }
  if (-not (Test-JsonObjectShape $cfg)) {
    $res.ReadFailed = $true; $res.Error = 'config.json 顶层不是 JSON 对象'; return $res
  }
  try {
    $proxies = Get-Prop $cfg 'proxies'
    if ($proxies -is [System.Array]) { $res.ProxyCount = @($proxies).Count }
    $pf = Get-Prop $cfg 'proxyfile'
    if ($pf -is [string]) { $res.ProxyFile = [string]$pf }
    $keys = Get-Prop $cfg 'server_keys'
    if ($keys -is [System.Array]) { $res.KeyCount = @($keys).Count }
    elseif ($keys -is [string] -and -not [string]::IsNullOrWhiteSpace([string]$keys)) { $res.KeyCount = 1 }
  } catch { }
  $warp = $null
  try { $warp = Get-Prop $cfg 'warp' } catch { $warp = $null }
  if ($warp -ne $null -and -not (Test-JsonObjectShape $warp)) {
    $res.ConfigError = $true; $res.Error = 'config.json 的 warp 必须是对象'
    return $res
  }
  if ($warp -ne $null) {
    if (Test-HasKey $warp 'enabled') {
      $rawEnabled = Get-Prop $warp 'enabled'
      if (-not ($rawEnabled -is [bool])) {
        $res.ConfigError = $true; $res.Error = 'config.json 的 warp.enabled 必须是 bool'
        return $res
      }
      $res.Enabled = [bool]$rawEnabled
    }
    if (Test-HasKey $warp 'proxy') {
      $rawProxy = Get-Prop $warp 'proxy'
      if (-not ($rawProxy -is [string])) {
        $res.ConfigError = $true; $res.Error = 'config.json 的 warp.proxy 必须是字符串'
        return $res
      }
      $res.Proxy = ([string]$rawProxy).Trim()
    }
  }
  if ($res.Proxy -ne '') {
    $fmt = Test-ProxyEndpointFormat -Url $res.Proxy
    if (-not $fmt.Ok) {
      $res.ConfigError = $true; $res.ProxyOk = $false; $res.ProxyError = $fmt.Error
      $res.Error = 'config.json 的 warp.proxy 非法：' + $fmt.Error
      return $res
    }
  }
  $res.Ok = $true
  $script:GatewayWarpCache = @{ Mtime = $mtime; Length = $len; Result = $res }
  $script:GatewayWarpLastGood = $res
  return $res
}

function Get-ClashManagedProxyPorts {
  # 只读 clash-verge.yaml 的 mixed-port / socks-port（正向证据）。HTTP port 不是 SOCKS5 入口，
  # 故意不读。读不到或没有端口时 Ok=false，auto 只监控不切换。
  $res = @{ Ok = $false; Ports = @(); MixedPort = 0; SocksPort = 0; Error = '' }
  $p = Join-Path $script:VergeRoot 'clash-verge.yaml'
  if (-not (Test-Path -LiteralPath $p)) { $res.Error = '找不到 clash-verge.yaml'; return $res }
  try { $lines = Get-Content -LiteralPath $p -Encoding utf8 } catch {
    $res.Error = '读取 clash-verge.yaml 失败'; return $res
  }
  foreach ($ln in @($lines)) {
    if ($ln -match '^\s*mixed-port:\s*(\d+)\s*$') { $res.MixedPort = [int]$Matches[1] }
    elseif ($ln -match '^\s*socks-port:\s*(\d+)\s*$') { $res.SocksPort = [int]$Matches[1] }
  }
  if ($res.MixedPort -gt 0) { $res.Ports += $res.MixedPort }
  if ($res.SocksPort -gt 0) { $res.Ports += $res.SocksPort }
  $res.Ok = @($res.Ports).Count -gt 0
  if (-not $res.Ok) { $res.Error = 'clash-verge.yaml 未暴露 mixed-port/socks-port' }
  return $res
}

function Test-WarpProxyCanFailover {
  # auto：必须是 loopback 且端口命中 clash-verge.yaml 的 mixed-port/socks-port；
  # 读不到 yaml 或端口不匹配 = 只监控。on 强制；off 只监控。HTTP port 永远不算证据。
  param([string]$ProxyUrl)
  if ($script:ProxyCheckFailover -eq 'off') {
    return @{ Allowed = $false; Reason = 'proxy_check_failover=off（只监控）' }
  }
  if ($script:ProxyCheckFailover -eq 'on') {
    return @{ Allowed = $true; Reason = 'proxy_check_failover=on（强制允许）' }
  }
  $f = Test-ProxyEndpointFormat -Url $ProxyUrl
  if (-not $f.Ok) { return @{ Allowed = $false; Reason = '代理地址非法：' + $f.Error } }
  $loop = [bool]$f.IsLoopback
  if (-not $loop -and $f.Kind -eq 'domain') {
    try {
      $task = [System.Net.Dns]::GetHostAddressesAsync([string]$f.Host)
      if ($task.Wait(1000)) {
        $addrs = @($task.Result)
        if (@($addrs).Count -gt 0) {
          $loop = $true
          foreach ($a in $addrs) {
            $b = $a.GetAddressBytes()
            $isLoop = ($a.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork -and $b[0] -eq 127) -or
              $a.Equals([System.Net.IPAddress]::IPv6Loopback)
            if (-not $isLoop) { $loop = $false; break }
          }
        }
      }
    } catch { $loop = $false }
  }
  if (-not $loop) {
    return @{ Allowed = $false; Reason = 'warp.proxy 不是本机 loopback 入口（' + (Format-ProxyEndpoint $ProxyUrl) + '），auto 只监控不切换' }
  }
  $cm = Get-ClashManagedProxyPorts
  if (-not $cm.Ok) {
    return @{ Allowed = $false; Reason = '读不到 clash-verge.yaml 的 mixed-port/socks-port：' + $cm.Error + '，auto 只监控' }
  }
  if (@($cm.Ports) -notcontains [int]$f.Port) {
    return @{ Allowed = $false; Reason = '端口 ' + [int]$f.Port + ' 不匹配 clash-verge.yaml 的 mixed-port/socks-port（' + (@($cm.Ports) -join '/') + '），auto 只监控' }
  }
  return @{ Allowed = $true; Reason = '本机 Clash 入口（端口 ' + [int]$f.Port + '）' }
}

function Resolve-ProxyCheckPlan {
  # 决定本轮走 legacy / warp-direct / warp-proxy。off 时完全不读 config.json 的 WARP 字段。
  # -ProxyCheckEndpoint 只在 ProbeOnly / DryRun 生效；正常循环忽略并 WARN 一次。
  $plan = @{ Mode = 'legacy'; ConfigError = $false; ErrorLogged = $false; Reason = ''; Endpoint = ''; Source = '';
    Enabled = $false; Stale = $false; UdpTarget = $script:ProxyCheckUdpTarget; UdpTargetError = '' }
  # proxy_check_failover=on 只提示一次（不再每轮刷）。auto 的 loopback/端口证据只在决定切换时看。
  $warnOnHint = {
    if ($script:ProxyCheckFailover -eq 'on' -and -not $script:WarpFailoverOnWarned) {
      $script:WarpFailoverOnWarned = $true
      Write-WdLog 'proxy-check proxy_check_failover=on：被检查的 SOCKS5 入口可能不是当前 WARP 承载者（auto 才校验 loopback + mixed-port/socks-port）' 'WARN'
    }
  }
  $ut = Test-HostPortTarget -Text $script:ProxyCheckUdpTarget
  if (-not $ut.Ok) { $plan.UdpTargetError = $ut.Error }
  if ($script:ProxyCheckMode -eq 'off') { $plan.Reason = 'proxy_check_mode=off'; return $plan }
  $useOverride = ($script:ProxyCheckEndpoint -ne '' -and ($ProxyCheckProbeOnly -or $DryRun))
  if ($script:ProxyCheckEndpoint -ne '' -and -not $useOverride -and -not $script:ProxyCheckEndpointWarned) {
    $script:ProxyCheckEndpointWarned = $true
    Write-WdLog 'proxy-check 正常循环忽略 -ProxyCheckEndpoint（该参数只在 -ProxyCheckProbeOnly / -DryRun 生效）' 'WARN'
  }
  if ($useOverride) {
    $plan.Mode = 'warp-proxy'
    $plan.Source = 'endpoint'
    $plan.Endpoint = $script:ProxyCheckEndpoint
    $fmt = Test-ProxyEndpointFormat -Url $script:ProxyCheckEndpoint
    if (-not $fmt.Ok) { $plan.ConfigError = $true; $plan.Reason = $fmt.Error }
    elseif ($plan.UdpTargetError -ne '') { $plan.ConfigError = $true; $plan.Reason = 'UDP 目标非法：' + $plan.UdpTargetError }
    if (-not $plan.ConfigError) { & $warnOnHint }
    return $plan
  }
  $settings = Read-GatewayWarpSettings -Path (Join-Path $PSScriptRoot 'config.json')
  if ($settings.ReadFailed -and $settings.Error -ne '') {
    $sig = 'gateway-warp|' + $settings.Error
    if ($script:WarpProxyConfigErrSig -ne $sig) {
      $script:WarpProxyConfigErrSig = $sig
      Write-WdLog ('proxy-check 读取 config.json 的 WARP 设置失败：' + $settings.Error) 'WARN'
    }
  }
  # 必须先处理 ConfigError：Read-GatewayWarpSettings 在类型/URL 非法时返回 Ok=false+ConfigError=true。
  # 普通模式按设计（§8.3）退回 legacy，但错误必须留痕；ProbeOnly 由启动分支按 ConfigError exit 2。
  if ($settings.ConfigError) {
    $sig = 'gateway-warp-config|' + [string]$settings.Error
    if ($script:WarpProxyConfigErrSig -ne $sig) {
      $script:WarpProxyConfigErrSig = $sig
      Write-WdLog ('proxy-check config error：' + (Shorten (Mask-Proxy (Mask-Text ([string]$settings.Error))) 200)) 'ERROR'
    }
    $plan.ErrorLogged = $true
    $plan.Mode = 'legacy'
    $plan.ConfigError = $true
    $plan.Source = 'config'
    $plan.Endpoint = [string]$settings.Proxy
    $plan.Reason = [string]$settings.Error
    return $plan
  }
  if (-not $settings.Ok) { $plan.Reason = [string]$settings.Error; return $plan }
  $plan.Enabled = [bool]$settings.Enabled
  $plan.Stale = [bool]$settings.Stale
  $plan.Source = 'config'
  if (-not $settings.ReadFailed -and $script:WarpProxyConfigErrSig -like 'gateway-warp*') {
    $script:WarpProxyConfigErrSig = ''
  }
  # ProbeOnly 是独立诊断入口：只要 config.json 有合法的 warp.proxy 就检查，不受
  # warp.enabled 影响（"无目标" 的定义是没有 warp.proxy 且没有 -ProxyCheckEndpoint）。
  if ($ProxyCheckProbeOnly -and -not [string]::IsNullOrWhiteSpace([string]$settings.Proxy)) {
    $plan.Mode = 'warp-proxy'
    $plan.Endpoint = [string]$settings.Proxy
    if ($plan.UdpTargetError -ne '') { $plan.ConfigError = $true; $plan.Reason = 'UDP 目标非法：' + $plan.UdpTargetError }
    if (-not $plan.ConfigError) { & $warnOnHint }
    return $plan
  }
  if ($script:ProxyCheckMode -eq 'force') {
    if ([string]::IsNullOrWhiteSpace([string]$settings.Proxy)) {
      $plan.Reason = 'proxy_check_mode=force 但 warp.proxy 为空'
      $sig = 'force-empty|' + $plan.Reason
      if ($script:WarpProxyConfigErrSig -ne $sig) {
        $script:WarpProxyConfigErrSig = $sig
        Write-WdLog ('proxy-check config error：' + $plan.Reason) 'ERROR'
      }
      $plan.ConfigError = $true
      $plan.ErrorLogged = $true
      return $plan
    }
    $plan.Mode = 'warp-proxy'
    $plan.Endpoint = [string]$settings.Proxy
  } else {
    if (-not $settings.Enabled) { $plan.Reason = 'warp.enabled=false'; return $plan }
    if ([string]::IsNullOrWhiteSpace([string]$settings.Proxy)) {
      $plan.Mode = 'warp-direct'
      $plan.Reason = 'warp.enabled=true 且 warp.proxy 为空（直连 WARP）'
      return $plan
    }
    $plan.Mode = 'warp-proxy'
    $plan.Endpoint = [string]$settings.Proxy
  }
  if ($plan.UdpTargetError -ne '') { $plan.ConfigError = $true; $plan.Reason = 'UDP 目标非法：' + $plan.UdpTargetError }
  if (-not $plan.ConfigError) { & $warnOnHint }
  return $plan
}

function Set-WarpProxyMode {
  # 模式切换清理（§6）：清轮冷却/transport 计数/恢复签到/WARP 状态/Clash 错误签名；
  # 不清 LastSeenNow、LastRefresh/LastSwitch、QuotaFailedAt（WARP 候选排序用 IgnoreQuotaPenalty）。
  param([bool]$Active)
  $target = if ($Active) { 'warp-proxy' } else { 'legacy' }
  if ($script:WarpProxyLastMode -ne '' -and $script:WarpProxyLastMode -ne $target) {
    $script:RoundCooldownUntil = [DateTime]::MinValue
    $script:TransportStrikes = 0
    $script:OkStreak = 0
    $script:StreakStart = [DateTime]::MinValue
    $script:LastStaleProbe = [DateTime]::MinValue
    $script:WarpProxyLastState = ''
    $script:WarpProxyLastLogAt = [DateTime]::MinValue
    $script:WarpProxyConfigErrSig = ''
    $script:LastClashErrSig = ''
    Write-WdLog ('proxy-check 模式切换：' + $script:WarpProxyLastMode + ' -> ' + $target + '（已清理轮冷却/transport 计数等状态）')
  }
  $script:WarpProxyLastMode = $target
  $script:WarpProxyActive = $Active
}

function Get-Prop {
  param($Obj, [string]$Name)
  if ($Obj -eq $null) { return $null }
  # 哈希表（@{...}）的键不在 PSObject.Properties 里：缺键时点访问在
  # StrictMode 下直接抛“找不到属性”，必须走 IDictionary 分支（索引缺键不抛，Contains 双保险）。
  # 两处 return 都带一元逗号：直接 return 数组会被输出流展开，空数组展开成“零个对象”，
  # 调用方拿到的是 $null 而不是 @()，单元素数组还会塌成标量。加逗号后原样传出。
  if ($Obj -is [System.Collections.IDictionary]) {
    if ($Obj.Contains($Name)) { return , $Obj[$Name] }
    return $null
  }
  $p = $Obj.PSObject.Properties[$Name]
  if ($p -eq $null) { return $null }
  return , $p.Value
}

function Test-JsonObjectShape {
  # 判断“是不是一个 JSON 对象”。不能用 `$x -is [PSCustomObject]`：PowerShell 把一切对象
  # 都包成 PSObject，该类型字面量对 $null / 数字 / 数组一律返回 True（实测确认），
  # 拿它做校验等于没校验。改用拒绝列表——ConvertFrom-Json 的产物只可能是
  # PSCustomObject / 数组 / 字符串 / 数字 / 布尔 / null，这个列表是完备的。
  param($Value)
  if ($null -eq $Value) { return $false }
  if ($Value -is [System.Array]) { return $false }
  if ($Value -is [string]) { return $false }
  if ($Value -is [System.ValueType]) { return $false }
  return $true
}

function Get-ShapeName {
  # 字符串值可能来自误路由的 HTTP 错误正文（如带 token 的 URL），先脱敏再截断，
  # 不能原样拼进异常/日志；类型名与数组项数不包含正文，保持原样。
  param($Value)
  if ($null -eq $Value) { return '<null>' }
  if ($Value -is [System.Array]) { return "数组（$(@($Value).Count) 项）" }
  if ($Value -is [string]) { return "字符串（$(Shorten (Mask-Text $Value) 120)）" }
  return $Value.GetType().Name
}

function Test-HasKey {
  # 判“键是否存在”而不是取值为 null：JSON 的 [] / 0 / "" 都是合法配置值，
  # 用 `if ((Get-Prop $c 'k') -ne $null)` 判会把 [] 当成缺键（空数组在布尔上下文为假），
  # 于是 exclude_contains: [] 反而回退内置默认值、关不掉排除。本函数专治这个。
  param($Obj, [string]$Name)
  if ($Obj -eq $null) { return $false }
  if ($Obj -is [System.Collections.IDictionary]) { return [bool]$Obj.Contains($Name) }
  return ($Obj.PSObject.Properties[$Name]) -ne $null
}

function ConvertTo-StrList {
  # JSON 数组 → 去空白字符串列表；null 项与空白项一律丢弃。
  # 空数组是合法输入（= 关闭该项），调用方用 Test-HasKey 区分“空”与“没有这个键”。
  # 只收 JSON 数组：标量/对象/非字符串元素一律报错。因为调用方是「整体替换内置默认值」，
  # 把数组漏写成裸字符串（编辑器补全常见）会让 7 条默认排除规则悄悄变成 1 条，
  # null 更糟——会被当成“主动关闭排除”，占位节点就此回到候选池。
  param($Value, [string]$KeyName)
  if ($null -eq $Value) {
    throw "看门狗配置 $KeyName 是 null：写 [] 表示关闭该项，写 [`"关键词`"] 才表示生效规则"
  }
  if ($Value -isnot [System.Array]) {
    throw "看门狗配置 $KeyName 必须是字符串数组（整体替换内置默认值），实际是 $($Value.GetType().Name)：$(Shorten ([string]$Value) 80)"
  }
  $out = @()
  foreach ($s in @($Value)) {
    if ($null -eq $s) { continue }
    if ($s -isnot [string]) { throw "看门狗配置 $KeyName 的元素必须是字符串，第 '$($s.GetType().Name)' 项不是" }
    $t = $s.Trim()
    if ($t -ne '') { $out += $t }
  }
  return , $out
}

function Resolve-ClashSecret {
  if ($script:ClashSecret -ne '' -and $script:ClashSecret -ne $null) { return $script:ClashSecret }
  $p = Join-Path $script:VergeRoot 'clash-verge.yaml'
  if (Test-Path -LiteralPath $p) {
    $m = Select-String -Path $p -Pattern '^secret:\s*(\S+)\s*$' | Select-Object -First 1
    if ($m -ne $null -and $m.Matches.Count -gt 0) {
      # 兼容 secret: "abc" / secret: 'abc' 的引号写法（裸值不受影响）。
      $script:ClashSecret = ([string]$m.Matches[0].Groups[1].Value).Trim('"', "'")
      return $script:ClashSecret
    }
  }
  throw 'Clash secret 为空：未找到 clash-verge.yaml 中的 secret，请用 -ClashSecret 传入'
}

function Strip-JsonComments {
  # 与 config.go:stripJSONComments 同语义：字符串内的 // 与 https:// 不受影响。
  param([string]$Text)
  $sb = New-Object Text.StringBuilder
  $inStr = $false
  $esc = $false
  $lineC = $false
  $blockC = $false
  $i = 0
  while ($i -lt $Text.Length) {
    $ch = $Text[$i]
    if ($lineC) {
      if ($ch -eq "`n" -or $ch -eq "`r") { $lineC = $false; $sb.Append($ch) | Out-Null }
      else { $sb.Append(' ') | Out-Null }
      $i++
      continue
    }
    if ($blockC) {
      if ($ch -eq '*' -and $i + 1 -lt $Text.Length -and $Text[$i + 1] -eq '/') {
        $sb.Append('  ') | Out-Null; $i += 2; $blockC = $false
      } else {
        if ($ch -eq "`n" -or $ch -eq "`r") { $sb.Append($ch) | Out-Null } else { $sb.Append(' ') | Out-Null }
        $i++
      }
      continue
    }
    if ($inStr) {
      $sb.Append($ch) | Out-Null
      if ($esc) { $esc = $false }
      elseif ($ch -eq '\') { $esc = $true }
      elseif ($ch -eq '"') { $inStr = $false }
      $i++
      continue
    }
    if ($ch -eq '"') { $inStr = $true; $sb.Append($ch) | Out-Null; $i++; continue }
    if ($ch -eq '/' -and $i + 1 -lt $Text.Length -and $Text[$i + 1] -eq '/') {
      $lineC = $true; $sb.Append('  ') | Out-Null; $i += 2; continue
    }
    if ($ch -eq '/' -and $i + 1 -lt $Text.Length -and $Text[$i + 1] -eq '*') {
      $blockC = $true; $sb.Append('  ') | Out-Null; $i += 2; continue
    }
    $sb.Append($ch) | Out-Null
    $i++
  }
  if ($blockC) { throw 'JSON 注释未闭合（有 /* 块注释没有对应 */）' }
  return $sb.ToString()
}

function Read-WatchdogConfig {
  # 看门狗专用配置文件（默认 watchdog.json，与 config.json 同目录）。
  # 支持 // 与 /* */ 注释（与 config.json 同一套 Strip-JsonComments 语义）。
  # 只认以下键，都可整段省略，省略即用内置默认值：
  #   selector                      代理组候选：字符串或字符串数组，按顺序取第一个存在的
  #                                 type=Selector 组（决定在哪个组里换节点）
  #   subscription_uid              profile uid；空字符串 = 自动取 profiles.yaml 的 current:
  #   max_candidates                单轮候选上限（1-300）
  #   exclude_contains              子串排除：节点名含任一子串即丢弃（整体替换内置默认值；
  #                                 [] = 关闭子串排除；内置精确排除表不受此键影响）
  #   deprioritize.contains         地区降权关键词（整体替换内置默认值；[] = 关闭）
  #   deprioritize.penalty_ms       降权罚分毫秒（0 = 关闭降权）
  #   transport_confirm_rounds      transport 连续确认轮数（1-10，默认 2；等价命令行
  #                                 -TransportConfirmRounds，命令行显式传参时以命令行为准）
  # 优先级：显式命令行参数 > 配置文件 > 内置默认值。配置文件只接管没在命令行显式传过的项，
  # 所以临时 -DeprioritizeContains 试一把仍然压过文件，改完不用先改文件。
  # 解析失败一律抛错中止启动，绝不静默回退默认值：悄悄丢掉排除规则的后果是
  # 把节点切到本该避开的地区/占位节点上，比直接报错危险得多。
  param([string]$Path)
  $cfg = $null
  $existed = $false
  if (-not [string]::IsNullOrWhiteSpace($Path) -and (Test-Path -LiteralPath $Path)) {
    $existed = $true
    $raw = Get-Content -LiteralPath $Path -Raw -Encoding utf8
    # 空/纯空白文件（编辑器新建、清空、复制中断）在 PS7 下 ConvertFrom-Json 不抛错
    # 而是返回 $null，会静默走“全用内置默认值”——直接拒绝，别让人以为规则已加载。
    if ([string]::IsNullOrWhiteSpace($raw)) {
      throw "看门狗配置文件 [$Path] 是空文件：请写入 {} 或删除该文件（空文件会退回内置默认值）"
    }
    try {
      $cfg = $raw | ConvertFrom-Json -Depth 10
    } catch {
      # config.json 支持 // 与 /* */ 注释，先原样解析，失败再去注释重试。
      try {
        $cfg = Strip-JsonComments -Text $raw | ConvertFrom-Json -Depth 10
      } catch {
        throw "看门狗配置文件 [$Path] 解析失败：$($_.Exception.Message)"
      }
    }
    if (-not (Test-JsonObjectShape $cfg)) {
      throw "看门狗配置文件 [$Path] 顶层必须是 JSON 对象（如 {`"exclude_contains`": [...], `"deprioritize`": {`"contains`": [...], `"penalty_ms`": 500}, `"transport_confirm_rounds`": 2}），实际是 $(Get-ShapeName $cfg)"
    }
    # 未知键直接报错（与 config.json 的 DisallowUnknownFields 同口径）。键名拼错
    # （exclude-contains / penalty / 尾随空格）若静默忽略，看门狗会照常以内置默认值
    # 跑起来，用户却以为自己的排除/降权规则生效了。
    $knownTop = @('selector', 'subscription_uid', 'max_candidates', 'exclude_contains', 'deprioritize',
      'transport_confirm_rounds', 'proxy_check_mode', 'proxy_check_interval_seconds',
      'proxy_check_recheck_seconds', 'proxy_check_timeout_seconds', 'proxy_check_udp_target',
      'proxy_check_require_udp', 'proxy_check_failover', 'proxy_switch_gap_seconds',
      'proxy_failover_budget_seconds', 'warp_exit_failure')
    $unknownTop = @(@($cfg.PSObject.Properties | ForEach-Object { $_.Name }) |
      Where-Object { $knownTop -notcontains $_ })
    if ($unknownTop.Count -gt 0) {
      throw "看门狗配置文件 [$Path] 含未知键：$($unknownTop -join ' / ')，只支持 $($knownTop -join '、')"
    }
  }
  if (-not $existed) { Write-WdLog "未找到看门狗配置文件 [$Path]，子串排除、地区降权与 transport 连续确认轮数均用内置默认值" 'INFO'; return }
  Write-WdLog "已解析看门狗配置文件：$Path"

  # 三个计数分开记，别混用一个：
  #   $seen     文件里识别到的叶子键个数（exclude_contains / deprioritize.contains /
  #             deprioritize.penalty_ms / transport_confirm_rounds）
  #   $covered  其中被显式命令行参数覆盖、文件值故意不生效的键个数
  #   $applied  其中真正被应用的键个数
  # 分开记才能在「文件里的键全被命令行覆盖」时不说出「空对象 {}：全部使用内置默认值」
  # 这种既不成立、又与上一行 INFO 自相矛盾的假话（deprioritize 的覆盖分支同样会踩到）。
  $seen = 0
  $covered = 0
  $applied = 0
  if (Test-HasKey $cfg 'selector') {
    $seen++
    $rawSel = Get-Prop $cfg 'selector'
    # 接受字符串（单个组名）或字符串数组（候选列表，按顺序取第一个存在的）。
    # 数组是 2026-10-24 加的：本地节点池与渔云订阅的组名不同，写死单个名字换 profile 就 404。
    $selList = @()
    if ($rawSel -is [string]) {
      if (-not [string]::IsNullOrWhiteSpace($rawSel)) { $selList = @($rawSel.Trim()) }
    } elseif ($rawSel -is [System.Array]) {
      $idx = 0
      foreach ($s in @($rawSel)) {
        $idx++
        if ($s -isnot [string]) {
          $tn = if ($null -eq $s) { 'null' } else { $s.GetType().Name }
          throw "看门狗配置 selector 第 $idx 项不是字符串（$tn）：候选列表必须是字符串数组"
        }
        $t = $s.Trim()
        if ($t -ne '') { $selList += $t }
      }
    }
    # 与命令行同一套归一化：允许 ["a","b"]，也允许 "a,b" 这种单字符串写法。
    $selList = @($selList | ForEach-Object { ([string]$_) -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' })
    if (@($selList).Count -eq 0) {
      throw "看门狗配置 selector 非法：[$rawSel]（必须是非空字符串，或字符串数组，如 [`"🚀 节点选择`", `"渔云CloudFisher`"]）"
    }
    if ($script:ExplicitParams -contains 'Selector') {
      $covered++
      Write-WdLog '命令行已显式传 -Selector，配置文件的 selector 不生效' 'INFO'
    } else {
      $script:SelectorCandidates = @($selList)
      $script:Selector = $selList[0]
      $applied++
      Write-WdLog "配置生效：selector 候选 = $(@($selList) -join ' → ')（取第一个存在的）"
    }
  }
  if (Test-HasKey $cfg 'subscription_uid') {
    $seen++
    $rawUid = Get-Prop $cfg 'subscription_uid'
    if (-not ($rawUid -is [string])) {
      throw "看门狗配置 subscription_uid 非法：[$rawUid]（必须是字符串；留空 '' 表示自动取 profiles.yaml 的 current:）"
    }
    $uid = ([string]$rawUid).Trim()
    if ($uid -ne '' -and $uid -notmatch '^[A-Za-z0-9_-]+$') {
      throw "看门狗配置 subscription_uid 非法：[$uid]（只允许字母数字下划线短横，或留空表示自动）"
    }
    if ($script:ExplicitParams -contains 'SubscriptionUid') {
      $covered++
      Write-WdLog '命令行已显式传 -SubscriptionUid，配置文件的 subscription_uid 不生效' 'INFO'
    } else {
      $script:SubscriptionUid = $uid
      $applied++
      if ($uid -eq '') { Write-WdLog '配置生效：subscription_uid 为空 → 自动取当前激活 profile' }
      else { Write-WdLog "配置生效：subscription_uid = $uid" }
    }
  }
  if (Test-HasKey $cfg 'max_candidates') {
    $seen++
    $rawMax = Get-Prop $cfg 'max_candidates'
    # 与 transport_confirm_rounds 同一口径：只收 JSON 整数，字符串 "120" / 浮点 120.0 一律拒。
    if (-not ($rawMax -is [int] -or $rawMax -is [long])) {
      $typeName = if ($null -eq $rawMax) { 'null' } else { $rawMax.GetType().Name }
      throw "看门狗配置 max_candidates 非法：[$rawMax]（类型 $typeName；必须是 1-300 的整数，不要加引号）"
    }
    # 区间必须与命令行的 ValidateRange(1,300) 一致，否则文件能写出命令行拒绝的组合。
    if ($rawMax -lt 1 -or $rawMax -gt 300) { throw "看门狗配置 max_candidates 越界：[$rawMax]（应为 1-300）" }
    if ($script:ExplicitParams -contains 'MaxCandidates') {
      $covered++
      Write-WdLog '命令行已显式传 -MaxCandidates，配置文件的 max_candidates 不生效' 'INFO'
    } else {
      $script:MaxCandidates = [int]$rawMax
      $applied++
      Write-WdLog "配置生效：max_candidates = $($script:MaxCandidates)"
    }
  }
  if (Test-HasKey $cfg 'exclude_contains') {
    $seen++
    $list = ConvertTo-StrList (Get-Prop $cfg 'exclude_contains') 'exclude_contains'
    $script:ExcludeContains = @($list)
    $applied++
    if (@($list).Count -eq 0) { Write-WdLog '配置 exclude_contains 为空数组：子串排除已关闭' 'WARN' }
    else { Write-WdLog "配置生效：子串排除 = $(@($list) -join ' / ')" }
  }
  if (Test-HasKey $cfg 'deprioritize') {
    $dep = Get-Prop $cfg 'deprioritize'
    if (-not (Test-JsonObjectShape $dep)) {
      throw "看门狗配置 deprioritize 必须是对象（{`"contains`": [...], `"penalty_ms`": 500}），实际是 $(Get-ShapeName $dep)"
    }
    $unknownDep = @(@($dep.PSObject.Properties | ForEach-Object { $_.Name }) |
      Where-Object { @('contains', 'penalty_ms') -notcontains $_ })
    if ($unknownDep.Count -gt 0) {
      throw "看门狗配置 deprioritize 含未知键：$($unknownDep -join ' / ')，只支持 contains、penalty_ms"
    }
    if (Test-HasKey $dep 'contains') {
      $seen++
      if ($script:ExplicitParams -contains 'DeprioritizeContains') {
        $covered++
        Write-WdLog '命令行已显式传 -DeprioritizeContains，配置文件的 deprioritize.contains 不生效' 'INFO'
      } else {
        $list = ConvertTo-StrList (Get-Prop $dep 'contains') 'deprioritize.contains'
        $script:DeprioritizeContains = @($list)
        $applied++
        if (@($list).Count -eq 0) { Write-WdLog '配置 deprioritize.contains 为空数组：地区降权已关闭' 'WARN' }
        else { Write-WdLog "配置生效：地区降权关键词 = $(@($list) -join ' / ')" }
      }
    }
    if (Test-HasKey $dep 'penalty_ms') {
      $seen++
      if ($script:ExplicitParams -contains 'DeprioritizePenaltyMs') {
        $covered++
        Write-WdLog '命令行已显式传 -DeprioritizePenaltyMs，配置文件的 deprioritize.penalty_ms 不生效' 'INFO'
      } else {
        $rawPenalty = Get-Prop $dep 'penalty_ms'
        $n = 0
        if (-not [int]::TryParse([string]$rawPenalty, [ref]$n)) { throw "看门狗配置 deprioritize.penalty_ms 非法：[$rawPenalty]（应为 0-60000 的整数）" }
        if ($n -lt 0 -or $n -gt 60000) { throw "看门狗配置 deprioritize.penalty_ms 越界：[$rawPenalty]（应为 0-60000）" }
        $script:DeprioritizePenaltyMs = $n
        $applied++
        Write-WdLog "配置生效：地区降权罚分 = ${n}ms"
      }
    }
  }
  if (Test-HasKey $cfg 'transport_confirm_rounds') {
    $seen++
    $rawRounds = Get-Prop $cfg 'transport_confirm_rounds'
    # 值必须是整数类型：字符串 "3"、浮点 2.0、指数 1e1 都会被静默强转成别的值
    # （1e1 → 10），那正是「校验从严、解析失败一律报错中止启动」要防的事。
    # JSON 整数解析成 int/int64；其他一律拒。
    if (-not ($rawRounds -is [int] -or $rawRounds -is [long])) {
      $typeName = if ($null -eq $rawRounds) { 'null' } else { $rawRounds.GetType().Name }
      throw "看门狗配置 transport_confirm_rounds 非法：[$rawRounds]（类型 $typeName；必须是 1-10 的整数，不要加引号、不要写小数）"
    }
    # 区间必须与命令行的 ValidateRange(1,10) 一致，否则文件能写出命令行拒绝的组合。
    if ($rawRounds -lt 1 -or $rawRounds -gt 10) { throw "看门狗配置 transport_confirm_rounds 越界：[$rawRounds]（应为 1-10）" }
    $n = [int]$rawRounds
    if ($script:ExplicitParams -contains 'TransportConfirmRounds') {
      $covered++
      Write-WdLog '命令行已显式传 -TransportConfirmRounds，配置文件的 transport_confirm_rounds 不生效' 'INFO'
    } else {
      $script:TransportConfirmRounds = $n
      $applied++
      Write-WdLog "配置生效：transport 连续确认轮数 = ${n}"
    }
  }
  # ---- WARP proxy 检查（Phase 1）新增键：类型/范围/枚举从严，写错启动即失败 ----
  if (Test-HasKey $cfg 'proxy_check_mode') {
    $seen++
    $rawMode = Get-Prop $cfg 'proxy_check_mode'
    if (-not ($rawMode -is [string])) {
      $tn = if ($null -eq $rawMode) { 'null' } else { $rawMode.GetType().Name }
      throw "看门狗配置 proxy_check_mode 非法：[$rawMode]（类型 $tn；必须是 auto / off / force）"
    }
    $modeVal = ([string]$rawMode).Trim().ToLowerInvariant()
    if (@('auto', 'off', 'force') -notcontains $modeVal) {
      throw "看门狗配置 proxy_check_mode 非法：[$rawMode]（必须是 auto / off / force）"
    }
    if ($script:ExplicitParams -contains 'ProxyCheckMode') {
      $covered++
      Write-WdLog '命令行已显式传 -ProxyCheckMode，配置文件的 proxy_check_mode 不生效' 'INFO'
    } else {
      $script:ProxyCheckMode = $modeVal
      $applied++
      Write-WdLog "配置生效：proxy_check_mode = $modeVal"
    }
  }
  foreach ($intKey in @(
      @{ Key = 'proxy_check_interval_seconds'; Min = 5; Max = 300; Explicit = 'ProxyCheckIntervalSeconds'; Field = 'ProxyCheckIntervalSeconds' },
      @{ Key = 'proxy_check_recheck_seconds'; Min = 0; Max = 30; Explicit = 'ProxyCheckRecheckSeconds'; Field = 'ProxyCheckRecheckSeconds' },
      @{ Key = 'proxy_check_timeout_seconds'; Min = 3; Max = 30; Explicit = 'ProxyCheckTimeoutSeconds'; Field = 'ProxyCheckTimeoutSeconds' },
      @{ Key = 'proxy_switch_gap_seconds'; Min = 0; Max = 30; Explicit = 'ProxySwitchGapSeconds'; Field = 'ProxySwitchGapSeconds' },
      @{ Key = 'proxy_failover_budget_seconds'; Min = 30; Max = 600; Explicit = 'ProxyFailoverBudgetSeconds'; Field = 'ProxyFailoverBudgetSeconds' }
    )) {
    if (-not (Test-HasKey $cfg $intKey.Key)) { continue }
    $seen++
    $rawInt = Get-Prop $cfg $intKey.Key
    if (-not ($rawInt -is [int] -or $rawInt -is [long])) {
      $tn = if ($null -eq $rawInt) { 'null' } else { $rawInt.GetType().Name }
      throw "看门狗配置 $($intKey.Key) 非法：[$rawInt]（类型 $tn；必须是 $($intKey.Min)-$($intKey.Max) 的整数，不要加引号、不要写小数）"
    }
    if ($rawInt -lt $intKey.Min -or $rawInt -gt $intKey.Max) {
      throw "看门狗配置 $($intKey.Key) 越界：[$rawInt]（应为 $($intKey.Min)-$($intKey.Max)）"
    }
    if ($script:ExplicitParams -contains $intKey.Explicit) {
      $covered++
      Write-WdLog "命令行已显式传 -$($intKey.Explicit)，配置文件的 $($intKey.Key) 不生效" 'INFO'
    } else {
      Set-Variable -Name ('script:' + $intKey.Field) -Value ([int]$rawInt)
      $applied++
      Write-WdLog "配置生效：$($intKey.Key) = $([int]$rawInt)"
    }
  }
  if (Test-HasKey $cfg 'proxy_check_udp_target') {
    $seen++
    $rawUdp = Get-Prop $cfg 'proxy_check_udp_target'
    if (-not ($rawUdp -is [string]) -or [string]::IsNullOrWhiteSpace([string]$rawUdp)) {
      throw "看门狗配置 proxy_check_udp_target 非法：[$rawUdp]（必须是非空字符串，如 1.1.1.1:53 或 [2606:4700:4700::1111]:53）"
    }
    $udpText = ([string]$rawUdp).Trim()
    $udpParsed = Test-HostPortTarget -Text $udpText
    if (-not $udpParsed.Ok) {
      throw "看门狗配置 proxy_check_udp_target 非法：[$udpText]（$($udpParsed.Error)；应为 host:port 或 [IPv6]:port）"
    }
    if ($script:ExplicitParams -contains 'ProxyCheckUdpTarget') {
      $covered++
      Write-WdLog '命令行已显式传 -ProxyCheckUdpTarget，配置文件的 proxy_check_udp_target 不生效' 'INFO'
    } else {
      $script:ProxyCheckUdpTarget = $udpText
      $applied++
      Write-WdLog "配置生效：proxy_check_udp_target = $udpText"
    }
  }
  if (Test-HasKey $cfg 'proxy_check_require_udp') {
    $seen++
    $rawReq = Get-Prop $cfg 'proxy_check_require_udp'
    if (-not ($rawReq -is [bool])) {
      $tn = if ($null -eq $rawReq) { 'null' } else { $rawReq.GetType().Name }
      throw "看门狗配置 proxy_check_require_udp 非法：[$rawReq]（类型 $tn；必须是 true/false，不要加引号）"
    }
    if ($script:ExplicitParams -contains 'ProxyCheckRequireUdp') {
      $covered++
      Write-WdLog '命令行已显式传 -ProxyCheckRequireUdp，配置文件的 proxy_check_require_udp 不生效' 'INFO'
    } else {
      $script:ProxyCheckRequireUdp = [bool]$rawReq
      $applied++
      Write-WdLog "配置生效：proxy_check_require_udp = $([bool]$rawReq)"
    }
  }
  if (Test-HasKey $cfg 'proxy_check_failover') {
    $seen++
    $rawFo = Get-Prop $cfg 'proxy_check_failover'
    if (-not ($rawFo -is [string])) {
      $tn = if ($null -eq $rawFo) { 'null' } else { $rawFo.GetType().Name }
      throw "看门狗配置 proxy_check_failover 非法：[$rawFo]（类型 $tn；必须是 auto / on / off）"
    }
    $foVal = ([string]$rawFo).Trim().ToLowerInvariant()
    if (@('auto', 'on', 'off') -notcontains $foVal) {
      throw "看门狗配置 proxy_check_failover 非法：[$rawFo]（必须是 auto / on / off）"
    }
    if ($script:ExplicitParams -contains 'ProxyCheckFailover') {
      $covered++
      Write-WdLog '命令行已显式传 -ProxyCheckFailover，配置文件的 proxy_check_failover 不生效' 'INFO'
    } else {
      $script:ProxyCheckFailover = $foVal
      $applied++
      Write-WdLog "配置生效：proxy_check_failover = $foVal"
    }
  }
  # ---- WARP 出口失败触发（2026-10-26）新增块：类型/范围/枚举从严，写错启动即失败 ----
  if (Test-HasKey $cfg 'warp_exit_failure') {
    $wef = Get-Prop $cfg 'warp_exit_failure'
    if (-not (Test-JsonObjectShape $wef)) {
      throw ('看门狗配置 warp_exit_failure 必须是对象（{"enabled": true, "threshold": 9, "cooldown_minutes": 15, "reset_rounds": 2, "action": "refresh_then_switch"}），实际是 ' + (Get-ShapeName $wef))
    }
    $knownWef = @('enabled', 'threshold', 'cooldown_minutes', 'reset_rounds', 'action')
    $unknownWef = @(@($wef.PSObject.Properties | ForEach-Object { $_.Name }) |
      Where-Object { $knownWef -notcontains $_ })
    if ($unknownWef.Count -gt 0) {
      throw "看门狗配置 warp_exit_failure 含未知键：$($unknownWef -join ' / ')，只支持 $($knownWef -join '、')"
    }
    if (Test-HasKey $wef 'enabled') {
      $seen++
      $rawWefEnabled = Get-Prop $wef 'enabled'
      if (-not ($rawWefEnabled -is [bool])) {
        $tn = if ($null -eq $rawWefEnabled) { 'null' } else { $rawWefEnabled.GetType().Name }
        throw "看门狗配置 warp_exit_failure.enabled 非法：[$rawWefEnabled]（类型 $tn；必须是 true/false，不要加引号）"
      }
      if ($script:ExplicitParams -contains 'WarpExitFailureEnabled') {
        $covered++
        Write-WdLog '命令行已显式传 -WarpExitFailureEnabled，配置文件的 warp_exit_failure.enabled 不生效' 'INFO'
      } else {
        $script:WarpExitFailureEnabled = [bool]$rawWefEnabled
        $applied++
        Write-WdLog "配置生效：warp_exit_failure.enabled = $([bool]$rawWefEnabled)"
      }
    }
    foreach ($wefInt in @(
        @{ Key = 'threshold'; Min = 1; Max = 100; Explicit = 'WarpExitFailureThreshold'; Field = 'WarpExitFailureThreshold' },
        @{ Key = 'cooldown_minutes'; Min = 0; Max = 120; Explicit = 'WarpExitFailureCooldownMinutes'; Field = 'WarpExitFailureCooldownMinutes' },
        @{ Key = 'reset_rounds'; Min = 1; Max = 10; Explicit = 'WarpExitFailureResetRounds'; Field = 'WarpExitFailureResetRounds' }
      )) {
      if (-not (Test-HasKey $wef $wefInt.Key)) { continue }
      $seen++
      $rawWefInt = Get-Prop $wef $wefInt.Key
      if (-not ($rawWefInt -is [int] -or $rawWefInt -is [long])) {
        $tn = if ($null -eq $rawWefInt) { 'null' } else { $rawWefInt.GetType().Name }
        throw "看门狗配置 warp_exit_failure.$($wefInt.Key) 非法：[$rawWefInt]（类型 $tn；必须是 $($wefInt.Min)-$($wefInt.Max) 的整数，不要加引号、不要写小数）"
      }
      if ($rawWefInt -lt $wefInt.Min -or $rawWefInt -gt $wefInt.Max) {
        throw "看门狗配置 warp_exit_failure.$($wefInt.Key) 越界：[$rawWefInt]（应为 $($wefInt.Min)-$($wefInt.Max)）"
      }
      if ($script:ExplicitParams -contains $wefInt.Explicit) {
        $covered++
        Write-WdLog "命令行已显式传 -$($wefInt.Explicit)，配置文件的 warp_exit_failure.$($wefInt.Key) 不生效" 'INFO'
      } else {
        Set-Variable -Name ('script:' + $wefInt.Field) -Value ([int]$rawWefInt)
        $applied++
        Write-WdLog "配置生效：warp_exit_failure.$($wefInt.Key) = $([int]$rawWefInt)"
      }
    }
    if (Test-HasKey $wef 'action') {
      $seen++
      $rawWefAction = Get-Prop $wef 'action'
      if (-not ($rawWefAction -is [string])) {
        $tn = if ($null -eq $rawWefAction) { 'null' } else { $rawWefAction.GetType().Name }
        throw "看门狗配置 warp_exit_failure.action 非法：[$rawWefAction]（类型 $tn；必须是 refresh_only / refresh_then_switch / switch_only）"
      }
      $wefActionVal = ([string]$rawWefAction).Trim().ToLowerInvariant()
      if (@('refresh_only', 'refresh_then_switch', 'switch_only') -notcontains $wefActionVal) {
        throw "看门狗配置 warp_exit_failure.action 非法：[$rawWefAction]（必须是 refresh_only / refresh_then_switch / switch_only）"
      }
      if ($script:ExplicitParams -contains 'WarpExitFailureAction') {
        $covered++
        Write-WdLog '命令行已显式传 -WarpExitFailureAction，配置文件的 warp_exit_failure.action 不生效' 'INFO'
      } else {
        $script:WarpExitFailureAction = $wefActionVal
        $applied++
        Write-WdLog "配置生效：warp_exit_failure.action = $wefActionVal"
      }
    }
  }
  # {} 是合法的“不改任何默认值”，但很容易是误建/误清空后的残留，单独提示一句。
  # 注意别把「键全被命令行覆盖」也说成「空对象 {}」——那与上一行的 INFO 自相矛盾。
  if ($applied -eq 0) {
    if ($covered -gt 0) {
      Write-WdLog "配置文件的 $covered 个键被显式命令行参数覆盖，未应用任何文件值（生效值取自命令行）" 'INFO'
    } else {
      Write-WdLog '配置文件已解析但未应用任何键（等同于空对象 {}）：全部使用内置默认值' 'WARN'
    }
  }
}

function Resolve-GatewayKey {
  if ($script:GatewayKey -ne '' -and $script:GatewayKey -ne $null) { return $script:GatewayKey }
  $p = Join-Path $PSScriptRoot 'config.json'
  if (-not (Test-Path -LiteralPath $p)) { throw "找不到网关 config.json：$p，请用 -GatewayKey 传入" }
  $raw = Get-Content -LiteralPath $p -Raw -Encoding utf8
  try {
    $cfg = $raw | ConvertFrom-Json -Depth 10
  } catch {
    # config.json 支持 // 与 /* */ 注释（config.go:stripJSONComments），先去注释再解析。
    $cfg = Strip-JsonComments -Text $raw | ConvertFrom-Json -Depth 10
  }
  if ($cfg.server_keys -eq $null -or @($cfg.server_keys).Count -eq 0) { throw 'config.json 里 server_keys 为空' }
  $script:GatewayKey = [string]@($cfg.server_keys)[0]
  return $script:GatewayKey
}

function Get-HttpErrorInfo {
  param($Err)
  $status = $null
  $body = ''
  $retryAfter = ''
  try {
    if ($Err.ErrorDetails -ne $null -and $Err.ErrorDetails.Message -ne $null -and $Err.ErrorDetails.Message -ne '') {
      $body = [string]$Err.ErrorDetails.Message
    }
  } catch { }
  try {
    $resp = $Err.Exception.Response
    if ($resp -ne $null) {
      try { $status = [int]$resp.StatusCode } catch { }
      try {
        # 网关冷却 429 带 Retry-After（本轮把匿名冷却封顶压到 15s，这个值通常就是
        # 它），拿它决定「退避早退后等多久再巡检」，比固定 RoundCooldownMinutes 贴合得多。
        $hdrs = Get-Prop $resp 'Headers'
        if ($hdrs -ne $null) {
          $vals = $null
          if ($hdrs -is [System.Net.Http.Headers.HttpHeaders]) {
            if ($hdrs.TryGetValues('Retry-After', [ref]$vals)) { $retryAfter = (@($vals) -join ',') }
          } else {
            $raw = $hdrs['Retry-After']
            if ($raw -ne $null) { $retryAfter = [string](@($raw) -join ',') }
          }
        }
      } catch { }
      if ($body -eq '') {
        # pwsh7 的 Response 是 HttpResponseMessage（无 GetResponseStream），优先读 Content。
        try {
          $content = Get-Prop $resp 'Content'
          if ($content -ne $null) {
            $task = $content.ReadAsStringAsync()
            if ($task -ne $null) { $body = [string]$task.GetAwaiter().GetResult() }
          }
        } catch { }
        if ($body -eq '') {
          try {
            $stream = $resp.GetResponseStream()
            if ($stream -ne $null) {
              $reader = New-Object IO.StreamReader($stream, [Text.Encoding]::UTF8)
              $body = $reader.ReadToEnd()
              $reader.Close()
            }
          } catch { }
        }
      }
    }
  } catch { }
  return @{ Status = $status; Body = $body; RetryAfter = $retryAfter }
}

function Invoke-ClashGet {
  param([string]$Path)
  $uri = $script:ClashBase + $Path
  return Invoke-RestMethod -Uri $uri -Headers @{ Authorization = "Bearer $(Resolve-ClashSecret)" } -TimeoutSec 15 -UseBasicParsing
}

function Invoke-ClashPut {
  param([string]$Path, [string]$Json)
  $uri = $script:ClashBase + $Path
  $bytes = [Text.Encoding]::UTF8.GetBytes($Json)
  $r = Invoke-WebRequest -Uri $uri -Method Put -Headers @{ Authorization = "Bearer $(Resolve-ClashSecret)" } `
    -Body $bytes -ContentType 'application/json; charset=utf-8' -TimeoutSec 15 -UseBasicParsing
  return [int]$r.StatusCode
}

function Get-WatchdogStatus {
  # 脚本专用只读接口：与 /v1/models 同样的 server_keys 鉴权，只返回计数与
  # 脱敏原因 verdict（transport/quota 才切，ratelimit/cooling/ok 只等）。
  # 密钥只放内存，从不打印不落盘；不再靠 /healthz 猜，不再打确认探针。
  $key = Resolve-GatewayKey
  $uri = $script:GatewayBase + '/v1/watchdog/status'
  return Invoke-RestMethod -Uri $uri -Headers @{ Authorization = "Bearer $key" } -TimeoutSec 10 -UseBasicParsing
}

function ConvertTo-WarpExitCounter {
  # /healthz 的计数只接受 JSON 整数；null / 缺失 / 字符串 / 对象一律按 0 处理。
  # 直接 [int64]$null 得 0，但 [int64]'abc' 会抛、[int64]$true 会得 1，所以先判类型。
  # 小数不能四舍五入：8.6 被转成 9 会越过 threshold 误触发；非有限值同样按 0。
  param($Value)
  if ($null -eq $Value) { return [int64]0 }
  if ($Value -is [bool]) { return [int64]0 }
  if ($Value -is [int] -or $Value -is [long]) { return [int64]$Value }
  if ($Value -is [double]) {
    $d = [double]$Value
    if ([double]::IsNaN($d) -or [double]::IsInfinity($d) -or $d -ne [Math]::Truncate($d)) { return [int64]0 }
    try { return [int64]$d } catch { return [int64]0 }
  }
  if ($Value -is [decimal]) {
    $dec = [decimal]$Value
    if ($dec -ne [decimal]::Truncate($dec)) { return [int64]0 }
    try { return [int64]$dec } catch { return [int64]0 }
  }
  return [int64]0
}

function ConvertTo-WarpExitTimestamp {
  # ConvertFrom-Json 会把 RFC3339 字符串自动转成 [datetime]；直接 [string] 会变成
  # 本地化格式（如 01/01/2026 00:00:00），既不便于日志比对，也丢掉时区信息。
  # 统一回 RFC3339（UTC，带小数秒）；非日期字符串原样返回，null 返回空串。
  param($Value)
  if ($null -eq $Value) { return '' }
  if ($Value -is [DateTime]) { return $Value.ToUniversalTime().ToString('o') }
  if ($Value -is [DateTimeOffset]) { return $Value.ToUniversalTime().ToString('o') }
  return [string]$Value
}

function Set-WarpExitHealthFailure {
  # /healthz 失败：统一打一条 WARN、记录签名，并退避 60s。同一签名 60s 内只打一次，
  # 网关没起时不会每轮巡检都刷日志、也不会每轮都白等 5s 超时。
  param([string]$Signature, [string]$Message)
  $now = Get-Date
  $script:WarpExitHealthBackoffUntil = $now.AddSeconds(60)
  if ($script:WarpExitHealthErrSig -ne $Signature -or
    $script:WarpExitHealthErrLogAt -eq [DateTime]::MinValue -or
    ($now - $script:WarpExitHealthErrLogAt).TotalSeconds -ge 60) {
    $script:WarpExitHealthErrSig = $Signature
    $script:WarpExitHealthErrLogAt = $now
    Write-WdLog ('WARP 出口健康检查失败：' + (Shorten (Mask-Text $Message) 200) + '（60s 内不再重试）') 'WARN'
  }
}

function Get-GatewayHealthz {
  # 只读 GET /healthz（无需鉴权）。warp.active==0 时网关回 503，但正文仍是合法 JSON，
  # 所以用 pwsh 7 的 -SkipHttpErrorCheck 直接拿正文，不把 503 当异常。
  # 5s 超时；失败后 60s 退避（见 Set-WarpExitHealthFailure）。返回的字段全部经
  # Get-Prop/Test-HasKey 安全读取，缺失/为 null 的计数按 0 处理。
  param([switch]$Force)
  $now = Get-Date
  if (-not $Force -and $now -lt $script:WarpExitHealthBackoffUntil) {
    return @{ Ok = $false; Skipped = $true; HasWarp = $false; Error = 'backoff' }
  }
  $uri = $script:GatewayBase + '/healthz'
  $resp = $null
  try {
    $resp = Invoke-WebRequest -Uri $uri -Method Get -TimeoutSec 5 -UseBasicParsing -SkipHttpErrorCheck
  } catch {
    $info = Get-HttpErrorInfo $_
    $statusTxt = ''
    try { $statusTxt = [string](Get-Prop $info 'Status') } catch { }
    $msg = 'GET /healthz 失败'
    if ($statusTxt -ne '' -and $statusTxt -ne '0') { $msg += "（HTTP $statusTxt）" }
    $msg += '：' + (Mask-Text $_.Exception.Message)
    Set-WarpExitHealthFailure -Signature ('healthz|' + $statusTxt + '|' + [string]$_.Exception.GetType().Name) -Message $msg
    return @{ Ok = $false; Skipped = $false; HasWarp = $false; Error = $msg; Status = 0 }
  }
  $status = 0
  try { $status = [int]$resp.StatusCode } catch { $status = 0 }
  $content = $resp.Content
  if ($content -is [byte[]]) { $content = [Text.Encoding]::UTF8.GetString($content) }
  $content = [string]$content
  if ($status -ne 200 -and $status -ne 503) {
    $msg = "GET /healthz 返回 HTTP $status（只接受 200/503）"
    Set-WarpExitHealthFailure -Signature ('healthz|status|' + $status) -Message $msg
    return @{ Ok = $false; Skipped = $false; HasWarp = $false; Error = $msg; Status = $status }
  }
  $doc = $null
  try {
    $doc = $content | ConvertFrom-Json -Depth 10 -ErrorAction Stop
  } catch {
    $msg = "GET /healthz 返回的不是 JSON：$(Shorten (Mask-Text $content) 120)"
    Set-WarpExitHealthFailure -Signature ('healthz|json|' + [string]$_.Exception.Message) -Message $msg
    return @{ Ok = $false; Skipped = $false; HasWarp = $false; Error = $msg; Status = $status }
  }
  if (-not (Test-JsonObjectShape $doc)) {
    $msg = "GET /healthz 顶层不是 JSON 对象（$(Get-ShapeName $doc)）"
    Set-WarpExitHealthFailure -Signature 'healthz|shape' -Message $msg
    return @{ Ok = $false; Skipped = $false; HasWarp = $false; Error = $msg; Status = $status }
  }
  # 解析成功：清掉退避与错误签名。
  $script:WarpExitHealthBackoffUntil = [DateTime]::MinValue
  $script:WarpExitHealthErrSig = ''
  $hasWarp = $false
  $warp = $null
  if (Test-HasKey $doc 'warp') {
    $warp = Get-Prop $doc 'warp'
    if (Test-JsonObjectShape $warp) { $hasWarp = $true }
    else { $warp = $null }
  }
  $out = @{
    Ok = $true; Skipped = $false; Status = $status; HasWarp = $hasWarp; Warp = $warp; Error = ''
    StartedAt = ''; LastSuccessAt = ''; LastFailureAt = ''; LastFailureKind = ''
    FailureStreak = [int64]0; FailuresTotal = [int64]0; CreateFailuresTotal = [int64]0
    HasFailureStreak = $false; WarpStatus = ''; WarpActive = [int64]0
    WarpStandby = [int64]0; WarpSize = [int64]0; WarpCooling = [int64]0
  }
  if ($hasWarp) {
    $out.StartedAt = ConvertTo-WarpExitTimestamp (Get-Prop $warp 'started_at')
    $out.LastSuccessAt = ConvertTo-WarpExitTimestamp (Get-Prop $warp 'last_success_at')
    $out.LastFailureAt = ConvertTo-WarpExitTimestamp (Get-Prop $warp 'last_failure_at')
    $out.LastFailureKind = [string](Get-Prop $warp 'last_failure_kind')
    $out.HasFailureStreak = Test-HasKey $warp 'failure_streak'
    $out.FailureStreak = ConvertTo-WarpExitCounter (Get-Prop $warp 'failure_streak')
    $out.FailuresTotal = ConvertTo-WarpExitCounter (Get-Prop $warp 'failures_total')
    $out.CreateFailuresTotal = ConvertTo-WarpExitCounter (Get-Prop $warp 'create_failures_total')
    $out.WarpStatus = [string](Get-Prop $warp 'status')
    $out.WarpActive = ConvertTo-WarpExitCounter (Get-Prop $warp 'active')
    $out.WarpStandby = ConvertTo-WarpExitCounter (Get-Prop $warp 'standby')
    $out.WarpSize = ConvertTo-WarpExitCounter (Get-Prop $warp 'size')
    $out.WarpCooling = ConvertTo-WarpExitCounter (Get-Prop $warp 'cooling')
  }
  return $out
}

function Test-HasFlagEmoji {
  # 不用 \p{IsRegional_Indicator}（.NET 抛 Unknown property，已实测），改用代理项区间判断。
  param([string]$Name)
  try {
    for ($i = 0; $i -lt $Name.Length - 1; $i++) {
      $hi = [int]$Name[$i]
      $lo = [int]$Name[$i + 1]
      if ($hi -eq 0xD83C -and $lo -ge 0xDDE6 -and $lo -le 0xDDFF) { return $true }
    }
  } catch { }
  return $false
}

function Test-NodeEligible {
  param([string]$Name, [ref]$DropReason)
  if ([string]::IsNullOrWhiteSpace($Name)) { if ($DropReason) { $DropReason.Value = '空名' }; return $false }
  foreach ($e in $script:ExcludeExact) { if ($Name -eq $e) { if ($DropReason) { $DropReason.Value = "精确排除[$e]" }; return $false } }
  foreach ($s in $script:ExcludeContains) { if ($Name.IndexOf($s, [StringComparison]::OrdinalIgnoreCase) -ge 0) { if ($DropReason) { $DropReason.Value = "子串排除[$s]" }; return $false } }
  foreach ($c in $script:CountryHints) { if ($Name.IndexOf($c, [StringComparison]::OrdinalIgnoreCase) -ge 0) { return $true } }
  if (Test-HasFlagEmoji -Name $Name) { return $true }
  # 允许名单之外的名字（如 Gemini解锁节点、VLESS-专线01）：保留为兜底候选，
  # 排在有延迟的国家节点之后（见 DelayRank 回退），避免候选池无声缩小。
  if ($DropReason) { $DropReason.Value = '' }
  return $true
}

function Get-ProxyMap {
  $root = Invoke-ClashGet -Path '/proxies'
  $inner = Get-Prop $root 'proxies'
  if ($inner -ne $null) { return $inner }
  return $root
}

function Get-RankedCandidates {
  # -IgnoreQuotaPenalty：WARP proxy failover 用（design §7.3）。WARP 模式下 QuotaFailedAt
  # 是 legacy 的旧降权记忆，不应继承；legacy 调用不传，默认行为完全不变。
  param($ProxyMap, [string[]]$SelectorAll, [string]$ExcludeNow, [switch]$IgnoreQuotaPenalty)
  $cands = @()
  $dropped = @()
  if ($ProxyMap -eq $null) { return @() }
  $props = @()
  try { $props = @($ProxyMap.PSObject.Properties) } catch { return @() }
  foreach ($prop in $props) {
    $name = $prop.Name
    if ($SelectorAll -ne $null -and @($SelectorAll) -notcontains $name) { continue }
    if ($name -eq $ExcludeNow) { continue }
    $node = $prop.Value
    # 非叶子组按 type 排除：组名换成「♻️ 自动选择」「🇺🇸 美国节点」都拦得住。
    $nodeType = ''
    try { $nodeType = [string](Get-Prop $node 'type') } catch { }
    if ($script:GroupTypes -contains $nodeType) { $dropped += "$name（非叶子组 $nodeType）"; continue }
    $reason = ''
    if (-not (Test-NodeEligible $name ([ref]$reason))) {
      if ($reason -ne '') { $dropped += "$name（$reason）" }
      continue
    }
    $alive = $false
    try { $alive = [bool](Get-Prop $node 'alive') } catch { $alive = $false }
    $delay = -1
    $hist = Get-Prop $node 'history'
    if ($hist -ne $null) {
      $arr = @($hist)
      if ($arr.Count -gt 0) {
        $last = $arr[$arr.Count - 1]
        try { $delay = [int](Get-Prop $last 'delay') } catch { $delay = -1 }
      }
    }
    $rank = 999999
    if ($alive -and $delay -gt 0) { $rank = $delay }
    $named = $false
    foreach ($c in $script:CountryHints) { if ($name.IndexOf($c, [StringComparison]::OrdinalIgnoreCase) -ge 0) { $named = $true; break } }
    if (-not $named -and (Test-HasFlagEmoji -Name $name)) { $named = $true }
    # 无国家名的兜底候选排最后（延迟再低也不抢国家节点前面），
    # 但仍优先于已死节点：alive 且有延迟的兜底 rank≈900050 < dead 的 999999。
    # 无延迟的兜底（rank=999999）与已死国家节点同级、按名字排序，可能先试死
    # 节点——可接受：无延迟本就代表 Clash 尚未测速/不可达，排后是保守选择。
    if (-not $named) { $rank = 900000 + ($rank % 100000) }
    $cands += (New-Object PSObject -Property @{ Name = $name; Alive = $alive; Delay = $delay; DelayRank = $rank; Demoted = $false; RegionDemoted = $false })
  }
  if (@($dropped).Count -gt 0) {
    # 同一集合每轮都打 WARN 是噪音：只在丢弃集合变化时打一条 INFO，日常巡检只在切换轮触发本函数，影响很小。
    $dropSig = (@($dropped | Sort-Object) -join '|')
    if ($script:LastDropSignature -ne $dropSig) {
      $script:LastDropSignature = $dropSig
      Write-WdLog "过滤丢弃 $(@($dropped).Count) 个：$(($dropped | Select-Object -First 8) -join '；')" 'INFO'
    }
  } else {
    $script:LastDropSignature = ''
  }
  # 近期 quota 失败降权（加法罚分，非硬跳过）：同出口 IP 段额度烧尽时，几分钟前刚
  # quota 失败的同名节点大概率仍失败，罚 300000 使其排到正常 alive 节点之后；
  # 钳位 999998 保证永不超过死亡线 999999（无国家名的兜底 alive 本就 900050 起，
  # 不钳会被已死节点反超）；记忆过期自动失效（该机场同名背后会换 IP，不可永久拉黑）。
  # 顺带清理过期条目，避免哈希表随节点改名无限增长（候选池本身只有几十个，影响可忽略）。
  # 窗口为 0 表示关闭：直接跳过本块（记录点同样守卫，见 Invoke-Failover）。
  if (-not $IgnoreQuotaPenalty) {
    $nowTs = Get-Date
    $demotedNames = @()
    foreach ($k in @($script:QuotaFailedAt.Keys)) {
      try { if (($nowTs - $script:QuotaFailedAt[$k]).TotalMinutes -ge $script:QuotaFailMemoryMinutes) { $script:QuotaFailedAt.Remove($k) } } catch { }
    }
    if ($script:QuotaFailMemoryMinutes -gt 0) {
      foreach ($c in $cands) {
        try {
          if ([string]::IsNullOrWhiteSpace($c.Name)) { continue }
          if ($script:QuotaFailedAt.Contains($c.Name)) {
            if (($nowTs - $script:QuotaFailedAt[$c.Name]).TotalMinutes -lt $script:QuotaFailMemoryMinutes) {
              $c.DelayRank = [Math]::Min($c.DelayRank + 300000, 999998)
              $c.Demoted = $true
              $demotedNames += $c.Name
            }
          }
        } catch { }
      }
    }
    if (@($demotedNames).Count -gt 0) {
      Write-WdLog "其中 $(@($demotedNames).Count) 个近期 quota 失败已降权（记忆窗口 $($script:QuotaFailMemoryMinutes) 分钟）：$(($demotedNames | Select-Object -First 8) -join ' / ')" 'INFO'
    }
  }
  # 地区降权（延迟低但不稳定，如新加坡）：命中子串的候选 DelayRank 加罚分，只降
  # 排序不排除；与 quota 降权可叠加（同钳 999998，永不反超死亡线 999999，保底仍
  # 可选）；已死/无延迟节点已在底，加罚无意义直接跳过。
  # 名单先过滤空白条目再计数：全空白列表视为关闭（日志与匹配同口径，不会出现
  # “日志称已启用但实际零命中”的分叉）。
  $regionKeys = @($script:DeprioritizeContains | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
  $regionDemotedNames = @()
  if ($script:DeprioritizePenaltyMs -gt 0 -and @($regionKeys).Count -gt 0) {
    foreach ($c in $cands) {
      try {
        if ([string]::IsNullOrWhiteSpace($c.Name)) { continue }
        if ($c.DelayRank -ge 999999) { continue }
        foreach ($s in $regionKeys) {
          if ([string]::IsNullOrWhiteSpace($s)) { continue }
          if ($c.Name.IndexOf($s, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            $c.DelayRank = [Math]::Min($c.DelayRank + $script:DeprioritizePenaltyMs, 999998)
            $c.RegionDemoted = $true
            $regionDemotedNames += $c.Name
            break
          }
        }
      } catch { }
    }
  }
  if (@($regionDemotedNames).Count -gt 0) {
    Write-WdLog "其中 $(@($regionDemotedNames).Count) 个命中地区降权（$($regionKeys -join '/')+$($script:DeprioritizePenaltyMs)ms）：$(($regionDemotedNames | Select-Object -First 8) -join ' / ')" 'INFO'
  }
  return @($cands | Sort-Object -Property DelayRank, Name)
}

function Get-DemoteMark {
  # 候选降权标记（quota / 地区 / 双降权），DryRun 排名与切换日志共用。
  param($c)
  try {
    $q = $false
    $r = $false
    try { $q = [bool](Get-Prop $c 'Demoted') } catch { }
    try { $r = [bool](Get-Prop $c 'RegionDemoted') } catch { }
    if ($q -and $r) { return '（quota+地区降权中）' }
    if ($q) { return '（quota降权中）' }
    if ($r) { return '（地区降权中）' }
  } catch { }
  return ''
}

function ConvertFrom-WatchdogProbe {
  # 探针端点的 verdict 直接就是网关自己的分类（与 /v1/watchdog/status 同一词表，
  # 同一套 classifyAnonymousVerdict），脚本不再对正文做子串判定：
  #   ok          → 该出口 IP 的匿名通道可用（真发上游拿到了 2xx）
  #   quota       → 该出口 IP 拒绝为这次匿名请求服务：额度烧尽（FreeUsageLimit/
  #                 BlackUsageLimit 类），或免费层的门拒绝了它（FreeTierError，
  #                 含只有 message 文案的变体）→ 换节点，新节点再报同一个错就继续换
  #   transport   → 出口路由坏（真发了上游，传输层失败）→ 换节点
  #   ratelimit   → 普通限流，等
  #   fingerprint → 上游拒了一个换节点解不开的东西：现在只有「探测回退」才会给出
  #                 这个 verdict（403 且正文里没有任何可认的类，例如 WAF 挑战页；
  #                 see probeFallbackVerdict）——网关状态流里免费层门已经改判 quota
  #   config      → 网关侧状态（模型不在目录/未启用匿名/模型被禁用）→ 停轮
  #   cooling     → 强制探针被短路了，这是网关 bug，必须记 ERROR 而不是当退避早退
  # attempted 与 cause 只进日志：上游错误正文一律不回传（回传就得重新做子串判定，
  # 也就重新绑回网关对正文里 opencode2api: 前缀的中和逻辑）。
  # 参数刻意不加 [string]：加了就会在绑定期把 byte[] 变成 "System.Byte[]"，
  # 正好毁掉下面那次显式解码。Content-Type 不保证是 JSON 时 pwsh 的 .Content 是
  # byte[]，直接字符串化会静默退化成 other（= 白白换节点）。
  param($Content)
  if ($Content -is [byte[]]) { $Content = [Text.Encoding]::UTF8.GetString($Content) }
  $Content = [string]$Content
  $verdict = ''
  $cause = ''
  $transport = ''
  $attempted = $false
  # 拿到 verdict 时这才是上游真答的状态码；解析失败退回 0（= 没有上游应答）。
  # 千万别丢：所有失败日志都打 status=，硬编码 200 会把「上游 429」打成一串
  # 自相矛盾的 status=200 class=quota，而排障第一眼要看的恰恰是这个。
  $status = 0
  try {
    $doc = ConvertFrom-Json $Content -ErrorAction Stop
    $status = [int](Get-Prop $doc 'status')
    $verdict = [string](Get-Prop $doc 'verdict')
    $cause = [string](Get-Prop $doc 'cause')
    $transport = [string](Get-Prop $doc 'transport')
    $attempted = [bool](Get-Prop $doc 'attempted')
  } catch {
    Write-WdLog "探针响应不是 JSON，无法分类：$(Shorten $Content 200)" 'ERROR'
  }
  if ([string]::IsNullOrWhiteSpace($verdict)) { $verdict = 'other' }
  $class = switch ($verdict) {
    'ok' { 'ok' }
    'quota' { 'quota' }
    'transport' { 'transport' }
    'ratelimit' { 'ratelimit' }
    'fingerprint' { 'fingerprint' }
    'cooling' { 'cooling' }
    'config' { 'config' }
    default { 'other' }
  }
  $body = "attempted=$attempted status=$status verdict=$verdict cause=$cause transport=$transport"
  return @{ Ok = ($verdict -eq 'ok'); Status = $status; Class = $class; Body = $body; Backoff = $false; RetryAfter = 0; Verdict = $verdict }
}

function Invoke-WatchdogProbe {
  # 网关专用探针端点 POST /v1/watchdog/probe。与普通推理请求的三点差别，正是
  # 看门狗需要的：
  #   1) 保证发出——忽略代理传输健康闸门与任何冷却。切完节点后要回答的是「这个
  #      出口 IP 现在到底答什么」，而不是「网关这一秒愿不愿意发」；旧探针在这里
  #      会拿到本地合成的 502（attempts=0），而 502 又被判成 transport，于是整池
  #      候选被烧在一个换节点根本解不开的闸门上。
  #   2) 不写池状态——不动冷却/健康/last_success/last_failure。旧探针每次都会给
  #      用户流量重新点着一次冷却（quota 轮那两段 verdict 复查就是在跟它搏斗）。
  #   3) 机器可读——verdict 字段即判据，正文不回传。
  param([string]$Nonce, [int]$TimeoutSec)
  $key = Resolve-GatewayKey
  $uri = $script:GatewayBase + '/v1/watchdog/probe'
  $payload = @{ model = $script:VerifyModel; nonce = $Nonce } | ConvertTo-Json -Compress
  $bytes = [Text.Encoding]::UTF8.GetBytes($payload)
  try {
    $r = Invoke-WebRequest -Uri $uri -Method Post -Headers @{ Authorization = "Bearer $key" } `
      -Body $bytes -ContentType 'application/json; charset=utf-8' -TimeoutSec $TimeoutSec -UseBasicParsing
    return ConvertFrom-WatchdogProbe $r.Content
  } catch {
    $info = Get-HttpErrorInfo $_
    $status = [int](Get-Prop $info 'Status')
    if ($status -eq 404) {
      # 老网关没有这个端点：永久回落旧探针，不必每轮再试一次。
      $script:ProbeSupported = $false
      Write-WdLog '网关没有 /v1/watchdog/probe（404），本进程回落旧探针 /v1/chat/completions（重启看门狗或升级网关后恢复）' 'WARN'
      return Test-GatewayNodeLegacy -Nonce $Nonce -TimeoutSec $TimeoutSec
    }
    # 探针端点只要拿到 verdict 就回 200；这里的非 200 是网关自己拒绝了这个请求
    # （鉴权 401、参数 400）或网关异常 5xx，按旧口径分类：4xx 配置错、5xx 传输侧。
    $class = 'other'
    if ($status -eq 401 -or $status -eq 403 -or $status -eq 404 -or $status -eq 400) { $class = 'config' }
    elseif ($status -ge 500) { $class = 'transport' }
    # Verdict 一并给空串：两条返回路径的键集保持一致，省得调用方以后点访问踩坑。
    return @{ Ok = $false; Status = $status; Class = $class; Body = (Shorten ([string](Get-Prop $info 'Body')) 200); Backoff = $false; RetryAfter = 0; Verdict = '' }
  }
}

# Test-GatewayNodeLegacy 是切换探针之前的实现，逐字保留：它仍要在 -ProbeEndpoint
# legacy 下可用，也是探针端点 404 时的回落目标。注意它区分不了「网关没发」和
# 「发了被拒」——两者都是 502 all upstream attempts failed。
function Test-FreeTierGateMessage {
  # 网关的免费层拒绝文案（"OpenCode's free tier can only be used from within
  # OpenCode."）。上游把它放在嵌套 error.type=FreeTierError 里；顶层信封变体
  # （{"code":null,"message":...,"type":"upstream_error"}）没有这个类，只能靠文案认。
  # 锚点集合与网关侧 watchdog.go 的 freeTierGateText **逐字一致**（两个锚点、同样
  # 大小写不敏感），两边增删锚点必须同步——只差一个锚点就会出现「Go 判门、脚本判
  # config」的分叉，那正是本次要消灭的失效模式（例如尾句改写成
  # "...can only be used from the official client." 时只有一个锚点必然漏判）。
  # **但作用域比网关宽**：脚本这里只有整份响应正文（没有解析后的 JSON 文档），所以
  # 任何位置出现该文案都命中——detail 字段、或「显式类 + 门文案」并存的正文（网关
  # 侧此时返回显式类，不会走到文案兜底）、以及非 JSON 的正文（例如恰好含这句话的
  # WAF 页面）。这就是 legacy 路径的残余差异：只会把更多 403 判成 quota（方向是
  # 「多换节点」），不会反向漏判；别再写成「一一对应」或「判据完全相同」。
  param([string]$Body)
  if ([string]::IsNullOrEmpty($Body)) { return $false }
  foreach ($gate in @('can only be used from within OpenCode', 'free tier can only be used')) {
    if ($Body.IndexOf($gate, [StringComparison]::OrdinalIgnoreCase) -ge 0) { return $true }
  }
  return $false
}

function Test-GatewayNodeLegacy {
  param([string]$Nonce, [int]$TimeoutSec)
  $key = Resolve-GatewayKey
  $uri = $script:GatewayBase + '/v1/chat/completions'
  $payload = @{
    model      = $script:VerifyModel
    messages   = @(@{ role = 'user'; content = "ping-verify-$Nonce" })
    stream     = $false
    max_tokens = 16
  } | ConvertTo-Json -Depth 5
  $bytes = [Text.Encoding]::UTF8.GetBytes($payload)
  try {
    $r = Invoke-WebRequest -Uri $uri -Method Post -Headers @{ Authorization = "Bearer $key" } `
      -Body $bytes -ContentType 'application/json; charset=utf-8' -TimeoutSec $TimeoutSec -UseBasicParsing
    return @{ Ok = $true; Status = [int]$r.StatusCode; Class = 'ok'; Body = ''; Backoff = $false; RetryAfter = 0 }
  } catch {
    $info = Get-HttpErrorInfo $_
    # Get-HttpErrorInfo 永远返回 @{Status;Body}（Body 缺失时为空串），这里点访问恒安全。
    $body = [string]$info.Body
    # 401/403/404/400 原则上是网关配置错误（key/模型名/参数），节点没问题，
    # 绝不切换；但带免费层门文案的 403 是上游的免费层拒绝（可恢复），必须摘出，
    # 否则一次门拒绝就会误判 config、中止整轮（22:39/22:41/23:12 三轮只试
    # 1~2 个候选即停即是此误杀）。
    $class = 'other'
    # Backoff=true 表示「网关自己说在退避」而不是「这个节点有问题」。**前缀是判据的
    # 必要条件，不能只看 429**：copyErrorResponse 只保留上游的 error.message 并把
    # type 改写成 upstream_error，所以真上游 quota 429 透传给客户端的正文里既没有
    # opencode2api: 也没有 FreeUsageLimit；网关冷却 429 则两个都有。只按状态码判会
    # 把「真限流」误认成「网关退避」，结果 quota 轮里一个候选都不切、QuotaFailedAt 也
    # 不记——而「按出口 IP 换节点逃 quota」正是免费层唯一有效的自救。
    $backoff = ($info.Status -eq 429 -and $body.Contains('opencode2api:'))
    # 网关冷却 429 优先于配额关键字：它描述的是网关状态，不是这个出口 IP 额度。
    # 免费层 403（两个变体：嵌套 error.type=FreeTierError，或只有 message 文案）判 quota，
    # 与网关 classifyAnonymousVerdict 现在的语义一致（2026-10-17 起 FreeTierError→quota）：
    # quota 不是 config，候选循环不会 break，新节点再报同一个错就继续换下一个。
    # 必须排在 config 之前：两者都是 403，顺序反了门拒绝又会把整轮停掉。
    # class 不只用于日志：候选验证处按 `Class -eq 'quota'` 写 $script:QuotaFailedAt
    # （见 Invoke-Failover），而 Get-RankedCandidates 据此给同名候选 +300000ms 的
    # **排序**罚分（钳位 999998，只降权不排除，保底仍可选；记忆窗口
    # -QuotaFailMemoryMinutes 后自动失效，绝不永久拉黑）。本次改动首次让「免费层门
    # 403」走到这条分支：同轮里连续几次门 403 会把刚试过的候选降权，下一轮不再优先
    # 重试它们。除此之外的分支先后不影响流程（切换决策仍只看服务端 verdict）。
    if ($info.Status -eq 429 -and $body.Contains('opencode2api:')) { $class = 'cooling' }
    elseif ($info.Status -eq 403 -and ($body.Contains('FreeTierError') -or (Test-FreeTierGateMessage $body))) { $class = 'quota' }
    elseif ($info.Status -eq 401 -or $info.Status -eq 403 -or $info.Status -eq 404 -or $info.Status -eq 400) { $class = 'config' }
    # 503 ModelUnavailable（模型被可用性探针禁用）同样是网关侧目录状态，与出口 IP
    # 无关：换节点只会拿到同一个 503。不摘出的话它会掉进下面的 502/503/504 → transport
    # 分支，逐候选切一遍再回滚——改动前它答 400，走的正是上面那条 config（立即停轮）。
    elseif ($info.Status -eq 503 -and $body.Contains('ModelUnavailable')) { $class = 'config' }
    # 2026-10-23：网关新增两条 503 —— free_tier_ip_blocked（出口 IP 被免费层判滥用）
    # 与 free_tier_lane_paused（lane 级熔断暂停）。两者都是**网关侧状态**，不摘出来
    # 就会掉进下面的 503 → transport，逐候选切一遍再回滚 —— 正是本地 2026-10-20 修过的
    # 「把网关退避当节点坏了」那个坑。
    #   - ip_blocked：换出口 IP 是唯一解法，但 Clash 换节点未必换 IP，且同轮里可能真能
    #     换到干净出口，所以按 quota 记（只降权、不 break，候选循环继续）。
    #   - lane_paused：窗口内换任何节点都无效，按 cooling 只等，不刷新不切换。
    elseif ($info.Status -eq 503 -and $body.Contains('free_tier_lane_paused')) { $class = 'cooling' }
    elseif ($info.Status -eq 503 -and $body.Contains('free_tier_ip_blocked')) { $class = 'quota' }
    # 2026-10-25：第 4 条自撰 503 —— warp_unavailable（WARP 开着但一个出口都拿不到，
    # 请求根本没到上游）。WARP 流量不经过 Clash，换候选节点对它无解；而且这是网关侧/
    # 主机侧状态（出口池起不来、UDP 到不了 Cloudflare 端点），一轮之内不会自愈，按
    # config 停轮。主探针端点在 WARP 开启时网关直接答 config（不真发），此分支只保护
    # legacy 探针（-ProbeEndpoint legacy / 端点 404 回落）那条真发请求的路径。
    elseif ($info.Status -eq 503 -and $body.Contains('warp_unavailable')) { $class = 'config' }
    elseif ($body.Contains('FreeUsageLimit') -or $body.Contains('BlackUsageLimit')) { $class = 'quota' }
    elseif ($info.Status -eq 502 -or $info.Status -eq 503 -or $info.Status -eq 504 -or $body.Contains('gateway_transport')) {
      $class = 'transport'
    }
    $raSecs = 0
    # 必须取 $info.RetryAfter：$retryAfter 是 Get-HttpErrorInfo 内部的局部变量，
    # 在这里恒为 $null。pwsh 里 $null -ne '' 为 True 会让分支照进，但 TryParse('')
    # 恒 False，于是 raSecs 永远是 0，静默期退回固定 RoundCooldownMinutes(2min)
    # 而不是网关给的 15s 窗口。
    $ra = [string]$info.RetryAfter
    if ($ra -ne '') {
      $parsed = 0
      if ([int]::TryParse(($ra -split ',')[0].Trim(), [ref]$parsed) -and $parsed -gt 0) { $raSecs = $parsed }
    }
    return @{ Ok = $false; Status = $info.Status; Class = $class; Body = $body; Backoff = $backoff; RetryAfter = $raSecs }
  }
}

function Test-GatewayNode {
  # 统一入口：能用探针端点就用（保证真发上游 + 机器可读 verdict），否则回落旧探针。
  param([string]$Nonce, [int]$TimeoutSec)
  if ($script:ProbeSupported) { return Invoke-WatchdogProbe -Nonce $Nonce -TimeoutSec $TimeoutSec }
  return Test-GatewayNodeLegacy -Nonce $Nonce -TimeoutSec $TimeoutSec
}

function Wait-GatewayProxyReady {
  # 切完节点、真正打探针之前，最多等 ProxyReadySeconds 秒，让网关把代理传输健康
  # 闸门恢复（/v1/watchdog/status 的 proxies.healthy 回到 proxies.total 即立刻继续，
  # 不等满）。只在闸门确实关着时花时间。
  #
  # 这一项**不是**探针能发出去的前提——探针会穿透闸门真发上游；它只是少让一次
  # 「切换后首次拨号超过 connect_timeout 被判坏」的瞬时状态白烧一个候选和一次额度。
  # 返回 $true 表示「可以打探针了」（含放弃等待）：探针无论如何都会真发，
  # 所以这里永远不该阻塞切换流程。
  param([int]$TimeoutSec)
  if ($TimeoutSec -le 0) { return $true }
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  do {
    $healthy = 0
    $total = 0
    try {
      $wd = Get-WatchdogStatus
      $prox = Get-Prop $wd 'proxies'
      $total = [int](Get-Prop $prox 'total')
      $healthy = [int](Get-Prop $prox 'healthy')
    } catch {
      return $true
    }
    # 读不到代理数据（total<=0）与「读不到 status」同等对待：等下去也不会变好，
    # 而探针反正会穿透闸门真发上游，所以立刻放行，绝不阻塞切换流程。
    if ($total -le 0) { return $true }
    if ($healthy -ge $total) { return $true }
    Start-Sleep -Milliseconds 700
  } while ((Get-Date) -lt $deadline)
  Write-WdLog "代理传输健康闸门 $TimeoutSec s 内未恢复（healthy/total 未齐），继续打探针：探针会穿透闸门真发上游" 'INFO'
  return $true
}

function Update-SelectedNow {
  param([string]$Name, [string]$Uid)
  $profilesYaml = Join-Path $script:VergeRoot 'profiles.yaml'
  $text = [IO.File]::ReadAllText($profilesYaml, [Text.Encoding]::UTF8)
  $eol = "`n"
  if ($text.Contains("`r`n")) { $eol = "`r`n" }
  $start = $text.IndexOf("- uid: $Uid")
  if ($start -lt 0) { $start = $text.IndexOf("uid: $Uid") }
  if ($start -lt 0) { throw "profiles.yaml 里找不到 uid: $Uid" }
  $next = $text.IndexOf("$eol- uid:", $start)
  if ($next -lt 0) { $next = $text.Length }
  $block = $text.Substring($start, $next - $start)
  $safe = $Name
  if ($safe -match '[:#''"]|^\s|\s$') { $safe = "'" + ($safe -replace "'", "''") + "'" }
  # uid 块下可能有多个 proxy-group 各带一个 now:（Verge 常见），必须锚定到
  # “- name: <Selector>”之后、下一个“- name:”之前的 now:，不能取块内第一个。
  $selName = [regex]::Escape($script:Selector)
  $m = [regex]::Match($block, "(?ms)^\s*-\s*name:\s*['""]?$selName['""]?\s*\r?\n(.*?)^\s*now:\s*(.*?)(\r?)$")
  if ($m.Success) {
    $old = $m.Groups[2].Value.Trim()
    $nowStart = $m.Groups[2].Index - $m.Index
    $nowLen = $m.Groups[2].Length
    $newBlock = $block.Substring(0, $m.Index + $nowStart) + $safe + $block.Substring($m.Index + $nowStart + $nowLen)
    $logMsg = "profiles.yaml selected 已更新：$old -> now: $Name"
  } else {
    # 没有本组的 now: 行。分两种情况，绝不能盲目补一块 selected:——
    # Verge 用 serde 解析 profiles.yaml，同一个 profile 里出现两个 selected: 键会报
    # duplicate field，整份 profiles.yaml 加载失败（所有 profile 一起丢），而且我们
    # 是 WriteAllText 直接落盘、不会抛异常，属于静默写坏。
    $selLit = $script:Selector
    if ($selLit -match '[:#''"]|^\s|\s$') { $selLit = "'" + ($selLit -replace "'", "''") + "'" }
    # 行尾用 lookahead 判界、不吃 \r：CRLF 文件里 `[ \t]*$` 撞上 \r 会整行匹配失败，
    # 于是掉到第三层再补一块 selected:，写出重复键。rest 用来识别 [] / 行尾注释等形态。
    $selKey = [regex]::Match($block, '(?m)^(?<ind>[ \t]*)selected:(?<rest>[^\r\n]*?)[ \t]*(?=\r?$)')
    if ($selKey.Success) {
      $ind = $selKey.Groups['ind'].Value
      $rest = $selKey.Groups['rest'].Value.Trim()
      if ($rest -ne '' -and $rest -ne '[]') {
        # 例如 `selected: # 注释` 或 flow 写法：不认识就别动，宁可拒绝也不写重复键。
        throw "profiles.yaml 的 selected: 不是空列表也不是 block 列表（[$(Shorten $rest 80)]），拒绝改写"
      }
      # 已有 selected:（例如先在 Verge UI 里切过别的组）→ 在列表头部追加一项。
      # 插在 `selected:` 行之后即可：indentless sequence 允许第一项紧跟 key。
      # 若紧跟的下一项用了别的缩进（手写文件），跟随它，避免同列表缩进不一致。
      $nextItem = [regex]::Match($block.Substring($selKey.Index + $selKey.Length), '^\r?\n(?<ii>[ \t]*)- ')
      $itemInd = if ($nextItem.Success) { $nextItem.Groups['ii'].Value } else { $ind }
      $itemText = $eol + $itemInd + '- name: ' + $selLit + $eol + $itemInd + '  now: ' + $safe
      if ($rest -eq '[]') {
        # selected: [] → 展开成列表（连 [] 一起替换，否则会写成 `selected: []- name:`）
        $replacement = $ind + 'selected:'
        $newBlock = $block.Substring(0, $selKey.Index) + $replacement + $itemText + $block.Substring($selKey.Index + $selKey.Length)
      } else {
        $insertAt = $selKey.Index + $selKey.Length
        $newBlock = $block.Substring(0, $insertAt) + $itemText + $block.Substring($insertAt)
      }
      $logMsg = "profiles.yaml 的 selected 已有其他组，追加 $($script:Selector) -> now: $Name"
    } else {
      # 连 selected: 都没有（本地配置首次导入时 Verge 还没写过）→ 在 updated: 后补一块。
      # 写法跟随 Verge：selected: 与 - name: 同级（indentless sequence），缩进沿用
      # 块内 updated: 的缩进。行尾用 lookahead 判界、不吃 \r：否则 CRLF 文件要么匹配
      # 不上（[ \t]*$ 撞 \r），要么多吃一个 \r 再拼 $eol 而多出空行。
      $anchor = [regex]::Match($block, '(?m)^(?<ind>[ \t]*)updated:[ \t]*\d+[ \t]*(?=\r?$)')
      $ind = '  '
      if ($anchor.Success) {
        $ind = $anchor.Groups['ind'].Value
        $insertAt = $anchor.Index + $anchor.Length
      } else {
        if (-not $block.EndsWith($eol)) { $block += $eol }
        $insertAt = $block.Length
      }
      $insert = $eol + $ind + 'selected:' + $eol + $ind + '- name: ' + $selLit + $eol + $ind + '  now: ' + $safe
      $newBlock = $block.Substring(0, $insertAt) + $insert + $block.Substring($insertAt)
      $logMsg = "profiles.yaml 尚无 selected 块，已补写：$($script:Selector) -> now: $Name"
    }
  }
  $newText = $text.Substring(0, $start) + $newBlock + $text.Substring($next)
  # 先写临时文件再原子替换，避免中途崩溃撕裂 profiles.yaml。
  $tmp = "$profilesYaml.tmp"
  [IO.File]::WriteAllText($tmp, $newText, (New-Object Text.UTF8Encoding $false))
  Move-Item -LiteralPath $tmp -Destination $profilesYaml -Force
  Write-WdLog $logMsg
}

function Switch-ClashNode {
  param([string]$Name, [string]$Uid)
  $sel = [uri]::EscapeDataString($script:Selector)
  $json = (@{ name = $Name } | ConvertTo-Json -Compress)
  $code = Invoke-ClashPut -Path "/proxies/$sel" -Json $json
  if ($code -ge 200 -and $code -lt 300) {
    # 切换成功必须 DELETE 全量连接（副作用：非网关流量也被掐断一次）：网关到
    # Clash 的 keep-alive 连接会钉住旧出口路由，不掐则随后的 big-pickle 验证
    # 可能仍走旧路由，把可用的新节点误判为坏节点。失败候选不单独删——下一次
    # 切换成功时会连带清理；回滚同样走本函数，同样清理。
    try {
      Invoke-WebRequest -Uri ($script:ClashBase + '/connections') -Method Delete `
        -Headers @{ Authorization = "Bearer $(Resolve-ClashSecret)" } -TimeoutSec 10 -UseBasicParsing | Out-Null
    } catch { }
    # 切换已成功（内核 2xx），profiles.yaml 的 selected 只是让 Verge UI/下次重载
    # 记得这个选择：它失败不能把候选判成失败、更不能中断候选循环。
    # uid 为空（启动时没解析出来）就直接跳过；uid 失效（运行中重新导入了 yaml）
    # 时按签名只报一次——否则一轮 100 个候选会刷 100 条同样的 WARN。
    if (-not [string]::IsNullOrWhiteSpace($Uid)) {
      try {
        Update-SelectedNow -Name $Name -Uid $Uid
        $script:SelectedWarnSig = ''
      } catch {
        $sig = "$Uid|$($_.Exception.Message)"
        if ($script:SelectedWarnSig -ne $sig) {
          $script:SelectedWarnSig = $sig
          Write-WdLog "已切到 [$Name]，但 profiles.yaml 的 selected 记录失败（不影响本轮切换）：$($_.Exception.Message)。若是重新导入过 yaml，下一轮会跟随新的 current: uid" 'WARN'
        }
      }
    }
    return $true
  }
  Write-WdLog "切换接口返回异常状态码：$code" 'WARN'
  return $false
}

function Get-ProfileBlock {
  param([string]$Uid)
  $profilesYaml = Join-Path $script:VergeRoot 'profiles.yaml'
  $text = [IO.File]::ReadAllText($profilesYaml, [Text.Encoding]::UTF8)
  $eol = "`n"
  if ($text.Contains("`r`n")) { $eol = "`r`n" }
  $start = $text.IndexOf("- uid: $Uid")
  if ($start -lt 0) { $start = $text.IndexOf("uid: $Uid") }
  if ($start -lt 0) { throw "profiles.yaml 里找不到 uid: $Uid" }
  $next = $text.IndexOf("$eol- uid:", $start)
  if ($next -lt 0) { $next = $text.Length }
  return @{ Path = $profilesYaml; Text = $text; Start = $start; End = $next; Block = $text.Substring($start, $next - $start) }
}

function Resolve-ActiveProfileUid {
  # 从 profiles.yaml 的 current: 读当前激活 profile 的 uid。
  # 为什么要自动解析：Clash Verge 每次「拖入本地 yaml」都会新建一个 uid（本地这份
  # opencode2api-节点.yaml 已换过好几轮），把 uid 写死在脚本里，重新导入一次就静默失联
  # （refresh 找不到 uid、selected.now 写不进去，而 selector 恰好还在的话看门狗仍会切换，
  # 只是每次切换都在后台刷一条失败日志）。留空即跟随当前激活配置，导入多少次都不用改。
  $profilesYaml = Join-Path $script:VergeRoot 'profiles.yaml'
  if (-not (Test-Path -LiteralPath $profilesYaml)) { throw "找不到 Clash Verge 的 profiles.yaml：$profilesYaml" }
  $text = [IO.File]::ReadAllText($profilesYaml, [Text.Encoding]::UTF8)
  $m = [regex]::Match($text, '(?m)^\s*current:\s*(?<u>[A-Za-z0-9_-]+)\s*$')
  if (-not $m.Success) { throw 'profiles.yaml 里没有 current: 行，无法自动定位当前激活配置；请用 -SubscriptionUid 显式指定' }
  return $m.Groups['u'].Value
}

function Resolve-SelectorCandidate {
  # 按候选顺序挑第一个在 /proxies 里存在、且 type=Selector 的组。
  # 必须限定 Selector：url-test / fallback 组的 now 由内核测速决定，PUT 手动选择会被拒；
  # 若把「链尾正在决定出口的组」当目标，本地这份配置会落到 ♻️ 自动选择（URLTest）上。
  param([string[]]$Candidates)
  $map = Get-ProxyMap
  if ($map -eq $null) { return $null }
  foreach ($c in @($Candidates)) {
    if ([string]::IsNullOrWhiteSpace($c)) { continue }
    $entry = Get-Prop $map $c
    if ($entry -eq $null) { continue }
    $t = ''
    try { $t = [string](Get-Prop $entry 'type') } catch { }
    if ($t -eq 'Selector') { return $c }
  }
  return $null
}

function Get-SelectorGroupNames {
  # 诊断用：列出当前配置里所有 Selector 组名（候选全不命中时提示用户改哪个）。
  param($ProxyMap)
  if ($ProxyMap -eq $null) { return @() }
  $out = @()
  try {
    # Get-ProxyMap 正常返回 PSCustomObject；哈希表走 IDictionary 分支（Get-Prop 同款处理）。
    $names = @()
    if ($ProxyMap -is [System.Collections.IDictionary]) { $names = @($ProxyMap.Keys) }
    else { $names = @($ProxyMap.PSObject.Properties | ForEach-Object { $_.Name }) }
    foreach ($n in $names) {
      $t = ''
      try { $t = [string](Get-Prop (Get-Prop $ProxyMap $n) 'type') } catch { }
      if ($t -eq 'Selector') { $out += $n }
    }
  } catch { }
  return @($out)
}

function Get-ProfileType {
  # profile 条目的 type: remote / local / merge / script。只用于日志与「跳过刷新」判定。
  param([string]$Block)
  $m = [regex]::Match($Block, '(?m)^\s*type:\s*(?<t>\S+)\s*$')
  if ($m.Success) { return $m.Groups['t'].Value }
  return ''
}

function Get-SubscriptionUrl {
  param([string]$Uid)
  $info = Get-ProfileBlock -Uid $Uid
  $m = [regex]::Match($info.Block, '(?m)^\s*url:\s*(\S+)\s*$')
  if (-not $m.Success) { throw "uid $Uid 不是远端订阅（无 url），无法刷新" }
  return $m.Groups[1].Value
}

function Get-NodeNames {
  # 仅统计 proxies: 节 span 内的条目；proxy-groups 里的引用（- name: 自动选择等）
  # 不是节点，计入会虚增数量、削弱 <10 写保护。
  param([string]$YamlText)
  $span = Get-SectionSpan -Text $YamlText -Key 'proxies'
  if ($span -eq $null) { return @() }
  $body = $YamlText.Substring($span.Start, $span.End - $span.Start)
  $names = @()
  foreach ($m in [regex]::Matches($body, '(?m)^\s*-\s*name:\s*(?<q>[''"]?)(?<n>.+?)\k<q>\s*$')) {
    $names += $m.Groups['n'].Value.Trim()
  }
  foreach ($m in [regex]::Matches($body, '(?m)^\s*-\s*\{\s*name:\s*(?<q>[''"]?)(?<n>.+?)\k<q>\s*[,}]')) {
    $names += $m.Groups['n'].Value.Trim()
  }
  return @($names | Sort-Object -Unique -CaseSensitive)
}

function Get-SectionSpan {
  param([string]$Text, [string]$Key)
  $secs = Get-TopSections -Text $Text
  if ($secs.Map.ContainsKey($Key)) { return $secs.Map[$Key] }
  return $null
}

function Get-TopSections {
  param([string]$Text)
  $lines = $Text -split "`n"
  $starts = @()
  for ($i = 0; $i -lt $lines.Count; $i++) {
    $m = [regex]::Match($lines[$i], '^(?<key>[A-Za-z][A-Za-z0-9_-]*):[ \t]*\r?$')
    if ($m.Success) { $starts += @{ Key = $m.Groups['key'].Value; Line = $i; Start = 0 } }
  }
  $off = 0
  $lineStart = @()
  for ($i = 0; $i -lt $lines.Count; $i++) { $lineStart += $off; $off += $lines[$i].Length + 1 }
  for ($j = 0; $j -lt $starts.Count; $j++) { $starts[$j].Start = $lineStart[$starts[$j].Line] }
  $map = @{}
  for ($i = 0; $i -lt $starts.Count; $i++) {
    $end = $Text.Length
    if ($i + 1 -lt $starts.Count) { $end = $starts[$i + 1].Start }
    $map[$starts[$i].Key] = @{ Start = $starts[$i].Start; End = $end }
  }
  return @{ Order = $starts; Map = $map }
}

function Sync-RuntimeText {
  param([string]$Runtime, [string]$Profile)
  $rt = Get-TopSections -Text $Runtime
  $pf = Get-TopSections -Text $Profile
  $rtKeys = @($rt.Order | ForEach-Object { $_.Key })
  foreach ($key in $script:SyncKeys) {
    if ($pf.Map.ContainsKey($key) -and $rtKeys -notcontains $key) {
      throw "新订阅有 [$key] 但运行配置无此节，拼接风险高，拒绝写入"
    }
  }
  $sb = New-Object Text.StringBuilder
  $pos = 0
  foreach ($entry in $rt.Order) {
    $key = $entry.Key
    if ($script:SyncKeys -notcontains $key) { continue }
    $span = $rt.Map[$key]
    $sb.Append($Runtime.Substring($pos, $span.Start - $pos)) | Out-Null
    if ($pf.Map.ContainsKey($key)) {
      $p = $pf.Map[$key]
      $sb.Append($Profile.Substring($p.Start, $p.End - $p.Start)) | Out-Null
    } else {
      # 新订阅缺该节：保留运行配置原节（常见于无 rules 的极简订阅），不崩溃。
      Write-WdLog "新订阅无 [$key] 节，保留运行配置原节" 'WARN'
      $sb.Append($Runtime.Substring($span.Start, $span.End - $span.Start)) | Out-Null
    }
    $pos = $span.End
  }
  $sb.Append($Runtime.Substring($pos)) | Out-Null
  $out = $sb.ToString()
  $after = Get-TopSections -Text $out
  foreach ($key in $script:SyncKeys) {
    if ($rt.Map.ContainsKey($key) -xor $after.Map.ContainsKey($key)) {
      throw "拼接后 [$key] 节丢失，拒绝写入"
    }
  }
  return $out
}

function Get-SubscriptionYaml {
  # 订阅下载统一走这里（正式刷新与 DryRun 共用）：先系统代理/直连，再经
  # 127.0.0.1:7890 兜底。只做下载+非空校验，不写任何文件、不耗网关配额。
  param([string]$Url, [string]$UaOverride = $null)
  $ua = $UaOverride
  if ([string]::IsNullOrWhiteSpace($ua)) { $ua = $SubscriptionUA }
  if ([string]::IsNullOrWhiteSpace($ua)) { $ua = 'clash-verge/v2.5.2' }
  Write-WdLog "订阅下载 UA：$ua（与 Verge 一致，否则机场会返回 base64 URI 列表而非 Clash YAML）"
  $resp = $null
  $errors = @()
  $attempts = @(
    @{ Label = '系统代理/直连'; Proxy = $null },
    @{ Label = '经 127.0.0.1:7890'; Proxy = 'http://127.0.0.1:7890' }
  )
  foreach ($a in $attempts) {
    try {
      $p = @{ Uri = $Url; TimeoutSec = 20; UseBasicParsing = $true; UserAgent = $ua }
      if ($a.Proxy -ne $null) { $p['Proxy'] = $a.Proxy }
      $resp = Invoke-WebRequest @p
      $len = $resp.RawContentLength
      if ($len -eq $null -or $len -lt 0) { $len = ([string]$resp.Content).Length }
      Write-WdLog "订阅下载成功（$($a.Label)，$len 字节）" 'OK'
      break
    } catch {
      $dlMsg = Mask-Text $_.Exception.Message
      try {
        # Get-HttpErrorInfo 形状固定（@{Status;Body}，Body 缺失时为空串），点访问恒安全。
        $dlInfo = Get-HttpErrorInfo $_
        if (-not [string]::IsNullOrWhiteSpace($dlInfo.Body)) { $dlMsg = "status=$($dlInfo.Status) $(Shorten (Mask-Text $dlInfo.Body) 150)" }
      } catch { }
      $errors += "$($a.Label)：$dlMsg"
    }
  }
  if ($resp -eq $null) {
    foreach ($e in $errors) { Write-WdLog "下载失败：$e" 'WARN' }
    return @{ Ok = $false; Yaml = ''; Resp = $null; Error = '订阅下载失败（直连与经 Clash 均失败）' }
  }
  $newYaml = [string]$resp.Content
  if ($newYaml -eq $null -or $newYaml.Trim() -eq '') { return @{ Ok = $false; Yaml = ''; Resp = $resp; Error = '订阅内容为空' } }
  return @{ Ok = $true; Yaml = $newYaml; Resp = $resp; Error = $null }
}

function Refresh-Subscription {
  param([string]$Uid, [string]$UaOverride = $null, [switch]$Force)
  # 本地 profile（type: local，没有 url: 行）没有可刷新的远端内容：直接跳过，不当失败。
  # 本地 yaml 是生成脚本产出的静态文件，刷新只会白跑一次「找不到订阅地址」；节点池
  # 以内核当前列表为准，逐节点切换完全不受影响。返回 Skipped 让 Invoke-Failover 记
  # INFO 而不是 ERROR，也不写失败退避（没有网络失败可言）。
  # uid 为空（启动时 profiles.yaml 还读不出来）时在这里重试解析；自动跟随的 uid 失效
  # （运行中重新导入 yaml 换了新 uid）时跟随 current:，并把新 uid 回传给本轮切换用。
  # 这一段排在节流之前：节流只该挡「下载」，不该把 uid 修正也一起挡掉。
  if ([string]::IsNullOrWhiteSpace($Uid)) {
    try {
      $Uid = Resolve-ActiveProfileUid
      $script:SubscriptionUid = $Uid
      Write-WdLog "uid 为空，已跟随当前激活配置：$Uid"
    } catch {
      return @{ Ok = $false; Skipped = $true; Error = '无法定位当前激活 profile，跳过刷新与 selected 记录' }
    }
  }
  $profileType = ''
  $hasUrl = $false
  # 自动跟随模式下每次 failover 都对一次 current:（一次小文件读，failover 频率很低）：
  # 只靠「旧 uid 消失」不够——在 Verge UI 切到另一份**已存在**的 profile 时旧 uid 还在，
  # current 却已经变了。钉死模式不做这一步。
  if ($script:UidAuto) {
    try {
      $curUid = Resolve-ActiveProfileUid
      if ($curUid -ne $Uid) {
        Write-WdLog "当前激活配置已变为 $curUid（原 $Uid），本轮跟随" 'INFO'
        $Uid = $curUid
        $script:SubscriptionUid = $curUid
      }
    } catch { }
  }
  try {
    $blk = Get-ProfileBlock -Uid $Uid
  } catch {
    if (-not $script:UidAuto) {
      # 显式钉死的 uid（CLI / watchdog.json）失效：报错，绝不悄悄改去刷新当前激活 profile。
      return @{ Ok = $false; Error = "钉死的 uid [$Uid] 在 profiles.yaml 里不存在（检查 subscription_uid / -SubscriptionUid）" }
    }
    try {
      $newUid = Resolve-ActiveProfileUid
      if ($newUid -ne $Uid) {
        Write-WdLog "profiles.yaml 里找不到原 uid [$Uid]（配置被重新导入过？），改为跟随 current=$newUid" 'WARN'
        $Uid = $newUid
        $script:SubscriptionUid = $newUid
        $blk = Get-ProfileBlock -Uid $Uid
      } else { throw }
    } catch {
      return @{ Ok = $false; Skipped = $true; Error = "profiles.yaml 里找不到 uid [$Uid]，跳过刷新" }
    }
  }
  $profileType = Get-ProfileType -Block $blk.Block
  $hasUrl = [regex]::IsMatch($blk.Block, '(?m)^\s*url:\s*\S+')
  if (-not $hasUrl) {
    # 不推进 LastRefresh：本地分支在节流判断之前就返回，打戳挡不住任何东西，反而会把
    # 「刚切到远端 profile 后的第一次刷新」无谓推迟 5 分钟。每次 failover 一条 INFO 可接受。
    Write-WdLog "当前配置 uid=$Uid 是本地配置（type=$profileType；无 url 行），跳过订阅刷新，用内核当前节点列表继续" 'INFO'
    return @{ Ok = $false; Skipped = $true; Uid = $Uid; Error = '本地配置无订阅地址，跳过刷新' }
  }
  # 远端配置才需要节流（避免每轮 failover 都下载/覆盖订阅）。
  if (-not $Force) {
    if ((Get-Date) -lt $script:LastRefresh) {
      $wait = [int]((($script:LastRefresh) - (Get-Date)).TotalSeconds)
      Write-WdLog "订阅刷新节流中（${wait}s 后可用），本次跳过刷新" 'WARN'
      return @{ Ok = $false; Error = 'throttled'; Throttled = $true }
    }
  }
  Write-WdLog "开始刷新订阅 uid=$Uid ..."
  try {
    $url = Get-SubscriptionUrl -Uid $Uid
  } catch {
    return @{ Ok = $false; Error = "读取订阅地址失败：$($_.Exception.Message)" }
  }
  Write-WdLog "订阅地址：$(Mask-Url $url)"
  $dl = Get-SubscriptionYaml -Url $url -UaOverride $UaOverride
  if (-not (Get-Prop $dl 'Ok')) {
    if ((Get-Prop $dl 'Error') -eq '订阅下载失败（直连与经 Clash 均失败）') {
      # 下载失败也给 2 分钟退避，避免每 15s 空转打机场；成功刷新是 5 分钟节流。
      $script:LastRefresh = (Get-Date).AddMinutes(2)
    }
    return @{ Ok = $false; Error = [string](Get-Prop $dl 'Error') }
  }
  $newYaml = [string](Get-Prop $dl 'Yaml')
  $resp = Get-Prop $dl 'Resp'
  if (-not ([regex]::IsMatch($newYaml, '(?m)^(proxies|proxy-providers):'))) {
    return @{ Ok = $false; Error = '订阅内容无 proxies/proxy-providers 节，拒绝写入' }
  }
  $newNames = Get-NodeNames -YamlText $newYaml
  # 纯 proxy-providers 订阅没有内联节点名可数：按提供者形态放行（候选以 Clash
  # 内核 all 列表为准），仅内联与提供者双空才拒绝。
  $hasProviders = [regex]::IsMatch($newYaml, '(?m)^proxy-providers:')
  if (@($newNames).Count -lt 10 -and -not $hasProviders) { return @{ Ok = $false; Error = "订阅节点过少（$(@($newNames).Count)），拒绝写入" } }
  if (@($newNames).Count -lt 10) { Write-WdLog '订阅内联节点不足 10 但含 proxy-providers，按提供者形态放行（节点增减预览以内核为准）' 'WARN' }

  if ($Uid -notmatch '^[A-Za-z0-9_-]+$') { return @{ Ok = $false; Error = "非法 uid [$Uid]，拒绝刷新" } }
  $vergeRoot = $script:VergeRoot
  $profileFile = Join-Path $vergeRoot "profiles\$Uid.yaml"
  $runtimeFile = Join-Path $vergeRoot 'clash-verge.yaml'
  if (-not (Test-Path -LiteralPath $profileFile)) { return @{ Ok = $false; Error = "profile 文件缺失（$profileFile），跳过刷新" } }
  if (-not (Test-Path -LiteralPath $runtimeFile)) { return @{ Ok = $false; Error = "运行配置缺失（$runtimeFile），跳过刷新" } }
  try {
    $infoProbe = Get-ProfileBlock -Uid $Uid
    if ($infoProbe -eq $null -or [string]::IsNullOrWhiteSpace($infoProbe.Block)) { throw 'empty' }
  } catch {
    return @{ Ok = $false; Error = "profiles.yaml 里找不到 uid [$Uid]，跳过刷新" }
  }
  $oldNames = @()
  if (Test-Path -LiteralPath $runtimeFile) {
    $oldNames = Get-NodeNames -YamlText ([IO.File]::ReadAllText($runtimeFile, [Text.Encoding]::UTF8))
  }
  $added = @($newNames | Where-Object { @($oldNames) -notcontains $_ })
  $removed = @($oldNames | Where-Object { @($newNames) -notcontains $_ })
  # 注意：即使差异只剩流量占位节点的改名（旧 50 → 新 50，仅“剩余流量”数值变化）
  # 也不得跳过下面的写文件+重载：该机场同名节点背后会换 IP/凭证，刷新本身即恢复手段
  # （9-18 多次 transport 都是刷新后原地复测即恢复）。只看名字判“无实质变化”是误判。
  if (@($newNames).Count -lt 10 -and $hasProviders) {
    # 提供者形态：新订阅无内联名可数，“消失 N”只是新旧统计口径不同，压住数字
    # 只留上面的 WARN 一行，候选以 Clash 内核 all 列表为准。
  } else {
    Write-WdLog "订阅节点：旧 $(@($oldNames).Count) → 新 $(@($newNames).Count)，新增 $(@($added).Count)，消失 $(@($removed).Count)"
    if (@($added).Count -gt 0) { Write-WdLog "新增：$(($added | Select-Object -First 10) -join ' / ')" }
    if (@($removed).Count -gt 0) { Write-WdLog "消失：$(($removed | Select-Object -First 10) -join ' / ')" }
  }

  $stamp = (Get-Date).ToString('yyyyMMdd-HHmmss')
  $noBom = New-Object Text.UTF8Encoding $false
  # profiles.yaml 的 extra/updated 改动延后：先做完拼接+校验，成功后才落盘，
  # 避免中间失败留下“新 epoch 配旧 profile”的不一致。
  $profilesYamlPath = Join-Path $vergeRoot 'profiles.yaml'
  $profilesYamlOld = [IO.File]::ReadAllText($profilesYamlPath, [Text.Encoding]::UTF8)
  Copy-Item -LiteralPath $profileFile -Destination "$profileFile.bak-$stamp" -Force
  Copy-Item -LiteralPath $runtimeFile -Destination "$runtimeFile.bak-$stamp" -Force
  Get-ChildItem -Path (Split-Path -Parent $profileFile) -Filter "$Uid.yaml.bak-*" |
    Sort-Object Name -Descending | Select-Object -Skip 5 | Remove-Item -Force -ErrorAction SilentlyContinue
  Get-ChildItem -Path (Split-Path -Parent $runtimeFile) -Filter 'clash-verge.yaml.bak-*' |
    Sort-Object Name -Descending | Select-Object -Skip 5 | Remove-Item -Force -ErrorAction SilentlyContinue

  # profile 与 runtime 双写：先经临时文件原子替换 profile（Update-SelectedNow
  # 同款做法），再拼接 runtime。单实例互斥已排除并发交错；进程崩溃在两写之间
  # 只会留下“新 profile + 旧 runtime”（下轮按旧列表继续，PUT 从未发生故内核
  # 与文件一致），不会撕裂单个文件；校验/重载失败走回滚。
  $profileTmp = "$profileFile.tmp"
  [IO.File]::WriteAllText($profileTmp, $newYaml, $noBom)
  Move-Item -LiteralPath $profileTmp -Destination $profileFile -Force
  Write-WdLog "profile 文件已更新：profiles\$Uid.yaml（已备份）" 'OK'

  $runtimeOld = [IO.File]::ReadAllText($runtimeFile, [Text.Encoding]::UTF8)
  $runtimeNew = $null
  try {
    $runtimeNew = Sync-RuntimeText -Runtime $runtimeOld -Profile $newYaml
  } catch {
    Copy-Item -LiteralPath "$profileFile.bak-$stamp" -Destination $profileFile -Force
    return @{ Ok = $false; Error = "运行配置拼接拒绝写入：$($_.Exception.Message)（profile 已回滚）" }
  }
  $runtimeTmp = "$runtimeFile.tmp"
  [IO.File]::WriteAllText($runtimeTmp, $runtimeNew, $noBom)
  Move-Item -LiteralPath $runtimeTmp -Destination $runtimeFile -Force
  Write-WdLog 'clash-verge.yaml 运行配置已同步（仅 proxies/groups/rules 类节）' 'OK'

  $exe = $MihomoExe
  if (-not (Test-Path -LiteralPath $exe)) {
    $proc = Get-Process -Name 'verge-mihomo' -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($proc -ne $null -and $proc.Path -ne $null -and $proc.Path -ne '') { $exe = $proc.Path }
  }
  $validateOut = ''
  if (Test-Path -LiteralPath $exe) {
    $validateOut = (& $exe -t -f $runtimeFile 2>&1 | Out-String)
    if ($LASTEXITCODE -ne 0) {
      Copy-Item -LiteralPath "$profileFile.bak-$stamp" -Destination $profileFile -Force
      Copy-Item -LiteralPath "$runtimeFile.bak-$stamp" -Destination $runtimeFile -Force
      # 全量校验错误写入独立诊断文件（只截短行打印到日志，避免 300 字截断把真正的 error 行丢掉）。
      $diagDir = Join-Path $PSScriptRoot '.tmp'
      try {
        if (-not (Test-Path -LiteralPath $diagDir)) { New-Item -ItemType Directory -Path $diagDir -Force | Out-Null }
        $diagFile = Join-Path $diagDir ("mihomo-validate-{0}.log" -f $stamp)
        [IO.File]::WriteAllText($diagFile, [string]$validateOut, (New-Object Text.UTF8Encoding $false))
        Get-ChildItem -LiteralPath $diagDir -Filter 'mihomo-validate-*.log' |
          Sort-Object Name -Descending | Select-Object -Skip 5 | Remove-Item -Force -ErrorAction SilentlyContinue
      } catch { $diagFile = '' }
      $errLine = ''
      foreach ($ln in ([string]$validateOut -split "`r?`n")) {
        if ($ln -match '(?i)\b(error|fatal|invalid|fail)') { $errLine = $ln.Trim(); break }
      }
      if ([string]::IsNullOrWhiteSpace($errLine)) { $errLine = Shorten $validateOut 300 }
      else { $errLine = Shorten $errLine 300 }
      if ([string]::IsNullOrWhiteSpace($diagFile)) { Write-WdLog "新配置校验失败（exit=$LASTEXITCODE）：$errLine" 'ERROR' }
      else { Write-WdLog "新配置校验失败（exit=$LASTEXITCODE）：$errLine（全量见 $diagFile）" 'ERROR' }
      return @{ Ok = $false; Error = "新配置校验失败（exit=$LASTEXITCODE），已回滚" }
    }
    Write-WdLog '新配置校验通过（verge-mihomo -t）' 'OK'
  } else {
    Write-WdLog '找不到 verge-mihomo.exe，跳过本地校验' 'WARN'
  }

  # 校验通过后才更新 profiles.yaml 的 extra/updated。
  $subInfo = $null
  try {
    foreach ($hk in @($resp.Headers.Keys)) {
      if ([string]$hk -ieq 'subscription-userinfo') { $subInfo = [string]$resp.Headers[$hk]; break }
    }
  } catch { }
  $info = Get-ProfileBlock -Uid $Uid
  $block = $info.Block
  $epoch = [string][DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
  $block = [regex]::Replace($block, '(?m)^(\s*updated:\s*)\d+\s*$', "`${1}$epoch")
  if ($subInfo -ne $null -and $subInfo -ne '') {
    foreach ($pair in [regex]::Matches($subInfo, '(upload|download|total|expire)=(\d+)')) {
      $k = $pair.Groups[1].Value
      $v = $pair.Groups[2].Value
      $block = [regex]::Replace($block, "(?m)^(\s*$k" + ':\s*)\d+\s*$', "`${1}$v")
    }
    Write-WdLog "profiles.yaml extra 已同步订阅用量头"
  }
  $newText = $info.Text.Substring(0, $info.Start) + $block + $info.Text.Substring($info.End)
  [IO.File]::WriteAllText($info.Path, $newText, $noBom)

  $reloadJson = (@{ path = $runtimeFile } | ConvertTo-Json -Compress)
  try {
    $code = Invoke-ClashPut -Path '/configs?force=true' -Json $reloadJson
  } catch {
    Copy-Item -LiteralPath "$profileFile.bak-$stamp" -Destination $profileFile -Force
    Copy-Item -LiteralPath "$runtimeFile.bak-$stamp" -Destination $runtimeFile -Force
    [IO.File]::WriteAllText($profilesYamlPath, $profilesYamlOld, $noBom)
    return @{ Ok = $false; Error = "内核重载请求失败：$($_.Exception.Message)，文件已回滚（内核仍跑旧配置）" }
  }
  if ($code -lt 200 -or $code -ge 300) {
    Copy-Item -LiteralPath "$profileFile.bak-$stamp" -Destination $profileFile -Force
    Copy-Item -LiteralPath "$runtimeFile.bak-$stamp" -Destination $runtimeFile -Force
    [IO.File]::WriteAllText($profilesYamlPath, $profilesYamlOld, $noBom)
    return @{ Ok = $false; Error = "内核重载返回 $code，文件已回滚（内核仍跑旧配置）" }
  }
  $ready = $false
  $selEnc = [uri]::EscapeDataString($script:Selector)
  for ($i = 0; $i -lt 15; $i++) {
    Start-Sleep -Seconds 2
    # /version 中途也会 200，用 selector 可读作为就绪信号更可靠。
    try { Invoke-ClashGet -Path "/proxies/$selEnc" | Out-Null; $ready = $true; break } catch { }
  }
  if (-not $ready) {
    # PUT 已成功=内核正跑新配置：此时回滚文件只会制造"文件旧/内核新"的分裂，
    # 下轮还按旧文件读写 selected.now 与节点基准全错位。保留新文件、只告警，
    # 与内核保持一致；就绪检查本就只是 30s 内的可读性确认。
    Write-WdLog '注意：内核 PUT 已成功（正跑新配置），只是 30s 就绪检查超时，保留新文件以与内核一致' 'WARN'
    return @{ Ok = $false; Error = '重载后内核 30s 未恢复响应（文件已与内核同步保留，请检查 Clash 状态）' }
  }
  Write-WdLog '内核重载完成' 'OK'
  $script:LastRefresh = (Get-Date).AddMinutes($RefreshCooldownMinutes)
  return @{ Ok = $true; Added = $added; Removed = $removed; Error = $null }
}

function Invoke-Failover {
  param([string]$Reason, [string]$Uid, [int]$MaxCandidates, [int]$SwitchGapSeconds, [int]$VerifyTimeoutSeconds,
    [string]$Verdict = '', [int]$RefreshSettleSeconds = 3)
  Write-WdLog "===== 进入故障切换（原因：$Reason） =====" 'WARN'
  # 先刷新订阅再决策：旧列表可能全是过期/高延迟节点，刷新后才有完整候选池。
  # 受 RefreshCooldownMinutes 节流（被节流/失败都用旧列表继续，不阻塞后续）。
  # Refresh-Subscription 内有文件 IO（备份/写回/回滚），磁盘满或文件被锁定时
  # 可能直接 throw：这里兜住转成失败返回，用旧列表继续切换，不让整轮逃逸。
  $rr = $null
  try {
    $rr = Refresh-Subscription -Uid $Uid
  } catch {
    $rr = @{ Ok = $false; Error = "订阅刷新异常：$(Mask-Text $_.Exception.Message)" }
  }
  $refreshOk = [bool](Get-Prop $rr 'Ok')
  # Refresh-Subscription 可能修正过 uid（运行中重新导入本地 yaml → current 变了）：
  # 本轮切换写 selected.now 必须用新 uid，否则每切一次都报一条找不到 uid。
  # 取 $script: 的那份而不是 $rr.Uid：刷新中途失败也有多个返回点，逐个塞 Uid 容易漏。
  $adoptedUid = [string]$script:SubscriptionUid
  if (-not [string]::IsNullOrWhiteSpace($adoptedUid) -and $adoptedUid -ne $Uid) {
    Write-WdLog "本轮切换的 profile uid 跟随为 $adoptedUid（配置重新导入过）"
    $Uid = $adoptedUid
  }
  if ($refreshOk) {
    $addedMsg = ''
    $addedNames = Get-Prop $rr 'Added'
    if ($addedNames -ne $null -and @($addedNames).Count -gt 0) { $addedMsg = "，新增 $(@($addedNames).Count)（$(($addedNames | Select-Object -First 5) -join ' / ')）" }
    Write-WdLog "订阅已刷新（新增/消失见上）$addedMsg" 'OK'
  } elseif ((Get-Prop $rr 'Skipped')) {
    # 本地配置（type: local）没有订阅地址：预期行为，不是故障，所以不打 ERROR、
    # 也不写失败退避。节点池用内核当前列表，候选循环照常跑。
    Write-WdLog "跳过订阅刷新：$((Get-Prop $rr 'Error'))（逐节点切换照常）"
  } elseif ((Get-Prop $rr 'Throttled')) {
    Write-WdLog '订阅刷新被节流，用当前列表继续'
  } else {
    # 刷新失败也给 2 分钟退避：拼接/校验/重载失败不写 LastRefresh，
    # 否则异常持续且切换碰巧成功时，下个 15s 周期会重新下载一次订阅地址。
    $script:LastRefresh = (Get-Date).AddMinutes(2)
    Write-WdLog "订阅刷新失败：$((Get-Prop $rr 'Error'))，用旧列表继续" 'ERROR'
  }
  # 快捷路径（transport / all_down / 节点失效 / quota 四类一视同仁）：
  # 该机场同名节点背后会换 IP/凭证，刷新换的正是这个名字对应的出口 IP。quota 按
  # 出口 IP 计额度也不例外：原 IP 额度烧尽，刷新后同名节点已指向另一个出口 IP，
  # 新 IP 的额度完全可能是满的。10-14 更正——早期版本这里按“quota 刷新不变 IP”跳过
  # 刷新后的复测，与本文件 9-18 的实证自相矛盾，实测白切（换节点+掐全部连接+再探针）。
  # 只在刷新真正成功时复测（节流/失败时不复测，避免每 15s 空烧一次配额探针）。
  if ($refreshOk) {
    Write-WdLog "订阅已刷新，等待 ${RefreshSettleSeconds}s 生效后原地复测当前节点（恢复则跳过切换）..."
    if ($RefreshSettleSeconds -gt 0) { Start-Sleep -Seconds $RefreshSettleSeconds }
    try {
      $selFast = [uri]::EscapeDataString($script:Selector)
      $detailFast = Invoke-ClashGet -Path "/proxies/$selFast"
      $nowFast = [string](Get-Prop $detailFast 'now')
      $mapFast = Get-ProxyMap
      $nodeFast = $null
      if ($mapFast -is [System.Collections.IDictionary]) {
        if (-not [string]::IsNullOrWhiteSpace($nowFast) -and $mapFast.Contains($nowFast)) { $nodeFast = $mapFast[$nowFast] }
      } else {
        try { $nodeFast = $mapFast.PSObject.Properties[$nowFast].Value } catch { }
      }
      $aliveFast = $false
      if ($nodeFast -ne $null) { try { $aliveFast = [bool](Get-Prop $nodeFast 'alive') } catch { } }
      if ([string]::IsNullOrWhiteSpace($nowFast) -or $nodeFast -eq $null) {
        Write-WdLog "刷新后当前节点已消失（now=$nowFast），继续切换" 'WARN'
      } elseif (-not $aliveFast) {
        Write-WdLog "刷新后当前节点仍 alive=false（now=$nowFast），继续切换" 'WARN'
      } else {
        Wait-GatewayProxyReady -TimeoutSec $script:ProxyReadySeconds | Out-Null
        $nonceFast = (Get-Date).ToString('yyyyMMdd-HHmmss') + '-' + (Get-Random -Maximum 999999)
        $vrFast = Test-GatewayNode -Nonce $nonceFast -TimeoutSec $VerifyTimeoutSeconds
        if ((Get-Prop $vrFast 'Ok')) {
          # 探针 200 ≠ 匿名通道恢复：quota 轮的匿名尝试必然再吃一次 429（网关冷却按指纹
          # 哈希只挡旧指纹，探针每次换新 nonce = 新指纹，请求照样发到上游并重新点着冷却），
          # 随后网关回退付费 key 才返回 200。所以 quota 轮必须复查 verdict：只有翻到
          # ok/ratelimit/cooling/fingerprint（本脚本对这几类一律只记日志、不切换）才算刷新真的换到了
          # 有额度的出口 IP；读不到或仍是 quota/transport 一律当没恢复，继续进切换循环。
          # fingerprint 现在只是探测回退的 verdict（正文里认不出类的 403，如 WAF 挑战页），
          # 网关状态流已经不会为免费层门给出它——门变 quota，所以「刷新没换到出口 IP」照样当没恢复。
          # 否则既白花一次真实付费推理，又把“已恢复”记成假账。
          $recovered = $true
          # 探针端点只测匿名通道（网关不拿付费 key 回退），所以 verdict=ok 就是这条
          # 出口 IP 的匿名通道真答了，不需要再复查 verdict——旧探针经付费 key 回退
          # 才拿到 200 的那个坑在探针上不存在。旧探针（legacy 或 404 回落）仍要复查。
          if ($Verdict -eq 'quota' -and -not $script:ProbeSupported) {
            $verdictFast = ''
            try {
              $wdFast = Get-WatchdogStatus
              $verdictFast = [string](Get-Prop (Get-Prop $wdFast 'anonymous') 'verdict')
            } catch {
              Write-WdLog "刷新后复查 verdict 失败（$(Mask-Text $_.Exception.Message)），按未恢复继续切换" 'WARN'
            }
            if (@('ok', 'ratelimit', 'cooling', 'fingerprint') -notcontains $verdictFast) {
              $recovered = $false
              if ([string]::IsNullOrWhiteSpace($verdictFast)) { $verdictFast = '(读不到)' }
              Write-WdLog "刷新后探针虽 200，但匿名通道仍 verdict=$verdictFast（疑似经付费 key 回退），继续切换" 'WARN'
            }
          }
          if ($recovered) {
            # 与候选验证通过（见本函数下方 QuotaFailedAt.Remove）同款证明：这条出口 IP 现在可用，
            # 清掉它可能残留的 quota 记忆，否则同名节点会带着 +300000ms 罚分被压到正常节点之后。
            try { if ($script:QuotaFailedAt.Contains($nowFast)) { $script:QuotaFailedAt.Remove($nowFast) } } catch { }
            $script:LastSeenNow = $nowFast
            Write-WdLog "刷新后当前节点已恢复（now=$nowFast），跳过切换" 'OK'
            return @{ Ok = $true; Node = $nowFast }
          }
          # 未恢复的原因上一行已写明：探针返回 200 时 class 不可能是 config，
          # 也不再落到下面那条“复测仍失败”（会与上一行自相矛盾），直接进切换循环。
        } else {
          if ([string](Get-Prop $vrFast 'Class') -eq 'config') {
            Write-WdLog "网关配置错误（如 key/模型名不对），停止本轮：先修网关配置" 'ERROR'
            return @{ Ok = $false; Node = $null }
          }
          if ([bool](Get-Prop $vrFast 'Backoff')) {
            # 冷却挂在代理传输上，不是挂在 Clash 节点上：此刻换节点对冷却毫无影响，
            # 只会把候选池白烧一遍（原实现因此在 10:23 一轮 12 秒内连试 8 个节点，
            # 全部撞同一个 429）。正确动作是原地停手，等冷却到期后由下一轮巡检接手。
            # RetryAfter 透传给调用方，让本轮静默期贴着网关给的窗口，而不是一律
            # RoundCooldownMinutes（冷却封顶 15s 时后者会把可行动作压掉好几分钟）。
            Write-WdLog "刷新后复测拿到 429：网关匿名通道正在退避（非节点故障），本轮不切换，Retry-After=$((Get-Prop $vrFast 'RetryAfter'))s" 'WARN'
            return @{ Ok = $false; Node = $null; RetryAfter = [int](Get-Prop $vrFast 'RetryAfter') }
          }
          Write-WdLog "刷新后复测仍失败：now=$nowFast status=$((Get-Prop $vrFast 'Status')) class=$((Get-Prop $vrFast 'Class')) $(Shorten ([string](Get-Prop $vrFast 'Body')) 200)，继续切换" 'WARN'
        }
      }
    } catch {
      Write-WdLog "刷新后复测异常：$($_.Exception.Message)，继续切换" 'WARN'
    }
  } elseif ($Verdict -eq 'quota') {
    # 刷新没成功（被节流或下载失败）时拿不到新出口 IP，复测多半仍败，直接进切换。
    # 注意 LastRefresh 在刷新失败时也会打戳（+2min），所以“节流”也可能只是几分钟前下载失败。
    if ((Get-Prop $rr 'Skipped')) {
      # 本地配置本来就没有可刷新的订阅，别让日志读起来像故障。
      Write-WdLog 'quota：当前是本地配置（无可刷新订阅），直接换节点试新出口'
    } else {
      Write-WdLog 'quota 本次刷新未成功（见上），没有新出口 IP 可试，跳过原地复测直接切换'
    }
  }
  Write-WdLog '===== 进入节点切换 =====' 'WARN'
  $sel = [uri]::EscapeDataString($script:Selector)
  try {
    $detail = Invoke-ClashGet -Path "/proxies/$sel"
  } catch {
    Write-WdLog "读取 Selector [$($script:Selector)] 失败：$($_.Exception.Message)（selector 名是否正确？）" 'ERROR'
    return @{ Ok = $false; Node = $null }
  }
  $original = [string](Get-Prop $detail 'now')
  Write-WdLog "切换前 Selector=$($script:Selector) now=$original"
  if ($Verdict -eq 'quota' -and -not [string]::IsNullOrWhiteSpace($original) -and $script:QuotaFailMemoryMinutes -gt 0) {
    # quota 按出口 IP 计：触发节点本身也记一笔，避免几分钟后下一轮又把它排第一重试
    # （9-18 17:25 切走新加坡02境外中转，17:29 又切回去白白 quota 失败即此坑）。
    $script:QuotaFailedAt[$original] = Get-Date
    Write-WdLog "当前节点记为近期 quota 失败（$($script:QuotaFailMemoryMinutes) 分钟内候选降权）" 'INFO'
  }
  $map = Get-ProxyMap
  $rawAll = Get-Prop $detail 'all'
  $all = @($rawAll | Where-Object { $_ -ne $null -and $_ -ne '' })
  if (@($all).Count -eq 0) {
    Write-WdLog "Selector [$($script:Selector)] 无 all 列表（非 Selector 或名字写错），放弃本轮" 'ERROR'
    return @{ Ok = $false; Node = $null }
  }
  $cands = Get-RankedCandidates -ProxyMap $map -SelectorAll $all -ExcludeNow $original
  $aliveCount = @($cands | Where-Object { $_.Alive -and $_.Delay -gt 0 }).Count
  Write-WdLog "候选池：符合条件 $(@($cands).Count) 个，其中 alive 有延迟 $aliveCount 个"
  if ($aliveCount -eq 0 -and @($cands).Count -gt 0) {
    # 刚导入的本地配置（url-test 组 lazy: true）没有 history，全部候选 delay=0、
    # DelayRank=999999，排序退化成名字序。必须说出来：否则看日志的人会以为
    # 「延迟从低到高」仍然成立，实际是先按字母试。
    Write-WdLog '候选全部没有测速数据（Clash 尚未对这些节点做过 url-test）：本轮按名字序试候选，不是按延迟；开一次 Verge 的组测速可改善' 'WARN'
  }
  if (@($cands).Count -eq 0) {
    Write-WdLog '候选池为空（过滤后无可用节点），不做无效切换' 'ERROR'
    return @{ Ok = $false; Node = $null }
  }
  $tried = @()
  # 候选池不足 $MaxCandidates 时 Select-Object -First 只返回池里的全部（不补齐、不报错），
  # 所以「候选数 < MaxCandidates」的订阅就是这一轮把池子试一遍：池子大小见上一行的
  # 「候选池：符合条件 N 个」，实际试了几个见失败行的「已试：」列表。
  # 空池（含只有当前节点一个可用的情况，当前节点已被 -ExcludeNow 排除）在上面单独拦截。
  $pick = @($cands | Select-Object -First $MaxCandidates)
  foreach ($c in $pick) {
    # 间隔只在成功后等待（给新连接收敛）；验证失败立刻试下一个，不空烧
    # SwitchGapSeconds（失败路径省掉全部间隔，6 候选约省 1 分钟）。
    $gap = $SwitchGapSeconds - ((Get-Date) - $script:LastSwitch).TotalSeconds
    if ($gap -gt 0 -and @($tried).Count -eq 0) { Start-Sleep -Seconds ([int][Math]::Ceiling($gap)) }
    $pickMark = Get-DemoteMark $c
    Write-WdLog "尝试 [$($c.Name)]（延迟 $($c.Delay)ms，alive=$($c.Alive)$pickMark）..."
    try {
      if (-not (Switch-ClashNode -Name $c.Name -Uid $Uid)) { $tried += $c.Name; continue }
    } catch {
      Write-WdLog "切换失败：$($_.Exception.Message)" 'WARN'
      $tried += $c.Name
      continue
    }
    $script:LastSwitch = Get-Date
    Start-Sleep -Seconds 2
    Wait-GatewayProxyReady -TimeoutSec $script:ProxyReadySeconds | Out-Null
    $nonce = (Get-Date).ToString('yyyyMMdd-HHmmss') + '-' + (Get-Random -Maximum 999999)
    $vr = Test-GatewayNode -Nonce $nonce -TimeoutSec $VerifyTimeoutSeconds
    $tried += $c.Name
    if ((Get-Prop $vr 'Ok')) {
      # 验证通过说明该出口 IP 可用：清掉它可能残留的 quota 记忆（同名背后已换 IP/恢复）。
      try { if ($script:QuotaFailedAt.Contains($c.Name)) { $script:QuotaFailedAt.Remove($c.Name) } } catch { }
      $script:LastSeenNow = $c.Name
      Write-WdLog "节点可用：$($c.Name)，big-pickle 验证通过" 'OK'
      return @{ Ok = $true; Node = $c.Name }
    }
    if ([bool](Get-Prop $vr 'Backoff')) {
      # 与上面「刷新后复测」同一理由：429 说明是网关自己在退避，这个候选没有
      # 被证明坏。此处早退而不落到底部的回滚块——回滚又是一次 Clash 切换，只会
      # 在冷却期内继续制造连接抖动，且对冷却时长毫无帮助。候选本身 alive+有延迟、
      # 未被证伪，留在它上面和留在任何其它节点上没有区别。
      # LastSeenNow 必须更新：我们已经切到 $c.Name 了，不记的话下一轮会把它当成
      # 「非本脚本切换」报一次疑似手动切换——而那条日志正是人工切换的取证依据。
      $script:LastSeenNow = $c.Name
      Write-WdLog "验证 [$($c.Name)] 拿到 429：网关匿名通道正在退避，该候选未被证伪，本轮停止切换后续候选（冷却挂在代理传输上，换节点无效），Retry-After=$((Get-Prop $vr 'RetryAfter'))s" 'WARN'
      return @{ Ok = $false; Node = $null; RetryAfter = [int](Get-Prop $vr 'RetryAfter') }
    }
    if ([string](Get-Prop $vr 'Class') -eq 'quota' -and $script:QuotaFailMemoryMinutes -gt 0) {
      # 同一轮内连续 quota 说明该出口 IP 段额度已尽：记一笔，排后面的同名节点若同段也会被验证，
      # 但至少下一轮（节流/冷却后）不会再把它排第一。
      $script:QuotaFailedAt[$c.Name] = Get-Date
    }
    Write-WdLog "验证失败：$($c.Name) status=$((Get-Prop $vr 'Status')) class=$((Get-Prop $vr 'Class')) $(Shorten ([string](Get-Prop $vr 'Body')) 200)，后续候选跳过间隔" 'WARN'
    if ([string](Get-Prop $vr 'Class') -eq 'config') {
      Write-WdLog "网关配置错误（如 key/模型名不对），停止本轮切换：先修网关配置" 'ERROR'
      break
    }
  }
  Write-WdLog "本轮候选全部失败（已试：$($tried -join ' / ')），回滚到 $original 并冷却 $RoundCooldownMinutes 分钟" 'ERROR'
  if ([string]::IsNullOrWhiteSpace($original)) {
    Write-WdLog '原节点名为空，跳过回滚' 'WARN'
  } else {
    # Switch-ClashNode 返回 $false（非 2xx）时不抛异常：必须判返回值，
    # 否则回滚失败却把 LastSeenNow 记成 $original，下一轮误报“外部切换”。
    try {
      if (Switch-ClashNode -Name $original -Uid $Uid) { $script:LastSeenNow = $original }
      else { Write-WdLog "回滚到 $original 返回失败（实际仍停在最后候选），下轮以实际 now 为准" 'WARN' }
    }
    catch { Write-WdLog "回滚到 $original 失败：$($_.Exception.Message)" 'ERROR' }
  }
  return @{ Ok = $false; Node = $null }
}

function Invoke-WarpProxyFailover {
  # WARP proxy 专用 failover（design §7.2）：不复用 legacy 的 Test-GatewayNode /
  # Wait-GatewayProxyReady / QuotaFailedAt 写入 / transport 计数。只用 SOCKS5 检查。
  param(
    [string]$ProxyUrl,
    [string]$UdpTarget,
    [int]$TimeoutSeconds = 8,
    [bool]$RequireUdp = $true,
    [string]$Uid,
    [int]$MaxCandidates = 120,
    [int]$SwitchGapSeconds = 2,
    [int]$FailoverBudgetSeconds = 120
  )
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  Write-WdLog '===== 进入 WARP proxy 故障切换 =====' 'WARN'
  $rr = $null
  try { $rr = Refresh-Subscription -Uid $Uid }
  catch { $rr = @{ Ok = $false; Error = ('订阅刷新异常：' + (Mask-Proxy (Mask-Text $_.Exception.Message))) } }
  $refreshOk = [bool](Get-Prop $rr 'Ok')
  $adoptedUid = [string]$script:SubscriptionUid
  if (-not [string]::IsNullOrWhiteSpace($adoptedUid) -and $adoptedUid -ne $Uid) {
    Write-WdLog "本轮 WARP 切换的 profile uid 跟随为 $adoptedUid（配置重新导入过）"
    $Uid = $adoptedUid
  }
  if ($refreshOk) {
    Write-WdLog '订阅已刷新' 'OK'
  } elseif (Get-Prop $rr 'Skipped') {
    Write-WdLog "跳过订阅刷新：$((Get-Prop $rr 'Error'))（逐节点切换照常）"
  } elseif (Get-Prop $rr 'Throttled') {
    Write-WdLog '订阅刷新被节流，用当前列表继续'
  } else {
    $script:LastRefresh = (Get-Date).AddMinutes(2)
    Write-WdLog "订阅刷新失败：$((Get-Prop $rr 'Error'))，用旧列表继续" 'ERROR'
  }
  if ($refreshOk) {
    $settle = [int]$script:RefreshSettleSeconds
    if ($settle -gt 0) { Start-Sleep -Seconds $settle }
    $rf = Test-WarpProxyCheck -ProxyUrl $ProxyUrl -TimeoutSeconds $TimeoutSeconds -RequireUdp $RequireUdp -UdpTarget $UdpTarget
    if ($rf.Class -eq 'config') {
      Write-WdLog ('proxy-check config error（刷新后复检）：' + (Shorten (Mask-Proxy (Mask-Text ([string]$rf.Reason))) 200)) 'ERROR'
      return @{ Ok = $false; Node = $null; ConfigError = $true }
    }
    if ($rf.Ok) {
      $nowTxt = ''
      try {
        $selEnc = [uri]::EscapeDataString($script:Selector)
        $d = Invoke-ClashGet -Path "/proxies/$selEnc"
        $nowTxt = [string](Get-Prop $d 'now')
      } catch { }
      if (-not [string]::IsNullOrWhiteSpace($nowTxt)) { $script:LastSeenNow = $nowTxt }
      Write-WdLog 'proxy-check recovered after subscription refresh；WARP egress 需自行重注册/验证，未切节点' 'OK'
      return @{ Ok = $true; Node = $nowTxt; ConfigError = $false }
    }
    Write-WdLog ('proxy-check 刷新后复检仍失败：stage=' + $rf.Stage + ' reason=' + (Shorten (Mask-Proxy (Mask-Text ([string]$rf.Reason))) 200) + '，继续切换') 'WARN'
  }
  if ($sw.Elapsed.TotalSeconds -ge $FailoverBudgetSeconds) {
    Write-WdLog "proxy-check failover 预算 $($FailoverBudgetSeconds)s 已耗尽（刷新阶段），停止本轮" 'WARN'
    return @{ Ok = $false; Node = $null; ConfigError = $false }
  }
  $sel = [uri]::EscapeDataString($script:Selector)
  $detail = $null
  try { $detail = Invoke-ClashGet -Path "/proxies/$sel" }
  catch {
    Write-WdLog "读取 Selector [$($script:Selector)] 失败：$(Mask-Text $_.Exception.Message)（selector 名是否正确？）" 'ERROR'
    return @{ Ok = $false; Node = $null; ConfigError = $false }
  }
  $original = [string](Get-Prop $detail 'now')
  Write-WdLog "WARP 切换前 Selector=$($script:Selector) now=$original"
  $rawAll = Get-Prop $detail 'all'
  $all = @($rawAll | Where-Object { $_ -ne $null -and $_ -ne '' })
  if (@($all).Count -eq 0) {
    Write-WdLog "Selector [$($script:Selector)] 无 all 列表（非 Selector 或名字写错），放弃本轮" 'ERROR'
    return @{ Ok = $false; Node = $null; ConfigError = $false }
  }
  $map = Get-ProxyMap
  $cands = Get-RankedCandidates -ProxyMap $map -SelectorAll $all -ExcludeNow $original -IgnoreQuotaPenalty
  Write-WdLog "WARP 候选池：符合条件 $(@($cands).Count) 个（已忽略 legacy quota 降权）"
  if (@($cands).Count -eq 0) {
    Write-WdLog 'WARP 候选池为空（过滤后无可用节点），不做无效切换' 'ERROR'
    return @{ Ok = $false; Node = $null; ConfigError = $false }
  }
  $pick = @($cands | Select-Object -First $MaxCandidates)
  $tried = @()
  foreach ($c in $pick) {
    if ($sw.Elapsed.TotalSeconds -ge $FailoverBudgetSeconds) {
      Write-WdLog "proxy-check failover 预算 $($FailoverBudgetSeconds)s 已耗尽，停止候选循环" 'WARN'
      break
    }
    Write-WdLog "proxy-check 尝试候选 [$($c.Name)]（延迟 $($c.Delay)ms，alive=$($c.Alive)）..."
    try {
      if (-not (Switch-ClashNode -Name $c.Name -Uid $Uid)) { $tried += $c.Name; continue }
    } catch {
      Write-WdLog "WARP 切换失败：$(Mask-Text $_.Exception.Message)" 'WARN'
      $tried += $c.Name
      continue
    }
    $script:LastSwitch = Get-Date
    if ($SwitchGapSeconds -gt 0) { Start-Sleep -Seconds $SwitchGapSeconds }
    $v = Test-WarpProxyCheck -ProxyUrl $ProxyUrl -TimeoutSeconds $TimeoutSeconds -RequireUdp $RequireUdp -UdpTarget $UdpTarget
    $tried += $c.Name
    if ($v.Class -eq 'config') {
      $script:LastSeenNow = $c.Name
      Write-WdLog ('proxy-check config error（候选 ' + $c.Name + '）：' + (Shorten (Mask-Proxy (Mask-Text ([string]$v.Reason))) 200) + '，停止候选循环（不回滚、不冷却）') 'ERROR'
      return @{ Ok = $false; Node = $c.Name; ConfigError = $true }
    }
    if ($v.Ok) {
      $script:LastSeenNow = $c.Name
      Write-WdLog "proxy-check candidate [$($c.Name)] ok，已切换" 'OK'
      return @{ Ok = $true; Node = $c.Name; ConfigError = $false }
    }
    Write-WdLog "proxy-check candidate [$($c.Name)] failed：stage=$($v.Stage) reason=$(Shorten (Mask-Proxy (Mask-Text ([string]$v.Reason))) 200)" 'WARN'
  }
  Write-WdLog "proxy-check all candidates failed（已试：$($tried -join ' / ')），rollback $original，冷却 $RoundCooldownMinutes 分钟" 'ERROR'
  if ([string]::IsNullOrWhiteSpace($original)) {
    Write-WdLog '原节点名为空，跳过回滚' 'WARN'
  } else {
    try {
      if (Switch-ClashNode -Name $original -Uid $Uid) { $script:LastSeenNow = $original }
      else { Write-WdLog "回滚到 $original 返回失败（实际仍停在最后候选），下轮以实际 now 为准" 'WARN' }
    } catch { Write-WdLog "回滚到 $original 失败：$(Mask-Text $_.Exception.Message)" 'ERROR' }
  }
  return @{ Ok = $false; Node = $null; ConfigError = $false }
}

function Reset-WarpExitTriggerState {
  # 清零 WARP 出口失败触发状态。BaselineStreak > 0 表示刚动作/刚切换：连击需再涨
  # threshold 才会重复触发；GraceRounds 是动作后不累计“出口池不可用”本地计数的轮数。
  param([int64]$BaselineStreak = 0, [switch]$ClearCooldown, [int]$GraceRounds = 0)
  $script:WarpExitBaselineStreak = [int64]$BaselineStreak
  $script:WarpExitZeroRounds = 0
  $script:WarpExitWaitLoggedStreak = -1
  $script:WarpExitLocalStrikes = 0
  $script:WarpExitLocalGraceRounds = [int]$GraceRounds
  if ($ClearCooldown) { $script:WarpExitCooldownUntil = [DateTime]::MinValue }
}

function Invoke-GatewayWarpReset {
  # POST /v1/watchdog/warp/reset（与 /v1/watchdog/status 同一 server_keys 鉴权）：
  # 让网关退役全部旧 WARP 出口，旧请求失败、新注册走刚切到的 Clash 节点。
  # 老网关没有该端点（404/405）时静默跳过；其他失败只回错误文本，由调用方记日志。
  $uri = $script:GatewayBase + '/v1/watchdog/warp/reset'
  try { $key = Resolve-GatewayKey } catch {
    return @{ Ok = $false; Status = 0; Unsupported = $false; Error = ('读取网关 key 失败：' + (Mask-Text $_.Exception.Message)) }
  }
  $body = [Text.Encoding]::UTF8.GetBytes('{}')
  try {
    $r = Invoke-WebRequest -Uri $uri -Method Post -Headers @{ Authorization = "Bearer $key" } -Body $body -ContentType 'application/json; charset=utf-8' -TimeoutSec 5 -UseBasicParsing
    $code = 0
    try { $code = [int]$r.StatusCode } catch { }
    return @{ Ok = ($code -ge 200 -and $code -lt 300); Status = $code; Unsupported = $false; Error = '' }
  } catch {
    $info = Get-HttpErrorInfo $_
    $code = 0
    try { $code = [int](Get-Prop $info 'Status') } catch { }
    if ($code -eq 404 -or $code -eq 405) {
      return @{ Ok = $false; Status = $code; Unsupported = $true; Error = '' }
    }
    return @{ Ok = $false; Status = $code; Unsupported = $false; Error = (Mask-Text $_.Exception.Message) }
  }
}

function Invoke-WarpExitFailureTrigger {
  # 由 Invoke-WarpProxyIteration 的中继健康分支调用。Acted=$true 表示真的动作了
  # （切换成功，或 refresh_only 刷新成功）；ActionFailed=$true 表示动作尝试过但失败
  # （切换失败/实际刷新失败），供 -Once 分别映射 exit 0/1。只使用本触发器自己的
  # cooldown/baseline，不碰 $script:RoundCooldownUntil（那是中继失败/legacy 的轮冷却）。
  # 主信号：/healthz 的 warp.failure_streak。副信号：failure_streak 缺失/为 0 但
  # warp.status=unavailable 或 warp.active=0 连续达到 threshold 轮（本地计数）。
  param($Health)
  $res = @{ Acted = $false; Switched = $false; Refreshed = $false; ResetSent = $false; ActionFailed = $false; Cause = ''; Node = '' }
  if (-not $script:WarpExitFailureEnabled) { return $res }
  if ($Health -eq $null -or -not [bool](Get-Prop $Health 'Ok')) { return $res }
  if (-not [bool](Get-Prop $Health 'HasWarp')) {
    $hadState = ($script:WarpExitBaselineStreak -gt 0) -or ($script:WarpExitLocalStrikes -gt 0) -or ($script:WarpExitLastObservedStreak -gt 0)
    Reset-WarpExitTriggerState -ClearCooldown
    $script:WarpExitLastObservedStreak = [int64]0
    $script:WarpExitLastSuccessAt = ''
    $script:WarpExitLastSuccessLogAt = [DateTime]::MinValue
    $script:WarpExitStartedAt = ''
    $script:WarpExitNoCountersWarned = $false
    $script:WarpExitFirstObservation = $true
    if ($hadState -and -not $script:WarpExitNoWarpLogged) {
      $script:WarpExitNoWarpLogged = $true
      Write-WdLog 'WARP 出口失败触发：/healthz 无 warp 段（WARP 未启用），已清零触发状态并跳过' 'INFO'
    }
    return $res
  }
  $script:WarpExitNoWarpLogged = $false

  $now = Get-Date
  $streak = [int64](Get-Prop $Health 'FailureStreak')
  $warpStatus = [string](Get-Prop $Health 'WarpStatus')
  $warpActive = [int64](Get-Prop $Health 'WarpActive')
  $warpSize = [int64](Get-Prop $Health 'WarpSize')
  $startedAt = [string](Get-Prop $Health 'StartedAt')
  $lastSuccess = [string](Get-Prop $Health 'LastSuccessAt')
  $kind = [string](Get-Prop $Health 'LastFailureKind')
  $failuresTotal = [int64](Get-Prop $Health 'FailuresTotal')
  $createFailures = [int64](Get-Prop $Health 'CreateFailuresTotal')
  $didReset = $false

  # 1) 网关重启（started_at 变化）= 新纪元：旧连击/基线/冷却全部作废。
  if ($startedAt -ne '' -and $script:WarpExitStartedAt -ne '' -and $startedAt -ne $script:WarpExitStartedAt) {
    Write-WdLog "WARP 出口失败触发：网关已重启（started_at $($script:WarpExitStartedAt) → $startedAt），清零连击基线与冷却" 'INFO'
    Reset-WarpExitTriggerState -ClearCooldown -GraceRounds $script:WarpExitFailureResetRounds
    $script:WarpExitLastObservedStreak = [int64]0
    $script:WarpExitNoCountersWarned = $false
    $script:WarpExitFirstObservation = $true
    $didReset = $true
  }
  if ($startedAt -ne '') { $script:WarpExitStartedAt = $startedAt }

  # 2) 新的真实 WARP 成功（last_success_at 变化/前进）：可用出口已出现，清零触发状态。
  if ($lastSuccess -ne '' -and $script:WarpExitLastSuccessAt -ne '' -and $lastSuccess -ne $script:WarpExitLastSuccessAt) {
    if (-not $didReset) {
      # 成功日志节流：正常成功每 5 分钟一条；实际清掉失败状态时立即记录。
      $hadWarpFailureState = (
        ($script:WarpExitBaselineStreak -gt 0) -or
        ($script:WarpExitLastObservedStreak -gt 0) -or
        ($script:WarpExitCooldownUntil -gt $now) -or
        ($script:WarpExitLocalStrikes -gt 0)
      )
      $successLogDue = (
        ($script:WarpExitLastSuccessLogAt -eq [DateTime]::MinValue) -or
        (($now - $script:WarpExitLastSuccessLogAt).TotalMinutes -ge $script:WarpExitSuccessLogIntervalMinutes)
      )
      if ($hadWarpFailureState -or $successLogDue) {
        Write-WdLog "WARP 出口失败触发：检测到新的 WARP 成功（last_success_at=$lastSuccess），清零连击基线与冷却" 'OK'
        $script:WarpExitLastSuccessLogAt = $now
      }
    }
    Reset-WarpExitTriggerState -ClearCooldown
    $script:WarpExitLastObservedStreak = [int64]0
    $script:WarpExitFirstObservation = $false
    $didReset = $true
  }
  if ($lastSuccess -ne '') { $script:WarpExitLastSuccessAt = $lastSuccess }

  # 3) 任何节点切换（含中继 failover）：以当前连击为新基线，避免旧连击立刻触发。
  # 首次观察时若 LastSwitch 已不是 MinValue（例如首轮中继 failover 刚切过），也要
  # 按“已观察到的切换”处理，否则同一进程里旧连击基线会被直接沿用。
  $lastSwitch = $script:LastSwitch
  $firstSwitchObservation = ($script:WarpExitLastSwitchSeen -eq $null)
  $switchObserved = $false
  if ($firstSwitchObservation) {
    $switchObserved = ($lastSwitch -ne [DateTime]::MinValue)
    $script:WarpExitLastSwitchSeen = $lastSwitch
  } elseif ($lastSwitch -ne $script:WarpExitLastSwitchSeen) {
    $switchObserved = $true
    $script:WarpExitLastSwitchSeen = $lastSwitch
  }
  if ($switchObserved) {
    if (-not $didReset) {
      Write-WdLog "WARP 出口失败触发：检测到节点切换（$($lastSwitch.ToString('HH:mm:ss'))），以当前连击 $streak 为新基线" 'INFO'
    }
    Reset-WarpExitTriggerState -BaselineStreak $streak -ClearCooldown -GraceRounds $script:WarpExitFailureResetRounds
    $script:WarpExitLastObservedStreak = $streak
    $script:WarpExitFirstObservation = $false
    $didReset = $true
  }

  # F1：旧网关 /healthz 没有 failure_streak/failures_total。主信号禁用，只保留
  # active=0/unavailable 副信号；兼容警告每次网关世代只打一次，升级/重启后重报。
  if (-not [bool](Get-Prop $Health 'HasFailureStreak')) {
    if (-not $script:WarpExitNoCountersWarned) {
      Write-WdLog 'WARP 出口失败触发：网关 /healthz 的 warp 段没有 failure_streak/failures_total（旧网关），主信号 failure_streak 禁用，仅保留 active=0/unavailable 副信号；请升级并重启网关' 'WARN'
      $script:WarpExitNoCountersWarned = $true
    }
  } else {
    $script:WarpExitNoCountersWarned = $false
  }

  # F2：进程启动/网关重启后的首次有效观察，若出口池已满（active >= size>0、status=ready）
  # 且历史 failure_streak 已达阈值，先记为基线、不立即切换：重启后看到的高连击可能是
  # 上一个进程/旧网关遗留的历史，真正 2xx 尚未发生。代价：需要再新增 threshold 次失败
  # 才会动作；基线之后的增量失败仍会触发（新故障不会被吞掉）。
  if ($script:WarpExitFirstObservation) {
    $script:WarpExitFirstObservation = $false
    if ([bool](Get-Prop $Health 'HasFailureStreak') -and $streak -ge $script:WarpExitFailureThreshold -and
      $warpSize -gt 0 -and $warpActive -ge $warpSize -and $warpStatus -eq 'ready') {
      $script:WarpExitBaselineStreak = [int64]$streak
      $script:WarpExitWaitLoggedStreak = -1
      $script:WarpExitLastObservedStreak = [int64]$streak
      Write-WdLog "WARP 出口失败触发：首次观察（进程启动/网关重启）出口池已满（active=$warpActive size=$warpSize, status=ready）且历史 failure_streak=$streak 已达阈值 $($script:WarpExitFailureThreshold)，先记为基线、不立即切换；之后的增量失败仍会触发（新失败需再累计 $($script:WarpExitFailureThreshold) 次）" 'WARN'
    }
  }

  # 主信号：failure_streak 上涨时打一条（同一数字不重复刷）。
  $prevStreak = [int64]$script:WarpExitLastObservedStreak
  if ($streak -gt $prevStreak -and $streak -gt 0) {
    Write-WdLog "WARP 出口失败连击 $streak/$($script:WarpExitFailureThreshold)（kind=$kind，累计 $failuresTotal 次，创建失败 $createFailures 次）" 'WARN'
  }
  if ($streak -gt 0) {
    # 主信号有数时不累计副信号，避免同一故障双重计数。
    $script:WarpExitLocalStrikes = 0
  }

  # 副信号：failure_streak 缺失/为 0，但出口池持续 unavailable / active==0。
  # 动作后的前 reset_rounds 轮与冷却期内不累计，给 WARP 重新注册留时间。
  $unavailable = ($streak -le 0) -and (($warpStatus -eq 'unavailable') -or ($warpActive -le 0))
  if ($unavailable) {
    if ($now -lt $script:WarpExitCooldownUntil) {
      $script:WarpExitLocalStrikes = 0
      if ($script:WarpExitLocalGraceRounds -gt 0) { $script:WarpExitLocalGraceRounds-- }
    } elseif ($script:WarpExitLocalGraceRounds -gt 0) {
      $script:WarpExitLocalGraceRounds--
      $script:WarpExitLocalStrikes = 0
    } else {
      $script:WarpExitLocalStrikes++
      Write-WdLog "WARP 出口池连续不可用 $($script:WarpExitLocalStrikes)/$($script:WarpExitFailureThreshold) 轮（failure_streak=$streak，status=$warpStatus active=$warpActive）" 'WARN'
    }
  } else {
    if ($script:WarpExitLocalStrikes -gt 0) {
      Write-WdLog "WARP 出口池恢复可用（本地不可用计数 $($script:WarpExitLocalStrikes) 清零）" 'OK'
    }
    $script:WarpExitLocalStrikes = 0
    if ($script:WarpExitLocalGraceRounds -gt 0) { $script:WarpExitLocalGraceRounds-- }
  }

  # 连击连续归零 reset_rounds 轮才清除动作基线；出口池不可用不算“恢复”。
  if ($streak -le 0 -and -not $unavailable) {
    if ($script:WarpExitBaselineStreak -gt 0) {
      $script:WarpExitZeroRounds++
      if ($script:WarpExitZeroRounds -ge $script:WarpExitFailureResetRounds) {
        Write-WdLog "WARP 出口失败连击已连续归零 $($script:WarpExitZeroRounds) 轮，清除触发基线并重新布防" 'OK'
        Reset-WarpExitTriggerState
      }
    } else {
      $script:WarpExitZeroRounds = 0
    }
  } elseif ($streak -gt 0) {
    $script:WarpExitZeroRounds = 0
  }

  $primaryTarget = [int64]$script:WarpExitBaselineStreak + [int64]$script:WarpExitFailureThreshold
  $cause = ''
  $causeDetail = ''
  if ($streak -ge $primaryTarget) {
    $cause = 'failure_streak'
    $causeDetail = "failure_streak=$streak 达阈值 $primaryTarget（kind=$kind，累计 $failuresTotal 次，创建失败 $createFailures 次）"
  } elseif ($script:WarpExitLocalStrikes -ge $script:WarpExitFailureThreshold) {
    $cause = 'unavailable_rounds'
    $causeDetail = "出口池连续不可用 $($script:WarpExitLocalStrikes) 轮达阈值 $($script:WarpExitFailureThreshold)（failure_streak=$streak，status=$warpStatus active=$warpActive）"
  }
  $script:WarpExitLastObservedStreak = $streak
  if ($cause -eq '') {
    $script:WarpExitWaitLoggedStreak = -1
    return $res
  }
  if ($now -lt $script:WarpExitCooldownUntil) {
    if ($script:WarpExitWaitLoggedStreak -ne $streak) {
      $script:WarpExitWaitLoggedStreak = $streak
      Write-WdLog "WARP 出口失败触发条件已满足（$causeDetail），但冷却中（$($script:WarpExitCooldownUntil.ToString('HH:mm:ss')) 后恢复），本轮不动作" 'WARN'
    }
    return $res
  }
  $lastSwitchGap = ($now - $script:LastSwitch).TotalSeconds
  if ($script:LastSwitch -ne [DateTime]::MinValue -and $lastSwitchGap -lt [double]$script:SwitchGapSeconds) {
    if ($script:WarpExitWaitLoggedStreak -ne $streak) {
      $script:WarpExitWaitLoggedStreak = $streak
      Write-WdLog "WARP 出口失败触发条件已满足（$causeDetail），但距上次切换不足 $($script:SwitchGapSeconds)s，本轮不动作" 'WARN'
    }
    return $res
  }

  Write-WdLog "WARP 出口失败触发：$causeDetail，action=$($script:WarpExitFailureAction)（cooldown $($script:WarpExitFailureCooldownMinutes) 分钟）" 'WARN'
  $action = $script:WarpExitFailureAction
  $refreshed = $false
  $refreshFailed = $false
  if ($action -ne 'switch_only') {
    $rr = $null
    try { $rr = Refresh-Subscription -Uid $script:SubscriptionUid }
    catch { $rr = @{ Ok = $false; Error = ('订阅刷新异常：' + (Mask-Text $_.Exception.Message)) } }
    if ([bool](Get-Prop $rr 'Ok')) {
      $refreshed = $true
      Write-WdLog 'WARP 出口失败触发：订阅已刷新' 'OK'
    } elseif (Get-Prop $rr 'Skipped') {
      Write-WdLog "WARP 出口失败触发：跳过订阅刷新：$((Get-Prop $rr 'Error'))" 'INFO'
    } elseif (Get-Prop $rr 'Throttled') {
      Write-WdLog 'WARP 出口失败触发：订阅刷新被节流，继续' 'WARN'
    } else {
      $refreshFailed = $true
      Write-WdLog "WARP 出口失败触发：订阅刷新失败：$((Get-Prop $rr 'Error'))，继续" 'WARN'
    }
  }
  $switched = $false
  $switchedNode = ''
  if ($action -ne 'refresh_only') {
    try {
      $sel = [uri]::EscapeDataString($script:Selector)
      $detail = Invoke-ClashGet -Path "/proxies/$sel"
      $original = [string](Get-Prop $detail 'now')
      $rawAll = Get-Prop $detail 'all'
      $all = @($rawAll | Where-Object { $_ -ne $null -and $_ -ne '' })
      if (@($all).Count -eq 0) {
        Write-WdLog "WARP 出口失败触发：Selector [$($script:Selector)] 无 all 列表，跳过切换" 'ERROR'
      } else {
        $map = Get-ProxyMap
        $cands = Get-RankedCandidates -ProxyMap $map -SelectorAll $all -ExcludeNow $original -IgnoreQuotaPenalty
        Write-WdLog "WARP 出口失败触发：候选池 $(@($cands).Count) 个（已忽略 legacy quota 降权），按排名尝试切换" 'INFO'
        $switchSw = [System.Diagnostics.Stopwatch]::StartNew()
        foreach ($c in @($cands | Select-Object -First $script:MaxCandidates)) {
          if ($switchSw.Elapsed.TotalSeconds -ge $script:ProxyFailoverBudgetSeconds) {
            Write-WdLog "WARP 出口失败触发：切换候选预算 $($script:ProxyFailoverBudgetSeconds)s 已耗尽，停止本轮切换" 'WARN'
            break
          }
          try {
            if (Switch-ClashNode -Name $c.Name -Uid $script:SubscriptionUid) {
              $switched = $true
              $switchedNode = $c.Name
              break
            }
            Write-WdLog "WARP 出口失败触发：切换 [$($c.Name)] 返回失败，试下一个候选" 'WARN'
          } catch {
            Write-WdLog "WARP 出口失败触发：切换 [$($c.Name)] 异常：$(Mask-Text $_.Exception.Message)" 'WARN'
          }
        }
      }
    } catch {
      Write-WdLog "WARP 出口失败触发：读取/切换候选失败：$(Mask-Text $_.Exception.Message)" 'ERROR'
    }
  }
  if ($switched) {
    $script:LastSwitch = Get-Date
    $script:LastSeenNow = $switchedNode
    $script:WarpExitLastSwitchSeen = $script:LastSwitch
    Write-WdLog "WARP 出口失败触发：已切换到 [$switchedNode]" 'OK'
    $reset = Invoke-GatewayWarpReset
    if ([bool](Get-Prop $reset 'Ok')) {
      $res.ResetSent = $true
      Write-WdLog 'WARP 出口失败触发：已请求网关退役旧 WARP 出口（POST /v1/watchdog/warp/reset 2xx）' 'OK'
    } elseif ([bool](Get-Prop $reset 'Unsupported')) {
      Write-WdLog "WARP 出口失败触发：网关不支持出口重置端点（HTTP $((Get-Prop $reset 'Status'))，旧版本），跳过" 'INFO'
    } else {
      Write-WdLog "WARP 出口失败触发：出口重置失败（HTTP $((Get-Prop $reset 'Status'))）：$((Get-Prop $reset 'Error'))" 'WARN'
    }
  } elseif ($action -eq 'refresh_only') {
    Write-WdLog 'WARP 出口失败触发：action=refresh_only，本轮只刷新订阅不切换' 'INFO'
  } else {
    Write-WdLog 'WARP 出口失败触发：本轮未切换节点（候选全部失败）' 'WARN'
  }

  # 动作后进入冷却并记录新基线：
  #   - 切换成功：baseline=当前连击，旧故障需再涨 threshold 才重复触发；
  #   - refresh_only 刷新成功：同样保留 baseline，给新出口 IP 一个机会；
  #   - 切换失败 / refresh_only 未成功（节流/失败/本地跳过）：baseline=0，
  #     冷却结束后同一 failure_streak 可重试，不会被旧基线卡住（verifier F1）。
  if ($switched -or ($action -eq 'refresh_only' -and $refreshed)) {
    $script:WarpExitBaselineStreak = [int64]$streak
  } else {
    $script:WarpExitBaselineStreak = [int64]0
  }
  $script:WarpExitZeroRounds = 0
  $script:WarpExitWaitLoggedStreak = -1
  $script:WarpExitLocalStrikes = 0
  $script:WarpExitLocalGraceRounds = [int]$script:WarpExitFailureResetRounds
  # 冷却从动作真正结束时起算：刷新/切换可能耗时几十秒，用动作前的 $now 会缩短冷却。
  $script:WarpExitCooldownUntil = (Get-Date).AddMinutes([int]$script:WarpExitFailureCooldownMinutes)
  $script:WarpExitLastObservedStreak = $streak
  $script:WarpExitFirstObservation = $false
  # refresh_only 刷新成功也算“动作了”（verifier F2）：-Once 能据此退出 0，不再显示为无动作。
  $res.Acted = ($switched -or ($action -eq 'refresh_only' -and $refreshed))
  # F4：切换类动作（refresh_then_switch / switch_only）没切成功，或 refresh_only 实际刷新
  # 失败，都算“尝试过但失败”；-Once 据此 exit 1。节流/本地跳过属于 no-op，不算失败。
  $res.ActionFailed = ((($action -ne 'refresh_only') -and (-not $switched)) -or
    (($action -eq 'refresh_only') -and $refreshFailed))
  $res.Switched = $switched
  $res.Refreshed = $refreshed
  $res.Cause = $cause
  $res.Node = $switchedNode
  return $res
}

function Invoke-WarpProxyIteration {
  # WARP proxy 状态机（design §6）。返回 @{Acted;ConfigError;ConfirmedDown;ActionFailed}，
  # 供 -Once 固定退出码。
  param($Plan)
  if ($Plan -eq $null -or $Plan.Mode -ne 'warp-proxy') {
    return @{ Acted = $false; ConfigError = $false; ConfirmedDown = $false }
  }
  if ($Plan.ConfigError) {
    $sig = 'plan|' + [string]$Plan.Reason
    if ($script:WarpProxyConfigErrSig -ne $sig) {
      $script:WarpProxyConfigErrSig = $sig
      Write-WdLog ('proxy-check config error：' + (Shorten (Mask-Proxy (Mask-Text ([string]$Plan.Reason))) 200)) 'ERROR'
    }
    $script:WarpProxyLastState = 'config'
    return @{ Acted = $false; ConfigError = $true; ConfirmedDown = $false }
  }
  $target = Format-ProxyEndpoint -Url $Plan.Endpoint
  $r = Invoke-WarpProxyCheckWithRecheck -ProxyUrl $Plan.Endpoint -TimeoutSeconds $script:ProxyCheckTimeoutSeconds -RequireUdp $script:ProxyCheckRequireUdp -UdpTarget $script:ProxyCheckUdpTarget -RecheckSeconds $script:ProxyCheckRecheckSeconds
  if ($r.ConfigError) {
    $sig = 'check|' + [string]$r.Stage + '|' + [string]$r.Reason
    if ($script:WarpProxyConfigErrSig -ne $sig) {
      $script:WarpProxyConfigErrSig = $sig
      Write-WdLog ('proxy-check config error：' + (Shorten (Mask-Proxy (Mask-Text ([string]$r.Reason))) 200)) 'ERROR'
    }
    $script:WarpProxyLastState = 'config'
    return @{ Acted = $false; ConfigError = $true; ConfirmedDown = $false }
  }
  if ($r.Recovered) {
    $script:WarpProxyConfigErrSig = ''
    if ($script:RoundCooldownUntil -gt (Get-Date)) {
      $script:RoundCooldownUntil = [DateTime]::MinValue
      Write-WdLog 'proxy-check jitter recovered，不切换（已清除 round cooldown）' 'OK'
    } else {
      Write-WdLog 'proxy-check jitter recovered，不切换' 'OK'
    }
    $script:WarpProxyLastState = 'ok'
    $script:WarpProxyLastLogAt = Get-Date
    return @{ Acted = $false; ConfigError = $false; ConfirmedDown = $false }
  }
  if ($r.Ok) {
    $script:WarpProxyConfigErrSig = ''
    $wasCooling = $script:RoundCooldownUntil -gt (Get-Date)
    if ($wasCooling) { $script:RoundCooldownUntil = [DateTime]::MinValue }
    $now = Get-Date
    $stateChanged = ($script:WarpProxyLastState -ne 'ok')
    $due = $stateChanged -or $script:WarpProxyLastLogAt -eq [DateTime]::MinValue -or
      (($now - $script:WarpProxyLastLogAt).TotalMinutes -ge $NormalLogMinutes)
    if ($due) {
      Write-WdLog "proxy-check ok target=$target tcp=$($r.TcpMs)ms udp=$($r.UdpMs)ms$(if ($wasCooling) { '（冷却已清除）' } else { '' })"
      $script:WarpProxyLastLogAt = $now
    }
    $script:WarpProxyLastState = 'ok'
    # WARP 出口失败触发只挂在中继检查通过的分支：中继失败路径完全不变。
    # /healthz 只读、失败 60s 退避；触发器的 cooldown/baseline 与轮冷却无关。
    if ($script:WarpExitFailureEnabled) {
      try {
        $hz = Get-GatewayHealthz
        if (-not [bool](Get-Prop $hz 'Skipped')) {
          $wr = Invoke-WarpExitFailureTrigger -Health $hz
          $script:WarpExitTriggerErrSig = ''
          if ([bool](Get-Prop $wr 'Acted')) {
            return @{ Acted = $true; ConfigError = $false; ConfirmedDown = $false; ActionFailed = $false }
          }
          if ([bool](Get-Prop $wr 'ActionFailed')) {
            return @{ Acted = $false; ConfigError = $false; ConfirmedDown = $false; ActionFailed = $true }
          }
        }
      } catch {
        $sig = 'warp-exit-trigger|' + [string]($_.Exception.Message)
        if ($script:WarpExitTriggerErrSig -ne $sig) {
          $script:WarpExitTriggerErrSig = $sig
          Write-WdLog ('WARP 出口失败触发异常：' + (Shorten (Mask-Text $_.Exception.Message) 200)) 'WARN'
        }
      }
    }
    return @{ Acted = $false; ConfigError = $false; ConfirmedDown = $false }
  }
  $failTxt = 'stage=' + $r.Stage + ' kind=' + $r.Kind + ' reason=' + (Shorten (Mask-Proxy (Mask-Text ([string]$r.Reason))) 200)
  if ($r.Rechecked) {
    Write-WdLog "proxy-check failed $failTxt，$($script:ProxyCheckRecheckSeconds)s 后复检仍失败（已确认不可用）" 'WARN'
  } else {
    Write-WdLog "proxy-check failed $failTxt，proxy_check_recheck_seconds=0（直接确认不可用，超时预算 $($script:ProxyCheckTimeoutSeconds)s）" 'WARN'
  }
  $script:WarpProxyLastState = 'down'
  $can = Test-WarpProxyCanFailover -ProxyUrl $Plan.Endpoint
  if (-not $can.Allowed) {
    Write-WdLog ('proxy-check 确认不可用但未切换：' + $can.Reason) 'ERROR'
    return @{ Acted = $false; ConfigError = $false; ConfirmedDown = $true }
  }
  if ((Get-Date) -lt $script:RoundCooldownUntil) {
    Write-WdLog "proxy-check 确认不可用，但上一轮失败后的冷却中（$($script:RoundCooldownUntil.ToString('HH:mm:ss')) 后恢复），本轮不切换"
    return @{ Acted = $false; ConfigError = $false; ConfirmedDown = $true }
  }
  $fr = Invoke-WarpProxyFailover -ProxyUrl $Plan.Endpoint -UdpTarget $script:ProxyCheckUdpTarget -TimeoutSeconds $script:ProxyCheckTimeoutSeconds -RequireUdp $script:ProxyCheckRequireUdp -Uid $script:SubscriptionUid -MaxCandidates $script:MaxCandidates -SwitchGapSeconds $script:ProxySwitchGapSeconds -FailoverBudgetSeconds $script:ProxyFailoverBudgetSeconds
  if (Get-Prop $fr 'ConfigError') {
    $sig = 'failover|' + [string](Get-Prop $fr 'Node')
    if ($script:WarpProxyConfigErrSig -ne $sig) {
      $script:WarpProxyConfigErrSig = $sig
      Write-WdLog 'proxy-check config error（failover 中），已停止候选循环' 'ERROR'
    }
    $script:WarpProxyLastState = 'config'
    return @{ Acted = $true; ConfigError = $true; ConfirmedDown = $true }
  }
  if (-not (Get-Prop $fr 'Ok')) {
    $script:RoundCooldownUntil = (Get-Date).AddMinutes($RoundCooldownMinutes)
    Write-WdLog "proxy-check failover 失败，round cooldown $RoundCooldownMinutes 分钟" 'ERROR'
    return @{ Acted = $true; ConfigError = $false; ConfirmedDown = $true }
  }
  $script:WarpProxyConfigErrSig = ''
  $script:WarpProxyLastState = 'ok'
  $script:WarpProxyLastLogAt = Get-Date
  return @{ Acted = $true; ConfigError = $false; ConfirmedDown = $true }
}

function Invoke-WatchIteration {
  param(
    [int]$MaxCandidates,
    [int]$SwitchGapSeconds, [int]$VerifyTimeoutSeconds,
    [string]$Uid, [int]$RoundCooldownMinutes,
    [int]$NormalLogMinutes = 20, [int]$RefreshSettleSeconds = 3,
    [ValidateRange(1, 10)][int]$TransportConfirmRounds = 2
  )
  # 每轮按当前 config.json 重算模式：warp-proxy 走独立状态机，其余保持 legacy 原样。
  $plan = Resolve-ProxyCheckPlan
  if ($plan.Mode -eq 'warp-proxy') {
    Set-WarpProxyMode -Active $true
    return Invoke-WarpProxyIteration -Plan $plan
  }
  Set-WarpProxyMode -Active $false
  if ($plan.Mode -eq 'warp-direct' -and -not $script:WarpDirectWarned) {
    $script:WarpDirectWarned = $true
    Write-WdLog 'proxy-check 检测到直连 WARP（warp.enabled=true 且 warp.proxy 为空）：仍走 legacy 巡检；legacy 可能因 proxies.all_down/节点失效切换 Clash 并 DELETE /connections，与 WARP 流量无关（已知风险）' 'WARN'
  }
  $wd = $null
  try { $wd = Get-WatchdogStatus }
  catch {
    Write-WdLog "网关 /v1/watchdog/status 不可达：$($_.Exception.Message)（网关未启动？稍后重试）" 'WARN'
    # 中断本身已每轮 WARN；置恢复标记使中断结束后的首个正常轮次立即补一条，而不是等下个心跳。
    $script:NeedRecoveryLog = $true
    # verdict 未知轮直接清零：transport 连续计数只认“连续可读轮”，中断不延续。
    $script:TransportStrikes = 0
    return @{ Acted = $false }
  }
  $prox = Get-Prop $wd 'proxies'
  $allDown = $false
  if ($prox -ne $null) { try { $allDown = [bool](Get-Prop $prox 'all_down') } catch { } }
  $anon = Get-Prop $wd 'anonymous'
  $verdict = 'ok'
  $cause = ''
  $cooling = 0
  $successAge = $null
  $failureAge = $null
  if ($anon -ne $null) {
    try { $verdict = [string](Get-Prop $anon 'verdict') } catch { }
    try { $cause = [string](Get-Prop $anon 'cause') } catch { }
    try { $cooling = [int64](Get-Prop $anon 'cooling_seconds') } catch { }
    # 注意：[int64]$null 结果是 0 且不抛异常，必须先判空再转，否则缺字段
    # （网关重启后无成功记录、零流量夜间）会被误读为“刚刚成功过”，证据门恒开。
    $tmpAge = Get-Prop $anon 'last_success_age_s'
    if ($tmpAge -ne $null) { try { $successAge = [int64]$tmpAge } catch { $successAge = $null } } else { $successAge = $null }
    $tmpAge = Get-Prop $anon 'last_failure_age_s'
    if ($tmpAge -ne $null) { try { $failureAge = [int64]$tmpAge } catch { $failureAge = $null } } else { $failureAge = $null }
    if ([string]::IsNullOrWhiteSpace($verdict)) { $verdict = 'ok' }
  }
  $recommend = $false
  try { $recommend = [bool](Get-Prop $wd 'recommend_switch') } catch { }

  $detail = $null
  $map = $null
  # 最多两轮：第一轮 404 时按 selector 候选列表重挑一次（换 profile 后组名变了的自愈路径），
  # 挑到不同组就用它再查一次；挑不到才记日志并放弃本轮。
  for ($lookupTry = 0; $lookupTry -lt 2; $lookupTry++) {
    try {
      $sel = [uri]::EscapeDataString($script:Selector)
      $detail = Invoke-ClashGet -Path "/proxies/$sel"
      $map = Get-ProxyMap
      $script:LastClashErrSig = ''
      break
    } catch {
      $errMsg = [string]$_.Exception.Message
      if ($errMsg -match '404|NotFound') {
        $picked = $null
        try { $picked = Resolve-SelectorCandidate -Candidates $script:SelectorCandidates } catch { $picked = $null }
        if ($lookupTry -eq 0 -and $picked -and $picked -ne $script:Selector) {
          Write-WdLog "Selector [$($script:Selector)] 不在当前配置里，按候选顺序改用 [$picked]" 'INFO'
          $script:Selector = $picked
          continue
        }
        # selector 组名不在当前配置里（换订阅/切 profile 后最常见），不是 Clash 没起。
        # 每轮都打会 15s 一条 ERROR 刷爆日志（5MB 轮转把排障记录滚掉），用签名去重。
        $sig = "selector404|$($script:Selector)"
        if ($script:LastClashErrSig -ne $sig) {
          $script:LastClashErrSig = $sig
          $availTxt = '（读不到）'
          try {
            $avail = @(Get-SelectorGroupNames -ProxyMap (Get-ProxyMap))
            # 不截断：真实组名往往排在第 10 个之后（本地这份里 🚀 节点选择 就排第 11），
            # 截断等于把用户最该看到的那个名字藏起来。Selector 组一般也就十几个。
            if (@($avail).Count -gt 0) { $availTxt = (@($avail) -join ' / ') }
          } catch { }
          Write-WdLog "Selector [$($script:Selector)] 不在当前 Clash 配置里（404），候选 [$($script:SelectorCandidates -join ' / ')] 均未命中。当前可用的 Selector 组：$availTxt。改 watchdog.json 的 selector 后需重启看门狗（配置只在启动时读一次），或用 -Selector 指定" 'ERROR'
        }
        $script:NeedRecoveryLog = $true
      } else {
        $sig = "clashdown|$errMsg"
        if ($script:LastClashErrSig -ne $sig) {
          $script:LastClashErrSig = $sig
          Write-WdLog "Clash 接口不可达：$errMsg（Clash 未启动？稍后重试）" 'WARN'
        }
        # 同上：中断恢复后的首个正常轮次立即补一条。
        $script:NeedRecoveryLog = $true
      }
      # 同网关不可达：verdict 未知轮不清零会把中断前的计数漏到恢复后。
      $script:TransportStrikes = 0
      return @{ Acted = $false }
    }
  }
  $now = [string](Get-Prop $detail 'now')
  # 外部切换检测：本脚本的切换都会同步更新 LastSeenNow（成功/恢复/回滚三处），
  # 这里对不上即非本脚本所为（多为 Verge 手动切换，9-18 14:20→14:40 即一例），
  # 打一条 INFO 留痕，避免复盘时误以为看门狗切的。首轮 LastSeenNow 为空只记录不告警。
  if (-not [string]::IsNullOrWhiteSpace($script:LastSeenNow) -and $now -ne $script:LastSeenNow) {
    Write-WdLog "检测到 Selector 当前节点变化（非本脚本切换，疑似手动切换）：$($script:LastSeenNow) -> $now" 'INFO'
  }
  $script:LastSeenNow = $now
  $nodeEntry = $null
  try { $nodeEntry = $map.PSObject.Properties[$now].Value } catch { }
  $nodeAlive = $false
  if ($nodeEntry -ne $null) { try { $nodeAlive = [bool](Get-Prop $nodeEntry 'alive') } catch { } }
  $causeShort = Shorten $cause 200
  $patrolLine = "巡检：selector now=$now alive=$nodeAlive all_down=$allDown verdict=$verdict cooling=${cooling}s recommend=$recommend cause=$causeShort"
  $inRoundCooldown = (Get-Date) -lt $script:RoundCooldownUntil
  $isAbnormal = $inRoundCooldown -or $nodeEntry -eq $null -or -not $nodeAlive -or $allDown `
    -or $verdict -eq 'transport' -or $verdict -eq 'quota' -or $verdict -eq 'ratelimit' -or $verdict -eq 'cooling'
  if (-not $isAbnormal) {
    # 正常情况降噪：首轮与恢复后首轮立即打印，其余按 NormalLogMinutes 心跳，其间静默计数。
    # 但有 transport 前科且无成功证据时，ok 只是“冷却过期、无证据”，不是“恢复”
    # （网关 verdict=ok 仅表示无未过期冷却）：打中性行并保留计数与恢复标记，
    # 等成功证据（last_success_age_s 推进或节流探针通过）或慢衰减后再定。
    $pendingTransport = $script:TransportStrikes -gt 0
    $proven = $false
    if ($successAge -ne $null -and $script:StreakStart -ne [DateTime]::MinValue) {
      try { $proven = ((Get-Date).AddSeconds(-[int64]$successAge)) -ge $script:StreakStart } catch { $proven = $false }
    }
    if ($pendingTransport -and -not $proven) {
      Write-WdLog "$patrolLine（冷却过期尚未验证恢复，transport 计数保留 $($script:TransportStrikes)/$TransportConfirmRounds）" 'WARN'
      $script:NeedRecoveryLog = $true
      $script:NormalSkipped = 0
    } elseif ($script:NeedRecoveryLog) {
      $script:NeedRecoveryLog = $false
      $skipped = $script:NormalSkipped
      $script:NormalSkipped = 0
      $script:LastNormalLog = Get-Date
      if ($skipped -gt 0) { Write-WdLog "$patrolLine（已恢复正常，前累计 $skipped 轮正常）" 'OK' }
      else { Write-WdLog "$patrolLine（已恢复正常）" 'OK' }
    } else {
      $due = $script:LastNormalLog -eq [DateTime]::MinValue -or ((Get-Date) - $script:LastNormalLog).TotalMinutes -ge $NormalLogMinutes
      if ($due) {
        $skipped = $script:NormalSkipped
        $script:NormalSkipped = 0
        $script:LastNormalLog = Get-Date
        if ($skipped -gt 0) { Write-WdLog "$patrolLine（过去约 $NormalLogMinutes 分钟共 $skipped 轮正常，心跳）" }
        else { Write-WdLog $patrolLine }
      } else {
        $script:NormalSkipped++
      }
    }
  } else {
    Write-WdLog $patrolLine
    # 标记异常：下一次恢复正常时立即补一条，避免卡在心跳周期里延迟感知。
    # 同时清零静默计数：恢复/心跳行只反映本次异常之后的正常轮数，
    # 否则长时间异常后恢复行会把异常发生前攒的旧数报出来，易误导。
    $script:NeedRecoveryLog = $true
    $script:NormalSkipped = 0
  }

  if ($inRoundCooldown) {
    Write-WdLog "上一轮全部失败后的冷却中，跳过触发（$($script:RoundCooldownUntil.ToString('HH:mm:ss')) 后恢复）"
    return @{ Acted = $false }
  }
  # transport 慢衰减：单个 ok/ratelimit/cooling/quota 轮不再清零——冷却 15s
  # 过期节拍与 15s 轮询共振时连续计数注定永远攒不够（故障形态：1/3→2/3→ok→1/3…）。
  # 连续 4 轮（≈60s，超过 45s 最大退避）非 transport 才清零。网关不可达/Clash
  # 不可达的中断轮仍按旧语义立即清零（见上，“只认连续可读轮”）。
  # 非 transport 轮在这里只记 OkStreak：下面的节点失效/all_down 分支会直接返回，
  # 到不了 transport 计数块，OkStreak 照记不影响它们。
  if ($verdict -eq 'transport') { $script:OkStreak = 0 }
  else {
    $script:OkStreak++
    if ($script:OkStreak -ge 4) { $script:TransportStrikes = 0 }
  }
  if ($nodeEntry -eq $null -or -not $nodeAlive) {
    $r = Invoke-Failover -Reason "当前节点失效（now=$now）" -Uid $Uid -MaxCandidates $MaxCandidates `
      -SwitchGapSeconds $SwitchGapSeconds -VerifyTimeoutSeconds $VerifyTimeoutSeconds `
      -Verdict $verdict -RefreshSettleSeconds $RefreshSettleSeconds
    if (-not (Get-Prop $r 'Ok')) { $script:RoundCooldownUntil = (Get-Date).AddMinutes($RoundCooldownMinutes) }
    $script:TransportStrikes = 0
    return @{ Acted = $true }
  }
  if ($allDown) {
    $r = Invoke-Failover -Reason '网关传输全坏（proxies.all_down=true）' -Uid $Uid -MaxCandidates $MaxCandidates `
      -SwitchGapSeconds $SwitchGapSeconds -VerifyTimeoutSeconds $VerifyTimeoutSeconds `
      -Verdict $verdict -RefreshSettleSeconds $RefreshSettleSeconds
    if (-not (Get-Prop $r 'Ok')) { $script:RoundCooldownUntil = (Get-Date).AddMinutes($RoundCooldownMinutes) }
    $script:TransportStrikes = 0
    return @{ Acted = $true }
  }
  # transport 抖动容忍：cause=transport (connection failed) 大多是网络抖动，单轮
  # 不刷新不切换，需 TransportConfirmRounds 轮仍为 transport 才进入刷新切换。
  # 计数只在常驻循环的进程内存里累积（重启清零）；单个 ok/限流轮不再清零，
  # 改由上面的 OkStreak 慢衰减（连续 4 轮非 transport 才清），否则冷却 15s 过期
  # 节拍与 15s 轮询共振会把计数永远卡在 2/3；quota/all_down/节点失效仍走下面的
  # 立即路径，不受此计数影响。
  # 网关 stale 机制下 cooling=0 的 transport 同样是有效票（过期未验证≠恢复）。
  if ($verdict -eq 'transport') {
    if ($script:TransportStrikes -eq 0) { $script:StreakStart = Get-Date }
    $script:TransportStrikes++
    if ($script:TransportStrikes -lt $TransportConfirmRounds) {
      Write-WdLog "匿名通道 transport 疑似网络抖动，只观察不刷新不切换（连续 $($script:TransportStrikes)/$TransportConfirmRounds，cause=$causeShort）" 'WARN'
      return @{ Acted = $false }
    }
    $script:TransportStrikes = 0
  } elseif ($verdict -eq 'ok' -and $script:TransportStrikes -gt 0) {
    # streak 收敛后的 ok 轮：先看网关成功证据（last_success_age_s 在 streak 开始
    # 之后推进即真恢复，零配额），无证据才花一次节流探针（5 分钟最多一次，
    # 每次新 nonce 绕指纹冷却）。正常心跳 ok 轮（计数为 0）永不探。
    $provenOk = $false
    if ($successAge -ne $null -and $script:StreakStart -ne [DateTime]::MinValue) {
      try { $provenOk = ((Get-Date).AddSeconds(-[int64]$successAge)) -ge $script:StreakStart } catch { $provenOk = $false }
    }
    if ($provenOk) {
      $script:TransportStrikes = 0
      $script:OkStreak = 0
    } elseif ((Get-Date) - $script:LastStaleProbe -ge [TimeSpan]::FromMinutes(5)) {
      $script:LastStaleProbe = Get-Date
      try {
        $nonceOk = (Get-Date).ToString('yyyyMMdd-HHmmss') + '-' + (Get-Random -Maximum 999999)
        $vrOk = Test-GatewayNode -Nonce $nonceOk -TimeoutSec $VerifyTimeoutSeconds
        if ((Get-Prop $vrOk 'Ok')) {
          $script:TransportStrikes = 0
          $script:OkStreak = 0
          $script:NeedRecoveryLog = $false
          $script:NormalSkipped = 0
          $script:LastNormalLog = Get-Date
          Write-WdLog "$patrolLine（验证探针通过，已恢复正常）" 'OK'
          return @{ Acted = $false }
        }
        if ([string](Get-Prop $vrOk 'Class') -eq 'config') {
          Write-WdLog "ok 轮验证探针报网关配置错误：$(Shorten ([string](Get-Prop $vrOk 'Body')) 200)（先修网关配置，计数保留）" 'ERROR'
          return @{ Acted = $false }
        }
        Write-WdLog "ok 轮验证探针仍失败：status=$((Get-Prop $vrOk 'Status')) class=$((Get-Prop $vrOk 'Class'))，transport 计数保留 $($script:TransportStrikes)/$TransportConfirmRounds" 'WARN'
      } catch {
        Write-WdLog "ok 轮验证探针异常：$(Mask-Text $_.Exception.Message)（计数保留）" 'WARN'
      }
      return @{ Acted = $false }
    } else {
      Write-WdLog "ok 轮（验证探针节流中），transport 计数保留 $($script:TransportStrikes)/$TransportConfirmRounds" 'WARN'
      return @{ Acted = $false }
    }
  }
  if ($verdict -eq 'transport' -or $verdict -eq 'quota') {
    # 网关 verdict 是跨节点/跨指纹的最坏快照（见 maxCooldownSnapshot 注释），单个
    # 残留指纹的旧冷却也可能报 transport/quota。切前二次确认一次：verdict 已翻转
    # 为 ok/ratelimit/cooling 说明只是残留，不花配额、不切换直接返回。
    try {
      $wdConfirm = Get-WatchdogStatus
      $verdictConfirm = ''
      try { $verdictConfirm = [string](Get-Prop (Get-Prop $wdConfirm 'anonymous') 'verdict') } catch { }
      if ($verdictConfirm -ne '' -and $verdictConfirm -ne 'transport' -and $verdictConfirm -ne 'quota') {
        Write-WdLog "verdict 二次确认已翻转为 $verdictConfirm（旧快照残留），本轮不切换" 'OK'
        return @{ Acted = $false }
      }
    } catch {
      Write-WdLog "verdict 二次确认失败（沿用首轮 verdict=$verdict 继续）：$(Mask-Text $_.Exception.Message)" 'WARN'
    }
    $r = Invoke-Failover -Reason "匿名通道 verdict=$verdict cause=$causeShort" -Uid $Uid -MaxCandidates $MaxCandidates `
      -SwitchGapSeconds $SwitchGapSeconds -VerifyTimeoutSeconds $VerifyTimeoutSeconds `
      -Verdict $verdict -RefreshSettleSeconds $RefreshSettleSeconds
    if (-not (Get-Prop $r 'Ok')) {
      # 退避早退会带回 Retry-After：贴着它静默，别用固定的 RoundCooldownMinutes
      # ——匿名冷却封顶已经是 15s，固定 2 分钟会把冷却结束后的可行动作压掉约
      # 1 分 45 秒。其余失败路径（真切换过、全部候选失败）仍按分钟计。
      $raHint = [int](Get-Prop $r 'RetryAfter')
      if ($raHint -gt 0) {
        $secs = $raHint + 5
        if ($secs -gt ($RoundCooldownMinutes * 60)) { $secs = $RoundCooldownMinutes * 60 }
        $script:RoundCooldownUntil = (Get-Date).AddSeconds($secs)
        Write-WdLog "本轮因网关退避停手，静默 ${secs}s（Retry-After=${raHint}s + 5s 余量）" 'WARN'
      } else {
        $script:RoundCooldownUntil = (Get-Date).AddMinutes($RoundCooldownMinutes)
      }
    } elseif (-not $script:ProbeSupported) {
      # 验证成功可能经付费 key 回退（网关匿名失败后自动走 KeyTiers），此时匿名
      # 通道并未恢复；复查一次 verdict，若同类 verdict 仍在冷却中则加一段短冷却
      # 再观察，避免下轮 15s 后空转切换（每次切换还会断开全部连接）。
      # 经匿名成功的请求会清冷却，复查为 ok，本分支不触发，不影响正常路径。
      # 探针端点只测匿名通道、且探针不写池状态，所以「经付费 key 回退才 200」在
      # 探针上不存在，这段复查只对旧探针有意义。
      try {
        $wdAfter = Get-WatchdogStatus
        $anonAfter = Get-Prop $wdAfter 'anonymous'
        $verdictAfter = ''
        $coolingAfter = 0
        if ($anonAfter -ne $null) {
          try { $verdictAfter = [string](Get-Prop $anonAfter 'verdict') } catch { }
          try { $coolingAfter = [int64](Get-Prop $anonAfter 'cooling_seconds') } catch { }
        }
        if (($verdictAfter -eq 'transport' -or $verdictAfter -eq 'quota') -and $coolingAfter -gt 0) {
          if ($coolingAfter -lt 5) { $coolingAfter = 5 }
          # cooling_seconds 同样是最坏快照（含无关指纹残留），只取它做恢复观察
          # 窗口时封顶 120s：连续故障下不压住下一次恢复，多压无益。
          if ($coolingAfter -gt 120) { $coolingAfter = 120 }
          $script:RoundCooldownUntil = (Get-Date).AddSeconds($coolingAfter)
          Write-WdLog "验证虽通过但匿名通道仍 verdict=$verdictAfter（cooling=${coolingAfter}s），疑似经付费 key 回退成功，冷却 ${coolingAfter}s 后再观察" 'WARN'
        }
      } catch {
        Write-WdLog "成功后复查 verdict 失败：$(Mask-Text $_.Exception.Message)（不影响本轮成功结论）" 'WARN'
      }
    }
    return @{ Acted = $true }
  }
  if ($verdict -eq 'ratelimit' -or $verdict -eq 'cooling') {
    Write-WdLog "匿名通道限流中（verdict=$verdict cooling=${cooling}s），只等待不切换 cause=$causeShort" 'WARN'
  }
  return @{ Acted = $false }
}

function Invoke-DryRun {
  param([string]$Uid)
  Write-WdLog '===== 只读试运行（零配置写入，运行日志除外；零切换；零网关配额消耗） ====='
  # WARP proxy 检查预览：只做一次 SOCKS5 检查（不复检、不切换、不写状态）。
  $dryPlan = Resolve-ProxyCheckPlan
  $dryTarget = if ($dryPlan.Endpoint -ne '') { Format-ProxyEndpoint -Url $dryPlan.Endpoint } else { '(无)' }
  Write-WdLog "proxy-check 设置：模式=$($dryPlan.Mode) target=$dryTarget source=$($dryPlan.Source) 间隔=$($script:ProxyCheckIntervalSeconds)s 复检=$($script:ProxyCheckRecheckSeconds)s 超时=$($script:ProxyCheckTimeoutSeconds)s failover=$($script:ProxyCheckFailover) 预算=$($script:ProxyFailoverBudgetSeconds)s udp=$($script:ProxyCheckUdpTarget) require_udp=$($script:ProxyCheckRequireUdp) stale=$($dryPlan.Stale)"
  if ($dryPlan.ConfigError) {
    if (-not $dryPlan.ErrorLogged) {
      Write-WdLog ('proxy-check config error：' + (Shorten (Mask-Proxy (Mask-Text ([string]$dryPlan.Reason))) 200)) 'ERROR'
    }
    Write-WdLog 'proxy-check 因配置错误回退 legacy（DryRun 不做 WARP 检查）' 'WARN'
  } elseif ($dryPlan.Mode -eq 'warp-proxy') {
    $dryResult = Test-WarpProxyCheck -ProxyUrl $dryPlan.Endpoint -TimeoutSeconds $script:ProxyCheckTimeoutSeconds -RequireUdp $script:ProxyCheckRequireUdp -UdpTarget $script:ProxyCheckUdpTarget
    Write-WdLog ('proxy-check dry-run target=' + $dryTarget + ' class=' + $dryResult.Class + ' ok=' + $dryResult.Ok +
      ' tcp=' + $dryResult.TcpOk + ' udp=' + $dryResult.UdpOk + ' kind=' + $dryResult.Kind + ' stage=' + $dryResult.Stage +
      ' duration=' + $dryResult.DurationMs + 'ms reason=' + (Shorten (Mask-Proxy (Mask-Text ([string]$dryResult.Reason))) 200))
  } elseif ($dryPlan.Mode -eq 'warp-direct') {
    Write-WdLog 'proxy-check 直连 WARP（warp.proxy 为空）：不做 SOCKS5 检查；legacy 巡检仍可能切换 Clash 节点（已知风险）' 'WARN'
  } else {
    Write-WdLog 'proxy-check 未启用（legacy 巡检）'
  }
  # WARP 出口失败触发预览：只读 /healthz，不刷新订阅、不切换、不写任何配置。
  if ($script:WarpExitFailureEnabled) {
    $hzDry = Get-GatewayHealthz -Force
    if ([bool](Get-Prop $hzDry 'Ok')) {
      if ([bool](Get-Prop $hzDry 'HasWarp')) {
        Write-WdLog ("WARP 出口失败触发（DryRun 只读，不动作）：enabled=true threshold=$($script:WarpExitFailureThreshold) cooldown=$($script:WarpExitFailureCooldownMinutes)分钟 reset_rounds=$($script:WarpExitFailureResetRounds) action=$($script:WarpExitFailureAction) healthz=$(Get-Prop $hzDry 'Status') status=$(Get-Prop $hzDry 'WarpStatus') active=$(Get-Prop $hzDry 'WarpActive') standby=$(Get-Prop $hzDry 'WarpStandby') cooling=$(Get-Prop $hzDry 'WarpCooling') failure_streak=$(Get-Prop $hzDry 'FailureStreak') failures_total=$(Get-Prop $hzDry 'FailuresTotal') create_failures=$(Get-Prop $hzDry 'CreateFailuresTotal') last_failure_kind=$(Get-Prop $hzDry 'LastFailureKind') started_at=$(Get-Prop $hzDry 'StartedAt') last_success_at=$(Get-Prop $hzDry 'LastSuccessAt')")
      } else {
        Write-WdLog 'WARP 出口失败触发（DryRun 只读）：/healthz 无 warp 段（WARP 未启用），不会触发'
      }
    } else {
      Write-WdLog "WARP 出口失败触发（DryRun 只读）：/healthz 不可用（$(Get-Prop $hzDry 'Error')）"
    }
  } else {
    Write-WdLog 'WARP 出口失败触发：enabled=false（不读 /healthz）'
  }
  $wd = Get-WatchdogStatus
  $prox = Get-Prop $wd 'proxies'
  $anon = Get-Prop $wd 'anonymous'
  $wdCause = ''
  try { $wdCause = Shorten ([string](Get-Prop $anon 'cause')) 200 } catch { }
  Write-WdLog "网关看门狗：status=$(Get-Prop $wd 'status') healthy=$(Get-Prop $prox 'healthy')/$(Get-Prop $prox 'total') all_down=$(Get-Prop $prox 'all_down') verdict=$(Get-Prop $anon 'verdict') cooling=$(Get-Prop $anon 'cooling_seconds') recommend=$(Get-Prop $wd 'recommend_switch') cause=$wdCause"
  $sel = [uri]::EscapeDataString($script:Selector)
  try { $detail = Invoke-ClashGet -Path "/proxies/$sel" }
  catch {
    if ([string]$_.Exception.Message -match '404|NotFound') {
      $availTxt = '（读不到）'
      try {
        $avail = @(Get-SelectorGroupNames -ProxyMap (Get-ProxyMap))
        if (@($avail).Count -gt 0) { $availTxt = (@($avail) -join ' / ') }
      } catch { }
      throw "Selector [$($script:Selector)] 不在当前 Clash 配置里（404）：候选 [$($script:SelectorCandidates -join ' / ')] 均未命中；当前可用的 Selector 组：$availTxt"
    }
    throw
  }
  $now = [string](Get-Prop $detail 'now')
  $rawAll = Get-Prop $detail 'all'
  $all = @($rawAll | Where-Object { $_ -ne $null -and $_ -ne '' })
  Write-WdLog "Selector=$($script:Selector) now=$now 节点总数=$(@($all).Count)"
  $map = Get-ProxyMap
  $cands = Get-RankedCandidates -ProxyMap $map -SelectorAll $all -ExcludeNow $now
  $exclTxt = if (@($script:ExcludeContains).Count -gt 0) { @($script:ExcludeContains) -join '/' } else { '已关闭' }
  $dryAlive = @($cands | Where-Object { $_.Alive -and $_.Delay -gt 0 }).Count
  if ($dryAlive -eq 0 -and @($cands).Count -gt 0) {
    # 与正式切换路径同一条告警：DryRun 正是用来判断「到底有没有测速数据」的场合。
    Write-WdLog '候选全部没有测速数据（Clash 尚未对这些节点做过 url-test）：实际切换会按名字序试候选，不是按延迟；开一次 Verge 的组测速可改善' 'WARN'
  }
  Write-WdLog "候选（生效子串排除=$exclTxt；另有内置精确排除：非叶子组/占位节点；按延迟(+地区/quota罚分)排序）共 $(@($cands).Count) 个，Top10："
  $i = 0
  foreach ($c in @($cands | Select-Object -First 10)) {
    $i++
    # quota 记忆是进程内存：DryRun 是新进程、记忆恒为空，quota 标记恒不显示属预期；
    # 地区降权是参数即时计算，DryRun 同样可见；显示代码与正式路径同源，留着无成本。
    $demoteMark = Get-DemoteMark $c
    Write-WdLog ("  {0,2}. {1}  delay={2}ms alive={3}{4}" -f $i, $c.Name, $c.Delay, $c.Alive, $demoteMark)
  }
  # 本地配置（type: local）没有订阅地址：不是错误，跳下载预览即可。
  # uid 失效要和「本地配置」分开报：否则先打一句「本地配置」，下一行又抛「找不到 uid」，
  # 两句话自相矛盾。
  $info = $null
  try { $info = Get-ProfileBlock -Uid $Uid } catch { $info = $null }
  $hasUrl = ($info -ne $null) -and [regex]::IsMatch($info.Block, '(?m)^\s*url:\s*\S+')
  if ($info -eq $null) {
    Write-WdLog "profiles.yaml 里找不到 uid [$Uid]：跳过订阅差异预览与 selected 检查（逐节点切换仍可用）" 'WARN'
  } elseif (-not $hasUrl) {
    Write-WdLog "本地配置（uid=$Uid，type=$(Get-ProfileType -Block $info.Block)；无 url 行）：跳过订阅差异预览；逐节点切换照常可用" 'INFO'
  } else {
    $url = Get-SubscriptionUrl -Uid $Uid
    Write-WdLog "订阅地址：$(Mask-Url $url)"
    # DryRun 只读：下载走正式刷新同一逻辑（含 7890 兜底），下面只做差异预览，不写任何文件。
    $dl = Get-SubscriptionYaml -Url $url -UaOverride $SubscriptionUA
    if (-not (Get-Prop $dl 'Ok')) { throw "订阅下载失败：$((Get-Prop $dl 'Error'))" }
    $newYaml = [string](Get-Prop $dl 'Yaml')
    $dryLen = $newYaml.Length
    Write-WdLog "订阅下载 OK（$dryLen 字节，DryRun 不写入）" 'OK'
    if (-not ([regex]::IsMatch($newYaml, '(?m)^(proxies|proxy-providers):'))) { throw '订阅内容无 proxies 节' }
    $newNames = Get-NodeNames -YamlText $newYaml
    $runtimeFile = Join-Path $script:VergeRoot 'clash-verge.yaml'
    $oldNames = Get-NodeNames -YamlText ([IO.File]::ReadAllText($runtimeFile, [Text.Encoding]::UTF8))
    $added = @($newNames | Where-Object { @($oldNames) -notcontains $_ })
    $removed = @($oldNames | Where-Object { @($newNames) -notcontains $_ })
    Write-WdLog "订阅差异预览：旧 $(@($oldNames).Count) → 新 $(@($newNames).Count)，新增 $(@($added).Count)，消失 $(@($removed).Count)"
    if (@($added).Count -gt 0) { Write-WdLog "新增：$(($added | Select-Object -First 10) -join ' / ')" }
    if (@($removed).Count -gt 0) { Write-WdLog "消失：$(($removed | Select-Object -First 10) -join ' / ')" }
    $runtimeOld = [IO.File]::ReadAllText($runtimeFile, [Text.Encoding]::UTF8)
    try {
      $merged = Sync-RuntimeText -Runtime $runtimeOld -Profile $newYaml
      if ($merged -eq $runtimeOld) { Write-WdLog '运行配置拼接预览：与当前一致（无需改动）' }
      else { Write-WdLog "运行配置拼接预览：会变更（旧 $($runtimeOld.Length) 字 → 新 $($merged.Length) 字），DryRun 不写入" }
    } catch {
      Write-WdLog "运行配置拼接预览：拒绝写入（$($_.Exception.Message)），正好验证了防护逻辑" 'WARN'
    }
  }
  if ($info -ne $null) {
    $selName = [regex]::Escape($script:Selector)
    if ([regex]::IsMatch($info.Block, "(?ms)^\s*-\s*name:\s*['""]?$selName['""]?\s*\r?\n.*?^\s*now:\s*.*?$")) {
      Write-WdLog 'profiles.yaml selected.now 定位 OK（DryRun 不写入）'
    } elseif ([regex]::IsMatch($info.Block, '(?m)^\s*selected:')) {
      Write-WdLog "profiles.yaml 的 selected 里还没有 selector [$($script:Selector)]：下次切换会在列表头部追加一项（DryRun 不写入）" 'INFO'
    } else {
      Write-WdLog "profiles.yaml 里还没有 selector [$($script:Selector)] 的 selected 块：首次成功切换时会自动补写（DryRun 不写入）" 'INFO'
    }
  }
  Write-WdLog '===== 试运行结束：一切只读检查通过 =====' 'OK'
}

$script:LastSwitch = [DateTime]::MinValue
$script:LastRefresh = [DateTime]::MinValue
$script:RoundCooldownUntil = [DateTime]::MinValue
$script:LastNormalLog = [DateTime]::MinValue
$script:NormalSkipped = 0
$script:NeedRecoveryLog = $false
# 近期 quota 失败记忆：节点名 -> 失败时间（进程内存，重启清空），见 -QuotaFailMemoryMinutes。
$script:QuotaFailedAt = @{}
# 上次打印的过滤丢弃集合签名（降噪用）与上次见到的 Selector 当前节点（外部切换检测用）。
$script:LastDropSignature = $null
$script:LastSeenNow = ''
# Clash 接口错误签名（selector 404 / 连不上各只报一次）与 selected.now 写失败签名
# （uid 失效时一轮 100 个候选会各报一条相同的 WARN）。
$script:LastClashErrSig = ''
$script:SelectedWarnSig = ''
# transport 计数（抖动容忍用）：verdict=transport 累计 N 轮才动作，见 Invoke-WatchIteration。
# 单个 ok/限流轮不再清零，改由 OkStreak 慢衰减（连续 4 轮非 transport 才清），
# 否则冷却过期节拍与轮询共振会把计数永远卡在 2/3；网关 stale 机制下 cooling=0
# 的 transport 同样是有效票。StreakStart 记本轮 streak 起点，供 last_success_age_s
# 判定“成功是否发生在 streak 之后”；LastStaleProbe 节流 ok 轮验证探针（5 分钟一次）。
$script:TransportStrikes = 0
$script:OkStreak = 0
$script:StreakStart = [DateTime]::MinValue
$script:LastStaleProbe = [DateTime]::MinValue
# WARP 出口失败触发状态（进程内存，重启清零）：
#   StartedAt/LastSuccessAt  网关 /healthz 的 warp 字段，用于识别重启与真实成功
#   LastSuccessLogAt         最近一次成功日志时间；成功日志节流用
#   SuccessLogIntervalMinutes 无失败状态时成功日志的最小间隔（默认 5 分钟）
#   LastSwitchSeen           $script:LastSwitch 的上次观察值（任何切换都清零基线）
#   BaselineStreak           动作/切换后的连击基线，需再涨 threshold 才重复触发
#   ZeroRounds               连击连续归零的轮数（达 reset_rounds 才清基线）
#   LocalStrikes             副信号：failure_streak=0 但出口池持续不可用的连续轮数
#   LocalGraceRounds         动作后的前 N 轮不累计副信号（等 WARP 重新注册）
#   CooldownUntil            本触发器自己的冷却，不碰 RoundCooldownUntil
#   HealthBackoffUntil        /healthz 失败后的 60s 退避
#   NoCountersWarned         旧网关缺 failure_streak 的兼容警告只打一次（F1）
#   FirstObservation         进程/网关重启后的首次有效观察，用于历史连击基线（F2）
$script:WarpExitNoCountersWarned = $false
$script:WarpExitFirstObservation = $true
$script:WarpExitStartedAt = ''
$script:WarpExitLastSuccessAt = ''
$script:WarpExitLastSuccessLogAt = [DateTime]::MinValue
$script:WarpExitSuccessLogIntervalMinutes = 5
$script:WarpExitLastSwitchSeen = $null
$script:WarpExitBaselineStreak = [int64]0
$script:WarpExitZeroRounds = 0
$script:WarpExitCooldownUntil = [DateTime]::MinValue
$script:WarpExitLastObservedStreak = [int64]0
$script:WarpExitLocalStrikes = 0
$script:WarpExitLocalGraceRounds = 0
$script:WarpExitWaitLoggedStreak = -1
$script:WarpExitNoWarpLogged = $false
$script:WarpExitHealthBackoffUntil = [DateTime]::MinValue
$script:WarpExitHealthErrSig = ''
$script:WarpExitHealthErrLogAt = [DateTime]::MinValue
$script:WarpExitTriggerErrSig = ''

try {
  # watchdog.json 只读且不依赖 Clash/网关：必须在实例互斥与所有 Resolve-* 之前读取，
  # 这样 -ProxyCheckProbeOnly 能在常驻看门狗运行时执行（design §0.1/M1）。
  try { Read-WatchdogConfig -Path $script:WatchdogConfigPath }
  catch {
    if ($ProxyCheckProbeOnly) {
      Write-WdLog ('proxy-check config error：' + (Mask-Proxy (Mask-Text $_.Exception.Message))) 'ERROR'
      exit 2
    }
    throw
  }
  if ($ProxyCheckProbeOnly) {
    # 独立诊断入口：只依赖 watchdog.json + config.json 的 warp.proxy（或 -ProxyCheckEndpoint），
    # 不读 Clash API/secret/profile，不获取网关 key，不申请互斥，不切换、不写配置/状态。
    $probePlan = Resolve-ProxyCheckPlan
    if ($probePlan.ConfigError) {
      if (-not $probePlan.ErrorLogged) {
        Write-WdLog ('proxy-check config error：' + (Shorten (Mask-Proxy (Mask-Text ([string]$probePlan.Reason))) 200)) 'ERROR'
      }
      exit 2
    }
    if ($probePlan.Mode -ne 'warp-proxy') {
      if (-not $probePlan.ErrorLogged) {
        Write-WdLog ('proxy-check config error：没有检查目标（mode=' + $probePlan.Mode + '，reason=' + $probePlan.Reason + '）；请设置 config.json 的 warp.proxy 或传 -ProxyCheckEndpoint') 'ERROR'
      }
      exit 2
    }
    $probeResult = Test-WarpProxyCheck -ProxyUrl $probePlan.Endpoint -TimeoutSeconds $script:ProxyCheckTimeoutSeconds -RequireUdp $script:ProxyCheckRequireUdp -UdpTarget $script:ProxyCheckUdpTarget
    Write-WdLog ('proxy-check probe-only target=' + (Format-ProxyEndpoint -Url $probePlan.Endpoint) +
      ' class=' + $probeResult.Class + ' ok=' + $probeResult.Ok + ' tcp=' + $probeResult.TcpOk +
      ' udp=' + $probeResult.UdpOk + ' kind=' + $probeResult.Kind + ' stage=' + $probeResult.Stage +
      ' duration=' + $probeResult.DurationMs + 'ms reason=' + (Shorten (Mask-Proxy (Mask-Text ([string]$probeResult.Reason))) 200))
    if ($probeResult.Class -eq 'config') { exit 2 }
    if ($probeResult.Ok) { exit 0 }
    exit 1
  }
  # 全局命名互斥保证单实例：start.ps1 常驻 + 手动再起一个会并发写三文件、互覆
  # 节流窗口并重复切换，拿到锁才往下走，拿不到直接退出（-Once 手动检查也一样，
  # 避免与常驻实例互相干扰）。Global 命名空间需提权，普通用户建不出来时降级
  # 到 Local（同会话防双开仍有效，覆盖“本机单用户跑网关+看门狗”的主流形态）；
  # 上次崩溃未释放（AbandonedMutex）视为已拿到锁继续，不算失败。
  $script:InstanceMutex = $null
  $mutexName = 'Global\opencode2api-clash-watchdog'
  try {
    $script:InstanceMutex = New-Object System.Threading.Mutex($false, $mutexName)
  } catch {
    Write-WdLog "全局互斥创建失败（可能非管理员），降级到会话级互斥：$(Mask-Text $_.Exception.Message)" 'WARN'
    $mutexName = 'Local\opencode2api-clash-watchdog'
    $script:InstanceMutex = New-Object System.Threading.Mutex($false, $mutexName)
  }
  $gotLock = $false
  try {
    $gotLock = $script:InstanceMutex.WaitOne(0)
  } catch [System.Threading.AbandonedMutexException] {
    $gotLock = $true
  }
  if (-not $gotLock) {
    Write-WdLog '已有看门狗实例在运行，本实例直接退出（避免并发写配置与重复切换）' 'ERROR'
    exit 1
  }
  Resolve-ClashSecret | Out-Null
  # profile uid 为空 = 跟随 profiles.yaml 的 current:（重新导入本地 yaml 会换 uid）。
  # 解析失败**不中止启动**：uid 只影响「订阅刷新 + selected.now 书签」，逐节点切换、
  # 探针、回滚全都不需要它。用 bookkeeping 的依赖否决核心功能是反的——真解析不出来时
  # 让 Refresh-Subscription 每轮自行再试（配置重新导入后 current 就有了）。
  $script:UidAuto = [string]::IsNullOrWhiteSpace($script:SubscriptionUid)
  if ($script:UidAuto) {
    try {
      $script:SubscriptionUid = Resolve-ActiveProfileUid
      Write-WdLog "profile uid 未指定，自动取当前激活配置：$($script:SubscriptionUid)"
    } catch {
      $script:SubscriptionUid = ''
      Write-WdLog "无法自动解析当前激活 profile（$($_.Exception.Message)）：先跳过订阅刷新与 selected 记录，逐节点切换照常；稍后每轮自动重试，也可用 -SubscriptionUid 指定" 'WARN'
    }
  }
  $regionCfg = '关闭'
  # selector 候选解析：取第一个在当前配置里存在的 Selector 组（本地节点池 ↔ 渔云组名不同）。
  # 必须在横幅之前，横幅要如实打印最终选中的组；解析失败不中止启动（Clash 可能还没起），
  # 每轮的 404 分支会再挑一次。
  try {
    $picked = Resolve-SelectorCandidate -Candidates $script:SelectorCandidates
    if ($picked) {
      if ($picked -ne $script:Selector) {
        Write-WdLog "selector 候选命中：$picked（[$($script:Selector)] 不在当前配置里）" 'INFO'
      }
      $script:Selector = $picked
    } else {
      $avail = @(Get-SelectorGroupNames -ProxyMap (Get-ProxyMap))
      $availTxt = if (@($avail).Count -gt 0) { (@($avail) | Select-Object -First 10) -join ' / ' } else { '（读不到）' }
      Write-WdLog "selector 候选都不在当前 Clash 配置里（候选：$($script:SelectorCandidates -join ' / ')）：每轮会报 404，直到切回含这些组的配置；当前可用的 Selector 组：$availTxt" 'ERROR'
      $script:Selector = $script:SelectorCandidates[0]
    }
  } catch {
    Write-WdLog "启动时解析 selector 失败（Clash 未启动？先用候选第一个 [$($script:Selector)]，每轮会重试）：$($_.Exception.Message)" 'WARN'
  }
  # 与 Get-RankedCandidates 同口径：先过滤空白条目再计数，全空白列表视为关闭，
  # 日志与行为读同一份 $script: 源，避免一侧被重赋值后分叉。
  $regionKeysForLog = @($script:DeprioritizeContains | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
  if ($script:DeprioritizePenaltyMs -gt 0 -and @($regionKeysForLog).Count -gt 0) { $regionCfg = "$($regionKeysForLog -join '/')+$($script:DeprioritizePenaltyMs)ms" }
  $excludeCfg = if (@($script:ExcludeContains).Count -gt 0) { @($script:ExcludeContains) -join '/' } else { '已关闭' }
  Write-WdLog "看门狗启动：selector=$($script:Selector) uid=$($script:SubscriptionUid) 候选上限=$($script:MaxCandidates) 网关=$($script:GatewayBase) 模型=$VerifyModel 轮询=${PollSeconds}s 正常心跳=${NormalLogMinutes}分钟 quota记忆=${QuotaFailMemoryMinutes}分钟 transport连续确认=${script:TransportConfirmRounds}轮 探针通道=$(if ($script:ProbeSupported) { 'watchdog-probe' } else { 'legacy' }) 闸门等待=${ProxyReadySeconds}s 刷新等待=${RefreshSettleSeconds}s 地区降权=${regionCfg} 子串排除=${excludeCfg}"
  $bannerPlan = Resolve-ProxyCheckPlan
  $bannerTarget = if ($bannerPlan.Endpoint -ne '') { Format-ProxyEndpoint -Url $bannerPlan.Endpoint } else { '(无)' }
  Write-WdLog "proxy-check 模式=$($bannerPlan.Mode) target=$bannerTarget source=$($bannerPlan.Source) 间隔=$($script:ProxyCheckIntervalSeconds)s 复检=$($script:ProxyCheckRecheckSeconds)s 超时=$($script:ProxyCheckTimeoutSeconds)s failover=$($script:ProxyCheckFailover) 预算=$($script:ProxyFailoverBudgetSeconds)s udp=$($script:ProxyCheckUdpTarget) require_udp=$($script:ProxyCheckRequireUdp)"
  $wefBanner = if ($script:WarpExitFailureEnabled) {
    "enabled threshold=$($script:WarpExitFailureThreshold) cooldown=$($script:WarpExitFailureCooldownMinutes)分钟 reset_rounds=$($script:WarpExitFailureResetRounds) action=$($script:WarpExitFailureAction)"
  } else { 'disabled' }
  Write-WdLog "WARP 出口失败触发=$wefBanner（中继健康时只读 /healthz；副信号本地连续轮数仅常驻循环推进）"
  if ($DryRun) {
    Invoke-DryRun -Uid $script:SubscriptionUid
    exit 0
  }
  if ($Once) {
    # -Once 是显式诊断入口：配置错误必须 exit 2（§0.9 #5/§11.3），不能像常驻循环那样回退 legacy。
    if ($bannerPlan.ConfigError) {
      if (-not $bannerPlan.ErrorLogged) {
        Write-WdLog ('proxy-check config error：' + (Shorten (Mask-Proxy (Mask-Text ([string]$bannerPlan.Reason))) 200)) 'ERROR'
      }
      exit 2
    }
    # -Once 只有一轮攒不出连续计数：传 1 保留单轮即动作的老语义（仍受内部二次确认门控）；
    # 常驻循环仍用 TransportConfirmRounds 轮防抖。
    if ($script:TransportConfirmRounds -ne 1) {
      Write-WdLog "-Once 单轮模式固定按 1 轮执行，不使用 transport 连续确认轮数（横幅里的 $($script:TransportConfirmRounds) 轮是配置值，本轮不用它）" 'INFO'
    }
    $onceResult = Invoke-WatchIteration `
      -MaxCandidates $script:MaxCandidates -SwitchGapSeconds 0 -VerifyTimeoutSeconds $VerifyTimeoutSeconds `
      -Uid $script:SubscriptionUid -RoundCooldownMinutes $RoundCooldownMinutes -NormalLogMinutes $NormalLogMinutes -RefreshSettleSeconds $RefreshSettleSeconds `
      -TransportConfirmRounds 1
    # legacy 返回 @{Acted}；WARP 模式额外带 ConfigError/ConfirmedDown/ActionFailed。固定退出码：
    # 2=config；1=确认 down 但未切（failover 关闭/非本机 auto/cooldown）、failover 失败，
    # 或 WARP 出口触发动作尝试过但失败（切换失败/实际刷新失败）；0=无动作/健康/抖动恢复/
    # 切换成功/刷新成功/节流或本地跳过。legacy 的 0/1 语义不变。
    if ((Get-Prop $onceResult 'ConfigError')) { exit 2 }
    if ((Get-Prop $onceResult 'ActionFailed')) { exit 1 }
    if ((Get-Prop $onceResult 'ConfirmedDown') -and -not (Get-Prop $onceResult 'Acted')) { exit 1 }
    if ($onceResult -ne $null -and $onceResult.Acted -and (Get-Date) -lt $script:RoundCooldownUntil) { exit 1 }
    exit 0
  }
  while ($true) {
    try {
      Invoke-WatchIteration `
        -MaxCandidates $script:MaxCandidates -SwitchGapSeconds $SwitchGapSeconds -VerifyTimeoutSeconds $VerifyTimeoutSeconds `
        -Uid $script:SubscriptionUid -RoundCooldownMinutes $RoundCooldownMinutes -NormalLogMinutes $NormalLogMinutes -RefreshSettleSeconds $RefreshSettleSeconds `
        -TransportConfirmRounds $script:TransportConfirmRounds | Out-Null
    } catch {
      Write-WdLog "本轮异常：$($_.Exception.Message)" 'WARN'
    }
    # WARP proxy 模式用 proxy_check_interval_seconds；每轮都按当轮重算的 WarpProxyActive 选择。
    if ($script:WarpProxyActive) { Start-Sleep -Seconds $script:ProxyCheckIntervalSeconds }
    else { Start-Sleep -Seconds $PollSeconds }
  }
} catch {
  Write-WdLog "启动失败：$($_.Exception.Message)" 'ERROR'
  exit 1
}
