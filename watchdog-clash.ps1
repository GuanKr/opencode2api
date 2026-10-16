<#
.SYNOPSIS
  Clash Verge (verge-mihomo) 节点自动切换看门狗 — 被动式(A 跑法)。

.DESCRIPTION
  每隔 PollSeconds 秒做一次只读巡检，只在命中以下信号时才动作：
    1) 当前 Selector 节点在 Mihomo 侧 alive=false / 消失        → 刷新后原地复测一次，仍坏才切换
    2) 网关 /v1/watchdog/status 报 proxies.all_down=true（传输全坏） → 刷新后原地复测一次，仍坏才切换
    3) 网关 /v1/watchdog/status 报 anonymous.verdict=transport（传输坏，常见为 cause=transport (connection failed) 网络抖动） → 累计 TransportConfirmRounds 轮（默认 2）仍坏才刷新，刷新后原地复测一次，仍坏才切换（单个 ok/限流轮不再清零，连续 4 轮非 transport 才衰减；网关冷却过期后 5 分钟内仍报 transport，防“两次失败→过期→误报恢复”死循环）
    4) 网关 /v1/watchdog/status 报 anonymous.verdict=quota（额度尽） → 与上面三类同样先刷新，刷新后原地复测一次（刷新换的是同名节点背后的出口 IP，额度可能已恢复），仍坏才切换；探针走网关的 /v1/watchdog/probe 时只测匿名通道、不回退付费 key，复测 200 就等于这条出口 IP 的匿名通道真恢复了（旧探针 legacy 才需要再复查 verdict 翻离 quota）
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
  探针通过即结束，失败则按延迟从低到高试下一个候选，最多 MaxCandidates 个。
  注意最坏单轮耗时：MaxCandidates(默认20) × VerifyTimeoutSeconds(默认120s) + SwitchGapSeconds ≈ 40 分钟，
  外加每个候选最多 ProxyReadySeconds(默认5s)+10s（闸门等待里最后一次 status 调用自身带 10s 超时）；
  刷新后的原地复测另加最多 1 × VerifyTimeoutSeconds（quota 轮现在也计入），刷新本身还有下载+校验+热重载，
  同步阻塞巡检循环属设计如此（不是卡死），调参前请知悉。
  进入切换前先刷新一次订阅(受 RefreshCooldownMinutes 节流，每轮最多 1 次)，
  再基于最新节点列表選取，避免旧列表里只剩高延迟节点可换，
  transport/all_down/节点失效/quota 四类在刷新成功后先等 RefreshSettleSeconds 秒，
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
  网关侧新增两个脚本专用接口（server_keys 鉴权，不动路由与配置结构）：
  GET /v1/watchdog/status 是只读 decision 快照；POST /v1/watchdog/probe 是强制真发上游的探针
  （忽略代理健康闸门与冷却、只测匿名通道、不写任何池状态）。切换 Selector 即换出口 IP，网关零重启生效。

  并发与密钥说明：脚本用全局命名互斥保证单实例（双开时后者直接退出），避免并发写三文件与重复切换；
  网关 server key 与推理 key 同权（可打全量推理），脚本只放网关本机、config.json 用文件 ACL 收紧，长期考虑只读 token 或回环绑定。
  每次切换成功后 DELETE /connections 是有意的：网关到 Clash 的 keep-alive 连接会钉住旧出口路由，
  不掐断则 big-pickle 验证可能走旧路由，把可用的新节点误判为坏节点；代价是每次切换掐断全部连接（含非网关流量）。

  筛选规则(对应截图要求，按最新确认)：
    只排除：中国大陆节点 + 流量/到期等信息占位节点 + 自动选择/DIRECT/故障转移这类非叶子；
    香港/台湾节点已按要求移出排除表——出口 IP 同样是境外 IP，匿名额度按出口 IP 计，
    不构成必须避开的理由；要临时排除见下方 watchdog.json；
    红框内两个占位节点（节点超时…/Gemini解锁请使用VLESS节点）用精确匹配排除，
    避免子串误杀 Gemini解锁类真实节点；
    日本直连 / 日本01 境外中转这类保留——它们仍是代理节点，出口 IP 仍是所标国家，
    “直连”指机场直连服务器，“境外中转”指经中转入口再出境，只是接入路径不同；
    无国家名的其余节点作为兜底候选排最后（默认选国家节点，不会无声丢弃）；
    地区降权（默认新加坡 +500ms）：延迟低但不稳定的地区只降排序、不排除，保底仍可选；
    排序：延迟从低到高；当前节点永不复选(换 IP 才有意义)。

  看门狗专用配置文件（-WatchdogConfig，默认脚本同目录 watchdog.json，只读从不写回）：
  这两项不再需要改脚本源码，直接在配置文件里增删，支持 // 与 /* */ 注释：
    {
      "exclude_contains": ["剩余","到期","重置","流量","套餐","官网","中国"],
      "deprioritize": { "contains": ["新加坡"], "penalty_ms": 500 }
    }
  语义：exclude_contains / deprioritize.contains 是整体替换内置默认值（不是追加），
  [] 即关闭该项；省略的键仍用内置默认值。deprioritize.penalty_ms 省略即用 500。
  优先级：显式命令行参数 > 配置文件 > 内置默认值——所以临时 -DeprioritizeContains
  试一把仍然压过文件，不必先改文件。exclude_contains 没有对应的命令行参数，
  它写错了只能改文件或把 -WatchdogConfig 指向别的路径。
  校验从严：未知键、类型不对（该给数组却给字符串/null）、空文件一律报错中止启动，
  不静默回退默认值——悄悄丢掉排除规则会把节点切到本该避开的地区，比直接报错危险。
  只在启动时读一次，改完 watchdog.json 必须重启看门狗才生效。
  新建文件请从 watchdog.example.json 复制；未创建则全部走内置默认值，行为与从前一致。

  本脚本会写的文件(试运行 -DryRun 除外，-DryRun 除运行日志外零写入)：
    - profiles/<uid>.yaml        订阅刷新时覆盖(先备份 .bak-yyyyMMdd-HHmmss)
    - profiles.yaml              订阅刷新时更新 extra/updated；每次切换成功后更新 selected.now
    - clash-verge.yaml           订阅刷新时同步 proxies/proxy-groups/rules 等节(先备份)
    - .tmp/clash-watchdog.log    运行日志(密钥/订阅 token 自动脱敏)
  只读不写的文件：
    - watchdog.json              看门狗专用配置(排除/降权规则)，默认取脚本同目录，-WatchdogConfig 可改路径
    - config.json                只读 server_keys[0]

  密钥来源(绝不落盘、不打印)：Clash secret 从 clash-verge.yaml 读取；
  网关 server key 从本目录 config.json 的 server_keys[0] 读取；也可用参数显式传入。

.PARAMETER DryRun
  只读试运行：打印看门狗快照（verdict/cause）、候选排名 Top10、订阅差异预览，除运行日志外不写任何配置文件、不切换、不验证(零网关配额消耗，订阅下载仍耗机场流量)。

.PARAMETER WatchdogConfig
  看门狗专用配置文件路径，默认脚本同目录 watchdog.json。文件不存在即全部用内置默认值；
  存在则用其中的 exclude_contains（子串排除）与 deprioritize（地区降权）覆盖对应内置默认值，
  省略的键不受影响。deprioritize 的两个键在显式传同名命令行参数时以命令行优先，
  exclude_contains 没有对应参数、写错只能改文件或换路径。只在启动时读一次，改完需重启看门狗。

.PARAMETER ProbeEndpoint
  验证探针走哪条通道。auto（默认）= 打网关的 POST /v1/watchdog/probe：它保证请求真的发到上游
  （忽略代理传输健康闸门与冷却）、不写任何池状态、并直接给出机器可读的 verdict；网关还没有这个
  端点时本进程自动回落旧的 /v1/chat/completions 探针并打一条 WARN。legacy = 强制旧探针。
  旧探针拿到的 502「all upstream attempts failed」无法区分「网关根本没发」和「发了被拒」，
  新探针则两种情况都能如实分类。
  已知副作用（探针通道独有）：网关自撰的冷却 429 永远不出现，所以「网关在退避 → 贴 Retry-After
  早退」这条路在探针通道下不可达；全候选失败只能退回 RoundCooldownMinutes(默认5分钟) 静默，
  而匿名冷却封顶才 15s。这是刻意的——探针的 429 一定是上游的，换出口 IP 才是唯一解法。

.PARAMETER ProxyReadySeconds
  切完节点、真正打探针之前，最多等多少秒让网关恢复代理传输健康闸门
  （/v1/watchdog/status 的 proxies.healthy 回到 proxies.total 就立刻继续）。只在闸门确实关着时
  花时间，默认 5，0 = 不等。注意这不是「探针能不能发出去」的前提——探针会穿透闸门；它只是少让
  「切换后首次拨号超时被判坏」的瞬时状态白烧一个候选和一次额度。

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
  [string]$Selector = '渔云CloudFisher',
  [string]$GatewayBase = 'http://127.0.0.1:19880',
  [string]$GatewayKey = '',
  [string]$VerifyModel = 'big-pickle',
  [ValidateRange(5, 300)][int]$PollSeconds = 15,
  [ValidateRange(1, 25)][int]$MaxCandidates = 20,
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
  [ValidatePattern('^[A-Za-z0-9_-]+$')][string]$SubscriptionUid = 'RYni6JpVUPX8',
  [string]$SubscriptionUA = 'clash-verge/v2.5.2',
  [int]$RefreshCooldownMinutes = 5,
  [int]$RoundCooldownMinutes = 5,
  # 近期 quota 失败记忆窗口（分钟）：验证时返回 quota 的节点在此窗口内候选降权，
  # 过期自动恢复——该机场同名背后会换 IP，永久拉黑会把已恢复的新 IP 挡在门外。
  # 0 = 关闭降权（不记录不降权）。
  [ValidateRange(0, 120)][int]$QuotaFailMemoryMinutes = 20,
  [ValidateRange(0, 60)][int]$RefreshSettleSeconds = 5,
  # transport 连续确认轮数：verdict=transport（多为网络抖动，如 transport (connection failed)）需连续 N 轮仍坏才刷新切换；
  # quota/all_down/节点失效不受此限制，仍立即动作。
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
$script:Selector = $Selector
$script:GatewayBase = $GatewayBase.TrimEnd('/')
$script:GatewayKey = $GatewayKey
$script:VerifyModel = $VerifyModel
$script:ProbeEndpoint = $ProbeEndpoint
# 运行时可降级：探针端点 404 一次就把本进程永久切回旧探针，不必每轮再试。
$script:ProbeSupported = ($ProbeEndpoint -ne 'legacy')
$script:ProxyReadySeconds = $ProxyReadySeconds
$script:QuotaFailMemoryMinutes = $QuotaFailMemoryMinutes
$script:DeprioritizeContains = $DeprioritizeContains
$script:DeprioritizePenaltyMs = $DeprioritizePenaltyMs
$script:LogPath = $LogPath
$script:VergeRoot = Join-Path $env:APPDATA 'io.github.clash-verge-rev.clash-verge-rev'

$script:ExcludeExact = @('自动选择', '故障转移', 'DIRECT', 'COMPATIBLE', 'GLOBAL', 'PASS', 'REJECT', 'REJECT-DROP', 'PASS-RULE',
  # 以下两个是红框内的信息占位节点（与真实节点同服同口，靠名字区分），必须精确排除：
  # 用精确匹配而不用子串，是为了不误杀 VLESS-美国01 / Gemini解锁类真实命名。
  '节点超时请手动更新订阅', 'Gemini解锁请使用VLESS节点')
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
  '香港', '台湾')
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
  # 异常文本可能原样包含请求 URI（含订阅 token）：记日志前先抹掉 http(s) URL。
  param([string]$Text)
  if ([string]::IsNullOrEmpty($Text)) { return '' }
  return ([regex]::Replace($Text, 'https?://\S+', '(url已脱敏)'))
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
  param($Value)
  if ($null -eq $Value) { return '<null>' }
  if ($Value -is [System.Array]) { return "数组（$(@($Value).Count) 项）" }
  if ($Value -is [string]) { return "字符串（$Value）" }
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
  # 只认两个键，都可整段省略，省略即用内置默认值：
  #   exclude_contains              子串排除：节点名含任一子串即丢弃（整体替换内置默认值；
  #                                 [] = 关闭子串排除；内置精确排除表不受此键影响）
  #   deprioritize.contains         地区降权关键词（整体替换内置默认值；[] = 关闭）
  #   deprioritize.penalty_ms       降权罚分毫秒（0 = 关闭降权）
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
      throw "看门狗配置文件 [$Path] 顶层必须是 JSON 对象（如 {`"exclude_contains`": [...]}），实际是 $(Get-ShapeName $cfg)"
    }
    # 未知键直接报错（与 config.json 的 DisallowUnknownFields 同口径）。键名拼错
    # （exclude-contains / penalty / 尾随空格）若静默忽略，看门狗会照常以内置默认值
    # 跑起来，用户却以为自己的排除/降权规则生效了。
    $unknownTop = @(@($cfg.PSObject.Properties | ForEach-Object { $_.Name }) |
      Where-Object { @('exclude_contains', 'deprioritize') -notcontains $_ })
    if ($unknownTop.Count -gt 0) {
      throw "看门狗配置文件 [$Path] 含未知键：$($unknownTop -join ' / ')，只支持 exclude_contains、deprioritize"
    }
  }
  if (-not $existed) { Write-WdLog "未找到看门狗配置文件 [$Path]，子串排除与地区降权均用内置默认值" 'INFO'; return }
  Write-WdLog "已解析看门狗配置文件：$Path"

  $applied = 0
  if (Test-HasKey $cfg 'exclude_contains') {
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
      if ($script:ExplicitParams -contains 'DeprioritizeContains') {
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
      if ($script:ExplicitParams -contains 'DeprioritizePenaltyMs') {
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
  # {} 是合法的“不改任何默认值”，但很容易是误建/误清空后的残留，单独提示一句。
  if ($applied -eq 0) { Write-WdLog '配置文件已解析但未应用任何键（空对象 {}）：全部使用内置默认值' 'WARN' }
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
  param($ProxyMap, [string[]]$SelectorAll, [string]$ExcludeNow)
  $cands = @()
  $dropped = @()
  if ($ProxyMap -eq $null) { return @() }
  $props = @()
  try { $props = @($ProxyMap.PSObject.Properties) } catch { return @() }
  foreach ($prop in $props) {
    $name = $prop.Name
    if ($SelectorAll -ne $null -and @($SelectorAll) -notcontains $name) { continue }
    if ($name -eq $ExcludeNow) { continue }
    $reason = ''
    if (-not (Test-NodeEligible $name ([ref]$reason))) {
      if ($reason -ne '') { $dropped += "$name（$reason）" }
      continue
    }
    $node = $prop.Value
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
  #   quota       → 该出口 IP 额度烧尽（FreeUsageLimit/BlackUsageLimit 类）→ 换节点
  #   transport   → 出口路由坏（真发了上游，传输层失败）→ 换节点
  #   ratelimit   → 普通限流，等
  #   fingerprint → 上游拒的是请求身份本身（403 FreeTierError），换节点无解
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
    # 绝不切换；但 403 + FreeTierError 是上游免费指纹门（可恢复），必须摘出，
    # 否则一次指纹波动就会误判 config、中止整轮（22:39/22:41/23:12 三轮只试
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
    # FreeTier-403 先于 config 摘出：与网关 verdict=fingerprint 对齐，只记日志、
    # 继续试下一个候选，绝不中止整轮（上游拒的是请求身份本身，换节点无解）。
    # class 只用于 config 中止与日志（切换决策只看服务端 verdict），顺序不影响流程。
    if ($info.Status -eq 429 -and $body.Contains('opencode2api:')) { $class = 'cooling' }
    elseif ($info.Status -eq 403 -and $body.Contains('FreeTierError')) { $class = 'cooling' }
    elseif ($info.Status -eq 401 -or $info.Status -eq 403 -or $info.Status -eq 404 -or $info.Status -eq 400) { $class = 'config' }
    # 503 ModelUnavailable（模型被可用性探针禁用）同样是网关侧目录状态，与出口 IP
    # 无关：换节点只会拿到同一个 503。不摘出的话它会掉进下面的 502/503/504 → transport
    # 分支，逐候选切一遍再回滚——改动前它答 400，走的正是上面那条 config（立即停轮）。
    elseif ($info.Status -eq 503 -and $body.Contains('ModelUnavailable')) { $class = 'config' }
    elseif ($body.Contains('FreeUsageLimit') -or $body.Contains('BlackUsageLimit')) { $class = 'quota' }
    elseif ($info.Status -eq 502 -or $info.Status -eq 503 -or $info.Status -eq 504 -or $body.Contains('gateway_transport')) {
      $class = 'transport'
    }
    $raSecs = 0
    # 必须取 $info.RetryAfter：$retryAfter 是 Get-HttpErrorInfo 内部的局部变量，
    # 在这里恒为 $null。pwsh 里 $null -ne '' 为 True 会让分支照进，但 TryParse('')
    # 恒 False，于是 raSecs 永远是 0，静默期退回固定 RoundCooldownMinutes(5min)
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
  # uid 块下可能有多个 proxy-group 各带一个 now:（Verge 常见），必须锚定到
  # “- name: <Selector>”之后、下一个“- name:”之前的 now:，不能取块内第一个。
  $selName = [regex]::Escape($script:Selector)
  $m = [regex]::Match($block, "(?ms)^\s*-\s*name:\s*['""]?$selName['""]?\s*\r?\n(.*?)^\s*now:\s*(.*?)(\r?)$")
  if (-not $m.Success) { throw "profiles.yaml 里找不到 selector [$($script:Selector)] 的 now: 行，拒绝写入" }
  $old = $m.Groups[2].Value.Trim()
  $safe = $Name
  if ($safe -match '[:#''"]|^\s|\s$') { $safe = "'" + ($safe -replace "'", "''") + "'" }
  $nowStart = $m.Groups[2].Index - $m.Index
  $nowLen = $m.Groups[2].Length
  $newBlock = $block.Substring(0, $m.Index + $nowStart) + $safe + $block.Substring($m.Index + $nowStart + $nowLen)
  $newText = $text.Substring(0, $start) + $newBlock + $text.Substring($next)
  # 先写临时文件再原子替换，避免中途崩溃撕裂 profiles.yaml。
  $tmp = "$profilesYaml.tmp"
  [IO.File]::WriteAllText($tmp, $newText, (New-Object Text.UTF8Encoding $false))
  Move-Item -LiteralPath $tmp -Destination $profilesYaml -Force
  Write-WdLog "profiles.yaml selected 已更新：$old -> now: $Name"
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
    Update-SelectedNow -Name $Name -Uid $Uid
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
    # 本地 profile（无 url）等情况：抛错会跳过“用旧列表继续”，这里转成失败返回。
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
    [string]$Verdict = '', [int]$RefreshSettleSeconds = 5)
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
  if ($refreshOk) {
    $addedMsg = ''
    $addedNames = Get-Prop $rr 'Added'
    if ($addedNames -ne $null -and @($addedNames).Count -gt 0) { $addedMsg = "，新增 $(@($addedNames).Count)（$(($addedNames | Select-Object -First 5) -join ' / ')）" }
    Write-WdLog "订阅已刷新（新增/消失见上）$addedMsg" 'OK'
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
    Write-WdLog 'quota 本次刷新未成功（见上），没有新出口 IP 可试，跳过原地复测直接切换'
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
  if (@($cands).Count -eq 0) {
    Write-WdLog '候选池为空（过滤后无可用节点），不做无效切换' 'ERROR'
    return @{ Ok = $false; Node = $null }
  }
  $tried = @()
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

function Invoke-WatchIteration {
  param(
    [int]$MaxCandidates,
    [int]$SwitchGapSeconds, [int]$VerifyTimeoutSeconds,
    [string]$Uid, [int]$RoundCooldownMinutes,
    [int]$NormalLogMinutes = 20, [int]$RefreshSettleSeconds = 5,
    [ValidateRange(1, 10)][int]$TransportConfirmRounds = 2
  )
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
  try {
    $sel = [uri]::EscapeDataString($script:Selector)
    $detail = Invoke-ClashGet -Path "/proxies/$sel"
    $map = Get-ProxyMap
  } catch {
    Write-WdLog "Clash 接口不可达：$($_.Exception.Message)（Clash 未启动？稍后重试）" 'WARN'
    # 同上：中断恢复后的首个正常轮次立即补一条。
    $script:NeedRecoveryLog = $true
    # 同网关不可达：verdict 未知轮不清零会把中断前的计数漏到恢复后。
    $script:TransportStrikes = 0
    return @{ Acted = $false }
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
      # ——匿名冷却封顶已经是 15s，固定 5 分钟会把冷却结束后的可行动作压掉约
      # 4 分 45 秒。其余失败路径（真切换过、全部候选失败）仍按分钟计。
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
  $wd = Get-WatchdogStatus
  $prox = Get-Prop $wd 'proxies'
  $anon = Get-Prop $wd 'anonymous'
  $wdCause = ''
  try { $wdCause = Shorten ([string](Get-Prop $anon 'cause')) 200 } catch { }
  Write-WdLog "网关看门狗：status=$(Get-Prop $wd 'status') healthy=$(Get-Prop $prox 'healthy')/$(Get-Prop $prox 'total') all_down=$(Get-Prop $prox 'all_down') verdict=$(Get-Prop $anon 'verdict') cooling=$(Get-Prop $anon 'cooling_seconds') recommend=$(Get-Prop $wd 'recommend_switch') cause=$wdCause"
  $sel = [uri]::EscapeDataString($script:Selector)
  $detail = Invoke-ClashGet -Path "/proxies/$sel"
  $now = [string](Get-Prop $detail 'now')
  $rawAll = Get-Prop $detail 'all'
  $all = @($rawAll | Where-Object { $_ -ne $null -and $_ -ne '' })
  Write-WdLog "Selector=$($script:Selector) now=$now 节点总数=$(@($all).Count)"
  $map = Get-ProxyMap
  $cands = Get-RankedCandidates -ProxyMap $map -SelectorAll $all -ExcludeNow $now
  $exclTxt = if (@($script:ExcludeContains).Count -gt 0) { @($script:ExcludeContains) -join '/' } else { '已关闭' }
  Write-WdLog "候选（生效子串排除=$exclTxt；另有内置精确排除：非叶子组/占位节点；按延迟(+地区/quota罚分)排序）共 $(@($cands).Count) 个，Top10："
  $i = 0
  foreach ($c in @($cands | Select-Object -First 10)) {
    $i++
    # quota 记忆是进程内存：DryRun 是新进程、记忆恒为空，quota 标记恒不显示属预期；
    # 地区降权是参数即时计算，DryRun 同样可见；显示代码与正式路径同源，留着无成本。
    $demoteMark = Get-DemoteMark $c
    Write-WdLog ("  {0,2}. {1}  delay={2}ms alive={3}{4}" -f $i, $c.Name, $c.Delay, $c.Alive, $demoteMark)
  }
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
  $info = Get-ProfileBlock -Uid $Uid
  $selName = [regex]::Escape($script:Selector)
  if ([regex]::IsMatch($info.Block, "(?ms)^\s*-\s*name:\s*['""]?$selName['""]?\s*\r?\n.*?^\s*now:\s*.*?$")) {
    Write-WdLog 'profiles.yaml selected.now 定位 OK（DryRun 不写入）'
  } else {
    Write-WdLog "profiles.yaml 里找不到 selector [$($script:Selector)] 的 now: 行（DryRun 不写入）" 'WARN'
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
# transport 计数（抖动容忍用）：verdict=transport 累计 N 轮才动作，见 Invoke-WatchIteration。
# 单个 ok/限流轮不再清零，改由 OkStreak 慢衰减（连续 4 轮非 transport 才清），
# 否则冷却过期节拍与轮询共振会把计数永远卡在 2/3；网关 stale 机制下 cooling=0
# 的 transport 同样是有效票。StreakStart 记本轮 streak 起点，供 last_success_age_s
# 判定“成功是否发生在 streak 之后”；LastStaleProbe 节流 ok 轮验证探针（5 分钟一次）。
$script:TransportStrikes = 0
$script:OkStreak = 0
$script:StreakStart = [DateTime]::MinValue
$script:LastStaleProbe = [DateTime]::MinValue

try {
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
  # 必须早于下面这行启动横幅与任何候选排序：横幅要如实打印生效的地区降权配置。
  Read-WatchdogConfig -Path $script:WatchdogConfigPath
  $regionCfg = '关闭'
  # 与 Get-RankedCandidates 同口径：先过滤空白条目再计数，全空白列表视为关闭，
  # 日志与行为读同一份 $script: 源，避免一侧被重赋值后分叉。
  $regionKeysForLog = @($script:DeprioritizeContains | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
  if ($script:DeprioritizePenaltyMs -gt 0 -and @($regionKeysForLog).Count -gt 0) { $regionCfg = "$($regionKeysForLog -join '/')+$($script:DeprioritizePenaltyMs)ms" }
  $excludeCfg = if (@($script:ExcludeContains).Count -gt 0) { @($script:ExcludeContains) -join '/' } else { '已关闭' }
  Write-WdLog "看门狗启动：selector=$Selector 网关=$($script:GatewayBase) 模型=$VerifyModel 轮询=${PollSeconds}s 正常心跳=${NormalLogMinutes}分钟 quota记忆=${QuotaFailMemoryMinutes}分钟 transport连续确认=${TransportConfirmRounds}轮 探针通道=$(if ($script:ProbeSupported) { 'watchdog-probe' } else { 'legacy' }) 闸门等待=${ProxyReadySeconds}s 地区降权=${regionCfg} 子串排除=${excludeCfg}"
  if ($DryRun) {
    Invoke-DryRun -Uid $SubscriptionUid
    exit 0
  }
  if ($Once) {
    # -Once 只有一轮攒不出连续计数：传 1 保留单轮即动作的老语义（仍受内部二次确认门控）；
    # 常驻循环仍用 TransportConfirmRounds 轮防抖。
    $onceResult = Invoke-WatchIteration `
      -MaxCandidates $MaxCandidates -SwitchGapSeconds 0 -VerifyTimeoutSeconds $VerifyTimeoutSeconds `
      -Uid $SubscriptionUid -RoundCooldownMinutes $RoundCooldownMinutes -NormalLogMinutes $NormalLogMinutes -RefreshSettleSeconds $RefreshSettleSeconds `
      -TransportConfirmRounds 1
    # Invoke-WatchIteration 永远返回 @{Acted}（$null 输入时 Get-Prop 守卫回 $null 走 false），点访问恒安全。
    if ($onceResult -ne $null -and $onceResult.Acted -and (Get-Date) -lt $script:RoundCooldownUntil) { exit 1 }
    exit 0
  }
  while ($true) {
    try {
      Invoke-WatchIteration `
        -MaxCandidates $MaxCandidates -SwitchGapSeconds $SwitchGapSeconds -VerifyTimeoutSeconds $VerifyTimeoutSeconds `
        -Uid $SubscriptionUid -RoundCooldownMinutes $RoundCooldownMinutes -NormalLogMinutes $NormalLogMinutes -RefreshSettleSeconds $RefreshSettleSeconds `
        -TransportConfirmRounds $TransportConfirmRounds | Out-Null
    } catch {
      Write-WdLog "本轮异常：$($_.Exception.Message)" 'WARN'
    }
    Start-Sleep -Seconds $PollSeconds
  }
} catch {
  Write-WdLog "启动失败：$($_.Exception.Message)" 'ERROR'
  exit 1
}
