package gateway

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	rand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
)

type proxyTransport struct {
	index    int
	name     string
	pool     bool
	client   *http.Client
	healthy  atomic.Bool
	checking atomic.Bool
}

type transportPool struct {
	items []*proxyTransport
}

// anonymousPool gives the shared OpenCode "public" credential an independent
// cooldown per proxy. Unlike key nodes, anonymous nodes are never rebound:
// changing proxy is the failover mechanism because Zen rate-limits them by IP.
type anonymousPool struct {
	nodes    []*anonymousNode
	next     atomic.Uint64
	cooldown time.Duration
	hasPool  bool
}

type anonymousNode struct {
	proxy         *proxyTransport
	failures      atomic.Uint32
	cooldownUntil atomic.Int64
	// coolHash is the fingerprint of the request that triggered the current
	// cooldown (see identity.RequestFingerprint). Empty means the cooldown is
	// not request-scoped (background refresh, or a caller that had no payload
	// to fingerprint) and therefore blocks every request.
	//
	// It is what makes the cooldown request-scoped rather than per-proxy: a
	// turn that was rate-limited must not also block an unrelated one, or a
	// single quota event freezes the whole anonymous lane for the window.
	coolHash atomic.Value
	// coolCause is a short summary of what triggered the current cooldown
	// (e.g. "FreeUsageLimitError (upstream HTTP 429)"), stored next to
	// cooldownUntil so the watchdog feed can report remaining seconds and
	// cause from the same cooldown (see maxCooldownSnapshot in watchdog.go).
	// Empty when the cooldown carries no user-traffic trigger.
	coolCause atomic.Value
	// lastCause remembers the most recent user-traffic failure cause even
	// after its cooldown expires, so the watchdog can keep reporting
	// transport until either a real user-traffic success lands or
	// staleCauseTTL passes. Only transport causes are surfaced from here
	// (see staleCauseSnapshot): only transport proves nothing recovered.
	lastCause atomic.Value
	// causeExpires is the UnixNano moment the stale lastCause stops being
	// reported (cooldownUntil at failure time + staleCauseTTL). It is
	// snapshotted at write time so read-only status polls can never extend it.
	causeExpires atomic.Int64
	// lastSuccess is the UnixNano moment of the most recent user-traffic
	// MarkSuccess. Background catalog refresh uses markRefreshSuccess and
	// must not touch it: a models-endpoint 200 proves less than a real
	// inference turn (and would fake "recovered" overnight with zero traffic).
	lastSuccess atomic.Int64
	// lastFailure is the UnixNano moment of the most recent user-traffic
	// MarkFailure, i.e. of the failures that carry a cause label.
	lastFailure atomic.Int64
}

type anonymousCursor struct {
	pool   *anonymousPool
	start  int
	offset int
	used   []bool
}

func newAnonymousPool(enabled bool, transports *transportPool, cooldown time.Duration) *anonymousPool {
	pool := &anonymousPool{cooldown: cooldown}
	if !enabled || transports == nil {
		return pool
	}
	pool.nodes = make([]*anonymousNode, 0, len(transports.items))
	for _, proxy := range transports.items {
		if proxy.pool {
			pool.hasPool = true
		}
		pool.nodes = append(pool.nodes, &anonymousNode{proxy: proxy})
	}
	return pool
}

func (p *anonymousPool) Len() int {
	if p == nil {
		return 0
	}
	return len(p.nodes)
}

func (p *anonymousPool) HasPool() bool {
	return p != nil && p.hasPool
}

func (p *anonymousPool) CursorFor(affinity string) anonymousCursor {
	if p == nil || len(p.nodes) == 0 {
		return anonymousCursor{pool: p}
	}
	start := 0
	if affinity == "" {
		start = int((p.next.Add(1) - 1) % uint64(len(p.nodes)))
	} else {
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(affinity))
		start = int(hash.Sum64() % uint64(len(p.nodes)))
	}
	return anonymousCursor{pool: p, start: start}
}

// Next visits each healthy, non-cooling proxy at most once per cursor.
// It never bypasses cooldown and is used by background catalog refresh,
// whose behavior must stay identical with or without request scoping.
//
// When the pool contains rotating proxy-pool transports (#pool), it continues
// cycling through them after the first pass so retry.max_attempts can draw a
// fresh upstream connection (and new exit IP) on every attempt.
//
// If every static proxy is cooling or unhealthy, Next makes one last-resort
// pass over the TRANSPORT-UNHEALTHY nodes only, choosing the earliest-expiring
// one. That set is deliberately narrow (merge decision D2): taking cooling
// nodes here too would hand the very node that just answered 429 straight back
// to the caller, cancel the 1x/2x/4x/8x backoff and burn free quota. Cooling
// is a quota/transport verdict, not a broken route — only an unhealthy route
// deserves a forced try. See NextForRequest for the fingerprint-scoped twin,
// and coolingFor, which mirrors both skip rules.
func (c *anonymousCursor) Next() *anonymousNode {
	if c.pool == nil || len(c.pool.nodes) == 0 {
		return nil
	}
	n := len(c.pool.nodes)
	if c.used == nil {
		c.used = make([]bool, n)
	}
	now := time.Now().UnixNano()
	for c.offset < n {
		idx := (c.start + c.offset) % n
		c.offset++
		node := c.pool.nodes[idx]
		if node.proxy.pool || (!c.used[idx] && node.proxy.healthy.Load() && node.cooldownUntil.Load() <= now) {
			c.used[idx] = true
			return node
		}
	}
	if c.pool.hasPool {
		for range n {
			idx := (c.start + c.offset) % n
			c.offset++
			node := c.pool.nodes[idx]
			if node.proxy.pool {
				return node
			}
		}
	}
	// Last-resort pass (merge decision D2): untried nodes whose TRANSPORT is
	// unhealthy, earliest cooldown first. Cooling nodes are excluded on
	// purpose — returning one here would cancel the backoff this cursor
	// exists to enforce.
	choice := -1
	var earliest int64
	for step := range n {
		idx := (c.start + step) % n
		if c.used[idx] {
			continue
		}
		if c.pool.nodes[idx].proxy.healthy.Load() {
			continue
		}
		until := c.pool.nodes[idx].cooldownUntil.Load()
		if choice < 0 || until < earliest {
			choice, earliest = idx, until
		}
	}
	if choice >= 0 {
		c.used[choice] = true
		return c.pool.nodes[choice]
	}
	return nil
}

// NextForRequest behaves like Next, except a cooling node still serves a
// request whose fingerprint differs from the one that triggered the cooldown.
// Retries replay identical bodies, so different content means a new turn that
// deserves one real upstream attempt (for example after the operator switched
// the exit IP behind a shared proxy endpoint). Transport-unhealthy proxies are
// always skipped, fingerprint or not.
func (c *anonymousCursor) NextForRequest(fingerprint string) *anonymousNode {
	if c.pool == nil || len(c.pool.nodes) == 0 {
		return nil
	}
	now := time.Now().UnixNano()
	for c.offset < len(c.pool.nodes) {
		node := c.pool.nodes[(c.start+c.offset)%len(c.pool.nodes)]
		c.offset++
		if !node.proxy.healthy.Load() {
			continue
		}
		if node.cooldownUntil.Load() <= now {
			return node
		}
		if stored, _ := node.coolHash.Load().(string); stored != "" && stored != fingerprint {
			return node
		}
	}
	// nil here, and only here, is the signal: no healthy node is available for
	// this turn. Two distinct situations produce it — every healthy node is
	// cooling for THIS fingerprint, or none is healthy — and doAnonymousUpstream
	// separates them with coolingFor, which counts healthy nodes only. That
	// gives 429 + Retry-After for the first and 503 proxy_error for the second
	// (merge decisions D2 and D3).
	//
	// There is deliberately no last-resort pass here, unlike Next. Returning a
	// transport-unhealthy node to ordinary traffic would contradict the rule
	// above and the case pinned by cooling_scope_test.go; and returning a node
	// cooling for this fingerprint would hand the caller straight back the exit
	// that just answered 429, voiding the 1x/2x/4x/8x ladder.
	return nil
}

// NextProbe returns the next proxy in rotation order regardless of transport
// health and cooldown, and it is the ONLY path allowed to do so. It exists for
// the watchdog's forced probe (watchdog.go), whose whole purpose is to measure
// the exit node that is live right now:
//
//   - a transport-unhealthy mark usually means a dial timed out while Clash was
//     re-routing a node switch, not that the new node cannot reach the upstream;
//   - a residual cooldown belongs to some earlier fingerprint (or to the
//     unscoped background catalog refresh) and says nothing about what this
//     node would answer now.
//
// Ordinary traffic must keep using NextForRequest. Widening this to every
// request would turn the anonymous backoff into a no-op, and the probe it
// serves writes no pool state at all (see handleWatchdogProbe), so bypassing
// those gates costs nothing a later turn cannot restore on its own.
func (c *anonymousCursor) NextProbe() *anonymousNode {
	if c.pool == nil || c.offset >= len(c.pool.nodes) {
		return nil
	}
	node := c.pool.nodes[(c.start+c.offset)%len(c.pool.nodes)]
	c.offset++
	return node
}

// MarkSuccess records a successful anonymous inference turn: it clears the
// cooldown and, as the only positive recovery proof the watchdog feed accepts,
// drops the stale transport memory and stamps the success moment.
func (p *anonymousPool) MarkSuccess(node *anonymousNode) {
	if node == nil {
		return
	}
	node.failures.Store(0)
	node.cooldownUntil.Store(0)
	node.coolCause.Store("")
	node.lastCause.Store("")
	node.causeExpires.Store(0)
	node.lastSuccess.Store(time.Now().UnixNano())
}

// markRefreshSuccess heals active cooling after a background catalog fetch
// without manufacturing recovery evidence: the stale transport cause stays
// for the watchdog (a models-endpoint 200 does not prove inference turns
// succeed) and lastSuccess keeps its user-traffic-only meaning. The cooldown
// clearing is exactly what MarkSuccess does, unchanged.
func (p *anonymousPool) markRefreshSuccess(node *anonymousNode) {
	if node == nil {
		return
	}
	node.failures.Store(0)
	node.cooldownUntil.Store(0)
	node.coolCause.Store("")
}

// hasCauseMemory reports whether the node still holds cause memory a snapshot
// can surface: the active coolCause, the stale lastCause, or the deadline that
// keeps that stale cause voting. Read-only, like the snapshot helpers.
// markRefreshSuccess clears the cooldown without the stale pair, so a node can
// reach RestoreProxy with zero failures and zero cooldown yet still be voting.
func (n *anonymousNode) hasCauseMemory() bool {
	active, _ := n.coolCause.Load().(string)
	stale, _ := n.lastCause.Load().(string)
	return active != "" || stale != "" || n.causeExpires.Load() != 0
}

// RestoreProxy clears cooldowns for anonymous nodes bound to a recovered
// proxy, mirroring nodePool.RestoreProxy for key nodes. Without this, an
// outage-era cooldown survives the proxy's recovery and the anonymous lane
// stays skipped even though the route is healthy again.
//
// The cause memory goes with the cooldown. A proxy only turns unhealthy through
// isProxyFailure (timeout / connection refused) and only comes back once an
// HTTP round trip through it succeeds, so a restore is precisely the
// transport-layer proof staleCauseSnapshot is waiting for: the only causes that
// can still vote there classify as transport, while a quota or ratelimit
// residue never votes at all (a proxy health check says nothing about the
// upstream allowance), so dropping the memory cannot hide a real allowance
// problem. Keeping it would instead leave the feed reporting transport with
// recommend_switch for up to staleCauseTTL after the route is demonstrably
// back, and the watchdog would switch a healthy node once for nothing.
// lastSuccess and lastFailure stay untouched: they are user-traffic evidence,
// so the script still has to see a real turn before it calls the node recovered.
func (p *anonymousPool) RestoreProxy(proxy *proxyTransport) int {
	if p == nil || proxy == nil {
		return 0
	}
	cleared := 0
	for _, node := range p.nodes {
		if node.proxy != proxy {
			continue
		}
		if node.cooldownUntil.Load() == 0 && node.failures.Load() == 0 && !node.hasCauseMemory() {
			continue
		}
		node.failures.Store(0)
		node.cooldownUntil.Store(0)
		node.coolCause.Store("")
		node.lastCause.Store("")
		node.causeExpires.Store(0)
		cleared++
	}
	return cleared
}

// maxAnonymousCooldown caps how long any single anonymous-lane cooldown may
// last, whichever rule produced it — the exponential ladder below or an
// upstream Retry-After. It is an absolute duration rather than a multiple of
// the base cooldown so that changing failure_cooldown_seconds cannot silently
// drag the ceiling along with it.
//
// The lane is rate limited per exit IP and, unlike key lanes, has no
// earliest-expiry fallback: a quota response asking for hours would keep the
// exit skipped long after a network change gave it a fresh allowance. Key
// lanes keep honoring Retry-After in full, since their cursor already falls
// back to the node that recovers first.
//
// It also bounds the client-visible damage. The cooldown is held on the proxy
// transport, not on the Clash node, so while it runs every request is refused
// locally and no node switch can shorten it — a long ceiling turns one upstream
// 429 into a long local outage.
const maxAnonymousCooldown = 15 * time.Second

// anonymousCooldownCeiling is the effective cap for one pool: never below the
// configured base, so a ceiling smaller than the base cannot shorten even the
// first failure's wait below what the operator asked for.
func anonymousCooldownCeiling(base time.Duration) time.Duration {
	return max(maxAnonymousCooldown, base)
}

// MarkFailure cools the node down after a failure that carries no user traffic
// in context (the background catalog refresh). The cooldown itself is exactly
// as it always was; the watchdog feed records nothing, because a
// models-endpoint probe must neither fake a user-traffic failure age nor cast
// a stale transport vote that would send the watchdog switching nodes.
// fingerprint scopes the cooldown to one request (see anonymousNode.coolHash);
// pass "" to block every request, which is what background work wants.
func (p *anonymousPool) MarkFailure(node *anonymousNode, resp *http.Response, err error, fingerprint string) {
	p.startCooldown(node, resp, err, "", fingerprint)
}

// markUserFailure is MarkFailure for a real inference turn: besides the same
// cooldown it records the watchdog feed's user-traffic evidence. cause is the
// label classifyAnonymousVerdict parses (see watchdogCause); it is never empty
// on this path, and its presence is what makes the failure count as user
// traffic (lastFailure plus the TTL-bounded stale transport memory).
func (p *anonymousPool) markUserFailure(node *anonymousNode, resp *http.Response, err error, cause, fingerprint string) {
	p.startCooldown(node, resp, err, cause, fingerprint)
}

// startCooldown applies the anonymous cooldown for one failed attempt and
// stores the watchdog cause label that belongs to it. An empty cause means
// "no user traffic": the cooldown still happens, only the watchdog memory is
// skipped. An empty fingerprint means "not request-scoped": the cooldown then
// blocks every request through this proxy, not just the one that failed.
//
// Rotating proxy-pool transports (#pool) are exempt, and the exemption lives
// HERE rather than in the two callers above so the cooldown, the cause memory
// and the skip rules in NextForRequest/coolingFor/coolCauseFor cannot drift
// apart. Such an entry rotates its own exit IP per connection, so cooling it
// would punish every other exit behind the same endpoint, and voting on it
// would send the watchdog switching nodes for a transport that is answering.
//
// The early return also skips lastFailure, and that is deliberate rather than
// an omission: a pool entry that failed must not publish a transport-failure
// age the watchdog would then weigh for a node switch, because it is exempt
// from the very cooldown that would justify one. Recovery evidence is
// unaffected — MarkSuccess stamps lastSuccess independently of this function,
// so the feed still sees a pool entry being used and does not read its
// silence as death.
func (p *anonymousPool) startCooldown(node *anonymousNode, resp *http.Response, err error, cause, fingerprint string) {
	if node == nil || (node.proxy != nil && node.proxy.pool) {
		return
	}
	if err == nil && resp != nil && resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
		return
	}
	failures := node.failures.Add(1)
	delay := p.cooldown * time.Duration(1<<min(failures-1, 3))
	if resp != nil {
		if retryAfter := parseRetryAfter(resp.Header.Get("Retry-After")); retryAfter > delay {
			delay = retryAfter
		}
	}
	// One ceiling for both rules above: a short Retry-After still never
	// shortens the ladder (the > comparison above), and a long one can never
	// outlive the anonymous lane's local-outage budget.
	delay = min(delay, anonymousCooldownCeiling(p.cooldown))
	// Scope the cooldown to the request that triggered it. Retries of this
	// turn replay the same fingerprint and are blocked; a different turn is
	// still served. Storing "" (background refresh) keeps the old
	// block-everything behavior, which is what that caller wants.
	node.coolHash.Store(fingerprint)
	node.coolCause.Store(cause)
	until := time.Now().Add(delay).UnixNano()
	node.cooldownUntil.Store(until)
	if cause == "" {
		return
	}
	// Stale watchdog memory: snapshotted once here so later status polls can
	// report it without ever extending it.
	node.lastFailure.Store(time.Now().UnixNano())
	node.lastCause.Store(cause)
	node.causeExpires.Store(until + int64(staleCauseTTL))
}

// staleCauseTTL bounds how long an expired transport cause keeps voting in
// the watchdog feed after its cooldown lapses with no new success. It must
// comfortably exceed the worst active wall — anonymousCooldownCeiling(base),
// i.e. 15s by default, or the base itself when the operator raises it above
// that — plus the script's TransportConfirmRounds window
// (3x15s poll = 45s), or the "two failures then expiry looks like recovery"
// resonance simply moves from ~15s to TTL. Five minutes keeps a real outage
// voting long enough to switch while capping the collateral of a transient
// blip (a recovered node is served again immediately once its cooldown lapses).
const staleCauseTTL = 5 * time.Minute

// maxCooldownSnapshot returns the longest still-active anonymous cooldown
// among transport-healthy proxies and its stored trigger in one pass. One
// pass keeps seconds and cause on the same node in the common case (two
// separate scans could straddle an expiry boundary and mix seconds of one
// node with the cause of another).
// Transport-unhealthy proxies are skipped, mirroring Next: their cooldowns
// cannot block any request (unhealthy nodes are never served), so surfacing
// them would cry wolf to the watchdog.
// This is a worst-case snapshot across all nodes, not a verdict for any
// particular request: one node's residual cooldown can outlive its own turns,
// so the watchdog must treat a quota/transport verdict as one vote (ideally
// confirmed twice), never as proof that the next turn needs a switch.
// It is still best-effort under concurrency: MarkSuccess clears
// cooldownUntil before coolCause, so a success landing between the two loads
// can surface a stale remain with an empty cause; callers treat that as plain
// cooling (wait), never as a switch signal.
func (p *anonymousPool) maxCooldownSnapshot() (time.Duration, string) {
	if p == nil {
		return 0, ""
	}
	now := time.Now().UnixNano()
	var longest time.Duration
	cause := ""
	for _, node := range p.nodes {
		if !node.proxy.healthy.Load() {
			continue
		}
		until := node.cooldownUntil.Load()
		if remain := time.Duration(until - now); remain > longest {
			longest = remain
			cause, _ = node.coolCause.Load().(string)
		}
	}
	return longest, cause
}

// coolingFor reports whether any proxy blocks this fingerprint only because
// of time-based anonymous cooldown (as opposed to transport health), plus
// the longest remaining delay. It mirrors the skip logic in NextForRequest, so
// the answer is exactly "can NextForRequest find nothing".
func (p *anonymousPool) coolingFor(fingerprint string) (bool, time.Duration) {
	if p == nil {
		return false, 0
	}
	now := time.Now().UnixNano()
	blocked := false
	var longest time.Duration
	for _, node := range p.nodes {
		if !node.proxy.healthy.Load() || node.cooldownUntil.Load() <= now {
			continue
		}
		if stored, _ := node.coolHash.Load().(string); stored != "" && stored != fingerprint {
			continue
		}
		blocked = true
		if remain := time.Duration(node.cooldownUntil.Load() - now); remain > longest {
			longest = remain
		}
	}
	return blocked, longest
}

// coolCauseFor returns the stored trigger of the longest fingerprint-matching
// cooldown (e.g. "FreeUsageLimitError (upstream HTTP 429)"), or "" when
// nothing cooling matches. It mirrors the skip logic in coolingFor so the
// reported cause always belongs to the block the caller just hit.
func (p *anonymousPool) coolCauseFor(fingerprint string) string {
	if p == nil {
		return ""
	}
	now := time.Now().UnixNano()
	var longest time.Duration
	cause := ""
	for _, node := range p.nodes {
		if !node.proxy.healthy.Load() || node.cooldownUntil.Load() <= now {
			continue
		}
		if stored, _ := node.coolHash.Load().(string); stored != "" && stored != fingerprint {
			continue
		}
		if remain := time.Duration(node.cooldownUntil.Load() - now); remain > longest {
			longest = remain
			cause, _ = node.coolCause.Load().(string)
		}
	}
	return cause
}

// staleCauseSnapshot returns the most recently expired transport cause
// still inside its TTL, or "" when none qualifies. Transport-only by design:
// an expired quota cause must not hold recommend_switch=true for minutes,
// an expired ratelimit cause must not park the script in wait, and an
// unknown cause classifies as ok without cooling anyway. Unhealthy proxies
// stay skipped, mirroring maxCooldownSnapshot.
func (p *anonymousPool) staleCauseSnapshot() string {
	if p == nil {
		return ""
	}
	now := time.Now().UnixNano()
	var bestExpiry int64
	stale := ""
	for _, node := range p.nodes {
		if !node.proxy.healthy.Load() {
			continue
		}
		expiry := node.causeExpires.Load()
		if expiry <= now {
			continue
		}
		cause, _ := node.lastCause.Load().(string)
		if cause == "" || classifyAnonymousVerdict(cause, false) != "transport" {
			continue
		}
		if expiry > bestExpiry {
			bestExpiry = expiry
			stale = cause
		}
	}
	return stale
}

// successFailureAges reports the newest user-traffic success and failure
// moments across transport-healthy proxies as ages. ok=false per side when
// that side was never recorded (process memory, restart clears, same as the
// telemetry minute metrics). Read-only: never extends any deadline.
func (p *anonymousPool) successFailureAges() (successAge, failureAge time.Duration, successOK, failureOK bool) {
	if p == nil {
		return 0, 0, false, false
	}
	now := time.Now().UnixNano()
	var newestSuccess, newestFailure int64
	for _, node := range p.nodes {
		if !node.proxy.healthy.Load() {
			continue
		}
		if ts := node.lastSuccess.Load(); ts > newestSuccess {
			newestSuccess = ts
		}
		if ts := node.lastFailure.Load(); ts > newestFailure {
			newestFailure = ts
		}
	}
	if newestSuccess > 0 {
		successAge, successOK = time.Duration(now-newestSuccess), true
	}
	if newestFailure > 0 {
		failureAge, failureOK = time.Duration(now-newestFailure), true
	}
	return successAge, failureAge, successOK, failureOK
}

func (p *transportPool) hasHealthy() bool {
	if p == nil {
		return false
	}
	for _, proxy := range p.items {
		if proxy.healthy.Load() {
			return true
		}
	}
	return false
}

// closeIdleConnections releases pooled keep-alive connections. Called on
// superseded runtimes after a hot-reload swap so old Transports do not hold
// idle conns until IdleConnTimeoutSeconds.
func (p *transportPool) closeIdleConnections() {
	if p == nil {
		return
	}
	for _, proxy := range p.items {
		if proxy == nil || proxy.client == nil {
			continue
		}
		if transport, ok := proxy.client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
}

func (p *transportPool) healthCounts() (total, healthy int) {
	if p == nil {
		return 0, 0
	}
	for _, proxy := range p.items {
		if proxy.healthy.Load() {
			healthy++
		}
	}
	return len(p.items), healthy
}

func newTransportPool(proxies []string, cfg config.PerformanceConfig, responseHeaderTimeout time.Duration) (*transportPool, error) {
	p := &transportPool{items: make([]*proxyTransport, 0, len(proxies))}
	for _, raw := range proxies {
		clean, isPool := config.ParseProxyEntry(raw)
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConns = cfg.MaxIdleConns
		transport.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
		transport.MaxConnsPerHost = cfg.MaxConnsPerHost
		transport.IdleConnTimeout = time.Duration(cfg.IdleConnTimeoutSeconds) * time.Second
		transport.ResponseHeaderTimeout = responseHeaderTimeout
		transport.ForceAttemptHTTP2 = !isPool
		transport.DisableKeepAlives = isPool
		if isPool {
			// Cloning DefaultTransport after its first dial inherits the
			// already-configured HTTP/2 ALPN and TLSNextProto map, which ignores
			// DisableKeepAlives and req.Close and multiplexes every request over
			// one CONNECT tunnel. An explicit empty TLSNextProto map and HTTP/1.1
			// ALPN force a brand-new TCP + CONNECT tunnel per attempt.
			transport.TLSNextProto = make(map[string]func(authority string, c *tls.Conn) http.RoundTripper)
			if transport.TLSClientConfig == nil {
				transport.TLSClientConfig = &tls.Config{NextProtos: []string{"http/1.1"}}
			} else {
				tlsCfg := transport.TLSClientConfig.Clone()
				tlsCfg.NextProtos = []string{"http/1.1"}
				transport.TLSClientConfig = tlsCfg
			}
		}
		transport.DialContext = (&net.Dialer{
			Timeout:   time.Duration(cfg.ConnectTimeoutSeconds) * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext
		if clean == "direct" {
			transport.Proxy = nil
		} else {
			u, err := url.Parse(clean)
			if err != nil {
				return nil, fmt.Errorf("parse proxy %s: %w", config.RedactURL(raw), err)
			}
			transport.Proxy = http.ProxyURL(u)
		}
		proxy := &proxyTransport{index: len(p.items), name: raw, pool: isPool, client: &http.Client{Transport: transport}}
		proxy.healthy.Store(true)
		p.items = append(p.items, proxy)
	}
	return p, nil
}

type proxyHealthResult struct {
	proxy      *proxyTransport
	err        error
	failed     bool
	wasHealthy bool
}

// CheckHealth concurrently rechecks only proxies already marked unhealthy.
// Healthy proxies are skipped before a check is claimed. Any HTTP response
// from the test URL proves that the route is reachable; only a timeout or
// connection refusal keeps the proxy unhealthy.
func (p *transportPool) CheckHealth(ctx context.Context, target string, timeout time.Duration) []proxyHealthResult {
	if p == nil {
		return nil
	}
	results := make(chan proxyHealthResult, len(p.items))
	checks := 0
	for _, proxy := range p.items {
		if proxy.pool || proxy.healthy.Load() || !proxy.checking.CompareAndSwap(false, true) {
			continue
		}
		// A real request may have restored the proxy between the first health
		// read and claiming this check.
		if proxy.healthy.Load() {
			proxy.checking.Store(false)
			continue
		}
		checks++
		go func() {
			results <- p.checkClaimedProxy(ctx, proxy, target, timeout)
		}()
	}
	out := make([]proxyHealthResult, 0, checks)
	for range checks {
		out = append(out, <-results)
	}
	return out
}

// checkClaimedProxy performs a check after the caller has acquired checking.
func (p *transportPool) checkClaimedProxy(ctx context.Context, proxy *proxyTransport, target string, timeout time.Duration) proxyHealthResult {
	defer proxy.checking.Store(false)
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet, target, nil)
	if err == nil {
		// Proxy liveness probe, not an inference turn: this must not claim to
		// be the opencode CLI (see httpx.GatewayUserAgent).
		req.Header.Set("User-Agent", httpx.GatewayUserAgent())
		resp, requestErr := proxy.client.Do(req)
		err = requestErr
		if resp != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			_ = resp.Body.Close()
		}
	}
	result := proxyHealthResult{proxy: proxy, err: err, wasHealthy: proxy.healthy.Load()}
	if err == nil {
		result.wasHealthy = proxy.healthy.Swap(true)
	} else if isProxyFailure(err) {
		result.failed = true
		result.wasHealthy = proxy.healthy.Swap(false)
	}
	return result
}

// isProxyFailure deliberately recognizes only failures that say the proxy
// route is unavailable. HTTP responses and unrelated transport/protocol errors
// must not evict a proxy.
func isProxyFailure(err error) bool {
	if errors.Is(err, errFirstEventTimeout) || upstreamAccepted(err) {
		return false // The upstream accepted HTTP; slow generation is not a broken proxy.
	}
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// upstreamNode keeps a key stable while allowing its proxy binding to change
// atomically when a proxy becomes unavailable.
type upstreamNode struct {
	key            string
	index          int
	preferredProxy int
	proxyIndex     atomic.Int64
	failures       atomic.Uint32
	cooldownUntil  atomic.Int64
}

type nodePool struct {
	nodes        []*upstreamNode
	transports   *transportPool
	next         atomic.Uint64
	cooldown     time.Duration
	bindingsMu   sync.Mutex
	bindingCount []int
}

// newNodePool distributes keys over proxies in round-robin order. When there
// are fewer keys than proxies, the remaining proxies are intentionally idle and
// can take over a key immediately if an active proxy fails.
func newNodePool(keys []string, transports *transportPool, cooldown time.Duration) (*nodePool, error) {
	if transports == nil || len(transports.items) == 0 {
		return nil, fmt.Errorf("at least one proxy transport is required")
	}
	pool := &nodePool{
		nodes:        make([]*upstreamNode, 0, len(keys)),
		transports:   transports,
		cooldown:     cooldown,
		bindingCount: make([]int, len(transports.items)),
	}
	if len(keys) == 0 {
		return pool, nil
	}
	for i, key := range keys {
		proxyIndex := i % len(transports.items)
		node := &upstreamNode{key: key, index: i, preferredProxy: proxyIndex}
		node.proxyIndex.Store(int64(proxyIndex))
		pool.nodes = append(pool.nodes, node)
		pool.bindingCount[proxyIndex]++
	}
	return pool, nil
}

// RestoreProxy moves keys that were originally assigned to a recovered proxy
// back to it. Proxies without an original key simply become healthy failover
// candidates again.
func (p *nodePool) RestoreProxy(recoveredProxy int) int {
	if p == nil || p.transports == nil || recoveredProxy < 0 || recoveredProxy >= len(p.transports.items) {
		return 0
	}
	p.bindingsMu.Lock()
	defer p.bindingsMu.Unlock()

	moved := 0
	for _, node := range p.nodes {
		current := int(node.proxyIndex.Load())
		if node.preferredProxy != recoveredProxy || current == recoveredProxy {
			continue
		}
		if current >= 0 && current < len(p.bindingCount) {
			p.bindingCount[current]--
		}
		p.bindingCount[recoveredProxy]++
		node.proxyIndex.Store(int64(recoveredProxy))
		node.failures.Store(0)
		node.cooldownUntil.Store(0)
		moved++
	}
	return moved
}

func (p *nodePool) Len() int { return len(p.nodes) }

// NodeByID resolves a node by its stable fingerprint. Callers only ever hold
// the fingerprint, never the key value, so the lookup cannot leak a secret.
func (p *nodePool) NodeByID(id string) *upstreamNode {
	if p == nil || id == "" {
		return nil
	}
	for _, node := range p.nodes {
		if config.Fingerprint(node.key) == id {
			return node
		}
	}
	return nil
}

func (p *nodePool) Proxy(node *upstreamNode) *proxyTransport {
	if p == nil || node == nil || p.transports == nil {
		return nil
	}
	index := int(node.proxyIndex.Load())
	if index < 0 || index >= len(p.transports.items) {
		return nil
	}
	return p.transports.items[index]
}

// RebindProxy moves every key currently using failedProxy to the least-loaded
// healthy proxy. Empty proxies are selected in configuration order; otherwise
// one of the least-loaded proxies is chosen at random. If every alternative is
// currently unhealthy, it still attempts the least-loaded alternative.
func (p *nodePool) RebindProxy(failedProxy int) int {
	if p == nil || p.transports == nil || len(p.transports.items) < 2 || failedProxy < 0 || failedProxy >= len(p.transports.items) {
		return 0
	}
	p.bindingsMu.Lock()
	defer p.bindingsMu.Unlock()

	moved := 0
	for _, node := range p.nodes {
		if int(node.proxyIndex.Load()) != failedProxy {
			continue
		}
		target := p.replacementLocked(failedProxy, true)
		if target < 0 {
			target = p.replacementLocked(failedProxy, false)
		}
		if target < 0 {
			continue
		}
		p.bindingCount[failedProxy]--
		p.bindingCount[target]++
		node.proxyIndex.Store(int64(target))
		node.failures.Store(0)
		node.cooldownUntil.Store(0)
		moved++
	}
	return moved
}

func (p *nodePool) replacementLocked(failedProxy int, healthyOnly bool) int {
	minimum := int(^uint(0) >> 1)
	candidates := make([]int, 0, len(p.transports.items)-1)
	for i := range p.transports.items {
		if i == failedProxy || healthyOnly && !p.transports.items[i].healthy.Load() {
			continue
		}
		count := p.bindingCount[i]
		if count == 0 {
			return i
		}
		if count < minimum {
			minimum = count
			candidates = candidates[:0]
			candidates = append(candidates, i)
		} else if count == minimum {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) == 0 {
		return -1
	}
	return candidates[rand.IntN(len(candidates))]
}

type nodeCursor struct {
	pool *nodePool
	next int
}

// Cursor reserves a different starting node for each concurrent request.
// Selection is delayed until Next, so a node marked failed by the preceding
// attempt is immediately skipped. Both Cursor and Next allocate no memory.
func (p *nodePool) Cursor() nodeCursor {
	if len(p.nodes) == 0 {
		return nodeCursor{pool: p}
	}
	return nodeCursor{pool: p, next: int((p.next.Add(1) - 1) % uint64(len(p.nodes)))}
}

// CursorFor returns a cursor whose first choice is stable for the supplied
// affinity key. This keeps every turn in a conversation on the same upstream
// key while retaining Next's cooldown-aware failover behavior. Empty affinity
// keys keep the round-robin behavior used by background tasks.
func (p *nodePool) CursorFor(affinity string) nodeCursor {
	if affinity == "" || len(p.nodes) == 0 {
		return p.Cursor()
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(affinity))
	return nodeCursor{pool: p, next: int(hash.Sum64() % uint64(len(p.nodes)))}
}

func (c *nodeCursor) Next() *upstreamNode {
	if c.pool == nil || len(c.pool.nodes) == 0 {
		return nil
	}
	now := time.Now().UnixNano()
	choice := -1
	var earliest int64
	for offset := 0; offset < len(c.pool.nodes); offset++ {
		i := (c.next + offset) % len(c.pool.nodes)
		until := c.pool.nodes[i].cooldownUntil.Load()
		if until <= now {
			choice = i
			break
		}
		if choice == -1 || until < earliest {
			choice, earliest = i, until
		}
	}
	if choice < 0 {
		return nil
	}
	c.next = (choice + 1) % len(c.pool.nodes)
	return c.pool.nodes[choice]
}

func (p *nodePool) MarkSuccess(node *upstreamNode) {
	node.failures.Store(0)
	node.cooldownUntil.Store(0)
}

func (p *nodePool) MarkFailure(node *upstreamNode, resp *http.Response, err error) {
	if err == nil && resp != nil && resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
		return
	}
	failures := node.failures.Add(1)
	multiplier := time.Duration(1 << min(failures-1, 3))
	delay := p.cooldown * multiplier
	if resp != nil {
		if retryAfter := parseRetryAfter(resp.Header.Get("Retry-After")); retryAfter > delay {
			delay = retryAfter
		}
	}
	node.cooldownUntil.Store(time.Now().Add(delay).UnixNano())
}

func parseRetryAfter(value string) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		return max(time.Until(when), 0)
	}
	return 0
}
