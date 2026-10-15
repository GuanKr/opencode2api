package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/models"
)

func TestClassifyAnonymousVerdict(t *testing.T) {
	cases := []struct {
		name    string
		cause   string
		cooling bool
		want    string
	}{
		{"transport", "transport (timeout)", true, "transport"},
		{"transportNoCooling", "transport (connection refused)", false, "transport"},
		{"transportWithResponse", "timeout (upstream HTTP 503)", true, "transport"},
		{"transportConnFailed", "connection failed (upstream HTTP 503)", true, "transport"},
		{"quotaFree", "FreeUsageLimitError (upstream HTTP 429)", true, "quota"},
		{"quotaBlack", "BlackUsageLimitError (upstream HTTP 429)", true, "quota"},
		{"quotaWord", "upstream quota exhausted", true, "quota"},
		// A 5xx tail outranks keywords: server-side wobble is transport,
		// never quota proof, even with a quota class name attached.
		{"quotaClassServerWobble", "FreeUsageLimitError (upstream HTTP 503)", true, "transport"},
		{"ratelimit", "RateLimitError (upstream HTTP 429)", true, "ratelimit"},
		{"ratelimitTooMany", "too many requests (upstream HTTP 429)", true, "ratelimit"},
		// FreeTierError is upstream refusing the request identity, not the
		// exit IP: it gets its own verdict and never a switch recommendation.
		{"fingerprint", "FreeTierError (upstream HTTP 403)", true, "fingerprint"},
		{"fingerprintNoCooling", "FreeTierError (upstream HTTP 403)", false, "fingerprint"},
		// A 5xx tail still outranks it: a flapping upstream is transport.
		{"fingerprintServerWobble", "FreeTierError (upstream HTTP 503)", true, "transport"},
		{"coolingUnknown", "", true, "cooling"},
		{"coolingUnknownCause", "upstream HTTP 503", true, "cooling"},
		{"okIdle", "", false, "ok"},
		{"okUnknownNoCooling", "upstream HTTP 503", false, "ok"},
	}
	for _, tc := range cases {
		if got := classifyAnonymousVerdict(tc.cause, tc.cooling); got != tc.want {
			t.Errorf("%s: classifyAnonymousVerdict(%q, %v) = %q, want %q", tc.name, tc.cause, tc.cooling, got, tc.want)
		}
	}
}

func watchdogTestTransports(healthy ...bool) *transportPool {
	p := &transportPool{}
	for i, h := range healthy {
		pt := &proxyTransport{index: i, name: "direct"}
		pt.healthy.Store(h)
		p.items = append(p.items, pt)
	}
	return p
}

func watchdogTestGateway(transports *transportPool, pool *anonymousPool) *Gateway {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Gateway{
		cfg: config.Config{
			ServerKeys: []string{"test-key"},
			Anonymous:  true,
			Retry:      config.RetryConfig{MaxAttempts: 3, TimeoutSeconds: 300},
			Upstream:   config.UpstreamConfig{Zen: "https://opencode.ai/zen", Go: "https://opencode.ai/zen/go"},
			Prefer:     config.TierGo,
		},
		logger:     logger,
		transports: transports,
		anonymous:  pool,
		catalog:    models.NewCatalog(config.TierGo, nil),
	}
}

func doWatchdogStatus(t *testing.T, g *Gateway) watchdogStatusResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/watchdog/status", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("watchdog status must be 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp watchdogStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

// watchdogCool429 is a plain 429 answer carrying no body: MarkFailure cools on
// the status alone, so it isolates cooldown state from cause production.
func watchdogCool429() *http.Response {
	return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{}}
}

func watchdogCoolingPool(t *testing.T, cause string) (*anonymousPool, *anonymousNode) {
	t.Helper()
	proxy := &proxyTransport{index: 0, name: "direct"}
	proxy.healthy.Store(true)
	node := &anonymousNode{proxy: proxy}
	pool := &anonymousPool{nodes: []*anonymousNode{node}, cooldown: 15 * time.Second}
	pool.markUserFailure(node, watchdogCool429(), nil, cause)
	if remain, _ := pool.maxCooldownSnapshot(); remain <= 0 {
		t.Fatalf("setup must leave active cooling, got %v", remain)
	}
	return pool, node
}

func TestMaxCooldownSnapshotPicksLongest(t *testing.T) {
	var nilPool *anonymousPool
	if _, cause := nilPool.maxCooldownSnapshot(); cause != "" {
		t.Fatalf("nil pool must report empty cause, got %q", cause)
	}
	proxy := &proxyTransport{index: 0, name: "direct"}
	proxy.healthy.Store(true)
	n1 := &anonymousNode{proxy: proxy}
	n2 := &anonymousNode{proxy: proxy}
	pool := &anonymousPool{nodes: []*anonymousNode{n1, n2}, cooldown: 15 * time.Second}
	if _, cause := pool.maxCooldownSnapshot(); cause != "" {
		t.Fatalf("fresh pool must report empty cause, got %q", cause)
	}
	now := time.Now()
	n1.coolCause.Store("RateLimitError (upstream HTTP 429)")
	n1.cooldownUntil.Store(now.Add(5 * time.Second).UnixNano())
	n2.coolCause.Store("FreeUsageLimitError (upstream HTTP 429)")
	n2.cooldownUntil.Store(now.Add(25 * time.Second).UnixNano())
	remain, cause := pool.maxCooldownSnapshot()
	if cause != "FreeUsageLimitError (upstream HTTP 429)" {
		t.Fatalf("longest cooldown cause must win, got %q", cause)
	}
	if remain <= 20*time.Second || remain > 25*time.Second {
		t.Fatalf("seconds must belong to the same (longest) cooldown, got %v", remain)
	}
}

func TestMaxCooldownSnapshotSkipsUnhealthy(t *testing.T) {
	pool, node := watchdogCoolingPool(t, "FreeUsageLimitError (upstream HTTP 429)")
	if remain, _ := pool.maxCooldownSnapshot(); remain <= 0 {
		t.Fatalf("setup must leave active cooling, got %v", remain)
	}
	// An unhealthy proxy never serves any request, so its residual cooldown
	// must not surface in the watchdog/health snapshot.
	node.proxy.healthy.Store(false)
	remain, cause := pool.maxCooldownSnapshot()
	if remain != 0 || cause != "" {
		t.Fatalf("unhealthy proxy cooldown must be hidden from snapshot, got %v/%q", remain, cause)
	}
}

func TestHandleWatchdogStatusRecommendSwitch(t *testing.T) {
	cases := []struct {
		name        string
		cause       string
		cooling     bool
		wantVerdict string
		wantSwitch  bool
	}{
		{"transportSwitches", "transport (timeout)", true, "transport", true},
		{"quotaSwitches", "FreeUsageLimitError (upstream HTTP 429)", true, "quota", true},
		{"ratelimitWaits", "RateLimitError (upstream HTTP 429)", true, "ratelimit", false},
		{"coolingWaits", "", true, "cooling", false},
		{"okIdle", "", false, "ok", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var pool *anonymousPool
			if tc.cooling {
				pool, _ = watchdogCoolingPool(t, tc.cause)
			} else {
				proxy := &proxyTransport{index: 0, name: "direct"}
				proxy.healthy.Store(true)
				pool = &anonymousPool{nodes: []*anonymousNode{{proxy: proxy}}, cooldown: 15 * time.Second}
			}
			g := watchdogTestGateway(watchdogTestTransports(true), pool)
			resp := doWatchdogStatus(t, g)
			if resp.Anonymous.Verdict != tc.wantVerdict {
				t.Fatalf("verdict = %q, want %q (cause %q)", resp.Anonymous.Verdict, tc.wantVerdict, resp.Anonymous.Cause)
			}
			if resp.RecommendSwitch != tc.wantSwitch {
				t.Fatalf("recommend_switch = %v, want %v (verdict %q)", resp.RecommendSwitch, tc.wantSwitch, resp.Anonymous.Verdict)
			}
			if resp.Proxies.Total != 1 || resp.Proxies.Healthy != 1 || resp.Proxies.Unhealthy != 0 || resp.Proxies.AllDown {
				t.Fatalf("healthy single proxy must report 1/1/0, got %+v", resp.Proxies)
			}
			if tc.cooling && resp.Anonymous.CoolingSeconds <= 0 {
				t.Fatalf("active cooling must report positive seconds, got %d", resp.Anonymous.CoolingSeconds)
			}
		})
	}
}

// A fingerprint rejection is upstream's answer about the request identity, so
// it must be visible as its own verdict while still never switching nodes.
func TestHandleWatchdogStatusFingerprintNeverSwitches(t *testing.T) {
	pool, _ := watchdogCoolingPool(t, "FreeTierError (upstream HTTP 403)")
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "fingerprint" {
		t.Fatalf("FreeTierError must report the fingerprint verdict, got %q (cause %q)", resp.Anonymous.Verdict, resp.Anonymous.Cause)
	}
	if resp.RecommendSwitch {
		t.Fatal("fingerprint rejection must never recommend a node switch")
	}
}

func TestHandleWatchdogStatusAllDownRecommends(t *testing.T) {
	proxy := &proxyTransport{index: 0, name: "direct"}
	proxy.healthy.Store(true)
	pool := &anonymousPool{nodes: []*anonymousNode{{proxy: proxy}}, cooldown: 15 * time.Second}
	g := watchdogTestGateway(watchdogTestTransports(false, false), pool)
	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "ok" {
		t.Fatalf("idle lane must be ok, got %q", resp.Anonymous.Verdict)
	}
	if !resp.Proxies.AllDown || resp.Status != "degraded" || !resp.RecommendSwitch {
		t.Fatalf("all-down proxies must force degraded+switch, got %+v status %q", resp.Proxies, resp.Status)
	}
}

func TestHandleWatchdogStatusEmptyPoolNotAllDown(t *testing.T) {
	proxy := &proxyTransport{index: 0, name: "direct"}
	proxy.healthy.Store(true)
	pool := &anonymousPool{nodes: []*anonymousNode{{proxy: proxy}}, cooldown: 15 * time.Second}
	g := watchdogTestGateway(watchdogTestTransports(), pool)
	resp := doWatchdogStatus(t, g)
	if resp.Proxies.Total != 0 || resp.Proxies.AllDown {
		t.Fatalf("zero-proxy config must not report all_down, got %+v", resp.Proxies)
	}
	if resp.Status != "ok" || resp.RecommendSwitch {
		t.Fatalf("zero-proxy idle lane must stay ok without switch, got status %q switch %v", resp.Status, resp.RecommendSwitch)
	}
}

func TestHandleWatchdogStatusRequiresAuth(t *testing.T) {
	proxy := &proxyTransport{index: 0, name: "direct"}
	proxy.healthy.Store(true)
	pool := &anonymousPool{nodes: []*anonymousNode{{proxy: proxy}}, cooldown: 15 * time.Second}
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	req := httptest.NewRequest(http.MethodGet, "/v1/watchdog/status", nil)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("watchdog status without key must be 401, got %d", rec.Code)
	}
}

// staleTestPool records one user-traffic failure and then lets its active
// cooldown lapse while staying inside the stale TTL, i.e. the exact
// "cooling expired with no new success" state the watchdog must not call
// recovered. No sleeps: expiry is forced by moving cooldownUntil to the past.
func staleTestPool(t *testing.T, cause string) (*anonymousPool, *anonymousNode) {
	t.Helper()
	proxy := &proxyTransport{index: 0, name: "direct"}
	proxy.healthy.Store(true)
	node := &anonymousNode{proxy: proxy}
	pool := &anonymousPool{nodes: []*anonymousNode{node}, cooldown: 15 * time.Second}
	pool.markUserFailure(node, nil, errors.New("dial failed"), cause)
	node.cooldownUntil.Store(time.Now().Add(-time.Second).UnixNano())
	if remain, _ := pool.maxCooldownSnapshot(); remain != 0 {
		t.Fatalf("setup must leave expired active cooling, got %v", remain)
	}
	return pool, node
}

func TestStaleTransportCauseVotesWithoutCooling(t *testing.T) {
	pool, _ := staleTestPool(t, "transport (connection failed)")
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "transport" {
		t.Fatalf("expired transport cause must stay transport, got %q (cause %q)", resp.Anonymous.Verdict, resp.Anonymous.Cause)
	}
	if resp.Anonymous.CoolingSeconds != 0 {
		t.Fatalf("stale vote must report cooling 0, got %d", resp.Anonymous.CoolingSeconds)
	}
	if resp.Anonymous.Cause != "transport (connection failed)" {
		t.Fatalf("stale cause must be preserved, got %q", resp.Anonymous.Cause)
	}
	if !resp.RecommendSwitch {
		t.Fatal("stale transport must keep recommend_switch=true")
	}
	if resp.Anonymous.LastFailureAgeS == nil {
		t.Fatal("stale transport must report last_failure_age_s")
	}
	if resp.Anonymous.LastSuccessAgeS != nil {
		t.Fatalf("no success happened, last_success_age_s must be omitted, got %d", *resp.Anonymous.LastSuccessAgeS)
	}
}

func TestStaleQuotaCauseDropped(t *testing.T) {
	pool, _ := staleTestPool(t, "FreeUsageLimitError (upstream HTTP 429)")
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "ok" {
		t.Fatalf("expired quota cause must not hold a switch vote, got %q", resp.Anonymous.Verdict)
	}
	if resp.RecommendSwitch {
		t.Fatal("expired quota must not recommend a switch")
	}
}

func TestStaleTTLExpiryReturnsOK(t *testing.T) {
	pool, node := staleTestPool(t, "transport (connection failed)")
	node.causeExpires.Store(time.Now().Add(-time.Second).UnixNano())
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "ok" || resp.Anonymous.Cause != "" {
		t.Fatalf("past-TTL stale cause must be forgotten, got verdict %q cause %q", resp.Anonymous.Verdict, resp.Anonymous.Cause)
	}
	if resp.RecommendSwitch {
		t.Fatal("past-TTL lane must not recommend a switch")
	}
}

// The stale window is anchored to the cooldown deadline: a transport cause
// must keep voting for staleCauseTTL *after* its own cooldown lapsed, not stop
// at the instant the cooldown was due to end. (The whole window is shifted
// into the past so no sleeping is involved.)
func TestStaleCauseSurvivesItsOwnCooldown(t *testing.T) {
	pool, node := watchdogCoolingPool(t, "transport (connection failed)")
	shift := -staleCauseTTL + time.Minute
	node.cooldownUntil.Store(node.cooldownUntil.Load() + int64(shift))
	node.causeExpires.Store(node.causeExpires.Load() + int64(shift))
	if remain, _ := pool.maxCooldownSnapshot(); remain != 0 {
		t.Fatalf("setup must leave the cooldown lapsed, got %v", remain)
	}
	if node.causeExpires.Load() <= time.Now().UnixNano() {
		t.Fatalf("setup must leave the stale window still open, got %v", time.Unix(0, node.causeExpires.Load()).Sub(time.Now()))
	}
	if got := pool.staleCauseSnapshot(); got != "transport (connection failed)" {
		t.Fatalf("cause must outlive its own cooldown by staleCauseTTL, got %q", got)
	}
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	if resp := doWatchdogStatus(t, g); resp.Anonymous.Verdict != "transport" || !resp.RecommendSwitch {
		t.Fatalf("lapsed-but-fresh transport cause must still switch, got verdict %q switch %v", resp.Anonymous.Verdict, resp.RecommendSwitch)
	}
}

// The feed is a worst-case snapshot across transport-healthy proxies: the
// freshest stale TTL wins, and a stale vote on an unhealthy (never served)
// proxy must not reach the script. Deadlines are set explicitly because the
// clock granularity is far coarser than the differences under test.
func TestStaleCauseSnapshotWorstCaseAcrossProxies(t *testing.T) {
	proxies := make([]*proxyTransport, 3)
	nodes := make([]*anonymousNode, 3)
	for i := range proxies {
		proxies[i] = &proxyTransport{index: i, name: fmt.Sprintf("p%d", i)}
		proxies[i].healthy.Store(true)
		nodes[i] = &anonymousNode{proxy: proxies[i]}
	}
	pool := &anonymousPool{nodes: nodes, cooldown: 15 * time.Second}
	now := time.Now()
	// Two expired cooldowns, both still inside the stale TTL, with different
	// deadlines: the later one is the freshest evidence.
	nodes[0].lastCause.Store("transport (timeout)")
	nodes[0].causeExpires.Store(now.Add(3 * time.Minute).UnixNano())
	nodes[1].lastCause.Store("transport (connection failed)")
	nodes[1].causeExpires.Store(now.Add(time.Minute).UnixNano())
	if got := pool.staleCauseSnapshot(); got != "transport (timeout)" {
		t.Fatalf("freshest stale cause must win, got %q", got)
	}
	// The freshest vote sits on a proxy that is not served: its residue cannot
	// cry wolf to the script, so the older healthy vote is reported instead.
	proxies[0].healthy.Store(false)
	if got := pool.staleCauseSnapshot(); got != "transport (connection failed)" {
		t.Fatalf("unhealthy proxy must be skipped, got %q", got)
	}
	// A quota residue must not be surfaced as transport evidence.
	nodes[1].lastCause.Store("FreeUsageLimitError (upstream HTTP 429)")
	if got := pool.staleCauseSnapshot(); got != "" {
		t.Fatalf("only transport residues may vote, got %q", got)
	}
	// Past-TTL on every proxy: the lane is simply ok again.
	proxies[0].healthy.Store(true)
	nodes[0].causeExpires.Store(now.Add(-time.Second).UnixNano())
	nodes[1].causeExpires.Store(now.Add(-time.Second).UnixNano())
	if got := pool.staleCauseSnapshot(); got != "" {
		t.Fatalf("past-TTL stale causes must be forgotten, got %q", got)
	}
}

func TestMarkSuccessClearsStale(t *testing.T) {
	pool, _ := staleTestPool(t, "transport (connection failed)")
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	pool.MarkSuccess(pool.nodes[0])
	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "ok" || resp.Anonymous.Cause != "" {
		t.Fatalf("user success must clear the stale vote, got verdict %q cause %q", resp.Anonymous.Verdict, resp.Anonymous.Cause)
	}
	if resp.Anonymous.LastSuccessAgeS == nil {
		t.Fatal("user success must report last_success_age_s")
	}
}

func TestRefreshSuccessKeepsStaleWithoutSuccessAge(t *testing.T) {
	pool, _ := staleTestPool(t, "transport (connection failed)")
	pool.markRefreshSuccess(pool.nodes[0])
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "transport" {
		t.Fatalf("background refresh must not fake recovery, got verdict %q", resp.Anonymous.Verdict)
	}
	if resp.Anonymous.LastSuccessAgeS != nil {
		t.Fatal("background refresh must not advance last_success_age_s")
	}
}

// A recovered proxy must take its stale transport memory with it: that vote
// exists only because nothing proved the route back, and a passing proxy
// health check is exactly the transport-layer proof. Left behind, the feed
// keeps recommending a switch for up to staleCauseTTL on a node that is
// demonstrably fine — one needless switch, and probably one switch back.
func TestRestoreProxyClearsStaleTransportVote(t *testing.T) {
	pool, node := staleTestPool(t, "transport (connection failed)")
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	before := doWatchdogStatus(t, g)
	if before.Anonymous.Verdict != "transport" || !before.RecommendSwitch {
		t.Fatalf("setup must leave a live stale transport vote, got verdict %q switch %v (cause %q)", before.Anonymous.Verdict, before.RecommendSwitch, before.Anonymous.Cause)
	}
	if cleared := pool.RestoreProxy(node.proxy); cleared != 1 {
		t.Fatalf("restore must report the healed node once, got %d", cleared)
	}
	// The pre-existing contract still holds: cooldown, counter and its label go.
	if remain, cause := pool.maxCooldownSnapshot(); remain != 0 || cause != "" {
		t.Fatalf("restore must clear the cooldown, got %v/%q", remain, cause)
	}
	if node.failures.Load() != 0 || node.cooldownUntil.Load() != 0 {
		t.Fatalf("restore must clear failures and cooldown, got failures %d cooldown %d", node.failures.Load(), node.cooldownUntil.Load())
	}
	if stale, _ := node.lastCause.Load().(string); stale != "" || node.causeExpires.Load() != 0 {
		t.Fatalf("restore must drop the stale cause memory, got %q expiring at %d", stale, node.causeExpires.Load())
	}
	after := doWatchdogStatus(t, g)
	if after.Anonymous.Verdict != "ok" || after.Anonymous.Cause != "" {
		t.Fatalf("a recovered proxy must read ok again, got verdict %q cause %q", after.Anonymous.Verdict, after.Anonymous.Cause)
	}
	if after.RecommendSwitch {
		t.Fatal("a recovered proxy must not keep recommending a node switch")
	}
	if after.Anonymous.CoolingSeconds != 0 {
		t.Fatalf("restore must leave no cooling behind, got %d s", after.Anonymous.CoolingSeconds)
	}
	// User-traffic evidence is untouched: the script still needs a real turn
	// before it calls the node recovered, and the failure age stays readable.
	if after.Anonymous.LastFailureAgeS == nil {
		t.Fatal("restore must not erase the recorded user-traffic failure age")
	}
	if after.Anonymous.LastSuccessAgeS != nil {
		t.Fatalf("restore must not fake a user-traffic success, got %d s", *after.Anonymous.LastSuccessAgeS)
	}
}

// The same fix through the real seam: the background health check that judges
// a proxy recovered (a passed check that saw the proxy unhealthy) is what
// reaches the pool, not the feed's own logic.
func TestProxyHealthCheckRestoreClearsStaleTransportVote(t *testing.T) {
	pool, node := staleTestPool(t, "transport (timeout)")
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	if resp := doWatchdogStatus(t, g); resp.Anonymous.Verdict != "transport" || !resp.RecommendSwitch {
		t.Fatalf("setup must leave a live stale transport vote, got verdict %q switch %v (cause %q)", resp.Anonymous.Verdict, resp.RecommendSwitch, resp.Anonymous.Cause)
	}
	g.applyProxyHealthResult(proxyHealthResult{proxy: node.proxy, err: nil, wasHealthy: false}, "scheduled health check", 0)
	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "ok" || resp.RecommendSwitch {
		t.Fatalf("a passed health check must stop the stale vote, got verdict %q switch %v (cause %q)", resp.Anonymous.Verdict, resp.RecommendSwitch, resp.Anonymous.Cause)
	}
}

// A background catalog refresh heals the cooldown but deliberately keeps the
// stale vote, which leaves the node with zero failures and zero cooldown. The
// restore must still drop the memory: skipping it as "nothing to clear" would
// leave a live vote on a proxy already reported healthy.
func TestRestoreProxyClearsStaleVoteAfterRefreshSuccess(t *testing.T) {
	pool, node := staleTestPool(t, "transport (connection failed)")
	pool.markRefreshSuccess(node)
	if node.failures.Load() != 0 || node.cooldownUntil.Load() != 0 {
		t.Fatalf("setup must leave nothing but the stale memory, got failures %d cooldown %d", node.failures.Load(), node.cooldownUntil.Load())
	}
	if got := pool.staleCauseSnapshot(); got != "transport (connection failed)" {
		t.Fatalf("background refresh must keep the stale vote, got %q", got)
	}
	if cleared := pool.RestoreProxy(node.proxy); cleared != 1 {
		t.Fatalf("restore must report the cleared stale memory, got %d", cleared)
	}
	if got := pool.staleCauseSnapshot(); got != "" {
		t.Fatalf("restore must drop the stale vote a refresh left behind, got %q", got)
	}
}

// The premise that makes dropping a recovered proxy's cause memory safe: the
// only causes that keep voting after their cooldown lapsed are the ones that
// classify as transport (staleCauseSnapshot filters the rest), while a quota,
// ratelimit or fingerprint residue never votes there — so a passing proxy
// health check, which proves the transport layer only, cannot be masking an
// allowance problem. If this table ever starts voting a non-transport residue,
// dropping the memory on restore would hide it and this test goes red.
func TestStaleVotesAreTransportOnly(t *testing.T) {
	cases := []struct{ cause, want string }{
		{"transport (connection failed)", "transport (connection failed)"},
		{"transport (timeout)", "transport (timeout)"},
		// A 5xx tail classifies as transport (server-side wobble, not
		// allowance), so it is votable and is dropped on restore as well.
		{"upstream error (upstream HTTP 502)", "upstream error (upstream HTTP 502)"},
		{"FreeUsageLimitError (upstream HTTP 429)", ""},
		{"BlackUsageLimitError (upstream HTTP 429)", ""},
		{"RateLimitError (upstream HTTP 429)", ""},
		{"FreeTierError (upstream HTTP 403)", ""},
		{"upstream error (upstream HTTP 429)", ""},
		{"", ""},
	}
	for _, tc := range cases {
		proxy := &proxyTransport{index: 0, name: "direct"}
		proxy.healthy.Store(true)
		node := &anonymousNode{proxy: proxy}
		pool := &anonymousPool{nodes: []*anonymousNode{node}, cooldown: 15 * time.Second}
		node.lastCause.Store(tc.cause)
		node.causeExpires.Store(time.Now().Add(time.Minute).UnixNano())
		if got := pool.staleCauseSnapshot(); got != tc.want {
			t.Errorf("cause %q: stale vote = %q, want %q", tc.cause, got, tc.want)
		}
	}
}

// Background catalog failures still cool the lane (unchanged baseline
// behavior), but they must leave no user-traffic evidence behind: no failure
// age, no stale transport vote, and a cooling verdict with an empty cause.
func TestBackgroundMarkFailureRecordsNoUserEvidence(t *testing.T) {
	proxy := &proxyTransport{index: 0, name: "direct"}
	proxy.healthy.Store(true)
	node := &anonymousNode{proxy: proxy}
	pool := &anonymousPool{nodes: []*anonymousNode{node}, cooldown: 15 * time.Second}
	pool.MarkFailure(node, nil, errors.New("dial failed"))
	remain, cause := pool.maxCooldownSnapshot()
	if remain <= 0 {
		t.Fatalf("background failure must still cool the node, got %v", remain)
	}
	if cause != "" {
		t.Fatalf("background failure must not label the cooldown, got %q", cause)
	}
	if node.lastFailure.Load() != 0 || node.causeExpires.Load() != 0 {
		t.Fatalf("background failure must not write watchdog memory, got failure=%d expiry=%d", node.lastFailure.Load(), node.causeExpires.Load())
	}
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "cooling" || resp.Anonymous.LastFailureAgeS != nil {
		t.Fatalf("background cooling must stay an unknown-cause cooldown, got verdict %q failureAge %v", resp.Anonymous.Verdict, resp.Anonymous.LastFailureAgeS)
	}
}

// Reading the feed must never create or extend evidence: no success stamp, no
// failure stamp, no TTL extension, no cooldown change.
func TestWatchdogStatusIsReadOnly(t *testing.T) {
	pool, node := watchdogCoolingPool(t, "transport (connection failed)")
	g := watchdogTestGateway(watchdogTestTransports(true), pool)
	untilBefore := node.cooldownUntil.Load()
	expiresBefore := node.causeExpires.Load()
	for range 3 {
		doWatchdogStatus(t, g)
	}
	if node.cooldownUntil.Load() != untilBefore {
		t.Fatalf("reading the feed must not move the cooldown: %d -> %d", untilBefore, node.cooldownUntil.Load())
	}
	if node.causeExpires.Load() != expiresBefore {
		t.Fatalf("reading the feed must not extend the stale TTL: %d -> %d", expiresBefore, node.causeExpires.Load())
	}
	if node.lastSuccess.Load() != 0 {
		t.Fatalf("reading the feed must not create a success stamp, got %d", node.lastSuccess.Load())
	}
	if node.failures.Load() == 0 {
		t.Fatal("setup must leave the node cooling with a recorded failure")
	}
}

func watchdogErrorResponse(status int, contentType, body string) *http.Response {
	header := http.Header{}
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}
	return &http.Response{
		StatusCode:    status,
		Header:        header,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

// watchdogChunkedResponse is watchdogErrorResponse for an answer whose length
// the upstream never announced (ContentLength -1, i.e. chunked). It is the
// only way to reach the peek-restore path for a payload above maxCausePeek: a
// known length above the bound is rejected before a single byte is read, so
// the restore of the not-yet-read remainder would otherwise never be
// exercised. body is taken as an io.ReadCloser on purpose, so a test can put a
// scripted (or failing) reader behind it.
func watchdogChunkedResponse(status int, contentType string, body io.ReadCloser) *http.Response {
	header := http.Header{}
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}
	return &http.Response{
		StatusCode:    status,
		Header:        header,
		Body:          body,
		ContentLength: -1,
	}
}

// scriptedUpstreamBody is an upstream error body on rails: it serves data as a
// plain sequential stream, except that the very first Read also reports
// cutErr — which is what a connection dying mid-body looks like to io.ReadAll
// (bytes plus an error in a single call). closed counts Close calls, because a
// restore that swallows the network body's Close would leak it silently.
type scriptedUpstreamBody struct {
	data   []byte
	cutAt  int // bytes handed out together with cutErr (-1: never cut)
	cutErr error
	pos    int
	closed int
}

func (b *scriptedUpstreamBody) Read(p []byte) (int, error) {
	if b.cutErr != nil && b.pos == 0 {
		b.pos = min(b.cutAt, len(b.data))
		return copy(p, b.data[:b.pos]), b.cutErr
	}
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}

func (b *scriptedUpstreamBody) Close() error {
	b.closed++
	return nil
}

// firstMismatch reports where two byte strings start to differ (-1 when equal).
func firstMismatch(got, want []byte) int {
	for i := range min(len(got), len(want)) {
		if got[i] != want[i] {
			return i
		}
	}
	if len(got) != len(want) {
		return min(len(got), len(want))
	}
	return -1
}

// A chunked error body larger than the peek bound is the one real-world shape
// that exercises the restore: the peek stops at the bound and everything past
// it is still sitting in the original body. The client must receive the whole
// document, and Close must still reach the original.
func TestWatchdogCauseChunkedOversizedBodyKeepsWholePayload(t *testing.T) {
	full := `{"error":{"type":"FreeUsageLimitError"},"padding":"` + strings.Repeat("x", maxCausePeek+64) + `"}`
	if len(full) <= maxCausePeek {
		t.Fatalf("fixture must exceed the peek bound, got %d bytes", len(full))
	}
	upstream := &scriptedUpstreamBody{data: []byte(full), cutAt: -1}
	resp := watchdogChunkedResponse(http.StatusTooManyRequests, "application/json", upstream)
	if got := watchdogCause(resp, nil); got != "upstream error (upstream HTTP 429)" {
		t.Fatalf("oversized chunked body must fall back to the status, got %q", got)
	}
	if upstream.pos <= maxCausePeek {
		t.Fatalf("fixture must make the peek run into the bound, it read %d bytes", upstream.pos)
	}
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != full {
		t.Fatalf("chunked upstream body must survive the peek byte for byte: got %d of %d bytes, first difference at %d", len(rest), len(full), firstMismatch(rest, []byte(full)))
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing the restored body must succeed, got %v", err)
	}
	if upstream.closed == 0 {
		t.Fatal("Close must reach the original upstream body, not a copy of its bytes")
	}
}

// A body that dies mid-read is the other way into the restore: the peek keeps
// the bytes it got before the error, and the still-open original must stay
// readable and closable behind them. Truncating the body to the peeked prefix
// here would silently drop the rest of an upstream error document.
func TestWatchdogCauseCutMidBodyKeepsPrefixAndClosesOriginal(t *testing.T) {
	const prefix = `{"error":{"type":"FreeUsageLimitErr`
	const rest = `or"},"message":"cut mid-body"}`
	upstream := &scriptedUpstreamBody{
		data:   []byte(prefix + rest),
		cutAt:  len(prefix),
		cutErr: io.ErrUnexpectedEOF,
	}
	resp := watchdogChunkedResponse(http.StatusBadGateway, "application/json", upstream)
	if got := watchdogCause(resp, nil); got != "upstream error (upstream HTTP 502)" {
		t.Fatalf("an unreadable body must still yield the status tail, got %q", got)
	}
	if upstream.pos != len(prefix) {
		t.Fatalf("fixture must cut the read right after the prefix, it read %d of %d bytes", upstream.pos, len(prefix))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the restored body must not fail, got %v", err)
	}
	if !strings.HasPrefix(string(body), prefix) {
		t.Fatalf("the peeked prefix must not be dropped, got %q", body)
	}
	if string(body) != prefix+rest {
		t.Fatalf("the still-open original must stay readable behind the prefix, got %q", body)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing the restored body must succeed, got %v", err)
	}
	if upstream.closed != 1 {
		t.Fatalf("Close must reach the original exactly once, got %d", upstream.closed)
	}
}

// The two shapes above only reach the restore because an unknown Content-Length
// defeats the cheap "too big to even look" exit; pin that boundary so a future
// guard change cannot quietly make the restore unreachable again.
func TestWatchdogCauseKnownLengthAboveBoundSkipsThePeek(t *testing.T) {
	upstream := &scriptedUpstreamBody{data: []byte(strings.Repeat("y", maxCausePeek+64)), cutAt: -1}
	resp := watchdogChunkedResponse(http.StatusTooManyRequests, "application/json", upstream)
	resp.ContentLength = int64(maxCausePeek + 1)
	if got := watchdogCause(resp, nil); got != "upstream error (upstream HTTP 429)" {
		t.Fatalf("known-oversized body must fall back to the status, got %q", got)
	}
	if upstream.pos != 0 {
		t.Fatalf("a known oversized length must be rejected before reading a byte, read %d", upstream.pos)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != string(upstream.data) {
		t.Fatalf("an unread body must pass through untouched, got %d of %d bytes", len(body), len(upstream.data))
	}
}

func TestWatchdogCauseLabelsUpstreamClassAndRestoresBody(t *testing.T) {
	body := `{"error":{"type":"FreeUsageLimitError","message":"free quota burned"}}`
	resp := watchdogErrorResponse(http.StatusTooManyRequests, "application/json", body)
	cause := watchdogCause(resp, nil)
	if cause != "FreeUsageLimitError (upstream HTTP 429)" {
		t.Fatalf("cause = %q, want the upstream class plus status", cause)
	}
	if classifyAnonymousVerdict(cause, true) != "quota" {
		t.Fatalf("a real quota answer must classify as quota, got %q", classifyAnonymousVerdict(cause, true))
	}
	// The label is a label only: the passthrough body must survive byte for byte.
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != body {
		t.Fatalf("upstream error body must survive intact, got %q", rest)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing the restored body must reach the original, got %v", err)
	}
}

func TestWatchdogCauseRateLimitAndFreeTier(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"rateLimit", http.StatusTooManyRequests, `{"error":{"type":"RateLimitError"}}`, "RateLimitError (upstream HTTP 429)"},
		{"freeTier", http.StatusForbidden, `{"error":{"type":"FreeTierError"}}`, "FreeTierError (upstream HTTP 403)"},
	}
	for _, tc := range cases {
		if got := watchdogCause(watchdogErrorResponse(tc.status, "application/json", tc.body), nil); got != tc.want {
			t.Errorf("%s: cause = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestWatchdogCauseTransportAndStatusOnly(t *testing.T) {
	if got := watchdogCause(nil, context.DeadlineExceeded); got != "transport (timeout)" {
		t.Fatalf("no-response timeout cause = %q", got)
	}
	if got := watchdogCause(nil, errors.New("dial tcp: connection refused")); got != "transport (connection failed)" {
		t.Fatalf("no-response dial cause = %q", got)
	}
	if got := watchdogCause(nil, nil); got != "transport (no usable route)" {
		t.Fatalf("no-response no-error cause = %q", got)
	}
	// A body that carries no error class still yields the status tail, and
	// that tail must stay parenthesized: a 5xx then classifies as transport
	// instead of falling back to "unknown cause, just wait".
	classless := watchdogCause(watchdogErrorResponse(http.StatusBadGateway, "text/plain", "bad gateway"), nil)
	if classless != "upstream error (upstream HTTP 502)" {
		t.Fatalf("classless upstream cause = %q", classless)
	}
	if got := classifyAnonymousVerdict(classless, true); got != "transport" {
		t.Fatalf("classless 5xx must classify as transport, got %q", got)
	}
	// A response plus a transport error keeps the transport prefix, which
	// outranks the status in the classifier.
	if got := watchdogCause(watchdogErrorResponse(http.StatusServiceUnavailable, "application/json", ""), errors.New("broken pipe")); got != "transport (connection failed) (upstream HTTP 503)" {
		t.Fatalf("response-with-error cause = %q", got)
	}
}

// A classless upstream answer must still carry the parenthesized status tail:
// the classifier parses it from there, so dropping the parentheses would turn a
// server-side wobble into "cooling" and stop the watchdog from switching.
func TestClasslessUpstreamAnswersKeepTheStatusTail(t *testing.T) {
	cases := []struct {
		status      int
		want        string
		wantVerdict string
	}{
		{http.StatusInternalServerError, "upstream error (upstream HTTP 500)", "transport"},
		{http.StatusBadGateway, "upstream error (upstream HTTP 502)", "transport"},
		// A 429 without a class is genuinely ambiguous (quota or throttle):
		// with no keyword to match it stays "wait", never a switch.
		{http.StatusTooManyRequests, "upstream error (upstream HTTP 429)", "cooling"},
		{http.StatusForbidden, "upstream error (upstream HTTP 403)", "cooling"},
	}
	for _, tc := range cases {
		cause := watchdogCause(watchdogErrorResponse(tc.status, "text/plain", "upstream said no"), nil)
		if cause != tc.want {
			t.Errorf("status %d: cause = %q, want %q", tc.status, cause, tc.want)
		}
		if got := upstreamStatusFromCause(cause); got != tc.status {
			t.Errorf("status %d: parsed status = %d, want %d", tc.status, got, tc.status)
		}
		if got := classifyAnonymousVerdict(cause, true); got != tc.wantVerdict {
			t.Errorf("status %d: verdict = %q, want %q", tc.status, got, tc.wantVerdict)
		}
	}
}

// The upstream error body must never be truncated by the label peek, however
// large it is, and the peek must never read a streaming body to EOF.
func TestWatchdogCauseOversizedBodyKeepsPayload(t *testing.T) {
	body := `{"error":{"type":"FreeUsageLimitError"},"padding":"` + strings.Repeat("x", maxCausePeek+64) + `"}`
	resp := watchdogErrorResponse(http.StatusTooManyRequests, "application/json", body)
	if got := watchdogCause(resp, nil); got != "upstream error (upstream HTTP 429)" {
		t.Fatalf("oversized body must fall back to the status, got %q", got)
	}
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != body {
		t.Fatalf("oversized upstream body must survive intact, got %d of %d bytes", len(rest), len(body))
	}
}

// slowBodyTail is how much of a gated slow body follows its error document: far
// beyond the 64 KB peek bound (and beyond the 256 KB one it replaced), so a
// peek that reads on towards the bound has real bytes to wait for.
const slowBodyTail = 1 << 20

// gatedSlowBody is an upstream error body on a slow unknown-length pipe: the
// first Read hands out a complete, small error document and every later Read
// blocks until the test releases a slice of the tail. That is the shape the
// label peek used to turn into dead waiting time — a slow body the peek read
// past its useful prefix made the switch to the next exit node wait for the
// whole budget, whatever the tail's size. Only waitFor ends a read the test
// did not release, so "did the peek wait for the tail?" is a fact to assert
// rather than a guess: blocked stays 0 and the elapsed time stays tiny only
// when the peek stopped as soon as it had what it came for.
type gatedSlowBody struct {
	head    string
	tail    string
	tailPos int
	closed  int
	release chan struct{} // one buffered token = one slice of tail
	blocked int           // reads that had to wait for a token
	rushed  bool          // set once the test stops caring about the waiting
	waitFor time.Duration
}

func (b *gatedSlowBody) Read(p []byte) (int, error) {
	if b.head != "" {
		head := b.head
		b.head = ""
		return copy(p, head), nil
	}
	if !b.rushed {
		select {
		case <-b.release:
		default:
			b.blocked++
			timer := time.NewTimer(b.waitFor)
			select {
			case <-b.release:
			case <-timer.C:
			}
			timer.Stop()
			b.rushed = true
		}
	}
	if b.tailPos >= len(b.tail) {
		return 0, io.EOF
	}
	n := copy(p, b.tail[b.tailPos:])
	b.tailPos += n
	return n, nil
}

func (b *gatedSlowBody) Close() error {
	b.closed++
	return nil
}

// releaseTail un-gates the body: after this the tail flows at full speed, so a
// test can measure the peek's waiting first and then read the passthrough
// without racing the fixture.
func (b *gatedSlowBody) releaseTail() {
	b.rushed = true
}

func newGatedSlowBody(head string) *gatedSlowBody {
	return &gatedSlowBody{
		head:    head,
		tail:    strings.Repeat("t", slowBodyTail),
		release: make(chan struct{}, 1),
		waitFor: 2 * time.Second,
	}
}

// A slow, unknown-length error body whose class sits at the front is the one
// shape where peeking could make a node switch wait: the label needs the small
// head, the tail only carries payload. The peek must stop at the head — both
// the class must survive and the upstream's slow tail must not be waited for —
// while the client still receives the document byte for byte.
func TestWatchdogCauseStopsPeekAtTypeWithoutDrainingSlowBody(t *testing.T) {
	const head = `{"error":{"type":"FreeUsageLimitError"},"message":"free quota burned"}`
	body := newGatedSlowBody(head)
	resp := watchdogChunkedResponse(http.StatusTooManyRequests, "application/json", body)

	start := time.Now()
	cause := watchdogCause(resp, nil)
	elapsed := time.Since(start)

	if body.blocked != 0 {
		t.Fatalf("the peek must not wait for a tail the class does not need, it blocked %d read(s)", body.blocked)
	}
	if elapsed > 750*time.Millisecond {
		t.Fatalf("labelling a slow body must not cost a wall-clock wait, took %v", elapsed)
	}
	if cause != "FreeUsageLimitError (upstream HTTP 429)" {
		t.Fatalf("a slow chunked quota answer must still carry the upstream class, got %q", cause)
	}
	if got := classifyAnonymousVerdict(cause, true); got != "quota" {
		t.Fatalf("stopping early must not cost the verdict, got %q", got)
	}
	// Everything the peek did not need still has to reach the client intact.
	body.releaseTail()
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if want := head + body.tail; string(rest) != want {
		t.Fatalf("the slow body must survive the peek byte for byte: got %d of %d bytes, first difference at %d", len(rest), len(want), firstMismatch(rest, []byte(want)))
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing the restored body must succeed, got %v", err)
	}
	if body.closed != 1 {
		t.Fatalf("Close must reach the original exactly once, got %d", body.closed)
	}
}

// drainBudgetBytes is what httpx.DrainAndClose charges a response the gateway
// abandons without looking at it (its io.LimitReader body, 64<<10). The label
// peek rides on that failure path, so its worst case must fit the same budget:
// it is asserted here as a literal on purpose, because a test written against
// maxCausePeek would follow the constant up and stop pinning anything.
const drainBudgetBytes = 64 << 10

// budgetBody is a non-JSON upstream error body (a giant HTML error page) whose
// reads are metered while the peek runs: the shape that must read all the way
// to the peek's bound, and therefore the worst case for how long a node switch
// can wait. Past the drain budget plus the peek's own oversize probe byte it
// refuses instead of serving more, so an over-reading peek cannot pass by
// accident. The meter is scoped to the peek (stopMetering) because the
// passthrough behind it is entitled to every remaining byte.
type budgetBody struct {
	data      string
	served    int
	metered   bool
	overDrain bool
	closed    int
}

func newBudgetBody(data string) *budgetBody {
	return &budgetBody{data: data, metered: true}
}

func (b *budgetBody) Read(p []byte) (int, error) {
	if b.metered && b.served > drainBudgetBytes+1 {
		b.overDrain = true
		return 0, errors.New("peek read past the drain budget")
	}
	if b.served >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.served:])
	b.served += n
	return n, nil
}

func (b *budgetBody) stopMetering() {
	b.metered = false
}

func (b *budgetBody) Close() error {
	b.closed++
	return nil
}

// The worst case of the peek is a body that never completes an error
// document: there is nothing to stop early on, so the bound is all the budget
// it may spend — one byte past the drain ceiling, which is the same oversize
// probe the peek has always paid, and no more. The upstream document must
// still arrive byte for byte behind the peeked prefix.
func TestWatchdogCauseWorstCasePeekStaysWithinDrainBudget(t *testing.T) {
	payload := strings.Repeat("<html>gateway said no</html>", 64<<10)
	body := newBudgetBody(payload)
	resp := watchdogChunkedResponse(http.StatusBadGateway, "text/html", body)

	if got := watchdogCause(resp, nil); got != "upstream error (upstream HTTP 502)" {
		t.Fatalf("a classless body must fall back to the status, got %q", got)
	}
	if body.overDrain {
		t.Fatalf("the peek must never read past the drain budget")
	}
	if body.served > drainBudgetBytes+1 {
		t.Fatalf("worst-case peek read %d bytes, budget is the %d-byte drain plus the oversize probe", body.served, drainBudgetBytes)
	}
	if body.served < drainBudgetBytes {
		t.Fatalf("fixture must drive the peek to its bound, it read %d bytes", body.served)
	}
	body.stopMetering()
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != payload {
		t.Fatalf("the classless body must survive the peek byte for byte: got %d of %d bytes, first difference at %d", len(rest), len(payload), firstMismatch(rest, []byte(payload)))
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing the restored body must succeed, got %v", err)
	}
	if body.closed != 1 {
		t.Fatalf("Close must reach the original exactly once, got %d", body.closed)
	}
}

// End-to-end seam for that shape: a slow oversized 429 whose class is in the
// first chunk must still land in the feed as quota with a switch
// recommendation, and the node switch it recommends must not have waited on
// the tail.
func TestObserveAnonymousResultSlowBodyStillRecordsQuotaSwitch(t *testing.T) {
	const head = `{"error":{"type":"FreeUsageLimitError","message":"no free credits left"}}`
	transports, err := newTransportPool([]string{"direct"}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool := newAnonymousPool(true, transports, 15*time.Second)
	g := watchdogTestGateway(transports, pool)
	freezeProxyHealth(g)
	body := newGatedSlowBody(head)
	upstream := watchdogChunkedResponse(http.StatusTooManyRequests, "application/json", body)

	start := time.Now()
	g.observeAnonymousResult(context.Background(), pool.nodes[0], upstream, nil)
	elapsed := time.Since(start)

	if body.blocked != 0 {
		t.Fatalf("recording the verdict must not wait for the slow tail, it blocked %d read(s)", body.blocked)
	}
	if elapsed > 750*time.Millisecond {
		t.Fatalf("labelling a slow body must not cost a wall-clock wait, took %v", elapsed)
	}
	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "quota" || !resp.RecommendSwitch {
		t.Fatalf("slow 429 FreeUsageLimitError must surface as quota+switch, got verdict %q switch %v (cause %q)", resp.Anonymous.Verdict, resp.RecommendSwitch, resp.Anonymous.Cause)
	}
	body.releaseTail()
	rest, err := io.ReadAll(upstream.Body)
	if err != nil {
		t.Fatal(err)
	}
	if want := head + body.tail; string(rest) != want {
		t.Fatalf("the passthrough body must survive the label: got %d of %d bytes, first difference at %d", len(rest), len(want), firstMismatch(rest, []byte(want)))
	}
}

func TestWatchdogCauseNeverReadsStreamOrSuccess(t *testing.T) {
	stream := watchdogErrorResponse(http.StatusTooManyRequests, "text/event-stream", "")
	if got := watchdogCause(stream, nil); got != "upstream error (upstream HTTP 429)" {
		t.Fatalf("event-stream body must not be peeked, got %q", got)
	}
	ok := watchdogErrorResponse(http.StatusOK, "text/event-stream", "")
	if got := watchdogCause(ok, nil); got != "upstream HTTP 200" {
		t.Fatalf("2xx must never be peeked, got %q", got)
	}
}

// End-to-end seam: what the request path records for a real anonymous
// attempt must land in the feed as a quota verdict, with the upstream body
// still passed through verbatim.
func TestObserveAnonymousResultRecordsQuotaVerdict(t *testing.T) {
	const payload = `{"error":{"type":"FreeUsageLimitError","message":"no free credits left"}}`
	transports, err := newTransportPool([]string{"direct"}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool := newAnonymousPool(true, transports, 15*time.Second)
	g := watchdogTestGateway(transports, pool)
	freezeProxyHealth(g)
	node := pool.nodes[0]
	upstream := watchdogErrorResponse(http.StatusTooManyRequests, "application/json", payload)
	g.observeAnonymousResult(context.Background(), node, upstream, nil)

	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "quota" || !resp.RecommendSwitch {
		t.Fatalf("429 FreeUsageLimitError must surface as quota+switch, got verdict %q switch %v (cause %q)", resp.Anonymous.Verdict, resp.RecommendSwitch, resp.Anonymous.Cause)
	}
	if resp.Anonymous.Cause != "FreeUsageLimitError (upstream HTTP 429)" {
		t.Fatalf("cause = %q", resp.Anonymous.Cause)
	}
	if resp.Anonymous.LastFailureAgeS == nil {
		t.Fatal("a real user-traffic failure must report last_failure_age_s")
	}
	if resp.Anonymous.LastSuccessAgeS != nil {
		t.Fatal("no success happened yet, last_success_age_s must be omitted")
	}
	rest, err := io.ReadAll(upstream.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != payload {
		t.Fatalf("the passthrough body must survive the label, got %q", rest)
	}
}

// Diagnostic requests must never write pool state (existing isolation).
func TestObserveAnonymousResultSkipsDiagnostic(t *testing.T) {
	transports, err := newTransportPool([]string{"direct"}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool := newAnonymousPool(true, transports, 15*time.Second)
	g := watchdogTestGateway(transports, pool)
	freezeProxyHealth(g)
	node := pool.nodes[0]
	ctx := WithDiagnosticRequest(context.Background())
	g.observeAnonymousResult(ctx, node, watchdogErrorResponse(http.StatusTooManyRequests, "application/json", `{"error":{"type":"FreeUsageLimitError"}}`), nil)
	if remain, _ := pool.maxCooldownSnapshot(); remain != 0 {
		t.Fatalf("diagnostic request must not cool the lane, got %v", remain)
	}
	if node.lastFailure.Load() != 0 {
		t.Fatalf("diagnostic request must not write watchdog memory, got %d", node.lastFailure.Load())
	}
}
