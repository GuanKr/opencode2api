package gateway

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/rotation"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/telemetry"
)

// The watchdog probe (watchdog.go): the one request that must always reach the
// upstream, and must never touch production state while doing so.
//
// Every case here is offline. The probe calls the upstream client directly and
// writes no pool state, so it cannot schedule the background proxy verification
// that dials the public internet; freezeProxyHealth parks the flag anyway so
// that stays true by construction if the path is ever refactored.

const probeTestKey = "probe-test-key"

// probeGateway wires a gateway whose anonymous lane points at baseURL, with one
// free test model in the catalog and the proxy health machinery frozen.
func probeGateway(t *testing.T, baseURL string) *Gateway {
	t.Helper()
	g := newStubGateway(t, baseURL)
	g.cfg.ServerKeys = []string{probeTestKey}
	// newStubGateway arms a 1s base cooldown; a fixture cooldown that can expire
	// mid-test would quietly turn "the gate is closed" into "the gate is open"
	// on a slow machine.
	g.anonymous.cooldown = 30 * time.Second
	registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
		config.TierZen: {"probe-free": wire.Chat},
	})
	freezeProxyHealth(g)
	return g
}

// sendProbe posts one probe through the real mux, so route registration and
// server-key authentication are covered together with the handler.
func sendProbe(t *testing.T, g *Gateway, body string) (*httptest.ResponseRecorder, watchdogProbeResponse) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/watchdog/probe", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+probeTestKey)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	// Wrapped exactly like production (runtime.go wraps the mux the same way) so
	// the outcome/telemetry bookkeeping inside the handler runs here too.
	telemetry.Middleware(g.monitor, g.logger, g.Handler()).ServeHTTP(rec, req)
	var out watchdogProbeResponse
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("probe answer is not JSON (status %d): %s", rec.Code, rec.Body.String())
		}
	}
	return rec, out
}

// stubUpstream serves the given status/body for every request and counts hits.
func stubUpstream(t *testing.T, status int, body string) (string, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server.URL, &hits
}

// closeTheGates reproduces the state the probe exists for: the single proxy is
// marked transport-unhealthy (a dial timed out while Clash re-routed a node
// switch) AND inside an unscoped cooldown (what the background catalog refresh
// leaves behind). Ordinary traffic finds nothing to try; a probe must not.
func closeTheGates(t *testing.T, g *Gateway) *anonymousNode {
	t.Helper()
	if g.anonymous.Len() != 1 {
		t.Fatalf("fixture wants exactly one anonymous node, got %d", g.anonymous.Len())
	}
	node := g.anonymous.nodes[0]
	node.proxy.healthy.Store(false)
	// cause "" marks it as background work, fingerprint "" blocks every request.
	g.anonymous.MarkFailure(node, nil, errors.New("catalog refresh failed"), "")
	if node.cooldownUntil.Load() <= time.Now().UnixNano() {
		t.Fatal("fixture did not arm a cooldown")
	}
	return node
}

// addUnreachableTransport appends a second anonymous node whose proxy client
// points at a closed loopback port, so any attempt through it fails to dial. It
// is how the rotation test tells the two transports apart without going near the
// public internet.
func addUnreachableTransport(t *testing.T, g *Gateway) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	extra, err := newTransportPool([]string{"http://" + address}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	proxy := extra.items[0]
	proxy.healthy.Store(true)
	g.transports.items = append(g.transports.items, proxy)
	g.anonymous.nodes = append(g.anonymous.nodes, &anonymousNode{proxy: proxy})
	if g.anonymous.Len() != 2 {
		t.Fatalf("fixture wants two anonymous nodes, got %d", g.anonymous.Len())
	}
}

// TestWatchdogProbeReachesUpstreamThroughClosedGates is the whole point of the
// endpoint: with every gate that would short-circuit a normal request closed,
// the probe still transmits exactly one request and reports what the upstream
// said. Reverting NextProbe to NextForRequest here drops hits to 0.
func TestWatchdogProbeReachesUpstreamThroughClosedGates(t *testing.T) {
	url, hits := stubUpstream(t, http.StatusOK, `{"id":"x","choices":[]}`)
	g := probeGateway(t, url)
	closeTheGates(t, g)

	rec, out := sendProbe(t, g, `{"model":"probe-free"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("probe must answer 200 once it has a verdict, got %d: %s", rec.Code, rec.Body.String())
	}
	if !out.Attempted || out.Verdict != "ok" || out.Status != http.StatusOK {
		t.Fatalf("probe must report a served request, got %+v", out)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("probe must reach the upstream exactly once, hits=%d — the forced path is not wired", got)
	}
}

// TestWatchdogProbeRotatesAcrossTransports pins that the probe does not pin
// one transport per model. A deterministic per-model cursor would keep probing
// the same (possibly broken) proxy forever and hide it; reverting the affinity
// argument turns this test green instead of red.
func TestWatchdogProbeRotatesAcrossTransports(t *testing.T) {
	url, hits := stubUpstream(t, http.StatusOK, `{"id":"x","choices":[]}`)
	g := probeGateway(t, url)
	addUnreachableTransport(t, g)

	if _, first := sendProbe(t, g, `{"model":"probe-free"}`); first.Verdict != "ok" {
		t.Fatalf("first probe must land on the reachable transport, got %+v", first)
	}
	_, second := sendProbe(t, g, `{"model":"probe-free"}`)
	if second.Verdict != "transport" {
		t.Fatalf("second probe must reach the other transport and report transport, got %+v — the cursor is pinned", second)
	}
	if hits.Load() != 1 {
		t.Fatalf("only the reachable transport may reach the upstream, hits=%d", hits.Load())
	}
}

// TestWatchdogProbeWritesNoPoolState pins the isolation contract: a probe is a
// measurement, and the node it used must look untouched afterwards. Dropping the
// diagnostic isolation or letting the probe call observeAnonymousResult /
// syncProxyResult re-arms the cooldown, stamps the watchdog's failure age and
// restores the proxy.
func TestWatchdogProbeWritesNoPoolState(t *testing.T) {
	url, hits := stubUpstream(t, http.StatusTooManyRequests,
		`{"error":{"type":"FreeUsageLimitError","message":"Rate limit exceeded"}}`)
	g := probeGateway(t, url)
	node := closeTheGates(t, g)
	before := node.cooldownUntil.Load()
	beforeFailures := node.failures.Load()

	rec, out := sendProbe(t, g, `{"model":"probe-free"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("probe must answer 200 with its verdict, got %d", rec.Code)
	}
	if out.Verdict != "quota" || out.Status != http.StatusTooManyRequests {
		t.Fatalf("a burned allowance must read as quota, got %+v", out)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("probe must reach the upstream exactly once, hits=%d", got)
	}
	if strings.Contains(rec.Body.String(), "Rate limit exceeded") {
		t.Fatalf("the upstream error document must not be echoed to the watchdog: %s", rec.Body.String())
	}
	if got := node.cooldownUntil.Load(); got != before {
		t.Fatalf("probe must not re-arm or extend the cooldown: before=%d after=%d", before, got)
	}
	if got := node.failures.Load(); got != beforeFailures {
		t.Fatalf("probe must not advance the failure ladder: before=%d after=%d", beforeFailures, got)
	}
	if cause, _ := node.coolCause.Load().(string); cause != "" {
		t.Fatalf("a background cooldown carries no cause; probe must not add one, got %q", cause)
	}
	if scope, _ := node.coolHash.Load().(string); scope != "" {
		t.Fatalf("probe must not re-scope the cooldown it found, got fingerprint %q", scope)
	}
	if node.proxy.healthy.Load() {
		t.Fatal("probe must not restore transport health — only a real round trip through the proxy may")
	}
	if _, _, successOK, failureOK := g.anonymous.successFailureAges(); successOK || failureOK {
		t.Fatalf("probe must stamp no success/failure age (success=%v failure=%v)", successOK, failureOK)
	}
	// The usage layer is fed only when a request carries a tier, so a probe
	// stays out of the usage page while still counting as a monitor request.
	snapshot := g.monitor.Snapshot()
	if len(snapshot.Upstream.Requests) != 0 {
		t.Fatalf("probe must not enter the usage detail ring, got %d entries", len(snapshot.Upstream.Requests))
	}
	if snapshot.Usage.Lifetime.Requests != 0 {
		t.Fatalf("probe must not be counted as usage, got %d requests", snapshot.Usage.Lifetime.Requests)
	}
	// The endpoint answers 200 even for a failed upstream, so the monitor's
	// success/error split has to come from the verdict.
	if snapshot.Lifetime.Total != 1 || snapshot.Lifetime.Errors != 1 || snapshot.Lifetime.Success != 0 {
		t.Fatalf("a failed probe must count as one errored request, got %+v", snapshot.Lifetime)
	}
}

// TestWatchdogProbeSucceededCountsAsSuccess is the other half of the outcome
// bookkeeping: an ok verdict must not be recorded as an error just because the
// verdict travels in the body.
func TestWatchdogProbeSucceededCountsAsSuccess(t *testing.T) {
	url, _ := stubUpstream(t, http.StatusOK, `{"id":"x","choices":[]}`)
	g := probeGateway(t, url)
	if _, out := sendProbe(t, g, `{"model":"probe-free"}`); out.Verdict != "ok" {
		t.Fatalf("expected an ok verdict, got %+v", out)
	}
	snapshot := g.monitor.Snapshot()
	if snapshot.Lifetime.Total != 1 || snapshot.Lifetime.Success != 1 || snapshot.Lifetime.Errors != 0 {
		t.Fatalf("a served probe must count as one successful request, got %+v", snapshot.Lifetime)
	}
}

// TestWatchdogProbeVerdictVocabulary pins that the probe reuses the status
// feed's own classification. The fallback cases (a body with no error class)
// deliberately mirror watchdog-clash.ps1's table for the same statuses, so
// adopting this endpoint cannot change a decision the script already made.
func TestWatchdogProbeVerdictVocabulary(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		verdict string
	}{
		{"quota class", http.StatusTooManyRequests, `{"error":{"type":"FreeUsageLimitError"}}`, "quota"},
		{"black quota class", http.StatusTooManyRequests, `{"error":{"type":"BlackUsageLimitError"}}`, "quota"},
		{"plain rate limit class", http.StatusTooManyRequests, `{"error":{"type":"RateLimitError"}}`, "ratelimit"},
		// The free tier's gate rejection is quota since 2026-10-17: the probe
		// must report it as "switch and keep switching", not as a fingerprint
		// no node change can clear. The classless-403 row further down still
		// covers the WAF-challenge reading.
		{"free tier gate", http.StatusForbidden, `{"error":{"type":"FreeTierError"}}`, "quota"},
		{"server wobble outranks class", http.StatusServiceUnavailable, `{"error":{"type":"FreeUsageLimitError"}}`, "transport"},
		{"classless 5xx", http.StatusBadGateway, `upstream exploded`, "transport"},
		{"classless 429 waits", http.StatusTooManyRequests, `slow down`, "ratelimit"},
		{"classless 400 is ours", http.StatusBadRequest, `{"message":"bad request shape"}`, "config"},
		{"classless 404", http.StatusNotFound, `not here`, "config"},
		// The two rows below are where the probe deliberately disagrees with the
		// legacy script (which read the status code first): a class wins, and a
		// burned per-IP allowance is something only a new exit IP can fix — so the
		// round keeps switching instead of stopping. See probeFallbackVerdict.
		{"403 upstream refusal is not our config", http.StatusForbidden, `<html>Attention Required</html>`, "fingerprint"},
		{"400 with a quota class still reads as quota", http.StatusBadRequest, `{"error":{"type":"FreeUsageLimitError"}}`, "quota"},
		{"403 with a quota class still reads as quota", http.StatusForbidden, `{"error":{"type":"FreeUsageLimitError"}}`, "quota"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url, hits := stubUpstream(t, tc.status, tc.body)
			g := probeGateway(t, url)
			rec, out := sendProbe(t, g, `{"model":"probe-free"}`)
			if rec.Code != http.StatusOK {
				t.Fatalf("probe must answer 200 with its verdict, got %d: %s", rec.Code, rec.Body.String())
			}
			if !out.Attempted {
				t.Fatalf("probe must have been transmitted, got %+v", out)
			}
			if out.Status != tc.status {
				t.Fatalf("status = %d, want %d (%+v)", out.Status, tc.status, out)
			}
			if out.Verdict != tc.verdict {
				t.Fatalf("verdict = %q, want %q (cause=%q)", out.Verdict, tc.verdict, out.Cause)
			}
			if hits.Load() != 1 {
				t.Fatalf("probe must reach the upstream exactly once, hits=%d", hits.Load())
			}
		})
	}
}

// TestWatchdogProbeReportsTransportFailure covers the case that used to arrive
// as an unattributable 502: the dial itself fails. Pointing the lane at a closed
// loopback port keeps it offline while producing a real connection error.
func TestWatchdogProbeReportsTransportFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close()

	g := probeGateway(t, url)
	node := g.anonymous.nodes[0]
	node.proxy.healthy.Store(true)
	rec, out := sendProbe(t, g, `{"model":"probe-free"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("probe must answer 200 with its verdict, got %d: %s", rec.Code, rec.Body.String())
	}
	if !out.Attempted || out.Status != 0 || out.Verdict != "transport" {
		t.Fatalf("a failed dial must read as a transport verdict, got %+v", out)
	}
	if out.Transport != "connection failed" {
		t.Fatalf("transport reason = %q, want %q", out.Transport, "connection failed")
	}
	if node.failures.Load() != 0 {
		t.Fatal("a probe must not advance the anonymous failure ladder")
	}
	if node.proxy.healthy.Load() != true {
		t.Fatal("a probe must not evict the proxy it just failed to reach")
	}
}

// TestOrdinaryTrafficStillObeysCooldown is the guard against the obvious
// overreach: the forced path must stay on the probe. If the bypass were widened
// to normal requests, an upstream 429 would stop backing anything off.
func TestOrdinaryTrafficStillObeysCooldown(t *testing.T) {
	url, hits := stubUpstream(t, http.StatusTooManyRequests,
		`{"error":{"type":"FreeUsageLimitError","message":"Rate limit exceeded"}}`)
	g := probeGateway(t, url)
	manager, err := rotation.New("", config.RotationConfig{})
	if err != nil {
		t.Fatal(err)
	}
	g.rotation = manager
	g.anonymous.cooldown = 30 * time.Second
	node := g.anonymous.nodes[0]
	node.proxy.healthy.Store(true)
	// Unscoped cooldown: what the background catalog refresh leaves behind, and
	// the one state that blocks every fingerprint.
	g.anonymous.MarkFailure(node, nil, errors.New("catalog refresh failed"), "")

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"probe-free","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer "+probeTestKey)
	rec := httptest.NewRecorder()
	telemetry.Middleware(g.monitor, g.logger, g.Handler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("a cooling lane must answer 429, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("the cooldown answer must carry Retry-After")
	}
	if !strings.Contains(rec.Body.String(), "opencode2api:") {
		t.Fatalf("the cooldown answer must keep the gateway prefix, got %s", rec.Body.String())
	}
	if hits.Load() != 0 {
		t.Fatalf("blocked traffic must not reach the upstream, hits=%d", hits.Load())
	}
}

// TestWatchdogProbeConfigAnswers covers the gateway-side states, which no node
// switch can change: they must be reported without spending an attempt.
func TestWatchdogProbeConfigAnswers(t *testing.T) {
	url, hits := stubUpstream(t, http.StatusOK, `{"id":"x","choices":[]}`)
	t.Run("anonymous disabled", func(t *testing.T) {
		g := probeGateway(t, url)
		g.cfg.Anonymous = false
		_, out := sendProbe(t, g, `{"model":"probe-free"}`)
		if out.Attempted || out.Verdict != "config" {
			t.Fatalf("a disabled lane must answer config without attempting, got %+v", out)
		}
	})
	t.Run("model outside the anonymous lane", func(t *testing.T) {
		g := probeGateway(t, url)
		_, out := sendProbe(t, g, `{"model":"no-such-model"}`)
		if out.Attempted || out.Verdict != "config" {
			t.Fatalf("an unroutable model must answer config without attempting, got %+v", out)
		}
	})
	t.Run("missing model is a client error", func(t *testing.T) {
		g := probeGateway(t, url)
		rec, _ := sendProbe(t, g, `{}`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("a probe without a model must be rejected, got %d", rec.Code)
		}
	})
	t.Run("unparsable body is a client error", func(t *testing.T) {
		g := probeGateway(t, url)
		rec, _ := sendProbe(t, g, `not json`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("a body that is not a JSON object must be rejected, got %d", rec.Code)
		}
	})
	t.Run("server key required", func(t *testing.T) {
		g := probeGateway(t, url)
		req := httptest.NewRequest(http.MethodPost, "/v1/watchdog/probe",
			strings.NewReader(`{"model":"probe-free"}`))
		rec := httptest.NewRecorder()
		g.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("the probe must require a server key, got %d", rec.Code)
		}
	})
	if hits.Load() != 0 {
		t.Fatalf("no config answer may reach the upstream, hits=%d", hits.Load())
	}
}

// TestNextProbeIgnoresGatesAndVisitsEachNodeOnce is the pool-level contract:
// every node is offered exactly once, whatever state it is in, and it is a
// different answer from Next (which is what keeps the bypass out of ordinary
// traffic).
func TestNextProbeIgnoresGatesAndVisitsEachNodeOnce(t *testing.T) {
	// Two loopback-free transports; nothing here is ever dialled.
	transports, err := newTransportPool([]string{"direct", "direct"}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool := newAnonymousPool(true, transports, time.Second)
	if len(pool.nodes) != 2 {
		t.Fatalf("fixture wants two anonymous nodes, got %d", len(pool.nodes))
	}
	for _, node := range pool.nodes {
		node.proxy.healthy.Store(false)
		pool.MarkFailure(node, nil, errors.New("catalog refresh failed"), "")
	}
	ordinary := pool.CursorFor("")
	if node := ordinary.NextForRequest("turn"); node != nil {
		t.Fatal("fixture must leave ordinary traffic with nothing to try")
	}

	cursor := pool.CursorFor("")
	seen := map[*anonymousNode]bool{}
	for i := 0; i < 2; i++ {
		node := cursor.NextProbe()
		if node == nil {
			t.Fatalf("probe stopped after %d nodes while both were eligible", i)
		}
		if seen[node] {
			t.Fatal("probe offered the same node twice in one cursor")
		}
		seen[node] = true
	}
	if extra := cursor.NextProbe(); extra != nil {
		t.Fatal("probe must stop once every node was offered")
	}
	// A separate cursor starts over, so a probe always has a node to use.
	fresh := pool.CursorFor("")
	if node := fresh.NextProbe(); node == nil {
		t.Fatal("a fresh probe cursor must offer a node")
	}
}
