package gateway

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
	"opencode2api/internal/telemetry"
)

// nextFor takes an addressable cursor: NextForRequest has a pointer receiver,
// so it cannot be called straight off the CursorFor result.
func nextFor(pool *anonymousNodes, fingerprint string) *anonymousNode {
	cursor := pool.CursorFor("")
	return cursor.NextForRequest(fingerprint)
}

// scopePool builds a one-node anonymous pool whose node is already inside a
// cooldown triggered by scopeFingerprint, and returns the pool plus the node.
// The transport is never dialled: every case here is decided by the skip logic.
func scopePool(t *testing.T, scopeFingerprint, cause string) (*anonymousNodes, *anonymousNode) {
	t.Helper()
	transports, err := transportPool([]string{"direct"}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool := anonymousPool(true, transports, 10*time.Second)
	if pool.Len() != 1 {
		t.Fatalf("fixture wants exactly one node, got %d", pool.Len())
	}
	node := pool.nodes[0]
	node.proxy.healthy.Store(true)
	pool.startCooldown(node, nil, errors.New("upstream HTTP 429"), cause, scopeFingerprint)
	return pool, node
}

// TestAnonymousCoolingIsScopedToTheTriggeringRequest is the whole point of the
// fingerprint scope: a turn that was rate-limited must not freeze unrelated
// traffic on the same proxy. Before the scope existed, one quota event blocked
// every turn for the whole window.
func TestAnonymousCoolingIsScopedToTheTriggeringRequest(t *testing.T) {
	const turnA, turnB = "fingerprint-a", "fingerprint-b"
	pool, node := scopePool(t, turnA, "FreeUsageLimitError (upstream HTTP 429)")

	if got := nextFor(pool, turnA); got != nil {
		t.Fatal("a retry of the same turn must stay blocked while its cooldown runs")
	}
	if got := nextFor(pool, turnB); got != node {
		t.Fatalf("a different turn must still get one real attempt, got %v", got)
	}
	// coolingFor mirrors that skip logic exactly, so it is what the 429 path
	// consults; it must agree with the cursor for both turns.
	if blocked, remain := pool.coolingFor(turnA); !blocked || remain <= 0 {
		t.Fatalf("coolingFor must report the blocking cooldown for the same turn, got blocked=%v remain=%v", blocked, remain)
	}
	if blocked, _ := pool.coolingFor(turnB); blocked {
		t.Fatal("coolingFor must not report a cooldown for an unrelated turn")
	}
	if cause := pool.coolCauseFor(turnB); cause != "" {
		t.Fatalf("cause must belong to the block the caller hit, got %q for an unrelated turn", cause)
	}
	if cause := pool.coolCauseFor(turnA); cause != "FreeUsageLimitError (upstream HTTP 429)" {
		t.Fatalf("cause for the blocking turn = %q", cause)
	}
	// The unscoped snapshot the watchdog uses is untouched: it still sees the
	// longest cooldown regardless of which turn triggered it.
	if remain, _ := pool.maxCooldownSnapshot(); remain <= 0 {
		t.Fatal("maxCooldownSnapshot must still report the cooldown to the watchdog feed")
	}
}

// TestAnonymousCoolingWithoutFingerprintBlocksEverything covers the other half:
// background refresh (and any caller with no payload to fingerprint) stores "",
// which must keep the original block-everything behavior rather than becoming
// a free pass.
func TestAnonymousCoolingWithoutFingerprintBlocksEverything(t *testing.T) {
	pool, _ := scopePool(t, "", "FreeUsageLimitError (upstream HTTP 429)")
	for _, fingerprint := range []string{"", "any-turn-at-all"} {
		if got := nextFor(pool, fingerprint); got != nil {
			t.Fatalf("an unscoped cooldown must block fingerprint %q too, got %v", fingerprint, got)
		}
		if blocked, _ := pool.coolingFor(fingerprint); !blocked {
			t.Fatalf("coolingFor must report blocked for fingerprint %q", fingerprint)
		}
	}
}

// TestAnonymousCoolingScopeIgnoresTransportHealth pins that the fingerprint
// bypass never overrides transport state: an unhealthy proxy is skipped for
// every turn, because no content difference makes a dead socket usable.
func TestAnonymousCoolingScopeIgnoresTransportHealth(t *testing.T) {
	pool, node := scopePool(t, "turn-a", "transport (timeout)")
	node.proxy.healthy.Store(false)
	if got := nextFor(pool, "turn-b"); got != nil {
		t.Fatalf("an unhealthy proxy must be skipped regardless of fingerprint, got %v", got)
	}
	if blocked, _ := pool.coolingFor("turn-b"); blocked {
		t.Fatal("coolingFor must not count an unhealthy proxy as a cooldown")
	}
}

// TestFingerprintScopeReachesTheWiring is the end-to-end guard for the one line
// that makes the whole feature work: doAnonymousUpstream's
// `cursor.NextForRequest(fingerprint)`. The pool-level cases above call the
// method directly, so reverting that single call to `cursor.Next()` left the
// whole suite green — which is exactly how the scope would silently disappear
// on the next upstream sync. Here the two requests really go through
// handleInference → doUpstream → doAnonymousUpstream.
func TestFingerprintScopeReachesTheWiring(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		// Only turn-a trips the free-tier quota; turn-b is served normally.
		if strings.Contains(string(body), `"turn-a"`) {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"type":"FreeUsageLimitError","message":"Rate limit exceeded"}}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"x","object":"chat.completion","choices":[]}`))
	}))
	t.Cleanup(server.Close)
	g := newStubGateway(t, server.URL)
	registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
		config.TierZen: {"probe-free": wire.Chat},
	})
	manager, err := rotation.New("", config.RotationConfig{})
	if err != nil {
		t.Fatal(err)
	}
	g.rotation = manager
	// Long enough that the third request still lands inside the window.
	g.anonymous.cooldown = 30 * time.Second
	// Keeps it offline: a 429 schedules a detached verifyProxyAfterError.
	freezeProxyHealth(g)

	send := func(turn string) *httptest.ResponseRecorder {
		body := `{"model":"probe-free","messages":[{"role":"user","content":"` + turn + `"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		rec := httptest.NewRecorder()
		telemetry.Middleware(g.monitor, g.logger, g.handleInference(wire.Chat)).ServeHTTP(rec, req)
		return rec
	}

	// 1. turn-a hits the free-tier quota and puts the node into cooldown. Its
	// 429 is the upstream's own passthrough (no gateway prefix), which is how
	// it is told apart from the cooldown 429 asserted in step 3.
	first := send("turn-a")
	if strings.Contains(first.Body.String(), "opencode2api:") {
		t.Fatalf("the first turn-a must be a passthrough, not a local cooldown: %s", first.Body.String())
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("expected 1 upstream call, got %d", got)
	}

	// 2. The SAME turn replayed is blocked locally: no upstream call, and the
	// answer is a cooldown 429 + Retry-After rather than a bare 502.
	rec := send("turn-a")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("a same-turn retry must be answered as cooldown 429, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("the cooldown answer must carry Retry-After so the client backs off exactly")
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("a blocked same-turn retry must not reach upstream, hits=%d", got)
	}

	// 3. A DIFFERENT turn still gets one real upstream attempt while the node is
	// cooling. With `cursor.Next()` here it would answer the cooling 429 without
	// ever dialling the stub — the regression this guards. Must run after step 2:
	// a real success calls MarkSuccess, which clears the cooldown by design.
	if code := send("turn-b").Code; code != http.StatusOK {
		t.Fatalf("a different turn must be served, got %d", code)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("turn-b must reach upstream (hits=2), got %d — the fingerprint scope is not wired", got)
	}
}
