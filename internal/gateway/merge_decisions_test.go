package gateway

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
	"opencode2api/internal/telemetry"
)

// The cases in this file pin merge decisions D1–D3. Each one is written so that
// reverting its decision turns it red; a test that passes either way says
// nothing about whether the merge landed correctly.

// ---------------------------------------------------------------------------
// D1 — the anonymous lane order is configuration, and defaults to /inference
// ---------------------------------------------------------------------------

// TestAnonymousLaneOrderDefaultsToInferenceOnly pins the default.
//
// The tempting default, ["inference","legacy"], is NOT "inference only". That
// extra fallback fires exactly when quota is exhausted: /inference answers 429,
// the gateway falls through to /zen/v1, that lane rejects with the 403 gate
// text, and watchdogCause reads the text as FreeTierError — so
// classifyAnonymousVerdict says quota, recommend_switch flips, and the failover
// script starts changing exit nodes for a problem no node switch can fix.
func TestAnonymousLaneOrderDefaultsToInferenceOnly(t *testing.T) {
	g := newStubGateway(t, "https://example.invalid/inference")
	got := g.anonymousUpstreams()
	if len(got) != 1 || got[0] != "https://example.invalid/inference" {
		t.Fatalf("default lane order = %v, want just the inference base — an unmeasured legacy fallback must stay opt-in", got)
	}
}

// TestAnonymousLaneOrderHonoursExplicitOptIn proves the legacy lane is wired,
// not merely switched off: naming it in the config is enough to get it.
func TestAnonymousLaneOrderHonoursExplicitOptIn(t *testing.T) {
	g := newStubGateway(t, "https://example.invalid/inference")
	g.cfg.Upstream.AnonymousLaneOrder = []string{config.LaneInference, config.LaneLegacy}
	got := g.anonymousUpstreams()
	if len(got) != 2 {
		t.Fatalf("opt-in must yield both lanes, got %v", got)
	}
	if got[0] != "https://example.invalid/inference" || !strings.HasSuffix(got[1], "/zen") {
		t.Fatalf("lane order = %v, want inference first and the legacy base second", got)
	}
}

// TestAnonymousLaneOrderReverses pins that the order is honoured in the
// direction the operator asked for, not merely appended to.
func TestAnonymousLaneOrderReverses(t *testing.T) {
	g := newStubGateway(t, "https://example.invalid/inference")
	g.cfg.Upstream.AnonymousLaneOrder = []string{config.LaneLegacy, config.LaneInference}
	got := g.anonymousUpstreams()
	if len(got) != 2 || !strings.HasSuffix(got[0], "/zen") || got[1] != "https://example.invalid/inference" {
		t.Fatalf("lane order = %v, want legacy first", got)
	}
}

// TestAnonymousLaneOrderDropsDuplicateBase: when the operator points
// upstream.anonymous at the legacy base itself, the two lanes name one origin
// and must collapse to a single attempt instead of dialing it twice.
func TestAnonymousLaneOrderDropsDuplicateBase(t *testing.T) {
	g := newStubGateway(t, models.LegacyZenBase)
	g.cfg.Upstream.AnonymousLaneOrder = []string{config.LaneInference, config.LaneLegacy}
	got := g.anonymousUpstreams()
	if len(got) != 1 || !strings.HasSuffix(got[0], "/zen") {
		t.Fatalf("lane list = %v, want exactly one entry for the shared base", got)
	}
}

// ---------------------------------------------------------------------------
// D2 — the last-resort pass covers transport-unhealthy nodes only
// ---------------------------------------------------------------------------

// mergeDecisionPool builds a one-node anonymous pool and returns it with the
// node. Nothing is dialled: every D2 case is decided by the skip logic.
func mergeDecisionPool(t *testing.T) (*anonymousPool, *anonymousNode) {
	t.Helper()
	transports, err := newTransportPool([]string{"direct"}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool := newAnonymousPool(true, transports, 10*time.Second)
	if pool.Len() != 1 {
		t.Fatalf("fixture wants exactly one node, got %d", pool.Len())
	}
	return pool, pool.nodes[0]
}

// TestNextLastResortSkipsCoolingNodes is the backoff guard. Next() may reach
// past its main scan, but only for a node whose TRANSPORT is broken. A node
// that is healthy and merely cooling is the exit that just answered 429;
// handing it to the next caller is what collapses an exponential ladder into a
// hot retry loop and burns the free quota it was meant to pace.
func TestNextLastResortSkipsCoolingNodes(t *testing.T) {
	pool, node := mergeDecisionPool(t)
	node.proxy.healthy.Store(true)
	pool.startCooldown(node, nil, errors.New("upstream HTTP 429"), "FreeUsageLimitError (upstream HTTP 429)", "turn-a")

	cursor := pool.CursorFor("")
	if got := cursor.Next(); got != nil {
		t.Fatalf("Next returned %v; a cooling node must stay unreachable through the last-resort pass", got)
	}
}

// TestNextLastResortReachesUnhealthyNodes is the other half: the pass exists so
// a fully broken route does not answer "nothing available" forever. Deleting it
// — one of the two ways this conflict was resolved — turns this red.
func TestNextLastResortReachesUnhealthyNodes(t *testing.T) {
	pool, node := mergeDecisionPool(t)
	node.proxy.healthy.Store(false)

	cursor := pool.CursorFor("")
	if got := cursor.Next(); got != node {
		t.Fatalf("Next returned %v, want the unhealthy node %v; the last-resort pass must still exist", got, node)
	}
}

// TestNextForRequestNeverLastResorts keeps the two cursors apart. Ordinary
// traffic must not inherit Next's last-resort pass: it skips unhealthy nodes
// unconditionally, and doAnonymousUpstream reads that nil to answer 429 (with a
// Retry-After the client can honour) or 503 (with nothing to try) — both better
// answers than a blind retry through a transport known to be down.
func TestNextForRequestNeverLastResorts(t *testing.T) {
	pool, _ := mergeDecisionPool(t)
	pool.nodes[0].proxy.healthy.Store(false)

	if got := nextFor(pool, "turn-a"); got != nil {
		t.Fatalf("NextForRequest returned %v; ordinary traffic must never be handed an unhealthy node", got)
	}
}

// ---------------------------------------------------------------------------
// D3 — "all proxies unavailable" answers 503 proxy_error
// ---------------------------------------------------------------------------

// TestProxyUnavailableAnswers503WithScriptReadableBody drives every client
// protocol with all transports marked unhealthy.
//
// The body has to satisfy the failover script in both directions at once: the
// gateway prefix marks it as ours, "proxy_error" makes it a transport verdict
// (switch nodes), and the absence of "ModelUnavailable" keeps it out of the
// catalog branch (stop the round). A 429 here would be worse than useless —
// the script would read it as backoff and stop, and no node switch would follow.
func TestProxyUnavailableAnswers503WithScriptReadableBody(t *testing.T) {
	cases := []struct {
		protocol wire.Protocol
		model    string
		path     string
		body     string
	}{
		{wire.Chat, "chat-free", "/v1/chat/completions", `{"model":"chat-free","messages":[{"role":"user","content":"hi"}]}`},
		{wire.Responses, "responses-free", "/v1/responses", `{"model":"responses-free","input":"hi"}`},
		{wire.Anthropic, "anthropic-free", "/v1/messages", `{"model":"anthropic-free","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`},
		{wire.SystemOne, "decision-free", "/v1/systemone", `{"model":"decision-free","state":{},"questions":[]}`},
	}
	for _, tc := range cases {
		t.Run(string(tc.protocol), func(t *testing.T) {
			g := mergeDecisionStub(t)
			for _, proxy := range g.transports.items {
				proxy.healthy.Store(false)
			}

			var handler http.HandlerFunc
			if tc.protocol == wire.SystemOne {
				handler = g.handleSystemOne
			} else {
				handler = g.handleInference(tc.protocol)
			}

			rec := httptest.NewRecorder()
			telemetry.Middleware(g.monitor, g.logger, handler).
				ServeHTTP(rec, httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body)))

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("all proxies down must answer 503, got %d: %s", rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Retry-After") != "" {
				t.Fatalf("a transport failure must not promise a retry window, got %q", rec.Header().Get("Retry-After"))
			}
			text := rec.Body.String()
			for _, want := range []string{"proxy_error", "opencode2api:"} {
				if !strings.Contains(text, want) {
					t.Fatalf("503 body must carry %q: %s", want, text)
				}
			}
			if strings.Contains(text, "ModelUnavailable") {
				t.Fatalf("proxy_error must stay clear of ModelUnavailable or the script stops the round: %s", text)
			}
		})
	}
}

// TestCooldownStillWinsOverProxyUnavailable is the ordering guard: a cursor that
// is empty because everything is COOLING must answer 429, never 503. Wrapping
// errAnonymousCooling in errProxyUnavailable would collapse that distinction and
// hand the script a transport verdict for a quota problem.
func TestCooldownStillWinsOverProxyUnavailable(t *testing.T) {
	g := coolingStub(t)
	first := drive(t, g, coolingRequestBody)
	if first.Code != http.StatusTooManyRequests {
		t.Fatalf("fixture: the first request must carry the upstream 429, got %d", first.Code)
	}
	second := drive(t, g, coolingRequestBody)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("a cooled lane must answer 429, got %d: %s", second.Code, second.Body.String())
	}
	if !strings.Contains(second.Body.String(), "opencode2api:") {
		t.Fatalf("the local cooldown must be gateway-authored: %s", second.Body.String())
	}
	if strings.Contains(second.Body.String(), "proxy_error") {
		t.Fatalf("a cooldown was reported as a transport failure: %s", second.Body.String())
	}
}

// mergeDecisionStub wires a gateway whose upstream answers 200 on any path,
// registers one model per client protocol (a model carries a single native
// protocol, so they cannot share a name), and parks proxy health checks so
// nothing dials the public internet.
func mergeDecisionStub(t *testing.T) *Gateway {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","object":"chat.completion","choices":[]}`))
	}))
	t.Cleanup(server.Close)
	g := newStubGateway(t, server.URL)
	// handleInference reads the rotation manager before it routes, so a nil one
	// panics long before the proxies-under-test code is reached.
	manager, err := rotation.New("", config.RotationConfig{})
	if err != nil {
		t.Fatal(err)
	}
	g.rotation = manager
	registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
		config.TierZen: {
			"chat-free":      wire.Chat,
			"responses-free": wire.Responses,
			"anthropic-free": wire.Anthropic,
			"decision-free":  wire.SystemOne,
		},
	})
	freezeProxyHealth(g)
	return g
}
