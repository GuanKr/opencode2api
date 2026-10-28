package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	modelcatalog "opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
	"opencode2api/internal/telemetry"
)

// The free tier scopes part of its allowance to the session id: once a
// conversation exhausts it, the same id is refused on every egress, and moving
// to a fresh proxy with the old id spends that proxy's allowance too. These
// cases pin the rotation contract: a quota 429 rotates, everything else does
// not, and the rotated generation survives into the next turn.

// sessionCapture records the upstream session header of every attempt.
type sessionCapture struct {
	mu   sync.Mutex
	seen []string
}

func (c *sessionCapture) record(r *http.Request) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = append(c.seen, r.Header.Get("x-opencode-session"))
	return len(c.seen)
}

func (c *sessionCapture) sessions() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.seen...)
}

// newSessionRotationGateway points both anonymous lanes at one stub per lane,
// so a single client request can make two attempts with the one direct
// transport this fixture has. reply receives the 1-based attempt number.
func newSessionRotationGateway(t *testing.T, reply func(attempt int, w http.ResponseWriter)) (*Gateway, *sessionCapture) {
	t.Helper()
	capture := &sessionCapture{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := capture.record(r)
		reply(attempt, w)
	})
	primary := httptest.NewServer(handler)
	t.Cleanup(primary.Close)
	legacy := httptest.NewServer(handler)
	t.Cleanup(legacy.Close)
	previous := modelcatalog.LegacyZenBase
	modelcatalog.LegacyZenBase = legacy.URL
	t.Cleanup(func() { modelcatalog.LegacyZenBase = previous })

	g := newStubGateway(t, primary.URL)
	manager, err := rotation.New("", config.RotationConfig{})
	if err != nil {
		t.Fatal(err)
	}
	g.rotation = manager
	g.cfg.Upstream.AnonymousLaneOrder = []string{config.LaneInference, config.LaneLegacy}
	g.cfg.Retry.MaxAttempts = 3
	registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
		config.TierZen: {"probe-free": wire.Chat},
	})
	freezeProxyHealth(g)
	return g, capture
}

func quota429(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = io.WriteString(w, `{"error":{"type":"FreeUsageLimitError","message":"Free usage limit reached","code":null}}`)
}

func rateLimit429(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = io.WriteString(w, `{"error":{"type":"RateLimitError","message":"Rate limit exceeded","code":null}}`)
}

func gate403(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	_, _ = io.WriteString(w, `{"error":{"type":"FreeTierError","message":"OpenCode's free tier can only be used from within OpenCode.","code":null}}`)
}

func ok200(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","choices":[]}`)
}

func chatRequest(t *testing.T, g *Gateway, content string) *httptest.ResponseRecorder {
	t.Helper()
	body := `{"model":"probe-free","messages":[{"role":"user","content":"` + content + `"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	telemetry.Middleware(g.monitor, g.logger, g.handleInference(wire.Chat)).ServeHTTP(rec, req)
	return rec
}

// TestQuota429RotatesTheUpstreamSession: the retry after a quota refusal must
// leave under a different session, or the next egress burns for the same reason.
func TestQuota429RotatesTheUpstreamSession(t *testing.T) {
	g, capture := newSessionRotationGateway(t, func(attempt int, w http.ResponseWriter) {
		if attempt == 1 {
			quota429(w)
			return
		}
		ok200(w)
	})
	rec := chatRequest(t, g, "one")
	if rec.Code != http.StatusOK {
		t.Fatalf("the retry must serve the request, got %d: %s", rec.Code, rec.Body.String())
	}
	seen := capture.sessions()
	if len(seen) != 2 {
		t.Fatalf("want two attempts on the two lanes, got %d: %v", len(seen), seen)
	}
	if seen[0] == seen[1] {
		t.Fatalf("the retried attempt must rotate the session, both were %q", seen[0])
	}
	for _, id := range seen {
		if !strings.HasPrefix(id, "ses_") {
			t.Fatalf("the upstream session must keep the ses_ shape, got %q", id)
		}
	}
}

// TestGate403DoesNotRotateTheSession: a 403 is the gate's body-shape verdict (or
// an IP-level rejection), not a quota proof. Rotating there would hide a
// shaping bug behind a quota story.
func TestGate403DoesNotRotateTheSession(t *testing.T) {
	g, capture := newSessionRotationGateway(t, func(_ int, w http.ResponseWriter) {
		gate403(w)
	})
	rec := chatRequest(t, g, "one")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("a 403 must reach the client unchanged, got %d: %s", rec.Code, rec.Body.String())
	}
	seen := capture.sessions()
	if len(seen) < 2 {
		t.Fatalf("want both lanes attempted, got %d: %v", len(seen), seen)
	}
	for index := 1; index < len(seen); index++ {
		if seen[index] != seen[0] {
			t.Fatalf("a 403 must not rotate the session: %v", seen)
		}
	}
}

// TestRateLimit429DoesNotRotateTheSession: throttling recovers on its own; a
// new session would only spend the conversation's rotations for nothing.
func TestRateLimit429DoesNotRotateTheSession(t *testing.T) {
	g, capture := newSessionRotationGateway(t, func(_ int, w http.ResponseWriter) {
		rateLimit429(w)
	})
	rec := chatRequest(t, g, "one")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("the last 429 must reach the client, got %d: %s", rec.Code, rec.Body.String())
	}
	seen := capture.sessions()
	if len(seen) < 2 {
		t.Fatalf("want both lanes attempted, got %d: %v", len(seen), seen)
	}
	for index := 1; index < len(seen); index++ {
		if seen[index] != seen[0] {
			t.Fatalf("a rate-limit 429 must not rotate the session: %v", seen)
		}
	}
}

// TestRotatedGenerationSurvivesTheNextTurn: the rotation is remembered per
// conversation, so the turn after the refusal does not fall back into the
// exhausted session. The explicit client session header keeps the conversation
// key stable while the payload (and therefore the cooling fingerprint) changes.
func TestRotatedGenerationSurvivesTheNextTurn(t *testing.T) {
	g, capture := newSessionRotationGateway(t, func(attempt int, w http.ResponseWriter) {
		if attempt == 1 {
			quota429(w)
			return
		}
		ok200(w)
	})
	for _, content := range []string{"one", "two"} {
		body := `{"model":"probe-free","messages":[{"role":"user","content":"` + content + `"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("x-opencode-session", "rotation-test-conversation")
		rec := httptest.NewRecorder()
		telemetry.Middleware(g.monitor, g.logger, g.handleInference(wire.Chat)).ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("turn %q must succeed, got %d: %s", content, rec.Code, rec.Body.String())
		}
	}
	seen := capture.sessions()
	if len(seen) != 3 {
		t.Fatalf("want two attempts on turn one and one on turn two, got %d: %v", len(seen), seen)
	}
	if seen[0] == seen[1] {
		t.Fatalf("the first turn must rotate, both were %q", seen[0])
	}
	if seen[2] != seen[1] {
		t.Fatalf("the next turn must stay on generation 1: %v", seen)
	}
}

// TestShouldRotateSessionOnlyOnQuota pins the trigger itself, independent of
// the loops: only a 429 whose class reads as quota rotates. A bare 429 is
// throttling (the watchdog's own rule), and 403 belongs to the gate.
func TestShouldRotateSessionOnlyOnQuota(t *testing.T) {
	cases := map[string]struct {
		status int
		body   string
		want   bool
	}{
		"free usage limit":  {http.StatusTooManyRequests, `{"error":{"type":"FreeUsageLimitError","message":"Free usage limit reached"}}`, true},
		"black usage limit": {http.StatusTooManyRequests, `{"error":{"type":"BlackUsageLimitError","message":"Free usage limit reached"}}`, true},
		"rate limit":        {http.StatusTooManyRequests, `{"error":{"type":"RateLimitError","message":"Rate limit exceeded"}}`, false},
		"bare 429":          {http.StatusTooManyRequests, `{"error":{"message":"Too Many Requests"}}`, false},
		"gate 403":          {http.StatusForbidden, `{"error":{"type":"FreeTierError","message":"OpenCode's free tier can only be used from within OpenCode."}}`, false},
		"server error":      {http.StatusInternalServerError, `{"error":{"type":"InternalServerError"}}`, false},
	}
	for name, testCase := range cases {
		resp := upstreamErrorResponse(testCase.status, testCase.body)
		if got := shouldRotateSession(resp, nil); got != testCase.want {
			t.Fatalf("%s: shouldRotateSession = %v, want %v", name, got, testCase.want)
		}
	}
	if shouldRotateSession(nil, io.EOF) {
		t.Fatal("a transport error must not rotate the session")
	}
	if shouldRotateSession(nil, nil) {
		t.Fatal("no response must not rotate the session")
	}
	if shouldRotateSession(upstreamErrorResponse(http.StatusTooManyRequests, `{"error":{"type":"FreeUsageLimitError","message":"limit"}}`), context.Canceled) {
		t.Fatal("a cancelled request must not rotate the session")
	}
	if shouldRotateSession(upstreamErrorResponse(http.StatusTooManyRequests, `{"error":{"type":"FreeUsageLimitError","message":"limit"}}`), context.DeadlineExceeded) {
		t.Fatal("a timed-out request must not rotate the session")
	}
}

// TestKeyLaneQuotaRotatesTheFreeModelSession: the Zen-key lane is the third
// caller of the session store. A free model's quota 429 must rotate the
// upstream session there too, and the next turn must reuse the new generation.
func TestKeyLaneQuotaRotatesTheFreeModelSession(t *testing.T) {
	capture := &sessionCapture{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if capture.record(r) == 1 {
			quota429(w)
			return
		}
		ok200(w)
	}))
	t.Cleanup(server.Close)

	g := newStubGateway(t, server.URL)
	zen, err := nodePool([]string{"key-1", "key-2"}, g.transports, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	g.zenNodes = zen
	freezeProxyHealth(g)

	ids := identity.RequestIDs{Session: identity.CanonicalSessionID("key-lane-conversation"), Request: "req-key-1"}
	bodies := map[config.Tier][]byte{config.TierZen: []byte(`{"model":"probe-free","stream":false,"messages":[{"role":"user","content":"hi"}]}`)}
	for turn := 0; turn < 2; turn++ {
		resp, err, _ := g.doKeyUpstream(context.Background(), chatDumpRoute(), bodies, ids, 0)
		if err != nil {
			t.Fatalf("turn %d: %v", turn, err)
		}
		if resp == nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("turn %d: want 200, got %+v", turn, resp)
		}
		resp.Body.Close()
	}

	seen := capture.sessions()
	if len(seen) != 3 {
		t.Fatalf("want two attempts in turn one and one in turn two, got %d: %v", len(seen), seen)
	}
	if seen[0] == seen[1] {
		t.Fatalf("the retry after a quota 429 must rotate the session, both %q", seen[0])
	}
	if seen[2] != seen[1] {
		t.Fatalf("the next turn must reuse generation 1: %v", seen)
	}
	for _, id := range seen {
		if !strings.HasPrefix(id, "ses_") {
			t.Fatalf("the upstream session must keep the ses_ shape, got %q", id)
		}
	}
}

// TestKeyLanePaidQuotaDoesNotRotateTheSession: a paid model's 429 is
// account-level. The key lane serves both kinds, so the free-model gate must
// keep the paid case off the rotation store.
func TestKeyLanePaidQuotaDoesNotRotateTheSession(t *testing.T) {
	capture := &sessionCapture{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capture.record(r)
		quota429(w)
	}))
	t.Cleanup(server.Close)

	g := newStubGateway(t, server.URL)
	zen, err := nodePool([]string{"key-1", "key-2"}, g.transports, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	g.zenNodes = zen
	freezeProxyHealth(g)

	route := chatDumpRoute()
	route.ID = "claude-sonnet-4"
	route.Anonymous = false
	ids := identity.RequestIDs{Session: identity.CanonicalSessionID("key-lane-paid"), Request: "req-paid"}
	bodies := map[config.Tier][]byte{config.TierZen: []byte(`{"model":"claude-sonnet-4","stream":false,"messages":[{"role":"user","content":"hi"}]}`)}
	resp, _, _ := g.doKeyUpstream(context.Background(), route, bodies, ids, 0)
	if resp != nil {
		resp.Body.Close()
	}

	seen := capture.sessions()
	if len(seen) < 2 {
		t.Fatalf("want the two keys attempted, got %d: %v", len(seen), seen)
	}
	for index := 1; index < len(seen); index++ {
		if seen[index] != seen[0] {
			t.Fatalf("a paid 429 must not rotate the session: %v", seen)
		}
	}
	if got := g.sessions.Generation(ids.Session); got != 0 {
		t.Fatalf("a paid 429 must leave the generation at 0, got %d", got)
	}
}

// TestShouldRotateSessionForOnlyFreeModels: a paid 429 is account-level, so a
// fresh session cannot clear it and rotating would only cost a cold prompt
// cache. The key lane serves both kinds, hence the model check.
func TestShouldRotateSessionForOnlyFreeModels(t *testing.T) {
	g := newStubGateway(t, "http://127.0.0.1:1")
	quota := func() *http.Response {
		return upstreamErrorResponse(http.StatusTooManyRequests, `{"error":{"type":"InsufficientQuota","message":"insufficient quota"}}`)
	}
	if !g.shouldRotateSessionFor("probe-free", quota(), nil) {
		t.Fatal("a free model's quota 429 must rotate")
	}
	if g.shouldRotateSessionFor("paid-model", quota(), nil) {
		t.Fatal("a paid model's quota 429 must not rotate")
	}
}

// TestShouldRotateSessionKeepsTheBodyIntact: the trigger peeks the error
// document to read its class; the response the caller may forward has to stay
// byte-identical.
func TestShouldRotateSessionKeepsTheBodyIntact(t *testing.T) {
	const payload = `{"error":{"type":"FreeUsageLimitError","message":"Free usage limit reached"}}`
	resp := upstreamErrorResponse(http.StatusTooManyRequests, payload)
	if !shouldRotateSession(resp, nil) {
		t.Fatal("fixture must classify as quota")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the restored body failed: %v", err)
	}
	if string(body) != payload {
		t.Fatalf("the peeking trigger changed the body:\n got %q\nwant %q", body, payload)
	}
}

// TestSessionGenerationsRotateAndExpire covers the store on its own: rotations
// accumulate, an idle conversation expires back to 0, and a nil store is inert
// (so a Gateway built without one cannot panic).
func TestSessionGenerationsRotateAndExpire(t *testing.T) {
	store := newSessionGenerations()
	now := time.Unix(1_700_000_000, 0)
	store.now = func() time.Time { return now }

	if got := store.Generation("conversation"); got != 0 {
		t.Fatalf("a conversation never rotated must start at 0, got %d", got)
	}
	if got := store.Rotate("conversation"); got != 1 {
		t.Fatalf("the first rotation must be 1, got %d", got)
	}
	if got := store.Generation("conversation"); got != 1 {
		t.Fatalf("the rotation must be readable, got %d", got)
	}
	if got := store.Rotate("conversation"); got != 2 {
		t.Fatalf("rotations must accumulate, got %d", got)
	}
	if got := store.Generation("other"); got != 0 {
		t.Fatalf("another conversation must not inherit the rotation, got %d", got)
	}

	now = now.Add(store.ttl + time.Second)
	if got := store.Generation("conversation"); got != 0 {
		t.Fatalf("an idle conversation must expire back to 0, got %d", got)
	}

	var nilStore *sessionGenerations
	if got := nilStore.Generation("conversation"); got != 0 {
		t.Fatalf("a nil store must report generation 0, got %d", got)
	}
	if got := nilStore.Rotate("conversation"); got != 0 {
		t.Fatalf("a nil store cannot rotate, got %d", got)
	}
	if got := store.Rotate(""); got != 0 {
		t.Fatalf("an empty key cannot rotate, got %d", got)
	}
}

// TestSessionGenerationsSweepIsThrottled: the sweep is O(active conversations)
// under the store's single mutex, and Generation runs at least twice per
// request, so it must not walk the map on every call.
func TestSessionGenerationsSweepIsThrottled(t *testing.T) {
	store := newSessionGenerations()
	now := time.Unix(1_700_000_000, 0)
	store.now = func() time.Time { return now }

	store.Generation("first")
	firstSweep := store.lastSweep
	if firstSweep.IsZero() {
		t.Fatal("the first access must sweep")
	}

	now = now.Add(time.Second)
	store.Generation("second")
	if !store.lastSweep.Equal(firstSweep) {
		t.Fatalf("the sweep ran again inside the interval: %v -> %v", firstSweep, store.lastSweep)
	}

	now = now.Add(2 * time.Minute)
	store.Generation("third")
	if !store.lastSweep.Equal(now) {
		t.Fatalf("the sweep did not run after the interval: %v", store.lastSweep)
	}
}
