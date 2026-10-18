package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
)

// Upstream's caller scope exists so two server keys replaying the same client
// session stop sharing one upstream session. Without this case the feature has
// no coverage at all: the other gateway tests build handlers directly and
// bypass authenticate, so callerScope returns "" on every request in the suite
// and the argument could be deleted from all four call sites with the suite
// still green.
//
// The assertions go through g.Handler() — the full mux — precisely because
// that is the only way authenticate runs and puts the caller into the context.

// callerScopeStub drives real requests through the mux and records the
// upstream session each one was sent with.
type callerScopeStub struct {
	mu       sync.Mutex
	sessions []string
}

func (s *callerScopeStub) record(v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions = append(s.sessions, v)
}

func (s *callerScopeStub) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sessions...)
}

func newCallerScopeGateway(t *testing.T, stub *callerScopeStub) *Gateway {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		stub.record(r.Header.Get("x-opencode-session"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","object":"chat.completion","choices":[]}`))
	}))
	t.Cleanup(server.Close)

	g := newStubGateway(t, server.URL)
	// Two distinct local keys: the caller is the fingerprint of whichever
	// matched, and that fingerprint is what scopes the derived session.
	g.cfg.ServerKeys = []string{"key-alpha", "key-beta"}
	manager, err := rotation.New("", config.RotationConfig{})
	if err != nil {
		t.Fatal(err)
	}
	g.rotation = manager
	registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
		config.TierZen: {"probe-free": wire.Chat},
	})
	freezeProxyHealth(g)
	return g
}

// drive posts through the mux with a fixed client session header, so the only
// thing that can make the upstream session differ is the server key.
func driveScoped(t *testing.T, g *Gateway, serverKey string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"probe-free","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer "+serverKey)
	req.Header.Set("x-opencode-session", "shared-client-signal")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("request with key %q: status %d: %s", serverKey, rec.Code, rec.Body.String())
	}
}

// TestCallerScopeSeparatesUpstreamSessionsAcrossServerKeys is the case the
// merge owed upstream: without it, "scope derived upstream sessions to the
// calling server key" is an untested claim.
func TestCallerScopeSeparatesUpstreamSessionsAcrossServerKeys(t *testing.T) {
	stub := &callerScopeStub{}
	g := newCallerScopeGateway(t, stub)

	driveScoped(t, g, "key-alpha")
	driveScoped(t, g, "key-beta")

	got := stub.all()
	if len(got) != 2 {
		t.Fatalf("want two upstream requests, got %d", len(got))
	}
	if got[0] == "" || got[1] == "" {
		t.Fatalf("upstream session header missing: %q / %q", got[0], got[1])
	}
	if got[0] == got[1] {
		t.Fatalf("two server keys replaying one client session landed on the same upstream session %q — "+
			"callerScope is not reaching DeriveRequestIDs", got[0])
	}
}

// TestCallerScopeIsDeterministicForOneKey is the control. Without it the case
// above could pass for the wrong reason — a random fallback session would also
// differ between the two requests — and would prove nothing about scoping.
// The derivation must be stable for a fixed (key, client signal) pair, or the
// upstream prompt cache would miss on every turn of one conversation.
func TestCallerScopeIsDeterministicForOneKey(t *testing.T) {
	stub := &callerScopeStub{}
	g := newCallerScopeGateway(t, stub)

	driveScoped(t, g, "key-alpha")
	driveScoped(t, g, "key-alpha")

	got := stub.all()
	if len(got) != 2 {
		t.Fatalf("want two upstream requests, got %d", len(got))
	}
	if got[0] != got[1] {
		t.Fatalf("one server key and one client signal produced two upstream sessions %q / %q — "+
			"the derivation is not deterministic, so every turn would miss the prompt cache", got[0], got[1])
	}
}

// TestCallerScopeIsNotEmptyBehindAuthenticate pins the precondition the other
// two depend on: after authenticate, the caller scope is a non-empty value.
// An empty scope degrades silently — DeriveRequestIDs falls back to unscoped
// sessions — so nothing else would notice its loss.
func TestCallerScopeIsNotEmptyBehindAuthenticate(t *testing.T) {
	stub := &callerScopeStub{}
	g := newCallerScopeGateway(t, stub)

	var captured string
	next := g.authenticate(func(w http.ResponseWriter, r *http.Request) {
		captured = callerScope(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer key-alpha")
	next(httptest.NewRecorder(), req)

	if captured == "" {
		t.Fatal("authenticate left an empty caller scope; DeriveRequestIDs would silently fall back to shared sessions")
	}
}
