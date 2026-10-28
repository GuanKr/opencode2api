package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

// assertPromptEnv checks the two <env> directory lines in an outbound body.
func assertPromptEnv(t *testing.T, body, working, root string) {
	t.Helper()
	for _, want := range []string{
		"Working directory: " + working,
		"Workspace root folder: " + root,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("outbound body is missing %q:\n%s", want, body)
		}
	}
}

// TestPromptEnvValuesFallsBackWithoutConfig covers the renderer's own zero-value
// path: a Gateway built directly in a test (or any caller that never ran
// Normalize) must still render the container path, never an empty line.
func TestPromptEnvValuesFallsBackWithoutConfig(t *testing.T) {
	working, root := promptEnvValues(config.PromptEnvConfig{})
	if working != config.DefaultPromptWorkingDirectory || root != working {
		t.Fatalf("zero prompt_env = (%q, %q), want the container default for both", working, root)
	}
	working, root = promptEnvValues(config.PromptEnvConfig{WorkingDirectory: " /srv/agent "})
	if working != "/srv/agent" || root != "/srv/agent" {
		t.Fatalf("working-only prompt_env = (%q, %q), want root to follow the trimmed working directory", working, root)
	}
}

// TestAnonymousLaneSendsTheConfiguredPromptEnvironment locks the anonymous call
// site: upstream.go passes g.cfg.PromptEnv into prepareAnonymousBody. A call
// site that drops the argument would silently render /workspace, and the direct
// shaper tests would stay green.
func TestAnonymousLaneSendsTheConfiguredPromptEnvironment(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","object":"chat.completion","choices":[]}`))
	}))
	t.Cleanup(server.Close)

	g := newStubGateway(t, server.URL)
	g.cfg.PromptEnv = config.PromptEnvConfig{WorkingDirectory: "/srv/anon", WorkspaceRoot: "/srv/anon-root"}
	freezeProxyHealth(g)
	bodiesByTier := map[config.Tier][]byte{
		config.TierZen: []byte(`{"model":"probe-free","stream":false,"messages":[{"role":"user","content":"hi"}]}`),
	}
	ids := identity.RequestIDs{
		Session: identity.CanonicalSessionID("prompt-env-anon"),
		Project: identity.RandomProjectID(),
		Request: identity.RandomID("req", 16),
	}

	resp, err, attempts := g.doAnonymousUpstream(context.Background(), chatDumpRoute(), bodiesByTier, ids, 0, "")
	if err != nil {
		t.Fatalf("anonymous attempt failed: %v", err)
	}
	if resp == nil {
		t.Fatal("anonymous attempt returned no response")
	}
	resp.Body.Close()
	if attempts != 1 {
		t.Fatalf("want exactly one upstream attempt, got %d", attempts)
	}

	mu.Lock()
	got := append([]string(nil), bodies...)
	mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("want exactly one outbound body, got %d", len(got))
	}
	assertPromptEnv(t, got[0], "/srv/anon", "/srv/anon-root")
}

// TestKeyLaneShapingSendsTheConfiguredPromptEnvironment locks shapeKeyBody: the
// free-model key lane must pass g.cfg.PromptEnv instead of a zero value.
func TestKeyLaneShapingSendsTheConfiguredPromptEnvironment(t *testing.T) {
	g := &Gateway{
		cfg: config.Config{
			PromptEnv: config.PromptEnvConfig{WorkingDirectory: "/srv/key-lane", WorkspaceRoot: "/srv/key-lane-root"},
			Upstream:  config.UpstreamConfig{Zen: "https://opencode.ai/zen", Go: "https://opencode.ai/zen/go"},
		},
		catalog: models.Catalog(config.TierZen, nil),
	}
	route := models.Route{
		ID: "probe-free", Tier: config.TierZen, Protocol: wire.Chat, Anonymous: true,
		Protocols: map[config.Tier]wire.Protocol{config.TierZen: wire.Chat},
	}
	body := []byte(`{"model":"probe-free","messages":[{"role":"user","content":"hi"}]}`)
	shaped, _, changed := g.shapeKeyBody(body, route, config.TierZen)
	if !changed {
		t.Fatal("a free model must be shaped on the key lane")
	}
	assertPromptEnv(t, string(shaped), "/srv/key-lane", "/srv/key-lane-root")
}
