package gateway

import (
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	wire "opencode2api/internal/protocol"
)

// An exit the free tier flagged stays out of rotation for the whole block
// window. This is enforced in Next (which upstream owns) and in
// NextForRequest (the path ordinary traffic takes in this build); without the
// latter, a blocked exit kept being drawn for every request and each attempt
// repeated the same 403 while the penalty was still running.
func TestNextForRequestSkipsBlockedExit(t *testing.T) {
	transports, err := transportPool([]string{"direct"}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool := anonymousPool(true, transports, 10*time.Second, anonymousBackoff{})
	node := pool.nodes[0]
	node.proxy.healthy.Store(true)

	if got := nextFor(pool, "turn"); got != node {
		t.Fatalf("a healthy, unflagged exit must be handed out, got %v", got)
	}

	// Flag it the way doAnonymousUpstream does after an abuse response.
	node.proxy.markBlocked(time.Now().Add(15 * time.Minute))
	if got := nextFor(pool, "turn"); got != nil {
		t.Fatal("a blocked exit must not be drawn again while its block runs")
	}
	// coolingFor counts healthy nodes only, so a blocked exit must not be
	// reported as a cooldown: that would answer 429 + Retry-After instead of
	// the free_tier_ip_blocked 503.
	if cooling, _ := pool.coolingFor("turn"); cooling {
		t.Fatal("a blocked exit is not a cooling exit; the block has its own 503")
	}

	node.proxy.markBlocked(time.Now().Add(-time.Second))
	if got := nextFor(pool, "turn"); got != node {
		t.Fatalf("an expired block must return the exit to rotation, got %v", got)
	}
}

// The 2.1 client groups /inference routes by provider family; the flat route
// still answers, so a wrong prefix is a silent fidelity loss rather than an
// error. Pin both the grouped and the flat case.
func TestUpstreamRequestUsesV2FamilyRoutes(t *testing.T) {
	cases := []struct {
		base     string
		protocol wire.Protocol
		want     string
	}{
		{"https://opencode.ai/inference", wire.Chat, "https://opencode.ai/inference/openai/v1/chat/completions"},
		{"https://opencode.ai/inference/", wire.Responses, "https://opencode.ai/inference/openai/v1/responses"},
		// The Anthropic lane carries the beta flag (withAnthropicBeta).
		{"https://opencode.ai/inference", wire.Anthropic, "https://opencode.ai/inference/anthropic/v1/messages?beta=true"},
		// A custom base and the /zen/go lane keep the flat route: the family
		// grouping belongs to /inference alone.
		{"https://proxy.example", wire.Chat, "https://proxy.example/v1/chat/completions"},
		{"https://opencode.ai/zen/go", wire.Chat, "https://opencode.ai/zen/go/v1/chat/completions"},
		// The v1 lane keeps the 1.x shape.
		{"https://opencode.ai/zen", wire.Chat, "https://opencode.ai/zen/v1/chat/completions"},
	}
	for _, tc := range cases {
		req, err := upstreamRequest(t.Context(), tc.base, tc.protocol, []byte(`{}`), identity.RequestIDs{Request: "req-route"}, anonymousZenKey)
		if err != nil {
			t.Fatalf("%s: %v", tc.base, err)
		}
		if got := req.URL.String(); got != tc.want {
			t.Fatalf("%s %s: got %s, want %s", tc.base, tc.protocol, got, tc.want)
		}
	}
}
