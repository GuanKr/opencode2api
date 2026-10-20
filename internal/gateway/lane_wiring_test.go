package gateway

import (
	"net/http"
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/models"
)

// D1: the lane order is the contract. With the default ["inference"] order and
// upstream.anonymous pointed at the v1 origin, the lane has no usable base and
// must report that instead of silently dialing the legacy origin the order
// excluded.
func TestAnonymousUpstreamsHasNoImplicitLegacyFallback(t *testing.T) {
	legacy := models.LegacyZenBase
	gateway := &Gateway{cfg: config.Config{Upstream: config.UpstreamConfig{
		Anonymous:          legacy,
		AnonymousLaneOrder: []string{config.LaneInference},
	}}}
	if got := gateway.anonymousUpstreams(); len(got) != 0 {
		t.Fatalf("anonymousUpstreams() = %q, want no upstream when the only lane resolves to the legacy origin", got)
	}

	// Once the operator opts into legacy, it is the lane that serves traffic.
	gateway.cfg.Upstream.AnonymousLaneOrder = []string{config.LaneInference, config.LaneLegacy}
	got := gateway.anonymousUpstreams()
	if len(got) != 1 || got[0] != legacy {
		t.Fatalf("anonymousUpstreams() = %q, want [%q] when legacy is opted in", got, legacy)
	}
}

// The Bun disguise is scoped to free models on the v1 and /inference lanes.
// Every other combination must keep the regular client: Go-tier traffic and
// paid models are not disguised, and a free model on the Go lane is not a
// free-tier request.
func TestInferenceClientPicksBunForFreeModelsOnV1AndInferenceOnly(t *testing.T) {
	gateway := &Gateway{catalog: models.Catalog(config.TierZen, nil)}
	regular := &http.Client{}
	bun := &http.Client{}
	proxy := &proxyTransport{client: regular, bun: bun}

	cases := []struct {
		name    string
		baseURL string
		model   string
		wantBun bool
	}{
		{"free on the v1 /zen lane", "https://opencode.ai/zen", "test-model-free", true},
		{"free on the /inference lane", "https://opencode.ai/inference", "test-model-free", true},
		{"free on the Go lane", "https://opencode.ai/zen/go", "test-model-free", false},
		{"paid model on /inference", "https://opencode.ai/inference", "test-model-paid", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := gateway.inferenceClient(proxy, tc.baseURL, tc.model)
			if (got == bun) != tc.wantBun {
				t.Fatalf("inferenceClient(%q, %q) picked bun=%v, want bun=%v", tc.baseURL, tc.model, got == bun, tc.wantBun)
			}
		})
	}
}
