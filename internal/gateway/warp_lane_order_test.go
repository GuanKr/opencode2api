package gateway

import (
	"context"
	"errors"
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
)

// TestWarpAnonymousLaneRefusesAnUnusableLaneOrder pins the guard the proxy lane
// already had: when upstream.anonymous is the v1 origin and the lane order
// excludes "legacy", anonymousUpstreams is empty. The WARP lane must report the
// route mistake (proxy_error) instead of making zero attempts, tripping the lane
// breaker, and answering warp_unavailable.
func TestWarpAnonymousLaneRefusesAnUnusableLaneOrder(t *testing.T) {
	g := newStubGateway(t, models.LegacyZenBase)
	g.cfg.Upstream.Anonymous = models.LegacyZenBase
	g.cfg.Upstream.AnonymousLaneOrder = []string{config.LaneInference}
	bodies := map[config.Tier][]byte{
		config.TierZen: []byte(`{"model":"probe-free","stream":false,"messages":[{"role":"user","content":"hi"}]}`),
	}
	ids := identity.RequestIDs{Session: identity.CanonicalSessionID("warp-empty-lane"), Request: "req-warp-empty"}

	for attempt := 0; attempt < 3; attempt++ {
		resp, err, attempts := g.doWarpAnonymousUpstream(context.Background(), chatDumpRoute(), bodies, ids, 0)
		if resp != nil {
			resp.Body.Close()
		}
		if !errors.Is(err, errProxyUnavailable) {
			t.Fatalf("call %d: an empty lane order must report proxy_error, got %v", attempt, err)
		}
		if errors.Is(err, errWARPUnavailable) {
			t.Fatalf("call %d: a config mistake must not be reported as WARP unavailability: %v", attempt, err)
		}
		if attempts != 0 {
			t.Fatalf("call %d: no upstream attempt can exist on an empty lane, got %d", attempt, attempts)
		}
	}
}
