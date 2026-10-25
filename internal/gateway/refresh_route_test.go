package gateway

import (
	"context"
	"testing"
	"time"

	"opencode2api/internal/models"
	"opencode2api/internal/warp"
)

// emptyLaneGateway builds an anonymous-enabled gateway whose lane order
// resolves to zero upstreams: upstream.anonymous sits on the legacy origin and
// the default ["inference"] order skips a base equal to that origin — exactly
// the configuration D1 requires every consumer to report as unavailable
// instead of dialing. The node is deliberately NOT cooling (the refresh cursor
// skips cooling nodes, which would mask the fabricated-success path) and is
// loaded with failure and breaker counters that markRefreshSuccess +
// NoteSuccess would clear.
func emptyLaneGateway(t *testing.T) (*Gateway, *anonymousNode) {
	t.Helper()
	g := newStubGateway(t, "http://127.0.0.1:1")
	freezeProxyHealth(g)
	g.cfg.Upstream.Anonymous = models.LegacyZenBase
	node := g.anonymous.nodes[0]
	node.proxy.healthy.Store(true)
	node.failures.Store(3)
	g.anonymous.consecutive.Store(2)
	g.anonymous.breakerStrikes.Store(1)
	return g, node
}

// A background catalog refresh with no usable lane must report the route as
// missing and keep nil, not "succeed": the lane walk runs zero FetchModels
// attempts while err stays nil, and the unfixed success branch called
// markRefreshSuccess + NoteSuccess on that non-attempt — healing the node's
// cooldown and closing the lane breaker on evidence nobody produced, against
// the recovery-evidence-is-user-traffic-only invariant. The request path
// answers 503 proxy_error for the same configuration; the refresh path must
// not be the one consumer that invents a success.
func TestRefreshAnonymousTierWithNoLaneReportsMissingInsteadOfSucceeding(t *testing.T) {
	g, node := emptyLaneGateway(t)

	listed := g.refreshAnonymousTier(context.Background(), "https://opencode.ai/inference")

	if listed != nil {
		t.Fatalf("a refresh with no usable lane must keep the previous catalog, got %v", listed)
	}
	if got := node.failures.Load(); got != 3 {
		t.Fatalf("markRefreshSuccess must not run on zero attempts, failures = %d", got)
	}
	if got := g.anonymous.consecutive.Load(); got != 2 {
		t.Fatalf("NoteSuccess must not run on zero attempts, consecutive = %d", got)
	}
	if got := g.anonymous.breakerStrikes.Load(); got != 1 {
		t.Fatalf("NoteSuccess must not run on zero attempts, breakerStrikes = %d", got)
	}
}

// The same gate must sit ahead of the WARP dispatch: with the switch on and no
// usable lane, the refresh returns before touching the exit pool. The context
// is capped so a regression that moves the gate below the dispatch can only
// burn the deadline, never hang the suite.
func TestRefreshAnonymousTierWithNoLaneStopsBeforeWARPEgress(t *testing.T) {
	g, _ := emptyLaneGateway(t)
	g.warp = &warp.Pool{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	listed := g.refreshAnonymousTier(ctx, "https://opencode.ai/inference")

	if listed != nil {
		t.Fatalf("a WARP refresh with no usable lane must keep the previous catalog, got %v", listed)
	}
	if got := g.anonymous.consecutive.Load(); got != 2 {
		t.Fatalf("NoteSuccess must not run on zero attempts, consecutive = %d", got)
	}
}
