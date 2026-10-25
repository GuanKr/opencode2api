package gateway

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/models"
	"opencode2api/internal/telemetry"
)

// freezeProxyHealth parks every proxy's health-check flag so a non-2xx
// upstream reply cannot schedule the background verification. Without it any
// 4xx/5xx stub reply makes syncProxyResult call verifyProxyAfterError, which
// dials proxyHealthCheckURL (the public internet) from a detached goroutine.
// That function starts with checking.CompareAndSwap(false, true) and returns
// immediately when the flag is already claimed, so parking it here is what
// keeps a test that scripts a 4xx/5xx upstream reply fully offline without
// changing any gateway behaviour. Call it for every gateway whose stub can
// answer with a non-2xx status.
func freezeProxyHealth(g *Gateway) {
	for _, proxy := range g.transports.items {
		proxy.checking.Store(true)
	}
}

// newStubGateway wires a Gateway at baseURL. The anonymous lane talks to
// upstream.anonymous (the free-tier base), not upstream.zen, so all three
// bases point at the stub and a key-tier fallback would also land on it.
func newStubGateway(t *testing.T, baseURL string) *Gateway {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	transports, err := transportPool([]string{"direct"}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	zenNodes, err := nodePool(nil, transports, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	goNodes, err := nodePool(nil, transports, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return &Gateway{
		cfg: config.Config{
			Anonymous: true,
			Retry:     config.RetryConfig{MaxAttempts: 3, TimeoutSeconds: 60},
			Upstream:  config.UpstreamConfig{Zen: baseURL, Go: baseURL, Anonymous: baseURL},
		},
		logger:     logger,
		transports: transports,
		zenNodes:   zenNodes,
		goNodes:    goNodes,
		anonymous:  anonymousPool(true, transports, time.Second, anonymousBackoff{}),
		catalog:    models.Catalog(config.TierZen, nil),
		monitor:    telemetry.Monitor(),
		sessions:   newSessionGenerations(),
	}
}
