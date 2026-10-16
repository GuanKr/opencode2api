package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
	"opencode2api/internal/telemetry"
)

// The anonymous cooldown used to be reported as "all upstream attempts failed"
// (502). That is load-bearing in the wrong direction: a 502 reads as a broken
// upstream, so clients retry it immediately, and watchdog-clash.ps1 reads it as
// "this node is broken, switch". Since the cooldown is held on the proxy
// transport and not on the Clash node, neither reaction can help, and one
// upstream 429 turned into 27 upstream-less 502s in a single hour on 2026-10-16
// plus a candidate-pool burn that switched 8 nodes in 12 seconds.

// coolingStub answers the first upstream call with the free tier's quota 429
// (which is what puts the anonymous node into its cooldown) and 500s after it,
// so the second request provably never reaches upstream.
func coolingStub(t *testing.T) *Gateway {
	t.Helper()
	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		hits++
		w.Header().Set("Content-Type", "application/json")
		if hits == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"type":"FreeUsageLimitError","message":"Rate limit exceeded"}}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
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
	// newStubGateway hands out a 1s cooldown; the cases below send two requests
	// back to back and must not depend on that being long enough to still be
	// cooling when the second one lands.
	g.anonymous.cooldown = 30 * time.Second
	// Keeps the whole case offline: a 4xx would otherwise schedule a detached
	// verifyProxyAfterError, and an all-unhealthy pool additionally reaches
	// kickProxyRecheck — both dial the public health-check URL. freezeProxyHealth
	// parks the per-proxy checking CAS that CheckHealth tries to claim, which is
	// what makes those probes no-ops. TestKickProxyRecheckGuards asserts the
	// guards so this cannot regress into real egress unnoticed.
	freezeProxyHealth(g)
	return g
}

// drive posts one chat request through the gateway and returns the recorder.
func drive(t *testing.T, g *Gateway, body string) *httptest.ResponseRecorder {
	t.Helper()
	handler := telemetry.Middleware(g.monitor, g.logger, g.handleInference(wire.Chat))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

const coolingRequestBody = `{"model":"probe-free","messages":[{"role":"user","content":"hi"}]}`

func TestAnonymousCooldownAnswers429WithRetryAfter(t *testing.T) {
	g := coolingStub(t)

	// First request: the upstream quota 429 reaches the client with its message.
	first := drive(t, g, coolingRequestBody)
	if first.Code != http.StatusTooManyRequests {
		t.Fatalf("upstream 429 must pass through, got %d: %s", first.Code, first.Body.String())
	}
	if !strings.Contains(first.Body.String(), "Rate limit exceeded") {
		t.Fatalf("first response must carry the upstream message, got %s", first.Body.String())
	}
	// The first reply is upstream-attributed: no gateway prefix, no retry window.
	// Only the locally blocked reply below may claim to be the gateway's own.
	if strings.Contains(first.Body.String(), "opencode2api:") || first.Header().Get("Retry-After") != "" {
		t.Fatalf("the upstream 429 must not be dressed up as a gateway cooldown, got %s", first.Body.String())
	}

	// Second request: the node is now cooling, so nothing goes upstream.
	second := drive(t, g, coolingRequestBody)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("a request blocked by the cooldown must answer 429, got %d: %s",
			second.Code, second.Body.String())
	}
	body := second.Body.String()
	if !strings.Contains(body, "opencode2api:") {
		t.Fatalf("a gateway-authored error must carry the opencode2api: prefix, got %s", body)
	}
	if !strings.Contains(body, "cooling") {
		t.Fatalf("body must say the lane is cooling, got %s", body)
	}
	// The upstream cause has to survive onto the wire: a locally blocked request
	// is the only place an operator learns *why* the lane is cooling without
	// reading the server log. Assert the FULL cause including its status tail —
	// the bolded lead already carries the bare class name, so matching on that
	// alone would pass even with the cause dropped.
	if !strings.Contains(body, "triggered by FreeUsageLimitError (upstream HTTP 429)") {
		t.Fatalf("body must carry the full upstream cause that triggered the cooldown, got %s", body)
	}
	// Retry-After is what makes the 429 actionable instead of just another
	// failure; without it a client picks its own backoff.
	raw := second.Header().Get("Retry-After")
	if raw == "" {
		t.Fatal("cooling 429 must carry Retry-After")
	}
	secs, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || secs < 1 {
		t.Fatalf("Retry-After must be a positive integer, got %q", raw)
	}
	if secs > int64(anonymousCooldownCeiling(g.anonymous.cooldown).Seconds()) {
		t.Fatalf("Retry-After %ds exceeds the anonymous cooldown ceiling", secs)
	}
}

// TestAnonymousCooldownDoesNotMaskUnhealthyPool pins the other side of the
// branch: with no cooldown to report, an unusable pool is still a genuine
// transport failure and must keep answering 502. Widening the cooling reply to
// every cursor-empty case would turn real outages into "come back later".
func TestAnonymousCooldownDoesNotMaskUnhealthyPool(t *testing.T) {
	g := coolingStub(t)
	for _, proxy := range g.transports.items {
		proxy.healthy.Store(false)
	}
	rec := drive(t, g, coolingRequestBody)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("an unusable pool with no cooldown must stay 502, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "" {
		t.Fatalf("a transport failure must not promise a retry window, got Retry-After=%q",
			rec.Header().Get("Retry-After"))
	}
	if strings.Contains(rec.Body.String(), "opencode2api:") {
		t.Fatalf("a transport failure is not a gateway-authored cooldown, got %s", rec.Body.String())
	}
}

// TestAnonymousCoolingMessageShape pins the two client-visible strings. The
// script greps for the prefix, and the class/cause split is what makes a log
// line readable at a glance.
func TestAnonymousCoolingMessageShape(t *testing.T) {
	if got := anonymousCoolingMessage(30, ""); !strings.HasPrefix(got, "opencode2api:") ||
		!strings.Contains(got, "30s") {
		t.Fatalf("cause-less message must lead with the prefix and the retry window, got %q", got)
	}
	got := anonymousCoolingMessage(30, "FreeUsageLimitError (upstream HTTP 429)")
	// The bolded lead is the bare class: the "(upstream HTTP N)" tail belongs to
	// the trailing cause, not to the headline a reader scans first.
	if !strings.Contains(got, "**[gateway]** **FreeUsageLimitError** anonymous lane cooling") {
		t.Fatalf("the bolded lead must be the bare trigger class, got %q", got)
	}
	if !strings.Contains(got, "triggered by FreeUsageLimitError (upstream HTTP 429)") {
		t.Fatalf("the full cause must be preserved in the tail, got %q", got)
	}
	if err := (errAnonymousCooling{retryAfter: 30 * time.Second}).Error(); !strings.Contains(err, "30s") {
		t.Fatalf("the internal error must name the retry window, got %q", err)
	}
}

// TestAnonymousCooldownStaysUnderTheCeiling pins the 15s ceiling. Both rules
// that can produce a wait are bounded by it: the exponential ladder (which used
// to top out at 8x the base, i.e. 80s here) and an upstream Retry-After asking
// for hours. The ceiling is what bounds a client-visible outage, because the
// cooldown sits on the proxy transport where no node switch can shorten it.
func TestAnonymousCooldownStaysUnderTheCeiling(t *testing.T) {
	base := 10 * time.Second
	cases := []struct {
		name       string
		failures   int
		retryAfter string
		want       time.Duration
	}{
		{"first failure keeps the base", 1, "", base},
		{"an hour-long Retry-After is clamped", 1, "3600", 15 * time.Second},
		{"a short Retry-After does not shorten the ladder", 1, "2", base},
		{"repeated failures stop escalating", 6, "", 15 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &anonymousPool{cooldown: base}
			node := &anonymousNode{}
			node.proxy = &proxyTransport{}
			node.proxy.healthy.Store(true)
			for i := 0; i < tc.failures; i++ {
				var resp *http.Response
				if tc.retryAfter != "" {
					resp = &http.Response{StatusCode: http.StatusTooManyRequests,
						Header: http.Header{"Retry-After": []string{tc.retryAfter}}}
				}
				// Unscoped (fingerprint ""): this case pins the backoff ladder, not the
				// request scope.
				pool.startCooldown(node, resp, nil, "FreeUsageLimitError (upstream HTTP 429)", "")
			}
			got := time.Until(time.Unix(0, node.cooldownUntil.Load()))
			if got > tc.want {
				t.Fatalf("cooldown %v exceeds the expected %v", got.Round(time.Second), tc.want)
			}
			// A slack of a second absorbs the clock advancing inside the helper.
			if got < tc.want-time.Second {
				t.Fatalf("cooldown %v is shorter than the expected %v", got.Round(time.Second), tc.want)
			}
			if got > maxAnonymousCooldown+time.Second {
				t.Fatalf("cooldown %v escaped the %v ceiling", got.Round(time.Second), maxAnonymousCooldown)
			}
		})
	}
}

// TestSystemOneCooldownAnswers429Too pins the second handler. It carries its own
// error path, so the cooling branch there was silently uncovered: deleting those
// lines left the whole package green.
func TestSystemOneCooldownAnswers429Too(t *testing.T) {
	g := coolingStub(t)
	registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
		config.TierZen: {"probe-free": wire.Chat, "decision-free": wire.SystemOne},
	})
	// Drive the node straight into its cooldown so the case does not depend on
	// whatever the upstream stub happens to answer first.
	g.anonymous.startCooldown(g.anonymous.nodes[0],
		&http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{}}, nil,
		"FreeUsageLimitError (upstream HTTP 429)", "")

	handler := telemetry.Middleware(g.monitor, g.logger, http.HandlerFunc(g.handleSystemOne))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/systemone",
		strings.NewReader(`{"model":"decision-free","state":{},"questions":[]}`)))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("systemone cooldown must answer 429, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("systemone cooldown 429 must carry Retry-After")
	}
	if !strings.Contains(rec.Body.String(), "opencode2api:") {
		t.Fatalf("body must mark the error as gateway-authored, got %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "all upstream attempts failed") {
		t.Fatalf("systemone must not answer a cooldown with the 502 wording, got %s", rec.Body.String())
	}
}

// TestKickProxyRecheckGuards pins the three conditions that keep the on-demand
// re-probe from turning into real egress or into a goroutine per request. It
// observes lastProxyKick rather than the probe itself, so no network is needed.
func TestKickProxyRecheckGuards(t *testing.T) {
	markAllUnhealthy := func(g *Gateway) {
		for _, proxy := range g.transports.items {
			proxy.healthy.Store(false)
		}
	}

	t.Run("a healthy proxy is never probed", func(t *testing.T) {
		g := coolingStub(t)
		g.kickProxyRecheck(context.Background())
		if g.lastProxyKick.Load() != 0 {
			t.Fatal("nothing to recover while a proxy is healthy, so no kick may be scheduled")
		}
	})

	t.Run("an all-down pool kicks once and then throttles", func(t *testing.T) {
		g := coolingStub(t)
		markAllUnhealthy(g)
		g.kickProxyRecheck(context.Background())
		first := g.lastProxyKick.Load()
		if first == 0 {
			t.Fatal("an all-down pool must schedule a re-probe")
		}
		// The per-proxy CAS only dedupes overlapping probes, not goroutine
		// creation, so the throttle is the thing that bounds an all-down spike.
		g.kickProxyRecheck(context.Background())
		if g.lastProxyKick.Load() != first {
			t.Fatal("a second kick inside the throttle window must be dropped")
		}
	})

	t.Run("an already cancelled request kicks nothing", func(t *testing.T) {
		g := coolingStub(t)
		markAllUnhealthy(g)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		g.kickProxyRecheck(ctx)
		if g.lastProxyKick.Load() != 0 {
			t.Fatal("a cancelled request must not schedule a probe")
		}
	})
}

// TestAnonymousCooldownCeilingNeverUndercutsTheBase guards the one footgun of
// an absolute ceiling: an operator who sets failure_cooldown_seconds above the
// ceiling must not get a first-failure wait shorter than the base they asked
// for.
func TestAnonymousCooldownCeilingNeverUndercutsTheBase(t *testing.T) {
	if got := anonymousCooldownCeiling(30 * time.Second); got != 30*time.Second {
		t.Fatalf("a base above the ceiling must win, got %v", got)
	}
	if got := anonymousCooldownCeiling(10 * time.Second); got != 15*time.Second {
		t.Fatalf("a base below the ceiling must not lower it, got %v", got)
	}
}
