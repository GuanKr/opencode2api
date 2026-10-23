package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opencode2api/internal/config"
)

// The breaker's half-open window is the only thing standing between "the lane
// is back" and "every queued request slams it at once". One winner, and the
// slot comes back when the winner reports in.
func TestAnonymousBreakerHalfOpenAdmitsOneProbe(t *testing.T) {
	pool := &anonymousNodes{backoff: anonymousBackoff{threshold: 1, base: 20 * time.Millisecond, max: 80 * time.Millisecond}}

	if !pool.AllowAttempt() {
		t.Fatal("a closed breaker must admit every attempt")
	}
	pool.ReleaseProbe()

	pause, tripped := pool.NoteFailure()
	if !tripped || pause != 20*time.Millisecond {
		t.Fatalf("first failure must trip for the base window, got %s/%v", pause, tripped)
	}
	if pool.AllowAttempt() {
		t.Fatal("an open breaker must refuse attempts")
	}

	// Expire the window without sleeping: the breaker stores an absolute
	// deadline, so moving it back is the same as waiting it out.
	pool.breakerUntil.Store(time.Now().Add(-time.Second).UnixNano())
	if !pool.AllowAttempt() {
		t.Fatal("an expired window must admit one half-open probe")
	}
	if pool.AllowAttempt() {
		t.Fatal("only one half-open probe may run at a time")
	}
	pool.ReleaseProbe()
	if !pool.AllowAttempt() {
		t.Fatal("a released probe slot must be usable again")
	}
}

// Each trip doubles the window and clamps at the configured ceiling; without
// the clamp a long-broken lane would back off past its own max_hours.
func TestAnonymousBreakerPauseDoublesAndCaps(t *testing.T) {
	pool := &anonymousNodes{backoff: anonymousBackoff{threshold: 1, base: 10 * time.Millisecond, max: 25 * time.Millisecond}}
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond}
	for i, expect := range want {
		// Force the window closed so the next failure trips instead of counting.
		pool.breakerUntil.Store(time.Now().Add(-time.Second).UnixNano())
		pause, tripped := pool.NoteFailure()
		if !tripped {
			t.Fatalf("trip %d: expected a trip", i)
		}
		if pause != expect {
			t.Fatalf("trip %d: got %s, want %s", i, pause, expect)
		}
	}
}

// A request-level rejection proves the lane is reachable, but only the request
// holding the half-open slot may say so. NoteNeutral must never clear a breaker
// that another request just tripped: that request would have no idea whether the
// lane is back, and the lane would resume hammering a failing upstream.
func TestAnonymousNoteNeutralClosesOnlyForTheProbeHolder(t *testing.T) {
	pool := &anonymousNodes{backoff: anonymousBackoff{threshold: 3, base: time.Hour, max: time.Hour}}
	// Trip it once through the real path (expired window -> immediate trip), so
	// the strike counter has something NoteNeutral could wrongly reset.
	pool.breakerUntil.Store(time.Now().Add(-time.Second).UnixNano())
	if _, tripped := pool.NoteFailure(); !tripped {
		t.Fatal("an expired window must trip on the next failure")
	}
	if _, paused := pool.pausedFor(); !paused {
		t.Fatal("a tripped breaker must be paused")
	}

	// Normal traffic does not hold the probe slot: it must not close the breaker.
	pool.NoteNeutral()
	if _, paused := pool.pausedFor(); !paused {
		t.Fatal("a request that does not hold the half-open slot must not close the breaker")
	}

	// The half-open winner does.
	pool.probing.Store(true)
	pool.NoteNeutral()
	if _, paused := pool.pausedFor(); paused {
		t.Fatal("the half-open holder must close the breaker on a neutral answer")
	}
	if got := pool.breakerStrikes.Load(); got != 1 {
		t.Fatalf("NoteNeutral must not reset the trip count, got %d", got)
	}
	if got := pool.consecutive.Load(); got != 0 {
		t.Fatalf("NoteNeutral must clear the consecutive counter, got %d", got)
	}
}

// breakerGateway builds a gateway whose anonymous pool has one healthy node and
// a breaker that trips on the first strike, so a single observed result decides
// the test.
func breakerGateway(t *testing.T) (*Gateway, *anonymousNode) {
	t.Helper()
	g := newStubGateway(t, "http://127.0.0.1:1")
	freezeProxyHealth(g)
	pool := anonymousPool(true, g.transports, time.Second, anonymousBackoff{threshold: 1, base: time.Hour, max: time.Hour})
	g.anonymous = pool
	node := pool.nodes[0]
	node.proxy.healthy.Store(true)
	return g, node
}

// A 429 is the free allowance being burned, not the lane being down. This build
// answers it with a 15s cooldown + Retry-After (D3), so it must not also pause
// the whole lane for an hour: one all-429 request across eight exits would
// otherwise trip the breaker on its own.
func TestObserveAnonymousResultDoesNotStrikeOnQuotaAnswers(t *testing.T) {
	g, node := breakerGateway(t)
	body := `{"error":{"type":"RateLimitError","message":"Rate limit exceeded"}}`
	resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}

	g.observeAnonymousResult(context.Background(), node, resp, nil, "turn")

	if _, paused := g.anonymous.pausedFor(); paused {
		t.Fatal("a 429 must not trip the lane breaker")
	}
	if got := g.anonymous.consecutive.Load(); got != 0 {
		t.Fatalf("a 429 must not count as a strike, got %d", got)
	}
	if node.cooldownUntil.Load() == 0 {
		t.Fatal("a 429 must still cool the exit down (D3)")
	}
}

// A transport failure is the opposite case: nothing reached the upstream, so
// the lane itself is the suspect and the breaker is exactly the right answer.
func TestObserveAnonymousResultStrikesOnTransportFailure(t *testing.T) {
	g, node := breakerGateway(t)
	g.observeAnonymousResult(context.Background(), node, nil, errors.New("dial tcp 127.0.0.1:1080: connect: connection refused"), "turn")

	if _, paused := g.anonymous.pausedFor(); !paused {
		t.Fatal("a transport failure must trip the lane breaker")
	}
}

// The status feed is what the failover script reads. A blocked exit used to be
// invisible there: the 403's short cooldown expired, the snapshot went empty and
// the feed said verdict=ok while every request answered free_tier_ip_blocked.
func TestWatchdogStatusSurfacesBlockedAndPausedLane(t *testing.T) {
	type anonymousStatus struct {
		BlockedExits   int    `json:"blocked_exits"`
		PausedSeconds  int64  `json:"paused_seconds"`
		CoolingSeconds int64  `json:"cooling_seconds"`
		Verdict        string `json:"verdict"`
		Cause          string `json:"cause"`
	}
	type statusBody struct {
		Anonymous       anonymousStatus `json:"anonymous"`
		RecommendSwitch bool            `json:"recommend_switch"`
	}
	read := func(g *Gateway) statusBody {
		t.Helper()
		rec := httptest.NewRecorder()
		g.handleWatchdogStatus(rec, httptest.NewRequest(http.MethodGet, "/v1/watchdog/status", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status must be 200, got %d", rec.Code)
		}
		// A fresh value every call: the fields carry omitempty, so reusing one
		// struct across scenarios would keep stale values for omitted fields.
		var body statusBody
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body
	}

	g, node := breakerGateway(t)
	g.cfg.Anonymous = true
	node.proxy.markBlocked(time.Now().Add(10 * time.Minute))

	body := read(g)
	if body.Anonymous.BlockedExits != 1 {
		t.Fatalf("blocked_exits must be reported, got %d", body.Anonymous.BlockedExits)
	}
	if body.Anonymous.Verdict != "quota" {
		t.Fatalf("a fully blocked lane must vote quota (only another exit helps), got %q", body.Anonymous.Verdict)
	}
	if !body.RecommendSwitch {
		t.Fatal("a fully blocked lane must recommend a switch")
	}

	// The same lane, now paused by the breaker instead: nothing to switch to,
	// so the feed must say "wait" rather than "the node is broken".
	g.anonymous.breakerUntil.Store(time.Now().Add(time.Hour).UnixNano())
	node.proxy.clearBlocked()
	body = read(g)
	if body.Anonymous.Verdict != "cooling" {
		t.Fatalf("a paused lane must vote cooling, got %q (%s)", body.Anonymous.Verdict, body.Anonymous.Cause)
	}
	if body.Anonymous.PausedSeconds <= 0 {
		t.Fatalf("paused_seconds must be reported, got %d", body.Anonymous.PausedSeconds)
	}
	if body.RecommendSwitch {
		t.Fatal("a paused lane must not recommend a switch")
	}
}

// A fully blocked lane outranks a cooling one: the block lasts
// free_tier_block_seconds, the cooldown 15s, and only another exit helps. The
// reported seconds must go with the cause that is actually reported, so the
// replaced cooldown's remainder is dropped rather than paired with a
// lane-level cause.
func TestWatchdogStatusBlockedWinsOverACoolingCause(t *testing.T) {
	g, node := breakerGateway(t)
	g.cfg.Anonymous = true
	// A cooling cause that would normally vote cooling, plus a live cooldown.
	g.anonymous.markUserFailure(node, nil, errors.New("upstream HTTP 400"), "upstream error (upstream HTTP 400)", "turn")
	if node.cooldownUntil.Load() == 0 {
		t.Fatal("fixture must leave a live cooldown")
	}
	node.proxy.markBlocked(time.Now().Add(10 * time.Minute))

	rec := httptest.NewRecorder()
	g.handleWatchdogStatus(rec, httptest.NewRequest(http.MethodGet, "/v1/watchdog/status", nil))
	var body struct {
		Anonymous struct {
			CoolingSeconds int64  `json:"cooling_seconds"`
			Verdict        string `json:"verdict"`
			Cause          string `json:"cause"`
		} `json:"anonymous"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Anonymous.Verdict != "quota" {
		t.Fatalf("a blocked lane must vote quota even with a cooling cause, got %q (%s)", body.Anonymous.Verdict, body.Anonymous.Cause)
	}
	if body.Anonymous.CoolingSeconds != 0 {
		t.Fatalf("the replaced cooldown's seconds must be dropped, got %d", body.Anonymous.CoolingSeconds)
	}
}

// RestoreProxy must clear a block even when the node has no cooldown to clear.
// A diagnostic request (the WebUI debug page) can mark the exit blocked while
// observeAnonymousResult deliberately records no cooldown, and the old guard
// skipped exactly that node.
func TestRestoreProxyClearsBlockWithoutACooldown(t *testing.T) {
	g, node := breakerGateway(t)
	node.proxy.markBlocked(time.Now().Add(10 * time.Minute))
	if node.cooldownUntil.Load() != 0 || node.failures.Load() != 0 {
		t.Fatal("fixture must leave the node without cooldown evidence")
	}

	g.anonymous.RestoreProxy(node.proxy)

	if node.proxy.blocked(time.Now()) {
		t.Fatal("a restored proxy must be un-blocked even when it had no cooldown")
	}
}

// A rotating #pool entry gets a new exit IP on every attempt, so one abusive
// exit must not bench the whole entry — and must not open the lane breaker when
// it is the only entry.
func TestPoolEntryIsNotBlockedByIPAbuse(t *testing.T) {
	transports, err := transportPool([]string{"http://127.0.0.1:1080#pool"}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool := anonymousPool(true, transports, time.Second, anonymousBackoff{})
	pool.nodes[0].proxy.markBlocked(time.Now().Add(time.Hour))
	if pool.nodes[0].proxy.blocked(time.Now()) {
		t.Fatal("a #pool entry must never be blocked: its next attempt leaves from a different IP")
	}
	if got := nextFor(pool, "turn"); got == nil {
		t.Fatal("a #pool entry must stay in rotation")
	}
}
