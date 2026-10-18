package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/jsonutil"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/telemetry"
)

func TestStaleMemoPrestripLifecycle(t *testing.T) {
	var memo staleMemo
	if memo.shouldPrestrip("ses_x") {
		t.Fatal("unknown session must not pre-strip")
	}
	if memo.shouldPrestrip("") {
		t.Fatal("empty session must not pre-strip")
	}
	var nilMemo *staleMemo
	if nilMemo.shouldPrestrip("ses_x") {
		t.Fatal("nil memo must not pre-strip")
	}
	if _, _, _, ok := nilMemo.snapshot("ses_x"); ok {
		t.Fatal("nil memo must have no snapshot")
	}
	nilMemo.noteStripped("ses_x", []string{"rs_a"}, "resp_old")
	nilMemo.refresh("ses_x")
	nilMemo.noteClean("ses_x")

	memo.noteStripped("ses_x", []string{"rs_a", "rs_b"}, "resp_old")
	if !memo.shouldPrestrip("ses_x") {
		t.Fatal("noted session must pre-strip")
	}
	ids, prev, full, ok := memo.snapshot("ses_x")
	if !ok || full {
		t.Fatal("snapshot must exist and not be full mode")
	}
	if prev != "resp_old" || len(ids) != 2 {
		t.Fatalf("snapshot must carry stripped ids and chain link, got ids=%v prev=%q", ids, prev)
	}
	for _, want := range []string{"rs_a", "rs_b"} {
		if _, ok := ids[want]; !ok {
			t.Fatalf("snapshot must contain %s, got %v", want, ids)
		}
	}
	// Mutating the returned copy must not affect memo state.
	delete(ids, "rs_a")
	ids["rs_evil"] = struct{}{}
	ids2, _, _, _ := memo.snapshot("ses_x")
	if len(ids2) != 2 {
		t.Fatalf("snapshot copy mutation must not affect memo, got %v", ids2)
	}
	if _, ok := ids2["rs_evil"]; ok {
		t.Fatalf("snapshot copy mutation must not affect memo, got %v", ids2)
	}
	for _, want := range []string{"rs_a", "rs_b"} {
		if _, ok := ids2[want]; !ok {
			t.Fatalf("snapshot copy mutation must not affect memo, got %v", ids2)
		}
	}
	memo.noteClean("ses_x")
	if memo.shouldPrestrip("ses_x") {
		t.Fatal("cleaned session must not pre-strip")
	}
	if _, _, _, ok := memo.snapshot("ses_x"); ok {
		t.Fatal("cleaned session must have no snapshot")
	}
}

func TestStaleMemoExpiryReleasesEntry(t *testing.T) {
	memo := staleMemo{ttl: time.Hour}
	memo.noteStripped("ses_x", []string{"rs_old"}, "resp_old")
	if !memo.shouldPrestrip("ses_x") {
		t.Fatal("fresh entry must pre-strip")
	}
	// Backdate past the TTL instead of sleeping: deterministic and fast.
	memo.mu.Lock()
	memo.last["ses_x"].ts = time.Now().Add(-2 * time.Hour)
	memo.mu.Unlock()
	if memo.shouldPrestrip("ses_x") {
		t.Fatal("expired entry must re-probe with the full shape")
	}
	// Idle past the TTL, the entry is lazily released (not retained): the
	// next touch frees the memory, and a still-poisoned turn re-learns
	// with one 400 instead of reusing a stale blacklist.
	if _, _, _, ok := memo.snapshot("ses_x"); ok {
		t.Fatal("expired entry must be released, not retained")
	}
	// Refreshing within the window still moves it forward untouched.
	memo.noteStripped("ses_x", []string{"rs_old"}, "resp_old")
	memo.refresh("ses_x")
	if !memo.shouldPrestrip("ses_x") {
		t.Fatal("refreshed entry must pre-strip again")
	}
	if ids2, prev2, _, ok := memo.snapshot("ses_x"); !ok || prev2 != "resp_old" || len(ids2) != 1 {
		t.Fatalf("refresh must keep the snapshot, got ids=%v prev=%q ok=%v", ids2, prev2, ok)
	}
}

// TestExpiryReprobeFailureRelearns locks the doUpstream expiry branch
// without a failing upstream (a real 400 would trigger the background proxy
// health probe, which dials the public internet and is off-limits): past the
// TTL the memo no longer remembers the session, so the failure path takes
// the first-poison branch (snapshot the current failing body) instead of
// refreshing an old blacklist.
func TestExpiryReprobeFailureRelearns(t *testing.T) {
	memo := staleMemo{ttl: time.Hour}
	memo.noteStripped("ses_exp", []string{"rs_old"}, "resp_old")
	memo.mu.Lock()
	memo.last["ses_exp"].ts = time.Now().Add(-2 * time.Hour)
	memo.mu.Unlock()
	if memo.shouldPrestrip("ses_exp") {
		t.Fatal("expired entry must re-probe with the full shape")
	}
	// Mirror of the doUpstream decision: hadOld -> refresh, else snapshot.
	if _, _, _, hadOld := memo.snapshot("ses_exp"); hadOld {
		t.Fatal("expired entry must be released before the failure path runs")
	}
	memo.noteStripped("ses_exp", []string{"rs_new"}, "resp_new")
	ids, prev, full, ok := memo.snapshot("ses_exp")
	if !ok || full || prev != "resp_new" || len(ids) != 1 {
		t.Fatalf("re-learn must snapshot the current failing body, got ids=%v prev=%q full=%v ok=%v", ids, prev, full, ok)
	}
	if _, ok := ids["rs_new"]; !ok {
		t.Fatalf("re-learn must snapshot the current failing body, got %v", ids)
	}
	if !memo.shouldPrestrip("ses_exp") {
		t.Fatal("re-learned entry must pre-strip again")
	}
}

// TestSelectiveFallbackMergesRejectedIDs locks the in-turn fallback branch
// without a failing upstream: a selective pre-strip that still 400s falls
// back to a full strip once, and the success learns what the refused body
// carried — its reasoning ids (that rejection is the proof rs_new is dead
// too) and its chain link (the selective pass keeps any link that is not the
// remembered dead one, so a fresh link survives into the refused body and only
// the full strip removes it).
func TestSelectiveFallbackMergesRejectedIDs(t *testing.T) {
	var memo staleMemo
	memo.noteStripped("ses_fb", []string{"rs_old"}, "resp_old")
	r := testSelectiveRoute()
	deny, deadPrev, full, ok := memo.snapshot("ses_fb")
	if !ok || full {
		t.Fatal("snapshot must exist and not be full mode")
	}
	fullBody := mustStaleBody(t, "resp_fresh",
		[]string{"rs_old", "rs_new"},
	)
	sel, changed := stripSelectiveReasoningInputs(r, map[config.Tier][]byte{config.TierZen: fullBody}, deny, deadPrev)
	if !changed {
		t.Fatal("selective strip must remove the blacklisted id")
	}
	if got := reasoningIDs(t, sel[config.TierZen]); len(got) != 1 || got[0] != "rs_new" {
		t.Fatalf("selective strip must keep healthy rs_new, got %v", got)
	}
	// Simulate the still-400: the full-strip fallback must drop everything,
	// the reasoning ids and the chain link alike.
	fullStripped, changed := stripStaleReasoningInputs(r, sel)
	if !changed {
		t.Fatal("fallback full strip must change the selective body")
	}
	if got := reasoningIDs(t, fullStripped[config.TierZen]); len(got) != 0 {
		t.Fatalf("fallback full strip must drop all reasoning, got %v", got)
	}
	// Branch semantic under test: what is learned comes from the refused
	// body (what the selective strip produced), NOT from the client's larger
	// body — healthy rs_new is only ever learned because the upstream rejected
	// it, and the fresh link is only ever learned because it survived into
	// the refused body and the full strip removed it.
	rejectedIDs, rejectedPrev := collectStaleReasoningIDs(sel[config.TierZen])
	if len(rejectedIDs) != 1 || rejectedIDs[0] != "rs_new" {
		t.Fatalf("the refused body must yield only the un-blacklisted id, got ids=%v prev=%q", rejectedIDs, rejectedPrev)
	}
	if rejectedPrev != "resp_fresh" {
		t.Fatalf("the refused body must still carry its fresh link, got %q", rejectedPrev)
	}
	memo.mergeRejected("ses_fb", rejectedIDs, rejectedPrev)
	ids2, prev2, full2, ok := memo.snapshot("ses_fb")
	if !ok || full2 {
		t.Fatalf("the entry must survive the fallback, got full=%v ok=%v", full2, ok)
	}
	if len(ids2) != 2 {
		t.Fatalf("the rejected id must be learned into the blacklist, got ids=%v", ids2)
	}
	for _, want := range []string{"rs_old", "rs_new"} {
		if _, ok := ids2[want]; !ok {
			t.Fatalf("the blacklist must hold %s, got %v", want, ids2)
		}
	}
	// The freshly issued link is what the refused body carried and what the
	// full strip removed, so it replaces the recorded one: a client chain only
	// moves forward, so the link replaced here can never come back.
	if prev2 != "resp_fresh" {
		t.Fatalf("the refused body must be recorded as the dead chain link, got %q", prev2)
	}
	// The next turn's selective strip now removes both ids AND that link, so
	// the refused body shape is gone and the 400 + fallback cannot repeat.
	sel2, changed := stripSelectiveReasoningInputs(r, map[config.Tier][]byte{config.TierZen: fullBody}, ids2, prev2)
	if !changed {
		t.Fatal("the merged blacklist must still strip something")
	}
	if got := reasoningIDs(t, sel2[config.TierZen]); len(got) != 0 {
		t.Fatalf("one pre-strip must now clear every dead id, got %v", got)
	}
	var afterSel map[string]any
	if err := json.Unmarshal(sel2[config.TierZen], &afterSel); err != nil {
		t.Fatal(err)
	}
	if _, still := afterSel["previous_response_id"]; still {
		t.Fatalf("one pre-strip must now drop the dead link too: %s", sel2[config.TierZen])
	}
	// Merging again is a no-op (no duplicates, no link churn, window still
	// moving). Once the link is recorded the selective pass strips it, so the
	// next refused body has none and cannot disturb the recorded one.
	memo.mergeRejected("ses_fb", rejectedIDs, "")
	ids3, prev3, _, _ := memo.snapshot("ses_fb")
	if len(ids3) != 2 {
		t.Fatalf("re-merging the same ids must not duplicate, got %v", ids3)
	}
	if prev3 != "resp_fresh" {
		t.Fatalf("an empty link must not disturb the recorded one, got %q", prev3)
	}
}

// TestMergeRejectedRespectsIDCap locks the memory bound: merging never grows
// a session past staleMemoMaxIDsPerSession and never flips the entry into
// full-strip mode, because a rejection says nothing about the snapshot being
// oversized.
func TestMergeRejectedRespectsIDCap(t *testing.T) {
	var memo staleMemo
	// Seed one below the cap so the merge has exactly one slot of headroom:
	// everything past that slot must be refused.
	big := make([]string, 0, staleMemoMaxIDsPerSession)
	for i := 0; i < staleMemoMaxIDsPerSession-1; i++ {
		big = append(big, fmt.Sprintf("rs_big_%04d", i))
	}
	memo.noteStripped("ses_cap", big, "resp_old")
	extra := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		extra = append(extra, fmt.Sprintf("rs_extra_%04d", i))
	}
	memo.mergeRejected("ses_cap", extra, "")
	ids, prev, full, ok := memo.snapshot("ses_cap")
	if !ok || full {
		t.Fatalf("the entry must survive a capped merge in selective mode, got full=%v ok=%v", full, ok)
	}
	if len(ids) != staleMemoMaxIDsPerSession {
		t.Fatalf("merge must stop at the per-session cap, got %d ids", len(ids))
	}
	if prev != "resp_old" {
		t.Fatalf("merge must not touch the chain link, got %q", prev)
	}
	if _, added := ids["rs_extra_0000"]; !added {
		t.Fatal("ids must be merged in order until the cap is reached")
	}
	if _, over := ids["rs_extra_0001"]; over {
		t.Fatal("ids past the cap must not be stored")
	}
	if !memo.shouldPrestrip("ses_cap") {
		t.Fatal("a capped merge must still leave the session pre-stripping")
	}
}

func TestStaleMemoOversizedSnapshotFallsBackToFull(t *testing.T) {
	var memo staleMemo
	big := make([]string, 0, staleMemoMaxIDsPerSession+10)
	for i := 0; i < staleMemoMaxIDsPerSession+10; i++ {
		big = append(big, fmt.Sprintf("rs_big_%04d", i))
	}
	memo.noteStripped("ses_big", big, "resp_old")
	if !memo.shouldPrestrip("ses_big") {
		t.Fatal("oversized entry must still pre-strip")
	}
	ids, _, full, ok := memo.snapshot("ses_big")
	if !ok || !full {
		t.Fatal("oversized snapshot must degrade to full-strip mode")
	}
	if len(ids) != 0 {
		t.Fatalf("full-mode snapshot must not retain ids, got %d", len(ids))
	}
}

func TestStaleMemoEvictsOldestHalf(t *testing.T) {
	memo := staleMemo{ttl: time.Hour}
	for i := 0; i < staleMemoMaxSessions; i++ {
		memo.noteStripped(fmt.Sprintf("ses_ev_%04d", i), []string{"rs_old"}, "")
	}
	// Fill to the cap with distinct sessions; the next note must evict.
	memo.noteStripped("ses_newest", []string{"rs_new"}, "")
	if got := len(memo.last); got > staleMemoMaxSessions/2+1 {
		t.Fatalf("eviction must drop back to about half, got %d entries", got)
	}
	if !memo.shouldPrestrip("ses_newest") {
		t.Fatal("newest entry must survive eviction")
	}
}

// TestRememberStaleHistoryWritePath locks the memo write branch of the
// self-heal: the first poison snapshots the failing body's ids and chain
// link, while later turns that carry NO evidence that the blacklist is
// incomplete (no pre-strip, or a full one) refresh the window instead of
// blacklisting the new ids.
func TestRememberStaleHistoryWritePath(t *testing.T) {
	g, _, _, _ := staleStubGateway(t)
	failing := mustStaleBody(t, "resp_old", []string{"rs_old", "rs_dup", "rs_dup"})

	g.rememberStaleHistory("ses_write", failing, "")
	ids, prev, full, ok := g.staleMemo.snapshot("ses_write")
	if !ok || full || prev != "resp_old" || len(ids) != 2 {
		t.Fatalf("first poison must snapshot the failing body, got ids=%v prev=%q full=%v ok=%v", ids, prev, full, ok)
	}
	if _, ok := ids["rs_old"]; !ok {
		t.Fatalf("first poison must blacklist the failing id, got %v", ids)
	}
	if !g.staleMemo.shouldPrestrip("ses_write") {
		t.Fatal("a written entry must pre-strip on the next turn")
	}

	// Same session, later turn, bigger failing body: rs_new is healthy
	// reasoning appended inside the window and must not be blacklisted.
	g.rememberStaleHistory("ses_write", mustStaleBody(t, "resp_old", []string{"rs_old", "rs_new"}), "")
	ids, prev, _, ok = g.staleMemo.snapshot("ses_write")
	if !ok || prev != "resp_old" || len(ids) != 2 {
		t.Fatalf("repeat poison must keep the old blacklist, got ids=%v prev=%q ok=%v", ids, prev, ok)
	}
	if _, blacklisted := ids["rs_new"]; blacklisted {
		t.Fatalf("healthy id appended inside the window must not be blacklisted, got %v", ids)
	}

	// A full pre-strip is not evidence either: it removes everything, so a
	// rejection afterwards says nothing about which ids are dead.
	g.rememberStaleHistory("ses_write", mustStaleBody(t, "resp_old", []string{"rs_old", "rs_new2"}), "full")
	ids, prev, full, ok = g.staleMemo.snapshot("ses_write")
	if !ok || full || prev != "resp_old" || len(ids) != 2 {
		t.Fatalf("a full pre-strip failure must keep the old blacklist, got ids=%v prev=%q full=%v ok=%v", ids, prev, full, ok)
	}
	if _, blacklisted := ids["rs_new2"]; blacklisted {
		t.Fatalf("a full pre-strip failure must not blacklist anything new, got %v", ids)
	}

	// noteClean (a clean turn) forgets the session, so the next poison is a
	// first poison again and re-snapshots the current body.
	g.staleMemo.noteClean("ses_write")
	if g.staleMemo.shouldPrestrip("ses_write") {
		t.Fatal("cleaned session must not pre-strip")
	}
	g.rememberStaleHistory("ses_write", mustStaleBody(t, "resp_fresh", []string{"rs_new"}), "")
	ids, prev, _, ok = g.staleMemo.snapshot("ses_write")
	if !ok || prev != "resp_fresh" || len(ids) != 1 {
		t.Fatalf("after a clean turn the next poison must re-snapshot, got ids=%v prev=%q ok=%v", ids, prev, ok)
	}
	if _, ok := ids["rs_new"]; !ok {
		t.Fatalf("re-learn must blacklist the current failing id, got %v", ids)
	}
}

// staleStubGateway builds a Gateway over a loopback 200 stub. The stub
// always answers 200, so no proxy health re-probe (and no real network)
// is involved: any non-2xx upstream response would spawn the background
// verifyProxyAfterError dial to the public internet.
func staleStubGateway(t *testing.T) (*Gateway, *sync.Mutex, *[]byte, *atomic.Int64) {
	t.Helper()
	var mu sync.Mutex
	var lastBody []byte
	var hits atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		lastBody = body
		mu.Unlock()
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(stub.Close)
	return newStubGateway(t, stub.URL), &mu, &lastBody, &hits
}

// newStubGateway wires a Gateway at baseURL. The anonymous lane talks to
// upstream.anonymous (the free-tier base), not upstream.zen, so all three
// bases point at the stub and a key-tier fallback would also land on it.
func newStubGateway(t *testing.T, baseURL string) *Gateway {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	transports, err := newTransportPool([]string{"direct"}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	zenNodes, err := newNodePool(nil, transports, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	goNodes, err := newNodePool(nil, transports, time.Second)
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
		anonymous:  newAnonymousPool(true, transports, time.Second),
		catalog:    models.NewCatalog(config.TierZen, nil),
		monitor:    telemetry.NewMonitor(),
	}
}

func staleTestRoute() models.Route {
	return models.Route{
		ID: "probe-free", Tier: config.TierZen, Protocol: wire.Responses,
		Protocols: map[config.Tier]wire.Protocol{config.TierZen: wire.Responses},
		Anonymous: true,
	}
}

// doStaleTurn sends one turn through doUpstream and returns the status plus
// a copy of the transmitted body. Response bodies are always drained.
func doStaleTurn(t *testing.T, ctx context.Context, g *Gateway, route models.Route, ids identity.RequestIDs, input map[string]any, lastBody *[]byte, mu *sync.Mutex) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	resp, _, err := g.doUpstream(ctx, route, map[config.Tier][]byte{config.TierZen: raw}, ids, "")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	mu.Lock()
	defer mu.Unlock()
	return resp.StatusCode, append([]byte(nil), (*lastBody)...)
}

// The doUpstream memo linkage, covered without any failing upstream (a
// failing upstream would trigger the background proxy health probe, which
// dials the public internet and is therefore off-limits to unit tests):
// a seeded memo is cleared by a clean first attempt, and kept when the
// attempt went out pre-stripped.
func TestDoUpstreamMemoLinkage(t *testing.T) {
	g, mu, lastBody, hits := staleStubGateway(t)
	route := staleTestRoute()
	ids := identity.RequestIDs{Session: "ses_link", Request: "req_link"}
	cleanInput := map[string]any{
		"model": "probe-free",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
		},
	}

	// A seeded memo is cleared by a clean first attempt (nothing blacklisted
	// present, so the fast path stays off and the 200 proves the history is
	// fine).
	g.staleMemo.noteStripped(ids.Session, []string{"rs_old"}, "resp_old")
	if status, _ := doStaleTurn(t, t.Context(), g, route, ids, cleanInput, lastBody, mu); status != http.StatusOK {
		t.Fatalf("clean attempt must succeed, got %d", status)
	}
	if status, _ := doStaleTurn(t, t.Context(), g, route, ids, cleanInput, lastBody, mu); status != http.StatusOK {
		t.Fatalf("clean attempt must succeed, got %d", status)
	}
	if hits.Load() != 2 {
		t.Fatalf("clean turns must each send exactly one attempt, got %d hits", hits.Load())
	}
	if g.staleMemo.shouldPrestrip(ids.Session) {
		t.Fatal("clean first attempt must clear the poison memory")
	}

	// A poisoned history goes out selectively pre-stripped (only the
	// blacklisted id removed, healthy reasoning kept) and the memory
	// survives the success (the stored history still carries the dead
	// references).
	g.staleMemo.noteStripped(ids.Session, []string{"rs_old"}, "resp_old")
	poisoned := map[string]any{
		"model": "probe-free",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "reasoning", "id": "rs_old",
				"summary": []any{map[string]any{"type": "summary_text", "text": "old"}}},
			map[string]any{"type": "reasoning", "id": "rs_new",
				"summary": []any{map[string]any{"type": "summary_text", "text": "new healthy"}}},
		},
	}
	if status, _ := doStaleTurn(t, t.Context(), g, route, ids, poisoned, lastBody, mu); status != http.StatusOK {
		t.Fatalf("pre-stripped attempt must succeed, got %d", status)
	}
	if hits.Load() != 3 {
		t.Fatalf("pre-stripped turn must send exactly one attempt, got %d hits", hits.Load())
	}
	sentIDs := reasoningIDs(t, *lastBody)
	for _, id := range sentIDs {
		if id == "rs_old" {
			t.Fatalf("selectively pre-stripped attempt must not carry blacklisted rs_old: %s", *lastBody)
		}
	}
	keepNew := false
	for _, id := range sentIDs {
		if id == "rs_new" {
			keepNew = true
		}
	}
	if !keepNew {
		t.Fatalf("selectively pre-stripped attempt must keep healthy rs_new: %s", *lastBody)
	}
	if !g.staleMemo.shouldPrestrip(ids.Session) {
		t.Fatal("pre-stripped success must keep the poison memory for later turns")
	}

	// A fresh chain link issued after the mark must survive the selective
	// pre-strip while the blacklisted id is still removed.
	withFreshLink := map[string]any{
		"model":                "probe-free",
		"previous_response_id": "resp_fresh",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "reasoning", "id": "rs_old",
				"summary": []any{map[string]any{"type": "summary_text", "text": "old"}}},
			map[string]any{"type": "reasoning", "id": "rs_new2",
				"summary": []any{map[string]any{"type": "summary_text", "text": "newer"}}},
		},
	}
	if status, _ := doStaleTurn(t, t.Context(), g, route, ids, withFreshLink, lastBody, mu); status != http.StatusOK {
		t.Fatalf("pre-stripped attempt must succeed, got %d", status)
	}
	var sentPayload map[string]any
	if err := json.Unmarshal(*lastBody, &sentPayload); err != nil {
		t.Fatal(err)
	}
	if got := jsonutil.StringAt(sentPayload, "previous_response_id"); got != "resp_fresh" {
		t.Fatalf("fresh chain link must be preserved, got %q (%s)", got, *lastBody)
	}
	for _, id := range reasoningIDs(t, *lastBody) {
		if id == "rs_old" {
			t.Fatalf("selectively pre-stripped attempt must not carry blacklisted rs_old: %s", *lastBody)
		}
	}
}

// TestPrestripHappensBeforeAnyUpstreamCall locks the read-path contract: the
// pre-strip must be applied to the outgoing bodies, so the very first
// upstream attempt already carries the stripped shape (nothing is sent
// first and stripped afterwards). The single recorded hit plus the
// blacklisted id being absent from it is the proof.
func TestPrestripHappensBeforeAnyUpstreamCall(t *testing.T) {
	g, mu, lastBody, hits := staleStubGateway(t)
	route := staleTestRoute()
	ids := identity.RequestIDs{Session: "ses_pre", Request: "req_pre"}
	g.staleMemo.noteStripped(ids.Session, []string{"rs_old"}, "resp_old")
	poisoned := map[string]any{
		"model": "probe-free",
		"input": []any{
			map[string]any{"type": "reasoning", "id": "rs_old",
				"summary": []any{map[string]any{"type": "summary_text", "text": "old"}}},
			map[string]any{"type": "reasoning", "id": "rs_new",
				"summary": []any{map[string]any{"type": "summary_text", "text": "new"}}},
		},
	}
	if status, _ := doStaleTurn(t, t.Context(), g, route, ids, poisoned, lastBody, mu); status != http.StatusOK {
		t.Fatalf("pre-stripped attempt must succeed, got %d", status)
	}
	if hits.Load() != 1 {
		t.Fatalf("pre-strip must avoid a second 400 round-trip, got %d hits", hits.Load())
	}
	if got := reasoningIDs(t, *lastBody); len(got) != 1 || got[0] != "rs_new" {
		t.Fatalf("first attempt must already be selectively stripped, got %v", got)
	}
}

// TestSelectivePrestripReportsBlacklistSize locks the telemetry plumbing:
// a selective pre-strip publishes both the mode and the stored blacklist
// size for the usage page.
func TestSelectivePrestripReportsBlacklistSize(t *testing.T) {
	g, mu, lastBody, _ := staleStubGateway(t)
	route := staleTestRoute()
	ids := identity.RequestIDs{Session: "ses_meta", Request: "req_meta"}
	g.staleMemo.noteStripped(ids.Session, []string{"rs_old", "rs_old2"}, "resp_old")
	meta := &telemetry.RequestMeta{}
	ctx := telemetry.WithRequestMeta(t.Context(), meta)
	poisoned := map[string]any{
		"model": "probe-free",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "reasoning", "id": "rs_old",
				"summary": []any{map[string]any{"type": "summary_text", "text": "old"}}},
			map[string]any{"type": "reasoning", "id": "rs_new",
				"summary": []any{map[string]any{"type": "summary_text", "text": "new"}}},
		},
	}
	if status, _ := doStaleTurn(t, ctx, g, route, ids, poisoned, lastBody, mu); status != http.StatusOK {
		t.Fatalf("pre-stripped attempt must succeed, got %d", status)
	}
	if !meta.Prestripped || meta.PrestripMode != "selective" {
		t.Fatalf("meta must mark selective prestrip, got %+v", meta)
	}
	if meta.PrestripIDs != 2 {
		t.Fatalf("meta must report the stored blacklist size, got %+v", meta)
	}
}

// TestOversizedSnapshotUsesFullPrestrip covers the full-mode branch of the
// pre-strip fast path: an oversized blacklist degrades to removing every
// reasoning reference, including healthy ones.
func TestOversizedSnapshotUsesFullPrestrip(t *testing.T) {
	g, mu, lastBody, _ := staleStubGateway(t)
	route := staleTestRoute()
	ids := identity.RequestIDs{Session: "ses_big", Request: "req_big"}
	big := make([]string, 0, staleMemoMaxIDsPerSession+1)
	for i := 0; i < staleMemoMaxIDsPerSession+1; i++ {
		big = append(big, fmt.Sprintf("rs_big_%04d", i))
	}
	g.staleMemo.noteStripped(ids.Session, big, "resp_old")
	if _, _, full, ok := g.staleMemo.snapshot(ids.Session); !ok || !full {
		t.Fatal("oversized snapshot must degrade to full-strip mode")
	}
	poisoned := map[string]any{
		"model": "probe-free",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "reasoning", "id": "rs_old",
				"summary": []any{map[string]any{"type": "summary_text", "text": "old"}}},
			map[string]any{"type": "reasoning", "id": "rs_new",
				"summary": []any{map[string]any{"type": "summary_text", "text": "new"}}},
		},
	}
	meta := &telemetry.RequestMeta{}
	ctx := telemetry.WithRequestMeta(t.Context(), meta)
	if status, _ := doStaleTurn(t, ctx, g, route, ids, poisoned, lastBody, mu); status != http.StatusOK {
		t.Fatalf("full pre-stripped attempt must succeed, got %d", status)
	}
	if got := reasoningIDs(t, *lastBody); len(got) != 0 {
		t.Fatalf("full pre-strip must drop all reasoning, got %v (%s)", got, *lastBody)
	}
	if !meta.Prestripped || meta.PrestripMode != "full" {
		t.Fatalf("meta must mark full prestrip, got %+v", meta)
	}
}

// TestDiagnosticBypassesMemo locks the production-memory isolation: a
// diagnostic request sees the real (poisoned) shape and neither clears nor
// records the memo, even on success.
func TestDiagnosticBypassesMemo(t *testing.T) {
	g, mu, lastBody, _ := staleStubGateway(t)
	route := staleTestRoute()
	ids := identity.RequestIDs{Session: "ses_diag", Request: "req_diag"}
	poisoned := map[string]any{
		"model": "probe-free",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "reasoning", "id": "rs_old",
				"summary": []any{map[string]any{"type": "summary_text", "text": "old"}}},
		},
	}
	g.staleMemo.noteStripped(ids.Session, []string{"rs_old"}, "resp_old")
	ctx := WithDiagnosticRequest(t.Context())
	if status, sent := doStaleTurn(t, ctx, g, route, ids, poisoned, lastBody, mu); status != http.StatusOK {
		t.Fatalf("diagnostic attempt must succeed, got %d", status)
	} else {
		keepPoison := false
		for _, id := range reasoningIDs(t, sent) {
			if id == "rs_old" {
				keepPoison = true
			}
		}
		if !keepPoison {
			t.Fatalf("diagnostic must exercise the real shape, got %s", sent)
		}
	}
	if !g.staleMemo.shouldPrestrip(ids.Session) {
		t.Fatal("diagnostic success must keep the production poison memory")
	}
	cleanInput := map[string]any{
		"model": "probe-free",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
		},
	}
	if status, _ := doStaleTurn(t, ctx, g, route, ids, cleanInput, lastBody, mu); status != http.StatusOK {
		t.Fatalf("diagnostic clean attempt must succeed, got %d", status)
	}
	if !g.staleMemo.shouldPrestrip(ids.Session) {
		t.Fatal("diagnostic clean success must not clear the production poison memory")
	}
}
