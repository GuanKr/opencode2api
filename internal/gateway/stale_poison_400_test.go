package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/jsonutil"
	"opencode2api/internal/models"
	"opencode2api/internal/telemetry"
)

// The scripted-400 cases below reach the stale-reasoning self-heal, the
// in-turn fallback and the memo write/read path inside doUpstream — the
// branches a 200-only stub can never enter. They stay offline: see
// freezeProxyHealth for why a non-2xx stub reply is otherwise unsafe here.

// stale400Body is the upstream 400 the gateway classifies as poison. It names
// exactly one id ("rs_named", which is not even in the test bodies), which is
// precisely why the memo cannot key on the id the server mentions: it has to
// blacklist the whole reasoning set it finds in the failing body.
const stale400Body = `{"error":{"type":"invalid_request_error","message":"Referenced reasoning item 'rs_named' was not found or has expired."}}`

// plain400Body mentions reasoning but says nothing about a gone or expired
// reference, so the classification gate must stop the self-heal here.
const plain400Body = `{"error":{"type":"invalid_request_error","message":"unknown reasoning field 'foo'"}}`

// staleChain400Body is the upstream 400 for a forgotten CHAIN LINK rather
// than a forgotten reasoning item: the response the request still points at no
// longer exists. It matches the same classifier on the same terms ("reasoning
// item" plus "not found").
const staleChain400Body = `{"error":{"type":"invalid_request_error","message":"Referenced reasoning item for response 'resp_fresh' was not found or has expired."}}`

// poisonStub records every body the gateway transmits and answers with the
// status/body the test scripts for that attempt number.
type poisonStub struct {
	hits  atomic.Int64
	mu    sync.Mutex
	all   [][]byte
	reply func(attempt int64, body []byte) (int, string)
}

func (s *poisonStub) sent() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.all...)
}

// freezeProxyHealth parks every proxy's health-check flag so a non-2xx
// upstream reply cannot schedule the background verification. Without it a
// 400 makes syncProxyResult call verifyProxyAfterError, which dials
// proxyHealthCheckURL (the public internet) from a detached goroutine. That
// function starts with checking.CompareAndSwap(false, true) and returns
// immediately when the flag is already claimed, so parking it here is what
// makes these cases fully offline without changing any gateway behaviour.
func freezeProxyHealth(g *Gateway) {
	for _, proxy := range g.transports.items {
		proxy.checking.Store(true)
	}
}

func poisonStubGateway(t *testing.T, reply func(attempt int64, body []byte) (int, string)) (*Gateway, *poisonStub) {
	t.Helper()
	stub := &poisonStub{reply: reply}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		attempt := stub.hits.Add(1)
		stub.mu.Lock()
		stub.all = append(stub.all, append([]byte(nil), body...))
		stub.mu.Unlock()
		status, payload := stub.reply(attempt, body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(payload))
	}))
	t.Cleanup(server.Close)
	g := newStubGateway(t, server.URL)
	freezeProxyHealth(g)
	return g, stub
}

// hasReasoningItem reports whether a transmitted body still replays a
// reasoning item. It deliberately avoids *testing.T (the stub answers on the
// server goroutine), so a scripting helper can key the poison on the shape
// actually sent instead of on an attempt counter.
func hasReasoningItem(body []byte) bool {
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return false
	}
	for _, raw := range jsonutil.SliceAt(payload, "input") {
		if m, _ := raw.(map[string]any); m != nil && jsonutil.StringAt(m, "type") == "reasoning" {
			return true
		}
	}
	return false
}

// chainLink returns the previous_response_id a transmitted body still
// carries ("" when it has none), for the same server-goroutine reason as
// hasReasoningItem.
func chainLink(body []byte) string {
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	return jsonutil.StringAt(payload, "previous_response_id")
}

// poisonedInput builds a Responses turn carrying one reasoning item per id
// ("" omits the chain link).
func poisonedInput(prev string, ids ...string) map[string]any {
	input := []any{
		map[string]any{"type": "message", "role": "user",
			"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
	}
	for _, id := range ids {
		input = append(input, map[string]any{"type": "reasoning", "id": id,
			"summary": []any{map[string]any{"type": "summary_text", "text": "thinking " + id}}})
	}
	payload := map[string]any{"model": "probe-free", "input": input}
	if prev != "" {
		payload["previous_response_id"] = prev
	}
	return payload
}

// poisonTurn sends one turn through doUpstream and returns the status plus
// the response document. Response bodies are always drained and closed.
func poisonTurn(t *testing.T, ctx context.Context, g *Gateway, route models.Route, ids identity.RequestIDs, input map[string]any) (int, []byte) {
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
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}

// TestPoison400WritesMemoOnSelfHeal is the memo write path reached through a
// real upstream 400: the failing turn self-heals, and the ids/chain link of
// the failing body become this session's blacklist for later turns.
func TestPoison400WritesMemoOnSelfHeal(t *testing.T) {
	g, stub := poisonStubGateway(t, func(attempt int64, _ []byte) (int, string) {
		if attempt == 1 {
			return http.StatusBadRequest, stale400Body
		}
		return http.StatusOK, `{"ok":true}`
	})
	ids := identity.RequestIDs{Session: "ses_poison", Request: "req_poison"}
	meta := &telemetry.RequestMeta{}
	ctx := telemetry.WithRequestMeta(t.Context(), meta)

	status, _ := poisonTurn(t, ctx, g, staleTestRoute(), ids, poisonedInput("resp_old", "rs_old"))
	if status != http.StatusOK {
		t.Fatalf("the strip retry must answer 200, got %d", status)
	}
	if stub.hits.Load() != 2 {
		t.Fatalf("one rejected attempt plus one strip retry expected, got %d hits", stub.hits.Load())
	}
	sent := stub.sent()
	if got := reasoningIDs(t, sent[0]); len(got) != 1 || got[0] != "rs_old" {
		t.Fatalf("first attempt must carry the client's reasoning, got %v", got)
	}
	if got := reasoningIDs(t, sent[1]); len(got) != 0 {
		t.Fatalf("strip retry must carry no reasoning at all, got %v", got)
	}
	deny, prev, full, ok := g.staleMemo.snapshot(ids.Session)
	if !ok || full {
		t.Fatalf("the healed turn must be remembered, got full=%v ok=%v", full, ok)
	}
	if prev != "resp_old" {
		t.Fatalf("the dead chain link must be remembered, got %q", prev)
	}
	if len(deny) != 1 {
		t.Fatalf("every reasoning id of the failing body must be blacklisted, got %v", deny)
	}
	if _, blacklisted := deny["rs_old"]; !blacklisted {
		t.Fatalf("rs_old must be blacklisted, got %v", deny)
	}
	if meta.Prestripped {
		t.Fatalf("a self-healed turn was not pre-stripped, got %+v", meta)
	}
	if meta.PrestripIDs != 1 {
		t.Fatalf("the usage page must see the stored blacklist size, got %+v", meta)
	}
	if meta.Attempts != 2 {
		t.Fatalf("attempt numbering must continue across the retry, got %d", meta.Attempts)
	}
}

// TestSelectivePrestripFallsBackToFullLearningRejectedIDs drives the in-turn
// fallback: a selective first attempt that is still rejected falls back to
// the full strip once, and the success learns the id the refused body carried
// on top of the old blacklist — the rejection is the proof it is dead too, and
// without it the session would pay this same 400 + fallback every turn for
// the rest of the window. The recorded chain link is left alone: the refused
// body was stripped of it already, so it says nothing new about the link.
func TestSelectivePrestripFallsBackToFullLearningRejectedIDs(t *testing.T) {
	g, stub := poisonStubGateway(t, func(attempt int64, _ []byte) (int, string) {
		if attempt == 1 {
			// Reject the selectively pre-stripped first attempt so the turn has
			// to fall back; the stripped retry (attempt 2) is accepted.
			return http.StatusBadRequest, stale400Body
		}
		return http.StatusOK, `{"ok":true}`
	})
	ids := identity.RequestIDs{Session: "ses_fallback", Request: "req_fallback"}
	meta := &telemetry.RequestMeta{}
	ctx := telemetry.WithRequestMeta(t.Context(), meta)
	// An earlier turn established the poison: only rs_old existed back then.
	g.staleMemo.noteStripped(ids.Session, []string{"rs_old"}, "resp_old")

	// rs_new2 arrived after the mark, so the first attempt kept it — and the
	// upstream refused it anyway, which is what makes it poison rather than
	// healthy reasoning this turn. The full-strip fallback is the only way to
	// succeed.
	status, _ := poisonTurn(t, ctx, g, staleTestRoute(), ids, poisonedInput("resp_old", "rs_old", "rs_new2"))
	if status != http.StatusOK {
		t.Fatalf("the full-strip fallback must answer 200, got %d", status)
	}
	if stub.hits.Load() != 2 {
		t.Fatalf("selective attempt plus one fallback expected, got %d hits", stub.hits.Load())
	}
	sent := stub.sent()
	if got := reasoningIDs(t, sent[0]); len(got) != 1 || got[0] != "rs_new2" {
		t.Fatalf("first attempt must be selectively stripped (rs_old gone, rs_new2 kept), got %v", got)
	}
	if got := reasoningIDs(t, sent[1]); len(got) != 0 {
		t.Fatalf("the fallback must strip every reasoning id, got %v", got)
	}
	deny, prev, full, ok := g.staleMemo.snapshot(ids.Session)
	if !ok || full || prev != "resp_old" {
		t.Fatalf("the window and its recorded chain link must survive the fallback, got prev=%q full=%v ok=%v", prev, full, ok)
	}
	if len(deny) != 2 {
		t.Fatalf("the blacklist must hold the rejected id on top of the old one, got %v", deny)
	}
	if _, learned := deny["rs_new2"]; !learned {
		t.Fatalf("the id the upstream refused must be learned, got %v", deny)
	}
	if _, still := deny["rs_old"]; !still {
		t.Fatalf("the original poisoned id must stay blacklisted, got %v", deny)
	}
	if !meta.Prestripped || meta.PrestripMode != "selective" {
		t.Fatalf("the usage page must see the first-attempt shape as selective, got %+v", meta)
	}
	if meta.PrestripIDs != 2 {
		t.Fatalf("the usage page must see the merged blacklist size, got %+v", meta)
	}
	if meta.Attempts != 2 {
		t.Fatalf("the fallback must be one extra round-trip, got %d attempts", meta.Attempts)
	}
}

// TestSelectivePrestripLearnsRejectedIDsInOneRoundTrip is the end-to-end
// regression for the per-turn double send: the upstream moved on to a
// different reasoning id, so the old blacklist stopped covering the body.
// Turn 1 still heals (selective attempt rejected -> full-strip fallback), and
// that rejection is the evidence that learns the new id. Turn 2 must then
// strip both ids on its FIRST attempt, so the upstream is hit exactly once and
// never sees another 400. Offline: the stub rejects exactly the bodies that
// still replay reasoning, and freezeProxyHealth keeps the 400 from spawning
// the background verification dial.
func TestSelectivePrestripLearnsRejectedIDsInOneRoundTrip(t *testing.T) {
	g, stub := poisonStubGateway(t, func(_ int64, body []byte) (int, string) {
		if hasReasoningItem(body) {
			return http.StatusBadRequest, stale400Body
		}
		return http.StatusOK, `{"ok":true}`
	})
	ids := identity.RequestIDs{Session: "ses_relearn", Request: "req_relearn"}
	meta := &telemetry.RequestMeta{}
	ctx := telemetry.WithRequestMeta(t.Context(), meta)
	// An earlier turn blacklisted rs_old; the upstream has since dropped
	// rs_new too, which the memo cannot know before it is proven.
	g.staleMemo.noteStripped(ids.Session, []string{"rs_old"}, "resp_old")
	input := poisonedInput("resp_old", "rs_old", "rs_new")

	// Turn 1: the selective pre-strip drops only rs_old, the upstream still
	// refuses the body, and the full-strip fallback succeeds.
	if status, _ := poisonTurn(t, ctx, g, staleTestRoute(), ids, input); status != http.StatusOK {
		t.Fatalf("turn 1 must heal through the full-strip fallback, got %d", status)
	}
	if stub.hits.Load() != 2 {
		t.Fatalf("turn 1 must cost one 400 plus one fallback, got %d hits", stub.hits.Load())
	}
	sent := stub.sent()
	if got := reasoningIDs(t, sent[0]); len(got) != 1 || got[0] != "rs_new" {
		t.Fatalf("turn 1 first attempt must carry only the not-yet-blacklisted id, got %v", got)
	}
	if got := reasoningIDs(t, sent[1]); len(got) != 0 {
		t.Fatalf("turn 1 fallback must strip every reasoning id, got %v", got)
	}
	deny, prev, full, ok := g.staleMemo.snapshot(ids.Session)
	if !ok || full || prev != "resp_old" {
		t.Fatalf("turn 1 must keep the entry and its recorded chain link, got prev=%q full=%v ok=%v", prev, full, ok)
	}
	for _, want := range []string{"rs_old", "rs_new"} {
		if _, blacklisted := deny[want]; !blacklisted {
			t.Fatalf("turn 1 must learn %s into the blacklist, got %v", want, deny)
		}
	}
	if meta.PrestripIDs != 2 {
		t.Fatalf("the usage page must see the merged blacklist, got %+v", meta)
	}

	// Turn 2: same session, same client body. One pre-strip now removes both
	// dead ids, so the turn is answered by the first attempt — one round-trip,
	// no 400, no fallback.
	before := stub.hits.Load()
	if status, _ := poisonTurn(t, ctx, g, staleTestRoute(), ids, input); status != http.StatusOK {
		t.Fatalf("turn 2 must answer 200 on its first attempt, got %d", status)
	}
	if got := stub.hits.Load() - before; got != 1 {
		t.Fatalf("turn 2 must cost exactly one upstream round-trip, got %d", got)
	}
	sent = stub.sent()
	if got := reasoningIDs(t, sent[len(sent)-1]); len(got) != 0 {
		t.Fatalf("turn 2 must pre-strip both learned ids, got %v", got)
	}
	if !meta.Prestripped || meta.PrestripMode != "selective" || meta.PrestripIDs != 2 {
		t.Fatalf("turn 2 must report the learned selective pre-strip, got %+v", meta)
	}
	if !g.staleMemo.shouldPrestrip(ids.Session) {
		t.Fatal("the window must stay armed for later turns")
	}
}

// TestSelectivePrestripLearnsFreshDeadChainLinkInOneRoundTrip is the
// end-to-end regression for the fresh-but-dead chain link. The memo holds no
// dead link at all, and the selective strip never removes a link unless it
// equals the remembered dead one — so resp_fresh survives the first attempt
// and is refused exactly like the stale reasoning ids beside it. The full
// strip that finally succeeds removes it, which is the proof it is dead too;
// if that were not learned, the session would pay this 400 + fallback on every
// turn for the whole window. Turn 2 must therefore pre-strip both dead ids
// AND the dead link on its first attempt, hitting the upstream once.
// Offline: the stub refuses any body that still carries a reasoning item or a
// chain link (the client only ever sends the dead one), and freezeProxyHealth
// keeps the 400 from spawning the background verification dial.
func TestSelectivePrestripLearnsFreshDeadChainLinkInOneRoundTrip(t *testing.T) {
	g, stub := poisonStubGateway(t, func(_ int64, body []byte) (int, string) {
		if hasReasoningItem(body) || chainLink(body) != "" {
			return http.StatusBadRequest, staleChain400Body
		}
		return http.StatusOK, `{"ok":true}`
	})
	ids := identity.RequestIDs{Session: "ses_chain", Request: "req_chain"}
	meta := &telemetry.RequestMeta{}
	ctx := telemetry.WithRequestMeta(t.Context(), meta)
	// The earlier poison knew nothing about a chain link, and the client has
	// since moved on to a new one that the upstream has also lost.
	g.staleMemo.noteStripped(ids.Session, []string{"rs_old"}, "")
	if _, prev, _, ok := g.staleMemo.snapshot(ids.Session); !ok || prev != "" {
		t.Fatalf("the preset entry must have no dead chain link, got prev=%q ok=%v", prev, ok)
	}
	input := poisonedInput("resp_fresh", "rs_old", "rs_new")

	// Turn 1: the selective pre-strip drops rs_old but has no dead link to
	// match, so resp_fresh stays in the body and the upstream refuses it; only
	// the full-strip fallback succeeds.
	if status, _ := poisonTurn(t, ctx, g, staleTestRoute(), ids, input); status != http.StatusOK {
		t.Fatalf("turn 1 must heal through the full-strip fallback, got %d", status)
	}
	if stub.hits.Load() != 2 {
		t.Fatalf("turn 1 must cost one 400 plus one fallback, got %d hits", stub.hits.Load())
	}
	sent := stub.sent()
	if got := reasoningIDs(t, sent[0]); len(got) != 1 || got[0] != "rs_new" {
		t.Fatalf("turn 1 first attempt must keep only the not-yet-blacklisted id, got %v", got)
	}
	if got := chainLink(sent[0]); got != "resp_fresh" {
		t.Fatalf("turn 1 first attempt must still carry the unlearned link, got %q", got)
	}
	if got := reasoningIDs(t, sent[1]); len(got) != 0 {
		t.Fatalf("turn 1 fallback must strip every reasoning id, got %v", got)
	}
	if got := chainLink(sent[1]); got != "" {
		t.Fatalf("turn 1 fallback must strip the chain link, got %q", got)
	}
	deny, prev, full, ok := g.staleMemo.snapshot(ids.Session)
	if !ok || full {
		t.Fatalf("turn 1 must keep the entry in selective mode, got full=%v ok=%v", full, ok)
	}
	for _, want := range []string{"rs_old", "rs_new"} {
		if _, blacklisted := deny[want]; !blacklisted {
			t.Fatalf("turn 1 must learn %s into the blacklist, got %v", want, deny)
		}
	}
	if prev != "resp_fresh" {
		t.Fatalf("turn 1 must learn the refused chain link, got prev=%q", prev)
	}
	if meta.PrestripIDs != 2 {
		t.Fatalf("the usage page must see the merged blacklist, got %+v", meta)
	}

	// Turn 2: same session, same client body. One pre-strip now removes both
	// dead ids and the dead link, so the turn is answered by the first
	// attempt — one round-trip, no 400, no fallback.
	before := stub.hits.Load()
	if status, _ := poisonTurn(t, ctx, g, staleTestRoute(), ids, input); status != http.StatusOK {
		t.Fatalf("turn 2 must answer 200 on its first attempt, got %d", status)
	}
	if got := stub.hits.Load() - before; got != 1 {
		t.Fatalf("turn 2 must cost exactly one upstream round-trip, got %d", got)
	}
	sent = stub.sent()
	last := sent[len(sent)-1]
	if got := reasoningIDs(t, last); len(got) != 0 {
		t.Fatalf("turn 2 must pre-strip both dead ids, got %v", got)
	}
	if got := chainLink(last); got != "" {
		t.Fatalf("turn 2 must pre-strip the learned dead link, got %q", got)
	}
	if !meta.Prestripped || meta.PrestripMode != "selective" || meta.PrestripIDs != 2 {
		t.Fatalf("turn 2 must report the learned selective pre-strip, got %+v", meta)
	}
	if !g.staleMemo.shouldPrestrip(ids.Session) {
		t.Fatal("the window must stay armed for later turns")
	}
}

// TestDiagnosticPoison400NeverWritesMemo locks the production-memory
// isolation: a diagnostic request still benefits from the in-turn self-heal
// (only memo access is skipped), but its 400 must leave no trace — neither a
// new entry nor a merge into an existing one, even though its failing body
// would be exactly the evidence a production turn learns from.
func TestDiagnosticPoison400NeverWritesMemo(t *testing.T) {
	g, stub := poisonStubGateway(t, func(attempt int64, _ []byte) (int, string) {
		if attempt == 1 {
			return http.StatusBadRequest, stale400Body
		}
		return http.StatusOK, `{"ok":true}`
	})
	ids := identity.RequestIDs{Session: "ses_diag400", Request: "req_diag400"}
	status, _ := poisonTurn(t, WithDiagnosticRequest(t.Context()), g, staleTestRoute(), ids, poisonedInput("resp_old", "rs_old"))
	if status != http.StatusOK {
		t.Fatalf("a diagnostic turn must still self-heal in-turn, got %d", status)
	}
	if stub.hits.Load() != 2 {
		t.Fatalf("the in-turn self-heal must stay available to diagnostics, got %d hits", stub.hits.Load())
	}
	if _, _, _, ok := g.staleMemo.snapshot(ids.Session); ok {
		t.Fatal("a diagnostic 400 must not write production memo state")
	}
	if g.staleMemo.shouldPrestrip(ids.Session) {
		t.Fatal("a diagnostic 400 must not arm the pre-strip fast path")
	}

	// With an entry in place, a diagnostic turn carrying an id the production
	// path would merge must leave that entry exactly as it was.
	g.staleMemo.noteStripped(ids.Session, []string{"rs_old"}, "resp_old")
	if status, _ := poisonTurn(t, WithDiagnosticRequest(t.Context()), g, staleTestRoute(), ids, poisonedInput("resp_old", "rs_old", "rs_new")); status != http.StatusOK {
		t.Fatalf("a diagnostic turn with an existing entry must still self-heal, got %d", status)
	}
	deny, prev, full, ok := g.staleMemo.snapshot(ids.Session)
	if !ok || full || prev != "resp_old" {
		t.Fatalf("a diagnostic turn must leave the entry intact, got prev=%q full=%v ok=%v", prev, full, ok)
	}
	if len(deny) != 1 {
		t.Fatalf("a diagnostic turn must never learn ids into the blacklist, got %v", deny)
	}
	if _, learned := deny["rs_new"]; learned {
		t.Fatalf("a diagnostic turn must never merge rejected ids, got %v", deny)
	}
}

// TestNonStale400IsNeitherHealedNorRemembered pins the classification gate:
// a 400 that only mentions reasoning (no gone/expired reference) is returned
// to the caller untouched, with no retry and nothing written to the memo.
func TestNonStale400IsNeitherHealedNorRemembered(t *testing.T) {
	g, stub := poisonStubGateway(t, func(int64, []byte) (int, string) {
		return http.StatusBadRequest, plain400Body
	})
	ids := identity.RequestIDs{Session: "ses_plain400", Request: "req_plain400"}
	meta := &telemetry.RequestMeta{}
	ctx := telemetry.WithRequestMeta(t.Context(), meta)

	status, body := poisonTurn(t, ctx, g, staleTestRoute(), ids, poisonedInput("resp_old", "rs_old"))
	if status != http.StatusBadRequest {
		t.Fatalf("the original 400 must reach the caller, got %d", status)
	}
	if string(body) != plain400Body {
		t.Fatalf("the upstream error document must survive intact, got %q", body)
	}
	if stub.hits.Load() != 1 {
		t.Fatalf("a non-stale 400 must not trigger a strip retry, got %d hits", stub.hits.Load())
	}
	if got := reasoningIDs(t, stub.sent()[0]); len(got) != 1 || got[0] != "rs_old" {
		t.Fatalf("the request must go out untouched, got %v", got)
	}
	if _, _, _, ok := g.staleMemo.snapshot(ids.Session); ok {
		t.Fatal("a non-stale 400 must not arm the poison memory")
	}
	if meta.Prestripped || meta.PrestripIDs != 0 {
		t.Fatalf("a non-stale 400 must stay unmarked, got %+v", meta)
	}
}
