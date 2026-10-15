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
	resp, _, err := g.doUpstream(ctx, route, map[config.Tier][]byte{config.TierZen: raw}, ids)
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

// TestSelectivePrestripFallsBackToFullKeepingBlacklist drives the in-turn
// fallback: a selective first attempt that is still rejected falls back to
// the full strip once, and the success must only move the window — the
// blacklist stays the pre-mark set, so healthy reasoning appended after the
// mark is never learned as poison.
func TestSelectivePrestripFallsBackToFullKeepingBlacklist(t *testing.T) {
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

	// rs_new2 arrived after the mark; it is healthy, but this turn is rejected
	// anyway so the fallback branch is the only way to succeed.
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
	deny, prev, _, ok := g.staleMemo.snapshot(ids.Session)
	if !ok || prev != "resp_old" {
		t.Fatalf("the remembered window must survive the fallback, got prev=%q ok=%v", prev, ok)
	}
	if len(deny) != 1 {
		t.Fatalf("the blacklist must stay the pre-mark set, got %v", deny)
	}
	if _, healthy := deny["rs_new2"]; healthy {
		t.Fatalf("healthy reasoning added after the mark must never be blacklisted, got %v", deny)
	}
	if _, still := deny["rs_old"]; !still {
		t.Fatalf("the original poisoned id must stay blacklisted, got %v", deny)
	}
	if !meta.Prestripped || meta.PrestripMode != "selective" {
		t.Fatalf("the usage page must see the first-attempt shape as selective, got %+v", meta)
	}
	if meta.PrestripIDs != 1 {
		t.Fatalf("the usage page must see the unchanged blacklist size, got %+v", meta)
	}
	if meta.Attempts != 2 {
		t.Fatalf("the fallback must be one extra round-trip, got %d attempts", meta.Attempts)
	}
}

// TestDiagnosticPoison400NeverWritesMemo locks the production-memory
// isolation: a diagnostic request still benefits from the in-turn self-heal
// (only memo access is skipped), but its 400 must leave no trace.
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
