package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
)

// The background free-model availability probe (probe.go: probeFreeModel) must
// mint a fresh session id and prompt nonce per request, for the same reason the
// watchdog probe must: the free tier scopes part of its allowance to the
// session id, so an exhausted id makes every later check answer 429 on healthy
// exits and, carried to a fresh exit, zeroes that exit's allowance too. A
// stable id also pinned the check to one exit, because CursorFor keeps a
// non-empty affinity on a single node. Reverting either the per-attempt session
// id or the prompt nonce turns this red.
func TestFreeModelAvailabilityProbeSendsAFreshSessionAndPromptEveryTime(t *testing.T) {
	type captured struct {
		session, child, body string
	}
	var mu sync.Mutex
	var seen []captured
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		seen = append(seen, captured{
			session: r.Header.Get("x-opencode-session"),
			child:   r.Header.Get("x-opencode-session-id"),
			body:    string(body),
		})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","choices":[]}`))
	}))
	t.Cleanup(server.Close)
	g := probeGateway(t, server.URL)

	for i := 0; i < 2; i++ {
		result := g.probeFreeModel(context.Background(), "probe-free")
		if !result.attempted || !result.success {
			t.Fatalf("availability check %d must reach and satisfy the stub, got %+v", i, result)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("two checks must reach the upstream twice, got %d", len(seen))
	}

	number := regexp.MustCompile(`Repeat this number: (\d+)`)
	for i, got := range seen {
		if !strings.HasPrefix(got.session, "ses_") || len(got.session) != 30 {
			t.Fatalf("check %d sent a non-canonical session id %q", i, got.session)
		}
		if got.child != got.session {
			t.Fatalf("check %d child session %q must follow the top-level session %q", i, got.child, got.session)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(got.body), &payload); err != nil {
			t.Fatalf("check %d body is not JSON: %v", i, err)
		}
		user := ""
		messages, _ := payload["messages"].([]any)
		for _, raw := range messages {
			message, ok := raw.(map[string]any)
			if !ok || message["role"] != "user" {
				continue
			}
			user, _ = message["content"].(string)
		}
		if !strings.Contains(user, "Reply with OK.") || !number.MatchString(user) {
			t.Fatalf("check %d user turn lost the baseline prompt or the nonce: %q", i, user)
		}
	}

	if seen[0].session == seen[1].session {
		t.Fatalf("both checks reused the session id %q; an exhausted id would poison every later exit", seen[0].session)
	}
	first, second := number.FindStringSubmatch(seen[0].body), number.FindStringSubmatch(seen[1].body)
	if first == nil || second == nil || first[1] == second[1] {
		t.Fatalf("the prompt nonce must change per check, got %v and %v", first, second)
	}
}
