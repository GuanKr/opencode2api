package gateway

import (
	"bytes"
	"encoding/json"
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

func TestIsStaleReasoningReference(t *testing.T) {
	cases := []struct {
		body string
		want bool
	}{
		// The gone/expired family this baseline matches.
		{`Referenced reasoning item 'rs_abc' was not found or has expired.`, true},
		{`reasoning reference rs_xyz does not exist`, true},
		{`{"error":{"message":"Referenced reasoning item 'rs_abc' has expired"}}`, true},
		// Negative cases: generic errors mentioning reasoning must not match.
		{`unknown reasoning field 'foo'`, false},
		{`invalid reasoning_effort value 'high'`, false},
		{`reasoning summary not issued yet, retry`, false},
		{`not json at all`, false},
		{``, false},
		// Baseline gap, pinned on purpose: the caller-bound
		// "encrypted_content was not issued to this caller" leg belongs to the
		// old local commit 74efb3c, which is not part of this port. The
		// baseline matcher only recognises the gone/expired family, so this
		// body must NOT trigger a full-strip replay here.
		{`{"error":{"type":"upstream_error","message":"invalid_request_error: Error from provider (Console): Upstream request failed: [invalid_request_error] reasoning ` + "`encrypted_content`" + ` was not issued to this caller"}}`, false},
	}
	for _, tc := range cases {
		if got := isStaleReasoningReference([]byte(tc.body)); got != tc.want {
			t.Errorf("isStaleReasoningReference(%q) = %v, want %v", tc.body, got, tc.want)
		}
	}
}

// TestStripStaleReasoningInputsDropsEverything locks the baseline's full
// self-heal strip, the shape the selective pre-strip falls back to: every
// reasoning reference and the chain link go, a non-Responses tier passes
// through, and an already clean payload reports no change (which is what
// keeps the 400 from being retried forever).
func TestStripStaleReasoningInputsDropsEverything(t *testing.T) {
	// Zen speaks Responses (the only protocol carrying reasoning input
	// items), Go speaks Chat and must be forwarded verbatim.
	route := models.Route{ID: "probe-free", Tier: config.TierZen, Protocol: wire.Responses,
		Protocols: map[config.Tier]wire.Protocol{config.TierZen: wire.Responses, config.TierGo: wire.Chat}}
	responses := mustStaleBody(t, "resp_old", []string{"rs_a", "rs_b"})
	chatBody := mustBody(t, map[string]any{"model": "chat", "input": []any{
		map[string]any{"type": "reasoning", "id": "rs_a"},
	}})
	bodies := map[config.Tier][]byte{config.TierZen: responses, config.TierGo: chatBody}
	out, changed := stripStaleReasoningInputs(route, bodies)
	if !changed {
		t.Fatal("stale payload must report a change")
	}
	if got := reasoningIDs(t, out[config.TierZen]); len(got) != 0 {
		t.Fatalf("full strip must drop all reasoning, got %v", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(out[config.TierZen], &payload); err != nil {
		t.Fatal(err)
	}
	if _, ok := payload["previous_response_id"]; ok {
		t.Fatalf("full strip must drop the chain link: %s", out[config.TierZen])
	}
	if !bytes.Equal(out[config.TierGo], chatBody) {
		t.Fatal("non-Responses tier must pass through untouched")
	}
	// A second pass over the stripped body is a no-op: that is what stops
	// the strip-and-retry loop from replaying a payload it already cleaned.
	if _, changed := stripStaleReasoningInputs(route, out); changed {
		t.Fatal("already stripped payload must report no change")
	}
}
