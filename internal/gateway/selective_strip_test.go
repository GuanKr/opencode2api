package gateway

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/jsonutil"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

func testSelectiveRoute() models.Route {
	return models.Route{
		ID: "probe-free", Tier: config.TierZen, Protocol: wire.Responses,
		Protocols: map[config.Tier]wire.Protocol{config.TierZen: wire.Responses, config.TierGo: wire.Responses},
		KeyTiers:  []config.Tier{config.TierGo},
	}
}

func mustBody(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// mustStaleBody builds a Responses body with one text message plus one
// reasoning item per id, under the given chain link ("" omits the link).
func mustStaleBody(t *testing.T, prev string, ids []string) []byte {
	t.Helper()
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
	return mustBody(t, payload)
}

func reasoningIDs(t *testing.T, body []byte) []string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, item := range jsonutil.SliceAt(payload, "input") {
		m, _ := item.(map[string]any)
		if m != nil && jsonutil.StringAt(m, "type") == "reasoning" {
			ids = append(ids, jsonutil.StringAt(m, "id"))
		}
	}
	return ids
}

func TestCollectStaleReasoningIDsOnlyRecordsStrippedIDs(t *testing.T) {
	body := mustBody(t, map[string]any{
		"model":                "probe-free",
		"previous_response_id": "resp_old",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "reasoning", "id": "rs_old",
				"summary": []any{map[string]any{"type": "summary_text", "text": "old"}}},
			map[string]any{"type": "reasoning", "id": "rs_old",
				"summary": []any{map[string]any{"type": "summary_text", "text": "dup"}}},
			map[string]any{"type": "reasoning",
				"summary": []any{map[string]any{"type": "summary_text", "text": "id-less"}}},
		},
	})
	ids, prev := collectStaleReasoningIDs(body)
	if prev != "resp_old" {
		t.Fatalf("must record dead chain link, got %q", prev)
	}
	if len(ids) != 1 || ids[0] != "rs_old" {
		t.Fatalf("must record only stripped reasoning ids deduped, got %v", ids)
	}
}

func TestStripSelectiveKeepsHealthyReasoning(t *testing.T) {
	r := testSelectiveRoute()
	body := mustBody(t, map[string]any{
		"model":                "probe-free",
		"previous_response_id": "resp_fresh",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "reasoning", "id": "rs_old",
				"summary": []any{map[string]any{"type": "summary_text", "text": "old"}}},
			map[string]any{"type": "reasoning", "id": "rs_new",
				"summary": []any{map[string]any{"type": "summary_text", "text": "new"}}},
			map[string]any{"type": "reasoning",
				"summary": []any{map[string]any{"type": "summary_text", "text": "id-less"}}},
		},
	})
	deny := map[string]struct{}{"rs_old": {}}
	// Fresh chain link must survive; only the dead link is dropped.
	out, changed := stripSelectiveReasoningInputs(r, map[config.Tier][]byte{config.TierZen: body}, deny, "resp_old")
	if !changed {
		t.Fatal("blacklisted id must cause a change")
	}
	kept := reasoningIDs(t, out[config.TierZen])
	if len(kept) != 1 || kept[0] != "rs_new" {
		t.Fatalf("must keep only healthy rs_new, got %v", kept)
	}
	var payload map[string]any
	if err := json.Unmarshal(out[config.TierZen], &payload); err != nil {
		t.Fatal(err)
	}
	if got := jsonutil.StringAt(payload, "previous_response_id"); got != "resp_fresh" {
		t.Fatalf("fresh chain link must be preserved, got %q", got)
	}
}

func TestStripSelectiveDropsDeadChainLinkOnly(t *testing.T) {
	r := testSelectiveRoute()
	body := mustBody(t, map[string]any{
		"model":                "probe-free",
		"previous_response_id": "resp_old",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
		},
	})
	out, changed := stripSelectiveReasoningInputs(r, map[config.Tier][]byte{config.TierZen: body}, nil, "resp_old")
	if !changed {
		t.Fatal("dead chain link must be dropped")
	}
	var payload map[string]any
	if err := json.Unmarshal(out[config.TierZen], &payload); err != nil {
		t.Fatal(err)
	}
	if _, ok := payload["previous_response_id"]; ok {
		t.Fatalf("dead previous_response_id must be removed: %s", out[config.TierZen])
	}
}

func TestStripSelectiveNoChangeWhenClean(t *testing.T) {
	r := testSelectiveRoute()
	body := mustBody(t, map[string]any{
		"model": "probe-free",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "reasoning", "id": "rs_new",
				"summary": []any{map[string]any{"type": "summary_text", "text": "new"}}},
		},
	})
	deny := map[string]struct{}{"rs_old": {}}
	out, changed := stripSelectiveReasoningInputs(r, map[config.Tier][]byte{config.TierZen: body}, deny, "resp_old")
	if changed {
		t.Fatal("clean history without blacklisted ids must not change (re-probe full)")
	}
	if string(out[config.TierZen]) != string(body) {
		t.Fatal("unchanged selective strip must return the input bytes untouched")
	}
}

// TestStripSelectiveDropsIdlessAlone isolates the id-less rule: with no
// blacklisted id present, a lone id-less reasoning item must still trigger
// the strip on its own.
func TestStripSelectiveDropsIdlessAlone(t *testing.T) {
	r := testSelectiveRoute()
	body := mustBody(t, map[string]any{
		"model": "probe-free",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "reasoning",
				"summary": []any{map[string]any{"type": "summary_text", "text": "id-less"}}},
		},
	})
	deny := map[string]struct{}{"rs_old": {}}
	out, changed := stripSelectiveReasoningInputs(r, map[config.Tier][]byte{config.TierZen: body}, deny, "resp_old")
	if !changed {
		t.Fatal("lone id-less reasoning item must trigger the selective strip")
	}
	if got := reasoningIDs(t, out[config.TierZen]); len(got) != 0 {
		t.Fatalf("id-less reasoning must be dropped, got %v", got)
	}
}

// TestSelectiveStripCoversEveryTier is the baseline stand-in for the
// reference's planner test: prepareRouteBodies encodes every tier up front
// and one strip must rewrite all of them, so a fallback tier can never
// resurrect a blacklisted id (while healthy ids survive on that tier too).
// A non-Responses tier must be left byte-identical.
func TestSelectiveStripCoversEveryTier(t *testing.T) {
	r := testSelectiveRoute()
	zen := mustStaleBody(t, "resp_old", []string{"rs_old", "rs_new"})
	goBody := mustStaleBody(t, "resp_old", []string{"rs_old", "rs_new"})
	deny := map[string]struct{}{"rs_old": {}}
	bodies := map[config.Tier][]byte{config.TierZen: zen, config.TierGo: goBody}
	out, changed := stripSelectiveReasoningInputs(r, bodies, deny, "resp_old")
	if !changed {
		t.Fatal("blacklisted id must cause a change")
	}
	for _, tier := range []config.Tier{config.TierZen, config.TierGo} {
		if got := reasoningIDs(t, out[tier]); len(got) != 1 || got[0] != "rs_new" {
			t.Fatalf("%s tier must lose rs_old and keep rs_new, got %v", tier, got)
		}
		if bytes.Equal(out[tier], bodies[tier]) {
			t.Fatalf("%s tier body must be rewritten", tier)
		}
	}
	// The input map must not be mutated in place: the caller's bodies (and
	// therefore an in-turn retry path) keep the original wire bytes.
	if !bytes.Equal(bodies[config.TierZen], zen) {
		t.Fatalf("selective strip must not mutate the input map, got %s", bodies[config.TierZen])
	}
	// A Chat tier carries no Responses reasoning items and must pass through.
	chatRoute := models.Route{ID: "chat", Tier: config.TierZen, Protocol: wire.Chat,
		Protocols: map[config.Tier]wire.Protocol{config.TierZen: wire.Chat}}
	chatBody := mustBody(t, map[string]any{"model": "chat", "input": []any{
		map[string]any{"type": "reasoning", "id": "rs_old"},
	}})
	out, changed = stripSelectiveReasoningInputs(chatRoute, map[config.Tier][]byte{config.TierZen: chatBody}, deny, "resp_old")
	if changed || !bytes.Equal(out[config.TierZen], chatBody) {
		t.Fatalf("non-Responses tier must pass through untouched, changed=%v", changed)
	}
}

func TestSummarizeReplayedReasoning(t *testing.T) {
	body := mustBody(t, map[string]any{
		"model":                "probe-free",
		"previous_response_id": "resp_old",
		"input": []any{
			map[string]any{"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "reasoning", "id": "rs_a",
				"encrypted_content": "blob"},
			map[string]any{"type": "reasoning", "id": "rs_b"},
		},
	})
	summary := summarizeReplayedReasoning(body)
	for _, want := range []string{"n=2", "encrypted=1", "head=[rs_a,rs_b]", "tail=[rs_a,rs_b]", "prev=resp_old"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary must report %q, got %q", want, summary)
		}
	}
	if summarizeReplayedReasoning([]byte("not json")) != "unparseable" {
		t.Fatalf("unparseable body must be reported, got %q", summarizeReplayedReasoning([]byte("not json")))
	}
}
