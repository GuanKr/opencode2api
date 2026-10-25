package gateway

import (
	"bytes"
	"encoding/json"
	"runtime"
	"strings"
	"testing"

	"opencode2api/internal/jsonutil"
	wire "opencode2api/internal/protocol"
)

// The free-tier shaper is the security-relevant part of the anonymous lane: the
// upstream gate only serves traffic that looks like an official CLI session, so
// the body must carry the official system prompt, the official tool set and
// stream framing. These cases pin that contract per protocol.
//
// Merge decision D4 (append-only tools, never rewrite what the client declared)
// was overturned on 2026-10-25: upstream's clint_ renaming is now the rule, and
// the response side restores the client names through wire.ToolRenames. The
// tests below are the replacement for that suite, not a port of it.

// decodeShaped decodes a shaped body. Shaping is expected to produce JSON for
// every reachable input; anything else is a fixture bug, not a pass.
func decodeShaped(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("shaped body is not JSON: %v\n%s", err, body)
	}
	return payload
}

// encodeFixture renders a payload for prepareAnonymousBody.
func encodeFixture(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("fixture is not encodable: %v", err)
	}
	return encoded
}

// clientChatTool builds a Chat tool declaration the way a third-party client
// sends its own tool: right name, its own description, its own parameter names.
func clientChatTool(name, description string, parameters map[string]any) map[string]any {
	if parameters == nil {
		parameters = map[string]any{
			"type":       "object",
			"properties": map[string]any{"file_path": map[string]any{"type": "string"}},
			"required":   []any{"file_path"},
		}
	}
	return map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        name,
			"description": description,
			"parameters":  parameters,
		},
	}
}

// shapeTools returns the tools array, failing when the shaper dropped the slot.
func shapeTools(t *testing.T, payload map[string]any) []any {
	t.Helper()
	raw, ok := payload["tools"].([]any)
	if !ok {
		t.Fatalf("shaped payload has no tools array: %v", payload["tools"])
	}
	return raw
}

// shapeToolNames lists tool names in wire order for Chat and the name-based
// protocols (Responses, Anthropic).
func shapeToolNames(t *testing.T, protocol wire.Protocol, items []any) []string {
	t.Helper()
	names := make([]string, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("tool entry is not an object: %T", item)
		}
		if protocol == wire.Chat {
			names = append(names, jsonutil.StringAt(entry, "function", "name"))
			continue
		}
		names = append(names, jsonutil.StringAt(entry, "name"))
	}
	return names
}

// shapeEntryByName finds one tool entry by its (wire) name.
func shapeEntryByName(t *testing.T, protocol wire.Protocol, items []any, want string) map[string]any {
	t.Helper()
	for _, item := range items {
		entry, _ := item.(map[string]any)
		var name string
		if protocol == wire.Chat {
			name = jsonutil.StringAt(entry, "function", "name")
		} else {
			name = jsonutil.StringAt(entry, "name")
		}
		if name == want {
			return entry
		}
	}
	t.Fatalf("no tool named %q in %v", want, shapeToolNames(t, protocol, items))
	return nil
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Stream framing, usage reporting and the 2.x completion ceiling
// ---------------------------------------------------------------------------

// TestFreeTierShapingForcesAgentChatShape pins the three wire changes every
// shaped Chat body needs: a streamed reply (the gate rejects plain JSON from
// agent traffic), include_usage so the collapsed document still reports tokens,
// and the 32000 completion ceiling a 2.1 client states.
func TestFreeTierShapingForcesAgentChatShape(t *testing.T) {
	body := []byte(`{"model":"opencode/probe-free","messages":[{"role":"user","content":"hi"}]}`)
	shaped, renames := prepareAnonymousBody(body, wire.Chat, false)
	if renames.Count() != 0 {
		t.Fatalf("a body with no client tools must report no renames, got %d", renames.Count())
	}
	if !strings.HasPrefix(string(shaped), `{"model":"opencode/probe-free"`) {
		t.Fatalf("the 2.x lane must keep the client's model-first key order, got %s", shaped)
	}
	payload := decodeShaped(t, shaped)
	if payload["stream"] != true {
		t.Fatalf("stream must be forced on, got %v", payload["stream"])
	}
	options, ok := payload["stream_options"].(map[string]any)
	if !ok || options["include_usage"] != true {
		t.Fatalf("stream_options.include_usage must be set, got %v", payload["stream_options"])
	}
	if got := payload["max_completion_tokens"]; got != float64(v2CompletionLimit) {
		t.Fatalf("max_completion_tokens = %v, want %d", got, v2CompletionLimit)
	}
	if names := shapeToolNames(t, wire.Chat, shapeTools(t, payload)); !equalStrings(names, officialToolNames(false)) {
		t.Fatalf("the full 2.x official set must be appended in order, got %v", names)
	}
}

// TestFreeTierShapingHonoursAnExplicitCompletionLimit: the added ceiling is a
// default, not an override. A caller that states its own limit keeps it.
func TestFreeTierShapingHonoursAnExplicitCompletionLimit(t *testing.T) {
	for name, payload := range map[string]map[string]any{
		"max_tokens": {
			"model": "opencode/probe-free", "max_tokens": 128,
			"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		},
		"max_completion_tokens": {
			"model": "opencode/probe-free", "max_completion_tokens": 256,
			"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		},
	} {
		shaped := decodeShaped(t, mustShape(t, encodeFixture(t, payload), wire.Chat, false))
		got, hasCompletion := shaped["max_completion_tokens"]
		if name == "max_tokens" && hasCompletion {
			t.Fatalf("max_completion_tokens must not be added beside max_tokens, got %v", got)
		}
		if name == "max_completion_tokens" && got != float64(256) {
			t.Fatalf("max_completion_tokens must keep the caller's value, got %v", got)
		}
	}
}

// mustShape runs the shaper and fails when it returns the input untouched, which
// would silently skip every assertion below.
func mustShape(t *testing.T, body []byte, protocol wire.Protocol, v1 bool) []byte {
	t.Helper()
	shaped, _ := prepareAnonymousBody(body, protocol, v1)
	if len(shaped) == 0 {
		t.Fatal("shaper returned an empty body")
	}
	return shaped
}

// TestFreeTierShapingForcesStreamOnEveryLane: the 1.x lane and the non-Chat
// protocols also have to stream. Only the usage option is Chat-specific.
func TestFreeTierShapingForcesStreamOnEveryLane(t *testing.T) {
	cases := []struct {
		protocol wire.Protocol
		v1       bool
		body     string
	}{
		{wire.Chat, true, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`},
		{wire.Anthropic, false, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`},
		{wire.Responses, false, `{"model":"m","input":[{"role":"user","content":"hi"}]}`},
	}
	for _, testCase := range cases {
		payload := decodeShaped(t, mustShape(t, []byte(testCase.body), testCase.protocol, testCase.v1))
		if payload["stream"] != true {
			t.Fatalf("%v (v1=%v): stream must be forced on, got %v", testCase.protocol, testCase.v1, payload["stream"])
		}
		if testCase.protocol != wire.Chat {
			if _, ok := payload["stream_options"]; ok {
				t.Fatalf("%v must not carry Chat's stream_options", testCase.protocol)
			}
			if _, ok := payload["max_completion_tokens"]; ok {
				t.Fatalf("%v must not carry Chat's max_completion_tokens", testCase.protocol)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Official system prompt injection
// ---------------------------------------------------------------------------

// TestFreeTierShapingFoldsChatSystemMessagesIntoOne pins the 1.x/2.x shape: a
// single leading system message carrying the official prompt and, after a blank
// line, the client's own system and developer instructions in the order sent.
func TestFreeTierShapingFoldsChatSystemMessagesIntoOne(t *testing.T) {
	body := encodeFixture(t, map[string]any{
		"model": "opencode/probe-free",
		"messages": []any{
			map[string]any{"role": "developer", "content": "dev rules"},
			map[string]any{"role": "system", "content": []any{
				map[string]any{"type": "text", "text": "sys rules"},
			}},
			map[string]any{"role": "user", "content": "hi"},
			map[string]any{"role": "assistant", "content": "hello"},
		},
	})
	payload := decodeShaped(t, mustShape(t, body, wire.Chat, false))
	messages, ok := payload["messages"].([]any)
	if !ok || len(messages) != 3 {
		t.Fatalf("want one folded system message plus the two turns, got %v", payload["messages"])
	}
	system, _ := messages[0].(map[string]any)
	if system["role"] != "system" {
		t.Fatalf("the folded message must lead and be a system message, got %v", system["role"])
	}
	content, _ := system["content"].(string)
	if !strings.Contains(content, opencodePromptMarker) {
		t.Fatalf("the official prompt is missing from the folded message")
	}
	if !strings.Contains(content, "\n\ndev rules\nsys rules\n\n") {
		t.Fatalf("client instructions must follow the official prompt in order, got %q", content)
	}
	if !strings.HasSuffix(content, "\n\n"+gatewayToolNote) {
		t.Fatalf("the gateway tool note must come last, got %q", content)
	}
	for index, raw := range messages {
		message, _ := raw.(map[string]any)
		if index > 0 && (message["role"] == "system" || message["role"] == "developer") {
			t.Fatalf("system/developer message %d survived the fold", index)
		}
	}
}

// TestFreeTierShapingInjectsAnthropicSystemInPlace covers all three starting
// states: absent, a plain string, and a block list.
func TestFreeTierShapingInjectsAnthropicSystemInPlace(t *testing.T) {
	absent := decodeShaped(t, mustShape(t, []byte(`{"model":"m","messages":[]}`), wire.Anthropic, false))
	if system, ok := absent["system"].(string); !ok || !strings.Contains(system, opencodePromptMarker) {
		t.Fatalf("a body without a system value must get the prompt as a string, got %v", absent["system"])
	}

	asString := decodeShaped(t, mustShape(t, []byte(`{"model":"m","system":"be terse","messages":[]}`), wire.Anthropic, false))
	system, _ := asString["system"].(string)
	if !strings.HasPrefix(system, opencodeSystemPrompt("m", false)) || !strings.Contains(system, "\n\nbe terse\n\n"+gatewayToolNote) {
		t.Fatalf("a string system value must keep its text after the prompt and before the note, got %q", system)
	}

	asBlocks := decodeShaped(t, mustShape(t, []byte(`{"model":"m","system":[{"type":"text","text":"be terse"}],"messages":[]}`), wire.Anthropic, false))
	blocks, ok := asBlocks["system"].([]any)
	if !ok || len(blocks) != 3 {
		t.Fatalf("a block system value must gain the prompt and the note, got %v", asBlocks["system"])
	}
	first, _ := blocks[0].(map[string]any)
	if first["type"] != "text" || !strings.Contains(first["text"].(string), opencodePromptMarker) {
		t.Fatalf("the leading block must be the official prompt, got %v", first)
	}
	second, _ := blocks[1].(map[string]any)
	if second["text"] != "be terse" {
		t.Fatalf("the client block must be preserved verbatim, got %v", second)
	}
	third, _ := blocks[2].(map[string]any)
	if third["text"] != gatewayToolNote {
		t.Fatalf("the trailing block must be the gateway note, got %v", third)
	}
}

// TestFreeTierShapingInjectsResponsesInstructions covers absent and present
// instructions values.
func TestFreeTierShapingInjectsResponsesInstructions(t *testing.T) {
	absent := decodeShaped(t, mustShape(t, []byte(`{"model":"m","input":[]}`), wire.Responses, false))
	instructions, _ := absent["instructions"].(string)
	if !strings.Contains(instructions, opencodePromptMarker) {
		t.Fatalf("instructions must be set when the client sent none, got %v", absent["instructions"])
	}

	present := decodeShaped(t, mustShape(t, []byte(`{"model":"m","instructions":"be terse","input":[]}`), wire.Responses, false))
	instructions, _ = present["instructions"].(string)
	if !strings.Contains(instructions, "\n\nbe terse\n\n"+gatewayToolNote) {
		t.Fatalf("the client instructions must follow the prompt and precede the note, got %q", instructions)
	}
}

// TestFreeTierShapingNeverDuplicatesTheOfficialPrompt: a client that already
// carries the prompt (an official CLI pointed at the gateway) must not receive
// it twice, no matter which slot holds it.
func TestFreeTierShapingNeverDuplicatesTheOfficialPrompt(t *testing.T) {
	cases := map[string]struct {
		protocol wire.Protocol
		body     string
	}{
		"chat": {
			wire.Chat,
			`{"model":"m","messages":[{"role":"system","content":"` + opencodePromptMarker + ` and the rest"},` +
				`{"role":"user","content":"hi"}]}`,
		},
		"anthropic": {
			wire.Anthropic,
			`{"model":"m","system":[{"type":"text","text":"` + opencodePromptMarker + `"}],"messages":[]}`,
		},
		"responses": {
			wire.Responses,
			`{"model":"m","instructions":"` + opencodePromptMarker + `","input":[]}`,
		},
	}
	for name, testCase := range cases {
		shaped, _ := prepareAnonymousBody([]byte(testCase.body), testCase.protocol, false)
		if count := strings.Count(string(shaped), opencodePromptMarker); count != 1 {
			t.Fatalf("%s: the prompt appears %d times, want exactly the client's one:\n%s", name, count, shaped)
		}
	}
}

// TestFreeTierShapingAppendsTheGatewayToolNote pins the suppression note: after
// the official prompt and after the client's own instructions, every shaped body
// ends with the note that tells the model only the clint_ tools exist in the
// caller's executor. Without it the appended official set invites calls the
// client never declared and cannot run.
func TestFreeTierShapingAppendsTheGatewayToolNote(t *testing.T) {
	chat := decodeShaped(t, mustShape(t, []byte(`{"model":"m","messages":[{"role":"system","content":"be terse"},`+
		`{"role":"user","content":"hi"}]}`), wire.Chat, false))
	messages, _ := chat["messages"].([]any)
	if len(messages) == 0 {
		t.Fatal("the shaped chat body lost its messages")
	}
	system, _ := messages[0].(map[string]any)
	content, _ := system["content"].(string)
	if strings.Count(content, gatewayToolNote) != 1 || !strings.HasSuffix(content, "\n\n"+gatewayToolNote) {
		t.Fatalf("the chat note must be the last system instruction exactly once, got %q", content)
	}
	if !strings.Contains(content, "be terse") {
		t.Fatalf("the client's instruction must survive ahead of the note, got %q", content)
	}

	anthropicString := decodeShaped(t, mustShape(t, []byte(`{"model":"m","system":"be terse","messages":[]}`), wire.Anthropic, false))
	if system, _ := anthropicString["system"].(string); !strings.HasSuffix(system, "\n\n"+gatewayToolNote) {
		t.Fatalf("a string system value must gain the note at the end, got %v", anthropicString["system"])
	}
	anthropicBlocks := decodeShaped(t, mustShape(t, []byte(`{"model":"m","system":[{"type":"text","text":"be terse"}],"messages":[]}`), wire.Anthropic, false))
	blocks, _ := anthropicBlocks["system"].([]any)
	if len(blocks) != 3 {
		t.Fatalf("a block system value must gain the prompt and the note, got %v", anthropicBlocks["system"])
	}
	if last, _ := blocks[len(blocks)-1].(map[string]any); last["text"] != gatewayToolNote {
		t.Fatalf("the last block must be the note, got %v", last)
	}

	responses := decodeShaped(t, mustShape(t, []byte(`{"model":"m","instructions":"be terse","input":[]}`), wire.Responses, false))
	if instructions, _ := responses["instructions"].(string); !strings.HasSuffix(instructions, "\n\n"+gatewayToolNote) {
		t.Fatalf("responses instructions must gain the note at the end, got %v", responses["instructions"])
	}
}

// TestFreeTierShapingNotesABodyWithNoSystemSlot: a client that sends no system
// message still needs to know which tools it can run. Prompt injection creates
// the leading slot, and the note must land in it rather than being dropped.
func TestFreeTierShapingNotesABodyWithNoSystemSlot(t *testing.T) {
	payload := decodeShaped(t, mustShape(t, []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`), wire.Chat, false))
	messages, _ := payload["messages"].([]any)
	first, _ := messages[0].(map[string]any)
	content, _ := first["content"].(string)
	if first["role"] != "system" || !strings.HasPrefix(content, opencodePromptMarker) || !strings.HasSuffix(content, gatewayToolNote) {
		t.Fatalf("want one leading system message carrying the prompt and the note, got %v", messages)
	}
}

// TestFreeTierShapingNotesABodyThatAlreadyCarriesThePrompt: the marker suppresses
// the prompt injection, not the tool shaping. A client that sends the official
// prompt itself still has its tools renamed, so it still needs the note.
func TestFreeTierShapingNotesABodyThatAlreadyCarriesThePrompt(t *testing.T) {
	shaped := mustShape(t, []byte(`{"model":"m","messages":[{"role":"system","content":"`+opencodePromptMarker+
		` and the rest"},{"role":"user","content":"hi"}]}`), wire.Chat, false)
	messages, _ := decodeShaped(t, shaped)["messages"].([]any)
	system, _ := messages[0].(map[string]any)
	content, _ := system["content"].(string)
	if strings.Count(content, opencodePromptMarker) != 1 {
		t.Fatalf("the client's prompt must not be duplicated, got %q", content)
	}
	if !strings.HasSuffix(content, "\n\n"+gatewayToolNote) {
		t.Fatalf("the note must still be appended, got %q", content)
	}
}

// TestGatewayToolNoteStaysScopedToTheOfficialSet pins the wording decision: the
// shaper passes client-declared server tools (Responses web_search, Anthropic
// server tools) through unrenamed because they have no client name to rewrite,
// and the upstream runs them. A blanket "every other tool is a placeholder"
// would tell the model not to use a capability that still works, so the
// prohibition must stay scoped to the official OpenCode set.
func TestGatewayToolNoteStaysScopedToTheOfficialSet(t *testing.T) {
	lower := strings.ToLower(gatewayToolNote)
	for _, blanket := range []string{"every other tool", "any other tool", "all other tools", "every tool"} {
		if strings.Contains(lower, blanket) {
			t.Fatalf("the note claims %q is a placeholder; client-declared server tools such as web_search stay callable:\n%s", blanket, gatewayToolNote)
		}
	}
	if !strings.Contains(gatewayToolNote, "official OpenCode") {
		t.Fatalf("the prohibition must name the official set it applies to, got %q", gatewayToolNote)
	}
}

// TestFreeTierShapingLeavesUnusableBodiesAlone: a body the shaper cannot parse,
// and a System One decision payload, are forwarded verbatim. Rewriting them
// would either corrupt a non-JSON body or turn a decision request into agent
// traffic.
func TestFreeTierShapingLeavesUnusableBodiesAlone(t *testing.T) {
	notJSON := []byte("this is not json")
	shaped, renames := prepareAnonymousBody(notJSON, wire.Chat, false)
	if !bytes.Equal(shaped, notJSON) || renames != nil {
		t.Fatalf("a non-JSON body must pass through untouched, got %s", shaped)
	}

	systemOne := []byte(`{"stream":false,"messages":[{"role":"user","content":"pick a model"}]}`)
	shaped, renames = prepareAnonymousBody(systemOne, wire.SystemOne, false)
	if !bytes.Equal(shaped, systemOne) || renames != nil {
		t.Fatalf("a System One payload must pass through untouched, got %s", shaped)
	}
}

// ---------------------------------------------------------------------------
// Tool renaming and completion
// ---------------------------------------------------------------------------

// TestFreeTierShapingRenamesClientTools pins the clint_ contract: every named
// client tool is renamed, its definition is otherwise preserved (the client's
// own parameter names still drive its executor), duplicates share one wire name,
// and the mapping restores the client name in the response.
func TestFreeTierShapingRenamesClientTools(t *testing.T) {
	body := encodeFixture(t, map[string]any{
		"model": "opencode/probe-free",
		"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
		},
		"tools": []any{
			clientChatTool("read_file", "client read", nil),
			clientChatTool("write file!", "client write", nil),
			clientChatTool("read_file", "client read duplicate", nil),
		},
	})
	shaped, renames := prepareAnonymousBody(body, wire.Chat, false)
	payload := decodeShaped(t, shaped)
	tools := shapeTools(t, payload)

	read := shapeEntryByName(t, wire.Chat, tools, "clint_read_file")
	function, _ := read["function"].(map[string]any)
	if function["description"] != "client read" {
		t.Fatalf("the first definition must win for a duplicate name, got %v", function["description"])
	}
	properties := jsonutil.MapAt(function, "parameters", "properties")
	if _, ok := properties["file_path"]; !ok {
		t.Fatalf("the client's own parameter names must survive the rename, got %v", function["parameters"])
	}

	shapeEntryByName(t, wire.Chat, tools, "clint_write_file")

	if renames.Count() != 2 {
		t.Fatalf("want two distinct client names renamed, got %d", renames.Count())
	}
	if got, ok := renames.Wire("read_file"); !ok || got != "clint_read_file" {
		t.Fatalf("renames.Wire(read_file) = %q, %v", got, ok)
	}
	if got := renames.Restore("clint_write_file"); got != "write file!" {
		t.Fatalf("renames.Restore must return the client's exact name, got %q", got)
	}

	// The official set follows the client entries, in the lane's order.
	names := shapeToolNames(t, wire.Chat, tools)
	official := officialToolNames(false)
	if !equalStrings(names[len(names)-len(official):], official) {
		t.Fatalf("the official set must be appended after the client tools, got %v", names)
	}
}

// TestFreeTierShapingDisambiguatesSanitizedCollisions: two different client
// names that reduce to the same wire name must not share it, or the response
// would restore the wrong tool.
func TestFreeTierShapingDisambiguatesSanitizedCollisions(t *testing.T) {
	body := encodeFixture(t, map[string]any{
		"model": "opencode/probe-free",
		"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
		},
		"tools": []any{
			clientChatTool("a b", "slashless", nil),
			clientChatTool("a/b", "slashed", nil),
		},
	})
	_, renames := prepareAnonymousBody(body, wire.Chat, false)

	first, _ := renames.Wire("a b")
	second, _ := renames.Wire("a/b")
	if first != "clint_a_b" || second != "clint_a_b_2" {
		t.Fatalf("colliding sanitized names must get suffixes, got %q and %q", first, second)
	}
	if renames.Restore(first) != "a b" || renames.Restore(second) != "a/b" {
		t.Fatalf("each wire name must restore its own client name, got %q and %q", renames.Restore(first), renames.Restore(second))
	}
}

// TestFreeTierShapingRewritesEveryChatReference keeps the request
// self-consistent: a renamed tool still has to match tool_choice, legacy
// function_call and the tool-call history, or the upstream rejects the turn.
func TestFreeTierShapingRewritesEveryChatReference(t *testing.T) {
	body := encodeFixture(t, map[string]any{
		"model": "opencode/probe-free",
		"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
			map[string]any{
				"role":    "assistant",
				"content": nil,
				"tool_calls": []any{map[string]any{
					"id":       "call_1",
					"type":     "function",
					"function": map[string]any{"name": "read_file", "arguments": "{}"},
				}},
				"function_call": map[string]any{"name": "read_file", "arguments": "{}"},
			},
		},
		"tools":       []any{clientChatTool("read_file", "client read", nil)},
		"tool_choice": map[string]any{"type": "function", "function": map[string]any{"name": "read_file"}},
		"function_call": map[string]any{
			"name": "read_file",
		},
	})
	payload := decodeShaped(t, mustShape(t, body, wire.Chat, false))

	choice, _ := payload["tool_choice"].(map[string]any)
	function, _ := choice["function"].(map[string]any)
	if function["name"] != "clint_read_file" {
		t.Fatalf("tool_choice must follow the rename, got %v", choice)
	}
	legacyCall, _ := payload["function_call"].(map[string]any)
	if legacyCall["name"] != "clint_read_file" {
		t.Fatalf("the legacy function_call must follow the rename, got %v", legacyCall)
	}

	messages, _ := payload["messages"].([]any)
	assistant, _ := messages[len(messages)-1].(map[string]any)
	calls, _ := assistant["tool_calls"].([]any)
	call, _ := calls[0].(map[string]any)
	callFunction, _ := call["function"].(map[string]any)
	if callFunction["name"] != "clint_read_file" {
		t.Fatalf("tool-call history must follow the rename, got %v", callFunction)
	}
	historyCall, _ := assistant["function_call"].(map[string]any)
	if historyCall["name"] != "clint_read_file" {
		t.Fatalf("legacy function_call history must follow the rename, got %v", historyCall)
	}
}

// TestFreeTierShapingLeavesStringToolChoice: "auto"/"none"/"required" are not
// names, and rewriting them would corrupt the request.
func TestFreeTierShapingLeavesStringToolChoice(t *testing.T) {
	body := encodeFixture(t, map[string]any{
		"model":       "opencode/probe-free",
		"messages":    []any{map[string]any{"role": "user", "content": "hi"}},
		"tools":       []any{clientChatTool("read_file", "client read", nil)},
		"tool_choice": "required",
	})
	payload := decodeShaped(t, mustShape(t, body, wire.Chat, false))
	if payload["tool_choice"] != "required" {
		t.Fatalf("a string tool_choice must pass through, got %v", payload["tool_choice"])
	}
}

// TestFreeTierShapingConvertsLegacyFunctions: the pre-tools "functions" array is
// promoted into the protocol's tool shape (and renamed), then removed so the
// upstream cannot see both forms.
func TestFreeTierShapingConvertsLegacyFunctions(t *testing.T) {
	for _, protocol := range []wire.Protocol{wire.Chat, wire.Responses} {
		body := encodeFixture(t, map[string]any{
			"model": "opencode/probe-free",
			"messages": []any{
				map[string]any{"role": "user", "content": "hi"},
			},
			"functions": []any{map[string]any{
				"name":        "lookup",
				"description": "legacy lookup",
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"query": map[string]any{"type": "string"}},
				},
			}},
		})
		payload := decodeShaped(t, mustShape(t, body, protocol, false))
		if _, ok := payload["functions"]; ok {
			t.Fatalf("%v: the legacy functions array must be removed", protocol)
		}
		entry := shapeEntryByName(t, protocol, shapeTools(t, payload), "clint_lookup")
		var description string
		var parameters map[string]any
		if protocol == wire.Chat {
			description = jsonutil.StringAt(entry, "function", "description")
			parameters = jsonutil.MapAt(entry, "function", "parameters")
		} else {
			description = jsonutil.StringAt(entry, "description")
			parameters = jsonutil.MapAt(entry, "parameters")
		}
		if description != "legacy lookup" {
			t.Fatalf("%v: the converted tool must keep its description, got %q", protocol, description)
		}
		if _, ok := jsonutil.MapAt(parameters, "properties")["query"]; !ok {
			t.Fatalf("%v: the converted tool must keep its parameters, got %v", protocol, parameters)
		}
	}
}

// TestFreeTierShapingKeepsBuiltInServerTools: Responses web search and Anthropic
// server tools are not client functions; they have no name to rename and must
// pass through as declared.
func TestFreeTierShapingKeepsBuiltInServerTools(t *testing.T) {
	responses := decodeShaped(t, mustShape(t, encodeFixture(t, map[string]any{
		"model": "opencode/probe-free",
		"input": []any{map[string]any{"role": "user", "content": "hi"}},
		"tools": []any{
			map[string]any{"type": "web_search"},
			map[string]any{"type": "function", "name": "lookup", "description": "d"},
		},
	}), wire.Responses, false))
	tools := shapeTools(t, responses)
	var serverTool map[string]any
	for _, item := range tools {
		entry, _ := item.(map[string]any)
		if entry["type"] == "web_search" {
			serverTool = entry
		}
	}
	if serverTool == nil {
		t.Fatalf("the built-in web_search tool was dropped: %v", shapeToolNames(t, wire.Responses, tools))
	}
	if _, renamed := serverTool["name"]; renamed {
		t.Fatalf("a built-in tool must not gain a name, got %v", serverTool)
	}
	shapeEntryByName(t, wire.Responses, tools, "clint_lookup")

	anthropic := decodeShaped(t, mustShape(t, encodeFixture(t, map[string]any{
		"model":    "opencode/probe-free",
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		"tools": []any{
			map[string]any{"type": "web_search_20250305", "name": "web_search"},
			map[string]any{"name": "lookup", "description": "d", "input_schema": map[string]any{"type": "object"}},
		},
	}), wire.Anthropic, false))
	tools = shapeTools(t, anthropic)
	server := shapeEntryByName(t, wire.Anthropic, tools, "web_search")
	if server["type"] != "web_search_20250305" {
		t.Fatalf("an Anthropic server tool must keep its type and name, got %v", server)
	}
	shapeEntryByName(t, wire.Anthropic, tools, "clint_lookup")
}

// TestFreeTierShapingRewritesAnthropicToolUse keeps the assistant history valid
// after the rename: a tool_use block naming the old tool would be rejected.
func TestFreeTierShapingRewritesAnthropicToolUse(t *testing.T) {
	body := encodeFixture(t, map[string]any{
		"model": "opencode/probe-free",
		"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
			map[string]any{"role": "assistant", "content": []any{
				map[string]any{"type": "tool_use", "id": "toolu_1", "name": "lookup", "input": map[string]any{}},
			}},
		},
		"tools": []any{
			map[string]any{"name": "lookup", "description": "d", "input_schema": map[string]any{"type": "object"}},
		},
		"tool_choice": map[string]any{"type": "tool", "name": "lookup"},
	})
	payload := decodeShaped(t, mustShape(t, body, wire.Anthropic, false))

	choice, _ := payload["tool_choice"].(map[string]any)
	if choice["name"] != "clint_lookup" {
		t.Fatalf("Anthropic tool_choice must follow the rename, got %v", choice)
	}
	messages, _ := payload["messages"].([]any)
	assistant, _ := messages[len(messages)-1].(map[string]any)
	blocks, _ := assistant["content"].([]any)
	block, _ := blocks[0].(map[string]any)
	if block["name"] != "clint_lookup" {
		t.Fatalf("the tool_use history must follow the rename, got %v", block)
	}
}

// TestFreeTierShapingRewritesResponsesHistory covers the Responses equivalents:
// a function_call item and a named tool_choice.
func TestFreeTierShapingRewritesResponsesHistory(t *testing.T) {
	body := encodeFixture(t, map[string]any{
		"model": "opencode/probe-free",
		"input": []any{
			map[string]any{"role": "user", "content": "hi"},
			map[string]any{"type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{}"},
		},
		"tools": []any{
			map[string]any{"type": "function", "name": "lookup", "description": "d"},
		},
		"tool_choice": map[string]any{"type": "function", "name": "lookup"},
	})
	payload := decodeShaped(t, mustShape(t, body, wire.Responses, false))

	choice, _ := payload["tool_choice"].(map[string]any)
	if choice["name"] != "clint_lookup" {
		t.Fatalf("Responses tool_choice must follow the rename, got %v", choice)
	}
	input, _ := payload["input"].([]any)
	call, _ := input[len(input)-1].(map[string]any)
	if call["name"] != "clint_lookup" {
		t.Fatalf("the function_call input item must follow the rename, got %v", call)
	}
}

// ---------------------------------------------------------------------------
// Official set rendering
// ---------------------------------------------------------------------------

// TestOfficialToolsetRendersPerProtocol pins the three wire shapes and the 2.x
// strict:false marker the captured client sends on every tool.
func TestOfficialToolsetRendersPerProtocol(t *testing.T) {
	chat := officialTool(wire.Chat, "read", false)
	if chat["type"] != "function" {
		t.Fatalf("Chat tool type = %v", chat["type"])
	}
	function, _ := chat["function"].(map[string]any)
	if strict, ok := function["strict"]; !ok || strict != false {
		t.Fatalf("the 2.x Chat shape must mark every tool strict:false, got %v", function["strict"])
	}

	legacy := officialTool(wire.Chat, "read", true)
	legacyFunction, _ := legacy["function"].(map[string]any)
	if _, ok := legacyFunction["strict"]; ok {
		t.Fatalf("the captured 1.x entries carry no strict field, got %v", legacyFunction["strict"])
	}

	anthropic := officialTool(wire.Anthropic, "read", false)
	if anthropic["name"] != "read" {
		t.Fatalf("Anthropic tool name = %v", anthropic["name"])
	}
	if _, ok := anthropic["input_schema"].(map[string]any); !ok {
		t.Fatalf("Anthropic tools must carry input_schema, got %v", anthropic)
	}

	responses := officialTool(wire.Responses, "read", false)
	if responses["type"] != "function" || responses["name"] != "read" {
		t.Fatalf("Responses tool shape = %v", responses)
	}
	if _, ok := responses["parameters"].(map[string]any); !ok {
		t.Fatalf("Responses tools must carry parameters, got %v", responses)
	}
}

// TestOfficialToolsetIsLaneSpecific: the 1.x lane has bash/task/todowrite and no
// shell/subagent/patch; the 2.x lane is the other way round. Mixing them is what
// makes the gate reject a body as "not from OpenCode".
func TestOfficialToolsetIsLaneSpecific(t *testing.T) {
	v2 := map[string]bool{}
	for _, name := range officialToolNames(false) {
		v2[name] = true
	}
	v1 := map[string]bool{}
	for _, name := range officialToolNames(true) {
		v1[name] = true
	}
	for _, name := range []string{"shell", "subagent", "patch", "question"} {
		if !v2[name] {
			t.Errorf("the 2.x set must contain %q", name)
		}
		if v1[name] {
			t.Errorf("the 1.x set must not contain %q", name)
		}
	}
	for _, name := range []string{"bash", "task", "todowrite"} {
		if !v1[name] {
			t.Errorf("the 1.x set must contain %q", name)
		}
		if v2[name] {
			t.Errorf("the 2.x set must not contain %q", name)
		}
	}
}

// TestV1BashDescriptionFollowsTheHostShell: the captured 1.x definition states
// its OS and shell inline, and the client describes the shell it actually has.
func TestV1BashDescriptionFollowsTheHostShell(t *testing.T) {
	captured, ok := officialToolDefinition("bash", true)
	if !ok {
		t.Fatal("opencode_tools_v1.json has no bash definition")
	}
	rendered := officialTool(wire.Chat, "bash", true)
	function, _ := rendered["function"].(map[string]any)
	description, _ := function["description"].(string)
	if runtime.GOOS != "windows" {
		if description != captured.Description {
			t.Fatalf("a non-Windows host must send the captured wording verbatim, got %q", description)
		}
		return
	}
	if !strings.Contains(description, "OS: windows, Shell: powershell") {
		t.Fatalf("a Windows host must describe powershell, got %q", description)
	}
	if strings.Contains(description, "OS: linux") {
		t.Fatalf("a Windows host must not claim a Linux shell, got %q", description)
	}
}

// TestV2ShellDescriptionRewritesOnlyTheOSPhrase guards the rewrite against a
// blanket replace: only the OS phrase changes, the rest of the measured text
// stays intact.
func TestV2ShellDescriptionRewritesOnlyTheOSPhrase(t *testing.T) {
	const base = "Execute a shell command. Commands run on Linux using bash. Prefer dedicated tools."
	got := v2ShellDescription(base)
	if runtime.GOOS != "windows" {
		if got != base {
			t.Fatalf("a non-Windows host must keep the captured wording, got %q", got)
		}
		return
	}
	if got != "Execute a shell command. Commands run on Windows using powershell. Prefer dedicated tools." {
		t.Fatalf("only the OS phrase may change, got %q", got)
	}
}
