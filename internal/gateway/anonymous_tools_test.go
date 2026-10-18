package gateway

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"opencode2api/internal/jsonutil"
	wire "opencode2api/internal/protocol"
)

// chatClientTool builds a Chat-shaped tool declaration the way a third-party
// client would send its own "write": correct name, its own description and its
// own parameter names.
func chatClientTool(name, description string) map[string]any {
	return map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        name,
			"description": description,
			"parameters": map[string]any{
				"type":       "object",
				"properties": map[string]any{"path": map[string]any{"type": "string"}},
				"required":   []any{"path"},
			},
		},
	}
}

// shapedBody runs the /inference lane's shaper, which is the gate this suite
// measures: genuine core definitions, append-only, client tools untouched.
//
// The third argument is the lane selector. It also asserts the /inference
// shaper returns no tool alias: renaming is the legacy /zen/v1 lane's
// mechanism alone, so an alias here would mean the two shapers have crossed
// and the response-side restore would rewrite names the client never sent
// (merge decision D4).
func shapedBody(t *testing.T, body string, protocol wire.Protocol) []byte {
	t.Helper()
	shaped, alias := prepareAnonymousBody([]byte(body), protocol, false)
	if alias != "" {
		t.Fatalf("inference lane returned tool alias %q; only the legacy /zen/v1 lane renames tools", alias)
	}
	return shaped
}

func shapedPayload(t *testing.T, body string, protocol wire.Protocol) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(shapedBody(t, body, protocol), &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func toolEntries(t *testing.T, payload map[string]any) []any {
	t.Helper()
	items, ok := payload["tools"].([]any)
	if !ok {
		t.Fatalf("tools is %T, want a list", payload["tools"])
	}
	return items
}

// toolName reads a tool name in the shape the given protocol actually uses:
// Chat nests it under "function", Responses and Anthropic put it at the top.
func toolName(t *testing.T, protocol wire.Protocol, item any) string {
	t.Helper()
	entry, ok := item.(map[string]any)
	if !ok {
		t.Fatalf("tool entry is %T, want an object", item)
	}
	if protocol == wire.Chat {
		return jsonutil.StringAt(entry, "function", "name")
	}
	return jsonutil.StringAt(entry, "name")
}

func toolDescription(t *testing.T, protocol wire.Protocol, item any) string {
	t.Helper()
	entry, _ := item.(map[string]any)
	if protocol == wire.Chat {
		return jsonutil.StringAt(entry, "function", "description")
	}
	return jsonutil.StringAt(entry, "description")
}

func hasKey(entry map[string]any, path ...string) bool {
	return jsonutil.AnyAt(entry, path...) != nil
}

// chatSystemText concatenates every system-role message content in order.
func chatSystemText(t *testing.T, payload map[string]any) string {
	t.Helper()
	messages, ok := payload["messages"].([]any)
	if !ok {
		t.Fatalf("messages is %T, want a list", payload["messages"])
	}
	var parts []string
	for _, item := range messages {
		entry, ok := item.(map[string]any)
		if !ok || !isSystemRole(jsonutil.StringAt(entry, "role")) {
			continue
		}
		if text, ok := entry["content"].(string); ok {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

func mustTools(t *testing.T, payload map[string]any) []any {
	t.Helper()
	return toolEntries(t, payload)
}

// 2026-10-15: a client that declared nothing at all received tool_calls for
// read, which it cannot execute. The gateway now marks the definitions it
// appends as placeholders in the system slot. The same note is what keeps a
// client whose own tools carry a core name honest: it credits those
// declarations instead of calling them placeholders too.
func TestAnonymousShapingNotesInjectedTools(t *testing.T) {
	payload := shapedPayload(t, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, wire.Chat)

	items := mustTools(t, payload)
	if len(items) != len(anonymousCoreTools) {
		t.Fatalf("tools = %d, want the core set of %d", len(items), len(anonymousCoreTools))
	}
	system := chatSystemText(t, payload)
	if want := strings.Join(anonymousCoreTools, ", "); !strings.Contains(system, want) {
		t.Fatalf("system text %q does not name the core tools as %q", system, want)
	}
	// The operative sentences are the measured benefit plus the promise this
	// gateway now makes about the client's own tools. Rewrite any of them into
	// something empty and this has to go red.
	for _, want := range []string{
		"do not call them",
		"keeps exactly the parameters it declared",
		"answer directly instead of calling a placeholder",
	} {
		if !strings.Contains(system, want) {
			t.Fatalf("system text %q is missing %q", system, want)
		}
	}
}

// The note text must not vary with what this particular request appended, or a
// client whose tool declaration changes between turns invalidates the upstream
// prompt-cache prefix on every flip.
func TestAnonymousShapingNoteTextIsStableAcrossRequests(t *testing.T) {
	only := shapedPayload(t, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, wire.Chat)
	withRead := shapedPayload(t, `{"model":"m","messages":[{"role":"user","content":"hi"}],"tools":[`+
		string(mustJSON(t, chatClientTool("read", "client read")))+`]}`, wire.Chat)

	first, second := chatSystemText(t, only), chatSystemText(t, withRead)
	if first != second {
		t.Fatalf("note text differs between requests:\n%q\n%q", first, second)
	}
}

// A client that already carries a genuine definition satisfies the gate on its
// own, so the gateway must add nothing: that traffic is a real agent session
// and its system prompt stays its own.
func TestAnonymousShapingLeavesGenuineClientAlone(t *testing.T) {
	tools := make([]any, 0, len(anonymousCoreTools))
	for _, name := range anonymousCoreTools {
		tools = append(tools, chatClientTool(name, "Agent tool "+name))
	}
	genuine, _ := knownOpencodeTool("read")
	for index, item := range tools {
		entry, _ := item.(map[string]any)
		if jsonutil.StringAt(entry, "function", "name") != "read" {
			continue
		}
		tools[index] = map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        "read",
				"description": genuine.Description,
				"parameters":  genuine.Parameters,
			},
		}
	}
	var payload map[string]any
	if err := json.Unmarshal(mustJSON(t, map[string]any{
		"model":    "m",
		"messages": []any{map[string]any{"role": "system", "content": "you are an agent"}},
		"tools":    tools,
	}), &payload); err != nil {
		t.Fatal(err)
	}
	if ensureAnonymousTools(payload, wire.Chat) {
		t.Fatalf("shaping reported a change, want the request forwarded untouched: %v", toolSummaries(t, payload))
	}
	if got := chatSystemText(t, payload); got != "you are an agent" {
		t.Fatalf("system text = %q, want the client prompt untouched", got)
	}
	if got := len(mustTools(t, payload)); got != len(anonymousCoreTools) {
		t.Fatalf("tools = %d, want %d", got, len(anonymousCoreTools))
	}
}

// An existing system message is extended instead of shadowed by a second one,
// so prompt-cache prefixes stay stable.
func TestAnonymousShapingMergesIntoExistingSystemMessage(t *testing.T) {
	payload := shapedPayload(t, `{"model":"m","messages":[{"role":"system","content":"be terse"},{"role":"user","content":"hi"}]}`, wire.Chat)

	messages, _ := payload["messages"].([]any)
	systems := 0
	for _, item := range messages {
		entry, ok := item.(map[string]any)
		if ok && isSystemRole(jsonutil.StringAt(entry, "role")) {
			systems++
		}
	}
	if systems != 1 {
		t.Fatalf("system messages = %d, want the note merged into the existing one", systems)
	}
	if got := chatSystemText(t, payload); !strings.HasPrefix(got, "be terse") || !strings.Contains(got, "placeholders") {
		t.Fatalf("system text = %q, want the client prompt kept and the note appended", got)
	}
	first, _ := messages[0].(map[string]any)
	if role := jsonutil.StringAt(first, "role"); role != "system" {
		t.Fatalf("first message role = %q, want the system message to stay first", role)
	}
}

// A system message whose content is a block list cannot be merged as text, so
// the note goes in front as its own message — without touching the client's.
func TestAnonymousShapingPrependsWhenSystemContentIsBlocks(t *testing.T) {
	payload := shapedPayload(t, `{"model":"m","messages":[{"role":"system","content":[{"type":"text","text":"be terse"}]},{"role":"user","content":"hi"}]}`, wire.Chat)

	messages, _ := payload["messages"].([]any)
	if len(messages) != 3 {
		t.Fatalf("messages = %d, want the note in front of the client's two messages", len(messages))
	}
	note, _ := messages[0].(map[string]any)
	if got, _ := note["content"].(string); !strings.Contains(got, "placeholders") {
		t.Fatalf("first message = %#v, want the note", messages[0])
	}
	client, _ := messages[1].(map[string]any)
	if client["role"] != "system" {
		t.Fatalf("client message role = %#v, want it kept as the system message", client["role"])
	}
	blocks, _ := client["content"].([]any)
	if len(blocks) != 1 || jsonutil.StringAt(blocks[0].(map[string]any), "text") != "be terse" {
		t.Fatalf("client system message was damaged: %#v", client)
	}
}

// 2026-10-15: a recognised name outside the core set keeps the client's schema.
// Measured: the free tier accepts a client definition on "write" next to a
// genuine one, so rewriting it would only misalign the model with the client.
func TestAnonymousShapingKeepsClientSchemaForNonCoreNames(t *testing.T) {
	var payload map[string]any
	if err := json.Unmarshal(mustJSON(t, map[string]any{
		"model":    "m",
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		"tools":    []any{chatClientTool("write", "Write a file (client's own schema)")},
	}), &payload); err != nil {
		t.Fatal(err)
	}
	if !ensureAnonymousTools(payload, wire.Chat) {
		t.Fatalf("shaping reported no change, want the core tools appended")
	}

	items := mustTools(t, payload)
	var write map[string]any
	for _, item := range items {
		if toolName(t, wire.Chat, item) == "write" {
			write, _ = item.(map[string]any)
		}
	}
	if write == nil {
		t.Fatal("the client tool is gone")
	}
	if got := jsonutil.StringAt(write, "function", "description"); got != "Write a file (client's own schema)" {
		t.Fatalf("write description = %q, want the client's own definition", got)
	}
	if hasKey(write, "function", "parameters", "properties", "filePath") {
		t.Fatalf("write parameters were rewritten to the OpenCode schema: %v", write)
	}
	// The core tools still have to arrive genuine, otherwise the free tier
	// rejects the request outright.
	genuine := 0
	for _, name := range anonymousCoreTools {
		for _, item := range items {
			if toolName(t, wire.Chat, item) == name && toolDescription(t, wire.Chat, item) != "Agent tool "+name {
				genuine++
			}
		}
	}
	if genuine != len(anonymousCoreTools) {
		t.Fatalf("genuine core definitions = %d, want %d", genuine, len(anonymousCoreTools))
	}
}

// The shape a real client sends: core names carrying the client's own
// parameters — OpenCode spells its path argument "path", most harnesses spell
// it "file_path" — next to a non-core tool. Those definitions have to reach the
// upstream untouched; only the core names the client left out may be appended.
// Replacing one would aim the model at a call the client cannot execute, which
// is a worse failure than the phantom tool the note warns about.
func TestAnonymousShapingKeepsClientCoreDefinitions(t *testing.T) {
	var payload map[string]any
	if err := json.Unmarshal(mustJSON(t, map[string]any{
		"model":    "m",
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		"tools": []any{
			map[string]any{
				"type": "function",
				"function": map[string]any{
					"name":        "read",
					"description": "Read a UTF-8 text file and return line-numbered content.",
					"parameters": map[string]any{
						"type":       "object",
						"properties": map[string]any{"file_path": map[string]any{"type": "string"}},
						"required":   []any{"file_path"},
					},
				},
			},
			chatClientTool("edit", "client edit"),
			chatClientTool("execute", "client execute"),
		},
	}), &payload); err != nil {
		t.Fatal(err)
	}
	if !ensureAnonymousTools(payload, wire.Chat) {
		t.Fatal("shaping reported no change, want the missing core names appended")
	}

	items := mustTools(t, payload)
	summaries := toolSummaries(t, payload)
	if got := summaries["read"]; got != "Read a UTF-8 text file and return line-numbered content." {
		t.Fatalf("read description = %q, want the client's own definition", got)
	}
	if got := summaries["edit"]; got != "client edit" {
		t.Fatalf("edit description = %q, want the client's own definition", got)
	}
	if got := summaries["execute"]; got != "client execute" {
		t.Fatalf("execute description = %q, want the client's own definition", got)
	}
	read, ok := findTool(t, items, "read")
	if !ok {
		t.Fatalf("the client's read tool is gone: %v", summaries)
	}
	if !hasKey(read, "function", "parameters", "properties", "file_path") {
		t.Fatalf("read lost its own parameter: %v", read)
	}
	if hasKey(read, "function", "parameters", "properties", "path") {
		t.Fatalf("read was rewritten to the OpenCode schema: %v", read)
	}

	// Only the names the client left out may be added, and they have to arrive
	// genuine or the free tier rejects the request outright.
	added := []string{"shell", "glob", "grep"}
	if got := len(items); got != 3+len(added) {
		t.Fatalf("tools = %d, want the client's 3 plus the %d missing core names: %v", got, len(added), summaries)
	}
	for _, name := range added {
		entry := findToolOrNil(t, items, name)
		definition, known := knownOpencodeTool(name)
		if entry == nil {
			t.Fatalf("%s is missing, the gate needs every core name: %v", name, summaries)
		}
		if !known || toolDescription(t, wire.Chat, entry) != definition.Description {
			t.Fatalf("%s did not arrive genuine: %v", name, entry)
		}
	}
}

func findTool(t *testing.T, items []any, name string) (map[string]any, bool) {
	t.Helper()
	entry := findToolOrNil(t, items, name)
	return entry, entry != nil
}

func findToolOrNil(t *testing.T, items []any, name string) map[string]any {
	t.Helper()
	for _, item := range items {
		if toolName(t, wire.Chat, item) != name {
			continue
		}
		entry, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("tool entry %q is %T, want an object", name, item)
		}
		return entry
	}
	return nil
}

// toolSummaries maps each declared tool to its description so a failure message
// shows what actually reached the request.
func toolSummaries(t *testing.T, payload map[string]any) map[string]string {
	t.Helper()
	summaries := map[string]string{}
	for _, item := range mustTools(t, payload) {
		summaries[toolName(t, wire.Chat, item)] = toolDescription(t, wire.Chat, item)
	}
	return summaries
}

// A tools slot the gateway cannot read is filled rather than forwarded: the
// gate rejects the request either way, so there is nothing to preserve.
func TestAnonymousShapingFillsUnusableToolsSlot(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `"read"`, `[]`} {
		payload := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hi"}}}
		var tools any
		if err := json.Unmarshal([]byte(raw), &tools); err != nil {
			t.Fatal(err)
		}
		payload["tools"] = tools
		if !ensureAnonymousTools(payload, wire.Chat) {
			t.Fatalf("tools=%s: shaping reported no change", raw)
		}
		if got := len(mustTools(t, payload)); got != len(anonymousCoreTools) {
			t.Fatalf("tools=%s: got %d definitions, want the core set of %d", raw, got, len(anonymousCoreTools))
		}
		if got := chatSystemText(t, payload); !strings.Contains(got, "placeholders") {
			t.Fatalf("tools=%s: system text %q, want the note", raw, got)
		}
	}
}

// tool_choice is not part of the upstream gate (measured: none/auto/required/
// named all return 200 with tools present), so shaping must not touch it.
func TestAnonymousShapingLeavesToolChoiceVerbatim(t *testing.T) {
	for _, choice := range []string{`"none"`, `"auto"`, `"required"`, `{"type":"function","function":{"name":"read"}}`} {
		payload := shapedPayload(t, `{"model":"m","messages":[{"role":"user","content":"hi"}],"tool_choice":`+choice+`}`, wire.Chat)
		var want, got any
		if err := json.Unmarshal([]byte(choice), &want); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(mustJSON(t, payload["tool_choice"]), &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("tool_choice = %#v, want %#v", got, want)
		}
	}
}

// Responses carries the note in "instructions", or merges it into an existing
// developer/system input item. The tools themselves must be Responses-shaped.
func TestAnonymousShapingNotesInjectedToolsForResponses(t *testing.T) {
	payload := shapedPayload(t, `{"model":"m","input":[{"role":"user","content":"hi"}]}`, wire.Responses)
	instructions, _ := payload["instructions"].(string)
	if !strings.Contains(instructions, "placeholders") {
		t.Fatalf("instructions = %#v, want the placeholder note", payload["instructions"])
	}
	for _, item := range mustTools(t, payload) {
		entry, _ := item.(map[string]any)
		if entry["type"] != "function" {
			t.Fatalf("Responses tool %v lost its type", entry)
		}
		if jsonutil.StringAt(entry, "name") == "" || !hasKey(entry, "parameters") {
			t.Fatalf("Responses tool is not in the Responses shape: %v", entry)
		}
	}

	merged := shapedPayload(t, `{"model":"m","instructions":"be terse","input":[{"role":"user","content":"hi"}]}`, wire.Responses)
	if got, _ := merged["instructions"].(string); !strings.HasPrefix(got, "be terse") || !strings.Contains(got, "placeholders") {
		t.Fatalf("instructions = %#v, want the client prompt kept and the note appended", merged["instructions"])
	}

	inItem := shapedPayload(t, `{"model":"m","input":[{"role":"developer","content":"be terse"},{"role":"user","content":"hi"}]}`, wire.Responses)
	if _, added := inItem["instructions"]; added {
		t.Fatalf("instructions was added although the client already has a developer item: %v", inItem["instructions"])
	}
	input, _ := inItem["input"].([]any)
	first, _ := input[0].(map[string]any)
	if got, _ := first["content"].(string); !strings.HasPrefix(got, "be terse") || !strings.Contains(got, "placeholders") {
		t.Fatalf("developer item content = %#v, want the note merged in", first["content"])
	}
}

// A null system slot counts as empty; anything else unusable is left alone
// rather than overwritten.
func TestAnonymousShapingFillsNullSystemSlots(t *testing.T) {
	responses := shapedPayload(t, `{"model":"m","instructions":null,"input":[{"role":"user","content":"hi"}]}`, wire.Responses)
	if got, _ := responses["instructions"].(string); !strings.Contains(got, "placeholders") {
		t.Fatalf("instructions = %#v, want the note", responses["instructions"])
	}

	anthropic := shapedPayload(t, `{"model":"m","system":null,"messages":[{"role":"user","content":"hi"}]}`, wire.Anthropic)
	if got, _ := anthropic["system"].(string); !strings.Contains(got, "placeholders") {
		t.Fatalf("system = %#v, want the note", anthropic["system"])
	}
}

// Anthropic carries the note in "system", as a string or as a text block, and
// its tools use input_schema rather than parameters.
func TestAnonymousShapingNotesInjectedToolsForAnthropic(t *testing.T) {
	payload := shapedPayload(t, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, wire.Anthropic)
	if got, _ := payload["system"].(string); !strings.Contains(got, "placeholders") {
		t.Fatalf("system = %#v, want the placeholder note", payload["system"])
	}
	for _, item := range mustTools(t, payload) {
		entry, _ := item.(map[string]any)
		if jsonutil.StringAt(entry, "name") == "" || !hasKey(entry, "input_schema") {
			t.Fatalf("Anthropic tool is not in the Anthropic shape: %v", entry)
		}
		if hasKey(entry, "parameters") {
			t.Fatalf("Anthropic tool carries a Chat-style parameters key: %v", entry)
		}
	}

	merged := shapedPayload(t, `{"model":"m","system":"be terse","messages":[{"role":"user","content":"hi"}]}`, wire.Anthropic)
	if got, _ := merged["system"].(string); !strings.HasPrefix(got, "be terse") || !strings.Contains(got, "placeholders") {
		t.Fatalf("system = %#v, want the client prompt kept and the note appended", merged["system"])
	}

	blocks := shapedPayload(t, `{"model":"m","system":[{"type":"text","text":"be terse"}],"messages":[{"role":"user","content":"hi"}]}`, wire.Anthropic)
	list, ok := blocks["system"].([]any)
	if !ok || len(list) != 2 {
		t.Fatalf("system = %#v, want the client block plus the note", blocks["system"])
	}
	first, _ := list[0].(map[string]any)
	if got := jsonutil.StringAt(first, "text"); got != "be terse" {
		t.Fatalf("first system block = %#v, want the client's own block kept", list[0])
	}
	last, _ := list[1].(map[string]any)
	if last["type"] != "text" || !strings.Contains(jsonutil.StringAt(last, "text"), "placeholders") {
		t.Fatalf("appended system block = %#v, want a text block holding the note", list[1])
	}
}

// System One payloads are forwarded verbatim: they are decision requests, not
// agent traffic, and injecting tools or a system note would make upstream
// reject them.
func TestAnonymousShapingLeavesSystemOneAlone(t *testing.T) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	if got, alias := prepareAnonymousBody(body, wire.SystemOne, false); string(got) != string(body) || alias != "" {
		t.Fatalf("SystemOne body = %s (tool alias %q), want it forwarded verbatim with no alias", got, alias)
	}
}

// A payload whose message list is unusable must not be corrupted by the note.
func TestAnonymousShapingNoteDegradesGracefully(t *testing.T) {
	payload := map[string]any{"model": "m", "messages": "not a list"}
	if !ensureAnonymousTools(payload, wire.Chat) {
		t.Fatal("shaping reported no change, want the core tools appended")
	}
	if got := len(mustTools(t, payload)); got != len(anonymousCoreTools) {
		t.Fatalf("tools = %d, want the core set", got)
	}
	if got, _ := payload["messages"].(string); got != "not a list" {
		t.Fatalf("messages = %#v, want the unusable value left alone", payload["messages"])
	}

	responses := map[string]any{"model": "m", "input": "not a list"}
	ensureAnonymousTools(responses, wire.Responses)
	if got, _ := responses["instructions"].(string); !strings.Contains(got, "placeholders") {
		t.Fatalf("instructions = %#v, want the note even without a usable input list", responses["instructions"])
	}
}

// Shaping has to be idempotent: the key lane shapes a body the anonymous lane
// may already have shaped, and a second pass must not stack notes.
func TestAnonymousShapingIsIdempotent(t *testing.T) {
	for name, body := range map[string]string{
		"chat plain":         `{"model":"m","messages":[{"role":"user","content":"hi"}]}`,
		"chat system":        `{"model":"m","messages":[{"role":"system","content":"be terse"},{"role":"user","content":"hi"}]}`,
		"responses plain":    `{"model":"m","input":[{"role":"user","content":"hi"}]}`,
		"responses instr":    `{"model":"m","instructions":"be terse","input":[{"role":"user","content":"hi"}]}`,
		"anthropic plain":    `{"model":"m","messages":[{"role":"user","content":"hi"}]}`,
		"anthropic blocks":   `{"model":"m","system":[{"type":"text","text":"be terse"}],"messages":[{"role":"user","content":"hi"}]}`,
		"client write only":  `{"model":"m","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"write","description":"client"}}]}`,
		"client read only":   `{"model":"m","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"read","description":"client"}}]}`,
		"client all stubs":   `{"model":"m","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"shell","description":"Agent tool shell"}},{"type":"function","function":{"name":"edit","description":"Agent tool edit"}},{"type":"function","function":{"name":"glob","description":"Agent tool glob"}},{"type":"function","function":{"name":"grep","description":"Agent tool grep"}},{"type":"function","function":{"name":"read","description":"Agent tool read"}}]}`,
		"systemone decision": `{"model":"m","messages":[{"role":"user","content":"hi"}]}`,
	} {
		for _, protocol := range []wire.Protocol{wire.Chat, wire.Responses, wire.Anthropic, wire.SystemOne} {
			if strings.HasPrefix(name, "responses") && protocol != wire.Responses {
				continue
			}
			if strings.HasPrefix(name, "anthropic") && protocol != wire.Anthropic {
				continue
			}
			if strings.HasPrefix(name, "systemone") && protocol != wire.SystemOne {
				continue
			}
			once := shapedBody(t, body, protocol)
			twice, _ := prepareAnonymousBody(once, protocol, false)
			if !bytes.Equal(once, twice) {
				t.Fatalf("%s/%s: second pass changed the body:\n%s\n%s", name, protocol, once, twice)
			}
			// The note text names placeholders more than once, so count its marker instead.
			if got := bytes.Count(twice, []byte("[gateway]")); got > 1 {
				t.Fatalf("%s/%s: note appears %d times", name, protocol, got)
			}
		}
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// protocolStubTool renders a placeholder definition in the shape its protocol
// uses, so a case under test cannot accidentally pass because the shape was
// wrong. It is the genuine renderer with the description marked as a stub.
func protocolStubTool(protocol wire.Protocol, name string) map[string]any {
	entry := anonymousTool(protocol, name)
	if protocol == wire.Chat {
		entry["function"].(map[string]any)["description"] = "Agent tool " + name
		return entry
	}
	entry["description"] = "Agent tool " + name
	return entry
}

func protocolStubToolset(protocol wire.Protocol) []any {
	tools := make([]any, 0, len(anonymousCoreTools))
	for _, name := range anonymousCoreTools {
		tools = append(tools, protocolStubTool(protocol, name))
	}
	return tools
}

func protocolPayload(t *testing.T, protocol wire.Protocol, tools []any) (map[string]any, []byte) {
	t.Helper()
	body := mustJSON(t, map[string]any{
		"model":    "m",
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		"tools":    tools,
	})
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	return payload, body
}

// The predicate that keeps a real client untouched is protocol-shaped, and
// Anthropic hides its schema under input_schema while Responses keeps
// parameters at the top. Reading the wrong key would silently disable the
// short-circuit on that protocol and start trading a client tool on every
// request, so all three shapes are pinned here.
func TestAnonymousShapingGenuineShortCircuitPerProtocol(t *testing.T) {
	for _, protocol := range []wire.Protocol{wire.Chat, wire.Responses, wire.Anthropic} {
		t.Run(string(protocol), func(t *testing.T) {
			tools := protocolStubToolset(protocol)
			// One genuine entry is all the gate needs; the rest stay the client's.
			tools[3] = anonymousTool(protocol, "grep")

			payload, before := protocolPayload(t, protocol, tools)
			if ensureAnonymousTools(payload, protocol) {
				t.Fatalf("shaping reported a change although a genuine definition was present: %v",
					payload["tools"])
			}
			if after := mustJSON(t, payload); !bytes.Equal(before, after) {
				t.Fatalf("payload changed although nothing had to be appended:\nbefore %s\nafter  %s", before, after)
			}
		})
	}
}

// protocolSystemText reads the slot each protocol carries the note in.
func protocolSystemText(t *testing.T, protocol wire.Protocol, payload map[string]any) string {
	t.Helper()
	switch protocol {
	case wire.Responses:
		text, _ := payload["instructions"].(string)
		return text
	case wire.Anthropic:
		text, _ := payload["system"].(string)
		return text
	default:
		return chatSystemText(t, payload)
	}
}

// A client that occupies every core name with its own schema still needs one
// genuine definition. Appending it under a name the client did not claim
// satisfies the gate (measured 2026-10-15) and leaves every client entry intact,
// so the gateway overwrites nothing here.
func TestAnonymousShapingAppendsSaturatorWhenCoreNamesAreClaimed(t *testing.T) {
	tools := protocolStubToolset(wire.Chat)
	// The client already claims one saturator name; the gateway must skip it.
	tools = append(tools, protocolStubTool(wire.Chat, "skill"))

	payload, _ := protocolPayload(t, wire.Chat, tools)
	if !ensureAnonymousTools(payload, wire.Chat) {
		t.Fatal("shaping reported no change, want a genuine definition appended")
	}
	items := mustTools(t, payload)
	if len(items) != len(tools)+1 {
		t.Fatalf("tools = %d, want the client's %d plus exactly one appended", len(items), len(tools)+1)
	}
	for _, name := range anonymousCoreTools {
		entry := findToolOrNil(t, items, name)
		if entry == nil || toolDescription(t, wire.Chat, entry) != "Agent tool "+name {
			t.Fatalf("the gateway rewrote the client's %q: %v", name, toolSummaries(t, payload))
		}
	}
	if skill := findToolOrNil(t, items, "skill"); skill == nil ||
		toolDescription(t, wire.Chat, skill) != "Agent tool skill" {
		t.Fatalf("the gateway wrote over a name the client already claimed: %v", toolSummaries(t, payload))
	}
	execute := findToolOrNil(t, items, "execute")
	if execute == nil || toolDescription(t, wire.Chat, execute) == "Agent tool execute" {
		t.Fatalf("want the genuine definition under an unclaimed name: %v", toolSummaries(t, payload))
	}
	system := chatSystemText(t, payload)
	if !strings.Contains(system, "do not call them") {
		t.Fatalf("system text = %q, want the appended-definition note", system)
	}
	if strings.Contains(system, "replaced that tool's definition") {
		t.Fatalf("nothing was replaced, so the trade wording must not appear: %q", system)
	}
}

// The saturator append decides the fate of a client that owns every core name,
// and it renders per protocol, so all three shapes are pinned.
func TestAnonymousShapingAppendsSaturatorPerProtocol(t *testing.T) {
	for _, protocol := range []wire.Protocol{wire.Chat, wire.Responses, wire.Anthropic} {
		t.Run(string(protocol), func(t *testing.T) {
			tools := protocolStubToolset(protocol)
			// One saturator claimed, the rest free: this is the append path.
			tools = append(tools, protocolStubTool(protocol, "skill"))
			payload, _ := protocolPayload(t, protocol, tools)
			if !ensureAnonymousTools(payload, protocol) {
				t.Fatal("shaping reported no change although no genuine definition was present")
			}
			items := mustTools(t, payload)
			if len(items) != len(tools)+1 {
				t.Fatalf("tools = %d, want %d", len(items), len(tools)+1)
			}
			for _, name := range append(claimedNames()[:0:0], append(anonymousCoreTools, "skill")...) {
				entry := findProtocolTool(t, items, protocol, name)
				if entry == nil || anonymousToolDescription(protocol, entry) != "Agent tool "+name {
					t.Fatalf("the gateway rewrote the client's %q", name)
				}
			}
			// The append must land on a name the client did not claim, and it must
			// be a genuine definition or the gate still rejects the request.
			appended := findProtocolTool(t, items, protocol, anonymousGateSaturators[0])
			genuine, _ := knownOpencodeTool(anonymousGateSaturators[0])
			if appended == nil || anonymousToolDescription(protocol, appended) != genuine.Description {
				t.Fatalf("the first unclaimed saturator did not arrive genuine")
			}
			if note := protocolSystemText(t, protocol, payload); !strings.Contains(note, "do not call them") {
				t.Fatalf("system text = %q, want the appended-definition note", note)
			}
		})
	}
}

// The last resort, and the reason a genuine definition still reaches the
// upstream without qualification: a client that already claims every saturator
// name as well gets exactly one entry traded. Real clients never get here — an
// OpenCode client ships genuine definitions and short-circuits earlier — but the
// guarantee is what makes the branch above safe to reason about.
func TestAnonymousShapingTradesOnlyWhenEverySaturatorIsClaimed(t *testing.T) {
	tools := protocolStubToolset(wire.Chat)
	for _, name := range anonymousGateSaturators {
		tools = append(tools, protocolStubTool(wire.Chat, name))
	}
	payload, _ := protocolPayload(t, wire.Chat, tools)
	if !ensureAnonymousTools(payload, wire.Chat) {
		t.Fatal("shaping reported no change although no genuine definition was present")
	}
	items := mustTools(t, payload)
	if len(items) != len(claimedNames()) {
		t.Fatalf("tools = %d, want the trade to keep the count at %d", len(items), len(claimedNames()))
	}
	genuine := 0
	for _, name := range claimedNames() {
		entry := findToolOrNil(t, items, name)
		if description := toolDescription(t, wire.Chat, entry); description != "Agent tool "+name {
			genuine++
			if name != anonymousCoreTools[0] {
				t.Fatalf("the victim was %q, want the gateway's own first core name", name)
			}
		}
	}
	if genuine != 1 {
		t.Fatalf("genuine definitions = %d, want exactly one so the gate stays satisfied", genuine)
	}
	system := chatSystemText(t, payload)
	if !strings.Contains(system, "replaced that tool's definition") {
		t.Fatalf("system text = %q, want the trade wording that admits the replacement", system)
	}
	if strings.Contains(system, "keeps exactly the parameters it declared") {
		t.Fatalf("the trade branch must not promise the client's parameters survived: %q", system)
	}
}

// claimedNames is every recognised name the last-resort case has to account
// for: the core set the gate wants plus the saturators the gateway would
// otherwise append under.
func claimedNames() []string {
	names := append([]string{}, anonymousCoreTools...)
	return append(names, anonymousGateSaturators...)
}

// findProtocolTool looks a tool up in the shape its protocol uses; the Chat-only
// findToolOrNil cannot read the flat Responses and Anthropic entries.
func findProtocolTool(t *testing.T, items []any, protocol wire.Protocol, name string) map[string]any {
	t.Helper()
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("tool entry is %T, want an object", item)
		}
		if anonymousToolName(protocol, entry) == name {
			return entry
		}
	}
	return nil
}
