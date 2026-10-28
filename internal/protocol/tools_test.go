package protocol

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

// newTestRenames builds the mapping a shaped free-tier request produces: every
// client tool got the clint_ prefix on the wire.
func newTestRenames() *ToolRenames {
	renames := NewToolRenames()
	renames.Add("read_file", "clint_read_file")
	return renames
}

// ---------------------------------------------------------------------------
// Name allocation
// ---------------------------------------------------------------------------

// TestClientToolWireNameSanitizes pins the character set upstreams accept and
// the fallback for a name that sanitizes to nothing.
func TestClientToolWireNameSanitizes(t *testing.T) {
	cases := map[string]string{
		"read_file":       "clint_read_file",
		"write file!":     "clint_write_file",
		"  weird//name  ": "clint_weird_name",
		"already-ok_9":    "clint_already-ok_9",
		"":                "clint_tool",
		"!!!":             "clint_tool",
		"ünïcode":         "clint_n_code",
	}
	for name, want := range cases {
		if got := ClientToolWireName(name, nil); got != want {
			t.Errorf("ClientToolWireName(%q) = %q, want %q", name, got, want)
		}
	}
}

// TestClientToolWireNameFitsTheUpstreamLimit: the name plus prefix must stay
// inside the 64-byte limit, otherwise the upstream rejects the declaration.
func TestClientToolWireNameFitsTheUpstreamLimit(t *testing.T) {
	long := strings.Repeat("a", 200)
	got := ClientToolWireName(long, nil)
	if len(got) != toolWireNameLimit {
		t.Fatalf("a long name must be truncated to %d bytes, got %d", toolWireNameLimit, len(got))
	}
	if !strings.HasPrefix(got, ClientToolPrefix) {
		t.Fatalf("truncation must keep the prefix, got %q", got)
	}
}

// TestClientToolWireNameDisambiguatesCollisions: two client names that reduce to
// the same wire name must not share it, or the response would restore the wrong
// tool.
func TestClientToolWireNameDisambiguatesCollisions(t *testing.T) {
	used := map[string]bool{}
	first := ClientToolWireName("a b", used)
	second := ClientToolWireName("a/b", used)
	third := ClientToolWireName("a.b", used)
	if first != "clint_a_b" || second != "clint_a_b_2" || third != "clint_a_b_3" {
		t.Fatalf("collisions must get _2, _3 suffixes, got %q, %q, %q", first, second, third)
	}
}

// TestToolRenamesStaysInvertible pins the map contract the response side relies
// on: one wire name per client name, and the first allocation wins.
func TestToolRenamesStaysInvertible(t *testing.T) {
	var nilRenames *ToolRenames
	if !nilRenames.Empty() || nilRenames.Count() != 0 {
		t.Fatal("a nil rename set must be empty")
	}
	if got := nilRenames.Restore("clint_x"); got != "clint_x" {
		t.Fatalf("a nil rename set has no table to restore from, got %q", got)
	}
	if _, ok := nilRenames.Wire("x"); ok {
		t.Fatal("a nil rename set must report no mapping")
	}

	renames := NewToolRenames()
	if !renames.Empty() {
		t.Fatal("a fresh set must be empty")
	}
	renames.Add("read_file", "clint_read_file")
	renames.Add("read_file", "clint_read_file") // same pair twice: no-op
	renames.Add("read_file", "clint_other")     // conflicting pair: first wins
	renames.Add("clint_read_file", "clint_read_file")
	if renames.Count() != 1 {
		t.Fatalf("want one mapping, got %d", renames.Count())
	}
	if wire, ok := renames.Wire("read_file"); !ok || wire != "clint_read_file" {
		t.Fatalf("Wire(read_file) = %q, %v", wire, ok)
	}
	if client, ok := renames.Client("clint_read_file"); !ok || client != "read_file" {
		t.Fatalf("Client(clint_read_file) = %q, %v", client, ok)
	}
	if got := renames.Restore("clint_read_file"); got != "read_file" {
		t.Fatalf("Restore must use the exact mapping, got %q", got)
	}
	if got := renames.Restore("clint_something_else"); got != "something_else" {
		t.Fatalf("Restore must strip the prefix when the mapping is gone, got %q", got)
	}
	if got := renames.Restore("plain"); got != "plain" {
		t.Fatalf("Restore must leave an unprefixed name alone, got %q", got)
	}
	if !renames.HasPrefix(ClientToolPrefix) || !renames.HasLongerPrefix("clint_") {
		t.Fatal("HasPrefix/HasLongerPrefix must see the allocated names")
	}
	if renames.HasLongerPrefix("clint_read_file") {
		t.Fatal("an exact name is not a longer prefix of itself")
	}
}

// ---------------------------------------------------------------------------
// Response rewriting (non-streaming)
// ---------------------------------------------------------------------------

// TestRenameResponseToolChat covers both places a Chat reply can name a tool:
// the modern tool_calls list (message and delta) and the legacy function_call.
func TestRenameResponseToolChat(t *testing.T) {
	body := []byte(`{"choices":[` +
		`{"message":{"tool_calls":[{"id":"c1","type":"function","function":{"name":"clint_read_file","arguments":"{}"}}],` +
		`"function_call":{"name":"clint_read_file","arguments":"{}"}}},` +
		`{"delta":{"tool_calls":[{"index":0,"function":{"name":"clint_read_file"}}]}}]}`)
	out := RenameResponseTool(body, Chat, newTestRenames())
	if strings.Contains(string(out), "clint_") {
		t.Fatalf("the wire name survived the rewrite: %s", out)
	}
	payload := decodePayload(t, out)
	choices := payload["choices"].([]any)
	first := choices[0].(map[string]any)
	message := first["message"].(map[string]any)
	call := message["tool_calls"].([]any)[0].(map[string]any)["function"].(map[string]any)
	if call["name"] != "read_file" {
		t.Fatalf("message tool call name = %v", call["name"])
	}
	legacy := message["function_call"].(map[string]any)
	if legacy["name"] != "read_file" {
		t.Fatalf("legacy function_call name = %v", legacy["name"])
	}
	delta := choices[1].(map[string]any)["delta"].(map[string]any)
	deltaCall := delta["tool_calls"].([]any)[0].(map[string]any)["function"].(map[string]any)
	if deltaCall["name"] != "read_file" {
		t.Fatalf("delta tool call name = %v", deltaCall["name"])
	}
}

// TestRenameResponseToolAnthropic covers both Anthropic shapes: a streaming
// content_block and a complete message content list.
func TestRenameResponseToolAnthropic(t *testing.T) {
	block := []byte(`{"type":"content_block_start","content_block":{"type":"tool_use","id":"toolu_1","name":"clint_read_file","input":{}}}`)
	out := RenameResponseTool(block, Anthropic, newTestRenames())
	if !strings.Contains(string(out), `"name":"read_file"`) {
		t.Fatalf("content_block name was not restored: %s", out)
	}

	message := []byte(`{"type":"message","content":[{"type":"text","text":"hi"},{"type":"tool_use","id":"toolu_1","name":"clint_read_file","input":{}}]}`)
	out = RenameResponseTool(message, Anthropic, newTestRenames())
	if !strings.Contains(string(out), `"name":"read_file"`) {
		t.Fatalf("content tool_use name was not restored: %s", out)
	}
}

// TestRenameResponseToolResponses covers the three envelopes Responses uses and
// leaves non-tool items alone even when they carry a name-ish field.
func TestRenameResponseToolResponses(t *testing.T) {
	cases := []string{
		`{"type":"response.output_item.added","item":{"type":"function_call","name":"clint_read_file"}}`,
		`{"type":"response.output_item.added","item":{"type":"custom_tool_call","name":"clint_read_file"}}`,
		`{"output":[{"type":"function_call","name":"clint_read_file"}]}`,
		`{"response":{"output":[{"type":"function_call","name":"clint_read_file"}]}}`,
	}
	for _, body := range cases {
		out := RenameResponseTool([]byte(body), Responses, newTestRenames())
		if !strings.Contains(string(out), `"name":"read_file"`) {
			t.Fatalf("Responses tool name was not restored in %s: %s", body, out)
		}
	}

	// A "message" item is not a tool call; its name must not be rewritten and
	// the bytes must come back untouched.
	untouched := []byte(`{"item":{"type":"message","name":"clint_read_file"}}`)
	if out := RenameResponseTool(untouched, Responses, newTestRenames()); !bytes.Equal(out, untouched) {
		t.Fatalf("a non-tool item was rewritten: %s", out)
	}
}

// TestRenameResponseToolFastPaths: the rewrite must be free and byte-identical
// when there is nothing to do, so the relay path does not re-encode replies that
// never carried an alias.
func TestRenameResponseToolFastPaths(t *testing.T) {
	plain := []byte(`{"choices":[{"message":{"content":"hello"}}]}`)
	if out := RenameResponseTool(plain, Chat, newTestRenames()); !bytes.Equal(out, plain) {
		t.Fatalf("a reply without an alias was rewritten: %s", out)
	}
	if out := RenameResponseTool(plain, Chat, nil); !bytes.Equal(out, plain) {
		t.Fatalf("a nil rename set must return the body unchanged: %s", out)
	}
	unknown := []byte(`{"choices":[{"message":{"tool_calls":[{"function":{"name":"clint_unknown"}}]}}]}`)
	if out := RenameResponseTool(unknown, Chat, newTestRenames()); !strings.Contains(string(out), `"name":"unknown"`) {
		t.Fatalf("an unmapped clint_ name must fall back to its stripped client name: %s", out)
	}
	broken := []byte(`{"choices":[{"function":{"name":"clint_read_file"}`)
	if out := RenameResponseTool(broken, Chat, newTestRenames()); !bytes.Equal(out, broken) {
		t.Fatalf("unparseable bytes must be returned unchanged: %s", out)
	}
}

// TestRenameResponseToolFoldsAnUnprefixedName pins the response-side fold added
// upstream on 2026-10-26: a model that names an official tool without the clint_
// prefix is restored onto the declared client tool when exactly one folds to it.
func TestRenameResponseToolFoldsAnUnprefixedName(t *testing.T) {
	renames := NewToolRenames()
	renames.Add("todo_write", "clint_todo_write")
	for _, sent := range []string{"todowrite", "TODO-WRITE", "todo.write"} {
		body := []byte(`{"choices":[{"message":{"tool_calls":[{"function":{"name":"` + sent + `"}}]}}]}`)
		out := RenameResponseTool(body, Chat, renames)
		if !strings.Contains(string(out), `"name":"todo_write"`) {
			t.Fatalf("%q was not folded onto the declared tool: %s", sent, out)
		}
	}
}

// TestResolveNeverGuessesAnAmbiguousFold: two declared tools folding to one key
// must leave the upstream's spelling alone rather than pick one.
func TestResolveNeverGuessesAnAmbiguousFold(t *testing.T) {
	renames := NewToolRenames()
	renames.Add("todo_write", "clint_todo_write")
	renames.Add("todo-write", "clint_todo-write")
	if got, changed := renames.Resolve("todowrite"); changed || got != "todowrite" {
		t.Fatalf("an ambiguous fold must be left alone, got %q (changed=%v)", got, changed)
	}
}

// ---------------------------------------------------------------------------
// Stream rewriting
// ---------------------------------------------------------------------------

// TestRenameStreamReaderReassemblesASplitChatName is the case the fragment
// logic exists for: Chat deltas may deliver the tool name a piece at a time, and
// the held-back prefix must not leak downstream while the middle frames may not
// carry a partial name either.
func TestRenameStreamReaderReassemblesASplitChatName(t *testing.T) {
	stream := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"clint_\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"read_\"}}]}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"file\",\"arguments\":\"{}\"}}]}}]}\n\n" +
		"data: [DONE]\n\n"

	out, err := io.ReadAll(RenameStreamReader(strings.NewReader(stream), Chat, newTestRenames()))
	if err != nil {
		t.Fatal(err)
	}
	text := string(out)
	if strings.Contains(text, "clint_") {
		t.Fatalf("a wire-name fragment leaked downstream: %s", text)
	}
	if strings.Contains(text, `"name":"read_"`) || strings.Contains(text, `"name":"file"`) {
		t.Fatalf("a partial name leaked downstream: %s", text)
	}
	if count := strings.Count(text, `"name":"read_file"`); count != 1 {
		t.Fatalf("want the restored name exactly once, got %d:\n%s", count, text)
	}
	if !strings.HasSuffix(text, "data: [DONE]\n\n") {
		t.Fatalf("the terminal frame must pass through unchanged: %s", text)
	}
}

// TestRenameStreamReaderPassesAnUnmappedNameThrough: a clint_ name the map does
// not know (an upstream-invented tool) must be forwarded rather than swallowed
// by the hold-back logic; the prefix is stripped as a best-effort recovery.
func TestRenameStreamReaderPassesAnUnmappedNameThrough(t *testing.T) {
	stream := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"clint_other\"}}]}}]}\n\n"
	out, err := io.ReadAll(RenameStreamReader(strings.NewReader(stream), Chat, newTestRenames()))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"name":"other"`) {
		t.Fatalf("an unmapped clint_ name must fall back to its stripped client name: %s", out)
	}
}

// TestRenameStreamReaderNonChatUsesTheObjectRewrite: Anthropic and Responses
// name a tool in a single frame, so the stateless rewrite applies and every
// other line — including CRLF terminators and non-data lines — stays byte-equal.
func TestRenameStreamReaderNonChatUsesTheObjectRewrite(t *testing.T) {
	stream := "event: content_block_start\r\n" +
		"data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"clint_read_file\"}}\r\n\r\n" +
		"data: {\"type\":\"message_stop\"}\r\n\r\n"

	out, err := io.ReadAll(RenameStreamReader(strings.NewReader(stream), Anthropic, newTestRenames()))
	if err != nil {
		t.Fatal(err)
	}
	text := string(out)
	if !strings.Contains(text, `"name":"read_file"`) {
		t.Fatalf("the tool name was not restored: %s", text)
	}
	if !strings.HasPrefix(text, "event: content_block_start\r\n") {
		t.Fatalf("a non-data line must keep its bytes and terminator: %q", text)
	}
	if !strings.HasSuffix(text, "data: {\"type\":\"message_stop\"}\r\n\r\n") {
		t.Fatalf("an untouched frame must keep its bytes and terminator: %q", text)
	}
	if strings.Contains(text, "clint_") {
		t.Fatalf("the wire name survived the rewrite: %s", text)
	}
}

// TestRenameStreamReaderWithoutRenamesIsAnIdentity: an empty mapping must not
// wrap the reader at all, so the common paid-model path pays nothing.
func TestRenameStreamReaderWithoutRenamesIsAnIdentity(t *testing.T) {
	reader := strings.NewReader("data: {}\n\n")
	if got := RenameStreamReader(reader, Chat, nil); got != io.Reader(reader) {
		t.Fatal("an empty rename set must return the reader unchanged")
	}
	if got := RenameStreamReader(reader, Chat, NewToolRenames()); got != io.Reader(reader) {
		t.Fatal("an empty rename set must return the reader unchanged")
	}
}

// TestRenameStreamReaderPreservesUnparseableFrames: a malformed frame is passed
// through, because dropping or truncating it would corrupt a stream the client
// is already reading.
func TestRenameStreamReaderPreservesUnparseableFrames(t *testing.T) {
	stream := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"function\":{\"name\":\"clint_read_file\"}}]}}\n\n"
	out, err := io.ReadAll(RenameStreamReader(strings.NewReader(stream), Chat, newTestRenames()))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != stream {
		t.Fatalf("an unparseable frame must pass through verbatim:\nwant: %q\ngot:  %q", stream, out)
	}
}

// TestRenameStreamReaderHandlesChunkBoundaries: the reader is built on bufio and
// one-line buffering, so a frame split across Read calls must still be rewritten
// as one unit. Feeding one byte at a time through io.Reader exercises the
// boundary handling without a pipe.
func TestRenameStreamReaderHandlesChunkBoundaries(t *testing.T) {
	stream := "data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"name\":\"clint_read_file\"}}\n\n"
	out, err := io.ReadAll(RenameStreamReader(&oneByteReader{data: []byte(stream)}, Anthropic, newTestRenames()))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"name":"read_file"`) {
		t.Fatalf("a frame delivered one byte at a time was not rewritten: %s", out)
	}
}

// oneByteReader yields its data one byte per Read, which forces bufio to stitch
// the frame together.
type oneByteReader struct {
	data []byte
}

func (reader *oneByteReader) Read(p []byte) (int, error) {
	if len(reader.data) == 0 {
		return 0, io.EOF
	}
	p[0] = reader.data[0]
	reader.data = reader.data[1:]
	return 1, nil
}

// TestRenameResponseToolKeepsJSONValid guards the encoding step: the rewritten
// document must still parse after the name swap.
func TestRenameResponseToolKeepsJSONValid(t *testing.T) {
	body := []byte(`{"choices":[{"message":{"content":"hi","tool_calls":[{"id":"c1","function":{"name":"clint_read_file","arguments":"{\"a\":1}"}}]}}]}`)
	out := RenameResponseTool(body, Chat, newTestRenames())
	var payload map[string]any
	if err := json.Unmarshal(out, &payload); err != nil {
		t.Fatalf("the rewritten response is not valid JSON: %v\n%s", err, out)
	}
}
