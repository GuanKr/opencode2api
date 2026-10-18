package protocol

import (
	"encoding/json"
	"testing"

	"opencode2api/internal/jsonutil"
)

func decodePayload(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

// TestPrepareRequestBytesReusesUntouchedChat: an untouched same-protocol
// payload must reuse the client bytes verbatim — no CloneMap round-trip (which
// also reshapes large numbers past 2^53), no re-marshal. The bignum field is
// the observable proof: a decode/encode cycle would come back as 9007199254740992.
func TestPrepareRequestBytesReusesUntouchedChat(t *testing.T) {
	raw := []byte(`{"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}],"bignum":9007199254740993}`)
	out, err := PrepareRequestBytes(Chat, Chat, decodePayload(t, raw), raw, "https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(raw) {
		t.Fatalf("untouched payload must reuse raw bytes verbatim, got %s", out)
	}
}

// A same-protocol payload that needs the reasoning repair must be re-encoded
// with the placeholder injected, never silently passed through.
func TestPrepareRequestBytesRepairsChatToolReasoning(t *testing.T) {
	raw := []byte(`{"model":"deepseek-chat","messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"t","arguments":"{}"}}]}]}`)
	out, err := PrepareRequestBytes(Chat, Chat, decodePayload(t, raw), raw, "https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	if string(out) == string(raw) {
		t.Fatal("a repaired payload must not reuse raw bytes")
	}
	repaired := decodePayload(t, out)
	msgs := jsonutil.SliceAt(repaired, "messages")
	if len(msgs) != 1 || jsonutil.StringAt(msgs[0].(map[string]any), "reasoning_content") == "" {
		t.Fatalf("expected injected reasoning_content, got %s", out)
	}
}

// Cross-protocol requests still go through the bridge and re-encode.
func TestPrepareRequestBytesTranscodesAnthropic(t *testing.T) {
	raw := []byte(`{"model":"claude-x","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`)
	out, err := PrepareRequestBytes(Anthropic, Chat, decodePayload(t, raw), raw, "https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	transcoded := decodePayload(t, out)
	if jsonutil.StringAt(transcoded["messages"].([]any)[0].(map[string]any), "role") != "user" {
		t.Fatalf("expected transcoded chat messages, got %s", out)
	}
}

// TestPrepareRequestBytesFloorsResponsesBudget guards the one same-protocol
// rewrite upstream added after the zero-copy path was designed: a Responses
// body whose max_output_tokens is below the upstream minimum must be rewritten,
// so forwarding the client's bytes verbatim would send an invalid request.
func TestPrepareRequestBytesFloorsResponsesBudget(t *testing.T) {
	for _, tc := range []struct {
		name  string
		raw   string
		reuse bool
		want  float64
	}{
		{"below the minimum is rewritten", `{"model":"gpt-x","max_output_tokens":8,"input":[]}`, false, minResponsesMaxOutputTokens},
		{"already valid keeps the bytes", `{"model":"gpt-x","max_output_tokens":4096,"input":[]}`, true, 4096},
		{"absent keeps the bytes", `{"model":"gpt-x","input":[]}`, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(tc.raw)
			out, err := PrepareRequestBytes(Responses, Responses, decodePayload(t, raw), raw, "https://example.com")
			if err != nil {
				t.Fatal(err)
			}
			if reused := string(out) == string(raw); reused != tc.reuse {
				t.Fatalf("raw reuse = %v, want %v (out %s)", reused, tc.reuse, out)
			}
			if n, ok := decodePayload(t, out)["max_output_tokens"].(float64); ok && n != tc.want {
				t.Fatalf("max_output_tokens = %v, want %v", n, tc.want)
			}
		})
	}
}

// A nil raw must always take the re-encode path, so a caller that never
// captured the client bytes cannot accidentally forward something else.
func TestPrepareRequestBytesNilRawAlwaysReencodes(t *testing.T) {
	raw := []byte(`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`)
	out, err := PrepareRequestBytes(Chat, Chat, decodePayload(t, raw), nil, "https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	if string(out) == string(raw) {
		t.Fatal("nil raw must never produce the client's bytes")
	}
	if jsonutil.StringAt(decodePayload(t, out), "model") != "gpt-x" {
		t.Fatalf("re-encode must preserve the model, got %s", out)
	}
}
