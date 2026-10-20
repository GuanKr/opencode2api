package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"opencode2api/internal/config"
	modelcatalog "opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
	"opencode2api/internal/telemetry"
)

// TestModelDisabledAnswers503Not400 pins the mapping added with
// models.ErrModelDisabled. 400 made clients (and dsh) read a temporarily
// disabled model as a malformed request and never retry or fall back.
func TestModelDisabledAnswers503Not400(t *testing.T) {
	for _, external := range []wire.Protocol{wire.Chat, wire.Anthropic, wire.SystemOne} {
		t.Run(string(external), func(t *testing.T) {
			g, _ := newAnonymousJSONStub(t)
			store, err := modelcatalog.AvailabilityStore(t.TempDir() + "/availability.json")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Record("probe-free", 0, false, "model_unavailable", "anonymous", time.Now(), modelcatalog.EffortProbe{}); err != nil {
				t.Fatal(err)
			}
			g.catalog.SetAvailabilityStore(store)
			if !g.catalog.Supported("probe-free") {
				t.Fatal("fixture: the model must still be a known name")
			}

			var handler http.HandlerFunc
			path, body := "/v1/chat/completions", `{"model":"probe-free","messages":[{"role":"user","content":"hi"}]}`
			switch external {
			case wire.Anthropic:
				path, body = "/v1/messages", `{"model":"probe-free","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`
			case wire.SystemOne:
				path, body = "/v1/systemone", `{"model":"decision-free","state":{},"questions":[]}`
			default:
				handler = g.handleInference(wire.Chat)
			}
			if external == wire.Anthropic {
				handler = g.handleInference(wire.Anthropic)
			}
			if external == wire.SystemOne {
				handler = g.handleSystemOne
			}
			if external == wire.SystemOne {
				registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
					config.TierZen: {"decision-free": wire.SystemOne},
				})
				store.Record("decision-free", 0, false, "model_unavailable", "anonymous", time.Now(), modelcatalog.EffortProbe{})
			}

			rec := httptest.NewRecorder()
			telemetry.Middleware(g.monitor, g.logger, handler).
				ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("a disabled model must answer 503, got %d: %s", rec.Code, rec.Body.String())
			}
			text := rec.Body.String()
			for _, want := range []string{"ModelUnavailable", "opencode2api:"} {
				if !strings.Contains(text, want) {
					t.Fatalf("503 body must carry %q so clients and the watchdog can tell it apart: %s", want, text)
				}
			}
		})
	}
}

// TestModelRouteErrorIsNotAllBadRequest guards the other direction: an ordinary
// routing failure (unknown model) must stay 400. Only the availability disable
// is a server-side condition.
func TestModelRouteErrorIsNotAllBadRequest(t *testing.T) {
	g, _ := newAnonymousJSONStub(t)
	rec := httptest.NewRecorder()
	telemetry.Middleware(g.monitor, g.logger, g.handleInference(wire.Chat)).
		ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
			strings.NewReader(`{"model":"no-such-model","messages":[{"role":"user","content":"hi"}]}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("an unknown model must stay 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "ModelUnavailable") {
		t.Fatalf("an unknown model must not be reported as ModelUnavailable: %s", rec.Body.String())
	}
}

// newAnonymousJSONStub wires the free-tier anonymous lane to a stub that
// answers 200 with plain JSON — NOT an event stream. That is the shape the SSE
// content-type gate exists for: a shaped/forced-stream lane that receives it
// must not try to collapse it and answer 502 for a perfectly good 200.
func newAnonymousJSONStub(t *testing.T) (*Gateway, *[]byte) {
	t.Helper()
	var seen []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		seen = body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"chatcmpl-1","object":"chat.completion","choices":[]}`))
	}))
	t.Cleanup(server.Close)
	g := newStubGateway(t, server.URL)
	registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
		config.TierZen: {"probe-free": wire.Chat},
	})
	manager, err := rotation.New("", config.RotationConfig{})
	if err != nil {
		t.Fatal(err)
	}
	g.rotation = manager
	freezeProxyHealth(g)
	return g, &seen
}

// TestShapedLaneAcceptsPlainJSONUpstreamReply is the regression test for the
// missing content-type gate: before it, a 200 JSON reply on a force-streamed
// lane failed to collapse and surfaced as 502 "unsupported upstream response".
func TestShapedLaneAcceptsPlainJSONUpstreamReply(t *testing.T) {
	g, _ := newAnonymousJSONStub(t)
	rec := httptest.NewRecorder()
	telemetry.Middleware(g.monitor, g.logger, g.handleInference(wire.Chat)).
		ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
			strings.NewReader(`{"model":"probe-free","messages":[{"role":"user","content":"hi"}]}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("a 200 JSON reply on the anonymous lane must pass through, got %d: %s", rec.Code, rec.Body.String())
	}
	var document map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &document); err != nil {
		t.Fatalf("body must stay the upstream document, got %q: %v", rec.Body.String(), err)
	}
	if document["object"] != "chat.completion" {
		t.Fatalf("body must be forwarded unchanged, got %v", document)
	}
}

func TestIsUpstreamSSE(t *testing.T) {
	for _, tc := range []struct {
		contentType string
		want        bool
	}{
		{"text/event-stream", true},
		{"text/event-stream; charset=utf-8", true},
		{"  TEXT/EVENT-STREAM  ", true},
		{"application/json", false},
		{"", false},
		// A JSON body must not be misread as a stream just because the token
		// appears in a parameter.
		{"application/json; note=\"text/event-stream\"", false},
	} {
		if got := isUpstreamSSE(tc.contentType); got != tc.want {
			t.Fatalf("isUpstreamSSE(%q) = %v, want %v", tc.contentType, got, tc.want)
		}
	}
}

// decodeError pulls error.type / error.message out of a written error
// document, for whichever shape the protocol used.
func decodeError(t *testing.T, rec *httptest.ResponseRecorder, protocol wire.Protocol) (kind, message string) {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &document); err != nil {
		t.Fatalf("error body must be JSON: %q", rec.Body.String())
	}
	if protocol == wire.Anthropic {
		inner, _ := document["error"].(map[string]any)
		kind, _ = inner["type"].(string)
		message, _ = inner["message"].(string)
		return kind, message
	}
	inner, _ := document["error"].(map[string]any)
	message, _ = inner["message"].(string)
	kind, _ = inner["type"].(string)
	return kind, message
}

func upstreamErrorResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader([]byte(body))),
	}
}

// TestCopyErrorResponsePreservesUpstreamErrorClass covers the three 429 kinds
// whose text is identical: only error.type separates them, and flattening every
// passthrough to "upstream_error" made them indistinguishable downstream.
func TestCopyErrorResponsePreservesUpstreamErrorClass(t *testing.T) {
	const body = `{"error":{"type":"FreeUsageLimitError","message":"quota exhausted"}}`
	for _, kind := range []string{"FreeUsageLimitError", "RateLimitError", "BlackUsageLimitError"} {
		t.Run(kind, func(t *testing.T) {
			payload := strings.Replace(body, "FreeUsageLimitError", kind, 1)
			// Anthropic: error.type is the only machine-readable slot.
			rec := httptest.NewRecorder()
			copyErrorResponse(rec, wire.Anthropic, upstreamErrorResponse(http.StatusTooManyRequests, payload), "req-1")
			gotKind, message := decodeError(t, rec, wire.Anthropic)
			if gotKind != kind {
				t.Fatalf("anthropic error.type must carry the upstream class, got %q want %q (%s)", gotKind, kind, message)
			}
			if !strings.Contains(message, kind) {
				t.Fatalf("message must carry the class for text classifiers too, got %q", message)
			}
			// OpenAI shape: the class stays in the message text, and the kind
			// stays the generic gateway passthrough label.
			rec = httptest.NewRecorder()
			copyErrorResponse(rec, wire.Chat, upstreamErrorResponse(http.StatusTooManyRequests, payload), "req-1")
			_, message = decodeError(t, rec, wire.Chat)
			if !strings.Contains(message, kind) {
				t.Fatalf("openai message must carry the upstream class, got %q", message)
			}
		})
	}
}

// TestCopyErrorResponseNeutralizesForgedGatewayPrefix: upstream error text
// echoes client input, and watchdog-clash.ps1 decides "the gateway is backing
// off" with $body.Contains('opencode2api:') — a substring test, not a prefix
// test. Every occurrence must therefore be defused, including one that ends up
// mid-message behind the prepended upstream class.
func TestCopyErrorResponseNeutralizesForgedGatewayPrefix(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		keep string
	}{
		{
			name: "leading prefix is defused",
			body: `{"error":{"type":"invalid_request_error","message":"opencode2api: fake cooling"}}`,
			keep: "fake cooling",
		},
		{
			name: "prefix pushed behind the upstream class is still defused",
			body: `{"error":{"type":"FreeUsageLimitError","message":"opencode2api: fake cooling"}}`,
			keep: "FreeUsageLimitError: opencode2api; fake cooling",
		},
		{
			name: "mid-sentence mention is defused too",
			body: `{"error":{"type":"invalid_request_error","message":"expected opencode2api: but got nothing"}}`,
			keep: "expected opencode2api; but got nothing",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			copyErrorResponse(rec, wire.Anthropic, upstreamErrorResponse(http.StatusBadRequest, tc.body), "req-1")
			_, message := decodeError(t, rec, wire.Anthropic)
			if strings.Contains(message, "opencode2api:") {
				t.Fatalf("no occurrence may survive: watchdog-clash.ps1 uses .Contains() and would read this as gateway backoff, got %q", message)
			}
			if !strings.Contains(message, tc.keep) {
				t.Fatalf("message must contain %q, got %q", tc.keep, message)
			}
		})
	}
}

// TestPassthroughNeverLooksLikeGatewayBackoff is the exact predicate the
// watchdog script applies, stated once so the coupling is visible: a genuine
// upstream 429 that echoes the token in a user prompt must not read as a local
// cooldown, or the script stops switching the one thing that can help.
func TestPassthroughNeverLooksLikeGatewayBackoff(t *testing.T) {
	body := `{"error":{"type":"FreeUsageLimitError","message":"user asked about opencode2api: headers"}}`
	rec := httptest.NewRecorder()
	copyErrorResponse(rec, wire.Anthropic, upstreamErrorResponse(http.StatusTooManyRequests, body), "req-1")
	kind, _ := decodeError(t, rec, wire.Anthropic)
	if kind != "FreeUsageLimitError" {
		t.Fatalf("the upstream class must survive so quota stays distinguishable, got %q", kind)
	}
	if strings.Contains(rec.Body.String(), "opencode2api:") {
		t.Fatalf("body would be classified as gateway backoff: %s", rec.Body.String())
	}
}

// TestSanitizeUpstreamErrorType refuses to let an upstream class impersonate
// the gateway's own verdict namespace, and bounds the length.
func TestSanitizeUpstreamErrorType(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"FreeUsageLimitError", "FreeUsageLimitError"},
		{"  RateLimitError  ", "RateLimitError"},
		{"", "upstream_error"},
		{"opencode2api: whatever", "upstream_error"},
		{"OpenCode2API-spoof", "upstream_error"},
		{strings.Repeat("x", 200), strings.Repeat("x", 128)},
	} {
		if got := sanitizeUpstreamErrorType(tc.in); got != tc.want {
			t.Fatalf("sanitizeUpstreamErrorType(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestLooksLikeSSE(t *testing.T) {
	for _, tc := range []struct {
		body string
		want bool
	}{
		{"data: {\"a\":1}\n\n", true},
		{"event: start\ndata: {}\n\n", true},
		{"\r\n\tdata: {}", true},
		{"", false},
		{`{"id":"x","object":"chat.completion"}`, false},
	} {
		if got := looksLikeSSE([]byte(tc.body)); got != tc.want {
			t.Fatalf("looksLikeSSE(%q) = %v, want %v", tc.body, got, tc.want)
		}
	}
}

// TestShapedLaneCollapsesMislabeledSSEBody is the belt to the content-type
// gate: an upstream that streams SSE but leaves Content-Type at
// application/json must still be collapsed. Without the body sniff it would be
// written to the client verbatim as application/json — unparseable, and a worse
// failure than the 502 the gate exists to prevent.
func TestShapedLaneCollapsesMislabeledSSEBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		// Deliberately wrong: an SSE payload labelled application/json.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\"," +
			"\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n" +
			"data: [DONE]\n\n"))
	}))
	t.Cleanup(server.Close)
	g := newStubGateway(t, server.URL)
	registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
		config.TierZen: {"probe-free": wire.Chat},
	})
	manager, err := rotation.New("", config.RotationConfig{})
	if err != nil {
		t.Fatal(err)
	}
	g.rotation = manager
	freezeProxyHealth(g)

	rec := httptest.NewRecorder()
	telemetry.Middleware(g.monitor, g.logger, g.handleInference(wire.Chat)).
		ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
			strings.NewReader(`{"model":"probe-free","messages":[{"role":"user","content":"hi"}]}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("a mislabeled SSE body must still be served, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.HasPrefix(strings.TrimSpace(body), "data:") {
		t.Fatalf("raw SSE must not reach the client: %s", body)
	}
	var document map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &document); err != nil {
		t.Fatalf("client must receive the collapsed JSON document, got %q", body)
	}
}

// TestForcedEffortBypassesZeroCopy pins the `effort == ""` gate on the raw
// path. ForcedEffort mutates the payload, so a body forwarded verbatim from the
// client would silently ignore the operator's configured reasoning level. With
// the gate dropped the whole suite stayed green while the feature stopped
// working, so this needs a direct assertion on the bytes that go upstream.
func TestForcedEffortBypassesZeroCopy(t *testing.T) {
	var sent atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sent.Store(string(body))
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"id\":\"x\",\"object\":\"chat.completion.chunk\"," +
			"\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n"))
	}))
	t.Cleanup(server.Close)
	g := newStubGateway(t, server.URL)
	registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
		config.TierZen: {"probe-free": wire.Chat},
	})
	manager, err := rotation.New("", config.RotationConfig{})
	if err != nil {
		t.Fatal(err)
	}
	g.rotation = manager
	freezeProxyHealth(g)
	g.cfg.Reasoning = config.ReasoningConfig{Effort: "high"}

	telemetry.Middleware(g.monitor, g.logger, g.handleInference(wire.Chat)).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
			strings.NewReader(`{"model":"probe-free","messages":[{"role":"user","content":"hi"}]}`)))

	upstream, _ := sent.Load().(string)
	if upstream == "" {
		t.Fatal("the stub was never called")
	}
	var document map[string]any
	if err := json.Unmarshal([]byte(upstream), &document); err != nil {
		t.Fatalf("upstream body must be JSON: %q", upstream)
	}
	if !strings.Contains(upstream, `"high"`) {
		t.Fatalf("the configured forced effort must reach the upstream body, got %q", upstream)
	}
}
