package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
	"opencode2api/internal/telemetry"
)

// usageShapeStub answers every upstream call with a minimal event stream. The
// anonymous lane forces stream:true on the wire and aggregates the reply back
// for non-streaming clients, so one SSE reply exercises both client shapes.
func usageShapeStub(t *testing.T) *Gateway {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
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
	// handleInference consults the group-rotation manager before routing. An
	// empty path keeps it memory-only (the same call gateway.go:67 makes).
	manager, err := rotation.New("", config.RotationConfig{})
	if err != nil {
		t.Fatal(err)
	}
	g.rotation = manager
	freezeProxyHealth(g)
	return g
}

// registerUsageModels publishes the free-tier test models. A model needs a
// declared native protocol once the catalog is non-empty: without one the
// catalog treats it as still-pending and routing rejects it.
func registerUsageModels(t *testing.T, g *Gateway, native map[config.Tier]map[string]wire.Protocol) {
	t.Helper()
	models := make([]string, 0, len(native[config.TierZen]))
	for model := range native[config.TierZen] {
		models = append(models, model)
	}
	g.catalog.ReplaceWithCapabilities(models, nil, native, nil, nil)
}

// recordedRequest drives one client request through the given gateway handler
// and returns the single entry it left on the monitor's request ring. The path
// matters: telemetry records it as the request's 端点 column.
func recordedRequest(t *testing.T, g *Gateway, path string, handler http.HandlerFunc, body string) *telemetry.UpstreamRequest {
	t.Helper()
	logger := g.logger
	wrapped := telemetry.Middleware(g.monitor, logger, handler)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)
	requests := g.monitor.Snapshot().Upstream.Requests
	if len(requests) != 1 {
		t.Fatalf("expected exactly 1 recorded request, got %d (status %d: %s)",
			len(requests), rec.Code, rec.Body.String())
	}
	return &requests[0]
}

// TestHandleInferenceRecordsClientShapeFields covers the gateway wiring that
// feeds the usage page's 推理强度 and 流式 columns. The telemetry tests
// hand-build RequestMeta, so they cannot catch this: deleting these two
// assignments in handleInference leaves the rest of the suite green, which is
// exactly how the fields were lost in the first place.
func TestHandleInferenceRecordsClientShapeFields(t *testing.T) {
	cases := []struct {
		name       string
		external   wire.Protocol
		path       string
		body       string
		wantStream bool
		wantEffort string
	}{
		{
			name:       "chat streaming with effort",
			external:   wire.Chat,
			path:       "/v1/chat/completions",
			body:       `{"model":"probe-free","stream":true,"reasoning_effort":"high","messages":[{"role":"user","content":"hi"}]}`,
			wantStream: true,
			wantEffort: "high",
		},
		{
			name:       "chat non-streaming keeps the client shape",
			external:   wire.Chat,
			path:       "/v1/chat/completions",
			body:       `{"model":"probe-free","stream":false,"reasoning_effort":"medium","messages":[{"role":"user","content":"hi"}]}`,
			wantStream: false,
			wantEffort: "medium",
		},
		{
			name:       "anthropic budget maps to a label",
			external:   wire.Anthropic,
			path:       "/v1/messages",
			body:       `{"model":"probe-free","stream":true,"max_tokens":64,"thinking":{"type":"enabled","budget_tokens":8192},"messages":[{"role":"user","content":"hi"}]}`,
			wantStream: true,
			wantEffort: "high",
		},
		{
			name:       "no reasoning setting records an empty label",
			external:   wire.Chat,
			path:       "/v1/chat/completions",
			body:       `{"model":"probe-free","messages":[{"role":"user","content":"hi"}]}`,
			wantStream: false,
			wantEffort: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := usageShapeStub(t)
			gateway := g.handleInference(tc.external)
			// telemetry.Middleware installs the RequestMeta the handler reads
			// and records it afterwards, exactly as the real server does.
			request := recordedRequest(t, g, tc.path, gateway, tc.body)
			if request.Stream == nil || *request.Stream != tc.wantStream {
				t.Fatalf("stream column must mirror the client request, got %v want %v (%+v)",
					request.Stream, tc.wantStream, request)
			}
			if request.ReasoningEffort != tc.wantEffort {
				t.Fatalf("effort column must mirror the client request, got %q want %q (%+v)",
					request.ReasoningEffort, tc.wantEffort, request)
			}
			if request.Endpoint != tc.path {
				t.Fatalf("endpoint column must carry the inbound path, got %q want %q", request.Endpoint, tc.path)
			}
		})
	}
}

// TestHandleSystemOneRecordsClientShapeFields pins the dedicated System One
// endpoint. It has its own handler, so it needs its own recording: before this
// it set only meta.Model and every column but 对话ID stayed blank for traffic
// arriving here rather than on the three message endpoints.
func TestHandleSystemOneRecordsClientShapeFields(t *testing.T) {
	g := usageShapeStub(t)
	registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
		config.TierZen: {"probe-free": wire.Chat, "decision-free": wire.SystemOne},
	})
	body := `{"model":"decision-free","stream":true,"reasoning_effort":"high","state":{},"questions":[]}`
	request := recordedRequest(t, g, "/v1/systemone", g.handleSystemOne, body)
	if request.Stream == nil || !*request.Stream {
		t.Fatalf("systemone must record the requested stream shape, got %+v", request)
	}
	if request.ReasoningEffort != "high" {
		t.Fatalf("systemone must record the requested effort, got %+v", request)
	}
	if request.Endpoint != "/v1/systemone" {
		t.Fatalf("endpoint column must carry the systemone path, got %q", request.Endpoint)
	}
}
