package telemetry

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"opencode2api/internal/protocol"
)

// TestReasoningEffortLabel pins the telemetry-only display label across the
// three inbound protocols. The label feeds the usage page's 推理强度 column and
// its filter; it never affects routing or the wire body.
func TestReasoningEffortLabel(t *testing.T) {
	cases := []struct {
		protocol protocol.Protocol
		payload  map[string]any
		want     string
	}{
		{protocol.Chat, map[string]any{"reasoning_effort": "high"}, "high"},
		{protocol.Chat, map[string]any{"reasoning_effort": " High "}, "high"},
		{protocol.Chat, map[string]any{"reasoning_effort": "X-High"}, "xhigh"},
		{protocol.Responses, map[string]any{"reasoning": "Extra_High"}, "extrahigh"},
		{protocol.Anthropic, map[string]any{"thinking": "extra high"}, "extrahigh"},
		{protocol.Responses, map[string]any{"reasoning": map[string]any{"effort": "medium"}}, "medium"},
		{protocol.Responses, map[string]any{"reasoning": "low"}, "low"},
		{protocol.Anthropic, map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 8192}}, "high"},
		{protocol.Anthropic, map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 1024}}, "low"},
		{protocol.Anthropic, map[string]any{"thinking": map[string]any{"type": "enabled"}}, ""},
		{protocol.Anthropic, map[string]any{"thinking": map[string]any{"type": "adaptive"}}, ""},
		{protocol.Anthropic, map[string]any{"thinking": map[string]any{"type": "disabled"}}, ""},
		{protocol.Chat, map[string]any{"reasoning_effort": "none"}, ""},
		{protocol.Chat, map[string]any{"reasoning_effort": "minimal"}, "minimal"},
		{protocol.Chat, map[string]any{}, ""},
		{protocol.Chat, nil, ""},
	}
	for _, tc := range cases {
		if got := protocol.ReasoningEffortLabel(tc.protocol, tc.payload); got != tc.want {
			t.Fatalf("protocol %q payload %v: expected %q, got %q", tc.protocol, tc.payload, tc.want, got)
		}
	}
}

// TestRecordCarriesRequestShapeFields pins that Record copies the client-shaped
// fields the usage page renders (端点 / 流式 / 推理强度 / 首字) onto the request
// ring. These were silently dropped when the usage page was ported onto the
// upstream baseline, which is what left those four columns permanently empty.
func TestRecordCarriesRequestShapeFields(t *testing.T) {
	monitor := Monitor()
	monitor.Record("/v1/responses", 200, 1200*time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_shape", Channel: "key",
		Stream: true, ReasoningEffort: "high", FirstByteMS: 180,
		UsageReported: true, Usage: protocol.Usage{Input: 100, Output: 50, Total: 150},
	})
	snapshot := monitor.Snapshot()
	if len(snapshot.Upstream.Requests) != 1 {
		t.Fatalf("expected 1 upstream request, got %d", len(snapshot.Upstream.Requests))
	}
	request := snapshot.Upstream.Requests[0]
	if request.Endpoint != "/v1/responses" {
		t.Fatalf("inbound endpoint must be recorded, got %q", request.Endpoint)
	}
	if request.Stream == nil || !*request.Stream {
		t.Fatalf("stream flag must be recorded as an explicit true, got %+v", request)
	}
	if request.ReasoningEffort != "high" {
		t.Fatalf("per-request reasoning effort must be recorded, got %+v", request)
	}
	if request.FirstByteMS != 180 {
		t.Fatalf("per-request first byte latency must be recorded, got %+v", request)
	}
	if request.DurationMS != 1200 {
		t.Fatalf("per-request duration must be recorded, got %+v", request)
	}
}

// TestMiddlewareRecordsFirstByteForStreamsOnly exercises the real measurement
// rather than a hand-set meta value: a stream that sleeps before its first
// write must report a non-zero TTFT, while a buffered non-stream response of
// the same shape must report 0 so the usage page shows the total instead of a
// bogus ~0s. Dropping either half of the meta.Stream guard turns this red.
func TestMiddlewareRecordsFirstByteForStreamsOnly(t *testing.T) {
	const upstreamDelay = 30 * time.Millisecond
	for _, tc := range []struct {
		name       string
		stream     bool
		wantFirstB bool
	}{
		{"stream", true, true},
		{"non-stream", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			monitor := Monitor()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			handler := Middleware(monitor, logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(upstreamDelay)
				_, _ = w.Write([]byte("ok"))
			}))
			request := httptest.NewRequest(http.MethodPost, "/v1/responses", http.NoBody)
			meta := &RequestMeta{
				Model: "model-a", Tier: "zen", Request: "req_ttft", Channel: "key",
				Stream: tc.stream, UsageReported: true,
				Usage: protocol.Usage{Input: 10, Output: 5, Total: 15},
			}
			handler.ServeHTTP(httptest.NewRecorder(), request.WithContext(WithRequestMeta(request.Context(), meta)))

			snapshot := monitor.Snapshot()
			if len(snapshot.Upstream.Requests) != 1 {
				t.Fatalf("expected 1 upstream request, got %d", len(snapshot.Upstream.Requests))
			}
			got := snapshot.Upstream.Requests[0].FirstByteMS
			if tc.wantFirstB {
				if got < int64(upstreamDelay.Milliseconds()) {
					t.Fatalf("stream TTFT must cover the delay before first write, got %dms", got)
				}
			} else if got != 0 {
				t.Fatalf("non-stream response must not report a TTFT, got %dms", got)
			}
			// The total is recorded either way: the usage page subtracts the
			// first byte from it, so a lost duration would be just as visible.
			if snapshot.Upstream.Requests[0].DurationMS < int64(upstreamDelay.Milliseconds()) {
				t.Fatalf("total duration must still cover the upstream delay, got %+v", snapshot.Upstream.Requests[0])
			}
		})
	}
}

// TestRequestShapeFieldsSurviveHistoryRoundTrip pins that the four fields also
// cross the usage-history file. The detail ring is persisted so the table
// survives restarts; losing them here would empty the columns again after a
// restart even though live traffic looked fine.
func TestRequestShapeFieldsSurviveHistoryRoundTrip(t *testing.T) {
	monitor := Monitor()
	monitor.Record("/v1/messages", 200, 900*time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_round", Channel: "key",
		Stream: true, ReasoningEffort: "max", FirstByteMS: 250,
		UsageReported: true, Usage: protocol.Usage{Input: 10, Output: 5, Total: 15},
	})
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	restored := Monitor()
	if _, err := restored.LoadUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	result, err := restored.UsageRangeCount("hour", 24, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Requests) != 1 {
		t.Fatalf("expected 1 restored request, got %d", len(result.Requests))
	}
	request := result.Requests[0]
	if request.Endpoint != "/v1/messages" || request.Stream == nil || !*request.Stream ||
		request.ReasoningEffort != "max" || request.FirstByteMS != 250 {
		t.Fatalf("restored request lost its shape fields, got %+v", request)
	}
}

// TestStreamShapeKeepsAllThreeStates pins the distinction the usage page's
// 流式 column renders: 流式 / 同步 / —. A row persisted before the field existed
// must stay unmarked after Load (the JS treats an absent key as "unknown"),
// while a freshly recorded sync request must emit an explicit false. A plain
// bool collapses the first case into the second and silently mislabels up to
// 24h of older history.
func TestStreamShapeKeepsAllThreeStates(t *testing.T) {
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	legacy := `{"version":1,"hours":[],"recent_requests":[{"time":"` + stamp + `",` +
		`"request_id":"req_legacy","model":"model-a","tier":"zen","channel":"anonymous",` +
		`"status":200,"success":true,"duration_ms":1000}]}`
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	monitor := Monitor()
	if _, err := monitor.LoadUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	// A fresh sync request on top of the legacy row, so one query sees both.
	monitor.Record("/v1/chat/completions", 200, 800*time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_fresh_sync", Channel: "anonymous",
		UsageReported: true, Usage: protocol.Usage{Input: 10, Output: 5, Total: 15},
	})
	result, err := monitor.UsageRangeCount("hour", 24, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, request := range result.Requests {
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		got[request.RequestID] = string(raw)
	}
	if strings.Contains(got["req_legacy"], `"stream"`) {
		t.Fatalf("a row recorded before the field existed must stay unmarked, got %s", got["req_legacy"])
	}
	if !strings.Contains(got["req_fresh_sync"], `"stream":false`) {
		t.Fatalf("a recorded sync request must emit an explicit false, got %s", got["req_fresh_sync"])
	}
	if !strings.Contains(got["req_fresh_sync"], `"endpoint":"/v1/chat/completions"`) {
		t.Fatalf("endpoint must survive alongside, got %s", got["req_fresh_sync"])
	}
}
