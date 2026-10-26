package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
	"opencode2api/internal/telemetry"
	"opencode2api/internal/warp"
)

// blockingReadCloser parks Read until Close. It gives the idle-reader tests a
// deterministic blocked read instead of a socket timeout.
type blockingReadCloser struct {
	started    chan struct{}
	closed     chan struct{}
	startOnce  sync.Once
	closeOnce  sync.Once
	closeCalls atomic.Int32
}

func newBlockingReadCloser() *blockingReadCloser {
	return &blockingReadCloser{started: make(chan struct{}), closed: make(chan struct{})}
}

func (b *blockingReadCloser) Read([]byte) (int, error) {
	b.startOnce.Do(func() { close(b.started) })
	<-b.closed
	return 0, io.ErrClosedPipe
}

func (b *blockingReadCloser) Close() error {
	b.closeCalls.Add(1)
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func TestIdleReadCloserReturnsTimeout(t *testing.T) {
	body := newBlockingReadCloser()
	var timedOut atomic.Bool
	reader := newIdleReadCloser(body, 20*time.Millisecond, func() { timedOut.Store(true) })

	start := time.Now()
	n, err := reader.Read(make([]byte, 8))
	if !errors.Is(err, wire.ErrStreamIdleTimeout) {
		t.Fatalf("Read error = %v, want ErrStreamIdleTimeout", err)
	}
	if n != 0 {
		t.Fatalf("Read returned %d bytes with the timeout, want 0", n)
	}
	if !timedOut.Load() {
		t.Fatal("onTimeout must run before the body is closed")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("idle Read took %s, want about 20ms", elapsed)
	}
	if got := body.closeCalls.Load(); got != 1 {
		t.Fatalf("body Close calls = %d, want 1", got)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := body.closeCalls.Load(); got != 1 {
		t.Fatalf("body Close calls after idempotent Close = %d, want 1", got)
	}
}

// Close while Read is parked must unblock Read. This is the client-disconnect
// path: waiting for a timer callback that Close already stopped would hang the
// handler forever.
func TestIdleReadCloserCloseUnblocksRead(t *testing.T) {
	body := newBlockingReadCloser()
	var timedOut atomic.Bool
	reader := newIdleReadCloser(body, time.Hour, func() { timedOut.Store(true) })

	result := make(chan error, 1)
	go func() {
		_, err := reader.Read(make([]byte, 8))
		result <- err
	}()
	<-body.started

	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-result:
		if errors.Is(err, wire.ErrStreamIdleTimeout) {
			t.Fatalf("Close reported an idle timeout, got %v", err)
		}
		if err == nil {
			t.Fatal("expected the body close error")
		}
	case <-time.After(time.Second):
		t.Fatal("Read stayed blocked after Close")
	}
	if timedOut.Load() {
		t.Fatal("Close must not mark the stream as timed out")
	}
	if got := body.closeCalls.Load(); got != 1 {
		t.Fatalf("body Close calls = %d, want 1", got)
	}
}

type recordingWarpSink struct {
	mu      sync.Mutex
	lease   *warp.Lease
	reason  string
	reasons []string
	calls   int
}

func (s *recordingWarpSink) Fail(lease *warp.Lease, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lease = lease
	s.reason = reason
	s.reasons = append(s.reasons, reason)
	s.calls++
}

func (s *recordingWarpSink) snapshot() (int, *warp.Lease, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.lease, s.reason
}

func (s *recordingWarpSink) reasonsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.reasons...)
}

func TestNewStreamIdleBodyRetiresWarpExitOnIdle(t *testing.T) {
	sink := &recordingWarpSink{}
	lease := new(warp.Lease)
	body := &warpBody{
		ReadCloser: newBlockingReadCloser(),
		lease: &warpLease{
			lease:   lease,
			pool:    sink,
			regular: &http.Client{},
			bun:     &http.Client{},
		},
	}

	reader := newStreamIdleBody(body, 20*time.Millisecond)
	if _, err := reader.Read(make([]byte, 8)); !errors.Is(err, wire.ErrStreamIdleTimeout) {
		t.Fatalf("Read error = %v, want ErrStreamIdleTimeout", err)
	}
	calls, gotLease, _ := sink.snapshot()
	if calls == 0 || gotLease != lease {
		t.Fatalf("Fail calls=%d lease=%p, want at least one call for %p", calls, gotLease, lease)
	}
	reasons := sink.reasonsSnapshot()
	if len(reasons) == 0 || reasons[0] != "stream idle timeout" {
		t.Fatalf("Fail reasons = %v, want stream idle timeout first", reasons)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestWarpStreamFailureRetiresExit(t *testing.T) {
	sink := &recordingWarpSink{}
	lease := new(warp.Lease)
	body := &warpBody{
		ReadCloser: io.NopCloser(strings.NewReader("")),
		lease:      &warpLease{lease: lease, pool: sink},
	}

	fail := warpStreamFailure(body)
	if fail == nil {
		t.Fatal("a *warpBody must yield a failure callback")
	}
	fail()
	calls, gotLease, reason := sink.snapshot()
	if calls != 1 || gotLease != lease || reason != "stream idle timeout" {
		t.Fatalf("Fail calls=%d lease=%p reason=%q, want 1/%p/stream idle timeout", calls, gotLease, reason, lease)
	}

	body.fail("stream read error")
	calls, _, reason = sink.snapshot()
	if calls != 2 || reason != "stream read error" {
		t.Fatalf("direct fail calls=%d reason=%q, want 2/stream read error", calls, reason)
	}

	diagnostic := &warpBody{
		ReadCloser: io.NopCloser(strings.NewReader("")),
		lease:      &warpLease{lease: lease, pool: sink},
		diagnostic: true,
	}
	diagnostic.fail("ignored")
	if calls, _, _ := sink.snapshot(); calls != 2 {
		t.Fatalf("diagnostic body must not retire the exit, calls=%d", calls)
	}
	if fail := warpStreamFailure(io.NopCloser(strings.NewReader(""))); fail != nil {
		t.Fatal("a non-WARP body must not yield a failure callback")
	}
}

const idleStreamChunk = `data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}` + "\n\n"

func newIdleStreamGateway(t *testing.T, idle time.Duration, logs io.Writer) *Gateway {
	t.Helper()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, idleStreamChunk)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	return newGatewayForUpstream(t, server.URL, idle, logs)
}

func newGatewayForUpstream(t *testing.T, upstream string, idle time.Duration, logs io.Writer) *Gateway {
	t.Helper()
	g := newStubGateway(t, upstream)
	if logs != nil {
		g.logger = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	g.streamIdleTimeout = idle
	manager, err := rotation.New("", config.RotationConfig{})
	if err != nil {
		t.Fatal(err)
	}
	g.rotation = manager
	registerUsageModels(t, g, map[config.Tier]map[string]wire.Protocol{
		config.TierZen: {"chat-free": wire.Chat},
	})
	freezeProxyHealth(g)
	return g
}

func TestHandleInferenceDoesNotIdleWrapNon2xxStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		// Exceed abuseProbeLimit and name a deterministic rejection so the
		// anonymous lane stops after one probe; the delayed tail then reaches
		// handleInference, where the idle wrapper must not apply to a 4xx body.
		_, _ = io.WriteString(w, `{"error":{"message":"maximum context length `+strings.Repeat("x", 16<<10)+` upstream failed`)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case <-time.After(600 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, ` after delay"}}`)
	}))
	t.Cleanup(server.Close)

	g := newGatewayForUpstream(t, server.URL, 100*time.Millisecond, nil)
	g.cfg.Retry.MaxAttempts = 1

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"chat-free","stream":true,"messages":[{"role":"user","content":"hi"}]}`)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	start := time.Now()
	telemetry.Middleware(g.monitor, g.logger, g.handleInference(wire.Chat)).ServeHTTP(rec, req)
	elapsed := time.Since(start)

	// A 400 body is not a committed stream. If the idle wrapper is installed
	// before the status check it cuts the body at 100ms; without it the handler
	// waits for the delayed tail and forwards the complete upstream message.
	if elapsed < 400*time.Millisecond {
		t.Fatalf("non-2xx body was cut after %s; idle wrapper must not apply", elapsed)
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "after delay") {
		t.Fatalf("the delayed non-2xx body was not forwarded: %q", rec.Body.String())
	}
}

func TestHandleInferenceClosesIdleCommittedStream(t *testing.T) {
	var logs bytes.Buffer
	g := newIdleStreamGateway(t, 500*time.Millisecond, &logs)
	before := len(g.monitor.Snapshot().Upstream.Requests)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"chat-free","stream":true,"messages":[{"role":"user","content":"hi"}]}`)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	start := time.Now()
	telemetry.Middleware(g.monitor, g.logger, g.handleInference(wire.Chat)).ServeHTTP(rec, req)
	elapsed := time.Since(start)

	// Without the idle wrapper this blocks until the 3s client context; the
	// bound below is what makes the test fail on that regression.
	if elapsed >= 2*time.Second {
		t.Fatalf("request waited %s, want the idle timeout before the 3s client context", elapsed)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"content":"hi"`) {
		t.Fatalf("the first chunk must reach the client before the timeout, got %q", body)
	}
	if strings.Contains(body, "upstream SSE stream failed") || strings.Contains(body, `"type":"error"`) {
		t.Fatalf("an idle timeout must not emit a synthetic error event, got %q", body)
	}
	if !strings.Contains(logs.String(), `"event":"stream_idle_timeout"`) {
		t.Fatalf("missing stream_idle_timeout log: %s", logs.String())
	}
	if !strings.Contains(logs.String(), `"timeout_ms":500`) {
		t.Fatalf("missing timeout_ms=500 in log: %s", logs.String())
	}

	requests := g.monitor.Snapshot().Upstream.Requests
	if len(requests) <= before {
		t.Fatal("the request was not recorded")
	}
	if outcome := requests[len(requests)-1].Outcome; outcome != "stream_error" {
		t.Fatalf("outcome = %q, want stream_error", outcome)
	}
}
