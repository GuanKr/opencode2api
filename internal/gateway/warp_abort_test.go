package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/warp"
)

type warpFailRecorder struct {
	reasons []string
}

func (r *warpFailRecorder) Fail(_ *warp.Lease, reason string) {
	r.reasons = append(r.reasons, reason)
}

type readError struct{}

func (readError) Read([]byte) (int, error) { return 0, errors.New("tunnel closed") }

func TestWarpBodyAbortedReadSkipsFail(t *testing.T) {
	lease := &warp.Lease{}
	lease.Abort(errors.New("exit rotated"))
	sink := &warpFailRecorder{}
	body := &warpBody{
		ReadCloser: io.NopCloser(readError{}),
		lease:      &warpLease{lease: lease, pool: sink},
	}
	if _, err := body.Read(make([]byte, 8)); !errors.Is(err, wire.ErrStreamAborted) {
		t.Fatalf("Read error = %v, want ErrStreamAborted", err)
	}
	if len(sink.reasons) != 0 {
		t.Fatalf("an aborted lease must not penalize its exit, got %v", sink.reasons)
	}
}

func TestWarpBodyLiveReadStillPenalizes(t *testing.T) {
	lease := &warp.Lease{}
	sink := &warpFailRecorder{}
	body := &warpBody{
		ReadCloser: io.NopCloser(readError{}),
		lease:      &warpLease{lease: lease, pool: sink},
	}
	if _, err := body.Read(make([]byte, 8)); err == nil || errors.Is(err, wire.ErrStreamAborted) {
		t.Fatalf("Read error = %v, want the raw read error", err)
	}
	if len(sink.reasons) != 1 || sink.reasons[0] != "stream read error" {
		t.Fatalf("a live lease must fail its exit on a read error, got %v", sink.reasons)
	}
}

func TestHealthWarpJSONFields(t *testing.T) {
	raw, err := json.Marshal(healthWarp{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	document := string(raw)
	for _, field := range []string{
		"status", "stack", "size", "active", "standby", "cooling",
		"started_at", "failures_total", "failure_streak",
		"create_failures_total", "last_failure_kind",
	} {
		if !strings.Contains(document, `"`+field+`"`) {
			t.Fatalf("health warp JSON is missing %q: %s", field, document)
		}
	}
	for _, field := range []string{"last_failure_at", "last_success_at"} {
		if strings.Contains(document, `"`+field+`"`) {
			t.Fatalf("health warp JSON must omit a zero %q: %s", field, document)
		}
	}
}

func TestWatchdogWarpResetWithoutPool(t *testing.T) {
	g := &Gateway{}
	rec := httptest.NewRecorder()
	g.handleWatchdogWarpReset(rec, httptest.NewRequest(http.MethodPost, "/v1/watchdog/warp/reset", nil))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "unavailable") {
		t.Fatalf("body = %q, want an unavailable status", rec.Body.String())
	}
}

func TestWatchdogWarpResetRouteWhenDisabled(t *testing.T) {
	g := &Gateway{
		cfg:    config.Config{ServerKeys: []string{"test-key"}},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/watchdog/warp/reset", nil)
	req.Header.Set("x-api-key", "test-key")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
}

// TestWarpBodyAbortedEOFCompletesReadAll pins the non-streaming contract: a
// body that was fully read before the rotation still ends at EOF, so io.ReadAll
// returns its complete payload instead of a rotation error.
func TestWarpBodyAbortedEOFCompletesReadAll(t *testing.T) {
	payload := `{"id":"chatcmpl-test","object":"chat.completion"}`
	lease := &warp.Lease{}
	lease.Abort(errors.New("exit rotated"))
	sink := &warpFailRecorder{}
	body := &warpBody{
		ReadCloser: io.NopCloser(strings.NewReader(payload)),
		lease:      &warpLease{lease: lease, pool: sink},
	}
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("io.ReadAll error = %v, want the complete payload", err)
	}
	if string(got) != payload {
		t.Fatalf("io.ReadAll = %q, want %q", got, payload)
	}
	if len(sink.reasons) != 0 {
		t.Fatalf("an aborted EOF must not penalize an exit, got %v", sink.reasons)
	}
}

func TestForwardStreamCompletedStreamSurvivesAbort(t *testing.T) {
	lease := &warp.Lease{}
	lease.Abort(errors.New("exit rotated"))
	sink := &warpFailRecorder{}
	body := &warpBody{
		ReadCloser: io.NopCloser(strings.NewReader(idleStreamChunk + "data: [DONE]\n\n")),
		lease:      &warpLease{lease: lease, pool: sink},
	}
	rec := httptest.NewRecorder()
	if _, _, err := wire.ForwardStream(context.Background(), rec, body, wire.Chat, "m"); err != nil {
		t.Fatalf("ForwardStream error = %v, want success after the terminal event", err)
	}
	if !strings.Contains(rec.Body.String(), "[DONE]") {
		t.Fatalf("the completed stream must be forwarded, got %q", rec.Body.String())
	}
	if len(sink.reasons) != 0 {
		t.Fatalf("a completed stream must not penalize an exit, got %v", sink.reasons)
	}
}

func TestForwardStreamAbortedAtEOFHasNoSyntheticError(t *testing.T) {
	lease := &warp.Lease{}
	lease.Abort(errors.New("exit rotated"))
	sink := &warpFailRecorder{}
	body := &warpBody{
		ReadCloser: io.NopCloser(strings.NewReader(idleStreamChunk)),
		lease:      &warpLease{lease: lease, pool: sink},
	}
	rec := httptest.NewRecorder()
	if _, _, err := wire.ForwardStream(context.Background(), rec, body, wire.Chat, "m"); !errors.Is(err, wire.ErrStreamAborted) {
		t.Fatalf("ForwardStream error = %v, want ErrStreamAborted", err)
	}
	document := rec.Body.String()
	if !strings.Contains(document, `"content":"hi"`) {
		t.Fatalf("the partial chunk must be forwarded, got %q", document)
	}
	if strings.Contains(document, "upstream SSE stream failed") {
		t.Fatalf("an aborted stream must not emit a synthetic error event, got %q", document)
	}
	if len(sink.reasons) != 0 {
		t.Fatalf("an aborted stream must not penalize an exit, got %v", sink.reasons)
	}
}

func TestTranscodeStreamAbortedAtEOFHasNoSyntheticError(t *testing.T) {
	lease := &warp.Lease{}
	lease.Abort(errors.New("exit rotated"))
	sink := &warpFailRecorder{}
	body := &warpBody{
		ReadCloser: io.NopCloser(strings.NewReader(idleStreamChunk)),
		lease:      &warpLease{lease: lease, pool: sink, regular: &http.Client{}, bun: &http.Client{}},
	}
	reader := newStreamIdleBody(body, time.Second)
	rec := httptest.NewRecorder()
	if _, _, err := wire.TranscodeStream(context.Background(), rec, reader, wire.Chat, wire.Anthropic, "m"); !errors.Is(err, wire.ErrStreamAborted) {
		t.Fatalf("TranscodeStream error = %v, want ErrStreamAborted", err)
	}
	document := rec.Body.String()
	if !strings.Contains(document, "hi") {
		t.Fatalf("the partial chunk must be forwarded, got %q", document)
	}
	if strings.Contains(document, "upstream SSE stream failed") {
		t.Fatalf("an aborted stream must not emit a synthetic error event, got %q", document)
	}
	if len(sink.reasons) != 0 {
		t.Fatalf("an aborted stream must not penalize an exit, got %v", sink.reasons)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// terminalThenBlockReadCloser yields one complete response and then parks like
// an upstream that delays EOF after its terminal event.
type terminalThenBlockReadCloser struct {
	data   []byte
	closed chan struct{}
	once   sync.Once
}

func newTerminalThenBlockReadCloser(data string) *terminalThenBlockReadCloser {
	return &terminalThenBlockReadCloser{data: []byte(data), closed: make(chan struct{})}
}

func (r *terminalThenBlockReadCloser) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	<-r.closed
	return 0, io.ErrClosedPipe
}

func (r *terminalThenBlockReadCloser) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

// gatedTerminalReadCloser returns one complete response only after the test
// releases it, which forces the idle timer to fire in the same Read that
// delivers the terminal event.
type gatedTerminalReadCloser struct {
	data  []byte
	ready chan struct{}
}

func newGatedTerminalReadCloser(data string) *gatedTerminalReadCloser {
	return &gatedTerminalReadCloser{data: []byte(data), ready: make(chan struct{})}
}

func (r *gatedTerminalReadCloser) Read(p []byte) (int, error) {
	<-r.ready
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func (r *gatedTerminalReadCloser) Close() error { return nil }

// A committed 2xx stream that already delivered its terminal event is complete,
// even when the idle timer fires while the read carrying that terminal is
// still blocked. The wrapper must not retire the exit; only the handler's
// classified idle branch may do that.
func TestIdleWrapperDoesNotRetireCompletedWarpStream(t *testing.T) {
	sink := &warpFailRecorder{}
	lease := new(warp.Lease)
	gated := newGatedTerminalReadCloser(idleStreamChunk + "data: [DONE]\n\n")
	body := &warpBody{
		ReadCloser: gated,
		lease: &warpLease{
			lease:   lease,
			pool:    sink,
			regular: &http.Client{},
			bun:     &http.Client{},
		},
	}
	reader := newStreamIdleBody(body, 20*time.Millisecond)
	rec := httptest.NewRecorder()

	result := make(chan error, 1)
	go func() {
		_, _, err := wire.ForwardStream(context.Background(), rec, reader, wire.Chat, "m")
		result <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	for !reader.(*idleReadCloser).timedOut.Load() {
		if time.Now().After(deadline) {
			t.Fatal("the idle timer never fired")
		}
		time.Sleep(time.Millisecond)
	}
	close(gated.ready)

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("ForwardStream error = %v, want success after the terminal event", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ForwardStream stayed blocked after the terminal event")
	}
	if !strings.Contains(rec.Body.String(), "[DONE]") {
		t.Fatalf("the completed stream must be forwarded, got %q", rec.Body.String())
	}
	if len(sink.reasons) != 0 {
		t.Fatalf("the idle wrapper retired a completed stream: %v", sink.reasons)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestIdleWrapperStopsBeforeDelayedEOF keeps the earlier terminal-then-block
// case: ForwardStream must not issue another Read after the terminal event.
func TestIdleWrapperStopsBeforeDelayedEOF(t *testing.T) {
	sink := &warpFailRecorder{}
	lease := new(warp.Lease)
	body := &warpBody{
		ReadCloser: newTerminalThenBlockReadCloser(idleStreamChunk + "data: [DONE]\n\n"),
		lease: &warpLease{
			lease:   lease,
			pool:    sink,
			regular: &http.Client{},
			bun:     &http.Client{},
		},
	}
	reader := newStreamIdleBody(body, 20*time.Millisecond)
	rec := httptest.NewRecorder()
	if _, _, err := wire.ForwardStream(context.Background(), rec, reader, wire.Chat, "m"); err != nil {
		t.Fatalf("ForwardStream error = %v, want success after the terminal event", err)
	}
	if reader.(*idleReadCloser).timedOut.Load() {
		t.Fatal("a terminal must not wait long enough for the idle timer")
	}
	if len(sink.reasons) != 0 {
		t.Fatalf("the idle wrapper retired a completed stream: %v", sink.reasons)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAnonymousCountsFailure(t *testing.T) {
	rotated := errors.Join(warp.ErrExitRotated, errors.New("context canceled"))
	cases := []struct {
		name      string
		err       error
		response  *http.Response
		leaseDown bool
		want      bool
	}{
		{name: "rotated lease", err: rotated, want: false},
		{name: "generic transport", err: errors.New("dial failed"), want: true},
		{name: "lease blocked", leaseDown: true, want: false},
		{name: "429 response", response: &http.Response{StatusCode: http.StatusTooManyRequests}, want: false},
		{name: "403 response", response: &http.Response{StatusCode: http.StatusForbidden}, want: true},
	}
	for _, testCase := range cases {
		if got := anonymousCountsFailure(testCase.err, testCase.response, testCase.leaseDown); got != testCase.want {
			t.Fatalf("%s: anonymousCountsFailure = %v, want %v", testCase.name, got, testCase.want)
		}
	}
}
