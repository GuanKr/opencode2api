package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
