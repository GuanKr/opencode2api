package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"

	"opencode2api/internal/config"
	"opencode2api/internal/warp"
)

func TestWarpReadOnlyContext(t *testing.T) {
	playground := WithWARPPoolReporting(WithDiagnosticRequest(context.Background()))
	cases := []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{"production", context.Background(), false},
		{"watchdog diagnostic", WithDiagnosticRequest(context.Background()), true},
		{"playground diagnostic", playground, false},
		{"playground selected key", WithDebugKeyOverride(playground, DebugKeyOverride{Tier: config.TierZen, KeyID: "zen-1"}), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := warpReadOnly(tc.ctx); got != tc.want {
				t.Fatalf("warpReadOnly() = %v, want %v", got, tc.want)
			}
		})
	}
}

type recordingWarpReportSink struct {
	fails   []string
	reports []warpReportRecord
}

type warpReportRecord struct {
	status int
	body   string
}

func (s *recordingWarpReportSink) Fail(_ *warp.Lease, reason string) {
	s.fails = append(s.fails, reason)
}

func (s *recordingWarpReportSink) Report(_ *warp.Lease, status int, body []byte) {
	s.reports = append(s.reports, warpReportRecord{status: status, body: string(body)})
}

func TestReportWARPAttemptHonorsDiagnosticPolicy(t *testing.T) {
	lease := new(warp.Lease)
	playground := WithWARPPoolReporting(WithDiagnosticRequest(context.Background()))
	watchdog := WithDiagnosticRequest(context.Background())

	t.Run("playground transport failure retires the exit", func(t *testing.T) {
		sink := &recordingWarpReportSink{}
		if readOnly := reportWARPAttempt(playground, sink, lease, nil, errors.New("dial failed")); readOnly {
			t.Fatal("the Playground must not be read-only")
		}
		if len(sink.fails) != 1 || sink.fails[0] != "transport error" {
			t.Fatalf("Fail calls = %v, want [transport error]", sink.fails)
		}
	})

	t.Run("playground 429 reports and preserves the body", func(t *testing.T) {
		sink := &recordingWarpReportSink{}
		resp := &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(strings.NewReader("rate limited"))}
		if readOnly := reportWARPAttempt(playground, sink, lease, resp, nil); readOnly {
			t.Fatal("the Playground must not be read-only")
		}
		if len(sink.reports) != 1 || sink.reports[0].status != http.StatusTooManyRequests {
			t.Fatalf("Report calls = %+v, want one 429", sink.reports)
		}
		rest, err := io.ReadAll(resp.Body)
		if err != nil || string(rest) != "rate limited" {
			t.Fatalf("the peek must restore the body, got %q err=%v", rest, err)
		}
	})

	t.Run("watchdog transport failure stays read-only", func(t *testing.T) {
		sink := &recordingWarpReportSink{}
		if readOnly := reportWARPAttempt(watchdog, sink, lease, nil, errors.New("dial failed")); !readOnly {
			t.Fatal("the watchdog probe must stay read-only")
		}
		if len(sink.fails) != 0 {
			t.Fatalf("Fail calls = %v, want none", sink.fails)
		}
	})

	t.Run("watchdog 429 stays read-only", func(t *testing.T) {
		sink := &recordingWarpReportSink{}
		resp := &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(strings.NewReader("rate limited"))}
		if readOnly := reportWARPAttempt(watchdog, sink, lease, resp, nil); !readOnly {
			t.Fatal("the watchdog probe must stay read-only")
		}
		if len(sink.reports) != 0 {
			t.Fatalf("Report calls = %+v, want none", sink.reports)
		}
	})

	t.Run("production success reports", func(t *testing.T) {
		sink := &recordingWarpReportSink{}
		resp := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}
		if readOnly := reportWARPAttempt(context.Background(), sink, lease, resp, nil); readOnly {
			t.Fatal("production traffic must report")
		}
		if len(sink.reports) != 1 || sink.reports[0].status != http.StatusOK {
			t.Fatalf("Report calls = %+v, want one 200", sink.reports)
		}
	})
}

func TestWarpBodyReadOnlySkipsReadFailure(t *testing.T) {
	sink := &recordingWarpReportSink{}
	body := &warpBody{
		ReadCloser: io.NopCloser(iotest.ErrReader(errors.New("tunnel closed"))),
		lease:      &warpLease{lease: new(warp.Lease), pool: sink},
		readOnly:   true,
	}
	if _, err := body.Read(make([]byte, 8)); err == nil {
		t.Fatal("Read error = nil, want the tunnel error")
	}
	if len(sink.fails) != 0 {
		t.Fatalf("a read-only body must not retire the exit, got %v", sink.fails)
	}
}
