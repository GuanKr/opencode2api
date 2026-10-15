package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"opencode2api/internal/telemetry"
)

func TestParseUsageTimeBareFlag(t *testing.T) {
	bare, isBare, err := parseUsageTime("2026-09-06")
	if err != nil || !isBare {
		t.Fatalf("bare date must parse with bare=true, got %v,%v,%v", bare, isBare, err)
	}
	if bare.Hour() != 0 || bare.Minute() != 0 {
		t.Fatalf("bare date must be midnight, got %s", bare)
	}
	if name, offset := bare.Zone(); name != "Asia/Shanghai" || offset != 8*3600 {
		t.Fatalf("bare date must be Beijing midnight, got zone %s offset %d", name, offset)
	}
	precise, isBare, err := parseUsageTime("2026-09-06T15:30:00+08:00")
	if err != nil || isBare {
		t.Fatalf("RFC3339 must parse with bare=false, got %v,%v,%v", precise, isBare, err)
	}
	if _, _, err := parseUsageTime("next friday"); err == nil {
		t.Fatal("garbage must fail parsing")
	}
}

func doUsageRequest(t *testing.T, query string) telemetry.UsageRangeResult {
	t.Helper()
	server := &Server{monitor: telemetry.NewMonitor()}
	req := httptest.NewRequest(http.MethodGet, "/api/usage?"+query, nil)
	rec := httptest.NewRecorder()
	server.handleUsage(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("usage %q must be 200, got %d: %s", query, rec.Code, rec.Body.String())
	}
	var result telemetry.UsageRangeResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestHandleUsageBareToIsInclusive(t *testing.T) {
	// granularity=hour discriminates: with the fix a bare same-day range
	// covers the whole day (24 points); without it the request would 400
	// (invalid time range, from == to).
	result := doUsageRequest(t, "granularity=hour&from=2026-09-06&to=2026-09-06")
	if len(result.Points) != 24 {
		t.Fatalf("bare same-day hour range must yield 24 points, got %d", len(result.Points))
	}
	wantFrom := "2026-09-05T16:00:00Z"
	if got := result.From.UTC().Format("2006-01-02T15:04:05Z"); got != wantFrom {
		t.Fatalf("range from must be Beijing 09-06 00:00, got %s", got)
	}
	wantTo := "2026-09-06T16:00:00Z"
	if got := result.To.UTC().Format("2006-01-02T15:04:05Z"); got != wantTo {
		t.Fatalf("bare to must advance one day (Beijing 09-07 00:00), got %s", got)
	}
	// Same-day bare range at day granularity must work, not 400.
	day := doUsageRequest(t, "granularity=day&from=2026-09-06&to=2026-09-06")
	if len(day.Points) != 1 {
		t.Fatalf("bare same-day day range must yield 1 point, got %d", len(day.Points))
	}
}

func TestHandleUsagePreciseToUnshifted(t *testing.T) {
	// A precise RFC3339 "to" is the exclusive end and must never gain a day:
	// a one-hour window stays one point, not 25.
	result := doUsageRequest(t, "granularity=hour&from=2026-09-06T00:00:00%2B08:00&to=2026-09-06T01:00:00%2B08:00")
	if len(result.Points) != 1 {
		t.Fatalf("precise one-hour window must yield 1 point, got %d", len(result.Points))
	}
	wantTo := "2026-09-05T17:00:00Z"
	if got := result.To.UTC().Format("2006-01-02T15:04:05Z"); got != wantTo {
		t.Fatalf("precise to must stay Beijing 09-06 01:00, got %s", got)
	}
}

func TestHandleUsagePagedFormAndErrors(t *testing.T) {
	// The paged form (no from/to) must default to the per-granularity page
	// size and include the current Beijing bucket.
	page := doUsageRequest(t, "granularity=day")
	if len(page.Points) != 30 {
		t.Fatalf("day default page must yield 30 points, got %d", len(page.Points))
	}
	if page.Granularity != "day" || page.Timezone != "Asia/Shanghai" {
		t.Fatalf("unexpected page metadata %+v", page)
	}
	// Paging back one page must shift the window by exactly 30 days.
	prev := doUsageRequest(t, "granularity=day&count=30&offset=1")
	if got := page.From.Sub(prev.From); got != 30*24*time.Hour {
		t.Fatalf("offset=1 must shift the window by 30 days, got %v", got)
	}
	for _, query := range []string{
		"granularity=year",
		"granularity=day&count=0",
		"granularity=day&offset=-1",
		"granularity=day&from=2026-09-06",
		"granularity=day&to=tomorrow",
		"granularity=hour&count=1000",
	} {
		server := &Server{monitor: telemetry.NewMonitor()}
		req := httptest.NewRequest(http.MethodGet, "/api/usage?"+query, nil)
		rec := httptest.NewRecorder()
		server.handleUsage(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("usage %q must be 400, got %d: %s", query, rec.Code, rec.Body.String())
		}
	}
}
