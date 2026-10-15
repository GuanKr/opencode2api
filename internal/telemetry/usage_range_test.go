package telemetry

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"opencode2api/internal/protocol"
)

func testHourBucket(hour time.Time, model, tier string, in, out uint64, cost float64) *usageHourBucket {
	bucket := newUsageHourBucket(hour.UTC().Truncate(time.Hour).Unix())
	bucket.Requests = 1
	bucket.Reported = 1
	bucket.Tokens = TokenCounts{Input: in, Output: out, Total: in + out}
	bucket.Models = map[string]TokenCounts{model: bucket.Tokens}
	bucket.Tiers = map[string]TokenCounts{tier: bucket.Tokens}
	bucket.CostUSD = cost
	bucket.ModelCosts = map[string]float64{model: cost}
	bucket.TierCosts = map[string]float64{tier: cost}
	return bucket
}

func TestBeijingDayWeekMonthAlignment(t *testing.T) {
	// 2026-09-05 00:30 Beijing = 2026-09-04 16:30 UTC (Saturday).
	utc := time.Date(2026, 9, 4, 16, 30, 0, 0, time.UTC)
	day := beijingDayStart(utc)
	if day.In(BeijingZone).Day() != 5 || day.In(BeijingZone).Hour() != 0 {
		t.Fatalf("day start must be Beijing 2026-09-05 00:00, got %s", day.In(BeijingZone))
	}
	week := beijingWeekStart(utc)
	// 2026-08-31 Monday is the week start for Saturday 2026-09-05.
	if week.In(BeijingZone).Day() != 31 || week.In(BeijingZone).Month() != 8 {
		t.Fatalf("week start must be Beijing Monday 2026-08-31, got %s", week.In(BeijingZone))
	}
	month := beijingMonthStart(utc)
	if month.In(BeijingZone).Day() != 1 || month.In(BeijingZone).Month() != 9 {
		t.Fatalf("month start must be Beijing 2026-09-01, got %s", month.In(BeijingZone))
	}
}

func TestUsageRangeRollupCostAndTopN(t *testing.T) {
	monitor := NewMonitor()
	// Two Beijing days: 09-04 (2 requests) and 09-05 (1 request).
	h1 := time.Date(2026, 9, 3, 16, 0, 0, 0, time.UTC) // 09-04 00:00 Beijing
	h2 := time.Date(2026, 9, 4, 16, 0, 0, 0, time.UTC) // 09-05 00:00 Beijing
	monitor.mu.Lock()
	monitor.usageHours[h1.Unix()] = testHourBucket(h1, "model-a", "zen", 1000, 500, 0.002)
	second := testHourBucket(h1.Add(time.Hour), "model-b", "go", 2000, 0, 0.002)
	monitor.usageHours[h1.Add(time.Hour).Unix()] = second
	monitor.usageHours[h2.Unix()] = testHourBucket(h2, "model-a", "zen", 100, 100, 0.0003)
	monitor.mu.Unlock()

	from := time.Date(2026, 9, 4, 0, 0, 0, 0, BeijingZone)
	to := time.Date(2026, 9, 6, 0, 0, 0, 0, BeijingZone)
	result, err := monitor.UsageRangeExplicit("day", from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != 2 {
		t.Fatalf("expected 2 daily points, got %d", len(result.Points))
	}
	if result.Total.Requests != 3 {
		t.Fatalf("expected 3 requests total, got %d", result.Total.Requests)
	}
	if result.Total.Tokens.Input != 3100 || result.Total.Tokens.Output != 600 {
		t.Fatalf("unexpected total tokens %+v", result.Total.Tokens)
	}
	if diff := result.Total.CostUSD - 0.0043; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("unexpected total cost %v", result.Total.CostUSD)
	}
	if result.Timezone != "Asia/Shanghai" {
		t.Fatalf("timezone must be Asia/Shanghai, got %q", result.Timezone)
	}

	// TopN trimming: 20 models must collapse to 15.
	big := NewMonitor()
	hour := time.Date(2026, 9, 4, 16, 0, 0, 0, time.UTC)
	bucket := newUsageHourBucket(hour.Unix())
	for i := 0; i < 20; i++ {
		model := fmt.Sprintf("model-%02d", i)
		bucket.Requests++
		bucket.Reported++
		tokens := TokenCounts{Input: uint64(100 + i), Output: 0, Total: uint64(100 + i)}
		addTokenCounts(&bucket.Tokens, tokens)
		addTokenMap(bucket.Models, model, tokens)
	}
	big.mu.Lock()
	big.usageHours[hour.Unix()] = bucket
	big.mu.Unlock()
	single, err := big.UsageRangeExplicit("day", from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(single.Total.Models) != usageRangeTopN {
		t.Fatalf("expected top %d models, got %d", usageRangeTopN, len(single.Total.Models))
	}
}

func TestUsageHistorySaveLoadRoundTrip(t *testing.T) {
	monitor := NewMonitor()
	monitor.SetPriceFunc(func(string) (ModelPrice, bool) { return ModelPrice{Input: 1, Output: 2, CacheRead: 0.5}, true })
	hour := time.Now().UTC().Truncate(time.Hour)
	monitor.mu.Lock()
	monitor.usageHours[hour.Unix()] = testHourBucket(hour, "model-a", "zen", 1000, 500, 0.002)
	monitor.mu.Unlock()
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	// Permission bits are not asserted here: 0600 is applied through
	// os.Chmod, which Windows maps to a read-only flag only.
	restored := NewMonitor()
	if _, err := restored.LoadUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	restored.mu.Lock()
	defer restored.mu.Unlock()
	bucket, ok := restored.usageHours[hour.Unix()]
	if !ok {
		t.Fatalf("hour bucket missing after reload")
	}
	if bucket.Requests != 1 || bucket.Tokens.Total != 1500 || bucket.CostUSD != 0.002 {
		t.Fatalf("unexpected restored bucket %+v", bucket)
	}
}

func TestUsageRangeValidation(t *testing.T) {
	monitor := NewMonitor()
	if _, err := monitor.UsageRangeExplicit("year", time.Now().Add(-time.Hour), time.Now()); err == nil {
		t.Fatalf("invalid granularity must fail")
	}
	if _, err := monitor.UsageRangeCount("day", 0, 0); err == nil {
		t.Fatalf("zero count must fail")
	}
	if _, err := monitor.UsageRangeCount("day", usageRangeMaxDayPoints+1, 0); err == nil {
		t.Fatalf("oversized count must fail")
	}
}

func TestUsageRangeCountIncludesCurrentBucket(t *testing.T) {
	monitor := NewMonitor()
	hour := time.Now().UTC().Truncate(time.Hour)
	monitor.mu.Lock()
	monitor.usageHours[hour.Unix()] = testHourBucket(hour, "model-now", "zen", 10, 5, 0.00002)
	monitor.mu.Unlock()
	result, err := monitor.UsageRangeCount("hour", 24, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != 24 {
		t.Fatalf("expected 24 hourly points, got %d", len(result.Points))
	}
	last := result.Points[len(result.Points)-1]
	if !last.Start.Equal(hour) {
		t.Fatalf("last point must be the current hour %s, got %s", hour, last.Start)
	}
	if result.Total.Requests != 1 || result.Total.Tokens.Total != 15 {
		t.Fatalf("current bucket missing from total: %+v", result.Total)
	}
}

func TestRecordHourCostFormulaUnknownAndTierSkip(t *testing.T) {
	monitor := NewMonitor()
	monitor.SetPriceFunc(func(model string) (ModelPrice, bool) {
		if model == "model-paid" {
			// USD per 1M tokens; cache reads priced below the base rate.
			return ModelPrice{Input: 1, Output: 2, CacheRead: 0.5}, true
		}
		return ModelPrice{}, false
	})
	meta := &RequestMeta{
		Model: "model-paid", Tier: "zen", UsageReported: true,
		Usage: protocol.Usage{Input: 1000000, Output: 500000, Total: 1500000},
	}
	monitor.Record("/v1/chat/completions", 200, time.Millisecond, meta)
	monitor.Record("/v1/chat/completions", 200, time.Millisecond, &RequestMeta{
		Model: "model-unknown", Tier: "go", UsageReported: true,
		Usage: protocol.Usage{Input: 100, Output: 100, Total: 200},
	})
	// Empty tier records minute stats but must not enter hourly usage.
	monitor.Record("/v1/chat/completions", 200, time.Millisecond, &RequestMeta{
		Model: "model-paid", UsageReported: true,
		Usage: protocol.Usage{Input: 50, Output: 50, Total: 100},
	})
	hour := time.Now().UTC().Truncate(time.Hour).Unix()
	monitor.mu.Lock()
	bucket, ok := monitor.usageHours[hour]
	monitor.mu.Unlock()
	if !ok {
		t.Fatalf("current hour bucket missing")
	}
	if bucket.Requests != 2 {
		t.Fatalf("tier-less request must skip hourly usage, got %d requests", bucket.Requests)
	}
	if diff := bucket.CostUSD - 2.0; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("expected $2.00 (1M*$1+0.5M*$2), got %v", bucket.CostUSD)
	}
	if cost := bucket.ModelCosts["model-paid"]; cost != 2.0 {
		t.Fatalf("expected model-paid cost $2.00, got %v", cost)
	}
	if _, exists := bucket.ModelCosts["model-unknown"]; exists {
		t.Fatalf("unknown price must record zero cost without a model_costs entry")
	}
	if !bucket.UnknownCost {
		t.Fatal("bucket with an unpriced model must set UnknownCost (tells $0-unknown apart from $0-free)")
	}
	if _, exists := bucket.Models["model-paid"]; !exists {
		t.Fatalf("model-paid tokens must be aggregated")
	}
}

func TestRecordHourCacheReadPricing(t *testing.T) {
	monitor := NewMonitor()
	monitor.SetPriceFunc(func(string) (ModelPrice, bool) {
		return ModelPrice{Input: 1, Output: 2, CacheRead: 0.5}, true
	})
	monitor.Record("/v1/responses", 200, time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", UsageReported: true,
		// Fresh 800@$1 + cached 200@$0.5 + output 500@$2 (reasoning 50 is
		// inside output, not double-counted).
		Usage: protocol.Usage{Input: 1000, Output: 500, Total: 1500, Cached: 200, Reasoning: 50},
	})
	hour := time.Now().UTC().Truncate(time.Hour).Unix()
	monitor.mu.Lock()
	bucket, ok := monitor.usageHours[hour]
	monitor.mu.Unlock()
	if !ok {
		t.Fatal("current hour bucket missing")
	}
	want := (800*1 + 200*0.5 + 500*2) / 1e6
	if diff := bucket.CostUSD - want; diff > 1e-12 || diff < -1e-12 {
		t.Fatalf("expected cache-aware cost %v, got %v", want, bucket.CostUSD)
	}
	if bucket.Tokens.Cached != 200 {
		t.Fatalf("cached tokens must be aggregated, got %+v", bucket.Tokens)
	}
	if bucket.UnknownCost {
		t.Fatal("known model must not set UnknownCost")
	}
}

func TestRecordEndpointTokensAndPerRequestUsage(t *testing.T) {
	monitor := NewMonitor()
	monitor.SetPriceFunc(func(string) (ModelPrice, bool) { return ModelPrice{Input: 1, Output: 2, CacheRead: 0.5}, true })
	monitor.Record("/v1/responses", 200, 1500*time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_1", Session: "ses_test", KeyID: "abc12", Channel: "key",
		Stream: true, UsageReported: true,
		Usage: protocol.Usage{Input: 1000, Output: 500, Total: 1500, Cached: 200, Reasoning: 50},
	})
	hour := time.Now().UTC().Truncate(time.Hour).Unix()
	monitor.mu.Lock()
	bucket, ok := monitor.usageHours[hour]
	monitor.mu.Unlock()
	if !ok {
		t.Fatalf("current hour bucket missing")
	}
	if bucket.Endpoints["/v1/responses"] != 1 {
		t.Fatalf("endpoint request count must be recorded, got %+v", bucket.Endpoints)
	}
	if tokens := bucket.EndpointTokens["/v1/responses"]; tokens.Total != 1500 {
		t.Fatalf("endpoint tokens must be recorded, got %+v", tokens)
	}
	if cost := bucket.EndpointCosts["/v1/responses"]; cost <= 0 {
		t.Fatalf("endpoint cost must be recorded, got %v", cost)
	}
	snapshot := monitor.Snapshot()
	if len(snapshot.Upstream.Requests) != 1 {
		t.Fatalf("expected 1 upstream request, got %d", len(snapshot.Upstream.Requests))
	}
	request := snapshot.Upstream.Requests[0]
	if request.Session != "ses_test" {
		t.Fatalf("per-request session must be recorded, got %+v", request)
	}
	if request.Tokens.Total != 1500 || request.Tokens.Cached != 200 {
		t.Fatalf("per-request tokens must be recorded, got %+v", request.Tokens)
	}
	if request.CostUSD <= 0 {
		t.Fatalf("per-request cost must be recorded, got %v", request.CostUSD)
	}
	result, err := monitor.UsageRangeCount("hour", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.Total.Endpoints["/v1/responses"] != 1 {
		t.Fatalf("range total must carry endpoint breakdown, got %+v", result.Total.Endpoints)
	}
}

func TestUsageHistoryRecentRequestsRoundTrip(t *testing.T) {
	monitor := NewMonitor()
	// Chronological insertion: the stale head lets Save-side Prune actually
	// fire instead of passing vacuously through Snapshot filtering.
	monitor.mu.Lock()
	monitor.recentRequests.Add(UpstreamRequest{
		Time: time.Now().UTC().Add(-25 * time.Hour), RequestID: "req_stale",
		Model: "model-a", Tier: "zen", Channel: "key", Status: 200, Success: true,
	})
	monitor.recentRequests.Add(UpstreamRequest{
		Time: time.Now().UTC().Add(-2 * time.Hour), RequestID: "req_day",
		Model: "model-a", Tier: "zen", Channel: "key", Status: 200, Success: true,
		Session: "ses_day",
	})
	monitor.mu.Unlock()
	monitor.Record("/v1/responses", 200, 1200*time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_fresh", Session: "ses_fresh", Channel: "key",
		Stream: true, UsageReported: true,
		Usage: protocol.Usage{Input: 100, Output: 50, Total: 150},
	})
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	restored := NewMonitor()
	if _, err := restored.LoadUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	// The detail is served by the range query, not the monitor: ask for the
	// day that ends now and the two in-window rows must come back.
	result, err := restored.UsageRangeCount("hour", 24, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Requests) != 2 {
		t.Fatalf("expected 2 restored requests (only older than 24h must be pruned), got %d", len(result.Requests))
	}
	requests := result.Requests
	if requests[0].RequestID != "req_fresh" || requests[0].Session != "ses_fresh" {
		t.Fatalf("newest restored request must come first, got %+v", requests[0])
	}
	if requests[0].Tokens.Total != 150 || requests[0].DurationMS != 1200 {
		t.Fatalf("restored request lost tokens/duration, got %+v", requests[0])
	}
	if requests[1].RequestID != "req_day" || requests[1].Session != "ses_day" {
		t.Fatalf("day-old request must survive with session intact, got %+v", requests[1])
	}
}

// TestMonitorRequestsStayOnOneHourWindow pins the split of the two windows:
// the ring keeps a day for /api/usage, while /api/monitor keeps the upstream
// one-hour response. The ring must not be cut to an hour by the snapshot
// either — that would destroy the day of history the usage page queries.
func TestMonitorRequestsStayOnOneHourWindow(t *testing.T) {
	monitor := NewMonitor()
	now := time.Now().UTC()
	monitor.mu.Lock()
	monitor.recentRequests.Add(UpstreamRequest{Time: now.Add(-23 * time.Hour), RequestID: "req_23h"})
	monitor.recentRequests.Add(UpstreamRequest{Time: now.Add(-30 * time.Minute), RequestID: "req_30m"})
	monitor.recentAttempts.Add(UpstreamAttempt{Time: now.Add(-30 * time.Minute), RequestID: "req_30m"})
	monitor.mu.Unlock()
	if requests := monitor.Snapshot().Upstream.Requests; len(requests) != 1 || requests[0].RequestID != "req_30m" {
		t.Fatalf("monitor must expose only the last hour of requests, got %+v", requests)
	}
	result, err := monitor.UsageRangeCount("hour", 24, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Requests) != 2 {
		t.Fatalf("range query must still serve the 23h-old row, got %+v", result.Requests)
	}
	if result.Requests[0].RequestID != "req_30m" || result.Requests[1].RequestID != "req_23h" {
		t.Fatalf("range detail must be newest-first, got %+v", result.Requests)
	}
}

// TestSaveAfterTrafficRewritesFile is the mirror of TestIdleSaveSkipsWrite:
// the dirty flag is the only thing that tells the saver that traffic
// happened, so losing it would freeze the on-disk history forever while
// every other test still passed.
func TestSaveAfterTrafficRewritesFile(t *testing.T) {
	monitor := NewMonitor()
	monitor.SetPriceFunc(func(string) (ModelPrice, bool) { return ModelPrice{Input: 1, Output: 2, CacheRead: 0.5}, true })
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	// First save establishes the file, then it disappears (operator cleanup,
	// volume remount, ...). Traffic after that must put it back.
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	monitor.Record("/v1/responses", 200, time.Second, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_dirty", Session: "ses_dirty", Channel: "key",
		UsageReported: true, Usage: protocol.Usage{Input: 100, Output: 50, Total: 150},
	})
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("save after traffic must rewrite the history file: %v", err)
	}
	restored := NewMonitor()
	if _, err := restored.LoadUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	result, err := restored.UsageRangeCount("hour", 24, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Requests) != 1 || result.Requests[0].RequestID != "req_dirty" {
		t.Fatalf("the rewritten file must carry the new request, got %+v", result.Requests)
	}
	if result.Total.CostUSD <= 0 {
		t.Fatalf("the rewritten file must carry the recorded cost, got %v", result.Total.CostUSD)
	}
}

// TestUsageHistoryRetentionPrune pins the 400-day window: buckets inside it
// survive a save/load round trip, buckets older are dropped on both sides.
// The probe ages are absolute (300 / 450 days) on purpose — deriving them
// from the constant would move the boundary along with the mutation and the
// test would pass either way.
func TestUsageHistoryRetentionPrune(t *testing.T) {
	if usageHistoryRetentionDays != 400 {
		t.Fatalf("usage history must be retained for 400 days, got %d", usageHistoryRetentionDays)
	}
	monitor := NewMonitor()
	now := time.Now().UTC()
	inside := now.Add(-300 * 24 * time.Hour).Truncate(time.Hour)
	outside := now.Add(-450 * 24 * time.Hour).Truncate(time.Hour)
	monitor.mu.Lock()
	monitor.usageHours[inside.Unix()] = testHourBucket(inside, "model-a", "zen", 10, 5, 0.001)
	monitor.usageHours[outside.Unix()] = testHourBucket(outside, "model-a", "zen", 10, 5, 0.001)
	monitor.mu.Unlock()
	if len(monitor.usageHours) != 2 {
		t.Fatal("both buckets must be seeded before the prune runs")
	}
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	monitor.mu.Lock()
	_, keptAfterSave := monitor.usageHours[inside.Unix()]
	_, droppedAfterSave := monitor.usageHours[outside.Unix()]
	monitor.mu.Unlock()
	if !keptAfterSave {
		t.Fatal("save must keep a bucket inside the retention window")
	}
	if droppedAfterSave {
		t.Fatal("save must prune a bucket older than the retention window")
	}
	restored := NewMonitor()
	if _, err := restored.LoadUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	restored.mu.Lock()
	_, kept := restored.usageHours[inside.Unix()]
	_, dropped := restored.usageHours[outside.Unix()]
	restored.mu.Unlock()
	if !kept {
		t.Fatalf("reload must keep the bucket %s inside the %d-day window", inside, usageHistoryRetentionDays)
	}
	if dropped {
		t.Fatalf("reload must not resurrect the expired bucket %s", outside)
	}
}

// TestUnreadableHistoryFencesSave covers the "open failed for another reason"
// branch (permissions, transient IO). The failure is injected because chmod
// cannot make a file unreadable on Windows the way it does on Unix.
func TestUnreadableHistoryFencesSave(t *testing.T) {
	original := openHistoryFile
	openHistoryFile = func(string) (*os.File, error) {
		return nil, &os.PathError{Op: "open", Path: "config.json.usage.history.json", Err: fs.ErrPermission}
	}
	defer func() { openHistoryFile = original }()

	monitor := NewMonitor()
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"hours":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := monitor.LoadUsageHistory(path); err == nil {
		t.Fatal("an unreadable history must fail Load")
	}
	// The on-disk history may be fine; the monitor is empty, so a save would
	// replace it with nothing.
	if err := monitor.SaveUsageHistory(path); err == nil {
		t.Fatal("save must be refused after an unreadable history")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"version"`) {
		t.Fatalf("the on-disk history must be preserved, got %q", data)
	}
}

// TestLoadRecoversInterruptedReplace covers a process killed between the two
// Windows renames: the complete history is left under <path>.replace with no
// main file. Load must adopt it instead of reporting an empty history, which
// the first save would then overwrite with nothing.
func TestLoadRecoversInterruptedReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json.usage.history.json")
	written := NewMonitor()
	hour := time.Now().UTC().Truncate(time.Hour)
	written.mu.Lock()
	written.usageHours[hour.Unix()] = testHourBucket(hour, "model-a", "zen", 1000, 500, 0.002)
	written.mu.Unlock()
	if err := written.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	// Simulate the interrupted replace: live file renamed aside, new file
	// never moved into place.
	if err := os.Rename(path, path+".replace"); err != nil {
		t.Fatal(err)
	}

	monitor := NewMonitor()
	recovered, err := monitor.LoadUsageHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if !recovered {
		t.Fatal("Load must report the recovered .replace backup")
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("the backup must be moved back into place: %v", statErr)
	}
	if _, statErr := os.Stat(path + ".replace"); !os.IsNotExist(statErr) {
		t.Fatal("the backup must be consumed by the recovery")
	}
	monitor.mu.Lock()
	bucket, ok := monitor.usageHours[hour.Unix()]
	monitor.mu.Unlock()
	if !ok || bucket.Tokens.Total != 1500 {
		t.Fatalf("the recovered history must hold the original buckets, got %+v", bucket)
	}
}

// TestLoadWithoutReplaceStaysEmpty is the negative half of the recovery: a
// genuinely absent history must still load as empty and must not be fenced.
func TestLoadWithoutReplaceStaysEmpty(t *testing.T) {
	monitor := NewMonitor()
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	recovered, err := monitor.LoadUsageHistory(path)
	if err != nil || recovered {
		t.Fatalf("a missing history must load silently, got recovered=%v err=%v", recovered, err)
	}
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatalf("saving must stay enabled without a failed load: %v", err)
	}
}

// TestRequestRingKeepsADayButNotTwo guards the 24h retention window of the
// request detail: /api/usage serves the day, anything older is pruned.
func TestRequestRingKeepsADayButNotTwo(t *testing.T) {
	monitor := NewMonitor()
	monitor.mu.Lock()
	monitor.recentRequests.Add(UpstreamRequest{Time: time.Now().UTC().Add(-23 * time.Hour), RequestID: "req_23h"})
	monitor.recentRequests.Add(UpstreamRequest{Time: time.Now().UTC().Add(-25 * time.Hour), RequestID: "req_25h"})
	monitor.mu.Unlock()
	// A Record (or any snapshot) applies the ring retention.
	monitor.Record("/v1/responses", 200, time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_now", Channel: "key",
		UsageReported: true, Usage: protocol.Usage{Input: 1, Output: 1, Total: 2},
	})
	result, err := monitor.UsageRangeCount("hour", 24, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Requests) != 2 {
		t.Fatalf("only entries inside the 24h window must survive, got %+v", result.Requests)
	}
	for _, request := range result.Requests {
		if request.RequestID == "req_25h" {
			t.Fatal("the 25h-old entry must be pruned from the ring")
		}
	}
}

func TestLoadCorruptFilePreserved(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	if err := os.WriteFile(path, []byte("{truncated"), 0600); err != nil {
		t.Fatal(err)
	}
	monitor := NewMonitor()
	if _, err := monitor.LoadUsageHistory(path); err == nil {
		t.Fatal("corrupt history must fail Load")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatal("corrupt file must be renamed away, not left in place")
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	preserved := false
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "config.json.usage.history.json.corrupt.") {
			preserved = true
		}
	}
	if !preserved {
		t.Fatal("corrupt history must be preserved as *.corrupt.<unix>")
	}
}

func TestLoadTwiceRefused(t *testing.T) {
	monitor := NewMonitor()
	monitor.Record("/v1/responses", 200, 1200*time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_first", Channel: "key",
		UsageReported: true, Usage: protocol.Usage{Input: 100, Output: 50, Total: 150},
	})
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	restored := NewMonitor()
	if _, err := restored.LoadUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	// A second Load would merge the same buckets again and double-count.
	if _, err := restored.LoadUsageHistory(path); err == nil {
		t.Fatal("second Load must be refused to avoid double-counting")
	}
}

func TestIdleSaveSkipsWrite(t *testing.T) {
	monitor := NewMonitor()
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	// First save always establishes the file, even with no traffic.
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	// Idle: nothing recorded since, so the minute tick must skip the
	// snapshot+marshal+write instead of rewriting the same bytes.
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatal("idle save must skip the disk write")
	}
}

func TestFailedLoadFencesSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	if err := os.WriteFile(path, []byte("{truncated"), 0600); err != nil {
		t.Fatal(err)
	}
	monitor := NewMonitor()
	if _, err := monitor.LoadUsageHistory(path); err == nil {
		t.Fatal("corrupt history must fail Load")
	}
	// The monitor is empty now; saving would overwrite the preserved
	// .corrupt copy's source of truth with nothing.
	if err := monitor.SaveUsageHistory(path); err == nil {
		t.Fatal("save must be refused after a failed load")
	}
}

func TestUsageHistoryPathIsConfigSibling(t *testing.T) {
	if got := UsageHistoryPath("config.json"); got != "config.json.usage.history.json" {
		t.Fatalf("unexpected history path %q", got)
	}
	if got := UsageHistoryPath(""); got != "" {
		t.Fatalf("empty config path must yield no history path, got %q", got)
	}
}
