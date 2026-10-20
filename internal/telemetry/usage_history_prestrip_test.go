package telemetry

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"opencode2api/internal/protocol"
)

// TestPrestripFlagsSurviveUsageHistoryRoundTrip locks the cross-card
// integration: the poisoned-history pre-strip markers added to UpstreamRequest
// must outlive a save/load of the usage history ring, otherwise the usage
// page only shows the pre-strip state for as long as the process lives.
func TestPrestripFlagsSurviveUsageHistoryRoundTrip(t *testing.T) {
	monitor := Monitor()
	monitor.Record("/v1/responses", 200, 900*time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_sel", Session: "ses_sel", Channel: "key",
		UsageReported: true, Usage: protocol.Usage{Input: 10, Output: 5, Total: 15},
		Prestripped: true, PrestripMode: "selective", PrestripIDs: 7,
	})
	monitor.Record("/v1/responses", 200, 800*time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_full", Session: "ses_full", Channel: "key",
		UsageReported: true, Prestripped: true, PrestripMode: "full",
	})
	monitor.Record("/v1/responses", 200, 700*time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_plain", Session: "ses_plain", Channel: "key",
	})

	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	// The persisted key names are the API/webui contract (webui reads
	// request.prestripped / prestrip_mode / prestrip_ids): a renamed tag would
	// keep the struct round-trip green in-process while silently blanking the
	// usage page for every request restored from disk.
	//
	// Assert on the decoded object, never on raw substrings: the writer is
	// free to change its whitespace (it is compact now), and a
	// `"prestripped": true` substring check would fail on
	// `"prestripped":true` while telling you nothing about the contract.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var stored struct {
		RecentRequests []map[string]any `json:"recent_requests"`
	}
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatalf("usage history must stay valid JSON: %v (%s)", err, raw)
	}
	wantByID := map[string]map[string]any{
		"req_sel":   {"prestripped": true, "prestrip_mode": "selective", "prestrip_ids": float64(7)},
		"req_full":  {"prestripped": true, "prestrip_mode": "full"},
		"req_plain": nil, // must not invent prestrip keys for a clean request
	}
	seen := make(map[string]bool, len(wantByID))
	for _, request := range stored.RecentRequests {
		id, _ := request["request_id"].(string)
		want, ok := wantByID[id]
		if !ok {
			continue
		}
		seen[id] = true
		if want == nil {
			for _, key := range []string{"prestripped", "prestrip_mode", "prestrip_ids"} {
				if _, present := request[key]; present {
					t.Fatalf("%s must not carry %s: %v", id, key, request)
				}
			}
			continue
		}
		for key, value := range want {
			if request[key] != value {
				t.Fatalf("%s must persist %s=%v, got %v", id, key, value, request[key])
			}
		}
	}
	for id := range wantByID {
		if !seen[id] {
			t.Fatalf("request %s missing from the persisted history: %s", id, raw)
		}
	}
	// The writer is deliberately compact: every save re-marshals the whole
	// file (up to ~9.6k hour buckets), and indentation would add ~40% to that
	// write for readers nobody has (Load and jq both parse JSON). Reverting to
	// MarshalIndent must fail here rather than silently reintroduce the churn.
	if bytes.Contains(raw, []byte("\n  ")) {
		t.Fatal("usage history must be written compact, not indented (write amplification)")
	}
	restored := Monitor()
	if _, err := restored.LoadUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	result, err := restored.UsageRangeCount("hour", 24, 0)
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]UpstreamRequest, len(result.Requests))
	for _, request := range result.Requests {
		byID[request.RequestID] = request
	}
	sel, ok := byID["req_sel"]
	if !ok {
		t.Fatalf("selective pre-strip request must survive the round-trip, got %+v", result.Requests)
	}
	if !sel.Prestripped || sel.PrestripMode != "selective" || sel.PrestripIDs != 7 {
		t.Fatalf("selective pre-strip flags must survive the round-trip, got %+v", sel)
	}
	full, ok := byID["req_full"]
	if !ok {
		t.Fatalf("full pre-strip request must survive the round-trip, got %+v", result.Requests)
	}
	if !full.Prestripped || full.PrestripMode != "full" || full.PrestripIDs != 0 {
		t.Fatalf("full pre-strip flags must survive the round-trip, got %+v", full)
	}
	if plain := byID["req_plain"]; plain.Prestripped || plain.PrestripMode != "" || plain.PrestripIDs != 0 {
		t.Fatalf("a clean request must stay unmarked after the round-trip, got %+v", plain)
	}
}

// TestPrestripFlagsOmittedWhenUnset keeps the persisted file small and the
// old records readable: unset pre-strip state must not appear on disk, so a
// history written by this build stays byte-compatible with the field set a
// previous build knew about.
func TestPrestripFlagsOmittedWhenUnset(t *testing.T) {
	monitor := Monitor()
	monitor.Record("/v1/responses", 200, 100*time.Millisecond, &RequestMeta{
		Model: "model-a", Tier: "zen", Request: "req_plain", Channel: "key",
	})
	path := filepath.Join(t.TempDir(), "config.json.usage.history.json")
	if err := monitor.SaveUsageHistory(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"prestripped", "prestrip_mode", "prestrip_ids"} {
		if strings.Contains(string(data), `"`+field+`"`) {
			t.Fatalf("clean request must not persist %q: %s", field, data)
		}
	}
}
