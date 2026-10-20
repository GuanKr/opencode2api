package gateway

import (
	"encoding/json"
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

// availabilityReportCatalog advertises one free and one paid model with
// deliberately different per-tier windows (Zen 200000 / Go 1000000).
func availabilityReportCatalog(prefer config.Tier) *models.ModelCatalog {
	catalog := models.Catalog(prefer, nil)
	catalog.ReplaceWithCapabilities(
		[]string{"test-model-free", "test-model-paid"},
		nil,
		map[config.Tier]map[string]wire.Protocol{
			config.TierZen: {"test-model-free": wire.Chat, "test-model-paid": wire.Chat},
		},
		nil,
		map[config.Tier]map[string]models.Metadata{
			config.TierZen: {"test-model-free": {ContextWindow: 200000}, "test-model-paid": {ContextWindow: 1000000}},
			config.TierGo:  {"test-model-free": {ContextWindow: 1000000}},
		},
	)
	return catalog
}

func availabilityReport(t *testing.T, catalog *models.ModelCatalog) []FreeModelStatus {
	t.Helper()
	registry, err := models.AvailabilityStore("")
	if err != nil {
		t.Fatal(err)
	}
	controller := &RuntimeController{}
	controller.current.Store(&gatewayRuntime{gateway: &Gateway{catalog: catalog}, availability: registry})
	return controller.AvailabilityReport()
}

// The availability endpoint is the free model page's only source, so the
// context window must travel with the probe state. The report must use the Zen
// window (Go advertises a different one here) and keep paid models out.
func TestAvailabilityReportCarriesZenContextWindow(t *testing.T) {
	report := availabilityReport(t, availabilityReportCatalog(config.TierZen))
	if len(report) != 1 || report[0].Model != "test-model-free" {
		t.Fatalf("report = %+v, want exactly the free model", report)
	}
	if report[0].ContextWindow != 200000 {
		t.Fatalf("ContextWindow = %d, want 200000 from the Zen metadata (Go advertises 1000000)", report[0].ContextWindow)
	}
	// The WebUI reads the flat JSON row, so pin the wire shape too: the embedded
	// probe state and the new field must both sit at the top level.
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(encoded, &rows); err != nil {
		t.Fatalf("decode %s: %v", encoded, err)
	}
	if len(rows) != 1 || rows[0]["model"] != "test-model-free" || rows[0]["context_window"] != float64(200000) {
		t.Fatalf("JSON rows = %s, want a flat model + context_window row", encoded)
	}
}

// Free models run on the anonymous lane, which is Zen regardless of the
// catalog's prefer tier, so the report must stay on the Zen window even when
// prefer=go (Go advertises 1000000 here).
func TestAvailabilityReportUsesZenWindowDespiteGoPrefer(t *testing.T) {
	report := availabilityReport(t, availabilityReportCatalog(config.TierGo))
	if len(report) != 1 {
		t.Fatalf("report = %+v, want exactly one free model", report)
	}
	if report[0].ContextWindow != 200000 {
		t.Fatalf("ContextWindow = %d, want 200000 from the Zen metadata (prefer is go)", report[0].ContextWindow)
	}
}

// No applied runtime yields an empty, non-nil report: the WebUI reads
// value.models and iterates it, so JSON must be [] rather than null.
func TestAvailabilityReportWithoutRuntimeIsEmpty(t *testing.T) {
	controller := &RuntimeController{}
	report := controller.AvailabilityReport()
	if report == nil || len(report) != 0 {
		t.Fatalf("report = %#v, want a non-nil empty slice", report)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "[]" {
		t.Fatalf("JSON = %s, want []", encoded)
	}
}
