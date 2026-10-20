package models

import (
	"encoding/json"
	"testing"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
)

// contextCatalog advertises one model on Zen and Go with deliberately different
// context windows, so a tier-blind lookup shows up as the wrong number.
func contextCatalog(t *testing.T, model string, prefer config.Tier) *ModelCatalog {
	t.Helper()
	catalog := Catalog(prefer, nil)
	catalog.ReplaceWithCapabilities(
		[]string{model},
		[]string{model},
		map[config.Tier]map[string]wire.Protocol{
			config.TierZen: {model: wire.Chat},
			config.TierGo:  {model: wire.Chat},
		},
		nil,
		map[config.Tier]map[string]Metadata{
			config.TierZen: {model: {ContextWindow: 200000, MaxOutput: 32000}},
			config.TierGo:  {model: {ContextWindow: 1000000, MaxOutput: 131072}},
		},
	)
	return catalog
}

// A free model is served anonymously, which is a Zen route, so the diagnostic
// must report the Zen window even though Go advertises a larger one.
func TestDiagnosticContextWindowFollowsAnonymousZenRoute(t *testing.T) {
	catalog := contextCatalog(t, "test-model-free", config.TierZen)
	diagnostic := catalog.Diagnostic("test-model-free", "", true, true, true)
	if diagnostic.RouteError != "" {
		t.Fatalf("route error = %q, want the anonymous route", diagnostic.RouteError)
	}
	if !diagnostic.Anonymous || diagnostic.Tier != config.TierZen {
		t.Fatalf("route = tier %q anonymous %v, want the anonymous Zen route", diagnostic.Tier, diagnostic.Anonymous)
	}
	if diagnostic.ContextWindow != 200000 {
		t.Fatalf("ContextWindow = %d, want 200000 from the serving Zen tier (Go advertises 1000000)", diagnostic.ContextWindow)
	}
}

// A free model stays on the anonymous Zen route even when the catalog prefers
// Go, so the window must not follow prefer. This is the shape that catches a
// "use the prefer tier" regression — the tier-blind bug this column exists to
// prevent.
func TestDiagnosticContextWindowFollowsAnonymousZenDespiteGoPrefer(t *testing.T) {
	catalog := contextCatalog(t, "test-model-free", config.TierGo)
	diagnostic := catalog.Diagnostic("test-model-free", "", true, true, true)
	if diagnostic.RouteError != "" {
		t.Fatalf("route error = %q, want the anonymous route", diagnostic.RouteError)
	}
	if !diagnostic.Anonymous || diagnostic.Tier != config.TierZen {
		t.Fatalf("route = tier %q anonymous %v, want the anonymous Zen route despite prefer=go", diagnostic.Tier, diagnostic.Anonymous)
	}
	if diagnostic.ContextWindow != 200000 {
		t.Fatalf("ContextWindow = %d, want 200000 from the serving Zen tier (prefer is go, which advertises 1000000)", diagnostic.ContextWindow)
	}
}

// When Go serves the request, the Go window is the one that applies.
func TestDiagnosticContextWindowFollowsGoRoute(t *testing.T) {
	catalog := contextCatalog(t, "test-model-paid", config.TierGo)
	diagnostic := catalog.Diagnostic("test-model-paid", "", true, true, false)
	if diagnostic.RouteError != "" {
		t.Fatalf("route error = %q, want the Go key route", diagnostic.RouteError)
	}
	if diagnostic.Anonymous || diagnostic.Tier != config.TierGo {
		t.Fatalf("route = tier %q anonymous %v, want the Go key route", diagnostic.Tier, diagnostic.Anonymous)
	}
	if diagnostic.ContextWindow != 1000000 {
		t.Fatalf("ContextWindow = %d, want 1000000 from the serving Go tier (Zen advertises 200000)", diagnostic.ContextWindow)
	}
}

// The WebUI consumes the serialized diagnostic, so the JSON name is part of the
// contract: a wrong tag would render a dash in every routing-table row.
func TestDiagnosticContextWindowJSONField(t *testing.T) {
	catalog := contextCatalog(t, "test-model-free", config.TierZen)
	diagnostic := catalog.Diagnostic("test-model-free", "", true, true, true)
	encoded, err := json.Marshal(diagnostic)
	if err != nil {
		t.Fatal(err)
	}
	var row map[string]any
	if err := json.Unmarshal(encoded, &row); err != nil {
		t.Fatalf("decode %s: %v", encoded, err)
	}
	if row["context_window"] != float64(200000) {
		t.Fatalf("context_window = %v in %s, want 200000", row["context_window"], encoded)
	}
}

// Without credentials no tier serves the model, but the diagnostics table still
// shows the window of the tier routing would try first (prefer order) instead
// of a wall of dashes.
func TestDiagnosticContextWindowFallsBackToAdvertisingTierWithoutRoute(t *testing.T) {
	catalog := contextCatalog(t, "test-model-paid", config.TierZen)
	diagnostic := catalog.Diagnostic("test-model-paid", "", false, false, false)
	if diagnostic.RouteError == "" {
		t.Fatal("want a routing error without any credential")
	}
	if diagnostic.ContextWindow != 200000 {
		t.Fatalf("ContextWindow = %d, want 200000 from the preferred advertising tier (Go advertises 1000000)", diagnostic.ContextWindow)
	}
}

// The fallback follows prefer order: with prefer=go the Go window wins.
func TestDiagnosticContextWindowFallbackFollowsGoPrefer(t *testing.T) {
	catalog := contextCatalog(t, "test-model-paid", config.TierGo)
	diagnostic := catalog.Diagnostic("test-model-paid", "", false, false, false)
	if diagnostic.RouteError == "" {
		t.Fatal("want a routing error without any credential")
	}
	if diagnostic.ContextWindow != 1000000 {
		t.Fatalf("ContextWindow = %d, want 1000000 from the preferred Go tier (Zen advertises 200000)", diagnostic.ContextWindow)
	}
}

// A Go-only model has no Zen window to show, so the fallback must use Go.
func TestDiagnosticContextWindowFallbackUsesGoOnlyTier(t *testing.T) {
	model := "test-go-only"
	catalog := Catalog(config.TierZen, nil)
	catalog.ReplaceWithCapabilities(
		nil,
		[]string{model},
		map[config.Tier]map[string]wire.Protocol{config.TierGo: {model: wire.Chat}},
		nil,
		map[config.Tier]map[string]Metadata{config.TierGo: {model: {ContextWindow: 1000000}}},
	)
	diagnostic := catalog.Diagnostic(model, "", false, false, false)
	if diagnostic.RouteError == "" {
		t.Fatal("want a routing error without any credential")
	}
	if diagnostic.ContextWindow != 1000000 {
		t.Fatalf("ContextWindow = %d, want the Go-only model's window 1000000", diagnostic.ContextWindow)
	}
}

// The preferred advertising tier may have no window at all; the fallback then
// uses the other advertising tier instead of reporting a dash.
func TestDiagnosticContextWindowFallbackSkipsTierWithoutWindow(t *testing.T) {
	model := "test-model-paid"
	catalog := Catalog(config.TierZen, nil)
	catalog.ReplaceWithCapabilities(
		[]string{model},
		[]string{model},
		map[config.Tier]map[string]wire.Protocol{
			config.TierZen: {model: wire.Chat},
			config.TierGo:  {model: wire.Chat},
		},
		nil,
		map[config.Tier]map[string]Metadata{
			config.TierZen: {model: {}}, // the preferred tier states no limit
			config.TierGo:  {model: {ContextWindow: 1000000}},
		},
	)
	diagnostic := catalog.Diagnostic(model, "", false, false, false)
	if diagnostic.RouteError == "" {
		t.Fatal("want a routing error without any credential")
	}
	if diagnostic.ContextWindow != 1000000 {
		t.Fatalf("ContextWindow = %d, want 1000000 from the other advertising tier", diagnostic.ContextWindow)
	}
}

// A model no tier advertises has no window; the column renders a dash.
func TestDiagnosticContextWindowZeroForUnknownModel(t *testing.T) {
	catalog := contextCatalog(t, "test-model-paid", config.TierZen)
	diagnostic := catalog.Diagnostic("not-in-catalog", "", false, false, false)
	if diagnostic.RouteError == "" {
		t.Fatal("want a routing error for a model no tier advertises")
	}
	if diagnostic.ContextWindow != 0 {
		t.Fatalf("ContextWindow = %d, want 0 for a model no tier advertises", diagnostic.ContextWindow)
	}
}

// The capability catalog is authoritative but may omit the limit; the pricing
// metadata then supplies it (see MetadataForTier).
func TestDiagnosticContextWindowFallsBackToPricingMetadata(t *testing.T) {
	model := "test-model-paid"
	catalog := Catalog(config.TierZen, nil)
	catalog.ReplaceWithCapabilities(
		[]string{model},
		nil,
		map[config.Tier]map[string]wire.Protocol{config.TierZen: {model: wire.Chat}},
		nil,
		nil,
	)
	catalog.SetPricingStore(&PricingRegistry{models: map[string]Price{model: {ID: model, ContextWindow: 300000}}})

	diagnostic := catalog.Diagnostic(model, "", true, false, false)
	if diagnostic.RouteError != "" {
		t.Fatalf("route error = %q, want the Zen key route", diagnostic.RouteError)
	}
	if diagnostic.ContextWindow != 300000 {
		t.Fatalf("ContextWindow = %d, want 300000 from the pricing metadata", diagnostic.ContextWindow)
	}
}
