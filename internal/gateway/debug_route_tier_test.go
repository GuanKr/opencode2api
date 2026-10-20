package gateway

import (
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

// DebugRouteForTier pins one key tier, so the context window it reports must be
// the pinned tier's window, not the automatic route's. This mirrors the real
// qwen3.8-max shape: Zen 262144, Go 1000000.
func TestDebugRouteForTierReportsPinnedTierWindow(t *testing.T) {
	model := "test-model-paid"
	catalog := models.Catalog(config.TierZen, nil)
	catalog.ReplaceWithCapabilities(
		[]string{model},
		[]string{model},
		map[config.Tier]map[string]wire.Protocol{
			config.TierZen: {model: wire.Chat},
			config.TierGo:  {model: wire.Chat},
		},
		nil,
		map[config.Tier]map[string]models.Metadata{
			config.TierZen: {model: {ContextWindow: 262144}},
			config.TierGo:  {model: {ContextWindow: 1000000}},
		},
	)
	controller := &RuntimeController{}
	controller.current.Store(&gatewayRuntime{gateway: &Gateway{
		catalog: catalog,
		cfg:     config.Config{ZenKeys: []string{"zen-key"}, GoKeys: []string{"go-key"}},
	}})

	diagnostic, err := controller.DebugRouteForTier(model, config.TierGo)
	if err != nil {
		t.Fatalf("DebugRouteForTier: %v", err)
	}
	if diagnostic.Tier != config.TierGo {
		t.Fatalf("Tier = %q, want go", diagnostic.Tier)
	}
	if diagnostic.ContextWindow != 1000000 {
		t.Fatalf("ContextWindow = %d, want 1000000 from the pinned Go tier (the automatic route would show Zen 262144)", diagnostic.ContextWindow)
	}
}
