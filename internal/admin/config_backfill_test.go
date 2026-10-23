package admin

import (
	"testing"

	"opencode2api/internal/config"
)

// A PUT built from the console form omits every field the form does not
// render. Losing these silently changes routing behaviour: the lane order picks
// which free-tier endpoint is dialled, max_anonymous_exits bounds how many
// exits a request burns, and a nil anonymous_backoff turns the lane-level
// breaker off.
func TestPreserveUnrenderedSettingsKeepsConsoleOmissions(t *testing.T) {
	current := config.Config{
		Upstream: config.UpstreamConfig{
			Anonymous:          "https://opencode.ai/inference",
			AnonymousLaneOrder: []string{"inference", "legacy"},
		},
		Performance: config.PerformanceConfig{
			MaxAnonymousExits:    40,
			FreeTierBlockSeconds: 1800,
		},
		Models: config.ModelsConfig{
			AnonymousBackoff: &config.AnonymousBackoffConfig{Threshold: 3, BaseHours: 2, MaxHours: 8},
		},
	}
	// What the form sends: the rendered fields only.
	update := ConfigUpdate{
		Performance: config.PerformanceConfig{MaxIdleConns: 64, FailureCooldownSeconds: 20},
	}

	got := preserveUnrenderedSettings(current, update)

	if len(got.Upstream.AnonymousLaneOrder) != 2 || got.Upstream.AnonymousLaneOrder[1] != "legacy" {
		t.Fatalf("anonymous_lane_order was dropped: %v", got.Upstream.AnonymousLaneOrder)
	}
	if got.Upstream.Anonymous != current.Upstream.Anonymous {
		t.Fatalf("upstream.anonymous was dropped: %q", got.Upstream.Anonymous)
	}
	if got.Performance.MaxAnonymousExits != 40 || got.Performance.FreeTierBlockSeconds != 1800 {
		t.Fatalf("anonymous exit knobs were reset: %+v", got.Performance)
	}
	if got.Performance.MaxIdleConns != 64 || got.Performance.FailureCooldownSeconds != 20 {
		t.Fatalf("rendered fields must come from the update: %+v", got.Performance)
	}
	if got.Models.AnonymousBackoff == nil || got.Models.AnonymousBackoff.Threshold != 3 || got.Models.AnonymousBackoff.MaxHours != 8 {
		t.Fatalf("models.anonymous_backoff was dropped: %+v", got.Models.AnonymousBackoff)
	}
	if got.Models.AnonymousBackoff == current.Models.AnonymousBackoff {
		t.Fatal("the backoff block must be copied, not aliased: a later Normalize would mutate the live config")
	}
}

// An explicit value in the update still wins, so an operator editing config.json
// and reloading is not fought by the backfill.
func TestPreserveUnrenderedSettingsPrefersExplicitValues(t *testing.T) {
	current := config.Config{
		Upstream:    config.UpstreamConfig{AnonymousLaneOrder: []string{"inference"}},
		Performance: config.PerformanceConfig{MaxAnonymousExits: 40, FreeTierBlockSeconds: 1800},
		Models:      config.ModelsConfig{AnonymousBackoff: &config.AnonymousBackoffConfig{Threshold: 3}},
	}
	update := ConfigUpdate{
		Upstream:    config.UpstreamConfig{Anonymous: "https://example.test/inference", AnonymousLaneOrder: []string{"legacy"}},
		Performance: config.PerformanceConfig{MaxAnonymousExits: 2, FreeTierBlockSeconds: 60},
		Models:      config.ModelsConfig{AnonymousBackoff: &config.AnonymousBackoffConfig{Threshold: 1, BaseHours: 1, MaxHours: 2}},
	}

	got := preserveUnrenderedSettings(current, update)

	if len(got.Upstream.AnonymousLaneOrder) != 1 || got.Upstream.AnonymousLaneOrder[0] != "legacy" {
		t.Fatalf("explicit lane order must win: %v", got.Upstream.AnonymousLaneOrder)
	}
	if got.Performance.MaxAnonymousExits != 2 || got.Performance.FreeTierBlockSeconds != 60 {
		t.Fatalf("explicit exit knobs must win: %+v", got.Performance)
	}
	if got.Models.AnonymousBackoff == nil || got.Models.AnonymousBackoff.Threshold != 1 {
		t.Fatalf("explicit backoff must win: %+v", got.Models.AnonymousBackoff)
	}
}

// The assembly, not just the helper, has to carry the unrendered settings
// through: a unit test on preserveUnrenderedSettings stays green if this call
// site ever drops it, which is the exact bug the backfill exists to prevent.
func TestBuildConfigCandidateCarriesUnrenderedSettings(t *testing.T) {
	current := config.Config{
		Listen:     "127.0.0.1:8080",
		ServerKeys: []string{"local-key"},
		Rotation:   config.RotationConfig{},
		Upstream: config.UpstreamConfig{
			Zen:                "https://opencode.ai/zen",
			Go:                 "https://opencode.ai/zen/go",
			Anonymous:          "https://opencode.ai/inference",
			AnonymousLaneOrder: []string{"inference", "legacy"},
		},
		Performance: config.PerformanceConfig{
			MaxIdleConns:         2048,
			MaxAnonymousExits:    40,
			FreeTierBlockSeconds: 1800,
		},
		Models: config.ModelsConfig{AnonymousBackoff: &config.AnonymousBackoffConfig{Threshold: 3, BaseHours: 2, MaxHours: 8}},
		WebUI:  config.WebUIConfig{Enabled: true, Listen: "127.0.0.1:8081", Username: "admin", PasswordHash: "hash", SessionTTLMinutes: 720},
	}
	// Exactly what webui/js/pages/config.js submits: no lane order, no exit
	// cap, no block window, no backoff block.
	update := ConfigUpdate{
		Listen:      "127.0.0.1:9090",
		ServerKeys:  []SecretInput{{Value: "local-key"}},
		Anonymous:   true,
		Upstream:    config.UpstreamConfig{Zen: "https://opencode.ai/zen", Go: "https://opencode.ai/zen/go"},
		Performance: config.PerformanceConfig{MaxIdleConns: 64, FailureCooldownSeconds: 15},
		Models:      config.ModelsConfig{RefreshSeconds: 300},
		WebUI:       WebUIView{Enabled: true, Listen: "127.0.0.1:8081", SessionTTLMinutes: 720},
	}

	candidate, code, err := buildConfigCandidate(current, update)
	if err != nil {
		t.Fatalf("unexpected rejection (%s): %v", code, err)
	}
	if candidate.Listen != "127.0.0.1:9090" || candidate.Performance.MaxIdleConns != 64 {
		t.Fatalf("submitted fields must be applied: %+v", candidate)
	}
	if len(candidate.Upstream.AnonymousLaneOrder) != 2 {
		t.Fatalf("anonymous_lane_order was dropped by the assembly: %v", candidate.Upstream.AnonymousLaneOrder)
	}
	if candidate.Performance.MaxAnonymousExits != 40 || candidate.Performance.FreeTierBlockSeconds != 1800 {
		t.Fatalf("anonymous exit knobs were dropped by the assembly: %+v", candidate.Performance)
	}
	if candidate.Models.AnonymousBackoff == nil || candidate.Models.AnonymousBackoff.MaxHours != 8 {
		t.Fatalf("anonymous_backoff was dropped by the assembly: %+v", candidate.Models.AnonymousBackoff)
	}
	if candidate.WebUI.Username != "admin" || candidate.WebUI.PasswordHash != "hash" {
		t.Fatalf("credentials the form does not carry must come from current: %+v", candidate.WebUI)
	}
}

func TestBuildConfigCandidateReportsTheFailingSecret(t *testing.T) {
	current := config.Config{ServerKeys: []string{"local-key"}}
	// A stale fingerprint: the form sent the id of a key that no longer exists.
	_, code, err := buildConfigCandidate(current, ConfigUpdate{ServerKeys: []SecretInput{{ID: "deadbeef"}}})
	if err == nil {
		t.Fatal("an unknown secret id must be rejected")
	}
	if code != "invalid_server_keys" {
		t.Fatalf("the rejection must name the field, got %q", code)
	}
}
