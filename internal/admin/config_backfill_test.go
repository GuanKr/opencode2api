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

// The console form now renders the committed-stream idle bound, so zero is an
// explicit "use the built-in default" and must not be backfilled from current.
func TestPreserveUnrenderedSettingsDoesNotBackfillStreamIdleTimeout(t *testing.T) {
	current := config.Config{Performance: config.PerformanceConfig{StreamIdleTimeoutSeconds: 300}}

	got := preserveUnrenderedSettings(current, ConfigUpdate{})
	if got.Performance.StreamIdleTimeoutSeconds != 0 {
		t.Fatalf("zero must survive so Normalize can apply the default: %+v", got.Performance)
	}

	got = preserveUnrenderedSettings(current, ConfigUpdate{
		Performance: config.PerformanceConfig{StreamIdleTimeoutSeconds: 0},
	})
	if got.Performance.StreamIdleTimeoutSeconds != 0 {
		t.Fatalf("an explicit zero must survive: %+v", got.Performance)
	}

	got = preserveUnrenderedSettings(current, ConfigUpdate{
		Performance: config.PerformanceConfig{StreamIdleTimeoutSeconds: 120},
	})
	if got.Performance.StreamIdleTimeoutSeconds != 120 {
		t.Fatalf("an explicit stream idle bound must win: %+v", got.Performance)
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

// A PUT that omits warp must keep the switch. config.WarpConfig is a value
// type, so it cannot tell "absent" from "explicitly off"; without the pointer
// an older console or a script silently disables WARP on every save.
func TestBuildConfigCandidateKeepsWarpWhenTheFormOmitsIt(t *testing.T) {
	current := config.Config{
		ServerKeys: []string{"local-key"},
		Warp:       config.WarpConfig{Enabled: true, PoolSize: 3, Stack: "ipv6"},
	}
	candidate, code, err := buildConfigCandidate(current, ConfigUpdate{})
	if err != nil {
		t.Fatalf("unexpected rejection (%s): %v", code, err)
	}
	if !candidate.Warp.Enabled || candidate.Warp.PoolSize != 3 || candidate.Warp.Stack != "ipv6" {
		t.Fatalf("an omitted warp block must keep the current switch: %+v", candidate.Warp)
	}

	explicit := config.WarpConfig{Enabled: false}
	candidate, code, err = buildConfigCandidate(current, ConfigUpdate{Warp: &explicit})
	if err != nil {
		t.Fatalf("unexpected rejection (%s): %v", code, err)
	}
	if candidate.Warp.Enabled {
		t.Fatalf("an explicit warp block must win: %+v", candidate.Warp)
	}
}

// The console form does not render prompt_env, so an unrelated save must keep
// the configured <env> strings instead of resetting them to the zero value (and
// Normalize would then quietly substitute its default).
func TestBuildConfigCandidateKeepsPromptEnvWhenTheFormOmitsIt(t *testing.T) {
	current := config.Config{
		ServerKeys: []string{"local-key"},
		PromptEnv:  config.PromptEnvConfig{WorkingDirectory: `D:\Program Files\opencode2api`, WorkspaceRoot: `D:\Program Files\opencode2api`},
	}
	candidate, code, err := buildConfigCandidate(current, ConfigUpdate{})
	if err != nil {
		t.Fatalf("unexpected rejection (%s): %v", code, err)
	}
	if candidate.PromptEnv != current.PromptEnv {
		t.Fatalf("an omitted prompt_env must keep the configured strings: %+v", candidate.PromptEnv)
	}

	explicit := config.PromptEnvConfig{WorkingDirectory: "/srv/agent", WorkspaceRoot: "/srv/agent-root"}
	candidate, code, err = buildConfigCandidate(current, ConfigUpdate{PromptEnv: &explicit})
	if err != nil {
		t.Fatalf("unexpected rejection (%s): %v", code, err)
	}
	if candidate.PromptEnv != explicit {
		t.Fatalf("an explicit prompt_env must win: %+v", candidate.PromptEnv)
	}
}

// The console form renders enabled/pool_size/stack but not warp.proxy, so a
// save from it would push the WARP outer path back to flaky direct UDP with
// applied:true and no log line — the same silent-loss shape as the lane order.
func TestBuildConfigCandidateKeepsWarpProxyWhenTheFormOmitsIt(t *testing.T) {
	current := config.Config{
		ServerKeys: []string{"local-key"},
		Warp:       config.WarpConfig{Enabled: true, PoolSize: 2, Stack: "ipv6", Proxy: "socks5://127.0.0.1:7890"},
	}
	// What the form submits: the warp block without the proxy field.
	submitted := config.WarpConfig{Enabled: true, PoolSize: 2, Stack: "ipv6"}
	candidate, code, err := buildConfigCandidate(current, ConfigUpdate{Warp: &submitted})
	if err != nil {
		t.Fatalf("unexpected rejection (%s): %v", code, err)
	}
	if candidate.Warp.Proxy != "socks5://127.0.0.1:7890" {
		t.Fatalf("warp.proxy was dropped by the console form: %+v", candidate.Warp)
	}
	// An omitted warp block keeps the whole current block, proxy included.
	candidate, code, err = buildConfigCandidate(current, ConfigUpdate{})
	if err != nil {
		t.Fatalf("unexpected rejection (%s): %v", code, err)
	}
	if candidate.Warp.Proxy != "socks5://127.0.0.1:7890" {
		t.Fatalf("warp.proxy was dropped with the omitted block: %+v", candidate.Warp)
	}
}
