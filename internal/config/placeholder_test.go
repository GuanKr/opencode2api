package config

import (
	"strings"
	"testing"
)

// The shipped example values are not credentials. Matching is exact after
// trimming and lowercasing, so a real key that merely starts with one of them
// survives — dropping a working key would take the tier offline silently.
func TestIsPlaceholderKey(t *testing.T) {
	placeholders := []string{"sk-your-zen-key", "  SK-Your-Zen-Key  ", "change-this-local-key"}
	for _, key := range placeholders {
		if !IsPlaceholderKey(key) {
			t.Fatalf("%q must be recognised as a placeholder", key)
		}
	}
	real := []string{
		"sk-your-zen-key-1",
		"sk-your-zen-key2",
		"sk-live-abcdef0123456789",
		"",
		"sk-your-go-key",
	}
	for _, key := range real {
		if IsPlaceholderKey(key) {
			t.Fatalf("%q must not be treated as a placeholder", key)
		}
	}
}

func TestDropPlaceholderKeysOnlyTouchesUpstreamPools(t *testing.T) {
	cfg := Config{
		ServerKeys: []string{"change-this-local-key"},
		ZenKeys:    []string{"sk-your-zen-key", "sk-live-zen"},
		GoKeys:     []string{" sk-your-zen-key ", "sk-live-go"},
	}
	got := DropPlaceholderKeys(cfg)

	if len(got.ZenKeys) != 1 || got.ZenKeys[0] != "sk-live-zen" {
		t.Fatalf("zen_keys must keep only the real key, got %v", got.ZenKeys)
	}
	if len(got.GoKeys) != 1 || got.GoKeys[0] != "sk-live-go" {
		t.Fatalf("go_keys must keep only the real key, got %v", got.GoKeys)
	}
	// A local server key is never sent upstream; filtering it would lock the
	// operator out of their own gateway.
	if len(got.ServerKeys) != 1 || got.ServerKeys[0] != "change-this-local-key" {
		t.Fatalf("server_keys must be left alone, got %v", got.ServerKeys)
	}
	// The input must not be mutated through a shared backing array. Checking
	// the length is not enough: an in-place `keys[:0]` compaction keeps len 2
	// and only moves the contents, so assert the element itself.
	if len(cfg.ZenKeys) != 2 || cfg.ZenKeys[0] != "sk-your-zen-key" || cfg.ZenKeys[1] != "sk-live-zen" {
		t.Fatalf("the caller's slice must not be rewritten in place, got %v", cfg.ZenKeys)
	}
}

// A config that only carries the example key has no usable credential. The
// failure has to say that, otherwise the operator reads "no key configured"
// while looking at a file that clearly lists one.
func TestNormalizePlaceholderOnlyKeysExplainThemselves(t *testing.T) {
	cfg := validTestConfig()
	cfg.Anonymous = false
	cfg.ZenKeys = []string{"sk-your-zen-key"}
	cfg.GoKeys = nil

	_, err := Normalize("", cfg)
	if err == nil {
		t.Fatal("a placeholder-only config must fail validation")
	}
	if !strings.Contains(err.Error(), "placeholder") {
		t.Fatalf("the error must name the placeholder cause, got %q", err)
	}
}

// Both anonymous-lane bounds are concrete after Normalize, so the console shows
// the number in force instead of a zero next to a README that says 8 and 900.
func TestNormalizeFillsAnonymousLaneDefaults(t *testing.T) {
	cfg := validTestConfig()
	cfg.Anonymous = true
	cfg.ZenKeys = nil
	cfg.GoKeys = nil
	cfg.Performance.MaxAnonymousExits = 0
	cfg.Performance.FreeTierBlockSeconds = 0

	got, err := Normalize("", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Performance.MaxAnonymousExits != DefaultMaxAnonymousExits {
		t.Fatalf("max_anonymous_exits = %d, want %d", got.Performance.MaxAnonymousExits, DefaultMaxAnonymousExits)
	}
	if got.Performance.FreeTierBlockSeconds != DefaultFreeTierBlockSeconds {
		t.Fatalf("free_tier_block_seconds = %d, want %d", got.Performance.FreeTierBlockSeconds, DefaultFreeTierBlockSeconds)
	}

	// An explicit value must survive: the default only fills the zero.
	cfg.Performance.MaxAnonymousExits = 2
	cfg.Performance.FreeTierBlockSeconds = 30
	got, err = Normalize("", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Performance.MaxAnonymousExits != 2 || got.Performance.FreeTierBlockSeconds != 30 {
		t.Fatalf("explicit bounds were overwritten: %+v", got.Performance)
	}
}

// Clone is what a runtime swap and GET /api/config hand out. Normalize writes
// the backoff block in place, so a shared pointer would let a read-only copy
// mutate the configuration in force.
func TestCloneCopiesAnonymousBackoff(t *testing.T) {
	original := &AnonymousBackoffConfig{Threshold: 4, BaseHours: 2, MaxHours: 6}
	cfg := validTestConfig()
	cfg.Models.AnonymousBackoff = original

	clone := Clone(cfg)
	if clone.Models.AnonymousBackoff == original {
		t.Fatal("Clone must not share the backoff pointer")
	}
	clone.Models.AnonymousBackoff.Threshold = 99
	if original.Threshold != 4 {
		t.Fatalf("mutating the clone changed the original: %d", original.Threshold)
	}
}

// validTestConfig is the smallest configuration Normalize accepts, so each test
// above can change exactly one field.
func validTestConfig() Config {
	return Config{
		Listen:     "127.0.0.1:8080",
		ServerKeys: []string{"local-test-key"},
		Upstream:   UpstreamConfig{Zen: "https://opencode.ai/zen", Go: "https://opencode.ai/zen/go"},
		Retry:      RetryConfig{MaxAttempts: 3, TimeoutSeconds: 300},
		Models:     ModelsConfig{RefreshSeconds: 300},
		Performance: PerformanceConfig{
			MaxIdleConns:           2048,
			MaxIdleConnsPerHost:    256,
			IdleConnTimeoutSeconds: 120,
			ConnectTimeoutSeconds:  5,
			FailureCooldownSeconds: 15,
		},
		Logging: LoggingConfig{Level: "info", RingSize: 2000},
		Prefer:  "go",
		WebUI:   WebUIConfig{Enabled: true, Listen: "127.0.0.1:8081", Username: "admin", PasswordHash: "$argon2id$test", SessionTTLMinutes: 720},
	}
}
