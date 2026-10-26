package config

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeFillsStreamIdleTimeout(t *testing.T) {
	cfg := validTestConfig()
	cfg.Anonymous = true
	cfg.ZenKeys = nil
	cfg.GoKeys = nil
	cfg.Performance.StreamIdleTimeoutSeconds = 0

	got, err := Normalize("", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Performance.StreamIdleTimeoutSeconds != DefaultStreamIdleTimeoutSeconds {
		t.Fatalf("stream_idle_timeout_seconds = %d, want %d", got.Performance.StreamIdleTimeoutSeconds, DefaultStreamIdleTimeoutSeconds)
	}
	if got.Performance.StreamIdleTimeout() != 60*time.Second {
		t.Fatalf("StreamIdleTimeout() = %s, want 1m", got.Performance.StreamIdleTimeout())
	}

	cfg.Performance.StreamIdleTimeoutSeconds = 300
	got, err = Normalize("", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Performance.StreamIdleTimeoutSeconds != 300 {
		t.Fatalf("an explicit bound was overwritten: %d", got.Performance.StreamIdleTimeoutSeconds)
	}
}

func TestNormalizeRejectsStreamIdleTimeoutOutOfRange(t *testing.T) {
	for _, value := range []int{-1, 86401} {
		cfg := validTestConfig()
		cfg.Anonymous = true
		cfg.ZenKeys = nil
		cfg.GoKeys = nil
		cfg.Performance.StreamIdleTimeoutSeconds = value
		_, err := Normalize("", cfg)
		if err == nil {
			t.Fatalf("stream_idle_timeout_seconds=%d must be rejected", value)
		}
		if !strings.Contains(err.Error(), "stream_idle_timeout_seconds") {
			t.Fatalf("error must name the field, got %q", err)
		}
	}
}

func TestPerformanceStreamIdleTimeoutZeroMeansDisabled(t *testing.T) {
	if got := (PerformanceConfig{}).StreamIdleTimeout(); got != 0 {
		t.Fatalf("zero StreamIdleTimeoutSeconds = %s, want 0", got)
	}
}
