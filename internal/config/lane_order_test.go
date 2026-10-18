package config

import "testing"

// The lane order is a new setting, so the two ways it can go wrong are worth
// pinning: a typo silently reordering the free-tier lanes, and an empty value
// quietly disabling the /inference lane the whole fork depends on.

// TestNormalizeLaneOrderDefaultsToInferenceOnly: an absent setting must leave
// the gateway exactly where it was before the legacy lane existed. Defaulting
// to both lanes is NOT the same thing — the legacy fallback fires on every
// quota 429, and its 403 gate text is classified as quota, which sends the
// failover script switching exit nodes for an endpoint problem.
func TestNormalizeLaneOrderDefaultsToInferenceOnly(t *testing.T) {
	for _, in := range [][]string{nil, {}} {
		got, err := NormalizeLaneOrder(in)
		if err != nil {
			t.Fatalf("NormalizeLaneOrder(%v): %v", in, err)
		}
		if len(got) != 1 || got[0] != LaneInference {
			t.Fatalf("NormalizeLaneOrder(%v) = %v, want [%q]", in, got, LaneInference)
		}
	}
}

// TestNormalizeLaneOrderRejectsUnknownTokens: a dropped typo must fail startup.
// Silently ignoring it would change which upstream every anonymous request
// reaches, which is the one thing this setting exists to keep visible.
func TestNormalizeLaneOrderRejectsUnknownTokens(t *testing.T) {
	_, err := NormalizeLaneOrder([]string{LaneInference, "inferance"})
	if err == nil {
		t.Fatal("a misspelled lane token must be a startup error, not a silent drop")
	}
}

// TestNormalizeLaneOrderTrimsAndDeduplicates keeps the value honest regardless
// of how it was typed: " Inference " and "inference" are one lane, and asking
// for legacy twice must not produce two attempts at the same origin.
func TestNormalizeLaneOrderTrimsAndDeduplicates(t *testing.T) {
	got, err := NormalizeLaneOrder([]string{" Inference ", "inference", " LEGACY "})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != LaneInference || got[1] != LaneLegacy {
		t.Fatalf("NormalizeLaneOrder = %v, want [%q %q]", got, LaneInference, LaneLegacy)
	}
}

// TestNormalizeLaneOrderFallsBackWhenAllEntriesAreBlank: whitespace is not a
// configuration. Falling back to the safe default beats coming up empty, which
// would leave anonymousUpstreams with nothing to try.
func TestNormalizeLaneOrderFallsBackWhenAllEntriesAreBlank(t *testing.T) {
	got, err := NormalizeLaneOrder([]string{"", "   "})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != LaneInference {
		t.Fatalf("NormalizeLaneOrder = %v, want the [%q] default", got, LaneInference)
	}
}
