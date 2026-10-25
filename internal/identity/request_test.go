package identity

import (
	"strings"
	"testing"
)

// officialSessionIDFixture is an id shaped the way the official client mints
// one: ses_ + a descending timestamp (counter 1, low bits ffe) + 14 base62.
const officialSessionIDFixture = "ses_000000000ffeAAAAAAAAAAAAAA"

// TestWithSessionGenerationZeroReturnsTheClientConversation: generation 0 is
// the conversation the client started, so nothing may move.
func TestWithSessionGenerationZeroReturnsTheClientConversation(t *testing.T) {
	ids := RequestIDs{
		Session:       CanonicalSessionID("conversation"),
		ChildSession:  CanonicalSessionID("child"),
		ParentSession: CanonicalSessionID("parent"),
		Request:       "req_1",
	}
	if got := ids.WithSessionGeneration(0); got != ids {
		t.Fatalf("generation 0 must return the ids unchanged, got %+v", got)
	}
	if got := ids.WithSessionGeneration(-1); got != ids {
		t.Fatalf("a negative generation must behave like 0, got %+v", got)
	}
}

// TestWithSessionGenerationRotatesEveryField: a rotated session must keep the
// official ses_ shape (the free tier 403s anything else), move all three
// session fields together so the parent/child headers stay consistent, and be
// deterministic for a given generation.
func TestWithSessionGenerationRotatesEveryField(t *testing.T) {
	ids := RequestIDs{
		Session:       CanonicalSessionID("conversation"),
		ChildSession:  CanonicalSessionID("child"),
		ParentSession: CanonicalSessionID("parent"),
	}
	first := ids.WithSessionGeneration(1)
	if first.Session == ids.Session || first.ChildSession == ids.ChildSession || first.ParentSession == ids.ParentSession {
		t.Fatalf("every session field must move, got %+v", first)
	}
	for name, value := range map[string]string{
		"session": first.Session,
		"child":   first.ChildSession,
		"parent":  first.ParentSession,
	} {
		if !isOfficialSessionID(value) {
			t.Fatalf("%s must keep the official ses_ shape, got %q", name, value)
		}
	}
	if again := ids.WithSessionGeneration(1); again != first {
		t.Fatalf("one generation must be deterministic:\n%+v\n%+v", first, again)
	}
	if second := ids.WithSessionGeneration(2); second.Session == first.Session {
		t.Fatalf("generation 2 must differ from generation 1, both %q", second.Session)
	}
}

// TestWithSessionGenerationStopsOfficialPassthrough pins the deliberate break
// with the generation-0 rule: a client-minted id passes through untouched, but
// once a generation exists the upstream session is ours, not the client's.
func TestWithSessionGenerationStopsOfficialPassthrough(t *testing.T) {
	if !isOfficialSessionID(officialSessionIDFixture) {
		t.Fatalf("fixture %q must look official", officialSessionIDFixture)
	}
	if CanonicalSessionID(officialSessionIDFixture) != officialSessionIDFixture {
		t.Fatal("an official id must pass through at generation 0")
	}
	rotated := RequestIDs{Session: officialSessionIDFixture}.WithSessionGeneration(1)
	if rotated.Session == officialSessionIDFixture {
		t.Fatal("a rotated session must not reuse the client's own id")
	}
	if !isOfficialSessionID(rotated.Session) {
		t.Fatalf("the rotated id must still be official-shaped, got %q", rotated.Session)
	}
	if strings.Contains(rotated.Session, "#gen") {
		t.Fatalf("the generation tag must be hashed, not embedded: %q", rotated.Session)
	}
}

// TestWithSessionGenerationLeavesAbsentFieldsAbsent: a conversation without a
// child or parent must not gain an empty-looking session header.
func TestWithSessionGenerationLeavesAbsentFieldsAbsent(t *testing.T) {
	rotated := RequestIDs{Session: CanonicalSessionID("conversation")}.WithSessionGeneration(3)
	if rotated.ChildSession != "" || rotated.ParentSession != "" {
		t.Fatalf("absent fields must stay absent, got %+v", rotated)
	}
}
