package gateway

import (
	"net/http"
	"testing"

	"opencode2api/internal/identity"
	wire "opencode2api/internal/protocol"
)

// The v2 identity is an affinity key plus the client's own child id, not four
// identical values. The three affinity headers carry the conversation (the
// canonical parent when the client names one); x-opencode-session-id carries the
// child, which is what a subagent keeps so the upstream can tell the two apart.
//
// This pin exists because the shape is quoted as measured fact in AGENTS.md and
// nothing else covers it: a refactor that made all four equal again would look
// harmless and would quietly change what the free-tier gate sees.
func TestV2SessionHeadersFollowTheAffinitySplit(t *testing.T) {
	childID := identity.CanonicalSessionID("ses_child")
	parentID := identity.CanonicalSessionID("ses_parent")

	cases := []struct {
		name      string
		ids       identity.RequestIDs
		wantAff   string
		wantChild string
		wantParen string
	}{
		{
			// The ordinary case: the client sends one session header, so all
			// four values coincide.
			name:      "plain conversation",
			ids:       identity.RequestIDs{Session: "ses_root"},
			wantAff:   "ses_root",
			wantChild: "ses_root",
		},
		{
			name:      "subagent keeps its child id",
			ids:       identity.RequestIDs{Session: "ses_root", ChildSession: childID},
			wantAff:   "ses_root",
			wantChild: childID,
		},
		{
			name:      "parent becomes the affinity key",
			ids:       identity.RequestIDs{Session: "ses_root", ChildSession: childID, ParentSession: "ses_parent"},
			wantAff:   parentID,
			wantChild: childID,
			wantParen: parentID,
		},
	}
	for _, tc := range cases {
		header := http.Header{}
		setV2Headers(header, wire.Chat, tc.ids)

		if got := header.Get("x-opencode-session"); got != tc.wantAff {
			t.Fatalf("%s: x-opencode-session = %q, want %q", tc.name, got, tc.wantAff)
		}
		if got := header.Get("x-session-affinity"); got != tc.wantAff {
			t.Fatalf("%s: x-session-affinity = %q, want %q", tc.name, got, tc.wantAff)
		}
		if got := header.Get("X-Session-Id"); got != tc.wantAff {
			t.Fatalf("%s: X-Session-Id = %q, want %q", tc.name, got, tc.wantAff)
		}
		if got := header.Get("x-opencode-session-id"); got != tc.wantChild {
			t.Fatalf("%s: x-opencode-session-id = %q, want %q", tc.name, got, tc.wantChild)
		}
		if got := header.Get("x-opencode-parent-session-id"); got != tc.wantParen {
			t.Fatalf("%s: x-opencode-parent-session-id = %q, want %q", tc.name, got, tc.wantParen)
		}
		// The v2 gate 403s on this header, so its absence is part of the shape.
		if header.Get("x-opencode-request") != "" {
			t.Fatalf("%s: x-opencode-request must never be sent on the v2 lane", tc.name)
		}
		if header.Get("x-opencode-client") != "cli" {
			t.Fatalf("%s: x-opencode-client = %q", tc.name, header.Get("x-opencode-client"))
		}
	}
}
