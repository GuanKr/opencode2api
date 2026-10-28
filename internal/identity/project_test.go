package identity

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

// TestDeriveRequestIDsMintsAFreshProjectWhenTheClientNamesNone: the free tier
// ledges quota per project id and egress together, so the old shared default was
// one shared bucket that popular exits had already burned. The derived id must
// be the official 40-hex shape and differ per request.
func TestDeriveRequestIDsMintsAFreshProjectWhenTheClientNamesNone(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	body := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hi"}}}
	shape := regexp.MustCompile(`^[0-9a-f]{40}$`)

	first := DeriveRequestIDs(req, body, "scope-a")
	second := DeriveRequestIDs(req, body, "scope-a")
	if !shape.MatchString(first.Project) {
		t.Fatalf("a nameless project must be 40 lowercase hex, got %q", first.Project)
	}
	if first.Project == second.Project {
		t.Fatalf("two requests reused the project id %q; a burned bucket would poison both", first.Project)
	}
}

// TestDeriveRequestIDsKeepsAClientNamedProject: a client that does name a
// project keeps it, so its own quota bucket and any server-side grouping stay
// stable.
func TestDeriveRequestIDsKeepsAClientNamedProject(t *testing.T) {
	const project = "0123456789abcdef0123456789abcdef01234567"
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("x-opencode-project", project)
	got := DeriveRequestIDs(req, map[string]any{}, "scope-a")
	if got.Project != project {
		t.Fatalf("a client-named project was replaced: %q", got.Project)
	}
}
