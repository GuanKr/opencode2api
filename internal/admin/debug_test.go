package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/gateway"
	"opencode2api/internal/telemetry"
)

func TestSetPlaygroundSessionIsFresh(t *testing.T) {
	first := make(http.Header)
	second := make(http.Header)
	setPlaygroundSession(first)
	setPlaygroundSession(second)

	session := first.Get("x-opencode-session")
	if session == "" {
		t.Fatal("playground session header is empty")
	}
	if session == second.Get("x-opencode-session") {
		t.Fatalf("playground session header repeated: %q", session)
	}
	if first.Get("x-opencode-session-id") != "" {
		t.Fatal("playground must send the session as affinity, not as a child session")
	}
}

func TestNewPlaygroundRequestMarksReportingAndFreshSession(t *testing.T) {
	base := httptest.NewRequest(http.MethodPost, "/api/debug/inference", nil)
	first, err := newPlaygroundRequest(base.Context(), "/v1/chat/completions", []byte("{}"), &telemetry.RequestMeta{}, "server-key", nil)
	if err != nil {
		t.Fatalf("newPlaygroundRequest: %v", err)
	}
	second, err := newPlaygroundRequest(base.Context(), "/v1/chat/completions", []byte("{}"), &telemetry.RequestMeta{}, "server-key", nil)
	if err != nil {
		t.Fatalf("newPlaygroundRequest: %v", err)
	}

	if !gateway.WARPPoolReportingEnabled(first.Context()) {
		t.Fatal("the Playground context must enable WARP pool reporting")
	}
	if gateway.WARPPoolReportingEnabled(base.Context()) {
		t.Fatal("the admin request context must stay unchanged")
	}
	if first.Header.Get("Authorization") != "Bearer server-key" {
		t.Fatalf("Authorization = %q", first.Header.Get("Authorization"))
	}
	if first.Header.Get("Content-Type") != "application/json" || first.Header.Get("Accept") != "application/json" {
		t.Fatalf("content headers = %v", first.Header)
	}
	session := first.Header.Get("x-opencode-session")
	if session == "" || session == second.Header.Get("x-opencode-session") {
		t.Fatalf("playground session headers must be fresh, got %q and %q", session, second.Header.Get("x-opencode-session"))
	}

	selected := &gateway.DebugKeyView{Tier: config.TierZen, ID: "zen-key"}
	third, err := newPlaygroundRequest(base.Context(), "/v1/chat/completions", []byte("{}"), &telemetry.RequestMeta{}, "server-key", selected)
	if err != nil {
		t.Fatalf("newPlaygroundRequest selected: %v", err)
	}
	if !gateway.WARPPoolReportingEnabled(third.Context()) {
		t.Fatal("a selected-key Playground request must keep WARP pool reporting")
	}
}

func TestStripPlaygroundParent(t *testing.T) {
	payload := map[string]any{
		"metadata": map[string]any{"parent_session_id": "ses_parent", "keep": "yes"},
	}
	stripPlaygroundParent(payload)
	metadata := payload["metadata"].(map[string]any)
	if _, ok := metadata["parent_session_id"]; ok {
		t.Fatal("metadata.parent_session_id must be stripped")
	}
	if metadata["keep"] != "yes" {
		t.Fatalf("unrelated metadata changed: %v", metadata)
	}
}
