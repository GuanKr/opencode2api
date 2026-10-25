package gateway

import (
	"encoding/json"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	wire "opencode2api/internal/protocol"
)

// internal/gateway/opencode_tools.json carries the one deliberate divergence
// from upstream: the OS phrase inside the shell description.
//
// Upstream re-synced that file from a Linux client on 2026-10-18 (b0a6584) and
// merge f017ca2 reverted it. The revert left no diff trace — the merged file is
// byte-identical to the merge base — so a future "take theirs on conflict" pass
// would flip it silently. These cases are the pin; their failure text says why
// it matters. (Since the 2026-10-25 merge the shaper appends the official
// definitions itself instead of comparing them against a client's, but the
// measured string is the same one the gate let through on 2026-10-15.)
//
// Read toolshape.go's comment above //go:embed before changing anything here.

const pinnedShellDescriptionFragment = "Commands run on Windows using powershell."

// TestShellDefinitionKeepsTheMeasuredWindowsWording pins the string that was
// measured passing the free tier's gate on 2026-10-15.
func TestShellDefinitionKeepsTheMeasuredWindowsWording(t *testing.T) {
	shell, ok := officialToolDefinition("shell", false)
	if !ok {
		t.Fatal("the shell definition vanished from opencode_tools.json")
	}
	if !strings.Contains(shell.Description, pinnedShellDescriptionFragment) {
		t.Fatalf("shell description lost the measured Windows wording; got %q. "+
			"Upstream re-synced this file to Linux/bash on 2026-10-18 and f017ca2 reverted it. "+
			"Only change it back after measuring that the Linux wording still passes the gate — "+
			"and note that a gate 403 is classified as quota, which makes the failover script "+
			"switch nodes for a problem no node switch can fix.", shell.Description)
	}
}

// TestRenderedShellDescriptionCarriesTheWindowsWording: the file is not the last
// word — v2ShellDescription rewrites the OS phrase at render time — so the pin
// has to hold on the bytes that actually go out, too.
func TestRenderedShellDescriptionCarriesTheWindowsWording(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("the rewrite only applies on Windows")
	}
	entry := officialTool(wire.Chat, "shell", false)
	function, _ := entry["function"].(map[string]any)
	description, _ := function["description"].(string)
	if !strings.Contains(description, pinnedShellDescriptionFragment) {
		t.Fatalf("the rendered shell definition lost the measured Windows wording; got %q", description)
	}
}

// TestOnlyShellCarriesOSSpecificText guards the other half of the decision:
// the revert was scoped to one tool. If a later sync brings Linux wording in
// elsewhere — an edit tool mentioning a POSIX path, say — this turns red too.
func TestOnlyShellCarriesOSSpecificText(t *testing.T) {
	var definitions map[string]struct {
		Description string `json:"description"`
	}
	if err := json.Unmarshal(opencodeToolsV2JSON, &definitions); err != nil {
		t.Fatalf("opencode_tools.json is not a JSON object of tool definitions: %v", err)
	}

	// Case-insensitive, because the spellings differ: PowerShell / powershell,
	// bash / Bash, posix, darwin, unix, win32.
	osWords := []string{"windows", "linux", "bash", "powershell", "posix", "darwin", "unix", "macos"}
	// Count tools, not word hits: the shell description legitimately contains two
	// of them ("Windows" and "powershell"), and that is still one tool.
	offenders := make([]string, 0, len(definitions))
	for name, def := range definitions {
		lowered := strings.ToLower(def.Description)
		for _, word := range osWords {
			if strings.Contains(lowered, word) {
				offenders = append(offenders, name)
				break
			}
		}
	}
	sort.Strings(offenders)
	if len(offenders) != 1 || offenders[0] != "shell" {
		t.Fatalf("expected exactly one tool carrying OS-specific wording (shell), found %v. "+
			"Upstream re-synced opencode_tools.json from a Linux client; anything OS-shaped "+
			"beyond shell means another definition drifted with it.", offenders)
	}
}

// TestOfficialToolOrderMatchesTheCapturedFiles keeps the explicit orders honest.
// v2OfficialToolOrder exists only because Go maps do not preserve key order, so
// a name added to the JSON but not to the slice (or vice versa) would silently
// ship a truncated official set — exactly the failure the gate punishes.
func TestOfficialToolOrderMatchesTheCapturedFiles(t *testing.T) {
	loadOfficialTools()
	for label, pair := range map[string]struct {
		order []string
		known map[string]opencodeToolDefinition
	}{
		"v2": {v2OfficialToolOrder, opencodeToolsV2},
		"v1": {opencodeToolsV1Order, opencodeToolsV1},
	} {
		if len(pair.order) == 0 {
			t.Fatalf("%s: no tool order was captured", label)
		}
		sortedOrder := append([]string(nil), pair.order...)
		sort.Strings(sortedOrder)
		keys := make([]string, 0, len(pair.known))
		for name := range pair.known {
			keys = append(keys, name)
		}
		sort.Strings(keys)
		if !reflect.DeepEqual(sortedOrder, keys) {
			t.Fatalf("%s: the explicit order and the captured definitions disagree:\norder=%v\ndefinitions=%v", label, sortedOrder, keys)
		}
		seen := map[string]bool{}
		for _, name := range pair.order {
			if seen[name] {
				t.Fatalf("%s: %q appears twice in the order", label, name)
			}
			seen[name] = true
		}
	}
}

// TestBothOfficialSetsCoverTheirRequiredNames is a plain sanity check on the
// files the shaping rules depend on: without a definition the shaper would
// append a placeholder schema, and the gate reads schemas.
func TestBothOfficialSetsCoverTheirRequiredNames(t *testing.T) {
	for _, name := range []string{"shell", "read", "edit", "glob", "grep", "write"} {
		if _, ok := officialToolDefinition(name, false); !ok {
			t.Errorf("opencode_tools.json has no definition for 2.x tool %q", name)
		}
	}
	for _, name := range []string{"bash", "read", "edit", "glob", "grep", "write"} {
		if _, ok := officialToolDefinition(name, true); !ok {
			t.Errorf("opencode_tools_v1.json has no definition for 1.x tool %q", name)
		}
	}
	if len(officialToolNames(true)) != 11 {
		t.Errorf("the captured 1.x set has %d tools, want 11", len(officialToolNames(true)))
	}
	if len(officialToolNames(false)) != 13 {
		t.Errorf("the captured 2.x set has %d tools, want 13", len(officialToolNames(false)))
	}
}
