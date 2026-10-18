package gateway

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

// internal/gateway/opencode_tools.json carries the one deliberate divergence
// from upstream: the OS phrase inside the shell description.
//
// Upstream re-synced that file from a Linux client on 2026-10-18 (b0a6584) and
// merge f017ca2 reverted it. The revert left no diff trace — the merged file is
// byte-identical to the merge base — so a future "take theirs on conflict" pass
// would flip it silently, and no existing test would notice: the tool tests
// compare output against knownOpencodeTool, i.e. against this same file. These
// cases are the pin. They fail on the next silent flip, and their failure text
// says why it matters.
//
// Read upstream.go's comment above //go:embed before changing anything here.

const pinnedShellDescriptionFragment = "Commands run on Windows using powershell."

// TestShellDefinitionKeepsTheMeasuredWindowsWording pins the string that was
// measured passing the free tier's gate on 2026-10-15.
func TestShellDefinitionKeepsTheMeasuredWindowsWording(t *testing.T) {
	shell, ok := knownOpencodeTool("shell")
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

// TestOnlyShellCarriesOSSpecificText guards the other half of the decision:
// the revert was scoped to one tool. If a later sync brings Linux wording in
// elsewhere — an edit tool mentioning a POSIX path, say — this turns red too.
func TestOnlyShellCarriesOSSpecificText(t *testing.T) {
	var definitions map[string]struct {
		Description string `json:"description"`
	}
	if err := json.Unmarshal(opencodeToolsJSON, &definitions); err != nil {
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

// TestToolDefinitionsStillCoverTheCoreSet is a plain sanity check on the file the
// shaping rules depend on: the core names must all be present, or ensureAnonymousTools
// appends nothing and the gate has no genuine definition to judge.
func TestToolDefinitionsStillCoverTheCoreSet(t *testing.T) {
	for _, name := range anonymousCoreTools {
		if _, ok := knownOpencodeTool(name); !ok {
			t.Errorf("opencode_tools.json has no definition for core tool %q; "+
				"the gate would receive a stub and reject the request", name)
		}
	}
	for _, name := range anonymousGateSaturators {
		if _, ok := knownOpencodeTool(name); !ok {
			t.Errorf("opencode_tools.json has no definition for saturator %q; the saturator "+
				"branch would append an empty schema", name)
		}
	}
}
