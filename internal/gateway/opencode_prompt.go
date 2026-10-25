package gateway

import (
	_ "embed"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	wire "opencode2api/internal/protocol"
)

// The free tier accepts only agent-shaped requests, and its abuse checks expect
// the system prompt the official CLI sends. These assets are the CLI's own
// wording: the v1 text for /zen (OpenCode 1.x tool names) and the v2 text for
// /inference and /zen/go (OpenCode 2.x tool names). The v2 file is the v1 file
// with the tool names the 2.x CLI uses.
//
//go:embed opencode_prompt_v1.txt
var opencodePromptV1 string

//go:embed opencode_prompt_v2.txt
var opencodePromptV2 string

// opencodePromptMarker identifies a body that already carries the official CLI
// system prompt, so a client that sends it itself is left alone.
const opencodePromptMarker = "You are opencode, an interactive CLI tool"

// opencodeSkills is the built-in skill listing the CLI appends after its
// environment block. It is static, so it is part of the injected prompt.
const opencodeSkills = `Skills provide specialized instructions and workflows for specific tasks.
Use the skill tool to load a skill when a task matches its description.
<available_skills>
  <skill>
    <name>customize-opencode</name>
    <description>Use ONLY when the user is editing or creating opencode's own configuration: opencode.json, opencode.jsonc, files under .opencode/, or files under ~/.config/opencode/. Also use when creating or fixing opencode agents, subagents, skills, plugins, MCP servers, or permission rules. Do not use for the user's own application code, or for any project that is not configuring opencode itself.</description>
    <location>&lt;built-in&gt;</location>
  </skill>
</available_skills>`

var (
	opencodeEnvOnce sync.Once
	opencodeWorkDir string
)

// opencodeEnvironment renders the <env> block the CLI puts after its base
// prompt. The working directory is resolved once per process; a platform
// without one falls back to the official container path.
func opencodeEnvironment(model string) string {
	opencodeEnvOnce.Do(func() {
		directory := ""
		if current, err := os.Getwd(); err == nil {
			directory = filepath.Clean(strings.TrimSpace(current))
		}
		if directory == "" || directory == "." {
			directory = "/workspace"
		}
		opencodeWorkDir = directory
	})
	apiModel := strings.TrimSpace(strings.TrimPrefix(model, "opencode/"))
	if apiModel == "" {
		apiModel = "unknown"
	}
	platform := runtime.GOOS
	if platform == "windows" {
		platform = "win32"
	}
	return strings.Join([]string{
		"You are powered by the model named " + apiModel + ". The exact model ID is opencode/" + apiModel,
		"Here is some useful information about the environment you are running in:",
		"<env>",
		"  Working directory: " + opencodeWorkDir,
		"  Workspace root folder: " + opencodeWorkDir,
		"  Is directory a git repo: yes",
		"  Platform: " + platform,
		"  Today's date: " + time.Now().UTC().Format("Mon Jan 02 2006"),
		"</env>",
	}, "\n")
}

// opencodeSystemPrompt assembles the full CLI system prompt for model and the
// upstream generation's tool naming.
func opencodeSystemPrompt(model string, v1 bool) string {
	base := opencodePromptV2
	if v1 {
		base = opencodePromptV1
	}
	return base + opencodeEnvironment(model) + "\n" + opencodeSkills
}

// injectOfficialPrompt prepends the official CLI system prompt to a shaped
// free-tier payload. It reports whether the payload changed; a body that
// already carries the marker is left untouched, so repeated shaping cannot
// duplicate the prompt. System One payloads are not chat bodies and are never
// touched.
func injectOfficialPrompt(payload map[string]any, protocol wire.Protocol, v1 bool) bool {
	if payload == nil || protocol == wire.SystemOne {
		return false
	}
	model, _ := payload["model"].(string)
	prompt := opencodeSystemPrompt(model, v1)
	switch protocol {
	case wire.Chat:
		return injectChatPrompt(payload, prompt)
	case wire.Anthropic:
		return injectAnthropicPrompt(payload, prompt)
	case wire.Responses:
		return injectResponsesPrompt(payload, prompt)
	default:
		return false
	}
}

// injectChatPrompt folds every system/developer message into the single system
// message the 1.x and 2.x CLIs send, then prepends it. Client instructions keep
// their order and are separated from the official prompt by a blank line.
func injectChatPrompt(payload map[string]any, prompt string) bool {
	messages, ok := payload["messages"].([]any)
	if !ok {
		return false
	}
	kept := make([]any, 0, len(messages)+1)
	var instructions []string
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			kept = append(kept, raw)
			continue
		}
		role, _ := message["role"].(string)
		if role != "system" && role != "developer" {
			kept = append(kept, raw)
			continue
		}
		content := message["content"]
		if carriesOpenCodePrompt(content) {
			return false
		}
		for _, part := range textFragments(content) {
			if part = strings.TrimSpace(part); part != "" {
				instructions = append(instructions, part)
			}
		}
	}
	system := prompt
	if joined := strings.Join(instructions, "\n"); joined != "" {
		system += "\n\n" + joined
	}
	payload["messages"] = append([]any{map[string]any{"role": "system", "content": system}}, kept...)
	return true
}

// injectAnthropicPrompt prepends the prompt to the top-level system value,
// keeping a caller's string or block list intact after it.
func injectAnthropicPrompt(payload map[string]any, prompt string) bool {
	system, exists := payload["system"]
	if exists && carriesOpenCodePrompt(system) {
		return false
	}
	if !exists {
		payload["system"] = prompt
		return true
	}
	switch value := system.(type) {
	case string:
		if strings.TrimSpace(value) == "" {
			payload["system"] = prompt
		} else {
			payload["system"] = prompt + "\n\n" + value
		}
	case []any:
		payload["system"] = append([]any{map[string]any{"type": "text", "text": prompt}}, value...)
	default:
		payload["system"] = prompt
	}
	return true
}

// injectResponsesPrompt prepends the prompt to the Responses instructions.
func injectResponsesPrompt(payload map[string]any, prompt string) bool {
	instructions, exists := payload["instructions"]
	if exists && carriesOpenCodePrompt(instructions) {
		return false
	}
	if text, ok := instructions.(string); exists && ok {
		if strings.TrimSpace(text) == "" {
			payload["instructions"] = prompt
		} else {
			payload["instructions"] = prompt + "\n\n" + text
		}
		return true
	}
	payload["instructions"] = prompt
	return true
}

// gatewayToolNote goes into the system slot of a shaped free-tier body, after
// the official prompt and after the client's own instructions. The shaper
// renames the client's own tools to clint_<name> and then appends the lane's
// full official OpenCode set, so the model sees both generations at once while
// only the clint_ tools exist in the caller's executor. A call to an appended
// official name is passed through unmapped (see protocol.renameToolInMap), so
// the client receives a tool call it never declared and cannot run.
//
// Measured 2026-10-15 against /inference: a system-slot note turned ghost
// tool_calls on a neutral prompt from 1/1 requests into 0/1, while changing the
// system slot cost no 403 because the gate compares tool definitions, not
// system text. A prompt that explicitly asks for a tool still gets one — that is
// the user asking, not a ghost call.
//
// Cost, stated plainly: upstream injected the official prompt because the gate
// is said to check the request against it (see 97cdb29), and this note is text
// no real CLI sends. It sits after the official prompt, the same shape upstream
// already produces when it appends the client's own instructions, and the
// 2026-10-15 measurement showed system-slot text alone did not cost a 403. That
// measurement predates the official prompt, so the gate's reaction to this body
// stays unverified — the same caveat AGENTS.md carries for the whole shaper.
//
// The wording names no tool and, deliberately, does not claim that every
// non-clint_ entry is a gateway placeholder. Responses and Anthropic clients can
// declare server-side tools (web_search and friends); those carry no client name
// to rewrite, so the shaper passes them through untouched and the upstream runs
// them. Condemning "everything else" would switch a working capability off, so
// the prohibition is scoped to the official OpenCode set the gateway itself
// appended. It still names none of those tools: the set differs by lane (11
// names on v1, 13 on v2), and enumerating either would make the note change
// whenever the lane or the captured set does.
const gatewayToolNote = "[gateway] The tools whose names begin with `clint_` are the client's own tools: " +
	"call them with exactly the parameters they declare. The gateway also appends the official OpenCode " +
	"tool set so this request looks like a CLI session, and this client cannot execute those tools: do not " +
	"call an official OpenCode tool here. If none of the `clint_` tools fits, answer directly instead."

// appendGatewayToolNote appends gatewayToolNote to the payload's system slot,
// after the official prompt and after the client's own instructions, so the
// suppression is the last instruction the model reads. It reports whether the
// payload changed; System One payloads are decision requests and are never
// touched.
func appendGatewayToolNote(payload map[string]any, protocol wire.Protocol) bool {
	if payload == nil || protocol == wire.SystemOne {
		return false
	}
	switch protocol {
	case wire.Chat:
		return appendChatToolNote(payload)
	case wire.Anthropic:
		return appendAnthropicToolNote(payload)
	case wire.Responses:
		return appendResponsesToolNote(payload)
	default:
		return false
	}
}

// appendChatToolNote writes the note into the first system/developer message it
// can extend, creating a leading system message when the body carries none. An
// exotic content shape is skipped rather than overwritten: the note is advisory
// and must never cost client data.
//
// In the current call order injectOfficialPrompt has already folded every
// system/developer message into a leading one, so the "none" branch is
// defensive only — it keeps the note from disappearing if the two steps are ever
// reordered. The end-to-end test for a body without a system slot therefore
// exercises that fold, not this branch.
func appendChatToolNote(payload map[string]any) bool {
	messages, ok := payload["messages"].([]any)
	if !ok {
		return false
	}
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		role, _ := message["role"].(string)
		if role != "system" && role != "developer" {
			continue
		}
		if carriesGatewayToolNote(message["content"]) {
			// Shaping ran before on this body (the caller is supposed to shape
			// once, but a re-used payload must not stack a second note).
			return false
		}
		switch content := message["content"].(type) {
		case string:
			if strings.TrimSpace(content) == "" {
				message["content"] = gatewayToolNote
			} else {
				message["content"] = content + "\n\n" + gatewayToolNote
			}
			return true
		case []any:
			message["content"] = append(content, map[string]any{"type": "text", "text": gatewayToolNote})
			return true
		}
	}
	payload["messages"] = append([]any{map[string]any{"role": "system", "content": gatewayToolNote}}, messages...)
	return true
}

// appendAnthropicToolNote appends the note after the top-level system value,
// keeping a caller's string or block list intact ahead of it.
func appendAnthropicToolNote(payload map[string]any) bool {
	if carriesGatewayToolNote(payload["system"]) {
		return false
	}
	switch system := payload["system"].(type) {
	case nil:
		payload["system"] = gatewayToolNote
	case string:
		if strings.TrimSpace(system) == "" {
			payload["system"] = gatewayToolNote
		} else {
			payload["system"] = system + "\n\n" + gatewayToolNote
		}
	case []any:
		payload["system"] = append(system, map[string]any{"type": "text", "text": gatewayToolNote})
	default:
		return false
	}
	return true
}

// appendResponsesToolNote appends the note after the Responses instructions.
func appendResponsesToolNote(payload map[string]any) bool {
	if carriesGatewayToolNote(payload["instructions"]) {
		return false
	}
	switch instructions := payload["instructions"].(type) {
	case nil:
		payload["instructions"] = gatewayToolNote
	case string:
		if strings.TrimSpace(instructions) == "" {
			payload["instructions"] = gatewayToolNote
		} else {
			payload["instructions"] = instructions + "\n\n" + gatewayToolNote
		}
	default:
		return false
	}
	return true
}

// carriesGatewayToolNote reports whether a system slot already carries the note.
func carriesGatewayToolNote(value any) bool {
	for _, part := range textFragments(value) {
		if strings.Contains(part, gatewayToolNote) {
			return true
		}
	}
	return false
}

// textFragments flattens a system or instruction value into its text: a plain
// string, a list of content blocks, or a block object carrying text or content.
func textFragments(value any) []string {
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return []string{typed}
	case []any:
		var parts []string
		for _, item := range typed {
			parts = append(parts, textFragments(item)...)
		}
		return parts
	case map[string]any:
		if text, ok := typed["text"].(string); ok {
			return textFragments(text)
		}
		if content, ok := typed["content"]; ok {
			return textFragments(content)
		}
	}
	return nil
}

// carriesOpenCodePrompt reports whether any text inside value already contains
// the marker.
func carriesOpenCodePrompt(value any) bool {
	for _, part := range textFragments(value) {
		if strings.Contains(part, opencodePromptMarker) {
			return true
		}
	}
	return false
}
