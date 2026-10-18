package protocol

import (
	"strings"

	"opencode2api/internal/jsonutil"
)

// Local: reasoning labels (telemetry-only display label).
// This file owns ReasoningEffortLabel; bridge.go keeps the conversion core.
// The label never affects routing or wire output.

// ReasoningEffortLabel extracts a short lowercase label for the
// client-requested reasoning intensity across the three inbound protocols.
// It mirrors the decode keys each protocol reads (Chat: reasoning_effort,
// Responses: reasoning, Anthropic: thinking) and normalizes through
// reasoningEffort, so "high", {effort:"high"} and {type:"enabled",
// budget_tokens:8192} all report "high". Empty means no reasoning setting.
func ReasoningEffortLabel(protocol Protocol, payload map[string]any) string {
	if payload == nil {
		return ""
	}
	var raw any
	switch protocol {
	case Responses:
		raw = jsonutil.FirstAny(payload["reasoning"], payload["reasoning_effort"])
	case Anthropic:
		raw = jsonutil.FirstAny(payload["thinking"], jsonutil.AnyAt(payload, "output_config", "effort"), payload["effort"])
		// An enabled/adaptive thinking block without budget_tokens is
		// malformed (upstream rejects it); labelling it by budget fallback
		// would report a phantom "high", so report unknown instead.
		if thinking, ok := raw.(map[string]any); ok {
			switch jsonutil.StringAt(thinking, "type") {
			case "enabled", "adaptive":
				if jsonutil.IntAt(thinking, "budget_tokens") <= 0 {
					return ""
				}
			}
		}
	default:
		raw = jsonutil.FirstAny(payload["reasoning_effort"], payload["reasoning"])
	}
	label, _ := reasoningEffort(raw).(string)
	label = strings.ToLower(strings.TrimSpace(label))
	label = strings.ReplaceAll(label, "-", "")
	label = strings.ReplaceAll(label, "_", "")
	label = strings.ReplaceAll(label, " ", "")
	switch label {
	case "", "none", "disabled", "off":
		return ""
	default:
		return label
	}
}
