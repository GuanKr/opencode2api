package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Local: request fingerprint (anonymous cooling scope).
// This file owns the semantic-content hash so anonymous cooling can tell
// "same turn retry" (block) from "new turn" (one real attempt).
//
// It was dropped in the 2026-10-15 port onto the upstream baseline and is
// back as of 2026-10-16 with the fingerprint-scoped cooling that consumed it.
// Note the backup branch also had HeaderToken here (a header-value validator);
// that was NOT revived, since nothing calls it on this branch.

// RequestFingerprint identifies the semantic content of a client inference
// request. Retries replay identical bodies, so equal fingerprints mean "same
// turn retry" while different ones mean "new turn". Only stable,
// client-controlled fields are hashed; transport flags are excluded.
// "system" is included because Anthropic carries it as a top-level field
// with strong semantics (string or blocks): same messages with a different
// system prompt is a new turn and must not be blocked by the old turn's
// cooldown. Raw JSON is hashed, so the string and blocks spellings of the
// same system text fingerprint differently; that only costs one extra real
// attempt (fail-open), which is the safe direction.
// "state" and "questions" are the System One decision payload's entire body:
// without them every request for a model hashes identically and a genuinely
// different question would be blocked as a same-turn retry (fail-closed).
// They are absent on every other protocol, so hashing them changes nothing
// elsewhere.
func RequestFingerprint(payload map[string]any) string {
	subset := map[string]any{
		"model":                payload["model"],
		"input":                payload["input"],
		"messages":             payload["messages"],
		"instructions":         payload["instructions"],
		"previous_response_id": payload["previous_response_id"],
		"system":               payload["system"],
		"tools":                payload["tools"],
		"state":                payload["state"],
		"questions":            payload["questions"],
	}
	data, err := json.Marshal(subset)
	if err != nil || data == nil {
		// Unmarshalable floats (NaN/Inf) cannot reach here: malformed
		// requests are rejected with 400 before routing. Fall back to a
		// debug dump so the fingerprint stays deterministic anyway.
		data = []byte(fmt.Sprintf("%v", subset))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
