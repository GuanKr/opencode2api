package config

import "strings"

// placeholderKeys are the example values the shipped documentation uses. They
// are not secrets and no upstream accepts them: a configuration that still
// carries one has simply not been filled in yet.
//
// "change-this-local-key" only ever appears under server_keys, which this file
// does not filter (a local key is not used against an upstream); it is listed
// so IsPlaceholderKey answers correctly wherever it is asked.
var placeholderKeys = []string{
	"sk-your-zen-key",
	"change-this-local-key",
}

// IsPlaceholderKey reports whether key is a documented placeholder rather than
// a real credential. Comparison is case-insensitive and ignores surrounding
// whitespace.
func IsPlaceholderKey(key string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(key))
	for _, placeholder := range placeholderKeys {
		if trimmed == placeholder {
			return true
		}
	}
	return false
}

// DropPlaceholderKeys removes placeholder credentials from the key pools. A
// placeholder can never authenticate, so keeping it only turns every fallback
// that reaches the keyed tiers into a 401 — which then hides the real error
// (the cause of the failover) from the client.
//
// Normalize runs this before the "at least one upstream key" check and before
// anything is persisted, so the running configuration, the console view and the
// file a save writes back all agree: a dropped key is gone, not merely ignored.
// That is the intended reading — an operator who left the example in place has
// no usable key, and the startup error says so explicitly.
func DropPlaceholderKeys(cfg Config) Config {
	drop := func(keys []string) []string {
		if len(keys) == 0 {
			return keys
		}
		kept := make([]string, 0, len(keys))
		for _, key := range keys {
			if IsPlaceholderKey(key) {
				continue
			}
			kept = append(kept, key)
		}
		return kept
	}
	cfg.ZenKeys = drop(cfg.ZenKeys)
	cfg.GoKeys = drop(cfg.GoKeys)
	return cfg
}
