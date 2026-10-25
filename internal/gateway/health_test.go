package gateway

import "testing"

// TestProxyBlocksReadiness pins the one condition shared by readiness and the
// no_healthy_proxies issue. WARP carries every free-model lane, so a pool with
// no healthy node must not mark the gateway degraded while the same document
// says ready=true.
func TestProxyBlocksReadiness(t *testing.T) {
	cases := map[string]struct {
		warpEnabled bool
		goKeys      int
		healthy     int
		want        bool
	}{
		"warp off, no healthy proxy":        {false, 0, 0, true},
		"warp off, healthy proxy":           {false, 0, 1, false},
		"warp on, no go key, no proxy":      {true, 0, 0, false},
		"warp on, go key, no proxy":         {true, 1, 0, true},
		"warp on, no go key, healthy proxy": {true, 0, 1, false},
	}
	for name, testCase := range cases {
		if got := proxyBlocksReadiness(testCase.warpEnabled, testCase.goKeys, testCase.healthy); got != testCase.want {
			t.Fatalf("%s: proxyBlocksReadiness = %v, want %v", name, got, testCase.want)
		}
	}
}
