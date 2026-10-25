package config

import "testing"

// warp.proxy only exists to carry UDP: an http(s) proxy would accept the
// configuration and then fail every handshake at runtime, so the scheme is
// rejected up front, credentials are rejected because the SOCKS5 handshake is
// no-auth only, and a source prefix cannot be bound behind a relay. Whatever
// passes here must also pass the runtime's own parser (proxyDialAddress in
// internal/warp): the scheme is canonicalized to lowercase so the two layers
// cannot disagree about a mixed-case URL that url.Parse lowercases.
func TestWarpProxyValidation(t *testing.T) {
	cases := []struct {
		name       string
		proxy      string
		source     string
		wantErr    bool
		wantProxy  string
	}{
		{name: "empty keeps direct", proxy: ""},
		{name: "socks5 accepted", proxy: "socks5://127.0.0.1:7890", wantProxy: "socks5://127.0.0.1:7890"},
		{name: "socks5h accepted", proxy: "socks5h://127.0.0.1:7890", wantProxy: "socks5h://127.0.0.1:7890"},
		{name: "socks5 trimmed", proxy: "  socks5://127.0.0.1:7890  ", wantProxy: "socks5://127.0.0.1:7890"},
		{name: "uppercase scheme accepted and canonicalized", proxy: "SOCKS5://127.0.0.1:7890", wantProxy: "socks5://127.0.0.1:7890"},
		{name: "http rejected", proxy: "http://127.0.0.1:7890", wantErr: true},
		{name: "https rejected", proxy: "https://proxy.example.test:1080", wantErr: true},
		{name: "bare host colon port rejected", proxy: "127.0.0.1:7890", wantErr: true},
		{name: "garbage rejected", proxy: "not a url", wantErr: true},
		{name: "credentials rejected", proxy: "socks5://user:pass@127.0.0.1:7890", wantErr: true},
		{name: "path rejected", proxy: "socks5://127.0.0.1:7890/extra", wantErr: true},
		{name: "query rejected", proxy: "socks5://127.0.0.1:7890?x=1", wantErr: true},
		{name: "empty host rejected", proxy: "socks5://:7890", wantErr: true},
		{name: "missing port rejected", proxy: "socks5://127.0.0.1", wantErr: true},
		{name: "zero port rejected", proxy: "socks5://127.0.0.1:0", wantErr: true},
		{name: "out of range port rejected", proxy: "socks5://127.0.0.1:70000", wantErr: true},
		{name: "source and proxy rejected", proxy: "socks5://127.0.0.1:7890", source: "2606:4700:110::/64", wantErr: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := WarpConfig{Enabled: true, Stack: "ipv6", Proxy: testCase.proxy, SourceIPv6: testCase.source}
			err := cfg.Normalize()
			if testCase.wantErr && err == nil {
				t.Fatalf("expected rejection for proxy=%q source=%q", testCase.proxy, testCase.source)
			}
			if !testCase.wantErr && err != nil {
				t.Fatalf("unexpected rejection for proxy=%q: %v", testCase.proxy, err)
			}
			if testCase.wantProxy != "" && cfg.Proxy != testCase.wantProxy {
				t.Fatalf("proxy must be canonicalized to %q, got %q", testCase.wantProxy, cfg.Proxy)
			}
		})
	}
}

// The IPv4 stack cannot present an IPv6 egress, so a source prefix stays
// rejected there; the proxy itself is stack-independent.
func TestWarpProxyIsStackIndependent(t *testing.T) {
	cfg := WarpConfig{Enabled: true, Stack: "ipv4", Proxy: "socks5://127.0.0.1:7890"}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("a proxy must be valid regardless of the tunnel stack: %v", err)
	}
	if cfg.Proxy != "socks5://127.0.0.1:7890" {
		t.Fatalf("proxy must survive normalization verbatim, got %q", cfg.Proxy)
	}
}
