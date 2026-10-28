package warp

import (
	"net/netip"
	"strings"
	"testing"
)

func TestEgressScopesIPv6(t *testing.T) {
	cases := []struct {
		addr   string
		want48 string
		want64 string
	}{
		{
			addr:   "2a09:bac5:1ef0:1234:5678:9abc:def0:1111",
			want48: "2a09:bac5:1ef0::/48",
			want64: "2a09:bac5:1ef0:1234::/64",
		},
		{
			addr:   "2a09:bac5:1ef0::1",
			want48: "2a09:bac5:1ef0::/48",
			want64: "2a09:bac5:1ef0::/64",
		},
		{
			addr:   "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff",
			want48: "2001:db8:ffff::/48",
			want64: "2001:db8:ffff:ffff::/64",
		},
	}
	for _, testCase := range cases {
		got48, got64 := egressScopes(netip.MustParseAddr(testCase.addr))
		if got48 != testCase.want48 || got64 != testCase.want64 {
			t.Fatalf("egressScopes(%s) = %q, %q; want %q, %q", testCase.addr, got48, got64, testCase.want48, testCase.want64)
		}
	}
}

func TestEgressScopesIPv4DoesNotPanic(t *testing.T) {
	for _, raw := range []string{"104.28.213.171", "::ffff:104.28.213.171"} {
		got48, got64 := egressScopes(netip.MustParseAddr(raw))
		if got48 != "" || got64 != "" {
			t.Fatalf("egressScopes(%s) = %q, %q; want empty scopes for IPv4", raw, got48, got64)
		}
	}
}

func TestEgressScopesInvalidAddress(t *testing.T) {
	got48, got64 := egressScopes(netip.Addr{})
	if got48 != "" || got64 != "" {
		t.Fatalf("egressScopes(invalid) = %q, %q; want empty", got48, got64)
	}
}

func TestEgressLogFields(t *testing.T) {
	ipv6 := egressLogFields(netip.MustParseAddr("2a09:bac5:1ef0:1234::1"))
	for _, fragment := range []string{"egress=2a09:bac5:1ef0:1234::1", "egress_48=2a09:bac5:1ef0::/48", "egress_64=2a09:bac5:1ef0:1234::/64"} {
		if !strings.Contains(ipv6, fragment) {
			t.Fatalf("IPv6 log fields %q missing %q", ipv6, fragment)
		}
	}
	if ipv4 := egressLogFields(netip.MustParseAddr("104.28.213.171")); ipv4 != "egress=104.28.213.171 egress_48=n/a egress_64=n/a" {
		t.Fatalf("IPv4 log fields = %q", ipv4)
	}
}
