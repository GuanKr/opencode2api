package warp

import (
	"reflect"
	"testing"
)

// The endpoint list comes from the Cloudflare registration and mixes both
// outer families. The tunnel's inner stack does NOT decide which outer family
// works (a stack=ipv6 tunnel still handshakes over IPv4 UDP), so nothing is
// dropped: the reachable family leads and the other stays behind as a
// fallback. Offline: no pool, no device, no network.

func TestOrderEndpointsByReachability(t *testing.T) {
	endpoints := []string{
		"162.159.192.1:2408",
		"[2606:4700:d0::a29f:c001]:2408",
		"[::ffff:162.159.193.1]:2408",
		"[2606:4700:d0::a29f:c002]:500",
		"162.159.193.1:2408",
	}
	v4 := []string{"162.159.192.1:2408", "[::ffff:162.159.193.1]:2408", "162.159.193.1:2408"}
	v6 := []string{"[2606:4700:d0::a29f:c001]:2408", "[2606:4700:d0::a29f:c002]:500"}

	cases := map[string]struct {
		preferIPv6  bool
		reachableV4 bool
		reachableV6 bool
		want        []string
	}{
		"only IPv4 routable": {
			reachableV4: true,
			want:        append(append([]string{}, v4...), v6...),
		},
		"only IPv6 routable": {
			reachableV6: true,
			want:        append(append([]string{}, v6...), v4...),
		},
		"both routable keep the IPv4 default": {
			reachableV4: true, reachableV6: true,
			want: append(append([]string{}, v4...), v6...),
		},
		"both routable with an IPv6 source": {
			preferIPv6: true, reachableV4: true, reachableV6: true,
			want: append(append([]string{}, v6...), v4...),
		},
		"neither routable still keeps both": {
			want: append(append([]string{}, v4...), v6...),
		},
	}
	for name, testCase := range cases {
		got := orderEndpointsByReachability(endpoints, testCase.preferIPv6, testCase.reachableV4, testCase.reachableV6)
		if !reflect.DeepEqual(got, testCase.want) {
			t.Fatalf("%s: got %v, want %v", name, got, testCase.want)
		}
	}
}

// TestOrderEndpointsByReachabilityNeverDropsAWorkingFamily pins the regression
// the stack filter introduced: stack=ipv6 used to discard every IPv4 outer
// endpoint, which is the only one an IPv4-only host can reach.
func TestOrderEndpointsByReachabilityNeverDropsAWorkingFamily(t *testing.T) {
	endpoints := []string{"162.159.192.1:2408", "[2606:4700:d0::a29f:c001]:2408"}
	for _, reachableV4 := range []bool{false, true} {
		for _, reachableV6 := range []bool{false, true} {
			got := orderEndpointsByReachability(endpoints, false, reachableV4, reachableV6)
			if len(got) != len(endpoints) {
				t.Fatalf("v4=%v v6=%v: dropped an endpoint: %v", reachableV4, reachableV6, got)
			}
		}
	}
}

func TestOrderEndpointsByReachabilityDropsUnparseableEntries(t *testing.T) {
	got := orderEndpointsByReachability([]string{"not-an-endpoint", ":2408", "162.159.192.1:2408"}, false, true, false)
	if want := []string{"162.159.192.1:2408"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got := orderEndpointsByReachability([]string{"162.159.192.1:2408"}, false, false, true); !reflect.DeepEqual(got, []string{"162.159.192.1:2408"}) {
		t.Fatalf("an IPv4-only list must survive an IPv6-first preference, got %v", got)
	}
}
