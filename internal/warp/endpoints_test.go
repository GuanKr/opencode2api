package warp

import (
	"reflect"
	"testing"
)

// The endpoint list comes from the Cloudflare registration and mixes both
// families. A tunnel of one family cannot handshake with a peer endpoint of the
// other, and each unreachable candidate costs a full handshake timeout, so the
// stack must decide which entries are worth dialling. Offline: no pool, no
// device, no network.

func TestFilterEndpointsByStack(t *testing.T) {
	endpoints := []string{
		"162.159.192.1:2408",
		"[2606:4700:d0::a29f:c001]:2408",
		"[::ffff:162.159.193.1]:2408",
		"[2606:4700:d0::a29f:c002]:500",
		"162.159.193.1:2408",
	}
	cases := map[string]struct {
		stack Stack
		want  []string
	}{
		"ipv4": {
			StackIPv4,
			[]string{"162.159.192.1:2408", "[::ffff:162.159.193.1]:2408", "162.159.193.1:2408"},
		},
		"ipv6": {
			StackIPv6,
			[]string{"[2606:4700:d0::a29f:c001]:2408", "[2606:4700:d0::a29f:c002]:500"},
		},
		"dual keeps everything in order": {
			StackDual,
			endpoints,
		},
	}
	for name, testCase := range cases {
		got := filterEndpointsByStack(endpoints, testCase.stack)
		if !reflect.DeepEqual(got, testCase.want) {
			t.Fatalf("%s: got %v, want %v", name, got, testCase.want)
		}
	}
}

// TestFilterEndpointsByStackDropsUnparseableEntries: a malformed entry must not
// survive as an endpoint the device would try to dial.
func TestFilterEndpointsByStackDropsUnparseableEntries(t *testing.T) {
	got := filterEndpointsByStack([]string{"not-an-endpoint", ":2408", "162.159.192.1:2408"}, StackIPv4)
	if want := []string{"162.159.192.1:2408"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got := filterEndpointsByStack([]string{"162.159.192.1:2408"}, StackIPv6); len(got) != 0 {
		t.Fatalf("an IPv6 tunnel must have no endpoint left, got %v", got)
	}
}
