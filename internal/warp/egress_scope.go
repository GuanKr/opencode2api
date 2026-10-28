package warp

import (
	"fmt"
	"net/netip"
)

// egressScopes returns the IPv6 /48 and /64 network scopes an egress belongs
// to. The upstream free-tier quota is scoped to the /64 (see egressKey), and
// production evidence suggests a /48 can share reputation, so both are logged
// for later analysis. IPv4 has no wider scope to report: its quota is the single
// address, so both strings are empty and callers print n/a.
func egressScopes(addr netip.Addr) (egress48, egress64 string) {
	if !addr.IsValid() {
		return "", ""
	}
	addr = addr.Unmap()
	if !addr.Is6() {
		return "", ""
	}
	return netip.PrefixFrom(addr, 48).Masked().String(), netip.PrefixFrom(addr, 64).Masked().String()
}

// egressLogFields renders the text key=value suffix carried by WARP lifecycle
// logs. It never includes a key, a token, or any other secret.
func egressLogFields(addr netip.Addr) string {
	egress48, egress64 := egressScopes(addr)
	if egress48 == "" {
		egress48 = "n/a"
	}
	if egress64 == "" {
		egress64 = "n/a"
	}
	return fmt.Sprintf("egress=%s egress_48=%s egress_64=%s", addr, egress48, egress64)
}
