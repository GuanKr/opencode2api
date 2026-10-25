package warp

import (
	"context"
	"net/netip"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestSameSourceEgressProbes answers one question with real traffic: do two
// fresh WARP registrations made from the SAME outer source (this host's single
// IPv4 address, no proxy) receive the same public IPv6 egress, or one each?
//
// Gated behind WARP_EGRESS_PROBE=1 because it dials Cloudflare for real
// (registration API + UDP handshake + trace through the tunnel); the default
// test run must stay offline. WARP_EGRESS_PROBE_COUNT overrides the sample
// count (default 2).
func TestSameSourceEgressProbes(t *testing.T) {
	if os.Getenv("WARP_EGRESS_PROBE") != "1" {
		t.Skip("real-network probe disabled; set WARP_EGRESS_PROBE=1 to run")
	}
	samples := 2
	if raw := os.Getenv("WARP_EGRESS_PROBE_COUNT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 2 || parsed > 6 {
			t.Fatalf("WARP_EGRESS_PROBE_COUNT must be 2..6, got %q", raw)
		}
		samples = parsed
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	r := newRegistrar("")
	type sample struct {
		device  string
		inner   string
		egress  string
		elapsed time.Duration
	}
	var samplesOut []sample
	for i := 0; i < samples; i++ {
		start := time.Now()
		var exit *exit
		var reg registration
		for attempt := 0; attempt < 3; attempt++ {
			candidateReg, err := r.register(ctx)
			if err != nil {
				t.Fatalf("registration %d/%d: %v", i+1, samples, err)
			}
			candidate, err := newExitWithStack(ctx, candidateReg, StackIPv6, netip.Addr{}, "")
			if err == nil {
				reg, exit = candidateReg, candidate
				break
			}
			t.Logf("exit %d/%d attempt %d (device %s) failed: %v", i+1, samples, attempt+1, candidateReg.ID, err)
		}
		if exit == nil {
			t.Fatalf("exit %d/%d: all attempts failed (outer handshake flaky?)", i+1, samples)
		}
		// newExitWithStack already traced once (that is exit.egress); trace two
		// more times through the SAME tunnel to tell per-exit stability from
		// per-connection randomness.
		retraces := []string{exit.egress.String()}
		for extra := 0; extra < 2; extra++ {
			again, err := exit.probeTarget(ctx, probeTargets(StackIPv6)[0])
			if err != nil {
				t.Fatalf("exit %d/%d retrace %d: %v", i+1, samples, extra+1, err)
			}
			retraces = append(retraces, again.String())
		}
		samplesOut = append(samplesOut, sample{
			device:  reg.ID,
			inner:   reg.AddressV6,
			egress:  exit.egress.String(),
			elapsed: time.Since(start).Round(time.Millisecond),
		})
		t.Logf("exit %d/%d: device=%s inner_v6=%s public_egress=%s retraces=%v handshake+probe=%s",
			i+1, samples, reg.ID, reg.AddressV6, exit.egress, retraces, time.Since(start).Round(time.Millisecond))
		exit.close()
	}

	innerDiffer := samplesOut[0].inner != samplesOut[1].inner
	allEgressSame := true
	for _, s := range samplesOut[1:] {
		if s.egress != samplesOut[0].egress {
			allEgressSame = false
		}
	}
	t.Logf("RESULT inner tunnel addresses differ: %v", innerDiffer)
	t.Logf("RESULT public egress identical across %d registrations from one outer source: %v", len(samplesOut), allEgressSame)
	for i, s := range samplesOut {
		t.Logf("RESULT sample %d: egress=%s", i+1, s.egress)
	}
}
