package warp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

// traceInfo dials the trace URL through the exit's tunnel and returns the
// ip=/loc=/colo= fields, so gated probes can record the public egress and its
// geography without shipping test helpers into production code.
func traceInfo(ctx context.Context, e *exit, target probeTarget) (ip, loc, colo string, err error) {
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	transport := &http.Transport{
		Proxy:               nil,
		ForceAttemptHTTP2:   false,
		DisableKeepAlives:   true,
		TLSHandshakeTimeout: 10 * time.Second,
		DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			return e.network.DialContext(ctx, target.network, address)
		},
	}
	defer transport.CloseIdleConnections()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, target.url, nil)
	if err != nil {
		return "", "", "", err
	}
	resp, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		return "", "", "", err
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(io.LimitReader(resp.Body, 64<<10))
	for scanner.Scan() {
		line := scanner.Text()
		for _, field := range []struct {
			key string
			out *string
		}{{"ip", &ip}, {"loc", &loc}, {"colo", &colo}} {
			if strings.HasPrefix(line, field.key+"=") {
				*field.out = strings.TrimPrefix(line, field.key+"=")
			}
		}
	}
	return ip, loc, colo, nil
}

// buildExitWithBind replicates newExitWithStack but injects a custom outer
// bind, so the gated probes can measure the same tunnel over simpleBind
// (direct) or the production socks5Bind (via Clash) without extra call sites.
func buildExitWithBind(ctx context.Context, reg registration, bind conn.Bind) (*exit, error) {
	localV6, err := parseAddress(reg.AddressV6)
	if err != nil || !localV6.Is6() {
		return nil, fmt.Errorf("invalid WARP IPv6 address %q", reg.AddressV6)
	}
	tunDevice, network, err := netstack.CreateNetTUN([]netip.Addr{localV6}, []netip.Addr{dnsV6}, 1280)
	if err != nil {
		return nil, err
	}
	privateKey, err := uapiKey(reg.PrivateKey)
	if err != nil {
		tunDevice.Close()
		return nil, err
	}
	peerKey, err := uapiKey(reg.PeerPublicKey)
	if err != nil {
		tunDevice.Close()
		return nil, err
	}
	endpoints, err := resolveEndpoints(ctx, reg.Endpoints, reg.Endpoint)
	if err != nil {
		tunDevice.Close()
		return nil, err
	}
	// The local family probe is meaningless behind a proxy; keep every
	// endpoint, IPv4 first as a plain policy.
	endpoints = orderEndpointsByReachability(endpoints, false, true, false)
	if len(endpoints) == 0 {
		tunDevice.Close()
		return nil, fmt.Errorf("no endpoint for device %s", reg.ID)
	}
	wg := device.NewDevice(tunDevice, bind, device.NewLogger(device.LogLevelError, "warpprobe: "))
	uapi := fmt.Sprintf(
		"private_key=%s\npublic_key=%s\nallowed_ip=0.0.0.0/0\nallowed_ip=::/0\nendpoint=%s\npersistent_keepalive_interval=25\n",
		privateKey, peerKey, endpoints[0])
	if err := wg.IpcSet(uapi); err != nil {
		wg.Close()
		<-wg.Wait()
		return nil, err
	}
	if err := wg.Up(); err != nil {
		wg.Close()
		<-wg.Wait()
		return nil, err
	}
	e := &exit{id: reg.ID, stack: StackIPv6, device: wg, network: network}
	egress, err := e.probe(ctx, wg, peerKey, endpoints)
	if err != nil {
		e.close()
		return nil, err
	}
	e.egress = egress
	return e, nil
}

// TestOuterTransportStability measures handshake success and latency for
// WARP tunnels built over the direct outer path and, when
// WARP_SOCKS_PROXY=host:port is set, over a SOCKS5 UDP relay (Clash mixed
// port). Each sample registers a fresh device and reports the public egress
// plus its loc=/colo= geography. Real network: gated behind WARP_SOCKS_PROBE=1.
func TestOuterTransportStability(t *testing.T) {
	if os.Getenv("WARP_SOCKS_PROBE") != "1" {
		t.Skip("real-network probe disabled; set WARP_SOCKS_PROBE=1 to run")
	}
	proxyAddr := os.Getenv("WARP_SOCKS_PROXY")
	perSeries := 3
	if raw := os.Getenv("WARP_SOCKS_PROBE_COUNT"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 1 && parsed <= 6 {
			perSeries = parsed
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	r := newRegistrar("")

	type result struct {
		via     string
		ok      bool
		elapsed time.Duration
		egress  string
		geo     string
		err     string
	}
	run := func(via string, bind func() conn.Bind) []result {
		var out []result
		for i := 0; i < perSeries; i++ {
			reg, err := r.register(ctx)
			if err != nil {
				t.Fatalf("%s registration %d: %v", via, i+1, err)
			}
			start := time.Now()
			exit, err := buildExitWithBind(ctx, reg, bind())
			res := result{via: via, elapsed: time.Since(start).Round(time.Millisecond)}
			if err != nil {
				res.err = err.Error()
			} else {
				res.ok = true
				res.egress = exit.egress.String()
				if ip, loc, colo, terr := traceInfo(ctx, exit, probeTargets(StackIPv6)[0]); terr == nil {
					res.geo = fmt.Sprintf("loc=%s colo=%s (ip=%s)", loc, colo, ip)
				}
				exit.close()
			}
			out = append(out, res)
			t.Logf("%s sample %d/%d: ok=%v elapsed=%s egress=%s %s err=%v",
				via, i+1, perSeries, res.ok, res.elapsed, res.egress, res.geo, res.err)
		}
		return out
	}

	direct := run("direct", func() conn.Bind { return &simpleBind{} })
	var relayed []result
	if proxyAddr != "" {
		relayed = run("socks5", func() conn.Bind { return &socks5Bind{proxy: proxyAddr} })
	}
	summarize := func(label string, rs []result) {
		okCount := 0
		var total time.Duration
		for _, res := range rs {
			if res.ok {
				okCount++
				total += res.elapsed
			}
		}
		avg := time.Duration(0)
		if okCount > 0 {
			avg = (total / time.Duration(okCount)).Round(time.Millisecond)
		}
		t.Logf("RESULT %s: %d/%d handshakes ok, avg success elapsed=%s", label, okCount, len(rs), avg)
	}
	summarize("direct", direct)
	if proxyAddr != "" {
		summarize("socks5", relayed)
	}
}
