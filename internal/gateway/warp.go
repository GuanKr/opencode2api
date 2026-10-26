package gateway

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/telemetry"
	"opencode2api/internal/warp"
)

// errWARPUnavailable marks a free-lane attempt that could not acquire a WARP
// exit at all. The client gets a 503 that points at the egress switch instead
// of a generic upstream failure.
var errWARPUnavailable = errors.New("WARP egress is unavailable")

// warpUnavailableMessage is what a client receives when the free lane is
// configured to leave through WARP but no exit could be acquired. It is a 503
// because the request never reached the upstream. The opencode2api: prefix is
// the same self-authored marker every other gateway-synthesised error carries:
// watchdog-clash.ps1 distinguishes these bodies from upstream pass-throughs by
// it, and an upstream echo of user input can otherwise impersonate the gateway.
const warpUnavailableMessage = "opencode2api: no WARP exit could carry this free-model request; the upstream was not contacted. Check the WARP switch and this host's UDP access to the Cloudflare endpoint."

// usesWARP reports whether this request's egress must bypass proxies. Only
// free models on the anonymous lane and the Zen key tier qualify; the Go plan
// and paid models keep their proxy bindings.
func (g *Gateway) usesWARP(model string, tier config.Tier, anonymous bool) bool {
	return g.warp != nil && g.catalog.IsFreeModel(model) && (anonymous || tier == config.TierZen)
}

// warpUsesBun mirrors inferenceClient for traffic that leaves through WARP:
// free-model traffic to the v1 /zen lane or the v2 /inference origin must look
// like the official Bun runtime. The other lanes use the regular client.
func (g *Gateway) warpUsesBun(baseURL, model string) bool {
	lane := strings.ToLower(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	return g.catalog.IsFreeModel(model) && (models.IsV1Upstream(baseURL) || strings.HasSuffix(lane, "/inference"))
}

// warpFailureSink retires an exit that cannot carry traffic. *warp.Pool is the
// production implementation; tests substitute a recorder.
type warpFailureSink interface {
	Fail(lease *warp.Lease, reason string)
}

// warpLease is one acquired WARP exit together with the HTTP clients that dial
// through it. Every client is per-attempt: the exit is released as soon as the
// response body is closed, so pooled connections must be closed at the same
// time.
type warpLease struct {
	lease   *warp.Lease
	pool    warpFailureSink
	regular *http.Client
	bun     *http.Client
	useBun  bool
	once    sync.Once
}

func (g *Gateway) newWarpLease(lease *warp.Lease, useBun bool) (*warpLease, error) {
	perf := g.cfg.Performance
	dial := func(ctx context.Context, _, address string) (net.Conn, error) {
		return lease.DialContext(ctx, lease.DialNetwork(), address)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = dial
	transport.ForceAttemptHTTP2 = false
	// A clone of DefaultTransport made after its first dial inherits the
	// configured HTTP/2 ALPN and TLSNextProto map, so the transport negotiates
	// h2 and then hands SETTINGS frames to the HTTP/1.x reader ("malformed HTTP
	// response"). Every WARP attempt must speak HTTP/1.1 over a fresh tunnel,
	// so pin the ALPN and disable the h2 wiring explicitly.
	transport.TLSNextProto = make(map[string]func(authority string, c *tls.Conn) http.RoundTripper)
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{NextProtos: []string{"http/1.1"}}
	} else {
		tlsCfg := transport.TLSClientConfig.Clone()
		tlsCfg.NextProtos = []string{"http/1.1"}
		transport.TLSClientConfig = tlsCfg
	}
	transport.MaxIdleConns = perf.MaxIdleConns
	transport.MaxIdleConnsPerHost = perf.MaxIdleConnsPerHost
	transport.MaxConnsPerHost = perf.MaxConnsPerHost
	transport.IdleConnTimeout = time.Duration(perf.IdleConnTimeoutSeconds) * time.Second
	transport.ResponseHeaderTimeout = g.attemptTimeout
	bun, err := httpx.NewBunTransport(httpx.BunOptions{
		DialContext:           dial,
		ConnectTimeout:        time.Duration(perf.ConnectTimeoutSeconds) * time.Second,
		ResponseHeaderTimeout: g.attemptTimeout,
		IdleConnTimeout:       transport.IdleConnTimeout,
		MaxIdleConnsPerHost:   perf.MaxIdleConnsPerHost,
	})
	if err != nil {
		return nil, err
	}
	return &warpLease{
		lease:   lease,
		pool:    g.warp,
		regular: &http.Client{Transport: transport},
		bun:     &http.Client{Transport: bun},
		useBun:  useBun,
	}, nil
}

func (l *warpLease) client() *http.Client {
	if l.useBun {
		return l.bun
	}
	return l.regular
}

func (l *warpLease) close() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.bun.CloseIdleConnections()
		l.regular.CloseIdleConnections()
		l.lease.Release()
	})
}

// warpBody ties the lease to the response body. Closing the body (or the
// gateway's drain helpers) releases the exit, and an upstream stream that dies
// mid-body retires the exit that carried it.
type warpBody struct {
	io.ReadCloser
	lease       *warpLease
	diagnostic  bool
	cancel      context.CancelFunc
	idleTimeout atomic.Bool
}

// markIdleTimeout tells this body that the idle wrapper closed it. The wrapper
// owns that classification, so the read error caused by the close must not be
// reported as a separate stream failure.
func (b *warpBody) markIdleTimeout() {
	if b != nil {
		b.idleTimeout.Store(true)
	}
}

func (b *warpBody) Read(data []byte) (int, error) {
	n, err := b.ReadCloser.Read(data)
	if err != nil {
		// EOF is a clean end for a complete non-streaming body, so it stays
		// EOF even after a rotation. The stream layers classify an aborted
		// stream through Aborted instead of through the read error.
		if err != io.EOF && b.Aborted() {
			return n, wire.ErrStreamAborted
		}
		if err != io.EOF && !b.diagnostic && !b.idleTimeout.Load() && !errors.Is(err, context.Canceled) {
			b.fail("stream read error")
		}
	}
	return n, err
}

// Aborted reports whether the lease carrying this body was aborted by a
// rotation. The stream layers use it to tell an exit cut apart from an
// upstream that simply ended its body.
func (b *warpBody) Aborted() bool {
	if b == nil || b.lease == nil || b.lease.lease == nil {
		return false
	}
	return b.lease.lease.Aborted()
}

func (b *warpBody) Close() error {
	err := b.ReadCloser.Close()
	if b.cancel != nil {
		b.cancel()
	}
	b.lease.close()
	return err
}

// fail retires the exit behind this body. Missing state is a no-op; the pool
// ignores a lease that was already aborted or reported its failure, so a late
// stream failure cannot penalize a restored exit a second time.
func (b *warpBody) fail(reason string) {
	if b == nil || b.lease == nil || b.lease.pool == nil || b.diagnostic {
		return
	}
	if b.lease.lease != nil && (b.lease.lease.Aborted() || b.lease.lease.Reported()) {
		return
	}
	b.lease.pool.Fail(b.lease.lease, reason)
}

// warpFailReason names the condition that should retire an exit. A stalled
// first token is treated as a tunnel failure: retrying the same userspace
// tunnel would only repeat the stall.
func warpFailReason(err error) string {
	switch {
	case errors.Is(err, errFirstEventTimeout):
		return "first event timeout"
	case errors.Is(err, errAttemptTimeout):
		return "attempt timeout"
	default:
		return "transport error"
	}
}

// doWARPAttempt sends one prepared request through an acquired WARP exit,
// feeds the outcome back into the pool, and returns the response with a body
// that releases the exit when closed. The egress label is returned for
// telemetry ("warp:<public address>").
func (g *Gateway) doWARPAttempt(ctx context.Context, req *http.Request, baseURL, model string, firstEventTimeout time.Duration) (*http.Response, string, error) {
	lease, err := g.warp.Acquire(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		return nil, "", fmt.Errorf("%w: %v", errWARPUnavailable, err)
	}
	// The attempt context ends with either the caller's request or the lease,
	// so a rotation interrupts an in-flight request immediately. It stays live
	// until the response body is closed: canceling it would abort the stream.
	attemptCtx, stopAttempt := lease.RequestContext(ctx)
	req = req.Clone(attemptCtx)
	owned, err := g.newWarpLease(lease, g.warpUsesBun(baseURL, model))
	if err != nil {
		stopAttempt()
		lease.Release()
		return nil, "", fmt.Errorf("%w: %v", errWARPUnavailable, err)
	}
	label := "warp:" + lease.EgressKey()
	setWarpEgress(ctx, label)
	diagnostic := isDiagnosticRequest(ctx)
	resp, err := doInferenceAttempt(owned.client(), req, firstEventTimeout, g.attemptTimeout)
	if err != nil {
		if lease.Aborted() {
			// The exit was rotated under this attempt. That is a retryable
			// transport failure, not a client cancellation, and the pool has
			// already retired the exit.
			owned.close()
			stopAttempt()
			return nil, label, fmt.Errorf("%w: %v", warp.ErrExitRotated, err)
		}
		if ctx.Err() == nil && !diagnostic {
			g.warp.Fail(lease, warpFailReason(err))
		}
		owned.close()
		stopAttempt()
		return nil, label, err
	}
	if !diagnostic {
		var body []byte
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests {
			body = peekResponseBody(resp)
		}
		g.warp.Report(lease, resp.StatusCode, body)
	}
	resp.Body = &warpBody{ReadCloser: resp.Body, lease: owned, diagnostic: diagnostic, cancel: stopAttempt}
	return resp, label, nil
}

// setWarpEgress records the exit that actually carries the attempt so request
// logs show the WARP address instead of the unused key proxy binding.
func setWarpEgress(ctx context.Context, label string) {
	if meta := telemetry.MetaFromContext(ctx); meta != nil {
		meta.Proxy = label
	}
}

// warpProxy is a name-only proxyTransport used by telemetry records for WARP
// attempts. The value is never used for dialing or health tracking.
func warpProxy(label string) *proxyTransport {
	if label == "" {
		return nil
	}
	return &proxyTransport{name: label}
}

// doWarpAnonymousUpstream is the anonymous lane with WARP as its only egress.
// It walks the same two upstream gates (v2 /inference first, v1 /zen second)
// as doAnonymousUpstream, but draws a WARP lease per attempt instead of a
// proxy. The lane breaker still applies; per-proxy cooldowns and the block
// cursor do not, because a block is a property of the WARP exit and rotation
// replaces it.
func (g *Gateway) doWarpAnonymousUpstream(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, attemptOffset int) (*http.Response, error, int) {
	if !g.anonymous.AllowAttempt() {
		return nil, errAnonymousLanePaused, 0
	}
	defer g.anonymous.ReleaseProbe()
	rawBody := bodies[config.TierZen]
	if len(rawBody) == 0 {
		return nil, errors.New("no prepared Zen request body"), 0
	}
	protocol := route.ProtocolFor(config.TierZen)
	upstreams := g.anonymousUpstreams()
	prepared := make([][]byte, len(upstreams))
	renames := make([]*wire.ToolRenames, len(upstreams))
	for index, baseURL := range upstreams {
		prepared[index], renames[index] = prepareAnonymousBody(rawBody, protocol, models.IsV1Upstream(baseURL))
	}
	// A 429 is scoped to the exit that carried it, so the request keeps
	// rotating exits until the configured attempt budget is spent; only then
	// does the last 429 reach the client.
	limit := g.cfg.Retry.MaxAttempts
	if limit < 1 {
		limit = 1
	}
	firstEventTimeout := time.Duration(g.cfg.Performance.FirstEventTimeoutSeconds) * time.Second
	var lastResponse *http.Response
	var lastErr error
	blocked, halted := false, false
	attempts := 0
	generation := g.sessions.Generation(ids.Session)
	for attempts < limit {
		if ctx.Err() != nil {
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			break
		}
		leaseBlocked := false
		for index, baseURL := range upstreams {
			if ctx.Err() != nil {
				break
			}
			setRequestShape(ctx, false, renames[index])
			attempts++
			if meta := telemetry.MetaFromContext(ctx); meta != nil {
				meta.Attempts = attemptOffset + attempts
				meta.Tier = string(config.TierZen)
			}
			if lastResponse != nil {
				httpx.DrainAndClose(lastResponse.Body)
				lastResponse = nil
			}
			attemptIDs := ids.WithSessionGeneration(generation)
			req, err := upstreamRequest(ctx, baseURL, protocol, prepared[index], attemptIDs, anonymousZenKey)
			if err != nil {
				return nil, err, attempts
			}
			// D5: dump the bytes that actually go on the wire, naming the base
			// they target. Emitted here, not in the preparation loop above, so
			// a candidate that is never attempted leaves no "shaped" record.
			g.dumpOutboundBodyForUpstream(route, config.TierZen, prepared[index], ids, "shaped", baseURL)
			setRequestCredential(ctx, config.TierZen, "anonymous", "anonymous", true, nil)
			started := time.Now()
			resp, egress, err := g.doWARPAttempt(ctx, req, baseURL, route.ID, firstEventTimeout)
			duration := time.Since(started)
			if ctx.Err() != nil {
				lastResponse, lastErr = resp, err
				if lastErr == nil && lastResponse == nil {
					lastErr = ctx.Err()
				}
				break
			}
			status := upstreamStatus(resp)
			g.recordUpstreamAttempt(ctx, route, ids, attemptOffset+attempts, "anonymous", "anonymous", true, warpProxy(egress), resp, err, duration)
			if err == nil && status/100 == 2 {
				g.anonymous.NoteSuccess()
				g.logger.Debug("anonymous WARP upstream accepted request", "component", "upstream", "event", "anonymous_attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "upstream", config.RedactURL(baseURL), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", egress, "status", status, "duration_ms", duration.Milliseconds())
				return resp, nil, attempts
			}
			lastResponse, lastErr = resp, err
			if err != nil {
				g.logger.Debug("anonymous WARP transport attempt failed", "component", "upstream", "event", "anonymous_attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "upstream", config.RedactURL(baseURL), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", egress, "duration_ms", duration.Milliseconds(), "error", err)
				// A broken tunnel affects both upstream gates; rotate the exit
				// instead of repeating the request on the same lease.
				break
			}
			if peekNetworkAbuseBlock(resp) {
				lastErr = errFreeTierIPBlocked
				blocked, leaseBlocked = true, true
				g.logger.Warn("free tier blocked the WARP exit; rotating it", "component", "upstream", "event", "free_tier_ip_blocked", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "upstream", config.RedactURL(baseURL), "proxy", egress)
				break
			}
			if isDeterministicClientResponse(status, peekResponseBody(resp)) {
				lastErr = errDeterministicClientRejection
				halted = true
				g.logger.Warn("upstream rejected the request; not trying other upstreams", "component", "upstream", "event", "deterministic_rejection", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "upstream", config.RedactURL(baseURL), "status", status, "duration_ms", duration.Milliseconds())
				break
			}
			if shouldRotateSession(resp, err) {
				// Same quota contract as the proxy lane: the session, not only the
				// exit, carries part of the free allowance.
				generation = g.sessions.Rotate(ids.Session)
				g.logger.Warn("free tier quota refused; rotating the upstream session", "component", "upstream", "event", "session_rotated", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "upstream", config.RedactURL(baseURL), "proxy", egress, "generation", generation)
			}
			g.logger.Debug("anonymous WARP upstream returned an error response; trying the next upstream or exit", "component", "upstream", "event", "anonymous_attempt_response_failed", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "upstream", config.RedactURL(baseURL), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", egress, "status", status, "duration_ms", duration.Milliseconds())
		}
		if halted || ctx.Err() != nil {
			break
		}
		if anonymousCountsFailure(lastErr, lastResponse, leaseBlocked) {
			if pause, tripped := g.anonymous.NoteFailure(); tripped {
				g.logger.Warn("anonymous lane circuit breaker opened", "component", "upstream", "event", "anonymous_breaker_open", "status", upstreamStatus(lastResponse), "paused_for", pause.String())
				break
			}
		}
	}
	if halted {
		g.anonymous.NoteNeutral()
		return lastResponse, errDeterministicClientRejection, attempts
	}
	if lastResponse != nil {
		if blocked {
			return lastResponse, errFreeTierIPBlocked, attempts
		}
		return lastResponse, nil, attempts
	}
	if blocked {
		return nil, errFreeTierIPBlocked, attempts
	}
	if lastErr == nil {
		lastErr = errWARPUnavailable
	}
	return nil, lastErr, attempts
}

// anonymousCountsFailure reports whether one anonymous WARP round should count
// against the lane breaker. A rotation abort is a property of the exit that
// carried the attempt, not of the lane: several requests interrupted by one
// 429 would otherwise trip the breaker together. Lease-scoped 429s and IP
// blocks are excluded for the same reason.
func anonymousCountsFailure(lastErr error, lastResponse *http.Response, leaseBlocked bool) bool {
	return !leaseBlocked &&
		upstreamStatus(lastResponse) != http.StatusTooManyRequests &&
		!errors.Is(lastErr, warp.ErrExitRotated)
}

// observeKeyResultWARP is observeKeyResult for a keyed attempt that left
// through WARP: the key still cools on an upstream rejection, but failures of
// the tunnel itself (no exit, dial or first-event failure) are not the key's
// fault and must not cool it.
func (g *Gateway) observeKeyResultWARP(ctx context.Context, nodes *upstreamNodes, node *upstreamNode, resp *http.Response, err error) {
	if isDiagnosticRequest(ctx) {
		return
	}
	status := upstreamStatus(resp)
	if err == nil && status/100 == 2 || isNonRetryableClientResponse(resp, err) {
		nodes.MarkSuccess(node)
		return
	}
	if err != nil && resp == nil {
		// The lease acquisition, dial, or first event failed: the upstream
		// never judged this key. The pool already retired the exit.
		return
	}
	// A 429 or a network-abuse 403 is scoped to the WARP exit, which the pool
	// has just retired; holding it against the key would stop a single-key
	// rotation after the first attempt instead of retrying on a fresh exit.
	if resp != nil && (resp.StatusCode == http.StatusTooManyRequests || peekNetworkAbuseBlock(resp)) {
		return
	}
	nodes.MarkFailure(node, resp, err)
}
