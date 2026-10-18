package gateway

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"opencode2api/internal/config"
	modelcatalog "opencode2api/internal/models"
)

const (
	proxyHealthCheckURL      = "https://cloudflare.com/cdn-cgi/trace"
	proxyHealthCheckInterval = 15 * time.Minute
	// Degraded-mode retry: when no proxy is healthy (e.g. a single-proxy Clash
	// node switch), waiting a full 15m for the next scheduled check feels
	// stuck. Poll unhealthy proxies at this faster cadence until one recovers.
	proxyHealthCheckDegradedInterval = 15 * time.Second
	proxyHealthCheckTimeout          = 10 * time.Second
)

// syncProxyResult updates proxy health from real traffic. Only timeouts and
// connection refusals mark a proxy unavailable. Any HTTP response — including
// 4xx/5xx — proves the route is reachable and restores a proxy evicted by an
// earlier transient failure, so a rate-limited exit is never blamed on the
// proxy route itself.
func (g *Gateway) syncProxyResult(ctx context.Context, proxy *proxyTransport, status int, err error) bool {
	if proxy == nil || proxy.pool {
		return false
	}
	if isProxyFailure(err) {
		g.rebindFailedProxy(proxy)
		g.verifyProxyAfterError(ctx, proxy, status)
		return true
	}
	if status >= 200 && status < 400 {
		wasHealthy := proxy.healthy.Swap(true)
		if !wasHealthy {
			g.restoreProxy(proxy)
		}
		return false
	}
	if err == nil && status >= 400 && status < 600 {
		// Receiving an HTTP status from the upstream proves the proxy route
		// itself is reachable; restore an unhealthy flag left by an earlier
		// transient timeout instead of keeping the route evicted.
		wasHealthy := proxy.healthy.Swap(true)
		if !wasHealthy {
			g.restoreProxy(proxy)
		}
		return false
	}
	if err != nil {
		g.verifyProxyAfterError(ctx, proxy, status)
		return false
	}
	return false
}

func (g *Gateway) verifyProxyAfterError(ctx context.Context, proxy *proxyTransport, status int) {
	if !proxy.checking.CompareAndSwap(false, true) {
		return
	}
	// The client request may finish or be cancelled while the verification is
	// running. Keep its values but give the proxy check an independent timeout.
	checkCtx := context.WithoutCancel(ctx)
	go func() {
		result := g.transports.checkClaimedProxy(checkCtx, proxy, proxyHealthCheckURL, proxyHealthCheckTimeout)
		g.applyProxyHealthResult(result, "upstream HTTP response", status)
	}()
}

func (g *Gateway) rebindFailedProxy(proxy *proxyTransport) (zenMoved, goMoved int) {
	if proxy == nil {
		return 0, 0
	}
	wasHealthy := proxy.healthy.Swap(false)
	return g.rebindUnavailableProxy(proxy, wasHealthy)
}

func (g *Gateway) rebindUnavailableProxy(proxy *proxyTransport, wasHealthy bool) (zenMoved, goMoved int) {
	zenMoved = g.zenNodes.RebindProxy(proxy.index)
	goMoved = g.goNodes.RebindProxy(proxy.index)
	if wasHealthy || zenMoved+goMoved > 0 {
		g.logger.Warn("proxy became unavailable", "component", "proxy", "event", "proxy_unavailable", "proxy", config.RedactURL(proxy.name), "zen_keys_moved", zenMoved, "go_keys_moved", goMoved)
	}
	return zenMoved, goMoved
}

func (g *Gateway) restoreProxy(proxy *proxyTransport) (zenMoved, goMoved int) {
	if proxy == nil {
		return 0, 0
	}
	zenMoved = g.zenNodes.RestoreProxy(proxy.index)
	goMoved = g.goNodes.RestoreProxy(proxy.index)
	// The anonymous lane carries its own per-proxy cooldowns; key recovery
	// alone would leave it skipped after an outage-era failure.
	anonCleared := g.anonymous.RestoreProxy(proxy)
	if zenMoved+goMoved > 0 || anonCleared > 0 {
		g.logger.Info("proxy connectivity restored", "component", "proxy", "event", "proxy_restored", "proxy", config.RedactURL(proxy.name), "zen_keys_moved", zenMoved, "go_keys_moved", goMoved, "anonymous_cleared", anonCleared)
	}
	return zenMoved, goMoved
}

func (g *Gateway) StartProxyHealthChecks(ctx context.Context) {
	check := func(source string) {
		results := g.transports.CheckHealth(ctx, proxyHealthCheckURL, proxyHealthCheckTimeout)
		for _, result := range results {
			g.applyProxyHealthResult(result, source, 0)
		}
	}
	go func() {
		normal := time.NewTicker(proxyHealthCheckInterval)
		defer normal.Stop()
		degraded := time.NewTicker(proxyHealthCheckDegradedInterval)
		defer degraded.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-normal.C:
				check("scheduled health check")
			case <-degraded.C:
				// Fast lane: only when every proxy is down. CheckHealth is a
				// no-op while anything is healthy, so this costs nothing in
				// the common case and caps single-proxy outage recovery at
				// ~15s instead of ~15m (15s interval leaves a 5s gap after
				// the 10s probe timeout, so ticks never overlap).
				if g.transports != nil && len(g.transports.items) > 0 && !g.transports.hasHealthy() {
					check("degraded health check")
				}
			}
		}
	}()
}

// kickProxyRecheck triggers one immediate async re-probe after a request hits
// the no-healthy-proxy path. Kicks are throttled to one per few seconds: the
// per-proxy checking CAS inside CheckHealth only dedupes overlapping probes,
// not goroutine creation, so an unthrottled call-per-request would spawn a
// short-lived goroutine per request during an all-down spike. The degraded
// ticker is the correctness backstop; dropped kicks only delay recovery by
// seconds. Detached from the request context so the probe survives client
// disconnects — but a request that is already cancelled kicks nothing.
//
// It is a no-op whenever any proxy is healthy, which is exactly what keeps it
// away from cooldown: a node inside its backoff window is still a healthy
// proxy, so a quota/ratelimit cooldown never triggers a probe that could not
// clear it anyway.
func (g *Gateway) kickProxyRecheck(ctx context.Context) {
	if g == nil || g.transports == nil || len(g.transports.items) == 0 || g.transports.hasHealthy() {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return
	}
	const kickThrottle = 5 * time.Second
	now := time.Now().UnixNano()
	last := g.lastProxyKick.Load()
	if now-last < int64(kickThrottle) {
		return
	}
	if !g.lastProxyKick.CompareAndSwap(last, now) {
		return
	}
	checkCtx := context.WithoutCancel(ctx)
	go func() {
		results := g.transports.CheckHealth(checkCtx, proxyHealthCheckURL, proxyHealthCheckTimeout)
		for _, result := range results {
			g.applyProxyHealthResult(result, "on-demand health check", 0)
		}
	}()
}

func (g *Gateway) applyProxyHealthResult(result proxyHealthResult, source string, upstreamStatus int) {
	if result.err == nil {
		if !result.wasHealthy {
			g.restoreProxy(result.proxy)
		}
		g.logger.Debug("proxy health check passed", "component", "proxy", "event", "health_check_passed", "source", source, "upstream_status", upstreamStatus, "proxy", config.RedactURL(result.proxy.name))
		return
	}
	if !result.failed {
		g.logger.Debug("proxy health check was inconclusive", "component", "proxy", "event", "health_check_inconclusive", "source", source, "upstream_status", upstreamStatus, "proxy", config.RedactURL(result.proxy.name), "error", result.err)
		return
	}
	if g.transports.hasHealthy() {
		zenMoved, goMoved := g.rebindUnavailableProxy(result.proxy, result.wasHealthy)
		if result.wasHealthy || zenMoved+goMoved > 0 {
			g.logger.Warn("proxy health check failed", "component", "proxy", "event", "health_check_failed", "source", source, "upstream_status", upstreamStatus, "proxy", config.RedactURL(result.proxy.name), "zen_keys_moved", zenMoved, "go_keys_moved", goMoved, "error", result.err)
			return
		}
	}
	g.logger.Debug("proxy health check is still failing", "component", "proxy", "event", "health_check_still_failing", "source", source, "upstream_status", upstreamStatus, "proxy", config.RedactURL(result.proxy.name), "error", result.err)
}

func (g *Gateway) StartModelRefresh(ctx context.Context) {
	refresh := func() {
		var zen, goModels []string
		var capabilities modelcatalog.Capabilities
		var capabilitiesErr error
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); zen = g.refreshZen(ctx) }()
		go func() { defer wg.Done(); goModels = g.refreshTier(ctx, g.cfg.Upstream.Go, g.goNodes) }()
		go func() {
			defer wg.Done()
			capabilityCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			capabilities, capabilitiesErr = g.refreshProtocolCapabilities(capabilityCtx)
		}()
		wg.Wait()
		if ctx.Err() != nil {
			return
		}
		if capabilitiesErr != nil {
			g.logger.Warn("OpenCode capability catalog refresh failed", "component", "models", "event", "capability_refresh_failed", "error", capabilitiesErr)
		}
		if zen != nil || goModels != nil {
			g.catalog.ReplaceWithCapabilities(zen, goModels, capabilities.Protocols, capabilities.Unsupported, capabilities.Metadata)
			g.syncRotation()
			if ctx.Err() == nil {
				if err := g.catalog.SaveCache(); err != nil {
					g.logger.Warn("model catalog cache write failed", "component", "models", "event", "catalog_cache_write_failed", "error", err)
				}
			}
			g.logger.Info("model catalog refreshed", "component", "models", "event", "catalog_refreshed", "models", len(g.catalog.List()))
		}
	}
	go func() {
		refresh()
		ticker := time.NewTicker(time.Duration(g.cfg.Models.RefreshSeconds) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refresh()
			}
		}
	}()
}

func (g *Gateway) refreshProtocolCapabilities(ctx context.Context) (modelcatalog.Capabilities, error) {
	if g.transports == nil || len(g.transports.items) == 0 {
		return modelcatalog.FetchCapabilities(ctx, &http.Client{Timeout: 30 * time.Second}, modelcatalog.CapabilitiesURL)
	}
	var lastErr error
	for _, proxy := range g.transports.items {
		if proxy == nil || !proxy.healthy.Load() {
			continue
		}
		capabilities, err := modelcatalog.FetchCapabilities(ctx, proxy.client, modelcatalog.CapabilitiesURL)
		if err == nil {
			return capabilities, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no healthy proxy available for OpenCode capability catalog")
	}
	return modelcatalog.Capabilities{}, lastErr
}

func (g *Gateway) refreshZen(ctx context.Context) []string {
	if models := g.refreshTier(ctx, g.cfg.Upstream.Zen, g.zenNodes); models != nil {
		return models
	}
	if !g.cfg.Anonymous {
		return nil
	}
	return g.refreshAnonymousTier(ctx, g.cfg.Upstream.Anonymous)
}

func (g *Gateway) refreshAnonymousTier(ctx context.Context, base string) []string {
	cursor := g.anonymous.CursorFor("")
	limit := g.anonymous.Len()
	if g.anonymous.HasPool() && g.cfg.Retry.MaxAttempts > limit {
		limit = g.cfg.Retry.MaxAttempts
	}
	for attempt := 1; attempt <= limit; attempt++ {
		node := cursor.Next()
		if node == nil {
			break
		}
		refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		var models []string
		var status int
		var err error
		// Prefer the legacy /zen/v1 models listing before falling back to the
		// configured anonymous upstream.
		for _, upstream := range g.anonymousUpstreams() {
			models, status, err = modelcatalog.FetchModels(refreshCtx, node.proxy.client, upstream, anonymousZenKey)
			if err == nil {
				break
			}
		}
		g.syncProxyResult(refreshCtx, node.proxy, status, err)
		cancel()
		if err == nil {
			g.anonymous.markRefreshSuccess(node)
			return models
		}
		// Background catalog refresh: no client payload to fingerprint, so the
		// cooldown stays unscoped (blocks every request) exactly as before.
		g.anonymous.MarkFailure(node, nil, err, "")
		g.logger.Debug("anonymous model catalog refresh attempt failed", "component", "models", "event", "anonymous_refresh_attempt_failed", "upstream", config.RedactURL(base), "attempt", attempt, "proxy", config.RedactURL(node.proxy.name), "error", err)
	}
	g.logger.Warn("anonymous model catalog refresh failed", "component", "models", "event", "anonymous_refresh_failed", "upstream", config.RedactURL(base))
	return nil
}

func (g *Gateway) refreshTier(ctx context.Context, base string, nodes *nodePool) []string {
	cursor := nodes.Cursor()
	for attempt := 0; attempt < g.cfg.Retry.MaxAttempts; attempt++ {
		node := cursor.Next()
		if node == nil {
			return nil
		}
		refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		proxy := nodes.Proxy(node)
		if proxy == nil {
			cancel()
			return nil
		}
		models, status, err := modelcatalog.FetchModels(refreshCtx, proxy.client, base, node.key)
		g.syncProxyResult(refreshCtx, proxy, status, err)
		cancel()
		if err == nil {
			nodes.MarkSuccess(node)
			return models
		}
		nodes.MarkFailure(node, nil, err)
		g.logger.Debug("model catalog refresh attempt failed", "component", "models", "event", "refresh_attempt_failed", "upstream", config.RedactURL(base), "attempt", attempt+1, "error", err)
	}
	g.logger.Warn("model catalog refresh failed", "component", "models", "event", "refresh_failed", "upstream", config.RedactURL(base))
	return nil
}
