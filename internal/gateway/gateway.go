// Package gateway routes inference requests through managed upstream pools.
//
// Upstreams fall into two lanes, named after the OpenCode client generation
// whose request shape they expect, not after their URL path: both lanes
// append the /v1/... route segment. The v1 lane is the https://opencode.ai/zen
// origin (models.V1Base, also the default upstream.zen), which
// models.IsV1Upstream detects. It expects the OpenCode 1.x client shape: the
// 1.x User-Agent, the x-opencode-request header, a git-commit-shaped project
// and, on the free tier, the 1.x toolset. The v2 lane is the /inference
// origin (upstream.anonymous) and the Go plan's /zen/go origin, which expect
// the OpenCode 2.x CLI shape. The anonymous free tier follows
// upstream.anonymous_lane_order, which defaults to the v2 /inference origin
// alone; the v1 origin is opt-in rather than tried first.
package gateway

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"opencode2api/internal/identity"
	"opencode2api/internal/jsonutil"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/rotation"
	"opencode2api/internal/telemetry"
	"opencode2api/internal/warp"
)

const maxRequestBody = 32 << 20

// anonymousZenKey is the shared credential the anonymous free tier sends as
// its API key.
const anonymousZenKey = "public"

// attemptDeadline returns the per-attempt deadline, capped at the request
// timeout. It is zero unless attempt_timeout_seconds is set: AttemptTimeout
// would otherwise fall back to the request timeout, which the request context
// already enforces.
func attemptDeadline(cfg config.Config) time.Duration {
	if cfg.Performance.AttemptTimeoutSeconds <= 0 {
		return 0
	}
	return cfg.Performance.AttemptTimeout(time.Duration(cfg.Retry.TimeoutSeconds) * time.Second)
}

// statusClientClosedRequest is nginx's 499: the client went away before the
// gateway could answer, so the failure is neither upstream nor ours.
const statusClientClosedRequest = 499

// proxyUnavailableMessage is returned when every attempt died on the proxy
// route itself, so operators look at their proxies instead of the upstream.
//
// It is sent only when the error a request finally failed with is tagged
// errProxyUnavailable: its last attempt never reached the upstream because the
// proxy route failed, or the anonymous lane had no proxy to try while none was
// healthy. Only that final error is inspected; earlier attempts may have failed
// for other reasons.
//
// The "opencode2api:" prefix is load-bearing: watchdog-clash.ps1 tells a
// gateway-authored backoff apart from a real upstream error by that substring,
// and this is a gateway-authored error. The wording must also stay clear of
// "ModelUnavailable" — the script reads 503 + that word as a catalog verdict
// and stops the whole failover round instead of switching nodes.
const proxyUnavailableMessage = "opencode2api: **[gateway]** all proxies are unavailable; the upstream could not be reached"

// Gateway serves the local inference API for one configuration. It owns the
// proxy transports and the key and anonymous node pools built from that
// config; applying a new config builds a new Gateway (see RuntimeController)
// instead of mutating this one.
type Gateway struct {
	rotation   *rotation.Manager
	cfg        config.Config
	logger     *slog.Logger
	transports *proxyTransports
	zenNodes   *upstreamNodes
	goNodes    *upstreamNodes
	anonymous  *anonymousNodes
	// warp, when non-nil, is the shared userspace WARP pool. Free-model
	// traffic on the anonymous lane and the Zen key tier leaves through it
	// instead of the configured proxies and transports.
	warp         *warp.Pool
	catalog      *models.ModelCatalog
	monitor      *telemetry.Metrics
	availability *models.AvailabilityRegistry
	// staleMemo is embedded by value: never copy a Gateway after use, and
	// note that hot-reload builds a fresh Gateway whose memo starts empty
	// (the first post-reload turn on a poisoned history simply self-heals
	// once, exactly as before this memory existed).
	staleMemo staleMemo
	// attemptTimeout bounds a whole upstream attempt (dial + write + headers
	// and, when the first-event wait is enabled, the first event), not just
	// the header wait. Zero arms no per-attempt deadline: each attempt is then
	// bounded only by the request-level budget.
	attemptTimeout time.Duration
	// lastProxyKick throttles on-demand proxy re-probes (see
	// kickProxyRecheck): without it an all-down spike would spawn one
	// short-lived probe goroutine per request while the per-proxy CAS only
	// dedupes overlapping probes, not goroutine creation.
	lastProxyKick atomic.Int64
	// freeTierBlock is how long an exit the free tier flagged stays out of the
	// anonymous cursor. Zero uses defaultFreeTierBlockTTL.
	freeTierBlock time.Duration
}

// New builds a Gateway for cfg: one proxy transport per runtime proxy (a
// single direct transport when none is configured), Zen and Go key pools
// bound to those transports, the anonymous node pool and a model catalog. Its
// rotation manager is not persisted; RuntimeController replaces it with the
// shared one.
func New(cfg config.Config, logger *slog.Logger, monitor *telemetry.Metrics, warpPool *warp.Pool) (*Gateway, error) {
	cfg.Rotation.Defaults()
	rotationManager, err := rotation.New("", cfg.Rotation)
	if err != nil {
		return nil, err
	}
	transports, err := transportPool(cfg.RuntimeProxies(), cfg.Performance, cfg.Performance.AttemptTimeout(time.Duration(cfg.Retry.TimeoutSeconds)*time.Second))
	if err != nil {
		return nil, err
	}
	cooldown := time.Duration(cfg.Performance.FailureCooldownSeconds) * time.Second
	zenNodes, err := nodePool(cfg.ZenKeys, transports, cooldown)
	if err != nil {
		return nil, fmt.Errorf("zen node pool: %w", err)
	}
	goNodes, err := nodePool(cfg.GoKeys, transports, cooldown)
	if err != nil {
		return nil, fmt.Errorf("go node pool: %w", err)
	}
	catalog := models.Catalog(cfg.Prefer, cfg.Models.Protocols)
	catalog.SetRefreshInterval(time.Duration(cfg.Models.RefreshSeconds) * time.Second)
	catalog.SetFreeOnly(cfg.Models.FreeOnly)
	return &Gateway{
		cfg:            cfg,
		rotation:       rotationManager,
		logger:         logger,
		transports:     transports,
		zenNodes:       zenNodes,
		goNodes:        goNodes,
		anonymous:      anonymousPool(cfg.Anonymous, transports, cooldown, anonymousBackoffFromConfig(cfg.Models.AnonymousBackoff)),
		warp:           warpPool,
		catalog:        catalog,
		monitor:        monitor,
		attemptTimeout: attemptDeadline(cfg),
		freeTierBlock:  time.Duration(cfg.Performance.FreeTierBlockSeconds) * time.Second,
	}, nil
}

// anonymousBackoffFromConfig converts the configured hours into the breaker's
// thresholds and durations. A nil block disables the breaker (threshold 0).
func anonymousBackoffFromConfig(cfg *config.AnonymousBackoffConfig) anonymousBackoff {
	if cfg == nil {
		return anonymousBackoff{}
	}
	return anonymousBackoff{
		threshold: int32(cfg.Threshold),
		base:      time.Duration(cfg.BaseHours) * time.Hour,
		max:       time.Duration(cfg.MaxHours) * time.Hour,
	}
}

// Handler returns the gateway's HTTP API: the server-key authenticated /v1
// model, inference and System One routes plus the open /healthz, wrapped in
// panic recovery.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", g.authenticate(g.handleModels))
	mux.HandleFunc("POST /v1/chat/completions", g.authenticate(g.handleInference(wire.Chat)))
	mux.HandleFunc("POST /v1/responses", g.authenticate(g.handleInference(wire.Responses)))
	mux.HandleFunc("POST /v1/messages", g.authenticate(g.handleInference(wire.Anthropic)))
	mux.HandleFunc("POST /v1/systemone", g.authenticate(g.handleSystemOne))
	mux.HandleFunc("GET /v1/watchdog/status", g.authenticate(g.handleWatchdogStatus))
	mux.HandleFunc("POST /v1/watchdog/probe", g.authenticate(g.handleWatchdogProbe))
	mux.HandleFunc("GET /healthz", g.handleHealth)
	return telemetry.Recover(g.logger, mux)
}

// Local: watchdog (read-only Clash decision feed) lives in watchdog.go;
// only the route hooks stay here.

// authenticate admits a request whose x-api-key header or Bearer token
// matches an enabled server key, and stores the caller's fingerprint in the
// request context and telemetry metadata. Anything else gets a 401 shaped for
// the endpoint: Anthropic for /v1/messages, Chat otherwise.
func (g *Gateway) authenticate(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		candidates := []string{strings.TrimSpace(r.Header.Get("x-api-key"))}
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			candidates = append(candidates, strings.TrimSpace(auth[7:]))
		}
		matched := ""
		for _, key := range g.cfg.ServerKeys {
			if !g.cfg.ServerKeyEnabled(key) {
				continue
			}
			for _, candidate := range candidates {
				if len(candidate) == len(key) && subtle.ConstantTimeCompare([]byte(candidate), []byte(key)) == 1 {
					matched = key
				}
			}
		}
		if matched == "" {
			protocol := wire.Chat
			if r.URL.Path == "/v1/messages" {
				protocol = wire.Anthropic
			}
			wire.WriteError(w, protocol, http.StatusUnauthorized, "invalid local API key", "authentication_error", "")
			return
		}
		// The Playground borrows the first enabled key to pass this check, so
		// its traffic gets its own bucket instead of being billed to that key.
		caller := config.Fingerprint(matched)
		if isDiagnosticRequest(r.Context()) {
			caller = telemetry.PlaygroundServerKey
		}
		if meta := telemetry.MetaFromRequest(r); meta != nil {
			meta.ServerKeyID = caller
		}
		next(w, r.WithContext(context.WithValue(r.Context(), callerContextKey{}, caller)))
	}
}

type callerContextKey struct{}

// callerScope returns the authenticated caller that request identities are
// scoped to, so different server keys never share an upstream session.
func callerScope(ctx context.Context) string {
	caller, _ := ctx.Value(callerContextKey{}).(string)
	return caller
}

// handleInference serves a message endpoint whose wire protocol is external.
// A rotation group alias is handed to handleRotation. Any other model is
// routed through the catalog, encoded per upstream tier, sent through
// doUpstream and translated back into external, streaming or not.
type requestShapeContextKey struct{}

type requestShape struct {
	shaped  bool
	renames *wire.ToolRenames
}

func setRequestShape(ctx context.Context, shaped bool, renames *wire.ToolRenames) {
	if state, ok := ctx.Value(requestShapeContextKey{}).(*requestShape); ok {
		state.shaped, state.renames = shaped, renames
	}
	if meta := telemetry.MetaFromContext(ctx); meta != nil {
		meta.Shaped = shaped
		meta.ToolRenames = renames.Count()
	}
}

func requestShapeFrom(ctx context.Context) requestShape {
	if state, ok := ctx.Value(requestShapeContextKey{}).(*requestShape); ok && state != nil {
		return *state
	}
	return requestShape{}
}

func (g *Gateway) handleInference(external wire.Protocol) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), requestShapeContextKey{}, &requestShape{}))
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
		if err != nil {
			wire.WriteError(w, external, http.StatusBadRequest, "request body is too large or unreadable", "invalid_request_error", "")
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			wire.WriteError(w, external, http.StatusBadRequest, "request body must be a JSON object", "invalid_request_error", "")
			return
		}
		model := jsonutil.StringAt(payload, "model")
		meta := telemetry.MetaFromRequest(r)
		if meta != nil {
			meta.Model = model
			meta.ReasoningEffort = wire.ReasoningEffortLabel(external, payload)
			// The client-requested shape is recorded up front: a spot past
			// the non-2xx return mislabels failed streams as sync.
			meta.Stream = jsonutil.BoolAt(payload, "stream")
		}
		// Derived before routing so every client-visible rejection can carry a
		// real x-request-id for log/telemetry correlation. Previously these
		// paths passed the literal "model" (or nothing), which produced
		// `x-request-id: model` or no header at all.
		//
		// Scoped to the calling server key, so two callers replaying the same
		// client session do not land on one upstream session. The scope is
		// already in the context — the auth middleware stored it before this
		// handler ran — so it is available this early.
		//
		// Two of the four request paths still derive again downstream
		// (handleRotation, forwardSystemOne). That is harmless: Session is
		// deterministic, so both derivations agree on it and only Request
		// differs, with the second one winning every consumer. It is not
		// deduplicated because those handlers are separate entry points.
		ids := identity.DeriveRequestIDs(r, payload, callerScope(r.Context()))
		if meta != nil {
			meta.Request = ids.Request
			meta.Session = ids.Session
		}
		if model == "" {
			wire.WriteError(w, external, http.StatusBadRequest, "model is required", "invalid_request_error", ids.Request)
			return
		}
		if id := g.rotation.GroupID(model); id != "" {
			g.handleRotation(w, r, external, payload, id, model)
			return
		}
		if !g.catalog.Supported(model) {
			wire.WriteError(w, external, http.StatusBadRequest, "the model uses an upstream protocol that opencode2api does not expose", "invalid_request_error", ids.Request)
			return
		}
		route, err := g.catalog.Route(model, len(g.cfg.ZenKeys) > 0, len(g.cfg.GoKeys) > 0, g.cfg.Anonymous)
		if override, selected := debugKeyOverrideFrom(r.Context()); selected {
			// A per-key diagnostic must not silently be served by another key,
			// another tier or the anonymous lane.
			route, err = g.catalog.RouteForTier(model, override.Tier, len(g.cfg.ZenKeys) > 0, len(g.cfg.GoKeys) > 0)
		}
		if err != nil {
			if errors.Is(err, models.ErrModelDisabled) {
				// Script contract: watchdog-clash.ps1 classifies 503 +
				// "ModelUnavailable" as config (stop this round). It must not
				// fall through to the 502/503/504 transport branch, which would
				// burn the whole candidate pool on a gateway-side catalog state
				// that switching exit IPs cannot change.
				writeModelUnavailable(w, external, err, ids.Request)
				return
			}
			wire.WriteError(w, external, http.StatusBadRequest, err.Error(), "invalid_request_error", ids.Request)
			return
		}
		if meta != nil {
			meta.Tier = string(route.Tier)
			meta.Protocol = route.Protocol
		}
		// A System One model has no message-shaped equivalent, so a decision
		// payload submitted on a message endpoint is relayed verbatim instead of
		// being converted. This keeps the model reachable for clients that can
		// only address /v1/chat/completions or /v1/responses — for example a
		// gateway whose OpenAI platform pins every request to Responses.
		if route.Protocol == wire.SystemOne {
			if len(jsonutil.MapAt(payload, "questions")) == 0 || payload["state"] == nil {
				wire.WriteError(w, external, http.StatusBadRequest, "this model uses System One decisions, not chat; send state and typed questions to /v1/systemone", "invalid_request_error", ids.Request)
				return
			}
			g.forwardSystemOne(w, r, body, payload, model, route)
			return
		}
		// Fingerprint the client payload BEFORE any shaping or conversion, so
		// the anonymous cooling scope describes what the caller asked for
		// rather than the gateway-rewritten body. A retry of this turn hashes
		// the same and stays blocked; a different turn is still served.
		fingerprint := identity.RequestFingerprint(payload)
		bodies, err := g.prepareRouteBodies(external, route, payload, body)
		if err != nil {
			wire.WriteError(w, external, http.StatusBadRequest, err.Error(), "invalid_request_error", ids.Request)
			return
		}
		// ids already derived, caller-scoped, before routing — see above.
		stream := jsonutil.BoolAt(payload, "stream")
		requestCtx, cancel := context.WithTimeout(r.Context(), time.Duration(g.cfg.Retry.TimeoutSeconds)*time.Second)
		defer cancel()
		resp, upstreamRoute, err := g.doUpstream(requestCtx, route, bodies, ids, fingerprint)
		if err != nil {
			// The error branch never forwards the last response body, so close
			// it here; otherwise a block or a paused lane leaks one upstream
			// connection on every request.
			if resp != nil && resp.Body != nil {
				httpx.DrainAndClose(resp.Body)
			}
			finalTier := route.Tier
			if meta != nil && meta.Tier != "" {
				finalTier = config.Tier(meta.Tier)
			}
			keyID, channel, anonymous := requestCredential(requestCtx)
			// A downstream disconnect is not an upstream failure: report the
			// real cause so telemetry and clients see a cancel, not a 502.
			if !errors.Is(err, context.DeadlineExceeded) && wire.ClientCanceled(r.Context(), err) {
				g.logger.Debug("client canceled before upstream response", "component", "upstream", "event", "request_canceled", "request_id", ids.Request, "tier", finalTier, "key_id", keyID, "channel", channel, "anonymous", anonymous)
				if meta != nil {
					meta.Outcome = "client_canceled"
				}
				wire.WriteError(w, external, statusClientClosedRequest, "client closed request", "client_canceled", ids.Request)
				return
			}
			// An exhausted request budget is a timeout, not a bad gateway: the
			// distinction matters to clients that retry on 502. Checked before the
			// cooldown hook so a request that lost its budget can never be
			// answered "come back later" for a lane that is in fact fine, and
			// before the proxy check because proxyUnavailableError unwraps to the
			// dial error too — a budget burned on dialing satisfies both.
			if errors.Is(err, context.DeadlineExceeded) {
				wire.WriteError(w, external, http.StatusGatewayTimeout, "upstream request timed out", "upstream_timeout", ids.Request)
				return
			}
			if g.writeCoolingResponse(w, external, err, meta, ids.Request, string(finalTier)) {
				return
			}
			// Nothing reached the upstream because the proxy route failed;
			// report that instead of blaming the upstream with a 502. Must stay
			// after the cooling hook: errAnonymousCooling is never wrapped in
			// errProxyUnavailable, so a cooling lane can still answer 429 here.
			if errors.Is(err, errProxyUnavailable) {
				g.logger.Warn("all upstream attempts failed", "component", "upstream", "event", "request_failed", "request_id", ids.Request, "tier", finalTier, "key_id", keyID, "channel", channel, "anonymous", anonymous, "error", err)
				wire.WriteError(w, external, http.StatusServiceUnavailable, proxyUnavailableMessage, "proxy_error", ids.Request)
				return
			}
			// WARP is enabled but no exit could carry the request, so it never
			// reached the upstream either. Same reasoning as proxy_error: a 502
			// would invite a retry into the same broken egress.
			if errors.Is(err, errWARPUnavailable) {
				g.logger.Warn("all upstream attempts failed", "component", "upstream", "event", "request_failed", "request_id", ids.Request, "tier", finalTier, "key_id", keyID, "channel", channel, "anonymous", anonymous, "error", err)
				wire.WriteError(w, external, http.StatusServiceUnavailable, warpUnavailableMessage, "warp_unavailable", ids.Request)
				return
			}
			// Every exit this request could use is under the free tier's IP
			// block. A 502 would read as a transient upstream fault and invite
			// the client to retry into the same penalty, so answer with the
			// block itself.
			if errors.Is(err, errFreeTierIPBlocked) {
				wire.WriteError(w, external, http.StatusServiceUnavailable, freeTierBlockedMessage, "free_tier_ip_blocked", ids.Request)
				return
			}
			// The anonymous lane's circuit breaker is open. Report the pause
			// distinctly so the operator does not look for a credential or a
			// model problem in the upstream error.
			if errors.Is(err, errAnonymousLanePaused) {
				wire.WriteError(w, external, http.StatusServiceUnavailable, anonymousLanePausedMessage, "free_tier_lane_paused", ids.Request)
				return
			}
			g.logger.Warn("all upstream attempts failed", "component", "upstream", "event", "request_failed", "request_id", ids.Request, "tier", finalTier, "key_id", keyID, "channel", channel, "anonymous", anonymous, "error", err)
			wire.WriteError(w, external, http.StatusBadGateway, "all upstream attempts failed", "upstream_error", ids.Request)
			return
		}
		defer resp.Body.Close()
		if meta != nil {
			meta.Tier = string(upstreamRoute.Tier)
			meta.Protocol = upstreamRoute.Protocol
		}
		w.Header().Set("x-request-id", ids.Request)
		if resp.StatusCode/100 != 2 {
			copyErrorResponse(w, external, resp, ids.Request)
			return
		}
		shape := requestShapeFrom(r.Context())
		toolRenames := shape.renames
		if stream {
			if meta != nil {
				meta.Stream = true
			}
			if g.monitor != nil {
				g.monitor.BeginStream()
				defer g.monitor.EndStream()
			}
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Accel-Buffering", "no")
			w.WriteHeader(resp.StatusCode)
			streamBody := wire.RenameStreamReader(resp.Body, upstreamRoute.Protocol, toolRenames)
			var usage wire.Usage
			var usageReported bool
			if external == upstreamRoute.Protocol {
				usage, usageReported, err = wire.ForwardStream(r.Context(), w, streamBody, upstreamRoute.Protocol, model)
			} else {
				usage, usageReported, err = wire.TranscodeStream(r.Context(), w, streamBody, upstreamRoute.Protocol, external, model)
			}
			if meta != nil {
				meta.Usage, meta.UsageReported = usage, usageReported
				if err != nil {
					meta.Outcome = "stream_error"
					if wire.ClientCanceled(r.Context(), err) {
						meta.Outcome = "client_canceled"
					}
				}
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				g.logger.Warn("downstream stream ended with an error", "component", "stream", "event", "stream_failed", "request_id", ids.Request, "model", model, "tier", upstreamRoute.Tier, "error", err)
			}
			return
		}
		responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		if err != nil {
			wire.WriteError(w, external, http.StatusBadGateway, "failed to read upstream response", "upstream_error", ids.Request)
			return
		}
		if (upstreamRoute.Anonymous || shape.shaped) &&
			(isUpstreamSSE(resp.Header.Get("Content-Type")) || looksLikeSSE(responseBody)) {
			// The anonymous lane, and a key-tier request shaped like it, is
			// force-streamed upstream (see prepareAnonymousBody); collapse
			// the events back into the single document this non-streaming
			// client asked for.
			//
			// The content-type gate is load-bearing: an upstream 200 reply that
			// is plain JSON rather than SSE must pass through untouched.
			// Aggregating it unconditionally would fail to parse a perfectly
			// good 200 and answer 502 — and the free/shaped lanes are exactly
			// where that mislabelled shape is most likely to show up.
			collapsed, err := wire.CollapseStream(bytes.NewReader(responseBody), upstreamRoute.Protocol, model)
			if err != nil {
				g.logger.Warn("forced stream collapse failed", "component", "conversion", "event", "stream_collapse_failed", "request_id", ids.Request, "model", model, "anonymous", upstreamRoute.Anonymous, "source_protocol", upstreamRoute.Protocol, "error", err)
				wire.WriteError(w, external, http.StatusBadGateway, "unsupported upstream response", "upstream_error", ids.Request)
				return
			}
			responseBody = collapsed
		}
		responseBody = wire.RenameResponseTool(responseBody, upstreamRoute.Protocol, toolRenames)
		if usage, reported := wire.ResponseUsage(upstreamRoute.Protocol, responseBody); meta != nil {
			meta.Usage, meta.UsageReported = usage, reported
		}
		if external != upstreamRoute.Protocol {
			responseBody, err = wire.ConvertResponse(upstreamRoute.Protocol, external, responseBody)
			if err != nil {
				g.logger.Warn("response protocol conversion failed", "component", "conversion", "event", "response_conversion_failed", "request_id", ids.Request, "model", model, "source_protocol", upstreamRoute.Protocol, "target_protocol", external, "error", err)
				wire.WriteError(w, external, http.StatusBadGateway, "unsupported upstream response", "upstream_error", ids.Request)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(responseBody)
	}
}

// handleSystemOne serves System One decision requests on their own endpoint.
// Only models whose upstream protocol is System One can be served here; a chat
// or responses model belongs on its own endpoint, where the bridge can convert
// it.
func (g *Gateway) handleSystemOne(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err != nil {
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, "request body is too large or unreadable", "invalid_request_error", "")
		return
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, "request body must be a JSON object", "invalid_request_error", "")
		return
	}
	model := jsonutil.StringAt(payload, "model")
	if meta := telemetry.MetaFromRequest(r); meta != nil {
		meta.Model = model
		// Same client-shape recording as handleInference (see gateway.go), so
		// the usage page's 推理强度 / 流式 columns are not blank for traffic
		// that arrives on this endpoint instead of the three message ones.
		meta.ReasoningEffort = wire.ReasoningEffortLabel(wire.SystemOne, payload)
		meta.Stream = jsonutil.BoolAt(payload, "stream")
	}
	// Derived before routing so the client-visible rejections below carry a
	// real x-request-id instead of the literal "model" — the same fix
	// handleInference got, applied here too. Scoped to the calling server key
	// so two callers replaying one client session do not share an upstream
	// session, and derived only once for the same reason.
	ids := identity.DeriveRequestIDs(r, payload, callerScope(r.Context()))
	if meta := telemetry.MetaFromRequest(r); meta != nil {
		meta.Request = ids.Request
		meta.Session = ids.Session
	}
	if model == "" {
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, "model is required", "invalid_request_error", ids.Request)
		return
	}
	if !g.catalog.Supported(model) {
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, "the model uses an upstream protocol that opencode2api does not expose", "invalid_request_error", ids.Request)
		return
	}
	route, err := g.catalog.Route(model, len(g.cfg.ZenKeys) > 0, len(g.cfg.GoKeys) > 0, g.cfg.Anonymous)
	if err != nil {
		if errors.Is(err, models.ErrModelDisabled) {
			writeModelUnavailable(w, wire.SystemOne, err, ids.Request)
			return
		}
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, err.Error(), "invalid_request_error", ids.Request)
		return
	}
	if route.Protocol != wire.SystemOne {
		wire.WriteError(w, wire.SystemOne, http.StatusBadRequest, fmt.Sprintf("the model does not use the %s protocol", wire.SystemOne), "invalid_request_error", ids.Request)
		return
	}
	g.forwardSystemOne(w, r, body, payload, model, route)
}

// forwardSystemOne relays a System One decision payload verbatim to the
// upstream systemone endpoint and returns the typed answer document unchanged.
// The payload pairs a free-form state with typed questions, which no
// message-shaped upstream protocol accepts, so it is never translated. Answers
// are non-streaming today; a streaming upstream reply is still relayed.
func (g *Gateway) forwardSystemOne(w http.ResponseWriter, r *http.Request, body []byte, payload map[string]any, model string, route models.Route) {
	meta := telemetry.MetaFromRequest(r)
	if meta != nil {
		meta.Model = model
		meta.Tier = string(route.Tier)
		meta.Protocol = route.Protocol
	}
	// One verbatim body serves every tier: a decision payload has no per-tier
	// encoding, so no protocol conversion is attempted.
	bodies := make(map[config.Tier][]byte, len(route.KeyTiers)+1)
	bodies[route.Tier] = body
	for _, tier := range route.KeyTiers {
		bodies[tier] = body
	}
	// Same cooling scope as handleInference; for System One the decisive
	// fields are state+questions, which RequestFingerprint hashes. Derived
	// here rather than reused from handleSystemOne: this is a separate entry
	// point (the message endpoints route a System One model straight here)
	// and it needs the ids as values, not just for the error paths above.
	ids := identity.DeriveRequestIDs(r, payload, callerScope(r.Context()))
	if meta != nil {
		meta.Request = ids.Request
		meta.Session = ids.Session
	}
	fingerprint := identity.RequestFingerprint(payload)
	requestCtx, cancel := context.WithTimeout(r.Context(), time.Duration(g.cfg.Retry.TimeoutSeconds)*time.Second)
	defer cancel()
	resp, upstreamRoute, err := g.doUpstream(requestCtx, route, bodies, ids, fingerprint)
	if err != nil {
		finalTier := route.Tier
		if meta != nil && meta.Tier != "" {
			finalTier = config.Tier(meta.Tier)
		}
		keyID, channel, anonymous := requestCredential(requestCtx)
		if !errors.Is(err, context.DeadlineExceeded) && wire.ClientCanceled(r.Context(), err) {
			g.logger.Debug("client canceled before upstream response", "component", "upstream", "event", "request_canceled", "request_id", ids.Request, "tier", finalTier, "key_id", keyID, "channel", channel, "anonymous", anonymous)
			if meta != nil {
				meta.Outcome = "client_canceled"
			}
			wire.WriteError(w, wire.SystemOne, statusClientClosedRequest, "client closed request", "client_canceled", ids.Request)
			return
		}
		// Same ordering as handleInference: an exhausted budget is a timeout, and it
		// must be decided before the cooldown hook can answer 429 for a lane
		// that is actually fine, and before the proxy check for the same reason.
		if errors.Is(err, context.DeadlineExceeded) {
			wire.WriteError(w, wire.SystemOne, http.StatusGatewayTimeout, "upstream request timed out", "upstream_timeout", ids.Request)
			return
		}
		if g.writeCoolingResponse(w, wire.SystemOne, err, meta, ids.Request, string(finalTier)) {
			return
		}
		if errors.Is(err, errProxyUnavailable) {
			g.logger.Warn("all upstream attempts failed", "component", "upstream", "event", "request_failed", "request_id", ids.Request, "tier", finalTier, "key_id", keyID, "channel", channel, "anonymous", anonymous, "error", err)
			wire.WriteError(w, wire.SystemOne, http.StatusServiceUnavailable, proxyUnavailableMessage, "proxy_error", ids.Request)
			return
		}
		if errors.Is(err, errWARPUnavailable) {
			g.logger.Warn("all upstream attempts failed", "component", "upstream", "event", "request_failed", "request_id", ids.Request, "tier", finalTier, "key_id", keyID, "channel", channel, "anonymous", anonymous, "error", err)
			wire.WriteError(w, wire.SystemOne, http.StatusServiceUnavailable, warpUnavailableMessage, "warp_unavailable", ids.Request)
			return
		}
		// The two lane-level verdicts the inference path already reports. They
		// used to fall through to 502 upstream_error here, which reads as "the
		// upstream is broken" and sends the failover script switching nodes:
		// wrong for a paused lane (nothing to switch to) and wrong for an IP
		// block (the block is in the log, the node is fine). Same codes, same
		// messages as handleInference so a client sees one vocabulary.
		if errors.Is(err, errFreeTierIPBlocked) {
			wire.WriteError(w, wire.SystemOne, http.StatusServiceUnavailable, freeTierBlockedMessage, "free_tier_ip_blocked", ids.Request)
			return
		}
		if errors.Is(err, errAnonymousLanePaused) {
			wire.WriteError(w, wire.SystemOne, http.StatusServiceUnavailable, anonymousLanePausedMessage, "free_tier_lane_paused", ids.Request)
			return
		}
		g.logger.Warn("all upstream attempts failed", "component", "upstream", "event", "request_failed", "request_id", ids.Request, "tier", finalTier, "key_id", keyID, "channel", channel, "anonymous", anonymous, "error", err)
		wire.WriteError(w, wire.SystemOne, http.StatusBadGateway, "all upstream attempts failed", "upstream_error", ids.Request)
		return
	}
	defer resp.Body.Close()
	if meta != nil {
		meta.Tier = string(upstreamRoute.Tier)
		meta.Protocol = upstreamRoute.Protocol
	}
	w.Header().Set("x-request-id", ids.Request)
	if resp.StatusCode/100 != 2 {
		copyErrorResponse(w, wire.SystemOne, resp, ids.Request)
		return
	}
	if contentType := resp.Header.Get("Content-Type"); strings.HasPrefix(contentType, "text/event-stream") {
		if meta != nil {
			// Upstream decided the response is an event stream even if the
			// client did not ask for one; the client really does receive a
			// stream, so this is the accurate shape to record.
			meta.Stream = true
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		wire.WriteError(w, wire.SystemOne, http.StatusBadGateway, "failed to read upstream response", "upstream_error", ids.Request)
		return
	}
	if usage, reported := wire.ResponseUsage(upstreamRoute.Protocol, responseBody); meta != nil {
		meta.Usage, meta.UsageReported = usage, reported
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(responseBody)
}

// prepareRouteBodies encodes input once for each key tier the route may use,
// the preferred tier first and then its KeyTiers, in that tier's upstream
// protocol and with any forced reasoning effort applied. A fallback tier the
// request cannot be converted for is skipped; only a conversion failure on
// the preferred tier is an error.
//
// raw is the exact client body the payload was decoded from; it lets a
// same-protocol primary tier skip the re-encode. nil disables the shortcut.
func (g *Gateway) prepareRouteBodies(from wire.Protocol, route models.Route, input map[string]any, raw []byte) (map[config.Tier][]byte, error) {
	tiers := make([]config.Tier, 0, len(route.KeyTiers)+1)
	seen := make(map[config.Tier]bool, len(route.KeyTiers)+1)
	addTier := func(tier config.Tier) {
		if tier != config.TierZen && tier != config.TierGo || seen[tier] {
			return
		}
		seen[tier] = true
		tiers = append(tiers, tier)
	}
	addTier(route.Tier)
	for _, tier := range route.KeyTiers {
		addTier(tier)
	}
	if len(tiers) == 0 {
		return nil, errors.New("no usable upstream tier")
	}
	bodies := make(map[config.Tier][]byte, len(tiers))
	// rawReuse is spent by the first tier that can safely take the client's
	// bytes verbatim. After that, a repair may have mutated input in place and
	// the original bytes no longer describe it, so later tiers re-encode.
	rawReuse := len(raw) > 0
	skipTier := func(tier config.Tier, protocol wire.Protocol, err error) bool {
		if tier == route.Tier {
			return false
		}
		// A fallback tier may use a stricter wire format than the preferred
		// tier. Do not reject a request before the preferred upstream has even
		// been tried; that tier is attempted only if the request actually
		// falls back.
		g.logger.Debug("skipping fallback tier with incompatible request shape", "component", "conversion", "event", "fallback_tier_skipped", "model", jsonutil.StringAt(input, "model"), "tier", tier, "protocol", protocol, "error", err)
		return true
	}
	for _, tier := range tiers {
		protocol := route.ProtocolFor(tier)
		baseURL := g.cfg.Upstream.Zen
		if tier == config.TierGo {
			baseURL = g.cfg.Upstream.Go
		}
		effort := g.resolvedEffort(jsonutil.StringAt(input, "model"), tier, protocol)
		// Zero-copy is only valid when nothing downstream would rewrite the
		// body: a forced effort mutates the payload, and any other tier than
		// the one holding raw must not claim those bytes.
		if rawReuse && effort == "" && tier == route.Tier {
			encoded, err := wire.PrepareRequestBytes(from, protocol, input, raw, baseURL)
			if err != nil {
				if skipTier(tier, protocol, err) {
					continue
				}
				return nil, fmt.Errorf("prepare %s upstream request: %w", tier, err)
			}
			rawReuse = false
			bodies[tier] = encoded
			continue
		}
		upstreamPayload, err := wire.PrepareRequest(from, protocol, input, baseURL)
		if err != nil {
			if skipTier(tier, protocol, err) {
				continue
			}
			return nil, fmt.Errorf("prepare %s upstream request: %w", tier, err)
		}
		if effort != "" {
			wire.ForcedEffort(protocol, upstreamPayload, effort)
		}
		encoded, err := jsonutil.Marshal(upstreamPayload)
		if err != nil {
			return nil, errors.New("request contains unsupported JSON values")
		}
		bodies[tier] = encoded
	}
	return bodies, nil
}

// copyErrorResponse relays a non-2xx upstream reply as an error in the
// client's protocol. It keeps the status and any Retry-After, and takes the
// message from a JSON body's error.message or message, falling back to the
// status text.
func copyErrorResponse(w http.ResponseWriter, protocol wire.Protocol, resp *http.Response, requestID string) {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	message := http.StatusText(resp.StatusCode)
	var value map[string]any
	upstreamType := ""
	if json.Unmarshal(body, &value) == nil {
		message = jsonutil.FirstString(jsonutil.StringAt(value, "error", "message"), jsonutil.StringAt(value, "message"), message)
		// Preserve the upstream error class (only the nested error.type; the
		// top-level "type" is just the envelope). Zen's 429 kinds share
		// identical text, so without this FreeUsageLimitError vs
		// RateLimitError vs BlackUsageLimitError are indistinguishable
		// downstream.
		upstreamType = jsonutil.StringAt(value, "error", "type")
	}
	if upstreamType != "" && !strings.HasPrefix(message, upstreamType) {
		message = upstreamType + ": " + message
	}
	// Neutralize a forged gateway prefix: upstream error text may echo
	// client-controlled input, and a leading "opencode2api:" would make a
	// passthrough indistinguishable from a gateway-originated error for
	// prefix-based classifiers (dsh; the Anthropic shape has no code field as
	// a second factor). Every occurrence matters: watchdog-clash.ps1 decides
	// "the gateway is backing off" with a substring match ($body.Contains), so
	// neutralizing only a leading hit would still let a forged prefix mid-body
	// stop the failover script. The colon is replaced so the text stays
	// readable but never matches.
	message = neutralizeForgedGatewayPrefix(message)
	kind := "upstream_error"
	if protocol == wire.Anthropic && upstreamType != "" {
		// The Anthropic shape has no code field, so error.type is the only
		// machine-readable slot: carry the upstream class there instead of
		// flattening every passthrough to "upstream_error". Gateway errors
		// never pass through here, so a structured upstream class cannot be
		// mistaken for a gateway verdict.
		kind = sanitizeUpstreamErrorType(upstreamType)
	}
	wire.WriteError(w, protocol, resp.StatusCode, message, kind, requestID)
}

// writeModelUnavailable maps a routing failure caused by a confirmed
// availability disable onto 503. It is a server-side condition, not a
// malformed request: answering 400 made clients (and dsh) read a temporarily
// disabled model as a bad request and never retry or fall back. Every entry
// point that can hit this error shares this mapping so the two cannot drift
// apart again.
func writeModelUnavailable(w http.ResponseWriter, protocol wire.Protocol, err error, requestID string) {
	detail := strings.TrimPrefix(err.Error(), models.ErrModelDisabled.Error()+": ")
	wire.WriteError(w, protocol, http.StatusServiceUnavailable,
		"opencode2api: **[gateway]** **ModelUnavailable** "+detail, "model_unavailable", requestID)
}

// neutralizeForgedGatewayPrefix defuses every "opencode2api:" occurrence in a
// passthrough message so forged upstream text cannot pose as a
// gateway-originated error. Upstream error text echoes client input, and the
// consumer is a substring test, not a prefix test: watchdog-clash.ps1 decides
// "the gateway is backing off" with $body.Contains('opencode2api:'). Defusing
// only a leading occurrence would leave "invalid_request_error: opencode2api:…"
// intact, because the upstream class is prepended above — and the script would
// then read a genuine upstream 429 as a local cooldown and stop switching
// nodes, which is the one recovery the free tier has. The colon becomes a
// semicolon, so the text stays readable and the token no longer matches.
func neutralizeForgedGatewayPrefix(message string) string {
	const prefix = "opencode2api:"
	if !strings.Contains(message, prefix) {
		return message
	}
	return strings.ReplaceAll(message, prefix, "opencode2api;")
}

// sanitizeUpstreamErrorType keeps an upstream error class safe for the
// structured Anthropic error.type slot: trimmed, length-bounded, and never
// allowed to impersonate the gateway's own verdict namespace.
func sanitizeUpstreamErrorType(upstreamType string) string {
	trimmed := strings.TrimSpace(upstreamType)
	if trimmed == "" || strings.HasPrefix(strings.ToLower(trimmed), "opencode2api") {
		return "upstream_error"
	}
	if len([]rune(trimmed)) > 128 {
		trimmed = string([]rune(trimmed)[:128])
	}
	return trimmed
}

// isUpstreamSSE reports whether an upstream content type is an SSE stream.
// It parses the media type strictly so a JSON body that merely mentions the
// token in a parameter cannot be misread as a stream.
func isUpstreamSSE(contentType string) bool {
	if mediatype, _, err := mime.ParseMediaType(contentType); err == nil {
		return strings.ToLower(mediatype) == "text/event-stream"
	}
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "text/event-stream")
}

// looksLikeSSE is the belt to isUpstreamSSE's braces. The content type alone
// is not enough to trust: a mislabeled SSE body (Content-Type left at
// application/json) would otherwise skip the collapse and be written to the
// client verbatim as application/json, which the client cannot parse — a worse
// failure than the 502 the gate was added to prevent. Body sniffing only ever
// adds a collapse, and collapsing something that was not a stream still fails
// loudly in the same place as before.
func looksLikeSSE(body []byte) bool {
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	return bytes.HasPrefix(trimmed, []byte("data:")) || bytes.HasPrefix(trimmed, []byte("event:"))
}
