package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"opencode2api/internal/buildinfo"
	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"opencode2api/internal/identity"
	"opencode2api/internal/jsonutil"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/telemetry"
)

// Local: watchdog (read-only Clash decision feed).
// This file owns GET /v1/watchdog/status, the verdict vocabulary it classifies,
// and the cause label its producer writes (watchdogCause). gateway.go keeps
// only the route registration hook; pool.go keeps the per-node cooldown state
// (cause + success/failure moments) the feed reads.
// The feed itself never writes pool state: every read is a snapshot.

// watchdogStatusResponse is the read-only decision feed for the external
// Clash watchdog script (server-key auth, same as /v1/models). It exposes
// only already-redacted values (proxy URLs stay redacted, keys stay hidden)
// plus a classified anonymous-lane verdict so the script can act without
// spending a probe request: transport failures and quota exhaustion need a
// node switch, plain rate limiting only needs waiting.
type watchdogStatusResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
	Proxies struct {
		Total     int  `json:"total"`
		Healthy   int  `json:"healthy"`
		Unhealthy int  `json:"unhealthy"`
		AllDown   bool `json:"all_down"`
	} `json:"proxies"`
	Anonymous struct {
		Enabled        bool   `json:"enabled"`
		CoolingSeconds int64  `json:"cooling_seconds,omitempty"`
		Cause          string `json:"cause,omitempty"`
		// Verdict classifies the current anonymous lane state for the
		// watchdog: "ok" (no action), "transport" (proxy broken, switch),
		// "quota" (free quota exhausted, switch), "ratelimit" (plain
		// throttling, only wait), "fingerprint" (upstream rejected the
		// request identity itself — switching nodes cannot help, see
		// AGENTS.md on the FreeTierError gate), "cooling" (cooling with
		// unknown cause).
		// The verdict derives from a worst-case snapshot across all
		// transport-healthy proxies, not from the next turn's own request:
		// one residual cooldown can recommend a switch the next turn does
		// not need, so the script should confirm twice before switching.
		Verdict string `json:"verdict"`
		// LastSuccessAgeS is how long ago the last user-traffic anonymous
		// success landed (omitted when none recorded this process). It is
		// the positive recovery proof: a verdict flip to ok with an old
		// age means "cooldown expired, not yet proven", not "recovered".
		// Background catalog refresh never advances it. Likewise
		// LastFailureAgeS marks the last user-traffic MarkFailure.
		LastSuccessAgeS *int64 `json:"last_success_age_s,omitempty"`
		LastFailureAgeS *int64 `json:"last_failure_age_s,omitempty"`
	} `json:"anonymous"`
	// RecommendSwitch is true for verdicts transport/quota, or when every
	// proxy is down (AllDown). Plain ratelimit/fingerprint/cooling/ok never
	// recommends: a fingerprint rejection is upstream's answer about the
	// request, and no exit node changes it.
	RecommendSwitch bool `json:"recommend_switch"`
}

func (g *Gateway) handleWatchdogStatus(w http.ResponseWriter, _ *http.Request) {
	proxyTotal, proxyHealthy := g.transports.healthCounts()
	resp := watchdogStatusResponse{Status: "ok", Version: buildinfo.Version}
	resp.Proxies.Total = proxyTotal
	resp.Proxies.Healthy = proxyHealthy
	resp.Proxies.Unhealthy = proxyTotal - proxyHealthy
	resp.Proxies.AllDown = proxyTotal > 0 && proxyHealthy == 0
	resp.Anonymous.Enabled = g.cfg.Anonymous
	// Single snapshot: seconds and cause must belong to the same cooldown.
	remain, cause := g.anonymous.maxCooldownSnapshot()
	if remain > 0 {
		resp.Anonymous.CoolingSeconds = int64(math.Ceil(remain.Seconds()))
	} else if cause == "" {
		// No active cooldown. A stale transport cause inside its TTL keeps
		// voting transport (cooling 0): expiry without any new success is
		// "no evidence", never "recovered". Non-transport causes are
		// intentionally dropped here (see staleCauseSnapshot) so an old
		// quota/ratelimit cannot hold recommend_switch or park the script.
		cause = g.anonymous.staleCauseSnapshot()
	}
	resp.Anonymous.Cause = cause
	resp.Anonymous.Verdict = classifyAnonymousVerdict(resp.Anonymous.Cause, resp.Anonymous.CoolingSeconds > 0)
	// Success/failure ages are positive evidence for the script's recovery
	// log gate. Read-only snapshot, never extends any deadline.
	if successAge, failureAge, successOK, failureOK := g.anonymous.successFailureAges(); successOK || failureOK {
		if successOK {
			secs := int64(successAge / time.Second)
			resp.Anonymous.LastSuccessAgeS = &secs
		}
		if failureOK {
			secs := int64(failureAge / time.Second)
			resp.Anonymous.LastFailureAgeS = &secs
		}
	}
	switch resp.Anonymous.Verdict {
	case "transport", "quota":
		resp.RecommendSwitch = true
	}
	if resp.Proxies.AllDown {
		resp.Status = "degraded"
		resp.RecommendSwitch = true
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// Local: the watchdog's forced probe. The status feed above answers "what does
// the gateway remember"; this answers "what does THIS exit node do right now",
// which is the only question worth asking after the operator switched nodes.
//
// It exists because the ordinary path cannot answer it. A request that finds no
// transport-healthy proxy, or one sitting in a cooldown, never reaches the
// upstream: it comes back as a locally synthesized 502 ("all upstream attempts
// failed", see anonymous_errors.go for why that is not a 429) carrying no
// information about the node — and the watchdog reads 502 as "this node is
// broken, switch", so a stale transport-health mark left over from a dial that
// timed out while Clash was re-routing made it burn its whole candidate pool
// against a gate that no node switch can clear.

// watchdogProbeResponse is the result of one forced anonymous attempt. It is a
// measurement, not a snapshot: attempted says whether anything was actually
// transmitted, and every other field describes that attempt.
//
// The upstream's error document is deliberately not echoed. The watchdog acts on
// the verdict; returning the body would re-couple the script to error text that
// the gateway rewrites on the client-facing passthrough path (see
// neutralizeForgedGatewayPrefix and passthrough_guards_test.go), and the probe
// runs over the network like the rest of this feed.
type watchdogProbeResponse struct {
	// Probe names the lane that was measured. Always "anonymous": the watchdog
	// switches exit nodes to escape the free tier's per-IP allowance, so a
	// probe answered by a paid key would be answering a question nobody asked.
	Probe string `json:"probe"`
	Model string `json:"model,omitempty"`
	// Attempted is false only when no request could be built or sent (no
	// anonymous proxy configured, model not served by the anonymous lane,
	// request construction failed). Those are gateway-side states and always
	// carry verdict "config"; they must never be reported as a node verdict.
	Attempted bool `json:"attempted"`
	// Status is the upstream HTTP status, or 0 when the attempt died in
	// transport (in which case Transport carries the short reason).
	Status int `json:"status"`
	// Verdict uses the same vocabulary as the status feed (see
	// classifyAnonymousVerdict), plus "config" for states no node switch can
	// change and "cooling" only if the forced path itself short-circuited,
	// which is a bug rather than an outcome.
	Verdict    string `json:"verdict"`
	Cause      string `json:"cause,omitempty"`
	Transport  string `json:"transport,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}

const (
	// maxWatchdogProbeBody bounds the probe request itself: the payload is one
	// short ping, so anything larger is a mistake worth rejecting.
	maxWatchdogProbeBody = 8 << 10
	// watchdogProbeTimeout bounds the single attempt. Same budget
	// probeAttempt gives the availability probe: long enough for a cold dial
	// on a freshly switched node, short enough that the script's own
	// -VerifyTimeoutSeconds is not the only thing bounding a dead node.
	watchdogProbeTimeout = 60 * time.Second
)

// handleWatchdogProbe serves POST /v1/watchdog/probe (server-key auth, like
// the status feed). It always answers 200 once a verdict exists, including for
// a failed upstream: the HTTP status would otherwise encode a second, coarser
// opinion about the same answer, and the script would be back to parsing bodies.
func (g *Gateway) handleWatchdogProbe(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWatchdogProbeBody))
	if err != nil {
		wire.WriteError(w, wire.Chat, http.StatusBadRequest, "probe body is too large or unreadable", "invalid_request_error", "")
		return
	}
	var input struct {
		Model string `json:"model"`
	}
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			wire.WriteError(w, wire.Chat, http.StatusBadRequest, "probe body must be a JSON object", "invalid_request_error", "")
			return
		}
	}
	model := strings.TrimSpace(input.Model)
	if model == "" {
		wire.WriteError(w, wire.Chat, http.StatusBadRequest, "model is required", "invalid_request_error", "")
		return
	}
	// Diagnostic context: a probe is not user traffic. The path below calls the
	// upstream client directly and writes no pool state, so this is belt and
	// braces against a future refactor routing probes through doUpstream.
	result := g.runWatchdogProbe(WithDiagnosticRequest(r.Context()), model)
	// A finished probe answers 200 whatever the upstream said, so the monitor's
	// success/error split has to come from the verdict rather than the status.
	// Model and Tier stay unset on purpose: telemetry.Record only files a
	// request into the usage layer (cost, tokens, the 24h detail ring) when a
	// tier is present, which keeps probes off the usage page while leaving them
	// visible as requests with a truthful outcome.
	if meta := telemetry.MetaFromRequest(r); meta != nil && result.Verdict != "ok" {
		meta.Outcome = "watchdog_probe_" + result.Verdict
	}
	httpx.WriteJSON(w, http.StatusOK, result)
}

// runWatchdogProbe sends exactly one request down the anonymous lane, ignoring
// the transport-health gate and every cooldown (anonymousCursor.NextProbe), and
// classifies the answer with the status feed's own vocabulary, so the two can
// never drift apart.
//
// It writes no pool state whatsoever: no cooldown, no health verdict, no
// lastSuccess/lastFailure, no stale-reasoning memo, and it never triggers a
// background proxy check. The node is left exactly as the pool found it, which
// is what makes the bypass free — and what keeps the feed's "user traffic only"
// contract intact, so a probe can never fake the recovery evidence the script
// waits for.
func (g *Gateway) runWatchdogProbe(ctx context.Context, model string) watchdogProbeResponse {
	result := watchdogProbeResponse{Probe: "anonymous", Model: model}
	proxyName := ""
	// One Info line per probe, including the config answers that never leave the
	// process. The status feed records nothing for a probe by design, and
	// telemetry/http.go only logs "request routed" once a channel is set, so
	// without this the operator's own node switching is only visible in the
	// script log. Nothing sensitive is echoed: the proxy URL is redacted and the
	// cause is the upstream's own error class, already bounded to 64 runes.
	defer func() {
		g.logger.Info("watchdog probe finished", "component", "upstream", "event", "watchdog_probe",
			"model", model, "verdict", result.Verdict, "status", result.Status,
			"attempted", result.Attempted, "duration_ms", result.DurationMS,
			"proxy", proxyName, "cause", result.Cause, "transport", result.Transport)
	}()
	if !g.cfg.Anonymous {
		result.Verdict = "config"
		result.Cause = "anonymous lane is disabled"
		return result
	}
	// Only models the anonymous lane actually serves are probeable. A paid
	// model, a disabled one, or an unknown one is gateway-side state: report it
	// as config instead of spending an attempt that could not be routed.
	route, err := g.catalog.Route(model, len(g.cfg.ZenKeys) > 0, len(g.cfg.GoKeys) > 0, g.cfg.Anonymous)
	if err != nil {
		result.Verdict = "config"
		result.Cause = err.Error()
		return result
	}
	if !route.Anonymous {
		result.Verdict = "config"
		result.Cause = "model is not served by the anonymous lane"
		return result
	}
	protocol := route.ProtocolFor(config.TierZen)
	// probePayload routes the body through prepareAnonymousBody, so the probe
	// carries the forced stream:true and the genuine core tool definitions —
	// the same agent shape real anonymous traffic sends, which is what the free
	// tier's gate judges.
	payload, err := probePayload(model, protocol, "")
	if err != nil {
		result.Verdict = "config"
		result.Cause = "probe request could not be built"
		return result
	}
	// Round-robin across transports (empty affinity) rather than pinning one per
	// model: with several proxies a permanently broken one would otherwise own
	// every probe and poison the verdict forever. The usual single-Clash-proxy
	// setup is unaffected — there is only one node to pick.
	//
	// CursorFor returns a value and NextProbe has a pointer receiver, so the
	// cursor needs a name (same reason as nextFor in the cooling tests).
	cursor := g.anonymous.CursorFor("")
	node := cursor.NextProbe()
	if node == nil {
		result.Verdict = "config"
		result.Cause = "no anonymous proxy is configured"
		return result
	}
	proxyName = config.RedactURL(node.proxy.name)
	ids := identity.RequestIDs{
		Session: identity.CanonicalSessionID("watchdog:" + model),
		Project: "opencode2api:default-project",
		Request: identity.RandomID("probe", 16),
	}
	ctx, cancel := context.WithTimeout(ctx, watchdogProbeTimeout)
	defer cancel()
	req, err := newUpstreamRequest(ctx, g.cfg.Upstream.Anonymous, protocol, payload, ids, anonymousZenKey)
	if err != nil {
		result.Verdict = "config"
		result.Cause = "probe request could not be created"
		return result
	}
	started := time.Now()
	resp, err := node.proxy.client.Do(req)
	result.Attempted = true
	result.DurationMS = time.Since(started).Milliseconds()
	if err != nil {
		if resp != nil {
			httpx.DrainAndClose(resp.Body)
		}
		result.Transport = transportCauseShort(err)
		result.Cause = watchdogCause(nil, err)
		result.Verdict = classifyAnonymousVerdict(result.Cause, false)
		return result
	}
	result.Status = resp.StatusCode
	if resp.StatusCode/100 == 2 {
		// The free lane answers as a stream; all the watchdog needs to know is
		// that this node was willing to serve it.
		httpx.DrainAndClose(resp.Body)
		result.Verdict = "ok"
		return result
	}
	// Same peeking path an ordinary failure takes, guards included: the
	// isEventStream and known-length bounds are exactly what keep a non-2xx event
	// stream or a multi-megabyte WAF page from being read to the bound.
	// upstreamErrorType puts the bytes back into resp.Body, so the drain below is
	// what actually closes the connection.
	result.Cause = watchdogCauseLabel(upstreamErrorType(resp), result.Status, nil)
	httpx.DrainAndClose(resp.Body)
	result.Verdict = classifyAnonymousVerdict(result.Cause, false)
	if result.Verdict == "ok" {
		result.Verdict = probeFallbackVerdict(result.Status)
	}
	return result
}

// probeFallbackVerdict classifies the non-2xx statuses whose cause carries no
// class at all (empty, non-JSON, or oversized error body), where
// classifyAnonymousVerdict can only answer "ok" — which for a probe would read
// as "this node is fine" about a request the upstream just refused.
//
// For a classless body the table follows watchdog-clash.ps1's classification of
// the very statuses: 401/400/404 stop the round as gateway config, 429 waits,
// everything else moves on to the next candidate. Two deliberate differences
// remain, and they only ever make the script try MORE candidates, never fewer:
//
//   - 403 is fingerprint, not config. The gateway never authors a 403 (its own
//     rejections are 401/400/429/503-ModelUnavailable/499/502/504), so a 403 seen
//     by a probe came from the upstream: the FreeTierError gate, or an edge/WAF
//     challenge page on that exit IP. Legacy mapped it to config, which made
//     Invoke-Failover break out of the candidate loop, roll back to the original
//     node and sleep — one challenged exit IP could void a whole round. The
//     script already treats a FreeTier 403 that way (fingerprint: log it, try the
//     next candidate), so this only removes the difference between the two kinds
//     of upstream 403.
//   - where a status and a class disagree (400/403 carrying a quota class), the
//     class wins here and the status won in legacy: the probe reports quota and
//     keeps switching, where legacy stopped the round. That is the point of the
//     change — a burned per-IP allowance is fixed by changing exit IP.
//
// What the table does replace is the one case legacy could not classify at all:
// a synthesized 502 with attempts=0, where nothing was ever sent.
func probeFallbackVerdict(status int) string {
	if status == http.StatusTooManyRequests {
		return "ratelimit"
	}
	switch status {
	case http.StatusUnauthorized, http.StatusBadRequest, http.StatusNotFound:
		return "config"
	case http.StatusForbidden:
		return "fingerprint"
	}
	return "transport"
}

// classifyAnonymousVerdict maps the stored anonymous cooldown cause (see
// watchdogCause, e.g. "FreeUsageLimitError (upstream HTTP 429)" or
// "transport (timeout)") to the watchdog action class. Quota-like upstream
// error classes mean the exit IP burned its free allowance (switch helps);
// bare transport causes mean the route is broken (switch helps); plain rate
// limiting only needs waiting (switch does not help).
// The trailing "(upstream HTTP N)" status outranks keyword matching: a 5xx
// is server-side wobble, never quota proof — e.g. "FreeUsageLimitError
// (upstream HTTP 503)" classifies as transport, not quota, so one flapping
// upstream cannot drive a node switch on a quota pretext.
func classifyAnonymousVerdict(cause string, cooling bool) string {
	if cause == "" {
		if cooling {
			return "cooling"
		}
		return "ok"
	}
	lower := strings.ToLower(cause)
	// Transport first: "transport (timeout)" must not be misread by the
	// generic "upstream HTTP" quota/ratelimit checks below.
	if strings.HasPrefix(lower, "transport") {
		return "transport"
	}
	if status := upstreamStatusFromCause(cause); status >= 500 {
		return "transport"
	}
	// FreeTierError is upstream refusing the request itself, not the exit IP
	// and not the proxy: the free lane demands the official client's identity,
	// and no node switch can satisfy it. It is reported as its own verdict so
	// the operator sees the real reason (and so a "wait for the cooldown to
	// expire" reading never hides it), while still never recommending a switch.
	if strings.Contains(lower, "freetier") {
		return "fingerprint"
	}
	for _, quota := range []string{"freeusagelimit", "blackusagelimit", "usage_limit", "quota", "exhausted", "insufficient", "credit", "balance"} {
		if strings.Contains(lower, quota) {
			return "quota"
		}
	}
	for _, limited := range []string{"ratelimit", "rate_limit", "rate limit", "too many requests", "throttl"} {
		if strings.Contains(lower, limited) {
			return "ratelimit"
		}
	}
	// Transport with a response attached ("transport (timeout) (upstream HTTP
	// 503)") still carries the "transport" prefix above; this list additionally
	// keeps the fixed short vocabulary recognizable in a cause that lost its
	// prefix. It runs after the quota/ratelimit checks, but a 5xx cause never
	// reaches it: the status check above already claimed server-side wobble as
	// transport regardless of keywords.
	for _, transport := range []string{"no usable route", "timeout", "connection failed"} {
		if strings.Contains(lower, transport) {
			return "transport"
		}
	}
	if cooling {
		return "cooling"
	}
	return "ok"
}

// upstreamStatusFromCause extracts the trailing "(upstream HTTP N)" status
// from a cooldown cause string (see watchdogCause). It returns 0 when the
// cause carries no parseable status.
func upstreamStatusFromCause(cause string) int {
	const marker = "(upstream HTTP "
	i := strings.LastIndex(cause, marker)
	if i < 0 {
		return 0
	}
	rest := cause[i+len(marker):]
	digits := 0
	for digits < len(rest) && rest[digits] >= '0' && rest[digits] <= '9' {
		digits++
	}
	if digits == 0 {
		return 0
	}
	status := 0
	for _, c := range rest[:digits] {
		status = status*10 + int(c-'0')
	}
	return status
}

// watchdogCause labels what failed an anonymous attempt for the watchdog feed.
// It is a label only: the upstream error document keeps reaching the client
// verbatim, and this label is the one thing the feed adds. Two conventions
// matter because classifyAnonymousVerdict parses the result:
//
//   - an upstream answer always ends in "(upstream HTTP N)" — with or without
//     a class name — so the status outranks the class (a 5xx is transport even
//     when it arrives under a quota-looking class, and a classless 5xx does
//     not degrade into "unknown cause, just wait");
//   - a failure that produced no response at all starts with "transport ",
//     so a broken exit route is never reported as an unknown cause.
//
// The class itself is the upstream's own error.type (e.g. "FreeTierError",
// "FreeUsageLimitError", "RateLimitError") rather than a locally rewritten
// name, so the vocabulary follows whatever the upstream calls itself.
func watchdogCause(resp *http.Response, err error) string {
	if resp == nil {
		return "transport (" + transportCauseShort(err) + ")"
	}
	status := resp.StatusCode
	if status/100 == 2 {
		// Not a failure (MarkFailure is never reached with one); kept honest
		// anyway so a future caller cannot label a success as an error.
		return fmt.Sprintf("upstream HTTP %d", status)
	}
	return watchdogCauseLabel(upstreamErrorType(resp), status, err)
}

// watchdogCauseLabel renders the same label from an already-extracted upstream
// error class, so the watchdog probe (which reads the error body itself and has
// no passthrough to preserve) and the ordinary failure path produce
// byte-identical strings and therefore identical verdicts.
func watchdogCauseLabel(typ string, status int, err error) string {
	switch {
	case typ != "":
		return fmt.Sprintf("%s (upstream HTTP %d)", typ, status)
	case err != nil:
		// A transport failure that still carries a response: keep the
		// transport prefix (it outranks the status in the classifier).
		return fmt.Sprintf("transport (%s) (upstream HTTP %d)", transportCauseShort(err), status)
	default:
		// No error class in the body (empty, non-JSON, unreadable, oversized):
		// the status tail still has to be parenthesized, because that is where
		// classifyAnonymousVerdict reads it. A bare "upstream HTTP 502" would
		// parse as no status at all and degrade a server-side wobble into
		// "unknown cause, just wait" instead of the transport verdict it is.
		return fmt.Sprintf("upstream error (upstream HTTP %d)", status)
	}
}

// transportCauseShort classifies a transport failure into the fixed short
// vocabulary the feed exposes ("no usable route", "timeout", "connection
// failed"). Raw error text never reaches the feed: transport errors embed
// proxy URLs with credentials, and this feed is read over the network.
func transportCauseShort(err error) string {
	if err == nil {
		return "no usable route"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return "timeout"
	}
	return "connection failed"
}

// maxCausePeek bounds the error-body peek behind upstreamErrorType. It is
// pinned to httpx.MaxDrainBytes — the budget httpx.DrainAndClose already
// charges the gateway when it abandons a response without looking at it —
// because the label is a net addition to a failure path that has to pay for
// that bounded drain anyway: peeking must not make the switch to the next exit
// node wait longer than the drain did. The two numbers used to be unrelated
// literals, and the drift was silent — shrinking the drain to 8 KB (making the
// peek eight times the budget it rides on) left the whole suite green, which
// is exactly why the peek now names the drain's budget instead of repeating
// its value. It was 256 KB before, which charged four times the drain budget
// on an unknown-length body: the peek read on to the bound while the upstream
// was still sending, so the user waited for bytes the label did not need.
//
// That ceiling buys latency over precision, deliberately. Measured on a real
// socket, labelling a slow chunked error body costs 27.10 µs instead of
// 31.63 ms. The price is paid only by bodies past the bound: when such a body
// declares its length, the ContentLength guard in upstreamErrorType skips the
// peek outright, so the cause degrades to "upstream error (upstream HTTP N)"
// and a 429 whose quota class (FreeUsageLimitError — the free allowance of
// that exit IP is gone) then classifies as "cooling" instead of "quota". That
// is one switch recommendation not made — a missed diagnosis, never a wrong
// one — and it happens for error bodies in the 64 KB–256 KB window that the
// previous 256 KB bound covered (it degraded there too, just not here). A
// console error payload is a small JSON document, so 64 KB stays orders of
// magnitude more than the class field can need.
const maxCausePeek = httpx.MaxDrainBytes

// causePeekChunk is how much upstreamErrorType pulls between two "do I
// already hold a complete error document?" checks. It only trades read calls
// against how much of a slow body is pulled before the first check — the loop
// below stops as soon as the answer is yes, so a small error body costs one
// read of at most this size, not the whole bound.
const causePeekChunk = 4 << 10

// upstreamErrorType returns the upstream error class from a bounded peek of
// an error body, leaving that body readable in full afterwards so the
// passthrough is untouched. It returns "" — reading nothing more than what
// fits the bound and restoring the rest — when the body is absent, already
// consumed, an event stream, oversized, unreadable, not JSON, or carries no
// error.type.
func upstreamErrorType(resp *http.Response) string {
	// Only a plain error response may be peeked: a 2xx has no EOF until the
	// upstream finishes (reading it would buffer a whole stream), and an
	// error response that arrives as an event stream would block the same
	// way. A response whose body is already spent (err != nil, e.g. a
	// redirect error) carries nothing to read.
	if resp.StatusCode/100 == 2 || isEventStream(resp) {
		return ""
	}
	if resp.Body == nil || resp.ContentLength > maxCausePeek {
		return ""
	}
	original := resp.Body
	var prefix bytes.Buffer
	typ, drained := peekCauseType(&prefix, original)
	if drained {
		// EOF with nothing left behind: the passthrough can be served from
		// the peeked bytes alone, so the network body is closed here.
		_ = original.Close()
		resp.Body = io.NopCloser(bytes.NewReader(prefix.Bytes()))
		return typ
	}
	// Incomplete, oversized, truncated or classless peek: only what the
	// classifier still finds in the bytes at hand is usable, so chain what was
	// read back in front of the still-open original. A plain NopCloser here
	// would either drop the remainder (truncating a passthrough) or swallow
	// the network body (forfeiting connection reuse) — a prefixedBody
	// propagates Close to the real body, and it is also the path a peek that
	// stopped early takes, so stopping early never truncates anything.
	resp.Body = &prefixedBody{Reader: io.MultiReader(bytes.NewReader(prefix.Bytes()), original), body: original}
	return typ
}

// peekCauseType appends at most maxCausePeek+1 bytes of r to data and returns
// the upstream error class as soon as what it holds is already a complete
// error document; drained reports that r reached EOF, so nothing is left
// behind in the network body.
//
// The read is chunked with an early exit rather than one io.ReadAll to the
// bound, because an unknown-length body is a stream, not a file: reading on to
// the bound (or to EOF) makes the node switch wait exactly as long as the
// upstream takes to send 64 KB, while the class itself sits at the front of a
// small document. The budget is unchanged otherwise — a body that never
// completes a document still costs the whole bound plus the one byte that
// proves it oversized, and every byte taken out of r is handed back by the
// caller, so the peek stays invisible to the passthrough either way.
func peekCauseType(data *bytes.Buffer, r io.Reader) (typ string, drained bool) {
	limited := io.LimitReader(r, maxCausePeek+1)
	buf := make([]byte, causePeekChunk)
	// One byte past the bound is enough to know the document is oversized; the
	// loop therefore never reads more than maxCausePeek+1 bytes in total.
	for data.Len() <= maxCausePeek {
		n, err := limited.Read(buf)
		if n > 0 {
			data.Write(buf[:n])
			if t := errorTypeFromDocument(data.Bytes()); t != "" {
				return t, errors.Is(err, io.EOF)
			}
		}
		if err != nil {
			// A body that ended (or died) before completing a document: the
			// status alone carries the cause, exactly as before.
			return "", errors.Is(err, io.EOF)
		}
	}
	return "", false
}

// errorTypeFromDocument returns the error class of a complete JSON error
// document, or "" when doc is not a complete document carrying one. A prefix
// that has not parsed yet is not a document: that is what keeps the peek
// reading until the value is whole (or the bound stops it) instead of trusting
// a half-read class.
func errorTypeFromDocument(doc []byte) string {
	// A whole JSON value is }-terminated once trailing whitespace is dropped,
	// and only an object can carry error.type at all. Rejecting the rest up
	// front is what keeps the caller's per-chunk check from re-parsing every
	// partial prefix of a body that never completes.
	if doc = bytes.TrimRight(doc, " \t\r\n"); len(doc) == 0 || doc[len(doc)-1] != '}' {
		return ""
	}
	var value map[string]any
	if json.Unmarshal(doc, &value) != nil {
		return ""
	}
	// Upstream-controlled string: bound it before it lands in the cooldown
	// cause and the watchdog feed.
	typ := jsonutil.StringAt(value, "error", "type")
	if len([]rune(typ)) > 64 {
		typ = string([]rune(typ)[:64])
	}
	return typ
}

func isEventStream(resp *http.Response) bool {
	return strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
}

// prefixedBody chains a peeked prefix back in front of the still-open
// remainder while propagating Close to the underlying network body.
type prefixedBody struct {
	io.Reader
	body io.Closer
}

func (b *prefixedBody) Close() error {
	if b == nil || b.body == nil {
		return nil
	}
	return b.body.Close()
}
