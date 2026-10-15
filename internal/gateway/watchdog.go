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
	"opencode2api/internal/httpx"
	"opencode2api/internal/jsonutil"
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
	if typ := upstreamErrorType(resp); typ != "" {
		return fmt.Sprintf("%s (upstream HTTP %d)", typ, status)
	}
	if err != nil {
		// A transport failure that still carries a response: keep the
		// transport prefix (it outranks the status in the classifier).
		return fmt.Sprintf("transport (%s) (upstream HTTP %d)", transportCauseShort(err), status)
	}
	// No error class in the body (empty, non-JSON, unreadable, oversized):
	// the status tail still has to be parenthesized, because that is where
	// classifyAnonymousVerdict reads it. A bare "upstream HTTP 502" would parse
	// as no status at all and degrade a server-side wobble into "unknown
	// cause, just wait" instead of the transport verdict it is.
	return fmt.Sprintf("upstream error (upstream HTTP %d)", status)
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

// maxCausePeek bounds the error-body peek behind upstreamErrorType. Console
// error payloads are small JSON; a huge body only contributes its status code
// to the cause while the intact original still flows downstream.
const maxCausePeek = 256 << 10

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
	data, readErr := io.ReadAll(io.LimitReader(original, maxCausePeek+1))
	if len(data) > maxCausePeek || readErr != nil {
		// Oversized or truncated peek: only the status is usable, so chain
		// what was read back in front of the still-open original. A plain
		// NopCloser here would either drop the remainder (truncating a
		// passthrough) or swallow the network body (forfeiting connection
		// reuse) — a prefixedBody propagates Close to the real body.
		resp.Body = &prefixedBody{Reader: io.MultiReader(bytes.NewReader(data), original), body: original}
		return ""
	}
	_ = original.Close()
	resp.Body = io.NopCloser(bytes.NewReader(data))
	var value map[string]any
	if json.Unmarshal(data, &value) != nil {
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
