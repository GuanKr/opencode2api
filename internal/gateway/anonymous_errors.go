package gateway

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	wire "opencode2api/internal/protocol"
	"opencode2api/internal/telemetry"
)

// Local: gateway-originated error shapes for the anonymous lane.
// This file owns errAnonymousCooling and its message; upstream.go keeps only
// the call-site hook and gateway.go the client-visible mapping.
//
// errAnonymousCooling means no upstream request was made because every
// anonymous node is inside its cooldown window. It maps to 429, not 502, and
// that distinction is load-bearing:
//
//   - A 502 reads as a broken upstream, so clients retry it immediately. While
//     the single anonymous node is cooling (up to anonymousCooldownCeiling of
//     the configured base, and never longer than maxAnonymousCooldown), that
//     turns one upstream 429 into a burst of 502s that never reach upstream —
//     27 of them landed in one hour on 2026-10-16.
//   - watchdog-clash.ps1 classifies a 502 as transport, i.e. "this node is
//     broken, switch". Since the cooldown is held on the proxy transport rather
//     than on the Clash node, switching nodes cannot shorten it, so the script
//     burned through its whole candidate list against the same cooldown.
//
// The 429 carries Retry-After, so a client backs off for exactly the remaining
// cooldown, and the body keeps the "opencode2api:" prefix that tells a gateway-
// authored error apart from a verbatim upstream one (upstream passthrough
// carries no prefix).
type errAnonymousCooling struct {
	retryAfter time.Duration
	// cause names what triggered the cooldown (the node's coolCause), so even a
	// request that was blocked locally still shows the original upstream reason.
	cause string
}

func (e errAnonymousCooling) Error() string {
	secs := int64(math.Ceil(e.retryAfter.Seconds()))
	if secs < 1 {
		secs = 1
	}
	if e.cause != "" {
		return fmt.Sprintf("anonymous lane cooling, retry in %ds (triggered by %s)", secs, e.cause)
	}
	return fmt.Sprintf("anonymous lane cooling, retry in %ds", secs)
}

// anonymousCoolingMessage renders the client-visible 429. The trigger class
// leads bolded (e.g. **FreeUsageLimitError**) so it is visible at a glance,
// with the full cause in the tail.
func anonymousCoolingMessage(secs int64, cause string) string {
	if cause == "" {
		return fmt.Sprintf("opencode2api: **[gateway]** anonymous lane cooling, retry in %ds (request was not sent upstream)", secs)
	}
	class := cause
	if i := strings.Index(cause, " ("); i >= 0 {
		class = cause[:i]
	}
	return fmt.Sprintf("opencode2api: **[gateway]** **%s** anonymous lane cooling, retry in %ds "+
		"(request was not sent upstream; cooling was triggered by %s)", class, secs, cause)
}

// writeAnonymousCoolingError renders a locally blocked request as 429 with
// Retry-After set to the remaining cooldown. The caller type-checks first so it
// can log the real reason; this only owns the wire shape.
func writeAnonymousCoolingError(w http.ResponseWriter, protocol wire.Protocol, cooling errAnonymousCooling, requestID string) {
	secs := int64(math.Ceil(cooling.retryAfter.Seconds()))
	if secs < 1 {
		secs = 1
	}
	// Retry-After is what makes this actionable for a client: without it the
	// 429 is just another failure and the caller picks its own backoff.
	w.Header().Set("Retry-After", strconv.FormatInt(secs, 10))
	wire.WriteError(w, protocol, http.StatusTooManyRequests,
		anonymousCoolingMessage(secs, cooling.cause), "anonymous_cooling", requestID)
}

// writeCoolingResponse maps a blocked request onto 429 and reports whether err
// was a cooldown at all. Both inference handlers share it: they differ only in
// the wire protocol they answer with. A false result means the caller must keep
// its own error path, so a genuine transport failure still reads as 502.
func (g *Gateway) writeCoolingResponse(w http.ResponseWriter, protocol wire.Protocol, err error,
	meta *telemetry.RequestMeta, requestID, tier string) bool {
	var cooling errAnonymousCooling
	if !errors.As(err, &cooling) {
		return false
	}
	secs := int64(math.Ceil(cooling.retryAfter.Seconds()))
	if secs < 1 {
		secs = 1
	}
	g.logger.Info("anonymous lane cooling, request not sent upstream",
		"component", "upstream", "event", "anonymous_cooling",
		"request_id", requestID, "tier", tier, "retry_after_s", secs, "cause", cooling.cause)
	if meta != nil {
		meta.Outcome = "anonymous_cooling"
	}
	writeAnonymousCoolingError(w, protocol, cooling, requestID)
	return true
}
