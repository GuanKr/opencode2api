package gateway

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"opencode2api/internal/config"
)

// errFreeTierIPBlocked reports that OpenCode's free gateway refused the request
// because it flagged the egress IP, not the request, the key or the session.
// Unlike the shape gates, another lane, key or retry cannot fix it: the same
// IP gets the same answer, and retrying only extends the penalty. The request
// must therefore leave the anonymous lane instead of walking its upstreams,
// and the exit must stay out of the cursor until the block expires.
var errFreeTierIPBlocked = errors.New("free tier blocked the egress IP")

// freeTierBlockedMessage is what a client receives while its exit is blocked.
// The upstream message tells people to turn off a VPN, which is wrong for a
// server that egresses through a proxy; this one names the real cause and the
// two ways out.
//
// The opencode2api: prefix is the script-facing marker for "the gateway wrote
// this, not the upstream" (watchdog-clash.ps1 matches it as a substring), so it
// is carried here exactly like proxyUnavailableMessage.
const freeTierBlockedMessage = "opencode2api: OpenCode is blocking free-model traffic from the IP this gateway leaves from, so retrying now cannot succeed. Route the request through another exit or wait for the block to expire."

// errAnonymousLanePaused reports that the anonymous lane's circuit breaker has
// paused the whole lane. Unlike an upstream rejection, another exit or a retry
// cannot succeed while the lane is paused; the caller should use an
// authenticated tier or report the pause itself.
var errAnonymousLanePaused = errors.New("anonymous lane paused by its circuit breaker")

// anonymousLanePausedMessage is what a client receives while the circuit
// breaker holds the anonymous lane shut. It names the cause so the operator is
// not sent looking for a credential or model problem.
//
// Prefixed for the same reason as freeTierBlockedMessage.
const anonymousLanePausedMessage = "opencode2api: The anonymous free tier is paused after repeated upstream failures. Configure an authenticated key, wait for the pause to expire, or restart the gateway after confirming the upstream block has lifted."

// freeTierBlockTTL keeps a blocked exit out of the anonymous cursor. Upstream
// does not publish the block duration, so this is a conservative backoff: long
// enough to stop the retry storm, short enough that an unblocked IP recovers
// without a restart.
const freeTierBlockTTL = time.Duration(config.DefaultFreeTierBlockSeconds) * time.Second

// freeTierBlockTTL returns the backoff applied to an exit the free tier flagged.
func (g *Gateway) freeTierBlockTTL() time.Duration {
	if g != nil && g.freeTierBlock > 0 {
		return g.freeTierBlock
	}
	return freeTierBlockTTL
}

// abuseProbeLimit bounds how much of a 403 body is read to classify it.
const abuseProbeLimit = 16 << 10

// isNetworkAbuseBlock reports whether status and body are the upstream's IP
// abuse rejection. The response is a 403 whose message names the egress
// network, in one of the wordings the gateway has been served:
//
//   - "…free models aren't available from your current network because we've
//     detected abusive traffic from this IP address…"
//   - "Forbidden, OpenCode Free Model Gateway Detect Abuse Traffic"
//   - "Abuse of traffic"
//
// The shape gate's 403 ("free tier can only be used from within OpenCode") is
// deliberately not matched: that one is a request-shape problem, which another
// lane or a refreshed body can still fix.
func isNetworkAbuseBlock(status int, body []byte) bool {
	if status != http.StatusForbidden {
		return false
	}
	text := strings.ToLower(string(body))
	if strings.Contains(text, "abusive traffic") ||
		strings.Contains(text, "abuse traffic") ||
		strings.Contains(text, "abuse of traffic") {
		return true
	}
	return strings.Contains(text, "free models aren't available") &&
		strings.Contains(text, "from your current network")
}

// peekResponseBody reads the start of a response body and puts it back in
// front of the rest, so classification and downstream error handling both see
// the full payload.
func peekResponseBody(resp *http.Response) []byte {
	if resp == nil || resp.Body == nil {
		return nil
	}
	prefix, err := io.ReadAll(io.LimitReader(resp.Body, abuseProbeLimit))
	resp.Body = replayBody(prefix, resp.Body)
	if err != nil {
		return nil
	}
	return prefix
}

// peekNetworkAbuseBlock classifies a response without consuming its body.
func peekNetworkAbuseBlock(resp *http.Response) bool {
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		return false
	}
	return isNetworkAbuseBlock(resp.StatusCode, peekResponseBody(resp))
}

// errDeterministicClientRejection reports that the upstream rejected the
// request itself: the body is malformed, too long for the model, or fails the
// same validation on every credential. Retrying on another key, lane or tier
// cannot change that, so the request must end and the original 4xx must reach
// the client. Without this, the first tier's real error is replaced by
// whatever the next tier answers — typically a 401 from an unconfigured key,
// which tells the client to re-authenticate instead of compacting.
var errDeterministicClientRejection = errors.New("upstream rejected the request deterministically")

// deterministicMarkers are the phrases an upstream uses for a request that will
// fail identically everywhere. They are deliberately all about length/context:
// the gateway reshapes every anonymous body (stream, tool definitions,
// max_completion_tokens), so a generic "invalid_request_error" 400 may be the
// gateway's own shaping rather than the client's mistake. Treating that as
// deterministic would skip a keyed tier that shapes differently and could have
// served the request, so the generic markers are not accepted here; a 413 is,
// because that status is the upstream's own verdict on the payload size.
var deterministicMarkers = []string{
	"context_length_exceeded",
	"maximum context length",
	"string_above_max_length",
	"prompt is too long",
}

// isDeterministicClientResponse reports whether status and body describe a
// request-level rejection rather than a credential or availability problem.
func isDeterministicClientResponse(status int, body []byte) bool {
	if status != http.StatusBadRequest && status != http.StatusUnprocessableEntity && status != http.StatusRequestEntityTooLarge {
		return false
	}
	text := strings.ToLower(string(body))
	for _, marker := range deterministicMarkers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

// replayBody returns a body that yields prefix and then the rest of original.
func replayBody(prefix []byte, original io.ReadCloser) io.ReadCloser {
	return &prefixBody{Reader: io.MultiReader(bytes.NewReader(prefix), original), closer: original}
}

type prefixBody struct {
	io.Reader
	closer io.Closer
}

func (b *prefixBody) Close() error { return b.closer.Close() }
