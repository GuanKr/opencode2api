package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"opencode2api/internal/config"
)

// The free tier's gate rejection, as seen from the watchdog.
//
// Background: the 403 the operator hit does not always carry a nested
// error.type. One observed variant is
//
//	{"error":{"type":"FreeTierError"}}
//
// and the other is a top-level envelope:
//
//	{"code":null,"message":"OpenCode's free tier can only be used from within
//	 OpenCode. Download it at https://opencode.ai and keep building.",
//	 "param":null,"type":"upstream_error"}
//
// The gateway publishes one cause label for both, and classifies it as quota so
// the watchdog switches exit nodes — and keeps switching when the next node
// answers the same (decided 2026-10-17, see AGENTS.md).
//
// These cases are offline: watchdogCause peeks an http.Response built in memory
// (no client is involved), the status cases go through observeAnonymousResult /
// handleWatchdogStatus with freezeProxyHealth parked, and the probe cases use an
// httptest stub on the loopback.

// freeTierGateEnvelope is the top-level-type variant. The message is the
// upstream's verbatim text (the apostrophe included); the envelope's own "type"
// is the client-visible kind, not an error class, which is exactly why the
// class has to come from the message.
const freeTierGateEnvelope = `{"code":null,"message":"OpenCode's free tier can only be used from within OpenCode. Download it at https://opencode.ai and keep building.","param":null,"type":"upstream_error"}`

// TestWatchdogCauseRecognizesFreeTierGateWithoutNestedType is the core of the
// fix: a classless envelope whose message names the gate must produce the same
// cause a nested FreeTierError does. Removing the message fallback turns this
// red with "upstream error (upstream HTTP 403)".
func TestWatchdogCauseRecognizesFreeTierGateWithoutNestedType(t *testing.T) {
	const want = "FreeTierError (upstream HTTP 403)"
	for _, tc := range []struct {
		name string
		body string
	}{
		{"top-level envelope", freeTierGateEnvelope},
		{"nested type", `{"error":{"type":"FreeTierError"}}`},
		{"message only", `{"message":"OpenCode's free tier can only be used from within OpenCode."}`},
		{"nested message only", `{"error":{"message":"OpenCode's free tier can only be used from within OpenCode."}}`},
		// copyErrorResponse prefixes the class onto the message it serves to
		// clients ("FreeTierError: OpenCode's free tier ..."), so the gate text
		// is not always at the start of the message.
		{"prefixed message", `{"message":"FreeTierError: OpenCode's free tier can only be used from within OpenCode."}`},
		{"upper case message", `{"message":"OPENCODE'S FREE TIER CAN ONLY BE USED FROM WITHIN OPENCODE."}`},
		// The gate sentence is the shared part; only the tail was reworded.
		{"reworded tail", `{"message":"OpenCode's free tier can only be used from within OpenCode. Get it today."}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := watchdogErrorResponse(http.StatusForbidden, "application/json", tc.body)
			if got := watchdogCause(resp, nil); got != want {
				t.Fatalf("cause = %q, want %q", got, want)
			}
			// Whatever the class came from, it must classify as quota: that is
			// what makes the watchdog switch.
			if got := classifyAnonymousVerdict(want, true); got != "quota" {
				t.Fatalf("gate cause verdict = %q, want quota", got)
			}
		})
	}
}

// TestWatchdogCauseGateTextOnlyOn403 pins the status guard on the message
// fallback: the gate sentence itself is honored only when the answer was a 403.
// A message is weaker evidence than a class, and an upstream error at another
// status can mention the free tier for reasons that have nothing to do with the
// gate. An explicit nested class stays status-independent (see the 503 row:
// FreeTierError on a 5xx must still reach the classifier's transport branch).
func TestWatchdogCauseGateTextOnlyOn403(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		want        string
		wantVerdict string
	}{
		{"403 gate envelope is the gate", http.StatusForbidden, "FreeTierError (upstream HTTP 403)", "quota"},
		// These three carry no class and no quota keyword, so with a cooldown
		// open (cooling=true, the state a failure leaves behind) they are "wait",
		// never a switch — which is the point: the gate sentence alone must not
		// turn them into a quota proof.
		{"400 with the gate sentence is not a gate proof", http.StatusBadRequest, "upstream error (upstream HTTP 400)", "cooling"},
		{"401 with the gate sentence is not a gate proof", http.StatusUnauthorized, "upstream error (upstream HTTP 401)", "cooling"},
		{"404 with the gate sentence is not a gate proof", http.StatusNotFound, "upstream error (upstream HTTP 404)", "cooling"},
		{"429 with the gate sentence stays a plain throttle", http.StatusTooManyRequests, "upstream error (upstream HTTP 429)", "cooling"},
		{"503 with the gate sentence is server wobble", http.StatusServiceUnavailable, "upstream error (upstream HTTP 503)", "transport"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := watchdogErrorResponse(tc.status, "application/json", freeTierGateEnvelope)
			if got := watchdogCause(resp, nil); got != tc.want {
				t.Fatalf("cause = %q, want %q", got, tc.want)
			}
			// cooling=true is the state a failure actually leaves behind, so
			// the classless rows classify as cooling (wait), never quota.
			if got := classifyAnonymousVerdict(tc.want, true); got != tc.wantVerdict {
				t.Fatalf("verdict = %q, want %q", got, tc.wantVerdict)
			}
		})
	}
	// The other half of the guard: an explicit nested class is a class at any
	// status, so the 5xx-precedence invariant keeps holding even for the class
	// name this change introduced.
	for _, status := range []int{http.StatusForbidden, http.StatusServiceUnavailable} {
		resp := watchdogErrorResponse(status, "application/json", `{"error":{"type":"FreeTierError"}}`)
		cause := watchdogCause(resp, nil)
		want := "FreeTierError (upstream HTTP " + strconv.Itoa(status) + ")"
		if cause != want {
			t.Fatalf("nested class at %d: cause = %q, want %q", status, cause, want)
		}
		wantVerdict := "quota"
		if status >= 500 {
			wantVerdict = "transport"
		}
		if got := classifyAnonymousVerdict(cause, true); got != wantVerdict {
			t.Fatalf("nested class at %d: verdict = %q, want %q", status, got, wantVerdict)
		}
	}
}

// TestWatchdogCauseGateMappingStaysNarrow pins that only the gate's own text is
// mapped. A 403 from anywhere else keeps the classless cause (and therefore the
// probe's fingerprint reading for a challenge page), and an unrelated message
// that merely mentions a free tier must not become a switch recommendation.
func TestWatchdogCauseGateMappingStaysNarrow(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"plain text 403", "upstream said no", "upstream error (upstream HTTP 403)"},
		{"html challenge page", "<html>Attention Required</html>", "upstream error (upstream HTTP 403)"},
		{"json without a message", `{"code":403,"type":"upstream_error"}`, "upstream error (upstream HTTP 403)"},
		{"unrelated free tier mention", `{"message":"your free tier balance is low"}`, "upstream error (upstream HTTP 403)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := watchdogErrorResponse(http.StatusForbidden, "application/json", tc.body)
			if got := watchdogCause(resp, nil); got != tc.want {
				t.Fatalf("cause = %q, want %q", got, tc.want)
			}
			// None of these carries a class, so the ordinary path stays a plain
			// (non-recommending) cooling state; only the probe's fallback table
			// turns the status itself into a verdict.
			if got := classifyAnonymousVerdict(tc.want, true); got != "cooling" {
				t.Fatalf("classless 403 verdict = %q, want cooling", got)
			}
		})
	}
	// An explicit nested class wins over the gate text: upstream said what it
	// meant, and an unrelated class must not be relabelled as the gate.
	explicit := watchdogErrorResponse(http.StatusForbidden,
		"application/json",
		`{"error":{"type":"invalid_request_error","message":"OpenCode's free tier can only be used from within OpenCode."}}`)
	if got := watchdogCause(explicit, nil); got != "invalid_request_error (upstream HTTP 403)" {
		t.Fatalf("an explicit class must win over the message, got %q", got)
	}
}

// TestWatchdogCauseGatePeekKeepsBodyIntact is the passthrough guard: the label
// is a label. The peek must not consume or truncate a single byte of the error
// document, and closing the restored body must still reach the original.
func TestWatchdogCauseGatePeekKeepsBodyIntact(t *testing.T) {
	resp := watchdogErrorResponse(http.StatusForbidden, "application/json", freeTierGateEnvelope)
	if got := watchdogCause(resp, nil); got != "FreeTierError (upstream HTTP 403)" {
		t.Fatalf("cause = %q", got)
	}
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != freeTierGateEnvelope {
		t.Fatalf("the gate body must survive the label byte for byte, got %q", rest)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("closing the restored body must succeed, got %v", err)
	}
}

// observeGateFailure drives the production wiring for one anonymous failure —
// the same call doAnonymousUpstream makes for a non-2xx answer — with the
// response handed to the label producer, and returns the resulting pool.
func observeGateFailure(t *testing.T, resp *http.Response) (*Gateway, *anonymousNodes) {
	t.Helper()
	transports, err := transportPool([]string{"direct"}, config.PerformanceConfig{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool := anonymousPool(true, transports, 15*time.Second, anonymousBackoff{})
	g := watchdogTestGateway(transports, pool)
	freezeProxyHealth(g)
	g.observeAnonymousResult(context.Background(), pool.nodes[0], resp, nil, "")
	return g, pool
}

// TestObserveAnonymousResultGateEnvelopeSurfacesQuotaSwitch is the end-to-end
// seam the watchdog actually reads: a failing turn whose response is the
// top-level envelope must leave the status feed recommending a switch, with the
// cause still naming the upstream gate. Wire only the label and this reads as
// "cooling"; wire only the verdict and the cause loses the gate's name.
func TestObserveAnonymousResultGateEnvelopeSurfacesQuotaSwitch(t *testing.T) {
	g, _ := observeGateFailure(t, watchdogErrorResponse(http.StatusForbidden, "application/json", freeTierGateEnvelope))
	resp := doWatchdogStatus(t, g)
	if resp.Anonymous.Verdict != "quota" {
		t.Fatalf("gate envelope must surface as quota, got verdict %q (cause %q)", resp.Anonymous.Verdict, resp.Anonymous.Cause)
	}
	if !resp.RecommendSwitch {
		t.Fatalf("gate envelope must recommend a switch, got %+v", resp.Anonymous)
	}
	if resp.Anonymous.Cause != "FreeTierError (upstream HTTP 403)" {
		t.Fatalf("cause = %q, want the gate class plus the status", resp.Anonymous.Cause)
	}
	if resp.Anonymous.LastFailureAgeS == nil {
		t.Fatal("a real user-traffic failure must report last_failure_age_s")
	}
}

// TestClassifyAnonymousVerdictKeeps5xxPrecedence is the invariant the gate
// change must not break: a 5xx tail outranks the class, so a flapping upstream
// cannot drive a node switch on a quota pretext — including when the class was
// recovered from the gate's message.
func TestClassifyAnonymousVerdictKeeps5xxPrecedence(t *testing.T) {
	if got := classifyAnonymousVerdict("FreeTierError (upstream HTTP 503)", true); got != "transport" {
		t.Fatalf("FreeTierError with a 503 tail = %q, want transport", got)
	}
	if got := classifyAnonymousVerdict("FreeTierError (upstream HTTP 502)", true); got != "transport" {
		t.Fatalf("FreeTierError with a 502 tail = %q, want transport", got)
	}
}

// TestWatchdogProbeGateRejectionVerdictsQuota is the probe half of the fix:
// after the operator switches nodes, the forced probe asks the new node the same
// question and must report quota for the gate body — which is what makes
// Invoke-Failover try the next candidate instead of stopping the round.
func TestWatchdogProbeGateRejectionVerdictsQuota(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"top-level envelope", freeTierGateEnvelope},
		{"nested type", `{"error":{"type":"FreeTierError"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url, hits := stubUpstream(t, http.StatusForbidden, tc.body)
			g := probeGateway(t, url)
			rec, out := sendProbe(t, g, `{"model":"probe-free"}`)
			if rec.Code != http.StatusOK {
				t.Fatalf("probe must answer 200 with its verdict, got %d: %s", rec.Code, rec.Body.String())
			}
			if !out.Attempted || out.Status != http.StatusForbidden {
				t.Fatalf("probe must report a transmitted 403, got %+v", out)
			}
			if out.Verdict != "quota" {
				t.Fatalf("gate verdict = %q, want quota (cause %q)", out.Verdict, out.Cause)
			}
			if out.Cause != "FreeTierError (upstream HTTP 403)" {
				t.Fatalf("probe cause = %q, want the gate class plus the status", out.Cause)
			}
			if hits.Load() != 1 {
				t.Fatalf("probe must reach the upstream exactly once, hits=%d", hits.Load())
			}
		})
	}
}

// TestWatchdogProbeClassless403StaysFingerprint is the guard on the other side:
// a 403 with no class and no gate text is a challenge page on that exit IP. It
// must keep the fallback fingerprint verdict rather than becoming a quota
// switch, because a challenge page is exactly the case the script treats
// differently from a gate rejection.
func TestWatchdogProbeClassless403StaysFingerprint(t *testing.T) {
	url, _ := stubUpstream(t, http.StatusForbidden, `<html><title>Attention Required! | Cloudflare</title></html>`)
	g := probeGateway(t, url)
	_, out := sendProbe(t, g, `{"model":"probe-free"}`)
	if out.Verdict != "fingerprint" {
		t.Fatalf("a classless 403 must keep the fingerprint verdict, got %+v", out)
	}
	if out.Cause != "upstream error (upstream HTTP 403)" {
		t.Fatalf("classless 403 cause = %q, want the status-only label", out.Cause)
	}
}

// TestWatchdogProbeGateTextOff403IsNotQuota is the probe-side half of the status
// guard: a non-403 answer carrying the gate sentence must not present as a
// burned allowance. A 429 is the interesting row — the classless fallback turns
// it into ratelimit (wait), where an unguarded message match would have sent
// the script switching nodes on a throttle.
func TestWatchdogProbeGateTextOff403IsNotQuota(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		verdict string
	}{
		{"429 with the gate sentence waits", http.StatusTooManyRequests, "ratelimit"},
		{"400 with the gate sentence is our config", http.StatusBadRequest, "config"},
		{"404 with the gate sentence is our config", http.StatusNotFound, "config"},
		{"503 with the gate sentence is transport", http.StatusServiceUnavailable, "transport"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url, _ := stubUpstream(t, tc.status, freeTierGateEnvelope)
			g := probeGateway(t, url)
			_, out := sendProbe(t, g, `{"model":"probe-free"}`)
			if out.Verdict != tc.verdict {
				t.Fatalf("status %d: verdict = %q, want %q (cause %q)", tc.status, out.Verdict, tc.verdict, out.Cause)
			}
			if out.Cause == "FreeTierError (upstream HTTP "+strconv.Itoa(tc.status)+")" || strings.Contains(out.Cause, "FreeTierError") {
				t.Fatalf("status %d: the gate text must not become a class off 403, got cause %q", tc.status, out.Cause)
			}
		})
	}
}

// TestWatchdogProbeBodyNeverEchoesTheGate keeps the feed's privacy contract on
// the new path: the probe reports the class it recovered, never the upstream
// document it recovered it from.
func TestWatchdogProbeBodyNeverEchoesTheGate(t *testing.T) {
	url, _ := stubUpstream(t, http.StatusForbidden, freeTierGateEnvelope)
	g := probeGateway(t, url)
	rec, _ := sendProbe(t, g, `{"model":"probe-free"}`)
	if strings.Contains(rec.Body.String(), "keep building") || strings.Contains(rec.Body.String(), "opencode.ai") {
		t.Fatalf("the probe must not echo the upstream error document: %s", rec.Body.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("probe answer must stay JSON: %v", err)
	}
}
