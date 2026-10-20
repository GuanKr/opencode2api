package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"opencode2api/internal/identity"
	"opencode2api/internal/jsonutil"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
	"opencode2api/internal/telemetry"
)

// DebugKeyOverride pins one inference request to a single configured upstream
// key so the Playground can exercise that key in isolation.
type DebugKeyOverride struct {
	Tier  config.Tier
	KeyID string
}

type debugKeyOverrideContextKey struct{}

type diagnosticContextKey struct{}

func WithDiagnosticRequest(ctx context.Context) context.Context {
	return context.WithValue(ctx, diagnosticContextKey{}, true)
}

func isDiagnosticRequest(ctx context.Context) bool {
	diagnostic, _ := ctx.Value(diagnosticContextKey{}).(bool)
	return diagnostic
}

func WithDebugKeyOverride(ctx context.Context, override DebugKeyOverride) context.Context {
	return context.WithValue(WithDiagnosticRequest(ctx), debugKeyOverrideContextKey{}, override)
}

func debugKeyOverrideFrom(ctx context.Context) (DebugKeyOverride, bool) {
	override, ok := ctx.Value(debugKeyOverrideContextKey{}).(DebugKeyOverride)
	return override, ok && override.KeyID != ""
}

// fingerprint is the client payload's semantic hash (identity.RequestFingerprint),
// captured before any shaping. It scopes anonymous cooldown: a retry of the same
// turn is blocked, a different turn still gets one real attempt.
func (g *Gateway) doUpstream(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, fingerprint string) (*http.Response, models.Route, error) {
	// Diagnostic requests (Playground auto mode included) must neither see
	// a pre-stripped shape nor leave traces in production memory: they
	// exercise the real request shape and never write production pool
	// state. Per-key probes additionally return before any memo access.
	// Note: ordinary diagnostics still allow the in-turn stale-reasoning
	// self-heal retry below (only memo reads/writes are skipped), so a
	// poisoned Playground turn succeeds with the stripped shape instead of
	// surfacing the raw 400; use a per-key probe only when the raw shape
	// itself must be exercised.
	diagnostic := isDiagnosticRequest(ctx)
	// recordBlacklistSize publishes the stored blacklist size for the usage
	// page. It re-reads the snapshot so the displayed count always equals
	// what is actually stored (post dedup/cap). The count is best-effort
	// display only: a concurrent clean/eviction slipping between the memo
	// write and this re-read may leave a stale size (or zero) for one turn,
	// which never affects memo correctness.
	recordBlacklistSize := func() {
		meta := telemetry.MetaFromContext(ctx)
		if meta == nil {
			return
		}
		if snapIDs, _, _, ok := g.staleMemo.snapshot(ids.Session); ok {
			meta.PrestripIDs = len(snapIDs)
		}
	}
	// Poisoned-history fast path: this conversation needed a strip-retry
	// recently, so its stored history still carries dead reasoning
	// references that would 400 again. Send the stripped shape on the first
	// attempt instead of burning a 400 round-trip every turn. The memo
	// expires (re-probing with the full shape), so server-side healing or a
	// dsh history rewrite resumes normal replay on its own. Diagnostic
	// requests bypass it so per-key probes always exercise the real shape.
	//
	// The fast path is selective: only the snapshotted blacklist is removed
	// (every reasoning id the failed body carried at poison-discovery time,
	// plus the dead chain link), while reasoning that arrived after the mark
	// is kept — the failing body had never seen it, so it cannot be what the
	// server rejected. A selective attempt that still 400s falls back to a
	// full strip once within the same turn. Past the TTL the memo no longer
	// remembers the session, so that turn re-probes full and re-learns from
	// its failing body instead of reusing anything old.
	prestripped := false
	prestripMode := ""
	if _, selected := debugKeyOverrideFrom(ctx); !selected && !diagnostic && g.staleMemo.shouldPrestrip(ids.Session) {
		deny, deadPrev, full, ok := g.staleMemo.snapshot(ids.Session)
		if ok && !full {
			if stripped, changed := stripSelectiveReasoningInputs(route, bodies, deny, deadPrev); changed {
				g.logger.Debug("poisoned history selectively pre-stripped on first attempt", "component", "upstream", "event", "stale_retry_pre_stripped", "request_id", ids.Request, "session_id", ids.Session, "model", route.ID, "tier", route.Tier, "prestrip_mode", "selective",
					"replayed", summarizeReplayedReasoning(bodies[route.Tier]))
				// Every tier body was encoded up front by prepareRouteBodies
				// and stripSelectiveReasoningInputs rewrites all of them,
				// so no lazily encoded fallback tier can resurrect the
				// blacklisted references (and healthy ones are kept).
				bodies = stripped
				prestripped = true
				prestripMode = "selective"
				if meta := telemetry.MetaFromContext(ctx); meta != nil {
					meta.Prestripped = true
					meta.PrestripMode = "selective"
					meta.PrestripIDs = len(deny)
				}
			}
			// No blacklisted id present: the stored history looks clean
			// already (e.g. the client rewrote it), so re-probe with the
			// full shape below. A clean 2xx then clears the memo.
			// Note: !ok (entry concurrently cleaned/evicted between the
			// shouldPrestrip and snapshot calls) intentionally falls through
			// with no pre-strip: treat it as unremembered and probe full.
		} else if ok && full {
			if stripped, changed := stripStaleReasoningInputs(route, bodies); changed {
				g.logger.Debug("poisoned history pre-stripped on first attempt", "component", "upstream", "event", "stale_retry_pre_stripped", "request_id", ids.Request, "session_id", ids.Session, "model", route.ID, "tier", route.Tier, "prestrip_mode", "full")
				bodies = stripped
				prestripped = true
				prestripMode = "full"
				if meta := telemetry.MetaFromContext(ctx); meta != nil {
					meta.Prestripped = true
					meta.PrestripMode = "full"
				}
			}
		}
	}
	resp, effectiveRoute, attempts, err := g.doUpstreamTiers(ctx, route, bodies, ids, 0, fingerprint)
	if attempts == 0 && prestripped {
		// Nothing was actually transmitted (no healthy anonymous node or
		// key tier accepted the request before any attempt): the prestrip
		// flags describe a shape never sent, so drop them rather than
		// persisting a claim about unsent bytes. Any real attempt leaves
		// attempts > 0 and keeps the flags.
		prestripped = false
		prestripMode = ""
		if meta := telemetry.MetaFromContext(ctx); meta != nil {
			meta.Prestripped = false
			meta.PrestripMode = ""
			meta.PrestripIDs = 0
		}
	}
	if _, selected := debugKeyOverrideFrom(ctx); selected {
		return resp, effectiveRoute, err
	}
	if err != nil || resp == nil || resp.StatusCode != http.StatusBadRequest {
		if err == nil && resp != nil && resp.StatusCode/100 == 2 && !prestripped && !diagnostic {
			// A clean first attempt proves the stored history replays fine.
			g.staleMemo.noteClean(ids.Session)
		}
		return resp, effectiveRoute, err
	}
	origBody := resp.Body
	errBody, readErr := io.ReadAll(io.LimitReader(origBody, 1<<20))
	// The original network body is always closed here: the retry path below
	// reuses the connection, otherwise downstream receives a fresh in-memory
	// reader over the cached bytes.
	httpx.DrainAndClose(origBody)
	if readErr != nil {
		resp.Body = io.NopCloser(bytes.NewReader(errBody))
		return resp, effectiveRoute, nil
	}
	// Restore the body so downstream error handling still sees the original
	// payload when no retry happens below.
	resp.Body = io.NopCloser(bytes.NewReader(errBody))
	// Log a capped snippet of the upstream error body so the 400 class is
	// visible without dumping full payloads. Bodies stay out of attempt
	// records; the hub redactor scrubs configured secrets from log output.
	if len(errBody) > 0 {
		redactor := config.SecretRedactor()
		redactor.Replace(g.cfg)
		snippet := redactor.String(string(errBody))
		if len(snippet) > 240 {
			snippet = snippet[:240]
		}
		g.logger.Info("upstream error returned or retried", "component", "upstream", "event", "upstream_error_body", "request_id", ids.Request, "model", route.ID, "status", resp.StatusCode, "snippet", snippet)
	}
	if !isStaleReasoningReference(errBody) {
		return resp, effectiveRoute, nil
	}
	g.logger.Debug("stale reasoning reference matched; retrying without them", "component", "upstream", "event", "stale_reasoning_matched", "request_id", ids.Request, "session_id", ids.Session, "model", route.ID, "tier", effectiveRoute.Tier, "prestrip_mode", prestripMode,
		"replayed", summarizeReplayedReasoning(bodies[effectiveRoute.Tier]))
	// A selective pre-strip that still 400s keeps newer reasoning the full
	// strip would drop, so fall back to the full strip once within the same
	// turn (opencode verification shows post-mark reasoning is healthy, so
	// the fallback is a safety net, not the steady state).
	if prestripMode == "selective" {
		stripped, changed := stripStaleReasoningInputs(effectiveRoute, bodies)
		if !changed {
			return resp, effectiveRoute, nil
		}
		g.logger.Info("selective prestrip still rejected; falling back to full strip", "component", "upstream", "event", "reasoning_reference_fallback", "request_id", ids.Request, "session_id", ids.Session, "model", route.ID, "tier", effectiveRoute.Tier, "attempt_offset", attempts)
		// The stale-reasoning self-heal replays the SAME client turn with reasoning
		// references stripped, so it keeps the original fingerprint on purpose: a
		// same-turn retry must stay inside the cooldown scope that blocked it.
		retryResp, retryRoute, _, retryErr := g.doUpstreamTiers(ctx, effectiveRoute, stripped, ids, attempts, fingerprint)
		if retryErr != nil || retryResp == nil || retryResp.StatusCode/100 != 2 {
			retryStatus := 0
			if retryResp != nil {
				retryStatus = retryResp.StatusCode
				httpx.DrainAndClose(retryResp.Body)
			}
			g.logger.Warn("reasoning reference retry failed; returning original error", "component", "upstream", "event", "reasoning_reference_retry_failed", "request_id", ids.Request, "session_id", ids.Session, "model", route.ID, "tier", effectiveRoute.Tier, "error", retryErr, "retry_status", retryStatus)
			fallback := *resp
			fallback.Body = io.NopCloser(bytes.NewReader(errBody))
			return &fallback, effectiveRoute, nil
		}
		// The blacklist was proven incomplete: the upstream rejected a body
		// that no longer carried the blacklisted ids, and only removing the
		// rest of them made the replay succeed. Merge those into the
		// blacklist so the next turn strips them on its first attempt
		// instead of paying this same 400 + fallback every turn.
		if !diagnostic {
			g.rememberStaleHistory(ids.Session, bodies[effectiveRoute.Tier], prestripMode)
			recordBlacklistSize()
		}
		return retryResp, retryRoute, nil
	}
	stripped, changed := stripStaleReasoningInputs(effectiveRoute, bodies)
	if !changed {
		return resp, effectiveRoute, nil
	}
	// The referenced reasoning items belong to an upstream chain this session
	// can no longer address (e.g. an interrupted stream). Replay once without
	// them instead of failing the client request outright. Attempt numbering
	// continues from the first round so monitoring never shows duplicate attempt
	// numbers for one request.
	//
	// The client's session is deliberately preserved: it carries key/proxy
	// affinity and upstream session correlation. Minting a fresh session made
	// the replay land on a different node with a cold prompt cache and detached
	// the reply from the chain the client still holds; the stale references are
	// already removed from the payload itself.
	g.logger.Info("retrying upstream without stale reasoning references", "component", "upstream", "event", "reasoning_reference_retry", "request_id", ids.Request, "model", route.ID, "tier", effectiveRoute.Tier, "attempt_offset", attempts)
	// The stale-reasoning self-heal replays the SAME client turn with reasoning
	// references stripped, so it keeps the original fingerprint on purpose: a
	// same-turn retry must stay inside the cooldown scope that blocked it.
	retryResp, retryRoute, _, retryErr := g.doUpstreamTiers(ctx, effectiveRoute, stripped, ids, attempts, fingerprint)
	if retryErr != nil || retryResp == nil || retryResp.StatusCode/100 != 2 {
		retryStatus := 0
		if retryResp != nil {
			retryStatus = retryResp.StatusCode
			httpx.DrainAndClose(retryResp.Body)
		}
		g.logger.Warn("reasoning reference retry failed; returning original error", "component", "upstream", "event", "reasoning_reference_retry_failed", "request_id", ids.Request, "model", route.ID, "tier", effectiveRoute.Tier, "error", retryErr, "retry_status", retryStatus)
		fallback := *resp
		fallback.Body = io.NopCloser(bytes.NewReader(errBody))
		return &fallback, effectiveRoute, nil
	}
	// The history needed surgery this turn; remember it so the next turns
	// pre-strip on the first attempt instead of burning a 400 each time.
	// prestripMode tells rememberStaleHistory whether this turn's failure is
	// fresh evidence (selective pre-strip still rejected) or just the same old
	// poison (no pre-strip / full pre-strip). Diagnostic requests never write
	// production memory.
	if !diagnostic {
		g.rememberStaleHistory(ids.Session, bodies[effectiveRoute.Tier], prestripMode)
		recordBlacklistSize()
	}
	return retryResp, retryRoute, nil
}

// rememberStaleHistory is the memo write path for a turn that needed
// reasoning surgery and then succeeded. It is a method (rather than inline
// code in doUpstream) so the branch can be exercised without a failing
// upstream: a real 400 would spawn the background proxy verification, which
// dials the public internet.
//
// prestripMode is what this turn's FIRST attempt was already stripped with
// ("" when nothing was pre-stripped), because it decides whether the failing
// body carries fresh evidence:
//
//   - "selective": the blacklist was proven INCOMPLETE. The rejected body no
//     longer carried the blacklisted ids, yet the upstream still refused it
//     and only the full strip (dropping everything it did carry — the
//     remaining reasoning ids AND the chain link) was accepted — so those
//     references are dead as well and get learned. Without this, a session
//     whose upstream moved on to other references pays one more 400 +
//     fallback every single turn for the whole window, because the entry it
//     holds can never grow. Note the chain link is not stripped by the
//     selective pass unless it equals the remembered dead one, so a
//     fresh-but-dead link reaches this point and must be learned here too.
//   - anything else ("" / "full"): the failure carries no new evidence, so an
//     already-remembered session only has its window moved. Ids appended
//     during the window are healthy — nothing rejected them — and must never
//     be blacklisted.
//
// Only the first-ever poison snapshots the current failing body (which is
// exactly the stripped id set). Past the TTL the memo no longer remembers the
// session, so the turn re-learns from its failing body instead.
func (g *Gateway) rememberStaleHistory(session string, failingBody []byte, prestripMode string) {
	if prestripMode == "selective" {
		failingIDs, failingPrev := collectStaleReasoningIDs(failingBody)
		g.staleMemo.mergeRejected(session, failingIDs, failingPrev)
		return
	}
	if _, _, _, hadOld := g.staleMemo.snapshot(session); hadOld {
		g.staleMemo.refresh(session)
		return
	}
	failingIDs, failingPrev := collectStaleReasoningIDs(failingBody)
	g.staleMemo.noteStripped(session, failingIDs, failingPrev)
}

// isStaleReasoningReference reports whether an upstream 400 body describes a
// reasoning item/reference the server no longer recognizes, such as
// "Referenced reasoning item 'rs_...' was not found or has expired".
// Generic validation errors that merely mention reasoning (e.g. "unknown
// reasoning field") must NOT match, so both the target phrase and the
// gone/expired marker are required.
func isStaleReasoningReference(body []byte) bool {
	text := strings.ToLower(string(body))
	if !strings.Contains(text, "reasoning item") && !strings.Contains(text, "reasoning reference") {
		return false
	}
	for _, marker := range []string{"not found", "expir", "does not exist", "no longer"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

// stripStaleReasoningInputs removes server-issued reasoning references from
// Responses-protocol upstream payloads: replayed "reasoning" input items and
// any previous_response_id chain link. Other tiers/protocols are passed
// through untouched. It reports whether any payload actually changed.
func stripStaleReasoningInputs(route models.Route, bodies map[config.Tier][]byte) (map[config.Tier][]byte, bool) {
	changed := false
	out := make(map[config.Tier][]byte, len(bodies))
	for tier, body := range bodies {
		if len(body) == 0 || route.ProtocolFor(tier) != wire.Responses {
			out[tier] = body
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			out[tier] = body
			continue
		}
		tierChanged := false
		if _, ok := payload["previous_response_id"]; ok {
			delete(payload, "previous_response_id")
			tierChanged = true
		}
		if raw, ok := payload["input"].([]any); ok {
			kept := make([]any, 0, len(raw))
			for _, item := range raw {
				if m, ok := item.(map[string]any); ok && jsonutil.StringAt(m, "type") == "reasoning" {
					tierChanged = true
					continue
				}
				kept = append(kept, item)
			}
			if tierChanged {
				payload["input"] = kept
			}
		}
		if !tierChanged {
			out[tier] = body
			continue
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			out[tier] = body
			continue
		}
		out[tier] = encoded
		changed = true
	}
	return out, changed
}

// collectStaleReasoningIDs extracts the snapshot blacklist from a failed
// Responses-protocol upstream payload: EVERY reasoning item id present in
// that body, plus the chain link seen then.
//
// The gateway cannot narrow this down to the one id the 400 names: the
// upstream error text is free-form prose, parsing it reliably would be a
// second, weaker copy of the same classification, and a wrong guess would
// leave a dead reference in the body (a wasted 400 every turn). So the
// blacklist is the whole reasoning set as of poison-discovery time, which
// means the entire pre-mark reasoning prefix is re-stripped on every turn
// of the window, not just the single id the server complained about. What
// selective stripping actually buys is that reasoning delivered AFTER the
// mark — the part the failing body had never seen — survives. Empty ids
// are skipped: id-less reasoning items can never replay validly, so
// selective stripping always drops them without needing to record them.
func collectStaleReasoningIDs(body []byte) (ids []string, prev string) {
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return nil, ""
	}
	prev = jsonutil.StringAt(payload, "previous_response_id")
	seen := make(map[string]struct{})
	for _, raw := range jsonutil.SliceAt(payload, "input") {
		m, _ := raw.(map[string]any)
		if m == nil || jsonutil.StringAt(m, "type") != "reasoning" {
			continue
		}
		id := jsonutil.StringAt(m, "id")
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, prev
}

// stripSelectiveReasoningInputs removes only the snapshotted blacklist from
// Responses-protocol upstream payloads: blacklisted reasoning items, any
// id-less reasoning items (never replayable), and the chain link only when
// it still equals the dead link seen at snapshot time (a fresh link issued
// after the mark is preserved, keeping newer healthy reasoning addressable).
// Because the blacklist is every reasoning id the failing body carried, the
// whole pre-mark reasoning prefix goes; what is kept is reasoning that
// arrived after the mark (see collectStaleReasoningIDs).
// Other tiers/protocols pass through untouched. It reports whether any
// payload actually changed.
func stripSelectiveReasoningInputs(route models.Route, bodies map[config.Tier][]byte, deny map[string]struct{}, deadPrev string) (map[config.Tier][]byte, bool) {
	changed := false
	out := make(map[config.Tier][]byte, len(bodies))
	for tier, body := range bodies {
		if len(body) == 0 || route.ProtocolFor(tier) != wire.Responses {
			out[tier] = body
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			out[tier] = body
			continue
		}
		tierChanged := false
		if current, ok := payload["previous_response_id"]; ok {
			if deadPrev != "" {
				// A non-string link can never be a freshly issued chain id,
				// so treat it as dead rather than burning a fallback
				// round-trip to learn the same thing.
				if prev, isStr := current.(string); !isStr || prev == deadPrev {
					delete(payload, "previous_response_id")
					tierChanged = true
				}
			}
		}
		if raw, ok := payload["input"].([]any); ok {
			kept := make([]any, 0, len(raw))
			removed := false
			for _, item := range raw {
				m, ok := item.(map[string]any)
				if !ok || jsonutil.StringAt(m, "type") != "reasoning" {
					kept = append(kept, item)
					continue
				}
				id := jsonutil.StringAt(m, "id")
				if id == "" {
					removed = true
					continue
				}
				if _, blacklisted := deny[id]; blacklisted {
					removed = true
					continue
				}
				kept = append(kept, item)
			}
			if removed {
				payload["input"] = kept
				tierChanged = true
			}
		}
		if !tierChanged {
			out[tier] = body
			continue
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			out[tier] = body
			continue
		}
		out[tier] = encoded
		changed = true
	}
	return out, changed
}

// summarizeReplayedReasoning describes the Responses reasoning input items
// the client asked to replay in a request body (debug diagnostics only):
// how many, how many carry encrypted_content, a hash of the ordered id
// list, plus head/tail ids and any previous_response_id chain link.
// Comparing two turns decides whether dsh replays the same dead references
// every turn or rotates in freshly delivered ones. Note it describes
// whatever body it is handed at the call site, and the two call sites sit on
// opposite sides of the pre-strip: the "selectively pre-stripped" line
// evaluates it before bodies is replaced, so it reports the client's own
// replayed ids, while "stale reasoning reference matched" runs on the
// already-stripped bodies and therefore reports n=0 for a full-mode
// pre-strip (n=0 there is expected, not a bug).
func summarizeReplayedReasoning(body []byte) string {
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return "unparseable"
	}
	var ids []string
	encrypted := 0
	for _, raw := range jsonutil.SliceAt(payload, "input") {
		m, _ := raw.(map[string]any)
		if m == nil || jsonutil.StringAt(m, "type") != "reasoning" {
			continue
		}
		ids = append(ids, jsonutil.StringAt(m, "id"))
		if jsonutil.StringAt(m, "encrypted_content") != "" {
			encrypted++
		}
	}
	sum := sha256.Sum256([]byte(strings.Join(ids, ",")))
	head, tail := "", ""
	if len(ids) > 0 {
		head = strings.Join(ids[:min(3, len(ids))], ",")
		tail = strings.Join(ids[max(0, len(ids)-3):], ",")
	}
	return fmt.Sprintf("n=%d encrypted=%d hash=%s head=[%s] tail=[%s] prev=%s",
		len(ids), encrypted, hex.EncodeToString(sum[:4]), head, tail,
		jsonutil.StringAt(payload, "previous_response_id"))
}

func (g *Gateway) doUpstreamTiers(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, attemptOffset int, fingerprint string) (*http.Response, models.Route, int, error) {
	var lastResponse *http.Response
	var lastErr error
	effectiveRoute := route
	attempts := attemptOffset
	g.dumpClientBodies(route, bodies, ids, attemptOffset)
	if override, selected := debugKeyOverrideFrom(ctx); selected {
		resp, err, used := g.doSelectedKeyUpstream(ctx, route, bodies, ids, override, attemptOffset)
		return resp, route, used, err
	}
	if route.Anonymous {
		resp, err, used := g.doAnonymousUpstream(ctx, route, bodies, ids, attempts, fingerprint)
		attempts += used
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			return resp, route, attempts, nil
		}
		lastResponse, lastErr = resp, err
		if len(route.KeyTiers) > 0 {
			g.logger.Debug("anonymous request did not succeed; entering preferred key tiers", "component", "upstream", "event", "anonymous_fallback", "request_id", ids.Request, "attempts", attempts, "key_tiers", route.KeyTiers)
		}
	}
	if ctx.Err() != nil {
		// The anonymous phase already consumed the request budget. Entering the
		// authenticated tiers here would only fire instant attempts against a
		// dead context, cooling keys that were never really tried.
		if lastResponse != nil {
			return lastResponse, effectiveRoute, attempts, nil
		}
		if lastErr == nil {
			lastErr = ctx.Err()
		}
		return nil, effectiveRoute, attempts, lastErr
	}

	keyTiers := route.KeyTiers
	if !route.Anonymous && len(keyTiers) == 0 && (route.Tier == config.TierZen || route.Tier == config.TierGo) {
		keyTiers = []config.Tier{route.Tier}
	}
	for _, tier := range keyTiers {
		keyRoute := route
		keyRoute.Tier = tier
		keyRoute.Anonymous = false
		keyRoute.Protocol = route.ProtocolFor(tier)
		effectiveRoute = keyRoute
		resp, err, used := g.doKeyUpstream(ctx, keyRoute, bodies, ids, attempts)
		attempts += used
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			if lastResponse != nil {
				httpx.DrainAndClose(lastResponse.Body)
			}
			return resp, keyRoute, attempts, nil
		}
		if resp != nil {
			if lastResponse != nil {
				httpx.DrainAndClose(lastResponse.Body)
			}
			lastResponse = resp
		}
		if err != nil {
			lastErr = err
		}
	}
	if lastResponse != nil {
		return lastResponse, effectiveRoute, attempts, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no usable upstream route")
	}
	if ctx.Err() == nil {
		// Same on-demand re-probe as the anonymous lane (throttled inside):
		// a just-fixed proxy recovers on the next request instead of waiting
		// for the degraded ticker. No-op while anything is healthy.
		g.kickProxyRecheck(ctx)
	}
	return nil, effectiveRoute, attempts, lastErr
}

// doAnonymousUpstream tries every currently available proxy at most once. For
// each proxy it walks the configured anonymous lane order (anonymousUpstreams),
// preparing one shaped body per candidate because the v1 /zen gate and
// the newer /inference gate validate tools differently. Any failure, including
// an HTTP error response, advances to the next upstream or proxy. Only a
// successful response ends the anonymous phase; exhausting the proxy cursor
// returns control to the preferred authenticated tiers.
//
// fingerprint scopes cooldown to this request: a cooling node still serves a
// turn whose content differs from the one that triggered it, so one quota event
// no longer freezes unrelated traffic on the same proxy. It is also what keeps
// NextForRequest's "everything healthy is cooling for THIS turn" answer
// meaningful — see the last-resort pass there and merge decision D2.
func (g *Gateway) doAnonymousUpstream(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, attemptOffset int, fingerprint string) (*http.Response, error, int) {
	var lastResponse *http.Response
	var lastErr error
	cursor := g.anonymous.CursorFor(ids.Session)
	limit := g.anonymous.Len()
	if g.anonymous.HasPool() && g.cfg.Retry.MaxAttempts > limit {
		limit = g.cfg.Retry.MaxAttempts
	}
	proxies := 0
	attempts := 0
	rawBody := bodies[config.TierZen]
	if len(rawBody) == 0 {
		return nil, errors.New("no prepared Zen request body"), 0
	}
	// The anonymous free tier only serves agent-shaped streaming requests
	// carrying the required tool definitions (anything else is rejected with 403
	// FreeTierError). Normalize the wire body per upstream gate (v1 /zen
	// vs /inference); the gateway collapses the stream back when the downstream
	// client asked for a plain JSON reply. Key tiers keep their original bodies.
	protocol := route.ProtocolFor(config.TierZen)
	upstreams := g.anonymousUpstreams()
	if len(upstreams) == 0 {
		// Reachable only when upstream.anonymous points at the v1 origin while
		// the lane order leaves "legacy" out (see anonymousUpstreams). Report an
		// unusable route instead of scanning the cursor for attempts that cannot
		// exist; the failover script keys on the 503 wording, so keep the
		// errProxyUnavailable wrap and log the config mistake for the operator.
		g.logger.Warn("anonymous lane has no usable upstream", "component", "upstream", "event", "anonymous_route_missing", "request_id", ids.Request, "anonymous", config.RedactURL(g.cfg.Upstream.Anonymous), "lane_order", strings.Join(g.cfg.Upstream.AnonymousLaneOrder, ","))
		return nil, fmt.Errorf("no anonymous upstream is configured: %w", errProxyUnavailable), 0
	}
	preparedBodies := make([][]byte, len(upstreams))
	toolAliases := make([]string, len(upstreams))
	shapedPhase := shapedDumpPhase(attemptOffset)
	for i, baseURL := range upstreams {
		preparedBodies[i], toolAliases[i] = prepareAnonymousBody(rawBody, protocol, models.IsV1Upstream(baseURL))
		// The shaped dump for this candidate is emitted inside the attempt loop,
		// not here: only attempted bytes reach the wire, and a dump of a
		// candidate that lost the race would make "shaped" a lie (see D5).
	}
	for proxies < limit {
		if ctx.Err() != nil {
			// The request-level budget is already gone. Stop scanning instead
			// of firing attempts that cannot succeed and then recording them
			// as failures against proxies that were never really tried.
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			break
		}
		node := cursor.NextForRequest(fingerprint)
		if node == nil {
			break
		}
		proxies++
		var nodeResp *http.Response
		var nodeErr error
		for index, baseURL := range upstreams {
			if ctx.Err() != nil {
				if lastErr == nil && lastResponse == nil {
					lastErr = ctx.Err()
				}
				break
			}
			attempts++
			if meta := telemetry.MetaFromContext(ctx); meta != nil {
				meta.Attempts = attemptOffset + attempts
				meta.Tier = string(config.TierZen)
			}
			setRequestShape(ctx, false, toolAliases[index])
			if lastResponse != nil {
				httpx.DrainAndClose(lastResponse.Body)
				lastResponse = nil
			}
			req, err := upstreamRequest(ctx, baseURL, protocol, preparedBodies[index], ids, anonymousZenKey)
			if err != nil {
				return nil, err, attempts
			}
			// D5: the anonymous lane holds one shaped body per candidate upstream and
			// only some of them are ever attempted, so the dump goes here — on the
			// wire path — not in the preparation loop above. Dumped there it would log
			// bytes for a candidate that lost the race and "shaped" would stop meaning
			// "what actually went out"; logged per attempt, the base it targets is
			// unambiguous.
			g.dumpOutboundBodyForUpstream(route, config.TierZen, preparedBodies[index], ids, shapedPhase, baseURL)
			if node.proxy.pool {
				req.Close = true
			}
			setRequestCredential(ctx, config.TierZen, "anonymous", "anonymous", true, node.proxy)
			started := time.Now()
			resp, err := doInferenceAttempt(g.inferenceClient(node.proxy, baseURL, route.ID), req, time.Duration(g.cfg.Performance.FirstEventTimeoutSeconds)*time.Second, g.attemptTimeout)
			duration := time.Since(started)
			if ctx.Err() != nil {
				// The parent budget expired while this attempt was in flight. Its
				// outcome says nothing about this proxy, so record nothing and stop
				// scanning.
				lastResponse, lastErr = resp, err
				if lastErr == nil && lastResponse == nil {
					lastErr = ctx.Err()
				}
				break
			}
			nodeResp, nodeErr = resp, err
			g.recordUpstreamAttempt(ctx, route, ids, attemptOffset+attempts, "anonymous", "anonymous", true, node.proxy, resp, err, duration)
			if err == nil && resp.StatusCode/100 == 2 {
				g.observeAnonymousResult(ctx, node, resp, nil, fingerprint)
				g.logger.Debug("anonymous upstream accepted request", "component", "upstream", "event", "anonymous_attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "upstream", config.RedactURL(baseURL), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", config.RedactURL(node.proxy.name), "status", resp.StatusCode, "duration_ms", duration.Milliseconds())
				return resp, nil, attempts
			}
			lastResponse = resp
			lastErr = asProxyUnavailable(node.proxy, err)
			if err != nil {
				g.logger.Debug("anonymous transport attempt failed", "component", "upstream", "event", "anonymous_attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "upstream", config.RedactURL(baseURL), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", config.RedactURL(node.proxy.name), "duration_ms", duration.Milliseconds(), "error", err)
				if isProxyFailure(err) {
					break
				}
			} else {
				g.logger.Debug("anonymous upstream returned an error response; trying the next upstream or proxy", "component", "upstream", "event", "anonymous_attempt_response_failed", "request_id", ids.Request, "attempt", attempts, "tier", config.TierZen, "upstream", config.RedactURL(baseURL), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", config.RedactURL(node.proxy.name), "status", resp.StatusCode, "duration_ms", duration.Milliseconds())
			}
			if index+1 < len(upstreams) && !node.proxy.healthy.Load() {
				break
			}
		}
		if ctx.Err() != nil {
			break
		}
		// Once per proxy, after every candidate upstream for it has been tried:
		// the inner loop may have fired several attempts, and a proxy carries
		// one cooldown state. Local's fingerprint argument rides along so the
		// cooldown it starts is scoped to this request.
		g.observeAnonymousResult(ctx, node, nodeResp, nodeErr, fingerprint)
	}
	if lastResponse != nil {
		return lastResponse, nil, attempts
	}
	if lastErr == nil {
		// No attempt was made, so the cursor was empty. Distinguish an active
		// cooldown — every healthy node sitting in its backoff window for THIS
		// request, which is a "come back later" the client must be told about —
		// from a genuinely unusable route, where 503 proxy_error is the honest
		// answer. coolingFor mirrors NextForRequest's skip logic exactly, so this
		// can only fire when that scan genuinely found nothing; it only counts
		// healthy nodes, so an all-unhealthy pool reports no cooldown and keeps
		// the transport wording.
		//
		// The two answers must never be merged: errAnonymousCooling is never
		// wrapped in errProxyUnavailable. writeCoolingResponse matches on the
		// type and answers 429 + Retry-After; wrapping it would let the 503
		// branch below swallow the backoff, and watchdog-clash.ps1 treats a 429
		// with the gateway prefix as "stop, the lane is backing off" while a 503
		// sends it switching nodes — for a cooldown that no node switch can
		// shorten, since the cooldown lives on the transport.
		if blocked, remain := g.anonymous.coolingFor(fingerprint); blocked && remain > 0 {
			lastErr = errAnonymousCooling{retryAfter: remain, cause: g.anonymous.coolCauseFor(fingerprint)}
		} else {
			lastErr = errors.New("no healthy anonymous proxies available")
			if !g.transports.hasHealthy() {
				lastErr = fmt.Errorf("no healthy anonymous proxies available: %w", errProxyUnavailable)
			}
			// Nothing explains this but an unusable route, so kick one immediate
			// async re-probe: a just-fixed proxy (e.g. the Clash node was
			// switched after the failure) recovers on the next request instead
			// of waiting out the degraded ticker. Throttled inside, and a no-op
			// while anything is healthy.
			g.kickProxyRecheck(ctx)
		}
	}
	return nil, lastErr, attempts
}

// anonymousUpstreams returns the ordered base URLs for the keyless free-tier
// lane, from UpstreamConfig.AnonymousLaneOrder.
//
// Upstream hardcoded [legacy, inference]. That is not adopted here: the local
// measurements in AGENTS.md only ever saw /inference pass, and a legacy-first
// default would put a doomed attempt in front of every anonymous request —
// one extra round trip each, and a 403 that watchdogCause reads as a quota
// verdict, which sends the failover script switching nodes for an endpoint
// choice no node switch can fix. So the order is configuration, defaulting to
// the pre-legacy behaviour. See merge decision D1.
//
// "inference" resolves to Upstream.Anonymous and is dropped when it is
// configured to the legacy base, so the two can never produce a duplicate
// attempt at the same origin. When the order resolves to no base at all the
// result is empty on purpose: the lane must never fall back to an origin the
// order excluded (D1). doAnonymousUpstream and the probe/watchdog consumers
// each report that as an unusable route instead.
func (g *Gateway) anonymousUpstreams() []string {
	legacy := strings.TrimRight(models.LegacyZenBase, "/")
	primary := strings.TrimRight(strings.TrimSpace(g.cfg.Upstream.Anonymous), "/")
	order := g.cfg.Upstream.AnonymousLaneOrder
	if len(order) == 0 {
		order = config.DefaultAnonymousLaneOrder()
	}
	upstreams := make([]string, 0, len(order))
	for _, lane := range order {
		base := ""
		switch lane {
		case config.LaneLegacy:
			base = legacy
		case config.LaneInference:
			base = primary
		}
		// An empty /inference base means NormalizeConfig has not filled it in
		// (or the operator set it to the legacy origin); skip it rather than
		// dialing "".
		if base == "" || strings.EqualFold(base, legacy) && lane != config.LaneLegacy {
			continue
		}
		upstreams = append(upstreams, base)
	}
	// No implicit legacy fallback: with the default ["inference"] order and
	// upstream.anonymous pointed at the v1 origin, this returns empty. Silently
	// dialing legacy here would resurrect exactly the v1-first behaviour D1
	// rejected, and on a doomed attempt watchdogCause reads as quota.
	return upstreams
}

// inferenceClient picks the client of proxy for a request for model to
// baseURL. Free-model traffic to the /zen and /inference lanes goes through
// the Bun client (see httpx.BunTransport), whose TLS ClientHello and HTTP/1.1
// header layout match the runtime of the official OpenCode clients; every
// other request, including the Go tier (/zen/go), keeps the regular client.
func (g *Gateway) inferenceClient(proxy *proxyTransport, baseURL, model string) *http.Client {
	lane := strings.ToLower(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if g.catalog.IsFreeModel(model) && (models.IsV1Upstream(baseURL) || strings.HasSuffix(lane, "/inference")) {
		return proxy.bun
	}
	return proxy.client
}

// v1ReadTool is the single tool name required by the legacy
// /zen/v1 free-tier gate. Unlike /inference, the /zen/v1 gate does not require
// a shell/edit/glob/grep toolset and does not validate tool descriptions or
// parameter schemas: a single tool named "read" is sufficient.
const v1ReadTool = "read"

// v1DeprecatedReadDescription is attached when the gateway must inject a
// placeholder "read" tool on the v1 /zen lane because the client
// request declared no read-related tool of its own. It explicitly tells the
// model that the tool is deprecated and must never be invoked.
const v1DeprecatedReadDescription = "DEPRECATED: This tool is deprecated and disabled. Do NOT call or reference this tool under any circumstances; ignore it completely."

// anonymousCoreTools are the tool names the /inference free-tier gate expects
// on an agent-shaped request. Only used on the /inference lane: the legacy
// /zen/v1 gate has its own, much weaker rule (see v1ReadTool).
//
// "shell" is the name OpenCode 2.x ships; the pre-2.x "bash" spelling is
// deliberately not injected on /inference because that gate validates the
// definition of every tool it recognises, and a "bash" definition the gateway
// synthesises does not match the one upstream expects. Since 2026-10-14 name
// presence is not enough on /inference either: the upstream compares each
// recognised tool's description and parameter schema against the official CLI's
// definitions byte-for-byte (minimal stand-ins 403), which is why the genuine
// definitions below are embedded.
//
// Measured 2026-10-15 against /inference, one variable at a time, 6s apart:
// a request is rejected with 403 FreeTierError unless "tools" is present,
// non-empty, and at least one entry carries a genuine OpenCode definition.
// One genuine tool is enough (a lone genuine read passes); stub definitions,
// unknown names, and an empty array all fail; and a recognised name carrying a
// client schema, or a genuine definition with a mutated description, passes as
// long as some other tool stays genuine. tool_choice is not part of the gate:
// none/auto/required/named all pass. The 403 is therefore a body-shape verdict,
// while 429 ("Rate limit exceeded") is the separate quota signal — do not read
// the 403 as "this exit IP is blocked".
//
// What the gate needs is one genuine definition, not five: a client schema on a
// recognised name passes as long as some other entry stays genuine. So the
// gateway appends the core names the client left out and never rewrites an entry
// the client did declare: a replaced definition makes the model call a tool with
// parameters the client does not implement, which is worse than the placeholder
// the note warns about. It is not measured that "bash", the pre-2.x spelling, is
// simply a name the tier does not recognise; the only evidence is that requests
// carrying it instead of "shell" were rejected, which is equally consistent with
// the gate wanting a recognised name present at all. Tool order is irrelevant
// and extra client tools are allowed.
var anonymousCoreTools = []string{"shell", "edit", "glob", "grep", "read"}

// anonymousGateSaturators are the names a genuine definition can be appended
// under when the client occupies every core name. Non-core, because the gate
// wants a core name present: measured 2026-10-15 against /inference, a genuine
// non-core definition next to a client core stub returns 200, while the same
// definition next to nothing, or next to a stub on an unrecognised name, is
// rejected with 403 FreeTierError. Tried in a fixed order and skipped when the
// client already claims the name, so the choice never moves between turns.
var anonymousGateSaturators = []string{"execute", "webfetch", "websearch", "question", "skill", "subagent"}

// opencodeToolsJSON holds the genuine OpenCode tool definitions (description
// and JSON schema) captured from the official client. At least one of them has
// to reach the free tier intact: a request whose tools are all stubs — even
// under the right names — is rejected with 403 FreeTierError. The gateway ships
// the real definitions for the tools it appends; whatever the client declared is
// forwarded exactly as declared.
//
// Upstream instead overwrote every recognised name with the genuine definition.
// That is not kept (merge decision D4): replacing a client-declared tool hands
// the model a schema the client does not implement — OpenCode spells its path
// argument "path" where most clients spell it "file_path" — and the mismatch
// surfaces as an execution-time "Unknown parameter(s)" from the client, which
// is strictly worse than a phantom tool the placeholder note warns about. These
// definitions are used only for what the gateway itself appends.
//
// The OS phrase inside the shell description is DELIBERATELY PINNED and must
// not be re-synced from upstream without measuring first. Read the history
// before changing this file — the next upstream sync will offer exactly this
// conflict again.
//
// The official CLI has no fixed "description it ships". It builds the sentence
// at runtime, substituting the platform and the name of whichever shell it
// picked: on Windows the preference order is [pwsh, powershell, git-bash,
// %COMSPEC%], taking the first one present. So there is no single correct
// string — on this host the real 2.0.32 CLI would emit "Windows using pwsh",
// not the "powershell" pinned here.
//
// What makes the pinned string right is measurement, not fidelity: on
// 2026-10-15 this exact Windows wording was measured passing the free tier's
// gate (200) on the /inference lane. Upstream re-synced the file from a Linux
// client on 2026-10-18 (commit b0a6584, "use the Linux shell tool description
// the client ships, so the anonymous fingerprint matches") and that change was
// reverted in f017ca2 — because its premise is contradicted by that same
// measurement. Had the gate held a Linux-captured reference, the Windows string
// would have failed. It passed. So the reference is not the Linux bytes, and
// swapping would trade a measured-passing string for a disproven one.
//
// Cost of diverging, stated plainly: carriesGenuineDefinition compares a
// client-declared entry against this file byte for byte, so a real CLI whose
// own shell resolves to pwsh — or to bash — is classified as not-genuine and
// falls through to the saturator branch. Bounded, because a real CLI declares
// all five core names and edit/glob/grep/read carry no OS text and still match.
//
// Blast radius if this file is flipped to the Linux wording: in the common case
// where a client declares edit/glob/grep/read but not shell, the appended shell
// is the ONLY genuine definition on the wire. If the gate wants that entry
// faithful, that is precisely the shape that flips back to 403 FreeTierError —
// and watchdog-clash.ps1 classifies a gate 403 as quota, which does not break
// the candidate loop, so the script would burn the whole pool re-checking
// nodes for a problem no node switch can fix.
//
// Known trade-off, accepted: a gateway running on Linux or macOS still injects
// this Windows wording, so an injected "shell" describes PowerShell there. The
// placeholder note warns the model off those definitions, and a wrong-but-
// plausible shell description is the lesser failure against a stable 403.
//
// The v1 /zen lane does not read this file at all: its gate wants a
// single tool named "read" and ignores descriptions and schemas entirely (see
// v1ReadTool).
//
//go:embed opencode_tools.json
var opencodeToolsJSON []byte

type opencodeToolDefinition struct {
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

var (
	opencodeToolsOnce sync.Once
	opencodeTools     map[string]opencodeToolDefinition
)

// knownOpencodeTool returns the genuine definition for an OpenCode tool name.
func knownOpencodeTool(name string) (opencodeToolDefinition, bool) {
	opencodeToolsOnce.Do(func() {
		definitions := map[string]opencodeToolDefinition{}
		if err := json.Unmarshal(opencodeToolsJSON, &definitions); err != nil {
			definitions = map[string]opencodeToolDefinition{}
		}
		opencodeTools = definitions
	})
	definition, ok := opencodeTools[name]
	return definition, ok
}

// prepareAnonymousBody returns a copy of body normalized for the anonymous
// free tier alongside any original client tool name that was aliased to "read"
// for the v1 /zen gate.
//
// On the v1 /zen gate (legacy == true), pre-injection of other tools is
// dropped: if the client declared a read-related tool (case-insensitive, e.g.
// "reAd", "Read_file", "readFile"), it is renamed in-place to "read" while
// keeping its original description and parameters, and its original name is
// returned so the gateway can restore it in the downstream response; if no
// read-related tool is present, a single deprecated "read" stub is injected.
//
// On the /inference gate (legacy == false), anonymousCoreTools with genuine
// CLI 2.x definitions are ensured. System One payloads are decision requests,
// not agent traffic, so they are forwarded verbatim.
func prepareAnonymousBody(body []byte, protocol wire.Protocol, legacy bool) ([]byte, string) {
	if protocol == wire.SystemOne {
		return body, ""
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return body, ""
	}
	changed := false
	if streaming, ok := payload["stream"].(bool); !ok || !streaming {
		payload["stream"] = true
		changed = true
	}
	if ensureAnonymousChatUsage(payload, protocol) {
		changed = true
	}
	var alias string
	if legacy {
		var toolChanged bool
		alias, toolChanged = ensureV1ReadTool(payload, protocol)
		if toolChanged {
			changed = true
		}
	} else if ensureAnonymousTools(payload, protocol) {
		changed = true
	}
	if !changed {
		return body, alias
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		// The alias is dropped with the failed re-encode: the body never left
		// the gateway, so there is nothing downstream to restore.
		return body, ""
	}
	return encoded, alias
}

// ensureAnonymousChatUsage keeps token usage available when a non-streaming
// request is forced onto the Chat SSE path. OpenAI-compatible Chat streams
// require stream_options.include_usage for the final usage event.
func ensureAnonymousChatUsage(payload map[string]any, protocol wire.Protocol) bool {
	if protocol != wire.Chat {
		return false
	}
	options, ok := payload["stream_options"].(map[string]any)
	if !ok {
		payload["stream_options"] = map[string]any{"include_usage": true}
		return true
	}
	includeUsage, ok := options["include_usage"].(bool)
	if ok && includeUsage {
		return false
	}
	options["include_usage"] = true
	return true
}

// ensureV1ReadTool satisfies the v1 /zen free-tier gate
// using only the single "read" tool requirement:
//  1. If the client already has an exact "read" tool, nothing is injected or
//     renamed.
//  2. If the client has a read-related tool (case-insensitive match such as
//     "reAd", "read_file", "ReadFile", etc.), that tool is renamed to "read"
//     (preserving its client description and schema), any matching tool_choice
//     or historical tool call in the request is rewritten to "read", and the
//     client's original tool name is returned so responses can be restored.
//  3. If the client has no read-related tool at all, a single "read" stub tool
//     is appended with a deprecated/ignore description so the model never
//     invokes it.
func ensureV1ReadTool(payload map[string]any, protocol wire.Protocol) (string, bool) {
	raw, exists := payload["tools"]
	if !exists {
		payload["tools"] = []any{v1DeprecatedReadTool(protocol)}
		return "", true
	}
	items, ok := raw.([]any)
	if !ok {
		return "", false
	}
	candidateIndex := -1
	candidateName := ""
	for index, item := range items {
		name := anonymousToolName(protocol, item)
		if name == "" {
			continue
		}
		if name == v1ReadTool {
			return "", false
		}
		if strings.EqualFold(name, v1ReadTool) {
			candidateIndex = index
			candidateName = name
			break
		}
		if candidateIndex < 0 && isV1ReadToolCandidate(name) {
			candidateIndex = index
			candidateName = name
		}
	}
	if candidateIndex >= 0 {
		setAnonymousToolName(protocol, items[candidateIndex], v1ReadTool)
		rewriteV1ToolReferences(payload, protocol, candidateName, v1ReadTool)
		payload["tools"] = items
		return candidateName, true
	}
	items = append(items, v1DeprecatedReadTool(protocol))
	payload["tools"] = items
	return "", true
}

// isV1ReadToolCandidate reports whether a client tool name represents a
// read-related tool (case-insensitive), such as "reAd", "read_file",
// "ReadFile", "file_read", or "view_file".
func isV1ReadToolCandidate(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	if lower == "" {
		return false
	}
	if lower == "read" {
		return true
	}
	normalized := strings.NewReplacer("_", "", "-", "", ".", "").Replace(lower)
	switch normalized {
	case "readfile", "fileread", "readfiles", "readtext", "readtextfile", "readcontent", "viewfile":
		return true
	}
	return strings.HasPrefix(lower, "read_") || strings.HasPrefix(lower, "read-") || strings.HasSuffix(lower, "_read") || strings.HasSuffix(lower, "-read")
}

// setAnonymousToolName updates the function name of a tool definition in-place
// while keeping all other client fields (description, parameters/input_schema,
// strict, etc.) intact.
func setAnonymousToolName(protocol wire.Protocol, item any, name string) {
	entry, ok := item.(map[string]any)
	if !ok {
		return
	}
	if protocol == wire.Chat {
		if function, ok := entry["function"].(map[string]any); ok {
			function["name"] = name
		}
		return
	}
	entry["name"] = name
}

// rewriteV1ToolReferences renames any named tool_choice and
// assistant tool-call history entries from fromName to toName so the outbound
// request stays self-consistent after a client read tool is aliased to "read".
func rewriteV1ToolReferences(payload map[string]any, protocol wire.Protocol, fromName, toName string) {
	if fromName == "" || fromName == toName {
		return
	}
	switch protocol {
	case wire.Chat:
		if choice, ok := payload["tool_choice"].(map[string]any); ok {
			if function, ok := choice["function"].(map[string]any); ok && jsonutil.StringAt(function, "name") == fromName {
				function["name"] = toName
			}
		}
		for _, rawMsg := range jsonutil.SliceAt(payload, "messages") {
			msg, _ := rawMsg.(map[string]any)
			for _, rawCall := range jsonutil.SliceAt(msg, "tool_calls") {
				call, _ := rawCall.(map[string]any)
				if function, ok := call["function"].(map[string]any); ok && jsonutil.StringAt(function, "name") == fromName {
					function["name"] = toName
				}
			}
		}
	case wire.Responses:
		if choice, ok := payload["tool_choice"].(map[string]any); ok && jsonutil.StringAt(choice, "name") == fromName {
			choice["name"] = toName
		}
		for _, rawItem := range jsonutil.SliceAt(payload, "input") {
			item, _ := rawItem.(map[string]any)
			if jsonutil.StringAt(item, "type") == "function_call" && jsonutil.StringAt(item, "name") == fromName {
				item["name"] = toName
			}
		}
	case wire.Anthropic:
		if choice, ok := payload["tool_choice"].(map[string]any); ok && jsonutil.StringAt(choice, "name") == fromName {
			choice["name"] = toName
		}
		for _, rawMsg := range jsonutil.SliceAt(payload, "messages") {
			msg, _ := rawMsg.(map[string]any)
			for _, rawBlock := range jsonutil.SliceAt(msg, "content") {
				block, _ := rawBlock.(map[string]any)
				if jsonutil.StringAt(block, "type") == "tool_use" && jsonutil.StringAt(block, "name") == fromName {
					block["name"] = toName
				}
			}
		}
	}
}

// v1DeprecatedReadTool builds the single placeholder "read" tool injected
// on the v1 /zen lane when the client request has no read-related tool
// of its own. Its description marks the tool as deprecated and instructs the
// model to ignore it completely.
func v1DeprecatedReadTool(protocol wire.Protocol) map[string]any {
	parameters := map[string]any{"type": "object", "properties": map[string]any{}}
	switch protocol {
	case wire.Chat:
		return map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        v1ReadTool,
				"description": v1DeprecatedReadDescription,
				"parameters":  parameters,
			},
		}
	case wire.Anthropic:
		return map[string]any{
			"name":         v1ReadTool,
			"description":  v1DeprecatedReadDescription,
			"input_schema": parameters,
		}
	default:
		return map[string]any{
			"type":        "function",
			"name":        v1ReadTool,
			"description": v1DeprecatedReadDescription,
			"parameters":  parameters,
		}
	}
}

// ensureAnonymousTools is the /inference gate's shaper. It runs only when the
// candidate base is NOT the v1 /zen lane; prepareAnonymousBody picks
// between this and ensureV1ReadTool. The two gates validate
// completely different things, so one shaper cannot serve both (merge
// decision D4).
//
// It makes the tool list pass the free tier's gate without
// touching what the client declared: the core names the client left out are
// appended with their genuine definitions, and every entry the client sent is
// forwarded verbatim. It reports whether the payload changed.
//
// One genuine definition satisfies the gate, so the minimum is always
// append-only. Replacing a client-declared core tool hands the model a schema
// the client does not implement — OpenCode spells its path argument "path"
// where most clients spell it "file_path" — and a wrong argument name fails at
// execution time, which is a worse outcome than the phantom tool the note warns
// about. The same reasoning covers a recognised name outside the core set
// (write, execute, webfetch, ...): the free tier accepts a client definition
// next to a genuine one, so there is nothing to gain by rewriting it.
//
// A missing, null or non-list "tools" is filled with the whole core set rather
// than forwarded: the gate rejects the request either way, so there is nothing
// to preserve. Upstream's parallel fix is adopted here — this used to pass nil,
// which resolves to the full core set too, but spelling it out keeps the
// invariant readable and immune to a future default change.
//
// A client can occupy every core name with its own schema and keep none of them
// genuine. That request still needs one genuine definition, and appending one
// under a name the client did not claim satisfies the gate (measured 2026-10-15)
// without touching anything the client sent — so that is what happens, and the
// gateway stops overwriting client tools entirely. Only a client that already
// claims every saturator name as well falls through to trading one entry, which
// keeps the "a genuine definition always reaches the upstream" invariant true
// without qualification.
//
// Whenever the gateway provides definitions of its own, a system-level note
// marks them as placeholders so the model does not emit calls the client cannot
// execute. The trade branch gets its own wording, because there the promise
// "your client declared it, so its parameters are the ones you must use" is
// exactly what the trade gave up.
func ensureAnonymousTools(payload map[string]any, protocol wire.Protocol) bool {
	items, ok := payload["tools"].([]any)
	if !ok {
		payload["tools"] = anonymousToolset(protocol, anonymousCoreTools)
		noteSystemSlot(payload, protocol, injectedToolNote())
		return true
	}
	present := make(map[string]bool, len(items))
	for _, item := range items {
		if name := anonymousToolName(protocol, item); name != "" {
			present[name] = true
		}
	}
	if missing := missingCoreTools(present); len(missing) > 0 {
		items = append(items, anonymousToolset(protocol, missing)...)
		payload["tools"] = items
		noteSystemSlot(payload, protocol, injectedToolNote())
		return true
	}
	if carriesGenuineDefinition(protocol, items) {
		return false
	}
	// Nothing genuine is present and nothing is missing, so the gate needs a real
	// definition. Appending one under a name the client did not claim costs the
	// client nothing but a placeholder it is told about in the note.
	for _, name := range anonymousGateSaturators {
		if present[name] {
			continue
		}
		items = append(items, anonymousTool(protocol, name))
		payload["tools"] = items
		noteSystemSlot(payload, protocol, injectedToolNote())
		return true
	}
	// Last resort, and it needs a client that declares every name above as well:
	// trade one entry for the real definition. The victim is the first core name
	// in the gateway's own order, never the client's last entry — client tool
	// order varies between turns, and a target that moves would churn the
	// prompt-cache prefix.
	for _, core := range anonymousCoreTools {
		for index, item := range items {
			if anonymousToolName(protocol, item) != core {
				continue
			}
			items[index] = anonymousTool(protocol, core)
			payload["tools"] = items
			noteSystemSlot(payload, protocol, tradedToolNote())
			return true
		}
	}
	return false
}

func isAnonymousCoreTool(name string) bool {
	for _, core := range anonymousCoreTools {
		if core == name {
			return true
		}
	}
	return false
}

func missingCoreTools(present map[string]bool) []string {
	missing := make([]string, 0, len(anonymousCoreTools))
	for _, name := range anonymousCoreTools {
		if !present[name] {
			missing = append(missing, name)
		}
	}
	return missing
}

// carriesGenuineDefinition reports whether one of the client's own entries is
// already the genuine OpenCode definition. That request satisfies the gate as it
// stands, so the gateway appends nothing to it and its system prompt stays its
// own.
func carriesGenuineDefinition(protocol wire.Protocol, items []any) bool {
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		definition, known := knownOpencodeTool(anonymousToolName(protocol, entry))
		if !known {
			continue
		}
		if anonymousToolDescription(protocol, entry) != definition.Description {
			continue
		}
		if sameJSONDocument(definition.Parameters, anonymousToolSchema(protocol, entry)) {
			return true
		}
	}
	return false
}

// anonymousToolDescription and anonymousToolSchema read a tool entry in the
// shape its protocol uses: Chat nests both under "function", Anthropic keeps the
// description at the top and the schema under "input_schema", and Responses
// keeps both at the top.
func anonymousToolDescription(protocol wire.Protocol, entry map[string]any) string {
	if protocol == wire.Chat {
		return jsonutil.StringAt(entry, "function", "description")
	}
	return jsonutil.StringAt(entry, "description")
}

func anonymousToolSchema(protocol wire.Protocol, entry map[string]any) any {
	switch protocol {
	case wire.Chat:
		return jsonutil.AnyAt(entry, "function", "parameters")
	case wire.Anthropic:
		return jsonutil.AnyAt(entry, "input_schema")
	default:
		return jsonutil.AnyAt(entry, "parameters")
	}
}

// sameJSONDocument compares two decoded JSON values through their canonical
// encoding, so key order and number spelling cannot make equal schemas look
// different.
func sameJSONDocument(left, right any) bool {
	leftBytes, err := json.Marshal(left)
	if err != nil {
		return false
	}
	rightBytes, err := json.Marshal(right)
	if err != nil {
		return false
	}
	return bytes.Equal(leftBytes, rightBytes)
}

// injectedToolNote marks the definitions the gateway itself appended as
// placeholders so the model does not emit calls the client cannot execute. It
// is written for the append-only case, where every entry the client sent is
// still the client's own, and it says so: a coding harness spells its path
// argument "file_path" where OpenCode spells it "path", and telling the model
// otherwise pushes it off the tools it can actually run.
// Measured 2026-10-15: before this note a client that declared no tools still
// received tool_calls for read (finish_reason=tool_calls, 1 of 1 request); with
// it a neutral question produced none. A prompt that explicitly tells the model
// to use a tool still gets one — that is the client asking, not a ghost call.
// The note is advisory, not enforced: a model can still call a placeholder, so
// the client has to survive one.
//
// The note names the whole core set rather than only what this request
// appended: a client whose tool declaration varies between turns would
// otherwise change the system prompt every turn and invalidate the upstream
// prompt-cache prefix. It also has to stay true for a client whose own tools
// carry the same names, which is why it credits the client's declarations
// explicitly: a coding harness spells its path argument "file_path" where
// OpenCode spells it "path", and telling the model those tools are placeholders
// pushes it away from the tools it can actually run.
//
// It is a system-level message rather than extra text inside a tool
// description on purpose: the free tier compares the definitions it
// recognises, so the genuine descriptions must stay byte-identical. Measured
// the same day: a client schema on a recognised name is accepted as long as
// some tool keeps a genuine definition, so a system-only change costs no 403.
func injectedToolNote() string {
	return "[gateway] This request passed through an API gateway that had to append some of its own " +
		"tool definitions (the names " + strings.Join(anonymousCoreTools, ", ") + ") because the upstream " +
		"endpoint requires at least one of them to be present. Definitions the gateway appended are " +
		"placeholders this client cannot execute: do not call them. Every tool your client declared is " +
		"real and keeps exactly the parameters it declared, including where its name is one of the " +
		"placeholders listed above. Prefer your client's own tools; if the only thing left is a " +
		"placeholder, answer directly instead of calling a placeholder."
}

// tradedToolNote is the wording for the one case where the gateway supplies a
// definition in place of the client's own entry. It must not promise that the
// client's parameters survived — that is precisely what the trade gave up — and
// it must not describe the action as an append, because nothing was appended.
//
// It is a system-level message for the same reason as injectedToolNote: the
// free tier compares the definitions it recognises, so the genuine descriptions
// must stay byte-identical.
func tradedToolNote() string {
	return "[gateway] This request passed through an API gateway that had to supply one of its own " +
		"tool definitions (the names " + strings.Join(anonymousCoreTools, ", ") + ") because the upstream " +
		"endpoint requires at least one of them to be present. Most of them are placeholders this client " +
		"cannot execute: do not call them. Every tool your client declared is real, except one: the " +
		"gateway replaced that tool's definition, so call it with the parameters it now declares and do " +
		"not assume they match your client's usual names. Prefer your client's own tools; if the only " +
		"thing left is a placeholder, answer directly instead of calling a placeholder."
}

// noteSystemSlot merges note into the payload's system slot, falling
// back to a fresh slot when the existing one is not plain text. A slot whose
// value is null counts as empty; one holding some other unusable value is left
// alone, and the request goes out without a note rather than losing client data.
func noteSystemSlot(payload map[string]any, protocol wire.Protocol, note string) {
	switch protocol {
	case wire.Anthropic:
		noteAnthropicSystem(payload, note)
	case wire.Responses:
		noteResponsesSystem(payload, note)
	default:
		noteChatSystem(payload, note)
	}
}

func noteChatSystem(payload map[string]any, note string) {
	messages, ok := payload["messages"].([]any)
	if !ok {
		return
	}
	for _, item := range messages {
		entry, ok := item.(map[string]any)
		if !ok || !isSystemRole(jsonutil.StringAt(entry, "role")) {
			continue
		}
		if text, ok := entry["content"].(string); ok {
			if text == "" {
				entry["content"] = note
			} else {
				entry["content"] = text + "\n\n" + note
			}
			return
		}
	}
	payload["messages"] = append([]any{map[string]any{"role": "system", "content": note}}, messages...)
}

func noteAnthropicSystem(payload map[string]any, note string) {
	switch existing := payload["system"].(type) {
	case nil:
		payload["system"] = note
	case string:
		if existing != "" {
			existing += "\n\n"
		}
		payload["system"] = existing + note
	case []any:
		payload["system"] = append(existing, map[string]any{"type": "text", "text": note})
	}
}

func noteResponsesSystem(payload map[string]any, note string) {
	if existing, ok := payload["instructions"]; ok && existing == nil {
		payload["instructions"] = note
		return
	}
	if text, ok := payload["instructions"].(string); ok {
		if text == "" {
			payload["instructions"] = note
		} else {
			payload["instructions"] = text + "\n\n" + note
		}
		return
	}
	if input, ok := payload["input"].([]any); ok {
		for _, item := range input {
			entry, ok := item.(map[string]any)
			if !ok || !isSystemRole(jsonutil.StringAt(entry, "role")) {
				continue
			}
			if text, ok := entry["content"].(string); ok && text != "" {
				entry["content"] = text + "\n\n" + note
				return
			}
		}
	}
	if _, exists := payload["instructions"]; !exists {
		payload["instructions"] = note
	}
}

func isSystemRole(role string) bool {
	return role == "system" || role == "developer"
}

func anonymousToolName(protocol wire.Protocol, item any) string {
	entry, ok := item.(map[string]any)
	if !ok {
		return ""
	}
	if protocol == wire.Chat {
		return jsonutil.StringAt(entry, "function", "name")
	}
	return jsonutil.StringAt(entry, "name")
}

// anonymousToolset returns the wire-shaped genuine CLI 2.x definitions for the
// given tool names on the /inference gate.
func anonymousToolset(protocol wire.Protocol, names []string) []any {
	if names == nil {
		names = anonymousCoreTools
	}
	tools := make([]any, 0, len(names))
	for _, name := range names {
		if tool := anonymousTool(protocol, name); tool != nil {
			tools = append(tools, tool)
		}
	}
	return tools
}

// anonymousTool renders one core tool in the shape the given protocol expects,
// using the genuine OpenCode CLI 2.x definition whenever one is known.
// SystemOne payloads never reach this path (prepareAnonymousBody forwards them
// verbatim), so it falls back to the Responses/default shape.
func anonymousTool(protocol wire.Protocol, name string) map[string]any {
	description := "Agent tool " + name
	parameters := map[string]any{"type": "object", "properties": map[string]any{}}
	if definition, ok := knownOpencodeTool(name); ok {
		description = definition.Description
		if definition.Parameters != nil {
			parameters = definition.Parameters
		}
	}
	switch protocol {
	case wire.Chat:
		return map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        name,
				"description": description,
				"parameters":  parameters,
			},
		}
	case wire.Anthropic:
		return map[string]any{
			"name":         name,
			"description":  description,
			"input_schema": parameters,
		}
	default:
		return map[string]any{
			"type":        "function",
			"name":        name,
			"description": description,
			"parameters":  parameters,
		}
	}
}

// requestBodyDumpLimit caps how much of an outbound body is written to the log.
const requestBodyDumpLimit = 64 << 10

// dumpOutboundBody records one request body when logging.dump_request_bodies is
// enabled. It exists so retry, translation and tool-shaping problems (stale
// rs_* references, re-encoded history, tier re-encoding, appended core tool
// definitions, the injected placeholder note) can be diagnosed without patching
// the binary. Credentials travel in headers and never in the body. The logger
// redacts configured secrets, but conversation content can still be sensitive.
// Reference IDs are retained for replay diagnostics.
//
// phase says which bytes these are. "client" is what the client sent, before the
// lane shapes the body; "shaped" is what goes on the wire. Both are logged so the
// pair can be compared: an equal sha256 proves the lane changed nothing, and a
// differing one localises the change to one transformation. Logging only the
// client-side body hides every shaping step, and that is exactly where the tool
// definitions and the placeholder note live.
func (g *Gateway) dumpOutboundBody(route models.Route, tier config.Tier, body []byte, ids identity.RequestIDs, phase string) {
	g.dumpOutboundBodyForUpstream(route, tier, body, ids, phase, "")
}

// dumpOutboundBodyForUpstream is dumpOutboundBody plus the candidate upstream
// base the bytes are destined for, on the lanes that have more than one.
//
// The anonymous lane prepares one shaped body per entry of anonymousUpstreams
// and only one of them can reach the wire, so phase alone cannot say which
// bytes actually went out. Without the base, a "shaped" line is a complete,
// well-formed log entry describing the lane that lost — see merge decision D5.
func (g *Gateway) dumpOutboundBodyForUpstream(route models.Route, tier config.Tier, body []byte, ids identity.RequestIDs, phase, upstreamBase string) {
	if !g.cfg.Logging.DumpRequestBodies || len(body) == 0 {
		return
	}
	sum := sha256.Sum256(body)
	truncated := len(body) > requestBodyDumpLimit
	payload := body
	if truncated {
		payload = body[:requestBodyDumpLimit]
	}
	args := []any{"component", "upstream", "event", "upstream_request_body",
		"request_id", ids.Request, "model", route.ID, "tier", tier, "protocol", route.ProtocolFor(tier),
		"phase", phase, "bytes", len(body), "sha256", hex.EncodeToString(sum[:8]),
		"truncated", truncated, "body", string(payload)}
	if upstreamBase != "" {
		args = append(args, "upstream", upstreamBase)
	}
	g.logger.Debug("outbound upstream request body", args...)
}

// dumpClientBodies logs the per-tier bodies a request arrived with, before any
// lane shaping. See dumpOutboundBody for the phase vocabulary.
func (g *Gateway) dumpClientBodies(route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, attemptOffset int) {
	if !g.cfg.Logging.DumpRequestBodies || len(bodies) == 0 {
		return
	}
	phase := "client"
	if attemptOffset > 0 {
		phase = "client-retry"
	}
	tiers := make([]string, 0, len(bodies))
	for tier := range bodies {
		tiers = append(tiers, string(tier))
	}
	sort.Strings(tiers)
	for _, name := range tiers {
		g.dumpOutboundBody(route, config.Tier(name), bodies[config.Tier(name)], ids, phase)
	}
}

// shapedDumpPhase names the shaped-phase line so a retry stays distinguishable
// from the first attempt, the way the client-side phase does.
func shapedDumpPhase(attemptOffset int) string {
	if attemptOffset > 0 {
		return "shaped-retry"
	}
	return "shaped"
}

// shapeKeyBody normalizes key-tier free-model wire bodies to agent shape
// (stream + required tools), mirroring the anonymous lane. Upstream now
// rejects non-agent-shaped free-tier requests on every lane with 403
// FreeTierError; paid models keep their original bodies. It reports the
// shaped body, any legacy "read" tool alias that must be restored in the
// response, and whether the body changed so the gateway can collapse the SSE
// stream a non-streaming client receives back.
func (g *Gateway) shapeKeyBody(body []byte, route models.Route, tier config.Tier) ([]byte, string, bool) {
	if !g.catalog.IsFreeModel(route.ID) {
		return body, "", false
	}
	baseURL := g.cfg.Upstream.Zen
	if tier == config.TierGo {
		baseURL = g.cfg.Upstream.Go
	}
	shaped, alias := prepareAnonymousBody(body, route.ProtocolFor(tier), models.IsV1Upstream(baseURL))
	return shaped, alias, !bytes.Equal(shaped, body)
}

// key. It performs no failover at all — no anonymous lane, no other key and no
// other tier — so the outcome describes that one key. It also leaves pool state
// untouched on purpose: a diagnostic request must never cool a production key or
// evict a proxy, which is exactly what made a bad Playground request degrade
// live traffic before.
func (g *Gateway) doSelectedKeyUpstream(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, override DebugKeyOverride, attemptOffset int) (*http.Response, error, int) {
	nodes := g.zenNodes
	baseURL := g.cfg.Upstream.Zen
	if override.Tier == config.TierGo {
		nodes = g.goNodes
		baseURL = g.cfg.Upstream.Go
	}
	node := nodes.NodeByID(override.KeyID)
	if node == nil {
		return nil, fmt.Errorf("selected %s key is no longer configured", override.Tier), 0
	}
	body := bodies[override.Tier]
	if len(body) == 0 {
		return nil, fmt.Errorf("no prepared %s request body", override.Tier), 0
	}
	shaped, alias, changed := g.shapeKeyBody(body, route, override.Tier)
	if changed {
		body = shaped
	}
	setRequestShape(ctx, changed, alias)
	g.dumpOutboundBody(route, override.Tier, body, ids, shapedDumpPhase(attemptOffset))
	proxy := nodes.Proxy(node)
	if proxy == nil {
		return nil, errors.New("selected upstream key has no proxy binding"), 0
	}
	if meta := telemetry.MetaFromContext(ctx); meta != nil {
		meta.Attempts = attemptOffset + 1
		meta.Tier = string(override.Tier)
	}
	keyID := config.KeyDisplayID(node.key)
	setRequestCredential(ctx, override.Tier, keyID, "key", false, proxy)
	req, err := upstreamRequest(ctx, baseURL, route.ProtocolFor(override.Tier), body, ids, node.key)
	if err != nil {
		return nil, err, 0
	}
	if proxy.pool {
		req.Close = true
	}
	started := time.Now()
	resp, err := doInferenceAttempt(g.inferenceClient(proxy, baseURL, route.ID), req, time.Duration(g.cfg.Performance.FirstEventTimeoutSeconds)*time.Second, g.attemptTimeout)
	duration := time.Since(started)
	g.recordUpstreamAttempt(ctx, route, ids, attemptOffset+1, keyID, "key", false, proxy, resp, err, duration)
	if err != nil {
		return nil, asProxyUnavailable(proxy, err), 1
	}
	return resp, nil, 1
}

func (g *Gateway) doKeyUpstream(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs, attemptOffset int) (*http.Response, error, int) {
	var lastResponse *http.Response
	var lastErr error
	nodes := g.zenNodes
	baseURL := g.cfg.Upstream.Zen
	if route.Tier == config.TierGo {
		nodes = g.goNodes
		baseURL = g.cfg.Upstream.Go
	}
	cursor := nodes.CursorFor(ids.Session)
	if nodes.Len() == 0 {
		return nil, fmt.Errorf("no %s nodes configured", route.Tier), 0
	}
	attempts := 0
	body := bodies[route.Tier]
	if len(body) == 0 {
		return nil, fmt.Errorf("no prepared %s request body", route.Tier), 0
	}
	shaped, alias, changed := g.shapeKeyBody(body, route, route.Tier)
	if changed {
		body = shaped
	}
	setRequestShape(ctx, changed, alias)
	g.dumpOutboundBody(route, route.Tier, body, ids, shapedDumpPhase(attemptOffset))
	for attempts < g.cfg.Retry.MaxAttempts {
		if ctx.Err() != nil {
			// Same guard as the anonymous loop: an exhausted request budget
			// must not be spent rotating keys that were never really tried.
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			break
		}
		node := cursor.Next()
		if node == nil {
			break
		}
		attempts++
		// Keep the request-level trace synchronized with the attempt that is
		// about to be sent. Only the redacted key suffix is retained.
		if meta := telemetry.MetaFromContext(ctx); meta != nil {
			meta.Attempts = attemptOffset + attempts
			meta.Tier = string(route.Tier)
		}
		if lastResponse != nil {
			httpx.DrainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		req, err := upstreamRequest(ctx, baseURL, route.Protocol, body, ids, node.key)
		if err != nil {
			return nil, err, attempts
		}
		proxy := nodes.Proxy(node)
		if proxy == nil {
			lastErr = errors.New("upstream key has no proxy binding")
			break
		}
		if proxy.pool {
			req.Close = true
		}
		keyID := config.KeyDisplayID(node.key)
		setRequestCredential(ctx, route.Tier, keyID, "key", false, proxy)
		attemptStarted := time.Now()
		resp, err := doInferenceAttempt(g.inferenceClient(proxy, baseURL, route.ID), req, time.Duration(g.cfg.Performance.FirstEventTimeoutSeconds)*time.Second, g.attemptTimeout)
		attemptDuration := time.Since(attemptStarted)
		if ctx.Err() != nil {
			// The request budget expired while this attempt was in flight. A
			// cancelled context says nothing about the key or the proxy: without
			// this guard the instant DeadlineExceeded would cool a healthy key
			// and evict a healthy proxy from the pool.
			lastResponse, lastErr = resp, err
			if lastErr == nil && lastResponse == nil {
				lastErr = ctx.Err()
			}
			break
		}
		g.observeKeyResult(ctx, nodes, node, proxy, resp, err)
		g.recordUpstreamAttempt(ctx, route, ids, attemptOffset+attempts, keyID, "key", false, proxy, resp, err, attemptDuration)
		if err == nil && resp.StatusCode/100 == 2 {
			g.logger.Debug("upstream accepted request", "component", "upstream", "event", "attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", keyID, "proxy", config.RedactURL(proxy.name), "status", resp.StatusCode, "duration_ms", attemptDuration.Milliseconds())
			return resp, nil, attempts
		}
		// Request-shape errors are deterministic and must leave this tier without
		// rotating through unrelated keys. The outer route may still try the next
		// tier in prefer order. Authentication, throttling, server, and transport
		// failures remain retryable inside this tier.
		if isNonRetryableClientResponse(resp, err) {
			g.logger.Debug("upstream rejected a non-retryable request", "component", "upstream", "event", "attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", keyID, "status", resp.StatusCode, "proxy", config.RedactURL(proxy.name), "duration_ms", attemptDuration.Milliseconds())
			return resp, nil, attempts
		}
		lastResponse = resp
		lastErr = asProxyUnavailable(proxy, err)
		if err != nil {
			g.logger.Debug("upstream transport attempt failed", "component", "upstream", "event", "attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", keyID, "proxy", config.RedactURL(proxy.name), "duration_ms", attemptDuration.Milliseconds(), "error", err)
		} else {
			g.logger.Debug("upstream returned a retryable response", "component", "upstream", "event", "attempt_retryable_response", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", keyID, "status", resp.StatusCode, "proxy", config.RedactURL(proxy.name), "duration_ms", attemptDuration.Milliseconds())
		}
	}
	if lastResponse != nil {
		return lastResponse, nil, attempts
	}
	return nil, lastErr, attempts
}

// Diagnostics use the same routing and transports as production, but must not
// update cooldowns, health, or bindings, including after successful attempts.
func (g *Gateway) observeKeyResult(ctx context.Context, nodes *upstreamNodes, node *upstreamNode, proxy *proxyTransport, resp *http.Response, err error) {
	if isDiagnosticRequest(ctx) {
		return
	}
	status := upstreamStatus(resp)
	proxyFailed := g.syncProxyResult(ctx, proxy, status, err)
	if err == nil && status/100 == 2 || isNonRetryableClientResponse(resp, err) {
		nodes.MarkSuccess(node)
		return
	}
	if !proxyFailed || nodes.Proxy(node) == proxy {
		nodes.MarkFailure(node, resp, err)
	}
}

func (g *Gateway) observeAnonymousResult(ctx context.Context, node *anonymousNode, resp *http.Response, err error, fingerprint string) {
	if isDiagnosticRequest(ctx) {
		return
	}
	status := upstreamStatus(resp)
	g.syncProxyResult(ctx, node.proxy, status, err)
	if err == nil && status/100 == 2 {
		g.anonymous.MarkSuccess(node)
	} else {
		// watchdogCause only labels the failure for the watchdog feed; the
		// upstream error document itself is still passed through untouched
		// (the label peeks the body at most and puts it back byte for byte).
		g.anonymous.markUserFailure(node, resp, err, watchdogCause(resp, err), fingerprint)
	}
}

func upstreamStatus(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func setRequestCredential(ctx context.Context, tier config.Tier, keyID, channel string, anonymous bool, proxy *proxyTransport) {
	meta := telemetry.MetaFromContext(ctx)
	if meta == nil {
		return
	}
	meta.Tier = string(tier)
	meta.KeyID = keyID
	meta.Channel = channel
	meta.Anonymous = anonymous
	meta.Proxy = ""
	if proxy != nil {
		meta.Proxy = config.RedactURL(proxy.name)
	}
}

func requestCredential(ctx context.Context) (string, string, bool) {
	meta := telemetry.MetaFromContext(ctx)
	if meta == nil {
		return "", "", false
	}
	return meta.KeyID, meta.Channel, meta.Anonymous
}

func (g *Gateway) recordUpstreamAttempt(ctx context.Context, route models.Route, ids identity.RequestIDs, attempt int, keyID, channel string, anonymous bool, proxy *proxyTransport, resp *http.Response, err error, duration time.Duration) {
	status := upstreamStatus(resp)
	success := err == nil && status >= 200 && status < 300
	outcome := "retryable_failure"
	if success {
		outcome = "success"
	} else if err != nil {
		outcome = "transport_error"
	} else if isNonRetryableClientResponse(resp, nil) {
		outcome = "rejected"
	}
	if meta := telemetry.MetaFromContext(ctx); meta != nil {
		meta.AttemptOutcome = outcome
		meta.Protocol = route.Protocol
		// Append a per-attempt leg (key:status:ms, anonymized for the
		// public lane) so the request log shows the whole attempt chain
		// at a glance. Only the redacted key suffix is retained.
		label := keyID
		if anonymous {
			label = "anon"
		}
		leg := label + ":"
		if err != nil {
			leg += classifyAttemptError(err)
		} else {
			leg += strconv.Itoa(status)
		}
		leg += ":" + strconv.FormatInt(max(duration.Milliseconds(), 0), 10) + "ms"
		meta.Legs = append(meta.Legs, leg)
	}
	if g.monitor == nil {
		return
	}
	proxyName := "unavailable"
	if proxy != nil {
		proxyName = config.RedactURL(proxy.name)
	}
	g.monitor.RecordAttempt(telemetry.UpstreamAttempt{
		Time: time.Now().UTC(), RequestID: ids.Request, Model: route.ID, Tier: string(route.Tier), Attempt: attempt,
		KeyID: keyID, Channel: channel, Anonymous: anonymous, Proxy: proxyName, Status: status,
		DurationMS: max(duration.Milliseconds(), 0), Success: success, Outcome: outcome,
	})
}

// upstreamRequest builds the POST for one attempt: baseURL plus the protocol's
// /v1/... route, carrying body. The header set follows the lane: setV1Headers
// when models.IsV1Upstream(baseURL), setV2Headers otherwise. key is sent as
// x-api-key (with anthropic-version) on the Anthropic protocol and as a Bearer
// token on every other one; the anonymous lane passes anonymousZenKey.
func upstreamRequest(ctx context.Context, baseURL string, protocol wire.Protocol, body []byte, ids identity.RequestIDs, key string) (*http.Request, error) {
	endpoint := strings.TrimRight(baseURL, "/") + wire.Path(protocol)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if models.IsV1Upstream(baseURL) {
		setV1Headers(req.Header, protocol, body, ids)
	} else {
		setV2Headers(req.Header, protocol, ids)
	}
	if protocol == wire.Anthropic {
		req.Header.Set("x-api-key", key)
		req.Header.Set("anthropic-version", "2023-06-01")
	} else {
		// The anonymous credential is the literal token "public". The 2026-10-14
		// gate rejects keyless requests (an absent header used to pass) and is
		// case-sensitive: "Bearer PUBLIC" 401s.
		req.Header.Set("Authorization", "Bearer "+key)
	}
	return req, nil
}

// setV1Headers renders the v1 lane's header set, that of an OpenCode 1.19.3
// model request on its opencode provider, as captured from the official
// release: the AI SDK User-Agent of the package serving the protocol, both
// session headers, the user message id as x-opencode-request (a fresh
// ascending msg_ id when none was derived), the git root commit as the
// project, the canonical parent session under both parent headers, and only
// the anthropic-beta flags the AI SDK derives from the body. The v1 client
// sends no x-session-affinity or X-Session-Id to this provider.
func setV1Headers(header http.Header, protocol wire.Protocol, body []byte, ids identity.RequestIDs) {
	header.Set("Accept", "*/*")
	header.Set("User-Agent", httpx.V1ModelUserAgent(v1ProviderUtils(protocol)))
	header.Set("x-opencode-client", "cli")
	header.Set("x-opencode-session", ids.Session)
	header.Set("x-opencode-session-id", ids.Session)
	message := ids.Message
	if message == "" {
		message = identity.AscendingID("msg", time.Now())
	}
	header.Set("x-opencode-request", message)
	header.Set("x-opencode-project", v1ProjectID(ids.Project))
	if ids.ParentSession != "" {
		parent := identity.CanonicalSessionID(ids.ParentSession)
		header.Set("x-opencode-parent-session-id", parent)
		header.Set("x-parent-session-id", parent)
	}
	if protocol == wire.Anthropic {
		if betas := v1AnthropicBetas(body); betas != "" {
			header.Set("anthropic-beta", betas)
		}
	}
}

// setV2Headers renders the OpenCode 2.x CLI header set the v2 gates
// fingerprint: the "opencode/<channel>/<version>/cli" User-Agent, every
// session header (x-opencode-session, x-opencode-session-id,
// x-session-affinity, X-Session-Id) set to the same descending-encoded id,
// and a bare 40-character hex x-opencode-project. The v2 gate rejects
// x-opencode-request and canonical prj_* project ids with 403 FreeTierError,
// so neither is sent. Anthropic requests carry a fixed anthropic-beta set.
func setV2Headers(header http.Header, protocol wire.Protocol, ids identity.RequestIDs) {
	header.Set("Accept", "application/json, text/event-stream")
	header.Set("x-opencode-client", "cli")
	header.Set("x-opencode-session", ids.Session)
	header.Set("x-session-affinity", ids.Session)
	header.Set("X-Session-Id", ids.Session)
	header.Set("User-Agent", httpx.V2UserAgent())
	header.Set("x-opencode-session-id", ids.Session)
	header.Set("x-opencode-project", v2ProjectID(ids.Project))
	if ids.ParentSession != "" {
		header.Set("x-parent-session-id", ids.ParentSession)
	}
	if protocol == wire.Anthropic {
		header.Set("anthropic-beta", "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14")
	}
}

// v1ProviderUtils names the provider-utils version of the AI SDK package the
// OpenCode 1.x client routes a protocol through, for the v1 User-Agent. System
// One has no v1 counterpart and reports the chat package.
func v1ProviderUtils(protocol wire.Protocol) string {
	switch protocol {
	case wire.Responses:
		return httpx.V1ResponsesSDK
	case wire.Anthropic:
		return httpx.V1AnthropicSDK
	default:
		return httpx.V1ChatSDK
	}
}

// gitCommitPattern matches the git root commit hash OpenCode 1.x uses as its
// project id.
var gitCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

// v1ProjectID renders the project header the way the OpenCode 1.x client
// does: the git root commit hash of the project, or "global" outside a git
// repository. A genuine value from a v1 client is kept verbatim; any other
// project is hashed into the commit shape (see v2ProjectID).
func v1ProjectID(project string) string {
	if project == "global" || gitCommitPattern.MatchString(project) {
		return project
	}
	return v2ProjectID(project)
}

// v1AnthropicBetaRequest holds the Messages body fields the AI SDK
// anthropic provider derives beta flags from. Fields whose JSON type varies
// stay raw so one unexpected value cannot hide the others.
type v1AnthropicBetaRequest struct {
	Model             string            `json:"model"`
	MCPServers        []json.RawMessage `json:"mcp_servers"`
	ContextManagement *struct {
		Edits []struct {
			Type string `json:"type"`
		} `json:"edits"`
	} `json:"context_management"`
	Container    json.RawMessage `json:"container"`
	OutputConfig struct {
		TaskBudget json.RawMessage `json:"task_budget"`
	} `json:"output_config"`
	Speed     json.RawMessage `json:"speed"`
	Fallbacks json.RawMessage `json:"fallbacks"`
	Thinking  struct {
		BlockBinding json.RawMessage `json:"block_binding"`
	} `json:"thinking"`
	Tools []struct {
		Type           string          `json:"type"`
		InputExamples  json.RawMessage `json:"input_examples"`
		AllowedCallers json.RawMessage `json:"allowed_callers"`
	} `json:"tools"`
}

// v1AnthropicToolBetas maps the server tool types of a Messages body to the
// beta flag the AI SDK anthropic provider sends with each.
var v1AnthropicToolBetas = map[string]string{
	"code_execution_20250522": "code-execution-2025-05-22",
	"code_execution_20250825": "code-execution-2025-08-25",
	"computer_20241022":       "computer-use-2024-10-22",
	"computer_20250124":       "computer-use-2025-01-24",
	"computer_20251124":       "computer-use-2025-11-24",
	"text_editor_20241022":    "computer-use-2024-10-22",
	"text_editor_20250124":    "computer-use-2025-01-24",
	"text_editor_20250429":    "computer-use-2025-01-24",
	"bash_20241022":           "computer-use-2024-10-22",
	"bash_20250124":           "computer-use-2025-01-24",
	"memory_20250818":         "context-management-2025-06-27",
	"web_fetch_20250910":      "web-fetch-2025-09-10",
	"web_fetch_20260209":      "code-execution-web-tools-2026-02-09",
	"web_search_20260209":     "code-execution-web-tools-2026-02-09",
	"advisor_20260301":        "advisor-tool-2026-03-01",
}

// v1AnthropicBetas returns the anthropic-beta value OpenCode 1.19.3 sends
// with a Messages body on the v1 lane, or "" when no flag applies. Its
// opencode provider sets no fixed betas: the AI SDK derives them from the
// request features (body features first, then tools, in the SDK's order), and
// per-tool eager_input_streaming has replaced the old
// fine-grained-tool-streaming flag.
func v1AnthropicBetas(body []byte) string {
	var request v1AnthropicBetaRequest
	// A field of an unexpected type fails alone; the rest still decode.
	_ = json.Unmarshal(body, &request)
	var betas []string
	add := func(beta string) {
		if !slices.Contains(betas, beta) {
			betas = append(betas, beta)
		}
	}
	if len(request.MCPServers) > 0 {
		add("mcp-client-2025-04-04")
	}
	if request.ContextManagement != nil {
		add("context-management-2025-06-27")
		for _, edit := range request.ContextManagement.Edits {
			if edit.Type == "compact_20260112" {
				add("compact-2026-01-12")
			}
		}
	}
	var container struct {
		Skills []json.RawMessage `json:"skills"`
	}
	if json.Unmarshal(request.Container, &container) == nil && len(container.Skills) > 0 {
		add("code-execution-2025-08-25")
		add("skills-2025-10-02")
		add("files-api-2025-04-14")
	}
	if isPresentJSON(request.OutputConfig.TaskBudget) {
		add("task-budgets-2026-03-13")
	}
	if string(request.Speed) == `"fast"` {
		add("fast-mode-2026-02-01")
	}
	var fallbacks []json.RawMessage
	if string(request.Fallbacks) == `"default"` {
		add("server-side-fallback-2026-07-01")
	} else if json.Unmarshal(request.Fallbacks, &fallbacks) == nil && len(fallbacks) > 0 {
		add("server-side-fallback-2026-06-01")
	}
	if isPresentJSON(request.Thinking.BlockBinding) {
		add("thinking-binding-controls-2026-08-01")
	}
	for _, tool := range request.Tools {
		if tool.Type == "" || tool.Type == "custom" {
			if anthropicSupportsStructuredOutput(request.Model) {
				add("structured-outputs-2025-11-13")
			}
			if isPresentJSON(tool.InputExamples) || isPresentJSON(tool.AllowedCallers) {
				add("advanced-tool-use-2025-11-20")
			}
		} else if beta := v1AnthropicToolBetas[tool.Type]; beta != "" {
			add(beta)
		}
	}
	return strings.Join(betas, ",")
}

func isPresentJSON(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

// claude3OrOlderPattern matches the Claude model families older than Claude 4
// (Claude Instant, Claude 2 and every Claude 3.x model) that the AI SDK treats
// as lacking structured output.
var claude3OrOlderPattern = regexp.MustCompile(`claude-(?:instant(?:-|$)|v?2(?:$|[-.:])|3(?:$|[-.]))`)

// anthropicSupportsStructuredOutput mirrors the AI SDK anthropic provider's
// model capability table, checked in the same order: the dated Claude 4.0
// releases and Claude 3 and older lack structured output, every other Claude
// model has it, and non-Claude models do not.
func anthropicSupportsStructuredOutput(model string) bool {
	for _, supported := range []string{
		"claude-opus-5", "claude-opus-4-8", "claude-opus-4-7", "claude-fable-5", "claude-sonnet-5",
		"claude-sonnet-4-6", "claude-opus-4-6", "claude-sonnet-4-5", "claude-opus-4-5", "claude-haiku-4-5",
		"claude-opus-4-1",
	} {
		if strings.Contains(model, supported) {
			return true
		}
	}
	if strings.Contains(model, "claude-sonnet-4-") || strings.Contains(model, "claude-opus-4-") ||
		strings.Contains(model, "claude-3-haiku") || claude3OrOlderPattern.MatchString(model) {
		return false
	}
	return strings.Contains(model, "claude-")
}

// v2ProjectID renders the project header in the shape the OpenCode 2.x CLI
// sends to the v2 gates: a bare 40-character lowercase hex id, here the first
// 20 bytes of a domain-separated SHA-256 of the project. The value itself is
// not checked upstream, only its shape, but hashing keeps one project mapped
// to one stable upstream project id. v1ProjectID reuses it to fit a
// non-genuine project into the git commit shape.
func v2ProjectID(project string) string {
	sum := sha256.Sum256([]byte("opencode-project\x00" + project))
	return hex.EncodeToString(sum[:20])
}

// isNonRetryableClientResponse reports whether resp is a 4xx that retrying on
// another key of the same tier cannot fix: every 4xx except 401, 403 and 429,
// which can be specific to the key. A transport error is never non-retryable.
func isNonRetryableClientResponse(resp *http.Response, err error) bool {
	return err == nil && resp != nil && resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusTooManyRequests
}
