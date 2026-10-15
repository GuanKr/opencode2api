package gateway

import (
	"sort"
	"sync"
	"time"
)

// staleMemo remembers conversations whose stored history carries dead
// reasoning references (a strip-retry was needed). dsh keeps replaying the
// same references every turn, so without memory each turn would burn one
// 400 round-trip before the self-heal. While remembered, the first attempt
// goes out pre-stripped; the memory expires so server-side healing or a dsh
// history rewrite resumes full replay on its own.
//
// The entry keeps a snapshot blacklist: the reasoning ids (plus the
// previous_response_id chain link) that were present in the failed request
// body at poison-discovery time — every one of them, because the upstream
// 400 names at most one and the gateway has no reliable way to tell which
// of them it considers dead. Later turns therefore strip that whole
// pre-mark reasoning prefix again, and what survives is the reasoning the
// failing body had never seen: the parts delivered after the mark. That is
// the whole point of the snapshot — without it a successful strip retry
// would have to be learned again from the next failing body, which by then
// also contains the healthy reasoning appended since.
//
// A strip retry that succeeds refreshes the window (keeping the old
// snapshot, never the larger current id set), so only 48h-idle entries
// expire; the next touch then lazily deletes the entry and re-probes the
// full shape, re-learning with one 400 if the poison persists. Worst case
// is bounded by the session cap below (idle entries self-release; the memo
// itself is process memory, cleared by restart or config hot-reload which
// rebuilds the Gateway).
// It stores only session IDs, opaque reasoning ids and timestamps — no
// request content. Zero value is ready to use, which also keeps
// test-constructed Gateways working.
type staleEntry struct {
	ts time.Time
	// ids holds the blacklisted reasoning item ids snapshotted from the
	// failed body — the whole reasoning set it carried, not only the id the
	// 400 named (see collectStaleReasoningIDs). Ids absent from this set
	// (i.e. reasoning that arrived after the mark) are kept. Nil together
	// with full=false and prev=="" means empty.
	ids map[string]struct{}
	// prev is the dead previous_response_id seen at snapshot time ("" when
	// the failed body carried none). Selective stripping drops the chain
	// link only when it still equals this value, so a fresh link issued
	// after the mark is preserved.
	prev string
	// full forces full stripping when the snapshot would be too large to
	// keep (or when no usable snapshot exists). It preserves the old
	// all-strip behavior for pathological histories.
	full bool
}

type staleMemo struct {
	mu   sync.Mutex
	last map[string]*staleEntry
	// ttl overrides staleMemoTTL when positive (tests only).
	ttl time.Duration
}

// effectiveTTL returns the test override when set, else the production TTL.
func (m *staleMemo) effectiveTTL() time.Duration {
	if m != nil && m.ttl > 0 {
		return m.ttl
	}
	return staleMemoTTL
}

// staleMemoTTL bounds how long one strip-retry keeps pre-stripping later
// turns: long enough to skip the doomed 400s across multi-day work (a
// session paused overnight resumes pre-stripped the next day), while
// refresh() on every successful strip retry keeps an actively-poisoned
// session's window moving. Entries idle past the TTL are deleted on next
// access, so abandoned sessions free their memory instead of lingering to
// the session-cap eviction.
const staleMemoTTL = 48 * time.Hour

const staleMemoMaxSessions = 4096

// staleMemoMaxIDsPerSession caps the per-session blacklist. A poisoned body
// carrying more reasoning ids than this falls back to full stripping so one
// giant history cannot blow up memo memory. Sizing: one reasoning item per
// thinking turn is the norm, and long agent sessions legitimately reach
// several hundred turns, so the cap keeps ample headroom above that. Cost
// per id is one short string plus map overhead (~a few hundred bytes), i.e.
// tens of KB for a few-hundred-id session; only actually-poisoned sessions
// hold entries, and the 4096-session cap plus oldest-half eviction bounds
// the total (worst case ~4M ids only if every session maxes its blacklist,
// on the order of hundreds of MB; idle entries self-release after the TTL
// above). Reaching even a fraction of that requires thousands of real
// poisoned upstream round-trips first, since an entry is only recorded
// after an actual 400 + successful strip retry.
const staleMemoMaxIDsPerSession = 1024

func (m *staleMemo) shouldPrestrip(session string) bool {
	if m == nil || session == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.last[session]
	if !ok {
		return false
	}
	ttl := m.effectiveTTL()
	// Idle past the TTL, the entry is deleted (lazy release): abandoned
	// sessions free memory on next touch, and the next turn re-probes the
	// full shape, re-learning with one 400 if the poison persists.
	if time.Since(entry.ts) > ttl {
		delete(m.last, session)
		return false
	}
	return true
}

// snapshot returns a copy of the blacklist for a session. The copy keeps
// callers from mutating memo state. ok is false when the session was never
// poisoned, was cleaned or evicted, or sat idle past the TTL (released
// lazily on this call, honoring the test override via effectiveTTL).
func (m *staleMemo) snapshot(session string) (ids map[string]struct{}, prev string, full bool, ok bool) {
	if m == nil || session == "" {
		return nil, "", false, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, exists := m.last[session]
	if !exists {
		return nil, "", false, false
	}
	if ttl := m.effectiveTTL(); time.Since(entry.ts) > ttl {
		delete(m.last, session)
		return nil, "", false, false
	}
	var copied map[string]struct{}
	if len(entry.ids) > 0 {
		copied = make(map[string]struct{}, len(entry.ids))
		for id := range entry.ids {
			copied[id] = struct{}{}
		}
	}
	return copied, entry.prev, entry.full, true
}

// noteStripped records the snapshot blacklist taken from the failed request
// body: every reasoning id that body carried (not just the one the 400
// named) plus the chain link seen then. An oversized id set degrades to
// full-strip mode so one giant history cannot blow up memo memory. An empty
// snapshot (no ids, no chain link) is intentionally retained: the failed
// body then carried only id-less reasoning blocks, and the entry keeps
// pre-stripping those on later turns instead of burning a 400 every time.
func (m *staleMemo) noteStripped(session string, ids []string, prev string) {
	if m == nil || session == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.last == nil {
		m.last = make(map[string]*staleEntry)
	}
	if len(m.last) >= staleMemoMaxSessions {
		// Drop the oldest half; sessions are cheap to re-learn with one 400.
		ordered := make([]string, 0, len(m.last))
		for id := range m.last {
			ordered = append(ordered, id)
		}
		sort.Slice(ordered, func(i, j int) bool { return m.last[ordered[i]].ts.Before(m.last[ordered[j]].ts) })
		for _, id := range ordered[:len(ordered)/2] {
			delete(m.last, id)
		}
	}
	entry := &staleEntry{ts: time.Now(), prev: prev}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		seen[id] = struct{}{}
	}
	if len(seen) > staleMemoMaxIDsPerSession {
		entry.full = true
	} else if len(seen) > 0 {
		entry.ids = seen
	}
	m.last[session] = entry
}

// refresh moves the prestrip window forward while keeping the existing
// blacklist. It is used when a strip retry (selective or full) succeeds:
// the poison is the same old history, so the snapshot must not be replaced
// by the current (larger) id set that includes healthy reasoning appended
// during the window. (Past the TTL there is nothing to refresh: the entry
// was already lazily released, and the turn re-learns via noteStripped.)
func (m *staleMemo) refresh(session string) {
	if m == nil || session == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.last[session]; ok {
		entry.ts = time.Now()
	}
}

func (m *staleMemo) noteClean(session string) {
	if m == nil || session == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.last, session)
}
