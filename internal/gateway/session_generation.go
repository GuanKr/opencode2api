package gateway

import (
	"sync"
	"time"
)

// sessionGenerationTTL is how long a rotated generation survives without being
// read. Generations are keyed by the generation-0 session id, which itself
// changes at UTC midnight, so this only bounds memory: a conversation quiet for
// longer than this starts again from generation 0.
const sessionGenerationTTL = 2 * time.Hour

// sessionSweepInterval bounds how often an access walks the whole generation
// map looking for idle entries.
const sessionSweepInterval = time.Minute

// sessionGenerations remembers how often each client conversation had to rotate
// its upstream session because the free tier answered a quota 429. The free
// tier scopes part of its allowance to the session id, so once a conversation
// exhausts it, the same id keeps being refused on every egress; rotating the id
// is the only way out, and the rotation must outlive the request that hit it so
// the next turn continues on the new session instead of falling back into the
// exhausted one.
//
// There is deliberately no cap on the generation: the operator controls how
// often it moves. The map is keyed by the generation-0 id, so a rotation is
// shared by every lane and tier of the conversation.
type sessionGenerations struct {
	mu  sync.Mutex
	now func() time.Time
	ttl time.Duration
	// sweepInterval throttles the TTL sweep: Generation runs on the hot path
	// of every free-model request (once per lane the conversation touches),
	// and a full-map walk under the store's single mutex on every call would
	// serialise every request behind the same lock once a gateway holds many
	// conversations.
	sweepInterval time.Duration
	lastSweep     time.Time
	gen           map[string]sessionGenerationEntry
}

type sessionGenerationEntry struct {
	generation int
	seen       time.Time
}

func newSessionGenerations() *sessionGenerations {
	return &sessionGenerations{
		now:           time.Now,
		ttl:           sessionGenerationTTL,
		sweepInterval: sessionSweepInterval,
		gen:           map[string]sessionGenerationEntry{},
	}
}

// Generation returns the generation a conversation must use now, or 0 when it
// never rotated. The call refreshes the entry's idle deadline.
func (s *sessionGenerations) Generation(key string) int {
	if s == nil || key == "" {
		return 0
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked(now)
	entry, ok := s.gen[key]
	if !ok {
		return 0
	}
	entry.seen = now
	s.gen[key] = entry
	return entry.generation
}

// Rotate advances the conversation's generation and returns the new value. The
// key is the generation-0 session id, never the rotated one, so successive
// rotations keep accumulating instead of each starting from zero.
func (s *sessionGenerations) Rotate(key string) int {
	if s == nil || key == "" {
		return 0
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked(now)
	entry := s.gen[key]
	entry.generation++
	entry.seen = now
	s.gen[key] = entry
	return entry.generation
}

// sweepLocked drops entries idle past the TTL, at most once per sweepInterval.
// It runs on access rather than on a ticker: a quiet gateway has nothing to
// sweep, and a busy one does not need a full-map walk per request.
func (s *sessionGenerations) sweepLocked(now time.Time) {
	if s.sweepInterval > 0 && now.Sub(s.lastSweep) < s.sweepInterval {
		return
	}
	s.lastSweep = now
	for key, entry := range s.gen {
		if now.Sub(entry.seen) > s.ttl {
			delete(s.gen, key)
		}
	}
}
