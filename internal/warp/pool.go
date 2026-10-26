package warp

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultPoolSize = 2

// egressCooldown keeps a public /64 (or IPv4 /32) that the upstream has
// rejected out of the pool for this long. The free tier scopes its quota to
// that block, so a new tunnel inside it would only repeat the same answer.
const egressCooldown = 30 * time.Minute

// refillRetryDelay spaces out a failed registration attempt. A successful
// attempt is only limited by the registrar's own 12-second pacing.
const refillRetryDelay = 5 * time.Second

// scaleDownDelay keeps exits created for a burst hot for a while after the
// burst, so a following burst reuses them instead of paying for new
// registrations.
const scaleDownDelay = 60 * time.Second

// ErrClosed is returned when a pool is used after Close. Callers use it to
// tell a retiring pool apart from a temporary capacity wait.
var ErrClosed = errors.New("WARP pool is closed")

// ErrExitRotated is the cancellation cause of every lease that was holding an
// exit when that exit left the rotation. A request interrupted this way is
// retryable on another exit; it is not a client cancellation.
var ErrExitRotated = errors.New("WARP exit rotated")

var (
	errLeaseReleased = errors.New("WARP lease released")
	errLeaseAborted  = errors.New("WARP lease aborted")
	errPoolReset     = errors.New("WARP pool reset")
)

type logger interface {
	Debugf(string, ...interface{})
	Infof(string, ...interface{})
	Warnf(string, ...interface{})
	Errorf(string, ...interface{})
}

// slogLogger adapts the structured logger used by the gateway to the short
// diagnostic messages this package emits at its own lifecycle boundaries.
type slogLogger struct {
	log *slog.Logger
}

func (l slogLogger) logf(level slog.Level, format string, args ...interface{}) {
	if l.log == nil {
		return
	}
	l.log.Log(context.Background(), level, fmt.Sprintf(format, args...), "component", "warp")
}

func (l slogLogger) Debugf(format string, args ...interface{}) {
	l.logf(slog.LevelDebug, format, args...)
}

func (l slogLogger) Infof(format string, args ...interface{}) {
	l.logf(slog.LevelInfo, format, args...)
}

func (l slogLogger) Warnf(format string, args ...interface{}) {
	l.logf(slog.LevelWarn, format, args...)
}

func (l slogLogger) Errorf(format string, args ...interface{}) {
	l.logf(slog.LevelError, format, args...)
}

type Pool struct {
	ctx     context.Context
	cancel  context.CancelFunc
	logger  logger
	size    int
	stack   Stack
	newExit func(context.Context) (*exit, error)

	mu         sync.Mutex
	startedAt  time.Time
	generation uint64
	active     []*exit
	inflight   int
	waiting    int
	next       int
	reserved   map[string]bool
	cooling    map[string]time.Time
	changed    chan struct{}
	closed     bool
	refilling  bool
	scaling    bool
	wait       sync.WaitGroup
	createMu   sync.Mutex

	failuresTotal       uint64
	failureStreak       int
	createFailuresTotal uint64
	lastFailureAt       time.Time
	lastFailureKind     string
	lastSuccessAt       time.Time
}

type Lease struct {
	pool   *Pool
	exit   *exit
	ctx    context.Context
	cancel context.CancelCauseFunc
	once   sync.Once
	// aborted is set before the lease context is canceled, so a reader that
	// sees the cancellation can tell a rotation from an ordinary release.
	aborted atomic.Bool
	// reported records that this lease already reported an exit failure. It
	// outlives the penalty so a late read or idle failure cannot penalize the
	// same exit again after a failed replacement restored it.
	reported atomic.Bool
}

// Snapshot is a point-in-time view of a pool for health and admin reporting.
type Snapshot struct {
	Stack string
	Size  int
	// Active is the number of exits currently in the ring. Standby counts the
	// idle part of it: exits not held by a lease and ready to replace a
	// penalized one immediately.
	Active  int
	Standby int
	Cooling int
	Egress  []string
	// Health counters: failures_total is monotonic and covers penalized exits
	// plus failed exit creations; create_failures_total is the creation subset.
	// failure_streak advances on every counted failure and is cleared only by a
	// successful upstream response, never by a successful registration.
	StartedAt           time.Time
	FailuresTotal       uint64
	FailureStreak       int
	CreateFailuresTotal uint64
	LastFailureAt       time.Time
	LastFailureKind     string
	LastSuccessAt       time.Time
}

// Options configure a WARP pool.
type Options struct {
	// Size is the upper bound on exits. The pool keeps as many exits hot as
	// the current concurrency needs (leases in flight plus queued requests,
	// plus a spare) and never exceeds Size.
	Size int
	// Stack selects the address family inside the tunnel. An IPv6 stack makes
	// the public egress an IPv6 address.
	Stack Stack
	// SourceIPv6, when valid, is the outer IPv6 prefix each exit binds its
	// WireGuard socket to. Every exit draws its own address from it, so a
	// replacement reaches Cloudflare from a new source and receives a new
	// public egress. One way past an egress-scoped free usage limit — but not
	// the only one: measured 2026-10-25, a replacement registration already
	// receives a fresh public egress from a single fixed outer source, so this
	// prefix is optional, not required, for egress rotation.
	SourceIPv6 netip.Prefix
	// Proxy, when set, is a socks5:// or socks5h:// URL (a bare host:port is
	// tolerated for direct callers) whose relay carries both the registration
	// API calls and every outer WireGuard packet (UDP ASSOCIATE). The
	// deployment this exists for is a host whose direct UDP to the WARP
	// endpoints is unreliable (measured 2026-10-25: 1/3 direct handshakes vs
	// 3/3 over a local mixed port) — the relay fixes the handshake path while
	// each registration still gets its own egress.
	Proxy string
}

// NewWithOptions creates a WARP pool whose exits use the selected IP stack and,
// optionally, rotate their outer IPv6 source within a prefix or relay their
// outer packets through a SOCKS5 server.
func NewWithOptions(ctx context.Context, log *slog.Logger, opts Options) (*Pool, error) {
	normalized, err := ParseStack(string(opts.Stack))
	if err != nil {
		return nil, err
	}
	if opts.SourceIPv6.IsValid() && (!opts.SourceIPv6.Addr().Is6() || opts.SourceIPv6.Bits() > 64) {
		return nil, fmt.Errorf("WARP outer source must be an IPv6 prefix of /64 or shorter, got %s", opts.SourceIPv6)
	}
	proxyDial := ""
	if opts.Proxy != "" {
		proxyDial, err = proxyDialAddress(opts.Proxy)
		if err != nil {
			return nil, err
		}
		if opts.SourceIPv6.IsValid() {
			return nil, errors.New("WARP outer source cannot be bound behind a SOCKS5 relay; set either warp.proxy or warp.source_ipv6")
		}
	}
	size := opts.Size
	if size <= 0 {
		size = DefaultPoolSize
	}
	r := newRegistrar(proxyDial)
	pool, err := newPool(ctx, slogLogger{log: log}, size, func(ctx context.Context) (*exit, error) {
		reg, err := r.register(ctx)
		if err != nil {
			return nil, err
		}
		var source netip.Addr
		if opts.SourceIPv6.IsValid() {
			source, err = randomSource(opts.SourceIPv6)
			if err != nil {
				return nil, err
			}
		}
		return newExitWithStack(ctx, reg, normalized, source, proxyDial)
	})
	if err != nil {
		return nil, err
	}
	pool.stack = normalized
	return pool, nil
}

// randomSource draws one host address from prefix. The bits above the prefix
// length are kept, every bit below is random, so two exits practically never
// share an outer source.
func randomSource(prefix netip.Prefix) (netip.Addr, error) {
	address := prefix.Masked().Addr().As16()
	var noise [16]byte
	if _, err := rand.Read(noise[:]); err != nil {
		return netip.Addr{}, fmt.Errorf("draw WARP outer source: %w", err)
	}
	keep := prefix.Bits()
	for index := range address {
		switch bits := keep - index*8; {
		case bits >= 8:
		case bits <= 0:
			address[index] = noise[index]
		default:
			mask := byte(0xFF) << (8 - bits)
			address[index] = address[index]&mask | noise[index]&^mask
		}
	}
	return netip.AddrFrom16(address), nil
}

func newPool(ctx context.Context, log logger, size int, create func(context.Context) (*exit, error)) (*Pool, error) {
	if size <= 0 {
		size = DefaultPoolSize
	}
	// The caller's context bounds the synchronous first registration, not the
	// pool: it is typically an initialization deadline that expires moments
	// after NewWithOptions returns. Deriving the pool lifetime from it would
	// cancel every later refill and replacement with "context canceled",
	// leaving the pool stuck on its first egress forever.
	poolCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	p := &Pool{
		ctx:       poolCtx,
		cancel:    cancel,
		logger:    log,
		size:      size,
		newExit:   create,
		startedAt: time.Now(),
		reserved:  make(map[string]bool),
		cooling:   make(map[string]time.Time),
		changed:   make(chan struct{}),
	}
	first, err := p.createExit(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	p.active = []*exit{first}
	if log != nil {
		log.Infof("OpenCode WARP/%s pool ready (max %d exits, one spare)", first.stack, size)
	}
	p.startRefill()
	return p, nil
}

// desiredLocked is the number of exits the pool should keep hot: one per
// in-flight lease, one for every request waiting for an exit, plus a spare, so
// a 429 can switch to a ready exit instead of waiting for a registration. Size
// is the upper bound; at idle a pool of two or more keeps one spare.
func (p *Pool) desiredLocked() int {
	want := p.inflight + p.waiting + 1
	if want < 2 {
		want = 2
	}
	if want > p.size {
		want = p.size
	}
	return want
}

func (p *Pool) Acquire(ctx context.Context) (*Lease, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrClosed
		}
		for attempts := 0; attempts < len(p.active); attempts++ {
			index := p.next % len(p.active)
			p.next = (p.next + 1) % len(p.active)
			active := p.active[index]
			leaseCtx, leaseCancel := context.WithCancelCause(p.ctx)
			lease := &Lease{pool: p, exit: active, ctx: leaseCtx, cancel: leaseCancel}
			if active.addRef(lease) {
				p.inflight++
				p.mu.Unlock()
				// Demand just rose: grow the ring toward it in the background.
				p.startRefill()
				return lease, nil
			}
			leaseCancel(errLeaseReleased)
		}
		p.waiting++
		changed := p.changed
		needRefill := len(p.active) < p.desiredLocked()
		p.mu.Unlock()
		if needRefill {
			// Queued demand is demand too: start growing before we block.
			p.startRefill()
		}
		var err error
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-p.ctx.Done():
			err = ErrClosed
		case <-changed:
		}
		p.mu.Lock()
		p.waiting--
		p.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
}

func (p *Pool) Report(lease *Lease, status int, body []byte) {
	if lease == nil || lease.exit == nil {
		return
	}
	rotate, reason := lease.exit.observe(status, body)
	if !rotate {
		if status/100 == 2 {
			p.noteSuccess(lease.exit)
		}
		return
	}
	if lease.Aborted() || !lease.markReported() {
		return
	}
	p.penalize(lease.exit, lease, reason)
}

// Fail drops an exit that cannot carry traffic even though the upstream never
// produced an HTTP response, such as a broken WARP handshake or a stalled first
// token. Retrying the same user-space tunnel would only repeat the stall.
func (p *Pool) Fail(lease *Lease, reason string) {
	if lease == nil || lease.exit == nil || reason == "" {
		return
	}
	if lease.Aborted() || !lease.markReported() {
		return
	}
	p.penalize(lease.exit, lease, reason)
}

func (p *Pool) Size() int { return p.size }

// Snapshot reports the pool's current capacity and the public egress address
// of every active exit. It never exposes keys or device tokens.
func (p *Pool) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	snapshot := Snapshot{
		Stack:               string(p.stack),
		Size:                p.size,
		Active:              len(p.active),
		Cooling:             len(p.cooling),
		StartedAt:           p.startedAt,
		FailuresTotal:       p.failuresTotal,
		FailureStreak:       p.failureStreak,
		CreateFailuresTotal: p.createFailuresTotal,
		LastFailureAt:       p.lastFailureAt,
		LastFailureKind:     p.lastFailureKind,
		LastSuccessAt:       p.lastSuccessAt,
	}
	for _, active := range p.active {
		if active.idle() {
			snapshot.Standby++
		}
		if active.egress.IsValid() {
			snapshot.Egress = append(snapshot.Egress, active.egress.String())
		}
	}
	return snapshot
}

// noteSuccess clears the failure streak and records the last proof that an
// exit actually carried a WARP response. A response from an exit that has
// already left the rotation proves nothing and is ignored.
func (p *Pool) noteSuccess(e *exit) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.activeIndexLocked(e) < 0 {
		return
	}
	p.failureStreak = 0
	p.lastSuccessAt = time.Now()
}

func (p *Pool) noteFailureLocked(kind string) {
	p.failuresTotal++
	p.failureStreak++
	p.lastFailureAt = time.Now()
	p.lastFailureKind = kind
}

func (p *Pool) noteCreateFailure() {
	p.mu.Lock()
	p.createFailuresTotal++
	p.noteFailureLocked("create")
	p.mu.Unlock()
}

// failureKind maps a penalize reason to the bounded label the health feed
// exposes. Raw error text never reaches that feed.
func failureKind(reason string) string {
	switch {
	case strings.HasPrefix(reason, "HTTP 429"):
		return "http_429"
	case strings.HasPrefix(reason, "HTTP 403"):
		return "http_403"
	case reason == "transport error":
		return "transport"
	case reason == "first event timeout":
		return "first_event_timeout"
	case reason == "attempt timeout":
		return "attempt_timeout"
	case reason == "stream idle timeout":
		return "stream_idle"
	case reason == "stream read error":
		return "stream_read"
	case reason == "create":
		return "create"
	default:
		return "other"
	}
}

func (p *Pool) penalize(bad *exit, exclude *Lease, reason string) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	index := p.activeIndexLocked(bad)
	if index < 0 {
		p.mu.Unlock()
		return
	}
	if !bad.markPenalized() {
		p.mu.Unlock()
		return
	}
	p.noteFailureLocked(failureKind(reason))
	if key := bad.egressKey(); key != "" {
		p.cooling[key] = time.Now().Add(egressCooldown)
		delete(p.reserved, key)
	}
	p.active = append(p.active[:index], p.active[index+1:]...)
	if p.next >= len(p.active) {
		p.next = 0
	}
	p.notifyLocked()
	p.wait.Add(1)
	// Capture the generation that produced this penalty while the pool lock is
	// held. A Reset between the spawn and the goroutine's first instruction
	// must not let the replacement adopt the new generation and restore an
	// exit from the old ring.
	generation := p.generation
	p.mu.Unlock()

	// Abort every other request on this exit now instead of letting it wait
	// for the replacement and the device close. The reporting lease is kept
	// alive so its 429/403 body can still be drained.
	bad.abortLeases(exclude, ErrExitRotated)

	if p.logger != nil {
		p.logger.Warnf("Rotating OpenCode WARP egress %s (%s)", bad.id, reason)
	}
	// Re-register immediately: the retired exit is replaced in the background
	// and the retry draws whichever active exit is ready first.
	go func() {
		defer p.wait.Done()
		p.replace(bad, reason, generation)
	}()
}

func (p *Pool) replace(old *exit, reason string, generation uint64) {
	replacement, err := p.createExit(p.ctx)
	p.mu.Lock()
	if p.closed || p.generation != generation {
		// A Reset happened while this registration was in flight. Its result
		// belongs to the old generation and must not join the new ring.
		p.mu.Unlock()
		if replacement != nil {
			p.releaseReservation(replacement)
			replacement.close()
		}
		old.retire()
		p.startRefill()
		return
	}
	if err != nil {
		restored := len(p.active) == 0
		if restored {
			old.clearPenalty()
			if key := old.egressKey(); key != "" {
				p.reserved[key] = true
				delete(p.cooling, key)
			}
			p.active = append(p.active, old)
			p.notifyLocked()
		}
		p.mu.Unlock()
		if !restored {
			old.retire()
		}
		if p.logger != nil {
			p.logger.Errorf("Cannot replace OpenCode WARP egress %s: %v", old.id, err)
		}
		p.startRefill()
		return
	}
	if p.activeIndexLocked(old) >= 0 {
		p.mu.Unlock()
		p.releaseReservation(replacement)
		replacement.close()
		old.retire()
		return
	}
	placed := false
	if len(p.active) < p.size {
		p.active = append(p.active, replacement)
		placed = true
	}
	p.notifyLocked()
	p.mu.Unlock()
	if !placed {
		p.releaseReservation(replacement)
		replacement.close()
		old.retire()
		return
	}
	if p.logger != nil {
		p.logger.Infof("OpenCode WARP egress %s replaced by %s at %s (%s), active=%d", old.id, replacement.id, replacement.egress, reason, len(p.active))
	}
	old.retire()
	p.startRefill()
}

func (p *Pool) refill() {
	p.mu.Lock()
	if p.closed || p.refilling || !p.needsRefillLocked() {
		p.mu.Unlock()
		return
	}
	generation := p.generation
	p.refilling = true
	p.mu.Unlock()

	replacement, err := p.createExit(p.ctx)
	p.mu.Lock()
	p.refilling = false
	stale := p.generation != generation
	placed := false
	if err == nil && !p.closed && !stale && len(p.active) < p.size {
		p.active = append(p.active, replacement)
		replacement = nil
		placed = true
	}
	if placed || p.closed {
		p.notifyLocked()
	}
	p.mu.Unlock()

	if replacement != nil {
		p.releaseReservation(replacement)
		replacement.close()
	}
	if stale {
		// Reset already started the new generation; this registration cannot
		// serve it, so start the replacement refill now instead of waiting for
		// the ordinary retry delay.
		p.startRefill()
		return
	}
	if err != nil && p.ctx.Err() == nil {
		if p.logger != nil {
			p.logger.Warnf("Cannot pre-register OpenCode WARP egress: %v", err)
		}
		// Every registration is already paced by the registrar; retry soon
		// instead of leaving the pool under its target for a long window.
		p.after(refillRetryDelay, p.startRefill)
		return
	}
	p.startRefill()
}

func (p *Pool) startRefill() {
	p.mu.Lock()
	if p.closed || p.refilling || !p.needsRefillLocked() {
		p.mu.Unlock()
		return
	}
	p.wait.Add(1)
	p.mu.Unlock()
	go func() {
		defer p.wait.Done()
		p.refill()
	}()
}

// released records that a lease was returned and trims the ring back toward
// the demand of the moment. Freed exits stay hot for scaleDownDelay, so a
// burst that pauses does not immediately pay for re-registering them.
func (p *Pool) released() {
	p.mu.Lock()
	if p.inflight > 0 {
		p.inflight--
	}
	closed := p.closed
	wantRefill := !closed && len(p.active) < p.desiredLocked()
	started := p.scaling
	if !closed && !started && len(p.active) > p.desiredLocked() {
		p.scaling = true
		started = true
	}
	p.mu.Unlock()
	if closed {
		return
	}
	if wantRefill {
		p.startRefill()
	}
	if started {
		p.after(scaleDownDelay, p.scaleDown)
	}
}

// scaleDown retires every idle exit above the current demand. Busy surplus
// exits are left to their release, which arms another pass if the ring is
// still over target then.
func (p *Pool) scaleDown() {
	p.mu.Lock()
	p.scaling = false
	if p.closed {
		p.mu.Unlock()
		return
	}
	want := p.desiredLocked()
	var dropped []*exit
	if surplus := len(p.active) - want; surplus > 0 {
		kept := p.active[:0]
		for _, candidate := range p.active {
			if surplus > 0 && candidate.idle() {
				dropped = append(dropped, candidate)
				surplus--
				continue
			}
			kept = append(kept, candidate)
		}
		p.active = kept
	}
	if p.next >= len(p.active) {
		p.next = 0
	}
	p.mu.Unlock()
	for _, drop := range dropped {
		p.releaseReservation(drop)
		drop.retire()
		if p.logger != nil {
			p.logger.Infof("OpenCode WARP egress %s scaled down at %s", drop.id, drop.egress)
		}
	}
}

// Reset takes every active exit out of rotation and aborts its leases, then
// refills the pool. It is the explicit rotation the watchdog uses after a
// network switch: each in-flight request on a retired exit fails now instead of
// waiting for a device close, and the exits close as soon as their leases are
// released. Reset reports how many exits it removed and is a no-op on a closed
// pool.
func (p *Pool) Reset(cause error) int {
	if cause == nil {
		cause = errPoolReset
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0
	}
	active := p.active
	p.active = nil
	p.next = 0
	p.generation++
	for _, item := range active {
		if key := item.egressKey(); key != "" {
			delete(p.reserved, key)
		}
	}
	p.notifyLocked()
	p.mu.Unlock()

	for _, item := range active {
		item.abortLeases(nil, cause)
		item.retire()
	}
	if len(active) > 0 && p.logger != nil {
		p.logger.Warnf("Resetting OpenCode WARP pool (%d exits): %v", len(active), cause)
	}
	p.startRefill()
	return len(active)
}

// after runs fn once delay has elapsed, unless the pool closes first.
func (p *Pool) after(delay time.Duration, fn func()) {
	p.wait.Add(1)
	go func() {
		defer p.wait.Done()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-p.ctx.Done():
		case <-timer.C:
			fn()
		}
	}()
}

func (p *Pool) createExit(ctx context.Context) (*exit, error) {
	p.createMu.Lock()
	defer p.createMu.Unlock()
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()
		if closed {
			return nil, ErrClosed
		}
		candidate, err := p.newExit(ctx)
		if err != nil {
			if !errors.Is(err, ErrClosed) {
				p.noteCreateFailure()
			}
			return nil, err
		}
		key := candidate.egressKey()
		now := time.Now()
		p.mu.Lock()
		for subnet, until := range p.cooling {
			if !until.After(now) {
				delete(p.cooling, subnet)
			}
		}
		usable := key != "" && !p.reserved[key] && !p.cooling[key].After(now)
		if usable {
			p.reserved[key] = true
		}
		p.mu.Unlock()
		if usable {
			if p.logger != nil {
				p.logger.Infof("OpenCode WARP/%s egress %s ready at %s", candidate.stack, candidate.id, candidate.egress)
			}
			return candidate, nil
		}
		lastErr = fmt.Errorf("WARP egress %s is already active or cooling down", key)
		if p.logger != nil {
			p.logger.Warnf("Discarding duplicate WARP exit %s: %v", candidate.id, lastErr)
		}
		candidate.close()
	}
	p.noteCreateFailure()
	return nil, lastErr
}

func (p *Pool) releaseReservation(e *exit) {
	if e == nil {
		return
	}
	key := e.egressKey()
	if key == "" {
		return
	}
	p.mu.Lock()
	delete(p.reserved, key)
	p.mu.Unlock()
}

func (p *Pool) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *Pool) activeIndexLocked(target *exit) int {
	for index, candidate := range p.active {
		if candidate == target {
			return index
		}
	}
	return -1
}

func (p *Pool) needsRefillLocked() bool {
	return len(p.active) < p.desiredLocked()
}

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	active := p.active
	p.active = nil
	p.notifyLocked()
	p.mu.Unlock()

	// Abort before canceling the pool context so these leases report the
	// close, not a bare parent cancellation.
	for _, item := range active {
		item.abortLeases(nil, ErrClosed)
	}
	p.cancel()
	for _, item := range active {
		p.releaseReservation(item)
		item.retire()
	}
	p.wait.Wait()
}

// Done is closed when the lease is aborted or released.
func (l *Lease) Done() <-chan struct{} {
	if l == nil || l.ctx == nil {
		return nil
	}
	return l.ctx.Done()
}

// Aborted reports whether the lease was canceled because its exit left the
// rotation, rather than released normally.
func (l *Lease) Aborted() bool {
	return l != nil && l.aborted.Load()
}

// Reported reports whether this lease already reported an exit failure. A
// lease reports at most once, so a late read or idle failure cannot penalize a
// restored exit a second time.
func (l *Lease) Reported() bool {
	return l != nil && l.reported.Load()
}

func (l *Lease) markReported() bool {
	return l != nil && l.reported.CompareAndSwap(false, true)
}

// Abort cancels the lease with cause and marks it aborted.
func (l *Lease) Abort(cause error) {
	if l == nil {
		return
	}
	if cause == nil {
		cause = errLeaseAborted
	}
	l.aborted.Store(true)
	if l.cancel != nil {
		l.cancel(cause)
	}
}

// RequestContext derives an attempt context from parent that is canceled when
// either parent ends or the lease is aborted or released. The stop function
// detaches the lease watcher, cancels the derived context, and must be called
// when the attempt ends.
func (l *Lease) RequestContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if l == nil || l.ctx == nil {
		return parent, func() {}
	}
	ctx, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(l.ctx, func() {
		cancel(context.Cause(l.ctx))
	})
	return ctx, func() {
		stop()
		cancel(nil)
	}
}

func (l *Lease) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if l == nil || l.exit == nil {
		return nil, errors.New("WARP lease is closed")
	}
	return l.exit.dialContext(ctx, network, address)
}

func (l *Lease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		if l.cancel != nil {
			l.cancel(errLeaseReleased)
		}
		if l.exit != nil {
			l.exit.release(l)
		}
		if l.pool != nil {
			l.pool.released()
		}
	})
}

func (l *Lease) EgressKey() string {
	if l == nil || l.exit == nil {
		return ""
	}
	return l.exit.egressKey()
}

// DialNetwork is the explicit TCP address family for this lease. Callers should
// pass it to DialContext instead of assuming IPv6.
func (l *Lease) DialNetwork() string {
	if l == nil || l.exit == nil {
		return ""
	}
	return l.exit.dialNetwork()
}

func (l *Lease) String() string {
	if l == nil || l.exit == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s/%s", l.exit.id, l.exit.egress)
}
