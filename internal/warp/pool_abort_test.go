package warp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"sync"
	"testing"
	"time"
)

// fakeExit is a pool exit without a WireGuard device or network stack. The
// pool only needs an id and a unique IPv6 egress to register, retire, and
// count it, so every test here stays offline.
func fakeExit(n int) *exit {
	return &exit{
		id:     fmt.Sprintf("exit-%d", n),
		stack:  StackIPv6,
		egress: netip.MustParseAddr(fmt.Sprintf("2001:db8:%x::1", n)),
	}
}

// fakeExitFactory numbers exits so each registration gets a distinct /64.
// failFrom makes every call from that position fail; blockFrom holds calls
// there until release is closed.
type fakeExitFactory struct {
	mu        sync.Mutex
	created   int
	failFrom  int
	blockFrom int
	release   chan struct{}
}

func (f *fakeExitFactory) create(context.Context) (*exit, error) {
	f.mu.Lock()
	f.created++
	n := f.created
	f.mu.Unlock()
	if f.blockFrom > 0 && n >= f.blockFrom && f.release != nil {
		<-f.release
	}
	if f.failFrom > 0 && n >= f.failFrom {
		return nil, errors.New("WARP registration refused")
	}
	return fakeExit(n), nil
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func poolActive(p *Pool) []*exit {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*exit(nil), p.active...)
}

func exitState(e *exit) (refs int, retired, closed bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.refs, e.retired, e.closed
}

func waitClosed(t *testing.T, e *exit) {
	t.Helper()
	waitFor(t, "exit "+e.id+" to close", func() bool {
		_, _, closed := exitState(e)
		return closed
	})
}

func TestPenalizeAbortsOtherLeasesButNotReportingLease(t *testing.T) {
	factory := &fakeExitFactory{failFrom: 2}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)
	reporter, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire reporter: %v", err)
	}
	other, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire other: %v", err)
	}
	if reporter.exit != other.exit {
		t.Fatal("the two leases must hold the same exit")
	}
	derived, stop := other.RequestContext(context.Background())
	defer stop()
	reporterDerived, stopReporter := reporter.RequestContext(context.Background())
	defer stopReporter()

	pool.Report(reporter, http.StatusTooManyRequests, nil)

	if reporter.Aborted() {
		t.Fatal("the reporting lease must survive so its 429 body can drain")
	}
	select {
	case <-reporterDerived.Done():
		t.Fatal("the reporting lease's attempt context must stay live")
	default:
	}
	select {
	case <-other.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the other lease must be canceled immediately")
	}
	if !other.Aborted() {
		t.Fatal("the other lease must report aborted")
	}
	if cause := context.Cause(other.ctx); !errors.Is(cause, ErrExitRotated) {
		t.Fatalf("other lease cause = %v, want ErrExitRotated", cause)
	}
	select {
	case <-derived.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the derived request context must follow the aborted lease")
	}
	if cause := context.Cause(derived); !errors.Is(cause, ErrExitRotated) {
		t.Fatalf("derived context cause = %v, want ErrExitRotated", cause)
	}

	other.Release()
	other.Release()
	reporter.Release()
	pool.Close()
	waitClosed(t, reporter.exit)
}

func TestFailedReplacementRestoresExit(t *testing.T) {
	factory := &fakeExitFactory{failFrom: 2}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	original := lease.exit
	pool.Report(lease, http.StatusTooManyRequests, nil)

	waitFor(t, "the failed replacement to restore the old exit", func() bool {
		active := poolActive(pool)
		return len(active) == 1 && active[0] == original
	})
	if refs, retired, closed := exitState(original); refs != 1 || retired || closed {
		t.Fatalf("restored exit state = refs %d, retired %v, closed %v; want 1, false, false", refs, retired, closed)
	}
	// A real successful response from the restored exit clears the streak.
	pool.Report(lease, http.StatusOK, nil)
	snapshot := pool.Snapshot()
	if snapshot.FailureStreak != 0 {
		t.Fatalf("failure streak = %d after a successful report, want 0", snapshot.FailureStreak)
	}
	if snapshot.LastSuccessAt.IsZero() {
		t.Fatal("a successful report must record last_success_at")
	}
	lease.Release()
}

func TestSuccessfulReplacementRetiresOldExitAfterLeaseRelease(t *testing.T) {
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	old := lease.exit
	pool.Report(lease, http.StatusTooManyRequests, nil)

	waitFor(t, "the replacement to join the ring", func() bool {
		active := poolActive(pool)
		return len(active) == 1 && active[0] != old
	})
	waitFor(t, "the old exit to retire", func() bool {
		_, retired, _ := exitState(old)
		return retired
	})
	if _, _, closed := exitState(old); closed {
		t.Fatal("the old exit must stay open while its reporting lease holds a reference")
	}
	lease.Release()
	waitClosed(t, old)
}

func TestResetAbortsAllLeasesAndRefills(t *testing.T) {
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), nil, 2, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)
	waitFor(t, "the idle pool to reach its two exits", func() bool {
		return pool.Snapshot().Active == 2
	})

	first, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	second, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	retired := map[string]bool{first.EgressKey(): true, second.EgressKey(): true}

	cause := errors.New("watchdog node switch")
	if removed := pool.Reset(cause); removed != 2 {
		t.Fatalf("Reset removed %d exits, want 2", removed)
	}
	for _, lease := range []*Lease{first, second} {
		if !lease.Aborted() {
			t.Fatalf("lease %s must be aborted by Reset", lease)
		}
		if got := context.Cause(lease.ctx); got != cause {
			t.Fatalf("lease %s cause = %v, want %v", lease, got, cause)
		}
		if _, retired, closed := exitState(lease.exit); !retired || closed {
			t.Fatalf("lease %s exit state = retired %v, closed %v; want true, false", lease, retired, closed)
		}
	}
	waitFor(t, "the pool to refill after Reset", func() bool {
		active := poolActive(pool)
		return len(active) > 0 && !retired[active[0].egressKey()]
	})
	first.Release()
	second.Release()
	waitClosed(t, first.exit)
	waitClosed(t, second.exit)

	pool.Close()
	if removed := pool.Reset(errors.New("late reset")); removed != 0 {
		t.Fatalf("Reset on a closed pool removed %d exits, want 0", removed)
	}
}

func TestCloseAbortsLeases(t *testing.T) {
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	pool.Close()

	if !lease.Aborted() {
		t.Fatal("Pool.Close must abort active leases")
	}
	if cause := context.Cause(lease.ctx); !errors.Is(cause, ErrClosed) {
		t.Fatalf("lease cause = %v, want ErrClosed", cause)
	}
	select {
	case <-lease.Done():
	default:
		t.Fatal("Pool.Close must cancel the lease context")
	}
	lease.Release()
	waitClosed(t, lease.exit)
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Acquire on a closed pool = %v, want ErrClosed", err)
	}
}

func TestSnapshotCountsFailuresAndSuccesses(t *testing.T) {
	// A create failure counts once and advances the failure streak, without a
	// registration success ever clearing it.
	createFailures := &fakeExitFactory{failFrom: 2}
	createPool, err := newPool(context.Background(), nil, 1, createFailures.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	snapshot := createPool.Snapshot()
	if snapshot.StartedAt.IsZero() {
		t.Fatal("started_at must be set")
	}
	if snapshot.FailuresTotal != 0 || snapshot.FailureStreak != 0 || snapshot.CreateFailuresTotal != 0 {
		t.Fatalf("fresh counters = %+v, want all zero", snapshot)
	}
	if _, err := createPool.createExit(context.Background()); err == nil {
		t.Fatal("the factory must fail the second exit creation")
	}
	snapshot = createPool.Snapshot()
	if snapshot.FailuresTotal != 1 || snapshot.CreateFailuresTotal != 1 || snapshot.FailureStreak != 1 {
		t.Fatalf("create failure counters = %+v, want total 1, create 1, streak 1", snapshot)
	}
	if snapshot.LastFailureKind != "create" || snapshot.LastFailureAt.IsZero() {
		t.Fatalf("create failure metadata = kind %q at %v", snapshot.LastFailureKind, snapshot.LastFailureAt)
	}
	if !snapshot.LastSuccessAt.IsZero() {
		t.Fatal("a create failure must not record a success")
	}
	createPool.Close()

	// A 429 penalize counts as http_429 and aborts the other lease. Only a
	// later 2xx from the fresh exit clears the streak.
	release := make(chan struct{})
	penaltyFactory := &fakeExitFactory{blockFrom: 2, release: release}
	pool, err := newPool(context.Background(), nil, 1, penaltyFactory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	pool.Report(lease, http.StatusTooManyRequests, nil)
	snapshot = pool.Snapshot()
	if snapshot.FailuresTotal != 1 || snapshot.FailureStreak != 1 || snapshot.LastFailureKind != "http_429" {
		t.Fatalf("429 counters = %+v, want total 1, streak 1, kind http_429", snapshot)
	}
	// A 2xx from the exit that was just removed is not recovery evidence.
	pool.Report(lease, http.StatusOK, nil)
	if got := pool.Snapshot().FailureStreak; got != 1 {
		t.Fatalf("a response from a penalized exit reset the streak to %d", got)
	}
	close(release)
	waitFor(t, "the blocked replacement to join the ring", func() bool {
		return pool.Snapshot().Active == 1
	})
	next, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire replacement: %v", err)
	}
	pool.Report(next, http.StatusOK, nil)
	snapshot = pool.Snapshot()
	if snapshot.FailureStreak != 0 {
		t.Fatalf("failure streak = %d after a successful report, want 0", snapshot.FailureStreak)
	}
	if snapshot.FailuresTotal != 1 {
		t.Fatalf("failures_total = %d after a success, want it monotonic at 1", snapshot.FailuresTotal)
	}
	if snapshot.LastSuccessAt.IsZero() {
		t.Fatal("a successful report must record last_success_at")
	}
	next.Release()
	lease.Release()
}

func TestFailureKindLabels(t *testing.T) {
	cases := map[string]string{
		"HTTP 429":                 "http_429",
		"HTTP 403":                 "http_403",
		"HTTP 403 network anomaly": "http_403",
		"transport error":          "transport",
		"first event timeout":      "first_event_timeout",
		"attempt timeout":          "attempt_timeout",
		"stream idle timeout":      "stream_idle",
		"stream read error":        "stream_read",
		"something unexpected":     "other",
	}
	for reason, want := range cases {
		if got := failureKind(reason); got != want {
			t.Fatalf("failureKind(%q) = %q, want %q", reason, got, want)
		}
	}
}
