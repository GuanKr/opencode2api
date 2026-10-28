package warp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustAcquire(t *testing.T, pool *Pool) *Lease {
	t.Helper()
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	return lease
}

func assertNotAborted(t *testing.T, leases ...*Lease) {
	t.Helper()
	for _, lease := range leases {
		if lease.Aborted() {
			t.Fatalf("lease %s was aborted by a soft failure", lease)
		}
		select {
		case <-lease.Done():
			t.Fatalf("lease %s context was canceled by a soft failure", lease)
		default:
		}
	}
}

func TestSoftFailureDrainsWithoutAbortingOtherLeases(t *testing.T) {
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	first := mustAcquire(t, pool)
	second := mustAcquire(t, pool)
	if first.exit != second.exit {
		t.Fatal("the two leases must hold the same exit")
	}
	old := first.exit

	pool.FailSoft(first, "stream idle timeout")

	assertNotAborted(t, first, second)
	if _, retired, _ := exitState(old); !retired {
		t.Fatal("a soft-drained exit must be retired so addRef refuses new leases")
	}
	for _, candidate := range poolActive(pool) {
		if candidate == old {
			t.Fatal("a soft-drained exit stayed in the active rotation")
		}
	}
	snapshot := pool.Snapshot()
	if snapshot.FailuresTotal != 1 || snapshot.SoftFailuresTotal != 1 || snapshot.FailureStreak != 0 {
		t.Fatalf("soft failure counters = total %d, soft %d, streak %d; want 1, 1, 0",
			snapshot.FailuresTotal, snapshot.SoftFailuresTotal, snapshot.FailureStreak)
	}
	if snapshot.Draining != 1 {
		t.Fatalf("draining = %d, want 1", snapshot.Draining)
	}
	if snapshot.LastFailureKind != "stream_idle" {
		t.Fatalf("last_failure_kind = %q, want stream_idle", snapshot.LastFailureKind)
	}

	waitFor(t, "a replacement exit", func() bool {
		active := poolActive(pool)
		return len(active) == 1 && active[0] != old
	})
	replacement := poolActive(pool)[0]
	third := mustAcquire(t, pool)
	if third.exit == old {
		t.Fatal("a new lease was handed the soft-drained exit")
	}
	if third.exit != replacement {
		t.Fatalf("new lease holds %s, want the replacement %s", third.exit.id, replacement.id)
	}

	first.Release()
	second.Release()
	waitClosed(t, old)
	waitFor(t, "draining to clear after the last lease", func() bool {
		return pool.Snapshot().Draining == 0
	})
	third.Release()
}

func TestSecondSoftFailureCoolsEgressWithoutAbortingRemainingLease(t *testing.T) {
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	first := mustAcquire(t, pool)
	second := mustAcquire(t, pool)
	third := mustAcquire(t, pool)
	old := first.exit
	key := old.egressKey()

	pool.FailSoft(first, "stream idle timeout")
	assertNotAborted(t, first, second, third)
	pool.mu.Lock()
	_, cooling := pool.cooling[key]
	pool.mu.Unlock()
	if cooling {
		t.Fatal("the first soft failure must only drain, not cool the egress")
	}

	pool.FailSoft(second, "stream read error")
	assertNotAborted(t, first, second, third)
	pool.mu.Lock()
	_, cooling = pool.cooling[key]
	_, reserved := pool.reserved[key]
	pool.mu.Unlock()
	if !cooling {
		t.Fatal("the second soft failure inside the window must cool the egress")
	}
	if reserved {
		t.Fatal("a cooled egress must not stay reserved")
	}

	snapshot := pool.Snapshot()
	if snapshot.FailuresTotal != 2 || snapshot.SoftFailuresTotal != 2 || snapshot.FailureStreak != 0 {
		t.Fatalf("two soft failures = total %d, soft %d, streak %d; want 2, 2, 0",
			snapshot.FailuresTotal, snapshot.SoftFailuresTotal, snapshot.FailureStreak)
	}
	if snapshot.LastFailureKind != "stream_read" {
		t.Fatalf("last_failure_kind = %q, want stream_read", snapshot.LastFailureKind)
	}
	if snapshot.Draining != 1 {
		t.Fatalf("draining = %d, want 1", snapshot.Draining)
	}
	waitFor(t, "a replacement exit", func() bool {
		active := poolActive(pool)
		return len(active) == 1 && active[0] != old
	})

	first.Release()
	second.Release()
	third.Release()
	waitClosed(t, old)
}

func TestSoftFailureOutsideWindowDoesNotCool(t *testing.T) {
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	first := mustAcquire(t, pool)
	second := mustAcquire(t, pool)
	old := first.exit
	key := old.egressKey()
	pool.mu.Lock()
	pool.softFailureTimes[key] = []time.Time{time.Now().Add(-softFailureWindow - time.Minute)}
	pool.mu.Unlock()

	pool.FailSoft(first, "stream idle timeout")
	assertNotAborted(t, first, second)
	pool.mu.Lock()
	_, cooling := pool.cooling[key]
	pool.mu.Unlock()
	if cooling {
		t.Fatal("a soft failure older than the window must not trigger cooling")
	}
	if got := pool.Snapshot().SoftFailuresTotal; got != 1 {
		t.Fatalf("soft_failures_total = %d, want 1", got)
	}

	first.Release()
	second.Release()
	waitClosed(t, old)
}

func TestHardFailureOnDrainingExitCoolsWithoutAbortingRemainingLeases(t *testing.T) {
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	first := mustAcquire(t, pool)
	second := mustAcquire(t, pool)
	third := mustAcquire(t, pool)
	old := first.exit
	key := old.egressKey()

	pool.FailSoft(first, "stream idle timeout")
	assertNotAborted(t, first, second, third)

	pool.Report(second, http.StatusTooManyRequests, nil)
	assertNotAborted(t, first, second, third)

	pool.mu.Lock()
	_, cooling := pool.cooling[key]
	pool.mu.Unlock()
	if !cooling {
		t.Fatal("a hard 429 on a draining exit must cool its egress")
	}
	snapshot := pool.Snapshot()
	if snapshot.FailuresTotal != 2 || snapshot.SoftFailuresTotal != 1 || snapshot.FailureStreak != 1 {
		t.Fatalf("draining hard failure counters = total %d, soft %d, streak %d; want 2, 1, 1",
			snapshot.FailuresTotal, snapshot.SoftFailuresTotal, snapshot.FailureStreak)
	}
	if snapshot.LastFailureKind != "http_429" {
		t.Fatalf("last_failure_kind = %q, want http_429", snapshot.LastFailureKind)
	}
	if snapshot.Draining != 1 {
		t.Fatalf("draining = %d, want 1", snapshot.Draining)
	}

	first.Release()
	second.Release()
	third.Release()
	waitClosed(t, old)
}

func TestCloseAbortsDrainingLeases(t *testing.T) {
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}

	first := mustAcquire(t, pool)
	second := mustAcquire(t, pool)
	old := first.exit
	pool.FailSoft(first, "stream idle timeout")

	pool.Close()
	for _, lease := range []*Lease{first, second} {
		if !lease.Aborted() {
			t.Fatalf("lease %s on a draining exit must be aborted by Close", lease)
		}
		if cause := context.Cause(lease.ctx); !errors.Is(cause, ErrClosed) {
			t.Fatalf("lease %s cause = %v, want ErrClosed", lease, cause)
		}
	}
	first.Release()
	second.Release()
	waitClosed(t, old)
}

func TestResetAbortsDrainingLeases(t *testing.T) {
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), nil, 2, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	first := mustAcquire(t, pool)
	second := mustAcquire(t, pool)
	old := first.exit
	pool.FailSoft(first, "stream idle timeout")

	cause := errPoolReset
	_ = pool.Reset(cause)
	for _, lease := range []*Lease{first, second} {
		if !lease.Aborted() {
			t.Fatalf("lease %s on a draining exit must be aborted by Reset", lease)
		}
		if got := context.Cause(lease.ctx); got != cause {
			t.Fatalf("lease %s cause = %v, want %v", lease, got, cause)
		}
	}
	first.Release()
	second.Release()
	waitClosed(t, old)
	waitFor(t, "the pool to refill after Reset", func() bool {
		return pool.Snapshot().Active >= 1
	})
}

// A lease can outlive the exit it holds: the stream body may only be closed
// after the exit was soft-drained and its last reference dropped. A late soft
// report from that lease must be ignored once the device is closed, not counted
// or turned into a cooling decision.
func TestLateSoftFailureOnClosedDrainingExitIsIgnored(t *testing.T) {
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	first := mustAcquire(t, pool)
	second := mustAcquire(t, pool)
	old := first.exit

	pool.FailSoft(first, "stream idle timeout")
	first.Release()
	second.Release()
	waitClosed(t, old)

	// Read the counters directly instead of through Snapshot: Snapshot prunes
	// the draining list itself, which would hide the stale entry this test is
	// about.
	pool.mu.Lock()
	beforeTotal, beforeSoft, beforeStreak := pool.failuresTotal, pool.softFailuresTotal, pool.failureStreak
	pool.mu.Unlock()

	pool.FailSoft(second, "stream read error")

	pool.mu.Lock()
	afterTotal, afterSoft, afterStreak := pool.failuresTotal, pool.softFailuresTotal, pool.failureStreak
	_, cooling := pool.cooling[old.egressKey()]
	draining := len(pool.draining)
	pool.mu.Unlock()

	if afterTotal != beforeTotal || afterSoft != beforeSoft || afterStreak != beforeStreak {
		t.Fatalf("late soft failure changed counters: before total=%d soft=%d streak=%d, after total=%d soft=%d streak=%d",
			beforeTotal, beforeSoft, beforeStreak, afterTotal, afterSoft, afterStreak)
	}
	if cooling {
		t.Fatal("a late soft failure on a closed draining exit must not cool the egress")
	}
	if draining != 0 {
		t.Fatalf("the closed draining exit stayed listed: draining=%d", draining)
	}
}

// When the replacement registration fails after a soft drain, the retired exit
// must not be restored into the ring: addRef would reject every new lease and
// Acquire would spin. The pool may stay empty until refill succeeds, and Close
// must still finish.
func TestSoftDrainReplacementFailureDoesNotRestoreRetiredExit(t *testing.T) {
	factory := &fakeExitFactory{failFrom: 2}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	lease := mustAcquire(t, pool)
	old := lease.exit
	pool.FailSoft(lease, "stream read error")

	for _, candidate := range poolActive(pool) {
		if candidate == old {
			t.Fatal("the soft-drained exit stayed in the active rotation")
		}
	}
	waitFor(t, "the failed replacement registration", func() bool {
		return pool.Snapshot().FailuresTotal >= 2 && pool.Snapshot().Active == 0
	})
	if _, retired, _ := exitState(old); !retired {
		t.Fatal("the old exit must stay retired after the replacement failure")
	}
	pool.Close()
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Acquire on a closed pool = %v, want ErrClosed", err)
	}
	lease.Release()
	waitClosed(t, old)
}

type scopedLogRecorder struct {
	mu    sync.Mutex
	lines []string
}

func (l *scopedLogRecorder) add(level, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, level+" "+fmt.Sprintf(format, args...))
}

func (l *scopedLogRecorder) Debugf(format string, args ...interface{}) {
	l.add("debug", format, args...)
}
func (l *scopedLogRecorder) Infof(format string, args ...interface{}) { l.add("info", format, args...) }
func (l *scopedLogRecorder) Warnf(format string, args ...interface{}) { l.add("warn", format, args...) }
func (l *scopedLogRecorder) Errorf(format string, args ...interface{}) {
	l.add("error", format, args...)
}

func (l *scopedLogRecorder) contains(fragment string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, fragment) {
			return true
		}
	}
	return false
}

func TestWarpLifecycleLogsCarryEgressScopes(t *testing.T) {
	logs := &scopedLogRecorder{}
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), logs, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	for _, fragment := range []string{"ready at", "egress=2001:db8:1::1", "egress_48=2001:db8:1::/48", "egress_64=2001:db8:1::/64"} {
		waitFor(t, "the ready log to contain "+fragment, func() bool { return logs.contains(fragment) })
	}

	first := mustAcquire(t, pool)
	second := mustAcquire(t, pool)
	pool.FailSoft(first, "stream idle timeout")
	for _, fragment := range []string{"Draining", "egress=2001:db8:1::1"} {
		waitFor(t, "the soft failure log to contain "+fragment, func() bool { return logs.contains(fragment) })
	}
	pool.FailSoft(second, "stream read error")
	waitFor(t, "the soft exhausted log", func() bool { return logs.contains("Cooling OpenCode WARP egress") })
	waitFor(t, "the replacement log", func() bool { return logs.contains("replaced by") })

	first.Release()
	second.Release()
	waitClosed(t, first.exit)
}
