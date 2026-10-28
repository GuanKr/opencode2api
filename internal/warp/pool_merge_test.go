package warp

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestResetClearsARefillPause: upstream's registration pacing pauses refills for
// up to five minutes after a failed or duplicate registration. A watchdog reset
// is an explicit rotation after a network switch, so it must not leave the new
// generation empty for the rest of that pause.
func TestResetClearsARefillPause(t *testing.T) {
	factory := &fakeExitFactory{}
	pool, err := newPool(context.Background(), nil, 1, factory.create)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(pool.Close)

	pool.mu.Lock()
	pool.refillPaused = time.Now().Add(time.Hour)
	pool.refillFailures = refillFailureThreshold
	pool.mu.Unlock()

	if removed := pool.Reset(errors.New("watchdog node switch")); removed != 1 {
		t.Fatalf("Reset removed %d exits, want 1", removed)
	}
	waitFor(t, "a replacement registration after Reset", func() bool {
		return len(poolActive(pool)) == 1
	})
	pool.mu.Lock()
	paused := pool.refillPaused
	pool.mu.Unlock()
	if !paused.IsZero() {
		t.Fatalf("Reset left the pool paused until %v", paused)
	}
}

// TestDuplicateEgressDoesNotCountAsACreateFailure: a saturated colo hands out
// fewer distinct /64 blocks than the preload asks for. That is capacity, not a
// tunnel failure: counting it would move the watchdog's primary failure_streak
// signal on a pool whose user traffic is healthy.
func TestDuplicateEgressDoesNotCountAsACreateFailure(t *testing.T) {
	first := fakeExit(1)
	pool := &Pool{
		ctx:      context.Background(),
		size:     2,
		preload:  1,
		newExit:  func(context.Context) (*exit, error) { return fakeExit(1), nil },
		reserved: map[string]bool{first.egressKey(): true},
		cooling:  map[string]time.Time{},
		changed:  make(chan struct{}),
	}
	_, err := pool.createExit(context.Background())
	if !errors.Is(err, errDuplicateEgress) {
		t.Fatalf("createExit error = %v, want errDuplicateEgress", err)
	}
	pool.mu.Lock()
	streak, creates := pool.failureStreak, pool.createFailuresTotal
	pool.mu.Unlock()
	if streak != 0 || creates != 0 {
		t.Fatalf("a duplicate egress was counted as a failure: streak=%d create_failures=%d", streak, creates)
	}
}
