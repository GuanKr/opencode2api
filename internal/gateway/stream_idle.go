package gateway

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	wire "opencode2api/internal/protocol"
)

// idleReadCloser closes an established stream when a single Read stays blocked
// for timeout. The timer starts before each Read and stops when it returns, so
// slow downstream writes, which happen after Read, are not counted as upstream
// idle.
type idleReadCloser struct {
	body      io.ReadCloser
	timeout   time.Duration
	onTimeout func()

	mu       sync.Mutex
	timer    *time.Timer
	timedOut atomic.Bool

	closeOnce sync.Once
	closeErr  error
}

// newIdleReadCloser wraps body with a per-read idle deadline. onTimeout runs
// before the body is closed.
func newIdleReadCloser(body io.ReadCloser, timeout time.Duration, onTimeout func()) *idleReadCloser {
	return &idleReadCloser{body: body, timeout: timeout, onTimeout: onTimeout}
}

func (r *idleReadCloser) Read(data []byte) (int, error) {
	r.mu.Lock()
	if r.timedOut.Load() {
		r.mu.Unlock()
		return 0, wire.ErrStreamIdleTimeout
	}
	timer := time.AfterFunc(r.timeout, r.expire)
	r.timer = timer
	r.mu.Unlock()

	n, err := r.body.Read(data)

	r.mu.Lock()
	if r.timer == timer {
		r.timer = nil
	}
	timer.Stop()
	r.mu.Unlock()
	if r.timedOut.Load() {
		return n, wire.ErrStreamIdleTimeout
	}
	return n, err
}

func (r *idleReadCloser) expire() {
	r.timedOut.Store(true)
	if aware, ok := r.body.(interface{ markIdleTimeout() }); ok {
		aware.markIdleTimeout()
	}
	if r.onTimeout != nil {
		r.onTimeout()
	}
	_ = r.closeBody()
}

// Aborted forwards the underlying body's rotation state so a stream cut at
// EOF can still be classified as exit rotation.
func (r *idleReadCloser) Aborted() bool {
	aborted, ok := r.body.(interface{ Aborted() bool })
	return ok && aborted.Aborted()
}

// Close stops the idle timer and closes the underlying body exactly once. It
// does not mark the stream as timed out.
func (r *idleReadCloser) Close() error {
	r.mu.Lock()
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	r.mu.Unlock()
	return r.closeBody()
}

func (r *idleReadCloser) closeBody() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.body.Close()
	})
	return r.closeErr
}

// newStreamIdleBody applies the configured idle deadline to a committed SSE
// body. It only bounds and closes the body; the caller decides whether the
// resulting timeout should penalize the exit, so a timer that fires in the
// same read as a terminal event cannot retire a completed stream.
func newStreamIdleBody(body io.ReadCloser, timeout time.Duration) io.ReadCloser {
	return newIdleReadCloser(body, timeout, nil)
}

// warpStreamFailure returns a callback that retires the WARP exit carrying a
// stream that the caller classified as idle. Non-WARP bodies return nil, so the
// same handler branch can serve every route.
func warpStreamFailure(body io.ReadCloser) func() {
	wb, ok := body.(*warpBody)
	if !ok {
		return nil
	}
	return func() { wb.fail("stream idle timeout") }
}
