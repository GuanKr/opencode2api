package httpx

import (
	"io"
)

// MaxDrainBytes is the budget DrainAndClose spends on a body the caller
// abandons without reading it. Draining that much and closing keeps the
// connection reusable in the common case (the peer usually already sent that
// much), while a slow or endless upstream cannot keep an abandoned response
// open indefinitely; anything past the budget is dropped with the close.
//
// It is exported so a caller that peeks at a response before giving up on it
// can size that peek as the same budget instead of a second, unrelated
// literal: the peek then rides on a failure path that already pays for this
// drain rather than adding a second read to it (see gateway.maxCausePeek).
// Lowering the drain must not silently leave the peeking side behind.
const MaxDrainBytes = 64 << 10

// DrainAndClose drains up to MaxDrainBytes of body and closes it.
func DrainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(body, MaxDrainBytes))
	_ = body.Close()
}
