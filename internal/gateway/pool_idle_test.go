package gateway

import (
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
)

// idleRecorder is a RoundTripper that only records CloseIdleConnections calls.
// Both proxy clients must be released when a superseded runtime is swapped out
// (hot reload): the regular http.Transport and the Bun-disguised one that
// free-model traffic uses.
type idleRecorder struct {
	closed atomic.Int32
}

func (r *idleRecorder) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("idleRecorder: no request expected")
}

func (r *idleRecorder) CloseIdleConnections() { r.closed.Add(1) }

func TestCloseIdleConnectionsClosesBunAndRegularClients(t *testing.T) {
	regular := &idleRecorder{}
	bun := &idleRecorder{}
	transports := &proxyTransports{items: []*proxyTransport{
		{client: &http.Client{Transport: regular}, bun: &http.Client{Transport: bun}},
		{},
	}}

	transports.closeIdleConnections()

	if got := regular.closed.Load(); got != 1 {
		t.Fatalf("regular client CloseIdleConnections calls = %d, want 1", got)
	}
	if got := bun.closed.Load(); got != 1 {
		t.Fatalf("bun client CloseIdleConnections calls = %d, want 1", got)
	}

	var nilTransports *proxyTransports
	nilTransports.closeIdleConnections()
}
