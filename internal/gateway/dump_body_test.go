package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

// dumpSink records what the stub upstream received. The handler writes it from
// the server goroutine while the test reads it from its own, so the copy is
// guarded the way poisonStub guards its own recordings.
type dumpSink struct {
	mu   sync.Mutex
	body []byte
}

func (s *dumpSink) record(body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.body = body
}

func (s *dumpSink) got() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body
}

// dumpRecords returns every outbound-body record the gateway logged, oldest
// first. A JSON handler keeps the body readable as one field instead of an
// escaped slog value.
func dumpRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line is not JSON: %s", line)
		}
		if record["event"] == "upstream_request_body" {
			out = append(out, record)
		}
	}
	return out
}

func dumpPhase(t *testing.T, records []map[string]any, phase string) map[string]any {
	t.Helper()
	var found map[string]any
	for _, record := range records {
		if record["phase"] == phase {
			found = record
		}
	}
	return found
}

func dumpPhases(records []map[string]any) []string {
	phases := make([]string, 0, len(records))
	for _, record := range records {
		phases = append(phases, record["phase"].(string))
	}
	return phases
}

// dumpTestGateway wires a gateway at the stub upstream with body dumping on and
// a log buffer the test can read. The stub and the sink it records into are the
// caller's to read after a request.
func dumpTestGateway(t *testing.T) (*Gateway, *bytes.Buffer, *dumpSink) {
	t.Helper()
	sink := &dumpSink{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sink.record(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","object":"chat.completion","choices":[]}`))
	}))
	t.Cleanup(server.Close)

	buf := &bytes.Buffer{}
	g := newStubGateway(t, server.URL)
	g.cfg.Logging = config.LoggingConfig{Level: "debug", DumpRequestBodies: true}
	g.logger = slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	freezeProxyHealth(g)
	return g, buf, sink
}

func chatDumpRoute() models.Route {
	return models.Route{
		ID: "probe-free", Tier: config.TierZen, Protocol: wire.Chat, Anonymous: true,
		Protocols: map[config.Tier]wire.Protocol{config.TierZen: wire.Chat},
	}
}

// The reason this dump exists is diagnosis, and the shaping steps live strictly
// between the client's body and the wire: stream forcing, the appended core tool
// definitions and the placeholder note. A log that only carries the client-side
// body cannot show any of them, so the shaped bytes have to be logged too — and
// they have to be the bytes that went out, not a reconstruction.
func TestOutboundDumpRecordsTheShapedBytesSentUpstream(t *testing.T) {
	g, buf, sink := dumpTestGateway(t)
	client := []byte(`{"model":"probe-free","stream":false,"messages":[{"role":"user","content":"hi"}],` +
		`"tools":[{"type":"function","function":{"name":"read","description":"client read",` +
		`"parameters":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}}}]}`)

	resp, _, _, err := g.doUpstreamTiers(context.Background(), chatDumpRoute(),
		map[config.Tier][]byte{config.TierZen: client}, identity.RequestIDs{Request: "req-dump"}, 0)
	if err != nil {
		t.Fatalf("anonymous attempt: %v", err)
	}
	if resp != nil {
		resp.Body.Close()
	}

	records := dumpRecords(t, buf)
	before, shaped := dumpPhase(t, records, "client"), dumpPhase(t, records, "shaped")
	if before == nil || shaped == nil {
		t.Fatalf("want a client and a shaped dump, got %v", dumpPhases(records))
	}
	// The shaped record is the wire, byte for byte.
	if shaped["body"] != string(sink.got()) {
		t.Fatalf("shaped dump is not what was sent:\nlogged: %s\nsent:  %s", shaped["body"], string(sink.got()))
	}
	if before["sha256"] == shaped["sha256"] {
		t.Fatal("shaping changed nothing; this fixture no longer exercises the shaped phase")
	}
	// Everything below only exists on the shaped side.
	sent := shaped["body"].(string)
	for _, want := range []string{`"stream":true`, "[gateway]", `"shell"`, `"include_usage":true`} {
		if !strings.Contains(sent, want) {
			t.Fatalf("shaped dump is missing %s: %s", want, sent)
		}
	}
	if strings.Contains(before["body"].(string), "[gateway]") {
		t.Fatalf("the client dump already carries the note, so the phases are mislabelled: %s", before["body"])
	}
	// The client's own tool definition has to survive shaping; the gateway
	// appends the missing core names instead of rewriting what the client sent.
	if !strings.Contains(sent, "client read") || !strings.Contains(sent, "file_path") {
		t.Fatalf("the client's tool definition was rewritten: %s", sent)
	}
}

// The documented diagnostic property: equal digests mean the lane changed
// nothing, so a shaped record identical to the client record is a real signal
// and not a sign the dump is stale.
func TestOutboundDumpShapedMatchesClientWhenShapingIsANoOp(t *testing.T) {
	g, buf, sink := dumpTestGateway(t)
	alreadyShaped, err := json.Marshal(map[string]any{
		"model":          "probe-free",
		"stream":         true,
		"stream_options": map[string]any{"include_usage": true},
		"messages":       []any{map[string]any{"role": "user", "content": "hi"}},
		"tools":          anonymousToolset(wire.Chat, nil),
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, _, _, err := g.doUpstreamTiers(context.Background(), chatDumpRoute(),
		map[config.Tier][]byte{config.TierZen: alreadyShaped}, identity.RequestIDs{Request: "req-dump"}, 0)
	if err != nil {
		t.Fatalf("anonymous attempt: %v", err)
	}
	if resp != nil {
		resp.Body.Close()
	}

	records := dumpRecords(t, buf)
	before, shaped := dumpPhase(t, records, "client"), dumpPhase(t, records, "shaped")
	if before == nil || shaped == nil {
		t.Fatalf("want a client and a shaped dump, got %v", dumpPhases(records))
	}
	if before["sha256"] != shaped["sha256"] {
		t.Fatalf("an already agent-shaped body must pass through untouched:\nclient: %s\nshaped: %s",
			before["body"], shaped["body"])
	}
	if shaped["body"] != string(sink.got()) {
		t.Fatalf("shaped dump is not what was sent:\nlogged: %s\nsent:  %s", shaped["body"], string(sink.got()))
	}
}

// Turning the switch off has to silence both phases; otherwise a deployment that
// believes it is not logging bodies is logging them.
func TestOutboundDumpStaysSilentWhenDisabled(t *testing.T) {
	g, buf, _ := dumpTestGateway(t)
	g.cfg.Logging.DumpRequestBodies = false
	resp, _, _, err := g.doUpstreamTiers(context.Background(), chatDumpRoute(),
		map[config.Tier][]byte{config.TierZen: []byte(`{"model":"probe-free","messages":[{"role":"user","content":"hi"}]}`)},
		identity.RequestIDs{Request: "req-dump"}, 0)
	if err != nil {
		t.Fatalf("anonymous attempt: %v", err)
	}
	if resp != nil {
		resp.Body.Close()
	}
	if records := dumpRecords(t, buf); len(records) != 0 {
		t.Fatalf("dumping is disabled but %d body records were written: %v", len(records), dumpPhases(records))
	}
}

// The key lanes log the same pair. "probe-free" counts as a free model by the
// catalog's name convention, so shapeKeyBody really does run here: the test
// asserts the shaping itself, not just that a record appeared, so deleting
// key-lane shaping turns it red instead of leaving it green.
func TestOutboundDumpRecordsShapedBytesOnTheKeyLane(t *testing.T) {
	g, buf, sink := dumpTestGateway(t)
	zen, err := newNodePool([]string{"key-1"}, g.transports, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	g.zenNodes = zen
	body := []byte(`{"model":"probe-free","stream":false,"messages":[{"role":"user","content":"hi"}]}`)

	resp, err, _ := g.doSelectedKeyUpstream(context.Background(), chatDumpRoute(),
		map[config.Tier][]byte{config.TierZen: body}, identity.RequestIDs{Request: "req-dump"},
		DebugKeyOverride{Tier: config.TierZen, KeyID: config.Fingerprint("key-1")}, 0)
	if err != nil {
		t.Fatalf("selected key attempt: %v", err)
	}
	if resp != nil {
		resp.Body.Close()
	}
	records := dumpRecords(t, buf)
	shaped := dumpPhase(t, records, "shaped")
	if shaped == nil {
		t.Fatalf("the key lane logged no shaped dump: %v", dumpPhases(records))
	}
	if shaped["body"] != string(sink.got()) {
		t.Fatalf("shaped dump is not what was sent:\nlogged: %s\nsent:  %s", shaped["body"], string(sink.got()))
	}
	if !strings.Contains(shaped["body"].(string), "[gateway]") {
		t.Fatalf("the key lane did not shape the body, so this case proves nothing: %s", shaped["body"])
	}

	// The non-diagnostic key lane shapes the same way and has to log the same way.
	plain := chatDumpRoute()
	plain.Anonymous = false
	keyed, _, _, err := g.doUpstreamTiers(context.Background(), plain,
		map[config.Tier][]byte{config.TierZen: body}, identity.RequestIDs{Request: "req-dump-2"}, 0)
	if err != nil {
		t.Fatalf("key tier attempt: %v", err)
	}
	if keyed != nil {
		keyed.Body.Close()
	}
	shapedCount := 0
	for _, record := range dumpRecords(t, buf) {
		if record["phase"] == "shaped" {
			shapedCount++
		}
	}
	if shapedCount != 2 {
		t.Fatalf("want a shaped dump for both key paths, got %d", shapedCount)
	}
}
