package protocol

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestForwardStreamAbortedHasNoSyntheticError(t *testing.T) {
	rec := httptest.NewRecorder()
	_, _, err := ForwardStream(context.Background(), rec, &errAfterReader{
		data: []byte(idleChatFrame),
		err:  ErrStreamAborted,
	}, Chat, "m")
	if !errors.Is(err, ErrStreamAborted) {
		t.Fatalf("error = %v, want ErrStreamAborted", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"content":"hi"`) {
		t.Fatalf("the first chunk must be forwarded, got %q", body)
	}
	if strings.Contains(body, "upstream SSE stream failed") {
		t.Fatalf("an aborted stream must not emit a synthetic error event, got %q", body)
	}
}

type abortedReaderStub struct {
	aborted bool
}

func (r abortedReaderStub) Read([]byte) (int, error) { return 0, io.EOF }

func (r abortedReaderStub) Aborted() bool { return r.aborted }

func TestRenameStreamReaderForwardsAborted(t *testing.T) {
	renames := NewToolRenames()
	renames.Add("client_tool", "wire_tool")
	reader := RenameStreamReader(abortedReaderStub{aborted: true}, Chat, renames)
	aborted, ok := reader.(interface{ Aborted() bool })
	if !ok || !aborted.Aborted() {
		t.Fatalf("RenameStreamReader must forward Aborted, got %#v", reader)
	}
	live := RenameStreamReader(abortedReaderStub{}, Chat, renames)
	if aborted, ok := live.(interface{ Aborted() bool }); !ok || aborted.Aborted() {
		t.Fatalf("RenameStreamReader must report a live source as not aborted, got %#v", live)
	}
}

func TestTranscodeStreamAbortedHasNoSyntheticError(t *testing.T) {
	rec := httptest.NewRecorder()
	_, _, err := TranscodeStream(context.Background(), rec, &errAfterReader{
		data: []byte(idleChatFrame),
		err:  ErrStreamAborted,
	}, Chat, Anthropic, "m")
	if !errors.Is(err, ErrStreamAborted) {
		t.Fatalf("error = %v, want ErrStreamAborted", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "hi") {
		t.Fatalf("the transcoded chunk must be forwarded, got %q", body)
	}
	if strings.Contains(body, "upstream SSE stream failed") {
		t.Fatalf("an aborted stream must not emit a synthetic error event, got %q", body)
	}
}
