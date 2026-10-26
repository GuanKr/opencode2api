package protocol

import (
	"context"
	"errors"
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
