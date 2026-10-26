package protocol

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

// errAfterReader yields a fixed prefix and then one injected read error, which
// is how a committed stream turns idle after its last upstream chunk.
type errAfterReader struct {
	data []byte
	err  error
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	if r.err != nil {
		err := r.err
		r.err = nil
		return 0, err
	}
	return 0, io.EOF
}

const idleChatFrame = `data: {"id":"chatcmpl-test","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}` + "\n\n"

func TestForwardStreamIdleTimeoutHasNoSyntheticError(t *testing.T) {
	rec := httptest.NewRecorder()
	_, _, err := ForwardStream(context.Background(), rec, &errAfterReader{
		data: []byte(idleChatFrame),
		err:  ErrStreamIdleTimeout,
	}, Chat, "m")
	if !errors.Is(err, ErrStreamIdleTimeout) {
		t.Fatalf("error = %v, want ErrStreamIdleTimeout", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"content":"hi"`) {
		t.Fatalf("the first chunk must be forwarded, got %q", body)
	}
	if strings.Contains(body, "upstream SSE stream failed") {
		t.Fatalf("an idle timeout must not emit a synthetic error event, got %q", body)
	}
}

func TestTranscodeStreamIdleTimeoutHasNoSyntheticError(t *testing.T) {
	rec := httptest.NewRecorder()
	_, _, err := TranscodeStream(context.Background(), rec, &errAfterReader{
		data: []byte(idleChatFrame),
		err:  ErrStreamIdleTimeout,
	}, Chat, Anthropic, "m")
	if !errors.Is(err, ErrStreamIdleTimeout) {
		t.Fatalf("error = %v, want ErrStreamIdleTimeout", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "hi") {
		t.Fatalf("the transcoded chunk must be forwarded, got %q", body)
	}
	if strings.Contains(body, "upstream SSE stream failed") {
		t.Fatalf("an idle timeout must not emit a synthetic error event, got %q", body)
	}
}

// A terminal event completes a response. The idle deadline must not be able to
// turn that success into a failure when the upstream delays EOF afterwards.
func TestForwardStreamStopsAtNormalTerminalBeforeIdle(t *testing.T) {
	rec := httptest.NewRecorder()
	_, _, err := ForwardStream(context.Background(), rec, &errAfterReader{
		data: []byte(idleChatFrame + "data: [DONE]\n\n"),
		err:  ErrStreamIdleTimeout,
	}, Chat, "m")
	if err != nil {
		t.Fatalf("error = %v, want success after the terminal event", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"content":"hi"`) || !strings.Contains(body, "[DONE]") {
		t.Fatalf("the completed stream must be forwarded, got %q", body)
	}
}

func TestTranscodeStreamStopsAtNormalTerminalBeforeIdle(t *testing.T) {
	rec := httptest.NewRecorder()
	_, _, err := TranscodeStream(context.Background(), rec, &errAfterReader{
		data: []byte(idleChatFrame + "data: [DONE]\n\n"),
		err:  ErrStreamIdleTimeout,
	}, Chat, Anthropic, "m")
	if err != nil {
		t.Fatalf("error = %v, want success after the terminal event", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "hi") {
		t.Fatalf("the completed transcoded stream must be forwarded, got %q", body)
	}
}
