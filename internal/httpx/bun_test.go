package httpx

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

// The Bun disguise is only worth anything if the header layout matches the
// runtime the free tier expects. This pins the observable layout:
// bunCommonHeaders in their WebKit spelling and order, other headers lowercased
// and sorted after them, then the headers fetch adds itself.
func TestBunRequestHeadMatchesBunLayout(t *testing.T) {
	body := []byte(`{"stream":true}`)
	req, err := http.NewRequest(http.MethodPost, "https://opencode.ai/inference/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer public")
	req.Header.Set("x-opencode-client", "cli")
	req.Header.Set("x-opencode-project", "0123456789abcdef0123456789abcdef01234567")

	head, decompress, err := bunRequestHead(req, body)
	if err != nil {
		t.Fatalf("bunRequestHead: %v", err)
	}
	if !decompress {
		t.Fatal("decompress = false, want true when the transport adds Accept-Encoding")
	}
	got := string(head)
	if !strings.HasPrefix(got, "POST /inference/v1/chat/completions HTTP/1.1\r\n") {
		t.Fatalf("request line = %q", strings.SplitN(got, "\r\n", 2)[0])
	}
	// WebKit-known headers keep their spelling and original order.
	auth := strings.Index(got, "Authorization: Bearer public\r\n")
	ctype := strings.Index(got, "Content-Type: application/json\r\n")
	if auth < 0 || ctype < 0 || auth > ctype {
		t.Fatalf("common header order wrong: Authorization at %d, Content-Type at %d\n%s", auth, ctype, got)
	}
	// Unknown headers are lowercased and come after every common header.
	client := strings.Index(got, "x-opencode-client: cli\r\n")
	if client < 0 || client < ctype {
		t.Fatalf("x-opencode-client should be lowercased after the common headers\n%s", got)
	}
	// fetch adds these itself, in this order, before the terminating blank line.
	tail := []string{
		"Connection: keep-alive\r\n",
		"User-Agent: Bun/" + v1BunVersion + "\r\n",
		"Accept: */*\r\n",
		"Host: opencode.ai\r\n",
		"Accept-Encoding: gzip, deflate, br, zstd\r\n",
		"Content-Length: " + strconv.Itoa(len(body)) + "\r\n",
	}
	previous := -1
	for _, want := range tail {
		at := strings.Index(got, want)
		if at < 0 {
			t.Fatalf("missing %q in\n%s", want, got)
		}
		if at < previous {
			t.Fatalf("%q out of order in\n%s", want, got)
		}
		previous = at
	}
	if !strings.HasSuffix(got, "\r\n\r\n") {
		t.Fatalf("head does not end with a blank line: %q", got)
	}
}

// Caller-supplied User-Agent and Accept-Encoding must survive untouched, and
// the transport must then leave the body compressed.
func TestBunRequestHeadKeepsCallerEncodingAndUserAgent(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://opencode.ai/zen", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("User-Agent", "opencode/2.0.32")
	req.Header.Set("Accept-Encoding", "gzip")
	head, decompress, err := bunRequestHead(req, nil)
	if err != nil {
		t.Fatalf("bunRequestHead: %v", err)
	}
	got := string(head)
	if decompress {
		t.Fatal("decompress = true, want false when the caller sets Accept-Encoding")
	}
	if !strings.Contains(got, "User-Agent: opencode/2.0.32\r\n") {
		t.Fatalf("caller User-Agent lost:\n%s", got)
	}
	if !strings.Contains(got, "Accept-Encoding: gzip\r\n") {
		t.Fatalf("caller Accept-Encoding lost:\n%s", got)
	}
	if strings.Contains(got, "Accept-Encoding: gzip, deflate, br, zstd") {
		t.Fatalf("transport overwrote caller Accept-Encoding:\n%s", got)
	}
	if strings.Contains(got, "Content-Length") {
		t.Fatalf("bodyless request must not carry Content-Length:\n%s", got)
	}
}

// A header value with a control character would split the request; the writer
// must refuse rather than emit it.
func TestBunRequestHeadRejectsHeaderInjection(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://opencode.ai/inference", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("x-opencode-session", "abc\r\nX-Evil: 1")
	if _, _, err := bunRequestHead(req, nil); err == nil {
		t.Fatal("bunRequestHead accepted a header value containing CRLF")
	}
}

func TestIsBunEncoding(t *testing.T) {
	for _, encoding := range []string{"gzip", "x-gzip", "deflate", "br", "zstd"} {
		if !isBunEncoding(encoding) {
			t.Errorf("isBunEncoding(%q) = false, want true", encoding)
		}
	}
	for _, encoding := range []string{"", "identity", "compress", "GZIP"} {
		if isBunEncoding(encoding) {
			t.Errorf("isBunEncoding(%q) = true, want false", encoding)
		}
	}
}

// Every encoding BunTransport advertises must decode back to the exact bytes,
// including both zlib-wrapped and raw DEFLATE for "deflate".
func TestNewBunDecoderRoundTripsAdvertisedEncodings(t *testing.T) {
	payload := []byte(strings.Repeat("opencode2api bun transport ", 64))
	encodings := []struct {
		name     string
		encoding string
		encode   func([]byte) []byte
	}{
		{"gzip", "gzip", func(p []byte) []byte {
			var b bytes.Buffer
			w := gzip.NewWriter(&b)
			_, _ = w.Write(p)
			_ = w.Close()
			return b.Bytes()
		}},
		{"x-gzip", "x-gzip", func(p []byte) []byte {
			var b bytes.Buffer
			w := gzip.NewWriter(&b)
			_, _ = w.Write(p)
			_ = w.Close()
			return b.Bytes()
		}},
		{"deflate zlib", "deflate", func(p []byte) []byte {
			var b bytes.Buffer
			w := zlib.NewWriter(&b)
			_, _ = w.Write(p)
			_ = w.Close()
			return b.Bytes()
		}},
		{"deflate raw", "deflate", func(p []byte) []byte {
			var b bytes.Buffer
			w, _ := flate.NewWriter(&b, flate.DefaultCompression)
			_, _ = w.Write(p)
			_ = w.Close()
			return b.Bytes()
		}},
		{"brotli", "br", func(p []byte) []byte {
			var b bytes.Buffer
			w := brotli.NewWriter(&b)
			_, _ = w.Write(p)
			_ = w.Close()
			return b.Bytes()
		}},
		{"zstd", "zstd", func(p []byte) []byte {
			var b bytes.Buffer
			w, _ := zstd.NewWriter(&b)
			_, _ = w.Write(p)
			_ = w.Close()
			return b.Bytes()
		}},
	}
	for _, tc := range encodings {
		t.Run(tc.name, func(t *testing.T) {
			reader, release, err := newBunDecoder(tc.encoding, bytes.NewReader(tc.encode(payload)))
			if err != nil {
				t.Fatalf("newBunDecoder(%q): %v", tc.encoding, err)
			}
			if release != nil {
				defer release()
			}
			decoded, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("read decoded body: %v", err)
			}
			if !bytes.Equal(decoded, payload) {
				t.Fatalf("decoded %d bytes, want %d", len(decoded), len(payload))
			}
		})
	}
}

func TestValidHeaderFieldRejectsControlCharacters(t *testing.T) {
	if !validHeaderField("x-opencode-session", "value with spaces and	tab") {
		t.Error("validHeaderField rejected a legal value")
	}
	for _, value := range []string{"a\r\nb: c", "a\nb", "a\x00b", "a\x7fb"} {
		if validHeaderField("x-header", value) {
			t.Errorf("validHeaderField accepted %q", value)
		}
	}
	if validHeaderField("bad header", "value") {
		t.Error("validHeaderField accepted a header name with a space")
	}
}
