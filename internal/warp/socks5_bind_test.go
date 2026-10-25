package warp

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"golang.zx2c4.com/wireguard/conn"
)

// fakeSOCKS5 is a loopback SOCKS5 server: it accepts the no-auth greeting,
// answers UDP ASSOCIATE with a real relay address, and echoes every UDP
// datagram back to its sender with the datagram's own destination header
// rebuilt — the minimum behaviour socks5Bind needs from a relay. The whole
// test stays on 127.0.0.1; no packet leaves the loopback interface.
type fakeSOCKS5 struct {
	tcp       net.Listener
	udp       *net.UDPConn
	fragFix   bool // echo a fragmented datagram before the good one
	bindAny   bool // answer UDP ASSOCIATE with 0.0.0.0 instead of the real IP
}

func startFakeSOCKS5(t *testing.T) *fakeSOCKS5 {
	t.Helper()
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake socks5 tcp: %v", err)
	}
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("fake socks5 udp: %v", err)
	}
	server := &fakeSOCKS5{tcp: tcp, udp: udp}
	go server.acceptTCP(t)
	go server.relayUDP(t)
	t.Cleanup(func() {
		tcp.Close()
		udp.Close()
	})
	return server
}

func (s *fakeSOCKS5) address() string { return s.tcp.Addr().String() }

func (s *fakeSOCKS5) acceptTCP(t *testing.T) {
	for {
		client, err := s.tcp.Accept()
		if err != nil {
			return
		}
		go func() {
			defer client.Close()
			greeting := make([]byte, 3)
			if _, err := io.ReadFull(client, greeting); err != nil {
				return
			}
			if greeting[1] != 0x01 || greeting[2] != 0x00 {
				client.Write([]byte{0x05, 0xFF}) // no acceptable method
				return
			}
			if _, err := client.Write([]byte{0x05, 0x00}); err != nil {
				return
			}
			associate := make([]byte, 10)
			if _, err := io.ReadFull(client, associate); err != nil {
				return
			}
			udpAddr := s.udp.LocalAddr().(*net.UDPAddr)
			announce := udpAddr.IP.To4()
			if s.bindAny {
				announce = net.IPv4zero.To4()
			}
			reply := []byte{0x05, 0x00, 0x00, 0x01}
			reply = append(reply, announce...)
			port := make([]byte, 2)
			binary.BigEndian.PutUint16(port, uint16(udpAddr.Port))
			reply = append(reply, port...)
			if _, err := client.Write(reply); err != nil {
				return
			}
			// Hold the control connection open; the bind closes it on Close.
			io.Copy(io.Discard, client)
		}()
	}
}

func (s *fakeSOCKS5) relayUDP(t *testing.T) {
	buf := make([]byte, 65535)
	for {
		n, sender, err := s.udp.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n < 4 {
			continue
		}
		offset := 4
		switch buf[3] {
		case 0x01:
			offset += 4
		case 0x04:
			offset += 16
		default:
			continue
		}
		offset += 2
		if offset > n {
			continue
		}
		header := append([]byte{}, buf[:offset]...)
		payload := append([]byte{}, buf[offset:n]...)
		if s.fragFix {
			// A datagram with FRAG!=0 must be dropped by the receiver, not
			// surfaced as a packet.
			fragmented := append(append([]byte{}, header...), 0xEE)
			fragmented[2] = 0x01
			s.udp.WriteToUDP(fragmented, sender)
		}
		s.udp.WriteToUDP(append(header, payload...), sender)
	}
}

func receiveWithTimeout(t *testing.T, receives []conn.ReceiveFunc) (int, conn.Endpoint, []byte) {
	t.Helper()
	type received struct {
		n        int
		endpoint conn.Endpoint
		packet   []byte
	}
	done := make(chan received, 1)
	go func() {
		packets := make([][]byte, 1)
		packets[0] = make([]byte, 4096)
		sizes := make([]int, 1)
		endpoints := make([]conn.Endpoint, 1)
		n, err := receives[0](packets, sizes, endpoints)
		if err != nil {
			return
		}
		done <- received{n: n, endpoint: endpoints[0], packet: packets[0][:sizes[0]]}
	}()
	select {
	case got := <-done:
		return got.n, got.endpoint, got.packet
	case <-time.After(5 * time.Second):
		t.Fatal("no datagram reached the bind within 5s")
		return 0, nil, nil
	}
}

// The bind must wrap outgoing datagrams in a SOCKS5 header addressed to the
// real endpoint, and must report the header's destination — not the relay's
// own address — as the source endpoint of incoming datagrams, so wireguard's
// roaming stays pointed at Cloudflare.
func TestSocks5BindRoundTripReportsTheRealEndpoint(t *testing.T) {
	server := startFakeSOCKS5(t)
	bind := &socks5Bind{proxy: server.address()}
	receives, _, err := bind.Open(0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bind.Close()

	endpoint, err := bind.ParseEndpoint("162.159.193.10:2408")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	if err := bind.Send([][]byte{[]byte("handshake-initiation")}, endpoint); err != nil {
		t.Fatalf("send: %v", err)
	}

	n, received, packet := receiveWithTimeout(t, receives)
	if n != 1 || string(packet) != "handshake-initiation" {
		t.Fatalf("payload did not survive the relay round trip: n=%d packet=%q", n, packet)
	}
	if received.DstToString() != "162.159.193.10:2408" {
		t.Fatalf("incoming datagrams must report the real destination, got %s", received.DstToString())
	}
	if err := bind.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := bind.Close(); err != nil {
		t.Fatalf("close must be idempotent: %v", err)
	}
}

// A relay that forwards fragmented datagrams must not surface them: FRAG!=0
// carries only a slice of the original datagram, and wireguard has no
// reassembly.
func TestSocks5BindSkipsFragmentedDatagrams(t *testing.T) {
	server := startFakeSOCKS5(t)
	server.fragFix = true
	bind := &socks5Bind{proxy: server.address()}
	receives, _, err := bind.Open(0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bind.Close()

	endpoint, err := bind.ParseEndpoint("[2606:4700:d0::a29f:c006]:2408")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	if err := bind.Send([][]byte{[]byte("v6-payload")}, endpoint); err != nil {
		t.Fatalf("send: %v", err)
	}

	_, received, packet := receiveWithTimeout(t, receives)
	if string(packet) != "v6-payload" {
		t.Fatalf("the fragmented datagram must be skipped, got %q", packet)
	}
	if received.DstToString() != "[2606:4700:d0::a29f:c006]:2408" {
		t.Fatalf("IPv6 destination must round trip, got %s", received.DstToString())
	}
}

// A server without the no-auth method, or one that refuses UDP ASSOCIATE,
// must fail Open with an error that names the proxy — these are the two
// ways a real deployment (Clash without UDP, an authenticated relay) says no.
func TestSocks5BindFailsClosedOnRefusals(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tcp: %v", err)
	}
	defer listener.Close()
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer client.Close()
				greeting := make([]byte, 3)
				if _, err := io.ReadFull(client, greeting); err != nil {
					return
				}
				client.Write([]byte{0x05, 0xFF})
			}()
		}
	}()
	bind := &socks5Bind{proxy: listener.Addr().String()}
	if _, _, err := bind.Open(0); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("a server without no-auth must be refused, got %v", err)
	}
}

// A relay that answers UDP ASSOCIATE with 0.0.0.0 (legal per RFC 1928 — the
// address is then meaningless) must still work: the bind falls back to the
// control connection's remote address, which also covers a hostname-configured
// proxy that a re-parse of the configured string could not resolve.
func TestSocks5BindFallsBackToTheControlConnectionAddress(t *testing.T) {
	server := startFakeSOCKS5(t)
	server.bindAny = true
	bind := &socks5Bind{proxy: server.address()}
	receives, _, err := bind.Open(0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bind.Close()

	endpoint, err := bind.ParseEndpoint("162.159.193.10:2408")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	if err := bind.Send([][]byte{[]byte("via-any")}, endpoint); err != nil {
		t.Fatalf("send: %v", err)
	}
	_, received, packet := receiveWithTimeout(t, receives)
	if string(packet) != "via-any" || received.DstToString() != "162.159.193.10:2408" {
		t.Fatalf("the unspecified relay address must be replaced by the control address: packet=%q endpoint=%s", packet, received.DstToString())
	}
}

// wireguard's BindUpdate closes a bind before reopening the same instance, so
// a second Open must say so instead of silently re-dialing, and a Send after
// Close must fail closed rather than write to the dead sockets.
func TestSocks5BindLifecycleGuards(t *testing.T) {
	server := startFakeSOCKS5(t)
	bind := &socks5Bind{proxy: server.address()}
	if _, _, err := bind.Open(0); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, _, err := bind.Open(0); err != conn.ErrBindAlreadyOpen {
		t.Fatalf("double open must report ErrBindAlreadyOpen, got %v", err)
	}
	endpoint, err := bind.ParseEndpoint("162.159.193.10:2408")
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	if err := bind.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := bind.Send([][]byte{[]byte("after-close")}, endpoint); err != net.ErrClosed {
		t.Fatalf("send after close must return net.ErrClosed, got %v", err)
	}
}

// proxyDialAddress is the last line of defence for the form: http(s) proxies
// cannot carry UDP, and a port that is not a number would only fail at dial
// time. A bare host:port without a scheme is accepted at this layer — the
// configuration layer is what requires an explicit socks5:// scheme — and the
// scheme comparison is case-insensitive because url.Parse lowercases what the
// configuration canonicalizes.
func TestProxyDialAddressValidation(t *testing.T) {
	cases := []struct {
		raw     string
		want    string
		wantErr bool
	}{
		{raw: "socks5://127.0.0.1:7890", want: "127.0.0.1:7890"},
		{raw: "SOCKS5://127.0.0.1:7890", want: "127.0.0.1:7890"},
		{raw: "SOCKS5H://proxy.example.test:1080", want: "proxy.example.test:1080"},
		{raw: "socks5h://proxy.example.test:1080", want: "proxy.example.test:1080"},
		{raw: "127.0.0.1:7890", want: "127.0.0.1:7890"},
		{raw: "http://127.0.0.1:7890", wantErr: true},
		{raw: "socks4://127.0.0.1:7890", wantErr: true},
		{raw: "socks5://no-port", wantErr: true},
		{raw: "socks5://127.0.0.1:7890/path", wantErr: true},
		{raw: "socks5://127.0.0.1:0", wantErr: true},
		{raw: "socks5://127.0.0.1:70000", wantErr: true},
	}
	for _, testCase := range cases {
		got, err := proxyDialAddress(testCase.raw)
		if testCase.wantErr {
			if err == nil {
				t.Fatalf("%q must be rejected", testCase.raw)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%q must parse: %v", testCase.raw, err)
		}
		if got != testCase.want {
			t.Fatalf("%q parsed to %q, want %q", testCase.raw, got, testCase.want)
		}
	}
}
