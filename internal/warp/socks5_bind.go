package warp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.zx2c4.com/wireguard/conn"
)

// socks5Bind is a wireguard conn.Bind that relays the outer UDP packets
// through a SOCKS5 server's UDP ASSOCIATE relay, so the WARP handshake and
// data plane reach Cloudflare from the proxy's exit instead of the host. The
// tunnel itself is unchanged: netstack still owns the inner addresses, and
// every registration still receives its own public egress (measured
// 2026-10-25: three exits registered through one fixed SOCKS5 exit each got a
// distinct egress, so the relay does not collapse egress rotation).
//
// Only a no-auth SOCKS5 server is supported; the expected deployment is a
// local mixed port (Clash/mihomo) that forwards UDP to the WARP endpoints.
// Measured the same day: direct outer handshakes from an IPv4-only host in
// China succeeded 1/3 with two 50s trace timeouts, while the same handshakes
// over the relay succeeded 3/3 in 1.9–21.7s.
type socks5Bind struct {
	// proxy is the host:port dial address of the SOCKS5 server.
	proxy string

	mu     sync.Mutex
	tcp    net.Conn
	udp    *net.UDPConn
	relay  *net.UDPAddr
	closed bool
}

func (b *socks5Bind) Open(port uint16) ([]conn.ReceiveFunc, uint16, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tcp != nil || b.udp != nil {
		return nil, 0, conn.ErrBindAlreadyOpen
	}
	b.closed = false
	tcp, err := net.DialTimeout("tcp", b.proxy, 5*time.Second)
	if err != nil {
		return nil, 0, fmt.Errorf("socks5 dial %s: %w", b.proxy, err)
	}
	tcp.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := tcp.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		tcp.Close()
		return nil, 0, fmt.Errorf("socks5 greeting: %w", err)
	}
	head := make([]byte, 2)
	if _, err := io.ReadFull(tcp, head); err != nil || head[0] != 0x05 || head[1] != 0x00 {
		tcp.Close()
		return nil, 0, fmt.Errorf("socks5 %s refused the no-auth method: %v", b.proxy, err)
	}
	// UDP ASSOCIATE with DST 0.0.0.0:0 asks for a relay without pinning a
	// single target; each Send carries the real destination in its header.
	if _, err := tcp.Write([]byte{0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		tcp.Close()
		return nil, 0, fmt.Errorf("socks5 associate: %w", err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(tcp, reply); err != nil || reply[1] != 0x00 {
		tcp.Close()
		return nil, 0, fmt.Errorf("socks5 %s refused UDP ASSOCIATE (no UDP support?): %v", b.proxy, err)
	}
	var host []byte
	switch reply[3] {
	case 0x01:
		host = make([]byte, 4)
	case 0x03:
		length := make([]byte, 1)
		if _, err := io.ReadFull(tcp, length); err != nil {
			tcp.Close()
			return nil, 0, fmt.Errorf("socks5 associate reply: %w", err)
		}
		host = make([]byte, length[0])
	case 0x04:
		host = make([]byte, 16)
	default:
		tcp.Close()
		return nil, 0, fmt.Errorf("socks5 associate reply: unknown address type %d", reply[3])
	}
	if _, err := io.ReadFull(tcp, host); err != nil {
		tcp.Close()
		return nil, 0, fmt.Errorf("socks5 associate reply: %w", err)
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(tcp, portBytes); err != nil {
		tcp.Close()
		return nil, 0, fmt.Errorf("socks5 associate reply: %w", err)
	}
	tcp.SetDeadline(time.Time{})
	relayIP := net.IP(host)
	if relayIP.IsUnspecified() {
		// The control connection's remote address is the relay's real host,
		// and it works for a hostname-configured proxy where re-parsing the
		// configured string cannot.
		if remote, ok := tcp.RemoteAddr().(*net.TCPAddr); ok && remote.IP != nil {
			relayIP = remote.IP
		}
	}
	relay := &net.UDPAddr{IP: relayIP, Port: int(binary.BigEndian.Uint16(portBytes))}
	udp, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		tcp.Close()
		return nil, 0, err
	}
	b.tcp, b.udp, b.relay = tcp, udp, relay
	// The relay's UDP session lives exactly as long as the control connection
	// (RFC 1928; mihomo terminates the relay when the TCP side closes). Drain
	// it so a half-close is noticed: on EOF the UDP socket tears down, which
	// surfaces as a receive error — a rotate-worthy failure — instead of a
	// silent stall that burns a first-event timeout.
	go func() {
		_, _ = io.Copy(io.Discard, tcp)
		b.Close()
	}()
	receive := func(packets [][]byte, sizes []int, endpoints []conn.Endpoint) (int, error) {
		buf := make([]byte, 65535)
		for {
			n, _, err := udp.ReadFromUDP(buf)
			if err != nil {
				return 0, err
			}
			// The relay's datagrams carry the remote destination in a SOCKS5
			// header; reporting that destination (not the relay's own address)
			// as the endpoint keeps wireguard's roaming logic pointed at the
			// real Cloudflare endpoint.
			if n < 4 || buf[2] != 0x00 {
				continue
			}
			var addr netip.Addr
			offset := 0
			switch buf[3] {
			case 0x01:
				if n < 4+4+2 {
					continue
				}
				var raw [4]byte
				copy(raw[:], buf[4:8])
				addr = netip.AddrFrom4(raw)
				offset = 8
			case 0x04:
				if n < 4+16+2 {
					continue
				}
				var raw [16]byte
				copy(raw[:], buf[4:20])
				addr = netip.AddrFrom16(raw)
				offset = 20
			default:
				continue
			}
			port := binary.BigEndian.Uint16(buf[offset : offset+2])
			offset += 2
			copy(packets[0], buf[offset:n])
			sizes[0] = n - offset
			endpoints[0] = &simpleEndpoint{addr: netip.AddrPortFrom(addr.Unmap(), port)}
			return 1, nil
		}
	}
	localPort := uint16(0)
	if local := udp.LocalAddr(); local != nil {
		if udpAddr, ok := local.(*net.UDPAddr); ok {
			localPort = uint16(udpAddr.Port)
		}
	}
	return []conn.ReceiveFunc{receive}, localPort, nil
}

func (b *socks5Bind) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	var errs []error
	if b.udp != nil {
		errs = append(errs, b.udp.Close())
		b.udp = nil
	}
	if b.tcp != nil {
		errs = append(errs, b.tcp.Close())
		b.tcp = nil
	}
	return errors.Join(errs...)
}

func (b *socks5Bind) SetMark(uint32) error { return nil }

func (b *socks5Bind) Send(bufs [][]byte, endpoint conn.Endpoint) error {
	ep, ok := endpoint.(*simpleEndpoint)
	if !ok {
		return conn.ErrWrongEndpointType
	}
	b.mu.Lock()
	udp, relay, closed := b.udp, b.relay, b.closed
	b.mu.Unlock()
	if closed || udp == nil || relay == nil {
		return net.ErrClosed
	}
	header := make([]byte, 0, 22)
	header = append(header, 0, 0, 0)
	addr := ep.addr.Addr().Unmap()
	if addr.Is4() {
		v4 := addr.As4()
		header = append(header, 0x01)
		header = append(header, v4[:]...)
	} else {
		v16 := addr.As16()
		header = append(header, 0x04)
		header = append(header, v16[:]...)
	}
	port := make([]byte, 2)
	binary.BigEndian.PutUint16(port, ep.addr.Port())
	header = append(header, port...)
	for _, buf := range bufs {
		packet := append(header, buf...)
		if _, err := udp.WriteToUDP(packet, relay); err != nil {
			return err
		}
	}
	return nil
}

func (*socks5Bind) ParseEndpoint(value string) (conn.Endpoint, error) {
	addr, err := netip.ParseAddrPort(value)
	if err != nil {
		return nil, err
	}
	return &simpleEndpoint{addr: addr}, nil
}

func (*socks5Bind) BatchSize() int { return 1 }

// proxyDialAddress extracts the host:port a socks5Bind dials from a
// configured proxy URL. It rejects anything but socks5/socks5h — matched
// case-insensitively, because the configuration layer canonicalizes to
// lowercase but this function is also the runtime's second line of defence —
// so a misconfigured http entry fails with a clear error instead of a dial
// that can never carry UDP.
func proxyDialAddress(raw string) (string, error) {
	hostPort := raw
	scheme := ""
	if index := indexScheme(hostPort); index >= 0 {
		scheme = hostPort[:index]
		hostPort = hostPort[index+3:]
	}
	switch strings.ToLower(scheme) {
	case "":
	case "socks5", "socks5h":
	default:
		return "", fmt.Errorf("warp.proxy must be a socks5:// or socks5h:// URL (http(s) proxies cannot carry the outer UDP), got %q", raw)
	}
	_, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", fmt.Errorf("warp.proxy: %q is not a host:port", raw)
	}
	if portNumber, err := strconv.Atoi(port); err != nil || portNumber < 1 || portNumber > 65535 {
		return "", fmt.Errorf("warp.proxy: %q has no valid port", raw)
	}
	return hostPort, nil
}

func indexScheme(value string) int {
	for index := 0; index+3 <= len(value); index++ {
		if value[index] == ':' && value[index+1] == '/' && value[index+2] == '/' {
			return index
		}
	}
	return -1
}
