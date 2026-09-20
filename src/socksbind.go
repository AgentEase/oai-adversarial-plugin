package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// socksBind performs a minimal SOCKS5 handshake (RFC 1928 / RFC 1929) against
// one fixed proxy endpoint and captures the server-bound address from the
// CONNECT reply's BND.ADDR / BND.PORT fields. BND.ADDR is defined as the
// address bound by the server, which may be private, behind NAT or otherwise
// different from the public IP seen by the upstream. The standard
// golang.org/x/net/proxy client discards those fields, so the probe track
// records them here as proxy-reported evidence, not verified public identity.
//
// The dialer supports the two negotiation paths the project proxies use:
// no-authentication and username/password authentication. The destination is
// always sent as a domain name (remote resolution), matching the socks5h
// semantics of the probe configuration.
type socksBind struct {
	host       string
	user       string
	pass       string
	mu         sync.Mutex
	last       string
	dials      int
	diagnostic socksDiagnostic
}

// Only fixed categories and protocol bytes are retained; never credentials,
// destination names, raw errors or the reported address itself.
type socksDiagnostic struct {
	Stage       string
	Error       string
	Reply       int
	AddressType int
	AddressKind string
}

func (b *socksBind) diagnostics() (socksDiagnostic, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.diagnostic, b.dials
}

func boundAddressKind(bound string) string {
	if bound == "" {
		return "empty"
	}
	ip := net.ParseIP(bound)
	if ip == nil {
		return "domain"
	}
	if ip.IsUnspecified() {
		return "unspecified"
	}
	if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return "non_public"
	}
	return "ip_unverified"
}

// store records the address reported by the proxy for the connection that
// just finished. Empty and non-meaningful values ("::", "0.0.0.0") are
// dropped so a stale-but-real value is never overwritten by a placeholder.
func (b *socksBind) store(bound string) {
	if bound == "" || bound == "::" || bound == "0.0.0.0" {
		return
	}
	b.mu.Lock()
	b.last = bound
	b.mu.Unlock()
}

// load returns the most recent meaningful bound address ("" when the proxy
// never reported one). Each probe transport has its own socksBind, and a
// single transport serves one attempt sequence, so the latest value always
// belongs to the attempt that just completed.
func (b *socksBind) load() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.last
}

func (b *socksBind) dialContext(ctx context.Context, network, address string) (_ net.Conn, dialErr error) {
	diagnostic := socksDiagnostic{Stage: "tcp_connect", Reply: -1, AddressType: -1, AddressKind: "not_received"}
	b.mu.Lock()
	b.dials++
	b.diagnostic = socksDiagnostic{Stage: "in_progress", Reply: -1, AddressType: -1, AddressKind: "not_received"}
	b.mu.Unlock()
	defer func() {
		if dialErr != nil {
			diagnostic.Error = "protocol_or_io"
			var netErr net.Error
			if errors.Is(dialErr, context.Canceled) {
				diagnostic.Error = "canceled"
			} else if errors.As(dialErr, &netErr) && netErr.Timeout() {
				diagnostic.Error = "timeout"
			} else if errors.Is(dialErr, io.EOF) || errors.Is(dialErr, io.ErrUnexpectedEOF) {
				diagnostic.Error = "truncated_reply"
			}
		}
		b.mu.Lock()
		b.diagnostic = diagnostic
		b.mu.Unlock()
	}()
	dialer := &net.Dialer{Timeout: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", b.host)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			conn.Close()
		}
	}()
	if deadline, has := ctx.Deadline(); has {
		conn.SetDeadline(deadline)
	} else {
		conn.SetDeadline(time.Now().Add(30 * time.Second))
	}

	// Method negotiation: offer no-auth plus username/password when the
	// proxy spec carries credentials.
	diagnostic.Stage = "greeting"
	methods := []byte{0x00}
	if b.user != "" || b.pass != "" {
		methods = []byte{0x00, 0x02}
	}
	greeting := append([]byte{0x05, byte(len(methods))}, methods...)
	if _, err := conn.Write(greeting); err != nil {
		return nil, fmt.Errorf("socks5 greeting: %w", err)
	}
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("socks5 greeting reply: %w", err)
	}
	if header[0] != 0x05 {
		return nil, fmt.Errorf("socks5 unexpected version %d", header[0])
	}
	switch header[1] {
	case 0x00:
		// No authentication required.
	case 0x02:
		diagnostic.Stage = "authentication"
		if b.user == "" && b.pass == "" {
			return nil, errors.New("socks5 server demands authentication but no credentials are configured")
		}
		if len(b.user) > 255 || len(b.pass) > 255 {
			return nil, errors.New("socks5 credentials exceed 255 bytes")
		}
		request := []byte{0x01, byte(len(b.user))}
		request = append(request, b.user...)
		request = append(request, byte(len(b.pass)))
		request = append(request, b.pass...)
		if _, err := conn.Write(request); err != nil {
			return nil, fmt.Errorf("socks5 auth write: %w", err)
		}
		if _, err := io.ReadFull(conn, header); err != nil {
			return nil, fmt.Errorf("socks5 auth reply: %w", err)
		}
		if header[1] != 0x00 {
			return nil, errors.New("socks5 authentication rejected")
		}
	case 0xFF:
		return nil, errors.New("socks5 server rejected all offered authentication methods")
	default:
		return nil, fmt.Errorf("socks5 server selected unsupported method %d", header[1])
	}

	diagnostic.Stage = "target_validation"
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("socks5 split target %q: %w", address, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("socks5 bad target port %q", portText)
	}
	if len(host) > 255 {
		return nil, errors.New("socks5 target host exceeds 255 bytes")
	}
	request := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	request = append(request, host...)
	request = binary.BigEndian.AppendUint16(request, uint16(port))
	diagnostic.Stage = "connect_write"
	if _, err := conn.Write(request); err != nil {
		return nil, fmt.Errorf("socks5 connect write: %w", err)
	}
	diagnostic.Stage = "connect_reply"
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return nil, fmt.Errorf("socks5 connect reply: %w", err)
	}
	diagnostic.Reply = int(head[1])
	diagnostic.AddressType = int(head[3])
	if head[0] != 0x05 {
		return nil, fmt.Errorf("socks5 unexpected reply version %d", head[0])
	}
	if head[1] != 0x00 {
		return nil, fmt.Errorf("socks5 connect failed: %s", socksReplyText(head[1]))
	}
	diagnostic.Stage = "bound_address"
	var boundHost string
	switch head[3] {
	case 0x01:
		raw := make([]byte, 4)
		if _, err := io.ReadFull(conn, raw); err != nil {
			return nil, fmt.Errorf("socks5 bound address: %w", err)
		}
		boundHost = net.IP(raw).String()
	case 0x04:
		raw := make([]byte, 16)
		if _, err := io.ReadFull(conn, raw); err != nil {
			return nil, fmt.Errorf("socks5 bound address: %w", err)
		}
		boundHost = net.IP(raw).String()
	case 0x03:
		if _, err := io.ReadFull(conn, header[:1]); err != nil {
			return nil, fmt.Errorf("socks5 bound address length: %w", err)
		}
		raw := make([]byte, int(header[0]))
		if _, err := io.ReadFull(conn, raw); err != nil {
			return nil, fmt.Errorf("socks5 bound address: %w", err)
		}
		boundHost = string(raw)
	default:
		return nil, fmt.Errorf("socks5 unsupported bound address type %d", head[3])
	}
	diagnostic.AddressKind = boundAddressKind(boundHost)
	diagnostic.Stage = "bound_port"
	portRaw := make([]byte, 2)
	if _, err := io.ReadFull(conn, portRaw); err != nil {
		return nil, fmt.Errorf("socks5 bound port: %w", err)
	}
	b.store(boundHost)
	diagnostic.Stage = "complete"
	conn.SetDeadline(time.Time{})
	ok = true
	return conn, nil
}

// socksReplyText maps the CONNECT reply status byte to a short message.
func socksReplyText(code byte) string {
	switch code {
	case 0x01:
		return "general SOCKS server failure"
	case 0x02:
		return "connection not allowed by ruleset"
	case 0x03:
		return "network unreachable"
	case 0x04:
		return "host unreachable"
	case 0x05:
		return "connection refused"
	case 0x06:
		return "TTL expired"
	case 0x07:
		return "command not supported"
	case 0x08:
		return "address type not supported"
	}
	return fmt.Sprintf("reply code %d", code)
}
