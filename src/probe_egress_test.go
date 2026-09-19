package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// A loopback SOCKS proxy reports a different bound address per connection.
// It keeps HTTP connections alive, so reuse would repeat the first address.
func TestProbeAttemptUsesFreshSOCKSConnectionAndBoundAddress(t *testing.T) {
	e := newPrefetchTestEngine(t)
	if err := os.WriteFile(e.cfg.Config.CredFile, []byte(`{"access_token":"mock-only"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var connections []net.Conn
	var workers sync.WaitGroup
	workers.Add(1)
	addresses := []string{"203.0.113.10", "203.0.113.11", "0.0.0.0"}
	state := synthStateToken(time.Now())
	go func() {
		defer workers.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			index := len(connections)
			connections = append(connections, conn)
			mu.Unlock()
			workers.Add(1)
			go func(index int, conn net.Conn) {
				defer workers.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				if index >= len(addresses) {
					return
				}
				header := make([]byte, 2)
				if _, err := io.ReadFull(conn, header); err != nil || header[0] != 5 {
					return
				}
				if _, err := io.CopyN(io.Discard, conn, int64(header[1])); err != nil {
					return
				}
				if _, err := conn.Write([]byte{5, 0}); err != nil {
					return
				}
				request := make([]byte, 5)
				if _, err := io.ReadFull(conn, request); err != nil || request[3] != 3 {
					return
				}
				if _, err := io.CopyN(io.Discard, conn, int64(request[4])+2); err != nil {
					return
				}
				reply := append([]byte{5, 0, 0, 1}, net.ParseIP(addresses[index]).To4()...)
				reply = append(reply, 0, 80)
				if _, err := conn.Write(reply); err != nil {
					return
				}
				reader := bufio.NewReader(conn)
				for {
					req, err := http.ReadRequest(reader)
					if err != nil {
						return
					}
					_, _ = io.Copy(io.Discard, req.Body)
					_ = req.Body.Close()
					// A mismatch is still a completed connection with address evidence.
					model := "gpt-6-astra"
					if index == 1 {
						model = "gpt-5.6-luna"
					}
					body := fmt.Sprintf("data: {\"type\":\"response.created\",\"response\":{\"model\":%q}}\n\n", model)
					_, err = fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n%s: %s\r\nContent-Type: text/event-stream\r\n\r\n%s", len(body), turnStateHeader, state, body)
					if err != nil {
						return
					}
				}
			}(index, conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	cfg := e.cfg.Config
	cfg.UpstreamURL = "http://probe.example.test/responses" // handled entirely by the loopback mock
	cfg.Timeout = 3 * time.Second
	proxy := "socks5h://" + listener.Addr().String()
	for i, expected := range []string{"203.0.113.10", "203.0.113.11", ""} {
		record, _ := e.probeOnce("gpt-6-astra", proxy, cfg)
		if record.EgressAddr != expected {
			t.Fatalf("attempt %d: bound address %q, want %q", i, record.EgressAddr, expected)
		}
		if i == 1 {
			if record.Success || !strings.Contains(record.Error, "model mismatch") {
				t.Fatal("a failed model check should retain the connection's address")
			}
		} else if !record.Success {
			t.Fatalf("attempt %d: unexpected failure: %s", i, record.Error)
		}
	}
	mu.Lock()
	count := len(connections)
	mu.Unlock()
	if count != 3 {
		t.Fatalf("got %d SOCKS connections for 3 probes", count)
	}
}
