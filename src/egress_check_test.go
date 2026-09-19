package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEgressCheckUsesSelectedProxyAndIndependentConnections(t *testing.T) {
	e := newPrefetchTestEngine(t)
	e.halted = true
	e.paused["gpt-6-astra"] = true
	var mu sync.Mutex
	connections := map[string]bool{}
	requests := 0
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		requests++
		connections[r.RemoteAddr] = true
		if r.Method != "GET" || r.URL.Host != "echo.invalid" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Chatgpt-Account-Id") != "" {
			t.Error("sampling must route only through the selected proxy without model/management credentials")
		}
		if requests == 2 {
			w.WriteHeader(503)
			fmt.Fprint(w, "SENSITIVE_ERROR_BODY")
			return
		}
		fmt.Fprint(w, `{"ip":"8.8.8.8"}`) // synthetic payload, no external call
	}))
	defer proxy.Close()
	e.cfg.Config.Proxies = []string{proxy.URL}
	e.disabledExits = map[string]bool{proxy.URL: true}
	id := exitID(e.cfg.Config, proxy.URL)
	result, status := e.checkEgress(id, "http://echo.invalid/?format=json")
	if status != 200 || result.Source != "ipify" || len(result.Samples) != 3 || result.Samples[0].IP != "8.8.8.8" || result.Samples[1].Error != "http_status" || result.Samples[1].StatusCode != 503 || result.Samples[2].IP != "8.8.8.8" {
		t.Fatalf("unexpected sampling result: %+v", result)
	}
	mu.Lock()
	count, uniqueConnections := requests, len(connections)
	mu.Unlock()
	if count != 3 || uniqueConnections != 3 {
		t.Fatal("each sample must use a new connection through the selected proxy")
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), "SENSITIVE_ERROR_BODY") || strings.Contains(string(encoded), proxy.URL) {
		t.Fatal("raw response bodies and proxy URLs must not appear in diagnostics")
	}
	if !e.halted || !e.paused["gpt-6-astra"] || !e.disabledExits[proxy.URL] || e.probesTotal != 0 || e.probesOK != 0 || e.queueActive || len(e.history) != 0 || e.egressChecking {
		t.Fatal("diagnostics must preserve probe state, counters, history and exit enablement")
	}
	for _, sample := range result.Samples {
		if _, err := time.Parse(time.RFC3339Nano, sample.Time); err != nil {
			t.Fatal("samples need individual timestamps")
		}
	}
}

func TestEgressSampleRejectsUnusableResponsesAndRedirects(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"private", `{"ip":"10.255.1.4"}`, "non_public_ip"},
		{"unspecified", `{"ip":"0.0.0.0"}`, "non_public_ip"},
		{"cgnat", `{"ip":"100.64.0.1"}`, "non_public_ip"},
		{"documentation", `{"ip":"2001:db8::1"}`, "non_public_ip"},
		{"invalid ip", `{"ip":"not-an-ip"}`, "non_public_ip"},
		{"html", "<html>SENSITIVE_ERROR_BODY</html>", "invalid_response"},
		{"oversized", strings.Repeat("x", 1025), "invalid_response"},
		{"public v6", `{"ip":"2606:4700:4700::1111"}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, tc.body) }))
			defer server.Close()
			got := sampleEgress(context.Background(), "direct", server.URL)
			if got.Error != tc.want || (tc.want != "" && got.IP != "") {
				t.Fatalf("got %+v, want error %q", got, tc.want)
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			t.Error("must not follow a redirect")
		}
		http.Redirect(w, r, "/unexpected", 302)
	}))
	defer server.Close()
	if got := sampleEgress(context.Background(), "direct", server.URL); got.Error != "http_status" || got.StatusCode != 302 {
		t.Fatalf("redirect must stay a failed sample: %+v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := sampleEgress(ctx, "direct", server.URL); got.Error != "timeout" {
		t.Fatalf("cancellation must remain bounded: %+v", got)
	}
	for _, ip := range []string{"127.0.0.1", "::1", "::", "169.254.1.1", "224.0.0.1", "fc00::1", "::ffff:10.0.0.1", "198.18.0.1", "::2", "3fff::1", "2001:20::1"} {
		if publicEgressIP(netip.MustParseAddr(ip)) {
			t.Fatalf("non-public address accepted: %s", ip)
		}
	}
}

func TestEgressCheckGuardsUnknownBusyAndChangedExits(t *testing.T) {
	e := newPrefetchTestEngine(t)
	e.cfg.Config.Proxies = []string{"direct"}
	id := exitID(e.cfg.Config, "direct")
	for _, body := range []string{`{"action":"check-egress"}`, `{"action":"check-egress","id":"arbitrary","proxy":"http://127.0.0.1"}`} {
		response, _ := probeControl([]byte(body))
		if response.StatusCode != 400 || !strings.Contains(string(response.Body), "unknown_exit") {
			t.Fatal("management must reject unknown IDs without making requests")
		}
	}
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		<-release
		fmt.Fprint(w, `{"ip":"1.1.1.1"}`)
	}))
	defer server.Close()
	// Release blocked handlers before closing the server if an assertion fails.
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	done := make(chan egressCheckResult, 1)
	go func() { result, _ := e.checkEgress(id, server.URL); done <- result }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("check did not reach the loopback service")
	}
	result, status := e.checkEgress(id, server.URL)
	if status != 409 || result.Error != "busy" {
		t.Fatal("only one diagnostic batch may run at a time")
	}
	e.mu.Lock()
	e.cfg.Config.Proxies = nil
	e.mu.Unlock()
	releaseOnce.Do(func() { close(release) })
	if result := <-done; result.Error != "exit_changed" || len(result.Samples) != 0 {
		t.Fatal("editing the exit must invalidate in-flight diagnostic results")
	}
}

func TestEgressSampleDoesNotBypassFailedProxyOrTLSValidation(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("a failed proxy must not fall back to direct")
	}))
	defer target.Close()
	closedProxy := httptest.NewServer(http.NotFoundHandler())
	closedProxy.Close()
	if got := sampleEgress(context.Background(), closedProxy.URL, target.URL); got.Error != "connection_failed" || got.IP != "" {
		t.Fatalf("proxy failure must remain a failure: %+v", got)
	}
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("an untrusted certificate must not be accepted")
	}))
	defer tlsServer.Close()
	if got := sampleEgress(context.Background(), "direct", tlsServer.URL); got.Error != "connection_failed" {
		t.Fatalf("TLS validation must stay enabled: %+v", got)
	}
}
