// Command probetest issues one minimal upstream probe request through a chosen
// egress (direct / socks5 / http proxy) and reports whether a fresh
// X-Codex-Turn-State is returned. It is a one-shot verification tool for the
// V1.5 probe-track design; it is not part of the plugin binary.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/net/proxy"
)

func buildTransport(spec string) (*http.Transport, error) {
	transport := &http.Transport{
		TLSClientConfig:   &tls.Config{},
		ForceAttemptHTTP2: true,
		IdleConnTimeout:   30 * time.Second,
	}
	spec = strings.TrimSpace(spec)
	if spec == "" || spec == "direct" {
		return transport, nil
	}
	parsed, err := url.Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("parse proxy url: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "socks5", "socks5h":
		var auth *proxy.Auth
		if parsed.User != nil {
			password, _ := parsed.User.Password()
			auth = &proxy.Auth{User: parsed.User.Username(), Password: password}
		}
		dialer, err := proxy.SOCKS5("tcp", parsed.Host, auth, proxy.Direct)
		if err != nil {
			return nil, fmt.Errorf("socks5 dialer: %w", err)
		}
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			type contextDialer interface {
				DialContext(context.Context, string, string) (net.Conn, error)
			}
			if cd, ok := dialer.(contextDialer); ok {
				return cd.DialContext(ctx, network, address)
			}
			return dialer.Dial(network, address)
		}
	case "http", "https":
		transport.Proxy = http.ProxyURL(parsed)
	default:
		return nil, fmt.Errorf("unsupported proxy scheme %q", parsed.Scheme)
	}
	return transport, nil
}

func main() {
	credPath := flag.String("cred", "", "auth json path (access_token/account_id)")
	proxySpec := flag.String("proxy", "direct", "direct or proxy URL (socks5://... / http://...)")
	model := flag.String("model", "gpt-6-astra", "model to probe")
	baseURL := flag.String("url", "https://chatgpt.com/backend-api/codex/responses", "upstream responses endpoint")
	timeout := flag.Duration("timeout", 60*time.Second, "overall timeout")
	flag.Parse()

	if *credPath == "" {
		fmt.Println("missing -cred")
		os.Exit(2)
	}
	raw, err := os.ReadFile(*credPath)
	if err != nil {
		fmt.Println("read cred:", err)
		os.Exit(2)
	}
	var cred struct {
		AccessToken string `json:"access_token"`
		AccountID   string `json:"account_id"`
	}
	if err := json.Unmarshal(raw, &cred); err != nil || cred.AccessToken == "" {
		fmt.Println("parse cred:", err)
		os.Exit(2)
	}

	body := map[string]any{
		"model": *model,
		"input": []any{map[string]any{
			"type": "message", "role": "user",
			"content": []any{map[string]any{"type": "input_text", "text": "hi"}},
		}},
		"stream": true,
		"store":  false,
	}
	encoded, _ := json.Marshal(body)

	transport, err := buildTransport(*proxySpec)
	if err != nil {
		fmt.Println("transport:", err)
		os.Exit(2)
	}
	client := &http.Client{Transport: transport, Timeout: *timeout}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *baseURL, strings.NewReader(string(encoded)))
	if err != nil {
		fmt.Println("request:", err)
		os.Exit(2)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cred.AccessToken)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Connection", "Keep-Alive")
	req.Header.Set("Originator", "codex-tui")
	req.Header.Set("User-Agent", "codex-tui/0.154.0 (Mac OS 26.5.2; arm64) iTerm.app/3.6.11 (codex-tui; 0.154.0)")
	if strings.TrimSpace(cred.AccountID) != "" {
		req.Header.Set("Chatgpt-Account-Id", cred.AccountID)
	}

	started := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("STATUS=error elapsed=", time.Since(started), "err=", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	fmt.Println("STATUS=", resp.StatusCode, "elapsed=", time.Since(started))
	fmt.Println("== relevant response headers ==")
	found := ""
	for name, values := range resp.Header {
		lower := strings.ToLower(name)
		if strings.Contains(lower, "turn") || strings.Contains(lower, "codex") || strings.Contains(lower, "session") {
			for _, value := range values {
				fmt.Printf("  %s: %s\n", name, value)
				if lower == "x-codex-turn-state" {
					found = value
				}
			}
		}
	}
	if found != "" {
		fmt.Println("TURN_STATE_FOUND len=", len(found), "preview=", found[:min(32, len(found))])
	} else {
		fmt.Println("TURN_STATE_FOUND=no (checking body)")
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	lines := 0
	bodyHit := ""
	for scanner.Scan() && lines < 400 {
		lines++
		line := scanner.Text()
		if strings.Contains(line, "turn") || strings.Contains(line, "state") {
			bodyHit = line
			break
		}
	}
	if bodyHit != "" {
		fmt.Println("BODY_HINT=", bodyHit[:min(300, len(bodyHit))])
	} else {
		fmt.Println("BODY_HINT=none (scanned", lines, "lines)")
	}
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20)); err != nil {
		fmt.Println("drain:", err)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
