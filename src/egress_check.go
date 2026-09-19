package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"time"
)

const egressCheckURL = "https://api64.ipify.org?format=json"

type egressSample struct {
	Time       string `json:"time"`
	IP         string `json:"ip,omitempty"`
	Error      string `json:"error,omitempty"`
	StatusCode int    `json:"status_code,omitempty"`
}

type egressCheckResult struct {
	ExitID  string         `json:"exit_id"`
	Source  string         `json:"source"`
	Samples []egressSample `json:"samples"`
	Error   string         `json:"error,omitempty"`
}

// The endpoint is fixed by the API caller, never taken from management input.
// These are independent diagnostic connections, not model probe evidence.
func (e *probeEngine) checkEgress(id, endpoint string) (egressCheckResult, int) {
	result := egressCheckResult{ExitID: id, Source: "ipify", Samples: []egressSample{}}
	e.mu.Lock()
	spec := ""
	for _, candidate := range e.cfg.Config.Proxies {
		if id != "" && exitID(e.cfg.Config, candidate) == id {
			spec = candidate
			break
		}
	}
	if spec == "" {
		e.mu.Unlock()
		result.Error = "unknown_exit"
		return result, http.StatusBadRequest
	}
	if e.egressChecking || e.shuttingDown {
		e.mu.Unlock()
		result.Error = "busy"
		return result, http.StatusConflict
	}
	e.egressChecking = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.egressChecking = false
		e.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if e.resolveExit(id, "") != spec {
			result.Samples = []egressSample{}
			result.Error = "exit_changed"
			return result, http.StatusConflict
		}
		result.Samples = append(result.Samples, sampleEgress(ctx, spec, endpoint))
	}
	// Do not attach a completed result to an exit edited while checking.
	if e.resolveExit(id, "") != spec {
		result.Samples = []egressSample{}
		result.Error = "exit_changed"
		return result, http.StatusConflict
	}
	return result, http.StatusOK
}

func sampleEgress(parent context.Context, spec, endpoint string) egressSample {
	sample := egressSample{Time: time.Now().UTC().Format(time.RFC3339Nano)}
	transport, _, err := buildProbeTransport(spec)
	if err != nil {
		sample.Error = "unsupported_proxy"
		return sample
	}
	defer transport.CloseIdleConnections()
	transport.DisableKeepAlives = true
	ctx, cancel := context.WithTimeout(parent, 6*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		sample.Error = "connection_failed"
		return sample
	}
	// No management/model authorization, account headers, cookies or state.
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Transport: transport, Timeout: 6 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		sample.Error = "connection_failed"
		if ctx.Err() != nil {
			sample.Error = "timeout"
		}
		return sample
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		sample.Error = "http_status"
		sample.StatusCode = resp.StatusCode
		return sample
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1025))
	if err != nil {
		sample.Error = "read_failed"
		return sample
	}
	var data struct {
		IP string `json:"ip"`
	}
	if len(body) > 1024 || json.Unmarshal(body, &data) != nil {
		sample.Error = "invalid_response"
		return sample
	}
	ip, err := netip.ParseAddr(strings.TrimSpace(data.IP))
	if err != nil || !publicEgressIP(ip) {
		sample.Error = "non_public_ip"
		return sample
	}
	sample.IP = ip.Unmap().String()
	return sample
}

func publicEgressIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if ip.Zone() != "" || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	for _, block := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32", "2001:2::/48", "2001:10::/28", "2001:20::/28", "3fff::/20", "100::/64"} {
		if netip.MustParsePrefix(block).Contains(ip) {
			return false
		}
	}
	return true
}
