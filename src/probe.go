package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/proxy"
)

// This file implements the V1.5 probe track: a background engine that, inside
// the validity window of each model's X-Codex-Turn-State, sends a minimal
// upstream request through a rotating proxy pool and captures a fresh state.
// The captured values are hot-swapped into the rewrite engine so business
// requests always overwrite with a currently-valid state.
//
// The probe track is deliberately isolated: it never touches the business
// request path, only reads auth material, and can be switched off entirely
// with probe.enabled=false.

const (
	probeDefaultsTTLMinutes      = 55
	probeDefaultsWindowMinutes   = 5
	probeDefaultsScanSeconds     = 30
	probeDefaultsIntervalSeconds = 5
	probeDefaultsAttemptsPerHop  = 3
	probeDefaultsMaxAttempts     = 30
	probeDefaultsCooldownMinutes = 20
	probeDefaultsTimeoutSeconds  = 60
	probeRequiredStateLength     = 292
	probeDefaultsPrompt          = "hi"
	probeDefaultsUpstreamURL     = "https://chatgpt.com/backend-api/codex/responses"
	probeDefaultsCredFile        = "/root/.cli-proxy-api/your-codex-auth.json"
	probeHistoryLimit            = 200
)

// probeConfig is the parsed probe-track configuration block.
type probeConfig struct {
	Enabled             bool
	Models              []string
	CredFile            string
	Proxies             []string
	TTL                 time.Duration
	Window              time.Duration
	ScanInterval        time.Duration
	ProbeInterval       time.Duration
	AttemptsPerHop      int
	MaxAttemptsPerRound int
	Cooldown            time.Duration
	Timeout             time.Duration
	Prompt              string
	UpstreamURL         string
	SecretsFile         string
}

// probeConfigYAML mirrors the YAML keys accepted under turn-state-override.probe.
type probeConfigYAML struct {
	Enabled          *bool    `yaml:"enabled"`
	Models           []string `yaml:"models"`
	CredFile         string   `yaml:"cred-file"`
	Proxies          []string `yaml:"proxies"`
	ProxiesFile      string   `yaml:"proxies-file"`
	TTLMinutes       *int     `yaml:"ttl-minutes"`
	WindowMinutes    *int     `yaml:"probe-window-minutes"`
	ScanSeconds      *int     `yaml:"scan-interval-seconds"`
	IntervalSeconds  *int     `yaml:"probe-interval-seconds"`
	AttemptsPerHop   *int     `yaml:"attempts-per-proxy"`
	MaxAttemptsRound *int     `yaml:"max-attempts-per-round"`
	CooldownMinutes  *int     `yaml:"cooldown-minutes"`
	TimeoutSeconds   *int     `yaml:"timeout-seconds"`
	Prompt           string   `yaml:"prompt"`
	UpstreamURL      string   `yaml:"upstream-url"`
}

type probeConfigState struct {
	Config probeConfig
	Error  string
}

// stateEntry is one captured upstream turn-state value.
type stateEntry struct {
	Model       string `json:"model"`
	Value       string `json:"value,omitempty"`
	ValueLength int    `json:"value_length"`
	GeneratedAt string `json:"generated_at,omitempty"`
	CapturedAt  string `json:"captured_at"`
	ExpiresAt   string `json:"expires_at,omitempty"`
	Source      string `json:"source,omitempty"`
	Proxy       string `json:"proxy,omitempty"`
	Valid       bool   `json:"valid"`
}

// probeRecord is one probe attempt for the audit trail.
type probeRecord struct {
	Time          string `json:"time"`
	Model         string `json:"model"`
	Proxy         string `json:"proxy"`
	Success       bool   `json:"success"`
	StatusCode    int    `json:"status_code,omitempty"`
	DurationMS    int64  `json:"duration_ms"`
	StateLength   int    `json:"state_length,omitempty"`
	ObservedModel string `json:"observed_model,omitempty"`
	Error         string `json:"error,omitempty"`
}

// probeFailure marks a model whose latest probe round exhausted all retries
// without obtaining an acceptable (292-byte, consistent) state. CooldownUntil
// is the end of the quiet period; new rounds are suppressed until it passes.
type probeFailure struct {
	Model      string `json:"model"`
	Attempts   int    `json:"attempts"`
	Rounds     int    `json:"rounds"`
	LastError  string `json:"last_error,omitempty"`
	LastLength int    `json:"last_length,omitempty"`
	FailedAt   string `json:"failed_at"`
	CooldownUntil string `json:"cooldown_until,omitempty"`
}

type probeEngine struct {
	mu             sync.Mutex
	cfg            probeConfigState
	values         map[string]stateEntry
	failures       map[string]probeFailure
	paused         map[string]bool
	probing        map[string]bool
	rejectDegraded bool
	history        []probeRecord
	proxyIndex     int
	consecutive    int
	lastScan       time.Time
	lastActivity   string
	stopCh         chan struct{}
	running        bool
	probesTotal    uint64
	probesOK       uint64
	lastError      string
}

var probeTrack = &probeEngine{
	values:   map[string]stateEntry{},
	failures: map[string]probeFailure{},
	paused:   map[string]bool{},
	probing:  map[string]bool{},
}

// ---------------------------------------------------------------------------
// configuration

func parseProbeConfig(block probeConfigYAML) probeConfig {
	cfg := probeConfig{
		Enabled:             block.Enabled != nil && *block.Enabled,
		Models:              append([]string(nil), block.Models...),
		CredFile:            strings.TrimSpace(block.CredFile),
		Proxies:             append([]string(nil), block.Proxies...),
		TTL:                 time.Duration(probeDefaultsTTLMinutes) * time.Minute,
		Window:              time.Duration(probeDefaultsWindowMinutes) * time.Minute,
		ScanInterval:        time.Duration(probeDefaultsScanSeconds) * time.Second,
		ProbeInterval:       time.Duration(probeDefaultsIntervalSeconds) * time.Second,
		AttemptsPerHop:      probeDefaultsAttemptsPerHop,
		MaxAttemptsPerRound: probeDefaultsMaxAttempts,
		Cooldown:            time.Duration(probeDefaultsCooldownMinutes) * time.Minute,
		Timeout:             time.Duration(probeDefaultsTimeoutSeconds) * time.Second,
		Prompt:              probeDefaultsPrompt,
		UpstreamURL:         probeDefaultsUpstreamURL,
		SecretsFile:         strings.TrimSpace(block.ProxiesFile),
	}
	if len(cfg.Models) == 0 {
		cfg.Models = []string{"gpt-6-astra", "gpt-5.6-sol", "gpt-5.6-luna", "gpt-5.6-terra"}
	}
	if cfg.CredFile == "" {
		cfg.CredFile = probeDefaultsCredFile
	}
	if block.TTLMinutes != nil && *block.TTLMinutes > 0 {
		cfg.TTL = time.Duration(*block.TTLMinutes) * time.Minute
	}
	if block.WindowMinutes != nil && *block.WindowMinutes > 0 {
		cfg.Window = time.Duration(*block.WindowMinutes) * time.Minute
	}
	if block.ScanSeconds != nil && *block.ScanSeconds > 0 {
		cfg.ScanInterval = time.Duration(*block.ScanSeconds) * time.Second
	}
	if block.IntervalSeconds != nil && *block.IntervalSeconds > 0 {
		cfg.ProbeInterval = time.Duration(*block.IntervalSeconds) * time.Second
	}
	if block.AttemptsPerHop != nil && *block.AttemptsPerHop > 0 {
		cfg.AttemptsPerHop = *block.AttemptsPerHop
	}
	if block.MaxAttemptsRound != nil && *block.MaxAttemptsRound > 0 {
		cfg.MaxAttemptsPerRound = *block.MaxAttemptsRound
	}
	if block.CooldownMinutes != nil && *block.CooldownMinutes > 0 {
		cfg.Cooldown = time.Duration(*block.CooldownMinutes) * time.Minute
	}
	if block.TimeoutSeconds != nil && *block.TimeoutSeconds > 0 {
		cfg.Timeout = time.Duration(*block.TimeoutSeconds) * time.Second
	}
	if trimmed := strings.TrimSpace(block.Prompt); trimmed != "" {
		cfg.Prompt = trimmed
	}
	if trimmed := strings.TrimSpace(block.UpstreamURL); trimmed != "" {
		cfg.UpstreamURL = trimmed
	}
	// Proxies may come inline or from the dedicated secrets file. The secrets
	// file also overrides inline entries when present.
	if len(cfg.Proxies) == 0 {
		cfg.Proxies = []string{"direct"}
	}
	return cfg
}

// loadProxiesFile reads the dedicated proxy secret file (one proxy per line,
// "#" comments and blank lines ignored) and returns the parsed list.
func loadProxiesFile(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var proxies []string
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		proxies = append(proxies, line)
	}
	if len(proxies) == 0 {
		return nil, fmt.Errorf("proxies file %s contains no entries", path)
	}
	return proxies, nil
}

func configureProbeTrack(block probeConfigYAML) error {
	cfg := parseProbeConfig(block)
	var cfgErr string
	if cfg.Enabled {
		if strings.TrimSpace(cfg.SecretsFile) != "" {
			if proxies, err := loadProxiesFile(cfg.SecretsFile); err == nil {
				cfg.Proxies = proxies
			} else {
				cfgErr = fmt.Sprintf("load proxies file: %v", err)
			}
		}
	}
	probeTrack.mu.Lock()
	probeTrack.stopLocked()
	probeTrack.cfg = probeConfigState{Config: cfg, Error: cfgErr}
	probeTrack.mu.Unlock()
	if cfg.Enabled {
		probeTrack.start()
	}
	return nil
}

// currentProbeConfig returns a copy of the active probe configuration.
func currentProbeConfig() probeConfigState {
	probeTrack.mu.Lock()
	defer probeTrack.mu.Unlock()
	return probeTrack.cfg
}

// ---------------------------------------------------------------------------
// engine lifecycle

func (e *probeEngine) start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.stopCh = make(chan struct{})
	stop := e.stopCh
	e.mu.Unlock()
	go e.loop(stop)
}

func (e *probeEngine) stop() {
	e.mu.Lock()
	e.stopLocked()
	e.mu.Unlock()
}

func (e *probeEngine) stopLocked() {
	if e.running && e.stopCh != nil {
		close(e.stopCh)
	}
	e.running = false
	e.stopCh = nil
}

func (e *probeEngine) loop(stop chan struct{}) {
	// Small initial delay so plugin registration completes first.
	select {
	case <-time.After(5 * time.Second):
	case <-stop:
		return
	}
	for {
		e.scanOnce(stop)
		e.mu.Lock()
		interval := e.cfg.Config.ScanInterval
		e.mu.Unlock()
		if interval <= 0 {
			interval = time.Duration(probeDefaultsScanSeconds) * time.Second
		}
		select {
		case <-time.After(interval):
		case <-stop:
			return
		}
	}
}

// scanOnce checks each model in priority order and probes the first one whose
// state is missing or enters the configured probe window.
func (e *probeEngine) scanOnce(stop chan struct{}) {
	e.mu.Lock()
	cfg := e.cfg.Config
	e.lastScan = time.Now().UTC()
	e.mu.Unlock()
	if !cfg.Enabled {
		return
	}
	for _, model := range cfg.Models {
		if e.probeSuppressed(model) {
			continue
		}
		if e.needsProbe(model, cfg) {
			e.probeModel(model, cfg, stop)
			select {
			case <-stop:
				return
			default:
			}
		}
	}
}

// needsProbe reports whether a model's current state is missing or about to
// expire (inside the configured probe window). A failure cooldown suppresses
// new rounds until cooldown-minutes have elapsed since the failed round.
func (e *probeEngine) needsProbe(model string, cfg probeConfig) bool {
	e.mu.Lock()
	entry, ok := e.values[model]
	failure, hasFailure := e.failures[model]
	e.mu.Unlock()
	if hasFailure {
		if until, err := time.Parse(time.RFC3339Nano, failure.CooldownUntil); err == nil && time.Now().UTC().Before(until) {
			return false
		}
	}
	if !ok || !entry.Valid || entry.Value == "" {
		return true
	}
	generated, okTime := parseTurnStateTimestamp(entry.Value)
	if !okTime {
		return true
	}
	remaining := time.Until(generated.Add(cfg.TTL))
	return remaining <= cfg.Window
}

// probeSuppressed reports whether automatic probing for a model is held off:
// the model was paused from the dashboard or a round is already running.
func (e *probeEngine) probeSuppressed(model string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.paused[model] || e.probing[model]
}

// setModelPaused pauses or resumes a model from the dashboard. Pausing keeps
// the model out of the automatic queue while preserving its value and failure
// record; resuming clears the stale annotation and starts one probe round in
// the background.
func (e *probeEngine) setModelPaused(model string, paused bool) {
	e.mu.Lock()
	if e.paused == nil {
		e.paused = map[string]bool{}
	}
	if paused {
		e.paused[model] = true
	} else {
		delete(e.paused, model)
		delete(e.failures, model)
	}
	e.mu.Unlock()
	if !paused {
		e.startProbeAsync(model)
	}
}

// startProbeAsync launches one probe round in the background for the manual
// resume action. probeModel guards against overlapping rounds itself.
func (e *probeEngine) startProbeAsync(model string) {
	e.mu.Lock()
	cfg := e.cfg.Config
	e.mu.Unlock()
	if !cfg.Enabled {
		return
	}
	go e.probeModel(model, cfg, make(chan struct{}))
}

// pausedModels lists the models currently paused from the dashboard.
func (e *probeEngine) pausedModels() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	models := make([]string, 0, len(e.paused))
	for _, model := range e.cfg.Config.Models {
		if e.paused[model] {
			models = append(models, model)
		}
	}
	return models
}

// setRejectDegraded toggles the degraded-model rejection switch.
func (e *probeEngine) setRejectDegraded(enabled bool) {
	e.mu.Lock()
	e.rejectDegraded = enabled
	e.mu.Unlock()
}

// rejectDegradedEnabled returns the current switch state.
func (e *probeEngine) rejectDegradedEnabled() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rejectDegraded
}

// degradedRejectReason reports the Chinese reason used by the degraded-model
// rejection when the switch is enabled and the model's latest failed round
// carries degradation evidence (length anomaly or model mismatch). Rate
// limits, timeouts and network errors do not count as degradation.
func (e *probeEngine) degradedRejectReason(model string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.rejectDegraded {
		return ""
	}
	candidate := strings.ToLower(strings.TrimSpace(model))
	for key, failure := range e.failures {
		if !strings.HasPrefix(candidate, strings.ToLower(key)) {
			continue
		}
		switch {
		case strings.Contains(failure.LastError, "state length"):
			return "上游状态长度异常（疑似风控降级）"
		case strings.Contains(failure.LastError, "model mismatch"):
			return "上游请求被路由至其它模型（模型不一致）"
		}
	}
	return ""
}

// degradedRejectMessage evaluates the switch for one request and returns the
// Chinese 403 message when the request targets a degraded model.
func degradedRejectMessage(models ...string) string {
	for _, candidate := range models {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if reason := probeTrack.degradedRejectReason(candidate); reason != "" {
			return fmt.Sprintf("模型 %s 当前处于风控降智状态：%s。请求已被 O/对抗插件拦截，请稍后重试或切换模型。", candidate, reason)
		}
	}
	return ""
}

// probeModel runs the probe sequence for one model: repeated rounds of up to
// attempts-per-proxy attempts per egress, rotating egress after each failed
// round, until an acceptable state is captured (success clears any failure
// mark) or max-attempts-per-round total attempts are exhausted (failure is
// recorded for the dashboard).
func (e *probeEngine) probeModel(model string, cfg probeConfig, stop chan struct{}) {
	e.mu.Lock()
	if e.probing == nil {
		e.probing = map[string]bool{}
	}
	if e.probing[model] {
		e.mu.Unlock()
		return
	}
	e.probing[model] = true
	proxies := append([]string(nil), cfg.Proxies...)
	startIndex := e.proxyIndex % len(proxies)
	e.mu.Unlock()
	if len(proxies) == 0 {
		e.mu.Lock()
		delete(e.probing, model)
		e.mu.Unlock()
		return
	}
	defer func() {
		e.mu.Lock()
		delete(e.probing, model)
		e.mu.Unlock()
	}()
	maxAttempts := cfg.MaxAttemptsPerRound
	if maxAttempts <= 0 {
		maxAttempts = probeDefaultsMaxAttempts
	}
	attempts := 0
	lastError := ""
	lastLength := 0
	for round := 0; attempts < maxAttempts; round++ {
		for hop := 0; hop < cfg.AttemptsPerHop && attempts < maxAttempts; hop++ {
			index := (startIndex + round*cfg.AttemptsPerHop + hop) % len(proxies)
			proxySpec := proxies[index]
			select {
			case <-stop:
				return
			default:
			}
			attempts++
			record, value := e.probeOnce(model, proxySpec, cfg)
			e.appendRecord(record)
			if value != "" {
				e.storeValue(model, value, proxySpec, cfg)
				e.mu.Lock()
				delete(e.failures, model)
				e.proxyIndex = index
				e.consecutive = 0
				e.mu.Unlock()
				return
			}
			lastError = record.Error
			if record.StateLength > 0 {
				lastLength = record.StateLength
			}
			select {
			case <-time.After(cfg.ProbeInterval):
			case <-stop:
				return
			}
		}
		// Round boundary: rotate the starting egress for the next round.
		e.mu.Lock()
		e.consecutive++
		e.proxyIndex = (startIndex + (round+1)*cfg.AttemptsPerHop) % len(proxies)
		e.mu.Unlock()
	}
	// Retries exhausted: annotate the failure for the dashboard and start the
	// cooldown window; the next round is suppressed until it elapses.
	now := time.Now().UTC()
	e.mu.Lock()
	e.failures[model] = probeFailure{
		Model:         model,
		Attempts:      attempts,
		Rounds:        (attempts + cfg.AttemptsPerHop - 1) / cfg.AttemptsPerHop,
		LastError:     lastError,
		LastLength:    lastLength,
		FailedAt:      now.Format(time.RFC3339Nano),
		CooldownUntil: now.Add(cfg.Cooldown).Format(time.RFC3339Nano),
	}
	e.mu.Unlock()
}

// probeOnce sends one minimal upstream request through the given egress and
// returns its record plus the captured state (empty on failure).
func (e *probeEngine) probeOnce(model, proxySpec string, cfg probeConfig) (probeRecord, string) {
	started := time.Now()
	record := probeRecord{
		Time:   started.UTC().Format(time.RFC3339Nano),
		Model:  model,
		Proxy:  proxySpec,
		Success: false,
	}
	cred, err := readProbeCredential(cfg.CredFile)
	if err != nil {
		record.DurationMS = time.Since(started).Milliseconds()
		record.Error = fmt.Sprintf("read cred: %v", err)
		e.noteError(record.Error)
		return record, ""
	}
	transport, err := buildProbeTransport(proxySpec)
	if err != nil {
		record.DurationMS = time.Since(started).Milliseconds()
		record.Error = fmt.Sprintf("transport: %v", err)
		e.noteError(record.Error)
		return record, ""
	}
	client := &http.Client{Transport: transport, Timeout: cfg.Timeout}

	payload := map[string]any{
		"model": model,
		"input": []any{map[string]any{
			"type": "message", "role": "user",
			"content": []any{map[string]any{"type": "input_text", "text": cfg.Prompt}},
		}},
		"stream": true,
		"store":  false,
	}
	encoded, _ := json.Marshal(payload)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.UpstreamURL, strings.NewReader(string(encoded)))
	if err != nil {
		record.DurationMS = time.Since(started).Milliseconds()
		record.Error = fmt.Sprintf("request: %v", err)
		e.noteError(record.Error)
		return record, ""
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cred.AccessToken)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Connection", "Keep-Alive")
	req.Header.Set("Originator", "codex-tui")
	req.Header.Set("User-Agent", "codex-tui/0.154.0 (Mac OS 26.5.2; arm64) iTerm.app/3.6.11 (codex-tui; 0.154.0)")
	if cred.AccountID != "" {
		req.Header.Set("Chatgpt-Account-Id", cred.AccountID)
	}

	resp, err := client.Do(req)
	record.DurationMS = time.Since(started).Milliseconds()
	if err != nil {
		record.Error = fmt.Sprintf("do: %v", err)
		e.noteError(record.Error)
		return record, ""
	}
	defer resp.Body.Close()
	record.StatusCode = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		// Read a bounded snippet for diagnostics.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		record.Error = fmt.Sprintf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
		e.noteError(record.Error)
		return record, ""
	}
	state := strings.TrimSpace(resp.Header.Get(turnStateHeader))
	// Read the first SSE data event: it carries the upstream model claim used
	// for the model-consistency check (bounded to a small number of lines).
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	firstEvent := ""
	for i := 0; i < 24 && scanner.Scan(); i++ {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			firstEvent = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			break
		}
	}
	// Drain in the background so the connection can be reused cleanly.
	go func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	}()
	if state == "" {
		record.Error = "no turn-state header in response"
		e.noteError(record.Error)
		return record, ""
	}
	// Model consistency: the upstream must confirm the requested model before
	// the captured state is accepted as valid.
	observedModel, okModel := probeUpstreamModel([]byte(firstEvent))
	if !okModel {
		record.Error = "model evidence missing in first event"
		e.noteError(record.Error)
		return record, ""
	}
	if !probeModelConsistent(model, observedModel) {
		record.Error = fmt.Sprintf("model mismatch: requested %s got %s", model, observedModel)
		record.ObservedModel = observedModel
		e.noteError(record.Error)
		return record, ""
	}
	// Length acceptance: only the pristine 292-byte state is valid. Other
	// lengths (for example 312) are treated as risk-controlled degraded
	// states and rejected, so the probe keeps retrying.
	if len(state) != probeRequiredStateLength {
		record.Error = fmt.Sprintf("state length %d != %d (suspected degraded)", len(state), probeRequiredStateLength)
		record.ObservedModel = observedModel
		record.StateLength = len(state)
		e.noteError(record.Error)
		return record, ""
	}
	record.Success = true
	record.StateLength = len(state)
	record.ObservedModel = observedModel
	e.mu.Lock()
	e.probesTotal++
	e.probesOK++
	e.lastError = ""
	e.lastActivity = fmt.Sprintf("%s via %s", model, proxySpec)
	e.mu.Unlock()
	return record, state
}

func (e *probeEngine) noteError(message string) {
	e.mu.Lock()
	e.probesTotal++
	e.lastError = message
	e.mu.Unlock()
}

// storeValue saves a freshly captured state as the active value for the model.
func (e *probeEngine) storeValue(model, value, proxySpec string, cfg probeConfig) {
	generated := ""
	expires := ""
	if ts, ok := parseTurnStateTimestamp(value); ok {
		generated = ts.UTC().Format(time.RFC3339)
		expires = ts.Add(cfg.TTL).UTC().Format(time.RFC3339)
	}
	entry := stateEntry{
		Model:       model,
		Value:       value,
		ValueLength: len(value),
		GeneratedAt: generated,
		CapturedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		ExpiresAt:   expires,
		Source:      "probe",
		Proxy:       proxySpec,
		Valid:       true,
	}
	e.mu.Lock()
	e.values[model] = entry
	e.mu.Unlock()
}

// activeValueFor returns the currently valid probe-captured state for a model.
func (e *probeEngine) activeValueFor(model string) string {
	cfg := currentProbeConfig().Config
	if !cfg.Enabled {
		return ""
	}
	e.mu.Lock()
	entry, ok := e.values[model]
	e.mu.Unlock()
	if !ok || !entry.Valid || entry.Value == "" {
		return ""
	}
	if ts, okTime := parseTurnStateTimestamp(entry.Value); okTime {
		if time.Since(ts) > cfg.TTL {
			return ""
		}
	}
	return entry.Value
}

func (e *probeEngine) appendRecord(record probeRecord) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.history) >= probeHistoryLimit {
		copy(e.history, e.history[1:])
		e.history[len(e.history)-1] = record
		return
	}
	e.history = append(e.history, record)
}

// ---------------------------------------------------------------------------
// helpers

type probeCredential struct {
	AccessToken string `json:"access_token"`
	AccountID   string `json:"account_id"`
}

func readProbeCredential(path string) (probeCredential, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return probeCredential{}, err
	}
	var cred probeCredential
	if err := json.Unmarshal(raw, &cred); err != nil {
		return probeCredential{}, err
	}
	if strings.TrimSpace(cred.AccessToken) == "" {
		return probeCredential{}, fmt.Errorf("access_token empty")
	}
	return cred, nil
}

// probeModelConsistent reports whether the upstream-reported model matches the
// requested model. Matching is case-insensitive and accepts suffix variants
// (for example "gpt-6-astra" vs "gpt-6-astra-preview").
func probeModelConsistent(requested, observed string) bool {
	requested = strings.ToLower(strings.TrimSpace(requested))
	observed = strings.ToLower(strings.TrimSpace(observed))
	if requested == "" || observed == "" {
		return false
	}
	return observed == requested || strings.HasPrefix(observed, requested)
}

// parseTurnStateTimestamp decodes the embedded Fernet timestamp of a
// X-Codex-Turn-State token: base64url([version][8-byte BE unix seconds][...]).
func parseTurnStateTimestamp(value string) (time.Time, bool) {
	raw, err := base64.URLEncoding.DecodeString(value)
	if err != nil || len(raw) < 9 || raw[0] != 0x80 {
		return time.Time{}, false
	}
	seconds := binary.BigEndian.Uint64(raw[1:9])
	return time.Unix(int64(seconds), 0).UTC(), true
}

// buildProbeTransport builds an HTTP transport bound to the given egress:
// "direct", "socks5://...", or "http(s)://...".
func buildProbeTransport(spec string) (*http.Transport, error) {
	transport := &http.Transport{
		TLSClientConfig:   &tls.Config{},
		ForceAttemptHTTP2: true,
		IdleConnTimeout:   30 * time.Second,
	}
	spec = strings.TrimSpace(spec)
	if spec == "" || strings.EqualFold(spec, "direct") {
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

// probeSummary describes the probe engine state for the management API.
func probeSummary() map[string]any {
	cfgState := currentProbeConfig()
	cfg := cfgState.Config
	probeTrack.mu.Lock()
	values := make([]stateEntry, 0, len(probeTrack.values))
	for _, model := range cfg.Models {
		if entry, ok := probeTrack.values[model]; ok {
			values = append(values, entry)
		} else {
			values = append(values, stateEntry{Model: model})
		}
	}
	history := make([]probeRecord, len(probeTrack.history))
	copy(history, probeTrack.history)
	// newest first
	for i, j := 0, len(history)-1; i < j; i, j = i+1, j-1 {
		history[i], history[j] = history[j], history[i]
	}
	failures := make([]probeFailure, 0, len(cfg.Models))
	for _, model := range cfg.Models {
		if failure, ok := probeTrack.failures[model]; ok {
			failures = append(failures, failure)
		}
	}
	paused := make([]string, 0, len(cfg.Models))
	for _, model := range cfg.Models {
		if probeTrack.paused[model] {
			paused = append(paused, model)
		}
	}
	summary := map[string]any{
		"enabled":      cfg.Enabled,
		"error":        cfgState.Error,
		"models":       append([]string(nil), cfg.Models...),
		"proxies":      append([]string(nil), cfg.Proxies...),
		"proxy_index":  probeTrack.proxyIndex,
		"ttl_minutes":  int(cfg.TTL / time.Minute),
		"window_minutes": int(cfg.Window / time.Minute),
		"scan_seconds": int(cfg.ScanInterval / time.Second),
		"interval_seconds": int(cfg.ProbeInterval / time.Second),
		"attempts_per_proxy": cfg.AttemptsPerHop,
		"cooldown_minutes": int(cfg.Cooldown / time.Minute),
		"paused":           paused,
		"reject_degraded":  probeTrack.rejectDegraded,
		"running":      probeTrack.running,
		"probes_total": probeTrack.probesTotal,
		"probes_ok":    probeTrack.probesOK,
		"last_error":   probeTrack.lastError,
		"last_activity": probeTrack.lastActivity,
		"values":       values,
		"failures":     failures,
		"history":      history,
	}
	probeTrack.mu.Unlock()
	return summary
}

// probeTrackShutdown is called from plugin.shutdown.
func probeTrackShutdown() {
	probeTrack.stop()
}
