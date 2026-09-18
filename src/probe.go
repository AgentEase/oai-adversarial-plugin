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
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// This file implements the manual probe track: on explicit dashboard request
// ("start-round") a single round walks the configured models in priority order
// and captures a fresh X-Codex-Turn-State through the rotating egress pool.
// Accepted captures (model-consistent, 292 bytes) replace the model's healthy
// baseline, which the rewrite engine serves to business requests. Nothing is
// scheduled automatically: no scan loop, no timers, no auto-recovery - idle
// is the default state, and business observations refresh baselines passively.
//
// The probe track never touches the business request path, only reads auth
// material, and is gated by probe.enabled.

const (
	probeDefaultsTTLMinutes      = 55
	probeDefaultsWindowMinutes   = 5
	probeDefaultsScanSeconds     = 30
	probeDefaultsIntervalSeconds = 5
	probeDefaultsAttemptsPerHop  = 3
	probeDefaultsMaxAttempts     = 30
	probeDefaultsCooldownMinutes = 20
	probeDefaultsSuspectThreshold = 3
	probeDefaultsExitCooldownMinutes = 180
	probeDefaultsExitFailThreshold = 3
	probeDefaultsExitPoolFailThreshold = 10
	probeDefaultsExitMinActive = 1
	probeDefaultsPrefetchMinutes = 5
	prefetchRetryWindow = 5 * time.Minute
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
	ExitCooldown        time.Duration
	ExitFailThreshold   int
	ExitPoolFailThreshold int
	ExitMinActive       int
	Prefetch            time.Duration
	SuspectThreshold    int
	Timeout             time.Duration
	Prompt              string
	UpstreamURL         string
	SecretsFile         string
	ProxyPools          map[string]bool
	ProxyLabels         map[string]string
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
	ExitCooldownMinutes *int  `yaml:"exit-cooldown-minutes"`
	ExitFailThreshold   *int  `yaml:"exit-fail-threshold"`
	ExitPoolFailThreshold *int `yaml:"exit-pool-fail-threshold"`
	ExitMinActive       *int  `yaml:"exit-min-active"`
	PrefetchMinutes     *int  `yaml:"prefetch-minutes"`
	SuspectThreshold *int     `yaml:"suspect-threshold"`
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
	EgressAddr    string `json:"egress_addr,omitempty"`
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

// probeSuspicion marks a model whose current probe round has accumulated
// enough degradation-evidence failures to be treated as degraded for the
// rejection switch before the round completes. It is cleared by a successful
// probe or promoted to a full probeFailure annotation when the round is
// exhausted.
type probeSuspicion struct {
	Model     string `json:"model"`
	Failures  int    `json:"failures"`
	LastError string `json:"last_error,omitempty"`
	Since     string `json:"since"`
}

// businessDegradation marks a model that real business traffic observed as
// degraded (length anomaly or model mismatch). Unlike probe suspicions it
// takes effect immediately - one unhealthy observation is enough - and it is
// cleared by a healthy business observation or a successful probe round.
type businessDegradation struct {
	Model  string `json:"model"`
	Reason string `json:"reason,omitempty"`
	Since  string `json:"since"`
}

// exitPenalty marks one egress temporarily removed from rotation after
// consecutive attempts that failed to yield a healthy state (length anomaly,
// model mismatch or transport-level errors). It is released automatically
// once Until passes; a successful capture clears the entry immediately.
type exitPenalty struct {
	Proxy     string `json:"proxy"`
	Failures  int    `json:"failures"`
	LastError string `json:"last_error,omitempty"`
	FirstAt   string `json:"first_at"`
	Until     string `json:"until"`
}

type probeEngine struct {
	mu             sync.Mutex
	cfg            probeConfigState
	values         map[string]stateEntry
	failures       map[string]probeFailure
	suspects       map[string]probeSuspicion
	business       map[string]businessDegradation
	exitPenalties  map[string]exitPenalty
	candidates     map[string]stateEntry
	prefetchGate   map[string]time.Time
	prefetchStop   chan struct{}
	lastAttempt    map[string]time.Time
	paused         map[string]bool
	probing        map[string]bool
	rejectDegraded bool
	history        []probeRecord
	proxyIndex     int
	consecutive    int
	lastActivity   string
	stopCh         chan struct{}
	running        bool
	runNote        string
	runStartedAt   string
	runFinishedAt  string
	seeded         int
	probesTotal    uint64
	probesOK       uint64
	lastError      string
}


var probeTrack = &probeEngine{
	values:        map[string]stateEntry{},
	failures:      map[string]probeFailure{},
	suspects:      map[string]probeSuspicion{},
	business:      map[string]businessDegradation{},
	exitPenalties: map[string]exitPenalty{},
	candidates:    map[string]stateEntry{},
	prefetchGate:  map[string]time.Time{},
	lastAttempt:   map[string]time.Time{},
	paused:        map[string]bool{},
	probing:       map[string]bool{},
	rejectDegraded: true,
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
		MaxAttemptsPerRound: 0, // 0 = auto: egress count × attempts-per-proxy
		Cooldown:            time.Duration(probeDefaultsCooldownMinutes) * time.Minute,
		ExitCooldown:        time.Duration(probeDefaultsExitCooldownMinutes) * time.Minute,
		ExitFailThreshold:   probeDefaultsExitFailThreshold,
		ExitPoolFailThreshold: probeDefaultsExitPoolFailThreshold,
		ExitMinActive:       probeDefaultsExitMinActive,
		Prefetch:            time.Duration(probeDefaultsPrefetchMinutes) * time.Minute,
		SuspectThreshold:    probeDefaultsSuspectThreshold,
		Timeout:             time.Duration(probeDefaultsTimeoutSeconds) * time.Second,
		Prompt:              probeDefaultsPrompt,
		UpstreamURL:         probeDefaultsUpstreamURL,
		SecretsFile:         strings.TrimSpace(block.ProxiesFile),
		ProxyPools:          map[string]bool{},
		ProxyLabels:         map[string]string{},
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
	if block.ExitCooldownMinutes != nil && *block.ExitCooldownMinutes >= 0 {
		cfg.ExitCooldown = time.Duration(*block.ExitCooldownMinutes) * time.Minute
	}
	if block.ExitFailThreshold != nil && *block.ExitFailThreshold > 0 {
		cfg.ExitFailThreshold = *block.ExitFailThreshold
	}
	if block.ExitPoolFailThreshold != nil && *block.ExitPoolFailThreshold > 0 {
		cfg.ExitPoolFailThreshold = *block.ExitPoolFailThreshold
	}
	if block.ExitMinActive != nil && *block.ExitMinActive > 0 {
		cfg.ExitMinActive = *block.ExitMinActive
	}
	if block.PrefetchMinutes != nil && *block.PrefetchMinutes >= 0 {
		cfg.Prefetch = time.Duration(*block.PrefetchMinutes) * time.Minute
	}
	if block.SuspectThreshold != nil && *block.SuspectThreshold > 0 {
		cfg.SuspectThreshold = *block.SuspectThreshold
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

// loadProxiesFile reads the dedicated proxy secret file (one proxy per line;
// full-line "#" comments and blank lines ignored) and returns the parsed list
// plus per-entry metadata. A trailing comment may declare a multi-exit pool
// and a display label: "socks5h://10.0.0.1:18321  # pool:ipv6" marks the
// entry as a rotating pool (higher cool-down tolerance).
func loadProxiesFile(path string) ([]string, map[string]bool, map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, nil, err
	}
	var proxies []string
	pools := map[string]bool{}
	labels := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		spec := line
		comment := ""
		if idx := strings.Index(line, "#"); idx >= 0 {
			spec = strings.TrimSpace(line[:idx])
			comment = strings.TrimSpace(line[idx+1:])
		}
		if spec == "" {
			continue
		}
		proxies = append(proxies, spec)
		for _, token := range strings.Fields(comment) {
			if value, ok := strings.CutPrefix(token, "pool:"); ok && strings.TrimSpace(value) != "" {
				pools[spec] = true
				labels[spec] = "IPv6 池"
			}
		}
	}
	if len(proxies) == 0 {
		return nil, nil, nil, fmt.Errorf("proxies file %s contains no entries", path)
	}
	return proxies, pools, labels, nil
}

func configureProbeTrack(block probeConfigYAML) error {
	cfg := parseProbeConfig(block)
	var cfgErr string
	if cfg.Enabled {
		if strings.TrimSpace(cfg.SecretsFile) != "" {
			proxies, pools, labels, err := loadProxiesFile(cfg.SecretsFile)
			if err == nil {
				cfg.Proxies = proxies
				cfg.ProxyPools = pools
				cfg.ProxyLabels = labels
			} else {
				cfgErr = fmt.Sprintf("load proxies file: %v", err)
			}
		}
	}
	probeTrack.mu.Lock()
	probeTrack.stopLocked()
	probeTrack.cfg = probeConfigState{Config: cfg, Error: cfgErr}
	// Policy migration: plain exits whose accumulated failures are below the
	// (possibly raised) threshold are released at once. Rotating pools are
	// released unconditionally - v1.5.17 never benches them any more, so any
	// cool-down left by the older policy is stale state.
	for spec, penalty := range probeTrack.exitPenalties {
		if cfg.ProxyPools[spec] {
			delete(probeTrack.exitPenalties, spec)
			markStateDirty()
			continue
		}
		if penalty.Until == "" {
			continue
		}
		threshold := cfg.ExitFailThreshold
		if penalty.Failures < threshold {
			delete(probeTrack.exitPenalties, spec)
			markStateDirty()
		}
	}
	probeTrack.mu.Unlock()
	// The probe track never auto-starts. The dashboard's manual control is
	// the only way to run full rounds; the prefetch watcher below is the one
	// narrow automatic exception: it probes a model only while its active
	// token's remaining validity is inside the prefetch window, parking a
	// fresh token as candidate for a seamless hand-off.
	probeTrack.ensurePrefetchWatcher()
	ensurePersistence()
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
	e.runNote = ""
	e.runStartedAt = time.Now().UTC().Format(time.RFC3339Nano)
	e.runFinishedAt = ""
	e.stopCh = make(chan struct{})
	stop := e.stopCh
	cfg := e.cfg.Config
	e.mu.Unlock()
	markStateDirty()
	go e.roundLoop(cfg, stop)
}

func (e *probeEngine) stop() {
	e.mu.Lock()
	e.stopLocked()
	e.mu.Unlock()
	markStateDirty()
}

func (e *probeEngine) stopLocked() {
	if e.running && e.stopCh != nil {
		close(e.stopCh)
	}
	e.running = false
	e.stopCh = nil
}

// roundLoop drives one user-initiated round: every configured model is tried
// once in priority order (paused models are skipped), sequentially. This is
// the ONLY way probing runs - nothing is scheduled automatically - and
// the loop exits when the operator stops it or the model list is exhausted.
// A successful capture (model consistent + 292 bytes) is stored as the
// model's persisted baseline; failures only annotate the dashboard.
func (e *probeEngine) roundLoop(cfg probeConfig, stop chan struct{}) {
	defer func() {
		e.mu.Lock()
		e.running = false
		e.runFinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		e.mu.Unlock()
		markStateDirty()
	}()
	if !cfg.Enabled {
		e.mu.Lock()
		e.runNote = "探测轨未启用（probe.enabled=false）"
		e.mu.Unlock()
		return
	}
	for _, model := range cfg.Models {
		select {
		case <-stop:
			return
		default:
		}
		e.mu.Lock()
		paused := e.paused[model]
		e.mu.Unlock()
		if paused {
			continue
		}
		e.probeModel(model, cfg, stop)
	}
}

// probeSuppressed reports whether probing for a model is held off: the model
// was paused from the dashboard or a round is already running for it.
func (e *probeEngine) probeSuppressed(model string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.paused[model] || e.probing[model]
}

// setModelPaused pauses or resumes a model from the dashboard. Pausing keeps
// the model out of rounds while preserving its value and failure record;
// resuming clears the stale annotation and immediately starts one probe for
// the model in the background (the user explicitly asked for it).
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
	markStateDirty()
	if !paused {
		e.probeModelAsync(model)
	}
}

// ---------------------------------------------------------------------------
// prefetch watch (smooth hand-off)

// ensurePrefetchWatcher starts the single background watcher once per process.
// It is the only automatic probing that exists in v1.5.15+: it never refreshes
// a value that is safely outside the prefetch window.
func (e *probeEngine) ensurePrefetchWatcher() {
	e.mu.Lock()
	if e.prefetchStop != nil {
		e.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	e.prefetchStop = stop
	e.mu.Unlock()
	go e.prefetchWatchLoop(stop)
}

// stopPrefetchWatcher shuts the watcher down (plugin shutdown).
func (e *probeEngine) stopPrefetchWatcher() {
	e.mu.Lock()
	stop := e.prefetchStop
	e.prefetchStop = nil
	e.mu.Unlock()
	if stop != nil {
		close(stop)
	}
}

// prefetchWatchLoop scans every 30 seconds for models whose active token is
// about to expire (remaining <= prefetch-minutes) and, only then, starts one
// background probe to park a fresh token as the candidate. Per-model attempts
// are throttled by prefetchRetryWindow so a failing probe cannot spin.
func (e *probeEngine) prefetchWatchLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			e.prefetchScan()
		}
	}
}

func (e *probeEngine) prefetchScan() {
	cfg := currentProbeConfig().Config
	if !cfg.Enabled || cfg.Prefetch <= 0 {
		return
	}
	now := time.Now().UTC()
	for _, model := range cfg.Models {
		e.mu.Lock()
		if e.paused[model] || e.probing[model] {
			e.mu.Unlock()
			continue
		}
		active, has := e.values[model]
		if !has || !active.Valid || active.Value == "" {
			e.mu.Unlock()
			continue
		}
		issued, okTime := parseTurnStateTimestamp(active.Value)
		if !okTime {
			e.mu.Unlock()
			continue
		}
		remaining := issued.Add(cfg.TTL).Sub(now)
		if remaining > cfg.Prefetch {
			e.mu.Unlock()
			continue
		}
		// A valid candidate already parked? Nothing to do.
		if candidate, ok := e.candidates[model]; ok && candidate.Valid && candidate.Value != "" && !entryExpired(candidate, cfg.TTL, now) {
			e.mu.Unlock()
			continue
		}
		if gate := e.prefetchGate[model]; !gate.IsZero() && now.Sub(gate) < prefetchRetryWindow {
			e.mu.Unlock()
			continue
		}
		if e.prefetchGate == nil {
			e.prefetchGate = map[string]time.Time{}
		}
		e.prefetchGate[model] = now
		e.mu.Unlock()
		e.probeModelAsync(model)
	}
}

// probeModelAsync launches one background probe round for a single model on
// explicit user request (the row's "probe now" control or resuming a paused
// model). It is independent of the sequential round control: different models
// may be probed concurrently, and a model that is already being probed is
// never started twice.
func (e *probeEngine) probeModelAsync(model string) {
	model = strings.TrimSpace(model)
	if model == "" {
		return
	}
	e.mu.Lock()
	cfg := e.cfg.Config
	busy := e.probing[model]
	e.mu.Unlock()
	if !cfg.Enabled || busy {
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
	markStateDirty()
}

// rejectDegradedEnabled returns the current switch state.
func (e *probeEngine) rejectDegradedEnabled() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rejectDegraded
}

// degradationEvidence maps a probe failure message to its Chinese degradation
// reason, or returns "" when the failure is not degradation evidence (rate
// limits, timeouts and network errors do not count).
func degradationEvidence(message string) string {
	switch {
	case strings.Contains(message, "state length"):
		return "上游状态长度异常（疑似风控降级）"
	case strings.Contains(message, "model mismatch"):
		return "上游请求被路由至其它模型（模型不一致）"
	}
	return ""
}

// noteProbeFailure advances the early "suspected degraded" tracking for one
// in-round failure. Only degradation-evidence failures count; the running
// count is kept (even below the threshold) so the series continues, and once
// the configured threshold is reached the model becomes rejection-eligible
// before the round completes. A successful probe or a completed round clears
// it (success) or promotes it (exhausted round).
func (e *probeEngine) noteProbeFailure(model string, record probeRecord, cfg probeConfig) {
	if degradationEvidence(record.Error) == "" {
		return
	}
	markStateDirty()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.suspects == nil {
		e.suspects = map[string]probeSuspicion{}
	}
	suspicion := e.suspects[model]
	if suspicion.Failures == 0 {
		suspicion = probeSuspicion{Model: model, Since: time.Now().UTC().Format(time.RFC3339Nano)}
	}
	suspicion.Failures++
	suspicion.LastError = record.Error
	e.suspects[model] = suspicion
}

// clearSuspect removes the early suspicion for a model (probe success or a
// completed round that produced a full annotation).
func (e *probeEngine) clearSuspect(model string) {
	e.mu.Lock()
	delete(e.suspects, model)
	e.mu.Unlock()
	markStateDirty()
}

// degradedRejectReason reports the Chinese reason used by the degraded-model
// rejection when the switch is enabled. Full failure annotations win; on top
// of them, in-round suspicions that crossed suspect-threshold are eligible so
// the rejection starts about fifteen seconds after degradation appears
// instead of waiting for the whole round.
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
		if reason := degradationEvidence(failure.LastError); reason != "" {
			return reason
		}
	}
	for key, mark := range e.business {
		if !strings.HasPrefix(candidate, strings.ToLower(key)) {
			continue
		}
		if mark.Reason != "" {
			return mark.Reason + "（业务流量确认）"
		}
	}
	for key, suspicion := range e.suspects {
		if !strings.HasPrefix(candidate, strings.ToLower(key)) {
			continue
		}
		threshold := e.cfg.Config.SuspectThreshold
		if threshold <= 0 {
			threshold = probeDefaultsSuspectThreshold
		}
		if suspicion.Failures < threshold {
			continue
		}
		if reason := degradationEvidence(suspicion.LastError); reason != "" {
			return fmt.Sprintf("%s（本轮已连续 %d 次失败，尚未达到正式判定）", reason, suspicion.Failures)
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

// effectiveMaxAttempts resolves the per-round attempt cap. Zero means auto:
// every egress is tried attempts-per-proxy times (e.g. 3 egresses × 3 tries
// = a round only gives up after all hops failed their full share).
func effectiveMaxAttempts(cfg probeConfig, proxyCount int) int {
	if cfg.MaxAttemptsPerRound > 0 {
		return cfg.MaxAttemptsPerRound
	}
	perHop := cfg.AttemptsPerHop
	if perHop <= 0 {
		perHop = probeDefaultsAttemptsPerHop
	}
	if total := proxyCount * perHop; total > 0 {
		return total
	}
	return probeDefaultsMaxAttempts
}

// noteBusinessDegradation marks a model immediately upon a single unhealthy
// business observation (state length anomaly or upstream model mismatch) and
// triggers one gentle recovery round (throttled to at most once per five
// minutes per model, and suppressed while a round or cooldown is active).
func (e *probeEngine) noteBusinessDegradation(model, reason string) {
	model = strings.TrimSpace(model)
	if model == "" {
		return
	}
	e.mu.Lock()
	if e.business == nil {
		e.business = map[string]businessDegradation{}
	}
	e.business[model] = businessDegradation{
		Model:  model,
		Reason: reason,
		Since:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	e.mu.Unlock()
	markStateDirty()
}

// clearBusinessDegradation removes the business mark (healthy observation or
// successful probe).
func (e *probeEngine) clearBusinessDegradation(model string) {
	e.mu.Lock()
	_, present := e.business[model]
	if present {
		delete(e.business, model)
	}
	e.mu.Unlock()
	if present {
		markStateDirty()
	}
}

// triggerRecovery was removed: recovery probing is user-initiated
// only (the dashboard round control). Unhealthy observations keep marking the
// model for the rejection switch, but never start upstream traffic on their
// own.

// observeBusinessState feeds one state value observed on real business
// traffic into the engine:
//
//   - healthy (length 292, model consistent) -> stored as the active value
//     with source "business" (zero upstream pressure), marks cleared;
//   - unhealthy (length anomaly, or state empty + model mismatch) -> one
//     observation is enough to mark the model degraded for the rejection
//     switch and to trigger a gentle recovery round;
//   - state empty with no mismatch -> no signal, ignored.
func observeBusinessState(model, state, observedModel string) {
	model = strings.TrimSpace(model)
	if model == "" {
		return
	}
	state = strings.TrimSpace(state)
	consistent := observedModel == "" || probeModelConsistent(model, observedModel)
	if state == "" {
		if observedModel != "" && !consistent {
			probeTrack.noteBusinessDegradation(model, "上游请求被路由至其它模型（模型不一致）")
		}
		return
	}
	if len(state) != probeRequiredStateLength {
		probeTrack.noteBusinessDegradation(model, fmt.Sprintf("业务请求观测到状态长度异常（%d 字节）", len(state)))
		return
	}
	if !consistent {
		probeTrack.noteBusinessDegradation(model, "上游请求被路由至其它模型（模型不一致）")
		return
	}
	cfg := currentProbeConfig().Config
	probeTrack.storeValue(model, state, "business", "", cfg)
	probeTrack.clearBusinessDegradation(model)
}

// ---------------------------------------------------------------------------
// exit (egress) circuit breaker

// availableProxies returns the rotation list for the next attempt: configured
// egresses that are not in an active cool-down. A cool-down that has expired
// is released here with a fresh failure window. When the usable set would drop
// below exit-min-active, the soonest-expiring cool-downs are released early so
// a round can always make progress; a fully empty rotation also falls back to
// the first configured egress.
func (e *probeEngine) availableProxies(proxies []string, now time.Time) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	minActive := e.cfg.Config.ExitMinActive
	if minActive <= 0 {
		minActive = probeDefaultsExitMinActive
	}
	usable := make([]string, 0, len(proxies))
	for _, spec := range proxies {
		penalty, ok := e.exitPenalties[spec]
		if !ok {
			usable = append(usable, spec)
			continue
		}
		until, err := time.Parse(time.RFC3339Nano, penalty.Until)
		if penalty.Until == "" || err != nil || !now.Before(until) {
			if penalty.Until != "" {
				// Cool-down finished: release with a fresh failure window.
				penalty.Failures = 0
				penalty.Until = ""
				e.exitPenalties[spec] = penalty
				markStateDirty()
			}
			usable = append(usable, spec)
		}
	}
	if len(usable) >= minActive {
		return usable
	}
	// Emergency release: free the soonest-expiring entries until the minimum
	// active count is met, so probing can never permanently starve.
	type hold struct {
		spec  string
		until time.Time
	}
	inUsable := func(spec string) bool {
		for _, item := range usable {
			if item == spec {
				return true
			}
		}
		return false
	}
	holds := make([]hold, 0, len(proxies))
	for _, spec := range proxies {
		if inUsable(spec) {
			continue
		}
		if penalty, ok := e.exitPenalties[spec]; ok {
			if until, err := time.Parse(time.RFC3339Nano, penalty.Until); err == nil {
				holds = append(holds, hold{spec: spec, until: until})
			}
		}
	}
	sort.Slice(holds, func(i, j int) bool { return holds[i].until.Before(holds[j].until) })
	for _, item := range holds {
		if len(usable) >= minActive {
			break
		}
		delete(e.exitPenalties, item.spec)
		usable = append(usable, item.spec)
		markStateDirty()
	}
	if len(usable) == 0 && len(proxies) > 0 {
		usable = append(usable, proxies[0])
	}
	return usable
}

// noteExitOutcome records one attempt result for an egress. A healthy capture
// clears its penalty immediately; for plain (fixed-endpoint) egresses a streak
// of consecutive failures reaching the configured threshold removes the egress
// from rotation for exit-cooldown-minutes (set 0 to disable the removal).
//
// Rotating pools (one endpoint that presents many source addresses, marked
// "# pool:..." in the proxies file) are never benched: every connection draws
// a fresh address, so a failing streak says nothing about the endpoint being
// broken - it only reflects the current upstream state, which the model-level
// tracks already describe. Their counters keep accumulating for the dashboard,
// and any cool-down recorded by an older policy is cleared on the next
// observation.
func (e *probeEngine) noteExitOutcome(spec string, healthy bool, errorText string) {
	if spec == "" {
		return
	}
	now := time.Now().UTC()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.exitPenalties == nil {
		e.exitPenalties = map[string]exitPenalty{}
	}
	if healthy {
		if _, ok := e.exitPenalties[spec]; ok {
			delete(e.exitPenalties, spec)
			markStateDirty()
		}
		return
	}
	penalty := e.exitPenalties[spec]
	if penalty.Proxy == "" {
		penalty = exitPenalty{Proxy: spec, FirstAt: now.Format(time.RFC3339Nano)}
	}
	penalty.Failures++
	penalty.LastError = errorText
	if e.cfg.Config.ProxyPools[spec] {
		penalty.Until = ""
		e.exitPenalties[spec] = penalty
		markStateDirty()
		return
	}
	cfg := e.cfg.Config
	threshold := cfg.ExitFailThreshold
	if threshold <= 0 {
		threshold = probeDefaultsExitFailThreshold
	}
	if penalty.Failures >= threshold && cfg.ExitCooldown > 0 {
		penalty.Until = now.Add(cfg.ExitCooldown).Format(time.RFC3339Nano)
	}
	e.exitPenalties[spec] = penalty
	markStateDirty()
}

// probeModel runs the probe sequence for one model: attempts are distributed
// across the egress pool (skipping cool-down entries) until an acceptable
// state is captured (success clears any failure mark) or the resolved attempt
// cap is exhausted (failure is recorded for the dashboard).
func (e *probeEngine) probeModel(model string, cfg probeConfig, stop chan struct{}) {
	e.mu.Lock()
	if e.probing == nil {
		e.probing = map[string]bool{}
	}
	if e.probing[model] {
		e.mu.Unlock()
		return
	}
	proxies := append([]string(nil), cfg.Proxies...)
	if len(proxies) == 0 {
		e.mu.Unlock()
		return
	}
	if e.lastAttempt == nil {
		e.lastAttempt = map[string]time.Time{}
	}
	e.probing[model] = true
	e.lastAttempt[model] = time.Now().UTC()
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.probing, model)
		e.mu.Unlock()
	}()
	maxAttempts := effectiveMaxAttempts(cfg, len(proxies))
	attempts := 0
	lastError := ""
	lastLength := 0
	cursor := 0
	for attempts < maxAttempts {
		select {
		case <-stop:
			return
		default:
		}
		rotation := e.availableProxies(proxies, time.Now().UTC())
		if len(rotation) == 0 {
			break
		}
		proxySpec := rotation[cursor%len(rotation)]
		cursor++
		attempts++
		record, value := e.probeOnce(model, proxySpec, cfg)
		e.appendRecord(record)
		if value != "" {
			e.noteExitOutcome(proxySpec, true, "")
			e.storeValue(model, value, "probe", proxySpec, cfg)
			e.mu.Lock()
			delete(e.failures, model)
			delete(e.suspects, model)
			delete(e.business, model)
			e.proxyIndex = 0
			e.mu.Unlock()
			markStateDirty()
			return
		}
		lastError = record.Error
		if record.StateLength > 0 {
			lastLength = record.StateLength
		}
		e.noteExitOutcome(proxySpec, false, record.Error)
		e.noteProbeFailure(model, record, cfg)
		select {
		case <-time.After(cfg.ProbeInterval):
		case <-stop:
			return
		}
	}
	// Retries exhausted: annotate the failure for the dashboard (attempt
	// counters, last error and a cool-down hint). Nothing restarts
	// automatically - the next attempt is a manual round. The
	// in-round suspicion is promoted to the full annotation.
	now := time.Now().UTC()
	e.mu.Lock()
	delete(e.suspects, model)
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
	markStateDirty()
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
	transport, binder, err := buildProbeTransport(proxySpec)
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
	if binder != nil {
		// The socks5 handshake captured the server-bound address (the real
		// rotating source IP this attempt went out through).
		record.EgressAddr = binder.load()
	}
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
	markStateDirty()
}

// storeValue saves a freshly captured state for the model with two-slot
// semantics: while the current active value is still valid it is kept serving
// and the new value parks in the candidate slot for a seamless hand-off; once
// the active value expires (or no active value exists) the new value becomes
// active immediately. This is the "smooth switch" the dashboard relies on:
// business traffic always overwrites with a currently-valid token while the
// prefetch machinery replenishes the next one invisibly.
func (e *probeEngine) storeValue(model, value, source, proxySpec string, cfg probeConfig) {
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
		Source:      source,
		Proxy:       proxySpec,
		Valid:       true,
	}
	now := time.Now().UTC()
	e.mu.Lock()
	if e.candidates == nil {
		e.candidates = map[string]stateEntry{}
	}
	active, ok := e.values[model]
	activeUsable := ok && active.Valid && active.Value != "" && !entryExpired(active, cfg.TTL, now)
	if activeUsable {
		// Keep serving the old token; park the fresh one as the next slot.
		e.candidates[model] = entry
	} else {
		e.values[model] = entry
		delete(e.candidates, model)
	}
	e.mu.Unlock()
	markStateDirty()
}

// entryExpired reports whether a stored value's embedded timestamp plus the
// TTL has passed; values without a decodable timestamp are treated as usable
// (expiry cannot be proven).
func entryExpired(entry stateEntry, ttl time.Duration, now time.Time) bool {
	if ts, ok := parseTurnStateTimestamp(entry.Value); ok {
		return !now.Before(ts.Add(ttl))
	}
	return false
}

// promoteCandidateLocked moves a valid candidate into the active slot when
// the active value is missing or expired. Caller must hold e.mu; returns true
// when state changed (caller marks dirty).
func (e *probeEngine) promoteCandidateLocked(model string, cfg probeConfig, now time.Time) bool {
	candidate, ok := e.candidates[model]
	if !ok || !candidate.Valid || candidate.Value == "" {
		return false
	}
	if entryExpired(candidate, cfg.TTL, now) {
		delete(e.candidates, model)
		return true
	}
	active, has := e.values[model]
	if has && active.Valid && active.Value != "" && !entryExpired(active, cfg.TTL, now) {
		return false
	}
	e.values[model] = candidate
	delete(e.candidates, model)
	return true
}

// activeValueFor returns the model's current healthy baseline value: one that
// is stored (seed / business observation / manual probe) and still inside the
// TTL window. Serving does not depend on the probe track being enabled - the
// baseline is the plugin's cached known-good state and must keep protecting
// requests while probing stays idle. Expired actives are seamlessly replaced
// by a parked candidate when one is available.
func (e *probeEngine) activeValueFor(model string) string {
	cfg := currentProbeConfig().Config
	now := time.Now().UTC()
	e.mu.Lock()
	dirty := e.promoteCandidateLocked(model, cfg, now)
	entry, ok := e.values[model]
	e.mu.Unlock()
	if dirty {
		markStateDirty()
	}
	if !ok || !entry.Valid || entry.Value == "" {
		return ""
	}
	if ts, okTime := parseTurnStateTimestamp(entry.Value); okTime {
		if now.Sub(ts) > cfg.TTL {
			return ""
		}
	}
	return entry.Value
}

// seedBaselinesFromAudit restores missing baseline entries from recorded
// history: first the deployment seeds file (last known-good values extracted
// from server records by the open-source-prep scanner), then the newest
// healthy (292-byte, decodable) turn-state values in the audit journal. A
// fresh plugin instance therefore keeps protecting traffic with the last
// known-good states before any business observation or manual probe refreshes
// them, and nothing is drafted into the baseline unless it passes the same
// length + decodability acceptance as probing.
func (e *probeEngine) seedBaselinesFromAudit() {
	type candidate struct {
		value string
		at    time.Time
	}
	best := map[string]candidate{}
	// Deployment seeds file first (richest history, includes backups).
	seedsPath := "/CLIProxyAPI/logs/.plugins/timezone-override/seeds.json"
	if override := strings.TrimSpace(os.Getenv("LKS_TZ_STATE_FILE")); override != "" {
		seedsPath = filepath.Join(filepath.Dir(override), "seeds.json")
	}
	if raw, err := os.ReadFile(seedsPath); err == nil {
		var seeds map[string]string
		if json.Unmarshal(raw, &seeds) == nil {
			for model, value := range seeds {
				model = strings.TrimSpace(model)
				value = strings.TrimSpace(value)
				if model == "" || len(value) != probeRequiredStateLength {
					continue
				}
				if ts, ok := parseTurnStateTimestamp(value); ok {
					best[model] = candidate{value: value, at: ts}
				}
			}
		}
	}
	history.mu.Lock()
	for _, record := range history.records {
		model := strings.TrimSpace(record.Model)
		if model == "" {
			model = strings.TrimSpace(record.RequestedModel)
		}
		if model == "" || record.TurnStateLength != probeRequiredStateLength || record.TurnStateValue == "" {
			continue
		}
		ts, ok := parseTurnStateTimestamp(record.TurnStateValue)
		if !ok {
			continue
		}
		if previous, exists := best[model]; !exists || ts.After(previous.at) {
			best[model] = candidate{value: record.TurnStateValue, at: ts}
		}
	}
	history.mu.Unlock()
	seeded := 0
	e.mu.Lock()
	if e.values == nil {
		e.values = map[string]stateEntry{}
	}
	cfg := e.cfg.Config
	for model, found := range best {
		if entry, ok := e.values[model]; ok && entry.Value != "" {
			continue
		}
		e.values[model] = stateEntry{
			Model:       model,
			Value:       found.value,
			ValueLength: len(found.value),
			GeneratedAt: found.at.UTC().Format(time.RFC3339),
			CapturedAt:  time.Now().UTC().Format(time.RFC3339Nano),
			ExpiresAt:   found.at.Add(cfg.TTL).UTC().Format(time.RFC3339),
			Source:      "seed",
			Valid:       true,
		}
		seeded++
	}
	e.seeded += seeded
	e.mu.Unlock()
	if seeded > 0 {
		markStateDirty()
	}
}

func (e *probeEngine) appendRecord(record probeRecord) {
	defer markStateDirty()
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
// "direct", "socks5://...", or "http(s)://...". For socks5 / socks5h it also
// returns a bind recorder that captures the server-reported egress address
// (the CONNECT reply's BND.ADDR) so the audit trail can show which rotating
// source address actually carried the attempt; other egresses return nil.
func buildProbeTransport(spec string) (*http.Transport, *socksBind, error) {
	transport := &http.Transport{
		TLSClientConfig:   &tls.Config{},
		ForceAttemptHTTP2: true,
		IdleConnTimeout:   30 * time.Second,
	}
	spec = strings.TrimSpace(spec)
	if spec == "" || strings.EqualFold(spec, "direct") {
		return transport, nil, nil
	}
	parsed, err := url.Parse(spec)
	if err != nil {
		return nil, nil, fmt.Errorf("parse proxy url: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "socks5", "socks5h":
		binder := &socksBind{host: parsed.Host}
		if parsed.User != nil {
			binder.user = parsed.User.Username()
			binder.pass, _ = parsed.User.Password()
		}
		transport.DialContext = binder.dialContext
		return transport, binder, nil
	case "http", "https":
		transport.Proxy = http.ProxyURL(parsed)
	default:
		return nil, nil, fmt.Errorf("unsupported proxy scheme %q", parsed.Scheme)
	}
	return transport, nil, nil
}

// probeSummary describes the probe engine state for the management API.
func probeSummary() map[string]any {
	cfgState := currentProbeConfig()
	cfg := cfgState.Config
	now := time.Now().UTC()
	probeTrack.mu.Lock()
	// Each value carries the real validity window derived from the token's own
	// embedded timestamp (issue time) plus the configured validity duration:
	// issued_at / expires_at / remaining_seconds / expired are computed fresh
	// on every summary so the dashboard never relies on a stale estimate.
	values := make([]map[string]any, 0, len(cfg.Models))
	for _, model := range cfg.Models {
		entry, ok := probeTrack.values[model]
		if !ok {
			entry = stateEntry{Model: model}
		}
		item := map[string]any{
			"model":        entry.Model,
			"value_length": entry.ValueLength,
			"source":       entry.Source,
			"proxy":        entry.Proxy,
			"captured_at":  entry.CapturedAt,
			"valid":        entry.Valid,
		}
		if entry.Value != "" {
			item["value_preview"] = previewValue(entry.Value, turnStatePreviewLength)
		}
		// Candidate slot (two-slot smooth hand-off): expose its validity when
		// a fresh token is parked and waiting for the active one to expire.
		if candidate, ok := probeTrack.candidates[model]; ok && candidate.Valid && candidate.Value != "" {
			citem := map[string]any{
				"source":      candidate.Source,
				"captured_at": candidate.CapturedAt,
			}
			if ts, okTime := parseTurnStateTimestamp(candidate.Value); okTime {
				expires := ts.Add(cfg.TTL)
				citem["issued_at"] = ts.Format(time.RFC3339)
				citem["expires_at"] = expires.Format(time.RFC3339)
				citem["remaining_seconds"] = int64(expires.Sub(now).Seconds())
				citem["expired"] = !now.Before(expires)
			}
			item["candidate"] = citem
		}
		if ts, okTime := parseTurnStateTimestamp(entry.Value); okTime {
			expires := ts.Add(cfg.TTL)
			item["issued_at"] = ts.Format(time.RFC3339)
			item["expires_at"] = expires.Format(time.RFC3339)
			item["remaining_seconds"] = int64(expires.Sub(now).Seconds())
			item["expired"] = !now.Before(expires)
		} else if entry.ExpiresAt != "" {
			// Legacy fallback: values recorded before the embedded-timestamp
			// pipeline still carry their stored window.
			item["issued_at"] = entry.GeneratedAt
			item["expires_at"] = entry.ExpiresAt
			if exp, err := time.Parse(time.RFC3339, entry.ExpiresAt); err == nil {
				item["remaining_seconds"] = int64(exp.Sub(now).Seconds())
				item["expired"] = !now.Before(exp)
			}
		}
		values = append(values, item)
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
	var suspects []probeSuspicion
	threshold := cfg.SuspectThreshold
	if threshold <= 0 {
		threshold = probeDefaultsSuspectThreshold
	}
	for _, model := range cfg.Models {
		if suspicion, ok := probeTrack.suspects[model]; ok && suspicion.Failures >= threshold {
			suspects = append(suspects, suspicion)
		}
	}
	businessMarks := make([]businessDegradation, 0, len(cfg.Models))
	for _, model := range cfg.Models {
		if mark, ok := probeTrack.business[model]; ok {
			businessMarks = append(businessMarks, mark)
		}
	}
	// Egress pool state: one entry per configured proxy with its current
	// rotation status, cool-down remainder and last error (for the pool tab).
	poolNow := time.Now().UTC()
	pool := make([]map[string]any, 0, len(cfg.Proxies))
	activeCount := 0
	for _, spec := range cfg.Proxies {
		item := map[string]any{"proxy": spec, "active": true}
		if cfg.ProxyPools[spec] {
			item["pool"] = true
		}
		if label := cfg.ProxyLabels[spec]; label != "" {
			item["label"] = label
		}
		if penalty, ok := probeTrack.exitPenalties[spec]; ok {
			item["failures"] = penalty.Failures
			if penalty.LastError != "" {
				item["last_error"] = penalty.LastError
			}
			if penalty.FirstAt != "" {
				item["first_at"] = penalty.FirstAt
			}
			if penalty.Until != "" {
				if until, err := time.Parse(time.RFC3339Nano, penalty.Until); err == nil {
					item["until"] = penalty.Until
					item["remaining_seconds"] = int64(until.Sub(poolNow).Seconds())
					if poolNow.Before(until) {
						item["active"] = false
					}
				}
			}
		}
		if item["active"] == true {
			activeCount++
		}
		pool = append(pool, item)
	}
	summary := map[string]any{
		"enabled":      cfg.Enabled,
		"error":        cfgState.Error,
		"models":       append([]string(nil), cfg.Models...),
		"proxies":      append([]string(nil), cfg.Proxies...),
		"proxies_state": pool,
		"pool_total":   len(cfg.Proxies),
		"pool_active":  activeCount,
		"exit_fail_threshold":   cfg.ExitFailThreshold,
		"exit_pool_fail_threshold": cfg.ExitPoolFailThreshold,
		"exit_cooldown_minutes": int(cfg.ExitCooldown / time.Minute),
		"exit_min_active":       cfg.ExitMinActive,
		"prefetch_minutes":      int(cfg.Prefetch / time.Minute),
		"proxy_index":  probeTrack.proxyIndex,
		"ttl_minutes":      int(cfg.TTL / time.Minute),
		"window_minutes":   int(cfg.Window / time.Minute),
		"scan_seconds": int(cfg.ScanInterval / time.Second),
		"interval_seconds": int(cfg.ProbeInterval / time.Second),
		"attempts_per_proxy": cfg.AttemptsPerHop,
		"max_attempts_per_round": effectiveMaxAttempts(cfg, len(cfg.Proxies)),
		"cooldown_minutes": int(cfg.Cooldown / time.Minute),
		"business":        businessMarks,
		"suspect_threshold": cfg.SuspectThreshold,
		"suspects":        suspects,
		"paused":           paused,
		"reject_degraded":  probeTrack.rejectDegraded,
		"running":      probeTrack.running,
		"run_started_at":  probeTrack.runStartedAt,
		"run_finished_at": probeTrack.runFinishedAt,
		"run_note":        probeTrack.runNote,
		"seeded":       probeTrack.seeded,
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
	probeTrack.stopPrefetchWatcher()
	closePersistence()
}
