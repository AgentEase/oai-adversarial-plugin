package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// accountRouting is deliberately evidence-driven. The usage callback records
// the selected AuthID after a request completes; the scheduler callback then
// receives only the host's current highest-priority candidate tier and may
// avoid credentials with fresh degradation evidence. It never reads tokens or
// reaches across priority tiers.
type accountRoutingConfig struct {
	Enabled          bool
	DegradedCooldown time.Duration
	FailureCooldown  time.Duration
	FailureThreshold int
}

type accountRoutingConfigState struct {
	Config accountRoutingConfig
	Error  string
}

type accountModelHealth struct {
	AuthID              string    `json:"auth_id,omitempty"`
	Account             string    `json:"account"`
	Model               string    `json:"model"`
	State               string    `json:"state"`
	TurnStateValue      string    `json:"turn_state_value,omitempty"`
	HealthyUntil        time.Time `json:"healthy_until,omitempty"`
	LastStateLength     int       `json:"last_state_length,omitempty"`
	ConsecutiveFailures int       `json:"consecutive_failures,omitempty"`
	LastStatusCode      int       `json:"last_status_code,omitempty"`
	LastReason          string    `json:"last_reason,omitempty"`
	ObservedAt          time.Time `json:"observed_at"`
	CooldownUntil       time.Time `json:"cooldown_until,omitempty"`
}

type schedulerCandidateDiagnostic struct {
	Account  string `json:"account"`
	Provider string `json:"provider,omitempty"`
	Status   string `json:"status,omitempty"`
	Health   string `json:"health"`
	Cooling  bool   `json:"cooling,omitempty"`
}

type schedulerRoutingDiagnostic struct {
	ObservedAt     time.Time                      `json:"observed_at"`
	Provider       string                         `json:"provider,omitempty"`
	Providers      []string                       `json:"providers,omitempty"`
	Model          string                         `json:"model"`
	CandidateCount int                            `json:"candidate_count"`
	EligibleCount  int                            `json:"eligible_count"`
	Outcome        string                         `json:"outcome"`
	Selected       string                         `json:"selected,omitempty"`
	Candidates     []schedulerCandidateDiagnostic `json:"candidates,omitempty"`
}

type accountRoutingManager struct {
	mu             sync.Mutex
	config         accountRoutingConfigState
	health         map[string]accountModelHealth
	roundRobin     uint64
	usageObserved  uint64
	schedulerCalls uint64
	routingHandled uint64
	lastScheduler  schedulerRoutingDiagnostic
}

var accountRouter = &accountRoutingManager{
	config: accountRoutingConfigState{Config: defaultAccountRoutingConfig()},
	health: map[string]accountModelHealth{},
}

func defaultAccountRoutingConfig() accountRoutingConfig {
	return accountRoutingConfig{
		DegradedCooldown: 180 * time.Minute,
		FailureCooldown:  10 * time.Minute,
		FailureThreshold: 2,
	}
}

func configureAccountRouting(configYAML []byte) error {
	cfg := defaultAccountRoutingConfig()
	trimmed := bytes.TrimSpace(configYAML)
	if len(trimmed) == 0 {
		accountRouter.setConfig(accountRoutingConfigState{Config: cfg})
		return nil
	}
	normalized, err := normalizePanelConfig(trimmed)
	if err != nil {
		accountRouter.setConfig(accountRoutingConfigState{Config: cfg, Error: err.Error()})
		return err
	}
	var root struct {
		ExperimentalEnabled *bool `yaml:"experimental-account-routing"`
		DegradedMinutes     *int  `yaml:"account-degraded-cooldown-minutes"`
		FailureMinutes      *int  `yaml:"account-failure-cooldown-minutes"`
		FailureThreshold    *int  `yaml:"account-failure-threshold"`
		AccountRouting      struct {
			Enabled                 *bool `yaml:"enabled"`
			DegradedCooldownMinutes *int  `yaml:"degraded-cooldown-minutes"`
			FailureCooldownMinutes  *int  `yaml:"failure-cooldown-minutes"`
			FailureThreshold        *int  `yaml:"failure-threshold"`
		} `yaml:"account-routing"`
	}
	if err := yaml.Unmarshal(normalized, &root); err != nil {
		state := accountRoutingConfigState{Config: cfg, Error: fmt.Sprintf("decode account routing config: %v", err)}
		accountRouter.setConfig(state)
		return fmt.Errorf("decode account routing config: %w", err)
	}
	if root.AccountRouting.Enabled != nil {
		cfg.Enabled = *root.AccountRouting.Enabled
	}
	if root.ExperimentalEnabled != nil {
		cfg.Enabled = *root.ExperimentalEnabled
	}
	degraded := root.AccountRouting.DegradedCooldownMinutes
	if root.DegradedMinutes != nil {
		degraded = root.DegradedMinutes
	}
	failure := root.AccountRouting.FailureCooldownMinutes
	if root.FailureMinutes != nil {
		failure = root.FailureMinutes
	}
	threshold := root.AccountRouting.FailureThreshold
	if root.FailureThreshold != nil {
		threshold = root.FailureThreshold
	}
	if degraded != nil {
		if *degraded < 1 || *degraded > 1440 {
			return accountRouter.configError(cfg, "account degraded cooldown must be 1-1440 minutes")
		}
		cfg.DegradedCooldown = time.Duration(*degraded) * time.Minute
	}
	if failure != nil {
		if *failure < 1 || *failure > 120 {
			return accountRouter.configError(cfg, "account failure cooldown must be 1-120 minutes")
		}
		cfg.FailureCooldown = time.Duration(*failure) * time.Minute
	}
	if threshold != nil {
		if *threshold < 1 || *threshold > 10 {
			return accountRouter.configError(cfg, "account failure threshold must be 1-10")
		}
		cfg.FailureThreshold = *threshold
	}
	accountRouter.setConfig(accountRoutingConfigState{Config: cfg})
	return nil
}

func (m *accountRoutingManager) setConfig(state accountRoutingConfigState) {
	m.mu.Lock()
	m.config = state
	if m.health == nil {
		m.health = map[string]accountModelHealth{}
	}
	m.mu.Unlock()
}

func (m *accountRoutingManager) configError(cfg accountRoutingConfig, message string) error {
	m.setConfig(accountRoutingConfigState{Config: cfg, Error: message})
	return fmt.Errorf("%s", message)
}

type schedulerPickRequest struct {
	Provider   string
	Providers  []string
	Model      string
	Stream     bool
	Candidates []schedulerAuthCandidate
}

type schedulerAuthCandidate struct {
	ID       string
	Provider string
	Priority int
	Status   string
}

type schedulerPickResponse struct {
	AuthID          string `json:"AuthID,omitempty"`
	DelegateBuiltin string `json:"DelegateBuiltin,omitempty"`
	Handled         bool   `json:"Handled"`
}

func pickAccountForRequest(raw []byte) (schedulerPickResponse, error) {
	var req schedulerPickRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return schedulerPickResponse{}, fmt.Errorf("decode scheduler request: %w", err)
	}
	return accountRouter.pick(req, time.Now().UTC()), nil
}

func (m *accountRoutingManager) pick(req schedulerPickRequest, now time.Time) schedulerPickResponse {
	model := routingModelKey(req.Model)
	if model == "" || !schedulerTargetsCodex(req) {
		return schedulerPickResponse{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.config.Config.Enabled || m.config.Error != "" {
		return schedulerPickResponse{}
	}
	m.schedulerCalls++
	diagnostic := schedulerRoutingDiagnostic{
		ObservedAt:     now,
		Provider:       req.Provider,
		Providers:      append([]string(nil), req.Providers...),
		Model:          req.Model,
		CandidateCount: len(req.Candidates),
		Outcome:        "delegated",
	}
	all := make([]schedulerAuthCandidate, 0, len(req.Candidates))
	healthy := make([]schedulerAuthCandidate, 0, len(req.Candidates))
	unknown := make([]schedulerAuthCandidate, 0, len(req.Candidates))
	blocked := 0
	for _, candidate := range req.Candidates {
		// Some CPA releases leave the per-candidate provider empty after the
		// request provider has already selected the Codex scheduler. Accept that
		// shape, but still reject an explicitly different provider.
		if strings.TrimSpace(candidate.ID) == "" ||
			(strings.TrimSpace(candidate.Provider) != "" && !providerLooksCodex(candidate.Provider)) {
			continue
		}
		all = append(all, candidate)
		key := accountHealthKey(candidate.ID, model)
		entry, ok := m.health[key]
		if ok && expireAccountHealth(&entry, now) {
			m.health[key] = entry
			markStateDirty()
		}
		candidateDiagnostic := schedulerCandidateDiagnostic{
			Account: publicAccountID(candidate.ID), Provider: candidate.Provider,
			Status: candidate.Status, Health: "unknown",
		}
		if ok && entry.State != "" {
			candidateDiagnostic.Health = entry.State
		}
		if ok && !entry.CooldownUntil.IsZero() && now.Before(entry.CooldownUntil) {
			candidateDiagnostic.Cooling = true
			diagnostic.Candidates = append(diagnostic.Candidates, candidateDiagnostic)
			blocked++
			continue
		}
		diagnostic.Candidates = append(diagnostic.Candidates, candidateDiagnostic)
		if ok && entry.State == "healthy" {
			healthy = append(healthy, candidate)
		} else {
			unknown = append(unknown, candidate)
		}
	}
	diagnostic.EligibleCount = len(all)
	if len(all) == 0 {
		diagnostic.Outcome = "no_eligible_candidates"
		m.lastScheduler = diagnostic
		return schedulerPickResponse{}
	}
	var pool []schedulerAuthCandidate
	switch {
	case len(healthy) > 0:
		pool = healthy
	case blocked > 0 && len(unknown) > 0:
		pool = unknown
	default:
		// Preserve CPA's configured scheduler when every candidate is unknown
		// or every candidate is cooling down.
		if blocked == len(all) {
			diagnostic.Outcome = "all_cooling"
		} else {
			diagnostic.Outcome = "all_unknown"
		}
		m.lastScheduler = diagnostic
		return schedulerPickResponse{}
	}
	sort.Slice(pool, func(i, j int) bool { return pool[i].ID < pool[j].ID })
	selected := pool[int(m.roundRobin%uint64(len(pool)))]
	m.roundRobin++
	m.routingHandled++
	diagnostic.Outcome = "selected"
	diagnostic.Selected = publicAccountID(selected.ID)
	m.lastScheduler = diagnostic
	return schedulerPickResponse{AuthID: selected.ID, Handled: true}
}

func schedulerTargetsCodex(req schedulerPickRequest) bool {
	if providerLooksCodex(req.Provider) {
		return true
	}
	for _, provider := range req.Providers {
		if providerLooksCodex(provider) {
			return true
		}
	}
	return false
}

func providerLooksCodex(provider string) bool {
	provider = strings.ToLower(strings.TrimSpace(provider))
	return strings.Contains(provider, "codex") || strings.Contains(provider, "openai") || strings.Contains(provider, "chatgpt")
}

type usageRecord struct {
	Provider        string
	Model           string
	AuthID          string
	AuthIndex       string
	AuthType        string
	Failed          bool
	Failure         usageFailure
	ResponseHeaders http.Header
}

type usageFailure struct {
	StatusCode int
	Body       string
}

func observeAccountUsage(raw []byte) error {
	var record usageRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return fmt.Errorf("decode usage record: %w", err)
	}
	accountRouter.observe(record, time.Now().UTC())
	return nil
}

func (m *accountRoutingManager) observe(record usageRecord, now time.Time) {
	model := routingModelKey(record.Model)
	if strings.TrimSpace(record.AuthID) == "" || model == "" || !providerLooksCodex(record.Provider+" "+record.AuthType) {
		return
	}
	stateValue := responseStateValue(record.ResponseHeaders)
	stateLength := len([]byte(stateValue))
	healthyUntil := accountStateExpiry(stateValue, now, currentProbeConfig().Config.TTL)
	key := accountHealthKey(record.AuthID, model)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.health == nil {
		m.health = map[string]accountModelHealth{}
	}
	entry := m.health[key]
	entry.AuthID = record.AuthID
	entry.Account = publicAccountID(record.AuthID)
	entry.Model = model
	entry.ObservedAt = now
	if stateLength > 0 {
		entry.LastStateLength = stateLength
	}
	if record.Failure.StatusCode > 0 {
		entry.LastStatusCode = record.Failure.StatusCode
	}
	m.usageObserved++

	switch {
	case !record.Failed && isAcceptedStateLength(stateLength):
		if healthyUntil.After(now) {
			entry.State = "healthy"
			entry.TurnStateValue = stateValue
			entry.HealthyUntil = healthyUntil
			entry.ConsecutiveFailures = 0
			entry.LastReason = "healthy_state"
			entry.CooldownUntil = time.Time{}
		} else {
			entry.State = "expired"
			entry.TurnStateValue = ""
			entry.HealthyUntil = healthyUntil
			entry.LastReason = "state_already_expired"
		}
	case stateLength > 0 && !isAcceptedStateLength(stateLength):
		entry.State = "degraded"
		entry.TurnStateValue = ""
		entry.ConsecutiveFailures++
		entry.LastReason = fmt.Sprintf("state_length_%d", stateLength)
		entry.CooldownUntil = now.Add(m.config.Config.DegradedCooldown)
	case record.Failed && (record.Failure.StatusCode == http.StatusUnauthorized || record.Failure.StatusCode == http.StatusForbidden):
		entry.State = "auth_error"
		entry.TurnStateValue = ""
		entry.ConsecutiveFailures++
		entry.LastReason = fmt.Sprintf("http_%d", record.Failure.StatusCode)
		entry.CooldownUntil = now.Add(m.config.Config.DegradedCooldown)
	case record.Failed && record.Failure.StatusCode == http.StatusTooManyRequests:
		entry.LastReason = "rate_limited"
	case record.Failed:
		entry.ConsecutiveFailures++
		entry.LastReason = classifyUsageFailure(record.Failure)
		if entry.ConsecutiveFailures >= m.config.Config.FailureThreshold {
			entry.State = "transient_failure"
			entry.TurnStateValue = ""
			entry.CooldownUntil = now.Add(m.config.Config.FailureCooldown)
		}
	default:
		if entry.State == "" {
			entry.LastReason = "completed_without_state"
		}
	}
	m.health[key] = entry
	markStateDirty()
}

// observeProbe connects the plugin's own bounded probe evidence to the
// scheduler. Unlike the generic usage callback, the probe has both the exact
// host AuthID and the upstream state/model evidence needed for a reliable
// health decision.
func (m *accountRoutingManager) observeProbe(authID, requestedModel string, record probeRecord, stateValue string, now time.Time) {
	model := routingModelKey(requestedModel)
	if strings.TrimSpace(authID) == "" || model == "" {
		return
	}
	healthyUntil := accountStateExpiry(stateValue, now, currentProbeConfig().Config.TTL)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.health == nil {
		m.health = map[string]accountModelHealth{}
	}
	key := accountHealthKey(authID, model)
	entry := m.health[key]
	entry.AuthID = authID
	entry.Account = publicAccountID(authID)
	entry.Model = model
	entry.ObservedAt = now
	entry.LastStateLength = record.StateLength
	entry.LastStatusCode = record.StatusCode

	switch {
	case record.Success && isAcceptedStateLength(record.StateLength) && probeModelConsistent(requestedModel, record.ObservedModel) && stateValue != "" && healthyUntil.After(now):
		entry.State = "healthy"
		entry.TurnStateValue = stateValue
		entry.HealthyUntil = healthyUntil
		entry.ConsecutiveFailures = 0
		entry.LastReason = "probe_healthy"
		entry.CooldownUntil = time.Time{}
	case record.StatusCode == http.StatusUnauthorized || record.StatusCode == http.StatusForbidden:
		entry.State = "auth_error"
		entry.TurnStateValue = ""
		entry.ConsecutiveFailures++
		entry.LastReason = fmt.Sprintf("probe_http_%d", record.StatusCode)
		entry.CooldownUntil = now.Add(m.config.Config.DegradedCooldown)
	case record.StateLength > 0 && !isAcceptedStateLength(record.StateLength):
		entry.State = "degraded"
		entry.TurnStateValue = ""
		entry.ConsecutiveFailures++
		entry.LastReason = fmt.Sprintf("probe_state_length_%d", record.StateLength)
		entry.CooldownUntil = now.Add(m.config.Config.DegradedCooldown)
	case record.ObservedModel != "" && !probeModelConsistent(requestedModel, record.ObservedModel):
		entry.State = "degraded"
		entry.TurnStateValue = ""
		entry.ConsecutiveFailures++
		entry.LastReason = "probe_model_mismatch"
		entry.CooldownUntil = now.Add(m.config.Config.DegradedCooldown)
	default:
		// Connection and proxy failures are not account-quality evidence. The
		// probe engine keeps those diagnostics and exit cooldowns separately.
		return
	}
	m.health[key] = entry
	markStateDirty()
}

func responseStateValue(headers http.Header) string {
	for key, values := range headers {
		if !strings.EqualFold(key, turnStateHeader) {
			continue
		}
		for i := len(values) - 1; i >= 0; i-- {
			if value := strings.TrimSpace(values[i]); value != "" {
				return value
			}
		}
	}
	return ""
}

func accountStateExpiry(value string, capturedAt time.Time, ttl time.Duration) time.Time {
	if strings.TrimSpace(value) == "" {
		return time.Time{}
	}
	if ttl <= 0 {
		ttl = time.Duration(probeDefaultsTTLMinutes) * time.Minute
	}
	if generatedAt, ok := parseTurnStateTimestamp(value); ok {
		return generatedAt.Add(ttl).UTC()
	}
	return capturedAt.Add(ttl).UTC()
}

func expireAccountHealth(entry *accountModelHealth, now time.Time) bool {
	if entry == nil || entry.State != "healthy" || entry.HealthyUntil.IsZero() || now.Before(entry.HealthyUntil) {
		return false
	}
	entry.State = "expired"
	entry.TurnStateValue = ""
	entry.LastReason = "account_state_ttl_expired"
	return true
}

// accountTurnState returns the selected account's own state. The second
// result reports whether account-scoped routing is enabled; callers must not
// fall back to a model-global state when it is true.
func (m *accountRoutingManager) accountTurnState(authID, model string, now time.Time) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.config.Config.Enabled || m.config.Error != "" {
		return "", false
	}
	model = routingModelKey(model)
	if strings.TrimSpace(authID) == "" || model == "" {
		return "", true
	}
	key := accountHealthKey(authID, model)
	entry, ok := m.health[key]
	if !ok {
		return "", true
	}
	if expireAccountHealth(&entry, now) {
		m.health[key] = entry
		markStateDirty()
	}
	if entry.State != "healthy" || entry.TurnStateValue == "" || !isAcceptedStateLength(len(entry.TurnStateValue)) {
		return "", true
	}
	return entry.TurnStateValue, true
}

// observeRequestState binds a healthy client-carried state to the AuthID that
// CPA selected before the request interceptor runs. Unlike response/probe
// capture, request adoption requires a valid embedded timestamp so an
// arbitrary length-matched string cannot create a synthetic healthy lease.
func (m *accountRoutingManager) observeRequestState(authID, model, value string, now time.Time) {
	model = routingModelKey(model)
	value = strings.TrimSpace(value)
	if strings.TrimSpace(authID) == "" || model == "" || !isAcceptedStateLength(len(value)) {
		return
	}
	generatedAt, ok := parseTurnStateTimestamp(value)
	if !ok {
		return
	}
	ttl := currentProbeConfig().Config.TTL
	if ttl <= 0 {
		ttl = time.Duration(probeDefaultsTTLMinutes) * time.Minute
	}
	healthyUntil := generatedAt.Add(ttl).UTC()
	if !healthyUntil.After(now) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.config.Config.Enabled || m.config.Error != "" {
		return
	}
	if m.health == nil {
		m.health = map[string]accountModelHealth{}
	}
	key := accountHealthKey(authID, model)
	entry := m.health[key]
	entry.AuthID = authID
	entry.Account = publicAccountID(authID)
	entry.Model = model
	entry.State = "healthy"
	entry.TurnStateValue = value
	entry.HealthyUntil = healthyUntil
	entry.LastStateLength = len(value)
	entry.ConsecutiveFailures = 0
	entry.LastStatusCode = 0
	entry.LastReason = "healthy_request_state"
	entry.ObservedAt = now
	entry.CooldownUntil = time.Time{}
	m.health[key] = entry
	markStateDirty()
}

func (m *accountRoutingManager) accountDegradedReason(authID, model string, now time.Time) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.config.Config.Enabled || m.config.Error != "" {
		return "", false
	}
	model = routingModelKey(model)
	if strings.TrimSpace(authID) == "" || model == "" {
		return "", true
	}
	key := accountHealthKey(authID, model)
	entry, ok := m.health[key]
	if !ok {
		return "", true
	}
	if expireAccountHealth(&entry, now) {
		m.health[key] = entry
		markStateDirty()
	}
	if entry.CooldownUntil.IsZero() || !now.Before(entry.CooldownUntil) {
		return "", true
	}
	switch entry.State {
	case "degraded":
		return "该账号已观测到异常 state 或模型不一致", true
	case "auth_error":
		return "该账号鉴权已失效", true
	case "transient_failure":
		return "该账号连续请求失败，正在短暂冷却", true
	default:
		return "", true
	}
}

// degradedRejectMessageForAccount isolates rejection evidence by AuthID when
// experimental account routing is enabled. A degraded account must never
// make another account's request fail, while disabling the experiment keeps
// the legacy model-global behavior intact.
func degradedRejectMessageForAccount(authID string, models ...string) string {
	now := time.Now().UTC()
	scopedMode := false
	for _, candidate := range models {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if reason, scoped := accountRouter.accountDegradedReason(authID, candidate, now); scoped {
			scopedMode = true
			if reason == "" {
				continue
			}
			return fmt.Sprintf("账号 %s 的模型 %s 当前不可用：%s。请求已被 O/对抗插件拦截，请稍后重试或切换账号。", publicAccountID(authID), candidate, reason)
		}
		return degradedRejectMessage(models...)
	}
	if scopedMode {
		return ""
	}
	return ""
}

func selectedAuthID(metadata map[string]any) string {
	if len(metadata) == 0 {
		return ""
	}
	value, _ := metadata["selected_auth_id"].(string)
	return strings.TrimSpace(value)
}

func classifyUsageFailure(failure usageFailure) string {
	body := strings.ToLower(failure.Body)
	switch {
	case strings.Contains(body, "server_is_overloaded"), strings.Contains(body, "at capacity"):
		return "overloaded"
	case failure.StatusCode >= 500:
		return fmt.Sprintf("http_%d", failure.StatusCode)
	default:
		return "request_failed"
	}
}

func routingModelKey(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.Contains(model, "astra"):
		return "astra"
	case strings.Contains(model, "sol"):
		return "sol"
	default:
		return ""
	}
}

func accountHealthKey(authID, model string) string { return authID + "\x00" + model }

func publicAccountID(authID string) string {
	sum := sha256.Sum256([]byte(authID))
	return "auth-" + hex.EncodeToString(sum[:6])
}

func accountRoutingSummary() map[string]any {
	accountRouter.mu.Lock()
	defer accountRouter.mu.Unlock()
	now := time.Now().UTC()
	entries := make([]accountModelHealth, 0, len(accountRouter.health))
	for key, entry := range accountRouter.health {
		if expireAccountHealth(&entry, now) {
			accountRouter.health[key] = entry
			markStateDirty()
		}
		entry.AuthID = ""
		entry.TurnStateValue = ""
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ObservedAt.Equal(entries[j].ObservedAt) {
			return entries[i].Account < entries[j].Account
		}
		return entries[i].ObservedAt.After(entries[j].ObservedAt)
	})
	return map[string]any{
		"enabled":                   accountRouter.config.Config.Enabled,
		"monitoring":                true,
		"error":                     accountRouter.config.Error,
		"degraded_cooldown_minutes": int(accountRouter.config.Config.DegradedCooldown / time.Minute),
		"failure_cooldown_minutes":  int(accountRouter.config.Config.FailureCooldown / time.Minute),
		"failure_threshold":         accountRouter.config.Config.FailureThreshold,
		"usage_observed":            accountRouter.usageObserved,
		"scheduler_calls":           accountRouter.schedulerCalls,
		"routing_handled":           accountRouter.routingHandled,
		"last_scheduler":            accountRouter.lastScheduler,
		"accounts":                  entries,
	}
}
