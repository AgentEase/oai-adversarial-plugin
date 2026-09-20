package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type hostAuthEntry struct {
	ID             string    `json:"id"`
	AuthIndex      string    `json:"auth_index"`
	Name           string    `json:"name"`
	Type           string    `json:"type"`
	Provider       string    `json:"provider"`
	Label          string    `json:"label"`
	Status         string    `json:"status"`
	Disabled       bool      `json:"disabled"`
	Unavailable    bool      `json:"unavailable"`
	Priority       int       `json:"priority"`
	Success        int64     `json:"success"`
	Failed         int64     `json:"failed"`
	LastRefresh    time.Time `json:"last_refresh"`
	NextRetryAfter time.Time `json:"next_retry_after"`
	Email          string    `json:"email"`
}
type hostAuthListResponse struct {
	Files []hostAuthEntry `json:"files"`
}
type hostAuthGetResponse struct {
	JSON json.RawMessage `json:"json"`
}

var hostAuthListFunc = func() ([]hostAuthEntry, error) {
	raw, err := callHost("host.auth.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var response hostAuthListResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decode auth list")
	}
	return response.Files, nil
}
var hostAuthGetFunc = func(index string) (json.RawMessage, error) {
	raw, err := callHost("host.auth.get", map[string]string{"auth_index": index})
	if err != nil {
		return nil, err
	}
	var response hostAuthGetResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decode auth credential")
	}
	return response.JSON, nil
}

func (e *probeEngine) resolveProbeCredential(cfg probeConfig) (probeCredential, error) {
	if cfg.AccountMode != "highest-priority" {
		return readProbeCredential(cfg.CredFile)
	}
	entries, err := hostAuthListFunc()
	if err != nil {
		return probeCredential{}, fmt.Errorf("list CPA credentials: %w", err)
	}
	eligible := e.eligibleProbeAccounts(entries, time.Now())
	limit := cfg.CandidateLimit
	if limit <= 0 || limit > len(eligible) {
		limit = len(eligible)
	}
	for _, entry := range eligible[:limit] {
		raw, err := hostAuthGetFunc(entry.AuthIndex)
		if err != nil {
			e.cooldownProbeAccount(entry.AuthIndex, 90*time.Second)
			continue
		}
		var cred probeCredential
		if json.Unmarshal(raw, &cred) != nil || strings.TrimSpace(cred.AccessToken) == "" {
			e.cooldownProbeAccount(entry.AuthIndex, cfg.AuthCooldown)
			continue
		}
		cred.AuthIndex = entry.AuthIndex
		cred.AuthID = entry.ID
		cred.Priority = entry.Priority
		cred.Label = maskedAccountLabel(entry)
		return cred, nil
	}
	return probeCredential{}, fmt.Errorf("no eligible CPA credential")
}

func (e *probeEngine) eligibleProbeAccounts(entries []hostAuthEntry, now time.Time) []hostAuthEntry {
	eligible := make([]hostAuthEntry, 0, len(entries))
	e.mu.Lock()
	if e.authCooldowns == nil {
		e.authCooldowns = map[string]time.Time{}
	}
	for id, until := range e.authCooldowns {
		if !now.Before(until) {
			delete(e.authCooldowns, id)
		}
	}
	for _, entry := range entries {
		provider := strings.ToLower(entry.Provider + " " + entry.Type)
		status := strings.ToLower(strings.TrimSpace(entry.Status))
		_, cooling := e.authCooldowns[entry.AuthIndex]
		if entry.AuthIndex == "" || entry.Disabled || entry.Unavailable || cooling || (!entry.NextRetryAfter.IsZero() && now.Before(entry.NextRetryAfter)) {
			continue
		}
		if !strings.Contains(provider, "codex") && !strings.Contains(provider, "openai") {
			continue
		}
		if status == "disabled" || status == "unavailable" || status == "error" {
			continue
		}
		eligible = append(eligible, entry)
	}
	e.mu.Unlock()
	sort.SliceStable(eligible, func(i, j int) bool {
		if eligible[i].Priority != eligible[j].Priority {
			return eligible[i].Priority > eligible[j].Priority
		}
		si, sj := eligible[i].Success-eligible[i].Failed, eligible[j].Success-eligible[j].Failed
		if si != sj {
			return si > sj
		}
		if !eligible[i].LastRefresh.Equal(eligible[j].LastRefresh) {
			return eligible[i].LastRefresh.After(eligible[j].LastRefresh)
		}
		return eligible[i].AuthIndex < eligible[j].AuthIndex
	})
	return eligible
}

// The watcher reads credential metadata every ten seconds but sends no model
// traffic unless the highest-ranked active credential actually changes.
func (e *probeEngine) authSelectionWatchLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			e.authSelectionScan()
		}
	}
}

func (e *probeEngine) authSelectionScan() {
	e.mu.Lock()
	cfg := e.cfg.Config
	suppressed := !cfg.Enabled || cfg.Prefetch <= 0 || cfg.AccountMode != "highest-priority" || e.cfg.Error != "" || e.halted || e.stopping || e.shuttingDown
	e.mu.Unlock()
	if suppressed {
		return
	}
	entries, err := hostAuthListFunc()
	if err != nil {
		return
	}
	eligible := e.eligibleProbeAccounts(entries, time.Now())
	if len(eligible) == 0 {
		return
	}
	selected := eligible[0].AuthIndex
	e.mu.Lock()
	defer e.mu.Unlock()
	// The host callback runs without our mutex. Recheck operator settings
	// before enqueueing so a concurrent stop or zero window cannot be lost.
	cfg = e.cfg.Config
	if !cfg.Enabled || cfg.Prefetch <= 0 || cfg.AccountMode != "highest-priority" || e.cfg.Error != "" || e.halted || e.stopping || e.shuttingDown {
		return
	}
	if !e.autoAuthSeen {
		e.autoAuthSeen = true
		e.lastAutoAuth = selected
		return
	}
	if selected == e.lastAutoAuth {
		return
	}
	e.lastAutoAuth = selected
	for _, model := range cfg.Models {
		if degradationDetectionEnabled(model, "") {
			e.enqueueTaskLocked(model, true)
		}
	}
}

func (e *probeEngine) cooldownProbeAccount(index string, duration time.Duration) {
	if index == "" || duration <= 0 {
		return
	}
	e.mu.Lock()
	if e.authCooldowns == nil {
		e.authCooldowns = map[string]time.Time{}
	}
	e.authCooldowns[index] = time.Now().Add(duration)
	e.mu.Unlock()
}

func maskedAccountLabel(entry hostAuthEntry) string {
	value := entry.Email
	if value == "" {
		value = entry.Label
	}
	if value == "" {
		value = entry.Name
	}
	parts := strings.SplitN(value, "@", 2)
	if len(parts) == 2 {
		local := parts[0]
		if len(local) > 3 {
			local = local[:3] + "***"
		}
		return local + "@" + parts[1]
	}
	if len(value) > 8 {
		return value[:5] + "***"
	}
	return "account"
}
