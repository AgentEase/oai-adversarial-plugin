package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestHighestPriorityCredentialSelectionAndCooldown(t *testing.T) {
	oldList, oldGet := hostAuthListFunc, hostAuthGetFunc
	t.Cleanup(func() { hostAuthListFunc = oldList; hostAuthGetFunc = oldGet })
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{
			{AuthIndex: "disabled", Provider: "codex", Disabled: true, Priority: 99},
			{AuthIndex: "low", Provider: "codex", Email: "low@example.com", Priority: 5, Success: 10},
			{AuthIndex: "high-b", Provider: "codex", Email: "bravo@example.com", Priority: 20, Success: 1, LastRefresh: time.Unix(20, 0)},
			{AuthIndex: "high-a", Provider: "codex", Email: "alpha@example.com", Priority: 20, Success: 2, LastRefresh: time.Unix(10, 0)},
		}, nil
	}
	hostAuthGetFunc = func(index string) (json.RawMessage, error) {
		return json.RawMessage(`{"access_token":"token-` + index + `","account_id":"account"}`), nil
	}
	e := &probeEngine{authCooldowns: map[string]time.Time{}}
	cfg := probeConfig{AccountMode: "highest-priority", CandidateLimit: 5, AuthCooldown: time.Hour}
	cred, err := e.resolveProbeCredential(cfg)
	if err != nil || cred.AuthIndex != "high-a" || cred.Priority != 20 || cred.Label != "alp***@example.com" {
		t.Fatalf("first=%+v err=%v", cred, err)
	}
	e.cooldownProbeAccount("high-a", time.Hour)
	cred, err = e.resolveProbeCredential(cfg)
	if err != nil || cred.AuthIndex != "high-b" {
		t.Fatalf("fallback=%+v err=%v", cred, err)
	}
}

func TestHighestPriorityCredentialRejectsInvalidAndNonCodex(t *testing.T) {
	oldList, oldGet := hostAuthListFunc, hostAuthGetFunc
	t.Cleanup(func() { hostAuthListFunc = oldList; hostAuthGetFunc = oldGet })
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{{AuthIndex: "other", Provider: "anthropic", Priority: 100}, {AuthIndex: "bad", Provider: "codex", Priority: 10}, {AuthIndex: "good", Type: "codex", Priority: 1}}, nil
	}
	hostAuthGetFunc = func(index string) (json.RawMessage, error) {
		if index == "bad" {
			return json.RawMessage(`{"account_id":"missing-token"}`), nil
		}
		return json.RawMessage(`{"access_token":"ok","account_id":"account"}`), nil
	}
	e := &probeEngine{}
	cred, err := e.resolveProbeCredential(probeConfig{AccountMode: "highest-priority", CandidateLimit: 3, AuthCooldown: time.Hour})
	if err != nil || cred.AuthIndex != "good" {
		t.Fatalf("credential=%+v err=%v", cred, err)
	}
}

func TestAuthSelectionScanTracksEnabledAccountChanges(t *testing.T) {
	oldList := hostAuthListFunc
	t.Cleanup(func() { hostAuthListFunc = oldList })
	selected := "first"
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{{AuthIndex: selected, Provider: "codex", Priority: 10}}, nil
	}
	e := &probeEngine{
		cfg:           probeConfigState{Config: probeConfig{Enabled: true, AccountMode: "highest-priority", Prefetch: time.Minute, Models: []string{"gpt-6-astra"}}},
		authCooldowns: map[string]time.Time{}, paused: map[string]bool{"gpt-6-astra": true}, probing: map[string]bool{},
	}
	e.authSelectionScan()
	if !e.autoAuthSeen || e.lastAutoAuth != "first" || len(e.queue) != 0 {
		t.Fatal("initial snapshot must not probe")
	}
	selected = "second"
	e.authSelectionScan()
	if e.lastAutoAuth != "second" || len(e.queue) != 0 {
		t.Fatal("account change was not tracked safely")
	}
}
