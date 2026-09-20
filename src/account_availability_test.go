package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEnabledAccountInventoryFollowsHostWithoutLosingSlots(t *testing.T) {
	e := accountTestEngine(t)
	original := hostAuthListFunc
	bEnabled := false
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		entries, _ := original()
		entries[1].Disabled = !bEnabled
		// An enabled credential with a temporary host error remains visible and
		// can be probed; only the host's explicit disabled state hides it.
		entries[0].Unavailable = true
		entries[0].Status = "error"
		entries[0].NextRetryAfter = time.Now().Add(time.Hour)
		return entries, nil
	}
	ticket := synthStateToken(time.Now())
	e.storeValue(accountTarget("fixture-B"), ticket, "probe", "", e.cfg.Config)
	e.halted = true
	e.cfg.Config.Prefetch = 0
	for _, enabled := range []bool{false, true, false} {
		bEnabled = enabled
		summary := probeSummary()
		rows := summary["values"].([]map[string]any)
		want := 1
		if enabled {
			want = 2
		}
		if len(rows) != want || summary["account_binding_ready"] != true {
			t.Fatal("host enable state not reflected while stopped")
		}
		for _, row := range rows {
			if row["account_email"] != "alpha@example.test" && row["account_email"] != "beta@example.test" {
				t.Fatal("missing email label")
			}
		}
		if e.activeValueFor(accountTarget("fixture-B")) != ticket {
			t.Fatal("hiding disabled account deleted its slot")
		}
		if len(e.queue) != 0 {
			t.Fatal("inventory display started probes")
		}
	}
}

func TestProbeFailuresNeverCooldownAccount(t *testing.T) {
	for _, scenario := range []string{"401", "403", "429", "mismatch", "length"} {
		t.Run(scenario, func(t *testing.T) {
			e := accountTestEngine(t)
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if calls == 1 {
					switch scenario {
					case "401":
						w.WriteHeader(401)
						return
					case "403":
						w.WriteHeader(403)
						return
					case "429":
						w.WriteHeader(429)
						return
					}
				}
				ticket := synthStateToken(time.Now())
				model := "gpt-6-astra"
				if calls == 1 && scenario == "mismatch" {
					model = "gpt-5.6-luna"
				}
				if calls == 1 && scenario == "length" {
					ticket = "invalid"
				}
				w.Header().Set(turnStateHeader, ticket)
				_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"model\":\"" + model + "\"}}\n\n"))
			}))
			defer server.Close()
			cfg := e.cfg.Config
			cfg.UpstreamURL = server.URL
			target := accountTarget("fixture-A")
			first, _ := e.probeOnce(target, "direct", cfg)
			if first.Success {
				t.Fatal("fixture must first fail")
			}
			if _, err := e.credentialForTarget(target, cfg); err != nil {
				t.Fatal("failure blocked credential", err)
			}
			e.syncProbeAccounts()
			if !e.targetAllowedLocked(target) {
				t.Fatal("failure removed eligible account")
			}
			second, value := e.probeOnce(target, "direct", cfg)
			if !second.Success || value == "" || calls != 2 {
				t.Fatal("next attempt did not recover on the same account")
			}
		})
	}
}

func TestLegacyAccountCooldownCannotBeRestored(t *testing.T) {
	e := accountTestEngine(t)
	var state persistedState
	raw := `{"skip_account_cooldowns":false,"account_health":[{"auth_id":"fixture-A","model":"astra","state":"degraded","cooldown_until":"2099-01-01T00:00:00Z"}]}`
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatal(err)
	}
	applyPersistedState(state)
	if _, err := e.credentialForTarget(accountTarget("fixture-A"), e.cfg.Config); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(collectState())
	if strings.Contains(string(encoded), "skip_account_cooldowns") || strings.Contains(string(encoded), "2099-01-01") {
		t.Fatal("legacy cooldown remains active")
	}
}
