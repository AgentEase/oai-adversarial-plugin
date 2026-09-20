package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRuntimeTimezonePersistsWithoutStartingProbes(t *testing.T) {
	e := newPrefetchTestEngine(t)
	previous := currentTimezone()
	t.Cleanup(func() { configuredTimezone.Store(previous) })
	e.halted = true
	e.paused["gpt-6-astra"] = true
	for _, zone := range []string{"Asia/Singapore", "UTC", "America/New_York"} {
		payload, _ := json.Marshal(map[string]string{"action": "target-timezone", "timezone": zone})
		response, err := probeControl(payload)
		if err != nil || response.StatusCode != 200 || currentTimezone() != zone {
			t.Fatalf("save zone failed: status=%d", response.StatusCode)
		}
	}
	e.settings = runtimeSettings{}
	configuredTimezone.Store(targetTimezone)
	loadRuntimeSettings()
	if err := configureTurnStateOverride([]byte("timezone: Asia/Tokyo\n")); err != nil {
		t.Fatal(err)
	}
	if currentTimezone() != "America/New_York" || !e.halted || !e.paused["gpt-6-astra"] || e.running {
		t.Fatal("restart/reconfigure lost panel override or started probes")
	}
	before, _ := os.ReadFile(runtimeSettingsPath())
	for _, zone := range []string{"", "Local", "Mars/Olympus", "../zone"} {
		if err := e.setTargetTimezone(zone); err == nil || currentTimezone() != "America/New_York" {
			t.Fatal("invalid timezone changed effective setting")
		}
	}
	after, _ := os.ReadFile(runtimeSettingsPath())
	if string(before) != string(after) {
		t.Fatal("invalid timezone modified disk settings")
	}
	blocked := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(blocked, "state.json"))
	if err := e.setTargetTimezone("UTC"); err == nil || currentTimezone() != "America/New_York" {
		t.Fatal("failed write changed effective timezone")
	}
}

func TestOriginalTicketLengthSurvivesResponseAndAccountRetry(t *testing.T) {
	e := accountTestEngine(t)
	ticket := synthStateToken(time.Now())
	for _, id := range []string{"fixture-A", "fixture-B"} {
		e.storeValue(accountTarget(id), ticket, "probe", "", e.cfg.Config)
	}
	for _, sample := range []struct {
		id     string
		length int
	}{{"fixture-A", 356}, {"fixture-A", 400}, {"fixture-B", 0}} {
		wantInjected := len(ticket)
		out := scopedRequest(t, sample.id, strings.Repeat("x", sample.length))
		if len(out.Headers.Get(turnStateHeader)) != wantInjected {
			t.Fatal("own ticket not injected")
		}
		history.observeTurnState("account-request", ticket, "response")
		history.mu.Lock()
		var record auditRecord
		for _, item := range history.records {
			if item.RequestID == "account-request" {
				record = item
			}
		}
		history.mu.Unlock()
		if record.TurnStateOriginalLength == nil || *record.TurnStateOriginalLength != sample.length || record.TurnStateInjectedLength != wantInjected || record.TurnStateLength != len(ticket) {
			t.Fatal("response/retry replaced request length evidence")
		}
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		var restored auditRecord
		if json.Unmarshal(raw, &restored) != nil || restored.TurnStateOriginalLength == nil || *restored.TurnStateOriginalLength != sample.length {
			t.Fatal("original length lost in snapshot")
		}
	}
	var legacy auditRecord
	if json.Unmarshal([]byte(`{"turn_state_length":292}`), &legacy) != nil || legacy.TurnStateOriginalLength != nil {
		t.Fatal("legacy original length invented")
	}
}
