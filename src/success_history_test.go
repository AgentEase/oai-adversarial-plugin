package main

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestProbeSuccessHistoryRetentionAndPersistence(t *testing.T) {
	e := newPrefetchTestEngine(t)
	e.halted = true
	for i := 0; i < 65; i++ {
		e.appendRecord(probeRecord{Model: strconv.Itoa(i), Success: true})
	}
	for i := 0; i < probeHistoryLimit+1; i++ {
		e.appendRecord(probeRecord{Model: "failed"})
	}
	if len(e.history) != probeHistoryLimit || e.history[0].Success {
		t.Fatal("mixed history should contain only the latest failures")
	}
	if len(e.successHistory) != 50 || e.successHistory[0].Model != "15" || e.successHistory[49].Model != "64" {
		t.Fatal("success history must independently retain the latest 50 successes")
	}
	if got := probeSummary()["success_history"].([]probeRecord); got[0].Model != "64" || got[49].Model != "15" {
		t.Fatal("public success history must be newest first")
	}
	savePersistedState()
	if _, err := os.Stat(os.Getenv("LKS_TZ_STATE_FILE")); err != nil {
		t.Fatal(err)
	}
	e.history = nil
	e.successHistory = nil
	loadPersistedState()
	loadPersistedState()
	if len(e.successHistory) != 50 || e.successHistory[0].Model != "15" || len(e.history) != probeHistoryLimit {
		t.Fatal("snapshot reload lost or duplicated independent successes")
	}
	if !e.halted || e.queueActive || e.probesTotal != 0 {
		t.Fatal("record retention must not start probes or change counters or user mode")
	}
}

func TestProbeSuccessHistorySnapshotMigration(t *testing.T) {
	e := newPrefetchTestEngine(t)
	var legacy persistedState
	if err := json.Unmarshal([]byte(`{"probe_history":[{"model":"old-success","success":true},{"model":"failure","success":false}]}`), &legacy); err != nil {
		t.Fatal(err)
	}
	applyPersistedState(legacy)
	if len(e.successHistory) != 1 || e.successHistory[0].Model != "old-success" {
		t.Fatal("legacy snapshots should recover their surviving successes")
	}
	legacy.ProbeSuccessHistory = []probeRecord{}
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var explicitEmpty persistedState
	if err := json.Unmarshal(encoded, &explicitEmpty); err != nil {
		t.Fatal(err)
	}
	applyPersistedState(explicitEmpty)
	if len(e.successHistory) != 0 || collectState().ProbeSuccessHistory == nil {
		t.Fatal("explicit empty success history must remain empty through snapshots")
	}
	for i := 0; i < 60; i++ {
		legacy.ProbeSuccessHistory = append(legacy.ProbeSuccessHistory,
			probeRecord{Model: strconv.Itoa(i), Success: true}, probeRecord{Model: "invalid-failure"})
	}
	applyPersistedState(legacy)
	if len(e.successHistory) != 50 || e.successHistory[0].Model != "10" || e.successHistory[49].Model != "59" {
		t.Fatal("restore must filter failures and retain only the newest 50 successes")
	}
}

func TestProbeSuccessHistoryLabelsAndRedaction(t *testing.T) {
	e := newPrefetchTestEngine(t)
	first := "socks5h://example-user:TEST_ONLY_A@192.0.2.1:1080"
	second := "socks5h://example-user:TEST_ONLY_B@192.0.2.1:1080"
	e.cfg.Config.ProxyLabels = map[string]string{first: "Exit A", second: "Exit B"}
	e.appendRecord(probeRecord{Success: true, Proxy: first, Error: "failure at " + first})
	e.appendRecord(probeRecord{Success: true, Proxy: second})
	for _, key := range []string{"history", "success_history"} {
		got := probeSummary()[key].([]probeRecord)
		if got[0].ProxyLabel != "Exit B" || got[1].ProxyLabel != "Exit A" {
			t.Fatal("labels must resolve before credentials are removed")
		}
		encoded, _ := json.Marshal(got)
		if strings.Contains(string(encoded), "TEST_ONLY_") || strings.Contains(string(encoded), "example-user") {
			t.Fatal("public histories leaked synthetic proxy credentials")
		}
	}
	e.cfg.Config.ProxyLabels[first] = "Renamed"
	if probeSummary()["success_history"].([]probeRecord)[1].ProxyLabel != "Renamed" || e.successHistory[0].Proxy != first {
		t.Fatal("summary must reflect renames without mutating private records")
	}
}
