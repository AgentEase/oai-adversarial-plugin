package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExitSuccessCountsSurviveHistoryAndRestart(t *testing.T) {
	e := newPrefetchTestEngine(t)
	state := synthStateToken(time.Now())
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			return
		}
		token := state
		switch r.URL.Path {
		case "/http-error":
			w.WriteHeader(503)
			return
		case "/model-error":
			payload.Model = "gpt-5.6-luna"
		case "/length-error":
			token = strings.Repeat("x", 312)
		case "/expired":
			token = synthStateToken(time.Now().Add(-2 * time.Hour))
		}
		w.Header().Set(turnStateHeader, token)
		fmt.Fprintf(w, "data: {\"response\":{\"model\":%q}}\n\n", payload.Model)
	}))
	defer upstream.Close()
	proxy, _ := url.Parse(upstream.URL)
	proxy.User = url.UserPassword("mock-user-a", "mock-pass")
	first := proxy.String()
	proxy.User = url.UserPassword("mock-user-b", "mock-pass")
	second := proxy.String()
	e.cfg.Config.Proxies = []string{first, second, "direct"}
	if err := os.WriteFile(e.cfg.Config.CredFile, []byte(`{"access_token":"mock-only"}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := e.cfg.Config
	cfg.UpstreamURL = upstream.URL + "/ok"
	for i := 0; i < 60; i++ {
		model := "gpt-6-astra"
		if i%2 != 0 {
			model = "gpt-5.6-sol"
		}
		record, _ := e.probeOnce(model, first, cfg)
		if !record.Success {
			t.Fatalf("mock success rejected: %s", record.Error)
		}
		e.appendRecord(record)
	}
	for _, spec := range []string{second, "direct"} {
		record, _ := e.probeOnce("gpt-6-astra", spec, cfg)
		if !record.Success {
			t.Fatal(record.Error)
		}
		e.appendRecord(record)
	}
	for _, path := range []string{"/http-error", "/model-error", "/length-error", "/expired"} {
		cfg.UpstreamURL = upstream.URL + path
		record, _ := e.probeOnce("gpt-6-astra", first, cfg)
		if record.Success {
			t.Fatalf("invalid response accepted: %s", path)
		}
		e.appendRecord(record)
	}
	for i := 0; i < 210; i++ {
		e.appendRecord(probeRecord{Proxy: first})
	}
	if len(e.successHistory) != 50 || len(e.history) != 200 || e.exitSuccessCounts[exitID(cfg, first)] != 60 || e.exitSuccessCounts[exitID(cfg, second)] != 1 || e.exitSuccessCounts[exitID(cfg, "direct")] != 1 {
		t.Fatal("counts used bounded history, merged credentials, or included failures")
	}
	since := e.exitSuccessSince
	if _, err := time.Parse(time.RFC3339Nano, since); err != nil {
		t.Fatal(err)
	}
	e.resetExit("")
	e.setExitEnabled(first, false)
	if err := e.saveExit(exitEdit{ID: defaultExitID(first), URL: first, Label: "Renamed", Multiplier: 1}); err != nil {
		t.Fatal(err)
	}
	rows := probeSummary()["proxies_state"].([]map[string]any)
	if rows[0]["success_count"] != "60" || rows[0]["disabled"] != true {
		t.Fatal("edit/reset/disable lost total")
	}
	savePersistedState()
	e.exitSuccessCounts = nil
	e.exitSuccessSince = ""
	loadPersistedState()
	loadPersistedState() // Reload must replace, never add to the saved totals.
	if e.exitSuccessCounts[defaultExitID(first)] != 60 || e.exitSuccessSince != since {
		t.Fatal("restart lost/duplicated counts or epoch")
	}
	snapshot := collectState()
	snapshot.ExitSuccessCounts[defaultExitID(first)] = 0
	if e.exitSuccessCounts[defaultExitID(first)] != 60 {
		t.Fatal("snapshot aliases live counters")
	}
}

func TestExitSuccessUsesAttemptIdentityDuringEdit(t *testing.T) {
	e := newPrefetchTestEngine(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	state := synthStateToken(time.Now())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.Header().Set(turnStateHeader, state)
		fmt.Fprint(w, "data: {\"response\":{\"model\":\"gpt-6-astra\"}}\n\n")
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	e.cfg.Config.Proxies = []string{server.URL}
	if err := os.WriteFile(e.cfg.Config.CredFile, []byte(`{"access_token":"mock-only"}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := e.cfg.Config
	cfg.UpstreamURL = "http://mock.invalid/responses"
	done := make(chan probeRecord, 1)
	go func() { record, _ := e.probeOnce("gpt-6-astra", server.URL, cfg); done <- record }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request not started")
	}
	id := defaultExitID(server.URL)
	if err := e.saveExit(exitEdit{ID: id, URL: "http://192.0.2.1:8080", Label: "Edited", Multiplier: 1}); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	select {
	case record := <-done:
		if !record.Success {
			t.Fatal(record.Error)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request not finished")
	}
	rows := probeSummary()["proxies_state"].([]map[string]any)
	if rows[0]["id"] != id || rows[0]["success_count"] != "1" || len(e.exitSuccessCounts) != 1 {
		t.Fatal("in-flight success attributed to wrong identity")
	}
}

func TestExitSuccessLegacySnapshotStartsFresh(t *testing.T) {
	e := newPrefetchTestEngine(t)
	applyPersistedState(persistedState{ProbesOK: 99, ProbeSuccessHistory: []probeRecord{{Proxy: "direct", Success: true}}})
	e.mu.Lock()
	e.ensureExitSuccessStatsLocked()
	e.mu.Unlock()
	if len(e.exitSuccessCounts) != 0 || e.exitSuccessSince == "" || e.probesOK != 99 || len(e.successHistory) != 1 {
		t.Fatal("legacy history was backfilled or changed")
	}
	since := e.exitSuccessSince
	e.mu.Lock()
	e.ensureExitSuccessStatsLocked()
	e.mu.Unlock()
	if e.exitSuccessSince != since {
		t.Fatal("counting epoch changed")
	}
}
