package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Real loopback requests verify yielding between retries, not just sorting a
// slice. A round uses two exits so a resumed cursor/budget reset is observable.
func TestPriorityProbeYieldsAndPreservesRound(t *testing.T) {
	for _, mode := range []string{"resume", "automatic", "settled", "pause", "stop-current", "stop-all", "halted-oneoff", "reordered", "edit-exit"} {
		t.Run(mode, func(t *testing.T) {
			e := newPrefetchTestEngine(t)
			high, low := "gpt-6-astra", "gpt-5.6-sol"
			if mode == "reordered" {
				high, low = low, high
			}
			lowEntered, highEntered := make(chan struct{}), make(chan struct{})
			lowRelease, highRelease := make(chan struct{}), make(chan struct{})
			var lowOnce, highOnce sync.Once
			var mu sync.Mutex
			type call struct {
				Model string
				Exit  int
			}
			var calls []call
			var callTimes []time.Time
			active, maxActive := 0, 0
			counts := map[string]int{}
			serve := func(exit int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					var payload struct {
						Model string `json:"model"`
					}
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					mu.Lock()
					calls = append(calls, call{payload.Model, exit})
					callTimes = append(callTimes, time.Now())
					counts[payload.Model]++
					first := counts[payload.Model] == 1
					active++
					maxActive = max(maxActive, active)
					mu.Unlock()
					defer func() { mu.Lock(); active--; mu.Unlock() }()
					if first && payload.Model == low {
						close(lowEntered)
						<-lowRelease
					}
					if first && payload.Model == high {
						close(highEntered)
						<-highRelease
					}
					w.WriteHeader(http.StatusServiceUnavailable)
				}
			}
			first, second, edited := httptest.NewServer(serve(0)), httptest.NewServer(serve(1)), httptest.NewServer(serve(2))
			t.Cleanup(first.Close)
			t.Cleanup(second.Close)
			t.Cleanup(edited.Close)
			t.Cleanup(func() {
				lowOnce.Do(func() { close(lowRelease) })
				highOnce.Do(func() { close(highRelease) })
				e.stop()
				waitPrefetchIdle(t, e)
			})
			e.cfg.Config.Models = []string{high, low}
			e.cfg.Config.Proxies = []string{first.URL, second.URL}
			e.cfg.Config.UpstreamURL = "http://mock-upstream.invalid/responses"
			e.cfg.Config.MaxAttemptsPerRound = 3
			e.cfg.Config.AttemptsPerHop = 2
			e.cfg.Config.ProbeInterval = 15 * time.Millisecond
			e.cfg.Config.ExitFailThreshold = 100
			e.cfg.Config.Timeout = 2 * time.Second
			e.halted = mode == "halted-oneoff"
			if err := os.WriteFile(e.cfg.Config.CredFile, []byte(`{"access_token":"mock-only"}`), 0o600); err != nil {
				t.Fatal(err)
			}
			await := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(3 * time.Second):
					t.Fatal("mock request did not arrive")
				}
			}
			lowForced := mode != "automatic" && mode != "settled"
			if !e.enqueueTask(low, lowForced) {
				t.Fatal("lower priority task rejected")
			}
			await(lowEntered)
			force := mode == "halted-oneoff"
			if !e.enqueueTask(high, force) {
				t.Fatal("higher priority task rejected")
			}
			if e.enqueueTask(high, force) {
				t.Fatal("duplicate priority task accepted")
			}
			lowOnce.Do(func() { close(lowRelease) })
			await(highEntered)
			e.mu.Lock()
			pending := append([]probeTask(nil), e.queue...)
			lowProbing := e.probing[low]
			_, prematurelyFailed := e.failures[low]
			e.mu.Unlock()
			if lowProbing || prematurelyFailed || len(pending) != 1 || pending[0].Model != low || pending[0].Force != lowForced || pending[0].Progress == nil || pending[0].Progress.Attempts != 1 {
				t.Fatalf("interrupted round was lost, restarted or failed: %+v", pending)
			}
			switch mode {
			case "settled":
				e.storeValue(low, synthStateToken(time.Now()), "business", "", e.cfg.Config)
			case "pause":
				e.setModelPaused(low, true)
			case "stop-current":
				e.stopCurrent()
			case "stop-all":
				e.stop()
			case "edit-exit":
				if err := e.saveExit(exitEdit{ID: defaultExitID(first.URL), URL: edited.URL, Attempts: 2, Multiplier: 1}); err != nil {
					t.Fatal(err)
				}
			}
			highOnce.Do(func() { close(highRelease) })
			waitPrefetchIdle(t, e)
			lastExit := 0
			if mode == "edit-exit" {
				lastExit = 2
			}
			want := []call{{low, 0}, {high, 0}, {high, 1}, {high, lastExit}, {low, 1}, {low, lastExit}}
			if mode == "pause" || mode == "settled" {
				want = want[:4]
			}
			if mode == "stop-current" || mode == "stop-all" {
				want = want[:2]
			}
			mu.Lock()
			got := append([]call(nil), calls...)
			times := append([]time.Time(nil), callTimes...)
			peak := maxActive
			mu.Unlock()
			if !reflect.DeepEqual(got, want) || peak != 1 {
				t.Fatalf("order/budget/serialization: got=%v want=%v peak=%d", got, want, peak)
			}
			for i := 1; i < len(times); i++ {
				if times[i].Sub(times[i-1]) < 10*time.Millisecond {
					t.Fatal("priority scheduling bypassed the serial interval")
				}
			}
			e.mu.Lock()
			failure, halted := e.failures[low], e.halted
			e.mu.Unlock()
			if len(want) == 6 && failure.Attempts != 3 {
				t.Fatalf("resumed attempt budget reset: %+v", failure)
			}
			if halted != (mode == "halted-oneoff" || mode == "stop-all") {
				t.Fatal("priority scheduling changed global halt")
			}
		})
	}
}
