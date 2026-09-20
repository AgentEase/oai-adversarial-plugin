package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpa-timezone/internal/statemirror"
)

func TestProbeSleepWindowUTC8(t *testing.T) {
	for _, tc := range []struct {
		hours    probeSleepHours
		now, end string
	}{
		{probeSleepHours{}, "2026-01-01T00:00:00Z", ""},
		{probeSleepHours{7, 7}, "2026-01-01T00:00:00Z", ""},
		{probeSleepHours{2, 7}, "2026-01-01T17:59:59Z", ""},
		{probeSleepHours{2, 7}, "2026-01-01T18:00:00Z", "2026-01-01T23:00:00Z"},
		{probeSleepHours{2, 7}, "2026-01-01T22:59:59Z", "2026-01-01T23:00:00Z"},
		{probeSleepHours{2, 7}, "2026-01-01T23:00:00Z", ""},
		{probeSleepHours{23, 7}, "2026-01-31T15:00:00Z", "2026-01-31T23:00:00Z"},
		{probeSleepHours{23, 7}, "2026-01-31T16:00:00Z", "2026-01-31T23:00:00Z"},
		{probeSleepHours{23, 0}, "2026-01-31T15:30:00Z", "2026-01-31T16:00:00Z"},
		{probeSleepHours{23, 0}, "2026-01-31T16:00:00Z", ""},
	} {
		now, _ := time.Parse(time.RFC3339, tc.now)
		// A different machine timezone must have no effect.
		got := sleepUntilText(tc.hours, now.In(time.FixedZone("other", -7*3600)))
		if got != tc.end {
			t.Fatalf("%+v at %s: got %s, want %s", tc.hours, tc.now, got, tc.end)
		}
	}
}

func TestRuntimeSleepHoursPersistAndValidate(t *testing.T) {
	e := newPrefetchTestEngine(t)
	e.halted = true
	e.paused["gpt-6-astra"] = true
	want := probeSleepHours{23, 7}
	res, _ := probeControl([]byte(`{"action":"sleep-hours","start_hour":23,"end_hour":7}`))
	if res.StatusCode != 200 || e.cfg.Config.SleepHours != want {
		t.Fatal("sleep hours not saved")
	}
	base := e.baseCfg.Config
	e.settings = runtimeSettings{}
	loadRuntimeSettings()
	loaded, err := applyRuntimeSettings(base, e.settings)
	if err != nil || loaded.SleepHours != want {
		t.Fatal("sleep hours did not survive restart")
	}
	for _, body := range []string{
		`{"action":"sleep-hours"}`, `{"action":"sleep-hours","start_hour":2}`,
		`{"action":"sleep-hours","start_hour":-1,"end_hour":7}`,
		`{"action":"sleep-hours","start_hour":2,"end_hour":24}`,
		`{"action":"sleep-hours","start_hour":2.5,"end_hour":7}`,
		`{"action":"sleep-hours","start_hour":"2","end_hour":7}`,
		`{"action":"check-egress","id":"direct"}`,
	} {
		res, _ = probeControl([]byte(body))
		if res.StatusCode != 400 || e.cfg.Config.SleepHours != want {
			t.Fatal("invalid or removed action changed settings")
		}
	}
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocked, []byte("test"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(blocked, "state.json"))
	if e.setSleepHours(probeSleepHours{}) == nil || e.cfg.Config.SleepHours != want {
		t.Fatal("failed persistence changed effective sleep")
	}
	if !e.halted || !e.paused["gpt-6-astra"] || e.queueActive || e.probesTotal != 0 {
		t.Fatal("save rearmed probes or changed participation")
	}
}

func sleepingHoursNow() probeSleepHours {
	// A 23-hour window keeps this test away from a real hour boundary.
	hour := time.Now().In(probeSleepZone).Hour()
	return probeSleepHours{(hour + 23) % 24, (hour + 22) % 24}
}

func TestProbeSleepRetainsBudgetAndHonorsStopPause(t *testing.T) {
	for _, mode := range []string{"wake", "stop", "pause"} {
		t.Run(mode, func(t *testing.T) {
			e := newPrefetchTestEngine(t)
			calls := make(chan int, 8)
			release := make(chan struct{})
			var once sync.Once
			var first atomic.Bool
			handler := func(exit int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					calls <- exit
					if !first.Swap(true) {
						<-release
					}
					w.WriteHeader(http.StatusServiceUnavailable)
				}
			}
			a, b := httptest.NewServer(handler(0)), httptest.NewServer(handler(1))
			t.Cleanup(a.Close)
			t.Cleanup(b.Close)
			t.Cleanup(func() { once.Do(func() { close(release) }); e.stop(); waitPrefetchIdle(t, e) })
			e.cfg.Config.Proxies = []string{a.URL, b.URL}
			e.cfg.Config.UpstreamURL = "http://sleep-test.invalid/responses"
			e.cfg.Config.MaxAttemptsPerRound = 3
			e.cfg.Config.AttemptsPerHop = 2
			e.cfg.Config.ExitFailThreshold = 100
			if err := os.WriteFile(e.cfg.Config.CredFile, []byte(`{"access_token":"mock-only"}`), 0o600); err != nil {
				t.Fatal(err)
			}
			if !e.enqueueTask("gpt-6-astra", true) {
				t.Fatal("manual probe rejected")
			}
			select {
			case exit := <-calls:
				if exit != 0 {
					t.Fatal("wrong initial exit")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("first request missing")
			}
			if err := e.setSleepHours(sleepingHoursNow()); err != nil {
				t.Fatal(err)
			}
			once.Do(func() { close(release) })
			select {
			case <-calls:
				t.Fatal("new request during sleep")
			case <-time.After(150 * time.Millisecond):
			}
			e.mu.Lock()
			failureCount := len(e.failures)
			e.mu.Unlock()
			if failureCount != 0 {
				t.Fatal("sleep falsely exhausted the round")
			}
			if mode == "stop" {
				e.stop()
			} else if mode == "pause" {
				e.setModelPaused("gpt-6-astra", true)
			}
			if mode != "wake" {
				waitPrefetchIdle(t, e)
			}
			if err := e.setSleepHours(probeSleepHours{}); err != nil {
				t.Fatal(err)
			}
			waitPrefetchIdle(t, e)
			if mode == "wake" {
				got := []int{}
				for len(calls) > 0 {
					got = append(got, <-calls)
				}
				if !reflect.DeepEqual(got, []int{1, 0}) {
					t.Fatalf("budget or rotation restarted: %v", got)
				}
			} else if len(calls) != 0 {
				t.Fatal("wake ignored stop or pause")
			}
		})
	}
}

func TestProbeSleepBlocksAutomaticAndManualAndKeepsTickets(t *testing.T) {
	e := newPrefetchTestEngine(t)
	now := time.Now()
	cfg := e.cfg.Config
	e.storeValue("gpt-6-astra", synthStateToken(now.Add(-cfg.TTL+time.Minute)), "seed", "", cfg)
	if err := e.setSleepHours(sleepingHoursNow()); err != nil {
		t.Fatal(err)
	}
	e.prefetchScan()
	if e.queueActive || e.probesTotal != 0 || len(e.prefetchGate) != 0 {
		t.Fatal("automatic scan entered sleep")
	}
	if !e.probeModelAsync("gpt-6-astra") {
		t.Fatal("manual work must be queued for after sleep")
	}
	time.Sleep(100 * time.Millisecond)
	e.mu.Lock()
	total := e.probesTotal
	e.mu.Unlock()
	if total != 0 {
		t.Fatal("manual request bypassed sleep")
	}
	e.stopCurrent()
	waitPrefetchIdle(t, e)
	// Existing candidate takeover stays available during sleep.
	e.mu.Lock()
	e.values["gpt-6-astra"] = stateEntry{Value: synthStateToken(now.Add(-2 * cfg.TTL)), Valid: true}
	e.candidates["gpt-6-astra"] = stateEntry{Value: synthStateToken(now), Valid: true}
	promoted := e.promoteCandidateLocked("gpt-6-astra", e.cfg.Config, now)
	e.mu.Unlock()
	if !promoted {
		t.Fatal("sleep blocked healthy ticket takeover")
	}
	if err := e.setSleepHours(probeSleepHours{}); err != nil {
		t.Fatal(err)
	}
	// Expired active and no successor: normal automatic scanning resumes.
	e.mu.Lock()
	e.values["gpt-6-astra"] = stateEntry{Value: synthStateToken(now.Add(-cfg.TTL)), Valid: true}
	e.mu.Unlock()
	e.prefetchScan()
	waitPrefetchIdle(t, e)
	if e.probesTotal != 1 {
		t.Fatal("automatic prefetch did not resume")
	}
}

func TestMirrorSleepContract(t *testing.T) {
	e := newPrefetchTestEngine(t)
	e.cfg.Config.SleepHours = probeSleepHours{2, 7}
	now := time.Date(2026, 1, 1, 3, 0, 0, 0, probeSleepZone)
	s := e.mirrorState(now)
	if s.Status != "sleeping" || s.Models[0].Activity != "sleeping" || s.SleepUntil == "" {
		t.Fatal("mirror did not expose sleep")
	}
	event := statemirror.Event{Schema: 1, Instance: "0123456789abcdef0123456789abcdef", StartedAt: now, SentAt: now, Sequence: 1, State: s}
	if err := event.Validate(); err != nil {
		t.Fatal(err)
	}
	event.State.SleepEndHour = 24
	if event.Validate() == nil {
		t.Fatal("invalid sleep hour accepted")
	}
	e.paused["gpt-6-astra"] = true
	e.halted = true
	s = e.mirrorState(now.Add(4 * time.Hour))
	if s.Status != "halted" || s.Models[0].Activity != "paused" {
		t.Fatal("wake changed explicit participation")
	}
}
