package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cpa-timezone/internal/statemirror"
)

func TestMirrorProjectionIsPrivateAndReadOnly(t *testing.T) {
	e := newPrefetchTestEngine(t)
	now := time.Now().UTC().Truncate(time.Second)
	e.cfg.Config.Models = []string{"gpt-6-astra", "gpt-5.6-sol", "gpt-5.6-luna"}
	e.halted = true
	e.paused["gpt-5.6-sol"] = true
	token := synthStateToken(now.Add(-time.Hour))
	e.values["gpt-6-astra"] = stateEntry{Value: token, ValueLength: 292, Source: "business", Valid: true, Proxy: "http://user:SECRET@proxy.invalid"}
	e.candidates["gpt-6-astra"] = stateEntry{Value: synthStateToken(now), ValueLength: 292, Valid: true, Source: "probe"}
	e.values["gpt-5.6-luna"] = e.values["gpt-6-astra"]
	e.lastActivity = "SECRET"
	e.business["gpt-6-astra"] = businessDegradation{Reason: "SECRET"}
	s := e.mirrorState(now)
	if s.Status != "halted" || s.PriorityModel != "gpt-6-astra" || s.Models[0].Candidate == nil || s.Models[0].Evidence != "business" || s.Models[1].Activity != "paused" || s.Models[2].Detection || s.Models[2].Active != nil {
		t.Fatalf("unexpected projection: %+v", s)
	}
	raw, _ := json.Marshal(s)
	for _, secret := range []string{"SECRET", "proxy.invalid", token, "value_preview", "last_activity", "proxies", "history"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("mirror leaked non-public information")
		}
	}
	if e.values["gpt-6-astra"].Value != token || e.candidates["gpt-6-astra"].Value == "" || !e.halted || len(e.queue) != 0 || e.probesTotal != 0 {
		t.Fatal("read-only projection changed engine state")
	}
	s2 := e.mirrorState(now.Add(time.Second))
	a, _ := json.Marshal(s)
	b, _ := json.Marshal(s2)
	if string(a) != string(b) {
		t.Fatal("countdown tick must not change projected state")
	}
	if s.Models[0].Active.Source != "business" || s.Models[0].Candidate.ExpiresAt != now.Add(e.cfg.Config.TTL).Format(time.RFC3339) {
		t.Fatal("source and real ticket timestamps must be preserved")
	}
}

func TestMirrorActivityMatchesDashboardWithoutChangingParticipation(t *testing.T) {
	e := newPrefetchTestEngine(t)
	e.running = true
	e.halted = true
	e.probing["gpt-6-astra"] = true
	s := e.mirrorState(time.Now())
	if s.Status != "running" || s.Models[0].Activity != "probing" {
		t.Fatal("one-off activity must stay visible while halted")
	}
	e.paused["gpt-6-astra"] = true
	if e.mirrorState(time.Now()).Models[0].Activity != "stopping" {
		t.Fatal("paused in-flight activity must report draining")
	}
	e.running = false
	delete(e.probing, "gpt-6-astra")
	if e.mirrorState(time.Now()).Models[0].Activity != "paused" {
		t.Fatal("actual pause must survive idle")
	}
	e.cfg.Config.Enabled = false
	e.cfg.Config.Models = []string{"gpt-5.6-luna"}
	if e.mirrorState(time.Now()).Status != "unchecked" {
		t.Fatal("unchecked policy must match the dashboard")
	}
}

func TestMirrorSenderRetriesLatestAndStops(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	var version atomic.Uint64
	var requests atomic.Int32
	delivered := make(chan statemirror.Event, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !statemirror.Verify(key, r.Header.Get("X-Mirror-Timestamp"), r.Header.Get("X-Mirror-Signature"), body, time.Now()) {
			t.Error("invalid push signature")
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("push must not include CPA credentials")
		}
		if requests.Add(1) == 1 {
			version.Store(2)
			w.WriteHeader(503)
			return
		}
		var event statemirror.Event
		_ = json.Unmarshal(body, &event)
		delivered <- event
		w.WriteHeader(204)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runStateMirror(ctx, server.URL, key, func(time.Time) statemirror.State {
			return statemirror.State{Status: "manual", ProbesTotal: version.Load(), Models: []statemirror.Model{}}
		}, 5*time.Millisecond, 60*time.Millisecond)
	}()
	var first statemirror.Event
	select {
	case first = <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("no retry")
	}
	if first.State.ProbesTotal != 2 {
		t.Fatal("retry must use latest state")
	}
	select {
	case next := <-delivered:
		if next.Sequence <= first.Sequence || next.Instance != first.Instance {
			t.Fatal("heartbeat identity/order invalid")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no heartbeat")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sender did not stop")
	}
}

func TestMirrorTransportRejectsRedirectAndCancels(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	now := time.Now().UTC()
	event := statemirror.Event{Schema: 1, Instance: strings.Repeat("a", 32), StartedAt: now, SentAt: now, Sequence: 1, State: statemirror.State{Status: "manual", Models: []statemirror.Model{}}}
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1); w.WriteHeader(204) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer redirect.Close()
	client := &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if sendMirror(context.Background(), client, redirect.URL, key, event) == nil || targetCalls.Load() != 0 {
		t.Fatal("redirect must not forward signature or data")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sendMirror(ctx, client, target.URL, key, event) == nil || targetCalls.Load() != 0 {
		t.Fatal("cancelled sender must not deliver")
	}
}

func TestMirrorSenderToReceiverIntegration(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	receiver, err := statemirror.NewReceiver("", key)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(receiver.Ingest))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runStateMirror(ctx, server.URL+"/ingest", key, func(time.Time) statemirror.State {
			return statemirror.State{Status: "halted", ProbesOK: 7, ProbesTotal: 20, Models: []statemirror.Model{}}
		}, 5*time.Millisecond, time.Second)
	}()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		view := receiver.View(time.Now())
		if view.Snapshot != nil {
			if view.Snapshot.State.ProbesOK != 7 || view.Snapshot.State.Status != "halted" {
				t.Fatal("state changed in transit")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("signed sender snapshot never reached actual receiver")
}
