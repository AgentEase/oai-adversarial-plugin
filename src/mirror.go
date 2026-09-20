package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"cpa-timezone/internal/statemirror"
)

// mirrorState is read-only: in particular it must not promote candidates,
// start work, touch budgets or expose ticket bytes/proxy/errors/history.
func (e *probeEngine) mirrorState(now time.Time) statemirror.State {
	e.mu.Lock()
	defer e.mu.Unlock()
	cfg := e.cfg.Config
	s := statemirror.State{Models: []statemirror.Model{}, ProbesOK: e.probesOK, ProbesTotal: e.probesTotal, PrefetchMinutes: int(cfg.Prefetch / time.Minute), IntervalSeconds: int(cfg.ProbeInterval / time.Second), SettingsError: e.settingsError != ""}
	s.SleepStartHour, s.SleepEndHour = cfg.SleepHours.Start, cfg.SleepHours.End
	s.SleepUntil = sleepUntilText(cfg.SleepHours, now)
	for _, name := range cfg.Models {
		if degradationDetectionEnabled(name, "") {
			s.PriorityModel = name
			break
		}
	}
	noDetection := len(cfg.Models) > 0 && s.PriorityModel == ""
	ready := cfg.Enabled && e.cfg.Error == "" && !noDetection && (cfg.AccountMode == "" || len(e.probeTargetsLocked(cfg)) > 0)
	auto := ready && cfg.Prefetch > 0 && !e.halted && s.PriorityModel != ""
	switch {
	case e.shuttingDown:
		s.Status = "offline"
	case e.stopping:
		s.Status = "stopping"
	case ready && s.SleepUntil != "" && (!e.halted || e.running):
		s.Status = "sleeping"
	case e.running:
		s.Status = "running"
	case e.cfg.Error != "":
		s.Status = "error"
	case noDetection:
		s.Status = "unchecked"
	case !cfg.Enabled:
		s.Status = "disabled"
	case e.halted:
		s.Status = "halted"
	case auto:
		s.Status = "standby"
	default:
		s.Status = "manual"
	}
	queued := map[string]bool{}
	for _, name := range queueModels(e.queue) {
		queued[name] = true
	}
	threshold := cfg.SuspectThreshold
	if threshold <= 0 {
		threshold = probeDefaultsSuspectThreshold
	}
	for _, name := range e.probeTargetsLocked(cfg) {
		m := statemirror.Model{Name: name, Detection: degradationDetectionEnabled(name, ""), Activity: "unchecked", Evidence: "none"}
		if m.Detection {
			switch {
			case (e.probing[name] || queued[name]) && (e.stopping || e.targetPausedLocked(name)):
				m.Activity = "stopping"
			case s.Status == "sleeping" && !e.targetPausedLocked(name) && (!e.halted || e.probing[name] || queued[name]):
				m.Activity = "sleeping"
			case queued[name]:
				m.Activity = "queued"
			case e.probing[name]:
				m.Activity = "probing"
			case e.targetPausedLocked(name):
				m.Activity = "paused"
			case !ready || e.shuttingDown:
				m.Activity = "unavailable"
			case e.halted:
				m.Activity = "halted"
			case auto:
				m.Activity = "standby"
			default:
				m.Activity = "manual"
			}
			if _, ok := e.business[name]; ok {
				m.Evidence = "business"
			} else if f, ok := e.failures[name]; ok {
				m.Evidence = "failed"
				m.FailureAttempts = f.Attempts
			} else if f, ok := e.suspects[name]; ok && f.Failures >= threshold {
				m.Evidence = "suspect"
				m.FailureAttempts = f.Failures
			}
			if entry, ok := e.values[name]; ok {
				m.Active = mirrorTicket(entry, cfg.TTL)
			}
			if entry, ok := e.candidates[name]; ok && entry.Valid && entry.Value != "" {
				ticket := mirrorTicket(entry, cfg.TTL)
				expires, err := time.Parse(time.RFC3339Nano, ticket.ExpiresAt)
				if err != nil || now.Before(expires) {
					m.Candidate = ticket
				}
			}
		}
		s.Models = append(s.Models, m)
	}
	if cfg.AccountMode != "" {
		s.Models = aggregateAccountMirror(s.Models, cfg.Models, now)
	}
	return s
}

func mirrorTicket(entry stateEntry, ttl time.Duration) *statemirror.Ticket {
	t := &statemirror.Ticket{Length: entry.ValueLength, Source: "unknown", Valid: stateEntryAccepted(entry)}
	switch entry.Source {
	case "probe", "business", "seed", "response", "stream", "websocket", "request", "config":
		t.Source = entry.Source
	}
	if issued, ok := parseTurnStateTimestamp(entry.Value); ok {
		t.IssuedAt = issued.Format(time.RFC3339)
		t.ExpiresAt = issued.Add(ttl).Format(time.RFC3339)
	} else {
		if parsed, err := time.Parse(time.RFC3339, entry.GeneratedAt); err == nil {
			t.IssuedAt = parsed.Format(time.RFC3339)
		}
		if parsed, err := time.Parse(time.RFC3339, entry.ExpiresAt); err == nil {
			t.ExpiresAt = parsed.Format(time.RFC3339)
		}
	}
	return t
}

var mirrorLifecycle struct {
	sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// Opt-in environment configuration is intentionally separate from management
// actions. A public reader can never change the push destination or key.
func startStateMirror() {
	mirrorLifecycle.Lock()
	defer mirrorLifecycle.Unlock()
	if mirrorLifecycle.cancel != nil {
		return
	}
	endpoint := os.Getenv("LKS_MIRROR_URL")
	if endpoint == "" {
		return
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		fmt.Fprint(os.Stderr, "[WARN] - State mirror disabled: invalid endpoint\n")
		return
	}
	key, err := statemirror.ReadKey(os.Getenv("LKS_MIRROR_KEY_FILE"))
	if err != nil {
		fmt.Fprint(os.Stderr, "[WARN] - State mirror disabled: invalid key file\n")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	mirrorLifecycle.cancel = cancel
	mirrorLifecycle.done = done
	go func() {
		defer close(done)
		runStateMirror(ctx, endpoint, key, probeTrack.mirrorState, time.Second, 30*time.Second)
	}()
}

func stopStateMirror() {
	mirrorLifecycle.Lock()
	defer mirrorLifecycle.Unlock()
	if mirrorLifecycle.cancel != nil {
		mirrorLifecycle.cancel()
		<-mirrorLifecycle.done
		mirrorLifecycle.cancel = nil
	}
}

// Reconcile cheap immutable projections once a second, including activity
// changes that aren't persisted. Only changed projections or heartbeats go on
// the wire. One bounded worker coalesces updates while a receiver is offline.
func runStateMirror(ctx context.Context, endpoint string, key []byte, snapshot func(time.Time) statemirror.State, interval, heartbeat time.Duration) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return
	}
	instance := hex.EncodeToString(random[:])
	started := time.Now().UTC()
	transport := &http.Transport{IdleConnTimeout: 30 * time.Second, MaxIdleConnsPerHost: 1}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var previous []byte
	var sent, next time.Time
	var sequence uint64
	failed := false
	retry := interval
	for {
		if ctx.Err() != nil {
			return
		}
		now := time.Now().UTC()
		if !now.Before(next) {
			state := snapshot(now)
			encoded, _ := json.Marshal(state)
			if !bytes.Equal(encoded, previous) || now.Sub(sent) >= heartbeat {
				sequence++
				event := statemirror.Event{Schema: 1, Instance: instance, StartedAt: started, Sequence: sequence, SentAt: now, State: state}
				if event.Validate() == nil && sendMirror(ctx, client, endpoint, key, event) == nil {
					if failed {
						fmt.Fprint(os.Stderr, "[INFO] - State mirror delivery restored\n")
					}
					failed = false
					previous = encoded
					sent = now
					retry = interval
					next = time.Time{}
				} else {
					if ctx.Err() != nil {
						return
					}
					if !failed {
						fmt.Fprint(os.Stderr, "[WARN] - State mirror snapshot or delivery unavailable; retrying\n")
					}
					failed = true
					next = now.Add(retry)
					retry = min(retry*2, 15*time.Second)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func sendMirror(ctx context.Context, client *http.Client, endpoint string, key []byte, event statemirror.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if len(body) > statemirror.MaxBody {
		return fmt.Errorf("mirror snapshot too large")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	stamp := event.SentAt.Format(time.RFC3339Nano)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Mirror-Timestamp", stamp)
	req.Header.Set("X-Mirror-Signature", statemirror.Signature(key, stamp, body))
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1024))
	if res.StatusCode != http.StatusNoContent {
		return fmt.Errorf("mirror delivery rejected")
	}
	return nil
}
