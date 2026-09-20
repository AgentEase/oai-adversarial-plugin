// Package statemirror defines the public, deliberately narrow status contract.
// It has no dependency on CPA, proxy configuration or ticket contents.
package statemirror

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

const MaxBody = 64 << 10
const StaleAfter = 90 * time.Second

type Ticket struct {
	Length    int    `json:"length"`
	Source    string `json:"source"`
	Valid     bool   `json:"valid"`
	IssuedAt  string `json:"issued_at,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

type Model struct {
	Name            string  `json:"name"`
	Detection       bool    `json:"detection"`
	Activity        string  `json:"activity"`
	Evidence        string  `json:"evidence"`
	FailureAttempts int     `json:"failure_attempts"`
	Active          *Ticket `json:"active,omitempty"`
	Candidate       *Ticket `json:"candidate,omitempty"`
}

type State struct {
	Status          string  `json:"status"`
	PriorityModel   string  `json:"priority_model"`
	ProbesOK        uint64  `json:"probes_ok"`
	ProbesTotal     uint64  `json:"probes_total"`
	PrefetchMinutes int     `json:"prefetch_minutes"`
	IntervalSeconds int     `json:"interval_seconds"`
	SettingsError   bool    `json:"settings_error"`
	SleepStartHour  int     `json:"sleep_start_hour,omitempty"`
	SleepEndHour    int     `json:"sleep_end_hour,omitempty"`
	SleepUntil      string  `json:"sleep_until,omitempty"`
	Models          []Model `json:"models"`
}

type Event struct {
	Schema    int       `json:"schema"`
	Instance  string    `json:"instance"`
	StartedAt time.Time `json:"started_at"`
	Sequence  uint64    `json:"sequence"`
	SentAt    time.Time `json:"sent_at"`
	State     State     `json:"state"`
}

// View carries receiver time, so browsers never need to trust their wall clock.
type View struct {
	ServerTime        time.Time `json:"server_time"`
	ReceivedAt        time.Time `json:"received_at"`
	Snapshot          *Event    `json:"snapshot"`
	StaleAfterSeconds int       `json:"stale_after_seconds"`
}

func Decode(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return errors.New("invalid mirror JSON")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("trailing mirror JSON")
	}
	return nil
}

var modelName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
var instanceName = regexp.MustCompile(`^[a-f0-9]{32}$`)

func oneOf(value string, allowed ...string) bool {
	for _, item := range allowed {
		if value == item {
			return true
		}
	}
	return false
}

func (e Event) Validate() error {
	bad := errors.New("invalid mirror snapshot")
	if e.Schema != 1 || !instanceName.MatchString(e.Instance) || e.StartedAt.IsZero() || e.SentAt.Before(e.StartedAt) || e.Sequence == 0 || e.Sequence > 1<<53-1 {
		return bad
	}
	s := e.State
	if !oneOf(s.Status, "disabled", "error", "unchecked", "stopping", "running", "halted", "standby", "manual", "offline", "sleeping") || s.Models == nil || len(s.Models) > 128 || s.ProbesOK > s.ProbesTotal || s.ProbesTotal > 1<<53-1 || s.PrefetchMinutes < 0 || s.IntervalSeconds < 0 {
		return bad
	}
	if s.SleepStartHour < 0 || s.SleepStartHour > 23 || s.SleepEndHour < 0 || s.SleepEndHour > 23 {
		return bad
	}
	if s.SleepUntil != "" {
		if _, err := time.Parse(time.RFC3339Nano, s.SleepUntil); err != nil {
			return bad
		}
	}
	seen := map[string]bool{}
	for _, m := range s.Models {
		if !modelName.MatchString(m.Name) || seen[m.Name] || m.FailureAttempts < 0 || !oneOf(m.Activity, "unchecked", "stopping", "queued", "probing", "paused", "unavailable", "halted", "standby", "manual", "sleeping") || !oneOf(m.Evidence, "none", "business", "failed", "suspect") {
			return bad
		}
		seen[m.Name] = true
		if !m.Detection && (m.Active != nil || m.Candidate != nil || m.Activity != "unchecked" || m.Evidence != "none" || m.FailureAttempts != 0) {
			return bad
		}
		for _, ticket := range []*Ticket{m.Active, m.Candidate} {
			if ticket == nil {
				continue
			}
			if ticket.Length < 0 || ticket.Length > 4096 || !oneOf(ticket.Source, "probe", "business", "seed", "response", "stream", "websocket", "request", "config", "unknown") {
				return bad
			}
			for _, stamp := range []string{ticket.IssuedAt, ticket.ExpiresAt} {
				if stamp != "" {
					if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
						return bad
					}
				}
			}
			if ticket.IssuedAt != "" && ticket.ExpiresAt != "" {
				a, _ := time.Parse(time.RFC3339Nano, ticket.IssuedAt)
				b, _ := time.Parse(time.RFC3339Nano, ticket.ExpiresAt)
				if !b.After(a) {
					return bad
				}
			}
		}
	}
	if s.PriorityModel != "" && !seen[s.PriorityModel] {
		return bad
	}
	return nil
}

func ReadKey(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("mirror key file is unreadable")
	}
	b = []byte(strings.TrimSpace(string(b)))
	if len(b) < 32 || len(b) > 4096 {
		return nil, errors.New("mirror key must contain 32 to 4096 bytes")
	}
	return b, nil
}

func Signature(key []byte, timestamp string, body []byte) string {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(timestamp + "\n"))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func Verify(key []byte, timestamp, signature string, body []byte, now time.Time) bool {
	t, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil || now.Sub(t) > time.Minute || t.Sub(now) > time.Minute {
		return false
	}
	got, err := hex.DecodeString(signature)
	if err != nil {
		return false
	}
	want, _ := hex.DecodeString(Signature(key, timestamp, body))
	return hmac.Equal(got, want)
}
