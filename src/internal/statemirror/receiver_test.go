package statemirror

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testKey = []byte(strings.Repeat("k", 32))

func sample() Event {
	now := time.Now().UTC()
	return Event{Schema: 1, Instance: strings.Repeat("a", 32), StartedAt: now.Add(-time.Minute), SentAt: now, Sequence: 1, State: State{Status: "standby", Models: []Model{}}}
}

func post(r *Receiver, e Event, mutate func(*http.Request)) int {
	b, _ := json.Marshal(e)
	req := httptest.NewRequest("POST", "/ingest", bytes.NewReader(b))
	stamp := e.SentAt.Format(time.RFC3339Nano)
	req.Header.Set("X-Mirror-Timestamp", stamp)
	req.Header.Set("X-Mirror-Signature", Signature(testKey, stamp, b))
	if mutate != nil {
		mutate(req)
	}
	w := httptest.NewRecorder()
	r.Ingest(w, req)
	return w.Code
}

func TestReceiverAuthenticationOrderPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	r, err := NewReceiver(path, testKey)
	if err != nil {
		t.Fatal(err)
	}
	e := sample()
	if post(r, e, func(req *http.Request) { req.Header.Del("X-Mirror-Signature") }) != 401 {
		t.Fatal("unsigned write accepted")
	}
	old := e
	old.SentAt = time.Now().Add(-2 * time.Minute)
	if post(r, old, nil) != 401 {
		t.Fatal("stale signature accepted")
	}
	if code := post(r, e, nil); code != 204 {
		t.Fatal(code)
	}
	received := r.View(time.Now()).ReceivedAt
	if post(r, e, nil) != 204 || !r.View(time.Now()).ReceivedAt.Equal(received) {
		t.Fatal("retry must be idempotent without refreshing liveness")
	}
	e.Sequence = 2
	e.State.ProbesTotal = 5
	if post(r, e, nil) != 204 {
		t.Fatal("new snapshot rejected")
	}
	e.Sequence = 1
	if post(r, e, nil) != 409 {
		t.Fatal("out-of-order snapshot accepted")
	}
	r, err = NewReceiver(path, testKey)
	if err != nil {
		t.Fatal(err)
	}
	if r.View(time.Now()).Snapshot.State.ProbesTotal != 5 || post(r, e, nil) != 409 {
		t.Fatal("restart lost state/order")
	}
	e.Instance = strings.Repeat("b", 32)
	e.StartedAt = e.StartedAt.Add(time.Second)
	if post(r, e, nil) != 204 {
		t.Fatal("new plugin instance rejected")
	}
	e.Instance = strings.Repeat("a", 32)
	e.StartedAt = e.StartedAt.Add(-time.Second)
	e.Sequence = 100
	if post(r, e, nil) != 409 {
		t.Fatal("retired instance overwrote new instance")
	}
}

func TestReceiverRejectsUnknownFieldsAndOversize(t *testing.T) {
	r, _ := NewReceiver("", testKey)
	e := sample()
	body, _ := json.Marshal(e)
	body = bytes.Replace(body, []byte(`"state":{`), []byte(`"state":{"token":"SECRET",`), 1)
	stamp := e.SentAt.Format(time.RFC3339Nano)
	req := httptest.NewRequest("POST", "/ingest", bytes.NewReader(body))
	req.Header.Set("X-Mirror-Timestamp", stamp)
	req.Header.Set("X-Mirror-Signature", Signature(testKey, stamp, body))
	w := httptest.NewRecorder()
	r.Ingest(w, req)
	if w.Code != 400 || r.View(time.Now()).Snapshot != nil {
		t.Fatal("unknown fields must never be stored/published")
	}
	w = httptest.NewRecorder()
	r.Ingest(w, httptest.NewRequest("POST", "/ingest", strings.NewReader(strings.Repeat("x", MaxBody+1))))
	if w.Code != 413 {
		t.Fatal("unbounded input")
	}
	w = httptest.NewRecorder()
	r.Ingest(w, httptest.NewRequest("GET", "/ingest", nil))
	if w.Code != 405 {
		t.Fatal("GET ingest accepted")
	}
	e.State.Status = "SECRET"
	if post(r, e, nil) != 400 {
		t.Fatal("arbitrary status text accepted")
	}
	e = sample()
	e.State.Models = []Model{{Name: "gpt-6-astra", Activity: "manual", Detection: true, Evidence: "none", Active: &Ticket{Source: "http://secret.invalid", Length: 292}}}
	if post(r, e, nil) != 400 {
		t.Fatal("arbitrary source accepted")
	}
}

func TestReceiverSSEInitialUpdateReconnect(t *testing.T) {
	r, _ := NewReceiver("", testKey)
	server := httptest.NewServer(http.HandlerFunc(r.Events))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	read := func(scanner *bufio.Scanner) View {
		t.Helper()
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				var v View
				if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &v) != nil {
					t.Fatal("bad SSE JSON")
				}
				return v
			}
		}
		t.Fatal("missing SSE state")
		return View{}
	}
	scanner := bufio.NewScanner(res.Body)
	if read(scanner).Snapshot != nil {
		t.Fatal("initial empty state missing")
	}
	e := sample()
	if post(r, e, nil) != 204 {
		t.Fatal("ingest failed")
	}
	if read(scanner).Snapshot.Sequence != 1 {
		t.Fatal("SSE update missing")
	}
	cancel()
	res.Body.Close()
	client := &http.Client{Timeout: time.Second}
	again, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Body.Close()
	if read(bufio.NewScanner(again.Body)).Snapshot.Sequence != 1 {
		t.Fatal("reconnect must start with full snapshot")
	}
}

func TestReceiverStorageFailureDoesNotPublish(t *testing.T) {
	r, _ := NewReceiver("", testKey)
	r.path = t.TempDir() // replacing a directory must fail
	if post(r, sample(), nil) != 503 || r.View(time.Now()).Snapshot != nil {
		t.Fatal("unpersisted update was acknowledged or published")
	}
}
