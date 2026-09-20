package statemirror

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type savedState struct {
	ReceivedAt time.Time `json:"received_at"`
	Event      Event     `json:"event"`
}

type Receiver struct {
	mu      sync.Mutex
	key     []byte
	path    string
	current *savedState
	clients map[chan struct{}]bool
}

func NewReceiver(path string, key []byte) (*Receiver, error) {
	if len(key) < 32 {
		return nil, errors.New("mirror key too short")
	}
	r := &Receiver{key: append([]byte(nil), key...), path: path, clients: map[chan struct{}]bool{}}
	if path != "" {
		raw, err := os.ReadFile(path)
		if err == nil {
			var saved savedState
			if len(raw) > MaxBody || Decode(raw, &saved) != nil || saved.Event.Validate() != nil || saved.ReceivedAt.IsZero() {
				return nil, errors.New("invalid stored mirror state")
			}
			r.current = &saved
		} else if !os.IsNotExist(err) {
			return nil, errors.New("cannot read stored mirror state")
		}
	}
	return r, nil
}

func (r *Receiver) viewLocked(now time.Time) View {
	v := View{ServerTime: now, StaleAfterSeconds: int(StaleAfter / time.Second)}
	if r.current != nil {
		v.ReceivedAt = r.current.ReceivedAt
		event := r.current.Event
		v.Snapshot = &event
	}
	return v
}

func (r *Receiver) View(now time.Time) View {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.viewLocked(now)
}

// Ingest belongs on a separate private listener. The public handler does not
// register it, even for authenticated callers.
func (r *Receiver) Ingest(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if req.URL.Path != "/ingest" {
		http.NotFound(w, req)
		return
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", 405)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, MaxBody))
	if err != nil {
		http.Error(w, "invalid body", 413)
		return
	}
	now := time.Now().UTC()
	stamp := req.Header.Get("X-Mirror-Timestamp")
	if !Verify(r.key, stamp, req.Header.Get("X-Mirror-Signature"), body, now) {
		http.Error(w, "unauthorized", 401)
		return
	}
	var event Event
	if Decode(body, &event) != nil || event.Validate() != nil || event.SentAt.Format(time.RFC3339Nano) != stamp {
		http.Error(w, "invalid snapshot", 400)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current != nil {
		old := r.current.Event
		if event.Instance == old.Instance {
			if !event.StartedAt.Equal(old.StartedAt) || event.Sequence < old.Sequence {
				http.Error(w, "old snapshot", 409)
				return
			}
			if event.Sequence == old.Sequence {
				a, _ := json.Marshal(event)
				b, _ := json.Marshal(old)
				if string(a) != string(b) {
					http.Error(w, "sequence conflict", 409)
					return
				}
				// A retry after a lost ACK is idempotent; never extend freshness.
				w.WriteHeader(http.StatusNoContent)
				return
			}
		} else if !event.StartedAt.After(old.StartedAt) {
			http.Error(w, "old instance", 409)
			return
		}
		if event.SentAt.Before(old.SentAt) {
			http.Error(w, "old timestamp", 409)
			return
		}
	}
	saved := &savedState{ReceivedAt: now, Event: event}
	if r.path != "" && saveMirror(r.path, saved) != nil {
		http.Error(w, "storage unavailable", 503)
		return
	}
	r.current = saved
	for ch := range r.clients {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func saveMirror(path string, saved *savedState) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".mirror-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		err = json.NewEncoder(f).Encode(saved)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}

func (r *Receiver) JSON(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(r.View(time.Now().UTC()))
}

func (r *Receiver) Events(w http.ResponseWriter, req *http.Request) {
	if _, ok := w.(http.Flusher); !ok {
		http.Error(w, "streaming unavailable", 500)
		return
	}
	ch := make(chan struct{}, 1)
	r.mu.Lock()
	if len(r.clients) >= 128 {
		r.mu.Unlock()
		http.Error(w, "too many viewers", 503)
		return
	}
	r.clients[ch] = true
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.clients, ch); r.mu.Unlock() }()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	send := func() error {
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		body, err := json.Marshal(r.View(time.Now().UTC()))
		if err != nil {
			return err
		}
		if _, err = fmt.Fprintf(w, "event: state\ndata: %s\n\n", body); err != nil {
			return err
		}
		return controller.Flush()
	}
	if send() != nil {
		return
	}
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-req.Context().Done():
			return
		case <-ch:
			if send() != nil {
				return
			}
		case <-tick.C:
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			if controller.Flush() != nil {
				return
			}
		}
	}
}
