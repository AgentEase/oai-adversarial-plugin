package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestMissingRequestTicketRestoresHealthyBaseline(t *testing.T) {
	e := accountTestEngine(t)
	ticket := synthStateToken(time.Now())
	e.storeValue(accountTarget("fixture-A"), ticket, "probe", "", e.cfg.Config)
	for _, force := range []bool{false, true} {
		turnStateOverride.Store(&turnStateOverrideState{Config: turnStateOverrideConfig{Enabled: true, Force: force, Models: e.cfg.Config.Models}})
		for _, headers := range []http.Header{nil, {}, {turnStateHeader: {""}}, {turnStateHeader: {" \t "}}} {
			raw, _ := json.Marshal(interceptRequest{RequestID: "missing-ticket", ToFormat: "codex", Model: "gpt-6-astra",
				Metadata: map[string]any{"selected_auth_id": "fixture-A", "selected_auth_index": "index-A"}, Headers: headers, Body: []byte(`{"input":"fixture"}`)})
			out, err := intercept(raw)
			if err != nil || out.Terminate || len(out.Headers.Get(turnStateHeader)) == 0 {
				t.Fatal("missing request ticket did not receive the healthy baseline")
			}
			history.mu.Lock()
			r := history.records[len(history.records)-1]
			history.mu.Unlock()
			if r.TurnStateOriginalLength == nil || *r.TurnStateOriginalLength != 0 || r.TurnStateInjectedLength != len(ticket) || r.TurnStateOverride != "applied" {
				t.Fatal("missing request did not record the forced baseline overwrite")
			}
		}
	}
	// A retry that now has no ticket still receives the healthy baseline.
	scopedRequest(t, "fixture-A", "old")
	scopedRequest(t, "fixture-A", "")
	history.mu.Lock()
	defer history.mu.Unlock()
	for _, r := range history.records {
		if r.RequestID == "account-request" && (r.TurnStateInjectedLength != len(ticket) || r.TurnStateOverride != "applied") {
			t.Fatal("retry did not retain baseline overwrite metadata")
		}
	}
}
