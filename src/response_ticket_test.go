package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestResponseTicketInjectionAudit(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, length := range []int{0, 292, 312, 332} {
			t.Run(strings.Join([]string{map[bool]string{false: "http", true: "stream"}[stream], strconv.Itoa(length)}, "/"), func(t *testing.T) {
				e := accountTestEngine(t)
				e.rejectDegraded = false
				ticket := synthStateToken(time.Now())
				e.storeValue(accountTarget("fixture-A"), ticket, "probe", "", e.cfg.Config)
				scopedRequest(t, "fixture-A", strings.Repeat("q", 356))
				value := strings.Repeat("x", length)
				if length == len(ticket) {
					value = ticket
				}
				payload := map[string]any{"RequestID": "account-request", "Model": "gpt-6-astra", "ResponseHeaders": http.Header{turnStateHeader: {value}}, "Body": []byte(`{"model":"gpt-6-astra","object":"response"}`), "ChunkIndex": -1}
				call := func() responseInterceptOutput {
					raw, _ := json.Marshal(payload)
					var out responseInterceptOutput
					var err error
					if stream {
						out, err = interceptStreamChunk(raw)
					} else {
						out, err = interceptNonStreamingResponse(raw)
					}
					if err != nil {
						t.Fatal(err)
					}
					return out
				}
				out := call()
				want := 0
				if length == 312 {
					want = len(ticket)
				}
				if len(out.Headers.Get(turnStateHeader)) != want {
					t.Fatal("unexpected actual response injection")
				}
				if stream && want > 0 {
					payload["ResponseHeaders"] = http.Header{turnStateHeader: {ticket}}
					payload["ChunkIndex"] = 0
					call() // A repeated repaired header must not erase original 312-byte evidence.
				}
				r := history.snapshot()["records"].([]auditRecord)[0]
				if r.TurnStateOriginalLength == nil || *r.TurnStateOriginalLength != 356 || r.TurnStateInjectedLength != len(ticket) {
					t.Fatal("response overwrote request audit")
				}
				if length == 0 {
					if r.TurnStateResponseOriginalLength != nil {
						t.Fatal("invented missing response evidence")
					}
				} else if r.TurnStateResponseOriginalLength == nil || *r.TurnStateResponseOriginalLength != length || r.TurnStateResponseInjectedLength != want {
					t.Fatal("incorrect response audit")
				}
				if length > 0 {
					raw, _ := json.Marshal(collectState())
					var state persistedState
					if json.Unmarshal(raw, &state) != nil || state.Records[0].TurnStateResponseOriginalLength == nil || *state.Records[0].TurnStateResponseOriginalLength != length || state.Records[0].TurnStateResponseInjectedLength != want {
						t.Fatal("response audit missing from persisted snapshot")
					}
				}
				scopedRequest(t, "fixture-A", "")
				r = history.snapshot()["records"].([]auditRecord)[0]
				if r.TurnStateResponseOriginalLength != nil || r.TurnStateResponseInjectedLength != 0 {
					t.Fatal("retry retained earlier response audit")
				}
			})
		}
	}
}

func TestResponseTicketAuditRejectsOtherAccountAndLegacyInference(t *testing.T) {
	e := accountTestEngine(t)
	ticket := synthStateToken(time.Now())
	e.storeValue(accountTarget("fixture-A"), ticket, "probe", "", e.cfg.Config)
	scopedRequest(t, "fixture-A", "")
	history.observeResponseTicket("account-request", "wrong-scope", strings.Repeat("x", 312), http.Header{turnStateHeader: {ticket}})
	r := history.snapshot()["records"].([]auditRecord)[0]
	if r.TurnStateResponseOriginalLength != nil {
		t.Fatal("cross-account response audit")
	}
	var legacy auditRecord
	if err := json.Unmarshal([]byte(`{"turn_state_source":"stream","turn_state_length":312,"turn_state_injected_length":292}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.TurnStateResponseOriginalLength != nil || legacy.TurnStateResponseInjectedLength != 0 {
		t.Fatal("invented legacy response injection")
	}
}
