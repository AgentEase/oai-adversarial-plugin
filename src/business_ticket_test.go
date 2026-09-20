package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func businessResponse(t *testing.T, stream bool, chunk int, state, model string) responseInterceptOutput {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"object": "response", "model": model})
	raw, _ := json.Marshal(map[string]any{
		"RequestID": "account-request", "Model": "gpt-6-astra", "ChunkIndex": chunk,
		"ResponseHeaders": http.Header{"x-codex-turn-state": {state}}, "Body": body,
	})
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

func businessRecord() auditRecord {
	history.mu.Lock()
	defer history.mu.Unlock()
	return history.records[len(history.records)-1]
}

func TestBusinessResponseTicketDecisions(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, decision := range []string{"updated-active", "updated-candidate", "same-active", "same-candidate", "older", "expired", "invalid-length", "model-mismatch", "missing", "awaiting-model"} {
			t.Run(map[bool]string{false: "http", true: "stream"}[stream]+"/"+decision, func(t *testing.T) {
				e := accountTestEngine(t)
				e.rejectDegraded = false
				target := accountTarget("fixture-A")
				now := time.Now()
				active := synthStateToken(now.Add(-10 * time.Minute))
				candidate := synthStateToken(now.Add(-2 * time.Minute))
				value := synthStateToken(now.Add(-time.Minute))
				model := "gpt-6-astra"
				if decision != "updated-active" {
					e.storeValue(target, active, "probe", "", e.cfg.Config)
				}
				switch decision {
				case "same-active":
					value = active
				case "same-candidate":
					e.storeValue(target, candidate, "probe", "", e.cfg.Config)
					value = candidate
				case "older":
					e.storeValue(target, candidate, "probe", "", e.cfg.Config)
					value = synthStateToken(now.Add(-5 * time.Minute))
				case "expired":
					value = synthStateToken(now.Add(-2 * e.cfg.Config.TTL))
				case "invalid-length":
					value = strings.Repeat("x", 312)
				case "model-mismatch":
					model = "gpt-5.6-luna"
				case "missing":
					value = ""
				case "awaiting-model":
					model = ""
				}
				scopedRequest(t, "fixture-A", "")
				if decision == "expired" || decision == "awaiting-model" {
					e.noteBusinessDegradation(target, "synthetic prior observation")
				}
				beforeActive, beforeCandidate := e.values[target], e.candidates[target]
				if stream {
					businessResponse(t, true, -1, value, "")
					if e.values[target] != beforeActive || e.candidates[target] != beforeCandidate {
						t.Fatal("learned a stream ticket before observing its model")
					}
				}
				// Later chunks can omit the header; the original must survive.
				header := value
				if stream {
					header = ""
				}
				businessResponse(t, stream, 0, header, model)
				if got := businessRecord().TurnStateResponseStatus; got != decision {
					t.Fatalf("decision=%s, want %s", got, decision)
				}
				switch decision {
				case "updated-active":
					if e.values[target].Value != value || e.values[target].Source != "business" {
						t.Fatal("new response ticket did not initialize the active slot")
					}
				case "updated-candidate":
					if e.candidates[target].Value != value || e.candidates[target].Source != "business" || e.values[target] != beforeActive {
						t.Fatal("new response ticket did not update only the standby slot")
					}
				default:
					if e.values[target] != beforeActive || e.candidates[target] != beforeCandidate {
						t.Fatal("unchanged/rejected ticket replaced or renewed a slot")
					}
				}
				if e.activeValueFor(accountTarget("fixture-B")) != "" || len(e.candidates) > 1 || e.probesTotal != 0 {
					t.Fatal("business learning crossed accounts or started a probe")
				}
				if decision == "expired" || decision == "awaiting-model" {
					if _, ok := e.business[target]; !ok {
						t.Fatal("expired/unconfirmed ticket cleared a prior business anomaly")
					}
				}
				if stream {
					businessResponse(t, true, 1, "", "")
					if businessRecord().TurnStateResponseStatus != decision {
						t.Fatal("later empty chunk erased the learning result")
					}
				}
				raw, _ := json.Marshal(collectState())
				var saved persistedState
				if json.Unmarshal(raw, &saved) != nil || saved.Records[0].TurnStateResponseStatus != decision || saved.Records[0].responseTicket != "" {
					t.Fatal("snapshot lost the outcome or exposed in-flight evidence")
				}
			})
		}
	}
}

func TestBusinessResponseRepairedHeaderAndRetry(t *testing.T) {
	e := accountTestEngine(t)
	e.rejectDegraded = false
	target := accountTarget("fixture-A")
	active := synthStateToken(time.Now().Add(-time.Minute))
	e.storeValue(target, active, "probe", "", e.cfg.Config)
	scopedRequest(t, "fixture-A", "")
	before := e.values[target]
	out := businessResponse(t, true, -1, strings.Repeat("x", 312), "")
	if headerValue(out.Headers, turnStateHeader) != active {
		t.Fatal("request protection/response repair changed")
	}
	businessResponse(t, true, 0, active, "gpt-6-astra")
	r := businessRecord()
	if r.TurnStateResponseStatus != "invalid-length" || r.TurnStateLength != 312 || *r.TurnStateResponseOriginalLength != 312 || e.values[target] != before || len(e.candidates) != 0 {
		t.Fatal("the plugin learned its own repaired header as an upstream ticket")
	}
	scopedRequest(t, "fixture-A", "")
	r = businessRecord()
	if r.TurnStateResponseStatus != "" || r.responseTicket != "" || r.responseModel != "" {
		t.Fatal("retry retained previous attempt evidence")
	}
	fresh := synthStateToken(time.Now())
	businessResponse(t, true, -1, fresh, "")
	if businessRecord().TurnStateResponseStatus != "awaiting-model" || len(e.candidates) != 0 {
		t.Fatal("retry learned using the previous attempt's model")
	}
	businessResponse(t, true, 0, "", "gpt-6-astra")
	if businessRecord().TurnStateResponseStatus != "updated-candidate" || e.candidates[target].Value != fresh {
		t.Fatal("retry failed to learn its own new response ticket")
	}
}

func TestBusinessResponseRequiresRecordedAccount(t *testing.T) {
	e := accountTestEngine(t)
	scopedRequest(t, "fixture-A", "")
	ticket := synthStateToken(time.Now())
	history.observeResponseBusiness("account-request", "wrong-scope", ticket, "gpt-6-astra")
	history.observeResponseBusiness("unknown-request", responseAccount("account-request", ""), ticket, "gpt-6-astra")
	if businessRecord().TurnStateResponseStatus != "" || len(e.values) != 0 {
		t.Fatal("accepted a response without the matching request account")
	}
}

func TestHeaderValueFromABIUsesCaseInsensitiveLookup(t *testing.T) {
	for _, key := range []string{"X-Codex-Turn-State", "x-codex-turn-state", "X-CODEX-TURN-STATE"} {
		if headerValue(http.Header{key: {" synthetic "}}, turnStateHeader) != "synthetic" {
			t.Fatal("missed a differently cased ABI header")
		}
	}
	if headerValue(http.Header{turnStateHeader: {""}, "x-codex-turn-state": {"old"}}, turnStateHeader) != "" {
		t.Fatal("resurrected a header explicitly cleared by the plugin")
	}
}

func TestBusinessWebSocketReportsHeaderVisibilityLimit(t *testing.T) {
	e := accountTestEngine(t)
	scopedRequest(t, "fixture-A", "")
	raw, _ := json.Marshal(webSocketResponseEvent{RequestID: "account-request", AuthID: "fixture-A", Model: "gpt-6-astra", Payload: []byte(`{"object":"response","model":"gpt-6-astra"}`)})
	if _, err := observeWebSocketEvent(raw); err != nil {
		t.Fatal(err)
	}
	if businessRecord().TurnStateResponseStatus != "headers-unavailable" || len(e.values) != 0 {
		t.Fatal("WebSocket event invented missing-header evidence or a learned ticket")
	}
	businessResponse(t, false, 0, synthStateToken(time.Now()), "gpt-6-astra")
	observeWebSocketEvent(raw)
	if businessRecord().TurnStateResponseStatus != "updated-active" {
		t.Fatal("WebSocket visibility notice overwrote actual response evidence")
	}
}
