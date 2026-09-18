package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestProbeUpstreamModel covers the transport shapes that carry the model the
// upstream actually served: non-streaming bodies, SSE events and websocket
// frames.
func TestProbeUpstreamModel(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		model   string
		ok      bool
	}{
		{"responses non-streaming", `{"id":"resp_1","object":"response","model":"gpt-6-luna","output":[]}`, "gpt-6-luna", true},
		{"chat completion", `{"id":"chatcmpl_1","object":"chat.completion","model":"gpt-6-astra","choices":[]}`, "gpt-6-astra", true},
		{"sse response.created", "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_2\",\"model\":\"gpt-6-luna\"}}\n\n", "gpt-6-luna", true},
		{"sse bare line", "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n", "gpt-6-astra", true},
		{"websocket frame", `{"type":"response.created","response":{"model":"gpt-6-luna","id":"resp_3"}}`, "gpt-6-luna", true},
		{"anthropic message_start", `{"type":"message_start","message":{"id":"msg_1","model":"claude-sonnet-4"}}`, "claude-sonnet-4", true},
		{"no model key", `{"type":"response.in_progress"}`, "", false},
		{"model key without identity", `{"type":"ping","model":"gpt-6-luna"}`, "gpt-6-luna", true},
		{"empty", ``, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model, ok := probeUpstreamModel([]byte(tc.payload))
			if ok != tc.ok || model != tc.model {
				t.Fatalf("probeUpstreamModel(%q) = %q, %v; want %q, %v", tc.payload, model, ok, tc.model, tc.ok)
			}
		})
	}
}

func TestProbeUpstreamModelRejectsOversizePayload(t *testing.T) {
	oversize := append(bytes.Repeat([]byte(" "), maxModelProbeSize+1), []byte(`"model":"x"`)...)
	if _, ok := probeUpstreamModel(oversize); ok {
		t.Fatal("oversize payload must be ignored")
	}
}

// TestObserveModelAndTurnState drives the audit state directly: a request is
// recorded first (like the request interceptor does), then the response side
// attaches the observed model and turn-state.
func TestObserveModelAndTurnState(t *testing.T) {
	var state auditState
	state.record(auditRecord{RequestID: "req-1", Model: "gpt-5.6-luna", conversion: conversion{Target: targetTimezone, Action: "inserted"}})
	state.record(auditRecord{RequestID: "req-2", Model: "gpt-6-astra", conversion: conversion{Target: targetTimezone, Action: "unchanged"}})

	state.observeModel("req-1", "", "gpt-6-luna")
	state.observeTurnState("req-1", "sample-state-0001...long", "stream")
	state.observeModel("req-2", "", "gpt-6-astra")

	snapshot := state.snapshot()
	records := snapshot["records"].([]auditRecord)
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	first := records[0] // newest first: req-2, req-1
	if first.ModelMismatch || !first.ModelChecked || first.UpstreamModel != "gpt-6-astra" {
		t.Fatalf("req-2 must be consistent: %+v", first)
	}
	second := records[1]
	if !second.ModelChecked || !second.ModelMismatch || second.UpstreamModel != "gpt-6-luna" {
		t.Fatalf("req-1 must be flagged as mismatched: %+v", second)
	}
	if second.TurnStateLength != len("sample-state-0001...long") || second.TurnStateSource != "stream" {
		t.Fatalf("turn-state observation missing: %+v", second)
	}
	if second.TurnStateValue != "sample-state-0001...long" || second.TurnStateTruncated {
		t.Fatalf("turn-state full value missing: %+v", second)
	}
	if snapshot["mismatches"].(int) != 1 {
		t.Fatalf("mismatch counter wrong: %v", snapshot["mismatches"])
	}
}

func TestTurnStateValueIsBounded(t *testing.T) {
	var state auditState
	state.record(auditRecord{RequestID: "big", conversion: conversion{Target: targetTimezone, Action: "unchanged"}})
	long := strings.Repeat("A", turnStateValueLimit+100)
	state.observeTurnState("big", long, "stream")
	record := state.snapshot()["records"].([]auditRecord)[0]
	if record.TurnStateLength != len(long) || len(record.TurnStateValue) != turnStateValueLimit || !record.TurnStateTruncated {
		t.Fatalf("bounded value wrong: len=%d truncated=%v", len(record.TurnStateValue), record.TurnStateTruncated)
	}
}

func TestObserveIgnoresUnknownRequests(t *testing.T) {
	var state auditState
	state.observeModel("missing", "", "gpt-6-luna")
	state.observeTurnState("missing", "value", "response")
	if snapshot := state.snapshot(); len(snapshot["records"].([]auditRecord)) != 0 {
		t.Fatal("observers must not create records")
	}
}

// TestInterceptRequestRecordsTurnState verifies the request-side header view is
// captured when the client provides X-Codex-Turn-State.
func TestInterceptRequestRecordsTurnState(t *testing.T) {
	history = auditState{}
	raw, _ := json.Marshal(interceptRequest{
		RequestID: "req-ts", ToFormat: "codex", Model: "gpt-5.6-luna",
		Headers: http.Header{turnStateHeader: {"sample-state-0001"}},
		Body:    []byte(`{"input":"hello"}`),
	})
	if _, err := intercept(raw); err != nil {
		t.Fatal(err)
	}
	records := history.snapshot()["records"].([]auditRecord)
	if len(records) != 1 || records[0].TurnStateLength != len("sample-state-0001") || records[0].TurnStateSource != "request" {
		t.Fatalf("request-side turn-state not recorded: %+v", records)
	}
}

// TestResponseInterceptorsRecordObservations feeds synthetic wire payloads for
// the three response-side hooks and checks the attached observations.
func TestResponseInterceptorsRecordObservations(t *testing.T) {
	history = auditState{}
	history.record(auditRecord{RequestID: "req-ns", Model: "gpt-5.6-luna", conversion: conversion{Target: targetTimezone, Action: "replaced"}})
	history.record(auditRecord{RequestID: "req-stream", Model: "gpt-5.6-luna", conversion: conversion{Target: targetTimezone, Action: "inserted"}})

	nonStream, _ := json.Marshal(responseInterceptRequest{
		RequestID:       "req-ns",
		Body:            []byte(`{"id":"resp_1","object":"response","model":"gpt-6-luna"}`),
		ResponseHeaders: http.Header{turnStateHeader: {"0123456789"}},
	})
	out, err := interceptNonStreamingResponse(nonStream)
	if err != nil || out.Headers != nil || out.Body != nil {
		t.Fatalf("observation must not modify the response: %+v %v", out, err)
	}

	headerInit, _ := json.Marshal(streamChunkInterceptRequest{RequestID: "req-stream", ChunkIndex: streamChunkHeaderInitIndex})
	if _, err := interceptStreamChunk(headerInit); err != nil {
		t.Fatal(err)
	}
	chunk, _ := json.Marshal(streamChunkInterceptRequest{
		RequestID: "req-stream", ChunkIndex: 0,
		Body: []byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n"),
	})
	if _, err := interceptStreamChunk(chunk); err != nil {
		t.Fatal(err)
	}

	event, _ := json.Marshal(webSocketResponseEvent{RequestID: "req-ns", Payload: []byte(`{"type":"response.completed","response":{"model":"gpt-6-luna"}}`)})
	if _, err := observeWebSocketEvent(event); err != nil {
		t.Fatal(err)
	}

	snapshot := history.snapshot()
	records := snapshot["records"].([]auditRecord)
	byID := map[string]auditRecord{}
	for _, record := range records {
		byID[record.RequestID] = record
	}
	if record := byID["req-ns"]; !record.ModelMismatch || record.UpstreamModel != "gpt-6-luna" || record.TurnStateLength != 10 {
		t.Fatalf("non-streaming observation wrong: %+v", record)
	}
	if record := byID["req-stream"]; !record.ModelMismatch || record.UpstreamModel != "gpt-6-astra" || record.ModelChecked != true {
		t.Fatalf("stream observation wrong: %+v", record)
	}
	if snapshot["mismatches"].(int) != 2 {
		t.Fatalf("mismatch counter wrong: %v", snapshot["mismatches"])
	}
}

// TestProbeModelConsistent drives the model-consistency check used to accept
// or reject a captured state.
func TestProbeModelConsistent(t *testing.T) {
	cases := []struct {
		requested string
		observed  string
		ok        bool
	}{
		{"gpt-6-astra", "gpt-6-astra", true},
		{"gpt-6-astra", "GPT-6-ASTRA", true},
		{"gpt-6-astra", "gpt-6-astra-preview", true},
		{"gpt-6-astra", "gpt-5.6-luna", false},
		{"gpt-5.6-sol", "gpt-5.6-sol", true},
		{"gpt-5.6-sol", "gpt-6-astra", false},
		{"", "gpt-6-astra", false},
		{"gpt-6-astra", "", false},
		{"gpt-6-astra", " gpt-6-astra ", true},
	}
	for _, tc := range cases {
		if got := probeModelConsistent(tc.requested, tc.observed); got != tc.ok {
			t.Errorf("probeModelConsistent(%q, %q) = %v; want %v", tc.requested, tc.observed, got, tc.ok)
		}
	}
}

// TestProbeRecordCarriesObservedModel verifies the audit record shape used by
// the dashboard for the consistency display.
func TestProbeRecordCarriesObservedModel(t *testing.T) {
	record := probeRecord{Model: "gpt-6-astra", ObservedModel: "gpt-6-astra", Success: true, StateLength: 292}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"observed_model":"gpt-6-astra"`, `"state_length":292`} {
		if !bytes.Contains(encoded, []byte(key)) {
			t.Fatalf("record missing %s: %s", key, encoded)
		}
	}
}

// TestProbeFailureShape verifies the failure annotation payload structure used
// by the dashboard when retries are exhausted.
func TestProbeFailureShape(t *testing.T) {
	failure := probeFailure{Model: "gpt-6-astra", Attempts: 30, Rounds: 10,
		LastError: "state length 312 != 292 (suspected degraded)", LastLength: 312,
		FailedAt: "2026-09-18T02:20:00Z", CooldownUntil: "2026-09-18T02:40:00Z"}
	encoded, err := json.Marshal(failure)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"attempts":30`, `"rounds":10`, `"last_length":312`, `"last_error"`, `"cooldown_until":"2026-09-18T02:40:00Z"`} {
		if !bytes.Contains(encoded, []byte(key)) {
			t.Fatalf("failure payload missing %s: %s", key, encoded)
		}
	}
	// Success clears the annotation (engine semantics contract).
	engine := &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{}}
	engine.failures["gpt-6-astra"] = failure
	delete(engine.failures, "gpt-6-astra")
	if _, ok := engine.failures["gpt-6-astra"]; ok {
		t.Fatal("failure must be removable on success")
	}
}

// TestProbePauseAndResume verifies the dashboard pause/resume controls: a
// paused model is excluded from the automatic queue while its value and
// annotation are preserved; resume clears the annotation and re-enters it.
func TestProbePauseAndResume(t *testing.T) {
	state := &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	state.cfg.Config = parseProbeConfig(probeConfigYAML{})
	if state.probeSuppressed("gpt-6-astra") {
		t.Fatal("fresh model must not be suppressed")
	}
	state.failures["gpt-6-astra"] = probeFailure{Model: "gpt-6-astra", Attempts: 30, Rounds: 10,
		LastError: "state length 312 != 292 (suspected degraded)"}
	state.setModelPaused("gpt-6-astra", true)
	if !state.probeSuppressed("gpt-6-astra") {
		t.Fatal("paused model must be suppressed")
	}
	if _, ok := state.failures["gpt-6-astra"]; !ok {
		t.Fatal("pause must keep the annotation")
	}
	if models := state.pausedModels(); len(models) != 1 || models[0] != "gpt-6-astra" {
		t.Fatalf("paused list wrong: %v", models)
	}
	state.setModelPaused("gpt-6-astra", false)
	if state.probeSuppressed("gpt-6-astra") {
		t.Fatal("resumed model must re-enter the queue")
	}
	if _, ok := state.failures["gpt-6-astra"]; ok {
		t.Fatal("resume must clear the stale annotation")
	}
}

// TestDegradedRejectDecision drives the degraded-model rejection switch:
// only length anomalies and model mismatches count as degradation, and only
// while the switch is on. In-round suspicions crossing suspect-threshold are
// rejection-eligible before the round completes.
func TestDegradedRejectDecision(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		suspects: map[string]probeSuspicion{}, paused: map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{})
	if message := degradedRejectMessage("gpt-6-astra"); message != "" {
		t.Fatalf("switch off must allow everything: %q", message)
	}
	probeTrack.failures["gpt-6-astra"] = probeFailure{LastError: "state length 312 != 292 (suspected degraded)"}
	if message := degradedRejectMessage("gpt-6-astra"); message != "" {
		t.Fatalf("switch off must allow even degraded: %q", message)
	}
	probeTrack.setRejectDegraded(true)
	if message := degradedRejectMessage("gpt-6-astra"); !strings.Contains(message, "风控降智") {
		t.Fatalf("length anomaly must be rejected with a Chinese message: %q", message)
	}
	if message := degradedRejectMessage("gpt-6-luna"); message != "" {
		t.Fatalf("unrelated model must pass: %q", message)
	}
	probeTrack.failures["gpt-5.6-luna"] = probeFailure{LastError: "model mismatch: requested gpt-5.6-luna got gpt-6-astra"}
	if message := degradedRejectMessage("gpt-5.6-luna"); !strings.Contains(message, "模型不一致") {
		t.Fatalf("model mismatch must be rejected: %q", message)
	}
	probeTrack.failures["gpt-5.6-sol"] = probeFailure{LastError: "status 429: rate limit exceeded"}
	if message := degradedRejectMessage("gpt-5.6-sol"); message != "" {
		t.Fatalf("rate limit must not count as degradation: %q", message)
	}
	// Early suspicion: below the threshold passes, at the threshold rejects.
	delete(probeTrack.failures, "gpt-5.6-terra")
	probeTrack.suspects["gpt-5.6-terra"] = probeSuspicion{Model: "gpt-5.6-terra", Failures: 2,
		LastError: "state length 312 != 292 (suspected degraded)"}
	if message := degradedRejectMessage("gpt-5.6-terra"); message != "" {
		t.Fatalf("below threshold must pass: %q", message)
	}
	probeTrack.suspects["gpt-5.6-terra"] = probeSuspicion{Model: "gpt-5.6-terra", Failures: 3,
		LastError: "state length 312 != 292 (suspected degraded)"}
	if message := degradedRejectMessage("gpt-5.6-terra"); !strings.Contains(message, "尚未达到正式判定") {
		t.Fatalf("threshold-crossing suspicion must be rejected: %q", message)
	}
}

// TestSuspectTracking drives the in-round suspicion counter: only degradation
// evidence counts, the count survives below-threshold failures, success
// clears it, and an exhausted round promotes it to the full annotation.
func TestSuspectTracking(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		suspects: map[string]probeSuspicion{}, paused: map[string]bool{}, probing: map[string]bool{}}
	cfg := parseProbeConfig(probeConfigYAML{})
	probeTrack.noteProbeFailure("gpt-6-astra", probeRecord{Error: "status 429: rate limit exceeded"}, cfg)
	if len(probeTrack.suspects) != 0 {
		t.Fatal("rate limit must not start a suspicion")
	}
	probeTrack.noteProbeFailure("gpt-6-astra", probeRecord{Error: "state length 312 != 292 (suspected degraded)"}, cfg)
	if probeTrack.suspects["gpt-6-astra"].Failures != 1 {
		t.Fatalf("first failure must count: %+v", probeTrack.suspects)
	}
	probeTrack.noteProbeFailure("gpt-6-astra", probeRecord{Error: "model mismatch: requested x got y"}, cfg)
	if probeTrack.suspects["gpt-6-astra"].Failures != 2 {
		t.Fatalf("count must continue below the threshold: %+v", probeTrack.suspects)
	}
	probeTrack.noteProbeFailure("gpt-6-astra", probeRecord{Error: "state length 312 != 292 (suspected degraded)"}, cfg)
	if probeTrack.suspects["gpt-6-astra"].Failures != 3 {
		t.Fatalf("third failure must reach the threshold: %+v", probeTrack.suspects)
	}
	probeTrack.clearSuspect("gpt-6-astra")
	if _, ok := probeTrack.suspects["gpt-6-astra"]; ok {
		t.Fatal("success must clear the suspicion")
	}
}

// TestProbeControlEndpoint drives the POST control payloads the dashboard
// sends: pause, resume and the reject switch.
func TestProbeControlEndpoint(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	response, err := probeControl([]byte(`{"model":"gpt-5.6-luna","action":"pause"}`))
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("pause failed: %+v %v", response, err)
	}
	if !probeTrack.paused["gpt-5.6-luna"] {
		t.Fatal("pause must mark the model")
	}
	response, err = probeControl([]byte(`{"model":"gpt-5.6-luna","action":"resume"}`))
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("resume failed: %+v %v", response, err)
	}
	if probeTrack.paused["gpt-5.6-luna"] {
		t.Fatal("resume must clear the mark")
	}
	response, err = probeControl([]byte(`{"action":"reject-degraded","enabled":true}`))
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("reject-degraded failed: %+v %v", response, err)
	}
	if !probeTrack.rejectDegradedEnabled() {
		t.Fatal("switch must be on")
	}
	response, _ = probeControl([]byte(`{"action":"bogus"}`))
	if response.StatusCode != 400 {
		t.Fatalf("unknown action must be rejected: %+v", response)
	}
	response, _ = probeControl([]byte(`{"action":"pause"}`))
	if response.StatusCode != 400 {
		t.Fatalf("pause without a model must be rejected: %+v", response)
	}
}

// TestInterceptDegradedRejection covers the full rejection path: with the
// switch on and a degradation-annotated failure present, intercept() must
// terminate the request with a 403 and a Chinese JSON message, and record
// the rejection without attempting timezone normalization. The rejection
// record must carry empty arrays (never nil) for original/paths so that the
// dashboard JSON never contains null for them.
func TestInterceptDegradedRejection(t *testing.T) {
	history = auditState{}
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	probeTrack.failures["gpt-6-astra"] = probeFailure{Model: "gpt-6-astra", Attempts: 30, Rounds: 10,
		LastError: "state length 312 != 292 (suspected degraded)"}
	probeTrack.setRejectDegraded(true)
	raw, _ := json.Marshal(interceptRequest{
		RequestID: "req-reject", ToFormat: "codex", Model: "gpt-6-astra",
		Body: []byte(`{"input":"hello"}`),
	})
	resp, err := intercept(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Terminate || resp.StatusCode != 403 {
		t.Fatalf("degraded model must be terminated with 403: %+v", resp)
	}
	if !strings.Contains(string(resp.ResponseBody), "风控降智") {
		t.Fatalf("rejection body must be Chinese: %s", resp.ResponseBody)
	}
	record := history.snapshot()["records"].([]auditRecord)[0]
	if !record.DegradedRejected {
		t.Fatalf("rejection must be recorded: %+v", record)
	}
	if record.Original == nil || record.Paths == nil {
		t.Fatalf("rejection record must keep empty arrays, not nil: %+v", record)
	}
	encoded, _ := json.Marshal(record)
	if bytes.Contains(encoded, []byte(`"original":null`)) || bytes.Contains(encoded, []byte(`"paths":null`)) {
		t.Fatalf("rejection record must not marshal null arrays: %s", encoded)
	}
	// With the switch off the same request passes through to normalization.
	probeTrack.setRejectDegraded(false)
	history = auditState{}
	resp, err = intercept(raw)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Terminate {
		t.Fatalf("switch off must not terminate: %+v", resp)
	}
}

// TestPersistenceRoundTrip saves a snapshot and restores it into a fresh
// engine + audit state, covering values, failures, suspicions, paused models,
// the rejection switch, counters, probe history and audit records.
func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(dir, "state.json"))

	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		suspects: map[string]probeSuspicion{}, paused: map[string]bool{}, probing: map[string]bool{}}
	cfg := parseProbeConfig(probeConfigYAML{})
	probeTrack.cfg.Config = cfg
	probeTrack.setRejectDegraded(false)
	probeTrack.setModelPaused("gpt-5.6-terra", true)
	probeTrack.storeValue("gpt-6-astra", "sample-state-0001", "probe", "direct", cfg)
	probeTrack.noteError("state length 312 != 292 (suspected degraded)")
	probeTrack.noteProbeFailure("gpt-5.6-luna", probeRecord{Error: "state length 312 != 292 (suspected degraded)"}, cfg)
	probeTrack.failures["gpt-5.6-sol"] = probeFailure{Model: "gpt-5.6-sol", Attempts: 30, Rounds: 10,
		LastError: "state length 312 != 292 (suspected degraded)", FailedAt: "2026-09-18T02:20:00Z"}
	probeTrack.appendRecord(probeRecord{Time: "2026-09-18T02:20:00Z", Model: "gpt-6-astra", Success: true})
	history = auditState{}
	history.record(auditRecord{RequestID: "persist-1", Model: "gpt-6-astra",
		conversion: conversion{Target: targetTimezone, Action: "inserted"},
		Time:       "2026-09-18T02:21:00Z"})
	savePersistedState()

	// Simulate a fresh plugin instance: wipe everything.
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		suspects: map[string]probeSuspicion{}, paused: map[string]bool{}, probing: map[string]bool{},
		rejectDegraded: true}
	probeTrack.cfg.Config = cfg
	history = auditState{}
	loadPersistedState()

	if probeTrack.rejectDegradedEnabled() {
		t.Fatal("switch must restore the saved off state")
	}
	if !probeTrack.paused["gpt-5.6-terra"] {
		t.Fatal("paused model must be restored")
	}
	if entry, ok := probeTrack.values["gpt-6-astra"]; !ok || entry.Value != "sample-state-0001" {
		t.Fatalf("value must be restored: %+v", entry)
	}
	if _, ok := probeTrack.failures["gpt-5.6-sol"]; !ok {
		t.Fatal("failure annotation must be restored")
	}
	if suspicion, ok := probeTrack.suspects["gpt-5.6-luna"]; !ok || suspicion.Failures != 1 {
		t.Fatalf("suspicion must be restored: %+v", suspicion)
	}
	if probeTrack.probesTotal != 1 {
		t.Fatalf("probe counters must be restored: %d", probeTrack.probesTotal)
	}
	if len(probeTrack.history) != 1 {
		t.Fatalf("probe history must be restored: %d", len(probeTrack.history))
	}
	snapshot := history.snapshot()
	if snapshot["total"] != uint64(1) {
		t.Fatalf("audit total must be restored: %v", snapshot["total"])
	}
	records := snapshot["records"].([]auditRecord)
	if len(records) != 1 || records[0].RequestID != "persist-1" {
		t.Fatalf("audit records must be restored: %+v", records)
	}
}

// TestBusinessObservation drives the event-driven strategy: one unhealthy
// business observation marks the model degraded immediately; a healthy one
// stores the value with source "business" and clears the mark.
func TestBusinessObservation(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		suspects: map[string]probeSuspicion{}, business: map[string]businessDegradation{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{})
	probeTrack.cfg.Config.Enabled = false // observations must never start upstream traffic in tests

	// One length-anomaly observation marks degraded immediately.
	observeBusinessState("gpt-6-astra", "short-312", "gpt-6-astra")
	if _, ok := probeTrack.business["gpt-6-astra"]; !ok {
		t.Fatal("single unhealthy observation must mark the model")
	}
	probeTrack.setRejectDegraded(true)
	if message := degradedRejectMessage("gpt-6-astra"); !strings.Contains(message, "业务流量确认") {
		t.Fatalf("business mark must feed the rejection switch: %q", message)
	}

	// One model-mismatch observation (state empty) marks too.
	observeBusinessState("gpt-5.6-sol", "", "gpt-5.6-luna")
	if _, ok := probeTrack.business["gpt-5.6-sol"]; !ok {
		t.Fatal("model mismatch must mark the model")
	}

	// A healthy observation stores the value and clears the mark.
	healthy := synthBusinessState(len("gpt-6-astra"))
	observeBusinessState("gpt-6-astra", healthy, "gpt-6-astra")
	if _, ok := probeTrack.business["gpt-6-astra"]; ok {
		t.Fatal("healthy observation must clear the mark")
	}
	if entry, ok := probeTrack.values["gpt-6-astra"]; !ok || entry.Source != "business" || entry.Value != healthy {
		t.Fatalf("healthy observation must store the value: %+v", entry)
	}

	// Empty state with no mismatch is ignored.
	observeBusinessState("gpt-5.6-terra", "", "")
	if len(probeTrack.business) != 1 {
		t.Fatalf("no-signal observation must not mark: %+v", probeTrack.business)
	}
}

// synthBusinessState builds a deterministic value of the required length so
// the healthy business-observation path can be exercised without real tokens.
func synthBusinessState(seed int) string {
	length := probeRequiredStateLength
	var builder strings.Builder
	for i := 0; i < length; i++ {
		builder.WriteByte(byte('A' + (seed+i)%26))
	}
	return builder.String()
}

// TestManualRoundControl verifies the manual probing model: rounds only run
// when started explicitly, a disabled track notes the refusal, and stop is
// idempotent. The single configured model has no credential file, so the
// round fails fast without network I/O.
func TestManualRoundControl(t *testing.T) {
	enabled := true
	one := 1
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{
		Enabled:          &enabled,
		Models:           []string{"gpt-6-astra"},
		IntervalSeconds:  &one,
		AttemptsPerHop:   &one,
		MaxAttemptsRound: &one,
	})

	waitFinished := func() {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			probeTrack.mu.Lock()
			running := probeTrack.running
			probeTrack.mu.Unlock()
			if !running {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("round must finish")
	}

	// Disabled track: start records the refusal and finishes immediately.
	probeTrack.cfg.Config.Enabled = false
	probeTrack.start()
	waitFinished()
	probeTrack.mu.Lock()
	note := probeTrack.runNote
	probeTrack.mu.Unlock()
	if !strings.Contains(note, "未启用") {
		t.Fatalf("disabled track must note the refusal: %q", note)
	}

	// Enabled track: one round with a credential-less model fails fast and
	// records its finish time.
	probeTrack.cfg.Config.Enabled = true
	probeTrack.start()
	waitFinished()
	probeTrack.mu.Lock()
	finished := probeTrack.runFinishedAt
	probeTrack.mu.Unlock()
	if finished == "" {
		t.Fatal("round must record its finish time")
	}

	// stop must be safe even when nothing is running.
	probeTrack.stop()
	probeTrack.stop()
	probeTrack.mu.Lock()
	running := probeTrack.running
	probeTrack.mu.Unlock()
	if running {
		t.Fatal("stop must leave the engine idle")
	}
}

// TestSeedBaselinesFromAudit restores missing baseline values from the
// deployment seeds file and the newest healthy (292-byte, decodable) audit
// records - the "last recorded 292-byte value as the initial baseline" rule.
func TestSeedBaselinesFromAudit(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(dir, "state.json"))
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{})
	old := synthStateToken(time.Now().Add(-30 * time.Minute))
	newer := synthStateToken(time.Now().Add(-10 * time.Minute))
	fileSeed := synthStateToken(time.Now().Add(-20 * time.Minute))
	seeds, _ := json.Marshal(map[string]string{
		"gpt-5.6-sol":   fileSeed,
		"gpt-5.6-terra": strings.Repeat("A", 312), // degraded: rejected
	})
	if err := os.WriteFile(filepath.Join(dir, "seeds.json"), seeds, 0o600); err != nil {
		t.Fatal(err)
	}
	history = auditState{}
	history.record(auditRecord{RequestID: "seed-1", Model: "gpt-6-astra",
		TurnStateLength: len(old), TurnStateValue: old})
	history.record(auditRecord{RequestID: "seed-2", Model: "gpt-6-astra",
		TurnStateLength: len(newer), TurnStateValue: newer})
	probeTrack.seedBaselinesFromAudit()
	entry, ok := probeTrack.values["gpt-6-astra"]
	if !ok || entry.Value != newer || entry.Source != "seed" || entry.ValueLength != probeRequiredStateLength {
		t.Fatalf("seed must adopt the newest healthy audit value: %+v", entry)
	}
	if entry, ok := probeTrack.values["gpt-5.6-sol"]; !ok || entry.Value != fileSeed || entry.Source != "seed" {
		t.Fatalf("seeds file value must be adopted: %+v", entry)
	}
	if _, ok := probeTrack.values["gpt-5.6-terra"]; ok {
		t.Fatal("a degraded (312-byte) seed must never be drafted")
	}
	// Existing entries are never overwritten by the seed pass.
	probeTrack.values["gpt-5.6-luna"] = stateEntry{Model: "gpt-5.6-luna", Value: "busy", Valid: true}
	probeTrack.seedBaselinesFromAudit()
	if got := probeTrack.values["gpt-5.6-luna"].Value; got != "busy" {
		t.Fatalf("seed must not touch existing entries: %q", got)
	}
}

// TestRepairTurnStateHeader drives the "discard and backfill" rule: an
// unhealthy upstream value is replaced by the model's healthy baseline on the
// response path, while a healthy value passes through untouched.
func TestRepairTurnStateHeader(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{})
	healthy := synthStateToken(time.Now().Add(-5 * time.Minute))
	probeTrack.values["gpt-5.6-luna"] = stateEntry{Model: "gpt-5.6-luna", Value: healthy,
		ValueLength: len(healthy), Valid: true}

	// Length anomaly: replaced with the healthy baseline.
	headers := repairTurnStateHeader("gpt-5.6-luna", "", "gpt-5.6-luna", strings.Repeat("A", 312))
	if headers == nil || headers.Get(turnStateHeader) != healthy {
		t.Fatalf("degraded state must be backfilled with the baseline: %+v", headers)
	}
	// Model mismatch: replaced too.
	headers = repairTurnStateHeader("gpt-5.6-luna", "", "gpt-6-astra", strings.Repeat("A", 292))
	if headers == nil || headers.Get(turnStateHeader) != healthy {
		t.Fatalf("model mismatch must be backfilled: %+v", headers)
	}
	// Healthy and consistent: untouched.
	if headers := repairTurnStateHeader("gpt-5.6-luna", "", "gpt-5.6-luna", healthy); headers != nil {
		t.Fatalf("healthy state must pass through: %+v", headers)
	}
	// No state at all: untouched.
	if headers := repairTurnStateHeader("gpt-5.6-luna", "", "gpt-5.6-luna", ""); headers != nil {
		t.Fatalf("absent state must pass through: %+v", headers)
	}
	// No baseline for the model: untouched (nothing to backfill with).
	if headers := repairTurnStateHeader("gpt-6-astra", "", "gpt-6-astra", strings.Repeat("B", 312)); headers != nil {
		t.Fatalf("missing baseline must pass through: %+v", headers)
	}
}

// TestParseTurnStateTimestamp decodes the Fernet timestamp embedded in a
// synthetic token with the production layout; malformed inputs are rejected.
func TestParseTurnStateTimestamp(t *testing.T) {
	// Deterministic synthetic sample with the same Fernet layout as production.
	const token = "gAAAAABqrJWYCQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0-P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn-AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq-wsbKztLW2t7i5uru8vb6_wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2A=="
	ts, ok := parseTurnStateTimestamp(token)
	if !ok {
		t.Fatal("valid token must decode")
	}
	if ts.Unix() < 1789690000 || ts.Unix() > 1789700000 {
		t.Fatalf("unexpected timestamp: %v (%d)", ts, ts.Unix())
	}
	for _, bad := range []string{"", "AAAA", token[:10], "not-base64!!"} {
		if _, ok := parseTurnStateTimestamp(bad); ok {
			t.Fatalf("malformed token must be rejected: %q", bad)
		}
	}
}

// TestProbeConfigDefaultsAndOverrides drives probe config parsing.
func TestProbeConfigDefaultsAndOverrides(t *testing.T) {
	enabled := true
	scan := 15
	window := 4
	attempts := 2
	cooldown := 3
	block := probeConfigYAML{
		Enabled:         &enabled,
		Models:          []string{" gpt-6-astra ", ""},
		TTLMinutes:      nil,
		ScanSeconds:     &scan,
		WindowMinutes:   &window,
		AttemptsPerHop:  &attempts,
		CooldownMinutes: &cooldown,
		Proxies:         []string{"direct", "socks5://a:b@1.2.3.4:443"},
	}
	cfg := parseProbeConfig(block)
	if !cfg.Enabled || cfg.ScanInterval != 15*time.Second || cfg.Window != 4*time.Minute || cfg.AttemptsPerHop != 2 || cfg.Cooldown != 3*time.Minute {
		t.Fatalf("overrides wrong: %+v", cfg)
	}
	if cfg.TTL != 55*time.Minute || cfg.ProbeInterval != 5*time.Second || cfg.Timeout != 60*time.Second {
		t.Fatalf("defaults wrong: %+v", cfg)
	}
	if len(cfg.Proxies) != 2 || cfg.Proxies[0] != "direct" {
		t.Fatalf("proxies wrong: %+v", cfg.Proxies)
	}
	// Empty config falls back to the default model priority list.
	empty := parseProbeConfig(probeConfigYAML{})
	if len(empty.Models) == 0 || empty.Models[0] != "gpt-6-astra" {
		t.Fatalf("default models wrong: %+v", empty.Models)
	}
	if empty.Enabled {
		t.Fatal("probe must default to disabled")
	}
}

// TestActiveValueLifecycle exercises the baseline serving rules without
// network: missing entries serve nothing, fresh decodable values serve, and
// values that provably exceed the TTL stop serving.
func TestActiveValueLifecycle(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	cfg := parseProbeConfig(probeConfigYAML{})
	probeTrack.cfg.Config = cfg
	if value := probeTrack.activeValueFor("gpt-6-astra"); value != "" {
		t.Fatalf("missing entry must serve nothing: %q", value)
	}
	fresh := synthStateToken(time.Now())
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra", Value: fresh, ValueLength: len(fresh), Valid: true}
	if value := probeTrack.activeValueFor("gpt-6-astra"); value != fresh {
		t.Fatalf("fresh value must serve: %q", value)
	}
	expired := synthStateToken(time.Now().Add(-2 * cfg.TTL))
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra", Value: expired, ValueLength: len(expired), Valid: true}
	if value := probeTrack.activeValueFor("gpt-6-astra"); value != "" {
		t.Fatalf("expired value must stop serving: %q", value)
	}
}

// synthStateToken builds a Fernet-layout token (byte-exact 217-byte payload,
// base64url 292 characters) whose embedded timestamp is the given instant, so
// the validity logic can be exercised without real tokens.
func synthStateToken(t time.Time) string {
	raw := make([]byte, 217) // [1B version][8B ts][16B IV][160B cipher][32B HMAC]
	raw[0] = 0x80
	binary.BigEndian.PutUint64(raw[1:9], uint64(t.Unix()))
	for i := 9; i < len(raw); i++ {
		raw[i] = byte('A' + i%26)
	}
	return base64.URLEncoding.EncodeToString(raw)
}

// TestProbeSummaryShape ensures the management payload carries the expected keys.
func TestProbeSummaryShape(t *testing.T) {
	summary := probeSummary()
	for _, key := range []string{"enabled", "models", "proxies", "values", "history", "ttl_minutes", "window_minutes", "running", "seeded"} {
		if _, ok := summary[key]; !ok {
			t.Fatalf("probe summary missing %s: %+v", key, summary)
		}
	}
}

// TestValidityDisplay drives the token-derived validity window exposed to the
// dashboard: issued_at/expires_at/remaining_seconds/expired are computed from
// the embedded timestamp plus the configured validity duration (no capture-time
// arithmetic).
func TestValidityDisplay(t *testing.T) {
	enabled := true
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	cfg := parseProbeConfig(probeConfigYAML{Enabled: &enabled, TTLMinutes: nil})
	probeTrack.cfg.Config = cfg
	if cfg.TTL != 55*time.Minute {
		t.Fatalf("default validity wrong: %v", cfg.TTL)
	}
	issued := time.Now().Add(-20 * time.Minute).Truncate(time.Second)
	fresh := synthStateToken(issued)
	if len(fresh) != probeRequiredStateLength {
		t.Fatalf("synthetic token must be 292 characters: %d", len(fresh))
	}
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra", Value: fresh,
		ValueLength: len(fresh), Source: "probe", Valid: true}
	expiredTs := time.Now().Add(-2 * cfg.TTL).Truncate(time.Second)
	stale := synthStateToken(expiredTs)
	probeTrack.values["gpt-5.6-sol"] = stateEntry{Model: "gpt-5.6-sol", Value: stale,
		ValueLength: len(stale), Source: "seed", Valid: true}

	summary := probeSummary()
	byModel := map[string]map[string]any{}
	for _, item := range summary["values"].([]map[string]any) {
		byModel[item["model"].(string)] = item
	}
	freshItem := byModel["gpt-6-astra"]
	if freshItem == nil {
		t.Fatalf("missing astra entry: %+v", summary["values"])
	}
	if got := freshItem["issued_at"]; got != issued.UTC().Format(time.RFC3339) {
		t.Fatalf("issued_at must come from the token: %v vs %v", got, issued.UTC().Format(time.RFC3339))
	}
	if got := freshItem["expires_at"]; got != issued.Add(cfg.TTL).UTC().Format(time.RFC3339) {
		t.Fatalf("expires_at wrong: %v", got)
	}
	if freshItem["expired"] != false {
		t.Fatalf("fresh token must not be expired: %+v", freshItem)
	}
	remaining, ok := freshItem["remaining_seconds"].(int64)
	if !ok || remaining < int64((cfg.TTL-21*time.Minute).Seconds()) || remaining > int64((cfg.TTL-19*time.Minute).Seconds()) {
		t.Fatalf("remaining_seconds out of range: %v (%T)", freshItem["remaining_seconds"], freshItem["remaining_seconds"])
	}
	staleItem := byModel["gpt-5.6-sol"]
	if staleItem == nil || staleItem["expired"] != true {
		t.Fatalf("stale token must be flagged expired: %+v", staleItem)
	}
	if remaining, ok := staleItem["remaining_seconds"].(int64); !ok || remaining >= 0 {
		t.Fatalf("expired token must report a negative remaining: %v", staleItem["remaining_seconds"])
	}
}

// TestTurnStateOverrideConfigParsing drives the config parser with realistic
// lifecycle payloads and checks activation, trimming and validation errors.
func TestTurnStateOverrideConfigParsing(t *testing.T) {
	turnStateOverride = atomic.Value{}
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })

	valid := []byte(`enabled: true
priority: 100
store:
  version: 1.3.0
turn-state-override:
  enabled: true
  models: ["gpt-6-astra", " gpt-6-luna "]
  value: "SAMPLE-1"
  force: true
`)
	if err := configureTurnStateOverride(valid); err != nil {
		t.Fatal(err)
	}
	summary := turnStateOverrideSummary()
	if summary["enabled"] != true || summary["force"] != true || summary["value_length"] != 8 {
		t.Fatalf("summary wrong: %+v", summary)
	}
	models := summary["models"].([]string)
	if len(models) != 2 || models[0] != "gpt-6-astra" || models[1] != "gpt-6-luna" {
		t.Fatalf("models not trimmed: %+v", models)
	}

	if err := configureTurnStateOverride([]byte(`turn-state-override:
  enabled: true
  models: []
  value: x
`)); err == nil {
		t.Fatal("empty models must be rejected")
	}
	if summary := turnStateOverrideSummary(); summary["error"] == "" {
		t.Fatalf("config error must be surfaced: %+v", summary)
	}

	if err := configureTurnStateOverride([]byte(`turn-state-override:
  enabled: true
  models: ["gpt-6-astra"]
  value: ""
`)); err == nil {
		t.Fatal("empty value must be rejected")
	}
}

// TestTurnStateOverrideApply drives the rewrite decision matrix.
func TestTurnStateOverrideApply(t *testing.T) {
	turnStateOverride = atomic.Value{}
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })
	// Isolate from other tests: the shared baseline table must be empty here.
	probeTrack.values = map[string]stateEntry{}

	config := `turn-state-override:
  enabled: true
  models: ["gpt-6-astra"]
  value: "REWRITTEN-STATE"
`
	if err := configureTurnStateOverride([]byte(config)); err != nil {
		t.Fatal(err)
	}

	headers, status := applyTurnStateOverride("gpt-6-astra", "", nil)
	if status != "applied-config" || headers.Get(turnStateHeader) != "REWRITTEN-STATE" {
		t.Fatalf("astra must be rewritten: %v %+v", status, headers)
	}
	headers, status = applyTurnStateOverride("GPT-6-ASTRA-preview", "", nil)
	if status != "applied-config" || headers.Get(turnStateHeader) != "REWRITTEN-STATE" {
		t.Fatalf("case/prefix match must rewrite: %v %+v", status, headers)
	}
	if _, status = applyTurnStateOverride("gpt-6-luna", "", nil); status != "" {
		t.Fatalf("unmatched model must not rewrite: %v", status)
	}
	if headers, status = applyTurnStateOverride("", "gpt-6-astra", nil); status != "applied-config" {
		t.Fatalf("requested model fallback must rewrite: %v %+v", status, headers)
	}

	// Fill mode keeps a healthy-length existing client value.
	healthyClient := synthBusinessState(3)
	clientHeaders := http.Header{turnStateHeader: {healthyClient}}
	if headers, status = applyTurnStateOverride("gpt-6-astra", "", clientHeaders); status != "skipped-existing" || headers != nil {
		t.Fatalf("fill mode must skip a healthy existing value: %v %+v", status, headers)
	}

	// An unhealthy-length client value is replaced even in fill mode.
	degradedHeaders := http.Header{turnStateHeader: {strings.Repeat("A", 312)}}
	if headers, status = applyTurnStateOverride("gpt-6-astra", "", degradedHeaders); status != "applied-config" || headers.Get(turnStateHeader) != "REWRITTEN-STATE" {
		t.Fatalf("degraded existing value must be replaced in fill mode: %v %+v", status, headers)
	}

	// Force mode replaces it.
	if err := configureTurnStateOverride([]byte(config + "  force: true\n")); err != nil {
		t.Fatal(err)
	}
	if headers, status = applyTurnStateOverride("gpt-6-astra", "", clientHeaders); status != "applied-config" || headers.Get(turnStateHeader) != "REWRITTEN-STATE" {
		t.Fatalf("force mode must replace: %v %+v", status, headers)
	}

	// Disabled config means no action at all.
	if err := configureTurnStateOverride([]byte(`turn-state-override:
  enabled: false
  models: ["gpt-6-astra"]
  value: "x"
`)); err != nil {
		t.Fatal(err)
	}
	if _, status = applyTurnStateOverride("gpt-6-astra", "", nil); status != "" {
		t.Fatalf("disabled config must not act: %v", status)
	}
}

// TestInterceptRequestAppliesRewrite checks the request interceptor returns the
// header replacement and records the override status alongside the timezone
// normalization.
func TestInterceptRequestAppliesRewrite(t *testing.T) {
	history = auditState{}
	turnStateOverride = atomic.Value{}
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })
	probeTrack.values = map[string]stateEntry{}
	if err := configureTurnStateOverride([]byte(`turn-state-override:
  enabled: true
  models: ["gpt-6-astra"]
  value: "REWRITTEN-STATE"
  force: true
`)); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(interceptRequest{
		RequestID: "req-rw", ToFormat: "codex", Model: "gpt-6-astra",
		Headers: http.Header{turnStateHeader: {"CLIENT-STATE"}},
		Body:    []byte(`{"input":"hello"}`),
	})
	resp, err := intercept(raw)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Headers == nil || resp.Headers.Get(turnStateHeader) != "REWRITTEN-STATE" {
		t.Fatalf("interceptor must return the header rewrite: %+v", resp)
	}
	record := history.snapshot()["records"].([]auditRecord)[0]
	if record.TurnStateOverride != "applied-config" {
		t.Fatalf("record must show applied-config: %+v", record)
	}
	// The observed request-side value stays the client's original view.
	if record.TurnStateValue != "CLIENT-STATE" || record.TurnStateSource != "request" {
		t.Fatalf("observation must keep the original view: %+v", record)
	}
}

// TestLifecycleConfigExtraction feeds a base64 config_yaml lifecycle payload as
// the host sends it.
func TestLifecycleConfigExtraction(t *testing.T) {
	turnStateOverride = atomic.Value{}
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })
	payload, _ := json.Marshal(map[string]any{
		"config_yaml":    []byte("turn-state-override:\n  enabled: true\n  models: [\"gpt-6-astra\"]\n  value: \"X\"\n"),
		"schema_version": 6,
	})
	configureTurnStateOverrideFromLifecycle(payload)
	if summary := turnStateOverrideSummary(); summary["enabled"] != true {
		t.Fatalf("lifecycle extraction failed: %+v", summary)
	}
	// Malformed payloads keep the previous state.
	configureTurnStateOverrideFromLifecycle([]byte("not json"))
	if summary := turnStateOverrideSummary(); summary["enabled"] != true {
		t.Fatalf("malformed payload must keep state: %+v", summary)
	}
}

// TestRegistrationDeclaresResponseCapabilities ensures the new hooks are
// advertised to the host, otherwise CPA never calls them.
func TestRegistrationDeclaresResponseCapabilities(t *testing.T) {
	result, err := handleMethod("plugin.register", nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(result)
	for _, key := range []string{
		`"response_interceptor":true`,
		`"response_stream_interceptor":true`,
		`"websocket_response_observer":true`,
		`"schema_version":6`,
	} {
		if !bytes.Contains(encoded, []byte(key)) {
			t.Errorf("registration missing %s in %s", key, encoded)
		}
	}
}

// TestResponseHookMethodsAreRouted verifies the exact method names the host
// dispatches match what handleMethod accepts.
func TestResponseHookMethodsAreRouted(t *testing.T) {
	for _, method := range []string{"response.intercept_after", "response.intercept_stream_chunk", "websocket.response_event"} {
		if _, err := handleMethod(method, []byte(`{"RequestID":"x"}`)); err != nil {
			t.Errorf("handleMethod(%s) failed: %v", method, err)
		}
	}
}

// TestAuditUpsertPreservesObservations checks a repeated request record keeps
// the already observed upstream model and turn-state.
func TestAuditUpsertPreservesObservations(t *testing.T) {
	var state auditState
	state.record(auditRecord{RequestID: "retry-1", Model: "gpt-5.6-luna", conversion: conversion{Target: targetTimezone, Action: "inserted"}})
	state.observeModel("retry-1", "", "gpt-6-luna")
	state.observeTurnState("retry-1", "abcdef", "stream")
	// A retry re-records the request level; observed fields must survive.
	state.record(auditRecord{RequestID: "retry-1", Model: "gpt-5.6-luna", conversion: conversion{Target: targetTimezone, Action: "unchanged"}})
	records := state.snapshot()["records"].([]auditRecord)
	if len(records) != 1 {
		t.Fatalf("retry must update in place: %d records", len(records))
	}
	record := records[0]
	if record.UpstreamModel != "gpt-6-luna" || !record.ModelMismatch || record.TurnStateLength != 6 {
		t.Fatalf("observations lost on upsert: %+v", record)
	}
}

// TestTurnStateInjectedLengthRecorded verifies the byte length written into
// the header is recorded, so the dashboard can show the field size even when
// no turn-state was observed on the request or response path.
func TestTurnStateInjectedLengthRecorded(t *testing.T) {
	history = auditState{}
	turnStateOverride = atomic.Value{}
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })
	probeTrack.values = map[string]stateEntry{}
	if err := configureTurnStateOverride([]byte(`turn-state-override:
  enabled: true
  models: ["gpt-6-astra"]
  value: "REWRITTEN-STATE"
  force: true
`)); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(interceptRequest{
		RequestID: "req-inj", ToFormat: "codex", Model: "gpt-6-astra",
		Body: []byte(`{"input":"hello"}`),
	})
	if _, err := intercept(raw); err != nil {
		t.Fatal(err)
	}
	record := history.snapshot()["records"].([]auditRecord)[0]
	if record.TurnStateLength != 0 {
		t.Fatalf("no observed turn-state expected: %+v", record)
	}
	if record.TurnStateOverride != "applied-config" || record.TurnStateInjectedLength != len("REWRITTEN-STATE") {
		t.Fatalf("injected length must be recorded: %+v", record)
	}
	// A retry keeps the recorded injection length.
	history.record(auditRecord{RequestID: "req-inj", Model: "gpt-6-astra", conversion: conversion{Target: targetTimezone, Action: "unchanged"}})
	after := history.snapshot()["records"].([]auditRecord)[0]
	if after.TurnStateInjectedLength != len("REWRITTEN-STATE") {
		t.Fatalf("injected length lost on retry: %+v", after)
	}
}
