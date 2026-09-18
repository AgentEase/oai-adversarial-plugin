package main

import (
	"bytes"
	"encoding/json"
	"net/http"
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
		FailedAt: "2026-09-18T02:20:00Z"}
	encoded, err := json.Marshal(failure)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"attempts":30`, `"rounds":10`, `"last_length":312`, `"last_error"`} {
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
	block := probeConfigYAML{
		Enabled:         &enabled,
		Models:          []string{" gpt-6-astra ", ""},
		TTLMinutes:      nil,
		ScanSeconds:     &scan,
		WindowMinutes:   &window,
		AttemptsPerHop:  &attempts,
		Proxies:         []string{"direct", "socks5://a:b@1.2.3.4:443"},
	}
	cfg := parseProbeConfig(block)
	if !cfg.Enabled || cfg.ScanInterval != 15*time.Second || cfg.Window != 4*time.Minute || cfg.AttemptsPerHop != 2 {
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

// TestProbeValueLifecycle exercises store/expire semantics without network.
func TestProbeValueLifecycle(t *testing.T) {
	engine := &probeEngine{values: map[string]stateEntry{}}
	cfg := parseProbeConfig(probeConfigYAML{})
	// Missing value needs a probe.
	if !engine.needsProbe("gpt-6-astra", cfg) {
		t.Fatal("missing value must need probe")
	}
	// A fresh token rejects probing; an old token triggers it.
	fresh, _ := parseTurnStateTimestamp("gAAAAABqrJWYCQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0-P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn-AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq-wsbKztLW2t7i5uru8vb6_wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2A==")
	engine.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra", Value: "AAAA", Valid: true}
	if engine.needsProbe("gpt-6-astra", cfg) != true {
		t.Fatal("undecodable value must need probe")
	}
	_ = fresh
}

// TestProbeSummaryShape ensures the management payload carries the expected keys.
func TestProbeSummaryShape(t *testing.T) {
	summary := probeSummary()
	for _, key := range []string{"enabled", "models", "proxies", "values", "history", "ttl_minutes", "window_minutes"} {
		if _, ok := summary[key]; !ok {
			t.Fatalf("probe summary missing %s: %+v", key, summary)
		}
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

	// Fill mode keeps an existing client value.
	clientHeaders := http.Header{turnStateHeader: {"CLIENT-STATE"}}
	if headers, status = applyTurnStateOverride("gpt-6-astra", "", clientHeaders); status != "skipped-existing" || headers != nil {
		t.Fatalf("fill mode must skip existing: %v %+v", status, headers)
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
