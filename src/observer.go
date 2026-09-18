package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	"gopkg.in/yaml.v3"
)

// This file implements the observation extensions that run alongside the
// timezone normalization:
//
//  1. Upstream model consistency: every recorded request keeps the model CPA
//     executed; the response-side interceptors (non-streaming, streaming and
//     websocket) probe the model that the upstream actually served. The
//     dashboard flags the record when the two disagree (like sub2api shows
//     "upstream response: <model> / model mismatch").
//  2. X-Codex-Turn-State tracking and rewrite: the header value seen on the
//     request, downstream response or stream is recorded as length plus a
//     short preview (bounded full value from v1.2.0), and matching requests
//     receive a configured replacement value before they reach the upstream.
//
// Everything here must stay cheap: stream and websocket paths call into this
// file for every chunk or event of hot traffic.

const (
	turnStateHeader         = "X-Codex-Turn-State"
	turnStatePreviewLength  = 32
	turnStateValueLimit     = 4096
	maxModelProbeSize       = 1 << 20
	maxModelProbeCandidates = 4
)

// turnStateOverrideConfig is the plugin-config block that drives the
// request-side X-Codex-Turn-State rewrite. It is read from the
// plugins.configs.timezone-override subtree (key "turn-state-override"):
//
//	turn-state-override:
//	  enabled: true
//	  models: ["gpt-6-astra"]
//	  value: "gAAAAAB..."
//	  force: true
type turnStateOverrideConfig struct {
	Enabled bool               `yaml:"enabled"`
	Models  []string           `yaml:"models"`
	Value   string             `yaml:"value"`
	Force   bool               `yaml:"force"`
	Probe   probeConfigYAML    `yaml:"probe"`
}

// turnStateOverrideState is the active rewrite configuration plus the last
// configuration error (shown on the dashboard when the config is invalid).
type turnStateOverrideState struct {
	Config turnStateOverrideConfig
	Error  string
}

var turnStateOverride atomic.Value // *turnStateOverrideState

// currentTurnStateOverride returns the active configuration snapshot or nil.
func currentTurnStateOverride() *turnStateOverrideState {
	if state, ok := turnStateOverride.Load().(*turnStateOverrideState); ok {
		return state
	}
	return nil
}

// configureTurnStateOverride parses the plugin config YAML handed to
// plugin.register / plugin.reconfigure and activates the rewrite rules. The
// config YAML is the full plugins.configs.<id> subtree, so unknown keys
// (enabled, priority, store, ...) are ignored.
func configureTurnStateOverride(configYAML []byte) error {
	state := &turnStateOverrideState{}
	var root struct {
		TurnStateOverride turnStateOverrideConfig `yaml:"turn-state-override"`
	}
	trimmed := bytes.TrimSpace(configYAML)
	if len(trimmed) > 0 {
		if err := yaml.Unmarshal(trimmed, &root); err != nil {
			state = &turnStateOverrideState{Error: fmt.Sprintf("decode turn-state-override config: %v", err)}
			turnStateOverride.Store(state)
			return fmt.Errorf("decode turn-state-override config: %w", err)
		}
	}
	config := root.TurnStateOverride
	config.Value = strings.TrimSpace(config.Value)
	models := make([]string, 0, len(config.Models))
	for _, model := range config.Models {
		if model = strings.TrimSpace(model); model != "" {
			models = append(models, model)
		}
	}
	config.Models = models
	if config.Enabled {
		switch {
		case len(config.Models) == 0 && !probeEnabled(config.Probe):
			state = &turnStateOverrideState{Config: config, Error: "turn-state-override.models must not be empty"}
			turnStateOverride.Store(state)
			return fmt.Errorf("turn-state-override.models must not be empty")
		case config.Value == "" && !probeEnabled(config.Probe):
			state = &turnStateOverrideState{Config: config, Error: "turn-state-override.value must not be empty (and probe is disabled)"}
			turnStateOverride.Store(state)
			return fmt.Errorf("turn-state-override.value must not be empty (and probe is disabled)")
		}
	}
	// The probe track may be enabled independently of the static value; it
	// supplies fresh per-model values when available.
	_ = configureProbeTrack(config.Probe)
	state = &turnStateOverrideState{Config: config}
	turnStateOverride.Store(state)
	return nil
}

// probeEnabled reports whether the probe block requests the background track.
func probeEnabled(block probeConfigYAML) bool {
	return block.Enabled != nil && *block.Enabled
}

// turnStateOverrideMatches reports whether the request model (executed or
// requested name) is targeted by the rewrite rules. Matching is a
// case-insensitive prefix comparison so a family entry such as "gpt-6-astra"
// also covers suffixed variants.
func turnStateOverrideMatches(config turnStateOverrideConfig, models ...string) bool {
	for _, candidate := range models {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		lowerCandidate := strings.ToLower(candidate)
		for _, target := range config.Models {
			if strings.HasPrefix(lowerCandidate, strings.ToLower(target)) {
				return true
			}
		}
	}
	return false
}

// applyTurnStateOverride evaluates the rewrite rules for one request and
// returns the header replacement plus the record status:
//
//	""                not applicable (disabled, no match, or no value)
//	"applied"         header replaced with a fresh probe-captured value
//	"applied-config"  header replaced with the configured static value
//	"skipped-existing" fill mode found an existing client value
//
// A fresh probe-track value (when present and unexpired) wins over the static
// configured value.
func applyTurnStateOverride(model, requestedModel string, headers http.Header) (http.Header, string) {
	state := currentTurnStateOverride()
	if state == nil {
		return nil, ""
	}
	probeValue := ""
	matched := false
	for _, candidate := range []string{model, requestedModel} {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if state.Config.Enabled && turnStateModelMatched(state.Config, candidate) {
			matched = true
		}
		if value := probeTrack.activeValueFor(candidate); value != "" {
			probeValue = value
			matched = true
			break
		}
	}
	if !matched {
		return nil, ""
	}
	if !probeEnabled(state.Config.Probe) {
		if !state.Config.Enabled || state.Config.Value == "" {
			return nil, ""
		}
		if !state.Config.Force && headerValue(headers, turnStateHeader) != "" {
			return nil, "skipped-existing"
		}
		return http.Header{turnStateHeader: []string{state.Config.Value}}, "applied-config"
	}
	// Probe mode: prefer the fresh captured value; fall back to the static
	// value only when it is configured and no probe value is available yet.
	if probeValue == "" {
		if !state.Config.Enabled || state.Config.Value == "" {
			return nil, ""
		}
		probeValue = state.Config.Value
		if !state.Config.Force && headerValue(headers, turnStateHeader) != "" {
			return nil, "skipped-existing"
		}
		return http.Header{turnStateHeader: []string{probeValue}}, "applied-config"
	}
	if !state.Config.Force && headerValue(headers, turnStateHeader) != "" {
		return nil, "skipped-existing"
	}
	return http.Header{turnStateHeader: []string{probeValue}}, "applied"
}

// turnStateModelMatched reports whether a single candidate model name is
// targeted by the static rewrite list (case-insensitive prefix).
func turnStateModelMatched(config turnStateOverrideConfig, candidate string) bool {
	lower := strings.ToLower(candidate)
	for _, target := range config.Models {
		if strings.HasPrefix(lower, strings.ToLower(target)) {
			return true
		}
	}
	return false
}

func headerValue(headers http.Header, name string) string {
	if headers == nil {
		return ""
	}
	return strings.TrimSpace(headers.Get(name))
}

func previewValue(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

// interceptNonStreamingResponse observes successful non-streaming execution
// responses before they are delivered downstream. The handler is observation
// only and never modifies the response.
func interceptNonStreamingResponse(raw []byte) (responseInterceptOutput, error) {
	var req responseInterceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return responseInterceptOutput{}, fmt.Errorf("decode response interception: %w", err)
	}
	if model, ok := probeUpstreamModel(req.Body); ok {
		history.observeModel(req.RequestID, "", model)
	}
	history.observeTurnState(req.RequestID, headerValue(req.ResponseHeaders, turnStateHeader), "response")
	return responseInterceptOutput{}, nil
}

// interceptStreamChunk observes every successful stream chunk before it is
// delivered downstream. The header-init call (ChunkIndex == -1) carries no
// payload; payload chunks carry one upstream event each. Observation only.
func interceptStreamChunk(raw []byte) (responseInterceptOutput, error) {
	var req streamChunkInterceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return responseInterceptOutput{}, fmt.Errorf("decode stream chunk interception: %w", err)
	}
	if req.ChunkIndex != streamChunkHeaderInitIndex {
		if model, ok := probeUpstreamModel(req.Body); ok {
			history.observeModel(req.RequestID, "", model)
		}
	}
	history.observeTurnState(req.RequestID, headerValue(req.ResponseHeaders, turnStateHeader), "stream")
	return responseInterceptOutput{}, nil
}

// observeWebSocketEvent observes upstream websocket response events (the Codex
// Desktop /v1/responses path). The observer interface is read-only, so this
// only records the upstream model.
func observeWebSocketEvent(raw []byte) (struct{}, error) {
	var event webSocketResponseEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return struct{}{}, fmt.Errorf("decode websocket event: %w", err)
	}
	if model, ok := probeUpstreamModel(event.Payload); ok {
		history.observeModel(event.RequestID, event.TraceID, model)
	}
	return struct{}{}, nil
}

// probeUpstreamModel extracts the model name that the upstream reported, from
// a non-streaming body, an SSE chunk or a websocket frame. It is deliberately
// permissive about the transport shape and conservative about false matches:
// a payload without the "model" key is rejected by a substring prefilter, and
// unknown shapes fall through to the next candidate.
func probeUpstreamModel(payload []byte) (string, bool) {
	if len(payload) == 0 || len(payload) > maxModelProbeSize || !bytes.Contains(payload, []byte(`"model"`)) {
		return "", false
	}
	for _, candidate := range jsonCandidates(payload) {
		var probe struct {
			Type     string `json:"type"`
			Object   string `json:"object"`
			Model    string `json:"model"`
			Response struct {
				Model string `json:"model"`
			} `json:"response"`
			Message struct {
				Model string `json:"model"`
			} `json:"message"`
		}
		if err := json.Unmarshal(candidate, &probe); err != nil {
			continue
		}
		switch {
		case probe.Response.Model != "":
			return probe.Response.Model, true
		case probe.Message.Model != "":
			return probe.Message.Model, true
		case probe.Model != "" && (probe.Type != "" || probe.Object != ""):
			return probe.Model, true
		}
	}
	return "", false
}

// jsonCandidates returns the JSON documents that may appear in a payload:
// the whole body when it starts with '{', plus the first few "data:" / raw
// lines of an SSE stream.
func jsonCandidates(payload []byte) [][]byte {
	trimmed := bytes.TrimSpace(payload)
	candidates := make([][]byte, 0, maxModelProbeCandidates)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		candidates = append(candidates, trimmed)
	}
	for _, line := range bytes.Split(payload, []byte("\n")) {
		if len(candidates) >= maxModelProbeCandidates {
			break
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 || (len(candidates) > 0 && bytes.Equal(line, candidates[0])) {
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			line = bytes.TrimSpace(line[len("data:"):])
		}
		if len(line) > 0 && line[0] == '{' {
			candidates = append(candidates, line)
		}
	}
	return candidates
}

// observeModel attaches the upstream-reported model to an existing record.
// Requests are recorded by the request interceptor before execution starts,
// so an observer call for an unknown request (for example non-Codex traffic)
// is intentionally ignored rather than creating observer-only records.
func (s *auditState) observeModel(requestID, traceID, upstreamModel string) {
	upstreamModel = strings.TrimSpace(upstreamModel)
	if requestID == "" || upstreamModel == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		record := &s.records[i]
		if record.RequestID != requestID && (traceID == "" || record.TraceID != traceID) {
			continue
		}
		record.UpstreamModel = upstreamModel
		record.ModelChecked = true
		basis := strings.TrimSpace(record.Model)
		if basis == "" {
			basis = strings.TrimSpace(record.RequestedModel)
		}
		record.ModelMismatch = basis != "" && !strings.EqualFold(basis, upstreamModel)
		return
	}
}

// observeTurnState records the size, the full value (bounded by
// turnStateValueLimit) and a short preview of the X-Codex-Turn-State header
// seen at the request, response or stream stage.
func (s *auditState) observeTurnState(requestID, value, source string) {
	value = strings.TrimSpace(value)
	if requestID == "" || value == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		if s.records[i].RequestID != requestID {
			continue
		}
		record := &s.records[i]
		record.TurnStateLength = len(value)
		record.TurnStateSource = source
		record.TurnStatePreview = previewValue(value, turnStatePreviewLength)
		if len(value) <= turnStateValueLimit {
			record.TurnStateValue = value
			record.TurnStateTruncated = false
		} else {
			record.TurnStateValue = value[:turnStateValueLimit]
			record.TurnStateTruncated = true
		}
		return
	}
}

// configureTurnStateOverrideFromLifecycle extracts the plugin config YAML from
// a plugin.register / plugin.reconfigure lifecycle payload (the request is
// JSON with a base64 config_yaml field) and activates the rewrite rules.
// Failures are recorded on the dashboard state instead of failing
// registration, so a bad rewrite config can never take the plugin down.
func configureTurnStateOverrideFromLifecycle(raw []byte) {
	var request struct {
		ConfigYAML []byte `json:"config_yaml"`
	}
	if err := json.Unmarshal(raw, &request); err != nil || len(request.ConfigYAML) == 0 {
		// Malformed lifecycle payloads keep the previous configuration.
		return
	}
	_ = configureTurnStateOverride(request.ConfigYAML)
}

// turnStateOverrideSummary describes the active rewrite configuration for the
// management API. The full rewrite value is never exposed; only its length and
// a short preview are reported.
func turnStateOverrideSummary() map[string]any {
	state := currentTurnStateOverride()
	if state == nil {
		return map[string]any{"enabled": false}
	}
	preview := ""
	if state.Config.Value != "" {
		preview = previewValue(state.Config.Value, turnStatePreviewLength)
	}
	models := state.Config.Models
	if models == nil {
		models = []string{}
	}
	return map[string]any{
		"enabled":       state.Config.Enabled,
		"models":        models,
		"force":         state.Config.Force,
		"value_length":  len(state.Config.Value),
		"value_preview": preview,
		"error":         state.Error,
		"probe":         probeSummary(),
	}
}
