package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	pluginID                   = "timezone-override"
	pluginVersion              = "1.5.3"
	historyLimit               = 200
	schemaVersion              = 6
	streamChunkHeaderInitIndex = -1
)

//go:embed web/index.html
var dashboard []byte

type interceptRequest struct {
	RequestID      string
	TraceID        string
	SourceFormat   string
	ToFormat       string
	Model          string
	RequestedModel string
	Headers        http.Header
	Body           []byte
}

type interceptResponse struct {
	Body            []byte      `json:"Body,omitempty"`
	Headers         http.Header `json:"Headers,omitempty"`
	Terminate       bool        `json:"Terminate,omitempty"`
	StatusCode      int         `json:"StatusCode,omitempty"`
	ResponseHeaders http.Header `json:"ResponseHeaders,omitempty"`
	ResponseBody    []byte      `json:"ResponseBody,omitempty"`
}

// responseInterceptRequest mirrors the successful non-streaming execution
// response handed to response.intercept_after.
type responseInterceptRequest struct {
	RequestID       string
	SourceFormat    string
	Model           string
	RequestedModel  string
	Stream          bool
	RequestHeaders  http.Header
	ResponseHeaders http.Header
	OriginalRequest []byte
	RequestBody     []byte
	Body            []byte
	StatusCode      int
	Metadata        map[string]any
}

// streamChunkInterceptRequest mirrors one successful stream chunk handed to
// response.intercept_stream_chunk.
type streamChunkInterceptRequest struct {
	RequestID       string
	SourceFormat    string
	Model           string
	RequestedModel  string
	RequestHeaders  http.Header
	ResponseHeaders http.Header
	OriginalRequest []byte
	RequestBody     []byte
	Body            []byte
	HistoryChunks   [][]byte
	ChunkIndex      int
	Metadata        map[string]any
}

// webSocketResponseEvent mirrors one upstream websocket response event handed
// to websocket.response_event.
type webSocketResponseEvent struct {
	RequestID      string
	TraceID        string
	SourceFormat   string
	Model          string
	RequestedModel string
	Provider       string
	AuthID         string
	AuthLabel      string
	AuthType       string
	EventType      string
	Payload        []byte
	Metadata       map[string]any
}

// responseInterceptOutput is the modification envelope for the response-side
// interceptors: mentioned headers are replaced, everything else is preserved.
type responseInterceptOutput struct {
	Headers      http.Header `json:"Headers,omitempty"`
	Body         []byte      `json:"Body,omitempty"`
	ClearHeaders []string    `json:"ClearHeaders,omitempty"`
}

type auditRecord struct {
	conversion
	RequestID        string `json:"request_id"`
	TraceID          string `json:"trace_id,omitempty"`
	Model            string `json:"model"`
	RequestedModel   string `json:"requested_model,omitempty"`
	Time             string `json:"time"`
	UpstreamModel    string `json:"upstream_model,omitempty"`
	ModelChecked     bool   `json:"model_checked"`
	ModelMismatch    bool   `json:"model_mismatch"`
	TurnStateLength  int    `json:"turn_state_length"`
	TurnStateSource  string `json:"turn_state_source,omitempty"`
	TurnStatePreview string `json:"turn_state_preview,omitempty"`
	TurnStateValue   string `json:"turn_state_value,omitempty"`
	TurnStateTruncated bool `json:"turn_state_truncated,omitempty"`
	TurnStateOverride  string `json:"turn_state_override,omitempty"`
	TurnStateInjectedLength int `json:"turn_state_injected_length,omitempty"`
}

type auditState struct {
	mu       sync.Mutex
	records  []auditRecord
	total    uint64
	inserted uint64
	replaced uint64
}

var history auditState

type managementRequest struct {
	Method string
	Path   string
}

type managementResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

func handleMethod(method string, raw []byte) (any, error) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		configureTurnStateOverrideFromLifecycle(raw)
		return map[string]any{
			"schema_version": schemaVersion,
			"metadata": map[string]any{
				"Name": "O/对抗插件", "Version": pluginVersion,
				"Author": "Local", "ConfigFields": []any{},
				// CPA requires a repository reference; this links to its extension SDK.
				// This plugin's implementation is delivered as local source.
				"GitHubRepository": "https://github.com/router-for-me/CLIProxyAPI",
			},
			"capabilities": map[string]bool{
				"request_interceptor":         true,
				"management_api":              true,
				"response_interceptor":        true,
				"response_stream_interceptor": true,
				"websocket_response_observer": true,
			},
		}, nil
	case "plugin.quiesce":
		return struct{}{}, nil
	case "request.intercept_before":
		return interceptResponse{}, nil
	case "request.intercept_after":
		return intercept(raw)
	case "response.intercept_after":
		return interceptNonStreamingResponse(raw)
	case "response.intercept_stream_chunk":
		return interceptStreamChunk(raw)
	case "websocket.response_event":
		return observeWebSocketEvent(raw)
	case "management.register":
		return map[string]any{
			"routes": []map[string]string{{"Method": "GET", "Path": "/timezone-override/requests"}},
			"resources": []map[string]string{{
				"Path": "/status", "Menu": "O/对抗插件",
				"Description": "查看请求的原时区、替换结果、上游模型一致性及 X-Codex-Turn-State 观测。",
			}},
		}, nil
	case "management.handle":
		return management(raw)
	case "plugin.shutdown":
		probeTrackShutdown()
		return struct{}{}, nil
	default:
		return nil, fmt.Errorf("unsupported plugin method: %s", method)
	}
}

func intercept(raw []byte) (interceptResponse, error) {
	var req interceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return interceptResponse{}, fmt.Errorf("decode interception: %w", err)
	}
	if req.ToFormat != "codex" {
		return interceptResponse{}, nil
	}
	body, result, err := normalizeRequest(req.Body, req.SourceFormat)
	if err != nil {
		payload, _ := json.Marshal(map[string]any{"error": map[string]string{
			"type": "timezone_normalization_error", "message": err.Error(),
		}})
		return interceptResponse{
			Terminate: true, StatusCode: http.StatusBadRequest,
			ResponseHeaders: http.Header{"Content-Type": {"application/json"}}, ResponseBody: payload,
		}, nil
	}
	overrideHeaders, overrideStatus := applyTurnStateOverride(req.Model, req.RequestedModel, req.Headers)
	injectedLength := 0
	if overrideHeaders != nil {
		injectedLength = len(overrideHeaders.Get(turnStateHeader))
	}
	history.record(auditRecord{
		conversion: result, RequestID: req.RequestID, TraceID: req.TraceID,
		Model: req.Model, RequestedModel: req.RequestedModel,
		Time:              time.Now().UTC().Format(time.RFC3339Nano),
		TurnStateOverride: overrideStatus, TurnStateInjectedLength: injectedLength,
	})
	history.observeTurnState(req.RequestID, headerValue(req.Headers, turnStateHeader), "request")
	response := interceptResponse{}
	if result.Action != "unchanged" {
		response.Body = body
	}
	response.Headers = overrideHeaders
	return response, nil
}

func (s *auditState) record(record auditRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record.RequestID != "" {
		for i := range s.records {
			if s.records[i].RequestID != record.RequestID {
				continue
			}
			existing := &s.records[i]
			// Observed fields (upstream model, turn-state view, override status)
			// stay untouched unless the retry re-applied them; only the
			// request-level view is refreshed so a retry never resets it.
			if record.TurnStateOverride != "" {
				existing.TurnStateOverride = record.TurnStateOverride
				if record.TurnStateInjectedLength > 0 {
					existing.TurnStateInjectedLength = record.TurnStateInjectedLength
				}
			}
			if record.Action != "" || len(record.Original) > 0 || record.Model != "" {
				existing.conversion = record.conversion
				existing.Model = record.Model
				if record.RequestedModel != "" {
					existing.RequestedModel = record.RequestedModel
				}
				if record.TraceID != "" {
					existing.TraceID = record.TraceID
				}
				existing.Time = record.Time
				if existing.ModelChecked {
					basis := strings.TrimSpace(existing.Model)
					if basis == "" {
						basis = strings.TrimSpace(existing.RequestedModel)
					}
					existing.ModelMismatch = basis != "" && !strings.EqualFold(basis, existing.UpstreamModel)
				}
			}
			return
		}
	}
	s.total++
	if record.Action == "inserted" {
		s.inserted++
	}
	if record.Action == "replaced" {
		s.replaced++
	}
	if len(s.records) == historyLimit {
		copy(s.records, s.records[1:])
		s.records[len(s.records)-1] = record
		return
	}
	s.records = append(s.records, record)
}

func (s *auditState) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]auditRecord, len(s.records))
	mismatches := 0
	overridden := 0
	for i := range s.records {
		records[len(s.records)-1-i] = s.records[i]
		if s.records[i].ModelMismatch {
			mismatches++
		}
		if s.records[i].TurnStateOverride == "applied" {
			overridden++
		}
	}
	return map[string]any{
		"plugin": pluginID, "version": pluginVersion, "target": targetTimezone,
		"limit": historyLimit, "total": s.total, "inserted": s.inserted,
		"replaced": s.replaced, "mismatches": mismatches, "overridden": overridden,
		"turn_state_override": turnStateOverrideSummary(),
		"records":            records,
	}
}

func management(raw []byte) (managementResponse, error) {
	var req managementRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return managementResponse{}, fmt.Errorf("decode management request: %w", err)
	}
	if req.Method != "GET" {
		return managementResponse{StatusCode: http.StatusMethodNotAllowed}, nil
	}
	switch {
	case strings.HasSuffix(req.Path, "/status"):
		return managementResponse{
			StatusCode: http.StatusOK,
			Headers:    http.Header{"Content-Type": {"text/html; charset=utf-8"}, "Cache-Control": {"no-store"}},
			Body:       dashboard,
		}, nil
	case strings.HasSuffix(req.Path, "/requests"):
		body, err := json.Marshal(history.snapshot())
		return managementResponse{
			StatusCode: http.StatusOK,
			Headers:    http.Header{"Content-Type": {"application/json; charset=utf-8"}, "Cache-Control": {"no-store"}},
			Body:       body,
		}, err
	default:
		return managementResponse{StatusCode: http.StatusNotFound}, nil
	}
}
