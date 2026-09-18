package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNormalizeExistingEnvironment(t *testing.T) {
	input := []byte(`{"model":"gpt-5.5","input":[{"role":"user","content":[{"type":"input_text","text":"<environment_context>\n<current_date>2026-09-14</current_date>\n<timezone>Asia/Shanghai</timezone>\n</environment_context>"},{"type":"input_image","image_url":"data:image/png;base64,AAA"}]}],"seed":9007199254740993,"tools":[{"type":"function","name":"zone","parameters":{"type":"object"}}]}`)
	out, info, err := normalizeRequest(input, "openai-response")
	if err != nil {
		t.Fatal(err)
	}
	if info.Action != "replaced" || len(info.Original) != 1 || info.Original[0] != "Asia/Shanghai" {
		t.Fatalf("unexpected conversion: %+v", info)
	}
	if !bytes.Contains(out, []byte(`9007199254740993`)) || bytes.Contains(out, []byte("Asia/Shanghai")) {
		t.Fatalf("number preservation or replacement failed")
	}
	var before, after map[string]json.RawMessage
	if err := json.Unmarshal(input, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out, &after); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"model", "seed", "tools"} {
		if !jsonEqual(before[key], after[key]) {
			t.Errorf("unrelated field changed: %s", key)
		}
	}
	if !bytes.Contains(out, []byte("2026-09-14")) || !bytes.Contains(out, []byte("data:image/png;base64,AAA")) {
		t.Error("date or image was not preserved")
	}
}

func TestNormalizeMissingTimezone(t *testing.T) {
	cases := []struct {
		name, body, source, expectedPath string
	}{
		{"responses input", `{"input":"hello","model":"gpt-5.5"}`, "openai-response", "$.instructions"},
		{"existing instructions", `{"input":[],"instructions":"Keep this instruction."}`, "openai-response", "$.instructions"},
		{"chat completions", `{"messages":[{"role":"user","content":"hello"}]}`, "openai", "$.messages[0].content"},
		{"context missing tag", `{"input":[{"role":"developer","content":"<environment_context>\n<cwd>/workspace</cwd>\n</environment_context>"}]}`, "openai-response", "$.input[0].content"},
		{"blank tag", `{"instructions":"<environment_context><timezone>  </timezone></environment_context>"}`, "openai-response", "$.instructions"},
		{"self closing tag", `{"input":"<environment_context><timezone/></environment_context>"}`, "openai-response", "$.input"},
		{"claude", `{"messages":[{"role":"user","content":"hello"}],"system":[{"type":"text","text":"Keep this instruction."}]}`, "claude", "$.system"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, info, err := normalizeRequest([]byte(tc.body), tc.source)
			if err != nil {
				t.Fatal(err)
			}
			if len(info.Original) != 0 || info.Action != "inserted" || len(info.Paths) != 1 || info.Paths[0] != tc.expectedPath {
				t.Fatalf("unexpected conversion: %+v", info)
			}
			if !json.Valid(out) || !bytes.Contains(out, []byte(targetTimezone)) {
				t.Fatalf("target timezone missing from valid JSON")
			}
			if strings.Contains(tc.body, "Keep this instruction.") && !bytes.Contains(out, []byte("Keep this instruction.")) {
				t.Error("existing instruction lost")
			}
			second, secondInfo, err := normalizeRequest(out, tc.source)
			if err != nil || secondInfo.Action != "unchanged" || !bytes.Equal(second, out) {
				t.Fatalf("normalization is not idempotent: %+v, %v", secondInfo, err)
			}
		})
	}
}

func TestNormalizeEscapedAndMultipleContexts(t *testing.T) {
	input := []byte(`{"input":[{"role":"user","content":[{"type":"input_text","text":"\u003cenvironment_context\u003e\u003ctimezone\u003eAsia/Shanghai\u003c/timezone\u003e\u003c/environment_context\u003e"}]},{"role":"developer","content":"<environment_context><timezone>Europe/London</timezone></environment_context>"}]}`)
	out, info, err := normalizeRequest(input, "openai-response")
	if err != nil || len(info.Original) != 2 || len(info.Paths) != 2 {
		t.Fatalf("unexpected multiple-context conversion: %+v %v", info, err)
	}
	if bytes.Count(out, []byte(targetTimezone)) != 2 || bytes.Contains(out, []byte("Europe/London")) || bytes.Contains(out, []byte("Asia/Shanghai")) {
		t.Error("escaped or repeated context was missed")
	}
}

func TestNormalizePreservesOrdinaryTextAndToolData(t *testing.T) {
	input := []byte(`{"input":[{"role":"user","content":"Explain Asia/Shanghai."},{"type":"function_call","call_id":"call_1","name":"read_file","arguments":"{\"timezone\":\"Asia/Shanghai\"}"},{"type":"function_call_output","call_id":"call_1","output":"<environment_context><timezone>Asia/Shanghai</timezone></environment_context>"},{"role":"assistant","content":[{"type":"output_text","text":"<environment_context><timezone>Asia/Shanghai</timezone></environment_context>"}]}],"previous_response_id":"resp_1"}`)
	out, info, err := normalizeRequest(input, "openai-response")
	if err != nil || info.Action != "inserted" {
		t.Fatalf("unexpected conversion: %+v %v", info, err)
	}
	var before, after map[string]json.RawMessage
	json.Unmarshal(input, &before)
	json.Unmarshal(out, &after)
	if !jsonEqual(before["input"], after["input"]) || !jsonEqual(before["previous_response_id"], after["previous_response_id"]) {
		t.Error("ordinary text, tool calls, history, or response linkage changed")
	}
}

func TestNormalizeRejectsMalformedPayload(t *testing.T) {
	for _, input := range []string{`null`, `[]`, `{"input":`, `{} {}`, `{"instructions":{"invalid":true}}`} {
		if _, _, err := normalizeRequest([]byte(input), "openai-response"); err == nil {
			t.Errorf("expected an error for %q", input)
		}
	}
}

func jsonEqual(a, b []byte) bool {
	var left, right any
	decode := func(raw []byte, target *any) {
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		d.Decode(target)
	}
	decode(a, &left)
	decode(b, &right)
	x, _ := json.Marshal(left)
	y, _ := json.Marshal(right)
	return bytes.Equal(x, y)
}
