package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync/atomic"
)

// targetTimezone is the default IANA zone used when the plugin config does
// not set one explicitly.
const targetTimezone = "America/Los_Angeles"

// configuredTimezone holds the active zone name and is swapped atomically on
// every (re)configure so hot reloads take effect without restarting the host.
var configuredTimezone atomic.Value // string

func init() { configuredTimezone.Store(targetTimezone) }

// currentTimezone returns the configured target timezone, falling back to the
// default before the first configuration is applied.
func currentTimezone() string {
	if zone, ok := configuredTimezone.Load().(string); ok && zone != "" {
		return zone
	}
	return targetTimezone
}

func timezoneTag() string { return "<timezone>" + currentTimezone() + "</timezone>" }
func environmentBlock() string {
	return "<environment_context>\n" + timezoneTag() + "\n</environment_context>"
}

var (
	environmentPattern = regexp.MustCompile(`(?is)<environment_context\s*>.*?</environment_context\s*>`)
	timezonePattern    = regexp.MustCompile(`(?is)<timezone\s*>(.*?)</timezone\s*>`)
	emptyZonePattern   = regexp.MustCompile(`(?is)<timezone\s*/>`)
	closingEnvPattern  = regexp.MustCompile(`(?is)</environment_context\s*>`)
)

type conversion struct {
	Original []string `json:"original"`
	Target   string   `json:"target"`
	Action   string   `json:"action"`
	Paths    []string `json:"paths"`
}

type normalizer struct {
	conversion
	changed bool
	blocks  int
}

func normalizeRequest(body []byte, sourceFormat string) ([]byte, conversion, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var request map[string]any
	if err := decoder.Decode(&request); err != nil || request == nil {
		return nil, conversion{}, fmt.Errorf("request body must be a JSON object")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, conversion{}, fmt.Errorf("request body must contain one JSON object")
	}
	n := normalizer{conversion: conversion{
		Original: []string{}, Target: currentTimezone(), Paths: []string{},
	}}
	n.textField(request, "instructions", "$.instructions")
	n.textField(request, "input", "$.input")
	n.contentField(request, "system", "$.system")
	for _, key := range []string{"input", "messages"} {
		messages, _ := request[key].([]any)
		for i, item := range messages {
			message, ok := item.(map[string]any)
			if !ok {
				continue
			}
			role, _ := message["role"].(string)
			if role != "system" && role != "developer" && role != "user" {
				continue
			}
			n.contentField(message, "content", fmt.Sprintf("$.%s[%d].content", key, i))
		}
	}
	if n.blocks == 0 {
		if err := n.inject(request, sourceFormat); err != nil {
			return nil, conversion{}, err
		}
	}
	switch {
	case len(n.Original) == 0:
		n.Action = "inserted"
	case n.changed:
		n.Action = "replaced"
	default:
		n.Action = "unchanged"
	}
	if !n.changed {
		return body, n.conversion, nil
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, conversion{}, fmt.Errorf("encode normalized request: %w", err)
	}
	return encoded, n.conversion, nil
}

func (n *normalizer) contentField(object map[string]any, key, path string) {
	n.textField(object, key, path)
	parts, _ := object[key].([]any)
	for i, item := range parts {
		part, ok := item.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := part["type"].(string)
		if kind == "text" || kind == "input_text" {
			n.textField(part, "text", fmt.Sprintf("%s[%d].text", path, i))
		}
	}
}

func (n *normalizer) textField(object map[string]any, key, path string) {
	text, ok := object[key].(string)
	if !ok {
		return
	}
	updated := environmentPattern.ReplaceAllStringFunc(text, func(block string) string {
		n.blocks++
		n.Paths = appendUnique(n.Paths, path)
		matches := timezonePattern.FindAllStringSubmatch(block, -1)
		for _, match := range matches {
			if original := strings.TrimSpace(match[1]); original != "" {
				n.Original = appendUnique(n.Original, original)
			}
		}
		tag := timezoneTag()
		if len(matches) > 0 {
			return timezonePattern.ReplaceAllString(block, tag)
		}
		if emptyZonePattern.MatchString(block) {
			return emptyZonePattern.ReplaceAllString(block, tag)
		}
		return closingEnvPattern.ReplaceAllStringFunc(block, func(closing string) string {
			return tag + "\n" + closing
		})
	})
	if updated != text {
		object[key] = updated
		n.changed = true
	}
}

func (n *normalizer) inject(request map[string]any, sourceFormat string) error {
	n.changed = true
	if strings.EqualFold(sourceFormat, "claude") {
		switch system := request["system"].(type) {
		case nil:
			request["system"] = environmentBlock()
		case string:
			request["system"] = appendContext(system)
		case []any:
			request["system"] = append(system, map[string]any{"type": "text", "text": environmentBlock()})
		default:
			return fmt.Errorf("unsupported system content")
		}
		n.Paths = append(n.Paths, "$.system")
		return nil
	}
	if messages, ok := request["messages"].([]any); ok {
		contextMessage := map[string]any{"role": "system", "content": environmentBlock()}
		request["messages"] = append([]any{contextMessage}, messages...)
		n.Paths = append(n.Paths, "$.messages[0].content")
		return nil
	}
	switch instructions := request["instructions"].(type) {
	case nil:
		request["instructions"] = environmentBlock()
	case string:
		request["instructions"] = appendContext(instructions)
	default:
		return fmt.Errorf("unsupported instructions content")
	}
	n.Paths = append(n.Paths, "$.instructions")
	return nil
}

func appendContext(text string) string {
	if text == "" {
		return environmentBlock()
	}
	return text + "\n\n" + environmentBlock()
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
