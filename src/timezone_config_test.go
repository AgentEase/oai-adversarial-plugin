package main

import (
	"strings"
	"testing"
)

func TestInvalidConfigDoesNotActivateTimezone(t *testing.T) {
	t.Cleanup(func() { configuredTimezone.Store(targetTimezone) })
	if err := configureTurnStateOverride([]byte("timezone: America/New_York\n")); err != nil {
		t.Fatal(err)
	}
	err := configureTurnStateOverride([]byte("timezone: Asia/Singapore\nturn-state-override:\n  enabled: true\n  models: []\n  value: review-fixture\n"))
	if err == nil {
		t.Fatal("invalid configuration unexpectedly accepted")
	}
	if currentTimezone() != "America/New_York" {
		t.Fatalf("rejected configuration still changed target timezone to %s", currentTimezone())
	}
}

func TestRequestUsesOneTimezoneSnapshot(t *testing.T) {
	t.Cleanup(func() { configuredTimezone.Store(targetTimezone) })
	configuredTimezone.Store("Asia/Singapore")
	n := normalizer{conversion: conversion{Target: currentTimezone()}}
	// Model a config reload after normalizeRequest captures conversion.Target
	// but before it traverses the request's content fields.
	configuredTimezone.Store("America/New_York")
	obj := map[string]any{"input": "<environment_context><timezone>UTC</timezone></environment_context>"}
	n.textField(obj, "input", "$.input")
	if !strings.Contains(obj["input"].(string), n.Target) {
		t.Fatalf("request target=%s but body=%s", n.Target, obj["input"])
	}
}
