package main

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultStateLengthPolicy(t *testing.T) {
	for _, n := range []int{0, 291, 292, 312, 332, 356, 400} {
		want := n == 292 || n == 332
		if isAcceptedStateLength(n) != want {
			t.Fatalf("length %d", n)
		}
		if isHealthyTurnState("gpt-6-astra", "gpt-6-astra", strings.Repeat("x", n)) != want {
			t.Fatalf("healthy %d", n)
		}
		if isHealthyTurnState("gpt-6-astra", "gpt-5.6-luna", strings.Repeat("x", n)) {
			t.Fatalf("mismatch %d", n)
		}
	}
}

func TestLengthPolicyRechecksCachedTicketsAndMirror(t *testing.T) {
	e := newPrefetchTestEngine(t)
	old := append([]int(nil), acceptedStateLengths()...)
	t.Cleanup(func() { stateLengthPolicy.Store(old) })
	now := time.Now().UTC()
	value := synthStateToken(now)
	entry := stateEntry{Value: value, ValueLength: len(value), Valid: true}
	e.values["gpt-6-astra"] = entry
	e.candidates["gpt-6-astra"] = entry
	stateLengthPolicy.Store([]int{len(value)})
	if e.activeValueFor("gpt-6-astra") == "" || !mirrorTicket(entry, time.Hour).Valid {
		t.Fatal("configured length did not accept cached ticket")
	}
	stateLengthPolicy.Store([]int{308})
	e.candidates["gpt-6-astra"] = entry
	if e.activeValueFor("gpt-6-astra") != "" || mirrorTicket(entry, time.Hour).Valid || e.settledBaseline("gpt-6-astra", e.cfg.Config, now) {
		t.Fatal("old acceptance policy remained active on cached ticket")
	}
	delete(e.values, "gpt-6-astra")
	e.mu.Lock()
	promoted := e.promoteCandidateLocked("gpt-6-astra", e.cfg.Config, now)
	e.mu.Unlock()
	if promoted {
		t.Fatal("disallowed candidate was promoted")
	}
}

func TestConfiguredStateLengthPolicy(t *testing.T) {
	old := append([]int(nil), acceptedStateLengths()...)
	t.Cleanup(func() { stateLengthPolicy.Store(old) })
	for _, config := range []string{
		"accepted-state-lengths: [308, 352]\n",
		"turn-state-override:\n  accepted-state-lengths: [308, 352]\n",
	} {
		if err := configureTurnStateOverride([]byte(config)); err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{292, 308, 312, 332, 352, 356} {
			if isAcceptedStateLength(n) != (n == 308 || n == 352) {
				t.Fatalf("configured policy ignored for length %d", n)
			}
		}
		if !isHealthyTurnState("gpt-6-astra", "gpt-6-astra", strings.Repeat("x", 308)) ||
			isHealthyTurnState("gpt-6-astra", "gpt-5.6-luna", strings.Repeat("x", 308)) {
			t.Fatal("length configuration bypassed model consistency")
		}
	}
	for _, invalid := range []string{"[]", "[0]", "[-1]", "[4097]", "[292,292]", "[292.5]", "['292']", "secret-invalid"} {
		for _, prefix := range []string{"accepted-state-lengths: ", "turn-state-override:\n  accepted-state-lengths: "} {
			if err := configureTurnStateOverride([]byte(prefix + invalid)); err == nil {
				t.Fatalf("invalid length policy accepted: %s", invalid)
			}
			if !isAcceptedStateLength(308) || isAcceptedStateLength(292) {
				t.Fatal("rejected policy replaced effective lengths")
			}
		}
	}
	if err := configureTurnStateOverride([]byte("enabled: true\n")); err != nil {
		t.Fatal(err)
	}
	if !isAcceptedStateLength(292) || !isAcceptedStateLength(332) || isAcceptedStateLength(308) {
		t.Fatal("omitted policy did not restore defaults")
	}
}
