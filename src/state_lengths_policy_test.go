package main

import (
	"strings"
	"testing"
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
