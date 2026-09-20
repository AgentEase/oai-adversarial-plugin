package main

import (
	"fmt"
	"sync/atomic"
)

// Defaults are empirical compatibility values, not account/plan identifiers.
// Operators may replace the entire list; no Team-account rule is inferred.
var stateLengthPolicy atomic.Value // immutable []int

func init() { stateLengthPolicy.Store([]int{292, 332}) }

func acceptedStateLengths() []int { return stateLengthPolicy.Load().([]int) }

func isAcceptedStateLength(length int) bool {
	for _, accepted := range acceptedStateLengths() {
		if length == accepted {
			return true
		}
	}
	return false
}

func parseStateLengths(raw any) ([]int, error) {
	if raw == nil {
		return []int{292, 332}, nil
	}
	items, ok := raw.([]any)
	if !ok || len(items) == 0 || len(items) > 32 {
		return nil, fmt.Errorf("accepted-state-lengths must contain 1-32 distinct integer lengths (1-4096)")
	}
	lengths := make([]int, 0, len(items))
	seen := map[int]bool{}
	for _, item := range items {
		n, ok := item.(int)
		if !ok || n < 1 || n > turnStateValueLimit || seen[n] {
			return nil, fmt.Errorf("accepted-state-lengths must contain 1-32 distinct integer lengths (1-4096)")
		}
		seen[n] = true
		lengths = append(lengths, n)
	}
	return lengths, nil
}
