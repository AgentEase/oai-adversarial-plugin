package main

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// Unit cases replace package-global engines. They test save/load explicitly;
	// a process-wide flush goroutine must not outlive those independent fixtures.
	// Exercise the actual once-only startup/flush lifecycle in its own process.
	if os.Getenv("LKS_TEST_ACCOUNT_STARTUP") != "1" {
		persistOnce.Do(func() {})
	}
	_ = os.Setenv("LKS_MIRROR_URL", "")
	os.Exit(m.Run())
}
