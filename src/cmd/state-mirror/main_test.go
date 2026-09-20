package main

import (
	"net/http/httptest"
	"strings"
	"testing"

	"cpa-timezone/internal/statemirror"
)

func TestPublicRoutesHaveNoWriteOrCPAPaths(t *testing.T) {
	r, _ := statemirror.NewReceiver("", []byte(strings.Repeat("k", 32)))
	handler := publicHandler(r)
	for _, path := range []string{"/ingest", "/probe-control", "/v0/management/timezone-override/requests", "/state/../ingest"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 404 {
			t.Fatalf("unexpected public route %s", path)
		}
	}
	for _, path := range []string{"/state", "/state/snapshot", "/state/events", "/ingest"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("POST", path, strings.NewReader(`{"action":"start-round"}`)))
		if w.Code != 405 {
			t.Fatal("public endpoint accepted a mutation")
		}
	}
	for _, path := range []string{"/state", "/state/", "/state/app.js", "/state/style.css", "/state/favicon.png", "/state/snapshot"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Security-Policy") == "" {
			t.Fatalf("invalid public response %s", path)
		}
		if strings.Contains(w.Body.String(), "probe-control") || strings.Contains(w.Body.String(), "management/") {
			t.Fatal("management code in public asset")
		}
	}
}
