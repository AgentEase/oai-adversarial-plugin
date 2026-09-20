package main

import (
	"cpa-timezone/internal/statemirror"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func accountTestEngine(t *testing.T) *probeEngine {
	t.Helper()
	e := newPrefetchTestEngine(t)
	list, get := hostAuthListFunc, hostAuthGetFunc
	old := currentTurnStateOverride()
	t.Cleanup(func() {
		e.stopPrefetchWatcher()
		e.stop()
		waitPrefetchIdle(t, e)
		hostAuthListFunc, hostAuthGetFunc = list, get
		history.mu.Lock()
		history.records = nil
		history.mu.Unlock()
		if old != nil {
			turnStateOverride.Store(old)
		} else {
			turnStateOverride.Store(&turnStateOverrideState{})
		}
	})
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{
			{ID: "fixture-A", AuthIndex: "index-A", Email: "alpha@example.test", Provider: "codex", Path: "/fixtures/A.json", Priority: 2},
			{ID: "fixture-B", AuthIndex: "index-B", Email: "beta@example.test", Provider: "codex", Path: "/fixtures/B.json", Priority: 1},
			{ID: "fixture-disabled", AuthIndex: "index-disabled", Provider: "codex", Disabled: true, Priority: 99},
		}, nil
	}
	hostAuthGetFunc = func(index string) (json.RawMessage, error) {
		return json.Marshal(map[string]string{"access_token": "synthetic-" + index, "account_id": "workspace-" + index})
	}
	e.cfg.Config.AccountMode = "all-accounts"
	e.cfg.Config.MaxAttemptsPerRound = 1
	e.cfg.Config.ExitSuccessCooldown = 0
	e.syncProbeAccounts()
	turnStateOverride.Store(&turnStateOverrideState{Config: turnStateOverrideConfig{Enabled: true, Models: e.cfg.Config.Models}})
	return e
}

func accountTarget(id string) string {
	return scopedTarget(credentialScope(id, "synthetic-index-"+strings.TrimPrefix(id, "fixture-"), "workspace-index-"+strings.TrimPrefix(id, "fixture-")), "gpt-6-astra")
}

func scopedRequest(t *testing.T, id, clientTicket string) interceptResponse {
	t.Helper()
	raw, err := json.Marshal(interceptRequest{RequestID: "account-request", ToFormat: "codex", Model: "gpt-6-astra",
		Metadata: map[string]any{"selected_auth_id": id, "selected_auth_index": "index-" + strings.TrimPrefix(id, "fixture-")}, Headers: http.Header{turnStateHeader: {clientTicket}, "Authorization": {"Bearer synthetic-client"}, "Chatgpt-Account-Id": {"untrusted-client-workspace"}}, Body: []byte(`{"input":"test"}`)})
	if err != nil {
		t.Fatal(err)
	}
	out, err := intercept(raw)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAccountTicketsNeverCrossRequestCredentials(t *testing.T) {
	e := accountTestEngine(t)
	if selectedAccount(map[string]any{"selected_auth_id": "fixture-A", "selected_auth_index": "index-B"}, nil) != "" {
		t.Fatal("mismatched host ID and index were trusted")
	}
	a, b := synthStateToken(time.Now().Add(-time.Minute)), synthStateToken(time.Now())
	e.storeValue(accountTarget("fixture-A"), a, "probe", "", e.cfg.Config)
	e.storeValue(accountTarget("fixture-B"), b, "probe", "", e.cfg.Config)
	// force:false does not permit A's healthy-length ticket to survive a retry on B.
	if got := scopedRequest(t, "fixture-B", a); got.Headers.Get(turnStateHeader) != b {
		t.Fatal("B did not receive its own ticket")
	}
	if got := scopedRequest(t, "fixture-A", b); got.Headers.Get(turnStateHeader) != a {
		t.Fatal("A did not receive its own ticket")
	}
	delete(e.values, accountTarget("fixture-B"))
	for _, id := range []string{"fixture-B", ""} {
		got := scopedRequest(t, id, a)
		if got.Headers.Get(turnStateHeader) != "" || len(got.ClearHeaders) != 1 || got.ClearHeaders[0] != turnStateHeader {
			t.Fatal("unbound client ticket was forwarded or borrowed")
		}
	}
	// Neither legacy shared slots nor a global static value can supply B.
	e.storeValue("gpt-6-astra", a, "seed", "", e.cfg.Config)
	turnStateOverride.Store(&turnStateOverrideState{Config: turnStateOverrideConfig{Enabled: true, Models: e.cfg.Config.Models, Value: a}})
	if got := scopedRequest(t, "fixture-B", a); got.Headers.Get(turnStateHeader) != "" {
		t.Fatal("legacy/static ticket crossed accounts")
	}
}

func TestAccountBusinessObservationAndFailureAreIsolated(t *testing.T) {
	e := accountTestEngine(t)
	a := synthStateToken(time.Now())
	scopedRequest(t, "fixture-A", "")
	raw, _ := json.Marshal(responseInterceptRequest{RequestID: "account-request", Model: "gpt-6-astra", Metadata: map[string]any{"selected_auth_id": "fixture-A"}, ResponseHeaders: http.Header{turnStateHeader: {a}}, Body: []byte(`{"model":"gpt-6-astra","object":"response"}`)})
	if _, err := interceptNonStreamingResponse(raw); err != nil {
		t.Fatal(err)
	}
	if e.activeValueFor(accountTarget("fixture-A")) != a || e.activeValueFor(accountTarget("fixture-B")) != "" {
		t.Fatal("response captured under the wrong account")
	}
	// B's stream may be repaired only by B's baseline, never A's.
	scopedRequest(t, "fixture-B", "")
	stream, _ := json.Marshal(streamChunkInterceptRequest{RequestID: "account-request", Model: "gpt-6-astra", Metadata: map[string]any{"selected_auth_id": "fixture-B"}, ResponseHeaders: http.Header{turnStateHeader: {"bad"}}})
	out, err := interceptStreamChunk(stream)
	if err != nil || out.Headers.Get(turnStateHeader) != "" {
		t.Fatal("stream borrowed another account baseline")
	}
	if scopedRequest(t, "fixture-A", "").Terminate || !scopedRequest(t, "fixture-B", "").Terminate {
		t.Fatal("business failure contaminated another account")
	}
	// AuthID-only usage cannot attribute a ticket after a credential replacement.
	usage, _ := json.Marshal(usageRecord{AuthID: "fixture-B", Provider: "codex", Model: "gpt-6-astra", ResponseModel: "gpt-6-astra", ResponseHeaders: http.Header{turnStateHeader: {a}}})
	if err := observeAccountUsage(usage); err != nil || e.activeValueFor(accountTarget("fixture-B")) != "" {
		t.Fatal("unbound usage captured a ticket")
	}
}

func TestAccountSlotCapacityAndPersistence(t *testing.T) {
	e := accountTestEngine(t)
	e.cfg.Config.Models = []string{"gpt-6-astra", "gpt-5.6-sol", "checked-model-three", "checked-model-four"}
	e.mu.Lock()
	targets := e.probeTargetsLocked(e.cfg.Config)
	e.mu.Unlock()
	if len(targets) != 8 {
		t.Fatalf("two accounts x four models: got %d targets", len(targets))
	}
	for _, target := range targets {
		e.storeValue(target, synthStateToken(time.Now().Add(-time.Minute)), "probe", "", e.cfg.Config)
		e.storeValue(target, synthStateToken(time.Now()), "probe", "", e.cfg.Config)
	}
	if len(e.values) != 8 || len(e.candidates) != 8 {
		t.Fatal("expected N x M x 2 independent slots")
	}
	e.paused[targets[0]] = true
	state := collectState()
	e.values = map[string]stateEntry{}
	e.candidates = map[string]stateEntry{}
	e.paused = map[string]bool{}
	applyPersistedState(state)
	if len(e.values) != 8 || len(e.candidates) != 8 || !e.paused[targets[0]] {
		t.Fatal("scope lost on persistence roundtrip")
	}
	for _, target := range targets {
		if e.activeValueFor(target) == "" {
			t.Fatal("restored ticket not available in its own scope")
		}
	}
	snapshot := e.mirrorState(time.Now())
	if len(snapshot.Models) != 4 || snapshot.Models[0].AccountsTotal != 2 || snapshot.Models[0].AccountsReady != 2 || snapshot.Models[0].Active != nil {
		t.Fatal("public mirror must aggregate, not pick an arbitrary account TTL")
	}
	encoded, _ := json.Marshal(snapshot)
	if strings.Contains(string(encoded), "fixture-") || strings.Contains(string(encoded), "auth-") {
		t.Fatal("public mirror leaked account identity")
	}
}

func TestProbeTargetPinsCredentialAndStopsOnRemoval(t *testing.T) {
	e := accountTestEngine(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer synthetic-index-B" || r.Header.Get("Chatgpt-Account-Id") != "workspace-index-B" {
			t.Error("wrong credential used")
		}
		w.Header().Set(turnStateHeader, synthStateToken(time.Now()))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n"))
	}))
	defer server.Close()
	cfg := e.cfg.Config
	cfg.UpstreamURL = server.URL
	record, value := e.probeOnce(accountTarget("fixture-B"), "direct", cfg)
	if !record.Success || value == "" || record.Account != targetAccount(accountTarget("fixture-B")) {
		t.Fatal("scoped probe failed")
	}
	original := hostAuthListFunc
	hostAuthListFunc = func() ([]hostAuthEntry, error) { entries, _ := original(); return entries[:1], nil }
	record, value = e.probeOnce(accountTarget("fixture-B"), "direct", cfg)
	if value != "" || record.Success || calls.Load() != 1 {
		t.Fatal("removed B fell back to A")
	}
}

func TestAccountPrefetchAndControlsRemainScoped(t *testing.T) {
	e := accountTestEngine(t)
	e.queueActive = true // inspect ordering without running any network request
	e.prefetchScan()
	if len(e.queue) != 2 || e.queue[0].Model == e.queue[1].Model {
		t.Fatal("all accounts were not queued independently")
	}
	a, b := accountTarget("fixture-A"), accountTarget("fixture-B")
	if !e.setModelPaused(a, true) || len(e.queue) != 1 || e.queue[0].Model != b {
		t.Fatal("pausing A affected B's task")
	}
	e.queue = nil
	e.halted = true
	e.prefetchScan()
	if len(e.queue) != 0 {
		t.Fatal("account scanner bypassed stop")
	}
	e.halted = false
	e.cfg.Config.SleepHours = sleepingHoursNow()
	e.prefetchScan()
	if len(e.queue) != 0 {
		t.Fatal("account scanner bypassed sleep")
	}
	e.queueActive = false
	e.stopping = false
	e.running = false
}

func TestFixedModeRequiresExactHostFileBinding(t *testing.T) {
	e := accountTestEngine(t)
	e.cfg.Config.AccountMode = "fixed"
	e.cfg.Config.CredFile = "/fixtures/B.json"
	e.mu.Lock()
	targets := e.probeTargetsLocked(e.cfg.Config)
	e.mu.Unlock()
	if len(targets) != 1 || targets[0] != accountTarget("fixture-B") {
		t.Fatal("fixed file did not bind the exact host identity")
	}
	e.cfg.Config.CredFile = "/different/B.json"
	e.mu.Lock()
	targets = e.probeTargetsLocked(e.cfg.Config)
	e.mu.Unlock()
	if len(targets) != 0 {
		t.Fatal("fixed file matched by basename instead of exact path")
	}
}

func TestCredentialReplacementNeverReusesSlotsOrLateResponses(t *testing.T) {
	e := accountTestEngine(t)
	oldTarget := accountTarget("fixture-A")
	oldTicket := synthStateToken(time.Now().Add(-time.Minute))
	e.storeValue(oldTarget, oldTicket, "probe", "", e.cfg.Config)
	scopedRequest(t, "fixture-A", "") // old in-flight request captures its owner
	original := hostAuthGetFunc
	hostAuthGetFunc = func(index string) (json.RawMessage, error) {
		if index == "index-A" {
			return json.Marshal(map[string]string{"access_token": "replacement-token", "account_id": "replacement-workspace"})
		}
		return original(index)
	}
	// Same CPA AuthID/path, different upstream principal. Old queued work stops.
	if _, err := e.credentialForTarget(oldTarget, e.cfg.Config); err == nil {
		t.Fatal("old task used a replaced credential")
	}
	e.syncProbeAccounts()
	newScope := credentialScope("fixture-A", "replacement-token", "replacement-workspace")
	newTarget := scopedTarget(newScope, "gpt-6-astra")
	if newTarget == oldTarget || e.activeValueFor(newTarget) != "" {
		t.Fatal("replacement inherited old slots")
	}
	late := synthStateToken(time.Now())
	raw, _ := json.Marshal(responseInterceptRequest{RequestID: "account-request", Model: "gpt-6-astra", Metadata: map[string]any{"selected_auth_id": "fixture-A"}, ResponseHeaders: http.Header{turnStateHeader: {late}}, Body: []byte(`{"object":"response","model":"gpt-6-astra"}`)})
	if _, err := interceptNonStreamingResponse(raw); err != nil {
		t.Fatal(err)
	}
	if e.candidates[oldTarget].Value != late || e.activeValueFor(newTarget) != "" {
		t.Fatal("late response attributed to replacement")
	}
	req, _ := json.Marshal(interceptRequest{RequestID: "replacement-request", ToFormat: "codex", Model: "gpt-6-astra", Metadata: map[string]any{"selected_auth_id": "fixture-A", "selected_auth_index": "index-A"}, Headers: http.Header{"Authorization": {"Bearer replacement-token"}, "Chatgpt-Account-Id": {"replacement-workspace"}, turnStateHeader: {oldTicket}}, Body: []byte(`{"input":"test"}`)})
	out, err := intercept(req)
	if err != nil || out.Headers.Get(turnStateHeader) != "" || len(out.ClearHeaders) != 1 {
		t.Fatal("replacement reused old ticket")
	}
	if e.targetAllowedLocked(newTarget) {
		t.Fatal("replacement identity was enabled without a host restart")
	}
	hostAuthGetFunc = original
	e.syncProbeAccounts()
	if selectedAccount(map[string]any{"selected_auth_id": "fixture-A", "selected_auth_index": "index-A"}, nil) != "" {
		t.Fatal("restoring old credentials silently cleared quarantine")
	}
	fresh := &probeEngine{}
	if !fresh.bindAccountLocked("fixture-A", newScope) {
		t.Fatal("new host process cannot bind replacement identity")
	}
}

func TestCredentialScopeRefreshAndWorkspaceIsolation(t *testing.T) {
	token := func(subject, nonce string) string {
		raw, _ := json.Marshal(map[string]string{"iss": "https://issuer.example.test", "sub": subject, "nonce": nonce})
		return "synthetic." + base64.RawURLEncoding.EncodeToString(raw) + ".signature"
	}
	a := credentialScope("same-file", token("user-a", "old"), "workspace-a")
	if a == "" || a != credentialScope("same-file", token("user-a", "new"), "workspace-a") {
		t.Fatal("same principal refresh lost slots")
	}
	for _, other := range []string{
		credentialScope("same-file", token("user-b", "old"), "workspace-a"),
		credentialScope("same-file", token("user-a", "old"), "workspace-b"),
		credentialScope("another-file", token("user-a", "old"), "workspace-a"),
	} {
		if a == other {
			t.Fatal("different user, workspace or credential shared slots")
		}
	}
	if credentialScope("same-file", "opaque-old", "") == credentialScope("same-file", "opaque-new", "") {
		t.Fatal("opaque replacement shared slots")
	}
	if selectedAccount(map[string]any{"selected_auth_id": "same-file"}, http.Header{"Chatgpt-Account-Id": {"workspace-a"}}) != "" {
		t.Fatal("identity header without actual upstream credential was trusted")
	}
}

func TestHighestPriorityDetectsCredentialReplacement(t *testing.T) {
	e := accountTestEngine(t)
	e.cfg.Config.AccountMode = "highest-priority"
	e.queueActive = true
	e.authSelectionScan()
	if len(e.queue) != 0 {
		t.Fatal("first scan must remain passive")
	}
	original := hostAuthGetFunc
	hostAuthGetFunc = func(index string) (json.RawMessage, error) {
		if index == "index-A" {
			return json.Marshal(map[string]string{"access_token": "replacement-token", "account_id": "replacement-workspace"})
		}
		return original(index)
	}
	e.authSelectionScan()
	if len(e.queue) != 0 || e.accountBindings["fixture-A"] != "" {
		t.Fatal("same file identity replacement must stay quarantined until restart")
	}
	e.queue = nil
	e.queueActive = false
}

func TestAccountOverridePreservesDisabledAndExemptModels(t *testing.T) {
	accountTestEngine(t)
	turnStateOverride.Store(&turnStateOverrideState{})
	if value, status := applyScopedOverride("", "gpt-6-astra", "", nil); value != nil || status != "" {
		t.Fatal("disabled rewrite changed unknown account request")
	}
	turnStateOverride.Store(&turnStateOverrideState{Config: turnStateOverrideConfig{Enabled: true, Models: []string{"gpt-6-astra"}}})
	if value, status := applyScopedOverride("", "gpt-5.6-luna", "", nil); value != nil || status != "" {
		t.Fatal("exempt model rewritten")
	}
}

func TestAccountMirrorDoesNotReportOnePausedAccountAsAllPaused(t *testing.T) {
	rows := []statemirror.Model{
		{Name: accountTarget("fixture-A"), Detection: true, Activity: "paused", Evidence: "none"},
		{Name: accountTarget("fixture-B"), Detection: true, Activity: "standby", Evidence: "none"},
	}
	for i := 0; i < 2; i++ {
		got := aggregateAccountMirror(rows, []string{"gpt-6-astra"}, time.Now())
		if len(got) != 1 || got[0].Activity != "standby" || got[0].AccountsTotal != 2 || got[0].Active != nil {
			t.Fatal("aggregate depends on first account")
		}
		rows[0], rows[1] = rows[1], rows[0]
	}
}

func TestOlderHostClearHeadersAndResponseBinding(t *testing.T) {
	e := accountTestEngine(t)
	ticket := synthStateToken(time.Now())
	out := scopedRequest(t, "fixture-B", ticket)
	// The host drops ClearHeaders, then merges its resulting map into the
	// original upstream request. An explicit empty replacement survives both.
	merged := http.Header{turnStateHeader: {ticket}}
	for key, values := range out.Headers {
		merged[key] = values
	}
	if merged.Get(turnStateHeader) != "" {
		t.Fatal("old host merge resurrected client ticket")
	}
	raw, _ := json.Marshal(responseInterceptRequest{RequestID: "account-request", Model: "gpt-6-astra", ResponseHeaders: http.Header{turnStateHeader: {ticket}}, Body: []byte(`{"object":"response","model":"gpt-6-astra"}`)})
	if _, err := interceptNonStreamingResponse(raw); err != nil {
		t.Fatal(err)
	}
	if e.activeValueFor(accountTarget("fixture-B")) != ticket || e.activeValueFor(accountTarget("fixture-A")) != "" {
		t.Fatal("metadata-free response lost exact request binding")
	}
	if responseAccount("account-request", "fixture-A") != "" || responseAccount("missing-request", "") != "" {
		t.Fatal("conflicting or missing request binding was trusted")
	}
}

func TestAccountScanRechecksOperatorGateAfterHostCallback(t *testing.T) {
	for _, mode := range []string{"stop", "sleep", "zero"} {
		t.Run(mode, func(t *testing.T) {
			e := accountTestEngine(t)
			e.queueActive = true
			original := hostAuthListFunc
			hostAuthListFunc = func() ([]hostAuthEntry, error) {
				e.mu.Lock()
				switch mode {
				case "stop":
					e.halted = true
				case "sleep":
					e.cfg.Config.SleepHours = sleepingHoursNow()
				case "zero":
					e.cfg.Config.Prefetch = 0
				}
				e.mu.Unlock()
				return original()
			}
			e.prefetchScan()
			if len(e.queue) != 0 {
				t.Fatal("scan ignored operator change during callback")
			}
			e.queueActive = false
		})
	}
}

func TestAccountStartupRestoresOwnedSlots(t *testing.T) {
	if os.Getenv("LKS_TEST_ACCOUNT_STARTUP") == "1" {
		hostAuthListFunc = func() ([]hostAuthEntry, error) {
			return []hostAuthEntry{{ID: "fixture-A", AuthIndex: "index-A", Provider: "codex"}, {ID: "fixture-B", AuthIndex: "index-B", Provider: "codex"}}, nil
		}
		hostAuthGetFunc = func(index string) (json.RawMessage, error) {
			return json.Marshal(map[string]string{"access_token": "synthetic-" + index, "account_id": "workspace-" + index})
		}
		if err := configureTurnStateOverride([]byte("operation-mode: business-only\noverride-models: [gpt-6-astra]\n")); err != nil {
			t.Fatal(err)
		}
		defer probeTrack.stopPrefetchWatcher()
		defer closePersistence()
		a := probeTrack.activeValueFor(accountTarget("fixture-A"))
		b := probeTrack.activeValueFor(accountTarget("fixture-B"))
		if a == "" || b == "" || a == b {
			t.Fatal("startup lost account attribution")
		}
		if got := scopedRequest(t, "fixture-B", a); got.Headers.Get(turnStateHeader) != b {
			t.Fatal("restart allowed cross-account injection")
		}
		if got := scopedRequest(t, "unknown-account", a); got.Headers.Get(turnStateHeader) != "" {
			t.Fatal("legacy slot assigned to an unknown account")
		}
		probeTrack.mu.Lock()
		entry := probeTrack.values[accountTarget("fixture-A")]
		entry.Value = synthStateToken(time.Now().Add(-2 * probeTrack.cfg.Config.TTL))
		probeTrack.values[entry.Model] = entry
		probeTrack.mu.Unlock()
		if successor := probeTrack.activeValueFor(accountTarget("fixture-A")); successor == "" || successor == a || successor == b {
			t.Fatal("candidate handoff lost its account")
		}
		if probeTrack.activeValueFor(accountTarget("fixture-B")) != b {
			t.Fatal("A handoff changed B")
		}
		return
	}
	now := time.Now().UTC()
	entry := func(target string, issued time.Time) stateEntry {
		value := synthStateToken(issued)
		return stateEntry{Model: target, Value: value, ValueLength: len(value), Valid: true}
	}
	state := persistedState{StateVersion: 2,
		Values:     []stateEntry{entry(accountTarget("fixture-A"), now.Add(-2*time.Minute)), entry(accountTarget("fixture-B"), now.Add(-time.Minute)), entry("gpt-6-astra", now.Add(-3*time.Minute))},
		Candidates: []stateEntry{entry(accountTarget("fixture-A"), now)},
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(file, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestAccountStartupRestoresOwnedSlots$")
	cmd.Env = append(os.Environ(), "LKS_TEST_ACCOUNT_STARTUP=1", "LKS_TZ_STATE_FILE="+file, "LKS_MIRROR_URL=")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("isolated startup failed: %v\n%s", err, output)
	}
}
