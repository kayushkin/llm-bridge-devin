package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
)

type eventRecorder struct {
	mu     sync.Mutex
	events []msg.Event
}

func (r *eventRecorder) emit(event msg.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *eventRecorder) waitFor(t *testing.T, timeout time.Duration, match func(msg.Event) bool) msg.Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		for _, event := range r.events {
			if match(event) {
				r.mu.Unlock()
				return event
			}
		}
		r.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var types []string
	for _, event := range r.events {
		types = append(types, string(event.Type))
	}
	t.Fatalf("no matching event within %s; saw %v", timeout, types)
	return msg.Event{}
}

func gatedStart(workDir string) StartParams {
	return StartParams{
		BridgeSessionID: "br_test",
		WorkDir:         workDir,
		Model:           "swe-2-flash-medium",
		DevinHooks: map[string][]json.RawMessage{
			"PreToolUse": {json.RawMessage(`{"matcher":".*","hooks":[{"type":"command","command":"true"}]}`)},
		},
	}
}

func startFakeBridge(t *testing.T, behavior string, tweak func(*Config)) (*Bridge, *eventRecorder, string) {
	t.Helper()
	cfg, logPath := fakeDevinConfig(t, behavior)
	if tweak != nil {
		tweak(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	recorder := &eventRecorder{}
	bridge := NewBridge(ctx, cfg, recorder.emit)
	t.Cleanup(func() {
		bridge.Shutdown()
		cancel()
	})
	if err := bridge.HandleStart(gatedStart(t.TempDir())); err != nil {
		t.Fatalf("start: %v", err)
	}
	return bridge, recorder, logPath
}

func TestTurnTranslatesToolCallsTextAndUsage(t *testing.T) {
	bridge, recorder, _ := startFakeBridge(t, "normal", nil)
	if err := bridge.HandleMessage(MessageParams{Content: "list files"}); err != nil {
		t.Fatal(err)
	}
	result := recorder.waitFor(t, 10*time.Second, func(e msg.Event) bool { return e.Type == msg.EventResult })
	if result.Result.Text != "done: a.txt" {
		t.Errorf("result text = %q, want the text after the last tool call", result.Result.Text)
	}
	usage := result.Result.Usage
	// Two model calls (90/10 and 100/20); devin's prompt answer reports only
	// the last (100/20), so the turn's usage must be the sum.
	if usage.InputTokens != 190 || usage.OutputTokens != 30 || usage.TotalTokens != 220 || usage.CacheReadTokens != 40 || usage.ContextTokens != 110 || usage.ContextLimit != 1000 {
		t.Errorf("usage = %+v, want the sum of both calls", usage)
	}
	if result.Result.APICalls != 2 || result.Result.Model != "swe-2-flash" {
		t.Errorf("api calls %d model %q", result.Result.APICalls, result.Result.Model)
	}
	if result.HarnessSessionID != "fake-session" || result.BridgeSessionID != "br_test" {
		t.Errorf("ids: bridge %q harness %q", result.BridgeSessionID, result.HarnessSessionID)
	}
	call := recorder.waitFor(t, time.Second, func(e msg.Event) bool { return e.Type == msg.EventToolCall })
	if call.ToolCall.Name != "exec" || string(call.ToolCall.Input) != `{"command":"ls"}` {
		t.Errorf("tool call = %+v", call.ToolCall)
	}
	toolResult := recorder.waitFor(t, time.Second, func(e msg.Event) bool { return e.Type == msg.EventToolResult })
	if toolResult.ToolResult.Output != "a.txt" || toolResult.ToolResult.IsError {
		t.Errorf("tool result = %+v", toolResult.ToolResult)
	}
	for _, event := range recorder.events {
		if event.Type != msg.EventSessionInfo && len(event.Raw) == 0 && event.Type != msg.EventResult {
			t.Errorf("%s event carries no raw frame", event.Type)
		}
	}
}

func TestStartRefusesWithoutPermissionGate(t *testing.T) {
	cfg, _ := fakeDevinConfig(t, "normal")
	bridge := NewBridge(context.Background(), cfg, (&eventRecorder{}).emit)
	defer bridge.Shutdown()
	params := gatedStart(t.TempDir())
	params.DevinHooks = nil
	err := bridge.HandleStart(params)
	if err == nil || !strings.Contains(err.Error(), "permission gate") {
		t.Fatalf("start without devin_hooks: %v, want a refusal naming the gate", err)
	}
	bridge.mu.Lock()
	running := bridge.process != nil
	bridge.mu.Unlock()
	if running {
		t.Fatal("devin was started without a gate")
	}
}

func TestStartRefusesFork(t *testing.T) {
	cfg, _ := fakeDevinConfig(t, "normal")
	bridge := NewBridge(context.Background(), cfg, (&eventRecorder{}).emit)
	defer bridge.Shutdown()
	params := gatedStart(t.TempDir())
	params.Fork = "parent"
	if err := bridge.HandleStart(params); err == nil || !strings.HasPrefix(err.Error(), "FORK_UNSUPPORTED") {
		t.Fatalf("fork: %v", err)
	}
}

func TestWatchdogCancelsThenKillsASilentTurn(t *testing.T) {
	bridge, recorder, _ := startFakeBridge(t, "hang", func(cfg *Config) { cfg.TurnIdleTimeout = 300 * time.Millisecond })
	if err := bridge.HandleMessage(MessageParams{Content: "hang"}); err != nil {
		t.Fatal(err)
	}
	// cancelGracePeriod is 30s; the fake ignores the cancel, so the
	// watchdog must kill it.
	stalled := recorder.waitFor(t, cancelGracePeriod+10*time.Second, func(e msg.Event) bool {
		return e.Type == msg.EventError && e.Error.Code == "TURN_IDLE_TIMEOUT"
	})
	if !stalled.Error.Retryable {
		t.Error("a stalled turn should be retryable")
	}
	bridge.mu.Lock()
	running, turn := bridge.process != nil, bridge.turnRunning
	bridge.mu.Unlock()
	if running {
		t.Error("devin still running after it ignored the cancel")
	}
	if turn {
		t.Error("turn still marked running")
	}
	for _, event := range recorder.events {
		if event.Type == msg.EventError && event.Error.Code == "PROCESS_DIED" {
			t.Error("a kill by the watchdog was reported as a crash")
		}
	}
}

func TestCrashIsReportedAndNextMessageReloadsSession(t *testing.T) {
	bridge, recorder, logPath := startFakeBridge(t, "die", nil)
	if err := bridge.HandleMessage(MessageParams{Content: "crash"}); err != nil {
		t.Fatal(err)
	}
	recorder.waitFor(t, 10*time.Second, func(e msg.Event) bool { return e.Type == msg.EventError && e.Error.Code == "PROCESS_DIED" })
	recorder.waitFor(t, 10*time.Second, func(e msg.Event) bool { return e.Type == msg.EventError && e.Error.Code == "TURN_FAILED" })
	if err := bridge.HandleMessage(MessageParams{Content: "again"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		methods, _ := os.ReadFile(logPath)
		if strings.Contains(string(methods), "session/load") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	methods, _ := os.ReadFile(logPath)
	t.Fatalf("the message after a crash did not reload the session; fake saw:\n%s", methods)
}

func TestResumeLoadsWithoutReplayingHistoryAsNewEvents(t *testing.T) {
	cfg, logPath := fakeDevinConfig(t, "normal")
	recorder := &eventRecorder{}
	bridge := NewBridge(context.Background(), cfg, recorder.emit)
	defer bridge.Shutdown()
	params := gatedStart(t.TempDir())
	params.Resume = true
	params.HarnessSessionID = "old-session"
	if err := bridge.HandleStart(params); err != nil {
		t.Fatal(err)
	}
	methods, _ := os.ReadFile(logPath)
	if !strings.Contains(string(methods), "session/load") || strings.Contains(string(methods), "session/new") {
		t.Fatalf("resume should load, not create; fake saw:\n%s", methods)
	}
	for _, event := range recorder.events {
		if event.Type == msg.EventSystem && event.System.Subtype == "user_message_chunk" {
			t.Fatal("replayed history was emitted as live events")
		}
	}
	if bridge.harnessSessionID != "old-session" {
		t.Errorf("harness session id = %q", bridge.harnessSessionID)
	}
}

func TestConfigSwitchesModelAndRefusesOneTheAccountLacks(t *testing.T) {
	bridge, _, _ := startFakeBridge(t, "normal", nil)
	if err := bridge.HandleConfig(`{"model":"claude-opus-5-5","effort":"high"}`); err != nil {
		t.Fatalf("switch to an offered model: %v", err)
	}
	if bridge.modelUID != "claude-opus-5-5-high" {
		t.Errorf("model = %q", bridge.modelUID)
	}
	err := bridge.HandleConfig(`{"effort":"medium"}`)
	if err == nil || !strings.Contains(err.Error(), "MODEL_UNAVAILABLE") {
		t.Fatalf("switch to a model the account lacks: %v", err)
	}
	if err := bridge.HandleConfig(`{"disabled_tools":["exec"]}`); err == nil {
		t.Fatal("a mid-session disabled_tools change was accepted but cannot apply")
	}
}

func TestSystemPromptRidesTheFirstPromptOnly(t *testing.T) {
	bridge := &Bridge{systemPrompt: "be brief"}
	first, err := bridge.buildPrompt(queuedMessage{content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].Type != "resource" || first[0].Resource.Text != "be brief" {
		t.Fatalf("first prompt = %+v", first)
	}
	bridge.systemPromptSent = true
	second, _ := bridge.buildPrompt(queuedMessage{content: "again"})
	if len(second) != 1 {
		t.Fatalf("second prompt repeats the system prompt: %+v", second)
	}
	if _, err := bridge.buildPrompt(queuedMessage{blocks: []msg.ContentBlock{{Type: msg.BlockAudio}}}); err == nil {
		t.Fatal("an audio block was accepted")
	}
}

func TestWriteDevinConfigKeepsUserSettingsAndPutsGateFirst(t *testing.T) {
	dir := t.TempDir()
	user := filepath.Join(dir, "user.json")
	os.WriteFile(user, []byte(`{"theme":"dark","disabled_tools":["webfetch"],"hooks":{"PreToolUse":[{"matcher":"exec","hooks":[{"type":"command","command":"user-hook"}]}]}}`), 0o600)
	out := filepath.Join(dir, "session.json")
	gate := json.RawMessage(`{"matcher":".*","hooks":[{"type":"command","command":"gate"}]}`)
	if err := writeDevinConfig(user, out, map[string][]json.RawMessage{"PreToolUse": {gate}}, []string{"exec", "webfetch"}); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(out)
	var config struct {
		Theme         string   `json:"theme"`
		DisabledTools []string `json:"disabled_tools"`
		Hooks         map[string][]struct {
			Hooks []struct {
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	if config.Theme != "dark" {
		t.Error("user setting lost")
	}
	if strings.Join(config.DisabledTools, ",") != "exec,webfetch" {
		t.Errorf("disabled tools = %v", config.DisabledTools)
	}
	pre := config.Hooks["PreToolUse"]
	if len(pre) != 2 || pre[0].Hooks[0].Command != "gate" || pre[1].Hooks[0].Command != "user-hook" {
		t.Errorf("PreToolUse order = %+v, want the gate first", pre)
	}
	if info, _ := os.Stat(out); info.Mode().Perm() != 0o600 {
		t.Errorf("session config mode %v", info.Mode().Perm())
	}
	if err := writeDevinConfig(user, out, nil, []string{"rm -rf"}); err == nil {
		t.Error("a malformed tool name was accepted")
	}
}

func TestReadMCPServersConvertsClaudeCodeShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp.json")
	os.WriteFile(path, []byte(`{"mcpServers":{
		"bridge":{"command":"/bin/bridge-mcp","env":{"B":"2","A":"1"}},
		"remote":{"type":"sse","url":"https://example.test/sse","headers":{"Authorization":"x"}}}}`), 0o600)
	servers, err := readMCPServers(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(servers) != 2 || servers[0].Name != "bridge" || servers[0].Command != "/bin/bridge-mcp" || servers[0].Args == nil {
		t.Fatalf("stdio server = %+v", servers)
	}
	if servers[0].Env[0].Name != "A" {
		t.Errorf("env not sorted: %+v", servers[0].Env)
	}
	if servers[1].Type != "sse" || servers[1].URL == "" || servers[1].Headers[0].Name != "Authorization" {
		t.Errorf("remote server = %+v", servers[1])
	}
	inline, err := readMCPServers("{\"mcpServers\":{}}\n")
	if err != nil || len(inline) != 0 {
		t.Errorf("inline JSON mcp_config: %v %v", inline, err)
	}
	os.WriteFile(path, []byte(`{"mcpServers":{"broken":{}}}`), 0o600)
	if _, err := readMCPServers(path); err == nil {
		t.Error("a server with neither command nor url was accepted")
	}
}

func TestResolveDevinModel(t *testing.T) {
	var listing struct {
		Families []devinModelFamily `json:"families"`
	}
	if err := json.Unmarshal([]byte(fakeCatalog), &listing); err != nil {
		t.Fatal(err)
	}
	catalog := listing.Families
	cases := []struct{ model, effort, want, err string }{
		{"claude-opus-5-5", "high", "claude-opus-5-5-high", ""},
		{"claude-opus-5-5", "", "claude-opus-5-5-medium", ""},
		{"swe-2-flash-medium", "", "swe-2-flash-medium", ""},
		{"claude-opus-5-5-high-fast", "high", "claude-opus-5-5-high-fast", ""},
		{"claude-opus-5-5", "ultra", "", "offers efforts"},
		{"gpt-9", "", "", "no \"gpt-9\""},
		{"swe-2-flash-medium", "high", "", "does not match effort"},
		{"", "", "", ""},
	}
	for _, c := range cases {
		got, err := resolveDevinModel(catalog, c.model, c.effort)
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("%s/%s: err %v, want %q", c.model, c.effort, err, c.err)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%s/%s = %q, %v; want %q", c.model, c.effort, got, err, c.want)
		}
	}
	if effortOfVariant(catalog, "claude-opus-5-5-high") != "high" || familyOfVariant(catalog, "claude-opus-5-5-high") != "claude-opus-5-5" {
		t.Error("variant read-back")
	}
}

func TestHookExecutionBecomesAHookEventNotAToolCall(t *testing.T) {
	recorder := &eventRecorder{}
	tr := newTranslator(recorder.emit)
	tr.beginTurn()
	frame := json.RawMessage(`{"sessionId":"s","update":{"sessionUpdate":"tool_call","toolCallId":"h1","title":"Hook block","status":"failed","_meta":{"cognition.ai/eventType":"hook_execution","cognition.ai/hookId":"gate","cognition.ai/phase":"pre_tool","cognition.ai/decision":"block","cognition.ai/exitCode":2,"cognition.ai/output":"no"}}}`)
	tr.handleSessionUpdate(frame, frame)
	if len(recorder.events) != 1 || recorder.events[0].Type != msg.EventHook {
		t.Fatalf("events = %+v", recorder.events)
	}
	hook := recorder.events[0].Hook
	if hook.Event != "PreToolUse" || hook.Decision != "block" || hook.ExitCode != 2 || hook.HookID != "gate" {
		t.Errorf("hook = %+v", hook)
	}
}

func TestUnclosedToolCallGetsAFailedResultAtTurnEnd(t *testing.T) {
	recorder := &eventRecorder{}
	tr := newTranslator(recorder.emit)
	tr.beginTurn()
	frame := json.RawMessage(`{"sessionId":"s","update":{"sessionUpdate":"tool_call","toolCallId":"c1","kind":"execute","rawInput":{"command":"sleep 60"},"_meta":{"cognition.ai/inferenceToolName":"exec"}}}`)
	tr.handleSessionUpdate(frame, frame)
	tr.finishTurn(promptResult{StopReason: "cancelled"}, "m", nil)
	var sawFailed bool
	for _, event := range recorder.events {
		if event.Type == msg.EventToolResult && event.ToolResult.ToolID == "c1" && event.ToolResult.IsError {
			sawFailed = true
		}
	}
	if !sawFailed {
		t.Fatal("a tool call devin never closed was left running")
	}
}

func TestOneShotContract(t *testing.T) {
	var out strings.Builder
	if code := runOneShot(strings.NewReader(`{"prompt":""}`), &out); code != 1 || strings.TrimSpace(out.String()) != `{"error":"prompt required"}` {
		t.Fatalf("empty prompt: %d %q", code, out.String())
	}
	out.Reset()
	if code := runOneShot(strings.NewReader(`{"prompt":"x","schema":{"type":"object"}}`), &out); code != 1 || !strings.Contains(out.String(), "schema") {
		t.Fatalf("schema: %d %q", code, out.String())
	}
}
