package main

// A fake `devin` for tests: the test binary re-runs itself with
// FAKE_DEVIN_BEHAVIOR set and answers `models list` and `acp` the way devin
// 3000.12 does, closely enough to exercise the wrapper without a model call.
//
// Behaviors:
//   normal  — a prompt gets a thought, a tool call, its result, text, and
//             end_turn with usage
//   hang    — a prompt is never answered and session/cancel is ignored
//   die     — the process exits when a prompt arrives
// FAKE_DEVIN_LOG, when set, receives every method the fake was sent.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestMain(m *testing.M) {
	if behavior := os.Getenv("FAKE_DEVIN_BEHAVIOR"); behavior != "" {
		runFakeDevin(behavior, os.Args[1:])
		os.Exit(0)
	}
	os.Exit(m.Run())
}

const fakeCatalog = `{"families":[
 {"family_uid":"claude-opus-5-5","slug":"claude-opus-5-5","variants":[{"model_uid":"claude-opus-5-5-medium"},{"model_uid":"claude-opus-5-5-high"},{"model_uid":"claude-opus-5-5-high-fast"}]},
 {"family_uid":"swe-2-flash","slug":"swe-2-flash","variants":[{"model_uid":"swe-2-flash-medium","cost_tier":"Free"}]}
]}`

func runFakeDevin(behavior string, args []string) {
	for i, arg := range args {
		if arg == "models" {
			fmt.Println(fakeCatalog)
			return
		}
		if arg == "acp" {
			_ = i
			fakeACP(behavior)
			return
		}
	}
	fmt.Fprintf(os.Stderr, "fake devin: unhandled args %v\n", args)
	os.Exit(2)
}

func fakeACP(behavior string) {
	var logFile *os.File
	if path := os.Getenv("FAKE_DEVIN_LOG"); path != "" {
		logFile, _ = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	}
	var writeMutex sync.Mutex
	write := func(v any) {
		writeMutex.Lock()
		defer writeMutex.Unlock()
		line, _ := json.Marshal(v)
		os.Stdout.Write(append(line, '\n'))
	}
	update := func(sessionID string, update map[string]any) {
		write(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": sessionID, "update": update}})
	}
	modelOption := func(current string) []map[string]any {
		return []map[string]any{{
			"id": "model", "category": "model", "type": "select", "currentValue": current,
			"options": []map[string]any{{"value": "swe-2-flash-medium"}, {"value": "claude-opus-5-5-high"}},
		}}
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for scanner.Scan() {
		var frame struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if json.Unmarshal(scanner.Bytes(), &frame) != nil {
			continue
		}
		if logFile != nil && frame.Method != "" {
			fmt.Fprintln(logFile, frame.Method)
		}
		reply := func(result any) {
			write(map[string]any{"jsonrpc": "2.0", "id": frame.ID, "result": result})
		}
		var params struct {
			SessionID string `json:"sessionId"`
			Value     string `json:"value"`
		}
		_ = json.Unmarshal(frame.Params, &params)
		switch frame.Method {
		case "initialize":
			reply(map[string]any{"protocolVersion": 1, "agentCapabilities": map[string]any{"loadSession": true}, "agentInfo": map[string]any{"name": "fake", "title": "Fake Devin", "version": "0"}})
		case "session/new":
			reply(map[string]any{"sessionId": "fake-session", "configOptions": modelOption("swe-2-flash-medium")})
			update("fake-session", map[string]any{"sessionUpdate": "available_commands_update", "availableCommands": []map[string]any{{"name": "compact"}}})
		case "session/load":
			update(params.SessionID, map[string]any{"sessionUpdate": "user_message_chunk", "content": map[string]any{"type": "text", "text": "old prompt"}})
			reply(map[string]any{"configOptions": modelOption("swe-2-flash-medium")})
		case "session/set_mode":
			reply(map[string]any{})
		case "session/set_config_option":
			reply(map[string]any{"configOptions": modelOption(params.Value)})
		case "session/delete":
			reply(map[string]any{})
		case "session/cancel":
			// hang ignores it; normal has nothing running.
		case "session/prompt":
			switch behavior {
			case "hang":
				continue
			case "die":
				os.Exit(3)
			}
			sid := params.SessionID
			update(sid, map[string]any{"sessionUpdate": "agent_thought_chunk", "content": map[string]any{"type": "text", "text": "thinking"}})
			update(sid, map[string]any{"sessionUpdate": "usage_update", "used": 100, "size": 1000, "_meta": map[string]any{"cognition.ai/inputTokens": 90, "cognition.ai/outputTokens": 10}})
			update(sid, map[string]any{"sessionUpdate": "tool_call", "toolCallId": "call_1", "kind": "execute", "title": "Ran ls", "rawInput": map[string]any{"command": "ls"}, "_meta": map[string]any{"cognition.ai/inferenceToolName": "exec"}})
			update(sid, map[string]any{"sessionUpdate": "tool_call_update", "toolCallId": "call_1", "status": "completed", "content": []map[string]any{{"type": "content", "content": map[string]any{"type": "text", "text": "a.txt"}}}})
			update(sid, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "done: "}})
			update(sid, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "a.txt"}})
			reply(map[string]any{"stopReason": "end_turn", "usage": map[string]any{"totalTokens": 120, "inputTokens": 100, "outputTokens": 20, "cachedReadTokens": 40}})
		default:
			if len(frame.ID) > 0 && frame.Method != "" {
				write(map[string]any{"jsonrpc": "2.0", "id": frame.ID, "error": map[string]any{"code": -32601, "message": "unknown"}})
			}
		}
	}
}

// fakeDevinConfig points a Config at the test binary acting as devin.
func fakeDevinConfig(t *testing.T, behavior string) (Config, string) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	wrapper := filepath.Join(dir, "devin")
	logPath := filepath.Join(dir, "methods.log")
	script := fmt.Sprintf("#!/bin/sh\nFAKE_DEVIN_BEHAVIOR=%s FAKE_DEVIN_LOG=%s exec %q \"$@\"\n", behavior, logPath, self)
	if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return Config{DevinPath: wrapper, UserConfigPath: filepath.Join(dir, "no-user-config.json")}, logPath
}
