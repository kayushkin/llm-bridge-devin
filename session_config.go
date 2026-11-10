package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"sort"
	"time"
)

// writeDevinConfig writes the config devin runs this session with: the user's
// own devin config, with the bridge's hooks put first under each event and the
// session's disabled tools added to devin's own top-level disabled_tools list,
// which takes them out of the model's tool list altogether. devin's --config
// replaces the user config file rather than adding to it, which is why the
// user's file is copied in.
func writeDevinConfig(userConfigPath, outPath string, bridgeHooks map[string][]json.RawMessage, disabledTools []string) error {
	config := map[string]json.RawMessage{}
	raw, err := os.ReadFile(userConfigPath)
	switch {
	case err == nil:
		if err := json.Unmarshal(raw, &config); err != nil {
			return fmt.Errorf("devin user config %s is not JSON: %w", userConfigPath, err)
		}
	case errors.Is(err, os.ErrNotExist):
	default:
		return fmt.Errorf("read devin user config: %w", err)
	}

	hooks := map[string][]json.RawMessage{}
	if existing, ok := config["hooks"]; ok {
		if err := json.Unmarshal(existing, &hooks); err != nil {
			return fmt.Errorf("hooks in devin user config %s: %w", userConfigPath, err)
		}
	}
	merged := map[string][]json.RawMessage{}
	for event, entries := range bridgeHooks {
		merged[event] = append(merged[event], entries...)
	}
	for event, entries := range hooks {
		merged[event] = append(merged[event], entries...)
	}
	encodedHooks, err := json.Marshal(merged)
	if err != nil {
		return err
	}
	config["hooks"] = encodedHooks

	if len(disabledTools) > 0 {
		var existing []string
		if raw, ok := config["disabled_tools"]; ok {
			if err := json.Unmarshal(raw, &existing); err != nil {
				return fmt.Errorf("disabled_tools in devin user config %s: %w", userConfigPath, err)
			}
		}
		union := map[string]bool{}
		for _, name := range append(existing, disabledTools...) {
			if !devinToolName.MatchString(name) {
				return fmt.Errorf("disabled tool %q is not a devin tool name", name)
			}
			union[name] = true
		}
		names := make([]string, 0, len(union))
		for name := range union {
			names = append(names, name)
		}
		sort.Strings(names)
		encoded, _ := json.Marshal(names)
		config["disabled_tools"] = encoded
	}

	out, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(outPath, out, 0o600)
}

var devinToolName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// claudeCodeMCPConfig is the {"mcpServers":{…}} file bridge-server provisions.
type claudeCodeMCPConfig struct {
	MCPServers map[string]struct {
		Command string            `json:"command"`
		Args    []string          `json:"args"`
		Env     map[string]string `json:"env"`
		Type    string            `json:"type"`
		URL     string            `json:"url"`
		Headers map[string]string `json:"headers"`
	} `json:"mcpServers"`
}

// readMCPServers turns the provisioned MCP config into ACP's server list.
func readMCPServers(path string) ([]mcpServer, error) {
	if path == "" {
		return []mcpServer{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read mcp_config: %w", err)
	}
	var config claudeCodeMCPConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, fmt.Errorf("parse mcp_config %s: %w", path, err)
	}
	names := make([]string, 0, len(config.MCPServers))
	for name := range config.MCPServers {
		names = append(names, name)
	}
	sort.Strings(names)
	servers := make([]mcpServer, 0, len(names))
	for _, name := range names {
		entry := config.MCPServers[name]
		server := mcpServer{Name: name, Args: entry.Args, Env: sortedPairs(entry.Env)}
		if server.Args == nil {
			server.Args = []string{}
		}
		switch {
		case entry.Command != "":
			server.Command = entry.Command
		case entry.URL != "":
			server.Type = entry.Type
			if server.Type == "" {
				server.Type = "http"
			}
			server.URL = entry.URL
			server.Headers = sortedPairs(entry.Headers)
		default:
			return nil, fmt.Errorf("mcp server %q has neither a command nor a url", name)
		}
		servers = append(servers, server)
	}
	return servers, nil
}

func sortedPairs(values map[string]string) []nameValuePair {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]nameValuePair, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, nameValuePair{Name: key, Value: values[key]})
	}
	return pairs
}

// askGate sends a session/request_permission to the bridge's permission gate,
// in the PreToolUse shape the gate takes. Anything but "allow" — including an
// unreachable gate — is a denial.
func (b *Bridge) askGate(request requestPermissionParams) (bool, string) {
	b.mu.Lock()
	url, bridgeID := b.start.DevinPermissionURL, b.bridgeSessionID
	b.mu.Unlock()
	if url == "" {
		return false, "no devin_permission_url in the start request"
	}
	name := ""
	var input any = request.ToolCall.RawInput
	if len(request.ToolCall.RawInput) == 0 {
		input = map[string]any{}
	}
	if request.ToolCall.Kind != "" {
		name = request.ToolCall.Kind
	}
	body, _ := json.Marshal(map[string]any{
		"session_id":      bridgeID,
		"tool_name":       name,
		"tool_input":      input,
		"tool_use_id":     request.ToolCall.ToolCallID,
		"hook_event_name": "PreToolUse",
	})
	client := &http.Client{Timeout: 24 * time.Hour}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return false, "gate unreachable: " + err.Error()
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return false, fmt.Sprintf("gate HTTP %d", resp.StatusCode)
	}
	var out struct {
		HookSpecificOutput struct {
			PermissionDecision       string `json:"permissionDecision"`
			PermissionDecisionReason string `json:"permissionDecisionReason"`
		} `json:"hookSpecificOutput"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		log.Printf("[permission] undecodable gate answer: %v", err)
		return false, "undecodable gate answer"
	}
	return out.HookSpecificOutput.PermissionDecision == "allow", out.HookSpecificOutput.PermissionDecisionReason
}

func buildRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return setting.Value
		}
	}
	return "unknown"
}
