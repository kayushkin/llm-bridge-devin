package main

// -oneshot: one stateless call. A msg.OneShotRequest arrives on stdin and a
// msg.OneShotResponse (or {"error":…} and exit 1) goes to stdout.
//
// It runs over ACP like a session, in a throwaway directory, in devin's Ask
// mode, with every tool blocked by a PreToolUse hook, so it answers from the
// model alone and still reports token usage, which `devin -p` does not.
//
// Schemas are refused: devin has no way to force a model to answer in a
// schema, and the one-shot contract forbids soft validation.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
)

func writeOneShotError(out io.Writer, message string) int {
	encoded, _ := json.Marshal(map[string]string{"error": message})
	fmt.Fprintln(out, string(encoded))
	return 1
}

func runOneShot(in io.Reader, out io.Writer) int {
	var request msg.OneShotRequest
	if err := json.NewDecoder(in).Decode(&request); err != nil {
		return writeOneShotError(out, "parse request: "+err.Error())
	}
	if strings.TrimSpace(request.Prompt) == "" {
		return writeOneShotError(out, "prompt required")
	}
	if len(request.Schema) > 0 {
		return writeOneShotError(out, "devin cannot force a JSON schema on its model; refusing rather than validate softly")
	}
	cfg := loadConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	response, err := oneShot(ctx, cfg, request)
	if err != nil {
		return writeOneShotError(out, err.Error())
	}
	if err := json.NewEncoder(out).Encode(response); err != nil {
		return 1
	}
	return 0
}

func oneShot(ctx context.Context, cfg Config, request msg.OneShotRequest) (*msg.OneShotResponse, error) {
	started := time.Now()
	catalog, err := loadDevinModelCatalog(cfg.DevinPath)
	if err != nil {
		return nil, err
	}
	modelUID, err := resolveDevinModel(catalog, request.Model, "")
	if err != nil {
		return nil, err
	}

	workDir, err := os.MkdirTemp("", "llm-bridge-devin-oneshot-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(workDir)
	blockAll, _ := json.Marshal(map[string]any{
		"matcher": "",
		"hooks": []map[string]any{{
			"type":    "command",
			"command": `printf '%s\n' '{"decision":"block","reason":"A one-shot call has no tools."}'`,
			"timeout": 10,
		}},
	})
	configPath := filepath.Join(workDir, ".llm-bridge-devin-config.json")
	if err := writeDevinConfig(cfg.UserConfigPath, configPath, map[string][]json.RawMessage{"PreToolUse": {blockAll}}, nil); err != nil {
		return nil, err
	}

	var textMutex sync.Mutex
	var text strings.Builder
	events := func(event msg.Event) {
		if event.Type == msg.EventStream && event.Stream != nil && event.Stream.Delta != nil {
			textMutex.Lock()
			text.WriteString(event.Stream.Delta.Text)
			textMutex.Unlock()
		}
	}
	bridge := NewBridge(ctx, cfg, events)
	defer bridge.Shutdown()
	bridge.mu.Lock()
	bridge.workDir = workDir
	bridge.devinConfigPath = configPath
	bridge.catalog = catalog
	bridge.mcpServers = []mcpServer{}
	bridge.systemPrompt = request.SystemPrompt
	bridge.mu.Unlock()
	if err := bridge.openSession("", modelUID); err != nil {
		return nil, err
	}
	bridge.mu.Lock()
	process, sessionID := bridge.process, bridge.harnessSessionID
	bridge.mu.Unlock()
	// A one-shot call is not a conversation anyone will open again; leave
	// nothing in devin's session list.
	defer func() {
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer deleteCancel()
		if err := process.conn.call(deleteCtx, "session/delete", cancelParams{SessionID: sessionID}, nil); err != nil {
			fmt.Fprintf(os.Stderr, "delete one-shot devin session %s: %v\n", sessionID, err)
		}
	}()
	if err := process.conn.call(ctx, "session/set_mode", setModeParams{SessionID: sessionID, ModeID: "ask"}, nil); err != nil {
		return nil, fmt.Errorf("set devin to ask mode: %w", err)
	}

	blocks := make([]msg.ContentBlock, 0, len(request.Images))
	for _, image := range request.Images {
		blocks = append(blocks, msg.ContentBlock{Type: msg.BlockImage, Image: &msg.ImageBlock{Source: msg.MediaSource{
			Kind: msg.MediaBase64, MediaType: image.MediaType, Data: image.DataBase64,
		}}})
	}
	prompt, err := bridge.buildPrompt(queuedMessage{content: request.Prompt, blocks: blocks})
	if err != nil {
		return nil, err
	}
	var result promptResult
	if err := process.conn.call(ctx, "session/prompt", promptParams{SessionID: sessionID, Prompt: prompt}, &result); err != nil {
		return nil, err
	}
	if result.StopReason == "refusal" {
		return nil, errors.New("the model refused")
	}
	response := &msg.OneShotResponse{
		DurationMs: time.Since(started).Milliseconds(),
		StopReason: result.StopReason,
	}
	textMutex.Lock()
	response.Text = text.String()
	textMutex.Unlock()
	bridge.mu.Lock()
	response.Model = familyOfVariant(catalog, bridge.modelUID)
	bridge.mu.Unlock()
	if result.Usage != nil {
		response.Usage = msg.TokenUsage{
			InputTokens:      result.Usage.InputTokens,
			OutputTokens:     result.Usage.OutputTokens,
			TotalTokens:      result.Usage.TotalTokens,
			CacheReadTokens:  result.Usage.CachedReadTokens,
			CacheWriteTokens: result.Usage.CachedWriteTokens,
			ReasoningTokens:  result.Usage.ThoughtTokens,
		}
	}
	return response, nil
}
