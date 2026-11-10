package main

// translator turns devin's session/update notifications into the bridge's
// canonical msg.Event stream. Every event carries the native frame in Raw.

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
)

const harness = msg.HarnessDevin

type toolCallState struct {
	name   string
	output string
	images []msg.ContentBlock
	done   bool
}

type translator struct {
	mu   sync.Mutex
	emit func(msg.Event)

	bridgeSessionID  string
	harnessSessionID string
	clientRequestID  string

	// Per turn.
	turnStarted      time.Time
	textSinceTool    strings.Builder
	messageID        string
	messageSequence  int
	toolCalls        map[string]*toolCallState
	toolCallOrder    []string
	contextUsed      int
	contextSize      int
	inferenceCalls   int
	callUsages       []msg.TokenUsage
	modelLabel       string
	agentStoppedSeen bool
}

func newTranslator(emit func(msg.Event)) *translator {
	return &translator{emit: emit, toolCalls: map[string]*toolCallState{}}
}

func (t *translator) event(eventType msg.EventType) msg.Event {
	return msg.Event{
		Type:             eventType,
		Harness:          harness,
		BridgeSessionID:  t.bridgeSessionID,
		HarnessSessionID: t.harnessSessionID,
		ClientRequestID:  t.clientRequestID,
		Timestamp:        time.Now(),
	}
}

func (t *translator) setSessionIDs(bridgeSessionID, harnessSessionID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.bridgeSessionID = bridgeSessionID
	t.harnessSessionID = harnessSessionID
}

func (t *translator) beginTurn() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.turnStarted = time.Now()
	t.textSinceTool.Reset()
	t.messageID = ""
	t.toolCalls = map[string]*toolCallState{}
	t.toolCallOrder = nil
	t.inferenceCalls = 0
	t.callUsages = nil
	t.agentStoppedSeen = false
}

// nextMessageID starts a new assistant message: the first text after the turn
// began or after a tool call. Devin does not number its messages, so the id is
// the harness session id plus a sequence this process keeps.
func (t *translator) currentMessageIDLocked() string {
	if t.messageID == "" {
		t.messageSequence++
		t.messageID = fmt.Sprintf("%s-m%d", t.harnessSessionID, t.messageSequence)
	}
	return t.messageID
}

// handleSessionUpdate translates one session/update. frame is the whole
// notification as devin sent it.
func (t *translator) handleSessionUpdate(params json.RawMessage, frame json.RawMessage) {
	var notification sessionNotification
	if err := json.Unmarshal(params, &notification); err != nil {
		t.emitUnparsed("session/update", frame, err)
		return
	}
	var update sessionUpdate
	if err := json.Unmarshal(notification.Update, &update); err != nil {
		t.emitUnparsed("session/update", frame, err)
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	switch update.SessionUpdate {
	case "agent_message_chunk":
		var block contentBlock
		_ = json.Unmarshal(update.Content, &block)
		if block.Type != "text" {
			e := t.event(msg.EventBlock)
			e.Block = &msg.BlockEvent{MessageID: t.currentMessageIDLocked(), Block: acpBlockToCanonical(block)}
			e.Raw = frame
			t.emit(e)
			return
		}
		t.textSinceTool.WriteString(block.Text)
		e := t.event(msg.EventStream)
		e.Stream = &msg.HarnessStream{
			Delta:     &msg.BlockDelta{Type: msg.DeltaText, Text: block.Text},
			MessageID: t.currentMessageIDLocked(),
		}
		e.Raw = frame
		t.emit(e)

	case "agent_thought_chunk":
		var block contentBlock
		_ = json.Unmarshal(update.Content, &block)
		e := t.event(msg.EventThinking)
		e.Thinking = &msg.ThinkingEvent{Text: block.Text}
		e.Raw = frame
		t.emit(e)

	case "user_message_chunk":
		// Devin echoes the prompt; the bridge records the user's message
		// itself, so this is forwarded as a system frame only.
		t.emitSystemLocked("user_message_chunk", "", frame)

	case "tool_call":
		t.handleToolCallLocked(update, frame)

	case "tool_call_update":
		t.handleToolCallUpdateLocked(update, frame)

	case "plan":
		lines := make([]string, 0, len(update.Entries))
		for _, entry := range update.Entries {
			lines = append(lines, fmt.Sprintf("[%s] %s", entry.Status, entry.Content))
		}
		e := t.event(msg.EventPlan)
		e.Plan = &msg.PlanEvent{Text: strings.Join(lines, "\n")}
		e.Raw = frame
		t.emit(e)

	case "usage_update":
		t.contextUsed = update.Used
		t.contextSize = update.Size
		// Devin sends usage_update twice per model call, the second time
		// tagged with the subagent context; count the untagged one.
		if _, tagged := update.Meta["cognition.ai/subagent_context"]; !tagged {
			t.inferenceCalls++
			t.callUsages = append(t.callUsages, msg.TokenUsage{
				InputTokens:   metaInt(update.Meta, "cognition.ai/inputTokens"),
				OutputTokens:  metaInt(update.Meta, "cognition.ai/outputTokens"),
				TotalTokens:   metaInt(update.Meta, "cognition.ai/inputTokens") + metaInt(update.Meta, "cognition.ai/outputTokens"),
				ContextTokens: update.Used,
				ContextLimit:  update.Size,
			})
		}
		t.emitSystemLocked("usage_update", fmt.Sprintf("%d/%d context tokens", update.Used, update.Size), frame)

	case "session_info_update":
		var info struct {
			Title string `json:"title"`
		}
		_ = json.Unmarshal(notification.Update, &info)
		t.emitSystemLocked("session_title", info.Title, frame)

	default:
		// available_commands_update, current_mode_update and
		// config_option_update are read by the bridge handler for
		// session_info; everything is still forwarded.
		t.emitSystemLocked(update.SessionUpdate, "", frame)
	}
}

func (t *translator) handleToolCallLocked(update sessionUpdate, frame json.RawMessage) {
	if metaString(update.Meta, "cognition.ai/eventType") == "hook_execution" {
		// A hook ran (our permission gate or a user hook). Forward it as a
		// hook event: it is not a tool the model called.
		e := t.event(msg.EventHook)
		output, _ := json.Marshal(metaString(update.Meta, "cognition.ai/output"))
		e.Hook = &msg.HookEvent{
			Event:      devinHookPhaseToEvent(metaString(update.Meta, "cognition.ai/phase")),
			HookID:     metaString(update.Meta, "cognition.ai/hookId"),
			Source:     msg.HookSourceHook,
			Phase:      "completed",
			Output:     output,
			Decision:   metaString(update.Meta, "cognition.ai/decision"),
			ExitCode:   metaInt(update.Meta, "cognition.ai/exitCode"),
			DurationMS: int64(metaInt(update.Meta, "cognition.ai/durationMs")),
		}
		e.Raw = frame
		t.emit(e)
		return
	}
	name := metaString(update.Meta, "cognition.ai/inferenceToolName")
	if name == "" {
		name = update.Kind
	}
	state := &toolCallState{name: name}
	t.toolCalls[update.ToolCallID] = state
	t.toolCallOrder = append(t.toolCallOrder, update.ToolCallID)
	// The next text belongs to a new assistant message.
	t.messageID = ""
	t.textSinceTool.Reset()

	input := update.RawInput
	if len(input) == 0 {
		input = json.RawMessage("{}")
	}
	e := t.event(msg.EventToolCall)
	e.ToolCall = &msg.ToolCallEvent{ToolID: update.ToolCallID, Name: name, Input: input}
	e.Raw = frame
	t.emit(e)

	if update.Status == "completed" || update.Status == "failed" {
		t.finishToolCallLocked(update.ToolCallID, update, frame)
	}
}

func (t *translator) handleToolCallUpdateLocked(update sessionUpdate, frame json.RawMessage) {
	state, ok := t.toolCalls[update.ToolCallID]
	if !ok {
		// An update for a call this process never saw start (a replay, or
		// a hook entry). Forward it.
		t.emitSystemLocked("tool_call_update", update.Status, frame)
		return
	}
	if len(update.Content) > 0 {
		output, images := toolCallContentText(update.Content)
		if output != "" {
			state.output = output
		}
		if len(images) > 0 {
			state.images = images
		}
	}
	if update.Status == "completed" || update.Status == "failed" {
		t.finishToolCallLocked(update.ToolCallID, update, frame)
	}
}

func (t *translator) finishToolCallLocked(toolCallID string, update sessionUpdate, frame json.RawMessage) {
	state := t.toolCalls[toolCallID]
	if state == nil || state.done {
		return
	}
	state.done = true
	output := state.output
	if output == "" && len(update.RawOutput) > 0 {
		output = string(update.RawOutput)
	}
	e := t.event(msg.EventToolResult)
	e.ToolResult = &msg.ToolResultEvent{
		ToolID:  toolCallID,
		Name:    state.name,
		Output:  output,
		Content: state.images,
		IsError: update.Status == "failed",
	}
	e.Raw = frame
	t.emit(e)
}

// toolCallContentText reads a tool call's content list: text joins into the
// output, images come back as canonical blocks.
func toolCallContentText(raw json.RawMessage) (string, []msg.ContentBlock) {
	var entries []toolCallContent
	if err := json.Unmarshal(raw, &entries); err != nil {
		return "", nil
	}
	var texts []string
	var images []msg.ContentBlock
	for _, entry := range entries {
		switch entry.Type {
		case "content":
			if entry.Content == nil {
				continue
			}
			switch entry.Content.Type {
			case "text":
				texts = append(texts, entry.Content.Text)
			case "image":
				images = append(images, *acpBlockToCanonical(*entry.Content))
			case "resource":
				if entry.Content.Resource != nil && entry.Content.Resource.URI != "tool://preview" {
					texts = append(texts, entry.Content.Resource.Text)
				}
			}
		case "diff":
			texts = append(texts, fmt.Sprintf("--- %s\n+++ %s\n%s", entry.Path, entry.Path, entry.NewText))
		}
	}
	return strings.Join(texts, "\n"), images
}

// devinHookPhaseToEvent names devin's hook phase by the hook event it runs
// for: pre_tool is PreToolUse, post_tool is PostToolUse; any other phase is
// passed through as devin names it.
func devinHookPhaseToEvent(phase string) string {
	switch phase {
	case "pre_tool":
		return "PreToolUse"
	case "post_tool":
		return "PostToolUse"
	}
	return phase
}

func acpBlockToCanonical(block contentBlock) *msg.ContentBlock {
	switch block.Type {
	case "text":
		return &msg.ContentBlock{Type: msg.BlockText, Text: &msg.TextBlock{Text: block.Text}}
	case "image":
		return &msg.ContentBlock{Type: msg.BlockImage, Image: &msg.ImageBlock{Source: msg.MediaSource{
			Kind: msg.MediaBase64, MediaType: block.MimeType, Data: block.Data,
		}}}
	}
	encoded, _ := json.Marshal(block)
	return &msg.ContentBlock{Type: msg.BlockType(block.Type), Extensions: map[string]json.RawMessage{"acp": encoded}}
}

// handleCognitionNotification forwards one of Cognition's own notifications
// (_cognition.ai/…) and reads what the turn result needs from it.
func (t *translator) handleCognitionNotification(method string, params json.RawMessage, frame json.RawMessage) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch method {
	case "_cognition.ai/agent_stopped":
		var stopped struct {
			Cause string `json:"cause"`
			Stats struct {
				ModelLabel string `json:"modelLabel"`
			} `json:"stats"`
		}
		_ = json.Unmarshal(params, &stopped)
		t.agentStoppedSeen = true
		if stopped.Stats.ModelLabel != "" {
			t.modelLabel = stopped.Stats.ModelLabel
		}
		t.emitSystemLocked("agent_stopped", stopped.Cause, frame)
	default:
		t.emitSystemLocked(strings.TrimPrefix(method, "_cognition.ai/"), "", frame)
	}
}

// finishTurn emits the turn's result once session/prompt has answered.
func (t *translator) finishTurn(result promptResult, model string, frame json.RawMessage) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// A tool call devin never closed (cancelled mid-run) still gets a result,
	// so no tool shows as running forever.
	for _, id := range t.toolCallOrder {
		if state := t.toolCalls[id]; state != nil && !state.done {
			t.finishToolCallLocked(id, sessionUpdate{Status: "failed"}, nil)
		}
	}
	usage := msg.TokenUsage{ContextTokens: t.contextUsed, ContextLimit: t.contextSize}
	if result.Usage != nil {
		usage.InputTokens = result.Usage.InputTokens
		usage.OutputTokens = result.Usage.OutputTokens
		usage.TotalTokens = result.Usage.TotalTokens
		usage.CacheReadTokens = result.Usage.CachedReadTokens
		usage.CacheWriteTokens = result.Usage.CachedWriteTokens
		usage.ReasoningTokens = result.Usage.ThoughtTokens
	}
	e := t.event(msg.EventResult)
	e.Result = &msg.ResultEvent{
		Text:          t.textSinceTool.String(),
		IsError:       result.StopReason == "refusal",
		Usage:         usage,
		DurationMS:    time.Since(t.turnStarted).Milliseconds(),
		NumTurns:      1,
		APICalls:      t.inferenceCalls,
		APICallUsages: t.callUsages,
		Model:         model,
	}
	e.Raw = frame
	t.emit(e)
}

func (t *translator) emitSystemLocked(subtype, message string, frame json.RawMessage) {
	e := t.event(msg.EventSystem)
	e.System = &msg.SystemEvent{Subtype: subtype, Message: message}
	e.Raw = frame
	t.emit(e)
}

func (t *translator) emitSystem(subtype, message string, frame json.RawMessage) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.emitSystemLocked(subtype, message, frame)
}

func (t *translator) emitUnparsed(method string, frame json.RawMessage, err error) {
	t.emitSystem("unparsed_"+method, err.Error(), frame)
}
