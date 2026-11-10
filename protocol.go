package main

// The parts of the Agent Client Protocol (protocol version 1) this wrapper
// reads and writes, as `devin acp` 3000.12 speaks them. Cognition's own
// fields arrive under `_meta` keys prefixed `cognition.ai/`.

import "encoding/json"

const acpProtocolVersion = 1

type initializeParams struct {
	ProtocolVersion    int                `json:"protocolVersion"`
	ClientCapabilities clientCapabilities `json:"clientCapabilities"`
	ClientInfo         implementationInfo `json:"clientInfo"`
}

// clientCapabilities advertises nothing: devin reads and writes files and runs
// commands itself, so every tool call goes through its own PreToolUse hook,
// which is where the bridge's permission gate sits.
type clientCapabilities struct {
	FS       fileSystemCapabilities `json:"fs"`
	Terminal bool                   `json:"terminal"`
}

type fileSystemCapabilities struct {
	ReadTextFile  bool `json:"readTextFile"`
	WriteTextFile bool `json:"writeTextFile"`
}

type implementationInfo struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version"`
}

type initializeResult struct {
	ProtocolVersion   int                `json:"protocolVersion"`
	AgentCapabilities agentCapabilities  `json:"agentCapabilities"`
	AgentInfo         implementationInfo `json:"agentInfo"`
	AuthMethods       []json.RawMessage  `json:"authMethods"`
}

type agentCapabilities struct {
	LoadSession        bool `json:"loadSession"`
	PromptCapabilities struct {
		Image           bool `json:"image"`
		Audio           bool `json:"audio"`
		EmbeddedContext bool `json:"embeddedContext"`
	} `json:"promptCapabilities"`
}

// mcpServer is one MCP server handed to the agent in session/new or
// session/load. Stdio servers carry command/args/env; HTTP and SSE servers
// carry type/url/headers.
type mcpServer struct {
	Name    string          `json:"name"`
	Type    string          `json:"type,omitempty"`
	Command string          `json:"command,omitempty"`
	Args    []string        `json:"args"`
	Env     []nameValuePair `json:"env"`
	URL     string          `json:"url,omitempty"`
	Headers []nameValuePair `json:"headers,omitempty"`
}

type nameValuePair struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type newSessionParams struct {
	Cwd        string      `json:"cwd"`
	MCPServers []mcpServer `json:"mcpServers"`
}

type loadSessionParams struct {
	SessionID  string      `json:"sessionId"`
	Cwd        string      `json:"cwd"`
	MCPServers []mcpServer `json:"mcpServers"`
}

type sessionModes struct {
	CurrentModeID  string `json:"currentModeId"`
	AvailableModes []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"availableModes"`
}

type configOption struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Category     string `json:"category"`
	Type         string `json:"type"`
	CurrentValue string `json:"currentValue"`
	Options      []struct {
		Value string `json:"value"`
		Name  string `json:"name"`
	} `json:"options"`
}

type sessionSetupResult struct {
	SessionID     string         `json:"sessionId"`
	Modes         *sessionModes  `json:"modes"`
	ConfigOptions []configOption `json:"configOptions"`
}

type setModeParams struct {
	SessionID string `json:"sessionId"`
	ModeID    string `json:"modeId"`
}

type setConfigOptionParams struct {
	SessionID string `json:"sessionId"`
	ConfigID  string `json:"configId"`
	Value     string `json:"value"`
}

type setConfigOptionResult struct {
	ConfigOptions []configOption `json:"configOptions"`
}

// contentBlock is an ACP content block, in a prompt or in an update.
type contentBlock struct {
	Type     string           `json:"type"`
	Text     string           `json:"text,omitempty"`
	MimeType string           `json:"mimeType,omitempty"`
	Data     string           `json:"data,omitempty"`
	URI      string           `json:"uri,omitempty"`
	Resource *embeddedContent `json:"resource,omitempty"`
}

type embeddedContent struct {
	URI      string `json:"uri"`
	MimeType string `json:"mimeType,omitempty"`
	Text     string `json:"text,omitempty"`
	Blob     string `json:"blob,omitempty"`
}

type promptParams struct {
	SessionID string         `json:"sessionId"`
	Prompt    []contentBlock `json:"prompt"`
}

type promptUsage struct {
	TotalTokens       int `json:"totalTokens"`
	InputTokens       int `json:"inputTokens"`
	OutputTokens      int `json:"outputTokens"`
	ThoughtTokens     int `json:"thoughtTokens"`
	CachedReadTokens  int `json:"cachedReadTokens"`
	CachedWriteTokens int `json:"cachedWriteTokens"`
}

type promptResult struct {
	StopReason string       `json:"stopReason"`
	Usage      *promptUsage `json:"usage"`
}

type cancelParams struct {
	SessionID string `json:"sessionId"`
}

type sessionNotification struct {
	SessionID string          `json:"sessionId"`
	Update    json.RawMessage `json:"update"`
}

// sessionUpdate holds the fields of every session/update variant this wrapper
// reads; which ones are set depends on SessionUpdate.
type sessionUpdate struct {
	SessionUpdate string `json:"sessionUpdate"`

	// agent_message_chunk, agent_thought_chunk, user_message_chunk
	Content json.RawMessage `json:"content"`

	// tool_call, tool_call_update
	ToolCallID string          `json:"toolCallId"`
	Title      string          `json:"title"`
	Kind       string          `json:"kind"`
	Status     string          `json:"status"`
	RawInput   json.RawMessage `json:"rawInput"`
	RawOutput  json.RawMessage `json:"rawOutput"`

	// plan
	Entries []struct {
		Content  string `json:"content"`
		Priority string `json:"priority"`
		Status   string `json:"status"`
	} `json:"entries"`

	// available_commands_update
	AvailableCommands []struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	} `json:"availableCommands"`

	// current_mode_update
	CurrentModeID string `json:"currentModeId"`

	// config_option_update
	ConfigOptions []configOption `json:"configOptions"`

	// usage_update
	Used int `json:"used"`
	Size int `json:"size"`

	Meta map[string]json.RawMessage `json:"_meta"`
}

// toolCallContent is one entry of a tool call's content list: either
// {"type":"content","content":<block>} or a diff or terminal reference.
type toolCallContent struct {
	Type       string        `json:"type"`
	Content    *contentBlock `json:"content"`
	Path       string        `json:"path"`
	OldText    *string       `json:"oldText"`
	NewText    string        `json:"newText"`
	TerminalID string        `json:"terminalId"`
}

type requestPermissionParams struct {
	SessionID string `json:"sessionId"`
	ToolCall  struct {
		ToolCallID string          `json:"toolCallId"`
		Title      string          `json:"title"`
		Kind       string          `json:"kind"`
		RawInput   json.RawMessage `json:"rawInput"`
	} `json:"toolCall"`
	Options []struct {
		OptionID string `json:"optionId"`
		Name     string `json:"name"`
		Kind     string `json:"kind"`
	} `json:"options"`
}

func metaString(meta map[string]json.RawMessage, key string) string {
	raw, ok := meta[key]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

func metaInt(meta map[string]json.RawMessage, key string) int {
	raw, ok := meta[key]
	if !ok {
		return 0
	}
	var n float64
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0
	}
	return int(n)
}
