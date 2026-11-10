package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
)

// StartParams is the harness protocol's start request: the base fields plus
// the session's harness_config merged on top.
type StartParams struct {
	BridgeSessionID  string `json:"bridge_session_id"`
	HarnessSessionID string `json:"harness_session_id,omitempty"`
	ClientID         string `json:"client_id,omitempty"`
	Resume           bool   `json:"resume,omitempty"`
	Fork             string `json:"fork,omitempty"`
	WorkDir          string `json:"work_dir,omitempty"`
	Model            string `json:"model,omitempty"`
	Effort           string `json:"effort,omitempty"`

	SystemPrompt       string `json:"system_prompt,omitempty"`
	AppendSystemPrompt string `json:"append_system_prompt,omitempty"`

	// MCPConfig is a Claude-Code-shaped MCP config ({"mcpServers":{…}}):
	// the path of the file bridge-server provisioned, or the JSON itself.
	MCPConfig string `json:"mcp_config,omitempty"`

	// DevinHooks is the hooks tree bridge-server builds for devin, in devin's
	// hooks.v1 shape: {"PreToolUse":[{"matcher":…,"hooks":[{"type":"command",
	// "command":…,"timeout":…}]}]}. It carries the bridge's permission gate,
	// so a start without it is refused.
	DevinHooks map[string][]json.RawMessage `json:"devin_hooks,omitempty"`

	// DevinPermissionURL is the same gate as a URL, for the rare
	// session/request_permission devin sends despite bypass mode.
	DevinPermissionURL string `json:"devin_permission_url,omitempty"`

	// DisabledTools are devin tool names (exec, write, web_search…) this
	// session may not call; devin's own disabled_tools setting removes them.
	DisabledTools []string `json:"disabled_tools,omitempty"`

	PermissionMode    string `json:"permission_mode,omitempty"`
	BypassPermissions bool   `json:"bypass_permissions,omitempty"`
}

type MessageParams struct {
	Content string             `json:"content"`
	Blocks  []msg.ContentBlock `json:"blocks,omitempty"`
}

// sessionConfigRequest is the body of the config:<json> method.
type sessionConfigRequest struct {
	Model         *string  `json:"model,omitempty"`
	Effort        *string  `json:"effort,omitempty"`
	DisabledTools []string `json:"disabled_tools,omitempty"`
}

type devinProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	conn   *acpConnection
	exited chan struct{}
	// killedOnPurpose is set before the wrapper kills devin, so the exit is
	// not reported as a crash.
	killedOnPurpose bool
}

type queuedMessage struct {
	content string
	blocks  []msg.ContentBlock
}

type Bridge struct {
	cfg        Config
	emit       func(msg.Event)
	translator *translator

	mu               sync.Mutex
	bridgeSessionID  string
	clientID         string
	harnessSessionID string
	workDir          string
	sessionDir       string
	devinConfigPath  string
	mcpServers       []mcpServer
	catalog          []devinModelFamily
	modelUID         string
	modelOptions     []string
	modeID           string
	slashCommands    []string
	systemPrompt     string
	systemPromptSent bool
	start            StartParams
	process          *devinProcess
	turnRunning      bool
	turnCancel       context.CancelFunc

	// replaying is true while session/load replays history; its updates
	// are not new events.
	replaying bool

	queue chan queuedMessage
	ctx   context.Context
}

func NewBridge(ctx context.Context, cfg Config, emit func(msg.Event)) *Bridge {
	b := &Bridge{
		cfg:        cfg,
		emit:       emit,
		translator: newTranslator(emit),
		queue:      make(chan queuedMessage, 64),
		ctx:        ctx,
	}
	go b.runTurns()
	return b
}

func (b *Bridge) sessionIDs() (string, string, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bridgeSessionID, b.harnessSessionID, b.clientID
}

func (b *Bridge) emitError(code, message string, retryable bool) {
	bridgeID, harnessID, clientID := b.sessionIDs()
	b.emit(msg.Event{
		Type:             msg.EventError,
		Harness:          harness,
		BridgeSessionID:  bridgeID,
		HarnessSessionID: harnessID,
		ClientRequestID:  clientID,
		Timestamp:        time.Now(),
		Error:            &msg.ErrorEvent{Code: code, Message: message, Retryable: retryable},
	})
}

// HandleStart starts devin and opens (or, on resume, loads) the session.
func (b *Bridge) HandleStart(params StartParams) error {
	if params.Fork != "" {
		return errors.New("FORK_UNSUPPORTED: devin's Agent Client Protocol server has no fork; its /fork works only in the devin TUI")
	}
	if len(params.DevinHooks["PreToolUse"]) == 0 {
		return errors.New("start carries no devin_hooks.PreToolUse permission gate; refusing to run devin ungated (bridge-server too old for devin?)")
	}
	workDir := params.WorkDir
	if workDir == "" {
		var err error
		if workDir, err = os.Getwd(); err != nil {
			return fmt.Errorf("no work_dir and no current directory: %w", err)
		}
	}
	catalog, err := loadDevinModelCatalog(b.cfg.DevinPath)
	if err != nil {
		return err
	}
	modelUID, err := resolveDevinModel(catalog, params.Model, params.Effort)
	if err != nil {
		return err
	}
	servers, err := readMCPServers(params.MCPConfig)
	if err != nil {
		return err
	}
	sessionDir, err := os.MkdirTemp("", "llm-bridge-devin-")
	if err != nil {
		return fmt.Errorf("make session directory: %w", err)
	}
	configPath := filepath.Join(sessionDir, "config.json")
	if err := writeDevinConfig(b.cfg.UserConfigPath, configPath, params.DevinHooks, params.DisabledTools); err != nil {
		return err
	}

	systemPrompt := params.SystemPrompt
	if params.AppendSystemPrompt != "" {
		systemPrompt = strings.TrimSpace(systemPrompt + "\n\n" + params.AppendSystemPrompt)
	}

	b.mu.Lock()
	b.bridgeSessionID = params.BridgeSessionID
	b.clientID = params.ClientID
	b.workDir = workDir
	b.sessionDir = sessionDir
	b.devinConfigPath = configPath
	b.mcpServers = servers
	b.catalog = catalog
	b.start = params
	b.systemPrompt = systemPrompt
	// A resumed session already has its system prompt in its history.
	b.systemPromptSent = params.Resume
	b.mu.Unlock()
	b.translator.setSessionIDs(params.BridgeSessionID, "")
	b.translator.clientRequestID = params.ClientID

	resumeID := ""
	if params.Resume {
		resumeID = params.HarnessSessionID
		if resumeID == "" {
			return errors.New("resume requested with no harness_session_id")
		}
	}
	if err := b.openSession(resumeID, modelUID); err != nil {
		return err
	}
	b.emitSessionInfo()
	return nil
}

// openSession spawns devin and creates or loads the ACP session.
func (b *Bridge) openSession(resumeID, modelUID string) error {
	process, initResult, err := b.spawnDevin()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(b.ctx, 2*time.Minute)
	defer cancel()

	b.mu.Lock()
	workDir, servers := b.workDir, b.mcpServers
	b.mu.Unlock()

	var setup sessionSetupResult
	if resumeID != "" {
		if !initResult.AgentCapabilities.LoadSession {
			return errors.New("this devin does not offer session/load, so the session cannot resume")
		}
		b.mu.Lock()
		b.replaying = true
		b.harnessSessionID = resumeID
		b.mu.Unlock()
		err = process.conn.call(ctx, "session/load", loadSessionParams{SessionID: resumeID, Cwd: workDir, MCPServers: servers}, &setup)
		b.mu.Lock()
		b.replaying = false
		b.mu.Unlock()
		if err != nil {
			return fmt.Errorf("load devin session %s: %w", resumeID, err)
		}
		if setup.SessionID == "" {
			setup.SessionID = resumeID
		}
	} else {
		if err := process.conn.call(ctx, "session/new", newSessionParams{Cwd: workDir, MCPServers: servers}, &setup); err != nil {
			return fmt.Errorf("new devin session: %w", err)
		}
	}
	if setup.SessionID == "" {
		return errors.New("devin returned no session id")
	}
	b.mu.Lock()
	b.harnessSessionID = setup.SessionID
	b.mu.Unlock()
	b.translator.setSessionIDs(b.bridgeSessionID, setup.SessionID)
	b.applyConfigOptions(setup.ConfigOptions)

	// The bridge's gate is the PreToolUse hook, which devin runs in every
	// mode. Bypass stops devin asking a second time, through
	// session/request_permission, about what the gate already allowed.
	if err := process.conn.call(ctx, "session/set_mode", setModeParams{SessionID: setup.SessionID, ModeID: "bypass"}, nil); err != nil {
		return fmt.Errorf("set devin session to bypass (the bridge gate decides): %w", err)
	}
	b.mu.Lock()
	b.modeID = "bypass"
	b.mu.Unlock()

	if modelUID != "" {
		if err := b.setModel(ctx, modelUID); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bridge) setModel(ctx context.Context, modelUID string) error {
	b.mu.Lock()
	process, sessionID, options := b.process, b.harnessSessionID, b.modelOptions
	b.mu.Unlock()
	if process == nil {
		return errors.New("devin is not running")
	}
	if len(options) > 0 && !containsString(options, modelUID) {
		return fmt.Errorf("MODEL_UNAVAILABLE: this Devin account may not use %q; it offers %v", modelUID, options)
	}
	var result setConfigOptionResult
	if err := process.conn.call(ctx, "session/set_config_option", setConfigOptionParams{SessionID: sessionID, ConfigID: "model", Value: modelUID}, &result); err != nil {
		return fmt.Errorf("set devin model %s: %w", modelUID, err)
	}
	b.applyConfigOptions(result.ConfigOptions)
	b.mu.Lock()
	if b.modelUID != modelUID {
		got := b.modelUID
		b.mu.Unlock()
		if len(result.ConfigOptions) > 0 {
			return fmt.Errorf("devin accepted model %s but reports %s", modelUID, got)
		}
		b.mu.Lock()
		b.modelUID = modelUID
	}
	b.mu.Unlock()
	return nil
}

func (b *Bridge) applyConfigOptions(options []configOption) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, option := range options {
		switch option.ID {
		case "model":
			b.modelUID = option.CurrentValue
			b.modelOptions = b.modelOptions[:0]
			for _, choice := range option.Options {
				b.modelOptions = append(b.modelOptions, choice.Value)
			}
		case "mode":
			b.modeID = option.CurrentValue
		}
	}
}

func (b *Bridge) spawnDevin() (*devinProcess, initializeResult, error) {
	b.mu.Lock()
	configPath, workDir := b.devinConfigPath, b.workDir
	b.mu.Unlock()

	cmd := exec.Command(b.cfg.DevinPath, "--config", configPath, "acp")
	cmd.Dir = workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, initializeResult{}, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, initializeResult{}, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, initializeResult{}, err
	}
	if err := cmd.Start(); err != nil {
		return nil, initializeResult{}, fmt.Errorf("start %s acp: %w", b.cfg.DevinPath, err)
	}
	process := &devinProcess{cmd: cmd, stdin: stdin, exited: make(chan struct{})}
	process.conn = newACPConnection(stdin, b.answerAgentRequest, b.handleNotification)
	go forwardDevinWarnings(stderr)
	go func() {
		_ = process.conn.readLoop(stdout)
	}()
	go func() {
		waitErr := cmd.Wait()
		close(process.exited)
		process.conn.close(fmt.Errorf("devin acp exited: %v", waitErr))
		b.mu.Lock()
		current := b.process == process
		if current {
			b.process = nil
		}
		purposeful := process.killedOnPurpose
		b.mu.Unlock()
		if current && !purposeful && b.ctx.Err() == nil {
			b.emitError("PROCESS_DIED", fmt.Sprintf("devin acp exited unexpectedly: %v; the next message restarts it and reloads the session", waitErr), true)
		}
	}()

	b.mu.Lock()
	b.process = process
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(b.ctx, time.Minute)
	defer cancel()
	var result initializeResult
	err = process.conn.call(ctx, "initialize", initializeParams{
		ProtocolVersion:    acpProtocolVersion,
		ClientCapabilities: clientCapabilities{},
		ClientInfo:         implementationInfo{Name: "llm-bridge-devin", Title: "LLM Bridge", Version: buildRevision()},
	}, &result)
	if err != nil {
		b.killProcess(process)
		return nil, result, fmt.Errorf("initialize devin acp: %w", err)
	}
	if result.ProtocolVersion != acpProtocolVersion {
		b.killProcess(process)
		return nil, result, fmt.Errorf("devin speaks ACP protocol %d; this wrapper speaks %d", result.ProtocolVersion, acpProtocolVersion)
	}
	log.Printf("devin acp ready: %s %s", result.AgentInfo.Title, result.AgentInfo.Version)
	return process, result, nil
}

// forwardDevinWarnings passes devin's WARN and ERROR log lines to our stderr.
// devin writes every level to its own log file under
// ~/.local/share/devin/cli/logs, so nothing is lost by leaving INFO there.
func forwardDevinWarnings(stderr io.Reader) {
	scanner := bufio.NewScanner(stderr)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		// devin 3000.12 warns about its own model registry's compaction
		// thresholds on every start, dozens of lines that say nothing about
		// this session.
		if strings.Contains(line, "Ignoring invalid compaction thresholds") {
			continue
		}
		if strings.Contains(line, " WARN ") || strings.Contains(line, " ERROR ") {
			log.Printf("[devin] %s", line)
		}
	}
}

func (b *Bridge) killProcess(process *devinProcess) {
	if process == nil || process.cmd.Process == nil {
		return
	}
	b.mu.Lock()
	process.killedOnPurpose = true
	if b.process == process {
		b.process = nil
	}
	b.mu.Unlock()
	_ = process.stdin.Close()
	_ = syscall.Kill(-process.cmd.Process.Pid, syscall.SIGKILL)
	<-process.exited
}

func (b *Bridge) handleNotification(method string, params json.RawMessage, frame json.RawMessage) {
	b.mu.Lock()
	replaying := b.replaying
	b.mu.Unlock()
	if replaying {
		return
	}
	switch {
	case method == "session/update":
		b.readSessionState(params)
		b.translator.handleSessionUpdate(params, frame)
	case strings.HasPrefix(method, "_cognition.ai/"):
		b.translator.handleCognitionNotification(method, params, frame)
	default:
		b.translator.emitSystem("acp_notification:"+method, "", frame)
	}
}

// readSessionState keeps what session_info reports in step with devin.
func (b *Bridge) readSessionState(params json.RawMessage) {
	var notification sessionNotification
	if json.Unmarshal(params, &notification) != nil {
		return
	}
	var update sessionUpdate
	if json.Unmarshal(notification.Update, &update) != nil {
		return
	}
	switch update.SessionUpdate {
	case "available_commands_update":
		commands := make([]string, 0, len(update.AvailableCommands))
		for _, command := range update.AvailableCommands {
			commands = append(commands, "/"+command.Name)
		}
		sort.Strings(commands)
		b.mu.Lock()
		changed := strings.Join(commands, ",") != strings.Join(b.slashCommands, ",")
		b.slashCommands = commands
		b.mu.Unlock()
		if changed {
			b.emitSessionInfo()
		}
	case "current_mode_update":
		b.mu.Lock()
		b.modeID = update.CurrentModeID
		b.mu.Unlock()
		b.emitSessionInfo()
	case "config_option_update":
		b.applyConfigOptions(update.ConfigOptions)
		b.emitSessionInfo()
	}
}

// answerAgentRequest answers requests devin sends us. With no fs or terminal
// capability offered, the only one expected is session/request_permission,
// which devin can still send when an organization policy overrides bypass;
// it goes to the same bridge gate the hook calls, and is refused if the gate
// cannot be reached.
func (b *Bridge) answerAgentRequest(method string, params json.RawMessage) (any, *rpcError) {
	switch method {
	case "session/request_permission":
		var request requestPermissionParams
		if err := json.Unmarshal(params, &request); err != nil {
			return nil, &rpcError{Code: -32602, Message: err.Error()}
		}
		allowed, reason := b.askGate(request)
		wantKind := "reject_once"
		if allowed {
			wantKind = "allow_once"
		}
		for _, option := range request.Options {
			if option.Kind == wantKind {
				log.Printf("[permission] %s → %s (%s)", request.ToolCall.Title, wantKind, reason)
				return map[string]any{"outcome": map[string]any{"outcome": "selected", "optionId": option.OptionID}}, nil
			}
		}
		return map[string]any{"outcome": map[string]any{"outcome": "cancelled"}}, nil
	}
	return nil, &rpcError{Code: -32601, Message: "llm-bridge-devin does not offer " + method}
}

// HandleMessage queues a user message; turns run one at a time.
func (b *Bridge) HandleMessage(params MessageParams) error {
	if params.Content == "" && len(params.Blocks) == 0 {
		return errors.New("empty message")
	}
	select {
	case b.queue <- queuedMessage{content: params.Content, blocks: params.Blocks}:
		return nil
	default:
		return errors.New("message queue full (64 waiting)")
	}
}

func (b *Bridge) runTurns() {
	for {
		select {
		case <-b.ctx.Done():
			return
		case message := <-b.queue:
			if err := b.runTurn(message); err != nil {
				log.Printf("turn: %v", err)
			}
		}
	}
}

func (b *Bridge) ensureProcess() error {
	b.mu.Lock()
	alive := b.process != nil
	harnessID, modelUID := b.harnessSessionID, b.modelUID
	b.mu.Unlock()
	if alive {
		return nil
	}
	if harnessID == "" {
		return errors.New("no devin session: start failed or was never sent")
	}
	log.Printf("devin is not running; restarting and loading session %s", harnessID)
	return b.openSession(harnessID, modelUID)
}

func (b *Bridge) runTurn(message queuedMessage) error {
	if err := b.ensureProcess(); err != nil {
		b.emitError("MESSAGE_FAILED", err.Error(), true)
		return err
	}
	prompt, err := b.buildPrompt(message)
	if err != nil {
		b.emitError("MESSAGE_FAILED", err.Error(), false)
		return err
	}

	b.mu.Lock()
	process, sessionID, modelUID, catalog := b.process, b.harnessSessionID, b.modelUID, b.catalog
	ctx, cancel := context.WithCancel(b.ctx)
	b.turnRunning = true
	b.turnCancel = cancel
	b.mu.Unlock()
	defer func() {
		cancel()
		b.mu.Lock()
		b.turnRunning = false
		b.turnCancel = nil
		b.mu.Unlock()
	}()

	b.translator.beginTurn()
	watchdogDone := make(chan struct{})
	stalled := make(chan time.Duration, 1)
	go b.watchTurn(process, sessionID, watchdogDone, stalled)

	var result promptResult
	callErr := process.conn.call(ctx, "session/prompt", promptParams{SessionID: sessionID, Prompt: prompt}, &result)
	close(watchdogDone)

	select {
	case silence := <-stalled:
		message := fmt.Sprintf("devin sent nothing for %s during the turn; the wrapper cancelled it", silence.Round(time.Second))
		if callErr != nil {
			message += " and restarted devin (" + callErr.Error() + ")"
		}
		b.emitError("TURN_IDLE_TIMEOUT", message, true)
		if callErr == nil {
			b.translator.finishTurn(result, familyOfVariant(catalog, modelUID), nil)
		}
		return errors.New(message)
	default:
	}
	if callErr != nil {
		b.emitError("TURN_FAILED", callErr.Error(), true)
		return callErr
	}
	b.mu.Lock()
	b.systemPromptSent = true
	modelUID = b.modelUID
	b.mu.Unlock()
	encoded, _ := json.Marshal(result)
	b.translator.finishTurn(result, familyOfVariant(catalog, modelUID), encoded)
	return nil
}

// watchTurn cancels a turn devin has gone silent on, and kills devin if the
// cancel is not answered.
func (b *Bridge) watchTurn(process *devinProcess, sessionID string, done <-chan struct{}, stalled chan<- time.Duration) {
	timeout := b.cfg.TurnIdleTimeout
	if timeout <= 0 {
		return
	}
	ticker := time.NewTicker(timeout / 20)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			silence := time.Since(process.conn.lastReceived())
			if silence < timeout {
				continue
			}
			log.Printf("[watchdog] no frame from devin for %s; cancelling the turn", silence.Round(time.Second))
			stalled <- silence
			_ = process.conn.notify("session/cancel", cancelParams{SessionID: sessionID})
			select {
			case <-done:
			case <-time.After(cancelGracePeriod):
				log.Printf("[watchdog] devin did not answer session/cancel in %s; killing it", cancelGracePeriod)
				b.killProcess(process)
			}
			return
		}
	}
}

// buildPrompt turns a bridge message into ACP content blocks. A new session's
// first prompt carries the system prompt ahead of the message, as an embedded
// resource: devin's ACP server takes no system prompt of its own.
func (b *Bridge) buildPrompt(message queuedMessage) ([]contentBlock, error) {
	var prompt []contentBlock
	b.mu.Lock()
	if !b.systemPromptSent && b.systemPrompt != "" {
		prompt = append(prompt, contentBlock{Type: "resource", Resource: &embeddedContent{
			URI: "llm-bridge://system-prompt", MimeType: "text/markdown", Text: b.systemPrompt,
		}})
	}
	b.mu.Unlock()
	if message.content != "" {
		prompt = append(prompt, contentBlock{Type: "text", Text: message.content})
	}
	for _, block := range message.blocks {
		switch block.Type {
		case msg.BlockText:
			if block.Text != nil {
				prompt = append(prompt, contentBlock{Type: "text", Text: block.Text.Text})
			}
		case msg.BlockImage:
			if block.Image == nil || block.Image.Source.Kind != msg.MediaBase64 {
				return nil, errors.New("devin takes images only as base64 data")
			}
			prompt = append(prompt, contentBlock{Type: "image", MimeType: block.Image.Source.MediaType, Data: block.Image.Source.Data})
		default:
			return nil, fmt.Errorf("devin's ACP prompt cannot carry a %s block", block.Type)
		}
	}
	return prompt, nil
}

// HandleInterrupt cancels the running turn (SIGINT from the bridge).
func (b *Bridge) HandleInterrupt() error {
	b.mu.Lock()
	process, sessionID, running := b.process, b.harnessSessionID, b.turnRunning
	b.mu.Unlock()
	if process == nil || !running {
		return nil
	}
	return process.conn.notify("session/cancel", cancelParams{SessionID: sessionID})
}

// HandleCompact runs devin's /compact command as a turn.
func (b *Bridge) HandleCompact() error {
	b.mu.Lock()
	commands := b.slashCommands
	b.mu.Unlock()
	if !containsString(commands, "/compact") {
		return fmt.Errorf("COMPACT_UNSUPPORTED: devin offers no /compact in this session (commands: %v)", commands)
	}
	return b.HandleMessage(MessageParams{Content: "/compact"})
}

// HandleConfig applies a config:<json> request: model and effort take effect
// on the next model call; disabled tools are fixed at start.
func (b *Bridge) HandleConfig(raw string) error {
	var request sessionConfigRequest
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if request.DisabledTools != nil {
		return errors.New("disabled_tools are fixed when devin starts (devin reads disabled_tools only for new sessions); this session keeps its old set")
	}
	if request.Model == nil && request.Effort == nil {
		return nil
	}
	b.mu.Lock()
	catalog := b.catalog
	model := familyOfVariant(catalog, b.modelUID)
	effort := effortOfVariant(catalog, b.modelUID)
	b.mu.Unlock()
	if request.Model != nil {
		model = *request.Model
		if request.Effort == nil {
			effort = ""
		}
	}
	if request.Effort != nil {
		effort = *request.Effort
	}
	uid, err := resolveDevinModel(catalog, model, effort)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(b.ctx, time.Minute)
	defer cancel()
	if err := b.setModel(ctx, uid); err != nil {
		return err
	}
	b.emitSessionInfo()
	return nil
}

func (b *Bridge) emitSessionInfo() {
	b.mu.Lock()
	info := &msg.SessionInfo{
		SystemPrompt:   b.systemPrompt,
		WorkingDir:     b.workDir,
		Model:          familyOfVariant(b.catalog, b.modelUID),
		Effort:         effortOfVariant(b.catalog, b.modelUID),
		PermissionMode: b.start.PermissionMode,
		SlashCommands:  append([]string(nil), b.slashCommands...),
	}
	for _, server := range b.mcpServers {
		info.MCPServers = append(info.MCPServers, msg.MCPServerInfo{Name: server.Name})
	}
	bridgeID, harnessID, clientID := b.bridgeSessionID, b.harnessSessionID, b.clientID
	b.mu.Unlock()
	b.emit(msg.Event{
		Type:             msg.EventSessionInfo,
		Harness:          harness,
		BridgeSessionID:  bridgeID,
		HarnessSessionID: harnessID,
		ClientRequestID:  clientID,
		Timestamp:        time.Now(),
		Info:             info,
	})
}

func (b *Bridge) Shutdown() {
	b.mu.Lock()
	process, sessionDir := b.process, b.sessionDir
	b.mu.Unlock()
	if process != nil {
		b.killProcess(process)
	}
	if sessionDir != "" {
		_ = os.RemoveAll(sessionDir)
	}
}

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}
