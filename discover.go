package main

// Discovery reads devin's own session index, sessions.db, read-only: `devin
// list` covers only the current directory, and the bridge wants every session
// on the machine.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
	_ "modernc.org/sqlite"
)

func devinSessionsDBPath() (string, error) {
	if dir := os.Getenv("DEVIN_SESSIONS_DIR"); dir != "" {
		return filepath.Join(dir, "sessions.db"), nil
	}
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		dataHome = filepath.Join(home, ".local", "share")
	}
	return filepath.Join(dataHome, "devin", "cli", "sessions.db"), nil
}

type devinSessionRow struct {
	ID               string
	WorkingDirectory string
	Title            string
	CreatedAt        time.Time
	LastActivityAt   time.Time
	MessageCount     int
}

func readDevinSessions(dbPath string, since time.Time) ([]devinSessionRow, error) {
	if _, err := os.Stat(dbPath); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dbPath, err)
	}
	defer db.Close()
	rows, err := db.Query(`
		SELECT s.id, s.working_directory, COALESCE(s.title, ''), s.created_at, s.last_activity_at,
		       (SELECT COUNT(*) FROM message_nodes m WHERE m.session_id = s.id)
		FROM sessions s
		WHERE s.hidden = 0 AND s.last_activity_at >= ?
		ORDER BY s.last_activity_at DESC`, since.Unix())
	if err != nil {
		return nil, fmt.Errorf("query devin sessions: %w", err)
	}
	defer rows.Close()
	var sessions []devinSessionRow
	for rows.Next() {
		var row devinSessionRow
		var created, active int64
		if err := rows.Scan(&row.ID, &row.WorkingDirectory, &row.Title, &created, &active, &row.MessageCount); err != nil {
			return nil, err
		}
		row.CreatedAt = time.Unix(created, 0).UTC()
		row.LastActivityAt = time.Unix(active, 0).UTC()
		sessions = append(sessions, row)
	}
	return sessions, rows.Err()
}

// runDiscover prints every devin session (or those active since sinceRFC3339)
// as msg.StoredSession JSON.
func runDiscover(out io.Writer, sinceRFC3339 string) int {
	since := time.Unix(0, 0)
	if sinceRFC3339 != "" {
		parsed, err := time.Parse(time.RFC3339, sinceRFC3339)
		if err != nil {
			fmt.Fprintf(os.Stderr, "changed-since: %v\n", err)
			return 2
		}
		since = parsed
	}
	dbPath, err := devinSessionsDBPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "discover: %v\n", err)
		return 1
	}
	rows, err := readDevinSessions(dbPath, since)
	if err != nil {
		fmt.Fprintf(os.Stderr, "discover: %v\n", err)
		return 1
	}
	stored := make([]msg.StoredSession, 0, len(rows))
	for _, row := range rows {
		stored = append(stored, msg.StoredSession{
			HarnessSessionID: row.ID,
			Harness:          harness,
			Project:          row.WorkingDirectory,
			Prompt:           row.Title,
			CreatedAt:        row.CreatedAt,
			UpdatedAt:        row.LastActivityAt,
			Path:             dbPath,
			TurnCount:        row.MessageCount,
		})
	}
	if err := json.NewEncoder(out).Encode(stored); err != nil {
		fmt.Fprintf(os.Stderr, "discover: %v\n", err)
		return 1
	}
	return 0
}

// runImportHistory loads a devin session through session/load and prints the
// history devin replays as canonical events. Exit 0 with no output for a
// session devin does not know.
func runImportHistory(sessionID string, out io.Writer) int {
	dbPath, err := devinSessionsDBPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "import-history: %v\n", err)
		return 1
	}
	rows, err := readDevinSessions(dbPath, time.Unix(0, 0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "import-history: %v\n", err)
		return 1
	}
	var row *devinSessionRow
	for i := range rows {
		if rows[i].ID == sessionID {
			row = &rows[i]
		}
	}
	if row == nil {
		return 0
	}
	cfg := loadConfig()
	encoder := json.NewEncoder(out)
	var emitErr error
	emit := func(event msg.Event) {
		if emitErr == nil {
			emitErr = encoder.Encode(event)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := replayDevinSession(ctx, cfg, row.ID, row.WorkingDirectory, emit); err != nil {
		fmt.Fprintf(os.Stderr, "import-history: %v\n", err)
		return 1
	}
	if emitErr != nil {
		fmt.Fprintf(os.Stderr, "import-history: %v\n", emitErr)
		return 1
	}
	return 0
}

// replayDevinSession runs a throwaway `devin acp` that loads the session, and
// translates what it replays. User messages come back as user_message events.
func replayDevinSession(ctx context.Context, cfg Config, sessionID, workDir string, emit func(msg.Event)) error {
	cmd := exec.CommandContext(ctx, cfg.DevinPath, "acp")
	cmd.Dir = workDir
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	defer func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	tr := newTranslator(emit)
	tr.setSessionIDs("", sessionID)
	userText := ""
	agentSpoke := false
	// endTurn closes the replayed turn the way a live one closes, with a
	// result, so the history has turn boundaries.
	endTurn := func() {
		if agentSpoke {
			tr.finishTurn(promptResult{StopReason: "end_turn"}, "", nil)
			agentSpoke = false
		}
	}
	flushUser := func() {
		if userText == "" {
			return
		}
		endTurn()
		e := tr.event(msg.EventUserMessage)
		e.Result = &msg.ResultEvent{Text: userText}
		emit(e)
		userText = ""
		tr.beginTurn()
	}
	conn := newACPConnection(stdin, nil, func(method string, params json.RawMessage, frame json.RawMessage) {
		if method != "session/update" {
			return
		}
		var notification sessionNotification
		if json.Unmarshal(params, &notification) != nil {
			return
		}
		var update sessionUpdate
		if json.Unmarshal(notification.Update, &update) != nil {
			return
		}
		switch update.SessionUpdate {
		case "user_message_chunk":
			var block contentBlock
			_ = json.Unmarshal(update.Content, &block)
			userText += block.Text
			return
		case "agent_message_chunk", "agent_thought_chunk", "tool_call", "tool_call_update", "plan":
			flushUser()
			agentSpoke = true
			tr.handleSessionUpdate(params, frame)
		}
	})
	go func() { _ = conn.readLoop(stdout) }()
	var initResult initializeResult
	if err := conn.call(ctx, "initialize", initializeParams{ProtocolVersion: acpProtocolVersion, ClientInfo: implementationInfo{Name: "llm-bridge-devin", Version: buildRevision()}}, &initResult); err != nil {
		return err
	}
	var setup sessionSetupResult
	if err := conn.call(ctx, "session/load", loadSessionParams{SessionID: sessionID, Cwd: workDir, MCPServers: []mcpServer{}}, &setup); err != nil {
		return err
	}
	flushUser()
	endTurn()
	return nil
}

func lookPath(name string) (string, error) {
	return exec.LookPath(name)
}
