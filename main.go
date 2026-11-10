// llm-bridge-devin runs Devin for Terminal (`devin acp`, the Agent Client
// Protocol server) behind llm-bridge-server's harness protocol: NDJSON
// requests on stdin, canonical msg.Event NDJSON on stdout, logs on stderr.
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
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/kayushkin/llm-bridge/msg"
	"github.com/kayushkin/llm-bridge/ndjson"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "-oneshot":
			os.Exit(runOneShot(os.Stdin, os.Stdout))
		case "-discover":
			os.Exit(runDiscover(os.Stdout, ""))
		case "-changed-since":
			if len(os.Args) < 3 {
				fmt.Fprintln(os.Stderr, "usage: llm-bridge-devin -changed-since <RFC3339>")
				os.Exit(2)
			}
			os.Exit(runDiscover(os.Stdout, os.Args[2]))
		case "-import-history":
			if len(os.Args) < 3 {
				fmt.Fprintln(os.Stderr, "usage: llm-bridge-devin -import-history <devin session id>")
				os.Exit(2)
			}
			os.Exit(runImportHistory(os.Args[2], os.Stdout))
		case "-version":
			fmt.Println(buildRevision())
			os.Exit(0)
		}
	}
	if os.Getenv("LLMBRIDGE_PTY_MODE") == "1" {
		execDevinPTY()
		return
	}

	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[llm-bridge-devin] ")

	cfg := loadConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var emitMutex sync.Mutex
	encoder := json.NewEncoder(os.Stdout)
	emit := func(event msg.Event) {
		emitMutex.Lock()
		defer emitMutex.Unlock()
		if err := encoder.Encode(event); err != nil {
			log.Printf("[emit] %v", err)
		}
	}
	bridge := NewBridge(ctx, cfg, emit)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for sig := range signals {
			if sig == syscall.SIGINT {
				if err := bridge.HandleInterrupt(); err != nil {
					log.Printf("interrupt: %v", err)
				}
				continue
			}
			bridge.Shutdown()
			cancel()
			os.Exit(0)
		}
	}()

	reader := bufio.NewReader(os.Stdin)
	for {
		line, readErr := ndjson.ReadLine(reader, ndjson.MaxLineBytes)
		if errors.Is(readErr, ndjson.ErrLineTooLong) {
			bridge.emitError("INVALID_PARAMS", fmt.Sprintf("request above %d bytes dropped", ndjson.MaxLineBytes), false)
			continue
		}
		if len(line) > 0 {
			handleRequest(bridge, line)
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				log.Printf("stdin: %v", readErr)
			}
			break
		}
	}
	log.Printf("stdin closed, shutting down")
	bridge.Shutdown()
}

func handleRequest(bridge *Bridge, line []byte) {
	var request struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params,omitempty"`
	}
	if err := json.Unmarshal(line, &request); err != nil {
		bridge.emitError("INVALID_PARAMS", "unparseable request: "+err.Error(), false)
		return
	}
	switch request.Method {
	case "start":
		var params StartParams
		if err := json.Unmarshal(request.Params, &params); err != nil {
			bridge.emitError("INVALID_PARAMS", err.Error(), false)
			return
		}
		bridge.mu.Lock()
		bridge.bridgeSessionID = params.BridgeSessionID
		bridge.clientID = params.ClientID
		bridge.mu.Unlock()
		if err := bridge.HandleStart(params); err != nil {
			code := "START_FAILED"
			if params.Resume {
				code = "RESUME_FAILED"
			}
			if strings.HasPrefix(err.Error(), "FORK_UNSUPPORTED") {
				code = "FORK_UNSUPPORTED"
			}
			bridge.emitError(code, err.Error(), false)
		}
	case "message":
		var params MessageParams
		if err := json.Unmarshal(request.Params, &params); err != nil {
			bridge.emitError("INVALID_PARAMS", err.Error(), false)
			return
		}
		if err := bridge.HandleMessage(params); err != nil {
			bridge.emitError("MESSAGE_FAILED", err.Error(), false)
		}
	case "compact":
		if err := bridge.HandleCompact(); err != nil {
			bridge.emitError("COMPACT_FAILED", err.Error(), false)
		}
	case "resume":
		// The process already holds the session; nothing to reopen.
	default:
		if strings.HasPrefix(request.Method, "config:") {
			if err := bridge.HandleConfig(strings.TrimPrefix(request.Method, "config:")); err != nil {
				bridge.emitError("CONFIG_FAILED", err.Error(), false)
			}
			return
		}
		bridge.emitError("UNKNOWN_METHOD", "llm-bridge-devin does not handle "+request.Method, false)
	}
}

// execDevinPTY replaces this process with devin's own terminal UI, for the
// bridge's pty session mode, resuming the session when there is one.
func execDevinPTY() {
	cfg := loadConfig()
	path, err := lookPath(cfg.DevinPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "llm-bridge-devin pty: %v\n", err)
		os.Exit(127)
	}
	args := []string{path}
	if model := os.Getenv("LLMBRIDGE_PTY_MODEL"); model != "" {
		args = append(args, "--model", model)
	}
	if resumeID := os.Getenv("LLMBRIDGE_PTY_RESUME_ID"); resumeID != "" {
		args = append(args, "--resume", resumeID)
	}
	if err := syscall.Exec(path, args, os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "llm-bridge-devin pty: exec %s: %v\n", path, err)
		os.Exit(127)
	}
}
