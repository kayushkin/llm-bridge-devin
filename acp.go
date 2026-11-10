package main

// A JSON-RPC 2.0 connection to `devin acp` over its stdin and stdout, as the
// Agent Client Protocol defines it: one JSON object per line, requests in both
// directions, and notifications (session/update and Cognition's _cognition.ai/
// methods) from the agent.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kayushkin/llm-bridge/ndjson"
)

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *rpcError) Error() string {
	if len(e.Data) > 0 {
		return fmt.Sprintf("acp error %d: %s (%s)", e.Code, e.Message, e.Data)
	}
	return fmt.Sprintf("acp error %d: %s", e.Code, e.Message)
}

type rpcFrame struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcReply struct {
	result json.RawMessage
	err    *rpcError
}

// agentRequestHandler answers a request the agent sends to us. It returns the
// result to send back, or an rpcError.
type agentRequestHandler func(method string, params json.RawMessage) (any, *rpcError)

// agentNotificationHandler receives every notification the agent sends, with
// the whole frame so the caller can forward it raw.
type agentNotificationHandler func(method string, params json.RawMessage, frame json.RawMessage)

type acpConnection struct {
	writer      io.Writer
	writeMutex  sync.Mutex
	nextID      atomic.Int64
	pendingLock sync.Mutex
	pending     map[int64]chan rpcReply

	onRequest      agentRequestHandler
	onNotification agentNotificationHandler

	// lastReceivedUnixNano is when the agent last sent anything; the turn
	// watchdog reads it.
	lastReceivedUnixNano atomic.Int64

	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func newACPConnection(writer io.Writer, onRequest agentRequestHandler, onNotification agentNotificationHandler) *acpConnection {
	c := &acpConnection{
		writer:         writer,
		pending:        map[int64]chan rpcReply{},
		onRequest:      onRequest,
		onNotification: onNotification,
		closed:         make(chan struct{}),
	}
	c.lastReceivedUnixNano.Store(time.Now().UnixNano())
	return c
}

func (c *acpConnection) lastReceived() time.Time {
	return time.Unix(0, c.lastReceivedUnixNano.Load())
}

func (c *acpConnection) write(frame rpcFrame) error {
	frame.JSONRPC = "2.0"
	line, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("encode acp frame: %w", err)
	}
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()
	if _, err := c.writer.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write to devin acp: %w", err)
	}
	return nil
}

// readLoop reads frames until the agent's stdout closes, then fails every
// call still waiting. It returns the read error, or nil at a clean EOF.
func (c *acpConnection) readLoop(reader io.Reader) error {
	buffered := bufio.NewReader(reader)
	var loopErr error
	for {
		line, err := ndjson.ReadLine(buffered, ndjson.MaxLineBytes)
		if errors.Is(err, ndjson.ErrLineTooLong) {
			log.Printf("[acp] dropped a frame above %d bytes from devin", ndjson.MaxLineBytes)
			continue
		}
		if len(line) > 0 {
			c.lastReceivedUnixNano.Store(time.Now().UnixNano())
			c.dispatch(line)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				loopErr = err
			}
			break
		}
	}
	c.close(fmt.Errorf("devin acp closed its output: %v", loopErr))
	return loopErr
}

func (c *acpConnection) close(reason error) {
	c.closeOnce.Do(func() {
		c.closeErr = reason
		close(c.closed)
		c.pendingLock.Lock()
		for id, ch := range c.pending {
			ch <- rpcReply{err: &rpcError{Code: -32099, Message: reason.Error()}}
			delete(c.pending, id)
		}
		c.pendingLock.Unlock()
	})
}

func (c *acpConnection) dispatch(line []byte) {
	var frame rpcFrame
	if err := json.Unmarshal(line, &frame); err != nil {
		log.Printf("[acp] unparseable frame from devin: %v", err)
		return
	}
	switch {
	case frame.Method != "" && len(frame.ID) > 0:
		// A request from the agent. Answer it off the read loop so a slow
		// answer never stops notifications from arriving.
		go c.answer(frame)
	case frame.Method != "":
		if c.onNotification != nil {
			c.onNotification(frame.Method, frame.Params, json.RawMessage(append([]byte(nil), line...)))
		}
	case len(frame.ID) > 0:
		var id int64
		if err := json.Unmarshal(frame.ID, &id); err != nil {
			log.Printf("[acp] response with a non-numeric id %s", frame.ID)
			return
		}
		c.pendingLock.Lock()
		ch, ok := c.pending[id]
		delete(c.pending, id)
		c.pendingLock.Unlock()
		if !ok {
			log.Printf("[acp] response to unknown request %d", id)
			return
		}
		ch <- rpcReply{result: frame.Result, err: frame.Error}
	}
}

func (c *acpConnection) answer(frame rpcFrame) {
	var result any
	var rerr *rpcError
	if c.onRequest == nil {
		rerr = &rpcError{Code: -32601, Message: "method not found: " + frame.Method}
	} else {
		result, rerr = c.onRequest(frame.Method, frame.Params)
	}
	reply := rpcFrame{ID: frame.ID}
	if rerr != nil {
		reply.Error = rerr
	} else {
		encoded, err := json.Marshal(result)
		if err != nil {
			reply.Error = &rpcError{Code: -32603, Message: "encode result: " + err.Error()}
		} else {
			reply.Result = encoded
		}
	}
	if err := c.write(reply); err != nil {
		log.Printf("[acp] answer %s: %v", frame.Method, err)
	}
}

// call sends a request and waits for its response. out may be nil.
func (c *acpConnection) call(ctx context.Context, method string, params any, out any) error {
	id := c.nextID.Add(1)
	encodedID, _ := json.Marshal(id)
	encodedParams, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encode %s params: %w", method, err)
	}
	ch := make(chan rpcReply, 1)
	c.pendingLock.Lock()
	select {
	case <-c.closed:
		c.pendingLock.Unlock()
		return c.closeErr
	default:
	}
	c.pending[id] = ch
	c.pendingLock.Unlock()

	if err := c.write(rpcFrame{ID: encodedID, Method: method, Params: encodedParams}); err != nil {
		c.pendingLock.Lock()
		delete(c.pending, id)
		c.pendingLock.Unlock()
		return err
	}
	select {
	case reply := <-ch:
		if reply.err != nil {
			return fmt.Errorf("%s: %w", method, reply.err)
		}
		if out != nil && len(reply.result) > 0 && string(reply.result) != "null" {
			if err := json.Unmarshal(reply.result, out); err != nil {
				return fmt.Errorf("decode %s result: %w", method, err)
			}
		}
		return nil
	case <-ctx.Done():
		c.pendingLock.Lock()
		delete(c.pending, id)
		c.pendingLock.Unlock()
		return fmt.Errorf("%s: %w", method, ctx.Err())
	}
}

func (c *acpConnection) notify(method string, params any) error {
	encoded, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encode %s params: %w", method, err)
	}
	return c.write(rpcFrame{Method: method, Params: encoded})
}
