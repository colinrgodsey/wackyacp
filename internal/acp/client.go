package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/colinrgodsey/wackyacp/internal/session"
)

var (
	ErrHarnessDied     = errors.New("harness process died (EOF on stdout)")
	ErrTurnFailed      = errors.New("acp turn error")
	ErrSessionMismatch = errors.New("session ownership assertion failed")
)

// TurnCallbacks receives streamed text deltas and warnings during a prompt turn.
type TurnCallbacks struct {
	OnChunk   func(text string) error
	OnWarning func(warning string) error
}

type activeTurn struct {
	sessionID string
	callbacks TurnCallbacks
	usage     UsageMetrics
	mu        sync.Mutex
	textBuf   strings.Builder
	closed    bool
	chunkMu   sync.Mutex
}

func (t *activeTurn) appendText(text string) {
	if text == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.textBuf.WriteString(text)
}

func (t *activeTurn) flushInternal(closeTurn bool) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	if closeTurn {
		t.closed = true
	}
	if t.textBuf.Len() == 0 {
		t.mu.Unlock()
		return nil
	}
	text := t.textBuf.String()
	t.textBuf.Reset()
	onChunk := t.callbacks.OnChunk
	t.mu.Unlock()

	if onChunk != nil && text != "" {
		t.chunkMu.Lock()
		defer t.chunkMu.Unlock()
		return onChunk(text)
	}
	return nil
}

func (t *activeTurn) flush() error {
	return t.flushInternal(false)
}

func (t *activeTurn) closeAndFlush() error {
	return t.flushInternal(true)
}

// Client implements a bidirectional ACP JSON-RPC client over stdio.
type Client struct {
	stdin     io.WriteCloser
	stdout    io.ReadCloser
	writeMu   sync.Mutex
	pendingMu sync.Mutex
	pending   map[int64]chan *rpcResponse
	reqID     atomic.Int64

	Capabilities AgentCapabilities
	activeTurnMu sync.RWMutex
	activeTurn   *activeTurn

	// PermissionMode controls how session/request_permission from the harness is
	// answered: "deny" (D117 default) always picks a reject/cancel option so the
	// harness can never run unapproved side-effecting tools; "approve" always
	// picks the first allow option so the bridge acts as an approving elbow for
	// trusted local harnesses (used for Claude via claude-agent-acp).
	PermissionMode string

	doneCh chan struct{}
	errMu  sync.RWMutex
	err    error
}

// NewClient initializes a new ACP client communicating over stdin and stdout.
func NewClient(stdin io.WriteCloser, stdout io.ReadCloser) *Client {
	c := &Client{
		stdin:   stdin,
		stdout:  stdout,
		pending: make(map[int64]chan *rpcResponse),
		doneCh:  make(chan struct{}),
	}
	go c.readLoop()
	return c
}

func (c *Client) readLoop() {
	scanner := bufio.NewScanner(c.stdout)
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 10*1024*1024) // 10MB max line buffer

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(line, &raw); err != nil {
			continue
		}

		// Check if it's a response (has id and either result or error)
		if _, hasID := raw["id"]; hasID {
			if _, hasResult := raw["result"]; hasResult {
				c.handleResponse(line)
				continue
			}
			if _, hasError := raw["error"]; hasError {
				c.handleResponse(line)
				continue
			}
		}

		// Check if it's an incoming request from harness (has id and method)
		if _, hasID := raw["id"]; hasID {
			if _, hasMethod := raw["method"]; hasMethod {
				c.handleIncomingRequest(line)
				continue
			}
		}

		// Check if it's an incoming notification (has method, no id)
		if _, hasMethod := raw["method"]; hasMethod {
			c.handleIncomingNotification(line)
			continue
		}
	}

	c.errMu.Lock()
	if err := scanner.Err(); err != nil {
		c.err = fmt.Errorf("reading harness stdout: %w", err)
	} else {
		c.err = ErrHarnessDied
	}
	c.errMu.Unlock()

	// Notify all pending callers of connection termination
	c.pendingMu.Lock()
	for _, ch := range c.pending {
		close(ch)
	}
	c.pending = make(map[int64]chan *rpcResponse)
	c.pendingMu.Unlock()

	close(c.doneCh)
}

func (c *Client) handleResponse(line []byte) {
	var resp rpcResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		return
	}

	var id int64
	switch v := resp.ID.(type) {
	case float64:
		id = int64(v)
	case int64:
		id = v
	default:
		return
	}

	c.pendingMu.Lock()
	ch, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()

	if ok {
		ch <- &resp
	}
}

// activeTurnRef returns the currently active turn, or nil if no turn is in
// flight. The reference is stable: Prompt clears activeTurn only after its own
// deferred closeAndFlush has run.
func (c *Client) activeTurnRef() *activeTurn {
	c.activeTurnMu.RLock()
	defer c.activeTurnMu.RUnlock()
	return c.activeTurn
}

// flushActiveTurn flushes whichever turn is active, if any. Call this when a
// logical message boundary crosses outside the agent_message_chunk stream
// (harness requests, non-text updates) so buffered deltas emit before the
// boundary is processed.
func (c *Client) flushActiveTurn() {
	if turn := c.activeTurnRef(); turn != nil {
		_ = turn.flush()
	}
}

func (c *Client) handleIncomingRequest(line []byte) {
	var req rpcRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return
	}

	switch req.Method {
	case "session/request_permission":
		c.flushActiveTurn()

		var params PermissionRequestParams
		raw, _ := json.Marshal(req.Params)
		_ = json.Unmarshal(raw, &params)

		if c.PermissionMode == "approve" {
			// Approve posture: pick the first allow option so the harness can run
			// its tools. Only meaningful for trusted local harnesses.
			var chosenOptionID string
			for _, opt := range params.Options {
				if opt.Kind == "allow_once" || opt.Kind == "allow_always" ||
					strings.Contains(strings.ToLower(opt.Name), "allow") ||
					strings.Contains(strings.ToLower(opt.OptionID), "allow") {
					chosenOptionID = opt.OptionID
					break
				}
			}
			outcome := map[string]any{"outcome": "cancelled"}
			if chosenOptionID != "" {
				outcome = map[string]any{"outcome": "selected", "optionId": chosenOptionID}
			}
			_ = c.sendResponse(req.ID, map[string]any{"outcome": outcome}, nil)

			if turn := c.activeTurnRef(); turn != nil && turn.callbacks.OnWarning != nil {
				_ = turn.callbacks.OnWarning(fmt.Sprintf("permission auto-approved: %s", params.ToolCall.Title))
			}
			break
		}

		// Auto-deny posture (D117 Decision)
		var chosenOptionID string
		for _, opt := range params.Options {
			if opt.Kind == "reject_once" || opt.Kind == "reject_always" ||
				strings.Contains(strings.ToLower(opt.Name), "deny") ||
				strings.Contains(strings.ToLower(opt.OptionID), "deny") {
				chosenOptionID = opt.OptionID
				break
			}
		}

		var outcome any
		if chosenOptionID != "" {
			outcome = map[string]any{
				"outcome":  "selected",
				"optionId": chosenOptionID,
			}
		} else {
			outcome = map[string]any{
				"outcome": "cancelled",
			}
		}

		// Send deny response back over ACP to unblock harness
		_ = c.sendResponse(req.ID, map[string]any{"outcome": outcome}, nil)

		// Surface warning over D112
		turn := c.activeTurnRef()
		if turn != nil && turn.callbacks.OnWarning != nil {
			warning := fmt.Sprintf("permission request auto-denied: %s", params.ToolCall.Title)
			if params.ToolCall.Title == "" {
				warning = fmt.Sprintf("permission request auto-denied for tool call %s", params.ToolCall.ToolCallID)
			}
			_ = turn.callbacks.OnWarning(warning)
		}

	default:
		// Unsupported incoming request
		_ = c.sendResponse(req.ID, nil, &RPCError{
			Code:    -32601,
			Message: fmt.Sprintf("method %q not supported", req.Method),
		})
	}
}

func (c *Client) handleIncomingNotification(line []byte) {
	var notif rpcRequest
	if err := json.Unmarshal(line, &notif); err != nil {
		return
	}

	if notif.Method != "session/update" {
		if turn := c.activeTurnRef(); turn != nil {
			_ = turn.flush()
			if turn.callbacks.OnWarning != nil {
				_ = turn.callbacks.OnWarning("unmapped acp notification: " + notif.Method)
			}
		}
		return
	}

	var params struct {
		SessionID string          `json:"sessionId"`
		Update    json.RawMessage `json:"update"`
	}
	raw, _ := json.Marshal(notif.Params)
	if err := json.Unmarshal(raw, &params); err != nil {
		return
	}

	turn := c.activeTurnRef()
	if turn == nil {
		return
	}
	if params.SessionID != "" && turn.sessionID != "" && turn.sessionID != params.SessionID {
		return
	}

	var update struct {
		SessionUpdate string          `json:"sessionUpdate"`
		Content       json.RawMessage `json:"content"`
		Used          int64           `json:"used"`
		Size          int64           `json:"size"`
	}
	if err := json.Unmarshal(params.Update, &update); err != nil {
		return
	}

	if update.SessionUpdate != "agent_message_chunk" {
		_ = turn.flush()
	}

	switch update.SessionUpdate {
	case "agent_message_chunk":
		var content struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(update.Content, &content); err == nil && content.Text != "" {
			turn.appendText(content.Text)
		}

	case "usage_update":
		turn.mu.Lock()
		defer turn.mu.Unlock()
		if update.Used > 0 {
			turn.usage.TotalTokens = update.Used
		}

	default:
		// Forward unmapped updates as warnings (per D117 and claude review)
		if turn.callbacks.OnWarning != nil {
			_ = turn.callbacks.OnWarning("unmapped acp session update: " + update.SessionUpdate)
		}
	}
}

func (c *Client) sendRequest(ctx context.Context, method string, params any) (*rpcResponse, error) {
	id := c.reqID.Add(1)
	respCh := make(chan *rpcResponse, 1)

	c.pendingMu.Lock()
	c.pending[id] = respCh
	c.pendingMu.Unlock()

	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}
	data, err := json.Marshal(req)
	if err != nil {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	c.writeMu.Lock()
	_, err = fmt.Fprintf(c.stdin, "%s\n", data)
	c.writeMu.Unlock()
	if err != nil {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, fmt.Errorf("writing request to harness: %w", err)
	}

	select {
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, ctx.Err()

	case <-c.doneCh:
		c.errMu.RLock()
		defer c.errMu.RUnlock()
		return nil, c.err

	case resp, ok := <-respCh:
		if !ok {
			c.errMu.RLock()
			defer c.errMu.RUnlock()
			return nil, c.err
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("%w: [%d] %s", ErrTurnFailed, resp.Error.Code, resp.Error.Message)
		}
		return resp, nil
	}
}

func (c *Client) sendNotification(method string, params any) error {
	notif := rpcRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
	}
	data, err := json.Marshal(notif)
	if err != nil {
		return fmt.Errorf("marshaling notification: %w", err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = fmt.Fprintf(c.stdin, "%s\n", data)
	return err
}

func (c *Client) sendResponse(id any, result any, rpcErr *RPCError) error {
	resp := rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   rpcErr,
	}
	if result != nil {
		raw, err := json.Marshal(result)
		if err != nil {
			return err
		}
		resp.Result = raw
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = fmt.Fprintf(c.stdin, "%s\n", data)
	return err
}

// Initialize performs ACP handshake and saves advertised capabilities.
func (c *Client) Initialize(ctx context.Context) (*InitializeResult, error) {
	params := map[string]any{
		"protocolVersion": 1,
		"clientCapabilities": map[string]any{
			"sessionCapabilities": map[string]any{
				"compaction": false,
			},
			"fs": map[string]any{
				"readTextFile":  false,
				"writeTextFile": false,
			},
		},
		"clientInfo": map[string]any{
			"name":    "wackyacp",
			"version": "0.1.0",
		},
	}

	resp, err := c.sendRequest(ctx, "initialize", params)
	if err != nil {
		return nil, fmt.Errorf("acp initialize handshake failed: %w", err)
	}

	var result InitializeResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return nil, fmt.Errorf("unmarshaling initialize result: %w", err)
	}

	c.Capabilities = result.AgentCapabilities
	return &result, nil
}

// ResumeSession attempts session/resume with the harness.
func (c *Client) ResumeSession(ctx context.Context, sessionID, agentFolder string) error {
	params := map[string]any{
		"sessionId": sessionID,
		"cwd":       agentFolder,
		"_meta": map[string]any{
			"agent_folder": agentFolder,
		},
	}
	resp, err := c.sendRequest(ctx, "session/resume", params)
	if err != nil {
		return err
	}

	var result ResumeSessionResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return fmt.Errorf("unmarshaling resume result: %w", err)
	}

	// Verify session ownership if metadata returned
	if result.Meta != nil {
		if folder, ok := result.Meta["agent_folder"].(string); ok && folder != "" && folder != agentFolder {
			return fmt.Errorf("%w: resumed session agent_folder %q != %q", ErrSessionMismatch, folder, agentFolder)
		}
	}
	return nil
}

// LoadSession attempts session/load with the harness.
func (c *Client) LoadSession(ctx context.Context, sessionID, agentFolder string) error {
	params := map[string]any{
		"sessionId": sessionID,
		"cwd":       agentFolder,
		"_meta": map[string]any{
			"agent_folder": agentFolder,
		},
	}
	resp, err := c.sendRequest(ctx, "session/load", params)
	if err != nil {
		return err
	}

	var result LoadSessionResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return fmt.Errorf("unmarshaling load result: %w", err)
	}

	// Verify session ownership if metadata returned
	if result.Meta != nil {
		if folder, ok := result.Meta["agent_folder"].(string); ok && folder != "" && folder != agentFolder {
			return fmt.Errorf("%w: loaded session agent_folder %q != %q", ErrSessionMismatch, folder, agentFolder)
		}
	}
	return nil
}

// NewSession creates a fresh session on the harness and returns the sessionID.
func (c *Client) NewSession(ctx context.Context, agentFolder string) (string, error) {
	params := map[string]any{
		"cwd":        agentFolder,
		"mcpServers": []any{},
		"_meta": map[string]any{
			"agent_folder": agentFolder,
		},
	}
	resp, err := c.sendRequest(ctx, "session/new", params)
	if err != nil {
		return "", fmt.Errorf("session/new failed: %w", err)
	}

	var result NewSessionResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return "", fmt.Errorf("unmarshaling new session result: %w", err)
	}
	if result.SessionID == "" {
		return "", errors.New("empty sessionId returned from session/new")
	}
	return result.SessionID, nil
}

// EstablishSession executes the capability-driven fallback chain:
// session/resume -> session/load -> session/new.
func (c *Client) EstablishSession(ctx context.Context, agentFolder string, saved *session.SessionData) (string, error) {
	if saved != nil && saved.SessionID != "" {
		// Verify ownership assertion on the saved record
		if saved.AgentFolder != "" && saved.AgentFolder != agentFolder {
			return "", fmt.Errorf("%w: saved session recorded %q != %q", ErrSessionMismatch, saved.AgentFolder, agentFolder)
		}

		// 1. Try session/resume if capability advertised
		if c.Capabilities.SessionCapabilities.Resume != nil {
			if err := c.ResumeSession(ctx, saved.SessionID, agentFolder); err == nil {
				return saved.SessionID, nil
			} else if errors.Is(err, ErrSessionMismatch) {
				fmt.Fprintf(os.Stderr, "wackyacp: warning: session ownership mismatch on resume: %v\n", err)
			}
		}

		// 2. Try session/load if capability advertised
		if c.Capabilities.LoadSession {
			if err := c.LoadSession(ctx, saved.SessionID, agentFolder); err == nil {
				return saved.SessionID, nil
			} else if errors.Is(err, ErrSessionMismatch) {
				fmt.Fprintf(os.Stderr, "wackyacp: warning: session ownership mismatch on load: %v\n", err)
			}
		}
	}

	// 3. Fall back to fresh session/new
	sessionID, err := c.NewSession(ctx, agentFolder)
	if err != nil {
		return "", err
	}

	// Persist new session file
	if _, err := session.WriteSession(agentFolder, sessionID); err != nil {
		return "", fmt.Errorf("saving session file: %w", err)
	}

	return sessionID, nil
}

// Prompt executes a prompt turn against the specified session.
func (c *Client) Prompt(ctx context.Context, sessionID, promptText string, callbacks TurnCallbacks) (*PromptResult, error) {
	turn := &activeTurn{
		sessionID: sessionID,
		callbacks: callbacks,
		usage: UsageMetrics{
			Backend: "wackyacp",
		},
	}

	c.activeTurnMu.Lock()
	c.activeTurn = turn
	c.activeTurnMu.Unlock()

	defer func() {
		_ = turn.closeAndFlush()
		c.activeTurnMu.Lock()
		c.activeTurn = nil
		c.activeTurnMu.Unlock()
	}()

	params := map[string]any{
		"sessionId": sessionID,
		"prompt": []map[string]any{
			{
				"type": "text",
				"text": promptText,
			},
		},
	}

	resp, err := c.sendRequest(ctx, "session/prompt", params)
	if err != nil {
		return nil, err
	}

	var promptResp struct {
		StopReason string `json:"stopReason"`
		Usage      *struct {
			InputTokens  int64 `json:"inputTokens"`
			OutputTokens int64 `json:"outputTokens"`
			TotalTokens  int64 `json:"totalTokens"`
		} `json:"usage"`
	}

	_ = json.Unmarshal(resp.Result, &promptResp)

	// Scoped lock: closeAndFlush re-acquires turn.mu, so the lock must be
	// released (via the closure's defer) before flushing.
	usage := func() UsageMetrics {
		turn.mu.Lock()
		defer turn.mu.Unlock()
		if promptResp.Usage != nil {
			turn.usage.PromptTokens = promptResp.Usage.InputTokens
			turn.usage.CompletionTokens = promptResp.Usage.OutputTokens
			turn.usage.TotalTokens = promptResp.Usage.TotalTokens
		} else if turn.usage.TotalTokens > 0 && turn.usage.PromptTokens == 0 {
			// When usage came via usage_update
			turn.usage.PromptTokens = turn.usage.TotalTokens
		}
		return turn.usage
	}()

	_ = turn.closeAndFlush()

	return &PromptResult{
		StopReason: promptResp.StopReason,
		Usage:      usage,
	}, nil
}

// Cancel sends a session/cancel notification to abort an in-flight prompt turn.
func (c *Client) Cancel(sessionID string) error {
	return c.sendNotification("session/cancel", map[string]any{
		"sessionId": sessionID,
	})
}
