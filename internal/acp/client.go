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
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

var (
	ErrHarnessDied     = errors.New("harness process died (EOF on stdout)")
	ErrTurnFailed      = errors.New("acp turn error")
	ErrSessionMismatch = errors.New("session ownership assertion failed")
)

// TurnCallbacks receives streamed text deltas, warnings, and tool events during a prompt turn.
type TurnCallbacks struct {
	OnChunk          func(text string) error
	OnWarning        func(warning string) error
	OnToolCall       func(*agentv1.ToolCall) error
	OnToolCallUpdate func(*agentv1.ToolCallUpdate) error
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
	case MethodSessionRequestPermission:
		c.flushActiveTurn()

		raw, err := json.Marshal(req.Params)
		if err != nil {
			c.respondPermissionError(req.ID, CodeInvalidParams, fmt.Sprintf("marshaling request params: %v", err))
			break
		}
		var params PermissionRequestParams
		if err := json.Unmarshal(raw, &params); err != nil {
			c.respondPermissionError(req.ID, CodeInvalidParams, fmt.Sprintf("invalid %s params: %v", MethodSessionRequestPermission, err))
			break
		}

		if c.PermissionMode == "approve" {
			// Approve posture: pick the first allow option so the harness can run
			// its tools. Only meaningful for trusted local harnesses.
			chosenOptionID := selectAllowOption(params.Options)
			var outcome map[string]any
			if chosenOptionID != "" {
				outcome = map[string]any{"outcome": OutcomeSelected, "optionId": chosenOptionID}
			} else {
				outcome = map[string]any{"outcome": OutcomeCancelled}
			}
			if err := c.sendResponse(req.ID, map[string]any{"outcome": outcome}, nil); err != nil {
				if turn := c.activeTurnRef(); turn != nil && turn.callbacks.OnWarning != nil {
					_ = turn.callbacks.OnWarning(fmt.Sprintf("failed to send permission response: %v", err))
				}
			}

			if turn := c.activeTurnRef(); turn != nil {
				title := params.ToolCall.Title
				if title == "" {
					title = params.ToolCall.ToolCallID
				}
				if chosenOptionID != "" {
					if turn.callbacks.OnWarning != nil {
						_ = turn.callbacks.OnWarning(fmt.Sprintf("permission auto-approved: %s", title))
					}
				} else {
					callID := params.ToolCall.ToolCallID
					if callID == "" {
						callID = newToolCallID()
					}
					toolName := extractToolName(params.ToolCall.ToolName, params.ToolCall.Name, params.ToolCall.Title, params.ToolCall.Kind)
					argsSummary := buildArgsSummary(params.ToolCall.inputPayload(), params.ToolCall.Title)
					if turn.callbacks.OnToolCall != nil {
						_ = turn.callbacks.OnToolCall(&agentv1.ToolCall{
							CallId:      callID,
							ToolName:    toolName,
							ArgsSummary: argsSummary,
							Denied:      true,
						})
					}
					if turn.callbacks.OnToolCallUpdate != nil {
						_ = turn.callbacks.OnToolCallUpdate(&agentv1.ToolCallUpdate{
							CallId:   callID,
							ToolName: toolName,
							Status:   ToolStatusDenied,
						})
					}
					if turn.callbacks.OnWarning != nil {
						_ = turn.callbacks.OnWarning(fmt.Sprintf("permission auto-approve failed (no allow option matched): %s", title))
					}
				}
			}
			break
		}

		// Auto-deny posture (D117 Decision)
		chosenOptionID := selectDenyOption(params.Options)
		var outcome map[string]any
		if chosenOptionID != "" {
			outcome = map[string]any{
				"outcome":  OutcomeSelected,
				"optionId": chosenOptionID,
			}
		} else {
			outcome = map[string]any{
				"outcome": OutcomeCancelled,
			}
		}

		// Send deny response back over ACP to unblock harness
		if err := c.sendResponse(req.ID, map[string]any{"outcome": outcome}, nil); err != nil {
			if turn := c.activeTurnRef(); turn != nil && turn.callbacks.OnWarning != nil {
				_ = turn.callbacks.OnWarning(fmt.Sprintf("failed to send permission response: %v", err))
			}
		}

		callID := params.ToolCall.ToolCallID
		if callID == "" {
			callID = newToolCallID()
		}
		toolName := extractToolName(params.ToolCall.ToolName, params.ToolCall.Name, params.ToolCall.Title, params.ToolCall.Kind)
		argsSummary := buildArgsSummary(params.ToolCall.inputPayload(), params.ToolCall.Title)

		// Surface tool call and update over D112 with denied status
		turn := c.activeTurnRef()
		if turn != nil {
			if turn.callbacks.OnToolCall != nil {
				_ = turn.callbacks.OnToolCall(&agentv1.ToolCall{
					CallId:      callID,
					ToolName:    toolName,
					ArgsSummary: argsSummary,
					Denied:      true,
				})
			}
			if turn.callbacks.OnToolCallUpdate != nil {
				_ = turn.callbacks.OnToolCallUpdate(&agentv1.ToolCallUpdate{
					CallId:   callID,
					ToolName: toolName,
					Status:   ToolStatusDenied,
				})
			}
		}

		// Surface warning over D112
		if turn != nil && turn.callbacks.OnWarning != nil {
			title := params.ToolCall.Title
			if title == "" {
				title = params.ToolCall.ToolCallID
			}
			warning := fmt.Sprintf("permission request auto-denied: %s", title)
			_ = turn.callbacks.OnWarning(warning)
		}

	default:
		// Unsupported incoming request
		if err := c.sendResponse(req.ID, nil, &RPCError{
			Code:    CodeMethodNotFound,
			Message: fmt.Sprintf("method %q not supported", req.Method),
		}); err != nil {
			if turn := c.activeTurnRef(); turn != nil && turn.callbacks.OnWarning != nil {
				_ = turn.callbacks.OnWarning(fmt.Sprintf("failed to send error response for unsupported method %q: %v", req.Method, err))
			}
		}
	}
}

func (c *Client) respondPermissionError(reqID any, code int, msg string) {
	if err := c.sendResponse(reqID, nil, &RPCError{
		Code:    code,
		Message: msg,
	}); err != nil {
		if turn := c.activeTurnRef(); turn != nil && turn.callbacks.OnWarning != nil {
			_ = turn.callbacks.OnWarning(fmt.Sprintf("failed to send permission error response: %v", err))
		}
	}
	if turn := c.activeTurnRef(); turn != nil && turn.callbacks.OnWarning != nil {
		_ = turn.callbacks.OnWarning(fmt.Sprintf("permission request error: %s", msg))
	}
}

// selectAllowOption chooses an approval option for trusted local harnesses.
// It prioritizes standard ACP kind values (allow_once, allow_always).
// If kind is missing, it falls back to token/boundary matching, never matching
// strings that indicate rejection (e.g. "disallow", "reject", "deny").
func selectAllowOption(options []PermissionOption) string {
	// 1. Kind-first selection
	for _, opt := range options {
		if opt.Kind == OptionKindAllowOnce || opt.Kind == OptionKindAllowAlways {
			return opt.OptionID
		}
	}

	// 2. Fallback: match on exact name/id or token boundary
	for _, opt := range options {
		if opt.Kind == OptionKindRejectOnce || opt.Kind == OptionKindRejectAlways {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(opt.Name))
		optID := strings.ToLower(strings.TrimSpace(opt.OptionID))

		if isRejectIdentifier(name) || isRejectIdentifier(optID) {
			continue
		}

		if isAllowTokenMatch(name) || isAllowTokenMatch(optID) {
			return opt.OptionID
		}
	}
	return ""
}

// selectDenyOption chooses a rejection option.
// It prioritizes standard ACP kind values (reject_once, reject_always).
// If kind is missing, it falls back to token/boundary matching.
func selectDenyOption(options []PermissionOption) string {
	// 1. Kind-first selection
	for _, opt := range options {
		if opt.Kind == OptionKindRejectOnce || opt.Kind == OptionKindRejectAlways {
			return opt.OptionID
		}
	}

	// 2. Fallback: match on exact name/id or token boundary
	for _, opt := range options {
		if opt.Kind == OptionKindAllowOnce || opt.Kind == OptionKindAllowAlways {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(opt.Name))
		optID := strings.ToLower(strings.TrimSpace(opt.OptionID))

		if isAllowTokenMatch(name) || isAllowTokenMatch(optID) {
			continue
		}

		if isRejectIdentifier(name) || isRejectIdentifier(optID) {
			return opt.OptionID
		}
	}
	return ""
}

func isRejectIdentifier(s string) bool {
	if strings.HasPrefix(s, "dis") || strings.HasPrefix(s, "reject") || strings.HasPrefix(s, "deny") {
		return true
	}
	words := strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == '_' || r == '-' || r == '/'
	})
	for _, w := range words {
		if w == "reject" || w == "deny" || w == "disallow" {
			return true
		}
	}
	return false
}

func isAllowTokenMatch(s string) bool {
	if s == "allow" || s == "allow_once" || s == "allow_always" || s == "allow once" || s == "allow always" {
		return true
	}
	words := strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == '_' || r == '-' || r == '/'
	})
	for _, w := range words {
		if w == "allow" {
			return true
		}
	}
	return false
}

func (c *Client) handleIncomingNotification(line []byte) {
	var notif rpcRequest
	if err := json.Unmarshal(line, &notif); err != nil {
		return
	}

	if notif.Method != MethodSessionUpdate {
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

	if update.SessionUpdate != UpdateKindAgentMessageChunk {
		_ = turn.flush()
	}

	switch update.SessionUpdate {
	case UpdateKindAgentMessageChunk:
		var content struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(update.Content, &content); err == nil && content.Text != "" {
			turn.appendText(content.Text)
		}

	case UpdateKindUsageUpdate:
		turn.mu.Lock()
		defer turn.mu.Unlock()
		if update.Used > 0 {
			turn.usage.TotalTokens = update.Used
		}

	case UpdateKindToolCall:
		var raw rawToolUpdate
		if err := json.Unmarshal(params.Update, &raw); err == nil {
			if turn.callbacks.OnToolCall != nil {
				call := mapToolCall(&raw)
				_ = turn.callbacks.OnToolCall(call)
			}
		}

	case UpdateKindToolCallUpdate:
		var raw rawToolUpdate
		if err := json.Unmarshal(params.Update, &raw); err == nil {
			if turn.callbacks.OnToolCallUpdate != nil {
				tcUpdate := mapToolCallUpdate(&raw)
				_ = turn.callbacks.OnToolCallUpdate(tcUpdate)
			}
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

	resp, err := c.sendRequest(ctx, MethodInitialize, params)
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
	resp, err := c.sendRequest(ctx, MethodSessionResume, params)
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
	resp, err := c.sendRequest(ctx, MethodSessionLoad, params)
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
	resp, err := c.sendRequest(ctx, MethodSessionNew, params)
	if err != nil {
		return "", fmt.Errorf("%s failed: %w", MethodSessionNew, err)
	}

	var result NewSessionResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return "", fmt.Errorf("unmarshaling new session result: %w", err)
	}
	if result.SessionID == "" {
		return "", fmt.Errorf("empty sessionId returned from %s", MethodSessionNew)
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

	resp, err := c.sendRequest(ctx, MethodSessionPrompt, params)
	if err != nil {
		return nil, err
	}

	var promptResp promptResponse
	if err := json.Unmarshal(resp.Result, &promptResp); err != nil {
		sample := string(resp.Result)
		if len(sample) > 120 {
			sample = sample[:120] + "..."
		}
		return nil, fmt.Errorf("decoding %s response (got %s): %w", MethodSessionPrompt, sample, err)
	}

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

	// Explicit flush before returning so chunk delivery errors propagate;
	// deferred closeAndFlush is an idempotent safety net.
	if err := turn.closeAndFlush(); err != nil {
		return nil, fmt.Errorf("flushing message buffer: %w", err)
	}

	return &PromptResult{
		StopReason: promptResp.StopReason,
		Usage:      usage,
	}, nil
}

// Cancel sends a session/cancel notification to abort an in-flight prompt turn.
func (c *Client) Cancel(sessionID string) error {
	return c.sendNotification(MethodSessionCancel, map[string]any{
		"sessionId": sessionID,
	})
}
