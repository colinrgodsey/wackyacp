package serve

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/colinrgodsey/wackyacp/internal/acp"
)

// Version is reported in agentInfo. Set at build time with -ldflags
// "-X github.com/colinrgodsey/wackyacp/internal/serve.Version=...".
var Version = "0.1.0"

// Update is one ACP session/update payload: the object sent under the
// "update" key of a session/update notification. Map-based on purpose - the
// payload shapes are small, stable, and the ACP spec reserves _meta for
// forward-compatible fields.
type Update = map[string]any

// JSON-RPC 2.0 wire types (mirroring internal/acp, which keeps its own
// copies for the client face; the wire format is identical).
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// RPCError is a JSON-RPC 2.0 error object.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("[%d] %s", e.Code, e.Message) }

// Server is one ACP connection: a newline-delimited JSON-RPC 2.0 session over
// (in, out). It holds no conversation state - the ACP session IS the
// wackypub agent session and all history lives in the agent folder.
// Multiple connections (the TCP listener mode) share one Backend; the
// Backend's turn gate serializes prompts across all of them.
type Server struct {
	in        io.Reader
	out       io.Writer
	outMu     sync.Mutex
	backend   *Backend
	sessionID string
}

// NewServer wires an ACP server over one connection pair. agentFolder is
// used for cwd validation and the stable session id.
func NewServer(in io.Reader, out io.Writer, backend *Backend, agentFolder string) *Server {
	return &Server{
		in:        in,
		out:       out,
		backend:   backend,
		sessionID: SessionID(agentFolder),
	}
}

// ChunkUpdate builds a text content-chunk session update. kind is one of the
// ACP chunk discriminators (agent_message_chunk, agent_thought_chunk,
// user_message_chunk).
func ChunkUpdate(kind, text string) Update {
	return Update{
		"sessionUpdate": kind,
		"content":       map[string]any{"type": "text", "text": text},
	}
}

// ToolCallUpdatePayload builds a tool_call / tool_call_update session update.
// kind is acp.UpdateKindToolCall for the announce and
// acp.UpdateKindToolCallUpdate for the outcome. resultText, when non-nil and
// non-empty, rides as a single text content block (the wackypub result head).
func ToolCallUpdatePayload(kind, toolCallID, name, status string, rawInput any, resultText *string) Update {
	u := Update{
		"sessionUpdate": kind,
		"toolCallId":    toolCallID,
		"title":         name,
		"name":          name,
		"status":        status,
	}
	if rawInput != nil {
		u["rawInput"] = rawInput
	}
	if resultText != nil && *resultText != "" {
		u["content"] = []any{
			map[string]any{"type": "content", "content": map[string]any{"type": "text", "text": *resultText}},
		}
	}
	return u
}

// Serve runs the read loop until the connection closes (EOF or read error).
// Requests are handled in their own goroutines so an in-flight prompt never
// blocks dispatch of session/cancel or another connection's traffic; all
// writes are serialized on one mutex.
func (s *Server) Serve(ctx context.Context) error {
	// A connection that dies mid-turn must not leave an orphaned turn behind:
	// the turn is owned by the connection that started it, and the agent
	// keeps running even though its frontend is gone. Stop only turns this
	// connection owns - a cancel from another connection is legitimate.
	defer func() {
		if ts := s.backend.ActiveTurn(); ts != nil && ts.owner == s {
			s.backend.CancelInFlight(ctx)
		}
	}()

	scanner := bufio.NewScanner(s.in)
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 10*1024*1024) // 10MB max line, matching the client face

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(line, &raw); err != nil {
			s.sendError(nil, acp.CodeParseError, "parse error")
			continue
		}
		id, hasID := raw["id"]
		method, hasMethod := raw["method"]
		if !hasMethod {
			// A response or stray frame: this server never issues requests in
			// v1, so there is nothing to correlate. Ignore.
			continue
		}
		// The RawMessage is the JSON-encoded value, quotes included; decode it.
		var methodStr string
		if err := json.Unmarshal(method, &methodStr); err != nil {
			// A method that is not a string is an invalid request.
			s.sendError(raw["id"], acp.CodeInvalidRequest, "invalid request")
			continue
		}
		lineCopy := append([]byte(nil), line...)
		if hasID {
			go s.handleRequest(ctx, methodStr, append(json.RawMessage(nil), id...), lineCopy)
		} else {
			go s.handleNotification(ctx, methodStr, lineCopy)
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func (s *Server) handleRequest(ctx context.Context, method string, id json.RawMessage, line []byte) {
	switch method {
	case acp.MethodInitialize:
		s.sendResponse(id, s.initializeResult(), nil)
	case acp.MethodSessionNew:
		s.handleSessionNew(ctx, id, line)
	case acp.MethodSessionLoad:
		s.handleSessionLoad(ctx, id, line)
	case acp.MethodSessionPrompt:
		s.handlePrompt(ctx, id, line)
	case acp.MethodSessionClose:
		if ts := s.backend.ActiveTurn(); ts != nil && ts.owner == s {
			s.backend.CancelInFlight(ctx)
		}
		s.sendResponse(id, map[string]any{}, nil)
	default:
		// v1 exclusions are enforced here: not advertised in agentCapabilities,
		// and answered with method-not-found rather than half-implemented.
		s.sendError(id, acp.CodeMethodNotFound, fmt.Sprintf("method not found: %s", method))
	}
}

func (s *Server) handleNotification(ctx context.Context, method string, line []byte) {
	if method == acp.MethodSessionCancel {
		// Cancel is a notification (no response by spec). It targets the agent
		// session, which is process-wide: any connection may cancel it.
		s.backend.CancelInFlight(ctx)
		return
	}
	// Unknown notifications are dropped, per JSON-RPC.
}

// initializeResult advertises exactly what v1 implements: the prompt
// baseline (text plus resource_link, per the ACP baseline obligation),
// loadSession (attach to the real session), and session close. Everything
// else - auth, modes, fork/list/delete, providers, nes, document - is absent
// on purpose.
func (s *Server) initializeResult() any {
	return map[string]any{
		"protocolVersion": 1,
		"agentCapabilities": map[string]any{
			"loadSession": true,
			"promptCapabilities": map[string]any{
				"image":           false,
				"audio":           false,
				"embeddedContext": false,
			},
			"sessionCapabilities": map[string]any{
				"close": true,
			},
		},
		"agentInfo": map[string]any{
			"name":    "wackyacp",
			"title":   "wackyacp serve",
			"version": Version,
		},
	}
}

func (s *Server) handleSessionNew(ctx context.Context, id json.RawMessage, line []byte) {
	var envelope struct {
		Params struct {
			Cwd        string `json:"cwd"`
			McpServers any    `json:"mcpServers"`
		} `json:"params"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		s.sendError(id, acp.CodeInvalidParams, "invalid params")
		return
	}
	if err := s.validateCwd(envelope.Params.Cwd); err != nil {
		s.sendError(id, acp.CodeInvalidParams, err.Error())
		return
	}
	if envelope.Params.Cwd != "" && envelope.Params.Cwd != s.backend.AgentFolder() {
		s.backend.logf("wackyacp serve: session/new cwd %s differs from agent folder %s; the agent operates in its own folder",
			envelope.Params.Cwd, s.backend.AgentFolder())
	}
	if envelope.Params.McpServers != nil {
		if list, ok := envelope.Params.McpServers.([]any); ok && len(list) > 0 {
			s.backend.logf("wackyacp serve: ignoring %d mcpServers in session/new (v1 has no MCP support)", len(list))
		}
	}
	s.sendResponse(id, map[string]any{"sessionId": s.sessionID}, nil)
}

// validateCwd mirrors the reference ACP server's up-front cwd check: the ACP
// spec requires an absolute path, and a missing directory is a
// misconfiguration the client can surface immediately instead of a failure
// deep in a prompt.
func (s *Server) validateCwd(cwd string) error {
	if cwd == "" {
		return nil
	}
	if !filepath.IsAbs(cwd) {
		return fmt.Errorf("cwd must be an absolute path, but received: %s", cwd)
	}
	st, err := os.Stat(cwd)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("cwd does not exist on the machine running the agent: %s", cwd)
		}
		return err
	}
	if !st.IsDir() {
		return fmt.Errorf("cwd is not a directory: %s", cwd)
	}
	return nil
}

func (s *Server) handleSessionLoad(ctx context.Context, id json.RawMessage, line []byte) {
	var envelope struct {
		Params struct {
			SessionID string `json:"sessionId"`
		} `json:"params"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		s.sendError(id, acp.CodeInvalidParams, "invalid params")
		return
	}
	if envelope.Params.SessionID != s.sessionID {
		s.sendError(id, acp.CodeInvalidParams, fmt.Sprintf("unknown session: %s", envelope.Params.SessionID))
		return
	}
	// Replay the real session history as session/update notifications, then
	// answer. History comes from ReadSession's content_json (1:1 genai.Content
	// fidelity: thought flags and function calls included).
	if err := s.backend.Replay(ctx, s.emitUpdate); err != nil {
		s.sendError(id, acp.CodeInternalError, fmt.Sprintf("loading session: %v", err))
		return
	}
	s.sendResponse(id, map[string]any{}, nil)
}

func (s *Server) handlePrompt(ctx context.Context, id json.RawMessage, line []byte) {
	var envelope struct {
		Params struct {
			SessionID string            `json:"sessionId"`
			Prompt    []json.RawMessage `json:"prompt"`
		} `json:"params"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		s.sendError(id, acp.CodeInvalidParams, "invalid params")
		return
	}
	if envelope.Params.SessionID != s.sessionID {
		s.sendError(id, acp.CodeInvalidParams, fmt.Sprintf("unknown session: %s", envelope.Params.SessionID))
		return
	}
	promptText, err := flattenPrompt(envelope.Params.Prompt)
	if err != nil {
		s.sendError(id, acp.CodeInvalidParams, err.Error())
		return
	}
	ts, ok := s.backend.BeginTurn(s)
	if !ok {
		// One conversation, one in-flight turn: the second prompter gets a
		// structured busy error (the D112 precedent), not a corrupted stream.
		s.sendError(id, -32000, "another prompt turn is already in flight for this agent; retry after it completes")
		return
	}
	stopReason, usage, err := s.backend.Prompt(ctx, ts, promptText, s.emitUpdate)
	// EndTurn before the response: a frontend that immediately follows up
	// with the next prompt must not race the turn gate.
	s.backend.EndTurn(ts)
	if err != nil {
		if ctx.Err() != nil {
			s.sendResponse(id, map[string]any{"stopReason": "cancelled"}, nil)
			return
		}
		s.sendError(id, acp.CodeInternalError, fmt.Sprintf("turn failed: %v", err))
		return
	}
	result := map[string]any{"stopReason": stopReason}
	if usage != nil {
		result["usage"] = usage
	}
	s.sendResponse(id, result, nil)
}

// emitUpdate sends one session/update notification for this connection.
func (s *Server) emitUpdate(u Update) error {
	return s.sendNotification(acp.MethodSessionUpdate, map[string]any{
		"sessionId": s.sessionID,
		"update":    u,
	})
}

func (s *Server) sendNotification(method string, params any) error {
	data, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return err
	}
	s.outMu.Lock()
	defer s.outMu.Unlock()
	_, err = fmt.Fprintf(s.out, "%s\n", data)
	return err
}

func (s *Server) sendResponse(id json.RawMessage, result any, rpcErr *RPCError) error {
	resp := map[string]any{"jsonrpc": "2.0", "id": id}
	if rpcErr != nil {
		resp["error"] = rpcErr
	} else {
		raw, err := json.Marshal(result)
		if err != nil {
			return err
		}
		// RawMessage keeps the pre-marshaled bytes intact; a bare []byte
		// would be re-encoded as base64.
		resp["result"] = json.RawMessage(raw)
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	s.outMu.Lock()
	defer s.outMu.Unlock()
	_, err = fmt.Fprintf(s.out, "%s\n", data)
	return err
}

func (s *Server) sendError(id json.RawMessage, code int, msg string) error {
	return s.sendResponse(id, nil, &RPCError{Code: code, Message: msg})
}

// flattenPrompt reduces an ACP prompt's ContentBlock array to the wackypub
// user message text. Text blocks pass through; resource links (the ACP
// baseline alongside text) become readable mentions the agent can act on
// with its own tools; images/audio and embedded resources degrade to labels
// rather than errors, so an unadvertised capability costs a prompt nothing.
func flattenPrompt(blocks []json.RawMessage) (string, error) {
	var parts []string
	for _, raw := range blocks {
		var b struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			URI      string `json:"uri"`
			Name     string `json:"name"`
			MimeType string `json:"mimeType"`
			Resource *struct {
				URI  string `json:"uri"`
				Text string `json:"text"`
			} `json:"resource"`
		}
		if err := json.Unmarshal(raw, &b); err != nil {
			continue
		}
		switch b.Type {
		case "text":
			parts = append(parts, b.Text)
		case "resource_link":
			label := b.Name
			if label == "" {
				label = b.URI
			}
			parts = append(parts, fmt.Sprintf("[resource: %s (%s)]", label, b.URI))
		case "resource":
			if b.Resource != nil {
				parts = append(parts, fmt.Sprintf("[embedded resource: %s]", b.Resource.URI))
			}
		case "image":
			parts = append(parts, fmt.Sprintf("[image: %s]", b.MimeType))
		case "audio":
			parts = append(parts, fmt.Sprintf("[audio: %s]", b.MimeType))
		}
	}
	text := strings.Join(parts, "\n")
	if strings.TrimSpace(text) == "" {
		return "", errors.New("prompt has no supported content")
	}
	return text, nil
}
