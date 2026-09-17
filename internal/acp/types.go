package acp

import (
	"encoding/json"
)

// JSON-RPC 2.0 wire types
type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id,omitempty"`
	Method  string `json:"method,omitempty"`
	Params  any    `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// RPCError represents a standard JSON-RPC 2.0 error object.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	return e.Message
}

// AgentCapabilities mirrors ACP initialization response capabilities.
type AgentCapabilities struct {
	LoadSession         bool                `json:"loadSession"`
	SessionCapabilities SessionCapabilities `json:"sessionCapabilities"`
}

// SessionCapabilities defines advertised session methods.
type SessionCapabilities struct {
	Resume bool `json:"resume"`
	List   any  `json:"list,omitempty"`
	Fork   any  `json:"fork,omitempty"`
	Close  any  `json:"close,omitempty"`
}

// InitializeResult represents the result of the ACP initialize handshake.
type InitializeResult struct {
	ProtocolVersion   int               `json:"protocolVersion"`
	AgentCapabilities AgentCapabilities `json:"agentCapabilities"`
	AgentInfo         map[string]any    `json:"agentInfo,omitempty"`
}

// NewSessionResult represents the result of a session/new request.
type NewSessionResult struct {
	SessionID string         `json:"sessionId"`
	Meta      map[string]any `json:"_meta,omitempty"`
}

// ResumeSessionResult represents the result of a session/resume request.
type ResumeSessionResult struct {
	Modes map[string]any `json:"modes,omitempty"`
	Meta  map[string]any `json:"_meta,omitempty"`
}

// LoadSessionResult represents the result of a session/load request.
type LoadSessionResult struct {
	Modes map[string]any `json:"modes,omitempty"`
	Meta  map[string]any `json:"_meta,omitempty"`
}

// PromptResult represents the completion of a prompt turn.
type PromptResult struct {
	StopReason string
	Usage      UsageMetrics
}

// UsageMetrics holds accumulated token usage.
type UsageMetrics struct {
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	Backend          string
}

// PermissionRequestParams represents an incoming session/request_permission call from harness.
type PermissionRequestParams struct {
	SessionID string             `json:"sessionId"`
	ToolCall  ToolCallInfo       `json:"toolCall"`
	Options   []PermissionOption `json:"options"`
}

type ToolCallInfo struct {
	ToolCallID string `json:"toolCallId"`
	Title      string `json:"title,omitempty"`
	Kind       string `json:"kind,omitempty"`
}

type PermissionOption struct {
	OptionID string `json:"optionId"`
	Name     string `json:"name"`
	Kind     string `json:"kind"`
}
