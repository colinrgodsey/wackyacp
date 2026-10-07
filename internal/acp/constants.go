package acp

// ACP JSON-RPC 2.0 protocol methods.
const (
	MethodInitialize               = "initialize"
	MethodSessionNew               = "session/new"
	MethodSessionResume            = "session/resume"
	MethodSessionLoad              = "session/load"
	MethodSessionPrompt            = "session/prompt"
	MethodSessionCancel            = "session/cancel"
	MethodSessionUpdate            = "session/update"
	MethodSessionRequestPermission = "session/request_permission"
	// MethodSessionSetConfigOption uses the spec name (snake_case, ACP v1 schema).
	// Harnesses in the wild accept both this and the legacy camelCase
	// "session/setConfigOption" (the acpshimbin fixture models that); we send the
	// spec name, and the bridge plus fixture parse both.
	MethodSessionSetConfigOption = "session/set_config_option"
)

// ACP session/update discriminators.
const (
	UpdateKindAgentMessageChunk = "agent_message_chunk"
	UpdateKindUsageUpdate       = "usage_update"
	UpdateKindToolCall          = "tool_call"
	UpdateKindToolCallUpdate    = "tool_call_update"
)

// ACP tool execution statuses (mirroring ACP spec).
const (
	ToolStatusPending    = "pending"
	ToolStatusInProgress = "in_progress"
	ToolStatusCompleted  = "completed"
	ToolStatusFailed     = "failed"
	ToolStatusDenied     = "denied"
)

// ACP permission option kinds.
const (
	OptionKindAllowOnce    = "allow_once"
	OptionKindAllowAlways  = "allow_always"
	OptionKindRejectOnce   = "reject_once"
	OptionKindRejectAlways = "reject_always"
)

// ACP permission request outcome discriminators.
const (
	OutcomeSelected  = "selected"
	OutcomeCancelled = "cancelled"
)

// JSON-RPC 2.0 standard error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// ACP methods and update kinds used by serve mode (the ACP server face).
const (
	MethodSessionClose          = "session/close"
	UpdateKindUserMessageChunk  = "user_message_chunk"
	UpdateKindAgentThoughtChunk = "agent_thought_chunk"
)
