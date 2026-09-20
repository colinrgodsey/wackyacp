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
	MethodSessionSetConfigOption   = "session/setConfigOption"
)

// ACP session/update discriminators.
const (
	UpdateKindAgentMessageChunk = "agent_message_chunk"
	UpdateKindUsageUpdate       = "usage_update"
	UpdateKindToolCall          = "tool_call"
	UpdateKindToolCallUpdate    = "tool_call_update"
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
