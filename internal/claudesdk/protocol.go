package claudesdk

// The Go side and the Node worker exchange newline-delimited JSON frames.
// Frames are purpose-built rather than ACP-shaped: ACP is what v1 replaces, and
// reusing its envelope would carry dead fields forever.

// Operations the worker accepts on the Go to Node channel.
const (
	opPrompt = "prompt"
	opCancel = "cancel"
)

// Frame types on the Node to Go channel.
const (
	frameReady       = "ready"
	frameEvent       = "event"
	frameResult      = "result"
	frameError       = "error"
	frameToolRequest = "tool_request"
)

// Event kinds inside frameEvent.
const (
	kindText       = "text"
	kindThinking   = "thinking"
	kindToolCall   = "tool_call"
	kindToolUpdate = "tool_update"
	kindWarning    = "warning"
)

// Error codes the worker reports on frameError. resume_failed is the one the
// driver handles by retrying once without resume, because a deleted or
// corrupted transcript is recoverable by starting over.
const (
	codeResumeFailed = "resume_failed"
	codeWorkerFailed = "worker_failed"
)

// systemPromptSpec is how the composed prompt is handed to the worker.
// Mode "append" keeps Claude Code's own preset and is the default. Mode
// "replace" asks for a custom prompt and is NOT viable against the pinned CLI:
// the bundled binary bakes SystemPromptPreset into the outgoing request, so the
// CLI rejects a custom prompt instead of half-applying it. That finding is
// claude's, verified against cli.js and a live probe; I did not re-run it.
// Keeping the mode separate from the content is what lets a future CLI that
// honours custom prompts turn this on without a bridge contract change.
type systemPromptSpec struct {
	Mode    string `json:"mode"`
	Content string `json:"content"`
	// Snapshot lets the SDK reuse its cached system prompt across turns. False
	// forces a fresh render, which is what an AGENTS.md or skill change needs.
	Snapshot bool `json:"snapshot"`
}

// workerRequest is one Go to Node frame.
type workerRequest struct {
	ID                    int64             `json:"id"`
	Op                    string            `json:"op"`
	SessionID             string            `json:"sessionId,omitempty"`
	Resume                bool              `json:"resume,omitempty"`
	Prompt                string            `json:"prompt,omitempty"`
	SystemPrompt          *systemPromptSpec `json:"systemPrompt,omitempty"`
	Skills                []string          `json:"skills,omitempty"`
	Cwd                   string            `json:"cwd,omitempty"`
	AdditionalDirectories []string          `json:"additionalDirectories,omitempty"`
	SettingSources        []string          `json:"settingSources,omitempty"`
	PermissionMode        string            `json:"permissionMode,omitempty"`
}

// usageFrame mirrors the SDK usage counters onto wackyacp's UsageMetrics shape.
type usageFrame struct {
	InputTokens  int64 `json:"inputTokens"`
	OutputTokens int64 `json:"outputTokens"`
	TotalTokens  int64 `json:"totalTokens"`
}

// workerEvent is one Node to Go frame. Which fields are meaningful depends on
// Type, and for Type == frameToolRequest on Name and Args.
type workerEvent struct {
	Type string `json:"type"`
	// ID correlates an event, result or error to the workerRequest that caused it.
	ID int64 `json:"id,omitempty"`

	Kind        string `json:"kind,omitempty"`
	Text        string `json:"text,omitempty"`
	CallID      string `json:"callId,omitempty"`
	ToolName    string `json:"toolName,omitempty"`
	ArgsSummary string `json:"argsSummary,omitempty"`
	Status      string `json:"status,omitempty"`
	Denied      bool   `json:"denied,omitempty"`

	SessionID  string      `json:"sessionId,omitempty"`
	StopReason string      `json:"stopReason,omitempty"`
	Usage      *usageFrame `json:"usage,omitempty"`

	Message string `json:"message,omitempty"`
	Code    string `json:"code,omitempty"`

	Name      string         `json:"name,omitempty"`
	Args      map[string]any `json:"args,omitempty"`
	RequestID int64          `json:"requestId,omitempty"`
}

// workerToolResponse answers a frameToolRequest. Scratchpad storage stays in
// Go, so the worker only ever sees the rendered result text.
type workerToolResponse struct {
	Type      string `json:"type"`
	RequestID int64  `json:"requestId"`
	OK        bool   `json:"ok"`
	Text      string `json:"text,omitempty"`
	Error     string `json:"error,omitempty"`
}
