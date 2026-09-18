package agy

import "encoding/json"

// ACP session/update discriminators emitted by this bridge.
const (
	updateMessageChunk = "agent_message_chunk"
	updateToolCall     = "tool_call"
	updateToolDone     = "tool_call_update"
)

// Content is an ACP content block. Only text is produced by agy.
type Content struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// Update is the update object of a session/update notification.
type Update struct {
	SessionUpdate string          `json:"sessionUpdate"`
	Content       *Content        `json:"content,omitempty"`
	ToolCallID    string          `json:"toolCallId,omitempty"`
	Title         string          `json:"title,omitempty"`
	RawInput      json.RawMessage `json:"rawInput,omitempty"`
	Status        string          `json:"status,omitempty"`
}
