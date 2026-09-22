package acp

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

// rawToolUpdate captures fields from ACP session/update tool notifications
// (both tool_call and tool_call_update variants from claude-acp and wackyagy).
type rawToolUpdate struct {
	SessionUpdate string          `json:"sessionUpdate"`
	ToolCallID    string          `json:"toolCallId"`
	CallID        string          `json:"callId"`
	ToolName      string          `json:"toolName"`
	Name          string          `json:"name"`
	Title         string          `json:"title"`
	Kind          string          `json:"kind"`
	RawInput      json.RawMessage `json:"rawInput"`
	Args          json.RawMessage `json:"args"`
	Input         json.RawMessage `json:"input"`
	Arguments     json.RawMessage `json:"arguments"`
	Status        string          `json:"status"`
	RawOutput     json.RawMessage `json:"rawOutput"`
	Output        json.RawMessage `json:"output"`
	Result        json.RawMessage `json:"result"`
	ResultHead    string          `json:"resultHead"`
	ResultBytes   int64           `json:"resultBytes"`
	ResultRef     string          `json:"resultRef"`

	// Nested fallback for servers that wrap tool call payload under toolCall or tool
	ToolCall *rawToolUpdate `json:"toolCall,omitempty"`
	Tool     *rawToolUpdate `json:"tool,omitempty"`
}

func (u *rawToolUpdate) id() string {
	if u.ToolCallID != "" {
		return u.ToolCallID
	}
	if u.CallID != "" {
		return u.CallID
	}
	if u.ToolCall != nil {
		if id := u.ToolCall.id(); id != "" {
			return id
		}
	}
	if u.Tool != nil {
		if id := u.Tool.id(); id != "" {
			return id
		}
	}
	return ""
}

func (u *rawToolUpdate) toolName() string {
	name := extractToolName(u.ToolName, u.Name, u.Title, u.Kind)
	if name == "unknown" {
		if u.ToolCall != nil {
			if n := u.ToolCall.toolName(); n != "unknown" {
				return n
			}
		}
		if u.Tool != nil {
			if n := u.Tool.toolName(); n != "unknown" {
				return n
			}
		}
	}
	return name
}

func (u *rawToolUpdate) inputPayload() json.RawMessage {
	if len(u.RawInput) > 0 {
		return u.RawInput
	}
	if len(u.Input) > 0 {
		return u.Input
	}
	if len(u.Args) > 0 {
		return u.Args
	}
	if len(u.Arguments) > 0 {
		return u.Arguments
	}
	if u.ToolCall != nil {
		if p := u.ToolCall.inputPayload(); len(p) > 0 {
			return p
		}
	}
	if u.Tool != nil {
		if p := u.Tool.inputPayload(); len(p) > 0 {
			return p
		}
	}
	return nil
}

func (u *rawToolUpdate) outputPayload() json.RawMessage {
	if len(u.RawOutput) > 0 {
		return u.RawOutput
	}
	if len(u.Output) > 0 {
		return u.Output
	}
	if len(u.Result) > 0 {
		return u.Result
	}
	if u.ToolCall != nil {
		if p := u.ToolCall.outputPayload(); len(p) > 0 {
			return p
		}
	}
	if u.Tool != nil {
		if p := u.Tool.outputPayload(); len(p) > 0 {
			return p
		}
	}
	return nil
}

// truncateRunes cuts s to at most max runes without splitting a rune.
func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max])
}

// extractToolName extracts or infers a clean tool name from available metadata.
func extractToolName(toolName, name, title, kind string) string {
	if toolName != "" {
		return toolName
	}
	if name != "" {
		return name
	}
	if title != "" {
		if strings.Contains(title, ":") {
			parts := strings.SplitN(title, ":", 2)
			t := strings.TrimSpace(parts[0])
			if t != "" {
				return t
			}
		}
		return title
	}
	if kind != "" {
		return kind
	}
	return "unknown"
}

// isSecretKey reports whether a map key names a secret-shaped value.
func isSecretKey(key string) bool {
	lower := strings.ToLower(key)
	for _, marker := range []string{"api_key", "apikey", "api-key", "token", "password", "passwd", "secret", "bearer", "authorization", "auth"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// redactSecretShapedValues replaces secret-shaped map values with [REDACTED].
func redactSecretShapedValues(args map[string]any) map[string]any {
	out := make(map[string]any, len(args))
	for k, v := range args {
		if isSecretKey(k) {
			out[k] = "[REDACTED]"
			continue
		}
		if sub, ok := v.(map[string]any); ok {
			out[k] = redactSecretShapedValues(sub)
			continue
		}
		out[k] = v
	}
	return out
}

const maxArgsSummaryLen = 200

// buildArgsSummary produces a redacted scalar preview of tool arguments truncated to 200 chars.
func buildArgsSummary(rawInput json.RawMessage, fallbackTitle string) string {
	if len(rawInput) > 0 {
		var m map[string]any
		if err := json.Unmarshal(rawInput, &m); err == nil {
			red := redactSecretShapedValues(m)
			keys := make([]string, 0, len(red))
			for k := range red {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var sb strings.Builder
			for i, k := range keys {
				if i > 0 {
					sb.WriteString(", ")
				}
				v := red[k]
				switch val := v.(type) {
				case string:
					fmt.Fprintf(&sb, "%s=%s", k, val)
				case float64:
					fmt.Fprintf(&sb, "%s=%v", k, val)
				case bool:
					fmt.Fprintf(&sb, "%s=%v", k, val)
				case json.Number:
					fmt.Fprintf(&sb, "%s=%s", k, val.String())
				default:
					if b, err := json.Marshal(val); err == nil {
						fmt.Fprintf(&sb, "%s=%s", k, string(b))
					} else {
						fmt.Fprintf(&sb, "%s=<%T>", k, val)
					}
				}
			}
			return truncateRunes(sb.String(), maxArgsSummaryLen)
		}

		var s string
		if err := json.Unmarshal(rawInput, &s); err == nil {
			return truncateRunes(s, maxArgsSummaryLen)
		}
		return truncateRunes(string(rawInput), maxArgsSummaryLen)
	}

	if fallbackTitle != "" {
		if strings.Contains(fallbackTitle, ":") {
			parts := strings.SplitN(fallbackTitle, ":", 2)
			summary := strings.TrimSpace(parts[1])
			return truncateRunes(summary, maxArgsSummaryLen)
		}
		return truncateRunes(fallbackTitle, maxArgsSummaryLen)
	}
	return ""
}

// normalizeToolStatus maps status strings to ACP canonical statuses
// (pending, in_progress, completed, failed, denied).
func normalizeToolStatus(status string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	switch s {
	case "error":
		return ToolStatusFailed
	case ToolStatusPending, ToolStatusInProgress, ToolStatusCompleted, ToolStatusFailed, ToolStatusDenied:
		return s
	case "":
		return ToolStatusCompleted
	default:
		return s
	}
}

// extractResultInfo extracts result bytes and a <=256 byte preview head.
func extractResultInfo(rawOutput json.RawMessage, explicitHead string, explicitBytes int64, explicitRef string) (int64, string, string) {
	resultBytes := explicitBytes
	resultHead := explicitHead
	resultRef := explicitRef

	if len(rawOutput) > 0 {
		if resultBytes == 0 {
			resultBytes = int64(len(rawOutput))
		}
		if resultHead == "" {
			var str string
			if err := json.Unmarshal(rawOutput, &str); err == nil {
				resultHead = str
			} else {
				resultHead = string(rawOutput)
			}
		}
	}

	if len(resultHead) > 256 {
		resultHead = truncateRunes(resultHead, 256)
	}
	return resultBytes, resultHead, resultRef
}

func mapToolCall(u *rawToolUpdate) *agentv1.ToolCall {
	callID := u.id()
	if callID == "" {
		callID = newToolCallID()
	}
	toolName := u.toolName()
	argsSummary := buildArgsSummary(u.inputPayload(), u.Title)
	denied := normalizeToolStatus(u.Status) == ToolStatusDenied
	return &agentv1.ToolCall{
		CallId:      callID,
		ToolName:    toolName,
		ArgsSummary: argsSummary,
		Denied:      denied,
	}
}

func mapToolCallUpdate(u *rawToolUpdate) *agentv1.ToolCallUpdate {
	callID := u.id()
	toolName := u.toolName()
	status := normalizeToolStatus(u.Status)
	resultBytes, resultHead, resultRef := extractResultInfo(u.outputPayload(), u.ResultHead, u.ResultBytes, u.ResultRef)
	return &agentv1.ToolCallUpdate{
		CallId:      callID,
		ToolName:    toolName,
		Status:      status,
		ResultBytes: resultBytes,
		ResultHead:  resultHead,
		ResultRef:   resultRef,
	}
}

func newToolCallID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
