package acp

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExtractToolName(t *testing.T) {
	tests := []struct {
		name     string
		toolName string
		nam      string
		title    string
		kind     string
		want     string
	}{
		{name: "toolName priority", toolName: "read_file", nam: "other", title: "Read: /tmp", kind: "read", want: "read_file"},
		{name: "name fallback", nam: "write_file", title: "Write: /tmp", kind: "write", want: "write_file"},
		{name: "title with colon", title: "Bash: git status", kind: "execute", want: "Bash"},
		{name: "title without colon", title: "execute_command", kind: "execute", want: "execute_command"},
		{name: "kind fallback", kind: "fetch", want: "fetch"},
		{name: "all empty fallback", want: "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractToolName(tc.toolName, tc.nam, tc.title, tc.kind)
			if got != tc.want {
				t.Fatalf("extractToolName() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRedactSecretShapedValues(t *testing.T) {
	input := map[string]any{
		"token":    "ghp_1234567890",
		"API_KEY":  "sk-secret123",
		"password": "supersecret",
		"auth":     "Bearer xyz",
		"safe_arg": "hello world",
		"nested": map[string]any{
			"api_secret": "sensitive",
			"count":      42,
		},
	}

	redacted := redactSecretShapedValues(input)
	if redacted["token"] != "[REDACTED]" {
		t.Errorf("token not redacted: %v", redacted["token"])
	}
	if redacted["API_KEY"] != "[REDACTED]" {
		t.Errorf("API_KEY not redacted: %v", redacted["API_KEY"])
	}
	if redacted["password"] != "[REDACTED]" {
		t.Errorf("password not redacted: %v", redacted["password"])
	}
	if redacted["auth"] != "[REDACTED]" {
		t.Errorf("auth not redacted: %v", redacted["auth"])
	}
	if redacted["safe_arg"] != "hello world" {
		t.Errorf("safe_arg altered: %v", redacted["safe_arg"])
	}
	nested, ok := redacted["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested is not map: %T", redacted["nested"])
	}
	if nested["api_secret"] != "[REDACTED]" {
		t.Errorf("nested api_secret not redacted: %v", nested["api_secret"])
	}
	if nested["count"] != 42 {
		t.Errorf("nested count altered: %v", nested["count"])
	}
}

func TestBuildArgsSummary(t *testing.T) {
	// JSON object with secrets
	raw := json.RawMessage(`{"command":"ls -la","api_key":"secret123"}`)
	summary := buildArgsSummary(raw, "")
	if summary != "api_key=[REDACTED], command=ls -la" {
		t.Fatalf("unexpected summary: %q", summary)
	}

	// Truncation at 200 runes
	longStr := strings.Repeat("a", 300)
	rawLong := json.RawMessage(`{"arg":"` + longStr + `"}`)
	summaryLong := buildArgsSummary(rawLong, "")
	if len([]rune(summaryLong)) > 200 {
		t.Fatalf("summary length %d exceeds 200 runes", len([]rune(summaryLong)))
	}

	// Fallback title with colon
	summaryTitle := buildArgsSummary(nil, "Bash: echo hello")
	if summaryTitle != "echo hello" {
		t.Fatalf("title summary = %q, want 'echo hello'", summaryTitle)
	}

	// Fallback title without colon
	summaryTitleNoColon := buildArgsSummary(nil, "simple_call")
	if summaryTitleNoColon != "simple_call" {
		t.Fatalf("title summary = %q, want 'simple_call'", summaryTitleNoColon)
	}
}

func TestNormalizeToolStatus(t *testing.T) {
	cases := map[string]string{
		"pending":     ToolStatusPending,
		"Pending ":    ToolStatusPending,
		"in_progress": ToolStatusInProgress,
		"completed":   ToolStatusCompleted,
		"":            ToolStatusCompleted,
		"failed":      ToolStatusFailed,
		"error":       ToolStatusFailed,
		"denied":      ToolStatusDenied,
		"DENIED":      ToolStatusDenied,
	}

	for in, want := range cases {
		got := normalizeToolStatus(in)
		if got != want {
			t.Errorf("normalizeToolStatus(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestExtractResultInfo(t *testing.T) {
	rawOutput := json.RawMessage(`"hello world output"`)
	bytes, head, ref := extractResultInfo(rawOutput, "", 0, "")
	if bytes != int64(len(rawOutput)) {
		t.Errorf("bytes = %d, want %d", bytes, len(rawOutput))
	}
	if head != "hello world output" {
		t.Errorf("head = %q, want 'hello world output'", head)
	}
	if ref != "" {
		t.Errorf("ref = %q, want empty", ref)
	}

	// Long output capped at 256 runes
	longOut := `"` + strings.Repeat("x", 500) + `"`
	_, headLong, _ := extractResultInfo(json.RawMessage(longOut), "", 0, "")
	if len([]rune(headLong)) > 256 {
		t.Errorf("head length %d exceeds 256 runes", len([]rune(headLong)))
	}

	// Explicit override preserves explicit values
	bytesExp, headExp, refExp := extractResultInfo(nil, "custom head", 1024, "artifact://1")
	if bytesExp != 1024 || headExp != "custom head" || refExp != "artifact://1" {
		t.Errorf("explicit info not preserved: %d, %q, %q", bytesExp, headExp, refExp)
	}
}

func TestMapToolCallAndCallUpdate(t *testing.T) {
	t.Run("flat tool_call", func(t *testing.T) {
		u := &rawToolUpdate{
			ToolCallID: "tc-123",
			ToolName:   "bash",
			RawInput:   json.RawMessage(`{"command":"echo test"}`),
			Status:     "in_progress",
		}
		call := mapToolCall(u)
		if call.CallId != "tc-123" {
			t.Errorf("CallId = %q, want tc-123", call.CallId)
		}
		if call.ToolName != "bash" {
			t.Errorf("ToolName = %q, want bash", call.ToolName)
		}
		if call.ArgsSummary != "command=echo test" {
			t.Errorf("ArgsSummary = %q, want 'command=echo test'", call.ArgsSummary)
		}
		if call.Denied {
			t.Errorf("Denied = true, want false")
		}
	})

	t.Run("nested toolCall", func(t *testing.T) {
		u := &rawToolUpdate{
			ToolCall: &rawToolUpdate{
				ToolCallID: "tc-nested",
				ToolName:   "file_reader",
				RawInput:   json.RawMessage(`{"path":"/etc/hosts"}`),
			},
		}
		call := mapToolCall(u)
		if call.CallId != "tc-nested" {
			t.Errorf("CallId = %q, want tc-nested", call.CallId)
		}
		if call.ToolName != "file_reader" {
			t.Errorf("ToolName = %q, want file_reader", call.ToolName)
		}
		if call.ArgsSummary != "path=/etc/hosts" {
			t.Errorf("ArgsSummary = %q, want 'path=/etc/hosts'", call.ArgsSummary)
		}
	})

	t.Run("tool_call_update with status and output", func(t *testing.T) {
		u := &rawToolUpdate{
			ToolCallID: "tc-123",
			ToolName:   "bash",
			Status:     "completed",
			RawOutput:  json.RawMessage(`"line 1\nline 2"`),
		}
		up := mapToolCallUpdate(u)
		if up.CallId != "tc-123" {
			t.Errorf("CallId = %q, want tc-123", up.CallId)
		}
		if up.ToolName != "bash" {
			t.Errorf("ToolName = %q, want bash", up.ToolName)
		}
		if up.Status != "completed" {
			t.Errorf("Status = %q, want completed", up.Status)
		}
		if up.ResultHead != "line 1\nline 2" {
			t.Errorf("ResultHead = %q, want 'line 1\\nline 2'", up.ResultHead)
		}
		if up.ResultBytes <= 0 {
			t.Errorf("ResultBytes = %d, want > 0", up.ResultBytes)
		}
	})
}
