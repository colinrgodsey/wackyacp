package acp

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestToolCallInfo_InputPayloadPrecedence pins the input-field resolution
// contract on the permission-request toolCall decode: the spec's rawInput
// beats the dialect aliases, and among the aliases the order is
// input > args > arguments.
func TestToolCallInfo_InputPayloadPrecedence(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"rawInput wins over all", `{ "toolCallId":"1","rawInput":{"a":1},"input":{"b":2},"args":{"c":3},"arguments":{"d":4} }`, "a"},
		{"input beats args and arguments", `{"toolCallId":"1","input":{"b":2},"args":{"c":3},"arguments":{"d":4}}`, "b"},
		{"args beats arguments", `{"toolCallId":"1","args":{"c":3},"arguments":{"d":4}}`, "c"},
		{"arguments alone", `{"toolCallId":"1","arguments":{"d":4}}`, "d"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var info ToolCallInfo
			if err := json.Unmarshal([]byte(tc.raw), &info); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got := string(info.inputPayload()); !strings.Contains(got, tc.want) {
				t.Errorf("inputPayload = %s, want to contain %q", got, tc.want)
			}
		})
	}

	var none ToolCallInfo
	if err := json.Unmarshal([]byte(`{"toolCallId":"1"}`), &none); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if none.inputPayload() != nil {
		t.Errorf("inputPayload with no input fields = %s, want nil", none.inputPayload())
	}
}

// TestToolCallInfo_DecodesDialectFields pins that the harness-dialect aliases
// keep decoding: a toolCall in the wire shape the agy bridge sends
// (toolName + input) must keep its name and its payload.
func TestToolCallInfo_DecodesDialectFields(t *testing.T) {
	var params PermissionRequestParams
	raw := `{"sessionId":"s1","toolCall":{"toolCallId":"c1","toolName":"run_command","title":"run","input":{"cmd":"ls"}},"options":[{"optionId":"o1","name":"Allow","kind":"allow_once"}]}`
	if err := json.Unmarshal([]byte(raw), &params); err != nil {
		t.Fatalf("unmarshal permission params: %v", err)
	}
	if params.ToolCall.ToolName != "run_command" {
		t.Errorf("ToolName = %q, want run_command", params.ToolCall.ToolName)
	}
	if got := string(params.ToolCall.inputPayload()); !strings.Contains(got, `"cmd":"ls"`) {
		t.Errorf("inputPayload = %s, want the input object", got)
	}
}
