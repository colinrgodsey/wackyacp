package acp

import "testing"

// TestMethodNamesMatchSpec pins the wire method names against the ACP v1
// schema. The set_config_option casing drifted once (audit
// tasks/wackyacp/code-cleanliness-audit, finding 6); this is what keeps the
// constants on the spec spelling.
func TestMethodNamesMatchSpec(t *testing.T) {
	got := map[string]string{
		"initialize":                 MethodInitialize,
		"session/new":                MethodSessionNew,
		"session/resume":             MethodSessionResume,
		"session/load":               MethodSessionLoad,
		"session/prompt":             MethodSessionPrompt,
		"session/cancel":             MethodSessionCancel,
		"session/update":             MethodSessionUpdate,
		"session/close":              MethodSessionClose,
		"session/request_permission": MethodSessionRequestPermission,
		"session/set_config_option":  MethodSessionSetConfigOption,
	}
	for want, name := range got {
		if name != want {
			t.Errorf("method %s = %q, want %q", want, name, want)
		}
	}
}

func TestUpdateKindNamesMatchSpec(t *testing.T) {
	got := map[string]string{
		"agent_message_chunk":       UpdateKindAgentMessageChunk,
		"usage_update":              UpdateKindUsageUpdate,
		"tool_call":                 UpdateKindToolCall,
		"tool_call_update":          UpdateKindToolCallUpdate,
		"session_info_update":       UpdateKindSessionInfoUpdate,
		"available_commands_update": UpdateKindAvailableCommandsUpdate,
		"agent_thought_chunk":       UpdateKindAgentThoughtChunk,
	}
	for want, name := range got {
		if name != want {
			t.Errorf("update kind %s = %q, want %q", want, name, want)
		}
	}
}
