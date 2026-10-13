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
