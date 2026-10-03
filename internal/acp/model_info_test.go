package acp

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestIsModelSlashCommand(t *testing.T) {
	tests := []struct {
		input       string
		wantIsModel bool
		wantTarget  string
		wantIsQuery bool
	}{
		{"", false, "", false},
		{"   ", false, "", false},
		{"hello world", false, "", false},
		{"/modeltrain", false, "", false},
		{"/model", true, "", true},
		{"/models", true, "", true},
		{"  /model  ", true, "", true},
		{"/MODEL", true, "", true},
		{"/models   ", true, "", true},
		{"/model list", true, "", true},
		{"/model get", true, "", true},
		{"/model ?", true, "", true},
		{"/model -h", true, "", true},
		{"/model --help", true, "", true},
		{"/model help", true, "", true},
		{"/model sonnet", true, "sonnet", false},
		{"/model set sonnet", true, "sonnet", false},
		{"/models opus", true, "opus", false},
		{"/model gemini-3.1-pro-high", true, "gemini-3.1-pro-high", false},
		{"/model   claude-sonnet-4-6  ", true, "claude-sonnet-4-6", false},
	}

	for _, tc := range tests {
		isModel, target, isQuery := IsModelSlashCommand(tc.input)
		if isModel != tc.wantIsModel || target != tc.wantTarget || isQuery != tc.wantIsQuery {
			t.Errorf("IsModelSlashCommand(%q) = (%v, %q, %v); want (%v, %q, %v)",
				tc.input, isModel, target, isQuery, tc.wantIsModel, tc.wantTarget, tc.wantIsQuery)
		}
	}
}

func TestExtractModelInfo(t *testing.T) {
	raw := json.RawMessage(`[
		{
			"id": "model",
			"name": "Model",
			"category": "model",
			"type": "select",
			"currentValue": "gemini-3.8-flash-high",
			"options": [
				{"value": "gemini-3.8-flash-high", "name": "Gemini 3.8 Flash (High)"},
				{"value": "gemini-3.1-pro-high", "name": "Gemini 3.1 Pro (High)"}
			]
		}
	]`)

	info := ExtractModelInfo(raw, "fallback")
	if info.CurrentModel != "gemini-3.8-flash-high" {
		t.Errorf("CurrentModel = %q, want gemini-3.8-flash-high", info.CurrentModel)
	}
	if len(info.Options) != 2 {
		t.Fatalf("len(Options) = %d, want 2", len(info.Options))
	}
	if info.Options[0].ID != "gemini-3.8-flash-high" || info.Options[0].Name != "Gemini 3.8 Flash (High)" {
		t.Errorf("option 0 = %+v", info.Options[0])
	}

	// Test object wrapper { "configOptions": [...] }
	wrapped := json.RawMessage(`{
		"configOptions": [
			{
				"key": "model",
				"selected": "sonnet",
				"options": [
					{"value": "sonnet", "label": "Claude 3.5 Sonnet"},
					{"value": "opus", "label": "Claude 3 Opus"}
				]
			}
		]
	}`)
	infoWrapped := ExtractModelInfo(wrapped, "fallback")
	if infoWrapped.CurrentModel != "sonnet" {
		t.Errorf("infoWrapped.CurrentModel = %q, want sonnet", infoWrapped.CurrentModel)
	}
	if len(infoWrapped.Options) != 2 || infoWrapped.Options[1].DisplayLabel() != "Claude 3 Opus" {
		t.Errorf("infoWrapped.Options = %+v", infoWrapped.Options)
	}

	// Test empty raw message
	emptyInfo := ExtractModelInfo(nil, "fallback")
	if emptyInfo.CurrentModel != "fallback" || len(emptyInfo.Options) != 0 {
		t.Errorf("emptyInfo = %+v", emptyInfo)
	}
}

func TestFormatModelList(t *testing.T) {
	info := ModelInfo{
		CurrentModel: "sonnet",
		Options: []ModelChoice{
			{ID: "sonnet", Name: "Claude 3.5 Sonnet"},
			{ID: "opus", Name: "Claude 3 Opus"},
		},
	}
	out := FormatModelList(info)
	if !strings.Contains(out, "Current model: **sonnet** (Claude 3.5 Sonnet)") {
		t.Errorf("expected current model with label, got:\n%s", out)
	}
	if !strings.Contains(out, "• **sonnet** — Claude 3.5 Sonnet") {
		t.Errorf("expected option sonnet, got:\n%s", out)
	}
	if !strings.Contains(out, "• **opus** — Claude 3 Opus") {
		t.Errorf("expected option opus, got:\n%s", out)
	}
	if !strings.Contains(out, "messages starting with `/model` run as session commands in the single-turn slot") {
		t.Errorf("expected single-turn slot note, got:\n%s", out)
	}

	// Test empty options
	emptyOut := FormatModelList(ModelInfo{CurrentModel: "gemini"})
	if !strings.Contains(emptyOut, "Current model: **gemini**") || !strings.Contains(emptyOut, "No selectable models") {
		t.Errorf("expected empty options formatting, got:\n%s", emptyOut)
	}
}

func TestFormatModelSetSuccessAndError(t *testing.T) {
	info := ModelInfo{
		CurrentModel: "opus",
		Options: []ModelChoice{
			{ID: "opus", Name: "Claude 3 Opus"},
		},
	}
	success := FormatModelSetSuccess("opus", info)
	if success != "Model set to **opus** (Claude 3 Opus)." {
		t.Errorf("FormatModelSetSuccess = %q", success)
	}

	rawErr := errors.New("timeout dialing harness")
	errOut := FormatModelSetError("gpt-4", rawErr)
	if !strings.Contains(errOut, "Failed to set model to \"gpt-4\": timeout dialing harness") {
		t.Errorf("FormatModelSetError = %q", errOut)
	}

	cfgErr := &ConfigOptionError{Code: -32602, Message: "unknown model: bad-model"}
	cfgErrOut := FormatModelSetError("bad-model", cfgErr)
	if !strings.Contains(cfgErrOut, "Failed to set model to \"bad-model\": unknown model: bad-model") {
		t.Errorf("FormatModelSetError with ConfigOptionError = %q", cfgErrOut)
	}
}
