package acp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ModelChoice represents a selectable model option advertised by an ACP harness.
type ModelChoice struct {
	ID    string `json:"id"`
	Name  string `json:"name,omitempty"`
	Label string `json:"label,omitempty"`
}

// DisplayLabel returns the human-friendly display name, label, or fallback ID.
func (m ModelChoice) DisplayLabel() string {
	if m.Label != "" {
		return m.Label
	}
	if m.Name != "" {
		return m.Name
	}
	return m.ID
}

// ModelInfo holds parsed model configuration options from an ACP harness.
type ModelInfo struct {
	CurrentModel string
	Options      []ModelChoice
	RawOptions   any
}

// ConfigOption is one entry of a harness configOptions array. The ACP spec
// models the model selector with id/currentValue; the claude-acp and wackyagy
// dialects use the key/category/selected/value aliases instead. Every field
// name real harnesses send on the wire stays decodable - dropping one is a
// bridge break, not a cleanup.
type ConfigOption struct {
	ID           string         `json:"id,omitempty"`
	Key          string         `json:"key,omitempty"`
	Category     string         `json:"category,omitempty"`
	CurrentValue string         `json:"currentValue,omitempty"`
	Selected     string         `json:"selected,omitempty"`
	Value        string         `json:"value,omitempty"`
	Options      []ConfigChoice `json:"options,omitempty"`
}

// ConfigChoice is one selectable option under a ConfigOption. Harnesses send
// either value or id for the option's identity (value first).
type ConfigChoice struct {
	Value string `json:"value,omitempty"`
	ID    string `json:"id,omitempty"`
	Name  string `json:"name,omitempty"`
	Label string `json:"label,omitempty"`
}

func (c ConfigOption) isModelOption() bool {
	return c.ID == "model" || c.Key == "model" || c.Category == "model"
}

// currentModel resolves the current model across the spec (currentValue) and
// dialect (selected, value) field names.
func (c ConfigOption) currentModel() string {
	switch {
	case c.CurrentValue != "":
		return c.CurrentValue
	case c.Selected != "":
		return c.Selected
	}
	return c.Value
}

func (c ConfigOption) choices() []ModelChoice {
	var out []ModelChoice
	for _, ch := range c.Options {
		id := ch.Value
		if id == "" {
			id = ch.ID
		}
		if id == "" {
			continue
		}
		out = append(out, ModelChoice{ID: id, Name: ch.Name, Label: ch.Label})
	}
	return out
}

// decodeConfigOptions extracts the configOptions array from a harness payload.
// Harnesses wrap it two ways: a bare [...] array or a {"configOptions": [...]}
// object. It returns the array's raw bytes (for verbatim passthrough) and
// whether one of the two shapes was found.
func decodeConfigOptions(raw json.RawMessage) (json.RawMessage, bool) {
	var list json.RawMessage
	if err := json.Unmarshal(raw, &list); err == nil && isJSONArray(list) {
		return list, true
	}
	var wrapped struct {
		ConfigOptions json.RawMessage `json:"configOptions"`
	}
	if err := json.Unmarshal(raw, &wrapped); err == nil && isJSONArray(wrapped.ConfigOptions) {
		return wrapped.ConfigOptions, true
	}
	return nil, false
}

func isJSONArray(b json.RawMessage) bool {
	t := bytes.TrimLeft(b, " \t\r\n")
	return len(t) > 0 && t[0] == '['
}

// ExtractModelInfo parses the raw configOptions payload from an ACP harness.
// It locates the model option (by id, key, or category == "model") and
// extracts the current model and available options list.
func ExtractModelInfo(raw json.RawMessage, fallback string) ModelInfo {
	info := ModelInfo{
		CurrentModel: fallback,
		RawOptions:   []any{},
	}
	if len(raw) == 0 {
		return info
	}

	listRaw, found := decodeConfigOptions(raw)
	if !found {
		// Unknown container shape: surface the payload verbatim so callers
		// can still show it.
		var parsed any
		if err := json.Unmarshal(raw, &parsed); err != nil {
			info.RawOptions = raw
		} else {
			info.RawOptions = parsed
		}
		return info
	}

	var options []ConfigOption
	if err := json.Unmarshal(listRaw, &options); err != nil {
		info.RawOptions = raw
		return info
	}

	for _, opt := range options {
		if !opt.isModelOption() {
			continue
		}
		if m := opt.currentModel(); m != "" {
			info.CurrentModel = m
		}
		info.Options = opt.choices()
		break
	}

	if info.CurrentModel == "" && len(info.Options) > 0 {
		info.CurrentModel = info.Options[0].ID
	}
	if info.CurrentModel == "" {
		info.CurrentModel = fallback
	}

	if isJSONArray(listRaw) && len(bytes.TrimLeft(listRaw, " \t\r\n")) > 2 {
		info.RawOptions = json.RawMessage(listRaw)
	} else {
		info.RawOptions = json.RawMessage(raw)
	}

	return info
}

// IsModelSlashCommand checks whether input text is a /model slash command.
func IsModelSlashCommand(text string) (isModel bool, targetModel string, isQuery bool) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return false, "", false
	}
	lower := strings.ToLower(trimmed)
	var rest string
	if lower == "/model" || lower == "/models" {
		return true, "", true
	} else if strings.HasPrefix(lower, "/model ") || strings.HasPrefix(lower, "/model\t") {
		rest = strings.TrimSpace(trimmed[6:])
	} else if strings.HasPrefix(lower, "/models ") || strings.HasPrefix(lower, "/models\t") {
		rest = strings.TrimSpace(trimmed[7:])
	} else {
		return false, "", false
	}

	lowerRest := strings.ToLower(rest)
	if rest == "" || lowerRest == "get" || lowerRest == "list" || lowerRest == "?" || lowerRest == "-h" || lowerRest == "--help" || lowerRest == "help" {
		return true, "", true
	}
	if strings.HasPrefix(lowerRest, "set ") {
		rest = strings.TrimSpace(rest[4:])
	}
	return true, rest, false
}

// FormatModelList formats available model options into a readable response.
func FormatModelList(info ModelInfo) string {
	var sb strings.Builder
	currentDisplay := fmt.Sprintf("**%s**", info.CurrentModel)
	for _, opt := range info.Options {
		if opt.ID == info.CurrentModel && opt.DisplayLabel() != opt.ID {
			currentDisplay = fmt.Sprintf("**%s** (%s)", opt.ID, opt.DisplayLabel())
			break
		}
	}

	if info.CurrentModel != "" {
		sb.WriteString(fmt.Sprintf("Current model: %s\n", currentDisplay))
	} else {
		sb.WriteString("Current model: *(none)*\n")
	}

	if len(info.Options) == 0 {
		sb.WriteString("\n*(No selectable models advertised by harness)*")
		return sb.String()
	}

	sb.WriteString("\nAvailable models:\n")
	for _, opt := range info.Options {
		display := opt.DisplayLabel()
		if display != "" && display != opt.ID {
			sb.WriteString(fmt.Sprintf("  • **%s** — %s\n", opt.ID, display))
		} else {
			sb.WriteString(fmt.Sprintf("  • **%s**\n", opt.ID))
		}
	}
	sb.WriteString("\nTo switch models, type: `/model <name>`\n*(Note: messages starting with `/model` run as session commands in the single-turn slot)*")
	return sb.String()
}

// FormatModelSetSuccess formats a successful model switch confirmation.
func FormatModelSetSuccess(confirmedModel string, info ModelInfo) string {
	display := fmt.Sprintf("**%s**", confirmedModel)
	for _, opt := range info.Options {
		if opt.ID == confirmedModel && opt.DisplayLabel() != opt.ID {
			display = fmt.Sprintf("**%s** (%s)", confirmedModel, opt.DisplayLabel())
			break
		}
	}
	return fmt.Sprintf("Model set to %s.", display)
}

// FormatModelSetError formats a model switch error.
func FormatModelSetError(target string, err error) string {
	var cfgErr *ConfigOptionError
	msg := err.Error()
	if errors.As(err, &cfgErr) {
		msg = cfgErr.Message
	}
	return fmt.Sprintf("Failed to set model to %q: %s\nUse `/model` to see available models.", target, msg)
}
