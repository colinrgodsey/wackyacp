package acp

import (
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

// ExtractModelInfo parses the raw configOptions JSON payload from an ACP harness.
// It locates the model option (by id, key, or category == "model") and extracts
// the current model and available options list.
func ExtractModelInfo(raw json.RawMessage, fallback string) ModelInfo {
	info := ModelInfo{
		CurrentModel: fallback,
		RawOptions:   []any{},
	}
	if len(raw) == 0 {
		return info
	}

	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		info.RawOptions = raw
		return info
	}

	var optionsList []any
	if obj, ok := parsed.(map[string]any); ok {
		if co, exists := obj["configOptions"]; exists {
			if list, ok := co.([]any); ok {
				optionsList = list
			}
		}
	} else if list, ok := parsed.([]any); ok {
		optionsList = list
	}

	for _, item := range optionsList {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id, _ := m["id"].(string)
		key, _ := m["key"].(string)
		cat, _ := m["category"].(string)
		if id != "model" && key != "model" && cat != "model" {
			continue
		}

		if cv, ok := m["currentValue"].(string); ok && cv != "" {
			info.CurrentModel = cv
		} else if sel, ok := m["selected"].(string); ok && sel != "" {
			info.CurrentModel = sel
		} else if val, ok := m["value"].(string); ok && val != "" {
			info.CurrentModel = val
		}

		if rawOpts, ok := m["options"].([]any); ok {
			for _, opt := range rawOpts {
				if optMap, ok := opt.(map[string]any); ok {
					choice := ModelChoice{}
					if v, ok := optMap["value"].(string); ok {
						choice.ID = v
					} else if idVal, ok := optMap["id"].(string); ok {
						choice.ID = idVal
					}
					if n, ok := optMap["name"].(string); ok {
						choice.Name = n
					}
					if l, ok := optMap["label"].(string); ok {
						choice.Label = l
					}
					if choice.ID != "" {
						info.Options = append(info.Options, choice)
					}
				}
			}
		}

		if info.CurrentModel == "" && len(info.Options) > 0 {
			info.CurrentModel = info.Options[0].ID
		}
		break
	}

	if info.CurrentModel == "" {
		info.CurrentModel = fallback
	}

	if len(optionsList) > 0 {
		info.RawOptions = optionsList
	} else {
		info.RawOptions = parsed
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
