package claudesdk

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// promptStateFileName holds the digest of the system prompt the last turn sent.
// Persisting it is what keeps Claude's prompt cache warm across bridge restarts;
// losing it only costs one cache miss, so a write failure is logged and ignored
// rather than failing a turn.
const promptStateFileName = ".wackyacp-prompt-state.json"

type promptState struct {
	PromptSHA string `json:"promptSha"`
}

func promptStatePath(agentFolder string) string {
	return filepath.Join(agentFolder, promptStateFileName)
}

// readPromptSHA returns the digest the previous bridge sent, or "" when there is
// none. An unreadable file means "no cache", not an error.
func readPromptSHA(agentFolder string) string {
	data, err := os.ReadFile(promptStatePath(agentFolder))
	if err != nil {
		return ""
	}
	var state promptState
	if err := json.Unmarshal(data, &state); err != nil {
		return ""
	}
	return state.PromptSHA
}

// writePromptSHA records the digest for the next bridge to compare against.
func writePromptSHA(agentFolder, sha string) {
	if sha == "" {
		return
	}
	data, err := json.Marshal(promptState{PromptSHA: sha})
	if err != nil {
		return
	}
	if err := os.WriteFile(promptStatePath(agentFolder), data, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "wackyacp claudesdk: writing prompt state:", err)
	}
}
