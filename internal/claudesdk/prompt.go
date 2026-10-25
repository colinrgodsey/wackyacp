// Package claudesdk drives the Claude Agent SDK directly instead of going
// through the claude-agent-acp adapter, so a bridged claude agent gets the same
// prompt composition and scratchpad access a native wackypub agent has.
//
// Go owns everything wackypub-specific: system prompt composition, skill
// discovery, scratchpad storage, and the session and OAuth refresh locks. A
// small Node worker owns the SDK call itself, because the SDK's public surface
// is TypeScript and re-implementing its internal CLI framing in Go would break
// on the next minor version bump.
package claudesdk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/colinrgodsey/wackypub/pkg/agent"
)

// SystemPromptMode selects how the composed wackypub prompt reaches Claude.
type SystemPromptMode string

const (
	// ModeAppend keeps Claude Code's own preset and appends the wackypub prompt.
	ModeAppend SystemPromptMode = "append"
	// ModeReplace drops Claude Code's preset and sends only the wackypub prompt.
	ModeReplace SystemPromptMode = "replace"
)

// ParseSystemPromptMode validates a --system-prompt-mode flag value. Empty is
// append, which is the documented default.
func ParseSystemPromptMode(raw string) (SystemPromptMode, error) {
	switch SystemPromptMode(strings.TrimSpace(raw)) {
	case "", ModeAppend:
		return ModeAppend, nil
	case ModeReplace:
		return ModeReplace, nil
	default:
		return "", fmt.Errorf("unknown --system-prompt-mode %q: want append or replace", raw)
	}
}

// ScratchpadAddendum is the fixed brief instruction block required by the card.
// It stays short on purpose: the full scratchpad skill is not loaded into
// Claude's prompt, only the discipline needed to address entries and hand them
// to other agents.
const ScratchpadAddendum = `<WACKYPUB_SCRATCHPAD_PROTOCOL>
## Wackypub scratchpad discipline

Your wackypub agent folder has a persistent scratchpad for large outputs, research deposits, reviews, and cross-agent hand-offs. You address entries by ID, never by path.

1. Entry IDs are opaque short slugs (for example varesota-56-review.txt). Never invent one. Use the ID a tool returns.
2. Inspect before reading whole entries: list_scratchpads, then search_scratchpad, then get_scratchpad with a line window, then diff_scratchpad to compare two versions.
3. Hand off large artifacts to another agent by ID, not by pasting body text: deposit the payload with create_scratchpad, then reference it in the message you send to the other agent.
4. Keep one entry per artifact and create a new entry rather than rewriting a good one, so a diff can show what moved.
</WACKYPUB_SCRATCHPAD_PROTOCOL>`

// ComposedPrompt is the rendered system prompt plus the digest that drives the
// SDK snapshot decision.
type ComposedPrompt struct {
	Content string
	SHA256  string
}

// ShouldSnapshot reports whether the SDK may reuse its cached system prompt for
// this turn. An empty previous digest means there is nothing cached yet, which
// is not the same as a match.
func (c *ComposedPrompt) ShouldSnapshot(previousSHA string) bool {
	return previousSHA != "" && previousSHA == c.SHA256
}

// ComposeSystemPromptForAgentFolder renders the agent's AGENTS.md through the
// exact macro expansion and auto-loaded skill block that native wackypub agents
// receive, then appends the scratchpad addendum.
//
// MEMORY.md is deliberately not composed here. Native agents receive it as a
// persistent-memory user turn rather than as system prompt, and deciding where
// it belongs for a bridged agent is a v2 question (card requirement list does
// not include it).
func ComposeSystemPromptForAgentFolder(agentFolder string) (*ComposedPrompt, error) {
	if !filepath.IsAbs(agentFolder) {
		return nil, fmt.Errorf("agent folder must be absolute: %q", agentFolder)
	}
	base, err := agent.RenderAgentSystemPrompt(filepath.Dir(agentFolder), filepath.Base(agentFolder))
	if err != nil {
		return nil, fmt.Errorf("composing system prompt for %s: %w", agentFolder, err)
	}
	content := strings.TrimRight(base, " \t\r") + "\n\n" + ScratchpadAddendum
	sum := sha256.Sum256([]byte(content))
	return &ComposedPrompt{Content: content, SHA256: hex.EncodeToString(sum[:])}, nil
}
